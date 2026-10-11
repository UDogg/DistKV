# Raft implementation notes

`internal/raft` implements Raft as described in the extended paper (Ongaro & Ousterhout, 2014), plus
the production extensions from Ongaro's thesis that DistKV depends on. This document describes the
design and argues why each deviation from "textbook" Raft is safe.

## Structure

A `Node` keeps all consensus state under one mutex and **never performs I/O to peers while holding
it**. Work is split across goroutines:

| Goroutine | Role |
|---|---|
| ticker | Election deadline (randomised in `[T, 2T)`), check-quorum on leaders |
| replicator (per follower) | Sends `AppendEntries` / `InstallSnapshot`; one RPC in flight, batching everything appended since the last one |
| syncer | Leader-side group commit: makes the leader's own appended entries durable and advances its `durableIndex` |
| applier | Applies committed entries in order, delivers results to proposers, takes snapshots |
| read loop | Batches ReadIndex requests into heartbeat rounds |

The in-memory log (`raftLog`) holds every entry after a *sentinel* (index and term of the last
compacted entry). Storage is a write-only mirror that is read only during recovery.

## Elections

* **Pre-vote (§9.6).** A node whose election timer fires first asks "would you vote for me in
  term+1?" without changing anyone's term. It starts a real election only if a quorum says yes. A
  node rejoining after a partition therefore cannot force a healthy leader to step down by
  inflating the term. *Test:* `TestPreVotePreventsDisruption` partitions a follower for 10+
  election timeouts and checks that neither the term nor the leader changes when it rejoins.
* **Leader stickiness.** A node that heard from a leader within the last election timeout rejects
  pre-votes and votes (unless the candidate is the target of a leadership transfer).
* **Check-quorum (§6.2).** Once per election timeout, a leader that has not heard from a quorum
  steps down, so clients stop waiting on a leader that cannot commit. *Test:*
  `TestCheckQuorumStepDown`.
* **No-op on election.** A new leader appends a no-op entry for its term. Entries from earlier terms
  are committed only indirectly, by committing a current-term entry (§5.4.2, the Figure 8 problem).
  The no-op also marks when the leader's commit index becomes trustworthy for reads.

## Replication

* **Fast back-tracking.** A rejecting follower returns `ConflictTerm` (the term of its entry at
  `PrevLogIndex`) and the first index of that term, or its log length if the log is too short. The
  leader jumps past the whole conflicting term instead of decrementing one entry at a time.
  *Test:* `TestLogBackup` (50 divergent entries in a minority partition).
* **Batching instead of pipelining.** Each follower has at most one RPC in flight. Every RPC
  carries all entries appended since the previous one (up to 1024 entries / 4 MiB), so throughput
  scales with batch size rather than round trips.
* **Committed entries are never overwritten.** A follower asked to truncate at or below its commit
  index aborts the process: this would indicate a safety bug, and it must not be masked.

## Durability and group commit

Raft's safety requires that a node only acknowledges what it has made durable. The current term and
vote must also be durable before a node responds to any RPC. DistKV meets both requirements while
avoiding a separate fsync per request:

* **Hard state** (term, vote) is written and synced before a vote is granted or a term change is
  acted on.
* **Leader.** `Propose` appends to the in-memory log and the storage buffer, then signals the
  syncer. The syncer syncs everything appended so far and advances `durableIndex`. When the leader
  counts the quorum for commitment, its own match index is `durableIndex`, never the last appended
  index. One fsync covers every proposal that arrived during the previous one.
* **Follower.** `HandleAppendEntries` appends under the lock, then **releases the lock while
  syncing**, so heartbeats, votes and the applier aren't blocked by the disk. Two counters keep
  this safe:
  * `durableIndex`: a follower never acknowledges entries beyond it. If a later RPC finds the
    entries already present but not yet synced, it syncs before acknowledging, rather than
    answering "success" on the strength of the in-memory log.
  * `logEpoch`, incremented on every truncation. After re-acquiring the lock, a syncer only
    advances `durableIndex` if no truncation happened meanwhile. If the term changed while it
    synced, it reports the new term instead of acknowledging.
* **Shared log engine.** Every shard on a node writes to one WAL. Concurrent `Sync` calls from
  different shards collapse into one `fdatasync` (see [ARCHITECTURE.md](ARCHITECTURE.md#storage-the-shared-log-engine)).

*Tests:* the in-memory test storage stages unsynced appends, and a simulated crash
(`DropUnsynced`) discards them. `TestPersistence`, `TestUnreliableChurn` and `TestRaftOnLogEngine`
crash and restart nodes and check, on every apply, that no two nodes ever apply different commands
at the same index.

## Snapshots and compaction

The applier snapshots the state machine every `SnapshotThreshold` applied entries. Because Apply,
Snapshot and Restore run on the same goroutine, the snapshot is consistent at `lastApplied` without
extra locking. The log is compacted to `snapshotIndex - SnapshotTrailing`. Keeping a trailing window
lets followers that are slightly behind catch up from the log instead of receiving a full snapshot.

A leader whose `nextIndex` for a follower falls at or below the sentinel sends `InstallSnapshot`. The
follower persists the snapshot, keeps its log suffix only if that suffix contains the snapshot's last
entry with a matching term, and hands the snapshot to the applier. Storage refuses to replace a
snapshot with an older one, so a local snapshot racing with an installed one can never go backwards.

On restart the node reconciles its log with its snapshot. If a crash interrupted an install and the
log does not contain the snapshot's last entry, the log is discarded in favour of the snapshot.

## Linearizable reads

### ReadIndex (§6.4)

The leader records its commit index (which is up to date once its no-op has committed), confirms it
is still leader through a round of heartbeats acknowledged by a quorum *sent after the read
arrived*, waits until the applier reaches that index, then reads locally. Reads that arrive during a
round wait for the next one, so a single round serves any number of concurrent reads. *Tests:*
`TestReadIndex` (an isolated leader must not serve reads) and `TestConcurrentReadIndexBatching`
(200 concurrent reads use far fewer than 200 rounds).

### Leader leases (§6.4.1), the default

For each follower, the leader records the **send time** of the most recent RPC that follower
acknowledged. If a quorum (including the leader) has acknowledged RPCs sent within the last
`0.9 × ElectionTimeout`, the leader holds a lease and serves reads with no network round trip.

**Why this is safe.** A follower acknowledging an RPC sent at time *t* received it at *t' ≥ t*.
Because of leader stickiness, it will not grant a vote until *t' + ElectionTimeout*. A new leader
needs votes from a quorum, and every quorum overlaps the quorum backing the lease. So no new leader
can be elected before *t + ElectionTimeout*, while the lease ends at *t + 0.9 × ElectionTimeout*.
The 10% margin absorbs clock-**rate** drift. Leases measure elapsed time on each machine's monotonic
clock and need no synchronised clocks.

Leases are disabled in three situations, where the argument above would not hold:

* While a leadership transfer is in progress, and for the rest of the term once `TimeoutNow` has
  been sent: the transfer target's election legitimately bypasses stickiness.
* When check-quorum is disabled, since stickiness is tied to it.
* <a name="leases-and-restarts"></a>**After a restart.** A follower that crashes forgets when it
  last heard from the leader and could otherwise vote immediately, shortening a lease it helped
  grant. Every node therefore refuses votes for one election timeout after starting.
  *Test:* `TestRestartedNodeFreezesVotes`.

*Test:* `TestLeaseExpiresBeforeNewLeaderElected` partitions the leader and polls its lease every
millisecond until a new leader appears. Across repeated trials, the old lease has always expired
first.

`--read-mode readindex` disables leases for environments where clock-rate drift cannot be bounded.

## Leadership transfer (§3.10)

`TransferLeadership(target)` stops accepting proposals, replicates until the target's log matches,
then sends `TimeoutNow`. The target starts an election immediately, flagged as a transfer so voters
ignore stickiness. DistKV uses this in three places:

1. **Graceful shutdown.** A node hands off every shard it leads before exiting, so rolling restarts
   cause no election gap (`TestChaosRollingRestart` sees zero errors).
2. **Leader balancing.** Shards move back to their preferred replica after failures.
3. **The admin API:** `POST /v1/admin/shards/{id}/transfer-leader?to=n2`.

## Client interaction and exactly-once semantics

A proposer whose leader steps down before its entry commits gets `ErrLeadershipLost`, because the
outcome is unknown: the entry may still be committed by the next leader. The HTTP layer maps this to
`503 outcome_unknown`, and the client retries with the **same** session sequence number. The state
machine's session table, itself replicated, then returns the original result rather than applying
the command again. Retried writes are therefore safe even for non-idempotent operations such as
compare-and-set.

## Known limitations

* **Static membership.** Members, shard count and replication factor are fixed for a cluster's
  lifetime; there are no joint-consensus or single-server membership changes. Replacing a failed
  machine means starting a new process with the same ID and address (it catches up by snapshot).
* **No shard splitting or rebalancing of data.** Leadership is balanced; data placement is fixed.
* **In-memory mode** (`--in-memory`) loses the term and vote on restart, so a node could vote twice
  in one term. Use it only for throwaway test clusters.
