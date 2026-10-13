# Architecture

DistKV is a **multi-Raft** system. The keyspace is split into a fixed number of shards, and every
shard is an independent Raft group replicated on `replication_factor` nodes. A node hosts replicas of
many shards and leads some of them. This document covers how the parts fit together. For the
consensus details and safety arguments, see [RAFT.md](RAFT.md).

## Components

| Package | Responsibility |
|---|---|
| `internal/cluster` | Static topology: members, key → shard (consistent hashing), shard → replicas (rendezvous hashing), placement fingerprint |
| `internal/raft` | Raft consensus (`Node`), log storage (`LogEngine`, `FileStorage`, `MemoryStorage`) |
| `internal/kv` | The replicated state machine: a deterministic TTL/LRU cache with sessions |
| `internal/transport` | Raft RPCs over HTTP with a compact binary codec, multiplexed by shard |
| `internal/server` | Node assembly: one `raft.Node` + `kv.Store` per local shard, HTTP API, routing and forwarding, leader balancing, expiry, metrics, graceful shutdown |
| `pkg/client` | Topology-aware Go client with leader caching, retries and exactly-once writes |
| `internal/tlsutil` | TLS 1.3 server/peer/client configurations, dev CA generation |

## Sharding

### Key → shard: consistent hashing with virtual nodes

Each shard owns `virtual_nodes` (default 128) points on a 64-bit hash ring. A key belongs to the
shard owning the first point clockwise from `hash(key)`. The hash is FNV-1a with a SplitMix64
finaliser: platform-independent, fast (≈35 ns and zero allocations per key) and well distributed. Tests check that the
coefficient of variation of shard sizes stays under 15% and that adding a shard moves only about
1/(N+1) of the keys.

The shard count is fixed when a cluster is created. Because the ring is consistent, future
shard-splitting support would move only the affected arcs.

### Shard → replicas: rendezvous hashing

For each shard, every member gets the score `hash(shard, member)`, and the top
`replication_factor` members host it. Rendezvous hashing needs no ring maintenance, spreads replicas
evenly (verified in tests), and gives each shard a deterministic **preferred replica**: the
highest-scoring member, which the leader balancer treats as its ideal leader.

Every node and every smart client computes the same topology from the same `Spec`. Data
placement is therefore a pure function of configuration and needs no coordination service.
Each node persists a fingerprint of the placement-relevant settings (`shards`, `replication`,
`vnodes`, member IDs) in `<data-dir>/CLUSTER`. A node started with an incompatible configuration
refuses to run rather than silently misrouting data.

## Request routing

```
client ──▶ any node ──┬─ local leader of the key's shard?  serve
                      ├─ client set X-Distkv-No-Forward?   421 + leader hint
                      └─ otherwise                         proxy to the known leader
                                                           (or a replica, which proxies once more)
```

* **Smart clients** (the Go client and the CLI) send `X-Distkv-No-Forward`. They receive
  `421 Misdirected Request` together with the leader's ID and address, cache it per shard, and
  from then on reach leaders in a single hop.
* **Plain HTTP clients** (curl, load balancers) can send any request to any node. Followers and
  nodes that don't host the shard proxy it, with at most two hops (`X-Distkv-Hops` prevents loops).
* **Stale reads** (`?consistency=stale`) are served by whichever replica receives them, from local
  state.

## The replicated state machine (`kv.Store`)

Every mutation is a `kv.Command` in a compact varint encoding. `Apply` must be deterministic, so it
never reads the wall clock:

* **Time.** The proposing leader stamps each command with its clock (`Now`). The store keeps a
  replicated *logical clock* (`max(Now)` over applied commands) and expires keys against it, using
  a min-heap of deadlines. Reads hide keys whose deadline has passed by the reader's wall clock, so
  a TTL is honoured to within clock skew even before physical removal. On idle shards the leader
  proposes a no-op about once per second if any key is due, so memory is reclaimed.
* **Capacity.** `MaxKeys` and `MaxBytes` are enforced with LRU eviction ordered by write recency.
  Reads don't go through the log, so read-recency cannot be replicated deterministically.
* **Versions.** A key's version is the Raft index of the write that produced it: unique and
  monotonic. `if_version=N` turns a write into compare-and-set (`0` = create only).
* **Exactly-once.** Commands may carry `(ClientID, Seq, AckBelow)`. The store caches each session's
  results by sequence number. A duplicate (such as a retry after a leader crash) gets the cached
  result instead of being applied again. `AckBelow` lets the client release cached results it no
  longer needs, which supports many concurrent in-flight requests per client. Idle sessions expire
  on the logical clock.
* **Snapshots** serialise items in LRU order, the sessions and the logical clock, using a
  canonical ordering. Tests verify that replicas fed the same log produce byte-identical snapshots.

## Storage: the shared log engine

A Raft node must make log entries durable before acknowledging them. With one file per shard, a
node hosting 16 shards issued up to 16 concurrent fsyncs. These contended on the device and caused
most of the write-latency tail. `raft.LogEngine` is a single write-ahead log shared by every shard
on a node:

```
<data-dir>/raft/wal/00000000000000000001.seg   preallocated 64 MiB segments
<data-dir>/raft/snap/group-<shard>.snap        latest snapshot per shard
```

* **Group commit.** `Sync()` records the write sequence it needs. The first caller performs
  `flush + fdatasync`, and every caller whose writes it covered returns together. In the engine test,
  800 durable appends from 16 concurrent groups take about 18 ms (≈22 µs each), compared with about
  700 µs per fsync for 16 separate files.
* **Preallocation + `fdatasync`.** Segments are `fallocate`d and written with `WriteAt`, so a
  commit never changes the file size and `fdatasync` doesn't journal inode updates. A zeroed
  record header marks the end of the log.
* **Records.** `[len u32][crc32c u32][type u8][uvarint group][payload]`. On open, replay stops at the
  first invalid record in the active segment (a torn write). The tail is truncated and re-zeroed. A
  bad record in a sealed segment is reported as corruption.
* **Logical compaction.** After a snapshot, `Compact` appends a *reset* record (the new sentinel),
  the hard state, and the retained tail of entries. Replay discards everything earlier for that
  group.
* **Segment GC.** A sealed segment can be deleted once every group with records in it has been
  reset in a later segment. When there are more than `MaxSegments` segments, groups pinning the
  oldest one (typically idle shards) are checkpointed forward, rewriting their small state into the
  active segment, and the segment is deleted.

`FileStorage` (one WAL per group) and `MemoryStorage` implement the same `raft.Storage` interface.
They are used by tests and embedded deployments.

## Transport

All Raft traffic between two nodes, for every shard, flows over a pool of keep-alive HTTP/1.1
connections: `POST /internal/raft/{shard}/{vote|append|snapshot|timeout-now}`. Bodies use a
hand-written binary codec. An earlier version used `encoding/gob`, and building a decoder per
request cost ~19% of CPU at 10k ops/s. The codec rejects every truncated prefix and is fuzzed in CI.

With TLS enabled, the same connections run TLS 1.3. With a cluster CA they also use mutual
authentication, and the Raft and admin handlers check that the caller presented a certificate
chaining to that CA.

## Background tasks per node

| Task | Purpose |
|---|---|
| Raft ticker | Election timeouts, check-quorum |
| Raft replicators | One goroutine per follower per led shard: batched `AppendEntries`, heartbeats, snapshots |
| Raft syncer | Leader-side group commit of the local log |
| Raft applier | Applies committed entries and snapshots, triggers snapshotting |
| Raft read loop | Batches ReadIndex requests into heartbeat rounds |
| Leader balancer | Every `--rebalance-interval`, hands one shard back to its preferred replica if that replica is healthy and caught up |
| Expiry loop | Advances the replicated clock on shards with expired keys |

## Lifecycle

* **Startup:** open the log engine, replay it, rebuild each shard's state machine from its snapshot
  plus the log tail, then start Raft and HTTP. A restarted node refuses to grant votes for one
  election timeout (see [RAFT.md](RAFT.md#leases-and-restarts)).
* **Graceful shutdown (SIGTERM):** transfer leadership of every led shard to its most up-to-date
  healthy peer, stop background tasks, drain HTTP, stop Raft, close the engine. Clients see no
  election gap. That's why the rolling-restart test has zero errors.
* **Crash:** peers detect the missing heartbeats; the shards the node led re-elect after
  `election-timeout` to `2×election-timeout`, and clients retry transparently.
