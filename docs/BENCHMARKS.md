# Benchmarks

## Environment and methodology

| | |
|---|---|
| Machine | One Linux VM with **4 vCPUs** and 16 GB RAM (kernel 6.18, Go 1.24.7) |
| Cluster | 3 `distkv` processes on the same VM, 16 shards, replication factor 3, default flags |
| Durability | **fsync on**: every acknowledged write is `fdatasync`ed on a quorum |
| Load | `distkv-bench` **on the same VM**: open loop at a fixed rate, 256 max in flight, 100,000 keys, 128-byte values, 80% reads / 20% writes, smart client (requests go straight to shard leaders), linearizable reads |
| Measurement | Latency is measured **from each operation's scheduled start time**, so client-side queueing during stalls is counted (no coordinated omission). A 3 s warmup is excluded. Availability = successful operations / attempted operations; an operation fails if it does not succeed within 5 s including retries. |

This is a deliberately unfavourable setup. Three Raft replicas and the load generator compete for the
same 4 cores and one disk, so a production deployment with one node per machine should do better.
All numbers come from single runs and are reproducible with the scripts below.

## Headline: 10,000 ops/s

`scripts/bench.sh 10000 60s`

```
duration 1m0.002s | target 10000 ops/s | concurrency 256 | keys 100000 | value 128B | reads 80%
ops 600000 | errors 0 | throughput 10000 ops/s | availability 100.0000% (worst second 100.00%)
latency (from scheduled start):
  all         600000  mean 1.126ms   p50 552µs     p90 2.24ms    p99 8.96ms    p99.9 29.184ms  max 91.417ms
  reads       479970  mean 870µs     p50 420µs     p90 1.664ms   p99 7.552ms   p99.9 24.576ms  max 61.08ms
  writes      120030  mean 2.148ms   p50 1.328ms   p90 3.808ms   p99 13.568ms  p99.9 55.808ms  max 91.417ms
```

* **10,000 ops/s sustained, 0 errors, p99 under 10 ms overall**, with every write durably
  replicated.
* Server-side histograms (`distkv_request_duration_seconds`) show **99.4% of reads complete under
  1 ms** inside the server. The rest of the client-observed read tail is CPU contention with the
  co-located load generator.
* Writes cost one replication round trip plus a group-committed `fdatasync` on the leader and on a
  follower. Their p99 (13.6 ms) is the part of the "<10 ms" goal this shared box does not meet for
  writes alone.

## Availability under crashes

`scripts/chaos-bench.sh 10000 60` runs the same workload while a nemesis **SIGKILLs a random node
every ~12 s** and restarts it 4 s later: 5 crashes in 60 s, each taking out the leader of about a
third of the shards.

| Election timeout | Ops | Errors | Availability | p50 | p90 | p99 | p99.9 | max |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 500 ms (default) | 600,000 | **0** | **100%** | 0.58 ms | 6.5 ms | 541 ms | 786 ms | 2.28 s |
| 250 ms (`--election-timeout 250ms --heartbeat 25ms`) | 600,001 | **0** | **100%** | 0.54 ms | 3.1 ms | 254 ms | 414 ms | 928 ms |

Every operation succeeded. Requests to a shard whose leader was killed wait (and retry) until a
new leader is elected, about one to two election timeouts later. Writes are retried with the same
session sequence number, so none is applied twice. The per-second view shows each crash as a
brief latency spike with no errors:

```
[nemesis] SIGKILL n3
[ 36s]    8929 ops/s  errors 0     p99 10.368ms
[ 37s]   10313 ops/s  errors 0     p99 868.352ms   <- election on ~1/3 of shards
[ 38s]   10757 ops/s  errors 0     p99 88.064ms    <- backlog drains
[ 39s]    9910 ops/s  errors 0     p99 7.232ms
```

A shorter election timeout halves the failover tail but makes spurious elections more likely
under GC pauses or network jitter. The default of 500 ms is the conservative choice.

## End-to-end chaos tests (`make chaos`)

Run in-process with 5 nodes and replication factor 3. The nemesis crashes, gracefully restarts or
revives a random node every 0.3–1.3 s.

| Test | Result |
|---|---|
| `TestChaosLinearizability`: 8 clients, puts/gets/deletes on 6 hot keys for 12 s | ~190,000 operations recorded; **history verified linearizable**. Control: with stale reads the checker reports a violation |
| `TestChaosAvailability`: 1,500 ops/s for 15 s | 22,499 / 22,499 succeeded (**100%**); p50 0.76 ms |
| `TestChaosRollingRestart`: every node restarted in turn under load | 10,000 / 10,000 succeeded; p99 2.8 ms (leadership is handed off before each shutdown) |

## Configuration variants (10,000 ops/s, 30 s each)

Each variant ran on a fresh cluster. All five completed 300,000 operations with **0 errors**.

| Variant | p50 | p90 | p99 | p99.9 | write p50 | write p99 |
|---|---:|---:|---:|---:|---:|---:|
| Default (fsync, lease reads) | 0.60 ms | 2.5 ms | 8.8 ms | 19.2 ms | 1.42 ms | 12.3 ms |
| `--no-sync` | 0.34 ms | 1.1 ms | 5.6 ms | 12.5 ms | 0.58 ms | 6.8 ms |
| `--read-mode readindex` | 1.98 ms | 7.7 ms | 21.8 ms | 38.4 ms | 2.75 ms | 21.8 ms |
| Stale reads (`client.Stale()`) | 0.57 ms | 2.4 ms | 9.6 ms | 24.8 ms | 1.36 ms | 14.0 ms |
| Mutual TLS (`TLS=1`) | 0.60 ms | 2.4 ms | 9.5 ms | 19.5 ms | 1.41 ms | 12.5 ms |

* **Lease reads vs. ReadIndex.** Leases cut read p99 by about 2.5×. ReadIndex adds a quorum
  heartbeat round per batch of reads, and the extra RPCs also slow writes on this CPU-bound box.
* **`--no-sync`** roughly halves write latency, trading away durability against simultaneous
  power loss on a majority of nodes.
* **TLS 1.3 with mutual authentication** costs almost nothing at this load, because connections
  are long-lived and handshakes are amortised.
* **Stale reads** don't help here: every replica shares the same 4 cores. They pay off when
  replicas run on separate machines, or during elections.

## Optimisation history

Every change below was made because a profile pointed at it. Each row is a run of the 10k ops/s
benchmark on the same VM.

| Step | p50 | p99 | What the profile showed |
|---|---:|---:|---|
| First working version: gob codec, ReadIndex for every read, one WAL file per shard | 13.8 ms | 75.8 ms | CPU saturated; `encoding/gob` decoder construction ~19% of CPU; heartbeat rounds for every read batch |
| Hand-written binary Raft codec + leader-lease reads | 1.1 ms | 15.6 ms | Fsync inside the follower's Raft lock; 16 concurrent fsync streams per node |
| Shared per-node log engine with cross-shard group commit, preallocation, `fdatasync` | 0.67 ms | 10.5 ms | HTTP plumbing and syscalls dominate; follower still syncs while holding the lock |
| Follower syncs outside the Raft lock; benchmark warmup | **0.55 ms** | **8.96 ms** | Remaining time spread across HTTP handling and the co-located load generator |

## Micro-benchmarks (`make microbench`)

| Benchmark | Result |
|---|---|
| `cluster.ShardForKey` | 34 ns/op, 0 allocs |
| `kv.Store.Apply` (put) | 250 ns/op |
| `kv.Store.Get` (parallel) | 122 ns/op |
| Raft codec: marshal + unmarshal a 32-entry `AppendEntries` | 1.7 µs/op, 2 allocs |
| Log engine: durable append from 16 concurrent groups | ≈22 µs per append (group commit) |

## Reproducing

```bash
make build
scripts/bench.sh 10000 60s                  # steady state
scripts/chaos-bench.sh 10000 60             # SIGKILL nemesis
EXTRA_FLAGS="--election-timeout 250ms --heartbeat 25ms" scripts/chaos-bench.sh 10000 60
make chaos                                  # in-process chaos tests
```

Use `bin/distkv-bench --json` for machine-readable output, including a per-second timeline.
