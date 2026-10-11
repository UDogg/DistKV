# DistKV

**A Raft-based, sharded, distributed key-value cache written in Go, with zero third-party dependencies.**

DistKV partitions the keyspace into shards with consistent hashing. Each shard is its own Raft
group replicated on three nodes ("multi-Raft"), so writes and reads spread across the whole
cluster instead of going through one leader. Consensus, storage engine, wire protocol, metrics
and client are all written from scratch on the Go standard library.

[![CI](https://github.com/UDogg/DistKV/actions/workflows/ci.yml/badge.svg)](https://github.com/UDogg/DistKV/actions/workflows/ci.yml)
![Go](https://img.shields.io/badge/go-1.23%2B-00ADD8)
![Dependencies](https://img.shields.io/badge/dependencies-0-brightgreen)
![License](https://img.shields.io/badge/license-Apache--2.0-blue)

| | |
|---|---|
| **Consensus** | Raft from scratch: leader election with **pre-vote** and **check-quorum**, log replication with fast conflict back-tracking, snapshots and `InstallSnapshot`, **leadership transfer**, **ReadIndex** and **leader-lease** linearizable reads |
| **Sharding** | Consistent-hash ring with virtual nodes (key → shard), rendezvous hashing (shard → replicas), one Raft group per shard, automatic leader balancing |
| **Storage** | Shared per-node write-ahead log with **group commit** across all shards, preallocated segments, `fdatasync`, CRC-protected records, torn-write recovery, segment GC |
| **Cache semantics** | Per-key TTLs, LRU eviction by key count or bytes, compare-and-set on versions, **exactly-once** retries through client sessions |
| **Operations** | Transparent request forwarding, smart topology-aware Go client, CLI, Prometheus metrics, Grafana dashboard, alerts, Docker / Compose / Kubernetes manifests, graceful hand-off on shutdown |
| **Security** | TLS 1.3 for clients and peers, with mutual TLS between nodes; Raft and admin endpoints require a certificate signed by the cluster CA |
| **Verification** | Raft fault-injection harness (partitions, message loss, crashes), Porcupine-style **linearizability checker**, end-to-end chaos tests, fuzzing |

## Results

All figures were measured on a **single 4-vCPU Linux VM** that ran all three server processes and the
load generator together, with **fsync on every commit** (full durability). The full methodology and
raw output are in [docs/BENCHMARKS.md](docs/BENCHMARKS.md).

| Scenario | Ops | Errors | Availability | p50 | p90 | p99 |
|---|---:|---:|---:|---:|---:|---:|
| Steady state, 10,000 ops/s, 80/20 read/write, 60 s | 600,000 | 0 | **100%** | **0.55 ms** | 2.2 ms | **8.96 ms** |
| Same load, a random node **SIGKILLed** every ~12 s (5 crashes) | 600,000 | 0 | **100%** | 0.58 ms | 6.5 ms | 541 ms¹ |
| Rolling restart of every node (graceful hand-off) | 10,000 | 0 | **100%** | 0.76 ms | 1.3 ms | 2.8 ms |

¹ The p99 under crashes reflects requests that waited out an election (~0.5–1 s with the default
`--election-timeout 500ms`) on the third of shards whose leader died. They were retried, not lost.

Correctness under faults is checked, not assumed. `TestChaosLinearizability` records the full
concurrent history of puts, gets and deletes (typically **~190k operations**) from 8 clients on a
5-node cluster while nodes crash and restart, and verifies that the history is **linearizable**. As a
control, switching the test to stale follower reads makes the checker flag a violation immediately.

## Quick start

```bash
# Build and start a local 3-node cluster with 16 shards
make cluster                          # or: scripts/cluster.sh start 3 16

bin/distkvctl put user:42 alice --ttl 10m     # prints OK version=N
bin/distkvctl get user:42
bin/distkvctl cas user:42 N bob                # compare-and-set: only if still at version N
bin/distkvctl cluster                  # shard leaders, terms, node health

# Plain HTTP works against any node (requests are forwarded to the shard leader)
curl -X PUT 'localhost:7002/v1/kv/greeting?ttl=30s' --data 'hello'
curl localhost:7003/v1/kv/greeting

# Benchmark: 10k ops/s for 60s against a fresh cluster
make bench

# Crash nodes while benchmarking and measure availability
scripts/chaos-bench.sh 10000 60

# Same cluster over mutual TLS (generates a throwaway CA)
TLS=1 scripts/cluster.sh start 3 16
bin/distkvctl --endpoints https://127.0.0.1:7001 --ca .cluster/certs/ca.pem cluster
```

Docker Compose (3 nodes + Prometheus + Grafana on :3000):

```bash
make compose-up
```

Kubernetes: [`deploy/kubernetes/distkv.yaml`](deploy/kubernetes/distkv.yaml) contains a StatefulSet,
headless service and PodDisruptionBudget.

## Using the Go client

```go
c, err := client.New(ctx, client.Config{Endpoints: []string{"http://10.0.0.1:7001"}})

ver, err := c.Put(ctx, "session:abc", data, client.WithTTL(10*time.Minute))
item, err := c.Get(ctx, "session:abc")                  // linearizable
item, err = c.Get(ctx, "session:abc", client.Stale())   // any replica, faster
_, err = c.CompareAndSwap(ctx, "session:abc", item.Version, newData)
if errors.Is(err, client.ErrConflict) { /* someone else won */ }
```

The client downloads the topology once, computes each key's shard locally and talks to the shard
leader directly, learning leaders from `421 Misdirected Request` hints. It retries with backoff until
the context expires. Every write carries a session ID and sequence number, so a write retried after a
leader crash is applied **exactly once**.

## Architecture

```
                    ┌──────────────────────────── smart client ─────────────────────────────┐
                    │  key ──hash──▶ shard (consistent-hash ring) ──▶ cached shard leader   │
                    └───────────────────────────────┬───────────────────────────────────────┘
                                                    │ HTTP (or any node; it forwards)
          ┌─────────────── node n1 ───────────────┐ │ ┌─────────────── node n2 ──────────────┐
          │  HTTP API · routing · forwarding      │◀┴▶│  HTTP API · routing · forwarding     │
          │                                       │   │                                      │
          │  shard 0  [Raft LEADER ]──┐ kv.Store  │   │  shard 0  [Raft follower]  kv.Store  │
          │  shard 1  [Raft follower] │ TTL · LRU │   │  shard 1  [Raft LEADER  ]  TTL · LRU │
          │  shard 2  [Raft follower] │ sessions  │   │  shard 2  [Raft follower]  sessions  │
          │     ...                   │           │   │     ...                              │
          │  ┌────────────────────────▼─────────┐ │   │  ┌────────────────────────────────┐  │
          │  │ LogEngine: one WAL, group commit │ │   │  │ LogEngine                      │  │
          │  └──────────────────────────────────┘ │   │  └────────────────────────────────┘  │
          └───────────────────────────────────────┘   └──────────────────────────────────────┘
                   ▲   Raft RPCs for all shards multiplexed over pooled keep-alive HTTP   ▲
                   └──────────────────────────────────────────────────────────────────────┘
```

Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the design, and [docs/RAFT.md](docs/RAFT.md)
for the consensus implementation and the safety arguments behind each optimisation.

### Request life cycle (write)

1. The client hashes the key to shard *s* and sends `PUT /v1/kv/key` to the cached leader of *s*.
2. The leader stamps the command with its clock, a TTL deadline and the client session, appends it
   to its log and wakes the replicators for *s*.
3. Each follower appends the batch to its node's shared WAL. Concurrent syncs from every shard on that
   node collapse into one `fdatasync`. The follower then acknowledges.
4. Once a quorum (the leader's own durable log counts) holds the entry, it is committed and applied
   to the replicated `kv.Store`, and the result goes back to the client.

### Linearizable reads without a log write

By default the leader serves reads under a **lease**. A quorum acknowledged a heartbeat sent less
than 0.9 × the election timeout ago, and those followers refuse to vote for anyone else for a full
election timeout, so no other leader can exist. If the lease has lapsed, the leader falls back to
**ReadIndex**, a batched heartbeat round. `--read-mode readindex` disables leases entirely for
environments where clock-rate drift cannot be bounded.

## Repository layout

```
cmd/distkv            server binary
cmd/distkvctl         command-line client
cmd/distkv-bench      open-loop load generator (coordinated-omission-safe)
pkg/client            Go client library
internal/raft         Raft consensus, log engine, file/memory storage
internal/kv           replicated cache state machine
internal/cluster      consistent hashing, replica placement
internal/server       node: shards, HTTP API, routing, balancing, metrics
internal/transport    Raft RPC transport + binary codec
internal/tlsutil      TLS configuration and dev certificate generation
internal/linearizability  history checker (Wing & Gong + memoisation)
internal/loadgen      workload driver shared by the bench tool and tests
internal/testcluster  in-process multi-node clusters for tests
test/e2e              chaos tests (linearizability, availability, rolling restart)
deploy/               Docker Compose, Prometheus, Grafana, Kubernetes
docs/                 architecture, Raft notes, API, operations, benchmarks
```

## Testing

```bash
make test-short   # unit + integration tests (~40s)
make race         # everything under the race detector, chaos tests included
make chaos        # verbose end-to-end fault injection
make fuzz         # fuzz the Raft wire codec
```

| Layer | What is verified |
|---|---|
| Raft core | Elections, re-election, no disruption from a partitioned node (pre-vote), check-quorum step-down, log back-tracking after a minority partition, persistence across crashes that lose unsynced writes, snapshot install, ReadIndex batching, lease expiry before a new leader can exist, leadership transfer, and a randomized churn test with crashes, partitions and 10% message loss that checks that no two nodes ever apply different commands at the same index |
| Storage | WAL round-trips, overwrite semantics, torn and corrupt tails, snapshot checksums, segment rotation and GC with idle groups, group commit, Raft running on the log engine through restarts |
| State machine | Determinism across replicas and through snapshot/restore, TTL, LRU by keys and bytes, CAS, session deduplication and expiry |
| End to end | CRUD, CAS counters without lost updates, TTL, forwarding through every node, 421 hints, stale reads, failover, graceful hand-off, full-cluster restart from disk, leader balancing, linearizability and availability under chaos |

## Documentation

- [Architecture](docs/ARCHITECTURE.md): components, data flow, sharding, storage engine
- [Raft implementation notes](docs/RAFT.md): safety arguments for pre-vote, leases, durability and group commit
- [HTTP API](docs/API.md): endpoints, headers, error codes
- [Operations guide](docs/OPERATIONS.md): configuration, deployment, monitoring, failure handling
- [Benchmarks](docs/BENCHMARKS.md): methodology, raw results, how to reproduce

## License

Apache 2.0. See [LICENSE](LICENSE).
