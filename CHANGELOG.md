# Changelog

## v0.1.0 — initial release

### Consensus
- Raft with pre-vote, check-quorum, leader stickiness, fast conflict back-tracking, snapshots
  with trailing log retention, `InstallSnapshot`, and leadership transfer.
- Linearizable reads via leader leases (default) or batched ReadIndex.
- Group commit: the leader syncs batches asynchronously, and followers sync without holding the
  Raft lock.

### Storage
- `LogEngine`: a shared per-node WAL with cross-shard group commit, preallocated segments,
  `fdatasync`, torn-write recovery and segment GC with checkpointing of idle groups.
- `FileStorage` (per-group WAL) and `MemoryStorage` (crash-faithful, for tests).

### Data model
- Sharded keyspace: consistent hashing with virtual nodes, rendezvous replica placement,
  configuration fingerprinting.
- Cache state machine: TTL, LRU by keys or bytes, compare-and-set, exactly-once sessions.

### Operations
- HTTP API with server-side forwarding and `421` leader hints; a smart Go client; the
  `distkvctl` CLI; the `distkv-bench` open-loop load generator.
- Prometheus metrics, a Grafana dashboard, alert rules, Docker / Compose / Kubernetes manifests.
- Graceful shutdown with leadership hand-off; automatic leader balancing.

### Performance (4-vCPU VM, 3 nodes, fsync on)
- 10,000 ops/s sustained with p50 0.55 ms and p99 8.96 ms; 100% availability, including under
  repeated SIGKILLs.

### Performance history (p99 of 30–60 s runs on the same VM; indicative)
| Change | p99 @ 10k ops/s |
|---|---|
| Baseline (gob codec, ReadIndex reads, per-shard WAL files) | 75.8 ms |
| Binary Raft codec + leader-lease reads | 15.6 ms |
| Shared log engine with cross-shard group commit | 10.5 ms |
| Follower sync outside the Raft lock | 8.96 ms |
