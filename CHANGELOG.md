# Changelog

## v0.2.0: polyglot, live dashboard, chaos lab

### New
- **Live dashboard** (TypeScript, embedded at `/ui/`): SSE-driven charts, node cards, a
  consistent-hash ring visualisation with a key finder, shard matrix, incident timeline, KV
  playground, SQLite history panel, dark/light themes, and a simulation mode for GitHub Pages.
- **`distkv-demo`**: supervises N real node processes with kill -9 / stop / start /
  partition / packet-loss controls, an adjustable traffic generator, client-observed availability,
  and a failover-aware API proxy.
- **Fault injection** (`--enable-fault-injection`, `/v1/admin/faults`): isolate a node, or drop
  or delay its Raft messages.
- **`/v1/events`** Server-Sent Events stream with per-node live load and per-shard follower lag.
- **Python** SDK and the **SQLite historian**; **TypeScript** SDK; **Rust** SDK and `distkv-top`.
- **Golden routing vectors** checked by all four languages.
- 5-node Compose stack with load generator and historian; Codespaces devcontainer; Pages,
  Polyglot, Release and CodeQL workflows; Dependabot; issue and PR templates.

### Docker
- The image now includes `distkv-demo`: the whole 5-node demo runs from one `docker run`
  (`make docker-demo`).
- Fixed: `/data` was root-owned while the server runs as non-root, so a fresh named volume
  was not writable. The image now creates `/data` owned by uid 65532.
- Verified end to end: the image (45.6 MB), the single-container demo, and the Compose stack
  (health checks, historian, Prometheus targets, Grafana provisioning, kill/start/partition/
  packet loss, persistence across `down`/`up`).
- `docs/run.html`: an HTML run guide, also published on GitHub Pages.
- The demo prints `http://localhost:PORT` when listening on all interfaces.
- The image cross-compiles on the build host (`--platform=$BUILDPLATFORM`), so multi-arch
  release images no longer run the Go toolchain under QEMU (arm64 build: ~35 s).
- CI cancels superseded runs of the same branch and caches Go builds keyed on `go.mod`.

### Fixed
- `go.mod` declared Go 1.23, but the tests use Go 1.24 APIs (`testing.T.Context`). CI installs
  the version from `go.mod`, so the tests would not have compiled there. The minimum is now Go 1.24.
- Preferred leaders are now balanced. Pure rendezvous ranking left one of 5 nodes preferred for
  none of 24 shards, idling a fifth of the cluster's write capacity. Routing vectors regenerated.
- The client now tries other replicas when a node answers `no_leader` (a partitioned former
  leader was being retried until timeout).

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
