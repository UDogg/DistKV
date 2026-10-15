# Changelog

## Unreleased: Game Day, live linearizability checking, incidents

### New
- **Live linearizability checking** (`distkv-demo --probe`, on by default). A probe client records
  every operation on a few keys. Barrier reads cut each key's history into epochs, and each epoch is
  verified with the Wing & Gong checker. Writes with unknown outcomes are carried into later epochs.
  The verdict is in `/demo/info` (`consistency`), shown as a live pill, a KPI tile and a details panel
  (epoch timeline per key, violations with their full histories). Every incident report says how
  many operations were verified across it, and a violation makes an incident SEV1.
- **Game Day**: five hypothesis-driven chaos experiments (leader loss, leader partition, flaky
  network, rolling restart, quorum loss). Each is run end to end (steady state → fault → heal →
  recovery → verification) and judged on client errors, failover times and linearizability. A
  scored Game Day report (A+ to F) links every incident. On a real 5-node demo: 5/5 held, A+.
- **Chaos Monkey**: random kills, stops, partitions and packet loss until switched off.
- **Incidents.** Every chaos action, or any unhealthy state the dashboard detects on its own (for
  example a container killed from the terminal), opens an incident. It resolves after two fully
  healthy snapshots in a row, and gets SEV1 to SEV3 from its impact.
- **Alerts** when an incident opens, escalates (another fault, or a shard leaderless for 5 s, i.e.
  lost quorum) and resolves. There's also a heal alert, a bell with the alert history, optional sound
  and desktop notifications, and a tab-title badge.
- **Live incident strip** with elapsed time, severity and recovery progress.
- **Incident reports**: summary, SLO findings, impact, per-shard failover times, charts of the
  incident window, a timeline and recommendations. Download as HTML or Markdown, copy, or print
  to PDF. Incidents are kept in the browser (localStorage) across reloads.
- **Log console** merging dashboard events, chaos actions, alerts, client stats and each node's
  own server log. It has level and source filters, search, follow/pause, and `.log`/`.jsonl` export.
  The demo supervisor serves node logs incrementally at `GET /demo/nodes/{id}/logs?offset=N`.
- Event timeline filters (faults, leadership, your actions, incidents). Clicking a shard event
  locates the shard on the ring and in the matrix.
- Keyboard shortcuts: `K` kill, `P` partition, `L` packet loss, `H` heal, `R` latest report, `A` alerts,
  `/` search logs, `?` help.

### Fixed
- Demo supervisor: a start issued right after a kill or graceful stop could be a no-op (the stop
  changed the node's state asynchronously), or could start a second process while the first was
  still shutting down. Stops now change state immediately and starts wait for the old process.
- Demo supervisor: a node's exit closed the *current* log file, which after a quick restart belonged
  to the new process.
- `/v1/events` now sends an SSE `retry: 500` hint. When the node serving the stream dies, browsers
  reconnect in 0.5 s instead of about 3 s, so the dashboard sees the failure sooner.
- Server logs printed the `node` attribute twice on every Raft line.
- The top bar overflowed horizontally on phone-width screens.

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
