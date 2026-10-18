# Changelog

## Unreleased: Game Day, live linearizability checking, incidents, a guided dashboard

### New: self-healing and a game layer
- **Self-healing autopilot** (on by default; `S` or the 🤖 button toggles it). Every broken node gets
  its own repair, all running in parallel, so nobody has to press Heal. Each fault has its own playbook:
  a graceful stop restarts warm; a `kill -9` is a power loss (power restored → cold boot, rebuilding the
  wiped memory from the log on disk → catch-up), so it takes longer; a partition is re-routed, then
  catches up; packet loss gets its link swapped. Stages finish on observed state (process running,
  node answering, replication lag gone), not on timers alone. The autopilot stands back during Game Day
  and rolling restarts. Chaos Monkey relies on it when it's on. Its actions appear in the timeline,
  logs and incident reports.
- **Repair HUD**: a cluster-health bar, one card per repair with its steps, progress, elapsed time and
  the live catch-up count ("4,209 changes to go"), a repair bar on each node card, and a result banner
  with a ★ rating. Booting nodes are shown as booting, not down.
- **Game layer**: XP and six levels, a ★–★★★ rating for every recovery, 13 achievements with unlock
  celebrations, and a trophy cabinet with your stats (`X`). Kept in this browser only.
- The simulation now models boot time (warm 0.7 s, cold 2.4 s) and gradual catch-up, so recovery is
  something to watch rather than instant.

### New: a dashboard anyone can follow
- **Welcome dialog** on the first visit: what DistKV is and why it matters, how it works in four ideas
  (shards, replicas, leaders, majority vote), every chaos button explained in everyday terms (`kill -9`
  is "pull the power plug"), and what to watch. It ends with "Show me around", "Break something!" or
  "I'll explore". It is remembered per browser; reopen it with **❓ What is this?** or `I`; force it with
  `?intro`, skip it with `?nointro`.
- **Guided tour** (`T`): a spotlight walk through the panels. Panels that don't exist in the current
  mode are skipped.
- **Plain-language tooltips** on every KPI tile, header badge, chaos button, per-node action and panel
  heading. They show on hover and on keyboard focus; on touch screens, tap the `?` button.
- **Live narrator**: after each fault, heal or recovery, a "What just happened" card explains it with
  the real numbers (shards affected, failover time, failed requests, answers verified), with a link to
  the incident report. It stays quiet during Game Day and Chaos Monkey, which narrate their own start
  and end. It can be switched off ("Stop explaining" or the welcome's checkbox).

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

### Performance
- Dashboard steady-state main-thread time down ~60% (69 → 28 ms/s in Chromium), live DOM down from
  ~7,600 to ~1,600 elements and event listeners from ~6,200 to 63. The hash ring is one SVG path per
  shard instead of one per virtual node. Panels skip unchanged re-renders, the shard matrix updates only
  changed rows, the timeline inserts only new events, and the log console keeps entries sorted on
  insert. Always-on decorative animations are gone and `prefers-reduced-motion` is respected.
  Background tabs keep tracking incidents but skip rendering and polling.
- Dashboard assets are served gzipped (compressed once in memory: 155 KB → 48 KB on the wire) with
  ETags, so reloads are 304s. CSS is minified.
- GitHub Pages demo: asset URLs carry a content hash (`app.js?v=…`), so a deploy takes effect at once
  instead of pairing a stale cached `app.js` with a new page for up to 10 minutes, while unchanged
  assets stay cached. Unknown paths redirect to the demo, Jekyll is skipped, `npm ci` is cached, and
  the deploy runs in its own job: only that job has Pages write access, and it runs no repository code.
- Bodies with a known `Content-Length` (Raft RPCs, KV PUTs, client responses) are read into one
  exact-size allocation instead of `io.ReadAll`'s doubling.
- Measured and *not* adopted: multiplexing Raft RPCs per peer (with aligned heartbeats) halved
  internal HTTP requests but did not reduce idle CPU, and saved only ~5% at moderate load. Idle cost is
  dominated by per-group heartbeat wake-ups, not HTTP. It wasn't worth a new wire protocol.

### Fixed
- **Raft: a proposal could hang forever on a single-node group** (and stall up to a heartbeat on larger
  ones). When a snapshot's compaction made the log durable while the leader's sync loop was syncing,
  the sync loop found nothing left to do and skipped advancing the commit index, and compaction didn't
  advance it either. Found as an intermittent CI hang in `TestNodeRecoversFromFileStorage`;
  reproduced deterministically by a new test.
- An isolated node (fault injection) kept serving `/v1/events` with its one-node view (every shard
  leaderless), so a dashboard observing through it reported a cluster-wide outage. It now ends the
  stream and answers new ones with 503. The dashboard retries (EventSource alone gives up on an error
  status) and reconnects through a node with a real view, or shows RECONNECTING until the partition
  heals.
- Game Day: a snapshot taken mid-hand-off during a graceful stop was counted as a failover, so the
  rolling-restart hypothesis was occasionally rejected although every hand-off succeeded. Such leader
  gaps now count as hand-offs.
- From a review of the branch: a failed node action could leave its button disabled; timeline
  events added below the visible head (from the historian) weren't shown; the node-log tail could
  resume mid-line; gzip and identity responses shared one strong ETag; the probe's per-key backlog
  was unbounded while a shard stayed leaderless; its start-up pin loop ignored cancellation.
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
