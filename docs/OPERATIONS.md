# Operations guide

## Configuration

Settings can come from flags, environment variables (`DISTKV_<FLAG>`, with dashes as underscores,
e.g. `DISTKV_ELECTION_TIMEOUT=300ms`) or a flat JSON file keyed by flag name (`--config`).
Precedence: **flags > environment > config file > defaults**.

```json
{ "id": "n1", "members": "n1=10.0.0.1:7001,n2=10.0.0.2:7001,n3=10.0.0.3:7001",
  "data-dir": "/var/lib/distkv", "shards": 32, "max-bytes-per-shard": 536870912 }
```

| Flag | Default | Notes |
|---|---|---|
| `--id` | – | This node's ID. Must appear in `--members`. |
| `--members` | – | **Every** member as `id=host:port`, identical on all nodes. |
| `--listen` | port of own member address | Bind address. |
| `--data-dir` | – | Required unless `--in-memory`. Holds `raft/` (WAL + snapshots) and `CLUSTER`. |
| `--shards` | 16 | Number of Raft groups. **Fixed for the life of the cluster.** Use 4–8× the node count so leadership can balance. |
| `--replication` | 3 | Replicas per shard. 3 tolerates 1 failure; 5 tolerates 2. |
| `--vnodes` | 128 | Ring points per shard. |
| `--heartbeat` | 50ms | Leader → follower heartbeat interval. |
| `--election-timeout` | 500ms | Base timeout; the actual one is randomised in `[T, 2T)`. Determines failover time (see Tuning). |
| `--snapshot-threshold` | 20000 | Applied entries between snapshots per shard. |
| `--read-mode` | lease | `lease` or `readindex` (no clock assumptions; adds a network round trip per batch of reads). |
| `--max-keys-per-shard` / `--max-bytes-per-shard` | 0 (unlimited) | LRU capacity. Must be identical on all nodes (eviction is replicated). |
| `--request-timeout` | 3s | End-to-end bound per client request. |
| `--rebalance-interval` | 5s | Leader balancing; 0 disables. |
| `--no-sync` | false | Skip fdatasync. Survives process crashes but not power loss. |
| `--shutdown-timeout` | 15s | Budget for graceful hand-off on SIGTERM. |
| `--log-level`, `--log-format` | info, text | `json` for log shippers. |
| `--pprof` | false | Expose `/debug/pprof`. |
| `--enable-fault-injection` | false | Expose `/v1/admin/faults` (partition, drop, delay). Demos and chaos testing only. |
| `--cors-origin` | – | `Access-Control-Allow-Origin` value, for dashboards served from another origin. |
| `--disable-ui` | false | Don't serve the embedded dashboard at `/ui/`. |
| `--tls-cert`, `--tls-key` | – | Serve HTTPS (TLS 1.3). The same certificate is presented to peers. Member addresses must be `https://`. |
| `--tls-ca` | – | CA bundle used to verify peers. Enables mutual TLS: Raft and admin endpoints then require a client certificate signed by this CA. |
| `--tls-client-auth` | false | Require client certificates on **every** connection, the KV API included. |

The placement-relevant settings (`members` IDs, `shards`, `replication`, `vnodes`) are
fingerprinted in `<data-dir>/CLUSTER`. A node refuses to start if they change.

## Sizing

* **Nodes:** at least 3 (`replication` ≤ nodes). Every node hosts about `shards × replication / nodes`
  replicas and leads about `shards / nodes` of them.
* **Memory:** the dataset is held in memory once per replica, plus each shard's un-snapshotted log tail
  (≤ `snapshot-threshold + 1024` entries). Bound it with `--max-bytes-per-shard`.
* **Disk:** each node keeps up to ~4 WAL segments of 64 MiB plus one snapshot per hosted shard. A
  low-latency SSD matters more than throughput, because commits are bound by `fdatasync` latency.
* **CPU:** at 10k ops/s (80% reads) the three server processes together used about 2 vCPUs.

## Deployment

* **Bare metal / VMs:** run `distkv` under systemd with `KillSignal=SIGTERM` and
  `TimeoutStopSec` greater than `--shutdown-timeout`.
* **Single container (demo):** `docker run -p 8080:8080 --entrypoint distkv-demo distkv:local --listen 0.0.0.0:8080 --dir /data/demo --historian=false`
  runs five nodes, traffic and the dashboard in one container (`make docker-demo`).
* **Docker Compose:** `deploy/docker-compose.yml` runs 5 nodes, a load generator, the SQLite historian,
  Prometheus and Grafana. Host-side clients need `--no-smart` / `smart=False`, because members advertise
  container hostnames. The image runs as non-root (uid 65532) and writes to `/data`.
  Step-by-step instructions are in [run.html](run.html).
* **Kubernetes:** `deploy/kubernetes/distkv.yaml`. It uses a StatefulSet with
  `podManagementPolicy: Parallel` (pods must start together to form a quorum), a headless service
  with `publishNotReadyAddresses` (peers must resolve each other before they are ready), pod
  anti-affinity, and a PodDisruptionBudget of `maxUnavailable: 1`.

### Rolling upgrades

Restart one node at a time and wait for `/readyz` before moving on. On SIGTERM a node transfers
leadership of every shard it leads to a caught-up peer before exiting, so clients see no
unavailability (the rolling-restart test records zero failed operations).

### Replacing a dead machine

Start a new process with the **same ID and address** and an empty data directory. It joins as a
follower and receives snapshots for every shard it hosts. Membership is static, so you can't
change the set of IDs in a running cluster.

### Backups

`<data-dir>/raft/snap/group-<shard>.snap` is a self-checksummed snapshot of a shard. For a
consistent per-shard backup, copy the newest snapshot from any replica. DistKV is primarily a
cache, so most deployments rely on replication instead.

## Monitoring

`/metrics` exposes Prometheus metrics. The Compose stack ships a Grafana dashboard
(`deploy/grafana/dashboards/distkv.json`) and alert rules (`deploy/prometheus/alerts.yml`).

| Metric | Use |
|---|---|
| `distkv_requests_total{op,status}` | Throughput and error ratio (availability SLO) |
| `distkv_request_duration_seconds{op}` | Server-side latency histogram (latency SLO) |
| `distkv_raft_has_leader{shard}`, `distkv_raft_is_leader{shard}` | Shard health, leader distribution |
| `distkv_raft_term{shard}`, `distkv_raft_elections_total` | Election churn |
| `distkv_raft_follower_lag_entries{shard,follower}` | Slow or partitioned followers |
| `distkv_raft_commit_index`, `distkv_raft_applied_index` | Write rate, apply lag |
| `distkv_raft_lease_reads_total`, `distkv_raft_read_index_rounds_total` | Read path in use |
| `distkv_kv_keys`, `distkv_kv_bytes`, `distkv_kv_evictions_total`, `distkv_kv_expirations_total` | Cache occupancy and churn |
| `distkv_kv_dedup_hits_total` | Retried writes absorbed by sessions (spikes during failovers) |
| `distkv_forwarded_requests_total` | Clients not routing to leaders (use the smart client) |

Shipped alerts: shard without a leader, node not ready, p99 above 10 ms, 99.9% availability
error-budget burn, lagging follower, election storm.

## Failure handling

| Event | What happens | Client impact |
|---|---|---|
| Follower crash | Leaders keep committing with the remaining quorum. On restart the follower replays its WAL and catches up from the log or a snapshot. | None |
| Leader crash | Followers' election timers fire after `[T, 2T)`. Pre-vote then a vote elect a new leader for each affected shard. | Requests to those shards wait for up to about 2T and are retried transparently (exactly once for writes). |
| Graceful stop | Leadership is transferred first. | None |
| Network partition | The majority side keeps (or elects) leaders. A leader on the minority side steps down within one election timeout (check-quorum), and its lease expires before a new leader can be elected, so it cannot serve stale reads. | Clients on the majority side are unaffected. Clients that can only reach the minority get 503s. |
| Partitioned node rejoins | Pre-vote stops it from disrupting the current leader despite its elevated election attempts. | None |
| Disk full / I/O error | The node panics instead of acknowledging writes it could not persist. | As for a crash |
| Torn write at crash | Replay stops at the first bad record of the active segment and zeroes the tail. | None (unacknowledged data only) |

## Tuning

* **Failover time vs. stability.** Failover takes about `election-timeout` to `2 × election-timeout`.
  Lower values speed up failover but risk spurious elections under GC pauses or network jitter.
  Keep `heartbeat` ≤ `election-timeout / 5`. See [BENCHMARKS.md](BENCHMARKS.md#failover-time-vs-election-timeout).
* **Read mode.** `lease` gives the lowest read latency. Use `readindex` if VMs may be paused for
  long periods or clocks may jump in rate. Neither mode depends on wall-clock agreement.
* **Stale reads** (`?consistency=stale`, `client.Stale()`) spread read load over all replicas
  and remain available during elections, at the cost of possibly lagging data.
* **`--no-sync`** cuts write latency roughly in half where losing the last few milliseconds on power
  loss is acceptable. Replication still protects against a single machine failure.

## Security

* **Transport encryption.** With `--tls-cert/--tls-key`, all client and peer traffic uses TLS 1.3.
* **Node authentication.** With `--tls-ca`, nodes present their certificates to each other
  (mutual TLS). `/internal/raft/` and `/v1/admin/` reject any caller without a certificate
  signed by the cluster CA (`403`), so an application or attacker cannot inject Raft messages or
  move leadership.
* **Client authentication.** `--tls-client-auth` requires client certificates on every
  connection. Clients pass `--ca/--cert/--key` (CLI, bench) or `client.Config.TLS` (Go).
* **Development certificates.** `distkvctl dev-certs <dir> [hosts...]` mints a throwaway CA and a
  shared node certificate. `TLS=1 scripts/cluster.sh start` uses it. Use real per-node
  certificates in production.

There is no per-key authorization: any client allowed to connect can read and write every key.
Without TLS, run DistKV on a private network and restrict `/internal/` and `/v1/admin/` at the
network layer.
