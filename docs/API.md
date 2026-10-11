# HTTP API

Every node serves the full API on one port (default `:7001`). Keys may contain any characters,
including `/`; percent-encode reserved characters (`?`, `#`, `%`, spaces).

## Key-value operations

### `GET /v1/kv/{key}`

Returns the value as raw bytes.

| Query | Meaning |
|---|---|
| `consistency=linearizable` (default) | Served by the shard leader after confirming leadership (lease or ReadIndex) |
| `consistency=stale` | Served by any replica from local state; may lag slightly behind |

Response headers: `X-Distkv-Version` (the Raft index of the last write), `X-Distkv-Expires-At`
(RFC 3339, only present when a TTL is set), `X-Distkv-Shard`, `X-Distkv-Leader`, `X-Distkv-Served-By`.

```bash
$ curl -i localhost:7001/v1/kv/user:42
HTTP/1.1 200 OK
Content-Type: application/octet-stream
X-Distkv-Version: 1187
X-Distkv-Shard: 9
X-Distkv-Leader: n1
X-Distkv-Served-By: n1

alice
```

### `PUT /v1/kv/{key}`

The request body is the value (max 1 MiB by default).

| Query | Meaning |
|---|---|
| `ttl=30s` | Expire the key after this Go duration |
| `if_version=N` | Compare-and-set: write only if the current version is `N`; `0` means only if the key does not exist |

```bash
$ curl -X PUT 'localhost:7001/v1/kv/user:42?ttl=10m' --data 'alice'
{ "key": "user:42", "version": 1187, "shard": 9 }

$ curl -X PUT 'localhost:7001/v1/kv/user:42?if_version=1000' --data 'bob'
HTTP/1.1 409 Conflict
{ "error": "version mismatch", "code": "conflict", "current_version": 1187 }
```

### `DELETE /v1/kv/{key}`

`200 {"deleted": true, ...}` if the key existed, otherwise `404`.

### Exactly-once writes

Clients that retry writes should send session headers. The server then applies each
`(client, sequence)` pair at most once and answers retries with the original result:

| Header | Meaning |
|---|---|
| `X-Distkv-Client-Id` | Random non-zero 64-bit client ID |
| `X-Distkv-Seq` | Per-client write sequence number (≥ 1); reuse it when retrying the same write |
| `X-Distkv-Ack-Below` | All sequence numbers below this have received a response (the server may drop their cached results) |

The Go client manages these automatically.

### Routing headers

| Header | Meaning |
|---|---|
| `X-Distkv-No-Forward: 1` | Don't proxy: answer `421` with the leader's ID and address if this node isn't the leader (used by smart clients) |
| `X-Distkv-Hops` | Set by servers when forwarding (loop protection) |

## Errors

Every non-2xx response has a JSON body:

```json
{ "error": "not the leader for this shard", "code": "not_leader", "shard": 3, "leader": "n2", "leader_addr": "http://10.0.0.2:7001" }
```

| HTTP | `code` | Meaning | Retry? |
|---|---|---|---|
| 400 | `bad_request` | Malformed key, TTL or version | No |
| 400 | `stale_request` | Session sequence already acknowledged | No |
| 404 | `not_found` | Key missing or expired | No |
| 409 | `conflict` | CAS precondition failed (`current_version` included) | Re-read first |
| 413 | `too_large` | Value exceeds `MaxValueBytes` | No |
| 421 | `not_leader` | Talk to `leader_addr` instead | Yes, immediately |
| 503 | `no_leader` | Election in progress (`Retry-After: 1`) | Yes, with backoff |
| 503 | `outcome_unknown` | Leader changed before the write committed | Yes, **with the same sequence number** |
| 503 | `shutting_down` / `unavailable` | Node draining or no replica reachable | Yes, another node |
| 504 | `timeout` | No quorum within `request-timeout` | Yes |

## Cluster and operations

| Endpoint | Description |
|---|---|
| `GET /v1/cluster` | Cluster-wide view aggregated from all nodes: spec, per-shard leader/term/commit, per-node reachability and leader counts |
| `GET /v1/status` | This node's view of each local shard: role, term, indices, followers' match index, lag and last contact |
| `POST /v1/admin/shards/{id}/transfer-leader?to=nID` | Move leadership (send to the current leader; omit `to` to pick the best peer) |
| `GET /v1/events?interval=1s` | **Server-Sent Events** stream of the `/v1/cluster` document (event `cluster`) with per-node live load (`load`: reads/writes/errors/forwarded per second, p50/p99/p99.9) and per-shard follower lag. Used by the dashboard |
| `GET/POST /v1/admin/faults` | Fault injection (only with `--enable-fault-injection`): `{"isolated":bool,"drop_percent":0-100,"delay_ms":0-10000}` partitions the node from its peers, or drops or delays its Raft messages. Client traffic is unaffected |
| `GET /ui/` | The embedded live dashboard (disable with `--disable-ui`) |
| `GET /healthz` | Liveness: the process is serving |
| `GET /readyz` | Readiness: every local shard has a known leader and the node isn't draining |
| `GET /metrics` | Prometheus metrics |
| `GET /debug/pprof/` | Go profiling (only with `--pprof`) |

## Internal

`POST /internal/raft/{shard}/{vote|append|snapshot|timeout-now}` carries Raft RPCs between nodes
in a binary encoding. When the cluster runs with `--tls-ca`, this endpoint and `/v1/admin/` require
a client certificate signed by the cluster CA. Otherwise, restrict them at the network layer.
