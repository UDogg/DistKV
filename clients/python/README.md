# DistKV for Python 🐍

Standard library only. Python 3.9+.

* **`distkv.Client`**: a smart client. It routes each key straight to its shard leader (routing
  verified bit for bit against the Go servers), follows `421` leader hints, retries with backoff,
  and makes writes **exactly-once** through a client session.
* **`distkv.historian`**: a small service that records cluster history and an incident timeline
  in **SQLite** for the live dashboard.

```python
from distkv import Client, Conflict, NotFound

c = Client("http://127.0.0.1:7101,http://127.0.0.1:7102")
v = c.put("user:42", "alice", ttl=600)          # seconds
item = c.get("user:42")                          # linearizable
print(item.text, item.version, item.shard, item.served_by)
try:
    c.compare_and_swap("user:42", v, "bob")
except Conflict as e:
    print("lost the race, now at", e.current_version)
c.get("user:42", stale=True)                     # any replica
```

CLI: `python -m distkv --endpoints http://127.0.0.1:7101 get user:42`

## Historian

```bash
python -m distkv.historian --endpoints http://127.0.0.1:7101 --db history.sqlite3 --listen 127.0.0.1:8090
```

| Endpoint | |
|---|---|
| `GET /history?minutes=15&points=300` | downsampled throughput, latency and health |
| `GET /events?limit=200` | leader changes, elections, node up/down, partitions, unavailable shards |
| `GET /nodes?minutes=15` | per-node samples |
| `GET /stats` | sample/event counts and database size |

The database runs in WAL mode, and old rows are pruned after `--retention-hours` (default 24).
`make demo` starts the historian automatically. Docker: `clients/python/Dockerfile`.

## Tests

```bash
python -m unittest discover -s tests                         # routing vectors, client, historian
DISTKV_ENDPOINTS=http://127.0.0.1:7101 python -m unittest tests.test_client   # + live cluster
```
