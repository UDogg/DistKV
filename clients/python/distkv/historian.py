"""DistKV historian: a small service that records cluster history in SQLite.

It polls ``GET /v1/cluster`` once per second, stores throughput/latency
samples and derives *events* (leader changes, elections, nodes going down or
up, partitions, shards losing their leader) by diffing consecutive
snapshots. The dashboard reads it to draw long-range charts and an incident
timeline that survive page reloads and dashboard restarts.

    python -m distkv.historian --endpoints http://127.0.0.1:7001 --db history.sqlite3

HTTP API (JSON, CORS enabled):
    GET /history?minutes=15&points=300   downsampled samples
    GET /events?limit=200&since=<ms>     derived events, newest first
    GET /nodes?minutes=15                per-node load samples
    GET /stats                           database statistics
    GET /healthz
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional

log = logging.getLogger("distkv.historian")

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
CREATE TABLE IF NOT EXISTS samples (
    ts             INTEGER PRIMARY KEY,   -- unix ms
    reads          REAL NOT NULL,
    writes         REAL NOT NULL,
    errors         REAL NOT NULL,
    forwarded      REAL NOT NULL,
    p50_ms         REAL NOT NULL,
    p99_ms         REAL NOT NULL,
    nodes_up       INTEGER NOT NULL,
    nodes_total    INTEGER NOT NULL,
    shards_healthy INTEGER NOT NULL,
    shards_total   INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS node_samples (
    ts        INTEGER NOT NULL,
    node      TEXT NOT NULL,
    reachable INTEGER NOT NULL,
    leads     INTEGER NOT NULL,
    reads     REAL NOT NULL,
    writes    REAL NOT NULL,
    p99_ms    REAL NOT NULL,
    PRIMARY KEY (ts, node)
);
CREATE TABLE IF NOT EXISTS events (
    id     INTEGER PRIMARY KEY AUTOINCREMENT,
    ts     INTEGER NOT NULL,
    kind   TEXT NOT NULL,      -- leader_change | election | node_down | node_up | partitioned | healed | shard_unavailable | shard_recovered
    node   TEXT,
    shard  INTEGER,
    detail TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_ts ON events (ts);
"""


class Store:
    """SQLite persistence. Safe for use from multiple threads."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.executescript(SCHEMA)

    def record(self, snap: dict, events: List[Dict[str, Any]]) -> None:
        load = snap.get("load", {})
        nodes = snap.get("nodes", [])
        shards = snap.get("shards", [])
        ts = int(snap.get("ts") or time.time() * 1000)
        with self._lock, self._db:
            self._db.execute(
                "INSERT OR REPLACE INTO samples VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    ts,
                    load.get("reads_per_sec", 0.0),
                    load.get("writes_per_sec", 0.0),
                    load.get("errors_per_sec", 0.0),
                    load.get("forwarded_per_sec", 0.0),
                    load.get("p50_ms", 0.0),
                    load.get("p99_ms", 0.0),
                    sum(1 for n in nodes if n.get("reachable")),
                    len(nodes),
                    snap.get("healthy_shards", 0),
                    len(shards),
                ),
            )
            self._db.executemany(
                "INSERT OR REPLACE INTO node_samples VALUES (?,?,?,?,?,?,?)",
                [
                    (
                        ts,
                        n["id"],
                        int(bool(n.get("reachable"))),
                        n.get("shards_led", 0),
                        (n.get("load") or {}).get("reads_per_sec", 0.0),
                        (n.get("load") or {}).get("writes_per_sec", 0.0),
                        (n.get("load") or {}).get("p99_ms", 0.0),
                    )
                    for n in nodes
                ],
            )
            self._db.executemany(
                "INSERT INTO events (ts, kind, node, shard, detail) VALUES (?,?,?,?,?)",
                [(ts, e["kind"], e.get("node"), e.get("shard"), e["detail"]) for e in events],
            )

    def history(self, minutes: float, points: int) -> List[Dict[str, Any]]:
        since = int(time.time() * 1000 - minutes * 60_000)
        bucket = max(1000, int(minutes * 60_000 / max(points, 1)))
        with self._lock:
            rows = self._db.execute(
                """SELECT (ts / ?) * ? AS t, AVG(reads) reads, AVG(writes) writes, AVG(errors) errors,
                          AVG(forwarded) forwarded, AVG(p50_ms) p50_ms, MAX(p99_ms) p99_ms,
                          MIN(nodes_up) nodes_up, MAX(nodes_total) nodes_total,
                          MIN(shards_healthy) shards_healthy, MAX(shards_total) shards_total
                   FROM samples WHERE ts >= ? GROUP BY t ORDER BY t""",
                (bucket, bucket, since),
            ).fetchall()
        return [dict(r) for r in rows]

    def node_history(self, minutes: float) -> List[Dict[str, Any]]:
        since = int(time.time() * 1000 - minutes * 60_000)
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM node_samples WHERE ts >= ? ORDER BY ts", (since,)
            ).fetchall()
        return [dict(r) for r in rows]

    def events(self, limit: int, since: int = 0) -> List[Dict[str, Any]]:
        with self._lock:
            rows = self._db.execute(
                "SELECT * FROM events WHERE ts >= ? ORDER BY id DESC LIMIT ?", (since, limit)
            ).fetchall()
        return [dict(r) for r in rows]

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            samples = self._db.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM samples").fetchone()
            kinds = self._db.execute("SELECT kind, COUNT(*) FROM events GROUP BY kind").fetchall()
        size = os.path.getsize(self.path) if os.path.exists(self.path) else 0
        return {
            "db_path": self.path,
            "db_bytes": size,
            "samples": samples[0],
            "first_ts": samples[1],
            "last_ts": samples[2],
            "events": {k: c for k, c in kinds},
        }

    def close(self) -> None:
        with self._lock:
            self._db.close()

    def prune(self, retention_hours: float) -> None:
        cutoff = int(time.time() * 1000 - retention_hours * 3_600_000)
        with self._lock, self._db:
            for table in ("samples", "node_samples", "events"):
                self._db.execute(f"DELETE FROM {table} WHERE ts < ?", (cutoff,))


def diff(prev: Optional[dict], cur: dict) -> List[Dict[str, Any]]:
    """Derive events by comparing two cluster snapshots."""
    if prev is None:
        return []
    out: List[Dict[str, Any]] = []
    pn = {n["id"]: n for n in prev.get("nodes", [])}
    for n in cur.get("nodes", []):
        before = pn.get(n["id"])
        if not before:
            continue
        if before.get("reachable") and not n.get("reachable"):
            out.append({"kind": "node_down", "node": n["id"], "detail": f"{n['id']} became unreachable"})
        elif not before.get("reachable") and n.get("reachable"):
            out.append({"kind": "node_up", "node": n["id"], "detail": f"{n['id']} is back"})
        was_iso = bool((before.get("faults") or {}).get("isolated"))
        is_iso = bool((n.get("faults") or {}).get("isolated"))
        if is_iso and not was_iso:
            out.append({"kind": "partitioned", "node": n["id"], "detail": f"{n['id']} partitioned from its peers"})
        elif was_iso and not is_iso:
            out.append({"kind": "healed", "node": n["id"], "detail": f"partition of {n['id']} healed"})
    ps = {s["shard"]: s for s in prev.get("shards", [])}
    for s in cur.get("shards", []):
        before = ps.get(s["shard"])
        if not before:
            continue
        old, new = before.get("leader") or "", s.get("leader") or ""
        if old and not new:
            out.append({"kind": "shard_unavailable", "shard": s["shard"], "node": old,
                        "detail": f"shard {s['shard']} lost its leader {old}"})
        elif new and not old:
            out.append({"kind": "shard_recovered", "shard": s["shard"], "node": new,
                        "detail": f"shard {s['shard']} recovered: leader {new} (term {s.get('term')})"})
        elif old != new:
            out.append({"kind": "leader_change", "shard": s["shard"], "node": new,
                        "detail": f"shard {s['shard']}: leader {old} -> {new} (term {s.get('term')})"})
        elif new and s.get("term", 0) > before.get("term", 0):
            out.append({"kind": "election", "shard": s["shard"], "node": new,
                        "detail": f"shard {s['shard']}: {new} re-elected in term {s.get('term')}"})
    return out


class Poller(threading.Thread):
    def __init__(self, store: Store, endpoints: List[str], interval: float, retention_hours: float) -> None:
        super().__init__(daemon=True)
        self.store = store
        self.endpoints = endpoints
        self.interval = interval
        self.retention_hours = retention_hours
        self.prev: Optional[dict] = None
        self.stop_event = threading.Event()
        self.last_error: Optional[str] = None

    def fetch(self) -> Optional[dict]:
        for ep in self.endpoints:
            try:
                with urllib.request.urlopen(ep.rstrip("/") + "/v1/cluster", timeout=2) as r:
                    return json.loads(r.read())
            except (OSError, ValueError) as e:
                self.last_error = f"{ep}: {e}"
        return None

    def poll_once(self) -> int:
        snap = self.fetch()
        if snap is None:
            return 0
        events = diff(self.prev, snap)
        self.store.record(snap, events)
        self.prev = snap
        for e in events:
            log.info("%s: %s", e["kind"], e["detail"])
        return len(events)

    def run(self) -> None:
        last_prune = 0.0
        while not self.stop_event.is_set():
            started = time.monotonic()
            try:
                self.poll_once()
                if started - last_prune > 60:
                    self.store.prune(self.retention_hours)
                    last_prune = started
            except Exception:  # keep recording no matter what
                log.exception("poll failed")
            self.stop_event.wait(max(0.0, self.interval - (time.monotonic() - started)))


def make_handler(store: Store):
    class Handler(BaseHTTPRequestHandler):
        server_version = "distkv-historian"

        def log_message(self, fmt: str, *args: Any) -> None:  # quiet
            log.debug(fmt, *args)

        def _send(self, status: int, payload: Any) -> None:
            data = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self) -> None:  # noqa: N802 (http.server API)
            u = urllib.parse.urlsplit(self.path)
            q = {k: v[-1] for k, v in urllib.parse.parse_qs(u.query).items()}
            try:
                if u.path == "/history":
                    self._send(200, store.history(float(q.get("minutes", 15)), int(q.get("points", 300))))
                elif u.path == "/nodes":
                    self._send(200, store.node_history(float(q.get("minutes", 15))))
                elif u.path == "/events":
                    self._send(200, store.events(int(q.get("limit", 200)), int(q.get("since", 0))))
                elif u.path == "/stats":
                    self._send(200, store.stats())
                elif u.path == "/healthz":
                    self._send(200, {"status": "ok"})
                else:
                    self._send(404, {"error": "not found"})
            except ValueError as e:
                self._send(400, {"error": str(e)})

    return Handler


def main(argv: Optional[List[str]] = None) -> None:
    p = argparse.ArgumentParser(description="Record DistKV cluster history in SQLite.")
    p.add_argument("--endpoints", default=os.environ.get("DISTKV_ENDPOINTS", "http://127.0.0.1:7001"))
    p.add_argument("--db", default=os.environ.get("DISTKV_HISTORY_DB", "distkv-history.sqlite3"))
    p.add_argument("--listen", default=os.environ.get("DISTKV_HISTORIAN_LISTEN", "127.0.0.1:8090"))
    p.add_argument("--interval", type=float, default=1.0, help="seconds between polls")
    p.add_argument("--retention-hours", type=float, default=24.0)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    store = Store(args.db)
    poller = Poller(store, [e.strip() for e in args.endpoints.split(",") if e.strip()], args.interval, args.retention_hours)
    poller.start()
    host, _, port = args.listen.rpartition(":")
    srv = ThreadingHTTPServer((host or "0.0.0.0", int(port)), make_handler(store))
    log.info("historian listening on http://%s, recording to %s", args.listen, args.db)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        poller.stop_event.set()
        srv.server_close()
        store.close()


if __name__ == "__main__":
    main()
