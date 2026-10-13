import json
import os
import tempfile
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer

from distkv.historian import Poller, Store, diff, make_handler


def snapshot(ts, leaders, reachable=("n1", "n2", "n3"), terms=None, isolated=()):
    nodes = [{"id": n, "reachable": n in reachable, "shards_led": list(leaders.values()).count(n),
              "faults": {"isolated": n in isolated},
              "load": {"reads_per_sec": 100.0, "writes_per_sec": 25.0, "p99_ms": 3.0}} for n in ("n1", "n2", "n3")]
    shards = [{"shard": s, "leader": l, "term": (terms or {}).get(s, 1)} for s, l in leaders.items()]
    return {"ts": ts, "nodes": nodes, "shards": shards, "healthy_shards": sum(1 for l in leaders.values() if l),
            "load": {"reads_per_sec": 300.0, "writes_per_sec": 75.0, "p50_ms": 0.5, "p99_ms": 3.0}}


class DiffTest(unittest.TestCase):
    def test_events(self):
        a = snapshot(1, {0: "n1", 1: "n2", 2: "n3"})
        b = snapshot(2, {0: "n2", 1: "", 2: "n3"}, reachable=("n2", "n3"), terms={2: 2}, isolated=("n3",))
        kinds = sorted(e["kind"] for e in diff(a, b))
        self.assertEqual(kinds, ["election", "leader_change", "node_down", "partitioned", "shard_unavailable"])
        self.assertEqual(diff(None, a), [])
        c = snapshot(3, {0: "n2", 1: "n1", 2: "n3"}, terms={2: 2})
        kinds = sorted(e["kind"] for e in diff(b, c))
        self.assertEqual(kinds, ["healed", "node_up", "shard_recovered"])


class StoreAndAPITest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.store = Store(os.path.join(self.dir.name, "h.sqlite3"))

    def tearDown(self):
        self.store.close()
        self.dir.cleanup()

    def test_record_query_and_persist(self):
        import time
        now = int(time.time() * 1000)
        a = snapshot(now - 2000, {0: "n1"})
        b = snapshot(now - 1000, {0: "n2"})
        self.store.record(a, [])
        self.store.record(b, diff(a, b))
        hist = self.store.history(minutes=1, points=60)
        self.assertEqual(len(hist), 2)
        self.assertAlmostEqual(hist[0]["reads"], 300.0)
        self.assertEqual(self.store.events(10)[0]["kind"], "leader_change")
        self.assertEqual(len(self.store.node_history(1)), 6)
        # Data survives reopening the database.
        reopened = Store(self.store.path)
        self.assertEqual(reopened.stats()["samples"], 2)
        reopened.prune(retention_hours=0)
        self.assertEqual(reopened.stats()["samples"], 0)
        reopened.close()

    def test_http_api(self):
        import time
        self.store.record(snapshot(int(time.time() * 1000), {0: "n1"}), [])
        srv = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.store))
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{srv.server_address[1]}"
        try:
            for path in ("/history?minutes=5", "/events", "/stats", "/nodes", "/healthz"):
                with urllib.request.urlopen(base + path) as r:
                    self.assertEqual(r.status, 200)
                    self.assertEqual(r.headers["Access-Control-Allow-Origin"], "*")
                    json.loads(r.read())
        finally:
            srv.shutdown()
            srv.server_close()

    def test_poller_against_fake_cluster(self):
        snaps = [snapshot(1000 + i, {0: "n1" if i < 2 else "n2"}) for i in range(4)]
        served = iter(snaps)
        poller = Poller(self.store, ["http://unused"], 1.0, 24)
        poller.fetch = lambda: next(served)
        events = sum(poller.poll_once() for _ in snaps)
        self.assertEqual(events, 1)
        self.assertEqual(self.store.stats()["samples"], 4)


if __name__ == "__main__":
    unittest.main()
