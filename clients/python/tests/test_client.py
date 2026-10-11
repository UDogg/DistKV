"""Client behaviour against an in-process fake cluster (and optionally a real one)."""

import json
import os
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from distkv import Client, Conflict, NotFound, Unavailable


class FakeNode:
    """One fake node. If `leader` is another node's URL it answers 421."""

    def __init__(self):
        self.store = {}
        self.version = 0
        self.redirect_to = None
        self.hits = 0
        self.seen_seqs = []
        node = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):
                pass

            def _reply(self, status, body=b"", headers=None, ctype="application/json"):
                self.send_response(status)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                for k, v in (headers or {}).items():
                    self.send_header(k, v)
                self.end_headers()
                self.wfile.write(body)

            def _route(self):
                if self.path == "/v1/cluster":
                    spec = {"members": [{"id": "a", "addr": A.url}, {"id": "b", "addr": B.url}],
                            "shards": 1, "replication_factor": 2, "virtual_nodes": 8}
                    return self._reply(200, json.dumps({"spec": spec, "shards": [{"shard": 0}]}).encode())
                node.hits += 1
                if node.redirect_to:
                    return self._reply(421, json.dumps({"error": "not leader", "code": "not_leader",
                                                        "leader_addr": node.redirect_to.url}).encode())
                key = self.path.split("/v1/kv/", 1)[1].split("?")[0]
                if self.command == "GET":
                    if key not in node.store:
                        return self._reply(404, b'{"code":"not_found"}')
                    v, ver = node.store[key]
                    return self._reply(200, v, {"X-Distkv-Version": str(ver), "X-Distkv-Shard": "0",
                                                "X-Distkv-Served-By": "b"}, "application/octet-stream")
                if self.command == "PUT":
                    body = self.rfile.read(int(self.headers["Content-Length"]))
                    node.seen_seqs.append(self.headers.get("X-Distkv-Seq"))
                    if "if_version=" in self.path:
                        want = int(self.path.split("if_version=")[1])
                        cur = node.store.get(key, (None, 0))[1]
                        if cur != want:
                            return self._reply(409, json.dumps({"code": "conflict", "current_version": cur}).encode())
                    node.version += 1
                    node.store[key] = (body, node.version)
                    return self._reply(200, json.dumps({"key": key, "version": node.version}).encode())
                if self.command == "DELETE":
                    if node.store.pop(key, None) is None:
                        return self._reply(404, b'{"code":"not_found"}')
                    return self._reply(200, b"{}")

            do_GET = do_PUT = do_DELETE = _route

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()


A = B = None


class ClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        global A, B
        A, B = FakeNode(), FakeNode()
        A.redirect_to = B  # A is a follower pointing at leader B

    @classmethod
    def tearDownClass(cls):
        for n in (A, B):
            n.srv.shutdown()
            n.srv.server_close()

    def test_crud_cas_and_leader_discovery(self):
        c = Client([A.url], timeout=2)
        v1 = c.put("k", "hello", ttl=30)
        self.assertEqual(c.get("k").text, "hello")
        with self.assertRaises(Conflict) as cm:
            c.put("k", "x", if_version=v1 + 100)
        self.assertEqual(cm.exception.current_version, v1)
        c.compare_and_swap("k", v1, "world")
        self.assertEqual(c.get("k").value, b"world")
        c.delete("k")
        with self.assertRaises(NotFound):
            c.get("k")
        self.assertLessEqual(A.hits, 1, "client should cache the leader after one redirect")
        self.assertTrue(all(s and s.isdigit() for s in B.seen_seqs), "writes must carry session sequence numbers")
        c.close()

    def test_unavailable_deadline(self):
        with self.assertRaises(Unavailable):
            Client(["http://127.0.0.1:1"], timeout=0.2)


@unittest.skipUnless(os.environ.get("DISTKV_ENDPOINTS"), "set DISTKV_ENDPOINTS to test against a live cluster")
class LiveClusterTest(unittest.TestCase):
    def test_roundtrip(self):
        c = Client(os.environ["DISTKV_ENDPOINTS"])
        for i in range(50):
            c.put(f"py:{i}", f"value-{i}")
        for i in range(50):
            it = c.get(f"py:{i}")
            self.assertEqual(it.text, f"value-{i}")
            self.assertEqual(it.shard, c.shard_for_key(f"py:{i}"), "client routing must match the server")


if __name__ == "__main__":
    unittest.main()
