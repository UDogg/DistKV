"""Routing parity with the Go implementation (golden vectors)."""

import pathlib
import unittest

from distkv.routing import Member, Topology, hash64

VECTORS = pathlib.Path(__file__).resolve().parents[3] / "testdata" / "routing" / "vectors.tsv"


def load():
    hashes, shards, replicas = [], [], {}
    for line in VECTORS.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#"):
            continue
        kind, *rest = line.split("\t")
        if kind == "hash":
            hashes.append((rest[0], int(rest[1])))
        elif kind == "shard":
            shards.append((rest[0], int(rest[1])))
        elif kind == "replicas":
            replicas[int(rest[0])] = rest[1].split(",")
    return hashes, shards, replicas


class RoutingParity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.hashes, cls.shards, cls.replicas = load()
        cls.topo = Topology(
            members=[Member(f"n{i}", f"http://127.0.0.1:{7100 + i}") for i in range(5, 0, -1)],
            shards=24, replication_factor=3, virtual_nodes=128,
        )

    def test_vectors_present(self):
        self.assertGreater(len(self.hashes), 200)

    def test_hash64(self):
        for key, want in self.hashes:
            self.assertEqual(hash64(key), want, key)

    def test_shard_for_key(self):
        for key, want in self.shards:
            self.assertEqual(self.topo.shard_for_key(key), want, key)

    def test_replicas(self):
        for shard, want in self.replicas.items():
            self.assertEqual(self.topo.replicas(shard), want, shard)


if __name__ == "__main__":
    unittest.main()
