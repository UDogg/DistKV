"""Key -> shard and shard -> replica placement, bit-for-bit identical to the
Go implementation (internal/cluster). Verified against
testdata/routing/vectors.tsv by the test suite."""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field
from typing import Dict, List, Sequence, Tuple

MASK64 = (1 << 64) - 1
DEFAULT_VIRTUAL_NODES = 128


def hash64(s: str) -> int:
    """FNV-1a (64-bit) followed by the SplitMix64 finaliser."""
    h = 14695981039346656037
    for b in s.encode("utf-8"):
        h ^= b
        h = (h * 1099511628211) & MASK64
    h ^= h >> 30
    h = (h * 0xBF58476D1CE4E5B9) & MASK64
    h ^= h >> 27
    h = (h * 0x94D049BB133111EB) & MASK64
    h ^= h >> 31
    return h


@dataclass(frozen=True)
class Member:
    id: str
    addr: str


@dataclass
class Topology:
    """A resolved cluster specification (see Go cluster.Topology)."""

    members: Sequence[Member]
    shards: int
    replication_factor: int = 3
    virtual_nodes: int = DEFAULT_VIRTUAL_NODES
    _ring_hashes: List[int] = field(init=False, repr=False)
    _ring_shards: List[int] = field(init=False, repr=False)
    _replicas: List[List[str]] = field(init=False, repr=False)
    _by_id: Dict[str, Member] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if self.shards <= 0:
            raise ValueError("shards must be positive")
        if self.virtual_nodes <= 0:
            self.virtual_nodes = DEFAULT_VIRTUAL_NODES
        self.members = sorted(self.members, key=lambda m: m.id)
        if self.replication_factor <= 0:
            self.replication_factor = min(3, len(self.members))
        if self.replication_factor > len(self.members):
            raise ValueError("replication factor exceeds member count")
        self._by_id = {m.id: m for m in self.members}
        points: List[Tuple[int, int]] = [
            (hash64(f"shard-{s}-vnode-{v}"), s)
            for s in range(self.shards)
            for v in range(self.virtual_nodes)
        ]
        points.sort()
        self._ring_hashes = [p[0] for p in points]
        self._ring_shards = [p[1] for p in points]
        self._replicas = [self._rendezvous(s) for s in range(self.shards)]
        self._balance_preferred()

    def _balance_preferred(self) -> None:
        """Spread preferred leaders evenly (see Go Topology.balancePreferred)."""
        count: Dict[str, int] = {m.id: 0 for m in self.members}
        for s, reps in enumerate(self._replicas):
            best = 0
            for i in range(1, len(reps)):
                if count[reps[i]] < count[reps[best]]:
                    best = i
            if best:
                self._replicas[s] = [reps[best]] + reps[:best] + reps[best + 1 :]
            count[self._replicas[s][0]] += 1

    def _rendezvous(self, shard: int) -> List[str]:
        scored = sorted(self.members, key=lambda m: (-hash64(f"{shard}/{m.id}"), m.id))
        return [m.id for m in scored[: self.replication_factor]]

    def shard_for_key(self, key: str) -> int:
        i = bisect.bisect_left(self._ring_hashes, hash64(key))
        if i == len(self._ring_hashes):
            i = 0
        return self._ring_shards[i]

    def replicas(self, shard: int) -> List[str]:
        return self._replicas[shard]

    def member(self, node_id: str) -> Member:
        return self._by_id[node_id]

    @classmethod
    def from_cluster_status(cls, status: dict) -> "Topology":
        spec = status["spec"]
        return cls(
            members=[Member(m["id"], m["addr"]) for m in spec["members"]],
            shards=spec["shards"],
            replication_factor=spec.get("replication_factor", 3),
            virtual_nodes=spec.get("virtual_nodes", DEFAULT_VIRTUAL_NODES),
        )
