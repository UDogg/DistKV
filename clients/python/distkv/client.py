"""Smart DistKV client.

Like the Go client it downloads the topology once, routes each key straight
to its shard leader (learning leaders from ``421`` hints), retries with
exponential backoff until the deadline, and makes writes exactly-once with a
client session (``X-Distkv-Client-Id`` / ``X-Distkv-Seq``).

    >>> from distkv import Client
    >>> c = Client(["http://127.0.0.1:7001"])
    >>> c.put("user:42", b"alice", ttl=600)
    >>> c.get("user:42").value
    b'alice'
"""

from __future__ import annotations

import http.client
import json
import random
import secrets
import ssl
import threading
import time
import urllib.parse
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple, Union

from .routing import Topology

__all__ = ["Client", "Item", "DistKVError", "NotFound", "Conflict", "Unavailable"]


class DistKVError(Exception):
    """Base class for client errors."""


class NotFound(DistKVError, KeyError):
    """The key does not exist (or has expired)."""


class Conflict(DistKVError):
    """A compare-and-set precondition failed."""

    def __init__(self, key: str, current_version: int):
        super().__init__(f"version conflict on {key!r} (current version {current_version})")
        self.key = key
        self.current_version = current_version


class Unavailable(DistKVError):
    """The request could not be completed before the deadline."""


@dataclass
class Item:
    key: str
    value: bytes
    version: int
    shard: int
    served_by: str
    expires_at: Optional[str] = None

    @property
    def text(self) -> str:
        return self.value.decode("utf-8")


class _Sessions:
    """Write sequence numbers and the acknowledgement watermark."""

    def __init__(self) -> None:
        self.client_id = (secrets.randbits(63)) | 1
        self._next = 1
        self._inflight: set = set()
        self._lock = threading.Lock()

    def begin(self) -> int:
        with self._lock:
            seq = self._next
            self._next += 1
            self._inflight.add(seq)
            return seq

    def end(self, seq: int) -> None:
        with self._lock:
            self._inflight.discard(seq)

    def ack_below(self) -> int:
        with self._lock:
            return min(self._inflight) if self._inflight else self._next


class Client:
    """Thread-safe DistKV client."""

    def __init__(
        self,
        endpoints: Union[str, Iterable[str]],
        timeout: float = 5.0,
        smart: bool = True,
        sessions: bool = True,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        if isinstance(endpoints, str):
            endpoints = endpoints.split(",")
        self.endpoints = [_normalise(e) for e in endpoints if e.strip()]
        if not self.endpoints:
            raise ValueError("no endpoints")
        self.timeout = timeout
        self.smart = smart
        self.ssl_context = ssl_context
        self._sessions = _Sessions() if sessions else None
        self._local = threading.local()
        self._leaders: Dict[int, str] = {}
        self._leaders_lock = threading.Lock()
        self._rr = 0
        status = self.cluster()
        self.topology = Topology.from_cluster_status(status)
        for s in status.get("shards", []):
            if s.get("leader_addr"):
                self._leaders[s["shard"]] = s["leader_addr"]

    # -- public API -------------------------------------------------------

    def get(self, key: str, stale: bool = False) -> Item:
        query = {"consistency": "stale"} if stale else {}
        status, headers, body = self._do("GET", key, query, None, any_replica=stale)
        if status == 404:
            raise NotFound(key)
        if status != 200:
            raise _error(key, status, body)
        return Item(
            key=key,
            value=body,
            version=int(headers.get("x-distkv-version", "0")),
            shard=int(headers.get("x-distkv-shard", "-1")),
            served_by=headers.get("x-distkv-served-by", ""),
            expires_at=headers.get("x-distkv-expires-at"),
        )

    def put(
        self,
        key: str,
        value: Union[bytes, str],
        ttl: Optional[float] = None,
        if_version: Optional[int] = None,
    ) -> int:
        """Write a key and return its new version. ``ttl`` is in seconds;
        ``if_version`` makes the write a compare-and-set (0 = create only)."""
        if isinstance(value, str):
            value = value.encode("utf-8")
        query: Dict[str, str] = {}
        if ttl:
            query["ttl"] = f"{int(ttl * 1000)}ms"
        if if_version is not None:
            query["if_version"] = str(if_version)
        status, _, body = self._do("PUT", key, query, value)
        if status == 200:
            return int(json.loads(body)["version"])
        if status == 409:
            raise Conflict(key, int(json.loads(body).get("current_version", 0)))
        raise _error(key, status, body)

    def compare_and_swap(self, key: str, expected_version: int, value: Union[bytes, str], ttl: Optional[float] = None) -> int:
        return self.put(key, value, ttl=ttl, if_version=expected_version)

    def delete(self, key: str) -> None:
        status, _, body = self._do("DELETE", key, {}, None)
        if status == 404:
            raise NotFound(key)
        if status != 200:
            raise _error(key, status, body)

    def cluster(self) -> dict:
        """Aggregated cluster status from the first reachable endpoint."""
        last: Optional[Exception] = None
        for ep in self.endpoints:
            try:
                status, _, body = self._request(ep, "GET", "/v1/cluster", None, {})
                if status == 200:
                    return json.loads(body)
                last = DistKVError(f"{ep}: HTTP {status}")
            except OSError as e:
                last = e
        raise Unavailable(f"no endpoint reachable: {last}")

    def shard_for_key(self, key: str) -> int:
        return self.topology.shard_for_key(key)

    def close(self) -> None:
        conns = getattr(self._local, "conns", {})
        for c in conns.values():
            c.close()
        conns.clear()

    def __enter__(self) -> "Client":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # -- internals --------------------------------------------------------

    def _do(self, method: str, key: str, query: Dict[str, str], body: Optional[bytes], any_replica: bool = False):
        shard = self.topology.shard_for_key(key)
        path = "/v1/kv/" + "/".join(urllib.parse.quote(p, safe="") for p in key.split("/"))
        if query:
            path += "?" + urllib.parse.urlencode(query)
        write = method != "GET"
        seq = self._sessions.begin() if (write and self._sessions) else 0
        deadline = time.monotonic() + self.timeout
        last = "no attempt"
        attempt = redirects = 0
        try:
            while time.monotonic() < deadline:
                base = self._target(shard, attempt, any_replica)
                headers = {}
                if self.smart:
                    headers["X-Distkv-No-Forward"] = "1"
                if seq:
                    headers["X-Distkv-Client-Id"] = str(self._sessions.client_id)
                    headers["X-Distkv-Seq"] = str(seq)
                    headers["X-Distkv-Ack-Below"] = str(self._sessions.ack_below())
                try:
                    status, hdrs, data = self._request(base, method, path, body, headers)
                except OSError as e:
                    last = str(e)
                    self._forget(shard, base)
                    attempt += 1
                    self._sleep(attempt, deadline)
                    continue
                if status == 421:
                    err = _json(data)
                    if err.get("leader_addr") and err["leader_addr"] != base and redirects < 3:
                        self._set_leader(shard, err["leader_addr"])
                        redirects += 1
                        continue
                    self._forget(shard, base)
                    last = err.get("error", "misdirected")
                elif status in (502, 503, 504):
                    err = _json(data)
                    last = f"{err.get('error')} ({err.get('code')})"
                    if err.get("code") in ("shutting_down", "unavailable", "no_leader"):
                        self._forget(shard, base)
                else:
                    if not any_replica and status < 500:
                        self._set_leader(shard, base)
                    return status, hdrs, data
                attempt += 1
                self._sleep(attempt, deadline)
            raise Unavailable(f"{method} {key!r}: {last}")
        finally:
            if seq:
                self._sessions.end(seq)

    def _target(self, shard: int, attempt: int, any_replica: bool) -> str:
        if not self.smart:
            self._rr += 1
            return self.endpoints[(self._rr + attempt) % len(self.endpoints)]
        reps = self.topology.replicas(shard)
        if any_replica:
            self._rr += 1
            return self.topology.member(reps[self._rr % len(reps)]).addr
        with self._leaders_lock:
            leader = self._leaders.get(shard)
        if leader:
            return leader
        return self.topology.member(reps[attempt % len(reps)]).addr

    def _set_leader(self, shard: int, addr: str) -> None:
        with self._leaders_lock:
            self._leaders[shard] = addr

    def _forget(self, shard: int, addr: str) -> None:
        with self._leaders_lock:
            if self._leaders.get(shard) == addr:
                del self._leaders[shard]

    @staticmethod
    def _sleep(attempt: int, deadline: float) -> None:
        d = 0.005 * (2 ** min(attempt, 6))
        d = d / 2 + random.random() * d
        time.sleep(max(0.0, min(d, deadline - time.monotonic())))

    def _conn(self, base: str) -> http.client.HTTPConnection:
        conns = getattr(self._local, "conns", None)
        if conns is None:
            conns = self._local.conns = {}
        c = conns.get(base)
        if c is None:
            u = urllib.parse.urlsplit(base)
            if u.scheme == "https":
                c = http.client.HTTPSConnection(u.hostname, u.port or 443, timeout=self.timeout, context=self.ssl_context)
            else:
                c = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=self.timeout)
            conns[base] = c
        return c

    def _request(self, base: str, method: str, path: str, body: Optional[bytes], headers: Dict[str, str]) -> Tuple[int, Dict[str, str], bytes]:
        for retry in (0, 1):  # one transparent retry on a stale keep-alive connection
            c = self._conn(base)
            try:
                c.request(method, path, body=body, headers=headers)
                r = c.getresponse()
                data = r.read()
                return r.status, {k.lower(): v for k, v in r.getheaders()}, data
            except (http.client.HTTPException, OSError):
                c.close()
                self._local.conns.pop(base, None)
                if retry or method != "GET":
                    raise
        raise AssertionError("unreachable")


def _normalise(endpoint: str) -> str:
    endpoint = endpoint.strip().rstrip("/")
    return endpoint if "://" in endpoint else "http://" + endpoint


def _json(data: bytes) -> dict:
    try:
        return json.loads(data)
    except ValueError:
        return {}


def _error(key: str, status: int, body: bytes) -> DistKVError:
    e = _json(body)
    return DistKVError(f"{key!r}: HTTP {status}: {e.get('error', '')} ({e.get('code', '')})")


def batch_get(client: Client, keys: List[str]) -> Dict[str, Optional[bytes]]:
    """Convenience helper: fetch many keys (None for missing)."""
    out: Dict[str, Optional[bytes]] = {}
    for k in keys:
        try:
            out[k] = client.get(k).value
        except NotFound:
            out[k] = None
    return out
