import { test } from "node:test";
import assert from "node:assert/strict";
import { Client, ConflictError, NotFoundError } from "../src/client.js";

// An in-memory fake cluster: node A redirects to leader B.
function fakeCluster() {
  const store = new Map<string, { v: Uint8Array; ver: number }>();
  let version = 0;
  const hits = { a: 0, b: 0 };
  const seqs: string[] = [];
  const spec = { members: [{ id: "a", addr: "http://a" }, { id: "b", addr: "http://b" }], shards: 1, replication_factor: 2, virtual_nodes: 8 };
  const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json" } });
  const fetchImpl: typeof fetch = async (input, init) => {
    const url = new URL(String(input));
    if (url.pathname === "/v1/cluster") return json(200, { spec, shards: [{ shard: 0 }] });
    if (url.host === "a") {
      hits.a++;
      return json(421, { error: "not leader", code: "not_leader", leader_addr: "http://b" });
    }
    hits.b++;
    const key = decodeURIComponent(url.pathname.slice("/v1/kv/".length));
    const method = init?.method ?? "GET";
    if (method === "GET") {
      const e = store.get(key);
      if (!e) return json(404, { code: "not_found" });
      return new Response(e.v as unknown as BodyInit, { status: 200, headers: { "x-distkv-version": String(e.ver), "x-distkv-shard": "0", "x-distkv-served-by": "b" } });
    }
    if (method === "PUT") {
      seqs.push(new Headers(init?.headers).get("x-distkv-seq") ?? "");
      const want = url.searchParams.get("if_version");
      const cur = store.get(key)?.ver ?? 0;
      if (want !== null && Number(want) !== cur) return json(409, { code: "conflict", current_version: cur });
      store.set(key, { v: init!.body as Uint8Array, ver: ++version });
      return json(200, { key, version });
    }
    if (!store.delete(key)) return json(404, { code: "not_found" });
    return json(200, {});
  };
  return { fetchImpl, hits, seqs };
}

test("CRUD, CAS and leader discovery", async () => {
  const f = fakeCluster();
  const c = await Client.connect({ endpoints: ["http://a"], fetch: f.fetchImpl });
  const v1 = await c.put("user:42", "alice", { ttlMs: 60_000 });
  assert.equal((await c.get("user:42")).text(), "alice");
  await assert.rejects(c.compareAndSwap("user:42", v1 + 9, "x"), (e: unknown) => e instanceof ConflictError && e.currentVersion === v1);
  await c.compareAndSwap("user:42", v1, "bob");
  assert.equal((await c.get("user:42")).text(), "bob");
  await c.delete("user:42");
  await assert.rejects(c.get("user:42"), NotFoundError);
  assert.ok(f.hits.a <= 1, "leader should be cached after one redirect");
  assert.ok(f.seqs.every((s) => /^\d+$/.test(s)), "writes carry session sequence numbers");
});

test("live cluster round trip", { skip: !process.env.DISTKV_ENDPOINTS }, async () => {
  const c = await Client.connect({ endpoints: process.env.DISTKV_ENDPOINTS!.split(",") });
  for (let i = 0; i < 30; i++) await c.put(`ts:${i}`, `v${i}`);
  for (let i = 0; i < 30; i++) {
    const it = await c.get(`ts:${i}`);
    assert.equal(it.text(), `v${i}`);
    assert.equal(it.shard, c.shardForKey(`ts:${i}`), "client routing must match the server");
  }
});
