import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { Topology, hash64 } from "../src/routing.js";

// build/test/routing.test.js -> repo root is five levels up.
const here = dirname(fileURLToPath(import.meta.url));
const vectors = readFileSync(resolve(here, "../../../../testdata/routing/vectors.tsv"), "utf8")
  .split("\n")
  .filter((l) => l && !l.startsWith("#"))
  .map((l) => l.split("\t"));

const topo = new Topology({
  members: [5, 4, 3, 2, 1].map((i) => ({ id: `n${i}`, addr: `http://127.0.0.1:${7100 + i}` })),
  shards: 24,
  replication_factor: 3,
  virtual_nodes: 128,
});

test("hash64 matches Go", () => {
  const rows = vectors.filter((v) => v[0] === "hash");
  assert.ok(rows.length > 200);
  for (const [, key, want] of rows) assert.equal(hash64(key), BigInt(want), key);
});

test("shardForKey matches Go", () => {
  for (const [, key, want] of vectors.filter((v) => v[0] === "shard")) assert.equal(topo.shardForKey(key), Number(want), key);
});

test("replica placement matches Go", () => {
  for (const [, shard, want] of vectors.filter((v) => v[0] === "replicas")) assert.deepEqual(topo.replicas(Number(shard)), want.split(","));
});
