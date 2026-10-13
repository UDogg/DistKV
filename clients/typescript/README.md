# @distkv/client 📦

A TypeScript/JavaScript client for DistKV with zero runtime dependencies. It runs on Node 18+,
Deno, Bun and in browsers (anything with `fetch`).

```ts
import { Client, ConflictError } from "@distkv/client";

const c = await Client.connect({ endpoints: ["http://127.0.0.1:7101"] });
const v = await c.put("user:42", "alice", { ttlMs: 60_000 });
const item = await c.get("user:42");             // linearizable
console.log(item.text(), item.version, item.shard);
try {
  await c.compareAndSwap("user:42", v, "bob");
} catch (e) {
  if (e instanceof ConflictError) console.log("current version", e.currentVersion);
}
```

* Routing uses `BigInt` for exact 64-bit FNV-1a + SplitMix64 and is verified against the Go
  servers' golden vectors. The same module powers the dashboard's hash-ring visualisation.
* It follows `421` leader hints, retries with backoff until `timeoutMs`, and gives writes
  exactly-once semantics through a session.

```bash
npm ci && npm test        # vectors + fake-cluster tests
DISTKV_ENDPOINTS=http://127.0.0.1:7101 npm test   # + live cluster
npm run build             # ESM + .d.ts into dist/
```
