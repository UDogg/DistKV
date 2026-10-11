import { Topology, type Spec } from "./routing.js";

/** Error hierarchy. */
export class DistKVError extends Error {
  constructor(message: string, readonly status?: number, readonly code?: string) {
    super(message);
    this.name = "DistKVError";
  }
}
export class NotFoundError extends DistKVError {
  constructor(key: string) {
    super(`key not found: ${key}`, 404, "not_found");
    this.name = "NotFoundError";
  }
}
export class ConflictError extends DistKVError {
  constructor(readonly key: string, readonly currentVersion: number) {
    super(`version conflict on ${key} (current version ${currentVersion})`, 409, "conflict");
    this.name = "ConflictError";
  }
}
export class UnavailableError extends DistKVError {
  constructor(message: string) {
    super(message, 503, "unavailable");
    this.name = "UnavailableError";
  }
}

export interface ClientOptions {
  /** Seed node URLs. */
  endpoints: string[];
  /** Per-operation deadline in milliseconds (default 5000). */
  timeoutMs?: number;
  /** Route requests straight to shard leaders (default true). */
  smart?: boolean;
  /** Exactly-once write sessions (default true). */
  sessions?: boolean;
  /** Custom fetch implementation (tests, proxies). */
  fetch?: typeof fetch;
}

export interface Item {
  key: string;
  value: Uint8Array;
  version: number;
  shard: number;
  servedBy: string;
  expiresAt?: string;
  /** Value decoded as UTF-8. */
  text(): string;
}

export interface PutOptions {
  /** Time to live in milliseconds. */
  ttlMs?: number;
  /** Compare-and-set on this version (0 = create only). */
  ifVersion?: number;
}

interface ApiError {
  error?: string;
  code?: string;
  leader_addr?: string;
  current_version?: number;
}

const decoder = new TextDecoder();

/** Session sequence numbers with an acknowledgement watermark. */
class Sessions {
  readonly clientId: bigint;
  private next = 1;
  private readonly inflight = new Set<number>();

  constructor() {
    const buf = new Uint32Array(2);
    globalThis.crypto.getRandomValues(buf);
    this.clientId = ((BigInt(buf[0]) << 32n) | BigInt(buf[1])) >> 1n | 1n;
  }
  begin(): number {
    const s = this.next++;
    this.inflight.add(s);
    return s;
  }
  end(seq: number): void {
    this.inflight.delete(seq);
  }
  ackBelow(): number {
    let low = this.next;
    for (const s of this.inflight) low = Math.min(low, s);
    return low;
  }
}

/**
 * Smart DistKV client: routes each key to its shard leader, follows 421
 * hints, retries with backoff and makes writes exactly-once.
 */
export class Client {
  private topology!: Topology;
  private readonly leaders = new Map<number, string>();
  private readonly endpoints: string[];
  private readonly timeoutMs: number;
  private readonly smart: boolean;
  private readonly sessions?: Sessions;
  private readonly fetchImpl: typeof fetch;
  private rr = 0;

  private constructor(opts: ClientOptions) {
    this.endpoints = opts.endpoints.map((e) => (e.includes("://") ? e : `http://${e}`).replace(/\/+$/, ""));
    if (this.endpoints.length === 0) throw new Error("no endpoints");
    this.timeoutMs = opts.timeoutMs ?? 5000;
    this.smart = opts.smart ?? true;
    this.sessions = opts.sessions === false ? undefined : new Sessions();
    this.fetchImpl = opts.fetch ?? globalThis.fetch.bind(globalThis);
  }

  /** Connect and download the cluster topology. */
  static async connect(opts: ClientOptions): Promise<Client> {
    const c = new Client(opts);
    const status = await c.cluster();
    c.topology = new Topology(status.spec as Spec);
    for (const s of status.shards ?? []) if (s.leader_addr) c.leaders.set(s.shard, s.leader_addr);
    return c;
  }

  shardForKey(key: string): number {
    return this.topology.shardForKey(key);
  }

  async cluster(): Promise<any> {
    let last: unknown;
    for (const ep of this.endpoints) {
      try {
        const r = await this.fetchImpl(`${ep}/v1/cluster`, { signal: AbortSignal.timeout(this.timeoutMs) });
        if (r.ok) return await r.json();
        last = new Error(`HTTP ${r.status}`);
      } catch (e) {
        last = e;
      }
    }
    throw new UnavailableError(`no endpoint reachable: ${String(last)}`);
  }

  async get(key: string, opts: { stale?: boolean } = {}): Promise<Item> {
    const r = await this.do("GET", key, opts.stale ? { consistency: "stale" } : {}, undefined, !!opts.stale);
    if (r.status === 404) throw new NotFoundError(key);
    if (r.status !== 200) throw await toError(key, r);
    const value = new Uint8Array(await r.arrayBuffer());
    return {
      key,
      value,
      version: Number(r.headers.get("x-distkv-version") ?? 0),
      shard: Number(r.headers.get("x-distkv-shard") ?? -1),
      servedBy: r.headers.get("x-distkv-served-by") ?? "",
      expiresAt: r.headers.get("x-distkv-expires-at") ?? undefined,
      text: () => decoder.decode(value),
    };
  }

  /** Write a key; resolves to its new version. */
  async put(key: string, value: string | Uint8Array, opts: PutOptions = {}): Promise<number> {
    const q: Record<string, string> = {};
    if (opts.ttlMs) q.ttl = `${Math.round(opts.ttlMs)}ms`;
    if (opts.ifVersion !== undefined) q.if_version = String(opts.ifVersion);
    const body = typeof value === "string" ? new TextEncoder().encode(value) : value;
    const r = await this.do("PUT", key, q, body);
    if (r.status === 200) return Number((await r.json()).version);
    if (r.status === 409) throw new ConflictError(key, Number(((await r.json()) as ApiError).current_version ?? 0));
    throw await toError(key, r);
  }

  compareAndSwap(key: string, expected: number, value: string | Uint8Array, ttlMs?: number): Promise<number> {
    return this.put(key, value, { ifVersion: expected, ttlMs });
  }

  async delete(key: string): Promise<void> {
    const r = await this.do("DELETE", key, {}, undefined);
    if (r.status === 404) throw new NotFoundError(key);
    if (r.status !== 200) throw await toError(key, r);
  }

  private target(shard: number, attempt: number, anyReplica: boolean): string {
    if (!this.smart) return this.endpoints[(this.rr++ + attempt) % this.endpoints.length];
    const reps = this.topology.replicas(shard);
    if (anyReplica) return this.topology.member(reps[this.rr++ % reps.length])!.addr;
    return this.leaders.get(shard) ?? this.topology.member(reps[attempt % reps.length])!.addr;
  }

  private forget(shard: number, addr: string): void {
    if (this.leaders.get(shard) === addr) this.leaders.delete(shard);
  }

  private async do(method: string, key: string, query: Record<string, string>, body: Uint8Array | undefined, anyReplica = false): Promise<Response> {
    const shard = this.topology.shardForKey(key);
    let path = "/v1/kv/" + key.split("/").map(encodeURIComponent).join("/");
    const qs = new URLSearchParams(query).toString();
    if (qs) path += "?" + qs;
    const seq = method !== "GET" && this.sessions ? this.sessions.begin() : 0;
    const deadline = Date.now() + this.timeoutMs;
    let last = "no attempt";
    let redirects = 0;
    try {
      for (let attempt = 0; Date.now() < deadline; ) {
        const base = this.target(shard, attempt, anyReplica);
        const headers: Record<string, string> = {};
        if (this.smart) headers["X-Distkv-No-Forward"] = "1";
        if (seq && this.sessions) {
          headers["X-Distkv-Client-Id"] = this.sessions.clientId.toString();
          headers["X-Distkv-Seq"] = String(seq);
          headers["X-Distkv-Ack-Below"] = String(this.sessions.ackBelow());
        }
        let r: Response;
        try {
          r = await this.fetchImpl(base + path, {
            method,
            headers,
            body: body as BodyInit | undefined,
            signal: AbortSignal.timeout(Math.max(1, deadline - Date.now())),
          });
        } catch (e) {
          last = String(e);
          this.forget(shard, base);
          await sleep(backoff(++attempt), deadline);
          continue;
        }
        if (r.status === 421) {
          const err = (await r.json().catch(() => ({}))) as ApiError;
          if (err.leader_addr && err.leader_addr !== base && redirects++ < 3) {
            this.leaders.set(shard, err.leader_addr);
            continue;
          }
          this.forget(shard, base);
          last = err.error ?? "misdirected";
        } else if (r.status === 502 || r.status === 503 || r.status === 504) {
          const err = (await r.json().catch(() => ({}))) as ApiError;
          last = `${err.error} (${err.code})`;
          if (err.code === "shutting_down" || err.code === "unavailable" || err.code === "no_leader") this.forget(shard, base);
        } else {
          if (!anyReplica && r.status < 500) this.leaders.set(shard, base);
          return r;
        }
        await sleep(backoff(++attempt), deadline);
      }
      throw new UnavailableError(`${method} ${key}: ${last}`);
    } finally {
      if (seq && this.sessions) this.sessions.end(seq);
    }
  }
}

function backoff(attempt: number): number {
  const d = 5 * 2 ** Math.min(attempt, 6);
  return d / 2 + Math.random() * d;
}

function sleep(ms: number, deadline: number): Promise<void> {
  return new Promise((res) => setTimeout(res, Math.max(0, Math.min(ms, deadline - Date.now()))));
}

async function toError(key: string, r: Response): Promise<DistKVError> {
  const e = (await r.json().catch(() => ({}))) as ApiError;
  return new DistKVError(`${key}: HTTP ${r.status}: ${e.error ?? ""}`, r.status, e.code);
}
