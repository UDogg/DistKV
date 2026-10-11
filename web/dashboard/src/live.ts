import type {
  Backend,
  ClusterStatus,
  ConnState,
  DemoInfo,
  Faults,
  HistorianEvent,
  HistorianStats,
  HistoryPoint,
  KVResult,
} from "./types";

/**
 * LiveBackend talks to a real cluster: snapshots arrive over Server-Sent
 * Events from /v1/events, chaos controls go to the demo supervisor
 * (/demo/*), and history comes from the Python historian (/historian/*).
 */
export class LiveBackend implements Backend {
  readonly mode = "live" as const;
  private demo: DemoInfo | null = null;

  /**
   * @param base API origin ("" = same origin, e.g. the demo supervisor)
   * @param historianUrl direct historian URL (?historian=...), used when the
   *        dashboard is served by a node rather than the demo supervisor
   */
  constructor(private readonly base: string = "", private readonly historianUrl: string | null = null) {}

  subscribe(onSnapshot: (s: ClusterStatus) => void, onConn: (c: ConnState) => void): void {
    const es = new EventSource(`${this.base}/v1/events?interval=1s`);
    es.addEventListener("cluster", (ev) => {
      onConn("live");
      onSnapshot(JSON.parse((ev as MessageEvent).data));
    });
    es.onerror = () => onConn("reconnecting"); // EventSource reconnects by itself
  }

  async demoInfo(): Promise<DemoInfo | null> {
    try {
      const r = await fetch(`${this.base}/demo/info`, { cache: "no-store" });
      this.demo = r.ok ? await r.json() : null;
    } catch {
      this.demo = null;
    }
    return this.demo;
  }

  async nodeAction(id: string, action: "kill" | "stop" | "start"): Promise<void> {
    await this.post(`/demo/nodes/${id}/${action}`);
  }

  async setFaults(id: string, f: Faults): Promise<void> {
    await this.post(`/demo/nodes/${id}/faults`, f);
  }

  async setLoad(rate: number, readRatio: number): Promise<void> {
    await this.post(`/demo/load`, { rate, read_ratio: readRatio });
  }

  async kv(op: "put" | "get" | "delete", key: string, value?: string, ttlSec?: number): Promise<KVResult> {
    const path = "/v1/kv/" + key.split("/").map(encodeURIComponent).join("/") + (op === "put" && ttlSec ? `?ttl=${ttlSec}s` : "");
    const t0 = performance.now();
    const r = await fetch(this.base + path, {
      method: op === "put" ? "PUT" : op === "get" ? "GET" : "DELETE",
      body: op === "put" ? value ?? "" : undefined,
    });
    const body = await r.text();
    const h = r.headers;
    return {
      ok: r.ok,
      status: r.status,
      ms: performance.now() - t0,
      shard: h.get("x-distkv-shard") ? Number(h.get("x-distkv-shard")) : undefined,
      servedBy: h.get("x-distkv-served-by") ?? undefined,
      leader: h.get("x-distkv-leader") ?? undefined,
      version: h.get("x-distkv-version") ? Number(h.get("x-distkv-version")) : undefined,
      body,
    };
  }

  async history(minutes: number): Promise<HistoryPoint[] | null> {
    return this.historian(`/history?minutes=${minutes}&points=240`);
  }

  async historianEvents(): Promise<HistorianEvent[] | null> {
    return this.historian(`/events?limit=100`);
  }

  async historianStats(): Promise<HistorianStats | null> {
    return this.historian(`/stats`);
  }

  private async historian<T>(path: string): Promise<T | null> {
    const root = this.historianUrl ?? (this.demo?.historian ? `${this.base}/historian` : null);
    if (!root) return null;
    try {
      const r = await fetch(`${root}${path}`, { cache: "no-store" });
      return r.ok ? ((await r.json()) as T) : null;
    } catch {
      return null;
    }
  }

  private async post(path: string, body?: unknown): Promise<void> {
    const r = await fetch(this.base + path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
    if (!r.ok) throw new Error(`${path}: HTTP ${r.status} ${await r.text()}`);
  }
}
