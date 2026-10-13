import { Topology } from "../../../clients/typescript/src/routing";
import type {
  Backend,
  ClusterStatus,
  ConnState,
  DemoInfo,
  DemoNode,
  Faults,
  HistorianEvent,
  HistorianStats,
  HistoryPoint,
  KVResult,
  ProcState,
} from "./types";

/**
 * SimBackend is an in-browser model of a DistKV cluster, used for the
 * GitHub Pages demo (no servers required). It uses the real routing code,
 * so shard placement, preferred leaders and key lookups are exactly what a
 * real cluster with the same spec would do; elections, failover timing,
 * leader balancing and load are modelled after measured behaviour.
 */

const ELECTION_MIN_MS = 300;
const ELECTION_JITTER_MS = 400;

interface SimNode {
  id: string;
  addr: string;
  state: ProcState;
  since: number;
  starts: number;
  pid: number;
  faults: Faults;
  startedAt: number;
}

interface SimShard {
  leader: string;
  term: number;
  commit: number;
  keys: number;
  electionAt: number; // when an in-progress election completes (0 = none)
  lag: Record<string, number>;
}

export class SimBackend implements Backend {
  readonly mode = "sim" as const;
  private readonly topo: Topology;
  private readonly nodes: SimNode[];
  private readonly shards: SimShard[];
  private rate = 1500;
  private readRatio = 0.8;
  private disturbance = 0; // decays; inflates latency after failures
  private readonly kvStore = new Map<string, { v: string; ver: number; exp?: number }>();
  private nextPid = 41000;
  private readonly hist: HistoryPoint[] = [];
  private okTotal = 0;
  private failedTotal = 0;

  constructor(nNodes = 5, nShards = 24) {
    const members = Array.from({ length: nNodes }, (_, i) => ({ id: `n${i + 1}`, addr: `http://127.0.0.1:${7101 + i}` }));
    this.topo = new Topology({ members, shards: nShards, replication_factor: 3, virtual_nodes: 128 });
    const now = Date.now();
    this.nodes = members.map((m) => ({
      ...m,
      state: "running",
      since: now,
      starts: 1,
      pid: this.nextPid++,
      faults: { isolated: false, drop_percent: 0, delay_ms: 0 },
      startedAt: now - 3_600_000 * Math.random(),
    }));
    this.shards = Array.from({ length: nShards }, (_, s) => ({
      leader: this.topo.replicas(s)[0],
      term: 1 + Math.floor(Math.random() * 3),
      commit: 1000 + Math.floor(Math.random() * 5000),
      keys: 600 + Math.floor(Math.random() * 400),
      electionAt: 0,
      lag: {},
    }));
    // Seed an hour of plausible history for the "SQLite" panel.
    for (let t = now - 15 * 60_000; t < now; t += 5000) {
      const wave = 1 + 0.15 * Math.sin(t / 60_000);
      this.hist.push({ t, reads: this.rate * this.readRatio * wave, writes: this.rate * (1 - this.readRatio) * wave, errors: 0,
        p50_ms: 0.55 + Math.random() * 0.1, p99_ms: 4 + Math.random() * 3, nodes_up: nNodes, shards_healthy: nShards, shards_total: nShards });
    }
  }

  subscribe(onSnapshot: (s: ClusterStatus) => void, onConn: (c: ConnState) => void): void {
    onConn("sim");
    const tick = () => {
      this.step(Date.now());
      onSnapshot(this.snapshot());
    };
    tick();
    setInterval(tick, 1000);
    // Elections resolve on their own schedule, faster than the snapshot rate.
    setInterval(() => this.step(Date.now()), 100);
  }

  private up(n: SimNode): boolean {
    return n.state === "running";
  }

  private connected(n: SimNode): boolean {
    return this.up(n) && !n.faults.isolated;
  }

  private node(id: string): SimNode {
    return this.nodes.find((n) => n.id === id)!;
  }

  private lastBalance = Date.now();

  private step(now: number): void {
    this.disturbance *= 0.93;
    for (let s = 0; s < this.shards.length; s++) {
      const sh = this.shards[s];
      const reps = this.topo.replicas(s).map((id) => this.node(id));
      const live = reps.filter((n) => this.connected(n));
      const leaderOk = sh.leader !== "" && this.connected(this.node(sh.leader));
      if (!leaderOk && sh.leader !== "") {
        sh.leader = "";
        sh.electionAt = now + ELECTION_MIN_MS + Math.random() * ELECTION_JITTER_MS;
        this.disturbance = Math.min(1, this.disturbance + 0.25);
      }
      if (sh.leader === "" && sh.electionAt === 0) sh.electionAt = now + ELECTION_MIN_MS + Math.random() * ELECTION_JITTER_MS;
      if (sh.leader === "" && now >= sh.electionAt && live.length >= 2) {
        // Pre-vote + vote: any connected replica with a quorum can win.
        sh.leader = live[Math.floor(Math.random() * live.length)].id;
        sh.term++;
        sh.electionAt = 0;
      }
      const drop = Math.max(0, ...reps.map((n) => n.faults.drop_percent));
      const prevLag = sh.lag;
      sh.lag = {};
      for (const n of reps) {
        if (n.id === sh.leader) continue;
        // A disconnected follower falls further behind every tick.
        sh.lag[n.id] = this.connected(n)
          ? Math.floor(Math.random() * (3 + drop * 4))
          : (prevLag[n.id] ?? 0) + Math.floor(this.rate / 400);
      }
    }
    // Leader balancing: move one shard per 3 s back to its preferred replica.
    if (now - this.lastBalance > 3000) {
      this.lastBalance = now;
      for (let s = 0; s < this.shards.length; s++) {
        const pref = this.topo.replicas(s)[0];
        const sh = this.shards[s];
        if (sh.leader && sh.leader !== pref && this.connected(this.node(pref))) {
          sh.leader = pref;
          sh.term++;
          break;
        }
      }
    }
  }

  private snapshot(): ClusterStatus {
    const now = Date.now();
    const healthy = this.shards.filter((s) => s.leader !== "").length;
    const availability = healthy / this.shards.length;
    const drop = Math.max(...this.nodes.map((n) => n.faults.drop_percent));
    const jitter = () => 0.9 + Math.random() * 0.2;
    const reads = this.rate * this.readRatio * jitter();
    const writes = this.rate * (1 - this.readRatio) * jitter();
    const p99 = (3 + this.rate / 900 + drop / 4) * jitter() + this.disturbance * 450;
    const p50 = 0.45 + this.rate / 12000 + this.disturbance * 3;
    // Clients retry through elections, so only requests to leaderless
    // shards that outlast the retry budget fail (rare, as measured).
    this.okTotal += Math.round(this.rate * availability);
    if (availability < 1 && Math.random() < 0.05) this.failedTotal += 1;
    // Every write commits on its shard.
    for (const sh of this.shards) if (sh.leader) sh.commit += Math.round((writes / this.shards.length) * (0.7 + Math.random() * 0.6));

    const liveNodes = this.nodes.filter((n) => this.up(n));
    const share = (n: SimNode) => this.shards.filter((s) => s.leader === n.id).length / Math.max(1, healthy);
    const status: ClusterStatus = {
      spec: { members: this.topo.members, shards: this.topo.shards, replication_factor: 3, virtual_nodes: 128 },
      fingerprint: "sim0c1a55e7",
      ts: now,
      observed_by: liveNodes[0]?.id ?? "n1",
      healthy_shards: healthy,
      load: { reads_per_sec: reads * availability, writes_per_sec: writes * availability, errors_per_sec: 0,
        forwarded_per_sec: 0, p50_ms: p50, p99_ms: p99, p999_ms: p99 * 1.8 },
      nodes: this.nodes.map((n) => ({
        id: n.id,
        addr: n.addr,
        reachable: this.up(n),
        ready: this.connected(n),
        shards_led: this.shards.filter((s) => s.leader === n.id).length,
        shards_hosted: this.shards.filter((_, i) => this.topo.replicas(i).includes(n.id)).length,
        version: "v0.2.0-sim",
        uptime_seconds: this.up(n) ? (now - n.startedAt) / 1000 : 0,
        faults: { ...n.faults },
        load: this.up(n)
          ? { reads_per_sec: reads * share(n), writes_per_sec: writes * share(n), errors_per_sec: 0, forwarded_per_sec: 0,
              p50_ms: p50 * jitter(), p99_ms: p99 * jitter(), p999_ms: p99 * 1.8 }
          : undefined,
      })),
      shards: this.shards.map((sh, i) => ({
        shard: i,
        replicas: this.topo.replicas(i),
        leader: sh.leader || undefined,
        leader_addr: sh.leader ? this.node(sh.leader).addr : undefined,
        term: sh.term,
        commit_index: sh.commit,
        keys: sh.keys,
        healthy: sh.leader !== "",
        lag: sh.leader ? sh.lag : undefined,
      })),
    };
    if (now - (this.hist.at(-1)?.t ?? 0) >= 5000) {
      this.hist.push({ t: now, reads: status.load.reads_per_sec, writes: status.load.writes_per_sec, errors: 0, p50_ms: p50,
        p99_ms: p99, nodes_up: liveNodes.length, shards_healthy: healthy, shards_total: this.shards.length });
      if (this.hist.length > 720) this.hist.shift();
    }
    return status;
  }

  async demoInfo(): Promise<DemoInfo> {
    return {
      demo: true,
      historian: true,
      shards: this.shards.length,
      client: {
        ok_total: this.okTotal,
        failed_total: this.failedTotal,
        availability: this.okTotal / Math.max(1, this.okTotal + this.failedTotal),
        availability_60s: 1,
        last_second: { ok: this.rate, failed: 0, p99_ms: 0 },
      },
      rate: this.rate,
      read_ratio: this.readRatio,
      nodes: this.nodes.map(
        (n): DemoNode => ({ id: n.id, addr: n.addr, pid: n.pid, state: n.state, since: new Date(n.since).toISOString(), starts: n.starts }),
      ),
    };
  }

  async nodeAction(id: string, action: "kill" | "stop" | "start"): Promise<void> {
    const n = this.node(id);
    const now = Date.now();
    if (action === "start") {
      if (n.state !== "running") Object.assign(n, { state: "running", since: now, starts: n.starts + 1, pid: this.nextPid++, startedAt: now });
      return;
    }
    if (n.state !== "running") return;
    if (action === "stop") {
      // Graceful: leadership is handed off first, so no election gap.
      for (let s = 0; s < this.shards.length; s++) {
        const sh = this.shards[s];
        if (sh.leader !== id) continue;
        const target = this.topo.replicas(s).find((r) => r !== id && this.connected(this.node(r)));
        if (target) {
          sh.leader = target;
          sh.term++;
        }
      }
    }
    Object.assign(n, { state: action === "kill" ? "killed" : "stopped", since: now });
  }

  async setFaults(id: string, f: Faults): Promise<void> {
    this.node(id).faults = { ...f };
  }

  async setLoad(rate: number, readRatio: number): Promise<void> {
    this.rate = rate;
    this.readRatio = readRatio;
  }

  async kv(op: "put" | "get" | "delete", key: string, value?: string, ttlSec?: number): Promise<KVResult> {
    const shard = this.topo.shardForKey(key);
    const sh = this.shards[shard];
    const ms = 0.4 + Math.random() * (op === "get" ? 0.6 : 1.6) + this.disturbance * 300;
    await new Promise((r) => setTimeout(r, ms));
    if (!sh.leader) {
      return { ok: false, status: 503, ms, shard, body: JSON.stringify({ error: "shard has no leader (election in progress)", code: "no_leader" }, null, 2) };
    }
    const base = { shard, servedBy: sh.leader, leader: sh.leader, ms };
    const now = Date.now();
    const cur = this.kvStore.get(key);
    const live = cur && (!cur.exp || cur.exp > now) ? cur : undefined;
    if (op === "get") {
      if (!live) return { ...base, ok: false, status: 404, body: JSON.stringify({ error: "key not found", code: "not_found" }, null, 2) };
      return { ...base, ok: true, status: 200, version: live.ver, body: live.v };
    }
    if (op === "delete") {
      if (!live) return { ...base, ok: false, status: 404, body: JSON.stringify({ error: "key not found", code: "not_found" }, null, 2) };
      this.kvStore.delete(key);
      sh.keys = Math.max(0, sh.keys - 1);
      return { ...base, ok: true, status: 200, body: JSON.stringify({ key, version: ++sh.commit, shard, deleted: true }, null, 2) };
    }
    const ver = ++sh.commit;
    if (!live) sh.keys++;
    this.kvStore.set(key, { v: value ?? "", ver, exp: ttlSec ? now + ttlSec * 1000 : undefined });
    return { ...base, ok: true, status: 200, version: ver, body: JSON.stringify({ key, version: ver, shard }, null, 2) };
  }

  async history(): Promise<HistoryPoint[]> {
    return this.hist;
  }

  async historianEvents(): Promise<HistorianEvent[]> {
    return [];
  }

  async historianStats(): Promise<HistorianStats> {
    return { db_bytes: 180_000 + this.hist.length * 96, samples: this.hist.length * 5, events: {} };
  }
}
