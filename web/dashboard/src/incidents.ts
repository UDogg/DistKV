import type { ClusterStatus, DemoInfo } from "./types";

/**
 * Incident tracking. An incident opens when someone injects a fault (a chaos
 * action) or when the dashboard sees the cluster become unhealthy on its own
 * (a node down, a partition, packet loss, a shard without a leader). It stays
 * open while anything is wrong and resolves once the cluster has been
 * completely healthy for two consecutive snapshots. Everything observed in
 * between is recorded for the incident report.
 */

export type Severity = "SEV1" | "SEV2" | "SEV3";

export interface Sample {
  ts: number;
  ops: number;
  errors: number;
  p99: number;
  healthy: number;
  total: number;
  up: number;
  nodes: number;
}

export interface TimelineItem {
  ts: number;
  kind: string;
  detail: string;
}

export interface Failover {
  shard: number;
  from: string;
  lostAt: number;
  electedAt?: number;
  to?: string;
  /**
   * The election finished between two snapshots (no leaderless snapshot was
   * seen), so electedAt - lostAt is an upper bound.
   */
  bounded?: boolean;
}

export interface ClientCount {
  ok: number;
  failed: number;
}

/** Consistency-probe counters at the start and end of an incident. */
export interface ConsistencyWindow {
  opsStart: number;
  opsEnd: number;
  violStart: number;
  violEnd: number;
  /** Epochs close a little after the fault heals; counting stops here. */
  finalUntil?: number;
}

/** After resolution, keep counting verified operations for this long. */
export const CONSISTENCY_GRACE_MS = 6000;

export interface Incident {
  id: number;
  title: string;
  trigger: string;
  triggerKind: "action" | "detected";
  status: "open" | "resolved";
  severity: Severity;
  openedAt: number;
  detectedAt?: number;
  healRequestedAt?: number;
  resolvedAt?: number;
  nodes: string[];
  shards: number[];
  failovers: Failover[];
  leaderChanges: number;
  /** Leadership hand-offs from gracefully stopping nodes seen mid-way. */
  handoffs?: number;
  quorumLost: number[];
  timeline: TimelineItem[];
  samples: Sample[];
  baseline: Sample[];
  clientStart?: ClientCount;
  clientEnd?: ClientCount;
  consistency?: ConsistencyWindow;
  mode: "live" | "sim";
}

export type Transition =
  | { type: "opened"; incident: Incident; detail: string }
  | { type: "escalated"; incident: Incident; detail: string }
  | { type: "resolved"; incident: Incident; detail: string };

interface Issue {
  key: string;
  text: string;
}

/** Leaderless for this long means the shard has lost quorum, not just electing. */
const QUORUM_LOST_MS = 5000;
/** How long a chaos action may take to show up before we call it harmless. */
const NO_IMPACT_MS = 5000;
const MAX_SAMPLES = 900;
const MAX_INCIDENTS = 25;

/** The things currently wrong with the cluster, from one snapshot. */
export function issuesOf(s: ClusterStatus): Issue[] {
  const out: Issue[] = [];
  for (const n of s.nodes) {
    if (!n.reachable) out.push({ key: `down:${n.id}`, text: `${n.id} down` });
    else if (n.faults?.isolated) out.push({ key: `iso:${n.id}`, text: `${n.id} partitioned` });
    else if ((n.faults?.drop_percent ?? 0) > 0 || (n.faults?.delay_ms ?? 0) > 0)
      out.push({ key: `loss:${n.id}`, text: `${n.id} ${n.faults!.drop_percent}% packet loss` });
  }
  const leaderless = s.shards.filter((sh) => !sh.leader).map((sh) => sh.shard);
  for (const sh of leaderless) out.push({ key: `noleader:${sh}`, text: `shard ${sh} has no leader` });
  return out;
}

/** A one-line description of a set of issues ("n3 down · 4 shards electing"). */
export function describe(issues: Issue[]): string {
  const nodes = issues.filter((i) => !i.key.startsWith("noleader:")).map((i) => i.text);
  const shards = issues.length - nodes.length;
  if (shards) nodes.push(`${shards} shard${shards === 1 ? "" : "s"} without a leader`);
  return nodes.join(" · ") || "healthy";
}

export class IncidentTracker {
  readonly incidents: Incident[] = [];
  active: Incident | null = null;
  private recent: Sample[] = [];
  private prev: ClusterStatus | null = null;
  private cleanSince = 0;
  private cleanStreak = 0;
  private lastActionAt = 0;
  private holds = 0;
  /** Nodes recently stopped gracefully: their leadership moves are hand-offs. */
  private readonly graceful = new Map<string, number>();
  private seen = new Set<string>();
  private leaderlessSince = new Map<number, number>();
  private nextId = 1;
  private finalizing: Incident | null = null;
  current: Issue[] = [];

  constructor(
    private readonly mode: "live" | "sim",
    private readonly emit: (t: Transition) => void,
    saved: Incident[] = [],
  ) {
    // Restored incidents are history only; an incident left open by a closed
    // tab can't be finished, so it is marked as such.
    for (const inc of saved) {
      if (inc.status === "open") {
        inc.status = "resolved";
        inc.resolvedAt = inc.samples.at(-1)?.ts ?? inc.openedAt;
        inc.timeline.push({ ts: inc.resolvedAt, kind: "note", detail: "dashboard closed before the incident resolved" });
      }
      this.incidents.push(inc);
    }
    this.nextId = Math.max(0, ...this.incidents.map((i) => i.id)) + 1;
  }

  /** Someone injected a fault or asked for a heal from the dashboard. */
  action(kind: "chaos" | "heal", detail: string, demo: DemoInfo | null): void {
    const now = Date.now();
    this.lastActionAt = now;
    if (kind === "heal") {
      if (this.active) {
        this.active.healRequestedAt ??= now;
        this.active.timeline.push({ ts: now, kind: "heal", detail });
      }
      return;
    }
    if (!this.active) {
      this.open(detail, "action", demo, now);
      return;
    }
    this.active.timeline.push({ ts: now, kind: "action", detail });
    this.active.healRequestedAt = undefined;
    const more = this.active.timeline.filter((e) => e.kind === "action").length - 1;
    this.active.title = `${this.active.trigger} (+${more} more fault${more === 1 ? "" : "s"})`;
    this.emit({ type: "escalated", incident: this.active, detail: `additional fault: ${detail}` });
  }

  /**
   * Records a graceful stop: the node hands its leaderships off before it
   * exits, so those leader moves are planned transfers, not failovers.
   */
  noteGracefulStop(node: string): void {
    this.graceful.set(node, Date.now());
  }

  /** Keeps the active incident open across a multi-step scenario. */
  hold(): void {
    this.holds++;
  }

  release(): void {
    this.holds = Math.max(0, this.holds - 1);
  }

  /** Feed every snapshot, with the events derived from it. */
  observe(s: ClusterStatus, demo: DemoInfo | null, events: { ts: number; kind: string; detail: string }[]): void {
    const sample: Sample = {
      ts: s.ts,
      ops: s.load.reads_per_sec + s.load.writes_per_sec,
      errors: s.load.errors_per_sec,
      p99: s.load.p99_ms,
      healthy: s.healthy_shards,
      total: s.shards.length,
      up: s.nodes.filter((n) => n.reachable).length,
      nodes: s.nodes.length,
    };
    this.recent.push(sample);
    if (this.recent.length > 30) this.recent.shift();
    const cons = demo?.consistency;
    const fin = this.finalizing;
    if (fin?.consistency && cons) {
      fin.consistency.opsEnd = cons.ops_checked;
      fin.consistency.violEnd = cons.violations_total;
      fin.severity = severityOf(fin);
      if (s.ts >= (fin.consistency.finalUntil ?? 0)) this.finalizing = null;
    }
    const issues = issuesOf(s);
    this.current = issues;
    for (const sh of s.shards) {
      if (sh.leader) this.leaderlessSince.delete(sh.shard);
      else if (!this.leaderlessSince.has(sh.shard)) this.leaderlessSince.set(sh.shard, s.ts);
    }

    if (!this.active && issues.length) this.open(describe(issues), "detected", demo, s.ts);
    const inc = this.active;
    if (inc) {
      inc.samples.push(sample);
      if (inc.samples.length > MAX_SAMPLES) inc.samples.shift();
      for (const e of events) inc.timeline.push(e);
      if (issues.length && inc.detectedAt === undefined) inc.detectedAt = s.ts;
      const detectionTick = inc.detectedAt === s.ts;
      this.trackShards(inc, s);
      for (const i of issues) {
        const [kind, id] = i.key.split(":");
        if (kind !== "noleader" && !inc.nodes.includes(id)) inc.nodes.push(id);
        if (kind === "noleader" && !inc.shards.includes(Number(id))) inc.shards.push(Number(id));
        // A new node-level fault that nobody just injected from here (those
        // were announced by action()) escalates the incident.
        if (kind !== "noleader" && !this.seen.has(i.key)) {
          this.seen.add(i.key);
          if (!detectionTick && s.ts - this.lastActionAt > 3000) this.emit({ type: "escalated", incident: inc, detail: i.text });
        }
      }
      // Leaderless past the election window: quorum is gone, writes fail.
      // One escalation per snapshot, however many shards it covers.
      const lost: number[] = [];
      for (const [sh, since] of this.leaderlessSince) {
        if (s.ts - since >= QUORUM_LOST_MS && !inc.quorumLost.includes(sh)) {
          inc.quorumLost.push(sh);
          lost.push(sh);
          inc.timeline.push({ ts: s.ts, kind: "quorum_lost", detail: `shard ${sh} has no quorum: unavailable for writes` });
        }
      }
      if (lost.length) {
        const which = lost.length === 1 ? `shard ${lost[0]}` : `${lost.length} shards (${lost.sort((x, y) => x - y).join(", ")})`;
        this.emit({ type: "escalated", incident: inc, detail: `${which} lost quorum: leaderless for ${QUORUM_LOST_MS / 1000}s, writes there fail` });
      }
      if (cons) {
        inc.consistency ??= { opsStart: cons.ops_checked, opsEnd: cons.ops_checked, violStart: cons.violations_total, violEnd: cons.violations_total };
        inc.consistency.opsEnd = cons.ops_checked;
        inc.consistency.violEnd = cons.violations_total;
      }
      inc.severity = severityOf(inc);
      inc.clientEnd = clientOf(demo) ?? inc.clientEnd;
      // A fault injected from here may take a moment to show; until it does
      // (or NO_IMPACT_MS passes), clean snapshots don't count.
      const waiting = inc.detectedAt === undefined && s.ts - inc.openedAt < NO_IMPACT_MS;
      if (issues.length || waiting) this.cleanStreak = 0;
      else if (++this.cleanStreak === 1) this.cleanSince = s.ts;
      // Two clean snapshots in a row, and no multi-step scenario still running.
      if (this.cleanStreak >= 2 && this.holds === 0) {
        if (inc.detectedAt === undefined) inc.timeline.push({ ts: s.ts, kind: "note", detail: "no impact was observed" });
        this.resolve(inc);
      }
    }
    this.prev = s;
  }

  private open(trigger: string, kind: "action" | "detected", demo: DemoInfo | null, ts: number): void {
    const inc: Incident = {
      id: this.nextId++,
      title: trigger,
      trigger,
      triggerKind: kind,
      status: "open",
      severity: "SEV3",
      openedAt: ts,
      nodes: [],
      shards: [],
      failovers: [],
      leaderChanges: 0,
      quorumLost: [],
      timeline: [{ ts, kind: kind === "action" ? "action" : "detected", detail: kind === "action" ? trigger : `detected: ${trigger}` }],
      samples: [],
      baseline: this.recent.slice(-15),
      clientStart: clientOf(demo),
      consistency: demo?.consistency
        ? { opsStart: demo.consistency.ops_checked, opsEnd: demo.consistency.ops_checked, violStart: demo.consistency.violations_total, violEnd: demo.consistency.violations_total }
        : undefined,
      mode: this.mode,
    };
    this.active = inc;
    this.seen = new Set();
    this.cleanSince = 0;
    this.cleanStreak = 0;
    this.incidents.unshift(inc);
    if (this.incidents.length > MAX_INCIDENTS) this.incidents.length = MAX_INCIDENTS;
    this.emit({ type: "opened", incident: inc, detail: trigger });
  }

  private resolve(inc: Incident): void {
    inc.status = "resolved";
    inc.resolvedAt = this.cleanSince || Date.now();
    for (const f of inc.failovers) if (!f.electedAt) f.electedAt = inc.resolvedAt;
    inc.severity = severityOf(inc);
    inc.timeline.push({ ts: inc.resolvedAt, kind: "resolved", detail: "all nodes healthy and every shard has a leader" });
    if (inc.consistency) {
      inc.consistency.finalUntil = inc.resolvedAt + CONSISTENCY_GRACE_MS;
      this.finalizing = inc;
    }
    this.active = null;
    this.emit({ type: "resolved", incident: inc, detail: "" });
  }

  private trackShards(inc: Incident, s: ClusterStatus): void {
    const a = this.prev;
    if (!a) return;
    const handedOff = (id: string) => s.ts - (this.graceful.get(id) ?? 0) < 15_000;
    const gone = new Set(s.nodes.filter((n) => (!n.reachable || n.faults?.isolated) && !handedOff(n.id)).map((n) => n.id));
    for (const sh of s.shards) {
      const o = a.shards[sh.shard];
      if (!o) continue;
      const was = o.leader ?? "";
      const now = sh.leader ?? "";
      // A graceful stop hands leadership off (TimeoutNow): the old leader
      // steps down and the target wins milliseconds later, and a snapshot
      // can land in between. That is a planned hand-off, not a failover.
      if (was && !now) {
        if (handedOff(was)) inc.handoffs = (inc.handoffs ?? 0) + 1;
        else inc.failovers.push({ shard: sh.shard, from: was, lostAt: s.ts });
      }
      // A fast election can complete between snapshots: the leader moved
      // straight off a dead or partitioned node. That is a failover too
      // (unlike a graceful transfer or rebalancing, where the old leader is fine).
      if (was && now && was !== now && gone.has(was))
        inc.failovers.push({ shard: sh.shard, from: was, lostAt: a.ts, electedAt: s.ts, to: now, bounded: true });
      if (!was && now) {
        const f = [...inc.failovers].reverse().find((x) => x.shard === sh.shard && !x.electedAt);
        if (f) Object.assign(f, { electedAt: s.ts, to: now });
      }
      if (was !== now) {
        inc.leaderChanges++;
        if (!inc.shards.includes(sh.shard)) inc.shards.push(sh.shard);
      }
    }
  }
}

function clientOf(demo: DemoInfo | null): ClientCount | undefined {
  const c = demo?.client;
  return c ? { ok: c.ok_total, failed: c.failed_total } : undefined;
}

/**
 * SEV1: clients saw failures or a shard lost quorum. SEV2: shards failed over
 * (leaders lost and re-elected). SEV3: degraded only (no leader was lost).
 */
export function severityOf(inc: Incident): Severity {
  const failed = inc.clientStart && inc.clientEnd ? inc.clientEnd.failed - inc.clientStart.failed : 0;
  const violations = inc.consistency ? inc.consistency.violEnd - inc.consistency.violStart : 0;
  if (failed > 0 || inc.quorumLost.length || violations > 0) return "SEV1";
  if (inc.failovers.length) return "SEV2";
  return "SEV3";
}

// ---------------------------------------------------------------------------
// Metrics for the report

export interface Summary {
  durationMs: number;
  detectMs?: number;
  recoverAfterHealMs?: number;
  failovers: number;
  /** Every failover finished between two snapshots: times are upper bounds. */
  failoversBounded: boolean;
  maxFailoverMs: number;
  medianFailoverMs: number;
  peakP99: number;
  baselineP99: number;
  sloBreachSeconds: number;
  minHealthy: number;
  total: number;
  minUp: number;
  nodes: number;
  serverErrors: number;
  clientOk?: number;
  clientFailed?: number;
  availability?: number;
  dipPercent: number;
  /** Operations the consistency probe verified across the incident. */
  verifiedOps?: number;
  violations?: number;
  /** Still counting epochs that span the incident. */
  verifying?: boolean;
}

const median = (xs: number[]) => {
  if (!xs.length) return 0;
  const s = [...xs].sort((a, b) => a - b);
  return s[Math.floor(s.length / 2)];
};

export function summarize(inc: Incident): Summary {
  const end = inc.resolvedAt ?? Date.now();
  const dur = inc.failovers.map((f) => (f.electedAt ?? end) - f.lostAt);
  const smp = inc.samples.length ? inc.samples : inc.baseline.slice(-1);
  const baseOps = median(inc.baseline.map((b) => b.ops));
  const minOps = Math.min(...smp.map((x) => x.ops));
  const clientOk = inc.clientStart && inc.clientEnd ? inc.clientEnd.ok - inc.clientStart.ok : undefined;
  const clientFailed = inc.clientStart && inc.clientEnd ? inc.clientEnd.failed - inc.clientStart.failed : undefined;
  return {
    durationMs: end - inc.openedAt,
    detectMs: inc.detectedAt !== undefined ? Math.max(0, inc.detectedAt - inc.openedAt) : undefined,
    recoverAfterHealMs: inc.healRequestedAt && inc.resolvedAt ? Math.max(0, inc.resolvedAt - inc.healRequestedAt) : undefined,
    failovers: inc.failovers.length,
    failoversBounded: inc.failovers.length > 0 && inc.failovers.every((f) => f.bounded),
    maxFailoverMs: Math.max(0, ...dur),
    medianFailoverMs: median(dur),
    peakP99: Math.max(0, ...smp.map((x) => x.p99)),
    baselineP99: median(inc.baseline.map((b) => b.p99)),
    sloBreachSeconds: smp.filter((x) => x.p99 >= 10).length,
    minHealthy: Math.min(...smp.map((x) => x.healthy)),
    total: smp[0]?.total ?? 0,
    minUp: Math.min(...smp.map((x) => x.up)),
    nodes: smp[0]?.nodes ?? 0,
    serverErrors: Math.round(smp.reduce((a, x) => a + x.errors, 0)),
    clientOk,
    clientFailed,
    availability: clientOk !== undefined && clientFailed !== undefined && clientOk + clientFailed > 0 ? clientOk / (clientOk + clientFailed) : undefined,
    dipPercent: baseOps > 0 ? Math.max(0, (1 - minOps / baseOps) * 100) : 0,
    verifiedOps: inc.consistency ? inc.consistency.opsEnd - inc.consistency.opsStart : undefined,
    violations: inc.consistency ? inc.consistency.violEnd - inc.consistency.violStart : undefined,
    verifying: !!inc.consistency && (inc.status === "open" || Date.now() < (inc.consistency.finalUntil ?? 0)),
  };
}
