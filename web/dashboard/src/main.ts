import { Topology } from "../../../clients/typescript/src/routing";
import { AlertCenter } from "./alerts";
import { TimeSeriesChart } from "./charts";
import { EXPERIMENTS, GAMEDAY_CSS, gameDayDocument, gameDayHTML, gameDayMarkdown, score, type ExperimentCtx, type GameDayRun } from "./gameday";
import { CONSISTENCY_GRACE_MS, describe, IncidentTracker, summarize, type Incident, type Transition } from "./incidents";
import { LiveBackend } from "./live";
import { LogConsole, type Level } from "./logs";
import { fmtDuration, REPORT_CSS, reportDocument, reportHTML, reportMarkdown } from "./report";
import { RingView } from "./ring";
import { SimBackend } from "./sim";
import type { Backend, ClusterStatus, ConnState, ConsistencyReport, DemoInfo, HistorianEvent, NodeSummary } from "./types";

// ---------------------------------------------------------------------------
// Utilities

const $ = <T extends HTMLElement = HTMLElement>(id: string) => document.getElementById(id) as T;
const PALETTE = ["#7c5cff", "#00d4ff", "#ff5ca8", "#2ee59d", "#ffb547", "#5c8aff", "#c45cff", "#5cffd9", "#ff8a5c"];
const WINDOW = 180; // seconds of live chart history

function esc(s: string): string {
  return s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!);
}

/** Strips trailing slashes in linear time (a /\/+$/ regex can backtrack quadratically). */
function trimSlashes(s: string): string {
  let end = s.length;
  while (end > 0 && s.charCodeAt(end - 1) === 47) end--;
  return s.slice(0, end);
}

function fmtNum(v: number): string {
  if (v >= 10_000) return `${(v / 1000).toFixed(1)}k`;
  if (v >= 1000) return v.toLocaleString(undefined, { maximumFractionDigits: 0 });
  return v.toFixed(v >= 100 ? 0 : 1);
}

function fmtMs(v: number): string {
  if (!v) return "—";
  if (v >= 1000) return `${(v / 1000).toFixed(2)}s`;
  return v >= 10 ? `${v.toFixed(0)}ms` : `${v.toFixed(2)}ms`;
}

function fmtDur(sec: number): string {
  if (sec < 60) return `${Math.floor(sec)}s`;
  if (sec < 3600) return `${Math.floor(sec / 60)}m`;
  return `${Math.floor(sec / 3600)}h${Math.floor((sec % 3600) / 60)}m`;
}

function toast(msg: string, kind: "ok" | "bad" | "info" = "info"): void {
  const t = document.createElement("div");
  t.className = `toast ${kind}`;
  t.textContent = msg;
  $("toasts").appendChild(t);
  setTimeout(() => t.remove(), 4200);
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// Most panels re-render every second with mostly identical markup; writing
// innerHTML anyway costs a DOM rebuild, style recalculation and layout.
const lastHTML = new WeakMap<Element, string>();
function setHTML(el: Element, html: string): void {
  if (lastHTML.get(el) === html) return;
  lastHTML.set(el, html);
  el.innerHTML = html;
}

function download(name: string, text: string, type: string): void {
  const url = URL.createObjectURL(new Blob([text], { type }));
  const a = document.createElement("a");
  a.href = url;
  a.download = name;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

const stamp = () => new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");

// ---------------------------------------------------------------------------
// Backend selection: ?sim, <meta distkv-mode=sim>, ?api=URL, or same origin.

async function pickBackend(): Promise<Backend> {
  const params = new URLSearchParams(location.search);
  const meta = document.querySelector<HTMLMetaElement>('meta[name="distkv-mode"]')?.content;
  if (params.has("sim") || meta === "sim" || location.protocol === "file:") return new SimBackend();
  const api = trimSlashes(params.get("api") ?? "");
  try {
    const r = await fetch(`${api}/v1/cluster`, { signal: AbortSignal.timeout(2500) });
    if (r.ok) return new LiveBackend(api, params.has("historian") ? trimSlashes(params.get("historian")!) : null);
  } catch {
    /* fall through to simulation */
  }
  return new SimBackend();
}

// ---------------------------------------------------------------------------
// Application state

interface Ev {
  ts: number;
  kind: string;
  detail: string;
}

const EVENT_STYLE: Record<string, [string, string]> = {
  leader_change: ["🔀", ""],
  election: ["🗳️", "warn"],
  node_down: ["💀", "bad"],
  node_up: ["🟢", "ok"],
  partitioned: ["✂️", "warn"],
  healed: ["🩹", "ok"],
  shard_unavailable: ["⛔", "bad"],
  shard_recovered: ["✅", "ok"],
  quorum_lost: ["🛑", "bad"],
  violation: ["🧬", "bad"],
  action: ["🧪", ""],
  incident_open: ["🚨", "bad incident"],
  incident_escalated: ["⚠️", "warn incident"],
  incident_resolved: ["✅", "ok incident"],
};

const EVENT_FILTERS: Record<string, (kind: string) => boolean> = {
  all: () => true,
  faults: (k) => ["node_down", "node_up", "partitioned", "healed", "shard_unavailable", "shard_recovered", "quorum_lost", "violation"].includes(k),
  leaders: (k) => ["leader_change", "election", "shard_recovered", "shard_unavailable"].includes(k),
  you: (k) => k === "action",
  incidents: (k) => k.startsWith("incident_"),
};

/** Log level for each derived event kind. */
const EVENT_LEVEL: Record<string, Level> = {
  node_down: "error",
  shard_unavailable: "error",
  quorum_lost: "error",
  partitioned: "warn",
  election: "warn",
};

/** What a chaos action is called in alerts and reports. */
const ACT_TEXT: Record<string, (id: string) => string> = {
  kill: (id) => `SIGKILL to ${id}`,
  stop: (id) => `graceful stop of ${id}`,
  start: (id) => `start ${id}`,
  isolate: (id) => `network partition of ${id}`,
  heal: (id) => `heal partition of ${id}`,
};

class App {
  private snap: ClusterStatus | null = null;
  private prev: ClusterStatus | null = null;
  private demo: DemoInfo | null = null;
  private topo: Topology | null = null;
  private ring: RingView | null = null;
  private readonly t: number[] = [];
  private readonly s = { reads: [] as number[], writes: [] as number[], errors: [] as number[], p50: [] as number[], p99: [] as number[] };
  private readonly events: Ev[] = [];
  private readonly nodeCards = new Map<string, HTMLElement>();
  private leaderChanges: number[] = [];
  private readonly tp: TimeSeriesChart;
  private readonly lat: TimeSeriesChart;
  private readonly histChart: TimeSeriesChart;
  private hoverShard: number | null = null;
  private loadSynced = false;
  private readonly alerts: AlertCenter;
  private readonly tracker: IncidentTracker;
  private readonly logs: LogConsole;
  private readonly logOffsets = new Map<string, number>();
  private pollingLogs = false;
  private eventFilter = "all";
  private reportId: number | null = null;
  private lastClientLog = 0;
  private lastClientFailed: number | null = null;
  private lastSave = 0;
  private locateTimer: ReturnType<typeof setTimeout> | undefined;
  private gd: GameDayRun | null = null;
  private gdRunning = false;
  private gdCancel = false;
  private readonly gdSelected = new Set(EXPERIMENTS.map((e) => e.id));
  private monkey = false;
  private reportGameDay = false;

  constructor(private readonly be: Backend) {
    const style = document.createElement("style");
    style.textContent = REPORT_CSS + GAMEDAY_CSS;
    document.head.appendChild(style);
    this.alerts = new AlertCenter(
      $("alert-stack"),
      $("alert-bell") as HTMLButtonElement,
      $("alert-count"),
      $("alert-panel"),
      (id) => this.openReport(id),
      () => this.scenario("heal"),
    );
    this.tracker = new IncidentTracker(be.mode, (t) => this.onIncident(t), this.loadIncidents());
    this.logs = new LogConsole(
      {
        list: $("log-list"),
        search: $("log-search") as HTMLInputElement,
        source: $("log-source") as HTMLSelectElement,
        levels: $("log-levels"),
        follow: $("log-follow") as HTMLInputElement,
        count: $("logs-count"),
      },
      (src) => (this.snap?.spec.members.some((m) => m.id === src) ? this.color(src) : undefined),
    );
    this.logs.onSelect = (e) => {
      const sh = e.attrs.find(([k]) => k === "shard")?.[1];
      if (sh !== undefined) this.locateShard(Number(sh));
    };
    this.logs.add("dashboard", "info", `dashboard started in ${be.mode === "sim" ? "simulation" : "live"} mode`);
    this.tp = new TimeSeriesChart($("chart-throughput") as HTMLCanvasElement, { minMax: 10 });
    this.lat = new TimeSeriesChart($("chart-latency") as HTMLCanvasElement, { log: true, unit: "ms", format: (v) => (v >= 1 ? v.toFixed(0) : v.toFixed(1)) });
    this.histChart = new TimeSeriesChart($("chart-history") as HTMLCanvasElement, { minMax: 10 });
    $("tp-legend").innerHTML = legend([["reads/s", PALETTE[1]], ["writes/s", PALETTE[0]], ["errors/s", "#ff5c7a"]]);
    $("lat-legend").innerHTML = legend([["p50", PALETTE[3]], ["p99", PALETTE[4]]]);
  }

  start(): void {
    if (this.be.mode === "sim") {
      $("sim-banner").hidden = false;
      $("chaos-sub").textContent = "simulated failures, real routing";
      $("mode-pill").textContent = "SIM";
    }
    this.be.subscribe((s) => this.onSnapshot(s), (c) => this.onConn(c));
    setInterval(() => this.pollDemo(), 2000);
    setInterval(() => this.pollNodeLogs(), 2000);
    setInterval(() => document.hidden || this.renderStrip(), 250);
    document.addEventListener("visibilitychange", () => {
      if (document.hidden) return;
      if (this.stale) this.renderAll();
      this.pollHistory();
      this.pollNodeLogs();
    });
    this.renderIncidents();
    // History needs to know whether a historian is running, so it waits
    // for the first demo-info response.
    this.pollDemo().then(() => this.pollHistory());
    setInterval(() => this.pollHistory(), 10_000);
    this.wireControls();
    this.wireIncidents();
    this.wireGameDay();
    this.gd = this.loadGameDay();
    this.renderGameDay();
  }

  private onConn(c: ConnState): void {
    if (c === "reconnecting") this.logs.add("dashboard", "warn", "event stream lost; reconnecting");
    const pill = $("conn-pill");
    pill.className = `pill ${c === "reconnecting" ? "pill-warn" : "pill-ok"}`;
    pill.textContent = c === "live" ? "LIVE · SSE" : c === "sim" ? "SIMULATION" : "RECONNECTING";
  }

  private color(id: string): string {
    const i = this.snap?.spec.members.findIndex((m) => m.id === id) ?? 0;
    return PALETTE[(i < 0 ? 0 : i) % PALETTE.length];
  }

  // ---- snapshots ----------------------------------------------------------

  private onSnapshot(s: ClusterStatus): void {
    this.prev = this.snap;
    this.snap = s;
    if (!this.topo || this.topo.shards !== s.spec.shards || this.topo.members.length !== s.spec.members.length) {
      this.topo = new Topology(s.spec);
      this.ring = new RingView($("ring") as unknown as SVGSVGElement, this.topo, (sh) => {
        this.hoverShard = sh;
        this.renderRingCenter();
      });
      $("ring-legend").innerHTML = legend(s.spec.members.map((m) => [m.id, this.color(m.id)] as [string, string]).concat([["no leader", "#ff5c7a"]]));
      $("ring-hint").textContent = `${s.spec.shards} shards × ${s.spec.virtual_nodes} vnodes, RF=${s.spec.replication_factor}`;
    }
    this.t.push(s.ts);
    this.s.reads.push(s.load.reads_per_sec);
    this.s.writes.push(s.load.writes_per_sec);
    this.s.errors.push(s.load.errors_per_sec);
    this.s.p50.push(s.load.p50_ms);
    this.s.p99.push(s.load.p99_ms);
    if (this.t.length > WINDOW) {
      this.t.shift();
      Object.values(this.s).forEach((a) => a.shift());
    }
    const tick = this.diff();
    this.tracker.observe(s, this.demo, tick);
    for (const e of tick) this.logs.add("cluster", EVENT_LEVEL[e.kind] ?? "info", e.detail, [], e.ts);
    if (this.tracker.active && Date.now() - this.lastSave > 10_000) this.saveIncidents();
    // A background tab keeps tracking (alerts and reports stay right) but
    // skips the DOM work; it catches up when it becomes visible.
    if (document.hidden) {
      this.stale = true;
      return;
    }
    this.renderAll();
  }

  private stale = false;

  private renderAll(): void {
    this.stale = false;
    if (!this.snap) return;
    this.render();
    this.renderIncidents();
    this.renderStrip();
    if (this.reportId !== null && this.tracker.active?.id === this.reportId) this.renderReport();
  }

  /** Derives events by comparing snapshots; returns this tick's events. */
  private diff(): Ev[] {
    const a = this.prev;
    const b = this.snap!;
    const tick: Ev[] = [];
    if (!a) return tick;
    const push = (kind: string, detail: string) => {
      const e = { ts: b.ts, kind, detail };
      this.events.unshift(e);
      tick.push(e);
      if (kind === "leader_change" || kind === "shard_recovered") this.leaderChanges.push(b.ts);
    };
    const pn = new Map(a.nodes.map((n) => [n.id, n]));
    for (const n of b.nodes) {
      const o = pn.get(n.id);
      if (!o) continue;
      if (o.reachable && !n.reachable) push("node_down", `${n.id} became unreachable`);
      if (!o.reachable && n.reachable) push("node_up", `${n.id} is back`);
      if (!o.faults?.isolated && n.faults?.isolated) push("partitioned", `${n.id} partitioned from its peers`);
      if (o.faults?.isolated && !n.faults?.isolated) push("healed", `partition of ${n.id} healed`);
    }
    const flashed: number[] = [];
    for (const sh of b.shards) {
      const o = a.shards[sh.shard];
      if (!o) continue;
      const was = o.leader ?? "";
      const now = sh.leader ?? "";
      if (was && !now) push("shard_unavailable", `shard ${sh.shard} lost its leader ${was}`);
      else if (!was && now) push("shard_recovered", `shard ${sh.shard}: ${now} elected (term ${sh.term})`);
      else if (was !== now) push("leader_change", `shard ${sh.shard}: ${was} → ${now} (term ${sh.term})`);
      else if (now && sh.term > o.term) push("election", `shard ${sh.shard}: ${now} re-elected (term ${sh.term})`);
      if (was !== now) flashed.push(sh.shard);
    }
    this.flash = flashed;
    if (this.events.length > 300) this.events.length = 300;
    const cutoff = b.ts - 5 * 60_000;
    this.leaderChanges = this.leaderChanges.filter((t) => t >= cutoff);
    return tick;
  }

  private flash: number[] = [];

  // ---- rendering ------------------------------------------------------------

  private render(): void {
    const s = this.snap!;
    this.renderHeader(s);
    this.renderKPIs(s);
    this.tp.set(this.t, [
      { name: "reads/s", color: PALETTE[1], fill: true, stack: true, values: this.s.reads },
      { name: "writes/s", color: PALETTE[0], fill: true, stack: true, values: this.s.writes },
      { name: "errors/s", color: "#ff5c7a", values: this.s.errors },
    ]);
    this.lat.set(this.t, [
      { name: "p50", color: PALETTE[3], values: this.s.p50 },
      { name: "p99", color: PALETTE[4], fill: true, values: this.s.p99 },
    ]);
    this.renderNodes(s);
    this.ring?.update(s.shards.map((x) => x.leader), (id) => this.color(id));
    this.renderRingCenter();
    this.renderMatrix(s);
    this.renderEvents();
  }

  private renderHeader(s: ClusterStatus): void {
    const pill = $("health-pill");
    const up = s.nodes.filter((n) => n.reachable).length;
    const total = s.shards.length;
    if (s.healthy_shards === total && up === s.nodes.length) {
      pill.className = "pill pill-ok";
      pill.textContent = "HEALTHY";
    } else if (s.healthy_shards === total) {
      pill.className = "pill pill-warn";
      pill.textContent = `DEGRADED · ${s.nodes.length - up} node(s) down`;
    } else {
      pill.className = "pill pill-bad";
      pill.textContent = `FAILOVER · ${total - s.healthy_shards} shard(s) without a leader`;
    }
    $("observed").textContent = `${s.fingerprint} · via ${s.observed_by} · ${new Date(s.ts).toLocaleTimeString()}`;
  }

  private renderKPIs(s: ClusterStatus): void {
    const ops = s.load.reads_per_sec + s.load.writes_per_sec;
    // Prefer what a real client saw (demo traffic generator, with retries);
    // otherwise fall back to the server-side 5xx ratio.
    const cr = this.demo?.client;
    let avail: number;
    let availSub: string;
    if (cr && cr.ok_total + cr.failed_total > 0) {
      avail = cr.availability;
      availSub = `${cr.ok_total.toLocaleString()} client ops · ${cr.failed_total} failed · SLO 99.9%`;
    } else {
      const tot = this.s.reads.reduce((a, b) => a + b, 0) + this.s.writes.reduce((a, b) => a + b, 0);
      const errs = this.s.errors.reduce((a, b) => a + b, 0);
      avail = tot > 0 ? 1 - errs / (tot + errs) : 1;
      availSub = `server 5xx ratio, last ${this.t.length}s`;
    }
    const up = s.nodes.filter((n) => n.reachable).length;
    const kpi = (label: string, value: string, sub: string, cls = "", glow = "") =>
      `<div class="kpi ${cls}" style="${glow ? `--glow:${glow}` : ""}"><div class="label">${label}</div><div class="value">${value}</div><div class="sub">${sub}</div></div>`;
    const p99cls = s.load.p99_ms === 0 ? "" : s.load.p99_ms < 10 ? "good" : s.load.p99_ms < 100 ? "warn" : "bad";
    setHTML($("kpis"), [
      kpi("Throughput", `${fmtNum(ops)}<small>ops/s</small>`, `${fmtNum(s.load.reads_per_sec)} reads · ${fmtNum(s.load.writes_per_sec)} writes`, "", "rgba(0,212,255,.3)"),
      kpi("p99 latency", fmtMs(s.load.p99_ms), `p50 ${fmtMs(s.load.p50_ms)} · SLO &lt; 10ms`, p99cls, "rgba(255,181,71,.25)"),
      kpi("Availability", `${(avail * 100).toFixed(avail === 1 ? 0 : 3)}%`, availSub, avail >= 0.999 ? "good" : "bad", "rgba(46,229,157,.25)"),
      kpi("Shards", `${s.healthy_shards}<small>/ ${s.shards.length}</small>`, "with an elected leader", s.healthy_shards === s.shards.length ? "good" : "bad"),
      kpi("Nodes", `${up}<small>/ ${s.nodes.length}</small>`, `RF=${s.spec.replication_factor} · tolerates ${Math.floor((s.spec.replication_factor - 1) / 2)} failure`, up === s.nodes.length ? "good" : "warn"),
      kpi("Leader moves", String(this.leaderChanges.length), "last 5 minutes", "", "rgba(255,92,168,.25)"),
      this.consistencyKPI(kpi),
    ].join(""));
  }

  private consistencyKPI(kpi: (label: string, value: string, sub: string, cls?: string, glow?: string) => string): string {
    const c = this.demo?.consistency;
    const tile = (html: string) => html.replace('<div class="kpi', '<div data-open="consistency" title="Click for details" class="kpi clickable');
    if (!c) {
      const sub = this.demo === null && this.demoPolled ? "the probe runs with make demo" : "waiting for the probe…";
      return tile(kpi("Linearizability", "—", sub));
    }
    if (c.verdict === "violation") return tile(kpi("Linearizability", "✗ <small>violated</small>", `${c.violations_total} non-linearizable histor${c.violations_total === 1 ? "y" : "ies"}`, "bad", "rgba(255,92,122,.35)"));
    if (c.verdict === "pending") return tile(kpi("Linearizability", "…", "first epochs being checked"));
    const ago = c.last_check ? Math.max(0, (Date.now() - c.last_check) / 1000) : 0;
    return tile(
      kpi(
        "Linearizable",
        `✓ ${fmtNum(c.ops_checked)}<small>ops</small>`,
        `${c.epochs.toLocaleString()} epochs checked${this.be.mode === "sim" ? " (simulated)" : ""} · ${ago < 5 ? "live" : `${ago.toFixed(0)}s ago`}`,
        "good",
        "rgba(46,229,157,.3)",
      ),
    );
  }

  private renderNodes(s: ClusterStatus): void {
    const host = $("nodes");
    const maxLead = Math.max(1, ...s.nodes.map((n) => n.shards_led));
    for (const n of s.nodes) {
      let card = this.nodeCards.get(n.id);
      if (!card) {
        card = document.createElement("div");
        card.className = "node";
        card.style.setProperty("--node", this.color(n.id));
        host.appendChild(card);
        this.nodeCards.set(n.id, card);
      }
      const proc = this.demo?.nodes.find((d) => d.id === n.id);
      const isolated = !!n.faults?.isolated;
      const lossy = (n.faults?.drop_percent ?? 0) > 0;
      const down = !n.reachable;
      card.classList.toggle("down", down);
      card.classList.toggle("partitioned", isolated);
      const state = down
        ? `<span class="state" style="color:var(--bad)">${proc?.state ?? "unreachable"}</span>`
        : isolated
          ? `<span class="state" style="color:var(--warn)">partitioned</span>`
          : lossy
            ? `<span class="state" style="color:var(--warn)">${n.faults!.drop_percent}% loss</span>`
            : n.ready
              ? `<span class="state" style="color:var(--ok)">ready</span>`
              : (n.uptime_seconds ?? 0) < 10
                ? `<span class="state" style="color:var(--muted)">starting</span>`
                : `<span class="state" style="color:var(--warn)" title="some local shards have no leader">degraded</span>`;
      const l = n.load;
      const controls = this.demo
        ? `<div class="node-actions">
            ${down
              ? `<button class="btn sm ok" data-node="${n.id}" data-act="start">▶ Start</button>`
              : `<button class="btn sm danger" data-node="${n.id}" data-act="kill">kill -9</button>
                 <button class="btn sm" data-node="${n.id}" data-act="stop">Graceful stop</button>
                 <button class="btn sm warn" data-node="${n.id}" data-act="${isolated ? "heal" : "isolate"}">${isolated ? "Heal" : "Partition"}</button>`}
           </div>`
        : "";
      setHTML(card, `
        <div class="node-head"><span class="dot"></span><strong>${esc(n.id)}</strong>${state}</div>
        <div class="node-meta">${esc(n.addr.replace(/^https?:\/\//, ""))}${proc && !down ? ` · pid ${proc.pid}` : ""}${n.uptime_seconds && !down ? ` · up ${fmtDur(n.uptime_seconds)}` : ""}</div>
        <div class="node-stats">
          <div><b>${n.shards_led}</b><span>leads</span></div>
          <div><b>${l ? fmtNum(l.reads_per_sec + l.writes_per_sec) : "—"}</b><span>ops/s</span></div>
          <div><b>${l ? fmtMs(l.p99_ms) : "—"}</b><span>p99</span></div>
        </div>
        <div class="leadbar" title="share of shard leaderships"><i style="width:${(n.shards_led / maxLead) * 100}%"></i></div>
        ${controls}`);
    }
  }

  private renderRingCenter(): void {
    if (!this.ring || !this.snap) return;
    const sh = this.hoverShard;
    if (sh === null) {
      if (!($("find-key") as HTMLInputElement).value) {
        this.ring.highlight(null);
        this.ring.setCenter(`${this.snap.healthy_shards}/${this.snap.shards.length}`, "SHARDS LED");
      }
      return;
    }
    this.ring.highlight(sh);
    const info = this.snap.shards[sh];
    this.ring.setCenter(`shard ${sh}`, info?.leader ? `LEADER ${info.leader.toUpperCase()} · TERM ${info.term}` : "ELECTING…");
  }

  private renderMatrix(s: ClusterStatus): void {
    const nodes = s.spec.members.map((m) => m.id);
    const reach = new Map(s.nodes.map((n) => [n.id, n.reachable && !n.faults?.isolated] as [string, boolean]));
    const head = `<thead><tr><th>shard</th>${nodes.map((n) => `<th class="node-col" style="--node:${this.color(n)}">${n}</th>`).join("")}<th>term</th><th>commit</th><th>keys</th></tr></thead>`;
    const rows: string[] = [];
    for (const sh of s.shards) {
      const flash = this.flash.includes(sh.shard) ? ' class="flash"' : "";
      let html = `<tr${flash}><td class="shard">${sh.shard}</td>`;
      for (const n of nodes) {
        const style = `style="--node:${this.color(n)}"`;
        if (!sh.replicas.includes(n)) {
          html += `<td class="none">·</td>`;
        } else if (sh.leader === n) {
          html += `<td class="leader" ${style} title="leader">★</td>`;
        } else if (!reach.get(n)) {
          html += `<td class="down" title="replica unreachable">✕</td>`;
        } else {
          const lag = sh.lag?.[n];
          html += `<td class="follower${lag && lag > 1000 ? " lagging" : ""}" ${style} title="follower${lag !== undefined ? `, ${lag} entries behind` : ""}">●</td>`;
        }
      }
      const liveReplicas = sh.replicas.filter((r) => reach.get(r)).length;
      const quorum = Math.floor(sh.replicas.length / 2) + 1;
      const state = sh.leader ? String(sh.term) : liveReplicas < quorum ? `no quorum (${liveReplicas}/${sh.replicas.length})` : "electing";
      html += `<td class="${sh.leader ? "" : "unhealthy"}">${state}</td><td>${sh.commit_index.toLocaleString()}</td><td>${sh.keys.toLocaleString()}</td></tr>`;
      rows.push(html);
    }
    // Replace only the rows that changed (usually a few commit counters).
    const table = $("matrix") as HTMLTableElement;
    const prev = this.matrixRows;
    if (head !== this.matrixHead || prev.length !== rows.length || !table.tBodies[0]) {
      table.innerHTML = head + "<tbody>" + rows.join("") + "</tbody>";
    } else {
      const body = table.tBodies[0];
      rows.forEach((r, i) => {
        if (r !== prev[i]) body.rows[i].outerHTML = r;
      });
    }
    this.matrixHead = head;
    this.matrixRows = rows;
  }

  private matrixHead = "";
  private matrixRows: string[] = [];
  private shownEvents: Ev[] = [];
  private shownFilter = "";

  private renderEvents(): void {
    const keep = EVENT_FILTERS[this.eventFilter] ?? EVENT_FILTERS.all;
    const list = this.events.filter((e) => keep(e.kind)).slice(0, 150);
    const item = (e: Ev, fresh: boolean) => {
      const [icon, cls] = EVENT_STYLE[e.kind] ?? ["•", ""];
      const sh = /shard (\d+)/.exec(e.detail)?.[1];
      const inc = /^incident #(\d+)/i.exec(e.detail)?.[1];
      const data = sh !== undefined ? ` data-shard="${sh}"` : inc !== undefined ? ` data-incident="${inc}"` : "";
      return `<li class="${cls}${data ? " clickable" : ""}${fresh ? " new" : ""}"${data}><time>${new Date(e.ts).toLocaleTimeString([], { hour12: false })}</time><span>${icon}</span><span>${esc(e.detail)}</span></li>`;
    };
    const ol = $("events");
    const prev = this.shownEvents;
    // Usually nothing changed, or new events arrived at the top: insert just
    // those (and animate only them) instead of rebuilding 150 items.
    // The fast path applies only if everything after the new head is exactly
    // what is shown (minus what the 150-item cap pushed off the end); events
    // added anywhere else (e.g. older ones loaded from the historian) need a
    // full render.
    const added = prev.length ? list.indexOf(prev[0]) : -1;
    const tail = list.length - added;
    const sameTail =
      added >= 0 &&
      this.shownFilter === this.eventFilter &&
      tail <= prev.length &&
      (tail === prev.length || list.length === 150) &&
      list.slice(added).every((e, i) => e === prev[i]);
    if (sameTail && list.length) {
      if (added > 0) {
        ol.insertAdjacentHTML("afterbegin", list.slice(0, added).map((e) => item(e, true)).join(""));
        while (ol.children.length > list.length) ol.lastElementChild!.remove();
      }
    } else {
      ol.innerHTML = list.length
        ? list.map((e) => item(e, false)).join("")
        : `<li><time></time><span>✨</span><span class="muted">${this.events.length ? "Nothing matches this filter yet." : "All quiet. Try the chaos buttons."}</span></li>`;
    }
    this.shownEvents = list;
    this.shownFilter = this.eventFilter;
  }

  /** Highlights a shard on the ring and in the matrix for a few seconds. */
  private locateShard(sh: number): void {
    if (!this.snap || !this.snap.shards[sh]) return;
    this.hoverShard = sh;
    this.renderRingCenter();
    this.flash = [sh];
    this.renderMatrix(this.snap);
    clearTimeout(this.locateTimer);
    this.locateTimer = setTimeout(() => {
      this.hoverShard = null;
      this.renderRingCenter();
    }, 3500);
  }

  // ---- demo / historian polling -----------------------------------------------

  private demoPolled = false;

  private async pollDemo(): Promise<void> {
    this.demo = await this.be.demoInfo();
    this.demoPolled = true;
    const isDemo = !!this.demo?.demo;
    $("mode-pill").hidden = !isDemo;
    $("control-card").hidden = !isDemo;
    $("gameday-card").hidden = !isDemo;
    document.querySelector<HTMLElement>("#incident-strip [data-scenario]")!.hidden = !isDemo;
    this.onConsistency(this.demo?.consistency);
    const c = this.demo?.client;
    if (c) {
      if (this.lastClientFailed !== null && c.failed_total > this.lastClientFailed) {
        const n = c.failed_total - this.lastClientFailed;
        this.logs.add("client", "error", `${n} operation${n === 1 ? "" : "s"} failed after exhausting retries`, [["failed_total", String(c.failed_total)]]);
      }
      this.lastClientFailed = c.failed_total;
      if (Date.now() - this.lastClientLog >= 10_000) {
        this.lastClientLog = Date.now();
        this.logs.add("client", "info", "traffic generator stats", [
          ["ok_per_sec", String(c.last_second.ok)],
          ["failed_per_sec", String(c.last_second.failed)],
          ["availability_60s", `${(c.availability_60s * 100).toFixed(3)}%`],
          ["ok_total", String(c.ok_total)],
          ["failed_total", String(c.failed_total)],
        ]);
      }
    }
    if (isDemo && !this.loadSynced) {
      this.loadSynced = true;
      ($("rate") as HTMLInputElement).value = String(this.demo!.rate);
      ($("ratio") as HTMLInputElement).value = String(Math.round(this.demo!.read_ratio * 100));
      $("rate-out").textContent = String(this.demo!.rate);
      $("ratio-out").textContent = String(Math.round(this.demo!.read_ratio * 100));
    }
  }

  private lastViolations: number | null = null;
  private lastConsistencyLog = 0;

  private onConsistency(c: ConsistencyReport | undefined): void {
    const pill = $("consistency-pill");
    pill.hidden = !c;
    if (!c) return;
    pill.className = `pill pill-btn ${c.verdict === "violation" ? "pill-bad" : c.verdict === "linearizable" ? "pill-ok" : "pill-muted"}`;
    pill.textContent = c.verdict === "violation" ? "✗ NOT LINEARIZABLE" : c.verdict === "linearizable" ? `✓ LINEARIZABLE · ${fmtNum(c.ops_checked)} ops` : "CHECKING…";
    if (this.lastViolations !== null && c.violations_total > this.lastViolations) {
      const v = c.violations.at(-1);
      this.alerts.raise({
        level: "critical",
        incidentId: this.tracker.active?.id,
        title: "Consistency violation detected",
        body: `The history of ${v?.key ?? "a probe key"} is not linearizable (${v?.ops.length ?? 0} operations). Open the consistency panel for the operations.`,
      });
      this.events.unshift({ ts: Date.now(), kind: "violation", detail: `consistency violation on ${v?.key ?? "?"}: history not linearizable` });
      this.logs.add("consistency", "error", "history is NOT linearizable", [["key", v?.key ?? "?"], ["ops", String(v?.ops.length ?? 0)]]);
      this.renderEvents();
    }
    this.lastViolations = c.violations_total;
    if (Date.now() - this.lastConsistencyLog >= 10_000 && c.epochs > 0) {
      this.lastConsistencyLog = Date.now();
      this.logs.add("consistency", c.verdict === "violation" ? "error" : "info", `verdict ${c.verdict}`, [
        ["ops_checked", String(c.ops_checked)],
        ["epochs", String(c.epochs)],
        ["pending", String(c.ops_pending)],
        ["indeterminate", String(c.indeterminate)],
      ]);
    }
    if (($("consistency-dialog") as HTMLDialogElement).open) this.renderConsistency();
  }

  private renderConsistency(): void {
    const c = this.demo?.consistency;
    const body = $("consistency-body");
    if (!c) {
      body.innerHTML = `<p class="muted">The consistency probe runs inside the demo supervisor (<code>make demo</code> / <code>make docker-demo</code>), and in the Pages simulation.</p>`;
      return;
    }
    const sim = this.be.mode === "sim";
    const verdict =
      c.verdict === "violation"
        ? `<div class="verdict bad">✗ Not linearizable</div>`
        : c.verdict === "linearizable"
          ? `<div class="verdict ok">✓ Linearizable</div>`
          : `<div class="verdict">Checking the first epochs…</div>`;
    const stat = (v: string, l: string, cls = "") => `<div class="cstat ${cls}"><b>${v}</b><span>${l}</span></div>`;
    // Epoch timeline: one row per key over the last 90 s; outages show up as
    // long epochs (a key's epoch can only close while its shard has a leader).
    const now = Date.now();
    const span = 90_000;
    const x = (t: number) => Math.max(0, Math.min(100, ((t - (now - span)) / span) * 100));
    const keys = [...new Set(c.recent.map((e) => e.key))].sort();
    const incidents = this.tracker.incidents.filter((i) => (i.resolvedAt ?? now) > now - span);
    const shade = incidents
      .map((i) => `<b class="ewin" style="left:${x(i.openedAt)}%;width:${Math.max(0.5, x(i.resolvedAt ?? now) - x(i.openedAt))}%" title="Incident #${i.id}"></b>`)
      .join("");
    const rows = keys
      .map((k) => {
        const cells = c.recent
          .filter((e) => e.key === k && e.end > now - span)
          .map((e) => {
            const l = x(e.start);
            const w = Math.max(0.4, x(e.end) - l);
            return `<i class="${e.ok ? "ok" : "bad"}${e.end - e.start > 4000 ? " long" : ""}" style="left:${l}%;width:${w}%" title="${new Date(e.start).toLocaleTimeString([], { hour12: false })}–${new Date(e.end).toLocaleTimeString([], { hour12: false })} · ${e.ops} ops · ${e.ok ? "linearizable" : "VIOLATION"}"></i>`;
          })
          .join("");
        return `<div class="erow"><span>${esc(k.replace("__probe/", ""))}</span><div class="etrack">${shade}${cells}</div></div>`;
      })
      .join("");
    const viol = c.violations.length
      ? c.violations
          .slice()
          .reverse()
          .map(
            (v) => `<details class="viol" open><summary>✗ ${esc(v.key)} at ${new Date(v.time).toLocaleTimeString([], { hour12: false })}: ${esc(v.error)}</summary>
            <table><tr><th>client</th><th>op</th><th>value</th><th>call</th><th>return</th></tr>
            ${v.ops.map((o) => `<tr><td>${o.client < 0 ? "barrier" : `w${o.client}`}</td><td>${o.kind}${o.unknown ? " (indeterminate)" : ""}</td><td>${o.kind === "delete" ? (o.found ? "deleted" : "absent") : o.kind === "get" && !o.found ? "absent" : esc(o.value ?? "")}</td><td>${new Date(o.call).toISOString().slice(17, 23)}</td><td>${o.return ? new Date(o.return).toISOString().slice(17, 23) : "—"}</td></tr>`).join("")}
            </table></details>`,
          )
          .join("")
      : `<p class="muted small">No violations. If one is ever found, the offending history (every operation with its invocation and response time) appears here.</p>`;
    body.innerHTML = `
      ${verdict}
      <p>${sim ? "<b>Simulated in this Pages demo.</b> With <code>make demo</code> this is real: " : ""}A probe client runs 4 workers that read, write and delete ${c.keys} keys (about 120 ops/s) through the real client, recording every operation's invocation and response time. Every 2 s the workers pause and one solo read per key pins its state, which closes that key's <b>epoch</b>. The epoch's history is then checked with the <b>Wing &amp; Gong</b> algorithm with Lowe's memoisation (as in Porcupine and Jepsen). Writes whose outcome the client never learned count as possibly applied at any later point, or never.</p>
      <div class="cstats">
        ${stat(c.ops_checked.toLocaleString(), "operations verified", c.verdict === "violation" ? "" : "good")}
        ${stat(c.epochs.toLocaleString(), "epochs checked")}
        ${stat(c.ops_pending.toLocaleString(), "awaiting an epoch close")}
        ${stat(c.indeterminate.toLocaleString(), "indeterminate writes")}
        ${stat(c.inconclusive.toLocaleString(), "inconclusive checks")}
        ${stat(c.violations_total.toLocaleString(), "violations", c.violations_total ? "bad" : "good")}
      </div>
      <h4>Epochs, last 90 seconds <small>one row per key · red bands: incidents · long bars: keys whose shard had no leader</small></h4>
      <div class="epochs">${rows || `<p class="muted small">No epochs yet.</p>`}</div>
      <h4>Violations</h4>
      ${viol}`;
  }

  private historyEventsLoaded = false;

  private async pollHistory(): Promise<void> {
    if (document.hidden) return;
    const pts = await this.be.history(15);
    const card = $("history-card");
    if (!pts) {
      $("history-stats").textContent = "historian not running";
      $("history-note").textContent = "Start the demo with the Python historian (default in `make demo`) to record history into SQLite.";
      this.histChart.set([], []);
      card.classList.add("muted");
      return;
    }
    card.classList.remove("muted");
    this.histChart.set(
      pts.map((p) => p.t),
      [
        { name: "reads/s", color: PALETTE[1], fill: true, stack: true, values: pts.map((p) => p.reads) },
        { name: "writes/s", color: PALETTE[0], fill: true, stack: true, values: pts.map((p) => p.writes) },
        { name: "errors/s", color: "#ff5c7a", values: pts.map((p) => p.errors) },
      ],
    );
    const st = await this.be.historianStats();
    if (st) {
      const evs = Object.values(st.events).reduce((a, b) => a + b, 0);
      $("history-stats").textContent = `${st.samples.toLocaleString()} samples · ${evs} events · ${(st.db_bytes / 1024).toFixed(0)} KiB on disk`;
    }
    if (!this.historyEventsLoaded) {
      // Pre-populate the timeline with persisted events (survives reloads).
      this.historyEventsLoaded = true;
      const evs: HistorianEvent[] | null = await this.be.historianEvents();
      if (evs?.length) {
        for (const e of evs.reverse()) this.events.push({ ts: e.ts, kind: e.kind, detail: e.detail });
        this.events.sort((a, b) => b.ts - a.ts);
        $("events-hint").textContent = "live + persisted (SQLite)";
        this.renderEvents();
      }
    }
  }

  // ---- controls ------------------------------------------------------------------

  private logAction(detail: string, level: Level = "warn"): void {
    this.events.unshift({ ts: Date.now(), kind: "action", detail });
    this.logs.add("chaos", level, detail);
    this.renderEvents();
  }

  /**
   * Runs one node action. Unless track is false (multi-step scenarios report
   * themselves), chaos opens or escalates an incident and a heal is recorded
   * against the open one.
   */
  private async act(id: string, act: string, opts: { track?: boolean; why?: string } = {}): Promise<void> {
    try {
      if (act === "isolate") await this.be.setFaults(id, { isolated: true, drop_percent: 0, delay_ms: 0 });
      else if (act === "heal") await this.be.setFaults(id, { isolated: false, drop_percent: 0, delay_ms: 0 });
      else await this.be.nodeAction(id, act as "kill" | "stop" | "start");
      if (act === "stop") this.tracker.noteGracefulStop(id);
      const text = (ACT_TEXT[act] ?? ((x: string) => `${act} ${x}`))(id) + (opts.why ? ` (${opts.why})` : "");
      const healing = act === "start" || act === "heal";
      this.logAction(`you: ${text}`, healing ? "info" : "warn");
      if (opts.track !== false) {
        this.tracker.action(healing ? "heal" : "chaos", text, this.demo);
        if (healing) this.healAlert(text);
      }
      toast(text, act === "kill" ? "bad" : "ok");
      setTimeout(() => this.pollDemo(), 300);
    } catch (e) {
      toast(String(e), "bad");
      this.logs.add("chaos", "error", `action failed: ${act} ${id}: ${e}`);
    }
  }

  private healAlert(what: string): void {
    const inc = this.tracker.active;
    this.alerts.raise({
      level: "info",
      title: inc ? `Healing incident #${inc.id}` : "Heal requested",
      body: inc ? `${what}. Waiting for every node and shard to be healthy…` : `${what}. Nothing was broken.`,
      incidentId: inc?.id,
    });
  }

  private async scenario(name: string): Promise<void> {
    const s = this.snap;
    if (!s) return;
    const live = s.nodes.filter((n) => n.reachable && !n.faults?.isolated);
    const pick = <T>(xs: T[]) => xs[Math.floor(Math.random() * xs.length)];
    switch (name) {
      case "kill-leader": {
        const busiest = [...live].sort((a, b) => b.shards_led - a.shards_led)[0];
        if (busiest) await this.act(busiest.id, "kill", { why: `busiest leader, ${busiest.shards_led} shards` });
        break;
      }
      case "partition": {
        const n = pick(live);
        if (n) await this.act(n.id, "isolate");
        break;
      }
      case "lossy": {
        const n = pick(live);
        if (!n) break;
        await this.be.setFaults(n.id, { isolated: false, drop_percent: 30, delay_ms: 5 });
        this.logAction(`you: 30% packet loss + 5ms delay on ${n.id}`);
        this.tracker.action("chaos", `30% packet loss + 5 ms delay on ${n.id}`, this.demo);
        toast(`30% packet loss on ${n.id}`);
        break;
      }
      case "rolling": {
        toast("Rolling restart: graceful stop → start, one node at a time");
        // One incident for the whole procedure, kept open until it finishes.
        this.tracker.action("chaos", "rolling restart (graceful stop → start, one node at a time)", this.demo);
        this.tracker.hold();
        try {
          for (const n of s.spec.members) {
            await this.act(n.id, "stop", { track: false });
            await sleep(2500);
            await this.act(n.id, "start", { track: false });
            await sleep(3500);
          }
        } finally {
          this.tracker.release();
        }
        this.tracker.action("heal", "rolling restart finished", this.demo);
        toast("Rolling restart complete", "ok");
        break;
      }
      case "heal": {
        // The snapshot can be a second old (a fault injected just now may not
        // show yet), so use the supervisor's process states, which change
        // immediately, and clear faults on every node that is up.
        await this.pollDemo();
        const procs = new Map((this.demo?.nodes ?? []).map((p) => [p.id, p.state] as [string, string]));
        await Promise.all(
          s.spec.members.map(async ({ id }) => {
            const node = this.snap?.nodes.find((n) => n.id === id);
            const state = procs.get(id);
            try {
              if (state ? state !== "running" : !node?.reachable) await this.be.nodeAction(id, "start");
              else await this.be.setFaults(id, { isolated: false, drop_percent: 0, delay_ms: 0 });
            } catch (e) {
              this.logs.add("chaos", "warn", `heal of ${id} failed: ${e}`);
            }
          }),
        );
        this.logAction("you: healed everything", "info");
        this.tracker.action("heal", "heal everything", this.demo);
        this.healAlert("Heal everything: restarting stopped nodes and clearing network faults");
        toast("Healing all nodes", "ok");
        break;
      }
    }
  }

  private wireControls(): void {
    $("nodes").addEventListener("click", (e) => {
      const b = (e.target as HTMLElement).closest<HTMLButtonElement>("button[data-node]");
      if (b) {
        // Re-enable when done: an unchanged card isn't re-rendered (setHTML),
        // so a failed action must not leave its button disabled.
        b.disabled = true;
        this.act(b.dataset.node!, b.dataset.act!).finally(() => (b.disabled = false));
      }
    });
    document.querySelectorAll<HTMLButtonElement>("[data-scenario]").forEach((b) =>
      b.addEventListener("click", async () => {
        b.disabled = true;
        try {
          await this.scenario(b.dataset.scenario!);
        } finally {
          b.disabled = false;
        }
      }),
    );

    const rate = $("rate") as HTMLInputElement;
    const ratio = $("ratio") as HTMLInputElement;
    let pending: ReturnType<typeof setTimeout> | undefined;
    const sendLoad = () => {
      $("rate-out").textContent = rate.value;
      $("ratio-out").textContent = ratio.value;
      clearTimeout(pending);
      pending = setTimeout(() => this.be.setLoad(Number(rate.value), Number(ratio.value) / 100).catch((e) => toast(String(e), "bad")), 200);
    };
    rate.addEventListener("input", sendLoad);
    ratio.addEventListener("input", sendLoad);

    const find = $("find-key") as HTMLInputElement;
    find.addEventListener("input", () => {
      const key = find.value;
      const out = $("find-result");
      if (!key || !this.ring || !this.snap) {
        this.ring?.locate(null);
        out.className = "find-result muted";
        out.textContent = "Type a key to see where it lives.";
        this.renderRingCenter();
        return;
      }
      const shard = this.ring.locate(key)!;
      const info = this.snap.shards[shard];
      out.className = "find-result";
      out.innerHTML = `<div class="big">shard ${shard}</div>
        <div>leader <b style="color:${info.leader ? this.color(info.leader) : "var(--bad)"}">${info.leader ?? "electing"}</b></div>
        <div class="muted">replicas ${info.replicas.map((r) => `<b style="color:${this.color(r)}">${r}</b>`).join(", ")}</div>`;
      this.ring.setCenter(`shard ${shard}`, `"${key.length > 14 ? key.slice(0, 13) + "…" : key}"`);
    });

    const form = $("kv-form") as HTMLFormElement;
    const run = async (op: "put" | "get" | "delete") => {
      const key = ($("kv-key") as HTMLInputElement).value.trim();
      if (!key) return toast("key required", "bad");
      const value = ($("kv-value") as HTMLInputElement).value;
      const ttl = Number(($("kv-ttl") as HTMLInputElement).value) || undefined;
      const out = $("kv-result");
      try {
        const r = await this.be.kv(op, key, value, ttl);
        this.logs.add("kv", r.ok || r.status === 404 ? "info" : "warn", `${op.toUpperCase()} ${key} → ${r.status}`, [
          ["shard", String(r.shard ?? "?")],
          ["served_by", r.servedBy ?? "?"],
          ["ms", r.ms.toFixed(1)],
        ]);
        const predicted = this.topo?.shardForKey(key);
        const match = r.shard === undefined ? "" : r.shard === predicted ? " ✓ matches client-side hash" : " ✗ mismatch!";
        out.innerHTML =
          `<span class="${r.ok ? "okc" : "errc"}">${op.toUpperCase()} ${esc(key)} → HTTP ${r.status}</span>  (${r.ms.toFixed(1)} ms)\n` +
          `<span class="k">shard</span>      ${r.shard ?? "?"}${match}\n` +
          `<span class="k">served by</span>  ${r.servedBy ?? "?"}${r.leader ? `   (leader ${r.leader})` : ""}\n` +
          (r.version !== undefined ? `<span class="k">version</span>    ${r.version}  (Raft log index of the write)\n` : "") +
          `\n${esc(r.body)}`;
      } catch (e) {
        out.innerHTML = `<span class="errc">${esc(String(e))}</span>`;
      }
    };
    form.addEventListener("submit", (e) => {
      e.preventDefault();
      run("put");
    });
    form.querySelectorAll<HTMLButtonElement>("button[data-op]").forEach((b) => {
      if (b.type !== "submit") b.addEventListener("click", () => run(b.dataset.op as "get" | "delete"));
    });

    const html = document.documentElement;
    try {
      const saved = localStorage.getItem("distkv-theme");
      if (saved) html.dataset.theme = saved;
    } catch {
      /* storage unavailable */
    }
    $("theme-toggle").addEventListener("click", () => {
      html.dataset.theme = html.dataset.theme === "light" ? "dark" : "light";
      try {
        localStorage.setItem("distkv-theme", html.dataset.theme);
      } catch {
        /* ignore */
      }
      this.tp.draw();
      this.lat.draw();
      this.histChart.draw();
    });

    $("event-filters").addEventListener("click", (e) => {
      const b = (e.target as HTMLElement).closest<HTMLButtonElement>("[data-filter]");
      if (!b) return;
      this.eventFilter = b.dataset.filter!;
      $("event-filters").querySelectorAll(".chip").forEach((c) => c.classList.toggle("on", c === b));
      this.renderEvents();
    });
    $("events").addEventListener("click", (e) => {
      const li = (e.target as HTMLElement).closest<HTMLElement>("li[data-shard], li[data-incident]");
      if (li?.dataset.shard) this.locateShard(Number(li.dataset.shard));
      else if (li?.dataset.incident) this.openReport(Number(li.dataset.incident));
    });

    $("log-pause").addEventListener("click", () => {
      const b = $("log-pause");
      const paused = b.dataset.paused !== "1";
      b.dataset.paused = paused ? "1" : "0";
      b.textContent = paused ? "▶ Resume" : "⏸ Pause";
      this.logs.setPaused(paused);
    });
    $("log-clear").addEventListener("click", () => this.logs.clear());
    $("log-download").addEventListener("click", () => download(`distkv-${stamp()}.log`, this.logs.exportText(), "text/plain"));
    $("log-download-json").addEventListener("click", () => download(`distkv-${stamp()}.jsonl`, this.logs.exportJSON(), "application/x-ndjson"));

    // Keyboard shortcuts, ignored while typing.
    document.addEventListener("keydown", (e) => {
      const t = e.target as HTMLElement;
      if (e.metaKey || e.ctrlKey || e.altKey || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName) || t.isContentEditable) return;
      if (document.querySelector("dialog[open]")) return;
      const chaos = !$("control-card").hidden;
      const run = (name: string) => {
        if (!chaos) return toast("Chaos controls need the demo (make demo / make docker-demo) or the Pages simulation", "bad");
        this.scenario(name);
      };
      switch (e.key) {
        case "k": case "K": run("kill-leader"); break;
        case "p": case "P": run("partition"); break;
        case "l": case "L": run("lossy"); break;
        case "h": case "H": run("heal"); break;
        case "r": case "R": {
          const latest = this.tracker.incidents[0];
          if (latest) this.openReport(latest.id);
          else toast("No incidents yet");
          break;
        }
        case "a": case "A": $("alert-bell").click(); break;
        case "g": case "G": if (chaos) this.runGameDay(); break;
        case "m": case "M": if (chaos) this.toggleMonkey(); break;
        case "c": case "C": $("consistency-pill").hidden ? toast("No consistency probe in this mode") : $("consistency-pill").click(); break;
        case "/": e.preventDefault(); $("log-search").focus(); $("logs-card").scrollIntoView({ behavior: "smooth", block: "center" }); break;
        case "?": ($("shortcuts-dialog") as HTMLDialogElement).showModal(); break;
        default: return;
      }
    });
    $("shortcuts-btn").addEventListener("click", () => ($("shortcuts-dialog") as HTMLDialogElement).showModal());
    const openConsistency = () => {
      this.renderConsistency();
      ($("consistency-dialog") as HTMLDialogElement).showModal();
    };
    $("consistency-pill").addEventListener("click", openConsistency);
    $("kpis").addEventListener("click", (e) => {
      if ((e.target as HTMLElement).closest("[data-open='consistency']")) openConsistency();
    });
    $("consistency-close").addEventListener("click", () => ($("consistency-dialog") as HTMLDialogElement).close());
    $("consistency-dialog").addEventListener("click", (e) => {
      if (e.target === $("consistency-dialog")) ($("consistency-dialog") as HTMLDialogElement).close();
    });
  }

  // ---- incidents, alerts and reports ---------------------------------------------

  private onIncident(t: Transition): void {
    const inc = t.incident;
    const now = Date.now();
    const event = (kind: string, detail: string) => {
      this.events.unshift({ ts: now, kind, detail });
      this.renderEvents();
    };
    if (t.type === "opened") {
      const what = inc.triggerKind === "action" ? `Fault injected: ${inc.trigger}.` : `Detected: ${inc.trigger}.`;
      // Not sticky: the incident strip carries the state once the alert is gone.
      this.alerts.raise({ level: "critical", incidentId: inc.id, heal: !!this.demo?.demo, title: `Incident #${inc.id} opened`, body: `${what} Watching the cluster recover…` });
      this.alerts.setOpenIncidents(1);
      event("incident_open", `Incident #${inc.id} opened: ${inc.trigger}`);
      this.logs.add("incident", "error", `incident #${inc.id} opened`, [["trigger", inc.trigger], ["kind", inc.triggerKind]]);
    } else if (t.type === "escalated") {
      this.alerts.raise({ level: "warning", incidentId: inc.id, title: `Incident #${inc.id} escalated`, body: capitalize(t.detail) });
      event("incident_escalated", `Incident #${inc.id} escalated: ${t.detail}`);
      this.logs.add("incident", "warn", `incident #${inc.id} escalated`, [["detail", t.detail]]);
    } else {
      const s = summarize(inc);
      const parts = [`${inc.severity}`];
      if (s.clientFailed !== undefined) parts.push(`${s.clientFailed} failed client ops`);
      if (s.availability !== undefined) parts.push(`${(s.availability * 100).toFixed(3)}% available`);
      parts.push(s.failovers ? `${s.failovers} failovers (slowest ${s.failoversBounded ? "≤ " : ""}${fmtDuration(s.maxFailoverMs)})` : "no failovers");
      this.alerts.clearIncident(inc.id);
      this.alerts.raise({ level: "resolved", incidentId: inc.id, title: `Incident #${inc.id} resolved in ${fmtDuration(s.durationMs)}`, body: parts.join(" · ") });
      this.alerts.setOpenIncidents(0);
      event("incident_resolved", `Incident #${inc.id} resolved in ${fmtDuration(s.durationMs)}: ${parts.join(", ")}`);
      this.logs.add("incident", "info", `incident #${inc.id} resolved`, [
        ["duration", fmtDuration(s.durationMs)],
        ["severity", inc.severity],
        ["failovers", String(s.failovers)],
        ["client_failed", String(s.clientFailed ?? "n/a")],
      ]);
      if (this.reportId === inc.id) this.renderReport();
    }
    this.saveIncidents();
    this.renderIncidents();
    this.renderStrip();
  }

  private renderStrip(): void {
    const inc = this.tracker.active;
    const strip = $("incident-strip");
    strip.hidden = !inc;
    if (!inc) return;
    const s = this.snap;
    $("strip-title").textContent = `Incident #${inc.id} · ${capitalize(inc.trigger)}`;
    const sev = $("strip-sev");
    sev.textContent = inc.severity;
    sev.className = `sev ${inc.severity.toLowerCase()}`;
    $("strip-elapsed").textContent = fmtDuration(Date.now() - inc.openedAt);
    const issues = this.tracker.current;
    $("strip-detail").textContent = issues.length
      ? describe(issues)
      : inc.detectedAt === undefined
        ? "waiting for the fault to take effect…"
        : "all clear, confirming recovery…";
    if (s) {
      const up = s.nodes.filter((n) => n.reachable && !n.faults?.isolated).length;
      const frac = (s.healthy_shards + up) / (s.shards.length + s.nodes.length);
      ($("strip-progress") as HTMLElement).style.width = `${(frac * 100).toFixed(1)}%`;
      $("strip-progress-label").textContent = `${inc.healRequestedAt ? "healing · " : ""}${s.healthy_shards}/${s.shards.length} shards led · ${up}/${s.nodes.length} nodes connected`;
    }
    strip.classList.toggle("healing", !!inc.healRequestedAt);
  }

  private renderIncidents(): void {
    const list = this.tracker.incidents;
    const open = list.filter((i) => i.status === "open").length;
    $("incidents-sub").textContent = list.length ? `${list.length} recorded${open ? " · 1 open" : ""} · click a row for its report` : "none yet: inject a fault to open one";
    if (!list.length) {
      setHTML($("incidents"), `<tbody><tr><td class="muted empty">No incidents yet. Use the chaos lab (or press <kbd>K</kbd>) and the report appears here when the cluster heals.</td></tr></tbody>`);
      return;
    }
    const rows = list
      .map((inc) => {
        const s = summarize(inc);
        const status = inc.status === "open" ? `<span class="st open">● open</span>` : `<span class="st ok">resolved</span>`;
        const avail = s.availability !== undefined ? `${(s.availability * 100).toFixed(s.availability === 1 ? 0 : 3)}%` : "—";
        return `<tr data-incident="${inc.id}" class="${inc.status}">
          <td class="id">#${inc.id}</td><td>${status}</td><td><span class="sev ${inc.severity.toLowerCase()}">${inc.severity}</span></td>
          <td class="trigger">${esc(capitalize(inc.trigger))}</td>
          <td class="mono">${new Date(inc.openedAt).toLocaleTimeString([], { hour12: false })}</td>
          <td class="mono">${fmtDuration(s.durationMs)}</td>
          <td class="num">${s.failovers}${s.failovers ? ` <span class="muted">${s.failoversBounded ? "≤ " : "max "}${fmtDuration(s.maxFailoverMs)}</span>` : ""}</td>
          <td class="num ${(s.clientFailed ?? 0) > 0 ? "bad" : ""}">${s.clientFailed ?? "—"}</td>
          <td class="num">${avail}</td>
          <td class="actions"><button class="btn sm" data-view="${inc.id}">📄 Report</button><button class="btn sm" data-md="${inc.id}" title="Download Markdown">⬇ MD</button></td>
        </tr>`;
      })
      .join("");
    setHTML($("incidents"), `<thead><tr><th>#</th><th>status</th><th>sev</th><th>trigger</th><th>started</th><th>duration</th><th>failovers</th><th>failed ops</th><th>availability</th><th></th></tr></thead><tbody>${rows}</tbody>`);
  }

  private find(id: number): Incident | undefined {
    return this.tracker.incidents.find((i) => i.id === id);
  }

  private openReport(id: number): void {
    if (!this.find(id)) return toast(`Incident #${id} is no longer recorded`, "bad");
    this.reportGameDay = false;
    this.reportId = id;
    this.renderReport();
    const d = $("report-dialog") as HTMLDialogElement;
    if (!d.open) d.showModal();
  }

  private openGameDayReport(): void {
    if (!this.gd) return toast("No Game Day has run yet");
    this.reportGameDay = true;
    this.reportId = null;
    this.renderReport();
    const d = $("report-dialog") as HTMLDialogElement;
    if (!d.open) d.showModal();
  }

  private renderReport(): void {
    const body = $("report-body");
    if (this.reportGameDay && this.gd) {
      const scroll = body.scrollTop;
      $("report-heading").textContent = `Game Day report${this.gdRunning ? " (live)" : ""}`;
      body.innerHTML = gameDayHTML(this.gd, this.tracker.incidents);
      body.scrollTop = scroll;
      return;
    }
    const inc = this.reportId !== null ? this.find(this.reportId) : undefined;
    if (!inc) return;
    const scroll = body.scrollTop;
    $("report-heading").textContent = `Incident #${inc.id} report${inc.status === "open" ? " (live)" : ""}`;
    body.innerHTML = reportHTML(inc);
    body.scrollTop = scroll;
  }

  private wireIncidents(): void {
    const d = $("report-dialog") as HTMLDialogElement;
    const current = () => (this.reportId !== null ? this.find(this.reportId) : undefined);
    $("report-close").addEventListener("click", () => d.close());
    d.addEventListener("close", () => {
      this.reportId = null;
      this.reportGameDay = false;
    });
    // Links between reports (a Game Day report links its incidents).
    $("report-body").addEventListener("click", (e) => {
      const a = (e.target as HTMLElement).closest<HTMLElement>("[data-report]");
      if (!a) return;
      e.preventDefault();
      this.openReport(Number(a.dataset.report));
    });
    d.addEventListener("click", (e) => {
      if (e.target === d) d.close(); // backdrop
    });
    $("report-md").addEventListener("click", () => {
      if (this.reportGameDay && this.gd) return download(`distkv-gameday-${stamp()}.md`, gameDayMarkdown(this.gd, this.tracker.incidents), "text/markdown");
      const inc = current();
      if (inc) download(`distkv-incident-${inc.id}.md`, reportMarkdown(inc), "text/markdown");
    });
    $("report-html").addEventListener("click", () => {
      if (this.reportGameDay && this.gd) return download(`distkv-gameday-${stamp()}.html`, gameDayDocument(this.gd, this.tracker.incidents), "text/html");
      const inc = current();
      if (inc) download(`distkv-incident-${inc.id}.html`, reportDocument(inc), "text/html");
    });
    $("report-copy").addEventListener("click", async () => {
      const inc = current();
      const md = this.reportGameDay && this.gd ? gameDayMarkdown(this.gd, this.tracker.incidents) : inc ? reportMarkdown(inc) : null;
      if (!md) return;
      try {
        await navigator.clipboard.writeText(md);
        toast("Report copied as Markdown", "ok");
      } catch {
        toast("Clipboard unavailable: use ⬇ Markdown instead", "bad");
      }
    });
    $("report-print").addEventListener("click", () => {
      const inc = current();
      const doc = this.reportGameDay && this.gd ? gameDayDocument(this.gd, this.tracker.incidents) : inc ? reportDocument(inc) : null;
      if (!doc) return;
      // Print the standalone document from a hidden frame ("Save as PDF").
      const f = document.createElement("iframe");
      f.style.cssText = "position:fixed;width:0;height:0;border:0;right:0;bottom:0";
      f.srcdoc = doc;
      f.onload = () => {
        f.contentWindow?.focus();
        f.contentWindow?.print();
        setTimeout(() => f.remove(), 60_000);
      };
      document.body.appendChild(f);
    });
    $("strip-report").addEventListener("click", () => {
      if (this.tracker.active) this.openReport(this.tracker.active.id);
    });
    $("incidents").addEventListener("click", (e) => {
      const t = e.target as HTMLElement;
      const md = t.closest<HTMLElement>("[data-md]");
      if (md) {
        const inc = this.find(Number(md.dataset.md));
        if (inc) download(`distkv-incident-${inc.id}.md`, reportMarkdown(inc), "text/markdown");
        return;
      }
      const row = t.closest<HTMLElement>("[data-incident]");
      if (row) this.openReport(Number(row.dataset.incident));
    });
    $("incidents-export").addEventListener("click", () =>
      download(`distkv-incidents-${stamp()}.json`, JSON.stringify(this.tracker.incidents, null, 2), "application/json"),
    );
    $("incidents-clear").addEventListener("click", () => {
      const keep = this.tracker.active;
      this.tracker.incidents.length = 0;
      if (keep) this.tracker.incidents.push(keep);
      this.saveIncidents();
      this.renderIncidents();
    });
  }

  private storageKey(): string {
    return `distkv-incidents:${this.be.mode}`;
  }

  private loadIncidents(): Incident[] {
    try {
      const raw = localStorage.getItem(this.storageKey());
      return raw ? (JSON.parse(raw) as Incident[]) : [];
    } catch {
      return [];
    }
  }

  private saveIncidents(): void {
    this.lastSave = Date.now();
    try {
      localStorage.setItem(this.storageKey(), JSON.stringify(this.tracker.incidents));
    } catch {
      /* storage full or unavailable: incidents stay in memory */
    }
  }

  // ---- Game Day and Chaos Monkey ---------------------------------------------------

  private steady(): boolean {
    const s = this.snap;
    return (
      !!s &&
      !this.tracker.active &&
      s.healthy_shards === s.shards.length &&
      s.nodes.every((n) => n.reachable && !n.faults?.isolated && !n.faults?.drop_percent && !n.faults?.delay_ms)
    );
  }

  /** Sleeps in small steps so a Stop takes effect promptly. */
  private async pause(ms: number, cancelled: () => boolean): Promise<void> {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      if (cancelled()) throw new Error("cancelled");
      await sleep(Math.min(250, end - Date.now()));
    }
  }

  private async waitUntil(pred: () => boolean, ms: number, cancelled: () => boolean): Promise<boolean> {
    const end = Date.now() + ms;
    while (Date.now() < end) {
      if (pred()) return true;
      if (cancelled()) throw new Error("cancelled");
      await sleep(250);
    }
    return pred();
  }

  private async ensureSteady(cancelled: () => boolean, step: (t: string) => void): Promise<void> {
    step("waiting for a steady state");
    if (await this.waitUntil(() => this.steady(), 20_000, cancelled)) return;
    step("healing to reach a steady state");
    await this.scenario("heal");
    if (!(await this.waitUntil(() => this.steady(), 30_000, cancelled))) throw new Error("the cluster never reached a steady state");
  }

  private gdStep(text: string, frac?: number): void {
    $("gd-step").textContent = text;
    if (frac !== undefined) ($("gd-bar") as HTMLElement).style.width = `${(frac * 100).toFixed(1)}%`;
  }

  private async runGameDay(): Promise<void> {
    if (this.gdRunning) return;
    const ids = EXPERIMENTS.filter((e) => this.gdSelected.has(e.id)).map((e) => e.id);
    if (!ids.length) return toast("Select at least one experiment", "bad");
    if (this.monkey) this.toggleMonkey();
    this.gdRunning = true;
    this.gdCancel = false;
    const cancelled = () => this.gdCancel;
    const cons0 = this.demo?.consistency;
    const run: GameDayRun = {
      startedAt: Date.now(),
      mode: this.be.mode,
      results: ids.map((id) => ({ id, state: "queued", startedAt: 0, checks: [], notes: [] })),
    };
    this.gd = run;
    const total = EXPERIMENTS.filter((e) => ids.includes(e.id)).reduce((a, e) => a + e.seconds + 10, 0);
    let elapsedEst = 0;
    this.alerts.raise({ level: "info", title: "Game Day started", body: `${ids.length} experiment${ids.length === 1 ? "" : "s"}, about ${Math.max(1, Math.round(total / 60))} min. Each one is verified automatically.` });
    this.logs.add("gameday", "info", "game day started", [["experiments", ids.join(",")]]);
    this.renderGameDay();
    for (const r of run.results) {
      const e = EXPERIMENTS.find((x) => x.id === r.id)!;
      if (this.gdCancel) {
        r.state = "aborted";
        r.error = "not run: Game Day stopped";
        continue;
      }
      r.state = "running";
      r.startedAt = Date.now();
      this.renderGameDay();
      const step = (t: string) => this.gdStep(`${e.icon} ${e.name}: ${t}`, Math.min(0.99, (elapsedEst + (Date.now() - r.startedAt) / 1000) / total));
      const ctx: ExperimentCtx = {
        snap: () => this.snap,
        act: (node, act, why) => this.act(node, act, { why }),
        loss: async (node, pct, delay) => {
          await this.be.setFaults(node, { isolated: false, drop_percent: pct, delay_ms: delay });
          const text = `${pct}% packet loss + ${delay} ms delay on ${node}`;
          this.logAction(`you: ${text}`);
          this.tracker.action("chaos", text, this.demo);
        },
        scenario: (name) => this.scenario(name),
        wait: async (ms, what) => {
          const end = Date.now() + ms;
          while (Date.now() < end) {
            step(`${what} (${Math.ceil((end - Date.now()) / 1000)} s)`);
            await this.pause(Math.min(500, end - Date.now()), cancelled);
          }
        },
        step,
      };
      try {
        await this.ensureSteady(cancelled, step);
        const before = Math.max(0, ...this.tracker.incidents.map((i) => i.id));
        step("injecting the fault");
        r.notes = await e.run(ctx);
        step("healing");
        await this.scenario("heal");
        step("waiting for recovery");
        await this.waitUntil(() => !this.tracker.active, 45_000, cancelled);
        step("verifying linearizability of the epochs that span the fault");
        await this.pause(CONSISTENCY_GRACE_MS + 800, cancelled);
        const inc = this.tracker.incidents.filter((i) => i.id > before).sort((a, b) => a.id - b.id)[0];
        if (!inc) {
          r.checks = [{ name: "the fault had an effect", ok: false, detail: "no incident was observed" }];
        } else {
          r.incidentId = inc.id;
          r.checks = e.verify(inc, summarize(inc), r.notes);
        }
        r.state = r.checks.filter((c) => !c.info).every((c) => c.ok) ? "pass" : "fail";
      } catch (err) {
        r.state = "aborted";
        r.error = String(err).replace(/^Error: /, "");
        if (this.gdCancel) await this.scenario("heal");
      }
      r.endedAt = Date.now();
      elapsedEst += e.seconds + 10;
      const failing = r.checks.filter((c) => !c.ok && !c.info).map((c) => c.name);
      if (r.state === "pass") this.alerts.raise({ level: "resolved", incidentId: r.incidentId, title: `Hypothesis held: ${e.name}`, body: r.checks.filter((c) => !c.info).map((c) => `✓ ${c.name}`).join(" · ") });
      else if (r.state === "fail") this.alerts.raise({ level: "warning", incidentId: r.incidentId, title: `Hypothesis rejected: ${e.name}`, body: `Failed: ${failing.join(", ")}` });
      this.logs.add("gameday", r.state === "pass" ? "info" : r.state === "fail" ? "error" : "warn", `experiment ${r.state}: ${e.name}`, [
        ["incident", String(r.incidentId ?? "—")],
        ...r.checks.map((c) => [c.name.split(" ").join("_"), c.ok ? "ok" : "FAIL"] as [string, string]),
      ]);
      this.renderGameDay();
    }
    run.endedAt = Date.now();
    run.cancelled = this.gdCancel;
    const cons1 = this.demo?.consistency;
    if (cons0 && cons1) {
      run.verifiedOps = cons1.ops_checked - cons0.ops_checked;
      run.violations = cons1.violations_total - cons0.violations_total;
    }
    this.gdRunning = false;
    const sc = score(run);
    this.alerts.raise({
      level: sc.passed === sc.total ? "resolved" : "warning",
      title: `Game Day complete: grade ${sc.grade}`,
      body: `${sc.passed} of ${sc.total} hypotheses held · ${sc.pct.toFixed(0)}% of checks passed${run.verifiedOps !== undefined ? ` · ${run.verifiedOps.toLocaleString()} ops verified linearizable` : ""}`,
    });
    this.logs.add("gameday", "info", "game day finished", [["grade", sc.grade], ["passed", `${sc.passed}/${sc.total}`]]);
    this.saveGameDay();
    this.renderGameDay();
    if (!run.cancelled) this.openGameDayReport();
  }

  private renderGameDay(): void {
    const run = this.gd;
    $("gd-run").hidden = this.gdRunning;
    $("gd-cancel").hidden = !this.gdRunning;
    $("gd-report").hidden = !run || this.gdRunning;
    $("gd-progress").hidden = !this.gdRunning;
    const results = new Map(run?.results.map((r) => [r.id, r]) ?? []);
    const chip: Record<string, string> = { queued: "queued", running: "running…", pass: "✓ held", fail: "✗ rejected", aborted: "aborted", idle: "" };
    $("gd-list").innerHTML = EXPERIMENTS.map((e) => {
      const r = results.get(e.id);
      const st = r?.state ?? "idle";
      const checks =
        r && (st === "pass" || st === "fail" || st === "aborted")
          ? `<ul class="gd-checks">${r.checks.map((c) => `<li class="${c.info ? "info" : c.ok ? "ok" : "bad"}">${c.info ? "ℹ" : c.ok ? "✓" : "✗"} ${esc(c.name)} <span class="muted">${esc(c.detail)}</span></li>`).join("")}${r.error ? `<li class="bad">✗ ${esc(r.error)}</li>` : ""}</ul>`
          : "";
      return `<li class="gd-item st-${st}">
        <label class="gd-pick"><input type="checkbox" data-exp="${e.id}" ${this.gdSelected.has(e.id) ? "checked" : ""} ${this.gdRunning ? "disabled" : ""} aria-label="${esc(e.name)}"></label>
        <div class="gd-main">
          <div class="gd-title">${e.icon} <b>${esc(e.name)}</b> <span class="muted small">~${e.seconds + 10}s</span>
            ${st !== "idle" ? `<span class="gd-chip st-${st}">${chip[st]}</span>` : ""}
            ${r?.incidentId ? `<a href="#" class="gd-link" data-inc="${r.incidentId}">incident #${r.incidentId}</a>` : ""}</div>
          <div class="gd-hyp">${esc(e.hypothesis)}</div>
          ${checks}
        </div>
      </li>`;
    }).join("");
    if (run && !this.gdRunning) $("gd-report").textContent = `📄 Game Day report · ${score(run).grade}`;
    if (this.reportGameDay && ($("report-dialog") as HTMLDialogElement).open) this.renderReport();
  }

  private toggleMonkey(): void {
    this.monkey = !this.monkey;
    const b = $("monkey-btn");
    b.setAttribute("aria-pressed", String(this.monkey));
    b.classList.toggle("on", this.monkey);
    b.textContent = this.monkey ? "🐒 Chaos Monkey: ON" : "🐒 Chaos Monkey: off";
    if (this.monkey) {
      this.alerts.raise({ level: "warning", title: "Chaos Monkey released", body: "A random fault every few seconds (kill, stop, partition or packet loss), healed after a few seconds. Press M or the button to stop it." });
      this.logs.add("monkey", "warn", "chaos monkey on");
      this.monkeyLoop();
    } else {
      this.logs.add("monkey", "info", "chaos monkey off");
    }
  }

  private async monkeyLoop(): Promise<void> {
    const stopped = () => !this.monkey;
    const rnd = (a: number, b: number) => a + Math.random() * (b - a);
    try {
      while (this.monkey) {
        await this.ensureSteady(stopped, () => {});
        await this.pause(rnd(3000, 7000), stopped);
        const live = (this.snap?.nodes ?? []).filter((n) => n.reachable && !n.faults?.isolated);
        const n = live[Math.floor(Math.random() * live.length)];
        if (!n) continue;
        const r = Math.random();
        if (r < 0.35) await this.act(n.id, "kill", { why: "chaos monkey" });
        else if (r < 0.55) await this.act(n.id, "stop", { why: "chaos monkey" });
        else if (r < 0.8) await this.act(n.id, "isolate", { why: "chaos monkey" });
        else {
          await this.be.setFaults(n.id, { isolated: false, drop_percent: 30, delay_ms: 5 });
          this.logAction(`chaos monkey: 30% packet loss + 5ms delay on ${n.id}`);
          this.tracker.action("chaos", `30% packet loss + 5 ms delay on ${n.id} (chaos monkey)`, this.demo);
        }
        await this.pause(rnd(4000, 8000), stopped);
        await this.scenario("heal");
        await this.waitUntil(() => !this.tracker.active, 30_000, stopped);
      }
    } catch {
      /* stopped */
    }
    // The monkey may have struck a moment ago: heal until the cluster is steady.
    try {
      for (let i = 0; i < 3 && !this.monkey; i++) {
        await this.scenario("heal");
        if (await this.waitUntil(() => this.steady(), 10_000, () => this.monkey)) break;
      }
    } catch {
      /* switched back on: the new loop takes over */
    }
  }

  private wireGameDay(): void {
    $("gd-run").addEventListener("click", () => this.runGameDay());
    $("gd-cancel").addEventListener("click", () => {
      this.gdCancel = true;
      this.gdStep("stopping: healing the cluster…");
    });
    $("gd-report").addEventListener("click", () => this.openGameDayReport());
    $("monkey-btn").addEventListener("click", () => this.toggleMonkey());
    $("gd-list").addEventListener("change", (e) => {
      const cb = (e.target as HTMLElement).closest<HTMLInputElement>("[data-exp]");
      if (!cb) return;
      if (cb.checked) this.gdSelected.add(cb.dataset.exp!);
      else this.gdSelected.delete(cb.dataset.exp!);
    });
    $("gd-list").addEventListener("click", (e) => {
      const a = (e.target as HTMLElement).closest<HTMLElement>("[data-inc]");
      if (!a) return;
      e.preventDefault();
      this.openReport(Number(a.dataset.inc));
    });
  }

  private loadGameDay(): GameDayRun | null {
    try {
      const raw = localStorage.getItem(`distkv-gameday:${this.be.mode}`);
      const run = raw ? (JSON.parse(raw) as GameDayRun) : null;
      // A run interrupted by a reload can't continue.
      run?.results.forEach((r) => {
        if (r.state === "running" || r.state === "queued") r.state = "aborted";
      });
      return run;
    } catch {
      return null;
    }
  }

  private saveGameDay(): void {
    try {
      localStorage.setItem(`distkv-gameday:${this.be.mode}`, JSON.stringify(this.gd));
    } catch {
      /* ignore */
    }
  }

  // ---- node logs -------------------------------------------------------------------

  private logNoteShown = false;

  private async pollNodeLogs(): Promise<void> {
    if (this.pollingLogs || !this.snap || document.hidden) return;
    this.pollingLogs = true;
    try {
      const ids = this.snap.spec.members.map((m) => m.id);
      const chunks = await Promise.all(ids.map((id) => this.be.nodeLogs(id, this.logOffsets.get(id) ?? -1)));
      chunks.forEach((c, i) => {
        if (!c) return;
        this.logOffsets.set(ids[i], c.offset);
        for (const line of c.lines) this.logs.addNodeLine(ids[i], line);
      });
      if (chunks.every((c) => c === null) && this.demoPolled && !this.demo?.demo && !this.logNoteShown) {
        this.logNoteShown = true;
        $("log-note").innerHTML =
          "Node server logs stream here when the dashboard is served by the demo supervisor (<code>make demo</code> / <code>make docker-demo</code>). " +
          "With Docker Compose, use <code>docker compose -f deploy/docker-compose.yml logs -f</code>.";
      }
    } finally {
      this.pollingLogs = false;
    }
  }
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function legend(items: [string, string][]): string {
  return items.map(([name, c]) => `<span><i style="background:${c}"></i>${esc(name)}</span>`).join("");
}

// Silence unused-type lint for NodeSummary (kept for documentation).
export type { NodeSummary };

pickBackend().then((be) => new App(be).start());
