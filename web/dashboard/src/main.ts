import { Topology } from "../../../clients/typescript/src/routing";
import { AlertCenter } from "./alerts";
import { TimeSeriesChart } from "./charts";
import { describe, IncidentTracker, summarize, type Incident, type Transition } from "./incidents";
import { LiveBackend } from "./live";
import { LogConsole, type Level } from "./logs";
import { fmtDuration, REPORT_CSS, reportDocument, reportHTML, reportMarkdown } from "./report";
import { RingView } from "./ring";
import { SimBackend } from "./sim";
import type { Backend, ClusterStatus, ConnState, DemoInfo, HistorianEvent, NodeSummary } from "./types";

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
  action: ["🧪", ""],
  incident_open: ["🚨", "bad incident"],
  incident_escalated: ["⚠️", "warn incident"],
  incident_resolved: ["✅", "ok incident"],
};

const EVENT_FILTERS: Record<string, (kind: string) => boolean> = {
  all: () => true,
  faults: (k) => ["node_down", "node_up", "partitioned", "healed", "shard_unavailable", "shard_recovered", "quorum_lost"].includes(k),
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

  constructor(private readonly be: Backend) {
    const style = document.createElement("style");
    style.textContent = REPORT_CSS;
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
    setInterval(() => this.renderStrip(), 250);
    this.renderIncidents();
    // History needs to know whether a historian is running, so it waits
    // for the first demo-info response.
    this.pollDemo().then(() => this.pollHistory());
    setInterval(() => this.pollHistory(), 10_000);
    this.wireControls();
    this.wireIncidents();
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
    this.render();
    this.renderIncidents();
    this.renderStrip();
    if (this.reportId !== null && this.tracker.active?.id === this.reportId) this.renderReport();
    if (this.tracker.active && Date.now() - this.lastSave > 10_000) this.saveIncidents();
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
    $("kpis").innerHTML = [
      kpi("Throughput", `${fmtNum(ops)}<small>ops/s</small>`, `${fmtNum(s.load.reads_per_sec)} reads · ${fmtNum(s.load.writes_per_sec)} writes`, "", "rgba(0,212,255,.3)"),
      kpi("p99 latency", fmtMs(s.load.p99_ms), `p50 ${fmtMs(s.load.p50_ms)} · SLO &lt; 10ms`, p99cls, "rgba(255,181,71,.25)"),
      kpi("Availability", `${(avail * 100).toFixed(avail === 1 ? 0 : 3)}%`, availSub, avail >= 0.999 ? "good" : "bad", "rgba(46,229,157,.25)"),
      kpi("Shards", `${s.healthy_shards}<small>/ ${s.shards.length}</small>`, "with an elected leader", s.healthy_shards === s.shards.length ? "good" : "bad"),
      kpi("Nodes", `${up}<small>/ ${s.nodes.length}</small>`, `RF=${s.spec.replication_factor} · tolerates ${Math.floor((s.spec.replication_factor - 1) / 2)} failure`, up === s.nodes.length ? "good" : "warn"),
      kpi("Leader moves", String(this.leaderChanges.length), "last 5 minutes", "", "rgba(255,92,168,.25)"),
    ].join("");
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
      card.innerHTML = `
        <div class="node-head"><span class="dot"></span><strong>${esc(n.id)}</strong>${state}</div>
        <div class="node-meta">${esc(n.addr.replace(/^https?:\/\//, ""))}${proc && !down ? ` · pid ${proc.pid}` : ""}${n.uptime_seconds && !down ? ` · up ${fmtDur(n.uptime_seconds)}` : ""}</div>
        <div class="node-stats">
          <div><b>${n.shards_led}</b><span>leads</span></div>
          <div><b>${l ? fmtNum(l.reads_per_sec + l.writes_per_sec) : "—"}</b><span>ops/s</span></div>
          <div><b>${l ? fmtMs(l.p99_ms) : "—"}</b><span>p99</span></div>
        </div>
        <div class="leadbar" title="share of shard leaderships"><i style="width:${(n.shards_led / maxLead) * 100}%"></i></div>
        ${controls}`;
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
    let html = `<thead><tr><th>shard</th>${nodes.map((n) => `<th class="node-col" style="--node:${this.color(n)}">${n}</th>`).join("")}<th>term</th><th>commit</th><th>keys</th></tr></thead><tbody>`;
    for (const sh of s.shards) {
      const flash = this.flash.includes(sh.shard) ? ' class="flash"' : "";
      html += `<tr${flash}><td class="shard">${sh.shard}</td>`;
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
    }
    $("matrix").innerHTML = html + "</tbody>";
  }

  private renderEvents(): void {
    const keep = EVENT_FILTERS[this.eventFilter] ?? EVENT_FILTERS.all;
    const list = this.events.filter((e) => keep(e.kind)).slice(0, 150);
    $("events").innerHTML = list.length
      ? list
          .map((e) => {
            const [icon, cls] = EVENT_STYLE[e.kind] ?? ["•", ""];
            const sh = /shard (\d+)/.exec(e.detail)?.[1];
            const inc = /^incident #(\d+)/i.exec(e.detail)?.[1];
            const data = sh !== undefined ? ` data-shard="${sh}"` : inc !== undefined ? ` data-incident="${inc}"` : "";
            return `<li class="${cls}${data ? " clickable" : ""}"${data}><time>${new Date(e.ts).toLocaleTimeString([], { hour12: false })}</time><span>${icon}</span><span>${esc(e.detail)}</span></li>`;
          })
          .join("")
      : `<li><time></time><span>✨</span><span class="muted">${this.events.length ? "Nothing matches this filter yet." : "All quiet. Try the chaos buttons."}</span></li>`;
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
    document.querySelector<HTMLElement>("#incident-strip [data-scenario]")!.hidden = !isDemo;
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

  private historyEventsLoaded = false;

  private async pollHistory(): Promise<void> {
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
        for (const n of s.nodes) {
          if (!n.reachable) await this.be.nodeAction(n.id, "start");
          else if (n.faults && (n.faults.isolated || n.faults.drop_percent || n.faults.delay_ms))
            await this.be.setFaults(n.id, { isolated: false, drop_percent: 0, delay_ms: 0 });
        }
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
        b.disabled = true;
        this.act(b.dataset.node!, b.dataset.act!);
      }
    });
    document.querySelectorAll<HTMLButtonElement>("[data-scenario]").forEach((b) =>
      b.addEventListener("click", async () => {
        b.disabled = true;
        await this.scenario(b.dataset.scenario!);
        b.disabled = false;
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
        case "/": e.preventDefault(); $("log-search").focus(); $("logs-card").scrollIntoView({ behavior: "smooth", block: "center" }); break;
        case "?": ($("shortcuts-dialog") as HTMLDialogElement).showModal(); break;
        default: return;
      }
    });
    $("shortcuts-btn").addEventListener("click", () => ($("shortcuts-dialog") as HTMLDialogElement).showModal());
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
      $("incidents").innerHTML = `<tbody><tr><td class="muted empty">No incidents yet. Use the chaos lab (or press <kbd>K</kbd>) and the report appears here when the cluster heals.</td></tr></tbody>`;
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
    $("incidents").innerHTML = `<thead><tr><th>#</th><th>status</th><th>sev</th><th>trigger</th><th>started</th><th>duration</th><th>failovers</th><th>failed ops</th><th>availability</th><th></th></tr></thead><tbody>${rows}</tbody>`;
  }

  private find(id: number): Incident | undefined {
    return this.tracker.incidents.find((i) => i.id === id);
  }

  private openReport(id: number): void {
    if (!this.find(id)) return toast(`Incident #${id} is no longer recorded`, "bad");
    this.reportId = id;
    this.renderReport();
    const d = $("report-dialog") as HTMLDialogElement;
    if (!d.open) d.showModal();
  }

  private renderReport(): void {
    const inc = this.reportId !== null ? this.find(this.reportId) : undefined;
    if (!inc) return;
    const body = $("report-body");
    const scroll = body.scrollTop;
    $("report-heading").textContent = `Incident #${inc.id} report${inc.status === "open" ? " (live)" : ""}`;
    body.innerHTML = reportHTML(inc);
    body.scrollTop = scroll;
  }

  private wireIncidents(): void {
    const d = $("report-dialog") as HTMLDialogElement;
    const current = () => (this.reportId !== null ? this.find(this.reportId) : undefined);
    $("report-close").addEventListener("click", () => d.close());
    d.addEventListener("close", () => (this.reportId = null));
    d.addEventListener("click", (e) => {
      if (e.target === d) d.close(); // backdrop
    });
    $("report-md").addEventListener("click", () => {
      const inc = current();
      if (inc) download(`distkv-incident-${inc.id}.md`, reportMarkdown(inc), "text/markdown");
    });
    $("report-html").addEventListener("click", () => {
      const inc = current();
      if (inc) download(`distkv-incident-${inc.id}.html`, reportDocument(inc), "text/html");
    });
    $("report-copy").addEventListener("click", async () => {
      const inc = current();
      if (!inc) return;
      try {
        await navigator.clipboard.writeText(reportMarkdown(inc));
        toast("Report copied as Markdown", "ok");
      } catch {
        toast("Clipboard unavailable: use ⬇ Markdown instead", "bad");
      }
    });
    $("report-print").addEventListener("click", () => {
      const inc = current();
      if (!inc) return;
      // Print the standalone document from a hidden frame ("Save as PDF").
      const f = document.createElement("iframe");
      f.style.cssText = "position:fixed;width:0;height:0;border:0;right:0;bottom:0";
      f.srcdoc = reportDocument(inc);
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

  // ---- node logs -------------------------------------------------------------------

  private logNoteShown = false;

  private async pollNodeLogs(): Promise<void> {
    if (this.pollingLogs || !this.snap) return;
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
