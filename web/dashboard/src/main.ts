import { Topology } from "../../../clients/typescript/src/routing";
import { TimeSeriesChart } from "./charts";
import { LiveBackend } from "./live";
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

// ---------------------------------------------------------------------------
// Backend selection: ?sim, <meta distkv-mode=sim>, ?api=URL, or same origin.

async function pickBackend(): Promise<Backend> {
  const params = new URLSearchParams(location.search);
  const meta = document.querySelector<HTMLMetaElement>('meta[name="distkv-mode"]')?.content;
  if (params.has("sim") || meta === "sim" || location.protocol === "file:") return new SimBackend();
  const api = params.get("api")?.replace(/\/+$/, "") ?? "";
  try {
    const r = await fetch(`${api}/v1/cluster`, { signal: AbortSignal.timeout(2500) });
    if (r.ok) return new LiveBackend(api);
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
  action: ["🧪", ""],
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

  constructor(private readonly be: Backend) {
    this.tp = new TimeSeriesChart($("chart-throughput") as HTMLCanvasElement, { minMax: 10 });
    this.lat = new TimeSeriesChart($("chart-latency") as HTMLCanvasElement, { log: true, unit: "ms", format: (v) => (v >= 1 ? v.toFixed(0) : v.toFixed(1)) });
    this.histChart = new TimeSeriesChart($("chart-history") as HTMLCanvasElement, { minMax: 10 });
    $("tp-legend").innerHTML = legend([["reads/s", PALETTE[1]], ["writes/s", PALETTE[0]], ["errors/s", "#ff5c7a"]]);
    $("lat-legend").innerHTML = legend([["p50", PALETTE[3]], ["p99", PALETTE[4]]]);
  }

  start(): void {
    if (this.be.mode === "sim") $("sim-banner").hidden = false;
    this.be.subscribe((s) => this.onSnapshot(s), (c) => this.onConn(c));
    setInterval(() => this.pollDemo(), 2000);
    // History needs to know whether a historian is running, so it waits
    // for the first demo-info response.
    this.pollDemo().then(() => this.pollHistory());
    setInterval(() => this.pollHistory(), 10_000);
    this.wireControls();
  }

  private onConn(c: ConnState): void {
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
    this.diff();
    this.render();
  }

  private diff(): void {
    const a = this.prev;
    const b = this.snap!;
    if (!a) return;
    const push = (kind: string, detail: string) => {
      this.events.unshift({ ts: b.ts, kind, detail });
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
    const list = this.events.slice(0, 120);
    $("events").innerHTML = list.length
      ? list
          .map((e) => {
            const [icon, cls] = EVENT_STYLE[e.kind] ?? ["•", ""];
            return `<li class="${cls}"><time>${new Date(e.ts).toLocaleTimeString()}</time><span>${icon}</span><span>${esc(e.detail)}</span></li>`;
          })
          .join("")
      : `<li><time></time><span>✨</span><span class="muted">All quiet. Try the chaos buttons.</span></li>`;
  }

  // ---- demo / historian polling -----------------------------------------------

  private async pollDemo(): Promise<void> {
    this.demo = await this.be.demoInfo();
    const isDemo = !!this.demo?.demo;
    $("mode-pill").hidden = !isDemo;
    $("control-card").hidden = !isDemo;
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

  private logAction(detail: string): void {
    this.events.unshift({ ts: Date.now(), kind: "action", detail });
    this.renderEvents();
  }

  private async act(id: string, act: string): Promise<void> {
    try {
      if (act === "isolate") await this.be.setFaults(id, { isolated: true, drop_percent: 0, delay_ms: 0 });
      else if (act === "heal") await this.be.setFaults(id, { isolated: false, drop_percent: 0, delay_ms: 0 });
      else await this.be.nodeAction(id, act as "kill" | "stop" | "start");
      const verb = { kill: "SIGKILL →", stop: "graceful stop →", start: "starting", isolate: "partitioning", heal: "healing" }[act] ?? act;
      this.logAction(`you: ${verb} ${id}`);
      toast(`${verb} ${id}`, act === "kill" ? "bad" : "ok");
      setTimeout(() => this.pollDemo(), 300);
    } catch (e) {
      toast(String(e), "bad");
    }
  }

  private async scenario(name: string): Promise<void> {
    const s = this.snap;
    if (!s) return;
    const live = s.nodes.filter((n) => n.reachable && !n.faults?.isolated);
    const pick = <T>(xs: T[]) => xs[Math.floor(Math.random() * xs.length)];
    switch (name) {
      case "kill-leader": {
        const busiest = [...live].sort((a, b) => b.shards_led - a.shards_led)[0];
        if (busiest) await this.act(busiest.id, "kill");
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
        toast(`30% packet loss on ${n.id}`);
        break;
      }
      case "rolling": {
        toast("Rolling restart: graceful stop → start, one node at a time");
        for (const n of s.spec.members) {
          await this.act(n.id, "stop");
          await sleep(2500);
          await this.act(n.id, "start");
          await sleep(3500);
        }
        toast("Rolling restart complete", "ok");
        break;
      }
      case "heal": {
        for (const n of s.nodes) {
          if (!n.reachable) await this.be.nodeAction(n.id, "start");
          else if (n.faults && (n.faults.isolated || n.faults.drop_percent || n.faults.delay_ms))
            await this.be.setFaults(n.id, { isolated: false, drop_percent: 0, delay_ms: 0 });
        }
        this.logAction("you: healed everything");
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
  }
}

function legend(items: [string, string][]): string {
  return items.map(([name, c]) => `<span><i style="background:${c}"></i>${esc(name)}</span>`).join("");
}

// Silence unused-type lint for NodeSummary (kept for documentation).
export type { NodeSummary };

pickBackend().then((be) => new App(be).start());
