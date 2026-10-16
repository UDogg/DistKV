/**
 * The newcomer's layer: a welcome dialog that explains what the dashboard
 * is and why it matters, a spotlight tour of the panels, plain-language
 * tooltips on anything marked data-explain, and a coach that narrates what
 * just happened after each fault and recovery. No jargon is required to
 * follow the demo; the jargon is still there, explained, for those who want it.
 */

export interface Explanation {
  t: string; // title
  b: string; // body (trusted HTML: static strings from this file only)
}

/** Plain-language explanations, keyed by the element's data-explain value. */
export const EXPLAIN: Record<string, Explanation> = {
  // Header pills
  "pill-health": { t: "Overall health", b: "<b>Green</b>: everything works. <b>Amber</b>: a server is down, but every piece of data is still being served by the others. <b>Red</b>: some data briefly has no leader while the servers vote for a new one, usually for less than a second." },
  "pill-conn": { t: "Live connection", b: "Whether this page is receiving live updates from the cluster. In simulation mode, the cluster runs right here in your browser." },
  "pill-mode": { t: "Simulation or demo", b: "<b>SIM</b>: a faithful model of the cluster running inside your browser, so nothing to install. <b>DEMO</b>: real DistKV server processes on the machine serving this page." },
  "pill-consistency": { t: "Correctness checker", b: "An independent checker records every read and write and proves the answers were correct: no stale data, no lost writes, no contradictions. Computer scientists call this <i>linearizability</i>. Click for the evidence." },
  alerts: { t: "Alerts", b: "A red alert pops up when something breaks, and a green one when the cluster has healed itself. The bell keeps the history." },
  guide: { t: "Guide", b: "Reopen the introduction, take the tour again, or turn the live explanations on or off." },

  // KPI tiles
  "kpi-throughput": { t: "Throughput: how busy it is", b: "Requests handled per second. Each request is someone saving (<i>write</i>) or looking up (<i>read</i>) a piece of data, like loading a profile or liking a post." },
  "kpi-latency": { t: "Speed (p99 latency)", b: "99 out of every 100 requests finish within this time. The goal is under <b>10 ms</b>, about one thirtieth of a blink. Watch it stay low even while you break things." },
  "kpi-availability": { t: "Availability: did requests succeed?", b: "The share of requests that worked. Big services aim for <b>99.9%</b> or better, which is less than 9 hours of trouble a year. The point of this whole system is to keep this number up while servers fail." },
  "kpi-shards": { t: "Shards with a leader", b: "The data is split into slices called <b>shards</b>, like a library spread over many shelves. Each shard needs one <b>leader</b> server to accept changes. When every shard has a leader, all data can be read and written." },
  "kpi-nodes": { t: "Servers up", b: "How many of the servers (<i>nodes</i>) are running. Every shard is copied to 3 servers (<i>RF=3</i>, the replication factor), so any one server can fail without losing or blocking any data." },
  "kpi-leader-moves": { t: "Leader moves", b: "How often a shard got a new leader in the last 5 minutes. A move happens after a failure (an election) or when the cluster rebalances work. It's healthy: it's the system healing itself." },
  "kpi-linearizable": { t: "Proven correct", b: "The number of operations an independent checker has verified as correct. Even across crashes and network cuts, every answer must be one that could really have happened. One wrong answer would turn this red." },

  // Charts
  "chart-throughput": { t: "Throughput over time", b: "Reads (lookups), writes (saves) and errors per second over the last few minutes. During a failure, look for a tiny dip, not a crash to zero." },
  "chart-latency": { t: "Speed over time", b: "How long requests take. <b>p50</b> is the typical request, <b>p99</b> the slowest 1%. A brief bump during an election is normal; it should settle within seconds." },

  // Chaos lab
  chaos: { t: "Chaos lab", b: "Break the cluster on purpose and watch it recover. Real companies do exactly this (\"chaos engineering\") to prove their systems survive failures <i>before</i> real failures happen." },
  "sc-kill-leader": { t: "Kill the busiest leader", b: "Pulls the plug on the server that's leading the most shards: it stops instantly, mid-sentence. The other copies notice the silence, vote, and pick new leaders in about a second." },
  "sc-partition": { t: "Partition a server", b: "Cuts one server off the network. It keeps running but can't talk to anyone, like a data center losing its internet link. The rest carry on without it." },
  "sc-lossy": { t: "30% packet loss", b: "Makes one server's network flaky: 30% of its messages vanish, like terrible Wi-Fi. The system re-sends what's lost, so answers stay correct; things just get a little slower." },
  "sc-rolling": { t: "Rolling restart", b: "Restarts every server one after another, the way companies install updates without any downtime. Availability should stay near 100% throughout." },
  "sc-heal": { t: "Heal everything", b: "Restarts stopped servers and repairs all network faults. The incident closes once every shard has a leader and every server is healthy again." },
  "load-rate": { t: "Generated load", b: "How many fake users' requests per second are hitting the cluster. Turn it up to see how it copes under pressure." },
  "load-ratio": { t: "Read ratio", b: "The mix of lookups versus saves. Most real apps read far more than they write (80% reads is typical)." },

  // Per-node actions
  "act-kill": { t: "kill -9 (crash it)", b: "<b>kill -9</b> sends <b>SIGKILL</b>, the computer command for \"stop right now, no goodbyes\". It's the same as a power cut or a crash: the server gets no chance to tidy up. Nothing is lost, because every change is already saved on a majority of servers." },
  "act-stop": { t: "Graceful stop", b: "Asks the server to shut down politely. It hands its leader jobs to other servers first, like a planned maintenance reboot. This should cause no failed requests at all." },
  "act-start": { t: "Start", b: "Turns the server back on. It downloads everything it missed while it was off, then rejoins as a normal copy." },
  "act-isolate": { t: "Partition (cut the network)", b: "Disconnects this server from all the others. It's alive but alone. It can't make changes on its own, because it can't win a majority vote. That rule is what prevents two halves of a split cluster from disagreeing (\"split brain\")." },
  "act-heal": { t: "Heal the partition", b: "Reconnects the server. It catches up on what it missed and rejoins." },
  node: { t: "A server (node)", b: "One computer in the cluster. <b>Leads</b> is how many shards it's currently the leader for; the bar shows its share of the leadership work. The cluster spreads leaders evenly on its own." },

  // Game Day and Chaos Monkey
  gameday: { t: "Game Day", b: "A fire drill for software: a scripted series of disasters, each with a prediction (\"the cluster keeps serving with no errors\") that's checked automatically. You get a graded report at the end." },
  monkey: { t: "Chaos Monkey", b: "Lets a \"monkey\" break random things every few seconds (crashes, network cuts, packet loss) and heal them a little later. It's named after Netflix's real tool, which breaks its own production servers on purpose." },

  // Panels
  incidents: { t: "Incidents and reports", b: "Every disaster, whether you caused it or it happened on its own, becomes an <b>incident</b>: when it started, how bad it was, how long recovery took. Open a report for the full story, and download it as a document." },
  nodes: { t: "Servers", b: "Each card is one server. You can crash it, shut it down politely, or cut its network, and watch how the others take over." },
  ring: { t: "Where data lives", b: "Every piece of data has a name (a <i>key</i>, like <code>user:42</code>). A formula turns the name into a spot on this ring, and the spot decides which shard stores it. Everyone computes the same answer, so no central directory is needed. Type a key to try it." },
  kv: { t: "Try it yourself", b: "Save (<b>PUT</b>), look up (<b>GET</b>) or delete a piece of data, just like an app would. The answer shows which shard stored it and which server handled it." },
  shards: { t: "The shard map", b: "Rows are shards (slices of the data); columns are servers. <b>★</b> = that shard's leader, <b>●</b> = a backup copy, <b>✕</b> = a copy that's offline. Crash a server and watch the ★s jump to other columns. <b>Term</b> counts elections; <b>commit</b> counts saved changes." },
  events: { t: "What happened, when", b: "A running diary of the cluster: elections, crashes, recoveries and the things you did. Click an event to see where it happened." },
  history: { t: "Long-term history", b: "Traffic over a longer period, saved to disk by a separate program so it survives reloads and restarts." },
  logs: { t: "Logs", b: "The raw diary the servers write as they work. Engineers read these to understand what happened during an incident. Search and filter them like a pro." },
};

const STORAGE = { welcome: "distkv-welcome-v1", coach: "distkv-coach" };

function get(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function set(key: string, value: string): void {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* storage unavailable: the guide still works for this visit */
  }
}

const reducedMotion = () => matchMedia("(prefers-reduced-motion: reduce)").matches;

// ---------------------------------------------------------------------------
// Tooltips

/**
 * One tooltip for the whole page, shown for any element with data-explain:
 * on mouse hover (after a short delay), on keyboard focus, or on a tap of an
 * ⓘ button (touch screens have no hover).
 */
export class Explainer {
  private readonly tip: HTMLElement;
  private target: HTMLElement | null = null;
  private timer: ReturnType<typeof setTimeout> | undefined;

  constructor(private readonly dict: Record<string, Explanation> = EXPLAIN) {
    this.tip = document.createElement("div");
    this.tip.className = "xtip";
    this.tip.id = "xtip";
    this.tip.setAttribute("role", "tooltip");
    this.tip.hidden = true;
    document.body.appendChild(this.tip);

    const find = (e: Event) => (e.target as HTMLElement | null)?.closest?.<HTMLElement>("[data-explain]") ?? null;
    document.addEventListener("pointerover", (e) => {
      if (e.pointerType !== "mouse") return;
      const el = find(e);
      if (el === this.target) return;
      clearTimeout(this.timer);
      if (!el) return this.hide();
      this.timer = setTimeout(() => this.show(el), this.target ? 0 : 280);
    });
    document.addEventListener("pointerleave", () => this.hide());
    document.addEventListener("focusin", (e) => {
      const el = find(e);
      if (el && el.matches(":focus-visible")) this.show(el);
      else this.hide();
    });
    // ⓘ buttons toggle on click, which is how touch screens reach them.
    document.addEventListener("click", (e) => {
      const i = (e.target as HTMLElement).closest<HTMLElement>(".ex-i");
      if (i) {
        e.preventDefault();
        if (this.target === i && !this.tip.hidden) this.hide();
        else this.show(i);
      } else if (!(e.target as HTMLElement).closest(".xtip")) {
        this.hide();
      }
    });
    document.addEventListener("keydown", (e) => e.key === "Escape" && this.hide());
    addEventListener("scroll", () => this.hide(), { passive: true });
  }

  show(el: HTMLElement): void {
    const x = this.dict[el.dataset.explain!];
    if (!x || !el.isConnected) return;
    this.target = el;
    this.tip.innerHTML = `<b>${x.t}</b><span>${x.b}</span>`;
    this.tip.hidden = false;
    el.setAttribute("aria-describedby", "xtip");
    const r = el.getBoundingClientRect();
    const w = Math.min(320, innerWidth - 24);
    this.tip.style.width = `${w}px`;
    const h = this.tip.offsetHeight;
    const below = r.bottom + 10 + h < innerHeight || r.top < h + 10;
    const left = Math.min(Math.max(12, r.left + r.width / 2 - w / 2), innerWidth - w - 12);
    this.tip.style.left = `${left}px`;
    this.tip.style.top = `${below ? r.bottom + 10 : r.top - h - 10}px`;
    this.tip.classList.toggle("above", !below);
  }

  hide(): void {
    clearTimeout(this.timer);
    this.target?.removeAttribute("aria-describedby");
    this.target = null;
    this.tip.hidden = true;
  }
}

// ---------------------------------------------------------------------------
// Coach: plain-language narration of what just happened

export interface Story {
  icon: string;
  title: string;
  body: string; // trusted HTML built by the caller from escaped values
  tone?: "info" | "bad" | "ok";
  action?: { label: string; run: () => void };
}

export class Coach {
  private readonly el: HTMLElement;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private enabledFlag: boolean;

  constructor() {
    this.enabledFlag = get(STORAGE.coach) !== "off";
    this.el = document.createElement("aside");
    this.el.className = "coach";
    this.el.setAttribute("aria-live", "polite");
    this.el.hidden = true;
    document.body.appendChild(this.el);
    this.el.addEventListener("pointerenter", () => this.el.classList.add("hold"));
    this.el.addEventListener("pointerleave", () => this.el.classList.remove("hold"));
    this.el.addEventListener("animationend", (e) => {
      if (e.animationName === "coach-timer") this.dismiss();
    });
  }

  get enabled(): boolean {
    return this.enabledFlag;
  }

  set enabled(on: boolean) {
    this.enabledFlag = on;
    set(STORAGE.coach, on ? "on" : "off");
    if (!on) this.dismiss();
  }

  say(s: Story): void {
    if (!this.enabledFlag) return;
    clearTimeout(this.timer);
    this.el.className = `coach ${s.tone ?? "info"}`;
    this.el.innerHTML = `
      <div class="coach-icon" aria-hidden="true">${s.icon}</div>
      <div class="coach-main">
        <div class="coach-kicker">What just happened</div>
        <b class="coach-title">${s.title}</b>
        <p>${s.body}</p>
        <div class="coach-actions">
          ${s.action ? `<button class="btn sm primary" data-coach="action">${s.action.label}</button>` : ""}
          <button class="btn sm" data-coach="close">Got it</button>
          <button class="linkish" data-coach="mute">Stop explaining</button>
        </div>
      </div>
      <i class="coach-timer" aria-hidden="true"></i>`;
    this.el.querySelector<HTMLElement>("[data-coach='close']")!.onclick = () => this.dismiss();
    this.el.querySelector<HTMLElement>("[data-coach='mute']")!.onclick = () => {
      this.enabled = false;
      document.dispatchEvent(new CustomEvent("distkv-coach", { detail: false }));
    };
    const act = this.el.querySelector<HTMLElement>("[data-coach='action']");
    if (act && s.action) act.onclick = () => (s.action!.run(), this.dismiss());
    this.el.hidden = false;
    // Without animations, the timer bar can't end the card: use a timeout.
    if (reducedMotion()) this.timer = setTimeout(() => this.el.matches(":hover") || this.dismiss(), 16000);
  }

  dismiss(): void {
    clearTimeout(this.timer);
    this.el.hidden = true;
  }
}

// ---------------------------------------------------------------------------
// Spotlight tour

export interface TourStep {
  target: string; // CSS selector; hidden or missing targets are skipped
  title: string;
  body: string;
  /** Used instead of body when the chaos controls aren't available. */
  bodyNoChaos?: string;
}

export const TOUR: TourStep[] = [
  { target: ".status-strip", title: "1 · The health light", body: "Green means everything works. If you break something it turns amber or red, then back to green by itself. That self-healing is the whole point." },
  { target: "#kpis", title: "2 · Vital signs", body: "How busy the cluster is, how fast it answers (goal: under 10 ms), and the share of requests that succeed (goal: 99.9%+). The last tile counts answers an independent checker has <b>proven correct</b>." },
  { target: "#control-card", title: "3 · The chaos lab", body: "This is where you break things: crash a server, cut its network, make its network flaky. Every button is safe to press, and hovering one explains it in plain words." },
  { target: "#nodes-card", title: "4 · The servers", body: "Each card is one computer. <b>Leads</b> counts the slices of data it's in charge of. Use <b>kill -9</b> to crash one instantly, or <b>Graceful stop</b> to shut it down politely." },
  { target: "#shards-card", title: "5 · Who's in charge of what", body: "The data is cut into slices (<b>shards</b>), each copied to 3 servers. <b>★</b> marks the leader of each slice. Crash a server and watch its ★s jump to other servers within about a second." },
  { target: "#ring-card", title: "6 · Where your data lives", body: "Type any name, like <code>user:42</code>, and see which shard and which server would store it. Every computer works this out with the same formula, so nobody has to ask." },
  { target: "#gameday-card", title: "7 · Game Day and Chaos Monkey", body: "<b>Game Day</b> runs a series of disaster drills and grades the cluster. <b>Chaos Monkey</b> breaks random things until you stop it. Both are what real reliability teams do." },
  { target: "#incidents-card", title: "8 · Incidents and reports", body: "Every failure becomes an incident with an alert when it starts, another when it heals, and a report you can download: how long recovery took, and how many requests (if any) failed." },
  { target: "#alert-bell", title: "9 · Your turn", body: "Alerts land here. Now press <b>💥 Kill busiest leader</b> and watch: the health light flickers, new leaders are elected, and a green \"resolved\" alert arrives a moment later.", bodyNoChaos: "Alerts land here: a red one if a server fails, a green one when the cluster has healed itself. To break things on purpose, open the simulation (this dashboard on GitHub Pages) or run <code>make demo</code>." },
];

export class Tour {
  private steps: TourStep[] = [];
  private i = 0;
  private readonly root: HTMLElement;
  private readonly spot: HTMLElement;
  private readonly pop: HTMLElement;
  private onReflow = () => this.place();
  private onKey = (e: KeyboardEvent) => {
    if (e.key === "Escape") this.end();
    else if (e.key === "ArrowRight") this.go(this.i + 1);
    else if (e.key === "ArrowLeft") this.go(this.i - 1);
    else return;
    e.preventDefault();
    e.stopImmediatePropagation();
  };
  onEnd: () => void = () => {};

  constructor(private readonly all: TourStep[] = TOUR) {
    this.root = document.createElement("div");
    this.root.className = "tour";
    this.root.hidden = true;
    this.root.innerHTML = `<div class="tour-block"></div><div class="tour-spot"></div><div class="tour-pop" role="dialog" aria-modal="true" aria-labelledby="tour-title"></div>`;
    document.body.appendChild(this.root);
    this.spot = this.root.querySelector(".tour-spot")!;
    this.pop = this.root.querySelector(".tour-pop")!;
    this.root.querySelector(".tour-block")!.addEventListener("click", () => this.go(this.i + 1));
  }

  get running(): boolean {
    return !this.root.hidden;
  }

  start(): void {
    this.steps = this.all.filter((s) => {
      const el = document.querySelector<HTMLElement>(s.target);
      return !!el && el.offsetParent !== null;
    });
    if (!this.steps.length) return;
    this.root.hidden = false;
    addEventListener("resize", this.onReflow);
    addEventListener("scroll", this.onReflow, { passive: true });
    document.addEventListener("keydown", this.onKey, true);
    this.go(0);
  }

  end(): void {
    if (!this.running) return;
    this.root.hidden = true;
    removeEventListener("resize", this.onReflow);
    removeEventListener("scroll", this.onReflow);
    document.removeEventListener("keydown", this.onKey, true);
    this.onEnd();
  }

  private go(i: number): void {
    if (i < 0) return;
    if (i >= this.steps.length) return this.end();
    this.i = i;
    const s = this.steps[i];
    const last = i === this.steps.length - 1;
    this.pop.innerHTML = `
      <div class="tour-count">${i + 1} of ${this.steps.length}</div>
      <h3 id="tour-title">${s.title.replace(/^\d+ · /, "")}</h3>
      <p>${(s.bodyNoChaos && document.getElementById("control-card")?.hidden ? s.bodyNoChaos : s.body)}</p>
      <div class="tour-nav">
        <button class="linkish" data-t="skip">Skip tour</button>
        <span>
          ${i > 0 ? `<button class="btn sm" data-t="back">← Back</button>` : ""}
          <button class="btn sm primary" data-t="next">${last ? (document.getElementById("control-card")?.hidden ? "Done" : "Let me try! 💥") : "Next →"}</button>
        </span>
      </div>`;
    this.pop.querySelector<HTMLElement>("[data-t='skip']")!.onclick = () => this.end();
    this.pop.querySelector<HTMLElement>("[data-t='next']")!.onclick = () => this.go(i + 1);
    const back = this.pop.querySelector<HTMLElement>("[data-t='back']");
    if (back) back.onclick = () => this.go(i - 1);
    const el = document.querySelector<HTMLElement>(s.target)!;
    el.scrollIntoView({ block: "center", behavior: reducedMotion() ? "auto" : "smooth" });
    this.place();
    // Smooth scrolling moves the target after this frame; follow it.
    setTimeout(() => this.place(), 350);
    setTimeout(() => this.place(), 700);
    this.pop.querySelector<HTMLElement>("[data-t='next']")!.focus({ preventScroll: true });
  }

  private place(): void {
    if (!this.running) return;
    const el = document.querySelector<HTMLElement>(this.steps[this.i].target);
    if (!el) return;
    const r = el.getBoundingClientRect();
    const pad = 8;
    Object.assign(this.spot.style, {
      left: `${r.left - pad}px`,
      top: `${r.top - pad}px`,
      width: `${r.width + pad * 2}px`,
      height: `${r.height + pad * 2}px`,
    });
    const w = Math.min(360, innerWidth - 24);
    this.pop.style.width = `${w}px`;
    const h = this.pop.offsetHeight;
    let top: number;
    if (r.bottom + pad + 14 + h < innerHeight) top = r.bottom + pad + 14;
    else if (r.top - pad - 14 - h > 0) top = r.top - pad - 14 - h;
    else top = Math.max(12, innerHeight - h - 12); // tall target: float over its lower part
    this.pop.style.top = `${top}px`;
    this.pop.style.left = `${Math.min(Math.max(12, r.left + r.width / 2 - w / 2), innerWidth - w - 12)}px`;
  }
}

// ---------------------------------------------------------------------------
// Welcome dialog

export interface WelcomeContext {
  sim: boolean;
  nodes: number;
  shards: number;
  rf: number;
  canBreak: boolean; // chaos controls are available in this mode
  coach: Coach;
  tour: Tour;
  breakSomething: () => void;
}

/** Whether the welcome should open by itself (first visit, or ?intro). */
export function shouldWelcome(): boolean {
  const q = new URLSearchParams(location.search);
  if (q.has("intro")) return true;
  if (q.has("nointro")) return false;
  return get(STORAGE.welcome) === null;
}

export function openWelcome(ctx: WelcomeContext): void {
  let dlg = document.getElementById("welcome") as HTMLDialogElement | null;
  if (!dlg) {
    dlg = document.createElement("dialog");
    dlg.id = "welcome";
    dlg.className = "welcome";
    dlg.setAttribute("aria-labelledby", "welcome-title");
    document.body.appendChild(dlg);
  }
  const d = dlg;
  const where = ctx.sim ? "right here in your browser" : "on the machine serving this page";
  const pages = [
    // 1 · why
    `<div class="wl-hero">${clusterArt()}</div>
     <div class="wl-kicker">Welcome to DistKV</div>
     <h2 id="welcome-title">A live database that heals itself. <span class="grad">Try to break it.</span></h2>
     <p class="wl-lead">Instagram, WhatsApp and your bank never keep your data on just one computer. Computers crash, lose power and lose their network every single day, so the data is copied across many machines that constantly agree on what's true. When one fails, the others take over in a blink, and you never notice.</p>
     <p>This page is a working miniature of that technology. <b>${ctx.nodes} servers</b> are storing data together ${where}. ${ctx.canBreak ? "You get the big red buttons." : ""}</p>
     <div class="wl-badges">
       <span>⚡ answers in under 10 ms</span><span>🛡️ survives a crashed server</span><span>✅ never gives a wrong answer</span>
     </div>`,
    // 2 · how
    `<div class="wl-kicker">How it works, in one minute</div>
     <h2>Four simple ideas</h2>
     <div class="wl-cards">
       <div><i>📚</i><b>Split up: shards</b><p>The data is cut into <b>${ctx.shards} slices</b>, like a library spread over ${ctx.shards} shelves, so no single computer does all the work.</p></div>
       <div><i>📑</i><b>Copied: replicas</b><p>Every slice is copied onto <b>${ctx.rf} different servers</b>. Lose one, and ${ctx.rf - 1} copies remain.</p></div>
       <div><i>👑</i><b>One boss each: leaders</b><p>For each slice, one server is the <b>leader</b> (★). Every change goes through it, and the other copies follow along.</p></div>
       <div><i>🗳️</i><b>Majority vote: Raft</b><p>If a leader goes silent, the remaining copies <b>vote</b> for a new one in about a second. A change only counts once a majority has saved it, so nothing is ever lost.</p></div>
     </div>
     <p class="muted small">Raft is the voting method used by real systems such as Kubernetes (etcd), CockroachDB and Consul.</p>`,
    // 3 · break it
    `<div class="wl-kicker">The buttons, decoded</div>
     <h2>Ways to break it (all safe to press)</h2>
     <table class="wl-table">
       <tr><td><span class="wl-btn bad">💥 kill -9</span></td><td><b>Pull the power plug.</b> The server dies instantly, mid-sentence. <span class="muted">"kill -9" (SIGKILL) is computer-speak for "stop now, no goodbyes".</span></td></tr>
       <tr><td><span class="wl-btn">⏹ Graceful stop</span></td><td><b>Ask it politely to shut down</b>, like a maintenance reboot. It hands its jobs over first.</td></tr>
       <tr><td><span class="wl-btn warn">✂️ Partition</span></td><td><b>Cut its network cable.</b> It's still running but can't talk to anyone.</td></tr>
       <tr><td><span class="wl-btn warn">📉 Packet loss</span></td><td><b>Give it terrible Wi-Fi:</b> 30% of its messages vanish.</td></tr>
       <tr><td><span class="wl-btn">🔁 Rolling restart</span></td><td><b>Restart every server one by one</b>, the way companies ship updates with no downtime.</td></tr>
       <tr><td><span class="wl-btn warn">🐒 Chaos Monkey</span></td><td><b>Random disasters every few seconds.</b> Named after Netflix's real tool that breaks its own servers on purpose.</td></tr>
       <tr><td><span class="wl-btn primary">🎯 Game Day</span></td><td><b>A fire drill:</b> a series of disasters, each checked automatically, with a grade at the end.</td></tr>
       <tr><td><span class="wl-btn ok">🩹 Heal</span></td><td><b>Fix everything</b> and watch it return to green.</td></tr>
     </table>
     ${ctx.canBreak ? "" : `<p class="muted small">The chaos buttons appear in the simulation (this site on GitHub Pages) and in the local demo (<code>make demo</code>).</p>`}`,
    // 4 · watch
    `<div class="wl-kicker">What to watch</div>
     <h2>How to tell it's working</h2>
     <ul class="wl-watch">
       <li><b class="okc">Availability stays near 100%</b>: the share of requests that succeeded, even while you break things. The industry bar is 99.9%.</li>
       <li><b class="okc">Speed stays under 10 ms</b>: 99 of every 100 requests finish within one thirtieth of a blink.</li>
       <li><b class="okc">Linearizable ✓</b>: an independent checker proves every answer was correct: nothing stale, nothing lost.</li>
       <li><b>🔔 Alerts</b>: a red one when something breaks, a green one when it has healed, and a report you can download.</li>
       <li><b>Hover anything</b> (or tap an <span class="ex-i demo" aria-hidden="true">?</span>) for a plain-English explanation.</li>
     </ul>
     <label class="wl-check"><input type="checkbox" id="wl-coach" ${ctx.coach.enabled ? "checked" : ""}> Explain what's happening as I go (recommended)</label>`,
  ];

  let page = 0;
  const render = () => {
    const last = page === pages.length - 1;
    d.innerHTML = `
      <button class="wl-x" aria-label="Close" data-w="close">✕</button>
      <div class="wl-page">${pages[page]}</div>
      <div class="wl-foot">
        <div class="wl-dots">${pages.map((_, i) => `<button aria-label="Page ${i + 1}" class="${i === page ? "on" : ""}" data-w="dot" data-i="${i}"></button>`).join("")}</div>
        <div class="wl-nav">
          ${page > 0 ? `<button class="btn" data-w="back">← Back</button>` : `<button class="linkish" data-w="close">Skip intro</button>`}
          ${last
            ? `<button class="btn" data-w="explore">I'll explore</button>
               <button class="btn" data-w="tour">🎬 Show me around</button>
               ${ctx.canBreak ? `<button class="btn primary" data-w="break">💥 Break something!</button>` : ""}`
            : `<button class="btn primary" data-w="next">${page === 0 ? "Show me how →" : "Next →"}</button>`}
        </div>
      </div>`;
    const coachBox = d.querySelector<HTMLInputElement>("#wl-coach");
    if (coachBox) coachBox.onchange = () => (ctx.coach.enabled = coachBox.checked);
    d.querySelector<HTMLElement>("[data-w='next'], [data-w='break'], [data-w='tour']")?.focus();
  };
  const finish = (then?: () => void) => {
    set(STORAGE.welcome, "seen");
    d.close();
    then?.();
  };
  d.onclick = (e) => {
    if (e.target === d) return finish(); // backdrop
    const b = (e.target as HTMLElement).closest<HTMLElement>("[data-w]");
    switch (b?.dataset.w) {
      case "close":
      case "explore":
        return finish();
      case "next":
        page++;
        return render();
      case "back":
        page--;
        return render();
      case "dot":
        page = Number(b.dataset.i);
        return render();
      case "tour":
        return finish(() => ctx.tour.start());
      case "break":
        return finish(ctx.breakSomething);
    }
  };
  d.onkeydown = (e) => {
    if ((e.target as HTMLElement).tagName === "INPUT") return;
    if (e.key === "ArrowRight" && page < pages.length - 1) (page++, render());
    else if (e.key === "ArrowLeft" && page > 0) (page--, render());
  };
  d.oncancel = () => set(STORAGE.welcome, "seen");
  render();
  d.showModal();
}

/** Five servers in a ring, one copy each of a shard, the leader crowned. */
function clusterArt(): string {
  const n = 5;
  const pts = Array.from({ length: n }, (_, i) => {
    const a = (i / n) * Math.PI * 2 - Math.PI / 2;
    return [Math.cos(a) * 62, Math.sin(a) * 62];
  });
  const colors = ["#7c5cff", "#00d4ff", "#ff5ca8", "#2ee59d", "#ffb547"];
  const links = pts.flatMap(([x1, y1], i) => pts.slice(i + 1).map(([x2, y2]) => `<line x1="${x1}" y1="${y1}" x2="${x2}" y2="${y2}"/>`)).join("");
  const nodes = pts
    .map(([x, y], i) => `<g class="wl-node" style="--d:${i * 0.4}s"><circle cx="${x}" cy="${y}" r="17" fill="${colors[i]}"/><text x="${x}" y="${y + 4}">n${i + 1}</text></g>`)
    .join("");
  const [lx, ly] = pts[0];
  return `<svg viewBox="-95 -95 190 190" aria-hidden="true"><g class="wl-links">${links}</g>${nodes}<text class="wl-crown" x="${lx}" y="${ly - 22}">👑</text></svg>`;
}
