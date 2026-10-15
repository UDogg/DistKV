/**
 * The log console: one searchable, filterable stream that merges what the
 * dashboard observes (cluster events, chaos actions, alerts, client stats)
 * with the nodes' own server logs when the demo supervisor can serve them.
 */

export type Level = "debug" | "info" | "warn" | "error";

export interface LogEntry {
  seq: number;
  ts: number;
  level: Level;
  source: string;
  msg: string;
  attrs: [string, string][];
}

const LEVELS: Level[] = ["debug", "info", "warn", "error"];
const MAX_ENTRIES = 4000;
const MAX_RENDERED = 500;

const esc = (s: string) => s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!);

/** Parses a server log line: logfmt (text handler) or JSON (--log-format json). */
export function parseLine(line: string): { ts: number; level: Level; msg: string; attrs: [string, string][] } {
  const norm = (l: unknown): Level => {
    const v = String(l ?? "info").toLowerCase();
    return v.startsWith("err") ? "error" : v.startsWith("warn") ? "warn" : v.startsWith("debug") ? "debug" : "info";
  };
  const t = line.trim();
  if (t.startsWith("{")) {
    try {
      const o = JSON.parse(t) as Record<string, unknown>;
      const { time, level, msg, ...rest } = o;
      return {
        ts: time ? Date.parse(String(time)) || Date.now() : Date.now(),
        level: norm(level),
        msg: String(msg ?? ""),
        attrs: Object.entries(rest).map(([k, v]) => [k, typeof v === "string" ? v : JSON.stringify(v)]),
      };
    } catch {
      /* not JSON after all */
    }
  }
  const attrs: [string, string][] = [];
  let ts = Date.now();
  let level: Level = "info";
  let msg = "";
  let matched = false;
  for (const m of t.matchAll(/([\w.]+)=("(?:[^"\\]|\\.)*"|\S*)/g)) {
    matched = true;
    let v = m[2];
    if (v.startsWith('"')) {
      try {
        v = JSON.parse(v);
      } catch {
        v = v.slice(1, -1);
      }
    }
    if (m[1] === "time") ts = Date.parse(v) || ts;
    else if (m[1] === "level") level = norm(v);
    else if (m[1] === "msg") msg = v;
    else attrs.push([m[1], v]);
  }
  if (!matched || !msg) msg = msg || t;
  return { ts, level, msg, attrs };
}

export interface ConsoleElements {
  list: HTMLElement;
  search: HTMLInputElement;
  source: HTMLSelectElement;
  levels: HTMLElement;
  follow: HTMLInputElement;
  count: HTMLElement;
}

export class LogConsole {
  private readonly entries: LogEntry[] = [];
  private seq = 0;
  private readonly sources = new Set<string>();
  private readonly enabled = new Set<Level>(["info", "warn", "error"]);
  private pending = false;
  private paused = false;
  onSelect: (e: LogEntry) => void = () => {};

  constructor(
    private readonly el: ConsoleElements,
    private readonly colorOf: (source: string) => string | undefined,
  ) {
    el.levels.innerHTML = LEVELS.map(
      (l) => `<button type="button" class="lvl lvl-${l}${this.enabled.has(l) ? " on" : ""}" data-level="${l}" aria-pressed="${this.enabled.has(l)}">${l}</button>`,
    ).join("");
    el.levels.addEventListener("click", (e) => {
      const b = (e.target as HTMLElement).closest<HTMLButtonElement>("[data-level]");
      if (!b) return;
      const l = b.dataset.level as Level;
      if (this.enabled.has(l)) this.enabled.delete(l);
      else this.enabled.add(l);
      b.classList.toggle("on", this.enabled.has(l));
      b.setAttribute("aria-pressed", String(this.enabled.has(l)));
      this.schedule();
    });
    el.search.addEventListener("input", () => this.schedule());
    el.source.addEventListener("change", () => this.schedule());
    el.follow.addEventListener("change", () => {
      if (el.follow.checked) this.scrollToEnd();
    });
    // Scrolling up pauses following; scrolling back to the bottom resumes it.
    el.list.addEventListener("scroll", () => {
      const atEnd = el.list.scrollHeight - el.list.scrollTop - el.list.clientHeight < 24;
      if (el.follow.checked !== atEnd) el.follow.checked = atEnd;
    });
    el.list.addEventListener("click", (e) => {
      const row = (e.target as HTMLElement).closest<HTMLElement>("[data-seq]");
      if (!row) return;
      const entry = this.entries.find((x) => x.seq === Number(row.dataset.seq));
      if (entry) {
        row.classList.toggle("open");
        this.onSelect(entry);
      }
    });
  }

  add(source: string, level: Level, msg: string, attrs: [string, string][] = [], ts = Date.now()): void {
    this.push({ seq: this.seq++, ts, level, source, msg, attrs });
  }

  /** A raw server log line from a node. */
  addNodeLine(node: string, line: string): void {
    if (!line.trim()) return;
    const p = parseLine(line);
    this.push({ seq: this.seq++, ts: p.ts, level: p.level, source: node, msg: p.msg, attrs: p.attrs.filter(([k]) => k !== "node") });
  }

  setPaused(p: boolean): void {
    this.paused = p;
    if (!p) this.schedule();
  }

  clear(): void {
    this.entries.length = 0;
    this.schedule();
  }

  /** Plain-text export of everything that passes the current filters. */
  exportText(): string {
    return this.filtered(Infinity)
      .map((e) => `${new Date(e.ts).toISOString()} ${e.level.toUpperCase().padEnd(5)} [${e.source}] ${e.msg}${e.attrs.length ? " " + e.attrs.map(([k, v]) => `${k}=${/\s/.test(v) ? JSON.stringify(v) : v}`).join(" ") : ""}`)
      .join("\n");
  }

  exportJSON(): string {
    return this.filtered(Infinity)
      .map((e) => JSON.stringify({ time: new Date(e.ts).toISOString(), level: e.level, source: e.source, msg: e.msg, ...Object.fromEntries(e.attrs) }))
      .join("\n");
  }

  private push(e: LogEntry): void {
    this.entries.push(e);
    if (this.entries.length > MAX_ENTRIES) this.entries.splice(0, this.entries.length - MAX_ENTRIES);
    if (!this.sources.has(e.source)) {
      this.sources.add(e.source);
      const opt = document.createElement("option");
      opt.value = e.source;
      opt.textContent = e.source;
      this.el.source.appendChild(opt);
    }
    this.schedule();
  }

  private filtered(limit: number): LogEntry[] {
    const q = this.el.search.value.trim().toLowerCase();
    const src = this.el.source.value;
    const out: LogEntry[] = [];
    // Node logs and dashboard events interleave by time, not arrival order.
    const sorted = [...this.entries].sort((a, b) => a.ts - b.ts || a.seq - b.seq);
    for (let i = sorted.length - 1; i >= 0 && out.length < limit; i--) {
      const e = sorted[i];
      if (!this.enabled.has(e.level)) continue;
      if (src && src !== "all" && e.source !== src) continue;
      if (q && !`${e.source} ${e.msg} ${e.attrs.map(([k, v]) => `${k}=${v}`).join(" ")}`.toLowerCase().includes(q)) continue;
      out.push(e);
    }
    return out.reverse();
  }

  private schedule(): void {
    if (this.pending || this.paused) return;
    this.pending = true;
    requestAnimationFrame(() => {
      this.pending = false;
      this.render();
    });
  }

  private render(): void {
    const rows = this.filtered(MAX_RENDERED);
    const q = this.el.search.value.trim();
    const hl = (s: string) => {
      const h = esc(s);
      if (!q) return h;
      const i = h.toLowerCase().indexOf(esc(q).toLowerCase());
      return i < 0 ? h : `${h.slice(0, i)}<mark>${h.slice(i, i + esc(q).length)}</mark>${h.slice(i + esc(q).length)}`;
    };
    this.el.list.innerHTML = rows.length
      ? rows
          .map((e) => {
            const c = this.colorOf(e.source);
            const d = new Date(e.ts);
            const time = `${d.toLocaleTimeString([], { hour12: false })}.${String(d.getMilliseconds()).padStart(3, "0")}`;
            return `<div class="log-row lvl-${e.level}" data-seq="${e.seq}"><time>${time}</time><span class="log-lvl">${e.level}</span><span class="log-src"${c ? ` style="--src:${c}"` : ""}>${esc(e.source)}</span><span class="log-msg">${hl(e.msg)}${e.attrs.length ? ` <span class="log-attrs">${e.attrs.map(([k, v]) => `<i>${esc(k)}</i>=${hl(v)}`).join(" ")}</span>` : ""}</span></div>`;
          })
          .join("")
      : `<div class="log-empty">No log lines match the filters.</div>`;
    this.el.count.textContent = `${rows.length === MAX_RENDERED ? `last ${MAX_RENDERED} of ` : ""}${this.entries.length.toLocaleString()} lines`;
    if (this.el.follow.checked) this.scrollToEnd();
  }

  private scrollToEnd(): void {
    this.el.list.scrollTop = this.el.list.scrollHeight;
  }
}
