/**
 * Alerts: banners that slide in when an incident opens, escalates or
 * resolves, a bell with the alert history, an optional sound, desktop
 * notifications, and a tab-title badge while an incident is open.
 */

export type AlertLevel = "critical" | "warning" | "resolved" | "info";

export interface Alert {
  id: number;
  ts: number;
  level: AlertLevel;
  title: string;
  body: string;
  incidentId?: number;
  /** Stays until dismissed or replaced by its incident's resolution. */
  sticky?: boolean;
  /** Offer a "Heal everything" button (when chaos controls are available). */
  heal?: boolean;
}

const ICONS: Record<AlertLevel, string> = { critical: "🚨", warning: "⚠️", resolved: "✅", info: "🩹" };

const esc = (s: string) => s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!);

function load(key: string, dflt: boolean): boolean {
  try {
    const v = localStorage.getItem(key);
    return v === null ? dflt : v === "1";
  } catch {
    return dflt;
  }
}

function save(key: string, v: boolean): void {
  try {
    localStorage.setItem(key, v ? "1" : "0");
  } catch {
    /* storage unavailable */
  }
}

export class AlertCenter {
  private readonly history: Alert[] = [];
  private nextId = 1;
  private unread = 0;
  private sound = load("distkv-alert-sound", false);
  private desktop = load("distkv-alert-desktop", false);
  private audio: AudioContext | null = null;
  private readonly baseTitle = document.title;
  private openIncidents = 0;

  constructor(
    private readonly stack: HTMLElement,
    private readonly bell: HTMLButtonElement,
    private readonly count: HTMLElement,
    private readonly panel: HTMLElement,
    private readonly onReport: (incidentId: number) => void,
    private readonly onHeal: () => void = () => {},
  ) {
    bell.addEventListener("click", (e) => {
      e.stopPropagation();
      panel.hidden = !panel.hidden;
      if (!panel.hidden) {
        this.unread = 0;
        this.renderBell();
        this.renderPanel();
      }
    });
    document.addEventListener("click", (e) => {
      if (!panel.hidden && !panel.contains(e.target as Node) && e.target !== bell) panel.hidden = true;
    });
    panel.addEventListener("change", (e) => {
      const t = e.target as HTMLInputElement;
      if (t.id === "alert-sound") {
        this.sound = t.checked;
        save("distkv-alert-sound", this.sound);
        if (this.sound) this.beep("resolved");
      }
      if (t.id === "alert-desktop") {
        this.desktop = t.checked;
        save("distkv-alert-desktop", this.desktop);
        if (this.desktop && "Notification" in window && Notification.permission === "default") Notification.requestPermission();
      }
    });
    panel.addEventListener("click", (e) => this.onAction(e));
    stack.addEventListener("click", (e) => this.onAction(e));
    this.renderBell();
  }

  raise(a: Omit<Alert, "id" | "ts">): Alert {
    const alert: Alert = { ...a, id: this.nextId++, ts: Date.now() };
    this.history.unshift(alert);
    if (this.history.length > 100) this.history.length = 100;
    if (this.panel.hidden) this.unread++;
    else this.renderPanel();
    this.renderBell();
    this.showBanner(alert);
    if (this.sound) this.beep(alert.level);
    if (this.desktop && document.hidden && "Notification" in window && Notification.permission === "granted") {
      try {
        new Notification(`${ICONS[alert.level]} ${alert.title}`, { body: alert.body, tag: `distkv-${alert.incidentId ?? alert.id}` });
      } catch {
        /* some browsers only allow notifications from a service worker */
      }
    }
    return alert;
  }

  /** Removes an incident's banners, so its resolution replaces them. */
  clearIncident(incidentId: number): void {
    this.stack.querySelectorAll<HTMLElement>(`[data-incident="${incidentId}"]`).forEach((el) => this.dismiss(el));
  }

  setOpenIncidents(n: number): void {
    this.openIncidents = n;
    document.title = n ? `🔴 Incident open · ${this.baseTitle}` : this.baseTitle;
  }

  private showBanner(a: Alert): void {
    const el = document.createElement("div");
    el.className = `alert alert-${a.level}${a.sticky ? " sticky" : ""}`;
    el.setAttribute("role", a.level === "critical" ? "alert" : "status");
    if (a.incidentId !== undefined) el.dataset.incident = String(a.incidentId);
    el.innerHTML = `
      <div class="alert-icon">${ICONS[a.level]}</div>
      <div class="alert-text">
        <div class="alert-title">${esc(a.title)}</div>
        <div class="alert-body">${esc(a.body)}</div>
        ${a.incidentId !== undefined || a.heal
          ? `<div class="alert-actions">${a.heal ? `<button class="btn sm ok" data-heal>🩹 Heal everything</button>` : ""}${a.incidentId !== undefined ? `<button class="btn sm" data-report="${a.incidentId}">${a.level === "resolved" ? "📄 View report" : "📄 Live report"}</button>` : ""}</div>`
          : ""}
      </div>
      <button class="alert-close" aria-label="Dismiss" data-dismiss>×</button>
      ${a.sticky ? "" : `<div class="alert-timer"></div>`}`;
    this.stack.prepend(el);
    // Keep the stack short: drop the oldest non-sticky banners.
    const items = [...this.stack.children] as HTMLElement[];
    items.filter((x) => !x.classList.contains("sticky")).slice(3).forEach((x) => x.remove());
    if (!a.sticky) setTimeout(() => this.dismiss(el), a.level === "info" || a.level === "warning" ? 8000 : 12_000);
  }

  private dismiss(el: HTMLElement): void {
    el.classList.add("leaving");
    setTimeout(() => el.remove(), 300);
  }

  private onAction(e: Event): void {
    const t = e.target as HTMLElement;
    const rep = t.closest<HTMLElement>("[data-report]");
    if (rep) {
      this.panel.hidden = true;
      this.onReport(Number(rep.dataset.report));
      return;
    }
    const heal = t.closest<HTMLButtonElement>("[data-heal]");
    if (heal) {
      heal.disabled = true;
      this.onHeal();
      return;
    }
    const close = t.closest<HTMLElement>("[data-dismiss]");
    if (close) this.dismiss(close.closest<HTMLElement>(".alert")!);
    if (t.closest("[data-clear-alerts]")) {
      this.history.length = 0;
      this.renderPanel();
    }
  }

  private renderBell(): void {
    this.count.hidden = this.unread === 0;
    this.count.textContent = this.unread > 9 ? "9+" : String(this.unread);
    this.bell.classList.toggle("ringing", this.openIncidents > 0);
  }

  private renderPanel(): void {
    const items = this.history.length
      ? this.history
          .map(
            (a) => `<li class="alert-item alert-${a.level}">
              <span>${ICONS[a.level]}</span>
              <div><b>${esc(a.title)}</b><div class="muted">${esc(a.body)}</div>
                <time>${new Date(a.ts).toLocaleTimeString([], { hour12: false })}</time>
                ${a.incidentId !== undefined ? ` · <a href="#" data-report="${a.incidentId}">report</a>` : ""}</div>
            </li>`,
          )
          .join("")
      : `<li class="muted alert-none">No alerts yet. Inject a fault in the chaos lab to see one.</li>`;
    this.panel.innerHTML = `
      <header><b>Alerts</b><button class="btn sm" data-clear-alerts>Clear</button></header>
      <div class="alert-settings">
        <label><input type="checkbox" id="alert-sound" ${this.sound ? "checked" : ""}> 🔊 Sound</label>
        <label><input type="checkbox" id="alert-desktop" ${this.desktop ? "checked" : ""}> 🖥️ Desktop notifications</label>
      </div>
      <ol class="alert-list">${items}</ol>`;
  }

  private beep(level: AlertLevel): void {
    try {
      this.audio ??= new AudioContext();
      const ctx = this.audio;
      // Falling tones for trouble, rising for recovery.
      const notes = level === "resolved" ? [523, 784] : level === "info" ? [660] : [880, 587];
      notes.forEach((f, i) => {
        const o = ctx.createOscillator();
        const g = ctx.createGain();
        o.type = "sine";
        o.frequency.value = f;
        const t = ctx.currentTime + i * 0.16;
        g.gain.setValueAtTime(0.0001, t);
        g.gain.exponentialRampToValueAtTime(0.15, t + 0.02);
        g.gain.exponentialRampToValueAtTime(0.0001, t + 0.15);
        o.connect(g).connect(ctx.destination);
        o.start(t);
        o.stop(t + 0.16);
      });
    } catch {
      /* audio unavailable */
    }
  }
}
