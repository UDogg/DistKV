/**
 * The game layer: experience points and levels for breaking things (and for
 * the cluster surviving them), stars for each recovery, achievements, and a
 * trophy cabinet. All of it is kept in this browser only.
 */

export interface Achievement {
  id: string;
  icon: string;
  name: string;
  how: string;
  xp: number;
}

export const ACHIEVEMENTS: Achievement[] = [
  { id: "plug", icon: "💥", name: "Pulled the plug", how: "Crash a server with kill -9", xp: 30 },
  { id: "kingslayer", icon: "👑", name: "Kingslayer", how: "Crash a server that leads 5 or more shards", xp: 50 },
  { id: "ninja", icon: "✂️", name: "Network ninja", how: "Cut a server off the network", xp: 30 },
  { id: "phoenix", icon: "🔥", name: "Phoenix", how: "Watch a server come back from a power loss on its own", xp: 50 },
  { id: "flawless", icon: "💎", name: "Flawless", how: "Survive a disaster with zero failed requests", xp: 50 },
  { id: "speedrun", icon: "⚡", name: "Speedrun", how: "Get back to full health in under 5 seconds", xp: 60 },
  { id: "double", icon: "🎯", name: "Double trouble", how: "Have two servers broken at the same time", xp: 60 },
  { id: "majority", icon: "🧨", name: "Majority lost", how: "Break enough servers that some data loses its majority, then recover", xp: 100 },
  { id: "survivor", icon: "🛡️", name: "Survivor", how: "Watch the cluster survive 10 disasters", xp: 100 },
  { id: "monkey", icon: "🐒", name: "Monkey tamer", how: "Let the Chaos Monkey run for a full minute", xp: 80 },
  { id: "marshal", icon: "🏆", name: "Fire marshal", how: "Finish a Game Day with grade A or better", xp: 150 },
  { id: "scholar", icon: "📖", name: "Curious mind", how: "Read 10 explanations (hover or tap ?)", xp: 30 },
  { id: "tourist", icon: "🎬", name: "Tour guide", how: "Finish the guided tour", xp: 20 },
];

const LEVELS = [
  { xp: 0, title: "Curious visitor" },
  { xp: 100, title: "Intern on call" },
  { xp: 300, title: "Junior SRE" },
  { xp: 700, title: "Site reliability engineer" },
  { xp: 1300, title: "Chaos engineer" },
  { xp: 2200, title: "Principal chaos wizard" },
];

interface State {
  xp: number;
  unlocked: Record<string, number>;
  disasters: number;
  survived: number;
  flawless: number;
  bestMs: number | null;
  stars: number;
  explained: string[]; // explanation keys read
}

const KEY = "distkv-game-v1";

function load(): State {
  const fresh: State = { xp: 0, unlocked: {}, disasters: 0, survived: 0, flawless: 0, bestMs: null, stars: 0, explained: [] };
  try {
    return { ...fresh, ...JSON.parse(localStorage.getItem(KEY) ?? "{}") };
  } catch {
    return fresh;
  }
}

export interface RecoveryResult {
  durationMs: number;
  failed: number | undefined; // client requests that failed, when known
  availability: number | undefined;
}

/** Stars for a recovery: fast and lossless earns three. */
export function starsFor(r: RecoveryResult): number {
  const lossless = r.failed === 0 || (r.failed === undefined && (r.availability ?? 1) >= 1);
  if (lossless && r.durationMs <= 12_000) return 3;
  if ((r.availability ?? 1) >= 0.999 && r.durationMs <= 25_000) return 2;
  return 1;
}

export class Game {
  private s: State = load();
  private readonly toasts: HTMLElement;
  private readonly queue: (() => void)[] = [];
  onChange: () => void = () => {};

  constructor() {
    this.toasts = document.createElement("div");
    this.toasts.className = "ach-stack";
    this.toasts.setAttribute("aria-live", "polite");
    document.body.appendChild(this.toasts);
  }

  get state(): Readonly<State> {
    return this.s;
  }

  level(): { n: number; title: string; into: number; span: number; next?: string } {
    let i = 0;
    while (i + 1 < LEVELS.length && this.s.xp >= LEVELS[i + 1].xp) i++;
    const cur = LEVELS[i];
    const nxt = LEVELS[i + 1];
    return { n: i + 1, title: cur.title, into: this.s.xp - cur.xp, span: nxt ? nxt.xp - cur.xp : 1, next: nxt?.title };
  }

  has(id: string): boolean {
    return id in this.s.unlocked;
  }

  addXP(n: number): void {
    const before = this.level().n;
    this.s.xp += n;
    const after = this.level();
    if (after.n > before) this.celebrate("⬆️", `Level ${after.n}: ${after.title}`, "Level up!", 0);
    this.save();
  }

  unlock(id: string): void {
    if (this.has(id)) return;
    const a = ACHIEVEMENTS.find((x) => x.id === id);
    if (!a) return;
    this.s.unlocked[id] = Date.now();
    this.celebrate(a.icon, a.name, "Achievement unlocked", a.xp);
    this.addXP(a.xp);
  }

  disaster(): void {
    this.s.disasters++;
    this.save();
  }

  /** A recovery finished: returns its stars and whether it's a personal best. */
  recovered(r: RecoveryResult): { stars: number; best: boolean; xp: number } {
    const stars = starsFor(r);
    const best = this.s.bestMs === null || r.durationMs < this.s.bestMs;
    if (best) this.s.bestMs = r.durationMs;
    this.s.survived++;
    this.s.stars += stars;
    if (r.failed === 0) this.s.flawless++;
    const xp = stars * 15 + (best && this.s.survived > 1 ? 20 : 0);
    this.addXP(xp);
    if (r.failed === 0) this.unlock("flawless");
    if (r.durationMs < 5000) this.unlock("speedrun");
    if (this.s.survived >= 10) this.unlock("survivor");
    return { stars, best, xp };
  }

  explained(key: string): void {
    if (this.s.explained.includes(key)) return;
    this.s.explained.push(key);
    if (this.s.explained.length >= 10) this.unlock("scholar");
    else this.save();
  }

  reset(): void {
    this.s = { xp: 0, unlocked: {}, disasters: 0, survived: 0, flawless: 0, bestMs: null, stars: 0, explained: [] };
    this.save();
  }

  private save(): void {
    try {
      localStorage.setItem(KEY, JSON.stringify(this.s));
    } catch {
      /* storage unavailable: progress lasts for this visit */
    }
    this.onChange();
  }

  /** Shows at most two celebrations at a time; the rest wait their turn. */
  private celebrate(icon: string, title: string, kicker: string, xp: number): void {
    this.queue.push(() => this.show(icon, title, kicker, xp));
    this.pump();
  }

  private pump(): void {
    while (this.queue.length && this.toasts.childElementCount < 2) this.queue.shift()!();
  }

  private show(icon: string, title: string, kicker: string, xp: number): void {
    const el = document.createElement("div");
    el.className = "ach-toast";
    const bits = Array.from({ length: 14 }, (_, i) => `<i style="--a:${(i / 14) * 360}deg;--c:${["#7c5cff", "#00d4ff", "#ff5ca8", "#2ee59d", "#ffb547"][i % 5]}"></i>`).join("");
    el.innerHTML = `<div class="ach-burst" aria-hidden="true">${bits}</div><div class="ach-icon">${icon}</div><div><div class="ach-kicker">${kicker}</div><b>${title}</b>${xp ? `<span class="ach-xp">+${xp} XP</span>` : ""}</div>`;
    this.toasts.appendChild(el);
    setTimeout(() => el.classList.add("out"), 3400);
    setTimeout(() => {
      el.remove();
      this.pump();
    }, 3850);
  }

  /** The trophy cabinet's contents. */
  cabinetHTML(): string {
    const lv = this.level();
    const s = this.s;
    const got = ACHIEVEMENTS.filter((a) => this.has(a.id)).length;
    const pct = Math.min(100, (lv.into / lv.span) * 100);
    return `
      <div class="cab-head">
        <div class="cab-level"><span>Level ${lv.n}</span><b>${lv.title}</b></div>
        <div class="cab-xp"><div class="xpbar"><i style="width:${lv.next ? pct.toFixed(1) : 100}%"></i></div>
          <small>${s.xp.toLocaleString()} XP${lv.next ? ` · ${(lv.span - lv.into).toLocaleString()} XP to <b>${lv.next}</b>` : " · max level"}</small></div>
      </div>
      <div class="cab-stats">
        <div><b>${s.disasters}</b><span>disasters thrown</span></div>
        <div><b>${s.survived}</b><span>survived</span></div>
        <div><b>${s.flawless}</b><span>with zero failed requests</span></div>
        <div><b>${s.bestMs === null ? "—" : `${(s.bestMs / 1000).toFixed(1)} s`}</b><span>fastest full recovery</span></div>
        <div><b>${"⭐".repeat(Math.min(3, Math.round(s.survived ? s.stars / s.survived : 0))) || "—"}</b><span>average rating</span></div>
      </div>
      <h4>Achievements <small>${got} of ${ACHIEVEMENTS.length}</small></h4>
      <ul class="cab-list">${ACHIEVEMENTS.map((a) => {
        const at = s.unlocked[a.id];
        return `<li class="${at ? "got" : ""}"><span class="cab-ico">${at ? a.icon : "🔒"}</span><div><b>${a.name}</b><span>${a.how}</span></div><em>${at ? "✓" : `+${a.xp}`}</em></li>`;
      }).join("")}</ul>`;
  }
}
