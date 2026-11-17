/**
 * The self-healing autopilot: the automation a real platform runs around a
 * database (think Kubernetes restarting a crashed pod, or a network team
 * re-routing around a cut link). It watches every node, and each broken one
 * gets its own repair, run in parallel with the others, in visible stages.
 *
 * Raft already keeps the data available while a node is down (that part
 * needs no help); the autopilot brings the missing node itself back, so the
 * cluster returns to full strength without anyone clicking Heal.
 *
 * Stages finish on what is observed (the process is up, the node answers,
 * it has caught up), never on a timer alone, so a progress bar never claims
 * a recovery that hasn't happened. The only timed parts are the delays a
 * real platform has: noticing the failure, and power coming back.
 */

export type FaultKind = "kill" | "stop" | "isolate" | "lossy";

/** What the autopilot can see of one node. */
export interface NodeObs {
  id: string;
  proc?: string; // supervisor process state: running, stopped, killed, exited…
  reachable: boolean;
  ready: boolean;
  isolated: boolean;
  lossy: boolean;
  lag: number; // log entries behind, summed over the shards it follows
}

interface StageDef {
  icon: string;
  label: string;
  detail: string;
  ms?: number; // minimum time in the stage
  act?: "start" | "clear"; // done once, on entering the stage
  until?: (o: NodeObs) => boolean; // observed condition to leave the stage
  expectMs?: number; // typical duration of an observed stage (drives its bar)
  lag?: boolean; // progress is the catch-up of the replication lag
}

/** A follower this close to the leader counts as caught up. */
const CAUGHT_UP = 20;
const synced = (o: NodeObs) => o.reachable && o.ready && !o.isolated && o.lag <= CAUGHT_UP;

const CATCH_UP: StageDef = {
  icon: "🔄",
  label: "Catching up",
  detail: "Downloading the changes it missed from the shard leaders…",
  until: synced,
  expectMs: 1200,
  lag: true,
};

export const PLAYBOOKS: Record<FaultKind, { title: string; stages: StageDef[] }> = {
  kill: {
    title: "Power loss",
    stages: [
      { icon: "⚡", label: "Power lost", detail: "The server died mid-sentence and everything in its memory is gone. Only what was saved to disk survives. Waiting for its heartbeats to time out…", ms: 1500 },
      { icon: "🔌", label: "Restoring power", detail: "Power is coming back on. The machine is rebooting…", ms: 2000 },
      { icon: "💾", label: "Cold boot", detail: "Its memory was wiped, so it replays the log saved on disk to rebuild it. This is why a crash takes longer to recover from than a clean stop.", act: "start", until: (o) => o.reachable, expectMs: 2600 },
      { ...CATCH_UP, detail: "Back online. Downloading every change it missed while it was dark…" },
    ],
  },
  stop: {
    title: "Service stopped",
    stages: [
      { icon: "👀", label: "Noticed", detail: "The service stopped. The autopilot noticed and is restarting it…", ms: 1000 },
      { icon: "🔁", label: "Restarting", detail: "A clean shutdown saved a snapshot, so this is a quick warm start: no log replay needed.", act: "start", until: (o) => o.reachable, expectMs: 900 },
      CATCH_UP,
    ],
  },
  isolate: {
    title: "Network cut",
    stages: [
      { icon: "📡", label: "Link down", detail: "The server is alive but unreachable. Locating the broken link…", ms: 1500 },
      { icon: "🛠️", label: "Repairing link", detail: "Re-routing its traffic around the broken link…", ms: 2000 },
      { ...CATCH_UP, act: "clear", detail: "Reconnected. It was cut off, so it's downloading the changes it missed…" },
    ],
  },
  lossy: {
    title: "Flaky network",
    stages: [
      { icon: "📉", label: "Packet loss", detail: "30% of its messages are vanishing. Raft re-sends them, but it's slow. Diagnosing…", ms: 1500 },
      { icon: "🛠️", label: "Swapping the link", detail: "Moving its traffic onto a healthy link…", ms: 1800 },
      { icon: "✅", label: "Verifying", detail: "Checking that messages flow cleanly again…", act: "clear", until: (o) => !o.lossy && synced(o), expectMs: 600 },
    ],
  },
};

export interface Repair {
  node: string;
  kind: FaultKind;
  startedAt: number;
  stage: number;
  stageAt: number;
  lagMax: number;
  doneAt?: number;
  acting: boolean;
  actedStage: number; // the stage whose action has been performed (-1 = none)
  retryAt: number;
  progress: number; // 0..1, for the bars
  stageFrac: number;
}

export interface Heal {
  start(id: string): Promise<void>;
  clearFaults(id: string): Promise<void>;
}

/** The fault a node shows, if any; null also while it is booting. */
export function detect(o: NodeObs): FaultKind | null {
  // Process states are polled every couple of seconds; a node that answers
  // is running, whatever the last poll said.
  if (o.reachable) return o.isolated ? "isolate" : o.lossy ? "lossy" : null;
  if (o.proc === "killed" || o.proc === "exited" || o.proc === "crashed") return "kill";
  if (o.proc === "stopped") return "stop";
  if (o.isolated) return "isolate";
  if (o.lossy) return "lossy";
  return null;
}

const SEVERITY: Record<FaultKind, number> = { lossy: 1, isolate: 2, stop: 3, kill: 4 };

export class Autopilot {
  readonly repairs = new Map<string, Repair>();
  /** A repair started, moved to its next stage, or finished. */
  onEvent: (type: "start" | "stage" | "done" | "error", r: Repair, detail?: string) => void = () => {};

  constructor(
    private readonly heal: Heal,
    public enabled = true,
  ) {}

  get active(): Repair[] {
    return [...this.repairs.values()].filter((r) => r.doneAt === undefined);
  }

  /** Drops every repair (switched off, or something else is in control). */
  cancelAll(): void {
    this.repairs.clear();
  }

  tick(obs: NodeObs[], now = Date.now(), paused = false): void {
    if (!this.enabled || paused) {
      if (this.repairs.size) this.cancelAll();
      return;
    }
    for (const o of obs) {
      const kind = detect(o);
      let r = this.repairs.get(o.id);
      // Finished repairs linger briefly so the bar can show "healed".
      if (r?.doneAt !== undefined) {
        if (!kind) {
          if (now - r.doneAt > 4000) this.repairs.delete(o.id);
          continue;
        }
        this.repairs.delete(o.id);
        r = undefined;
      }
      // A worse fault replaces the repair in progress (a partitioned node
      // that is then killed needs power, not a cable).
      if (kind && (!r || SEVERITY[kind] > SEVERITY[r.kind])) {
        r = { node: o.id, kind, startedAt: now, stage: 0, stageAt: now, lagMax: 0, acting: false, actedStage: -1, retryAt: 0, progress: 0, stageFrac: 0 };
        this.repairs.set(o.id, r);
        this.onEvent("start", r);
      }
      if (r) this.advance(r, o, now);
    }
  }

  private advance(r: Repair, o: NodeObs, now: number): void {
    const stages = PLAYBOOKS[r.kind].stages;
    for (;;) {
      const st = stages[r.stage];
      if (st.act && r.actedStage < r.stage) {
        if (!r.acting && now >= r.retryAt) this.perform(r, st.act);
        break; // wait for the action's result before judging the stage
      }
      const elapsed = now - r.stageAt;
      if (st.lag) r.lagMax = Math.max(r.lagMax, o.lag);
      const timeOk = !st.ms || elapsed >= st.ms;
      const condOk = !st.until || st.until(o);
      if (timeOk && condOk) {
        if (r.stage === stages.length - 1) {
          r.doneAt = now;
          r.progress = 1;
          r.stageFrac = 1;
          this.onEvent("done", r);
          return;
        }
        r.stage++;
        r.stageAt = now;
        this.onEvent("stage", r);
        continue;
      }
      // Progress within the stage: time for timed stages, the lag for
      // catch-up, otherwise an estimate that never reaches 100% by itself.
      let frac: number;
      if (st.ms && !st.until) frac = Math.min(1, elapsed / st.ms);
      else if (st.lag && r.lagMax > CAUGHT_UP && o.reachable) frac = Math.min(0.97, 1 - Math.max(0, o.lag - CAUGHT_UP) / r.lagMax);
      else frac = Math.min(0.9, 1 - Math.exp(-elapsed / (st.expectMs ?? 1000)));
      r.stageFrac = frac;
      r.progress = (r.stage + frac) / stages.length;
      break;
    }
  }

  private perform(r: Repair, act: "start" | "clear"): void {
    r.acting = true;
    const stage = r.stage;
    (act === "start" ? this.heal.start(r.node) : this.heal.clearFaults(r.node))
      .then(() => {
        if (this.repairs.get(r.node) !== r) return; // replaced meanwhile
        r.actedStage = stage;
        r.stageAt = Date.now();
      })
      .catch((e) => {
        r.retryAt = Date.now() + 1500;
        this.onEvent("error", r, String(e));
      })
      .finally(() => (r.acting = false));
  }
}
