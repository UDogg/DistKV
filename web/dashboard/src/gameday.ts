import { summarize, type Incident, type Summary } from "./incidents";
import { fmtDuration, REPORT_CSS } from "./report";
import type { ClusterStatus } from "./types";

/**
 * Game Day: chaos experiments in the style of the Principles of Chaos
 * Engineering. Each experiment states a hypothesis about the system's
 * steady state under a fault, injects the fault, heals it, and checks the
 * hypothesis against what was measured: client errors, failover times and
 * the consistency probe's linearizability verdict.
 */

export interface Check {
  name: string;
  ok: boolean;
  detail: string;
  /** Informational: shown, but doesn't decide the verdict. */
  info?: boolean;
}

export interface ExperimentCtx {
  snap(): ClusterStatus | null;
  act(node: string, act: "kill" | "stop" | "start" | "isolate" | "heal", why?: string): Promise<void>;
  loss(node: string, dropPercent: number, delayMs: number): Promise<void>;
  scenario(name: string): Promise<void>;
  wait(ms: number, what: string): Promise<void>;
  step(text: string): void;
}

export interface Experiment {
  id: string;
  icon: string;
  name: string;
  hypothesis: string;
  /** Rough wall time, for the UI. */
  seconds: number;
  /** Injects the fault and observes it; returns notes for the report. */
  run(ctx: ExperimentCtx): Promise<string[]>;
  verify(inc: Incident, s: Summary, notes: string[]): Check[];
}

export type ExperimentState = "idle" | "queued" | "running" | "pass" | "fail" | "aborted";

export interface ExperimentResult {
  id: string;
  state: ExperimentState;
  startedAt: number;
  endedAt?: number;
  incidentId?: number;
  checks: Check[];
  notes: string[];
  error?: string;
}

export interface GameDayRun {
  startedAt: number;
  endedAt?: number;
  mode: "live" | "sim";
  results: ExperimentResult[];
  verifiedOps?: number;
  violations?: number;
  cancelled?: boolean;
}

// ---------------------------------------------------------------------------
// Checks

const noFailedOps = (s: Summary): Check =>
  s.clientFailed === undefined
    ? { name: "no failed client operations", ok: s.serverErrors === 0, detail: `${s.serverErrors} server errors (no client counts in this mode)` }
    : {
        name: "no failed client operations",
        ok: s.clientFailed === 0,
        detail: `${s.clientFailed} failed of ${(s.clientOk! + s.clientFailed).toLocaleString()} (${((s.availability ?? 1) * 100).toFixed(3)}% available)`,
      };

const linearizable = (s: Summary): Check =>
  s.verifiedOps === undefined
    ? { name: "history is linearizable", ok: true, info: true, detail: "no consistency probe in this mode" }
    : { name: "history is linearizable", ok: (s.violations ?? 0) === 0, detail: `${s.verifiedOps.toLocaleString()} operations verified, ${s.violations ?? 0} violations` };

const failoverWithin = (s: Summary, ms: number): Check => ({
  name: `every failover within ${fmtDuration(ms)}`,
  ok: s.failovers > 0 && s.maxFailoverMs <= ms,
  detail: s.failovers ? `${s.failovers} failovers, slowest ${s.failoversBounded ? "≤ " : ""}${fmtDuration(s.maxFailoverMs)}` : "no failover observed: the fault had no effect",
});

const recovers = (s: Summary, ms: number): Check => ({
  name: `healthy within ${fmtDuration(ms)} of the heal`,
  ok: s.recoverAfterHealMs !== undefined && s.recoverAfterHealMs <= ms,
  detail: s.recoverAfterHealMs !== undefined ? `fully healthy ${fmtDuration(s.recoverAfterHealMs)} after the heal` : "never recovered",
});

const busiest = (snap: ClusterStatus | null) =>
  [...(snap?.nodes ?? [])].filter((n) => n.reachable && !n.faults?.isolated).sort((a, b) => b.shards_led - a.shards_led)[0];

// ---------------------------------------------------------------------------
// The catalog

export const EXPERIMENTS: Experiment[] = [
  {
    id: "leader-loss",
    icon: "💥",
    name: "Lose the busiest leader",
    hypothesis: "SIGKILL of the node leading the most shards costs clients nothing: each affected shard elects a new leader within 2 s and the history stays linearizable.",
    seconds: 15,
    async run(ctx) {
      const n = busiest(ctx.snap());
      if (!n) throw new Error("no live node");
      await ctx.act(n.id, "kill", `busiest leader, ${n.shards_led} shards`);
      await ctx.wait(5000, `observing the failover of ${n.id}'s shards`);
      return [`killed ${n.id}, which led ${n.shards_led} shards`];
    },
    verify: (_inc, s) => [failoverWithin(s, 2000), noFailedOps(s), linearizable(s), recovers(s, 10_000)],
  },
  {
    id: "partition-leader",
    icon: "✂️",
    name: "Partition a leader from its peers",
    hypothesis: "An isolated leader steps down (check-quorum), the majority elects replacements within 2 s, no request fails, and no stale read is served from the old side.",
    seconds: 16,
    async run(ctx) {
      const n = busiest(ctx.snap());
      if (!n) throw new Error("no live node");
      await ctx.act(n.id, "isolate", `leads ${n.shards_led} shards`);
      await ctx.wait(6000, `observing the partition of ${n.id}`);
      return [`partitioned ${n.id}, which led ${n.shards_led} shards`];
    },
    verify: (_inc, s) => [failoverWithin(s, 2000), noFailedOps(s), linearizable(s), recovers(s, 10_000)],
  },
  {
    id: "flaky-network",
    icon: "📉",
    name: "Flaky network on two nodes",
    hypothesis: "With 30% packet loss and 5 ms extra delay on two nodes for 8 s, retries absorb the loss: no request fails and the history stays linearizable.",
    seconds: 16,
    async run(ctx) {
      const nodes = (ctx.snap()?.nodes ?? []).filter((n) => n.reachable).slice(0, 2);
      for (const n of nodes) await ctx.loss(n.id, 30, 5);
      await ctx.wait(8000, `observing lossy links on ${nodes.map((n) => n.id).join(" and ")}`);
      return [`30% loss + 5 ms delay on ${nodes.map((n) => n.id).join(", ")}`];
    },
    verify: (_inc, s) => [noFailedOps(s), linearizable(s), recovers(s, 10_000)],
  },
  {
    id: "rolling-restart",
    icon: "🔁",
    name: "Rolling restart",
    hypothesis: "Planned maintenance is invisible: graceful stops hand leadership off first, so there are zero failovers and zero failed requests while every node restarts.",
    seconds: 35,
    async run(ctx) {
      ctx.step("restarting every node, one at a time");
      await ctx.scenario("rolling");
      return ["graceful stop → start of every node, one at a time"];
    },
    verify: (_inc, s) => [
      { name: "no failovers (hand-offs only)", ok: s.failovers === 0, detail: `${s.failovers} failovers` },
      noFailedOps(s),
      linearizable(s),
    ],
  },
  {
    id: "quorum-loss",
    icon: "🛑",
    name: "Lose a shard's majority",
    hypothesis: "DistKV is CP: with two of a shard's three replicas down, that shard refuses writes rather than diverge, and once a replica returns it recovers with no lost or stale data.",
    seconds: 22,
    async run(ctx) {
      const snap = ctx.snap();
      if (!snap) throw new Error("no cluster status");
      // A shard whose replicas are all up; take down two of them.
      const sh = snap.shards.find((x) => x.replicas.every((r) => snap.nodes.find((n) => n.id === r)?.reachable));
      if (!sh) throw new Error("no fully replicated shard");
      const [a, b] = sh.replicas.slice(1);
      await ctx.act(a, "kill", `replica of shard ${sh.shard}`);
      await ctx.act(b, "kill", `second replica of shard ${sh.shard}`);
      await ctx.wait(7000, `shard ${sh.shard} without a majority`);
      return [`killed ${a} and ${b}, two of shard ${sh.shard}'s replicas (${sh.replicas.join(", ")})`, `shard:${sh.shard}`];
    },
    verify: (inc, s, notes) => {
      const shard = Number(notes.find((n) => n.startsWith("shard:"))?.slice(6));
      return [
        {
          name: `shard ${shard} refused service (no split brain)`,
          ok: inc.quorumLost.includes(shard),
          detail: inc.quorumLost.includes(shard) ? `${inc.quorumLost.length} shard(s) had no leader until a replica returned` : "the shard kept a leader: the fault had no effect",
        },
        linearizable(s),
        recovers(s, 15_000),
        { ...noFailedOps(s), name: "client impact (expected)", ok: true, info: true },
      ];
    },
  },
];

// ---------------------------------------------------------------------------
// Scoring and the Game Day report

export function score(run: GameDayRun): { pct: number; grade: string; passed: number; total: number } {
  const done = run.results.filter((r) => r.state === "pass" || r.state === "fail");
  const checks = done.flatMap((r) => r.checks.filter((c) => !c.info));
  const ok = checks.filter((c) => c.ok).length;
  const pct = checks.length ? (ok / checks.length) * 100 : 0;
  const grade = pct >= 97 ? "A+" : pct >= 90 ? "A" : pct >= 80 ? "B" : pct >= 70 ? "C" : pct >= 50 ? "D" : "F";
  return { pct, grade, passed: done.filter((r) => r.state === "pass").length, total: run.results.length };
}

const esc = (s: string) => s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!);
const exp = (id: string) => EXPERIMENTS.find((e) => e.id === id)!;
const fmtDate = (ts: number) => new Date(ts).toLocaleString([], { hour12: false });

export function gameDayHTML(run: GameDayRun, incidents: Incident[]): string {
  const sc = score(run);
  const rows = run.results
    .map((r) => {
      const e = exp(r.id);
      const inc = incidents.find((i) => i.id === r.incidentId);
      const s = inc ? summarize(inc) : undefined;
      const badge = { pass: `<span class="rpt-badge ok">PASS</span>`, fail: `<span class="rpt-badge bad">FAIL</span>`, aborted: `<span class="rpt-badge open">ABORTED</span>` }[r.state as "pass"] ?? `<span class="rpt-badge">${r.state.toUpperCase()}</span>`;
      return `<section class="gd-exp">
        <h3>${e.icon} ${esc(e.name)} ${badge}${inc ? ` <a href="#" class="gd-inc" data-report="${inc.id}">incident #${inc.id} · ${inc.severity}</a>` : ""}</h3>
        <p class="gd-hyp"><b>Hypothesis.</b> ${esc(e.hypothesis)}</p>
        ${r.notes.filter((n) => !n.startsWith("shard:")).map((n) => `<p class="muted">Fault: ${esc(n)}${s ? ` · recovered in ${fmtDuration(s.durationMs)}` : ""}</p>`).join("")}
        ${r.error ? `<p class="gd-err">${esc(r.error)}</p>` : ""}
        <ul class="rpt-findings">${r.checks.map((c) => `<li class="${c.info ? "info" : c.ok ? "ok" : "bad"}">${c.info ? "ℹ" : c.ok ? "✓" : "✗"} <b>${esc(c.name)}</b>: ${esc(c.detail)}</li>`).join("")}</ul>
      </section>`;
    })
    .join("");
  const dur = (run.endedAt ?? Date.now()) - run.startedAt;
  return `<article class="rpt gd">
  <header class="rpt-head">
    <div class="rpt-kicker">DistKV Game Day${run.mode === "sim" ? " · simulation" : ""}</div>
    <h1>${sc.passed} of ${sc.total} hypotheses held</h1>
    <div class="gd-score"><div class="gd-grade g-${sc.grade[0].toLowerCase()}">${sc.grade}</div>
      <div><b>${sc.pct.toFixed(0)}%</b> of checks passed<br><span class="muted">${fmtDate(run.startedAt)} · ${fmtDuration(dur)}${run.cancelled ? " · cancelled" : ""}</span></div>
      ${run.verifiedOps !== undefined ? `<div class="gd-lin ${run.violations ? "bad" : "ok"}">${run.violations ? `✗ ${run.violations} linearizability violation(s)` : `✓ ${run.verifiedOps.toLocaleString()} operations verified linearizable`}<br><span class="muted">across the whole Game Day (Wing &amp; Gong)</span></div>` : ""}
    </div>
  </header>
  <section>
    <h2>Method</h2>
    <p>Each experiment waits for a steady state (every node up, every shard led, no open incident), injects its fault, observes, heals, waits for recovery and ${6} more seconds for the consistency probe to close its epochs, then checks the hypothesis against the measurements: client errors from the demo's retrying client, failover times from cluster snapshots (±1 s), and the probe's linearizability verdict.</p>
  </section>
  ${rows}
  <footer class="rpt-foot">Generated by the DistKV dashboard at ${fmtDate(Date.now())}.</footer>
</article>`;
}

export const GAMEDAY_CSS = `
.rpt .gd-score { display: flex; flex-wrap: wrap; align-items: center; gap: 18px; margin-top: 12px; }
.rpt .gd-grade { width: 74px; height: 74px; border-radius: 18px; display: grid; place-items: center; font-size: 34px; font-weight: 900; color: #fff; background: #5c6bd6; }
.rpt .gd-grade.g-a { background: linear-gradient(135deg, #12a36b, #2ee59d); }
.rpt .gd-grade.g-b { background: linear-gradient(135deg, #5c8aff, #00d4ff); }
.rpt .gd-grade.g-c { background: linear-gradient(135deg, #c47a00, #ffb547); }
.rpt .gd-grade.g-d, .rpt .gd-grade.g-f { background: linear-gradient(135deg, #d6334f, #ff5c7a); }
.rpt .gd-lin { padding: 8px 12px; border-radius: 10px; font-weight: 700; }
.rpt .gd-lin.ok { background: rgba(18,163,107,.08); color: #12a36b; } .rpt .gd-lin.bad { background: rgba(214,51,79,.08); color: #d6334f; }
.rpt .gd-exp { border-top: 1px solid rgba(80,90,180,.18); padding-top: 6px; margin-top: 14px; }
.rpt .gd-exp h3 { font-size: 16px; margin: 10px 0 6px; display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
.rpt .gd-hyp { color: #2b3160; }
.rpt .gd-inc { font-size: 12px; font-weight: 600; color: #5c6bd6; }
.rpt .gd-err { color: #d6334f; }
.rpt .rpt-findings li.info { background: rgba(92,107,214,.07); }
`;

export function gameDayDocument(run: GameDayRun, incidents: Incident[]): string {
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>DistKV Game Day</title>
<style>
body { margin: 0; padding: 32px 16px; background: #f4f6fb; color: #141833; font: 14px/1.55 Inter, ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }
.rpt { max-width: 860px; margin: 0 auto; background: #fff; border-radius: 14px; padding: 32px 36px; box-shadow: 0 8px 30px rgba(40,50,120,.1); }
${REPORT_CSS}
${GAMEDAY_CSS}
@media print { body { background: #fff; padding: 0; } .rpt { box-shadow: none; padding: 0; } }
</style></head><body>
${gameDayHTML(run, incidents)}
</body></html>`;
}

export function gameDayMarkdown(run: GameDayRun, incidents: Incident[]): string {
  const sc = score(run);
  const L: string[] = [`# DistKV Game Day: ${sc.passed} of ${sc.total} hypotheses held (grade ${sc.grade}, ${sc.pct.toFixed(0)}% of checks)`, ""];
  L.push(`${fmtDate(run.startedAt)} · ${fmtDuration((run.endedAt ?? Date.now()) - run.startedAt)}${run.mode === "sim" ? " · simulation" : ""}${run.cancelled ? " · cancelled" : ""}`, "");
  if (run.verifiedOps !== undefined)
    L.push(run.violations ? `❌ ${run.violations} linearizability violation(s)` : `✅ ${run.verifiedOps.toLocaleString()} operations verified linearizable across the Game Day`, "");
  for (const r of run.results) {
    const e = exp(r.id);
    const inc = incidents.find((i) => i.id === r.incidentId);
    L.push(`## ${e.icon} ${e.name}: ${r.state.toUpperCase()}${inc ? ` (incident #${inc.id}, ${inc.severity})` : ""}`, "");
    L.push(`**Hypothesis.** ${e.hypothesis}`, "");
    for (const n of r.notes.filter((x) => !x.startsWith("shard:"))) L.push(`Fault: ${n}`, "");
    if (r.error) L.push(`Error: ${r.error}`, "");
    for (const c of r.checks) L.push(`- ${c.info ? "ℹ️" : c.ok ? "✅" : "❌"} **${c.name}**: ${c.detail}`);
    L.push("");
  }
  return L.join("\n");
}
