import { CONSISTENCY_GRACE_MS, summarize, type Failover, type Incident, type Sample, type Summary } from "./incidents";

/**
 * Incident reports: a post-incident review generated from what the dashboard
 * recorded. The same markup is shown in the in-page viewer and exported as a
 * standalone HTML document (printable to PDF); a Markdown version is offered
 * for pasting into issues and chat.
 */

const esc = (s: string) => s.replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]!);

export function fmtDuration(ms: number): string {
  if (ms < 1000) return `${Math.round(ms)} ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)} s`;
  const m = Math.floor(ms / 60_000);
  return `${m} min ${Math.round((ms % 60_000) / 1000)} s`;
}

const fmtTime = (ts: number) => new Date(ts).toLocaleTimeString([], { hour12: false });
const fmtDate = (ts: number) => new Date(ts).toLocaleString([], { hour12: false });
const pct = (x: number) => `${(x * 100).toFixed(x === 1 ? 0 : 3)}%`;
const ms = (v: number) => (v >= 100 ? `${v.toFixed(0)} ms` : `${v.toFixed(1)} ms`);

const SEV_TEXT: Record<string, string> = {
  SEV1: "clients saw failures or a shard lost quorum",
  SEV2: "shards failed over; clients were protected by retries",
  SEV3: "degraded only; no shard lost its leader",
};

interface Finding {
  ok: boolean;
  text: string;
}

function findings(inc: Incident, s: Summary): Finding[] {
  const out: Finding[] = [];
  if (s.availability !== undefined) {
    out.push({
      ok: s.availability >= 0.999,
      text: `Availability during the incident was ${pct(s.availability)} (${s.clientOk!.toLocaleString()} ok, ${s.clientFailed} failed) against a 99.9% SLO.`,
    });
  } else {
    out.push({ ok: s.serverErrors === 0, text: `The servers returned ${s.serverErrors} errors (no client-side counts in this mode).` });
  }
  if (s.failovers) {
    out.push({
      ok: s.maxFailoverMs < 2000,
      text: inc.failovers.every((f) => f.bounded)
        ? `${s.failovers} shard${s.failovers === 1 ? "" : "s"} failed over, each electing a new leader within one snapshot interval (≤ ${fmtDuration(s.maxFailoverMs)}).`
        : `${s.failovers} shard${s.failovers === 1 ? "" : "s"} failed over; the slowest elected a new leader in ${fmtDuration(s.maxFailoverMs)} (median ${fmtDuration(s.medianFailoverMs)})${inc.quorumLost.length ? ", counting shards that had to wait for a majority of their replicas to return" : ""}.`,
    });
  } else {
    out.push({ ok: true, text: "No shard lost its leader." });
  }
  if (s.verifiedOps !== undefined) {
    const v = s.violations ?? 0;
    out.push({
      ok: v === 0,
      text: v
        ? `Linearizability was VIOLATED: the consistency probe found ${v} non-linearizable ${v === 1 ? "history" : "histories"} (Wing & Gong checker).`
        : `Linearizability held: the consistency probe verified ${s.verifiedOps.toLocaleString()} operations with the Wing & Gong checker${s.verifying ? " so far (still verifying epochs that span the incident)" : `, from the start of the incident until ${CONSISTENCY_GRACE_MS / 1000} s after it resolved`}. No stale reads, lost writes or reordering.`,
    });
  }
  if (inc.quorumLost.length) {
    out.push({ ok: false, text: `Shard${inc.quorumLost.length === 1 ? "" : "s"} ${inc.quorumLost.join(", ")} lost quorum: a majority of replicas was unreachable, so writes there could not commit.` });
  }
  out.push({
    ok: s.sloBreachSeconds === 0,
    text: s.sloBreachSeconds
      ? `p99 latency exceeded the 10 ms SLO for ${s.sloBreachSeconds} s (peak ${ms(s.peakP99)}, baseline ${ms(s.baselineP99)}): requests waited for elections.`
      : `p99 latency stayed under the 10 ms SLO (peak ${ms(s.peakP99)}).`,
  });
  if (s.minUp < s.nodes) {
    out.push(
      inc.quorumLost.length
        ? { ok: false, text: `Capacity dropped to ${s.minUp} of ${s.nodes} nodes: more than replication factor 3 tolerates (one replica per shard), so some shards lost a majority.` }
        : { ok: true, text: `Capacity dropped to ${s.minUp} of ${s.nodes} nodes; replication factor 3 tolerates one replica per shard, and no shard lost a majority.` },
    );
  }
  return out;
}

function narrative(inc: Incident, s: Summary): string[] {
  const out: string[] = [];
  out.push(
    inc.triggerKind === "action"
      ? `At ${fmtTime(inc.openedAt)} the chaos lab injected a fault: ${inc.trigger}.`
      : `At ${fmtTime(inc.openedAt)} the dashboard detected a problem without anyone injecting it: ${inc.trigger}.`,
  );
  if (s.detectMs !== undefined && inc.triggerKind === "action") out.push(`The cluster's state reflected it within ${fmtDuration(s.detectMs)}.`);
  if (s.failovers) {
    const lost = `${s.failovers} shard${s.failovers === 1 ? "" : "s"} lost ${s.failovers === 1 ? "its" : "their"} leader`;
    out.push(
      s.minHealthy === s.total
        ? `${lost}; Raft elections (pre-vote, then vote) chose new leaders so quickly that no once-per-second snapshot caught a shard without one (each took ≤ ${fmtDuration(s.maxFailoverMs)}).`
        : `${lost}; Raft elections (pre-vote, then vote) chose new leaders in at most ${fmtDuration(s.maxFailoverMs)}. At the worst point ${s.minHealthy} of ${s.total} shards had a leader.`,
    );
  } else if (inc.nodes.length) {
    out.push(`No leader was lost, so writes kept committing on every shard${s.minUp < s.nodes ? " with the remaining replicas" : ""}.`);
  }
  if (s.clientFailed !== undefined) {
    out.push(
      s.clientFailed === 0
        ? `The demo's client saw no failed operations: requests that hit a shard mid-election were retried transparently (writes exactly once).`
        : `The demo's client saw ${s.clientFailed} failed operation${s.clientFailed === 1 ? "" : "s"} that outlasted its retry budget.`,
    );
  }
  if (inc.healRequestedAt && s.recoverAfterHealMs !== undefined) out.push(`A heal was requested at ${fmtTime(inc.healRequestedAt)}; the cluster was fully healthy ${fmtDuration(s.recoverAfterHealMs)} later.`);
  if (inc.resolvedAt) out.push(`The incident resolved at ${fmtTime(inc.resolvedAt)}, ${fmtDuration(s.durationMs)} after it began.`);
  return out;
}

function recommendations(inc: Incident, s: Summary): string[] {
  const out: string[] = [];
  if (inc.quorumLost.length) out.push("Keep at most ⌊(RF−1)/2⌋ replicas of any shard down at once; use RF=5 to tolerate two failures.");
  if ((s.clientFailed ?? 0) > 0) out.push("Increase the client's retry budget (request timeout) beyond the worst observed failover time.");
  if (s.maxFailoverMs > 1500) out.push("Failover is bounded by the election timeout (T…2T); lower --election-timeout for faster failover if the network is stable.");
  if (s.sloBreachSeconds > 0) out.push("Latency spikes come from requests waiting for elections; graceful stops (leadership transfer) avoid them during planned maintenance.");
  if (!out.length) out.push("No action needed: the cluster absorbed this fault within its SLOs.");
  return out;
}

// ---------------------------------------------------------------------------
// Sparklines

interface SparkOpts {
  label: string;
  color: string;
  unit: string;
  window: [number, number];
  fmt: (v: number) => string;
  log?: boolean;
}

function spark(points: Sample[], pick: (x: Sample) => number, o: SparkOpts): string {
  const W = 860;
  const H = 104;
  const pad = { l: 4, r: 4, t: 20, b: 16 };
  if (points.length < 2) return "";
  const t0 = points[0].ts;
  const t1 = points.at(-1)!.ts;
  const span = Math.max(1, t1 - t0);
  const raw = points.map(pick);
  const f = (v: number) => (o.log ? Math.log10(Math.max(0.1, v)) : v);
  const vals = raw.map(f);
  const lo = o.log ? Math.min(...vals) : 0;
  const hi = Math.max(lo + 1e-9, ...vals);
  const x = (ts: number) => pad.l + ((ts - t0) / span) * (W - pad.l - pad.r);
  const y = (v: number) => pad.t + (1 - (v - lo) / (hi - lo)) * (H - pad.t - pad.b);
  const line = points.map((p, i) => `${x(p.ts).toFixed(1)},${y(vals[i]).toFixed(1)}`).join(" ");
  const area = `${x(t0)},${H - pad.b} ${line} ${x(t1)},${H - pad.b}`;
  const [w0, w1] = o.window;
  const sx = Math.max(pad.l, x(w0));
  const ex = Math.min(W - pad.r, x(w1));
  return `<figure class="rpt-spark">
  <svg viewBox="0 0 ${W} ${H}" role="img" aria-label="${esc(o.label)}">
    <rect x="${sx.toFixed(1)}" y="${pad.t - 4}" width="${Math.max(1, ex - sx).toFixed(1)}" height="${H - pad.t - pad.b + 4}" class="rpt-window"/>
    <polygon points="${area}" fill="${o.color}" fill-opacity=".14"/>
    <polyline points="${line}" fill="none" stroke="${o.color}" stroke-width="1.8" stroke-linejoin="round"/>
    <text x="${pad.l}" y="13" class="rpt-spark-label">${esc(o.label)}</text>
    <text x="${W - pad.r}" y="13" text-anchor="end" class="rpt-spark-label">peak ${esc(o.fmt(Math.max(...raw)))}${esc(o.unit)} · min ${esc(o.fmt(Math.min(...raw)))}${esc(o.unit)}</text>
    <text x="${pad.l}" y="${H - 2}" class="rpt-spark-axis">${fmtTime(t0)}</text>
    <text x="${W - pad.r}" y="${H - 2}" text-anchor="end" class="rpt-spark-axis">${fmtTime(t1)}</text>
  </svg>
</figure>`;
}

// ---------------------------------------------------------------------------
// HTML

const ICON: Record<string, string> = {
  action: "🧪", detected: "🔎", heal: "🩹", resolved: "✅", note: "ℹ️", quorum_lost: "🛑",
  leader_change: "🔀", election: "🗳️", node_down: "💀", node_up: "🟢", partitioned: "✂️", healed: "🩹",
  shard_unavailable: "⛔", shard_recovered: "✅",
};

export function reportHTML(inc: Incident): string {
  const s = summarize(inc);
  const status = inc.status === "open" ? `<span class="rpt-badge open">OPEN</span>` : `<span class="rpt-badge ok">RESOLVED</span>`;
  const end = inc.resolvedAt ?? Date.now();
  const series = [...inc.baseline, ...inc.samples];
  const win: [number, number] = [inc.openedAt, end];
  const stat = (label: string, value: string, cls = "") => `<div class="rpt-stat ${cls}"><b>${value}</b><span>${label}</span></div>`;
  const fnd = findings(inc, s);
  const fo = [...inc.failovers].sort((a, b) => a.lostAt - b.lostAt);
  const tl = [...inc.timeline].sort((a, b) => a.ts - b.ts);
  return `<article class="rpt">
  <header class="rpt-head">
    <div class="rpt-kicker">DistKV incident report · #${inc.id}${inc.mode === "sim" ? " · simulation" : ""}</div>
    <h1>${esc(capitalize(inc.title))}</h1>
    <div class="rpt-meta">${status}<span class="rpt-badge ${inc.severity.toLowerCase()}">${inc.severity}</span>
      ${s.verifiedOps !== undefined ? (s.violations ? `<span class="rpt-badge bad">✗ NOT LINEARIZABLE</span>` : `<span class="rpt-badge ok">✓ LINEARIZABLE${s.verifying ? " (verifying)" : ""}</span>`) : ""}
      <span>${esc(SEV_TEXT[inc.severity])}</span></div>
    <div class="rpt-meta muted">${fmtDate(inc.openedAt)} → ${inc.resolvedAt ? fmtTime(inc.resolvedAt) : "ongoing"}</div>
  </header>

  <section class="rpt-stats">
    ${stat("time to recover", fmtDuration(s.durationMs), inc.status === "open" ? "warn" : "")}
    ${stat("client availability", s.availability !== undefined ? pct(s.availability) : "n/a", s.availability === undefined ? "" : s.availability >= 0.999 ? "good" : "bad")}
    ${stat("failed client ops", s.clientFailed !== undefined ? String(s.clientFailed) : "n/a", (s.clientFailed ?? 0) > 0 ? "bad" : "good")}
    ${stat("shard failovers", String(s.failovers), s.failovers ? "warn" : "good")}
    ${stat("slowest failover", s.failovers ? `${s.failoversBounded ? "≤ " : ""}${fmtDuration(s.maxFailoverMs)}` : "—")}
    ${stat("peak p99", ms(s.peakP99), s.peakP99 < 10 ? "good" : "warn")}
  </section>

  <section>
    <h2>Summary</h2>
    ${narrative(inc, s).map((p) => `<p>${esc(p)}</p>`).join("\n    ")}
  </section>

  <section>
    <h2>Findings</h2>
    <ul class="rpt-findings">${fnd.map((f) => `<li class="${f.ok ? "ok" : "bad"}">${f.ok ? "✓" : "✗"} ${esc(f.text)}</li>`).join("")}</ul>
  </section>

  <section>
    <h2>Impact</h2>
    <table class="rpt-table">
      <tr><th>Trigger</th><td>${esc(inc.trigger)} <span class="muted">(${inc.triggerKind === "action" ? "chaos lab" : "detected"})</span></td></tr>
      <tr><th>Nodes affected</th><td>${inc.nodes.length ? inc.nodes.map(esc).join(", ") : "none"}</td></tr>
      <tr><th>Shards affected</th><td>${inc.shards.length ? [...inc.shards].sort((a, b) => a - b).join(", ") : "none"}</td></tr>
      <tr><th>Lowest healthy shards</th><td>${s.minHealthy} / ${s.total}</td></tr>
      <tr><th>Lowest nodes up</th><td>${s.minUp} / ${s.nodes}</td></tr>
      <tr><th>Leader changes</th><td>${inc.leaderChanges}</td></tr>
      <tr><th>Throughput dip</th><td>${s.dipPercent.toFixed(0)}% below baseline at the worst second</td></tr>
      <tr><th>Server errors</th><td>${s.serverErrors}</td></tr>
      ${s.verifiedOps !== undefined ? `<tr><th>Operations verified linearizable</th><td>${s.verifiedOps.toLocaleString()}${s.violations ? ` · <b style="color:var(--rpt-bad)">${s.violations} violation(s)</b>` : ""}</td></tr>` : ""}
      ${s.detectMs !== undefined && inc.triggerKind === "action" ? `<tr><th>Time to detect</th><td>${fmtDuration(s.detectMs)}</td></tr>` : ""}
      ${s.recoverAfterHealMs !== undefined ? `<tr><th>Recovery after heal</th><td>${fmtDuration(s.recoverAfterHealMs)}</td></tr>` : ""}
    </table>
  </section>

  ${series.length > 2 ? `<section>
    <h2>Metrics <small>shaded: incident window</small></h2>
    ${spark(series, (x) => x.ops, { label: "throughput", color: "#00a3cc", unit: " ops/s", window: win, fmt: (v) => v.toFixed(0) })}
    ${spark(series, (x) => x.p99, { label: "p99 latency (log)", color: "#e8971f", unit: " ms", window: win, fmt: (v) => v.toFixed(1), log: true })}
    ${spark(series, (x) => x.healthy, { label: "shards with a leader", color: "#7c5cff", unit: "", window: win, fmt: (v) => v.toFixed(0) })}
  </section>` : ""}

  ${fo.length ? `<section>
    <h2>Failovers <small>timed from once-per-second cluster snapshots (±1 s)</small></h2>
    <table class="rpt-table rpt-grid">
      <tr><th>shard</th><th>old leader</th><th>leader lost</th><th>new leader</th><th>failover time</th></tr>
      ${fo.map((f) => `<tr><td>${f.shard}</td><td>${esc(f.from)}</td><td>${fmtTime(f.lostAt)}</td><td>${esc(f.to ?? "—")}</td><td>${failoverTime(f)}</td></tr>`).join("\n      ")}
    </table>
  </section>` : ""}

  <section>
    <h2>Timeline</h2>
    <ol class="rpt-timeline">
      ${tl.map((e) => `<li><time>${fmtTime(e.ts)}</time><span class="rel">+${((e.ts - inc.openedAt) / 1000).toFixed(1)}s</span><span>${ICON[e.kind] ?? "•"}</span><span>${esc(e.detail)}</span></li>`).join("\n      ")}
    </ol>
  </section>

  <section>
    <h2>Recommendations</h2>
    <ul>${recommendations(inc, s).map((r) => `<li>${esc(r)}</li>`).join("")}</ul>
  </section>

  <footer class="rpt-foot">Generated by the DistKV dashboard at ${fmtDate(Date.now())}. Metrics and failover times come from once-per-second cluster snapshots (±1 s).</footer>
</article>`;
}

/** A self-contained HTML document for download or printing to PDF. */
export function reportDocument(inc: Incident): string {
  return `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>DistKV incident #${inc.id}</title>
<style>
body { margin: 0; padding: 32px 16px; background: #f4f6fb; color: #141833; font: 14px/1.55 Inter, ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; }
.rpt { max-width: 860px; margin: 0 auto; background: #fff; border-radius: 14px; padding: 32px 36px; box-shadow: 0 8px 30px rgba(40,50,120,.1); }
${REPORT_CSS}
@media print { body { background: #fff; padding: 0; } .rpt { box-shadow: none; padding: 0; } }
</style></head><body>
${reportHTML(inc)}
</body></html>`;
}

/** Styles for report markup; injected into the dashboard and the export. */
export const REPORT_CSS = `
.rpt { --rpt-muted: #5d6690; --rpt-line: rgba(80,90,180,.18); --rpt-ok: #12a36b; --rpt-bad: #d6334f; --rpt-warn: #c47a00; }
.rpt h1 { font-size: 24px; margin: 4px 0 8px; letter-spacing: -.3px; }
.rpt h2 { font-size: 13px; text-transform: uppercase; letter-spacing: 1.2px; color: var(--rpt-muted); margin: 26px 0 8px; }
.rpt h2 small { text-transform: none; letter-spacing: 0; font-weight: 500; margin-left: 6px; }
.rpt p { margin: 0 0 8px; }
.rpt .muted { color: var(--rpt-muted); }
.rpt-kicker { font-size: 11px; text-transform: uppercase; letter-spacing: 1.4px; color: var(--rpt-muted); font-weight: 700; }
.rpt-meta { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; margin-top: 4px; font-size: 13px; }
.rpt-badge { font-size: 11px; font-weight: 800; letter-spacing: .8px; padding: 2px 8px; border-radius: 999px; border: 1px solid currentColor; }
.rpt-badge.ok { color: var(--rpt-ok); } .rpt-badge.open, .rpt-badge.bad { color: var(--rpt-bad); }
.rpt-badge.sev1 { color: #fff; background: var(--rpt-bad); border-color: var(--rpt-bad); }
.rpt-badge.sev2 { color: #fff; background: var(--rpt-warn); border-color: var(--rpt-warn); }
.rpt-badge.sev3 { color: #fff; background: #5c6bd6; border-color: #5c6bd6; }
.rpt-stats { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; margin-top: 20px; }
@media (max-width: 640px) { .rpt-stats { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
.rpt-stat { border: 1px solid var(--rpt-line); border-radius: 12px; padding: 10px 12px; }
.rpt-stat b { display: block; font-size: 20px; font-variant-numeric: tabular-nums; }
.rpt-stat span { font-size: 11px; color: var(--rpt-muted); text-transform: uppercase; letter-spacing: 1px; font-weight: 700; }
.rpt-stat.good b { color: var(--rpt-ok); } .rpt-stat.bad b { color: var(--rpt-bad); } .rpt-stat.warn b { color: var(--rpt-warn); }
.rpt-findings { list-style: none; padding: 0; margin: 0; }
.rpt-findings li { padding: 6px 10px; border-radius: 8px; margin-bottom: 4px; }
.rpt-findings li.ok { background: rgba(18,163,107,.08); } .rpt-findings li.bad { background: rgba(214,51,79,.08); }
.rpt-table { border-collapse: collapse; width: 100%; font-size: 13px; }
.rpt-table th, .rpt-table td { text-align: left; padding: 6px 8px; border-bottom: 1px solid var(--rpt-line); vertical-align: top; }
.rpt-table th { color: var(--rpt-muted); font-weight: 600; width: 34%; }
.rpt-grid th { width: auto; }
.rpt-spark { margin: 0 0 6px; }
.rpt-spark svg { width: 100%; height: auto; display: block; }
.rpt-window { fill: rgba(214,51,79,.08); stroke: rgba(214,51,79,.35); stroke-dasharray: 3 3; }
.rpt-spark-label { font-size: 12px; fill: var(--rpt-muted); font-weight: 600; }
.rpt-spark-axis { font-size: 11px; fill: var(--rpt-muted); }
.rpt-timeline { list-style: none; padding: 0; margin: 0; font-size: 13px; }
.rpt-timeline li { display: grid; grid-template-columns: 64px 56px 22px 1fr; gap: 6px; padding: 4px 0; border-bottom: 1px dashed var(--rpt-line); }
.rpt-timeline time, .rpt-timeline .rel { font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 11px; color: var(--rpt-muted); padding-top: 2px; }
.rpt-foot { margin-top: 26px; font-size: 11px; color: var(--rpt-muted); }
`;

// ---------------------------------------------------------------------------
// Markdown

export function reportMarkdown(inc: Incident): string {
  const s = summarize(inc);
  const L: string[] = [];
  L.push(`# Incident #${inc.id}: ${capitalize(inc.title)}`, "");
  L.push(`**${inc.status === "open" ? "OPEN" : "RESOLVED"}** · **${inc.severity}** (${SEV_TEXT[inc.severity]})  `);
  L.push(`${fmtDate(inc.openedAt)} → ${inc.resolvedAt ? fmtTime(inc.resolvedAt) : "ongoing"} · duration ${fmtDuration(s.durationMs)}${inc.mode === "sim" ? " · simulation" : ""}`, "");
  L.push("## Summary", "", ...narrative(inc, s).map((p) => p), "");
  L.push("## Findings", "", ...findings(inc, s).map((f) => `- ${f.ok ? "✅" : "❌"} ${f.text}`), "");
  L.push("## Impact", "", "| | |", "|---|---|");
  L.push(`| Trigger | ${inc.trigger} (${inc.triggerKind === "action" ? "chaos lab" : "detected"}) |`);
  L.push(`| Client availability | ${s.availability !== undefined ? pct(s.availability) : "n/a"} |`);
  L.push(`| Failed client ops | ${s.clientFailed ?? "n/a"} |`);
  if (s.verifiedOps !== undefined) L.push(`| Linearizability | ${s.violations ? `❌ ${s.violations} violation(s)` : `✅ ${s.verifiedOps.toLocaleString()} ops verified`} |`);
  L.push(`| Shard failovers | ${s.failovers}${s.failovers ? ` (slowest ${fmtDuration(s.maxFailoverMs)}, median ${fmtDuration(s.medianFailoverMs)})` : ""} |`);
  L.push(`| Lowest healthy shards | ${s.minHealthy} / ${s.total} |`);
  L.push(`| Lowest nodes up | ${s.minUp} / ${s.nodes} |`);
  L.push(`| Peak p99 | ${ms(s.peakP99)} (baseline ${ms(s.baselineP99)}) |`);
  L.push(`| Leader changes | ${inc.leaderChanges} |`);
  L.push(`| Nodes affected | ${inc.nodes.join(", ") || "none"} |`);
  L.push(`| Shards affected | ${[...inc.shards].sort((a, b) => a - b).join(", ") || "none"} |`, "");
  if (inc.failovers.length) {
    L.push("## Failovers", "", "| shard | old leader | lost | new leader | failover time |", "|---|---|---|---|---|");
    for (const f of [...inc.failovers].sort((a, b) => a.lostAt - b.lostAt))
      L.push(`| ${f.shard} | ${f.from} | ${fmtTime(f.lostAt)} | ${f.to ?? "—"} | ${failoverTime(f)} |`);
    L.push("");
  }
  L.push("## Timeline", "");
  for (const e of [...inc.timeline].sort((a, b) => a.ts - b.ts)) L.push(`- \`${fmtTime(e.ts)}\` (+${((e.ts - inc.openedAt) / 1000).toFixed(1)}s) ${e.detail}`);
  L.push("", "## Recommendations", "", ...recommendations(inc, s).map((r) => `- ${r}`), "");
  L.push(`_Generated by the DistKV dashboard at ${fmtDate(Date.now())}. Metrics and failover times come from once-per-second cluster snapshots (±1 s)._`, "");
  return L.join("\n");
}

function failoverTime(f: Failover): string {
  if (!f.electedAt) return "ongoing";
  return `${f.bounded ? "≤ " : ""}${fmtDuration(f.electedAt - f.lostAt)}`;
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}
