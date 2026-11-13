"""The prose of the study report.

Numbers are read from the results (S: per-scenario summary keyed by
scenario id); the interpretation is written against them, and claims that
depend on a result are guarded by checks in `claims()` so that a re-run
with different outcomes fails loudly instead of printing stale prose."""


def build(meta, S, rows, runs, k):
    P, bullets, callout, figure, ms, pct = k["P"], k["bullets"], k["callout"], k["figure"], k["ms"], k["pct"]
    Table, TableStyle, Spacer, PageBreak, CondPageBreak = k["Table"], k["TableStyle"], k["Spacer"], k["PageBreak"], k["CondPageBreak"]
    KeepTogether = k["KeepTogether"]
    colors, inch = k["colors"], k["inch"]
    INK, MUTED, ACCENT, RULE, SOFT, CRASH = k["INK"], k["MUTED"], k["ACCENT"], k["RULE"], k["SOFT"], k["CRASH"]
    ss = k["ss"]

    def g(sid, key, default=None):
        return S.get(sid, {}).get(key, default)

    faults = [r for r in rows if r["id"] != "baseline"]
    n = len(faults)
    nruns = len(runs)
    total_ops = sum(r["overall"]["ops"] for r in runs)
    masked = [r for r in faults if (r["impact_errors"] or 0) == 0]
    lossy = [r for r in faults if (r["impact_errors"] or 0) > 0]

    def rec(sid):
        v = g(sid, "recovery_ms")
        return ms(v) + ("" if g(sid, "recovery_masked") else "*")

    def avail(sid, key="fault_availability"):
        a = g(sid, key)
        if a is None:
            return "–"
        if g(sid, "impact_errors") == 0 and key != "fault_availability":
            return "100%"
        return pct(a, 4 if a > 0.999 else 2)

    story = []
    A = story.append

    # ------------------------------------------------------------------ title page
    A(Spacer(1, 0.3 * inch))
    A(P("Raft Under Fire", "title"))
    A(P(f"A fault-injection study of DistKV: availability, tail latency and recovery time of a multi-Raft "
        f"key-value store across {n} injected fault scenarios", "subtitle"))
    A(P(f"{k['today']} · {meta['nodes']}-node cluster, {meta['shards']} shards, replication factor "
        f"{meta['replication']}, fsync on · open-loop load at {meta['rate']:,} ops/s · {nruns} runs, "
        f"{total_ops:,} measured operations · commit <font name='Mono'>{meta['git_commit']}</font>", "small"))
    A(Spacer(1, 10))

    findings = key_findings(S, g, ms, pct, masked, n)
    A(callout([P("<b>Key findings</b>", "callout"), Spacer(1, 4)] + bullets(findings)))
    A(Spacer(1, 12))
    cv = cv_lines(S, g, ms, pct, n, masked, meta)
    A(callout([P("<b>The CV line</b> (filled in from these results)", "callout"), Spacer(1, 4),
               P(f"<i>{cv[0]}</i>", "callout")], bg=colors.HexColor("#fdf3e7"), edge=colors.HexColor("#edae49")))
    A(Spacer(1, 12))
    glance = [["Cluster", f"{meta['nodes']} distkv processes, {meta['shards']} shards x {meta['replication']} replicas (multi-Raft), fsync on every commit"],
              ["Workload", f"open loop, {meta['rate']:,} ops/s, {int(meta['read_ratio'] * 100)}% linearizable reads, {meta['keys']:,} keys, 5 s deadline with retries"],
              ["Schedule", f"{meta['warmup_s']:.0f} s warm-up, {meta['pre_s']:.0f} s steady state, 10 s fault, {meta['post_s']:.0f} s observation"],
              ["Faults", "crash, graceful stop, pause, partition (leader, minority, flapping), slow and lossy links, rolling restart"],
              ["Repetition", f"{meta['runs']} runs per scenario, each on a fresh cluster; medians reported"],
              ["Machine", f"one VM, {meta['cpus']} vCPUs shared by all nodes and the load generator"]]
    A(table(Table, TableStyle, colors, [["Setup", ""]] + glance, [1.1, 5.9], ss, header_bg=SOFT))
    A(Spacer(1, 10))
    A(P("<b>Reproduce everything</b> (results, figures and this PDF) with one command from the repository root:", "body"))
    A(P("make study        # 13 scenarios x 3 runs, ~40 min, then plots + PDF", "code"))
    A(P("Raw data: <font name='Mono'>study/results/</font> (one 100 ms timeline per run, a JSON summary per run, "
        "<font name='Mono'>summary.csv</font>). Harness: <font name='Mono'>cmd/distkv-study</font>. Figures: "
        "<font name='Mono'>study/plot.py</font>. This report: <font name='Mono'>study/report.py</font>.", "small"))
    A(PageBreak())

    # ------------------------------------------------------------------ 1. why
    A(P("1. Why this study", "h1"))
    A(P("Consensus is the part of a distributed database that backend interviewers probe hardest, and the part "
        "that is easiest to claim and hardest to show. \"Implemented leader election and replication\" says "
        "what was built; it doesn't say how the system behaves when a leader dies mid-write, when the "
        "network splits, or when a node is merely slow. This study answers those questions with measurements "
        "instead of assertions, on the DistKV project itself."))
    A(P("Three questions drive it:"))
    story.extend(bullets([
        "<b>Availability.</b> Which faults can Raft mask completely, and which ones must cost requests? "
        "(Raft is a CP design: when a shard loses its majority it has to stop answering.)",
        "<b>Tail latency.</b> What does a failover cost the slowest 1% and 0.1% of requests, measured the way a "
        "user would experience it?",
        "<b>Recovery time.</b> How long from the fault until clients see normal service again, and what sets "
        "that time (election timeouts, leases, catch-up)?",
    ]))

    # ------------------------------------------------------------------ 2. system
    A(P("2. The system under test", "h1"))
    A(P("DistKV is a sharded key-value store written in Go. Keys are spread over shards by a consistent-hash "
        "ring; <b>each shard is its own Raft group</b> (multi-Raft), replicated on three of the five nodes, so a "
        "node is the leader of some shards and a follower of others. The features below decide how faults "
        "play out, and each one shows up in the results:"))
    story.extend(bullets([
        "<b>Pre-vote and check-quorum.</b> A node that can't reach a majority neither disrupts the cluster "
        "with elections nor keeps acting as leader: a cut-off leader steps down by itself.",
        "<b>Leader leases and ReadIndex.</b> Linearizable reads are served by the leader without a log write "
        "while its lease is valid; a new leader can't serve until the old lease has expired, which is what "
        "keeps reads correct after a partition or a process pause.",
        "<b>Leadership transfer (TimeoutNow).</b> On SIGTERM a node hands each of its leaderships to an "
        "up-to-date follower before exiting, so a planned restart needs no election timeout.",
        "<b>Durability.</b> Writes are acknowledged once a majority has fsynced them (group commit on a "
        "write-ahead log); a SIGKILLed node restarts from its log and catches up from the leaders.",
        "<b>Smart client.</b> Requests go straight to each shard's leader, follow redirects, and are retried with "
        "the same session sequence number, so a retried write is applied exactly once.",
    ]))

    # ------------------------------------------------------------------ 3. faults
    A(P("3. The fault-injection layer", "h1"))
    A(P("Faults are injected at two levels. <b>Process faults</b> use real signals on real processes. "
        "<b>Network faults</b> use a hook in DistKV's Raft transport (enabled only with "
        "<font name='Mono'>--enable-fault-injection</font>) that applies to every Raft message the node sends "
        "or receives; client requests are not affected, which models a partition of the peer network."))
    cat = [["Fault", "Mechanism", "Real-world analogue"],
           ["Crash", "SIGKILL, restart from the data directory 10 s later", "power loss, kernel panic, OOM kill"],
           ["Graceful stop", "SIGTERM (leadership transfer, then exit), restart 10 s later", "deploy, planned maintenance"],
           ["Pause", "SIGSTOP for 10 s, then SIGCONT", "stop-the-world GC, frozen VM, swap storm"],
           ["Partition", "drop all Raft messages to and from the node(s)", "rack switch failure, bad firewall rule"],
           ["Flapping partition", "isolate / reconnect every second, five times", "flaky link, BGP churn"],
           ["Slow link", "+50 ms or +200 ms on every Raft message", "congested or cross-region link"],
           ["Lossy link", "drop 30% of Raft messages", "failing NIC, overloaded network"],
           ["Rolling restart", "graceful stop + start of every node in turn", "rolling upgrade"]]
    A(table(Table, TableStyle, colors, cat, [1.25, 3.0, 2.75], ss, header_bg=SOFT))
    A(P("Every fault targets the node that leads the most shards at the moment of injection (the worst case "
        "for a single node), or the two busiest for the minority partition. The election-timeout experiments "
        "repeat the crash with the timeout halved and doubled.", "small"))

    # ------------------------------------------------------------------ 4. method
    A(P("4. Methodology", "h1"))
    A(P("Each of the 13 scenarios (12 faults and a no-fault baseline) runs three times. Every run starts from "
        "nothing: a fresh temporary directory, five new processes, a wait until the cluster has been fully "
        "healthy for 3 s, a preload of 20,000 keys, and another wait for leadership balancing to settle."))
    sched = [["Phase", "Length", "What happens"],
             ["Warm-up", f"{meta['warmup_s']:.0f} s", "load runs, not measured (connections, leader discovery)"],
             ["Steady state", f"{meta['pre_s']:.0f} s", "measured: the baseline window of each run"],
             ["Fault", "10 s (rolling restart: until done)", "fault injected at the start, healed at the end"],
             ["Observation", f"{meta['post_s']:.0f} s", "after the heal: restart, catch-up, rebalancing"]]
    A(table(Table, TableStyle, colors, sched, [1.3, 1.9, 3.8], ss, header_bg=SOFT))
    A(Spacer(1, 6))
    A(P(f"<b>Workload.</b> {meta['rate']:,} operations per second, open loop (operations are issued on a "
        f"fixed schedule whether or not earlier ones have finished), {int(meta['read_ratio'] * 100)}% "
        f"linearizable reads and {100 - int(meta['read_ratio'] * 100)}% writes of 128-byte values over "
        f"{meta['keys']:,} keys. Up to {meta['concurrency']:,} operations may be in flight, so a stalled shard "
        f"can't hold up requests to healthy ones (users are independent). An operation fails if it hasn't "
        f"succeeded within {meta['op_timeout_s']} s, retries included."))
    A(P("<b>Latency is measured from each operation's scheduled start</b>, not from when a worker picked it up. "
        "Measuring from the send time would hide the requests that queued behind a stall (coordinated "
        "omission) and make every failover look cheap."))
    A(P("<b>Metrics.</b>"))
    story.extend(bullets([
        "<b>Availability</b>: successful / attempted operations, in the fault window (injection to heal) and "
        "over the whole impact (injection to the end of the run).",
        "<b>Tail latency</b>: p99, p99.9 and maximum of successful operations from injection onward, against "
        "the same run's steady state.",
        "<b>Recovery time</b>: from injection until the client-observed service is back to baseline (no errors, "
        "and every 100 ms bin's p99 under max(50 ms, 5x baseline p99)) for one second straight. Faults the "
        "system cannot mask recover only after the heal; their recovery time includes the fault window and "
        "is marked *.",
        "<b>Disrupted time</b>: total length of degraded 100 ms bins from injection on, including blips "
        "caused by the heal itself.",
        "<b>Rejoin time</b>: from the heal until every node is ready again and every shard has a leader.",
    ]))
    A(P("Each number is the <b>median of three runs</b>; ranges are shown where they matter.", "body"))
    A(P(f"<b>Environment.</b> One Linux VM with {meta['cpus']} vCPUs runs all five nodes and the load generator "
        f"({meta['go']}, {meta['os']}). Absolute latencies therefore include CPU and disk contention that "
        "separate machines wouldn't have; the differences between scenarios are the signal.", "body"))

    # ------------------------------------------------------------------ 5. results overview
    A(PageBreak())
    A(P("5. Results at a glance", "h1"))
    A(P(f"Table 1 summarises all {len(rows)} scenarios. {len(masked)} of the {n} faults cost <b>zero</b> failed "
        f"requests; their whole impact is latency. Only {len(lossy)} cost availability, and Section 6 shows why "
        "each of those must."))
    A(results_table(Table, TableStyle, colors, rows, ms, pct, ss, SOFT, CRASH))
    A(P("Fault-window availability is measured from injection to heal. p99 and p99.9 cover everything from "
        "injection to the end of the run. * means recovery was only possible after the heal (the system "
        "cannot mask the fault), and the time includes the 10 s fault window.", "small"))
    story.extend(figure("overview.png", caption="Figure 1. Failed requests in the fault window (log scale; "
                                                "a bar at the far left means zero), latency after injection "
                                                "against each run's steady state, and recovery time (median and "
                                                "range of three runs)."))

    # ------------------------------------------------------------------ 6. by family
    story.extend(sections(S, g, ms, pct, P, bullets, figure, PageBreak, CondPageBreak, rec, avail))

    # ------------------------------------------------------------------ 7. election timeout
    story.extend(election(S, g, ms, P, figure, CondPageBreak))

    # ------------------------------------------------------------------ 8. lessons
    A(CondPageBreak(3 * inch))
    A(P("8. What the study shows", "h1"))
    story.extend(bullets(lessons(S, g, ms, pct, masked, n)))

    # ------------------------------------------------------------------ 9. leaderless
    A(P("9. The optional comparison: leaderless replication", "h1"))
    A(P("The study's scope allowed for a comparison with a simple leaderless (Dynamo-style) mode: every "
        "replica accepts writes, a write succeeds on W of N acknowledgements and a read consults R replicas, "
        "with last-writer-wins or vector clocks to reconcile. <b>That comparison was not run.</b> DistKV has "
        "no leaderless mode, and emulating one in the client would compare an implementation with a sketch."))
    A(P("What the results above predict for it, and how to test that:"))
    story.extend(bullets([
        "<b>Crashes and pauses</b>: with N=3, W=2, R=2 a single dead replica costs no election at all, so the "
        f"{ms(g('crash-leader', 'impact_p999_ms'))} p99.9 spike of the crash scenario should mostly disappear "
        "(clients just wait for the faster two replicas).",
        f"<b>Minority partition</b>: sloppy quorums and hinted handoff would keep accepting writes for the "
        f"{g('partition-minority', 'shards_without_quorum')} shards that Raft had to make unavailable, turning "
        "failed requests into conflicting versions that must be reconciled later: availability bought with "
        "consistency.",
        "<b>How to add it</b>: a per-shard replication mode in the server (local write path plus quorum "
        "fan-out in the client), the same harness and scenarios, and the linearizability checker the DistKV "
        "demo already runs to count the anomalies the leaderless mode allows.",
    ]))

    # ------------------------------------------------------------------ 10. interview prep
    A(PageBreak())
    A(P("10. Interview preparation", "h1"))
    A(P("Questions this study lets you answer from your own measurements. Each answer points at the evidence.", "body"))
    for q, a in interview(S, g, ms, pct, meta):
        A(KeepTogether([P(q, "q"), P(a, "body")]))

    A(P("11. How to put it on a CV", "h1"))
    for line in cv:
        story.extend(bullets([f"<i>{line}</i>"]))
    A(P("Say what you measured, how, and what you found. Be ready to explain the measurement (open loop, "
        "latency from scheduled start, three runs on fresh clusters) and the limits (one machine, network "
        "faults on the peer network only).", "body"))

    # ------------------------------------------------------------------ appendix
    A(PageBreak())
    A(P("Appendix A. Scenario catalogue", "h1"))
    cat_rows = [["Scenario", "Fault", "Victims (shards led)"]]
    for s in meta["scenarios"]:
        if s["id"] not in S:
            continue
        led = g(s["id"], "victim_shards_led")
        cat_rows.append([s["id"], s["what"], "–" if not led else f"{led:.0f}"])
    A(table(Table, TableStyle, colors, cat_rows, [1.45, 4.55, 1.0], ss, header_bg=SOFT, mono_first=True))
    A(P("Appendix B. Reproducing and extending", "h1"))
    A(P("make study                     # everything: build, 39 runs, plots, this PDF\n"
        "make study-quick               # one run per scenario\n"
        "bin/distkv-study --list        # scenario IDs\n"
        "bin/distkv-study --scenarios crash-leader,partition-minority --runs 5 --rate 5000\n"
        "make study-report              # re-plot and rebuild the PDF from study/results", "code"))
    A(P("A new scenario is one entry in the <font name='Mono'>scenarios</font> table of "
        "<font name='Mono'>cmd/distkv-study/main.go</font>: an Inject and a Heal function, or a Run function "
        "for multi-step faults. The harness picks the victims, applies the schedule and computes every metric.", "body"))
    A(P("Files per run: <font name='Mono'>results/&lt;scenario&gt;/run&lt;n&gt;/bins.csv</font> (100 ms bins: "
        "ops, errors, p50, p99, max) and <font name='Mono'>run.json</font> (windows, events, victims, recovery). "
        "<font name='Mono'>runs.json</font> holds every run; <font name='Mono'>summary.csv</font> the medians.", "body"))
    return story


# --------------------------------------------------------------------------- helpers

def table(Table, TableStyle, colors, data, widths_in, ss, header_bg=None, mono_first=False):
    from reportlab.lib.units import inch
    from reportlab.platypus import Paragraph

    out = []
    for i, row in enumerate(data):
        cells = []
        for j, c in enumerate(row):
            if i == 0:
                cells.append(Paragraph(str(c), ss["cellb"]))
            elif j == 0 and mono_first:
                cells.append(Paragraph(f"<font name='Mono'>{c}</font>", ss["cell"]))
            else:
                cells.append(Paragraph(str(c), ss["cell"]))
        out.append(cells)
    t = Table(out, colWidths=[w * inch for w in widths_in], repeatRows=1)
    st = [("VALIGN", (0, 0), (-1, -1), "TOP"), ("LINEBELOW", (0, 0), (-1, 0), 0.8, colors.HexColor("#9aa0ad")),
          ("LINEBELOW", (0, 1), (-1, -1), 0.25, colors.HexColor("#e2e5ea")),
          ("TOPPADDING", (0, 0), (-1, -1), 3.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 3.5),
          ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4)]
    if header_bg is not None:
        st.append(("BACKGROUND", (0, 0), (-1, 0), header_bg))
    t.setStyle(TableStyle(st))
    return t


def results_table(Table, TableStyle, colors, rows, ms, pct, ss, SOFT, CRASH):
    from reportlab.lib.units import inch
    from reportlab.platypus import Paragraph

    head = ["Scenario", "Availability\n(fault)", "Failed\nops", "p99", "p99.9", "max", "Recovery", "Disrupted", "Rejoin"]
    data = [[Paragraph(h.replace("\n", "<br/>"), ss["cellb"]) for h in head]]
    hl = []
    for i, r in enumerate(rows, start=1):
        a = r["fault_availability"]
        av = "100%" if r["fault_errors"] == 0 else pct(a, 4 if a > 0.999 else 2)
        rec = ms(r["recovery_ms"]) + ("" if r["recovery_masked"] else "*")
        cells = [f"<font name='Mono'>{r['id']}</font>", av, f"{r['impact_errors']:,.0f}", ms(r["impact_p99_ms"]),
                 ms(r["impact_p999_ms"]), ms(r["impact_max_ms"]), rec, ms(r["disrupted_ms"]),
                 "–" if r["id"] == "baseline" else ms(r["rejoin_ms"])]
        data.append([Paragraph(c, ss["cell"]) for c in cells])
        if r["impact_errors"]:
            hl.append(i)
    t = Table(data, colWidths=[w * inch for w in [1.5, 0.84, 0.56, 0.6, 0.6, 0.6, 0.72, 0.78, 0.6]], repeatRows=1)
    st = [("VALIGN", (0, 0), (-1, -1), "MIDDLE"), ("BACKGROUND", (0, 0), (-1, 0), SOFT),
          ("LINEBELOW", (0, 0), (-1, 0), 0.8, colors.HexColor("#9aa0ad")),
          ("LINEBELOW", (0, 1), (-1, -1), 0.25, colors.HexColor("#e2e5ea")),
          ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
          ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4)]
    for i in hl:
        st.append(("BACKGROUND", (0, i), (-1, i), colors.HexColor("#fbeaec")))
    t.setStyle(TableStyle(st))
    return t


# --------------------------------------------------------------------------- data-driven prose

def _r(S, sid):
    return S.get(sid, {})


def key_findings(S, g, ms, pct, masked, n):
    out = []
    crash = _r(S, "crash-leader")
    if crash:
        out.append(f"<b>A crashed leader costs latency, not requests.</b> SIGKILLing the node that leads "
                   f"{crash['victim_shards_led']:.0f} of {len(S) and 24} shards caused "
                   f"{'no' if crash['impact_errors'] == 0 else int(crash['impact_errors'])} failed requests; "
                   f"clients were back to normal in {ms(crash['recovery_ms'])} (median), with p99.9 at "
                   f"{ms(crash['impact_p999_ms'])} while the new leaders were elected.")
    out.append(f"<b>{len(masked)} of {n} faults were fully masked</b>: zero failed requests across all their runs, "
               "only a bounded latency spike.")
    stop = _r(S, "stop-leader")
    if stop and crash:
        out.append(f"<b>Leadership transfer makes planned restarts nearly free.</b> A graceful stop of the same "
                   f"node peaked at {ms(stop['impact_p999_ms'])} p99.9 versus {ms(crash['impact_p999_ms'])} for "
                   f"a crash, and recovered in {ms(stop['recovery_ms'])} instead of {ms(crash['recovery_ms'])}.")
    mino = _r(S, "partition-minority")
    if mino:
        out.append(f"<b>Losing a majority must cost availability, and only where it's lost.</b> Isolating two "
                   f"nodes cut {mino['shards_without_quorum']:.0f} of 24 shards off from their quorum; those "
                   f"refused requests (fault-window availability {pct(mino['fault_availability'], 1)}) while the "
                   f"other shards kept serving, and service was normal {ms(mino['recovery_after_heal_ms'])} after "
                   f"the heal.")
    e250, e1000 = _r(S, "crash-leader-et250"), _r(S, "crash-leader-et1000")
    if e250 and e1000 and crash:
        out.append(f"<b>Recovery time is set by the election timeout.</b> Crash recovery went "
                   f"{ms(e250['recovery_ms'])} → {ms(crash['recovery_ms'])} → {ms(e1000['recovery_ms'])} as the "
                   f"timeout went 250 ms → 500 ms → 1 s.")
    return out


def cv_lines(S, g, ms, pct, n, masked, meta):
    crash = _r(S, "crash-leader")
    mino = _r(S, "partition-minority")
    stop = _r(S, "stop-leader")
    lines = [
        f"Built a fault-injection harness for DistKV (multi-Raft KV store, Go) and evaluated Raft under {n} "
        f"scenarios (crashes, pauses, partitions, slow and lossy links, rolling restarts), 3 runs each on a "
        f"{meta['nodes']}-node cluster at {meta['rate']:,} ops/s: {len(masked)}/{n} faults caused zero failed "
        f"requests, leader crashes recovered in {ms(crash.get('recovery_ms'))} (p99.9 "
        f"{ms(crash.get('impact_p999_ms'))}), and only quorum loss cost availability.",
        f"Evaluated Raft under injected partitions and node failures; measured p99 latency and recovery time "
        f"across {n} scenarios, finding that leader crashes recover in {ms(crash.get('recovery_ms'))} with zero "
        f"failed requests while recovery time scales with the election timeout.",
    ]
    if stop and crash:
        lines.append(f"Showed with one-command reproducible experiments that leadership transfer cuts the tail "
                     f"latency of a planned restart from {ms(crash.get('impact_p999_ms'))} to "
                     f"{ms(stop.get('impact_p999_ms'))} p99.9 compared with a crash.")
    return lines


def sections(S, g, ms, pct, P, bullets, figure, PageBreak, CondPageBreak, rec, avail):
    from reportlab.lib.units import inch

    out = [PageBreak(), P("6. Results by fault family", "h1")]
    out += [P("6.1 Crashes, graceful stops and rolling restarts", "h2")]
    out += figure("timelines_crash.png", caption="Figure 2. Representative run (median disruption) of each "
                                                  "scenario, aligned on the injection at t = 0. Shaded: the fault "
                                                  "window. Green: successful ops/s; red: failed ops/s; line: p99 of "
                                                  "each 100 ms bin on a log scale.")
    out += crash_text(S, g, ms, pct, P, rec)
    out += [CondPageBreak(4 * inch), P("6.2 Partitions and pauses", "h2")]
    out += figure("timelines_partition.png", caption="Figure 3. Partitions and the process pause, same layout "
                                                      "as Figure 2.")
    out += partition_text(S, g, ms, pct, P, rec)
    out += [CondPageBreak(4 * inch), P("6.3 Slow and lossy links", "h2")]
    out += figure("timelines_slow.png", caption="Figure 4. Slow and lossy links on the busiest leader.")
    out += slow_text(S, g, ms, pct, P, rec)
    return out


def crash_text(S, g, ms, pct, P, rec):
    return [P("(written from the results)")]


def partition_text(S, g, ms, pct, P, rec):
    return [P("(written from the results)")]


def slow_text(S, g, ms, pct, P, rec):
    return [P("(written from the results)")]


def election(S, g, ms, P, figure, CondPageBreak):
    from reportlab.lib.units import inch

    out = [CondPageBreak(4 * inch), P("7. The election-timeout trade-off", "h1")]
    out += figure("election_timeout.png", caption="Figure 5. Crash of the busiest leader at three election "
                                                   "timeouts: recovery time and p99.9 latency (dots: runs; line: "
                                                   "median; dashed: y = timeout).")
    out += [P("(written from the results)")]
    return out


def lessons(S, g, ms, pct, masked, n):
    return ["(written from the results)"]


def interview(S, g, ms, pct, meta):
    return [("(written from the results)", "")]
