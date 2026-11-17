"""The prose of the study report.

Numbers are read from the results (S: per-scenario summary keyed by
scenario id) and the interpretation is written against them. Wording that
depends on an outcome (zero failures, no disruption) is conditional on the
data, so a re-run regenerates consistent text; the interpretation itself was
written for these results and should be re-read after a re-run."""


OLD_CLIENT = "pause-leader-no-attempt-timeout"
K = {}  # report helpers (diagram, cite, diagrams module), set by build()


def build(meta, S, rows, runs, k):
    P, bullets, callout, figure, ms, pct = k["P"], k["bullets"], k["callout"], k["figure"], k["ms"], k["pct"]
    Table, TableStyle, Spacer, PageBreak, CondPageBreak = k["Table"], k["TableStyle"], k["Spacer"], k["PageBreak"], k["CondPageBreak"]
    KeepTogether = k["KeepTogether"]
    colors, inch = k["colors"], k["inch"]
    INK, MUTED, ACCENT, RULE, SOFT, CRASH = k["INK"], k["MUTED"], k["ACCENT"], k["RULE"], k["SOFT"], k["CRASH"]
    ss = k["ss"]
    K.update(k)
    cite, diagram, D, quote, StatTiles = k["cite"], k["diagram"], k["diagrams"], k["quote"], k["StatTiles"]
    NAVY, AMBER = k["NAVY"], k["AMBER"]

    def g(sid, key, default=None):
        return S.get(sid, {}).get(key, default)

    # The old-client pause is a before/after comparison, not a separate fault.
    faults = [r for r in rows if r["id"] not in ("baseline", OLD_CLIENT)]
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

    # ------------------------------------------------------------------ executive summary
    c, pa, pn, pm = S["crash-leader"], S["pause-leader"], S[OLD_CLIENT], S["partition-minority"]
    A(P("Executive summary", "h1"))
    A(P(f"Twelve kinds of failure were injected into a live five-node DistKV cluster while it served "
        f"{meta['rate']:,} requests a second, three times each, on a fresh cluster every time. Every request's outcome "
        f"was recorded, {total_ops:,} in all. The question was not whether Raft works on paper "
        f"{cite(1, 2)}, but what each failure costs real clients in availability, tail latency and recovery "
        f"time, and why.", "lead"))
    A(StatTiles([(ms(c["recovery_ms"]), "recovery from a crashed\nleader, zero failures", CRASH),
                 (f"{len(masked)}/{n}", "faults with zero\nfailed requests", ACCENT),
                 (f"{pm['shards_without_quorum']:.0f}/24", "shards refused requests\nwhen a majority was lost", AMBER),
                 (f"{ms(pn['recovery_ms'])} → {ms(pa['recovery_ms'])}", "paused-leader recovery,\nbefore → after the fix", NAVY)]))
    A(Spacer(1, 12))
    findings = key_findings(S, g, ms, pct, masked, n)
    A(callout([P("<b>Key findings</b>", "callout"), Spacer(1, 4)] + bullets(findings)))
    A(Spacer(1, 10))
    cv = cv_lines(S, g, ms, pct, n, masked, meta)
    A(callout([P("<b>The CV line</b>, filled in from these results", "callout"), Spacer(1, 4),
               P(f"<i>“{cv[0]}”</i>", "callout")], bg=colors.HexColor("#fdf3e7"), edge=AMBER))
    A(PageBreak())

    # ------------------------------------------------------------------ 1. why
    A(P("1. Why this study", "h1"))
    A(P("Consensus is the part of a distributed database that backend interviewers probe hardest, and the part "
        "that is easiest to claim and hardest to show. \"Implemented leader election and replication\" says "
        "what was built; it doesn't say how the system behaves when a leader dies mid-write, when the "
        "network splits, or when a node is merely slow. This study answers those questions with measurements "
        "instead of assertions, on the DistKV project itself."))
    A(P(f"The failures are not hypothetical. A study of 136 network-partitioning failures across 25 widely used "
        f"distributed systems found that most of them had catastrophic effects, from data loss to broken locks "
        f"{cite(12)}; years of Jepsen analyses have found consistency bugs in one production database after "
        f"another {cite(13)}; and partially failed, slow or frozen nodes (gray failures) are behind many cloud "
        f"incidents precisely because the system's own failure detectors don't see them {cite(8)}. Injecting "
        f"faults on purpose and measuring what clients experience is the practice Netflix named chaos "
        f"engineering {cite(9)}."))
    A(P("Three questions drive it:"))
    story.extend(bullets([
        "<b>Availability.</b> Which faults can Raft mask completely, and which ones must cost requests? "
        "(Raft is a CP design: when a shard loses its majority it has to stop answering.)",
        f"<b>Tail latency.</b> What does a failover cost the slowest 1% and 0.1% of requests, measured the way a "
        f"user would experience it? At scale, the tail is what users feel {cite(11)}.",
        "<b>Recovery time.</b> How long from the fault until clients see normal service again, and what sets "
        "that time (election timeouts, leases, catch-up)?",
    ]))

    # ------------------------------------------------------------------ 2. system
    A(P("2. The system under test", "h1"))
    A(P(f"DistKV is a sharded key-value store written in Go. Keys are spread over shards by a consistent-hash "
        f"ring; <b>each shard is its own Raft group</b> {cite(1)} (multi-Raft), replicated on three of the five "
        f"nodes, so a node is the leader of some shards and a follower of others (Figure {K['fig_next']()}). A write commits once "
        f"a majority of its shard's replicas have it on disk; reads are linearizable {cite(7)}."))
    story.extend(diagram(D.architecture(), "DistKV's architecture. The client hashes each key to a shard and sends "
                                           "the request straight to that shard's leader. Each node hosts a mix of "
                                           "leader and follower replicas, so losing a node removes some leaders "
                                           "but no shard's majority."))
    A(P("The features below decide how faults play out, and each one shows up in the results:"))
    story.extend(bullets([
        f"<b>Pre-vote and check-quorum</b> {cite(2, 14)}. A node that can't reach a majority neither disrupts the "
        f"cluster with elections nor keeps acting as leader: a cut-off leader steps down by itself.",
        f"<b>Leader leases and ReadIndex</b> {cite(2, 3)}. Linearizable reads are served by the leader without a "
        f"log write while its lease is valid; a new leader can't serve until the old lease has expired, which is "
        f"what keeps reads correct after a partition or a process pause.",
        f"<b>Leadership transfer (TimeoutNow)</b> {cite(2)}. On SIGTERM a node hands each of its leaderships to an "
        f"up-to-date follower before exiting, so a planned restart needs no election timeout.",
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
    story.extend(diagram(D.fault_layer(), "Where each fault enters. Signals act on the whole process; the transport "
                                          "hook drops or delays Raft messages in both directions; client requests "
                                          "always get through, so clients keep talking to a partitioned node."))
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
    glance = [["Cluster", f"{meta['nodes']} distkv processes, {meta['shards']} shards x {meta['replication']} replicas (multi-Raft), fsync on every commit"],
              ["Workload", f"open loop, {meta['rate']:,} ops/s, {int(meta['read_ratio'] * 100)}% linearizable reads, {meta['keys']:,} keys, 5 s deadline with retries"],
              ["Schedule", f"{meta['warmup_s']:.0f} s warm-up, {meta['pre_s']:.0f} s steady state, 10 s fault, {meta['post_s']:.0f} s observation"],
              ["Faults", "crash, graceful stop, pause, partition (leader, minority, flapping), slow and lossy links, rolling restart"],
              ["Repetition", f"{meta['runs']} runs per scenario, each on a fresh cluster; medians reported"],
              ["Machine", f"one VM, {meta['cpus']} vCPUs shared by all nodes and the load generator"],
              ["Reproduce", "<font name='Mono'>make study</font> (~40 min): results, figures and this PDF"]]
    A(table(Table, TableStyle, colors, glance, [1.1, 5.9], ss, zebra=True))
    A(Spacer(1, 8))
    A(P(f"Each of the {len(rows)} scenarios ({n} faults, the old-client comparison and a no-fault baseline) runs "
        "three times. Every run starts from "
        "nothing: a fresh temporary directory, five new processes, a wait until the cluster has been fully "
        "healthy for 3 s, a preload of 20,000 keys, and another wait for leadership balancing to settle."))
    story.extend(diagram(D.timeline(meta), "Anatomy of a run and the window each metric uses. Load starts with the "
                                           "warm-up and never pauses; the fault is injected after 10 s of steady "
                                           "state and healed 10 s later (the rolling restart heals when it finishes)."))
    A(P(f"<b>Workload.</b> {meta['rate']:,} operations per second, open loop (operations are issued on a "
        f"fixed schedule whether or not earlier ones have finished), {int(meta['read_ratio'] * 100)}% "
        f"linearizable reads and {100 - int(meta['read_ratio'] * 100)}% writes of 128-byte values over "
        f"{meta['keys']:,} keys. Up to {meta['concurrency']:,} operations may be in flight, so a stalled shard "
        f"can't hold up requests to healthy ones (users are independent). An operation fails if it hasn't "
        f"succeeded within {meta['op_timeout_s']} s, retries included."))
    A(P("<b>Latency is measured from each operation's scheduled start</b>, not from when a worker picked it up. "
        "Measuring from the send time would hide the requests that queued behind a stall (coordinated "
        f"omission {cite(10)}) and make every failover look cheap."))
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
        "<b>Rejoin time</b>: from the heal until every node is ready again and every shard has a leader "
        "(cluster status polled every 100 ms from a node the fault didn't touch).",
    ]))
    A(P("Each number is the <b>median of three runs</b>; ranges are shown where they matter.", "body"))
    A(P(f"<b>Environment.</b> One Linux VM with {meta['cpus']} vCPUs runs all five nodes and the load generator "
        f"({meta['go']}, {meta['os']}). Absolute latencies therefore include CPU and disk contention that "
        "separate machines wouldn't have; the differences between scenarios are the signal.", "body"))
    A(P("<b>Threats to validity.</b> Three runs per scenario show the spread but are not enough for confidence "
        "intervals. Times are resolved to 100 ms bins. Network faults act on Raft traffic between nodes only; "
        "client-to-node links stay healthy. The victim is the busiest leader at injection time, which varies "
        "between runs (5 to 11 shards), and the leader balancer keeps making its routine transfers during a "
        "fault. One machine runs everything, so the absolute numbers are pessimistic for a real deployment and "
        "optimistic for a cross-region one.", "body"))

    # ------------------------------------------------------------------ 5. results overview
    A(PageBreak())
    A(P("5. Results at a glance", "h1"))
    A(P(f"Table 1 summarises all {len(rows)} scenarios. With the fixed client, {len(masked)} of the {n} faults cost "
        f"<b>zero</b> failed requests in every run; their whole impact is latency. The one exception, losing a "
        f"majority, must cost availability (Section 6.2). Shaded rows lost requests; the "
        f"<font name='Mono'>pause-leader (old client)</font> row is the same pause with the client as it was "
        f"before this study, the bug the study found (Section 6.2)."))
    A(results_table(Table, TableStyle, colors, rows, ms, pct, ss, SOFT, CRASH))
    A(P("Fault-window availability is measured from injection to heal. p99 and p99.9 cover everything from "
        "injection to the end of the run. * means recovery was only possible after the heal (the system "
        "cannot mask the fault), and the time includes the 10 s fault window.", "small"))
    story.extend(figure("overview.png", caption="Failed requests in the fault window (log scale; "
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
    A(quote("Raft did its job every time. The one real outage in this study was a client that kept waiting on a "
            "frozen leader the cluster had already replaced."))
    A(Spacer(1, 8))
    story.extend(bullets(lessons(S, g, ms, pct, masked, n)))

    # ------------------------------------------------------------------ 9. leaderless
    A(P("9. The optional comparison: leaderless replication", "h1"))
    A(P(f"The study's scope allowed for a comparison with a simple leaderless (Dynamo-style) mode: every "
        f"replica accepts writes, a write succeeds on W of N acknowledgements and a read consults R replicas, "
        f"with last-writer-wins or vector clocks to reconcile {cite(6)}. <b>That comparison was not run.</b> DistKV has "
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
        cat_rows.append([s["id"].replace("-no-attempt-timeout", "-no-attempt-<br/>timeout"), s["what"], "–" if not led else f"{led:.0f}"])
    A(table(Table, TableStyle, colors, cat_rows, [1.95, 4.15, 0.9], ss, header_bg=SOFT, mono_first=True))
    A(P("Appendix B. Reproducing and extending", "h1"))
    A(P(f"make study                     # everything: build, {nruns} runs, plots, this PDF\n"
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
    A(CondPageBreak(3 * inch))
    A(P("Appendix C. Glossary", "h1"))
    for term, meaning in GLOSSARY:
        A(P(f"<b><font color='#12192b'>{term}</font></b>: {meaning}", "term"))
    A(PageBreak())
    A(P("References", "h1"))
    for i, ref in enumerate(REFERENCES, start=1):
        A(P(f"<font color='#00798c'><b>[{i}]</b></font>  {ref}", "ref"))
    return story


# --------------------------------------------------------------------------- helpers

def table(Table, TableStyle, colors, data, widths_in, ss, header_bg=None, mono_first=False, zebra=False):
    from reportlab.lib.units import inch
    from reportlab.platypus import Paragraph

    out = []
    for i, row in enumerate(data):
        cells = []
        for j, c in enumerate(row):
            if i == 0 and not zebra:
                cells.append(Paragraph(str(c), ss["cellb"]))
            elif j == 0 and zebra:
                cells.append(Paragraph(f"<b>{c}</b>", ss["cellb"]))
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
    start = 0 if zebra else 1
    st.append(("ROWBACKGROUNDS", (0, start), (-1, -1), [colors.white, colors.HexColor("#f6f7fa")]))
    if zebra:
        st = [x for x in st if not (x[0] == "LINEBELOW" and x[1] == (0, 0))]
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
        label = "pause-leader (old client)" if r["id"] == OLD_CLIENT else r["id"]
        cells = [f"<font name='Mono'>{label}</font>", av, f"{r['impact_errors']:,.0f}", ms(r["impact_p99_ms"]),
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
    c, st, mino = _r(S, "crash-leader"), _r(S, "stop-leader"), _r(S, "partition-minority")
    pa, pn = _r(S, "pause-leader"), _r(S, OLD_CLIENT)
    e250, e1000 = _r(S, "crash-leader-et250"), _r(S, "crash-leader-et1000")
    s50 = _r(S, "slow-link-50ms")
    return [
        f"<b>{len(masked)} of {n} faults cost zero failed requests</b> in all three runs: crashes, pauses, leader "
        f"and flapping partitions, slow and lossy links, graceful stops and a full rolling restart. Their whole "
        f"impact was latency.",
        f"<b>A crashed leader costs latency, not requests.</b> SIGKILLing the node that led "
        f"{c['victim_shards_led']:.0f} of 24 shards failed nothing; clients were back to normal in "
        f"{ms(c['recovery_ms'])}, with p99.9 at {ms(c['impact_p999_ms'])} while new leaders were elected.",
        f"<b>Found and fixed a client bug.</b> A paused (SIGSTOP) leader failed "
        f"{pct(1 - pn['fault_availability'], 1)} of requests for {ms(pn['recovery_ms'])}: its kernel still accepted "
        f"connections, so requests waited out their whole deadline. A per-attempt timeout brought it to "
        f"{pct(pa['fault_availability'], 0)} availability and {ms(pa['recovery_ms'])} recovery.",
        f"<b>Recovery time is set by the election timeout:</b> {ms(e250['recovery_ms'])} → {ms(c['recovery_ms'])} → "
        f"{ms(e1000['recovery_ms'])} as the timeout goes 250 ms → 500 ms → 1 s.",
        f"<b>Only losing a majority costs availability, and only on the shards that lost it:</b> "
        f"{mino['shards_without_quorum']:.0f} of 24 shards refused requests "
        f"({pct(mino['fault_availability'], 1)} availability in the window) while the rest served normally.",
        f"<b>Planned restarts are invisible:</b> leadership transfer kept a graceful stop at p99.9 "
        f"{ms(st['impact_p999_ms'])} (crash: {ms(c['impact_p999_ms'])}). <b>Slow links slow writes, not reads:</b> "
        f"+50 ms took write p99 to {ms(s50['fault_write_p99_ms'])} while lease-based reads stayed at "
        f"{ms(s50['fault_read_p99_ms'])}.",
    ]


def cv_lines(S, g, ms, pct, n, masked, meta):
    c, pa, pn = _r(S, "crash-leader"), _r(S, "pause-leader"), _r(S, OLD_CLIENT)
    return [
        f"Evaluated Raft under injected partitions, pauses and node failures; measured p99 latency and recovery "
        f"time across {n} scenarios (3 runs each, {meta['rate']:,} ops/s, {meta['nodes']}-node cluster), finding "
        f"{len(masked)}/{n} caused zero failed requests, leader crashes recovered in {ms(c['recovery_ms'])}, and a "
        f"client gray-failure bug: a paused leader dropped availability to {pct(pn['fault_availability'], 0)} until "
        f"a per-attempt timeout restored {pct(pa['fault_availability'], 0)} and cut recovery from "
        f"{ms(pn['recovery_ms'])} to {ms(pa['recovery_ms'])}.",
        f"Built a one-command fault-injection harness for DistKV (Go, multi-Raft) covering crashes, pauses, "
        f"partitions, slow and lossy links and rolling restarts; {len(masked)} of {n} faults were fully masked, and "
        f"recovery time tracked the election timeout ({ms(_r(S, 'crash-leader-et250')['recovery_ms'])} at 250 ms, "
        f"{ms(_r(S, 'crash-leader-et1000')['recovery_ms'])} at 1 s).",
        f"Found and fixed a gray-failure bug with fault injection: a frozen Raft leader cost "
        f"{pct(1 - pn['fault_availability'], 1)} of requests and {ms(pn['recovery_ms'])} of recovery because "
        f"clients waited on a socket that never answered; bounded per-attempt retries made it "
        f"{pct(pa['fault_availability'], 0)} available with {ms(pa['recovery_ms'])} recovery.",
    ]


def sections(S, g, ms, pct, P, bullets, figure, PageBreak, CondPageBreak, rec, avail):
    from reportlab.lib.units import inch

    out = [PageBreak(), P("6. Results by fault family", "h1")]
    out += [P("6.1 Crashes, graceful stops and rolling restarts", "h2")]
    out += figure("timelines_crash.png", caption="Representative run (median disruption) of each "
                                                  "scenario, aligned on the injection at t = 0. Shaded: the fault "
                                                  "window. Green: successful ops/s; red: failed ops/s; line: p99 of "
                                                  "each 100 ms bin on a log scale.")
    ct = crash_text(S, g, ms, pct, P, rec)
    c = _r(S, "crash-leader")
    out += ct[:1] + K["diagram"](K["diagrams"].failover(c["recovery_ms"]),
                                 "A leader crash, step by step. The randomised election timeout dominates; pre-vote and "
                                 "the vote itself take a round trip, and the client finds the new leader through "
                                 "retries and redirects. The bar at the bottom is the measured median.") + ct[1:]
    out += [CondPageBreak(4 * inch), P("6.2 Partitions and pauses", "h2")]
    out += figure("timelines_partition.png", caption="Partitions and the process pause, same layout "
                                                      "as Figure " + str(K["fig"]("timelines_crash.png")) + ".")
    out += partition_text(S, g, ms, pct, P, rec)
    out += [CondPageBreak(4 * inch), P("6.3 Slow and lossy links", "h2")]
    out += figure("timelines_slow.png", caption="Slow and lossy links on the busiest leader.")
    out += slow_text(S, g, ms, pct, P, rec)
    return out


def crash_text(S, g, ms, pct, P, rec):
    c, st, rr = _r(S, "crash-leader"), _r(S, "stop-leader"), _r(S, "rolling-restart")
    out = []
    if c:
        out.append(P(
            f"<b>Crash (SIGKILL).</b> The victim led a median of {c['victim_shards_led']:.0f} shards. Their "
            f"followers stopped hearing heartbeats, waited out the randomised election timeout and elected new "
            f"leaders; the client found them through redirects. Requests to those shards stalled in the "
            f"meantime, which is the latency spike in Figure {K['fig']('timelines_crash.png')} (p99.9 {ms(c['impact_p999_ms'])}, max "
            f"{ms(c['impact_max_ms'])}), but <b>{'none' if c['impact_errors'] == 0 else int(c['impact_errors'])} "
            f"failed</b>: retries absorbed the gap, and session sequence numbers kept every retried write "
            f"exactly-once. Service was back to baseline after {ms(c['recovery_ms'])}. When the node restarted "
            f"10 s later it replayed its write-ahead log and caught up; the cluster was fully healthy again "
            f"{ms(c['rejoin_ms'])} after the restart."))
    if st and c:
        out.append(P(
            f"<b>Graceful stop (SIGTERM).</b> The same kind of node, stopped politely, hands each leadership to an "
            f"up-to-date follower (TimeoutNow) before it exits, so no shard waits for an election timeout. p99 "
            f"after injection was {ms(st['impact_p99_ms'])} against {ms(st['baseline_p99_ms'])} at steady state, "
            f"p99.9 {ms(st['impact_p999_ms'])} versus {ms(c['impact_p999_ms'])} for the crash, with "
            f"{'no measurable disruption at all' if not st['disrupted_ms'] else ms(st['disrupted_ms']) + ' of disruption'}. "
            f"This is the difference between a deploy and an outage, and it is entirely a property of the "
            f"shutdown path."))
    if rr:
        out.append(P(
            f"<b>Rolling restart.</b> Every node was stopped and restarted in turn, each time waiting for full "
            f"health, as an upgrade would; all five were done within {ms(rr['fault_window_ms'])}. "
            f"{'No request failed' if rr['impact_errors'] == 0 else str(int(rr['impact_errors'])) + ' requests failed'}, "
            f"p99 stayed at {ms(rr['impact_p99_ms'])} and p99.9 at {ms(rr['impact_p999_ms'])}, indistinguishable "
            f"from the baseline, and {'not one 100 ms bin was degraded' if not rr['disrupted_ms'] else ms(rr['disrupted_ms']) + ' was degraded'}: "
            f"leadership transfer on the way down and leader balancing on the way up move work off and back on "
            f"without a single election timeout."))
    return out


def partition_text(S, g, ms, pct, P, rec):
    pl, pm, pf = _r(S, "partition-leader"), _r(S, "partition-minority"), _r(S, "partition-flapping")
    pa, pn = _r(S, "pause-leader"), _r(S, "pause-leader-no-attempt-timeout")
    out = []
    if pl:
        out.append(P(
            f"<b>Partitioned leader.</b> A leader cut off from its peers keeps running and keeps receiving client "
            f"requests. Check-quorum demotes it once it has missed a majority of acknowledgements for an election "
            f"timeout, and the majority side elects new leaders (pre-vote keeps the isolated node from disrupting "
            f"them with ever-higher terms). Writes sent to it can't commit and reads can't renew its lease, so "
            f"it answers with errors that send the client elsewhere. Result: "
            f"{'no failed requests' if pl['impact_errors'] == 0 else str(int(pl['impact_errors'])) + ' failed requests'}, "
            f"p99.9 {ms(pl['impact_p999_ms'])}, recovery {ms(pl['recovery_ms'])}, close to a crash: from the "
            f"cluster's point of view the two are the same event."))
    if pm:
        out.append(P(
            f"<b>Minority partition: the CP trade-off, measured.</b> Isolating the two busiest nodes left "
            f"{pm['shards_without_quorum']:.0f} of 24 shards with only one reachable replica. Raft must not elect "
            f"a leader there (one of three is not a majority), so those shards became unavailable, exactly as the "
            f"theory requires; the other {24 - pm['shards_without_quorum']:.0f} shards failed over and kept "
            f"serving. Those {pm['shards_without_quorum']:.0f} shards carry "
            f"{pm['shards_without_quorum'] / 24:.1%} of the traffic, yet fault-window availability was "
            f"{pct(pm['fault_availability'], 1)}: only about half of their requests failed. The client deadline "
            f"explains it. Requests to the stranded shards fail after 5 s, so in Figure {K['fig']('timelines_partition.png')} failures stop 5 s into "
            f"the partition; requests issued in its last 5 s were still waiting when it healed and succeeded late "
            f"(hence the p99 of {ms(pm['impact_p99_ms'])}). Availability under quorum loss is therefore partly a "
            f"choice of client timeout. "
            f"Service was normal {ms(pm['recovery_after_heal_ms'])} after the heal, "
            f"and the refusal is the guarantee working: a replica cut off from its majority cannot accept a write "
            f"the majority hasn't seen, so the two sides can never diverge."))
        out += K["diagram"](K["diagrams"].minority(pm["shards_without_quorum"]),
                            f"Why exactly {pm['shards_without_quorum']:.0f} shards stopped. A shard needs two of its "
                            f"three replicas to elect a leader. Shards with two replicas on the cut-off nodes can't, "
                            f"and refuse requests (CP {K['cite'](4, 5)}); the rest elect a leader on the majority side.")
    if pf:
        out.append(P(
            f"<b>Flapping partition.</b> Isolating and reconnecting the leader every second, five times, is "
            f"harder than a clean cut: each reconnection can trigger a new election, and the balancer wants to "
            f"move leaders back. Pre-vote keeps a rejoining node from forcing elections with a stale term, and "
            f"leadership settles on the stable majority. Result: "
            f"{'no failed requests' if pf['impact_errors'] == 0 else str(int(pf['impact_errors'])) + ' failed requests'}, "
            f"p99.9 {ms(pf['impact_p999_ms'])}, {ms(pf['disrupted_ms'])} of disrupted time over the 10 s of churn."))
    if pa and pn:
        out.append(P(
            f"<b>Pause: the study's bug.</b> Freezing the leader with SIGSTOP looks like a crash to its peers "
            f"(they re-elect within an election timeout), but not to clients: the paused process's kernel still "
            f"accepts TCP connections, so requests sent to the cached leader neither fail nor return. With the "
            f"original client, which let each attempt use the whole 5 s deadline, that cost "
            f"<b>{pct(1 - pn['fault_availability'], 1)} of requests</b> in the fault window and "
            f"<b>{ms(pn['recovery_ms'])}</b> of recovery, although Raft had recovered in under a second. The fix "
            f"is a per-attempt timeout (1 s by default): a hung node now costs one attempt, the client forgets "
            f"the cached leader and retries another replica, and writes stay exactly-once through their session "
            f"sequence numbers. With it the same fault caused "
            f"{'no failed requests' if pa['impact_errors'] == 0 else str(int(pa['impact_errors'])) + ' failed requests'} "
            f"and recovered in {ms(pa['recovery_ms'])}; its p99.9 of {ms(pa['impact_p999_ms'])} is that one "
            f"timed-out attempt. Leader leases are what make the pause safe at all: the "
            f"new leader may not serve reads until the old lease has expired, so when the frozen node wakes up "
            f"it cannot serve a stale read from a lease it no longer holds."))
        out += K["diagram"](K["diagrams"].gray_failure(
            [("failed requests", pct(1 - pn["fault_availability"], 1)), ("recovery", ms(pn["recovery_ms"]))],
            [("failed requests", "0" if pa["impact_errors"] == 0 else str(int(pa["impact_errors"]))), ("recovery", ms(pa["recovery_ms"]))]),
            f"The gray-failure bug {K['cite'](8)}, before and after. A frozen process looks dead to its peers but "
            f"alive to clients, whose connections are still accepted by its kernel. Bounding each attempt turns a "
            f"5 s hang into one 1 s detour.")
    return out


def slow_text(S, g, ms, pct, P, rec):
    s50, s200, lo = _r(S, "slow-link-50ms"), _r(S, "slow-link-200ms"), _r(S, "lossy-link-30")
    out = []
    if s50 and s200:
        out.append(P(
            f"<b>Slow links slow writes, not reads.</b> Delaying the leader's Raft messages adds the delay to "
            f"every commit round trip, so writes on its shards slow down for the whole fault: write p99 was "
            f"{ms(s50['fault_write_p99_ms'])} with +50 ms and {ms(s200['fault_write_p99_ms'])} with +200 ms, about "
            f"twice the injected delay (most likely because a delayed send also holds back the next batch to the "
            f"same follower). Reads were untouched ({ms(s50['fault_read_p99_ms'])} and {ms(s200['fault_read_p99_ms'])} "
            f"p99): a leader holding a valid lease answers linearizable reads without asking its peers. Nothing "
            f"failed, and the delay never caused a failover: heartbeats that are 200 ms late still arrive inside "
            f"the 500 ms election timeout. To check that rather than infer it, one run of each slow and lossy "
            f"scenario was repeated with <font name='Mono'>--keep-logs</font>: the server logs show no step-down "
            f"and no timeout-driven election during the fault; the only leadership changes were the leader "
            f"balancer's routine transfers (one or two per run), which would have happened anyway. A slow leader is not a dead leader, so the degradation lasts exactly as "
            f"long as the fault (the * recovery in Table 1). Detecting a slow leader and moving leadership away "
            f"is a policy Raft leaves to the operator."))
    if lo:
        out.append(P(
            f"<b>Lossy links.</b> Dropping 30% of the leader's Raft messages is absorbed by retransmission and "
            f"pipelining: {'no failed requests' if lo['impact_errors'] == 0 else str(int(lo['impact_errors'])) + ' failed requests'}, "
            f"write p99 {ms(lo['fault_write_p99_ms'])} in the window, p99.9 {ms(lo['impact_p999_ms'])} overall."))
    return out


def election(S, g, ms, P, figure, CondPageBreak):
    from reportlab.lib.units import inch

    out = [CondPageBreak(4 * inch), P("7. The election-timeout trade-off", "h1")]
    out += figure("election_timeout.png", caption="Crash of the busiest leader at three election "
                                                   "timeouts: recovery time and p99.9 latency (dots: runs; line: "
                                                   "median; dashed: y = timeout).")
    e250, e500, e1000 = _r(S, "crash-leader-et250"), _r(S, "crash-leader"), _r(S, "crash-leader-et1000")
    if e250 and e500 and e1000:
        out.append(P(
            f"Recovery from a crash is dominated by one parameter. Halving the election timeout to 250 ms cut "
            f"recovery to {ms(e250['recovery_ms'])} and p99.9 to {ms(e250['impact_p999_ms'])}; doubling it to "
            f"1 s raised them to {ms(e1000['recovery_ms'])} and {ms(e1000['impact_p999_ms'])}. Recovery is roughly "
            f"one to two timeouts: followers wait a randomised timeout before campaigning, and the client needs a "
            f"round of redirects to find the new leader. None of the three settings failed a request."))
        out.append(P(
            "Why not always use a short timeout? Because a timeout is also the bar a healthy leader must clear. "
            "A GC pause, an fsync stall or a congested link longer than the timeout triggers a needless election, "
            "and on a loaded machine that happens more often the lower the bar. The defaults here (500 ms "
            "elections, 50 ms heartbeats) sit between the two; a deployment on dedicated hardware with a fast "
            "network could go lower, one spanning regions must go higher. The +200 ms slow-link scenario shows "
            "the other side of the trade: write latency rose to about 400 ms without a failover, because late "
            "heartbeats still beat the 500 ms timeout. A 250 ms timeout would have turned that slowness into "
            "repeated elections."))
    return out


def lessons(S, g, ms, pct, masked, n):
    c, st, pm = _r(S, "crash-leader"), _r(S, "stop-leader"), _r(S, "partition-minority")
    pa, pn = _r(S, "pause-leader"), _r(S, "pause-leader-no-attempt-timeout")
    out = [
        f"<b>Raft masks single-node failures; clients see latency, not errors.</b> {len(masked)} of {n} faults "
        f"cost zero failed requests over three runs each. The price is a tail-latency spike bounded by the "
        f"election timeout ({ms(c.get('impact_p999_ms'))} p99.9 for a crashed leader).",
        "<b>Availability is lost only where a majority is lost, and then it must be.</b> The minority partition "
        f"made {pm.get('shards_without_quorum', 0):.0f} shards refuse requests and left the rest untouched: "
        "consistency over availability, scoped to the affected shards thanks to multi-Raft.",
        f"<b>Recovery time is a tunable, not a constant.</b> It tracks the election timeout almost linearly; "
        f"choosing the timeout is choosing between failover speed and false elections.",
        f"<b>Planned operations should never look like failures.</b> Leadership transfer made a graceful stop "
        f"nearly invisible (p99.9 {ms(st.get('impact_p999_ms'))} versus {ms(c.get('impact_p999_ms'))} for a crash), "
        f"and a full rolling restart failed no requests.",
    ]
    if pa and pn:
        out.append(
            f"<b>The weakest link was the client, not the consensus.</b> A paused leader cost "
            f"{pct(1 - pn['fault_availability'], 1)} of requests and {ms(pn['recovery_ms'])} because the client "
            f"waited on a node that accepted connections but never answered. Raft had already recovered. A "
            f"per-attempt timeout fixed it ({pct(pa['fault_availability'], 1)} available, {ms(pa['recovery_ms'])} "
            f"recovery). Gray failures (slow, frozen, half-alive nodes) are what end-to-end fault injection "
            f"finds and unit tests don't.")
    out.append("<b>Slow is not dead.</b> Slow and lossy links never triggered a timeout-driven election "
               "(checked in the server logs); they cost write latency on the leader's shards. Detecting a slow leader and moving leadership away is a policy Raft leaves "
               "to the operator.")
    return out


def interview(S, g, ms, pct, meta):
    c, st, pm, pl = _r(S, "crash-leader"), _r(S, "stop-leader"), _r(S, "partition-minority"), _r(S, "partition-leader")
    pa, pn = _r(S, "pause-leader"), _r(S, "pause-leader-no-attempt-timeout")
    e250, e1000 = _r(S, "crash-leader-et250"), _r(S, "crash-leader-et1000")
    return [
        ("What happens to in-flight requests when a Raft leader crashes?",
         f"Followers stop receiving heartbeats, wait a randomised election timeout and elect a new leader among "
         f"the up-to-date replicas. In-flight requests to the dead leader fail at the transport level and are "
         f"retried; in my measurements nothing failed and clients were back to normal in "
         f"{ms(c.get('recovery_ms'))}, with p99.9 at {ms(c.get('impact_p999_ms'))} during the election. Writes "
         f"that may have committed before the crash are retried with the same session sequence number, so they "
         f"apply exactly once."),
        ("How do you avoid stale reads from an old leader after a partition?",
         "Two mechanisms. Check-quorum makes a leader that can't reach a majority step down. Leader leases make "
         "a new leader wait until the old leader's lease must have expired before serving lease-based reads; "
         "otherwise reads go through ReadIndex, which confirms leadership with a heartbeat round. The pause "
         "scenario is the sharpest test: a frozen leader wakes up believing it still leads, and its lease has "
         "already run out."),
        ("What does CAP mean in practice for your system?",
         f"DistKV is CP per shard. When I isolated two of five nodes, the {pm.get('shards_without_quorum', 0):.0f} "
         f"shards that had two replicas on that side refused requests (availability "
         f"{pct(pm.get('fault_availability'), 1)} in that window) rather than risk divergence; the other shards "
         f"kept serving. Sharding into many Raft groups limits the blast radius of losing quorum."),
        ("How would you choose the election timeout?",
         f"From measurements: recovery went from {ms(e250.get('recovery_ms'))} at 250 ms to "
         f"{ms(e1000.get('recovery_ms'))} at 1 s. Lower is faster failover but more false elections from GC "
         f"pauses, fsync stalls or network jitter; it should comfortably exceed the p99.9 of heartbeat round trips "
         f"on the real network, and spread across a randomised range to avoid split votes."),
        ("What was the most interesting thing you found?",
         f"A bug in the client, not in Raft. A SIGSTOPped leader failed {pct(1 - pn.get('fault_availability', 1), 1)} "
         f"of requests for {ms(pn.get('recovery_ms'))} because its kernel still accepted connections and every "
         f"request waited out its 5 s deadline. A per-attempt timeout that forgets the hung node fixed it: "
         f"{pct(pa.get('fault_availability'), 1)} availability and {ms(pa.get('recovery_ms'))} recovery."),
        ("How did you make the measurements trustworthy?",
         f"Open-loop load at a fixed {meta['rate']:,} ops/s, with latency measured from each request's scheduled "
         f"start so requests that queued behind a stall count (no coordinated omission). Every run starts a fresh "
         f"cluster, every scenario runs three times and I report medians with ranges, and a no-fault baseline on "
         f"the same schedule gives the noise floor. Everything reruns with one command."),
        ("Why is a graceful shutdown so much cheaper than a crash?",
         f"On SIGTERM the node transfers each leadership to an up-to-date follower and tells it to campaign "
         f"immediately (TimeoutNow), so nobody waits for an election timeout. p99.9 was {ms(st.get('impact_p999_ms'))} "
         f"versus {ms(c.get('impact_p999_ms'))} for a crash of the same node."),
        ("What would you test next?",
         "Disk faults (fsync errors, full disks, torn writes), asymmetric partitions where A reaches B but not "
         "the reverse, clock skew against the lease logic, membership changes under load, and the leaderless "
         "comparison in Section 9, with the linearizability checker counting the anomalies each mode allows."),
    ]


GLOSSARY = [
    ("Availability", "the share of attempted requests that succeeded within their deadline (5 s, retries included)."),
    ("Check-quorum", "a leader steps down if it hasn't heard from a majority for an election timeout, so a cut-off leader stops acting as one."),
    ("Coordinated omission", "the measurement error of timing requests from when they were sent rather than when they were due, which hides stalls."),
    ("Election timeout", "how long a follower waits without hearing from a leader before it campaigns; randomised to avoid split votes."),
    ("Gray failure", "a partial failure (slow, frozen, half-working) that some observers see and others don't."),
    ("Lease", "a time-bounded promise that lets a leader serve reads without a round trip, valid only while no other leader can exist."),
    ("Linearizability", "every operation appears to take effect atomically at one instant between its call and its return."),
    ("Multi-Raft", "running one Raft group per shard, so leadership and quorum are per shard rather than per cluster."),
    ("Open loop", "issuing requests on a fixed schedule regardless of responses, as independent users do."),
    ("p99 / p99.9", "the latency that 99% / 99.9% of requests beat; the slowest 1% / 0.1% are above it."),
    ("Pre-vote", "a candidate first asks whether it could win before incrementing its term, so a rejoining node can't disrupt a healthy leader."),
    ("Quorum (majority)", "more than half of a shard's replicas: two of three here. Nothing commits without one."),
    ("ReadIndex", "a linearizable read that confirms leadership with one heartbeat round instead of writing to the log."),
    ("TimeoutNow", "the message a leader sends to hand leadership to an up-to-date follower immediately, used on graceful shutdown."),
]

REFERENCES = [
    "Diego Ongaro and John Ousterhout. <i>In Search of an Understandable Consensus Algorithm.</i> USENIX Annual "
    "Technical Conference (ATC), 2014.",
    "Diego Ongaro. <i>Consensus: Bridging Theory and Practice.</i> PhD thesis, Stanford University, 2014. (Pre-vote, "
    "leadership transfer, leases and ReadIndex.)",
    "Cary G. Gray and David R. Cheriton. <i>Leases: An Efficient Fault-Tolerant Mechanism for Distributed File Cache "
    "Consistency.</i> ACM Symposium on Operating Systems Principles (SOSP), 1989.",
    "Eric A. Brewer. <i>Towards Robust Distributed Systems.</i> Keynote, ACM Symposium on Principles of Distributed "
    "Computing (PODC), 2000.",
    "Seth Gilbert and Nancy Lynch. <i>Brewer's Conjecture and the Feasibility of Consistent, Available, "
    "Partition-Tolerant Web Services.</i> ACM SIGACT News 33(2), 2002.",
    "Giuseppe DeCandia et al. <i>Dynamo: Amazon's Highly Available Key-value Store.</i> ACM Symposium on Operating "
    "Systems Principles (SOSP), 2007.",
    "Maurice P. Herlihy and Jeannette M. Wing. <i>Linearizability: A Correctness Condition for Concurrent Objects.</i> "
    "ACM Transactions on Programming Languages and Systems 12(3), 1990.",
    "Peng Huang et al. <i>Gray Failure: The Achilles' Heel of Cloud-Scale Systems.</i> Workshop on Hot Topics in "
    "Operating Systems (HotOS), 2017.",
    "Ali Basiri et al. <i>Chaos Engineering.</i> IEEE Software 33(3), 2016.",
    "Gil Tene. <i>How NOT to Measure Latency.</i> Conference talk, 2015 (coordinated omission).",
    "Jeffrey Dean and Luiz André Barroso. <i>The Tail at Scale.</i> Communications of the ACM 56(2), 2013.",
    "Ahmed Alquraan et al. <i>An Analysis of Network-Partitioning Failures in Cloud Systems.</i> USENIX Symposium on "
    "Operating Systems Design and Implementation (OSDI), 2018.",
    "Kyle Kingsbury. <i>Jepsen analyses of distributed databases.</i> jepsen.io.",
    "The etcd authors. <i>etcd-io/raft</i>, the Raft library used by etcd and others (pre-vote and check-quorum "
    "options). github.com/etcd-io/raft.",
]
