"""Vector diagrams for the study report (reportlab graphics).

Each function returns a Drawing sized to the text width; numbers that come
from the results are passed in, so the diagrams stay true to the data."""
from reportlab.graphics.shapes import Circle, Drawing, Group, Line, Polygon, Rect, String
from reportlab.lib import colors

NAVY = colors.HexColor("#12192b")
INK = colors.HexColor("#1f2333")
MUTED = colors.HexColor("#6b7182")
FAINT = colors.HexColor("#c9ccd4")
RULE = colors.HexColor("#e1e4ea")
SOFT = colors.HexColor("#f3f5f8")
TEAL = colors.HexColor("#00798c")
TEAL_SOFT = colors.HexColor("#d9eef1")
CORAL = colors.HexColor("#d1495b")
CORAL_SOFT = colors.HexColor("#f8dde1")
AMBER = colors.HexColor("#edae49")
AMBER_SOFT = colors.HexColor("#fbedd4")
GREEN = colors.HexColor("#2a9d8f")
WHITE = colors.white
NODE = [colors.HexColor(c) for c in ("#7c5cff", "#00a6c8", "#ff5ca8", "#2a9d8f", "#e69a1c")]

W = 504  # 7 in of text width


def text(x, y, s, size=7.5, font="Sans", color=INK, anchor="start"):
    return String(x, y, s, fontName=font, fontSize=size, fillColor=color, textAnchor=anchor)


def chip(x, y, s, size=6.6, font="Sans", color=INK, anchor="middle", bg=WHITE, pad=2.5):
    from reportlab.pdfbase.pdfmetrics import stringWidth

    w = stringWidth(s, font, size)
    x0 = x - w / 2 if anchor == "middle" else (x - w if anchor == "end" else x)
    g = Group()
    g.add(Rect(x0 - pad, y - 2.2, w + 2 * pad, size + 3.6, rx=2, ry=2, fillColor=bg, strokeColor=None))
    g.add(String(x, y, s, fontName=font, fontSize=size, fillColor=color, textAnchor=anchor))
    return g


def box(x, y, w, h, fill=WHITE, stroke=FAINT, sw=0.8, r=5, dash=None):
    b = Rect(x, y, w, h, rx=r, ry=r, fillColor=fill, strokeColor=stroke, strokeWidth=sw)
    if dash:
        b.strokeDashArray = dash
    return b


def arrow(x1, y1, x2, y2, color=INK, sw=0.9, head=4.5, dash=None):
    g = Group()
    ln = Line(x1, y1, x2, y2, strokeColor=color, strokeWidth=sw)
    if dash:
        ln.strokeDashArray = dash
    g.add(ln)
    import math

    a = math.atan2(y2 - y1, x2 - x1)
    p1 = (x2 - head * math.cos(a - 0.45), y2 - head * math.sin(a - 0.45))
    p2 = (x2 - head * math.cos(a + 0.45), y2 - head * math.sin(a + 0.45))
    g.add(Polygon([x2, y2, p1[0], p1[1], p2[0], p2[1]], fillColor=color, strokeColor=color, strokeWidth=0.5))
    return g


def badge(x, y, s, fill=TEAL, color=WHITE, size=6.5, pad=3.5):
    from reportlab.pdfbase.pdfmetrics import stringWidth

    w = stringWidth(s, "Sans-Bold", size) + 2 * pad
    g = Group()
    g.add(Rect(x, y, w, size + 5, rx=(size + 5) / 2, ry=(size + 5) / 2, fillColor=fill, strokeColor=None))
    g.add(text(x + w / 2, y + 3, s, size, "Sans-Bold", color, "middle"))
    return g, w


def caption_title(d, s, x=0, y=None):
    d.add(text(x, (y if y is not None else d.height - 10), s, 8, "Sans-Bold", NAVY))


# ---------------------------------------------------------------------------

def architecture():
    """Smart client, five nodes, shards as Raft groups of three."""
    H = 232
    d = Drawing(W, H)
    d.add(box(0, 0, W, H, fill=SOFT, stroke=None, r=8))

    # Client
    d.add(box(10, 150, 100, 62, fill=WHITE, stroke=TEAL, sw=1.1))
    d.add(text(60, 197, "Smart client", 8.5, "Sans-Bold", NAVY, "middle"))
    d.add(text(60, 185, "hash(key) → shard", 6.3, "Mono", MUTED, "middle"))
    d.add(text(60, 175, "shard → leader", 6.3, "Mono", MUTED, "middle"))
    d.add(text(60, 160, "retries · sessions", 6.5, "Sans", MUTED, "middle"))

    # Hash ring
    cx, cy, rr = 60, 82, 36
    d.add(Circle(cx, cy, rr, fillColor=None, strokeColor=FAINT, strokeWidth=5))
    import math

    for i in range(24):
        a = 2 * math.pi * i / 24
        col = NODE[i % 5]
        x1, y1 = cx + (rr - 2.5) * math.cos(a), cy + (rr - 2.5) * math.sin(a)
        x2, y2 = cx + (rr + 2.5) * math.cos(a), cy + (rr + 2.5) * math.sin(a)
        d.add(Line(x1, y1, x2, y2, strokeColor=col, strokeWidth=3.2))
    d.add(text(cx, cy + 2, "24 shards", 7, "Sans-Bold", NAVY, "middle"))
    d.add(text(cx, cy - 7, "on a hash ring", 6.2, "Sans", MUTED, "middle"))
    d.add(text(cx, 30, "FNV-1a + SplitMix64,", 6.2, "Sans", MUTED, "middle"))
    d.add(text(cx, 21, "128 virtual nodes each", 6.2, "Sans", MUTED, "middle"))

    # Nodes
    x0, nw, gap, ny, nh = 160, 62, 7.5, 128, 84
    leaders = [5, 5, 5, 5, 4]
    for i in range(5):
        x = x0 + i * (nw + gap)
        d.add(box(x, ny, nw, nh, fill=WHITE, stroke=NODE[i], sw=1.2))
        d.add(Rect(x, ny + nh - 15, nw, 15, rx=5, ry=5, fillColor=NODE[i], strokeColor=None))
        d.add(Rect(x, ny + nh - 15, nw, 7, fillColor=NODE[i], strokeColor=None))
        d.add(text(x + nw / 2, ny + nh - 11, f"n{i + 1}", 8, "Sans-Bold", WHITE, "middle"))
        # replica chips: leaders filled, followers outlined (each node hosts ~14 replicas)
        k = 0
        for row in range(3):
            for col in range(5):
                if k >= 14:
                    break
                cxp, cyp = x + 9 + col * 11, ny + 52 - row * 11
                lead = k < leaders[i]
                d.add(Rect(cxp - 3.5, cyp - 3.5, 7, 7, rx=1.5, ry=1.5, fillColor=NODE[i] if lead else WHITE,
                           strokeColor=NODE[i], strokeWidth=0.8))
                k += 1
        d.add(text(x + nw / 2, ny + 8, "WAL · fsync", 6, "Mono", MUTED, "middle"))
    d.add(text(x0, ny + nh + 10, "5 nodes · each hosts ~14 shard replicas (filled = leader, outlined = follower)", 6.8,
               "Sans", MUTED))

    # client → leaders
    d.add(arrow(110, 186, x0 - 2, 186, TEAL, 1.1))
    d.add(text(114, 197, "direct to", 5.9, "Sans", TEAL))
    d.add(text(114, 190, "the leader", 5.9, "Sans", TEAL))
    d.add(arrow(x0 - 2, 160, 110, 160, MUTED, 0.8, dash=[2, 2]))
    d.add(text(114, 150, "redirect", 5.9, "Sans", MUTED))
    d.add(text(114, 143, "(421 + hint)", 5.9, "Sans", MUTED))

    # Raft group zoom
    zx, zy, zw, zh = 160, 12, 330, 98
    d.add(box(zx, zy, zw, zh, fill=WHITE, stroke=FAINT))
    d.add(text(zx + 10, zy + zh - 14, "One shard = one Raft group on 3 of the 5 nodes", 7.6, "Sans-Bold", NAVY))
    lx, ly = zx + 46, zy + 42
    d.add(Circle(lx, ly, 15, fillColor=NODE[1], strokeColor=None))
    d.add(text(lx, ly - 2.5, "n2", 7.5, "Sans-Bold", WHITE, "middle"))
    d.add(text(lx, ly - 25, "leader", 6.5, "Sans-Bold", NODE[1], "middle"))
    for j, (fx, fy, name, col) in enumerate([(zx + 170, zy + 62, "n4", NODE[3]), (zx + 170, zy + 22, "n5", NODE[4])]):
        d.add(Circle(fx, fy, 12, fillColor=WHITE, strokeColor=col, strokeWidth=1.4))
        d.add(text(fx, fy - 2.5, name, 7, "Sans-Bold", col, "middle"))
        d.add(arrow(lx + 16, ly + (6 if j == 0 else -6), fx - 13, fy, INK, 0.8))
    d.add(text(zx + 88, zy + 66, "AppendEntries", 6.3, "Mono", MUTED))
    d.add(text(zx + 88, zy + 13, "heartbeats 50 ms", 6.3, "Mono", MUTED))
    steps = ["1  leader appends + fsyncs", "2  replicates to followers", "3  commits on a majority (2 of 3)",
             "4  applies, then answers"]
    for i, s in enumerate(steps):
        d.add(text(zx + 205, zy + 66 - i * 13, s, 6.8, "Sans", INK))
    return d


def fault_layer():
    """Where each fault is injected."""
    H = 200
    d = Drawing(W, H)
    d.add(box(0, 0, W, H, fill=SOFT, stroke=None, r=8))

    # harness
    d.add(box(12, 132, 118, 54, fill=NAVY, stroke=None))
    d.add(text(71, 168, "distkv-study", 8.5, "Sans-Bold", WHITE, "middle"))
    d.add(text(71, 156, "harness", 7, "Sans", colors.HexColor("#b9c2d6"), "middle"))
    d.add(text(71, 142, "open-loop load + faults", 6.3, "Sans", colors.HexColor("#b9c2d6"), "middle"))

    # node
    nx, ny, nw, nh = 196, 18, 190, 168
    d.add(box(nx, ny, nw, nh, fill=WHITE, stroke=NODE[0], sw=1.3))
    d.add(text(nx + 10, ny + nh - 13, "distkv process (victim)", 7.6, "Sans-Bold", NAVY))
    layers = [("HTTP API (client requests)", TEAL_SOFT, TEAL), ("KV state machine", SOFT, MUTED),
              ("Raft, one per shard", SOFT, MUTED), ("Raft transport", SOFT, MUTED)]
    ly = ny + nh - 44
    for name, fill, col in layers:
        d.add(box(nx + 12, ly, nw - 24, 22, fill=fill, stroke=col, sw=0.7, r=3))
        d.add(text(nx + nw / 2, ly + 8, name, 7, "Sans", INK, "middle"))
        ly -= 30
    # fault hook gate on transport
    gy = ny + nh - 44 - 3 * 30
    d.add(box(nx + nw - 50, gy - 4, 52, 30, fill=CORAL, stroke=None, r=4))
    d.add(text(nx + nw - 24, gy + 15, "fault", 7, "Sans-Bold", WHITE, "middle"))
    d.add(text(nx + nw - 24, gy + 5, "hook", 7, "Sans-Bold", WHITE, "middle"))

    # signals
    sigs = [("SIGKILL", "crash", CORAL), ("SIGTERM", "graceful stop", TEAL), ("SIGSTOP", "pause", AMBER)]
    for i, (sname, what, col) in enumerate(sigs):
        y = 112 - i * 22
        g, w = badge(18, y, sname, fill=col)
        d.add(g)
        d.add(text(18 + w + 5, y + 3, what, 6.6, "Sans", INK))
        d.add(arrow(106, y + 5, nx - 2, y + 5 + (i - 1) * 4, col, 0.9))
    d.add(text(18, 30, "process faults: real signals", 6.6, "Sans-Bold", MUTED))
    d.add(text(18, 20, "on real processes", 6.6, "Sans", MUTED))

    # client arrow, unaffected
    d.add(arrow(130, 160, nx + 10, ny + nh - 33, TEAL, 1.1))
    d.add(text(136, 179, "client traffic:", 6.2, "Sans-Bold", TEAL))
    d.add(text(136, 171, "never faulted", 6.2, "Sans", TEAL))

    # peers
    px = 428
    for i, (name, col) in enumerate([("n3", NODE[2]), ("n4", NODE[3]), ("n5", NODE[4])]):
        y = 132 - i * 46
        d.add(Circle(px + 28, y, 15, fillColor=WHITE, strokeColor=col, strokeWidth=1.3))
        d.add(text(px + 28, y - 2.5, name, 7.5, "Sans-Bold", col, "middle"))
        d.add(arrow(nx + nw + 3, gy + 11, px + 12, y, CORAL, 0.9, dash=[3, 2]))
        d.add(arrow(px + 12, y - 5, nx + nw + 3, gy + 5, MUTED, 0.6, dash=[1.5, 2]))
    d.add(text(px + 28, 172, "peers", 7, "Sans-Bold", MUTED, "middle"))
    d.add(text(W - 8, 8, "isolate = drop all · delay = +d ms · drop = p %", 6.2, "Sans", CORAL, "end"))
    return d


def timeline(meta):
    """Phases of a run and the windows each metric uses."""
    H = 168
    d = Drawing(W, H)
    total = meta["warmup_s"] + meta["pre_s"] + 10 + meta["post_s"]
    x0, x1 = 16, W - 16
    sc = (x1 - x0) / total

    def X(t):
        return x0 + t * sc

    inj = meta["warmup_s"] + meta["pre_s"]
    heal = inj + 10
    phases = [("warm-up", 0, meta["warmup_s"], SOFT, MUTED), ("steady state", meta["warmup_s"], inj, TEAL_SOFT, TEAL),
              ("fault", inj, heal, CORAL_SOFT, CORAL), ("observation", heal, total, SOFT, MUTED)]
    y = 112
    for t, lab, col in [(inj, "inject", CORAL), (heal, "heal", TEAL)]:
        d.add(Line(X(t), 8, X(t), y + 34, strokeColor=col, strokeWidth=0.9, strokeDashArray=[2, 2]))
        d.add(chip(X(t), y + 38, lab, 7, "Sans-Bold", col))
    for name, a, b, fill, col in phases:
        d.add(Rect(X(a), y, (b - a) * sc, 26, fillColor=fill, strokeColor=WHITE, strokeWidth=1.5))
        d.add(text((X(a) + X(b)) / 2, y + 15, name, 7.6, "Sans-Bold", col, "middle"))
        d.add(text((X(a) + X(b)) / 2, y + 5, f"{b - a:.0f} s", 6.5, "Sans", col, "middle"))
    d.add(text(x0, y + 38, "load starts: open loop, fixed rate", 6.4, "Sans", MUTED))

    def bracket(a, b, yy, lab, col):
        xa, xb = X(a), X(b)
        d.add(Line(xa + 1, yy, xb - 1, yy, strokeColor=col, strokeWidth=1.2))
        d.add(Line(xa + 1, yy - 3, xa + 1, yy + 3, strokeColor=col, strokeWidth=1.2))
        d.add(Line(xb - 1, yy - 3, xb - 1, yy + 3, strokeColor=col, strokeWidth=1.2))
        d.add(chip((xa + xb) / 2, yy - 11, lab, 6.4, "Sans", col))

    bracket(meta["warmup_s"], inj, 96, "baseline (p99 reference)", TEAL)
    bracket(inj, heal, 96, "fault window: availability", CORAL)
    bracket(inj, total, 70, "impact window: p99, p99.9, max, disrupted time", NAVY)
    d.add(arrow(X(inj), 40, X(inj + 1.2), 40, CORAL, 1.2))
    d.add(chip(X(inj + 1.2) + 4, 37.5, "recovery: until 1 s of clean 100 ms bins (no errors, p99 under bound)", 6.4,
               "Sans", CORAL, "start"))
    d.add(arrow(X(heal), 20, X(heal + 1.0), 20, TEAL, 1.2))
    d.add(chip(X(heal + 1.0) + 4, 17.5, "rejoin: until every node is ready and every shard led", 6.4, "Sans", TEAL, "start"))
    return d


def failover(rec_ms, et_ms=500):
    """Crash of a leader: what happens, and when (measured)."""
    H = 190
    d = Drawing(W, H)
    d.add(box(0, 0, W, H, fill=SOFT, stroke=None, r=8))
    lanes = [("client", TEAL), ("n1 · leader", NODE[0]), ("n2 · follower", NODE[1]), ("n3 · follower", NODE[2])]
    lx0, lx1 = 96, W - 18
    ys = [150, 116, 82, 48]
    for (name, col), y in zip(lanes, ys):
        d.add(text(12, y - 3, name, 7.4, "Sans-Bold", col))
        d.add(Line(lx0, y, lx1, y, strokeColor=FAINT, strokeWidth=0.8))
    total = rec_ms * 1.22
    sc = (lx1 - lx0) / total

    def X(t):
        return lx0 + t * sc

    d.add(text(X(0), ys[1] - 3.5, "✕", 12, "Sans-Bold", CORAL, "middle"))
    d.add(chip(X(0), ys[1] + 9, "SIGKILL", 6.4, "Sans-Bold", CORAL, bg=SOFT))
    d.add(Line(X(0) + 6, ys[1], X(total), ys[1], strokeColor=CORAL, strokeWidth=0.8, strokeDashArray=[1, 2]))
    t_el = et_ms * 1.15
    d.add(Rect(X(0), ys[2] - 4, t_el * sc, 8, fillColor=AMBER_SOFT, strokeColor=AMBER, strokeWidth=0.6))
    d.add(chip(X(t_el / 2), ys[2] - 15, f"randomised election timeout ({et_ms}–{2 * et_ms} ms)", 6.3, "Sans", INK, bg=SOFT))
    d.add(arrow(X(t_el), ys[2] - 1, X(t_el + 18), ys[3] + 2, AMBER, 0.9))
    d.add(arrow(X(t_el + 26), ys[3] + 2, X(t_el + 44), ys[2] - 1, AMBER, 0.9))
    d.add(chip(X(t_el + 22), ys[3] - 13, "pre-vote, then vote", 6.3, "Sans", MUTED, bg=SOFT))
    t_lead = t_el + 50
    d.add(Circle(X(t_lead), ys[2], 4.5, fillColor=NODE[1], strokeColor=None))
    d.add(chip(X(t_lead) + 8, ys[2] - 14, "n2 leads (term + 1)", 6.5, "Sans-Bold", NODE[1], "start", bg=SOFT))
    d.add(arrow(X(70), ys[0], X(82), ys[1] + 4, TEAL, 0.9))
    d.add(chip(X(90), ys[1] + 13, "connection refused → retry", 6.2, "Sans", MUTED, "start", bg=SOFT))
    d.add(arrow(X(300), ys[0], X(318), ys[2] + 5, MUTED, 0.8, dash=[2, 2]))
    d.add(chip(X(326), ys[2] + 14, "no leader yet → back off", 6.2, "Sans", MUTED, "start", bg=SOFT))
    d.add(arrow(X(t_lead + 22), ys[0], X(t_lead + 40), ys[2] + 5, TEAL, 1.1))
    d.add(arrow(X(t_lead + 52), ys[2] + 5, X(t_lead + 70), ys[0] - 1, GREEN, 1.1))
    d.add(chip(X(t_lead + 74), ys[0] + 4, "✓ served", 7, "Sans-Bold", GREEN, "start", bg=SOFT))
    d.add(Line(X(0), 22, X(rec_ms), 22, strokeColor=NAVY, strokeWidth=1.3))
    d.add(Line(X(0), 18, X(0), 26, strokeColor=NAVY, strokeWidth=1.3))
    d.add(Line(X(rec_ms), 18, X(rec_ms), 26, strokeColor=NAVY, strokeWidth=1.3))
    d.add(text(X(rec_ms / 2), 9, f"measured client recovery: {rec_ms:.0f} ms (median) · zero failed requests", 7,
               "Sans-Bold", NAVY, "middle"))
    return d


def gray_failure(before, after):
    """The paused-leader bug: before and after the per-attempt timeout."""
    H = 196
    d = Drawing(W, H)
    pw = (W - 14) / 2

    def panel(px, title, col, fill, fixed):
        d.add(box(px, 0, pw, H, fill=fill, stroke=None, r=8))
        d.add(text(px + 12, H - 16, title, 8.4, "Sans-Bold", col))
        lanes = [("client", TEAL), ("n1 · paused", AMBER), ("n2 · new leader", NODE[1])]
        ys = [H - 46, H - 86, H - 126]
        for (name, c), y in zip(lanes, ys):
            d.add(text(px + 10, y - 3, name, 6.8, "Sans-Bold", c))
            d.add(Line(px + 74, y, px + pw - 10, y, strokeColor=FAINT, strokeWidth=0.8))
        x0 = px + 80
        # n1 frozen band
        d.add(Rect(x0, ys[1] - 4, pw - 92, 8, fillColor=AMBER_SOFT, strokeColor=None))
        d.add(text(px + pw - 14, ys[1] + 7, "SIGSTOP: kernel still accepts TCP", 6, "Sans", AMBER, "end"))
        d.add(arrow(x0 + 4, ys[0], x0 + 14, ys[1] + 5, TEAL, 0.9))
        if not fixed:
            d.add(Rect(x0 + 14, ys[0] - 3, pw - 112, 6, fillColor=CORAL_SOFT, strokeColor=CORAL, strokeWidth=0.6))
            d.add(text(x0 + 14 + (pw - 112) / 2, ys[0] + 7, "waits for the whole 5 s deadline", 6.5, "Sans", CORAL, "middle"))
            d.add(text(px + pw - 16, ys[0] - 14, "✕ fails", 7.5, "Sans-Bold", CORAL, "end"))
        else:
            d.add(Rect(x0 + 14, ys[0] - 3, 46, 6, fillColor=AMBER_SOFT, strokeColor=AMBER, strokeWidth=0.6))
            d.add(text(x0 + 37, ys[0] + 7, "1 s attempt", 6.5, "Sans", AMBER, "middle"))
            d.add(chip(x0 - 72, ys[0] - 17, "then forget n1, skip it", 5.8, "Sans", MUTED, "start", bg=colors.HexColor("#eaf6f4")))
            d.add(arrow(x0 + 62, ys[0], x0 + 76, ys[2] + 5, TEAL, 1.0))
            d.add(arrow(x0 + 84, ys[2] + 5, x0 + 98, ys[0] - 1, GREEN, 1.0))
            d.add(text(x0 + 102, ys[0] + 3, "✓ served", 7.5, "Sans-Bold", GREEN))
        return ys

    panel(0, "Before: the original client", CORAL, colors.HexColor("#fdf1f2"), False)
    panel(pw + 14, "After: per-attempt timeout (1 s)", GREEN, colors.HexColor("#eaf6f4"), True)
    for px, stats, col in [(0, before, CORAL), (pw + 14, after, GREEN)]:
        for i, (k, v) in enumerate(stats):
            bx = px + 12 + i * (pw - 24) / 2
            d.add(box(bx, 10, (pw - 24) / 2 - 6, 34, fill=WHITE, stroke=None, r=5))
            d.add(text(bx + 8, 26, v, 12, "Sans-Bold", col))
            d.add(text(bx + 8, 15, k, 6.3, "Sans", MUTED))
    return d


def minority(no_quorum, shards=24):
    """Two nodes cut off: which shards keep a majority."""
    H = 176
    d = Drawing(W, H)
    d.add(box(0, 0, W, H, fill=SOFT, stroke=None, r=8))
    # islands
    d.add(box(14, 72, 150, 92, fill=CORAL_SOFT, stroke=CORAL, sw=0.9, dash=[3, 2]))
    d.add(text(89, 148, "minority side", 7.6, "Sans-Bold", CORAL, "middle"))
    d.add(box(194, 72, 296, 92, fill=TEAL_SOFT, stroke=TEAL, sw=0.9, dash=[3, 2]))
    d.add(text(342, 148, "majority side", 7.6, "Sans-Bold", TEAL, "middle"))
    for i, x in enumerate([52, 126]):
        d.add(Circle(x, 108, 17, fillColor=WHITE, strokeColor=NODE[i], strokeWidth=1.5))
        d.add(text(x, 105.5, f"n{i + 1}", 8, "Sans-Bold", NODE[i], "middle"))
    for j, x in enumerate([262, 342, 422]):
        d.add(Circle(x, 108, 17, fillColor=WHITE, strokeColor=NODE[j + 2], strokeWidth=1.5))
        d.add(text(x, 105.5, f"n{j + 3}", 8, "Sans-Bold", NODE[j + 2], "middle"))
    # partition line
    d.add(Line(179, 70, 179, 166, strokeColor=CORAL, strokeWidth=2, strokeDashArray=[4, 3]))
    d.add(text(179, 60, "partition", 6.6, "Sans-Bold", CORAL, "middle"))

    def shard(x, label, reps, ok, note):
        d.add(box(x, 6, 228, 48, fill=WHITE, stroke=GREEN if ok else CORAL, sw=1))
        d.add(text(x + 10, 42, label, 7.2, "Sans-Bold", NAVY))
        cx = x + 10
        for r in reps:
            n = int(r[1]) - 1
            on_min = n < 2
            d.add(Rect(cx, 13, 26, 13, rx=3, ry=3, fillColor=CORAL_SOFT if on_min else TEAL_SOFT,
                       strokeColor=NODE[n], strokeWidth=0.8))
            d.add(text(cx + 13, 16.5, r, 6.6, "Sans-Bold", NODE[n], "middle"))
            cx += 30
        d.add(text(x + 106, 25, note[0], 6.6, "Sans-Bold", GREEN if ok else CORAL))
        d.add(text(x + 106, 14, note[1], 6.3, "Sans", MUTED))

    shard(14, "shard with replicas on n1, n2, n4", ["n1", "n2", "n4"], False,
          ("1 of 3 reachable: no quorum", f"refuses requests ({no_quorum:.0f} of {shards})"))
    shard(262, "shard with replicas on n1, n3, n5", ["n1", "n3", "n5"], True,
          ("2 of 3 on the majority side", f"elects a leader, serves ({shards - no_quorum:.0f} of {shards})"))
    return d


def cover_art(c, x, y, size):
    """Five nodes on a ring, one struck: drawn straight onto the canvas."""
    import math

    c.saveState()
    c.translate(x, y)
    r = size / 2
    c.setStrokeColor(colors.HexColor("#2b3657"))
    c.setLineWidth(1)
    for k in range(3):
        c.circle(0, 0, r * (0.55 + k * 0.22), stroke=1, fill=0)
    pts = []
    for i in range(5):
        a = math.pi / 2 + 2 * math.pi * i / 5
        pts.append((r * 0.77 * math.cos(a), r * 0.77 * math.sin(a)))
    c.setStrokeColor(colors.HexColor("#3c4a72"))
    c.setLineWidth(0.8)
    for i in range(5):
        for j in range(i + 1, 5):
            if i == 0 or j == 0:
                c.setDash(2, 3)
                c.setStrokeColor(colors.HexColor("#7a3445"))
            else:
                c.setDash()
                c.setStrokeColor(colors.HexColor("#3c4a72"))
            c.line(pts[i][0], pts[i][1], pts[j][0], pts[j][1])
    c.setDash()
    for i, (px, py) in enumerate(pts):
        col = CORAL if i == 0 else NODE[i]
        c.setFillColor(col)
        c.setStrokeColor(colors.HexColor("#12192b"))
        c.setLineWidth(3)
        c.circle(px, py, r * 0.13, stroke=1, fill=1)
        c.setFillColor(WHITE)
        c.setFont("Sans-Bold", r * 0.1)
        c.drawCentredString(px, py - r * 0.035, f"n{i + 1}")
    # bolt on the struck node
    bx, by = pts[0]
    c.setFillColor(AMBER)
    s = r * 0.012
    bolt = [(0, 14), (-6, 0), (-1, 0), (-4, -12), (6, 3), (1, 3), (4, 14)]
    p = c.beginPath()
    p.moveTo(bx + r * 0.2 + bolt[0][0] * s * 10 / 10 * 1.6, by + r * 0.16 + bolt[0][1] * s * 1.6)
    for qx, qy in bolt[1:]:
        p.lineTo(bx + r * 0.2 + qx * s * 1.6, by + r * 0.16 + qy * s * 1.6)
    p.close()
    c.drawPath(p, stroke=0, fill=1)
    c.restoreState()
