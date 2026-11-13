#!/usr/bin/env python3
"""Plots for the DistKV fault-injection study.

Reads study/results (written by cmd/distkv-study) and writes PNG figures and
a summary table (summary.csv, summary.json) next to the data.

    python3 study/plot.py [results-dir]
"""
import csv
import json
import statistics
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

RESULTS = Path(sys.argv[1] if len(sys.argv) > 1 else "study/results")
FIGS = RESULTS / "figures"

# One colour per fault category, used consistently in every figure.
CAT_COLOR = {"baseline": "#8a8f98", "crash": "#d1495b", "partition": "#edae49", "slow": "#00798c"}
OK, ERR, P99 = "#2a9d8f", "#d1495b", "#3d405b"

plt.rcParams.update({
    "figure.dpi": 150, "savefig.dpi": 200, "font.size": 9, "axes.titlesize": 10, "axes.labelsize": 9,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.alpha": 0.25,
    "legend.frameon": False, "legend.fontsize": 8,
})


def load():
    meta = json.loads((RESULTS / "meta.json").read_text())
    runs = json.loads((RESULTS / "runs.json").read_text())
    catalog = {s["id"]: s for s in meta["scenarios"]}
    order = [s["id"] for s in meta["scenarios"] if any(r["scenario"] == s["id"] for r in runs)]
    by = {sid: sorted([r for r in runs if r["scenario"] == sid], key=lambda r: r["run"]) for sid in order}
    return meta, catalog, order, by


def bins(sid, run):
    with open(RESULTS / sid / f"run{run}" / "bins.csv") as f:
        return [{k: float(v) for k, v in row.items()} for row in csv.DictReader(f)]


def med(xs):
    xs = [x for x in xs if x is not None]
    return statistics.median(xs) if xs else None


def rng(xs):
    xs = [x for x in xs if x is not None]
    return (min(xs), max(xs)) if xs else (None, None)


def recovery(r):
    """Client-observed recovery in ms: masked (during the fault) if the
    system recovered before the heal, else fault duration + after-heal."""
    if r["recovery_ms"] >= 0:
        return r["recovery_ms"], True
    if r["recovery_after_heal_ms"] >= 0:
        return (r["heal_ms"] - r["inject_ms"]) + r["recovery_after_heal_ms"], False
    return None, False


def summary(order, by, catalog):
    rows = []
    for sid in order:
        rs = by[sid]
        rec = [recovery(r) for r in rs]
        masked = all(m for _, m in rec)
        row = {
            "id": sid, "title": catalog[sid]["title"], "category": catalog[sid]["category"], "runs": len(rs),
            "victim_shards_led": med([sum(r.get("victim_shards_led") or [0]) for r in rs]),
            "shards_without_quorum": med([r.get("shards_without_quorum", 0) for r in rs]),
            "baseline_p99_ms": med([r["baseline"]["p99_ms"] for r in rs]),
            "fault_availability": med([r["fault"]["availability"] for r in rs]),
            "fault_availability_range": rng([r["fault"]["availability"] for r in rs]),
            "impact_availability": med([r["impact"]["availability"] for r in rs]),
            "overall_availability": med([r["overall"]["availability"] for r in rs]),
            "fault_errors": med([r["fault"]["errors"] for r in rs]),
            "impact_errors": med([r["impact"]["errors"] for r in rs]),
            "fault_p50_ms": med([r["fault"]["p50_ms"] for r in rs]),
            "fault_p99_ms": med([r["fault"]["p99_ms"] for r in rs]),
            "fault_p99_range": rng([r["fault"]["p99_ms"] for r in rs]),
            "fault_p999_ms": med([r["fault"]["p999_ms"] for r in rs]),
            "impact_p99_ms": med([r["impact"]["p99_ms"] for r in rs]),
            "impact_p999_ms": med([r["impact"]["p999_ms"] for r in rs]),
            "impact_max_ms": med([r["impact"]["max_ms"] for r in rs]),
            "fault_write_p99_ms": med([r["fault"]["write_p99_ms"] for r in rs]),
            "fault_read_p99_ms": med([r["fault"]["read_p99_ms"] for r in rs]),
            "recovery_ms": med([v for v, _ in rec]),
            "recovery_range": rng([v for v, _ in rec]),
            "recovery_masked": masked,
            "recovery_after_heal_ms": med([r["recovery_after_heal_ms"] for r in rs if r["recovery_after_heal_ms"] >= 0]),
            "disrupted_ms": med([r["disrupted_ms"] for r in rs]),
            "disrupted_range": rng([r["disrupted_ms"] for r in rs]),
            "rejoin_ms": med([r["rejoin_ms"] for r in rs if r["rejoin_ms"] >= 0]),
            "fault_window_ms": med([r["heal_ms"] - r["inject_ms"] for r in rs]),
            "errors": [r.get("error") for r in rs if r.get("error")],
        }
        rows.append(row)
    return rows


def fmt_ms(v):
    if v is None:
        return "–"
    if v == 0:
        return "0 ms"
    return f"{v / 1000:.2f} s" if v >= 1000 else f"{v:.0f} ms" if v >= 10 else f"{v:.1f} ms"


def timeline_grid(order, by, catalog, ids, path, cols=2):
    ids = [i for i in ids if i in by]
    n = len(ids)
    rows = (n + cols - 1) // cols
    fig, axes = plt.subplots(rows, cols, figsize=(7.2, 1.95 * rows), squeeze=False)
    for ax in axes.flat[n:]:
        ax.remove()
    for ax, sid in zip(axes.flat, ids):
        # The run with the median disruption is the representative one.
        rs = sorted(by[sid], key=lambda r: r["disrupted_ms"])
        r = rs[len(rs) // 2]
        b = bins(sid, r["run"])
        t0 = r["inject_ms"]
        t = [(x["t_ms"] - t0) / 1000 for x in b]
        ok = [(x["ops"] - x["errors"]) * 10 for x in b]  # per second
        er = [x["errors"] * 10 for x in b]
        ax.fill_between(t, ok, step="post", color=OK, alpha=0.35, lw=0, label="ok ops/s")
        ax.fill_between(t, er, step="post", color=ERR, alpha=0.8, lw=0, label="errors/s")
        ax.set_ylim(0, max(ok + er) * 1.15 or 1)
        ax.set_ylabel("ops/s", color=OK)
        ax.axvspan(0, (r["heal_ms"] - t0) / 1000, color=CAT_COLOR[catalog[sid]["category"]], alpha=0.12, lw=0)
        ax.set_xlim(min(t), max(t))
        ax2 = ax.twinx()
        ax2.plot(t, [max(x["p99_ms"], 0.1) for x in b], color=P99, lw=0.8, label="p99")
        ax2.set_yscale("log")
        ax2.set_ylim(0.5, 10000)
        ax2.set_ylabel("p99 (ms)", color=P99)
        ax2.grid(False)
        ax2.spines["right"].set_visible(True)
        ax.set_title(catalog[sid]["title"], loc="left", fontsize=8.5)
        ax.set_xlabel("seconds from fault injection")
    handles = [plt.Rectangle((0, 0), 1, 1, color=OK, alpha=0.35), plt.Rectangle((0, 0), 1, 1, color=ERR),
               plt.Line2D([0], [0], color=P99, lw=1), plt.Rectangle((0, 0), 1, 1, color="#999", alpha=0.25)]
    fig.legend(handles, ["successful ops/s", "failed ops/s", "p99 latency per 100 ms (log)", "fault window"],
               loc="upper center", ncol=4, bbox_to_anchor=(0.5, 1.0))
    height = 1.95 * rows
    fig.tight_layout(rect=(0, 0, 1, 1 - 0.38 / height))
    fig.savefig(path)
    plt.close(fig)


def bars(rows, path):
    rows = [r for r in rows]
    names = [r["id"] for r in rows]
    colors = [CAT_COLOR[r["category"]] for r in rows]
    fig, axes = plt.subplots(1, 3, figsize=(7.2, 0.33 * len(rows) + 1.2), sharey=True)
    y = list(range(len(rows)))[::-1]

    # Unavailability during the fault window, as a percentage (log).
    ax = axes[0]
    un = [max((1 - r["fault_availability"]) * 100, 0.0005) for r in rows]
    ax.barh(y, un, color=colors)
    ax.set_xscale("log")
    ax.set_xlim(0.0005, 100)
    ax.set_title("Failed requests\n(fault window, %)")
    for yi, r, u in zip(y, rows, un):
        lab = "0" if r["fault_errors"] == 0 else f"{(1 - r['fault_availability']) * 100:.3g}%"
        ax.text(u * 1.3, yi, lab, va="center", fontsize=7)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))

    ax = axes[1]
    base = [max(r["baseline_p99_ms"], 0.1) for r in rows]
    p99 = [max(r["impact_p99_ms"], 0.1) for r in rows]
    p999 = [max(r["impact_p999_ms"], 0.1) for r in rows]
    ax.barh([v + 0.25 for v in y], base, height=0.25, color="#c9ccd1", label="baseline p99")
    ax.barh(y, p99, height=0.25, color=colors, label="p99")
    ax.barh([v - 0.25 for v in y], p999, height=0.25, color=colors, alpha=0.45, label="p99.9")
    ax.set_xscale("log")
    ax.set_title("Latency after injection\n(ms, log)")
    ax.legend(handles=[plt.Rectangle((0, 0), 1, 1, color="#c9ccd1"), plt.Rectangle((0, 0), 1, 1, color="#555b6e"),
                       plt.Rectangle((0, 0), 1, 1, color="#555b6e", alpha=0.45)],
              labels=["baseline p99", "p99", "p99.9"], loc="upper center", bbox_to_anchor=(0.5, -0.12 * 8 / max(len(rows), 4)), ncol=3, handlelength=1, columnspacing=0.8)

    ax = axes[2]
    rec = [(r["recovery_ms"] or 0) / 1000 for r in rows]
    lo = [(r["recovery_range"][0] or 0) / 1000 for r in rows]
    hi = [(r["recovery_range"][1] or 0) / 1000 for r in rows]
    ax.barh(y, rec, color=colors, xerr=[[a - b for a, b in zip(rec, lo)], [b - a for a, b in zip(rec, hi)]],
            error_kw={"lw": 0.8, "capsize": 2})
    for yi, r, v in zip(y, rows, rec):
        lab = fmt_ms(r["recovery_ms"]) + ("" if r["recovery_masked"] else " *")
        ax.text(v + 0.2, yi, lab, va="center", fontsize=7)
    ax.set_title("Recovery time\n(s, median and range)")
    ax.set_xlim(0, max(hi + [1]) * 1.45)
    axes[0].set_yticks(y, names)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


def election_timeout(by, path):
    ids = [("crash-leader-et250", 250), ("crash-leader", 500), ("crash-leader-et1000", 1000)]
    ids = [(i, et) for i, et in ids if i in by]
    if len(ids) < 2:
        return False
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.4))
    for ax, key, label in [(axes[0], "recovery", "Recovery time (ms)"), (axes[1], "p999", "p99.9 latency after injection (ms)")]:
        xs, meds, lo, hi = [], [], [], []
        for sid, et in ids:
            if key == "recovery":
                vals = [recovery(r)[0] for r in by[sid]]
            else:
                vals = [r["impact"]["p999_ms"] for r in by[sid]]
            vals = [v for v in vals if v is not None]
            xs.append(et)
            meds.append(statistics.median(vals))
            lo.append(min(vals))
            hi.append(max(vals))
            ax.scatter([et] * len(vals), vals, color=CAT_COLOR["crash"], alpha=0.35, s=12)
        ax.plot(xs, meds, color=CAT_COLOR["crash"], marker="o", lw=1.5)
        ax.plot(xs, xs, color="#999", ls="--", lw=0.8, label="y = election timeout")
        ax.set_xlabel("election timeout (ms)")
        ax.set_title(label, loc="left")
        ax.set_xticks(xs)
        ax.legend(loc="upper left")
        ax.set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    return True


def main():
    meta, catalog, order, by = load()
    FIGS.mkdir(parents=True, exist_ok=True)
    rows = summary(order, by, catalog)
    (RESULTS / "summary.json").write_text(json.dumps(rows, indent=2) + "\n")
    with open(RESULTS / "summary.csv", "w", newline="") as f:
        cols = ["id", "category", "runs", "fault_availability", "impact_availability", "fault_errors", "baseline_p99_ms",
                "fault_p99_ms", "fault_p999_ms", "impact_p99_ms", "impact_p999_ms", "impact_max_ms", "recovery_ms",
                "recovery_masked", "recovery_after_heal_ms", "disrupted_ms", "rejoin_ms", "shards_without_quorum"]
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    groups = {
        "timelines_crash.png": ["baseline", "crash-leader", "stop-leader", "rolling-restart", "crash-leader-et250", "crash-leader-et1000"],
        "timelines_partition.png": ["partition-leader", "partition-minority", "partition-flapping", "pause-leader",
                                    "pause-leader-no-attempt-timeout"],
        "timelines_slow.png": ["slow-link-50ms", "slow-link-200ms", "lossy-link-30"],
    }
    for name, ids in groups.items():
        if any(i in by for i in ids):
            timeline_grid(order, by, catalog, ids, FIGS / name)
    bars(rows, FIGS / "overview.png")
    election_timeout(by, FIGS / "election_timeout.png")
    for r in rows:
        print(f"{r['id']:<22} avail(fault) {r['fault_availability'] * 100:9.4f}%  p99 {fmt_ms(r['impact_p99_ms']):>8}  "
              f"p99.9 {fmt_ms(r['impact_p999_ms']):>8}  recovery {fmt_ms(r['recovery_ms']):>8}{'' if r['recovery_masked'] else '*'}  "
              f"disrupted {fmt_ms(r['disrupted_ms']):>8}  rejoin {fmt_ms(r['rejoin_ms']):>8}")
    print(f"figures in {FIGS}")


if __name__ == "__main__":
    main()
