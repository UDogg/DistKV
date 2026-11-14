#!/usr/bin/env python3
"""Builds the study's PDF report from study/results.

    python3 study/plot.py && python3 study/report.py [results-dir] [out.pdf]

Every number in the report is read from the results, so re-running the
experiments and this script regenerates a consistent document. The prose
lives in narrative.py and the vector diagrams in diagrams.py.
"""
import json
import os
import re
import sys
from datetime import date
from pathlib import Path

import matplotlib
from reportlab import rl_config
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (BaseDocTemplate, CondPageBreak, Flowable, Frame, Image, KeepTogether,
                                NextPageTemplate, PageBreak, PageTemplate, Paragraph, Spacer, Table, TableStyle)
from reportlab.platypus.tableofcontents import TableOfContents

# Byte-identical output for identical results: no timestamps or random IDs.
rl_config.invariant = 1

RESULTS = Path(sys.argv[1] if len(sys.argv) > 1 else "study/results")
OUT = Path(sys.argv[2] if len(sys.argv) > 2 else RESULTS.parent / "raft-fault-injection-study.pdf")
FIGS = RESULTS / "figures"

# DejaVu ships with matplotlib, so the report renders the same everywhere
# (and has the arrows, check marks and math symbols the base-14 fonts lack).
FONT_DIR = Path(matplotlib.get_data_path()) / "fonts" / "ttf"
for name, file in [("Sans", "DejaVuSans"), ("Sans-Bold", "DejaVuSans-Bold"), ("Sans-Oblique", "DejaVuSans-Oblique"),
                   ("Sans-BoldOblique", "DejaVuSans-BoldOblique"), ("Mono", "DejaVuSansMono"),
                   ("Mono-Bold", "DejaVuSansMono-Bold")]:
    pdfmetrics.registerFont(TTFont(name, str(FONT_DIR / f"{file}.ttf")))
pdfmetrics.registerFontFamily("Sans", normal="Sans", bold="Sans-Bold", italic="Sans-Oblique", boldItalic="Sans-BoldOblique")

NAVY = colors.HexColor("#12192b")
INK = colors.HexColor("#1f2333")
MUTED = colors.HexColor("#5b6172")
ACCENT = colors.HexColor("#00798c")
CRASH = colors.HexColor("#d1495b")
AMBER = colors.HexColor("#edae49")
RULE = colors.HexColor("#d9dce3")
SOFT = colors.HexColor("#f3f5f8")
TEAL_SOFT = colors.HexColor("#e6f3f5")
PAGE_W, PAGE_H = letter
MARGIN = 0.75 * inch

ss = {
    "h1": ParagraphStyle("h1", fontName="Sans-Bold", fontSize=16, leading=20, textColor=NAVY),
    "h2": ParagraphStyle("h2", fontName="Sans-Bold", fontSize=11.5, leading=15, textColor=ACCENT, spaceBefore=10, spaceAfter=4),
    "body": ParagraphStyle("body", fontName="Sans", fontSize=9.4, leading=14, textColor=INK, spaceAfter=6, alignment=TA_LEFT),
    "lead": ParagraphStyle("lead", fontName="Sans", fontSize=10.6, leading=15.5, textColor=INK, spaceAfter=8),
    "small": ParagraphStyle("small", fontName="Sans", fontSize=7.8, leading=10.8, textColor=MUTED, spaceAfter=4),
    "caption": ParagraphStyle("caption", fontName="Sans", fontSize=7.8, leading=10.8, textColor=MUTED, spaceBefore=4, spaceAfter=10),
    "bullet": ParagraphStyle("bullet", fontName="Sans", fontSize=9.4, leading=13.4, textColor=INK, leftIndent=13,
                             bulletIndent=2, spaceAfter=3.5, bulletColor=ACCENT,
                             bulletFontName="Sans", bulletFontSize=6.5),
    "cell": ParagraphStyle("cell", fontName="Sans", fontSize=7.7, leading=10, textColor=INK),
    "cellb": ParagraphStyle("cellb", fontName="Sans-Bold", fontSize=7.7, leading=10, textColor=NAVY),
    "code": ParagraphStyle("code", fontName="Mono", fontSize=8, leading=11.5, textColor=colors.HexColor("#e8ecf6"),
                           backColor=NAVY, borderPadding=(7, 8, 7, 8), spaceBefore=6, spaceAfter=10),
    "callout": ParagraphStyle("callout", fontName="Sans", fontSize=9.8, leading=14.2, textColor=INK),
    "quote": ParagraphStyle("quote", fontName="Sans-Oblique", fontSize=12, leading=17, textColor=NAVY),
    "q": ParagraphStyle("q", fontName="Sans-Bold", fontSize=9.6, leading=13.5, textColor=NAVY, spaceBefore=7, spaceAfter=2),
    "ref": ParagraphStyle("ref", fontName="Sans", fontSize=8.2, leading=11.6, textColor=INK, leftIndent=22,
                          firstLineIndent=-22, spaceAfter=5),
    "term": ParagraphStyle("term", fontName="Sans", fontSize=8.8, leading=12.5, textColor=INK, spaceAfter=5),
    "toc0": ParagraphStyle("toc0", fontName="Sans-Bold", fontSize=10, leading=13, textColor=NAVY, leftIndent=0, spaceBefore=5),
    "toc1": ParagraphStyle("toc1", fontName="Sans", fontSize=8.6, leading=11.5, textColor=MUTED, leftIndent=26),
}


# ---------------------------------------------------------------------------
# Flowables

class SectionHeader(Flowable):
    """A numbered badge and the section title; feeds the TOC and outline."""

    def __init__(self, text):
        super().__init__()
        m = re.match(r"^(Appendix )?([0-9A-Z]+)\.\s+(.*)$", text)
        if m:
            self.num, self.title = m.group(2), m.group(3)
            self.toc = (f"Appendix {m.group(2)}. " if m.group(1) else f"{m.group(2)}. ") + m.group(3)
        else:
            self.num, self.title, self.toc = "", text, text
        self.key = "sec-" + re.sub(r"[^a-z0-9]+", "-", self.toc.lower()).strip("-")

    def wrap(self, aw, ah):
        self.width = aw
        return aw, 40

    def draw(self):
        c = self.canv
        c.setFillColor(NAVY if self.num else ACCENT)
        c.roundRect(0, 8, 28, 28, 6, stroke=0, fill=1)
        c.setFillColor(colors.white)
        c.setFont("Sans-Bold", 13 if len(self.num) < 2 else 11)
        c.drawCentredString(14, 17, self.num or "◆")
        c.setFillColor(NAVY)
        c.setFont("Sans-Bold", 16)
        c.drawString(40, 15, self.title)
        c.setStrokeColor(RULE)
        c.setLineWidth(0.8)
        c.line(40, 6, self.width, 6)
        c.bookmarkPage(self.key)


class StatTiles(Flowable):
    """A row of big-number tiles."""

    def __init__(self, tiles, height=58):
        super().__init__()
        self.tiles, self.h = tiles, height

    def wrap(self, aw, ah):
        self.width = aw
        return aw, self.h

    def draw(self):
        c = self.canv
        n = len(self.tiles)
        gap = 8
        w = (self.width - gap * (n - 1)) / n
        for i, (value, label, col) in enumerate(self.tiles):
            x = i * (w + gap)
            c.setFillColor(SOFT)
            c.roundRect(x, 0, w, self.h, 7, stroke=0, fill=1)
            c.setFillColor(col)
            c.roundRect(x, self.h - 4, w, 4, 2, stroke=0, fill=1)
            size = 19
            while size > 10 and pdfmetrics.stringWidth(value, "Sans-Bold", size) > w - 20:
                size -= 0.5
            c.setFont("Sans-Bold", size)
            c.drawString(x + 10, self.h - 30, value)
            c.setFillColor(MUTED)
            c.setFont("Sans", 7.4)
            for j, line in enumerate(label.split("\n")):
                c.drawString(x + 10, self.h - 43 - j * 9, line)


# ---------------------------------------------------------------------------
# Document

class Report(BaseDocTemplate):
    def __init__(self, path, cover, **kw):
        super().__init__(path, pagesize=letter, leftMargin=MARGIN, rightMargin=MARGIN, topMargin=0.85 * inch,
                         bottomMargin=0.85 * inch, **kw)
        self.cover = cover
        self.section = ""
        self.page_section = ""
        self._first = True
        full = Frame(0, 0, PAGE_W, PAGE_H, id="cover", leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
        body = Frame(MARGIN, 0.85 * inch, PAGE_W - 2 * MARGIN, PAGE_H - 1.7 * inch, id="body")
        self.addPageTemplates([PageTemplate("cover", [full], onPage=self.draw_cover),
                               PageTemplate("body", [body], onPageEnd=self.decorate)])

    def handle_documentBegin(self):
        super().handle_documentBegin()
        self.section = self.page_section = ""  # each pass of multiBuild starts afresh

    def handle_pageBegin(self):
        super().handle_pageBegin()
        self.page_section = self.section  # the running header names the section a page starts in
        self._first = True

    def afterFlowable(self, f):
        from reportlab.platypus.doctemplate import ActionFlowable

        if isinstance(f, ActionFlowable):
            return  # page breaks and template switches aren't content
        first, self._first = self._first, False
        if isinstance(f, SectionHeader):
            self.section = f.toc
            if first:
                self.page_section = f.toc
            if f.toc == "Contents":
                return
            self.notify("TOCEntry", (0, f.toc, self.page, f.key))
            self.canv.addOutlineEntry(f.toc, f.key, level=0, closed=False)
        elif isinstance(f, Paragraph) and f.style.name == "h2":
            text = f.getPlainText()
            key = "sub-" + re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
            self.canv.bookmarkPage(key)
            self.notify("TOCEntry", (1, text, self.page, key))
            self.canv.addOutlineEntry(text, key, level=1)

    def decorate(self, c, doc):
        c.saveState()
        # running header
        c.setFillColor(NAVY)
        c.setFont("Sans-Bold", 7)
        c.drawString(MARGIN, PAGE_H - 0.5 * inch, "RAFT UNDER FIRE")
        c.setFillColor(MUTED)
        c.setFont("Sans", 7)
        c.drawRightString(PAGE_W - MARGIN, PAGE_H - 0.5 * inch, self.page_section)
        c.setStrokeColor(ACCENT)
        c.setLineWidth(1.4)
        c.line(MARGIN, PAGE_H - 0.56 * inch, MARGIN + 34, PAGE_H - 0.56 * inch)
        c.setStrokeColor(RULE)
        c.setLineWidth(0.6)
        c.line(MARGIN + 36, PAGE_H - 0.56 * inch, PAGE_W - MARGIN, PAGE_H - 0.56 * inch)
        # footer
        c.setFillColor(MUTED)
        c.setFont("Sans", 7)
        c.drawString(MARGIN, 0.5 * inch, "DistKV · A Raft fault-injection study")
        c.setFillColor(NAVY)
        c.roundRect(PAGE_W - MARGIN - 26, 0.44 * inch, 26, 15, 7.5, stroke=0, fill=1)
        c.setFillColor(colors.white)
        c.setFont("Sans-Bold", 7.5)
        c.drawCentredString(PAGE_W - MARGIN - 13, 0.44 * inch + 4.3, str(doc.page))
        c.restoreState()

    def draw_cover(self, c, doc):
        from diagrams import cover_art

        k = self.cover
        c.saveState()
        band = PAGE_H * 0.6
        c.setFillColor(NAVY)
        c.rect(0, PAGE_H - band, PAGE_W, band, stroke=0, fill=1)
        # subtle grid
        c.setStrokeColor(colors.HexColor("#1b2440"))
        c.setLineWidth(0.5)
        for gx in range(0, int(PAGE_W) + 1, 24):
            c.line(gx, PAGE_H - band, gx, PAGE_H)
        for gy in range(int(PAGE_H - band), int(PAGE_H) + 1, 24):
            c.line(0, gy, PAGE_W, gy)
        c.setFillColor(ACCENT)
        c.rect(0, PAGE_H - band, PAGE_W, 5, stroke=0, fill=1)
        cover_art(c, PAGE_W - 2.2 * inch, PAGE_H - 2.55 * inch, 2.9 * inch)
        x = MARGIN
        c.setFillColor(colors.HexColor("#5fd0dd"))
        c.setFont("Sans-Bold", 8.5)
        c.drawString(x, PAGE_H - 1.15 * inch, "D I S T K V   ·   R E S E A R C H   R E P O R T")
        c.setFillColor(colors.white)
        c.setFont("Sans-Bold", 40)
        c.drawString(x, PAGE_H - 1.95 * inch, "Raft")
        c.drawString(x, PAGE_H - 2.55 * inch, "Under Fire")
        c.setFillColor(colors.HexColor("#c4cbe0"))
        c.setFont("Sans", 12.5)
        for i, line in enumerate(["A fault-injection study of a multi-Raft",
                                  "key-value store: availability, tail latency",
                                  "and recovery time under 12 injected faults"]):
            c.drawString(x, PAGE_H - 3.05 * inch - i * 17, line)
        c.setFillColor(colors.HexColor("#8f9bbd"))
        c.setFont("Sans", 8.5)
        c.drawString(x, PAGE_H - 4.1 * inch, f"{k['today']}   ·   commit {k['commit']}   ·   generated from the results by study/report.py")
        # stat tiles straddling the band edge
        tiles = k["tiles"]
        tw, th, gap = (PAGE_W - 2 * MARGIN - 3 * 10) / 4, 74, 10
        ty = PAGE_H - band - th / 2
        for i, (value, label, col) in enumerate(tiles):
            tx = MARGIN + i * (tw + gap)
            c.setFillColor(colors.HexColor("#0b1020"))
            c.roundRect(tx + 1.5, ty - 2, tw, th, 8, stroke=0, fill=1)
            c.setFillColor(colors.white)
            c.setStrokeColor(RULE)
            c.roundRect(tx, ty, tw, th, 8, stroke=1, fill=1)
            c.setFillColor(col)
            c.roundRect(tx, ty + th - 5, tw, 5, 2.5, stroke=0, fill=1)
            c.setFont("Sans-Bold", 22)
            c.drawString(tx + 12, ty + th - 34, value)
            c.setFillColor(MUTED)
            c.setFont("Sans", 7.6)
            for j, line in enumerate(label.split("\n")):
                c.drawString(tx + 12, ty + th - 48 - j * 9.5, line)
        # below the band
        y = ty - 0.55 * inch
        c.setFillColor(NAVY)
        c.setFont("Sans-Bold", 11)
        c.drawString(MARGIN, y, "In one sentence")
        c.setFillColor(INK)
        c.setFont("Sans", 10)
        wrapped = _wrap(k["sentence"], "Sans", 10, PAGE_W - 2 * MARGIN)
        for i, line in enumerate(wrapped):
            c.drawString(MARGIN, y - 16 - i * 14.5, line)
        y2 = y - 16 - len(wrapped) * 14.5 - 18
        c.setFillColor(NAVY)
        c.setFont("Sans-Bold", 11)
        c.drawString(MARGIN, y2, "Reproduce it")
        c.setFillColor(NAVY)
        c.roundRect(MARGIN, y2 - 34, 4.3 * inch, 22, 5, stroke=0, fill=1)
        c.setFillColor(colors.HexColor("#5fd0dd"))
        c.setFont("Mono-Bold", 9.5)
        c.drawString(MARGIN + 10, y2 - 26, "$ make study")
        c.setFillColor(colors.HexColor("#8f9bbd"))
        c.setFont("Mono", 7.8)
        c.drawString(MARGIN + 1.2 * inch, y2 - 26, "# ~40 min: data, figures and this PDF")
        c.setFillColor(MUTED)
        c.setFont("Sans", 7.5)
        c.drawString(MARGIN, 0.6 * inch, "github.com/UDogg/DistKV · study/")
        c.drawRightString(PAGE_W - MARGIN, 0.6 * inch, k["footnote"])
        c.restoreState()


def _wrap(s, font, size, width):
    words, lines, cur = s.split(), [], ""
    for w in words:
        t = (cur + " " + w).strip()
        if pdfmetrics.stringWidth(t, font, size) > width:
            lines.append(cur)
            cur = w
        else:
            cur = t
    if cur:
        lines.append(cur)
    return lines


# ---------------------------------------------------------------------------
# Helpers handed to the narrative

def P(text, style="body"):
    if style == "h1":
        return SectionHeader(text)
    return Paragraph(text, ss[style])


def bullets(items, style="bullet"):
    return [Paragraph(t, ss[style], bulletText="●") for t in items]


def callout(flowables, bg=TEAL_SOFT, edge=ACCENT, width=7.0 * inch):
    t = Table([[flowables]], colWidths=[width])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), bg), ("LINEBEFORE", (0, 0), (0, -1), 3.5, edge),
        ("LEFTPADDING", (0, 0), (-1, -1), 14), ("RIGHTPADDING", (0, 0), (-1, -1), 14),
        ("TOPPADDING", (0, 0), (-1, -1), 10), ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
    ]))
    return t


def quote(text):
    return callout([Paragraph(text, ss["quote"])], bg=colors.white, edge=AMBER)


_fig = [0]
_fignums = {}


def fig(name):
    """The number of a figure already placed, by file name or label."""
    return _fignums[name]


def fig_next():
    return _fig[0] + 1


def _caption(text):
    _fig[0] += 1
    return Paragraph(f"<b><font color='#12192b'>Figure {_fig[0]}.</font></b> {text}", ss["caption"])


def figure(name, width=7.0 * inch, caption=None):
    path = FIGS / name
    if not path.exists():
        return []
    from PIL import Image as PILImage  # matplotlib depends on Pillow

    w, h = PILImage.open(path).size
    out = [Image(str(path), width=width, height=width * h / w)]
    if caption:
        out.append(_caption(caption))
        _fignums[name] = _fig[0]
    return [KeepTogether(out)]


def diagram(drawing, caption, label=None):
    cap = _caption(caption)
    if label:
        _fignums[label] = _fig[0]
    return [KeepTogether([Spacer(1, 2), drawing, cap])]


def cite(*ns):
    return f"<font color='#00798c' size='8'>[{', '.join(str(n) for n in ns)}]</font>"


def ms(v):
    if v is None:
        return "–"
    if v == 0:
        return "0 ms"
    if v >= 1000:
        return f"{v / 1000:.2f} s"
    if v >= 10:
        return f"{v:.0f} ms"
    return f"{v:.1f} ms"


def pct(a, digits=3):
    if a is None:
        return "–"
    if a >= 1:
        return "100%"
    return f"{a * 100:.{digits}f}%"


def load():
    meta = json.loads((RESULTS / "meta.json").read_text())
    rows = json.loads((RESULTS / "summary.json").read_text())
    runs = json.loads((RESULTS / "runs.json").read_text())
    return meta, {r["id"]: r for r in rows}, rows, runs


def main():
    meta, S, rows, runs = load()
    import diagrams
    from narrative import OLD_CLIENT, build

    faults = [r for r in rows if r["id"] not in ("baseline", OLD_CLIENT)]
    masked = [r for r in faults if (r["impact_errors"] or 0) == 0]
    total_ops = sum(r["overall"]["ops"] for r in runs)
    today = date.today().strftime("%B %Y")
    pa, pn, c = S["pause-leader"], S[OLD_CLIENT], S["crash-leader"]
    cover = {
        "today": today, "commit": meta["git_commit"],
        "tiles": [(str(len(faults)), "fault scenarios,\ncrash to partition", CRASH),
                  (str(len(runs)), "runs, each on a\nfresh 5-node cluster", ACCENT),
                  (f"{total_ops / 1e6:.2f}M", "operations, timed from\nscheduled start", NAVY),
                  (f"{len(masked)}/{len(faults)}", "faults with zero\nfailed requests", AMBER)],
        "sentence": (f"Raft masked {len(masked)} of {len(faults)} injected faults without a single failed request, "
                     f"recovered from a crashed leader in {ms(c['recovery_ms'])}, refused requests only where a "
                     f"majority was lost, and exposed a gray-failure bug in the client: a paused leader cost "
                     f"{pct(1 - pn['fault_availability'], 1)} of requests until a per-attempt timeout fixed it "
                     f"({ms(pn['recovery_ms'])} → {ms(pa['recovery_ms'])} recovery)."),
        "footnote": f"{meta['nodes']} nodes · {meta['shards']} shards · RF {meta['replication']} · {meta['rate']:,} ops/s · {meta['runs']} runs per scenario",
    }

    toc = TableOfContents(levelStyles=[ss["toc0"], ss["toc1"]], dotsMinLevel=0)
    front = [Spacer(1, 1), NextPageTemplate("body"), PageBreak(), P("Contents", "h1"), Spacer(1, 6), toc, PageBreak()]
    k = dict(fig=fig, fig_next=fig_next, P=P, bullets=bullets, callout=callout, quote=quote, figure=figure, diagram=diagram, cite=cite, ms=ms,
             pct=pct, ss=ss, Table=Table, TableStyle=TableStyle, Spacer=Spacer, PageBreak=PageBreak,
             CondPageBreak=CondPageBreak, KeepTogether=KeepTogether, StatTiles=StatTiles, colors=colors, inch=inch,
             INK=INK, MUTED=MUTED, ACCENT=ACCENT, CRASH=CRASH, AMBER=AMBER, NAVY=NAVY, RULE=RULE, SOFT=SOFT,
             TEAL_SOFT=TEAL_SOFT, today=today, diagrams=diagrams)

    def story():
        _fig[0] = 0
        _fignums.clear()
        return front[:5] + [TableOfContents(levelStyles=[ss["toc0"], ss["toc1"]], dotsMinLevel=0)] + front[6:] + \
            build(meta, S, rows, runs, k)

    doc = Report(str(OUT), cover, title="Raft Under Fire: a fault-injection study of DistKV", author="DistKV",
                 subject="Availability, tail latency and recovery time of Raft under injected faults",
                 keywords="Raft, consensus, fault injection, chaos engineering, distributed systems, DistKV")
    # Two passes: the first collects page numbers for the table of contents.
    s1 = story()
    doc.multiBuild(s1)
    print(f"wrote {OUT} ({os.path.getsize(OUT) / 1024:.0f} KiB)")


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent))
    main()
