#!/usr/bin/env python3
"""Builds the study's PDF report from study/results.

    python3 study/plot.py && python3 study/report.py [results-dir] [out.pdf]

Every number in the report is read from the results, so re-running the
experiments and this script regenerates a consistent document.
"""
import json
import os
import sys
from datetime import date
from pathlib import Path

import matplotlib
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (CondPageBreak, Image, KeepTogether, PageBreak, Paragraph, SimpleDocTemplate, Spacer,
                                Table, TableStyle)

RESULTS = Path(sys.argv[1] if len(sys.argv) > 1 else "study/results")
OUT = Path(sys.argv[2] if len(sys.argv) > 2 else RESULTS.parent / "raft-fault-injection-study.pdf")
FIGS = RESULTS / "figures"

# DejaVu ships with matplotlib, so the report renders the same everywhere
# (and has the arrows and math symbols the base-14 PDF fonts lack).
FONT_DIR = Path(matplotlib.get_data_path()) / "fonts" / "ttf"
pdfmetrics.registerFont(TTFont("Sans", str(FONT_DIR / "DejaVuSans.ttf")))
pdfmetrics.registerFont(TTFont("Sans-Bold", str(FONT_DIR / "DejaVuSans-Bold.ttf")))
pdfmetrics.registerFont(TTFont("Sans-Oblique", str(FONT_DIR / "DejaVuSans-Oblique.ttf")))
pdfmetrics.registerFont(TTFont("Sans-BoldOblique", str(FONT_DIR / "DejaVuSans-BoldOblique.ttf")))
pdfmetrics.registerFont(TTFont("Mono", str(FONT_DIR / "DejaVuSansMono.ttf")))
pdfmetrics.registerFontFamily("Sans", normal="Sans", bold="Sans-Bold", italic="Sans-Oblique", boldItalic="Sans-BoldOblique")

INK = colors.HexColor("#1f2333")
MUTED = colors.HexColor("#5b6172")
ACCENT = colors.HexColor("#00798c")
CRASH = colors.HexColor("#d1495b")
RULE = colors.HexColor("#d9dce3")
SOFT = colors.HexColor("#f3f5f8")
TEAL_SOFT = colors.HexColor("#e6f3f5")

ss = {
    "title": ParagraphStyle("title", fontName="Sans-Bold", fontSize=24, leading=29, textColor=INK, spaceAfter=6),
    "subtitle": ParagraphStyle("subtitle", fontName="Sans", fontSize=12.5, leading=17, textColor=MUTED, spaceAfter=14),
    "h1": ParagraphStyle("h1", fontName="Sans-Bold", fontSize=15, leading=19, textColor=INK, spaceBefore=12, spaceAfter=6),
    "h2": ParagraphStyle("h2", fontName="Sans-Bold", fontSize=11.5, leading=15, textColor=ACCENT, spaceBefore=9, spaceAfter=4),
    "body": ParagraphStyle("body", fontName="Sans", fontSize=9.6, leading=14, textColor=INK, spaceAfter=6, alignment=TA_LEFT),
    "small": ParagraphStyle("small", fontName="Sans", fontSize=8, leading=11, textColor=MUTED, spaceAfter=4),
    "bullet": ParagraphStyle("bullet", fontName="Sans", fontSize=9.6, leading=13.6, textColor=INK, leftIndent=12,
                             bulletIndent=2, spaceAfter=3),
    "cell": ParagraphStyle("cell", fontName="Sans", fontSize=7.8, leading=10, textColor=INK),
    "cellb": ParagraphStyle("cellb", fontName="Sans-Bold", fontSize=7.8, leading=10, textColor=INK),
    "code": ParagraphStyle("code", fontName="Mono", fontSize=8.2, leading=11.5, textColor=INK, backColor=SOFT,
                           borderPadding=(5, 6, 5, 6), spaceBefore=4, spaceAfter=8),
    "callout": ParagraphStyle("callout", fontName="Sans", fontSize=10, leading=14.5, textColor=INK),
    "q": ParagraphStyle("q", fontName="Sans-Bold", fontSize=9.6, leading=13.5, textColor=INK, spaceBefore=6, spaceAfter=2),
}


def P(text, style="body"):
    return Paragraph(text, ss[style])


def bullets(items, style="bullet"):
    return [Paragraph(t, ss[style], bulletText="•") for t in items]


def callout(flowables, bg=TEAL_SOFT, edge=ACCENT, width=7.0 * inch):
    t = Table([[flowables]], colWidths=[width])
    t.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), bg), ("LINEBEFORE", (0, 0), (0, -1), 3, edge),
        ("LEFTPADDING", (0, 0), (-1, -1), 12), ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 9), ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
    ]))
    return t


def figure(name, width=7.0 * inch, caption=None):
    path = FIGS / name
    if not path.exists():
        return []
    from PIL import Image as PILImage  # matplotlib depends on Pillow

    w, h = PILImage.open(path).size
    out = [Image(str(path), width=width, height=width * h / w)]
    if caption:
        out.append(P(caption, "small"))
    return [KeepTogether(out)]


def ms(v, signed=False):
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
    s = f"{a * 100:.{digits}f}%"
    return s


def on_page(canvas, doc):
    canvas.saveState()
    canvas.setFont("Sans", 7.5)
    canvas.setFillColor(MUTED)
    if doc.page > 1:
        canvas.drawString(0.75 * inch, 0.5 * inch, "Raft fault-injection study · DistKV")
        canvas.drawRightString(letter[0] - 0.75 * inch, 0.5 * inch, f"{doc.page}")
        canvas.setStrokeColor(RULE)
        canvas.line(0.75 * inch, 0.65 * inch, letter[0] - 0.75 * inch, 0.65 * inch)
    canvas.restoreState()


def load():
    meta = json.loads((RESULTS / "meta.json").read_text())
    rows = json.loads((RESULTS / "summary.json").read_text())
    runs = json.loads((RESULTS / "runs.json").read_text())
    return meta, {r["id"]: r for r in rows}, rows, runs


def main():
    meta, S, rows, runs = load()
    from narrative import build  # the prose lives next to this file

    story = build(meta, S, rows, runs, dict(P=P, bullets=bullets, callout=callout, figure=figure, ms=ms, pct=pct,
                                            ss=ss, Table=Table, TableStyle=TableStyle, Spacer=Spacer,
                                            PageBreak=PageBreak, CondPageBreak=CondPageBreak, KeepTogether=KeepTogether,
                                            colors=colors, inch=inch, INK=INK, MUTED=MUTED, ACCENT=ACCENT,
                                            CRASH=CRASH, RULE=RULE, SOFT=SOFT, TEAL_SOFT=TEAL_SOFT,
                                            today=date.today().strftime("%B %Y")))
    doc = SimpleDocTemplate(str(OUT), pagesize=letter, leftMargin=0.75 * inch, rightMargin=0.75 * inch,
                            topMargin=0.7 * inch, bottomMargin=0.85 * inch,
                            title="Raft fault-injection study on DistKV", author="DistKV",
                            subject="Availability, tail latency and recovery time of Raft under injected faults")
    doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
    print(f"wrote {OUT} ({os.path.getsize(OUT) / 1024:.0f} KiB)")


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parent))
    main()
