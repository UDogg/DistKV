// A small, dependency-free, DPR-aware time-series chart on <canvas>.

export interface Series {
  name: string;
  color: string;
  /** Fill the area under the line. */
  fill?: boolean;
  /** Stack on top of the previous stacked series. */
  stack?: boolean;
  values: number[];
}

export interface ChartOptions {
  unit?: string;
  /** Format a y value for axis labels and tooltips. */
  format?: (v: number) => string;
  /** Use a log10 y axis (for latency). */
  log?: boolean;
  minMax?: number;
}

const tip = (() => {
  let el: HTMLDivElement | null = null;
  return () => {
    if (!el) {
      el = document.createElement("div");
      el.className = "chart-tip";
      el.hidden = true;
      document.body.appendChild(el);
    }
    return el;
  };
})();

export class TimeSeriesChart {
  private times: number[] = [];
  private series: Series[] = [];
  private hoverX: number | null = null;
  private readonly ctx: CanvasRenderingContext2D;

  constructor(private readonly canvas: HTMLCanvasElement, private readonly opts: ChartOptions = {}) {
    this.ctx = canvas.getContext("2d")!;
    new ResizeObserver(() => this.draw()).observe(canvas);
    canvas.addEventListener("mousemove", (e) => {
      this.hoverX = e.offsetX;
      this.draw();
      this.showTip(e);
    });
    canvas.addEventListener("mouseleave", () => {
      this.hoverX = null;
      tip().hidden = true;
      this.draw();
    });
  }

  set(times: number[], series: Series[]): void {
    this.times = times;
    this.series = series;
    this.draw();
  }

  private fmt(v: number): string {
    if (this.opts.format) return this.opts.format(v);
    if (v >= 1000) return `${(v / 1000).toFixed(v >= 10000 ? 0 : 1)}k`;
    return v >= 10 ? v.toFixed(0) : v.toFixed(v >= 1 ? 1 : 2);
  }

  private stacked(): number[][] {
    const out: number[][] = [];
    let base: number[] | null = null;
    for (const s of this.series) {
      if (s.stack && base) {
        const b: number[] = base;
        out.push(s.values.map((v, i) => v + (b[i] ?? 0)));
      } else {
        out.push(s.values.slice());
      }
      if (s.stack) base = out[out.length - 1];
    }
    return out;
  }

  draw(): void {
    const c = this.canvas;
    const dpr = window.devicePixelRatio || 1;
    const w = c.clientWidth;
    const h = c.clientHeight || 220;
    if (c.width !== Math.round(w * dpr) || c.height !== Math.round(h * dpr)) {
      c.width = Math.round(w * dpr);
      c.height = Math.round(h * dpr);
    }
    const ctx = this.ctx;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, w, h);
    const css = getComputedStyle(document.documentElement);
    const grid = css.getPropertyValue("--grid").trim();
    const muted = css.getPropertyValue("--muted").trim();

    const pad = { l: 44, r: 10, t: 10, b: 22 };
    const pw = w - pad.l - pad.r;
    const ph = h - pad.t - pad.b;
    const ys = this.stacked();
    const n = this.times.length;
    let max = this.opts.minMax ?? 1;
    for (const s of ys) for (const v of s) if (Number.isFinite(v)) max = Math.max(max, v);

    const log = !!this.opts.log;
    const lo = log ? Math.log10(0.1) : 0;
    const hi = log ? Math.log10(Math.max(max * 1.5, 10)) : max * 1.15;
    const y = (v: number) => {
      const vv = log ? Math.log10(Math.max(v, 0.1)) : v;
      return pad.t + ph - ((vv - lo) / (hi - lo)) * ph;
    };
    const x = (i: number) => pad.l + (n <= 1 ? pw : (i / (n - 1)) * pw);

    // Grid + y labels.
    ctx.font = "11px ui-monospace, Menlo, monospace";
    ctx.fillStyle = muted;
    ctx.strokeStyle = grid;
    ctx.lineWidth = 1;
    const ticks = log
      ? [0.1, 1, 10, 100, 1000, 10000].filter((t) => Math.log10(t) <= hi + 1e-9)
      : [0, 0.25, 0.5, 0.75, 1].map((f) => (f * hi) as number);
    for (const t of ticks) {
      const yy = Math.round(y(t)) + 0.5;
      ctx.beginPath();
      ctx.moveTo(pad.l, yy);
      ctx.lineTo(w - pad.r, yy);
      ctx.stroke();
      ctx.fillText(this.fmt(t) + (this.opts.unit ?? ""), 2, yy + 4);
    }
    if (n > 1) {
      const span = (this.times[n - 1] - this.times[0]) / 1000;
      ctx.fillText(span >= 120 ? `-${Math.round(span / 60)}m` : `-${Math.round(span)}s`, pad.l, h - 6);
      ctx.fillText("now", w - pad.r - 22, h - 6);
    }

    // Series (draw stacked ones back to front so fills layer nicely).
    for (let k = this.series.length - 1; k >= 0; k--) {
      const s = this.series[k];
      const vals = ys[k];
      if (vals.length < 2) continue;
      ctx.beginPath();
      vals.forEach((v, i) => (i ? ctx.lineTo(x(i), y(v)) : ctx.moveTo(x(i), y(v))));
      ctx.strokeStyle = s.color;
      ctx.lineWidth = 2;
      ctx.lineJoin = "round";
      ctx.stroke();
      if (s.fill) {
        ctx.lineTo(x(vals.length - 1), y(log ? 0.1 : 0));
        ctx.lineTo(x(0), y(log ? 0.1 : 0));
        ctx.closePath();
        const g = ctx.createLinearGradient(0, pad.t, 0, pad.t + ph);
        g.addColorStop(0, s.color + "66");
        g.addColorStop(1, s.color + "05");
        ctx.fillStyle = g;
        ctx.fill();
      }
    }

    // Hover crosshair.
    if (this.hoverX !== null && n > 1) {
      const i = Math.round(((this.hoverX - pad.l) / pw) * (n - 1));
      if (i >= 0 && i < n) {
        ctx.strokeStyle = muted;
        ctx.setLineDash([3, 3]);
        ctx.beginPath();
        ctx.moveTo(x(i) + 0.5, pad.t);
        ctx.lineTo(x(i) + 0.5, pad.t + ph);
        ctx.stroke();
        ctx.setLineDash([]);
        this.series.forEach((s, k) => {
          ctx.fillStyle = s.color;
          ctx.beginPath();
          ctx.arc(x(i), y(ys[k][i]), 3.5, 0, Math.PI * 2);
          ctx.fill();
        });
      }
    }
  }

  private showTip(e: MouseEvent): void {
    const n = this.times.length;
    const pw = this.canvas.clientWidth - 54;
    const i = Math.round(((e.offsetX - 44) / pw) * (n - 1));
    const el = tip();
    if (n < 2 || i < 0 || i >= n) {
      el.hidden = true;
      return;
    }
    const time = new Date(this.times[i]).toLocaleTimeString();
    el.innerHTML =
      `<div>${time}</div>` +
      this.series
        .map((s) => `<div><span style="color:${s.color}">■</span> ${s.name}: ${this.fmt(s.values[i] ?? 0)}${this.opts.unit ?? ""}</div>`)
        .join("");
    el.hidden = false;
    el.style.left = `${e.clientX + 14}px`;
    el.style.top = `${e.clientY + 10}px`;
  }
}
