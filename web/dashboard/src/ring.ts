import { Topology, hash64 } from "../../../clients/typescript/src/routing";

const SVG = "http://www.w3.org/2000/svg";
const TWO64 = 2 ** 64;
const R = 128;

function angle(h: bigint): number {
  return (Number(h) / TWO64) * Math.PI * 2 - Math.PI / 2;
}

function polar(a: number, r = R): [number, number] {
  return [Math.cos(a) * r, Math.sin(a) * r];
}

/**
 * Renders the consistent-hash ring: every arc between two virtual-node
 * points belongs to the shard of the point that ends it, and is coloured by
 * the node currently leading that shard.
 */
export class RingView {
  // One path per shard (its arcs as subpaths): 24 elements instead of one
  // per virtual node (3,072 by default), so recolouring is cheap.
  private readonly paths: SVGPathElement[] = [];
  private readonly colors: string[] = [];
  private readonly marker: SVGLineElement;
  private readonly title: SVGTextElement;
  private readonly sub: SVGTextElement;
  private highlighted: number | null = null;

  constructor(svg: SVGSVGElement, readonly topo: Topology, onHover: (shard: number | null) => void) {
    svg.innerHTML = "";
    const bg = document.createElementNS(SVG, "circle");
    bg.setAttribute("r", String(R));
    bg.setAttribute("fill", "none");
    bg.setAttribute("stroke", "rgba(140,150,255,.08)");
    bg.setAttribute("stroke-width", "26");
    svg.appendChild(bg);

    const d: string[][] = Array.from({ length: topo.shards }, () => []);
    const ring = topo.ring;
    for (let i = 0; i < ring.length; i++) {
      const prev = ring[(i - 1 + ring.length) % ring.length];
      let a0 = angle(prev.hash);
      const a1 = angle(ring[i].hash);
      if (i === 0) a0 -= Math.PI * 2; // wrap-around arc
      const [x0, y0] = polar(a0);
      const [x1, y1] = polar(a1);
      const large = a1 - a0 > Math.PI ? 1 : 0;
      d[ring[i].shard].push(`M${x0.toFixed(2)},${y0.toFixed(2)}A${R},${R} 0 ${large} 1 ${x1.toFixed(2)},${y1.toFixed(2)}`);
    }
    for (let shard = 0; shard < topo.shards; shard++) {
      const p = document.createElementNS(SVG, "path");
      p.setAttribute("d", d[shard].join(""));
      p.setAttribute("fill", "none");
      p.setAttribute("stroke-width", "22");
      p.setAttribute("stroke", "#444");
      p.dataset.shard = String(shard);
      const t = document.createElementNS(SVG, "title");
      t.textContent = `shard ${shard}`;
      p.appendChild(t);
      this.paths.push(p);
      this.colors.push("");
      svg.appendChild(p);
    }
    // One delegated listener pair for the whole ring.
    svg.addEventListener("pointerover", (e) => {
      const s = (e.target as Element).closest?.("path[data-shard]") as SVGPathElement | null;
      if (s) onHover(Number(s.dataset.shard));
    });
    svg.addEventListener("pointerout", (e) => {
      const to = (e.relatedTarget as Element | null)?.closest?.("path[data-shard]");
      if (!to) onHover(null);
    });

    this.marker = document.createElementNS(SVG, "line");
    this.marker.classList.add("marker");
    this.marker.setAttribute("visibility", "hidden");
    svg.appendChild(this.marker);

    this.title = document.createElementNS(SVG, "text");
    this.title.classList.add("center-title");
    this.title.setAttribute("text-anchor", "middle");
    this.title.setAttribute("y", "0");
    svg.appendChild(this.title);
    this.sub = document.createElementNS(SVG, "text");
    this.sub.classList.add("center-sub");
    this.sub.setAttribute("text-anchor", "middle");
    this.sub.setAttribute("y", "16");
    svg.appendChild(this.sub);
    this.setCenter(`${topo.shards} shards`, `${topo.ring.length} VNODES · 2⁶⁴ RING`);
  }

  setCenter(title: string, sub: string): void {
    if (this.title.textContent !== title) this.title.textContent = title;
    if (this.sub.textContent !== sub) this.sub.textContent = sub;
  }

  /** Colour each shard's arcs by its leader's colour (dashed red = no leader). */
  update(leaders: (string | undefined)[], color: (node: string) => string): void {
    leaders.forEach((leader, shard) => {
      const p = this.paths[shard];
      const c = leader ? color(leader) : "#ff5c7a";
      if (!p || this.colors[shard] === c) return;
      this.colors[shard] = c;
      p.setAttribute("stroke", c);
      if (leader) p.removeAttribute("stroke-dasharray");
      else p.setAttribute("stroke-dasharray", "2 3");
    });
  }

  /** Highlight one shard (or clear with null). */
  highlight(shard: number | null): void {
    if (shard === this.highlighted) return;
    this.highlighted = shard;
    this.paths.forEach((p, s) => p.classList.toggle("dim", shard !== null && s !== shard));
  }

  /** Point at the ring position of a key. Returns its shard. */
  locate(key: string | null): number | null {
    if (key === null) {
      this.marker.setAttribute("visibility", "hidden");
      this.highlight(null);
      return null;
    }
    const h = hash64(key);
    const a = angle(h);
    const [x0, y0] = polar(a, R - 22);
    const [x1, y1] = polar(a, R + 22);
    this.marker.setAttribute("x1", String(x0));
    this.marker.setAttribute("y1", String(y0));
    this.marker.setAttribute("x2", String(x1));
    this.marker.setAttribute("y2", String(y1));
    this.marker.setAttribute("visibility", "visible");
    const shard = this.topo.shardForKey(key);
    this.highlight(shard);
    return shard;
  }
}
