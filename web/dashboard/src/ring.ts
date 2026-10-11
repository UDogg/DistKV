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
  private readonly arcsByShard: SVGPathElement[][] = [];
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

    for (let s = 0; s < topo.shards; s++) this.arcsByShard.push([]);
    const ring = topo.ring;
    for (let i = 0; i < ring.length; i++) {
      const prev = ring[(i - 1 + ring.length) % ring.length];
      let a0 = angle(prev.hash);
      const a1 = angle(ring[i].hash);
      if (i === 0) a0 -= Math.PI * 2; // wrap-around arc
      const [x0, y0] = polar(a0);
      const [x1, y1] = polar(a1);
      const large = a1 - a0 > Math.PI ? 1 : 0;
      const p = document.createElementNS(SVG, "path");
      p.setAttribute("d", `M${x0.toFixed(2)},${y0.toFixed(2)} A${R},${R} 0 ${large} 1 ${x1.toFixed(2)},${y1.toFixed(2)}`);
      p.setAttribute("fill", "none");
      p.setAttribute("stroke-width", "22");
      p.setAttribute("stroke", "#444");
      const shard = ring[i].shard;
      p.addEventListener("mouseenter", () => onHover(shard));
      p.addEventListener("mouseleave", () => onHover(null));
      const t = document.createElementNS(SVG, "title");
      t.textContent = `shard ${shard}`;
      p.appendChild(t);
      this.arcsByShard[shard].push(p);
      svg.appendChild(p);
    }

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
    this.title.textContent = title;
    this.sub.textContent = sub;
  }

  /** Colour each shard's arcs by its leader's colour (grey = no leader). */
  update(leaders: (string | undefined)[], color: (node: string) => string): void {
    leaders.forEach((leader, shard) => {
      const c = leader ? color(leader) : "#ff5c7a";
      for (const p of this.arcsByShard[shard] ?? []) {
        if (p.getAttribute("stroke") !== c) p.setAttribute("stroke", c);
        p.setAttribute("stroke-dasharray", leader ? "" : "2 3");
      }
    });
  }

  /** Highlight one shard (or clear with null). */
  highlight(shard: number | null): void {
    if (shard === this.highlighted) return;
    this.highlighted = shard;
    this.arcsByShard.forEach((arcs, s) => arcs.forEach((p) => p.classList.toggle("dim", shard !== null && s !== shard)));
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
