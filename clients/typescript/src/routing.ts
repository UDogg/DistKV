/**
 * Key -> shard and shard -> replica placement, bit-for-bit identical to the
 * Go implementation (internal/cluster). 64-bit arithmetic uses BigInt.
 * Verified against testdata/routing/vectors.tsv.
 */

const MASK64 = (1n << 64n) - 1n;
const FNV_OFFSET = 14695981039346656037n;
const FNV_PRIME = 1099511628211n;
const encoder = new TextEncoder();

export const DEFAULT_VIRTUAL_NODES = 128;

/** FNV-1a (64-bit) followed by the SplitMix64 finaliser. */
export function hash64(s: string): bigint {
  let h = FNV_OFFSET;
  for (const b of encoder.encode(s)) {
    h ^= BigInt(b);
    h = (h * FNV_PRIME) & MASK64;
  }
  h ^= h >> 30n;
  h = (h * 0xbf58476d1ce4e5b9n) & MASK64;
  h ^= h >> 27n;
  h = (h * 0x94d049bb133111ebn) & MASK64;
  h ^= h >> 31n;
  return h;
}

export interface Member {
  id: string;
  addr: string;
}

export interface Spec {
  members: Member[];
  shards: number;
  replication_factor?: number;
  virtual_nodes?: number;
}

/** A point on the consistent-hash ring. */
export interface RingPoint {
  hash: bigint;
  shard: number;
}

export class Topology {
  readonly members: Member[];
  readonly shards: number;
  readonly replicationFactor: number;
  readonly virtualNodes: number;
  readonly ring: RingPoint[];
  private readonly replicaTable: string[][];
  private readonly byId = new Map<string, Member>();

  constructor(spec: Spec) {
    if (spec.shards <= 0) throw new Error("shards must be positive");
    this.members = [...spec.members].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    this.shards = spec.shards;
    this.virtualNodes = spec.virtual_nodes && spec.virtual_nodes > 0 ? spec.virtual_nodes : DEFAULT_VIRTUAL_NODES;
    this.replicationFactor =
      spec.replication_factor && spec.replication_factor > 0 ? spec.replication_factor : Math.min(3, this.members.length);
    if (this.replicationFactor > this.members.length) throw new Error("replication factor exceeds member count");
    for (const m of this.members) this.byId.set(m.id, m);

    const ring: RingPoint[] = [];
    for (let s = 0; s < this.shards; s++) {
      for (let v = 0; v < this.virtualNodes; v++) ring.push({ hash: hash64(`shard-${s}-vnode-${v}`), shard: s });
    }
    ring.sort((a, b) => (a.hash < b.hash ? -1 : a.hash > b.hash ? 1 : a.shard - b.shard));
    this.ring = ring;
    this.replicaTable = [];
    for (let s = 0; s < this.shards; s++) this.replicaTable.push(this.rendezvous(s));
    this.balancePreferred();
  }

  /** Spread preferred leaders evenly (see Go Topology.balancePreferred). */
  private balancePreferred(): void {
    const count = new Map(this.members.map((m) => [m.id, 0] as [string, number]));
    this.replicaTable.forEach((reps, s) => {
      let best = 0;
      for (let i = 1; i < reps.length; i++) if (count.get(reps[i])! < count.get(reps[best])!) best = i;
      if (best) this.replicaTable[s] = [reps[best], ...reps.slice(0, best), ...reps.slice(best + 1)];
      const pref = this.replicaTable[s][0];
      count.set(pref, count.get(pref)! + 1);
    });
  }

  private rendezvous(shard: number): string[] {
    return this.members
      .map((m) => ({ id: m.id, score: hash64(`${shard}/${m.id}`) }))
      .sort((a, b) => (a.score > b.score ? -1 : a.score < b.score ? 1 : a.id < b.id ? -1 : 1))
      .slice(0, this.replicationFactor)
      .map((x) => x.id);
  }

  /** Index of the ring point owning hash h (binary search). */
  ringIndex(h: bigint): number {
    let lo = 0;
    let hi = this.ring.length;
    while (lo < hi) {
      const mid = (lo + hi) >>> 1;
      if (this.ring[mid].hash < h) lo = mid + 1;
      else hi = mid;
    }
    return lo === this.ring.length ? 0 : lo;
  }

  shardForKey(key: string): number {
    return this.ring[this.ringIndex(hash64(key))].shard;
  }

  replicas(shard: number): string[] {
    return this.replicaTable[shard];
  }

  member(id: string): Member | undefined {
    return this.byId.get(id);
  }
}
