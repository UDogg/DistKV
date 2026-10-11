//! Key -> shard and shard -> replica placement, bit-for-bit identical to the
//! Go implementation (`internal/cluster`). Verified against
//! `testdata/routing/vectors.tsv`.

use std::collections::HashMap;

pub const DEFAULT_VIRTUAL_NODES: usize = 128;

/// FNV-1a (64-bit) followed by the SplitMix64 finaliser.
pub fn hash64(s: &str) -> u64 {
    let mut h: u64 = 14695981039346656037;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(1099511628211);
    }
    h ^= h >> 30;
    h = h.wrapping_mul(0xbf58476d1ce4e5b9);
    h ^= h >> 27;
    h = h.wrapping_mul(0x94d049bb133111eb);
    h ^= h >> 31;
    h
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub id: String,
    pub addr: String,
}

#[derive(Debug, Clone)]
pub struct Topology {
    pub members: Vec<Member>,
    pub shards: usize,
    pub replication_factor: usize,
    pub virtual_nodes: usize,
    ring: Vec<(u64, usize)>,
    replicas: Vec<Vec<String>>,
}

impl Topology {
    pub fn new(mut members: Vec<Member>, shards: usize, replication_factor: usize, virtual_nodes: usize) -> Result<Self, String> {
        if shards == 0 {
            return Err("shards must be positive".into());
        }
        members.sort_by(|a, b| a.id.cmp(&b.id));
        let rf = if replication_factor == 0 { members.len().min(3) } else { replication_factor };
        if rf > members.len() {
            return Err("replication factor exceeds member count".into());
        }
        let vn = if virtual_nodes == 0 { DEFAULT_VIRTUAL_NODES } else { virtual_nodes };
        let mut ring = Vec::with_capacity(shards * vn);
        for s in 0..shards {
            for v in 0..vn {
                ring.push((hash64(&format!("shard-{s}-vnode-{v}")), s));
            }
        }
        ring.sort();
        let mut t = Topology { members, shards, replication_factor: rf, virtual_nodes: vn, ring, replicas: Vec::new() };
        t.replicas = (0..shards).map(|s| t.rendezvous(s)).collect();
        t.balance_preferred();
        Ok(t)
    }

    fn rendezvous(&self, shard: usize) -> Vec<String> {
        let mut scored: Vec<(u64, &str)> = self.members.iter().map(|m| (hash64(&format!("{shard}/{}", m.id)), m.id.as_str())).collect();
        scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(b.1)));
        scored.into_iter().take(self.replication_factor).map(|(_, id)| id.to_string()).collect()
    }

    /// Spread preferred leaders evenly (see Go `Topology.balancePreferred`).
    fn balance_preferred(&mut self) {
        let mut count: HashMap<String, usize> = self.members.iter().map(|m| (m.id.clone(), 0)).collect();
        for reps in self.replicas.iter_mut() {
            let mut best = 0;
            for i in 1..reps.len() {
                if count[&reps[i]] < count[&reps[best]] {
                    best = i;
                }
            }
            if best != 0 {
                let pref = reps.remove(best);
                reps.insert(0, pref);
            }
            *count.get_mut(&reps[0]).unwrap() += 1;
        }
    }

    pub fn shard_for_key(&self, key: &str) -> usize {
        let h = hash64(key);
        let i = self.ring.partition_point(|p| p.0 < h);
        self.ring[if i == self.ring.len() { 0 } else { i }].1
    }

    pub fn replicas(&self, shard: usize) -> &[String] {
        &self.replicas[shard]
    }

    pub fn member(&self, id: &str) -> Option<&Member> {
        self.members.iter().find(|m| m.id == id)
    }
}
