// Package cluster describes how DistKV partitions the keyspace.
//
// Two independent, deterministic mappings are used:
//
//  1. key -> shard: a consistent-hash ring with virtual nodes. Each shard
//     owns many small arcs of a 64-bit ring, which keeps shard sizes within
//     a few percent of each other and means that adding shards in the
//     future moves only ~1/N of the keys.
//
//  2. shard -> replicas: rendezvous (highest-random-weight) hashing picks
//     ReplicationFactor distinct nodes for every shard. Each shard is an
//     independent Raft group ("multi-Raft"), so load and leadership spread
//     across the whole cluster rather than funnelling through one leader.
//
// Every node and smart client computes the same topology from the same
// inputs, so no coordination service is required to route requests.
package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// ShardID identifies a shard (a Raft group).
type ShardID uint32

// NodeID identifies a server.
type NodeID string

// Member is a server in the cluster.
type Member struct {
	ID   NodeID `json:"id"`
	Addr string `json:"addr"` // base URL, e.g. http://10.0.0.1:7001
}

// Spec is the static cluster definition. It must be identical on every
// node.
type Spec struct {
	Members           []Member `json:"members"`
	Shards            int      `json:"shards"`
	ReplicationFactor int      `json:"replication_factor"`
	VirtualNodes      int      `json:"virtual_nodes"`
}

// Topology is a resolved Spec.
type Topology struct {
	spec     Spec
	byID     map[NodeID]Member
	ring     []ringPoint
	replicas [][]NodeID // replicas[shard], in preference order
}

type ringPoint struct {
	hash  uint64
	shard ShardID
}

// DefaultVirtualNodes is the number of ring points per shard.
const DefaultVirtualNodes = 128

// New validates spec and builds the topology.
func New(spec Spec) (*Topology, error) {
	if len(spec.Members) == 0 {
		return nil, errors.New("cluster: no members")
	}
	if spec.Shards <= 0 {
		return nil, errors.New("cluster: shards must be positive")
	}
	if spec.ReplicationFactor <= 0 {
		spec.ReplicationFactor = min(3, len(spec.Members))
	}
	if spec.ReplicationFactor > len(spec.Members) {
		return nil, fmt.Errorf("cluster: replication factor %d exceeds %d members", spec.ReplicationFactor, len(spec.Members))
	}
	if spec.VirtualNodes <= 0 {
		spec.VirtualNodes = DefaultVirtualNodes
	}
	members := append([]Member(nil), spec.Members...)
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })
	spec.Members = members

	t := &Topology{spec: spec, byID: make(map[NodeID]Member, len(members))}
	for _, m := range members {
		if m.ID == "" {
			return nil, errors.New("cluster: member with empty id")
		}
		if _, dup := t.byID[m.ID]; dup {
			return nil, fmt.Errorf("cluster: duplicate member %q", m.ID)
		}
		if _, err := url.Parse(m.Addr); err != nil || m.Addr == "" {
			return nil, fmt.Errorf("cluster: member %q has invalid address %q", m.ID, m.Addr)
		}
		t.byID[m.ID] = m
	}

	t.ring = make([]ringPoint, 0, spec.Shards*spec.VirtualNodes)
	for s := 0; s < spec.Shards; s++ {
		for v := 0; v < spec.VirtualNodes; v++ {
			t.ring = append(t.ring, ringPoint{hash: hash64(fmt.Sprintf("shard-%d-vnode-%d", s, v)), shard: ShardID(s)})
		}
	}
	sort.Slice(t.ring, func(i, j int) bool {
		if t.ring[i].hash != t.ring[j].hash {
			return t.ring[i].hash < t.ring[j].hash
		}
		return t.ring[i].shard < t.ring[j].shard
	})

	t.replicas = make([][]NodeID, spec.Shards)
	for s := 0; s < spec.Shards; s++ {
		t.replicas[s] = t.rendezvous(ShardID(s), spec.ReplicationFactor)
	}
	return t, nil
}

// rendezvous ranks members by hash(shard, member) and returns the top n.
func (t *Topology) rendezvous(shard ShardID, n int) []NodeID {
	type scored struct {
		id    NodeID
		score uint64
	}
	scores := make([]scored, 0, len(t.spec.Members))
	for _, m := range t.spec.Members {
		scores = append(scores, scored{m.ID, hash64(fmt.Sprintf("%d/%s", shard, m.ID))})
	}
	sort.Slice(scores, func(i, j int) bool {
		if scores[i].score != scores[j].score {
			return scores[i].score > scores[j].score
		}
		return scores[i].id < scores[j].id
	})
	out := make([]NodeID, n)
	for i := range out {
		out[i] = scores[i].id
	}
	return out
}

// Spec returns the normalised specification.
func (t *Topology) Spec() Spec { return t.spec }

// NumShards returns the number of shards.
func (t *Topology) NumShards() int { return t.spec.Shards }

// Members returns all members sorted by ID.
func (t *Topology) Members() []Member { return t.spec.Members }

// Member looks up a member by ID.
func (t *Topology) Member(id NodeID) (Member, bool) {
	m, ok := t.byID[id]
	return m, ok
}

// ShardForKey maps a key to its shard.
func (t *Topology) ShardForKey(key string) ShardID {
	h := hash64(key)
	i := sort.Search(len(t.ring), func(i int) bool { return t.ring[i].hash >= h })
	if i == len(t.ring) {
		i = 0
	}
	return t.ring[i].shard
}

// Replicas returns the nodes hosting shard, in preference order. The first
// replica is the shard's preferred leader.
func (t *Topology) Replicas(shard ShardID) []NodeID {
	return t.replicas[shard]
}

// ShardsOn returns the shards that have a replica on node.
func (t *Topology) ShardsOn(node NodeID) []ShardID {
	var out []ShardID
	for s, reps := range t.replicas {
		for _, r := range reps {
			if r == node {
				out = append(out, ShardID(s))
				break
			}
		}
	}
	return out
}

// Fingerprint is a stable digest of everything that determines data
// placement. Nodes persist it to refuse starting with an incompatible
// configuration (which would silently corrupt routing).
func (t *Topology) Fingerprint() string {
	var b strings.Builder
	fmt.Fprintf(&b, "shards=%d;rf=%d;vnodes=%d;", t.spec.Shards, t.spec.ReplicationFactor, t.spec.VirtualNodes)
	for _, m := range t.spec.Members {
		fmt.Fprintf(&b, "%s;", m.ID)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:8])
}

// hash64 is a 64-bit FNV-1a followed by a SplitMix64 finaliser, which
// gives a well-distributed, platform-independent hash with no dependencies.
func hash64(s string) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

// ParseMembers parses "id=addr,id=addr" into members.
func ParseMembers(s string) ([]Member, error) {
	var out []Member
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("cluster: bad member %q (want id=url)", part)
		}
		if !strings.Contains(addr, "://") {
			addr = "http://" + addr
		}
		out = append(out, Member{ID: NodeID(id), Addr: strings.TrimRight(addr, "/")})
	}
	if len(out) == 0 {
		return nil, errors.New("cluster: empty member list")
	}
	return out, nil
}
