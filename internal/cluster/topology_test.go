package cluster

import (
	"fmt"
	"math"
	"testing"
)

func members(n int) []Member {
	var ms []Member
	for i := 1; i <= n; i++ {
		ms = append(ms, Member{ID: NodeID(fmt.Sprintf("n%d", i)), Addr: fmt.Sprintf("http://127.0.0.1:%d", 7000+i)})
	}
	return ms
}

func TestValidation(t *testing.T) {
	if _, err := New(Spec{}); err == nil {
		t.Fatal("expected error for empty spec")
	}
	if _, err := New(Spec{Members: members(2), Shards: 4, ReplicationFactor: 3}); err == nil {
		t.Fatal("expected error for rf > members")
	}
	dup := append(members(2), Member{ID: "n1", Addr: "http://x"})
	if _, err := New(Spec{Members: dup, Shards: 4}); err == nil {
		t.Fatal("expected error for duplicate member")
	}
}

func TestDeterministicAcrossMemberOrder(t *testing.T) {
	a, _ := New(Spec{Members: members(5), Shards: 16, ReplicationFactor: 3})
	ms := members(5)
	ms[0], ms[4] = ms[4], ms[0]
	b, _ := New(Spec{Members: ms, Shards: 16, ReplicationFactor: 3})
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatalf("fingerprints differ")
	}
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("key-%d", i)
		if a.ShardForKey(k) != b.ShardForKey(k) {
			t.Fatalf("routing differs for %s", k)
		}
	}
	for s := 0; s < 16; s++ {
		ra, rb := a.Replicas(ShardID(s)), b.Replicas(ShardID(s))
		for i := range ra {
			if ra[i] != rb[i] {
				t.Fatalf("placement differs for shard %d", s)
			}
		}
	}
}

func TestReplicasDistinctAndBalanced(t *testing.T) {
	topo, err := New(Spec{Members: members(5), Shards: 64, ReplicationFactor: 3})
	if err != nil {
		t.Fatal(err)
	}
	load := map[NodeID]int{}
	preferred := map[NodeID]int{}
	for s := 0; s < 64; s++ {
		reps := topo.Replicas(ShardID(s))
		if len(reps) != 3 {
			t.Fatalf("shard %d has %d replicas", s, len(reps))
		}
		seen := map[NodeID]bool{}
		for _, r := range reps {
			if seen[r] {
				t.Fatalf("shard %d has duplicate replica %s", s, r)
			}
			seen[r] = true
			load[r]++
		}
		preferred[reps[0]]++
	}
	// 64*3/5 = 38.4 replicas per node on average.
	for id, l := range load {
		if l < 25 || l > 52 {
			t.Errorf("node %s hosts %d replicas; poorly balanced: %v", id, l, load)
		}
	}
	for id, p := range preferred {
		if p < 5 || p > 22 {
			t.Errorf("node %s is preferred leader for %d shards: %v", id, p, preferred)
		}
	}
	total := 0
	for _, id := range []NodeID{"n1", "n2", "n3", "n4", "n5"} {
		total += len(topo.ShardsOn(id))
	}
	if total != 64*3 {
		t.Fatalf("ShardsOn total = %d", total)
	}
}

// Preferred leaders must be spread nearly evenly even for small shard
// counts (exact balance is not always possible given the replica sets).
func TestPreferredLeadersBalanced(t *testing.T) {
	for _, c := range []struct{ nodes, shards int }{{5, 24}, {3, 8}, {7, 16}, {4, 6}, {9, 64}} {
		topo, _ := New(Spec{Members: members(c.nodes), Shards: c.shards})
		pref := map[NodeID]int{}
		for s := 0; s < c.shards; s++ {
			pref[topo.Replicas(ShardID(s))[0]]++
		}
		lo, hi := c.shards, 0
		for _, m := range topo.Members() {
			lo, hi = min(lo, pref[m.ID]), max(hi, pref[m.ID])
		}
		if lo == 0 || hi-lo > 2 {
			t.Errorf("%d nodes/%d shards: preferred leaders unbalanced: %v", c.nodes, c.shards, pref)
		}
	}
}

func TestKeyDistribution(t *testing.T) {
	const shards, keys = 16, 160_000
	topo, _ := New(Spec{Members: members(3), Shards: shards})
	counts := make([]int, shards)
	for i := 0; i < keys; i++ {
		counts[topo.ShardForKey(fmt.Sprintf("user:%d", i))]++
	}
	mean := float64(keys) / shards
	var sq float64
	for _, c := range counts {
		sq += (float64(c) - mean) * (float64(c) - mean)
	}
	cv := math.Sqrt(sq/shards) / mean
	if cv > 0.15 {
		t.Fatalf("key distribution too skewed: cv=%.3f counts=%v", cv, counts)
	}
}

// Adding a shard should move roughly 1/(N+1) of keys, not reshuffle all.
func TestConsistentHashingMinimalMovement(t *testing.T) {
	a, _ := New(Spec{Members: members(3), Shards: 16})
	b, _ := New(Spec{Members: members(3), Shards: 17})
	moved := 0
	const keys = 50_000
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("k%d", i)
		if a.ShardForKey(k) != b.ShardForKey(k) {
			moved++
		}
	}
	frac := float64(moved) / keys
	if frac > 0.12 {
		t.Fatalf("%.1f%% of keys moved when adding one shard (expected ~6%%)", frac*100)
	}
}

func TestParseMembers(t *testing.T) {
	ms, err := ParseMembers("n1=127.0.0.1:7001, n2=http://h2:7002/")
	if err != nil {
		t.Fatal(err)
	}
	if ms[0].Addr != "http://127.0.0.1:7001" || ms[1].Addr != "http://h2:7002" {
		t.Fatalf("parsed %+v", ms)
	}
	if _, err := ParseMembers("bad"); err == nil {
		t.Fatal("expected error")
	}
}

func BenchmarkShardForKey(b *testing.B) {
	topo, _ := New(Spec{Members: members(5), Shards: 64})
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		topo.ShardForKey("some-moderately-long-key-name")
	}
}
