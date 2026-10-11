package cluster

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite testdata/routing/vectors.tsv")

const vectorsPath = "../../testdata/routing/vectors.tsv"

// vectorKeys exercise ASCII, separators, unicode and long keys.
func vectorKeys() []string {
	keys := []string{"", "a", "hello", "user:42", "user:42/email", "path/to/ключ", "日本語キー", "emoji-🚀", "with space", "%2F"}
	for i := 0; i < 200; i++ {
		keys = append(keys, fmt.Sprintf("key-%d", i))
	}
	keys = append(keys, strings.Repeat("x", 1000))
	return keys
}

func vectorTopology(t *testing.T) *Topology {
	t.Helper()
	var ms []Member
	for i := 1; i <= 5; i++ {
		ms = append(ms, Member{ID: NodeID(fmt.Sprintf("n%d", i)), Addr: fmt.Sprintf("http://127.0.0.1:%d", 7100+i)})
	}
	topo, err := New(Spec{Members: ms, Shards: 24, ReplicationFactor: 3, VirtualNodes: 128})
	if err != nil {
		t.Fatal(err)
	}
	return topo
}

// TestRoutingVectors pins the routing functions with golden vectors that
// every client SDK (Python, TypeScript, Rust) checks against, so all
// languages route each key to the same shard and replicas as the servers.
func TestRoutingVectors(t *testing.T) {
	topo := vectorTopology(t)
	var b bytes.Buffer
	b.WriteString("# DistKV routing conformance vectors (tab-separated).\n")
	b.WriteString("# Regenerate: go test ./internal/cluster -run TestRoutingVectors -update\n")
	b.WriteString("spec\tshards=24\treplication=3\tvnodes=128\tmembers=n1,n2,n3,n4,n5\n")
	for _, k := range vectorKeys() {
		fmt.Fprintf(&b, "hash\t%s\t%d\n", k, hash64(k))
	}
	for _, k := range vectorKeys() {
		fmt.Fprintf(&b, "shard\t%s\t%d\n", k, topo.ShardForKey(k))
	}
	for s := 0; s < 24; s++ {
		var ids []string
		for _, r := range topo.Replicas(ShardID(s)) {
			ids = append(ids, string(r))
		}
		fmt.Fprintf(&b, "replicas\t%d\t%s\n", s, strings.Join(ids, ","))
	}
	fmt.Fprintf(&b, "fingerprint\t%s\n", topo.Fingerprint())

	if *update {
		if err := os.WriteFile(vectorsPath, b.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(vectorsPath)
	if err != nil {
		t.Fatalf("%v (run with -update to create)", err)
	}
	if !bytes.Equal(want, b.Bytes()) {
		t.Fatalf("routing changed: %s is out of date. Changing routing breaks every existing cluster and client; "+
			"if intended, rerun with -update", vectorsPath)
	}
}
