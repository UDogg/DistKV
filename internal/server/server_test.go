package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/UDogg/DistKV/internal/api"
	"github.com/UDogg/DistKV/internal/cluster"
	"github.com/UDogg/DistKV/internal/server"
	"github.com/UDogg/DistKV/internal/testcluster"
	"github.com/UDogg/DistKV/pkg/client"
)

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

func TestCRUDThroughSmartClient(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 8})
	c := tc.Client()

	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("user:%d", i)
		if _, err := c.Put(ctx(t), k, []byte("v"+k)); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	shardsUsed := map[int]bool{}
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("user:%d", i)
		it, err := c.Get(ctx(t), k)
		if err != nil || string(it.Value) != "v"+k {
			t.Fatalf("get %s: %v %+v", k, err, it)
		}
		shardsUsed[it.Shard] = true
	}
	if len(shardsUsed) < 6 {
		t.Errorf("keys landed on only %d of 8 shards", len(shardsUsed))
	}
	if err := c.Delete(ctx(t), "user:1"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Get(ctx(t), "user:1"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("get after delete: %v", err)
	}
	if err := c.Delete(ctx(t), "user:1"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("double delete: %v", err)
	}
	// Keys with slashes and unicode survive routing.
	weird := "path/to/ключ?#%"
	if _, err := c.Put(ctx(t), weird, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if it, err := c.Get(ctx(t), weird); err != nil || string(it.Value) != "x" {
		t.Fatalf("weird key: %v", err)
	}
}

func TestCompareAndSwap(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 4})
	c := tc.Client()
	v1, err := c.CompareAndSwap(ctx(t), "lock", 0, []byte("owner-a"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.CompareAndSwap(ctx(t), "lock", 0, []byte("owner-b"))
	var ce *client.ConflictError
	if !errors.As(err, &ce) || ce.CurrentVersion != v1 {
		t.Fatalf("expected conflict at version %d, got %v", v1, err)
	}
	if _, err := c.CompareAndSwap(ctx(t), "lock", v1, []byte("owner-b")); err != nil {
		t.Fatal(err)
	}
}

// Concurrent increments via CAS loops must not lose updates.
func TestConcurrentCASCounter(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 4})
	c := tc.Client()
	const workers, incs = 8, 25
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < incs; i++ {
				for {
					var cur int
					var ver uint64
					it, err := c.Get(ctx(t), "counter")
					if err == nil {
						fmt.Sscanf(string(it.Value), "%d", &cur)
						ver = it.Version
					} else if !errors.Is(err, client.ErrNotFound) {
						t.Error(err)
						return
					}
					_, err = c.CompareAndSwap(ctx(t), "counter", ver, []byte(fmt.Sprint(cur+1)))
					if err == nil {
						break
					}
					if !errors.Is(err, client.ErrConflict) {
						t.Error(err)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	it, err := c.Get(ctx(t), "counter")
	if err != nil {
		t.Fatal(err)
	}
	if string(it.Value) != fmt.Sprint(workers*incs) {
		t.Fatalf("counter = %s, want %d (lost updates)", it.Value, workers*incs)
	}
}

func TestTTL(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 2})
	c := tc.Client()
	if _, err := c.Put(ctx(t), "session", []byte("tok"), client.WithTTL(300*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	it, err := c.Get(ctx(t), "session")
	if err != nil || it.ExpiresAt.IsZero() {
		t.Fatalf("get: %v %+v", err, it)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := c.Get(ctx(t), "session"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("expired key still readable: %v", err)
	}
}

// Plain HTTP clients (curl) work against any node: followers and nodes not
// hosting the shard transparently forward to the leader.
func TestServerSideForwarding(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 5, Shards: 8, ReplicationFactor: 3})
	for i, ep := range tc.Endpoints() {
		key := fmt.Sprintf("fwd-%d", i)
		req, _ := http.NewRequest(http.MethodPut, ep+"/v1/kv/"+key+"?ttl=1m", strings.NewReader("hello"))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var wr api.WriteResponse
		json.NewDecoder(resp.Body).Decode(&wr)
		resp.Body.Close()
		if resp.StatusCode != 200 || wr.Version == 0 {
			t.Fatalf("PUT via %s: %d %+v", ep, resp.StatusCode, wr)
		}
		// Read back through every node.
		for _, ep2 := range tc.Endpoints() {
			resp, err := http.Get(ep2 + "/v1/kv/" + key)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != 200 || string(body) != "hello" {
				t.Fatalf("GET via %s: %d %q", ep2, resp.StatusCode, body)
			}
			if resp.Header.Get(api.HeaderVersion) == "" {
				t.Fatalf("missing version header")
			}
		}
	}
}

func TestMisdirectedHint(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1})
	leader := tc.LeaderOf(0)
	var follower string
	for i, id := range tc.IDs() {
		if id != leader {
			follower = tc.Endpoints()[i]
			break
		}
	}
	req, _ := http.NewRequest(http.MethodGet, follower+"/v1/kv/x", nil)
	req.Header.Set(api.HeaderNoForward, "1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var e api.ErrorResponse
	json.NewDecoder(resp.Body).Decode(&e)
	if resp.StatusCode != http.StatusMisdirectedRequest || e.Leader != string(leader) || e.LeaderAddr == "" {
		t.Fatalf("got %d %+v, want 421 pointing at %s", resp.StatusCode, e, leader)
	}
}

func TestStaleReadsFromFollower(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 1})
	c := tc.Client()
	if _, err := c.Put(ctx(t), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	served := map[string]bool{}
	deadline := time.Now().Add(3 * time.Second)
	for len(served) < 3 && time.Now().Before(deadline) {
		it, err := c.Get(ctx(t), "k", client.Stale())
		if err != nil {
			if errors.Is(err, client.ErrNotFound) {
				continue // follower not caught up yet
			}
			t.Fatal(err)
		}
		served[it.ServedBy] = true
	}
	if len(served) < 2 {
		t.Fatalf("stale reads only served by %v", served)
	}
}

func TestValidation(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 1, Shards: 1})
	ep := tc.Endpoints()[0]
	cases := []struct {
		method, path string
		body         []byte
		want         int
	}{
		{"PUT", "/v1/kv/k?ttl=banana", nil, 400},
		{"PUT", "/v1/kv/k?ttl=-1s", nil, 400},
		{"PUT", "/v1/kv/k?if_version=x", nil, 400},
		{"PUT", "/v1/kv/" + strings.Repeat("k", 2000), nil, 400},
		{"PUT", "/v1/kv/big", bytes.Repeat([]byte("x"), 2<<20), 413},
		{"GET", "/v1/kv/missing", nil, 404},
	}
	for _, tcse := range cases {
		req, _ := http.NewRequest(tcse.method, ep+tcse.path, bytes.NewReader(tcse.body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != tcse.want {
			t.Errorf("%s %s: got %d, want %d", tcse.method, tcse.path[:min(len(tcse.path), 40)], resp.StatusCode, tcse.want)
		}
	}
}

func TestClusterStatusAndMetrics(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 6})
	c := tc.Client()
	c.Put(ctx(t), "a", []byte("1"))
	st, err := c.Cluster(ctx(t))
	if err != nil {
		t.Fatal(err)
	}
	if st.HealthyShards != 6 || len(st.Nodes) != 3 || len(st.Shards) != 6 {
		t.Fatalf("cluster status: %+v", st)
	}
	led := 0
	for _, n := range st.Nodes {
		if !n.Reachable {
			t.Fatalf("node %s unreachable", n.ID)
		}
		led += n.ShardsLed
	}
	if led != 6 {
		t.Fatalf("shards led = %d", led)
	}
	resp, err := http.Get(tc.Endpoints()[0] + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{"distkv_raft_term{shard=", "distkv_requests_total", "distkv_kv_keys", "distkv_build_info"} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("metrics missing %s", want)
		}
	}
	for _, path := range []string{"/healthz", "/readyz", "/v1/status"} {
		resp, err := http.Get(tc.Endpoints()[1] + path)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("%s: %v %v", path, err, resp)
		}
		resp.Body.Close()
	}
}

func TestLeaderFailover(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 4})
	c := tc.Client()
	for i := 0; i < 20; i++ {
		if _, err := c.Put(ctx(t), fmt.Sprintf("k%d", i), []byte("before")); err != nil {
			t.Fatal(err)
		}
	}
	victim := tc.LeaderOf(0)
	tc.Kill(victim)
	// All shards must recover and every acknowledged write must survive.
	for i := 0; i < 20; i++ {
		it, err := c.Get(ctx(t), fmt.Sprintf("k%d", i))
		if err != nil || string(it.Value) != "before" {
			t.Fatalf("k%d after failover: %v", i, err)
		}
	}
	for i := 0; i < 20; i++ {
		if _, err := c.Put(ctx(t), fmt.Sprintf("k%d", i), []byte("after")); err != nil {
			t.Fatalf("write after failover: %v", err)
		}
	}
	tc.Restart(victim)
	tc.WaitHealthy(5 * time.Second)
}

func TestGracefulShutdownHandsOffLeadership(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 6})
	var victim cluster.NodeID
	for s := 0; s < 6 && victim == ""; s++ {
		victim = tc.LeaderOf(s)
	}
	start := time.Now()
	tc.Shutdown(victim)
	// Leadership was transferred, so every shard has a leader almost
	// immediately rather than after an election timeout.
	tc.WaitHealthy(time.Second)
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("graceful shutdown took %v", d)
	}
}

func TestPersistentRestart(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 2, Persistent: true, SnapshotThreshold: 50})
	c := tc.Client()
	for i := 0; i < 300; i++ {
		if _, err := c.Put(ctx(t), fmt.Sprintf("k%d", i), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	// Full-cluster restart: everything must come back from disk.
	for _, id := range tc.IDs() {
		tc.Kill(id)
	}
	for _, id := range tc.IDs() {
		tc.Restart(id)
	}
	tc.WaitHealthy(10 * time.Second)
	for i := 0; i < 300; i += 7 {
		it, err := c.Get(ctx(t), fmt.Sprintf("k%d", i))
		if err != nil || string(it.Value) != fmt.Sprint(i) {
			t.Fatalf("k%d after restart: %v", i, err)
		}
	}
}

func TestRefusesMismatchedDataDir(t *testing.T) {
	dir := t.TempDir()
	spec := cluster.Spec{Members: []cluster.Member{{ID: "n1", Addr: "http://127.0.0.1:1"}}, Shards: 4}
	s, err := server.New(server.Config{NodeID: "n1", Spec: spec, DataDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	s.Kill()
	spec.Shards = 8
	if _, err := server.New(server.Config{NodeID: "n1", Spec: spec, DataDir: dir}); err == nil {
		t.Fatal("expected refusal to start with a different shard count")
	}
}

func TestLeaderRebalancing(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 9, RebalanceInterval: 100 * time.Millisecond})
	topo := tc.Server(tc.IDs()[0]).Topology()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		balanced := true
		for s := 0; s < 9; s++ {
			if tc.LeaderOf(s) != topo.Replicas(cluster.ShardID(s))[0] {
				balanced = false
			}
		}
		if balanced {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("leaders never converged to preferred replicas")
}
