package server_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/UDogg/DistKV/internal/api"
	"github.com/UDogg/DistKV/internal/testcluster"
)

func TestEventStreamAndLiveLoad(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 4})
	c := tc.Client()
	stop := make(chan struct{})
	go func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			c.Put(ctx(t), fmt.Sprintf("k%d", i%50), []byte("v"))
			c.Get(ctx(t), fmt.Sprintf("k%d", i%50))
		}
	}()
	defer close(stop)

	resp, err := http.Get(tc.Endpoints()[0] + "/v1/events?interval=300ms")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	deadline := time.Now().Add(8 * time.Second)
	events := 0
	for sc.Scan() && time.Now().Before(deadline) {
		line := sc.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var st api.ClusterStatus
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &st); err != nil {
			t.Fatal(err)
		}
		events++
		if st.Timestamp == 0 || st.ObservedBy == "" || len(st.Nodes) != 3 {
			t.Fatalf("incomplete snapshot: %+v", st)
		}
		if st.Load.ReadsPerSec > 0 && st.Load.WritesPerSec > 0 && st.Load.P99Ms > 0 {
			if events < 2 {
				continue
			}
			for _, sh := range st.Shards {
				if sh.Leader != "" && len(sh.Lag) != 2 {
					t.Fatalf("shard %d: expected lag for 2 followers, got %v", sh.Shard, sh.Lag)
				}
			}
			return // saw live traffic flowing through the stream
		}
	}
	t.Fatalf("no live load observed after %d events", events)
}

func TestFaultInjectionIsolatesNode(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 3, Shards: 6, ElectionTimeout: 200 * time.Millisecond})
	c := tc.Client()
	victimIdx := 0
	victim := tc.IDs()[victimIdx]
	ep := tc.Endpoints()[victimIdx]

	post := func(f api.Faults) {
		body, _ := json.Marshal(f)
		resp, err := http.Post(ep+"/v1/admin/faults", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("faults: %d", resp.StatusCode)
		}
	}
	post(api.Faults{Isolated: true})

	// Every shard must end up led by one of the two connected nodes.
	deadline := time.Now().Add(10 * time.Second)
	for {
		moved := true
		for s := 0; s < 6; s++ {
			if l := tc.LeaderOf(s); l == "" || l == victim {
				moved = false
			}
		}
		if moved {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("isolated node kept leadership")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The majority keeps serving writes.
	for i := 0; i < 20; i++ {
		if _, err := c.Put(ctx(t), fmt.Sprintf("p%d", i), []byte("x")); err != nil {
			t.Fatalf("write during partition: %v", err)
		}
	}
	post(api.Faults{})
	tc.WaitHealthy(10 * time.Second)

	// Bad input is rejected.
	resp, _ := http.Post(ep+"/v1/admin/faults", "application/json", strings.NewReader(`{"drop_percent":500}`))
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid fault accepted: %d", resp.StatusCode)
	}
}

func TestDashboardServed(t *testing.T) {
	tc := testcluster.Start(t, testcluster.Options{Nodes: 1, Shards: 1})
	resp, err := http.Get(tc.Endpoints()[0] + "/")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || resp.Request.URL.Path != "/ui/" {
		t.Fatalf("GET / -> %d %s", resp.StatusCode, resp.Request.URL.Path)
	}
}
