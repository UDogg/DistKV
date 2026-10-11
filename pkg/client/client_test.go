package client

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/UDogg/DistKV/internal/api"
)

func TestSessionTrackerAckWatermark(t *testing.T) {
	s := sessionTracker{next: 1, inflight: map[uint64]struct{}{}}
	if got := s.ackBelow(); got != 1 {
		t.Fatalf("empty tracker ackBelow = %d", got)
	}
	a, b, c := s.begin(), s.begin(), s.begin() // 1, 2, 3
	if got := s.ackBelow(); got != a {
		t.Fatalf("ackBelow = %d, want %d", got, a)
	}
	s.end(b) // out-of-order completion must not advance past 1
	if got := s.ackBelow(); got != a {
		t.Fatalf("ackBelow after completing 2 = %d, want %d", got, a)
	}
	s.end(a)
	if got := s.ackBelow(); got != c {
		t.Fatalf("ackBelow after completing 1,2 = %d, want %d", got, c)
	}
	s.end(c)
	if got := s.ackBelow(); got != 4 {
		t.Fatalf("ackBelow when idle = %d, want 4", got)
	}
}

func TestSessionTrackerConcurrent(t *testing.T) {
	s := sessionTracker{next: 1, inflight: map[uint64]struct{}{}}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				seq := s.begin()
				if ack := s.ackBelow(); ack > seq {
					t.Errorf("ackBelow %d exceeds in-flight seq %d", ack, seq)
				}
				s.end(seq)
			}
		}()
	}
	wg.Wait()
	if got := s.ackBelow(); got != 5001 {
		t.Fatalf("final watermark %d", got)
	}
}

func TestEscapeKey(t *testing.T) {
	cases := map[string]string{
		"simple":        "simple",
		"a/b/c":         "a/b/c",
		"sp ace?#":      "sp%20ace%3F%23",
		"100%":          "100%25",
		"user:42/email": "user:42/email",
	}
	for in, want := range cases {
		if got := escapeKey(in); got != want {
			t.Errorf("escapeKey(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestBackoffBounds(t *testing.T) {
	for attempt := 0; attempt < 20; attempt++ {
		d := backoff(attempt)
		if d <= 0 || d > 500*time.Millisecond {
			t.Fatalf("backoff(%d) = %v out of bounds", attempt, d)
		}
	}
}

func TestConflictErrorIs(t *testing.T) {
	var err error = &ConflictError{Key: "k", CurrentVersion: 3}
	if !errors.Is(err, ErrConflict) {
		t.Fatal("ConflictError should match ErrConflict")
	}
	var ce *ConflictError
	if !errors.As(err, &ce) || ce.CurrentVersion != 3 {
		t.Fatal("errors.As failed")
	}
}

// fakeCluster serves a single-shard, two-node topology in which node A
// always redirects to node B, exercising leader discovery.
func TestFollowsMisdirectedHints(t *testing.T) {
	var hitsA, hitsB atomic.Int32
	var b *httptest.Server
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/cluster" {
			writeCluster(w, r.Host, b.Listener.Addr().String())
			return
		}
		hitsA.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMisdirectedRequest)
		w.Write([]byte(`{"error":"not leader","code":"not_leader","leader":"b","leader_addr":"` + b.URL + `"}`))
	}))
	defer a.Close()
	b = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hitsB.Add(1)
		if r.Header.Get(api.HeaderNoForward) == "" {
			t.Errorf("smart client did not set %s", api.HeaderNoForward)
		}
		if r.Method == http.MethodPut && r.Header.Get(api.HeaderSeq) == "" {
			t.Errorf("write without session sequence")
		}
		w.Header().Set(api.HeaderVersion, "7")
		w.Write([]byte("value"))
	}))
	defer b.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := New(ctx, Config{Endpoints: []string{a.URL}})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		it, err := c.Get(ctx, "k")
		if err != nil {
			t.Fatal(err)
		}
		if string(it.Value) != "value" || it.Version != 7 {
			t.Fatalf("got %+v", it)
		}
	}
	// After learning the leader, the client must go straight to it.
	if hitsA.Load() > 1 {
		t.Fatalf("client kept asking the follower: %d requests", hitsA.Load())
	}
	if hitsB.Load() != 5 {
		t.Fatalf("leader saw %d requests, want 5", hitsB.Load())
	}
}

func writeCluster(w http.ResponseWriter, aHost, bHost string) {
	w.Header().Set("Content-Type", "application/json")
	// Both members host the single shard; leader unknown to force discovery.
	w.Write([]byte(`{"spec":{"members":[{"id":"a","addr":"http://` + aHost + `"},{"id":"b","addr":"http://` + bHost +
		`"}],"shards":1,"replication_factor":2,"virtual_nodes":8},"shards":[{"shard":0,"replicas":["a","b"]}]}`))
}

func TestUnavailableAfterDeadline(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/cluster" {
			writeCluster(w, r.Host, r.Host)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte(`{"error":"election","code":"no_leader"}`))
	}))
	defer srv.Close()
	c, err := New(context.Background(), Config{Endpoints: []string{srv.URL}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = c.Get(ctx, "k")
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if time.Since(start) > time.Second {
		t.Fatalf("retries did not respect the context deadline")
	}
}
