package metrics

import (
	"strings"
	"sync"
	"testing"
)

func TestExposition(t *testing.T) {
	r := NewRegistry()
	c := r.NewCounterVec("distkv_requests_total", "Requests.", "op", "code")
	g := r.NewGaugeVec("distkv_up", "Up.")
	h := r.NewHistogramVec("distkv_latency_seconds", "Latency.", []float64{0.001, 0.01}, "op")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.With("get", "200").Inc()
			h.With("get").Observe(0.005)
		}()
	}
	wg.Wait()
	c.With("put", "409").Add(2)
	g.With().Set(1)
	h.With("get").Observe(0.0005)
	h.With("get").Observe(5)
	r.RegisterCollector(func(w *Writer) {
		w.Gauge("distkv_raft_term", "Term.", 7, "shard", "3")
		w.Gauge("distkv_raft_term", "Term.", 9, "shard", "4")
	})

	var b strings.Builder
	r.Write(&b)
	out := b.String()
	for _, want := range []string{
		`distkv_requests_total{op="get",code="200"} 100`,
		`distkv_requests_total{op="put",code="409"} 2`,
		`distkv_up 1`,
		`distkv_latency_seconds_bucket{op="get",le="0.001"} 1`,
		`distkv_latency_seconds_bucket{op="get",le="0.01"} 101`,
		`distkv_latency_seconds_bucket{op="get",le="+Inf"} 102`,
		`distkv_latency_seconds_count{op="get"} 102`,
		`distkv_raft_term{shard="3"} 7`,
		`# TYPE distkv_latency_seconds histogram`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if strings.Count(out, "# TYPE distkv_raft_term") != 1 {
		t.Errorf("collector TYPE line duplicated")
	}
}

func TestLabelEscaping(t *testing.T) {
	if got := formatLabels([]string{"k", "a\"b\\c\nd"}); got != `{k="a\"b\\c\nd"}` {
		t.Fatalf("got %s", got)
	}
}
