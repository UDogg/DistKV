package server

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/UDogg/DistKV/internal/api"
	"github.com/UDogg/DistKV/internal/histogram"
)

// liveStats tracks recent client traffic for the dashboard and /v1/events:
// per-second rates and latency quantiles over a sliding five-second window.
// Only requests that entered the cluster at this node are counted, so the
// cluster total is the sum over nodes without double counting forwards.
type liveStats struct {
	reads, writes, errors, forwarded atomic.Uint64

	mu       sync.Mutex
	window   [5]*histogram.Histogram // ring of per-second histograms
	cur      int
	last     api.Load
	lastTick time.Time
}

func newLiveStats() *liveStats {
	ls := &liveStats{lastTick: time.Now()}
	for i := range ls.window {
		ls.window[i] = histogram.New()
	}
	return ls
}

func (ls *liveStats) record(read bool, d time.Duration, status int) {
	if read {
		ls.reads.Add(1)
	} else {
		ls.writes.Add(1)
	}
	if status >= 500 {
		ls.errors.Add(1)
	}
	ls.mu.Lock()
	h := ls.window[ls.cur]
	ls.mu.Unlock()
	h.Record(d)
}

// tick closes the current one-second bucket. Called once per second.
func (ls *liveStats) tick() {
	now := time.Now()
	ls.mu.Lock()
	defer ls.mu.Unlock()
	secs := now.Sub(ls.lastTick).Seconds()
	if secs <= 0 {
		return
	}
	ls.lastTick = now
	merged := histogram.New()
	for _, h := range ls.window {
		merged.Merge(h)
	}
	ms := func(q float64) float64 { return float64(merged.Quantile(q).Microseconds()) / 1000 }
	ls.last = api.Load{
		ReadsPerSec:     float64(ls.reads.Swap(0)) / secs,
		WritesPerSec:    float64(ls.writes.Swap(0)) / secs,
		ErrorsPerSec:    float64(ls.errors.Swap(0)) / secs,
		ForwardedPerSec: float64(ls.forwarded.Swap(0)) / secs,
		P50Ms:           ms(0.5),
		P99Ms:           ms(0.99),
		P999Ms:          ms(0.999),
	}
	ls.cur = (ls.cur + 1) % len(ls.window)
	ls.window[ls.cur] = histogram.New()
}

func (ls *liveStats) snapshot() api.Load {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return ls.last
}
