// Package loadgen drives a key-value workload against DistKV and reports
// throughput, availability and latency percentiles.
//
// It is an open-loop generator: operations are scheduled at a fixed rate
// regardless of how fast earlier ones complete, and latency is measured
// from each operation's *scheduled* start time. This avoids "coordinated
// omission", where a stalled system makes a closed-loop benchmark issue
// fewer requests and so hide its worst latencies.
package loadgen

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/UDogg/DistKV/internal/histogram"
	"github.com/UDogg/DistKV/pkg/client"
)

// Config describes a workload.
type Config struct {
	// Rate is the target operations per second. Zero runs closed-loop
	// (each worker issues its next op as soon as the previous finishes).
	Rate     int
	Duration time.Duration
	// Warmup runs the workload for this long before measuring, so that
	// connection establishment and leader discovery are excluded.
	Warmup      time.Duration
	Concurrency int
	Keys        int
	ValueSize   int
	// ReadRatio is the fraction of operations that are reads.
	ReadRatio float64
	// StaleReads serves reads from any replica.
	StaleReads bool
	TTL        time.Duration
	OpTimeout  time.Duration
	// KeyPrefix namespaces the keyspace.
	KeyPrefix string
	// OnSecond, if set, receives per-second progress.
	OnSecond func(Second)
}

// Second is a per-second progress sample.
type Second struct {
	Elapsed time.Duration
	Ops     uint64
	Errors  uint64
	P99     time.Duration
}

// Report summarises a run.
type Report struct {
	Config       Config
	Elapsed      time.Duration
	Ops          uint64
	Errors       uint64
	Reads        *histogram.Histogram
	Writes       *histogram.Histogram
	All          *histogram.Histogram
	ErrorSamples []string
	Timeline     []Second
}

// Throughput returns achieved successful operations per second.
func (r *Report) Throughput() float64 {
	return float64(r.Ops-r.Errors) / r.Elapsed.Seconds()
}

// Availability returns the fraction of operations that succeeded.
func (r *Report) Availability() float64 {
	if r.Ops == 0 {
		return 0
	}
	return float64(r.Ops-r.Errors) / float64(r.Ops)
}

// WorstSecondAvailability returns the lowest per-second success ratio.
func (r *Report) WorstSecondAvailability() float64 {
	worst := 1.0
	for _, s := range r.Timeline {
		if s.Ops > 0 {
			worst = min(worst, float64(s.Ops-s.Errors)/float64(s.Ops))
		}
	}
	return worst
}

// Preload writes every key once so reads hit existing data.
func Preload(ctx context.Context, c *client.Client, cfg Config) error {
	cfg = withDefaults(cfg)
	val := make([]byte, cfg.ValueSize)
	var wg sync.WaitGroup
	var firstErr atomic.Value
	keys := make(chan int)
	for w := 0; w < cfg.Concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := range keys {
				if _, err := c.Put(ctx, key(cfg, k), val); err != nil {
					firstErr.CompareAndSwap(nil, err)
				}
			}
		}()
	}
	for k := 0; k < cfg.Keys; k++ {
		select {
		case keys <- k:
		case <-ctx.Done():
		}
	}
	close(keys)
	wg.Wait()
	if err, ok := firstErr.Load().(error); ok {
		return err
	}
	return ctx.Err()
}

func withDefaults(cfg Config) Config {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 256
	}
	if cfg.Keys <= 0 {
		cfg.Keys = 10_000
	}
	if cfg.ValueSize <= 0 {
		cfg.ValueSize = 128
	}
	if cfg.OpTimeout <= 0 {
		cfg.OpTimeout = 5 * time.Second
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "bench:"
	}
	return cfg
}

func key(cfg Config, k int) string { return fmt.Sprintf("%s%08d", cfg.KeyPrefix, k) }

type job struct {
	scheduled time.Time
	read      bool
	key       int
}

// Run executes the workload.
func Run(ctx context.Context, c *client.Client, cfg Config) *Report {
	cfg = withDefaults(cfg)
	rep := &Report{Config: cfg, Reads: histogram.New(), Writes: histogram.New(), All: histogram.New()}
	val := make([]byte, cfg.ValueSize)
	rand.Read(val)

	var ops, errs atomic.Uint64
	var secOps, secErrs atomic.Uint64
	secHist := atomic.Pointer[histogram.Histogram]{}
	secHist.Store(histogram.New())
	var errMu sync.Mutex

	runCtx, cancel := context.WithTimeout(ctx, cfg.Warmup+cfg.Duration)
	defer cancel()
	measureFrom := time.Now().Add(cfg.Warmup)

	do := func(j job, rng *rand.Rand) {
		opCtx, opCancel := context.WithTimeout(ctx, cfg.OpTimeout)
		var err error
		k := key(cfg, j.key)
		if j.read {
			var opts []client.ReadOption
			if cfg.StaleReads {
				opts = append(opts, client.Stale())
			}
			_, err = c.Get(opCtx, k, opts...)
			if errors.Is(err, client.ErrNotFound) {
				err = nil
			}
		} else {
			var opts []client.WriteOption
			if cfg.TTL > 0 {
				opts = append(opts, client.WithTTL(cfg.TTL))
			}
			_, err = c.Put(opCtx, k, val, opts...)
		}
		opCancel()
		lat := time.Since(j.scheduled)
		if j.scheduled.Before(measureFrom) {
			return
		}
		ops.Add(1)
		secOps.Add(1)
		if err != nil {
			errs.Add(1)
			secErrs.Add(1)
			errMu.Lock()
			if len(rep.ErrorSamples) < 20 {
				rep.ErrorSamples = append(rep.ErrorSamples, err.Error())
			}
			errMu.Unlock()
			return
		}
		rep.All.Record(lat)
		secHist.Load().Record(lat)
		if j.read {
			rep.Reads.Record(lat)
		} else {
			rep.Writes.Record(lat)
		}
	}

	newJob := func(rng *rand.Rand, at time.Time) job {
		return job{scheduled: at, read: rng.Float64() < cfg.ReadRatio, key: rng.Intn(cfg.Keys)}
	}

	start := time.Now()
	var wg sync.WaitGroup

	// Per-second progress.
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-time.After(cfg.Warmup):
		case <-runCtx.Done():
			return
		}
		secOps.Store(0)
		secErrs.Store(0)
		secHist.Store(histogram.New())
		start := time.Now()
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-t.C:
				h := secHist.Swap(histogram.New())
				s := Second{Elapsed: time.Since(start).Round(time.Second), Ops: secOps.Swap(0), Errors: secErrs.Swap(0), P99: h.Quantile(0.99)}
				rep.Timeline = append(rep.Timeline, s)
				if cfg.OnSecond != nil {
					cfg.OnSecond(s)
				}
			}
		}
	}()

	if cfg.Rate <= 0 {
		// Closed loop.
		for w := 0; w < cfg.Concurrency; w++ {
			wg.Add(1)
			go func(seed int64) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(seed))
				for runCtx.Err() == nil {
					do(newJob(rng, time.Now()), rng)
				}
			}(int64(w) + time.Now().UnixNano())
		}
	} else {
		jobs := make(chan job, cfg.Concurrency*4)
		for w := 0; w < cfg.Concurrency; w++ {
			wg.Add(1)
			go func(seed int64) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(seed))
				for j := range jobs {
					do(j, rng)
				}
			}(int64(w) + time.Now().UnixNano())
		}
		interval := time.Second / time.Duration(cfg.Rate)
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		next := start
	schedule:
		for {
			now := time.Now()
			for !next.After(now) {
				select {
				case jobs <- newJob(rng, next):
				case <-runCtx.Done():
					break schedule
				}
				next = next.Add(interval)
			}
			select {
			case <-runCtx.Done():
				break schedule
			case <-time.After(next.Sub(time.Now())):
			}
		}
		close(jobs)
	}
	wg.Wait()
	rep.Elapsed = time.Since(start) - cfg.Warmup
	rep.Ops, rep.Errors = ops.Load(), errs.Load()
	return rep
}

// Format renders a human-readable report.
func (r *Report) Format() string {
	line := func(name string, h *histogram.Histogram) string {
		if h.Count() == 0 {
			return fmt.Sprintf("  %-7s %10s\n", name, "-")
		}
		return fmt.Sprintf("  %-7s %10d  mean %-9v p50 %-9v p90 %-9v p99 %-9v p99.9 %-9v max %v\n",
			name, h.Count(), h.Mean(), h.Quantile(0.5), h.Quantile(0.9), h.Quantile(0.99), h.Quantile(0.999), h.Max())
	}
	s := fmt.Sprintf("duration %v | target %d ops/s | concurrency %d | keys %d | value %dB | reads %.0f%%\n",
		r.Elapsed.Round(time.Millisecond), r.Config.Rate, r.Config.Concurrency, r.Config.Keys, r.Config.ValueSize, r.Config.ReadRatio*100)
	s += fmt.Sprintf("ops %d | errors %d | throughput %.0f ops/s | availability %.4f%% (worst second %.2f%%)\n",
		r.Ops, r.Errors, r.Throughput(), r.Availability()*100, r.WorstSecondAvailability()*100)
	s += "latency (from scheduled start):\n"
	s += line("all", r.All) + line("reads", r.Reads) + line("writes", r.Writes)
	if len(r.ErrorSamples) > 0 {
		s += "sample errors:\n"
		for _, e := range r.ErrorSamples[:min(5, len(r.ErrorSamples))] {
			s += "  " + e + "\n"
		}
	}
	return s
}
