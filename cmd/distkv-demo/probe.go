package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/UDogg/DistKV/internal/linearizability"
	"github.com/UDogg/DistKV/pkg/client"
)

// The consistency probe checks, continuously and while you inject faults,
// that the cluster is linearizable: a few workers read, write and delete a
// small set of keys through a real client, every operation is recorded with
// its invocation and response time, and the histories are verified with
// the Wing & Gong checker (internal/linearizability), Jepsen style.
//
// A history can't be checked once at the end of an endless demo, so it is
// cut into epochs. At the end of each epoch the workers pause, and one
// barrier read per key runs with nothing else in flight on that key. That
// read pins the key's state: it closes the epoch (and the epoch's history
// is checked) and opens the next one, whose history starts from the value
// it observed. A key whose barrier read fails (its shard has no leader)
// keeps accumulating until a later barrier succeeds.
//
// Writes whose outcome the client never learned may still take effect
// later (the entry can commit after the client gave up), so they are
// carried into the following epochs for carryWindow as operations that may
// take effect at any point, or never.

const (
	probeKeyPrefix = "__probe/"
	carryWindow    = 30 * time.Second
	probeMaxSteps  = 2_000_000
	maxViolations  = 5
	// maxPending bounds a key's unchecked history: while its shard stays
	// leaderless no barrier can close the epoch, so stop adding operations
	// to it (the checker's search grows quickly with concurrent unknowns).
	maxPending    = 600
	maxEpochViews = 300
)

type probeOptions struct {
	keys, workers int
	rate          int           // total operations per second
	epoch         time.Duration // time between barriers
	opTimeout     time.Duration
}

// EpochView is one checked epoch of one key, for the dashboard.
type EpochView struct {
	Key   string `json:"key"`
	Start int64  `json:"start"` // unix ms
	End   int64  `json:"end"`
	Ops   int    `json:"ops"`
	Ok    bool   `json:"ok"`
}

// OpView is a recorded operation in a violation report.
type OpView struct {
	Client  int    `json:"client"`
	Kind    string `json:"kind"`
	Value   string `json:"value,omitempty"`
	Found   bool   `json:"found"`
	Unknown bool   `json:"unknown,omitempty"`
	Call    int64  `json:"call"`             // unix ms
	Return  int64  `json:"return,omitempty"` // unix ms; 0 = never returned
}

// Violation is an epoch that failed the check.
type Violation struct {
	Time  int64    `json:"time"`
	Key   string   `json:"key"`
	Ops   []OpView `json:"ops"`
	Error string   `json:"error"`
}

// ConsistencyReport is exposed in /demo/info.
type ConsistencyReport struct {
	Enabled         bool        `json:"enabled"`
	Verdict         string      `json:"verdict"` // "pending", "linearizable" or "violation"
	OpsChecked      uint64      `json:"ops_checked"`
	OpsPending      int         `json:"ops_pending"`
	Epochs          uint64      `json:"epochs"`
	Indeterminate   uint64      `json:"indeterminate"`
	Inconclusive    uint64      `json:"inconclusive"`
	Keys            int         `json:"keys"`
	LastCheck       int64       `json:"last_check"`
	Violations      []Violation `json:"violations"` // the latest few
	ViolationsTotal uint64      `json:"violations_total"`
	Recent          []EpochView `json:"recent"`
}

type carried struct {
	op    linearizability.Op
	until time.Time
}

type probe struct {
	c      *client.Client
	opt    probeOptions
	logger *slog.Logger
	start  time.Time
	run    string // distinguishes values from earlier runs on persistent data

	// gate pauses the workers for barrier reads: each operation holds a
	// read lock, the barrier takes the write lock.
	gate sync.RWMutex

	mu      sync.Mutex
	pin     map[string]linearizability.Op   // barrier read that opened the epoch
	seg     map[string][]linearizability.Op // operations since
	opened  map[string]time.Time
	carried []carried
	rep     ConsistencyReport
}

func newProbe(c *client.Client, opt probeOptions, logger *slog.Logger) *probe {
	p := &probe{
		c: c, opt: opt, logger: logger, start: time.Now(),
		run:    fmt.Sprintf("%x", rand.Uint32()),
		pin:    map[string]linearizability.Op{},
		seg:    map[string][]linearizability.Op{},
		opened: map[string]time.Time{},
	}
	p.rep = ConsistencyReport{Enabled: true, Verdict: "pending", Keys: opt.keys, Violations: []Violation{}, Recent: []EpochView{}}
	return p
}

func (p *probe) key(i int) string { return fmt.Sprintf("%sk%d", probeKeyPrefix, i) }

// now is the checker's clock: monotonic nanoseconds since the probe started.
func (p *probe) now() int64 { return int64(time.Since(p.start)) }

func (p *probe) wall(t int64) int64 { return p.start.Add(time.Duration(t)).UnixMilli() }

func (p *probe) report() ConsistencyReport {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := p.rep
	r.OpsPending = 0
	for _, s := range p.seg {
		r.OpsPending += len(s)
	}
	// Clone keeps empty slices non-nil, so they encode as [] rather than null.
	r.Violations = slices.Clone(p.rep.Violations)
	r.Recent = slices.Clone(p.rep.Recent)
	return r
}

func (p *probe) runProbe(ctx context.Context) {
	// Pin every key before any worker starts: the keys may hold values from
	// an earlier run (the demo's data directory persists).
	for i := 0; i < p.opt.keys; i++ {
		k := p.key(i)
		for {
			if op, ok := p.barrierRead(ctx, k); ok {
				p.mu.Lock()
				p.pin[k], p.opened[k] = op, time.Now()
				p.mu.Unlock()
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	var wg sync.WaitGroup
	for w := 0; w < p.opt.workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			p.worker(ctx, w)
		}(w)
	}
	t := time.NewTicker(p.opt.epoch)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-t.C:
			p.barrier(ctx)
		}
	}
}

func (p *probe) worker(ctx context.Context, w int) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(w)))
	interval := time.Duration(float64(time.Second) * float64(p.opt.workers) / float64(max(1, p.opt.rate)))
	for seq := 0; ctx.Err() == nil; seq++ {
		began := time.Now()
		p.gate.RLock()
		if op, ok := p.do(ctx, rng, w, seq); ok {
			p.mu.Lock()
			p.seg[op.Key] = append(p.seg[op.Key], op)
			p.mu.Unlock()
		}
		p.gate.RUnlock()
		if d := interval - time.Since(began); d > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(d):
			}
		}
	}
}

// do runs one random operation and reports whether it belongs in the history
// (a failed read constrains nothing and is dropped).
func (p *probe) do(ctx context.Context, rng *rand.Rand, w, seq int) (linearizability.Op, bool) {
	key := p.key(rng.Intn(p.opt.keys))
	p.mu.Lock()
	backlog := len(p.seg[key])
	p.mu.Unlock()
	if backlog >= maxPending {
		return linearizability.Op{}, false
	}
	opCtx, cancel := context.WithTimeout(ctx, p.opt.opTimeout)
	defer cancel()
	op := linearizability.Op{ClientID: w, Key: key, Call: p.now()}
	switch r := rng.Intn(10); {
	case r < 5:
		op.Kind = linearizability.Get
		it, err := p.c.Get(opCtx, key)
		op.Return = p.now()
		switch {
		case err == nil:
			op.Found, op.Value = true, string(it.Value)
		case errors.Is(err, client.ErrNotFound):
		default:
			return op, false
		}
	case r < 9:
		op.Kind = linearizability.Put
		op.Value = fmt.Sprintf("%s-w%d-%d", p.run, w, seq)
		_, err := p.c.Put(opCtx, key, []byte(op.Value))
		op.Return = p.now()
		op.Unknown = err != nil
	default:
		op.Kind = linearizability.Delete
		err := p.c.Delete(opCtx, key)
		op.Return = p.now()
		switch {
		case err == nil:
			op.Found = true
		case errors.Is(err, client.ErrNotFound):
		default:
			op.Unknown = true
		}
	}
	if ctx.Err() != nil {
		return op, false // shutting down: outcome meaningless
	}
	return op, true
}

func (p *probe) barrierRead(ctx context.Context, key string) (linearizability.Op, bool) {
	opCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	op := linearizability.Op{ClientID: -1, Kind: linearizability.Get, Key: key, Call: p.now()}
	it, err := p.c.Get(opCtx, key)
	op.Return = p.now()
	switch {
	case err == nil:
		op.Found, op.Value = true, string(it.Value)
	case errors.Is(err, client.ErrNotFound):
	default:
		return op, false
	}
	return op, true
}

type epochWork struct {
	key      string
	history  []linearizability.Op
	opened   time.Time
	segOps   int
	unknown  int
	closedAt time.Time
}

func (p *probe) barrier(ctx context.Context) {
	p.gate.Lock() // waits for in-flight operations, blocks new ones
	var work []epochWork
	now := time.Now()
	// One solo read per key, all keys at once (they're independent).
	reads := make([]linearizability.Op, p.opt.keys)
	oks := make([]bool, p.opt.keys)
	var wg sync.WaitGroup
	for i := 0; i < p.opt.keys; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reads[i], oks[i] = p.barrierRead(ctx, p.key(i))
		}(i)
	}
	wg.Wait()
	for i := 0; i < p.opt.keys; i++ {
		k, read := p.key(i), reads[i]
		if !oks[i] {
			continue // shard unavailable: keep accumulating
		}
		p.mu.Lock()
		seg := p.seg[k]
		h := segmentHistory(p.pin[k], p.carriedFor(k, now), seg, read)
		unknown := 0
		for _, op := range seg {
			if op.Unknown {
				unknown++
				p.carried = append(p.carried, carried{op: op, until: now.Add(carryWindow)})
			}
		}
		work = append(work, epochWork{key: k, history: h, opened: p.opened[k], segOps: len(seg), unknown: unknown, closedAt: now})
		p.pin[k], p.seg[k], p.opened[k] = read, nil, now
		p.mu.Unlock()
	}
	p.gate.Unlock()

	for _, w := range work {
		res := linearizability.Check(w.history, probeMaxSteps)
		p.record(w, res)
	}
}

// carriedFor returns the indeterminate writes on key still within their
// window, and drops expired ones. Called with p.mu held.
func (p *probe) carriedFor(key string, now time.Time) []linearizability.Op {
	var out []linearizability.Op
	kept := p.carried[:0]
	for _, c := range p.carried {
		if now.After(c.until) {
			continue
		}
		kept = append(kept, c)
		if c.op.Key == key {
			out = append(out, c.op)
		}
	}
	p.carried = kept
	return out
}

func (p *probe) record(w epochWork, res linearizability.Result) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r := &p.rep
	r.Epochs++
	r.LastCheck = time.Now().UnixMilli()
	r.Indeterminate += uint64(w.unknown)
	ok := res.Ok
	switch {
	case res.Ok:
		r.OpsChecked += uint64(w.segOps)
		if r.Verdict == "pending" {
			r.Verdict = "linearizable"
		}
	case res.Incomplete:
		r.Inconclusive++
		ok = true // not evidence of a violation
	default:
		r.Verdict = "violation"
		r.ViolationsTotal++
		v := Violation{Time: time.Now().UnixMilli(), Key: w.key, Error: res.String()}
		hist := append([]linearizability.Op(nil), w.history...)
		sort.Slice(hist, func(i, j int) bool { return hist[i].Call < hist[j].Call })
		for _, op := range hist {
			if len(v.Ops) == 60 {
				break
			}
			ov := OpView{Client: op.ClientID, Kind: [...]string{"get", "put", "delete"}[op.Kind], Value: op.Value, Found: op.Found,
				Unknown: op.Unknown, Call: p.wall(op.Call)}
			if !op.Unknown {
				ov.Return = p.wall(op.Return)
			}
			v.Ops = append(v.Ops, ov)
		}
		r.Violations = append(r.Violations, v)
		if len(r.Violations) > maxViolations {
			r.Violations = r.Violations[len(r.Violations)-maxViolations:]
		}
		p.logger.Error("consistency probe: history is NOT linearizable", "key", w.key, "ops", len(w.history))
	}
	r.Recent = append(r.Recent, EpochView{Key: w.key, Start: w.opened.UnixMilli(), End: w.closedAt.UnixMilli(), Ops: w.segOps, Ok: ok})
	if len(r.Recent) > maxEpochViews {
		r.Recent = r.Recent[len(r.Recent)-maxEpochViews:]
	}
}

// segmentHistory assembles the history of one key's epoch: the state pinned
// by the opening barrier read (as a synthetic write before it, when the key
// existed), the opening read itself, indeterminate writes carried over from
// earlier epochs, the epoch's operations, and the closing barrier read.
func segmentHistory(pin linearizability.Op, carriedOps, seg []linearizability.Op, closing linearizability.Op) []linearizability.Op {
	h := make([]linearizability.Op, 0, len(seg)+len(carriedOps)+3)
	if pin.Found {
		h = append(h, linearizability.Op{ClientID: -2, Kind: linearizability.Put, Key: pin.Key, Value: pin.Value,
			Call: pin.Call - 2, Return: pin.Call - 1})
	}
	h = append(h, pin)
	h = append(h, carriedOps...)
	h = append(h, seg...)
	return append(h, closing)
}
