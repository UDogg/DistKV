// Command distkv-study runs a reproducible fault-injection study of DistKV's
// Raft implementation: for each scenario it starts a fresh multi-process
// cluster, drives an open-loop workload, injects one fault on a fixed
// schedule (crash, graceful stop, process pause, partition, slow or lossy
// links, rolling restart), heals it, and records every operation's outcome.
//
// Each run produces 100 ms timeline bins and a JSON summary of
// availability, tail latency and recovery time; plot.py and report.py in
// study/ turn them into figures and a PDF.
//
//	go run ./cmd/distkv-study --runs 3            # everything (~35 min)
//	go run ./cmd/distkv-study --scenarios crash-leader,partition-leader --runs 1
//
// Latency is measured from each operation's scheduled start (open loop), so
// time spent queued behind a stall is counted, not hidden.
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/UDogg/DistKV/internal/api"
	"github.com/UDogg/DistKV/internal/loadgen"
	"github.com/UDogg/DistKV/pkg/client"
)

// ---------------------------------------------------------------------------
// Cluster of real distkv processes

type proc struct {
	id, addr, port string
	cmd            *exec.Cmd
	exited         chan struct{}
}

type cluster struct {
	bin, dir, members string
	shards, rf        int
	extra             []string
	nodes             []*proc
	http              *http.Client
}

func newCluster(bin, dir string, n, shards, rf, basePort int, extra []string) *cluster {
	c := &cluster{bin: bin, dir: dir, shards: shards, rf: rf, extra: extra, http: &http.Client{Timeout: 2 * time.Second}}
	var m []string
	for i := 1; i <= n; i++ {
		port := strconv.Itoa(basePort + i)
		p := &proc{id: fmt.Sprintf("n%d", i), port: port, addr: "http://127.0.0.1:" + port}
		c.nodes = append(c.nodes, p)
		m = append(m, fmt.Sprintf("%s=127.0.0.1:%s", p.id, port))
	}
	c.members = strings.Join(m, ",")
	return c
}

func (c *cluster) node(id string) *proc {
	for _, p := range c.nodes {
		if p.id == id {
			return p
		}
	}
	return nil
}

func (c *cluster) start(p *proc) error {
	data := filepath.Join(c.dir, p.id)
	if err := os.MkdirAll(data, 0o755); err != nil {
		return err
	}
	logf, err := os.OpenFile(filepath.Join(c.dir, p.id+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	args := append([]string{"--id", p.id, "--members", c.members, "--shards", strconv.Itoa(c.shards),
		"--replication", strconv.Itoa(c.rf), "--data-dir", data, "--enable-fault-injection", "--disable-ui"}, c.extra...)
	cmd := exec.Command(c.bin, args...)
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		logf.Close()
		return err
	}
	p.cmd, p.exited = cmd, make(chan struct{})
	go func(done chan struct{}) {
		cmd.Wait()
		logf.Close()
		close(done)
	}(p.exited)
	return nil
}

func (c *cluster) signal(p *proc, sig syscall.Signal) error {
	if p.cmd == nil || p.cmd.Process == nil {
		return fmt.Errorf("%s: not running", p.id)
	}
	return p.cmd.Process.Signal(sig)
}

// kill sends sig and waits for the process to exit.
func (c *cluster) kill(p *proc, sig syscall.Signal, wait time.Duration) error {
	if err := c.signal(p, sig); err != nil {
		return err
	}
	select {
	case <-p.exited:
		return nil
	case <-time.After(wait):
		c.signal(p, syscall.SIGKILL)
		<-p.exited
		return fmt.Errorf("%s: did not exit within %s", p.id, wait)
	}
}

func (c *cluster) stopAll() {
	for _, p := range c.nodes {
		if p.cmd != nil && p.exited != nil {
			select {
			case <-p.exited:
			default:
				p.cmd.Process.Signal(syscall.SIGCONT) // a paused process can't die
				p.cmd.Process.Kill()
				<-p.exited
			}
		}
	}
}

func (c *cluster) faults(p *proc, f api.Faults) error {
	body, _ := json.Marshal(f)
	resp, err := c.http.Post(p.addr+"/v1/admin/faults", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: faults: HTTP %d", p.id, resp.StatusCode)
	}
	return nil
}

func (c *cluster) status(via *proc) (*api.ClusterStatus, error) {
	resp, err := c.http.Get(via.addr + "/v1/cluster")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var st api.ClusterStatus
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return nil, err
	}
	return &st, nil
}

func healthy(st *api.ClusterStatus) bool {
	if st == nil || st.HealthyShards != len(st.Shards) {
		return false
	}
	for _, n := range st.Nodes {
		if !n.Reachable || !n.Ready {
			return false
		}
	}
	return true
}

// waitHealthy waits until every shard has a leader and every node is ready,
// continuously for `stable`.
func (c *cluster) waitHealthy(ctx context.Context, timeout, stable time.Duration) error {
	deadline := time.Now().Add(timeout)
	var since time.Time
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ok := false
		for _, p := range c.nodes {
			if st, err := c.status(p); err == nil {
				ok = healthy(st)
				break
			}
		}
		switch {
		case !ok:
			since = time.Time{}
		case since.IsZero():
			since = time.Now()
		case time.Since(since) >= stable:
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("cluster did not become healthy")
}

// busiest returns node IDs ordered by how many shards they lead.
func (c *cluster) busiest() ([]string, map[string]int, [][]string, error) {
	var st *api.ClusterStatus
	var err error
	for _, p := range c.nodes {
		if st, err = c.status(p); err == nil {
			break
		}
	}
	if st == nil {
		return nil, nil, nil, err
	}
	var replicas [][]string
	for _, sh := range st.Shards {
		replicas = append(replicas, sh.Replicas)
	}
	led := map[string]int{}
	var ids []string
	for _, n := range st.Nodes {
		led[n.ID] = n.ShardsLed
		ids = append(ids, n.ID)
	}
	sort.SliceStable(ids, func(i, j int) bool {
		if led[ids[i]] != led[ids[j]] {
			return led[ids[i]] > led[ids[j]]
		}
		return ids[i] < ids[j]
	})
	return ids, led, replicas, nil
}

// ---------------------------------------------------------------------------
// Scenarios

type env struct {
	c       *cluster
	victims []*proc
	mark    func(kind, detail string) // records a timeline event
}

type scenario struct {
	ID       string
	Category string
	Title    string
	What     string
	Victims  int           // how many of the busiest leaders to target
	Hold     time.Duration // fault window
	Extra    []string      // extra distkv flags (e.g. election timeout)
	// AttemptTimeout overrides the client's per-attempt timeout (negative
	// disables it, as the client behaved before it existed).
	AttemptTimeout time.Duration
	// Inject applies the fault; Heal undoes it at the end of Hold. A nil
	// Heal with a non-nil Run means Run drives the whole window itself.
	Inject, Heal func(e *env) error
	Run          func(ctx context.Context, e *env) error
}

func eachVictim(f func(e *env, p *proc) error) func(e *env) error {
	return func(e *env) error {
		for _, p := range e.victims {
			if err := f(e, p); err != nil {
				return err
			}
		}
		return nil
	}
}

func setFaults(f api.Faults) func(e *env) error {
	return eachVictim(func(e *env, p *proc) error { return e.c.faults(p, f) })
}

var noFaults = api.Faults{}

func crashScenario(id, title string, extra []string, what string) scenario {
	return scenario{
		ID: id, Category: "crash", Title: title, Victims: 1, Hold: 10 * time.Second, Extra: extra, What: what,
		Inject: eachVictim(func(e *env, p *proc) error { return e.c.kill(p, syscall.SIGKILL, 5*time.Second) }),
		Heal:   eachVictim(func(e *env, p *proc) error { return e.c.start(p) }),
	}
}

var scenarios = []scenario{
	{ID: "baseline", Category: "baseline", Title: "No fault", Hold: 10 * time.Second,
		What: "The same schedule with nothing injected: the reference for every other scenario."},
	crashScenario("crash-leader", "Crash the busiest leader (SIGKILL)", nil,
		"SIGKILL the node leading the most shards; restart it from its data directory 10 s later (power loss: in-memory state is gone, the WAL survives)."),
	crashScenario("crash-leader-et250", "Crash, election timeout 250 ms", []string{"--election-timeout", "250ms", "--heartbeat", "25ms"},
		"As crash-leader, with the election timeout halved (heartbeat 25 ms)."),
	crashScenario("crash-leader-et1000", "Crash, election timeout 1 s", []string{"--election-timeout", "1s", "--heartbeat", "100ms"},
		"As crash-leader, with the election timeout doubled (heartbeat 100 ms)."),
	{ID: "stop-leader", Category: "crash", Title: "Graceful stop of the busiest leader (SIGTERM)", Victims: 1, Hold: 10 * time.Second,
		What:   "SIGTERM: the node transfers its leaderships (TimeoutNow) before exiting; restarted 10 s later.",
		Inject: eachVictim(func(e *env, p *proc) error { return e.c.kill(p, syscall.SIGTERM, 15*time.Second) }),
		Heal:   eachVictim(func(e *env, p *proc) error { return e.c.start(p) })},
	{ID: "pause-leader", Category: "slow", Title: "Pause the busiest leader (SIGSTOP)", Victims: 1, Hold: 10 * time.Second,
		What:   "Freeze the process for 10 s (a long GC pause or a stalled VM), then SIGCONT. Its leases must lapse before others take over.",
		Inject: eachVictim(func(e *env, p *proc) error { return e.c.signal(p, syscall.SIGSTOP) }),
		Heal:   eachVictim(func(e *env, p *proc) error { return e.c.signal(p, syscall.SIGCONT) })},
	{ID: "pause-leader-no-attempt-timeout", Category: "slow", Title: "Pause, client without a per-attempt timeout", Victims: 1, Hold: 10 * time.Second,
		AttemptTimeout: -1,
		What:           "As pause-leader with the client's per-attempt timeout disabled, as it was before this study: each request waits on the frozen leader for its whole 5 s deadline.",
		Inject:         eachVictim(func(e *env, p *proc) error { return e.c.signal(p, syscall.SIGSTOP) }),
		Heal:           eachVictim(func(e *env, p *proc) error { return e.c.signal(p, syscall.SIGCONT) })},
	{ID: "partition-leader", Category: "partition", Title: "Partition the busiest leader", Victims: 1, Hold: 10 * time.Second,
		What:   "Cut the leader off from all peers for 10 s (clients can still reach it). Check-quorum must demote it and the majority elect replacements.",
		Inject: setFaults(api.Faults{Isolated: true}), Heal: setFaults(noFaults)},
	{ID: "partition-minority", Category: "partition", Title: "Partition two nodes (a minority)", Victims: 2, Hold: 10 * time.Second,
		What:   "Isolate the two busiest leaders for 10 s. Shards with two of their three replicas on that side lose quorum and must refuse requests (CP).",
		Inject: setFaults(api.Faults{Isolated: true}), Heal: setFaults(noFaults)},
	{ID: "partition-flapping", Category: "partition", Title: "Flapping partition (1 s on, 1 s off, x5)", Victims: 1, Hold: 10 * time.Second,
		What: "Isolate and reconnect the busiest leader five times in a row, faster than leadership can settle.",
		Run: func(ctx context.Context, e *env) error {
			for i := 0; i < 5; i++ {
				if err := setFaults(api.Faults{Isolated: true})(e); err != nil {
					return err
				}
				e.mark("isolate", fmt.Sprintf("flap %d", i+1))
				if !sleepCtx(ctx, time.Second) {
					return ctx.Err()
				}
				if err := setFaults(noFaults)(e); err != nil {
					return err
				}
				e.mark("reconnect", fmt.Sprintf("flap %d", i+1))
				if !sleepCtx(ctx, time.Second) {
					return ctx.Err()
				}
			}
			return nil
		}},
	{ID: "slow-link-50ms", Category: "slow", Title: "Slow links: +50 ms on the busiest leader", Victims: 1, Hold: 10 * time.Second,
		What:   "Delay every Raft message to and from the leader by 50 ms for 10 s (a congested or distant link).",
		Inject: setFaults(api.Faults{DelayMs: 50}), Heal: setFaults(noFaults)},
	{ID: "slow-link-200ms", Category: "slow", Title: "Slow links: +200 ms on the busiest leader", Victims: 1, Hold: 10 * time.Second,
		What:   "As slow-link-50ms with 200 ms, close to the 500 ms election timeout.",
		Inject: setFaults(api.Faults{DelayMs: 200}), Heal: setFaults(noFaults)},
	{ID: "lossy-link-30", Category: "slow", Title: "Lossy links: 30% of the leader's messages dropped", Victims: 1, Hold: 10 * time.Second,
		What:   "Drop 30% of Raft messages to and from the leader for 10 s.",
		Inject: setFaults(api.Faults{DropPercent: 30}), Heal: setFaults(noFaults)},
	{ID: "rolling-restart", Category: "crash", Title: "Rolling restart of all nodes", Hold: 45 * time.Second,
		What: "Gracefully stop and restart every node in turn, waiting for full health before the next, as an upgrade would.",
		Run: func(ctx context.Context, e *env) error {
			for _, p := range e.c.nodes {
				if err := e.c.kill(p, syscall.SIGTERM, 15*time.Second); err != nil {
					return err
				}
				e.mark("stop", p.id)
				if !sleepCtx(ctx, time.Second) {
					return ctx.Err()
				}
				if err := e.c.start(p); err != nil {
					return err
				}
				e.mark("start", p.id)
				if err := e.c.waitHealthy(ctx, 20*time.Second, 500*time.Millisecond); err != nil {
					return err
				}
			}
			return nil
		}},
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// ---------------------------------------------------------------------------
// Measurement

type opRec struct {
	t    float64 // scheduled time, ms since the measurement window opened
	lat  float64 // ms from scheduled start to completion
	read bool
	ok   bool
}

type event struct {
	TMs    float64 `json:"t_ms"`
	Kind   string  `json:"kind"`
	Detail string  `json:"detail,omitempty"`
}

type window struct {
	Ops          int     `json:"ops"`
	Errors       int     `json:"errors"`
	Availability float64 `json:"availability"`
	P50          float64 `json:"p50_ms"`
	P99          float64 `json:"p99_ms"`
	P999         float64 `json:"p999_ms"`
	Max          float64 `json:"max_ms"`
	ReadP99      float64 `json:"read_p99_ms"`
	WriteP99     float64 `json:"write_p99_ms"`
}

type runResult struct {
	Scenario string   `json:"scenario"`
	Run      int      `json:"run"`
	Victims  []string `json:"victims"`
	VictimLd []int    `json:"victim_shards_led"`
	Shards   int      `json:"shards"`
	// ShardsNoQuorum: shards with a majority of replicas among the victims.
	ShardsNoQuorum int     `json:"shards_without_quorum"`
	InjectMs       float64 `json:"inject_ms"`
	HealMs         float64 `json:"heal_ms"`
	EndMs          float64 `json:"end_ms"`
	Events         []event `json:"events"`
	Baseline       window  `json:"baseline"`
	Fault          window  `json:"fault"`   // inject → heal
	After          window  `json:"after"`   // heal → end
	Impact         window  `json:"impact"`  // inject → end
	Overall        window  `json:"overall"` // the whole measured run
	// Recovery: time from injection until client-observed service is back
	// to baseline (no errors, p99 within bound) for 1 s straight; -1 if
	// that never happened before the heal.
	RecoveryMs float64 `json:"recovery_ms"`
	// RecoveryAfterHealMs: the same, measured from the heal, for faults the
	// system cannot mask (e.g. lost quorum).
	RecoveryAfterHealMs float64 `json:"recovery_after_heal_ms"`
	// DisruptedMs: total time of degraded 100 ms bins from injection on.
	DisruptedMs float64 `json:"disrupted_ms"`
	// RejoinMs: from the heal until every node is ready and every shard led.
	RejoinMs  float64 `json:"rejoin_ms"`
	BoundMs   float64 `json:"degraded_p99_bound_ms"`
	Error     string  `json:"error,omitempty"`
	StartedAt string  `json:"started_at"`
}

func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}

func summarize(ops []opRec, from, to float64) window {
	var all, reads, writes []float64
	w := window{}
	for _, o := range ops {
		if o.t < from || o.t >= to {
			continue
		}
		w.Ops++
		if !o.ok {
			w.Errors++
			continue
		}
		all = append(all, o.lat)
		if o.read {
			reads = append(reads, o.lat)
		} else {
			writes = append(writes, o.lat)
		}
	}
	slices.Sort(all)
	slices.Sort(reads)
	slices.Sort(writes)
	if w.Ops > 0 {
		w.Availability = float64(w.Ops-w.Errors) / float64(w.Ops)
	}
	w.P50, w.P99, w.P999 = quantile(all, .5), quantile(all, .99), quantile(all, .999)
	if len(all) > 0 {
		w.Max = all[len(all)-1]
	}
	w.ReadP99, w.WriteP99 = quantile(reads, .99), quantile(writes, .99)
	return w
}

type bin struct {
	t                float64
	ops, errs        int
	p50, p99, maxLat float64
}

const binMs = 100.0

func binsOf(ops []opRec, end float64) []bin {
	n := int(math.Ceil(end / binMs))
	lats := make([][]float64, n)
	out := make([]bin, n)
	for i := range out {
		out[i].t = float64(i) * binMs
	}
	for _, o := range ops {
		i := int(o.t / binMs)
		if i < 0 || i >= n {
			continue
		}
		out[i].ops++
		if !o.ok {
			out[i].errs++
			continue
		}
		lats[i] = append(lats[i], o.lat)
	}
	for i := range out {
		slices.Sort(lats[i])
		out[i].p50, out[i].p99 = quantile(lats[i], .5), quantile(lats[i], .99)
		if len(lats[i]) > 0 {
			out[i].maxLat = lats[i][len(lats[i])-1]
		}
	}
	return out
}

// recovered returns the start of the first 1 s of clean bins at or after
// from (and before limit), or -1.
func recovered(bins []bin, from, limit, bound float64) float64 {
	need := int(1000 / binMs)
	run := 0
	for _, b := range bins {
		if b.t+binMs <= from {
			continue // the bin holding `from` counts
		}
		if b.t >= limit {
			break
		}
		if b.ops > 0 && b.errs == 0 && b.p99 <= bound {
			run++
			if run == need {
				return b.t - float64(need-1)*binMs
			}
		} else {
			run = 0
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// One run

type config struct {
	bin, out                    string
	nodes, shards, rf, basePort int
	rate, keys, concurrency     int
	readRatio                   float64
	pre, post, warmup           time.Duration
}

func runOne(ctx context.Context, cfg config, sc scenario, run int) (*runResult, []bin, error) {
	res := &runResult{Scenario: sc.ID, Run: run, StartedAt: time.Now().UTC().Format(time.RFC3339), RecoveryMs: -1, RecoveryAfterHealMs: -1, RejoinMs: -1}
	dir, err := os.MkdirTemp("", "distkv-study-")
	if err != nil {
		return nil, nil, err
	}
	defer os.RemoveAll(dir)
	c := newCluster(cfg.bin, dir, cfg.nodes, cfg.shards, cfg.rf, cfg.basePort, sc.Extra)
	defer c.stopAll()
	for _, p := range c.nodes {
		if err := c.start(p); err != nil {
			return nil, nil, err
		}
	}
	if err := c.waitHealthy(ctx, 60*time.Second, 3*time.Second); err != nil {
		return nil, nil, fmt.Errorf("startup: %w", err)
	}
	var eps []string
	for _, p := range c.nodes {
		eps = append(eps, p.addr)
	}
	cl, err := client.New(ctx, client.Config{Endpoints: eps, AttemptTimeout: sc.AttemptTimeout})
	if err != nil {
		return nil, nil, err
	}
	lcfg := loadgen.Config{Rate: cfg.rate, Warmup: cfg.warmup, Duration: cfg.pre + sc.Hold + cfg.post,
		Concurrency: cfg.concurrency, Keys: cfg.keys, ReadRatio: cfg.readRatio, KeyPrefix: "study:"}
	// Preload with modest concurrency: the run's high in-flight limit
	// exists for the open-loop phase, not for a bulk load.
	pre := lcfg
	pre.Concurrency = 256
	if err := loadgen.Preload(ctx, cl, pre); err != nil {
		return nil, nil, fmt.Errorf("preload: %w", err)
	}
	// Leadership balancing may still be moving leaders after a preload; let
	// it settle so every run starts from the same steady state.
	if err := c.waitHealthy(ctx, 30*time.Second, 2*time.Second); err != nil {
		return nil, nil, err
	}

	var mu sync.Mutex
	ops := make([]opRec, 0, cfg.rate*int((lcfg.Duration+lcfg.Warmup)/time.Second+1))
	var t0 time.Time // when the measurement window opens
	lcfg.OnOp = func(sched time.Time, lat time.Duration, read bool, err error) {
		r := opRec{t: float64(sched.Sub(t0).Microseconds()) / 1000, lat: float64(lat.Microseconds()) / 1000, read: read, ok: err == nil}
		mu.Lock()
		ops = append(ops, r)
		mu.Unlock()
	}
	now := func() float64 { return float64(time.Since(t0).Microseconds()) / 1000 }
	var evMu sync.Mutex
	mark := func(kind, detail string) {
		evMu.Lock()
		res.Events = append(res.Events, event{TMs: now(), Kind: kind, Detail: detail})
		evMu.Unlock()
	}

	t0 = time.Now().Add(cfg.warmup) // loadgen opens its window after the warmup
	done := make(chan *loadgen.Report, 1)
	go func() { done <- loadgen.Run(ctx, cl, lcfg) }()

	// The fault schedule, in parallel with the load.
	var faultErr error
	var statusMu sync.Mutex
	type sample struct {
		t  float64
		ok bool
	}
	var samples []sample
	pollCtx, stopPoll := context.WithCancel(ctx)
	defer stopPoll()
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-pollCtx.Done():
				return
			case <-t.C:
				ok := false
				statusMu.Lock()
				victims := slices.Clone(res.Victims)
				statusMu.Unlock()
				for _, p := range c.nodes {
					if slices.Contains(victims, p.id) {
						continue // ask a node the fault didn't touch
					}
					if st, err := c.status(p); err == nil {
						ok = healthy(st)
						break
					}
				}
				statusMu.Lock()
				samples = append(samples, sample{now(), ok})
				statusMu.Unlock()
			}
		}
	}()

	if !sleepCtx(ctx, time.Until(t0.Add(cfg.pre))) {
		return nil, nil, ctx.Err()
	}
	ids, led, replicas, err := c.busiest()
	if err != nil {
		faultErr = err
	}
	e := &env{c: c, mark: mark}
	for i := 0; i < sc.Victims && i < len(ids); i++ {
		e.victims = append(e.victims, c.node(ids[i]))
		statusMu.Lock()
		res.Victims = append(res.Victims, ids[i])
		statusMu.Unlock()
		res.VictimLd = append(res.VictimLd, led[ids[i]])
	}
	// Shards that lose a majority of their replicas if every victim is down
	// or cut off: Raft must make them unavailable until the heal.
	for _, reps := range replicas {
		hit := 0
		for _, r := range reps {
			if slices.Contains(res.Victims, r) {
				hit++
			}
		}
		if hit > len(reps)/2 {
			res.ShardsNoQuorum++
		}
	}
	res.Shards = len(replicas)
	res.InjectMs = now()
	mark("inject", strings.Join(res.Victims, ","))
	switch {
	case sc.Run != nil:
		holdCtx, cancel := context.WithTimeout(ctx, sc.Hold)
		if err := sc.Run(holdCtx, e); err != nil && faultErr == nil {
			faultErr = err
		}
		cancel()
	case sc.Inject != nil:
		if err := sc.Inject(e); err != nil && faultErr == nil {
			faultErr = err
		}
		sleepCtx(ctx, time.Until(t0.Add(cfg.pre+sc.Hold)))
		if sc.Heal != nil {
			if err := sc.Heal(e); err != nil && faultErr == nil {
				faultErr = err
			}
		}
	default:
		sleepCtx(ctx, time.Until(t0.Add(cfg.pre+sc.Hold)))
	}
	res.HealMs = now()
	mark("heal", "")
	rep := <-done
	stopPoll()
	res.EndMs = (cfg.pre + sc.Hold + cfg.post).Seconds() * 1000
	if faultErr != nil {
		res.Error = faultErr.Error()
	}
	_ = rep

	mu.Lock()
	defer mu.Unlock()
	res.Baseline = summarize(ops, 0, res.InjectMs)
	res.Fault = summarize(ops, res.InjectMs, res.HealMs)
	res.After = summarize(ops, res.HealMs, res.EndMs)
	res.Impact = summarize(ops, res.InjectMs, res.EndMs)
	res.Overall = summarize(ops, 0, res.EndMs)
	bins := binsOf(ops, res.EndMs)
	// A bin is degraded if any request failed or its p99 exceeds 5x the
	// baseline p99, and at least 50 ms: fault-free runs show isolated
	// 20-40 ms bins (fsync stalls on a shared disk) that are not faults.
	res.BoundMs = math.Max(50, 5*res.Baseline.P99)
	if r := recovered(bins, res.InjectMs, res.HealMs, res.BoundMs); r >= 0 {
		res.RecoveryMs = math.Max(0, r-res.InjectMs)
	}
	if r := recovered(bins, res.HealMs, res.EndMs, res.BoundMs); r >= 0 {
		res.RecoveryAfterHealMs = math.Max(0, r-res.HealMs)
	}
	for _, b := range bins {
		if b.t >= res.InjectMs && b.ops > 0 && (b.errs > 0 || b.p99 > res.BoundMs) {
			res.DisruptedMs += binMs
		}
	}
	statusMu.Lock()
	for _, s := range samples {
		if s.t >= res.HealMs && s.ok {
			res.RejoinMs = s.t - res.HealMs
			break
		}
	}
	statusMu.Unlock()
	return res, bins, nil
}

func writeBins(path string, bins []bin) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"t_ms", "ops", "errors", "p50_ms", "p99_ms", "max_ms"})
	for _, b := range bins {
		w.Write([]string{fmt.Sprint(b.t), fmt.Sprint(b.ops), fmt.Sprint(b.errs),
			strconv.FormatFloat(b.p50, 'f', 3, 64), strconv.FormatFloat(b.p99, 'f', 3, 64), strconv.FormatFloat(b.maxLat, 'f', 3, 64)})
	}
	w.Flush()
	return w.Error()
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

// ---------------------------------------------------------------------------

func main() {
	var cfg config
	var names string
	var runs int
	var list bool
	flag.StringVar(&cfg.bin, "bin", "bin/distkv", "distkv binary")
	flag.StringVar(&cfg.out, "out", "study/results", "output directory")
	flag.IntVar(&cfg.nodes, "nodes", 5, "cluster size")
	flag.IntVar(&cfg.shards, "shards", 24, "shards")
	flag.IntVar(&cfg.rf, "replication", 3, "replication factor")
	flag.IntVar(&cfg.basePort, "base-port", 7400, "node i listens on base-port+i")
	flag.IntVar(&cfg.rate, "rate", 3000, "offered load, ops/s (open loop)")
	flag.IntVar(&cfg.keys, "keys", 20000, "keyspace size")
	flag.IntVar(&cfg.concurrency, "concurrency", 16384, "maximum in-flight operations (high: users are independent, so a stalled shard must not queue the others)")
	flag.Float64Var(&cfg.readRatio, "read-ratio", 0.8, "fraction of reads")
	flag.DurationVar(&cfg.warmup, "warmup", 5*time.Second, "unmeasured warmup")
	flag.DurationVar(&cfg.pre, "pre", 10*time.Second, "steady state measured before the fault")
	flag.DurationVar(&cfg.post, "post", 20*time.Second, "observation after the heal")
	flag.StringVar(&names, "scenarios", "all", "comma-separated scenario IDs, or all")
	flag.IntVar(&runs, "runs", 3, "repetitions per scenario")
	flag.BoolVar(&list, "list", false, "list the scenarios and exit")
	flag.Parse()
	log.SetFlags(log.Ltime)

	if list {
		for _, s := range scenarios {
			fmt.Printf("%-22s %s\n", s.ID, s.Title)
		}
		return
	}
	selected := scenarios
	if names != "all" {
		selected = nil
		for _, n := range strings.Split(names, ",") {
			i := slices.IndexFunc(scenarios, func(s scenario) bool { return s.ID == strings.TrimSpace(n) })
			if i < 0 {
				log.Fatalf("unknown scenario %q (see --list)", n)
			}
			selected = append(selected, scenarios[i])
		}
	}
	if _, err := os.Stat(cfg.bin); err != nil {
		log.Fatalf("distkv binary %s: %v (run make build)", cfg.bin, err)
	}
	ctx := context.Background()
	if err := os.MkdirAll(cfg.out, 0o755); err != nil {
		log.Fatal(err)
	}
	commit, _ := exec.Command("git", "rev-parse", "--short", "HEAD").Output()
	meta := map[string]any{
		"started_at": time.Now().UTC().Format(time.RFC3339), "git_commit": strings.TrimSpace(string(commit)),
		"go": runtime.Version(), "os": runtime.GOOS + "/" + runtime.GOARCH, "cpus": runtime.NumCPU(),
		"nodes": cfg.nodes, "shards": cfg.shards, "replication": cfg.rf, "rate": cfg.rate, "read_ratio": cfg.readRatio,
		"keys": cfg.keys, "concurrency": cfg.concurrency, "runs": runs, "pre_s": cfg.pre.Seconds(), "post_s": cfg.post.Seconds(),
		"warmup_s": cfg.warmup.Seconds(), "op_timeout_s": 5, "bin_ms": binMs, "fsync": true,
	}
	var catalog []map[string]any
	for _, s := range scenarios {
		catalog = append(catalog, map[string]any{"id": s.ID, "category": s.Category, "title": s.Title, "what": s.What, "hold_s": s.Hold.Seconds(), "extra_flags": s.Extra,
			"client_attempt_timeout_s": s.AttemptTimeout.Seconds()})
	}
	meta["scenarios"] = catalog
	if err := writeJSON(filepath.Join(cfg.out, "meta.json"), meta); err != nil {
		log.Fatal(err)
	}

	var all []*runResult
	if b, err := os.ReadFile(filepath.Join(cfg.out, "runs.json")); err == nil {
		json.Unmarshal(b, &all) // keep results of scenarios not re-run
	}
	for _, sc := range selected {
		all = slices.DeleteFunc(all, func(r *runResult) bool { return r.Scenario == sc.ID })
		for r := 1; r <= runs; r++ {
			log.Printf("%s run %d/%d …", sc.ID, r, runs)
			res, bins, err := runOne(ctx, cfg, sc, r)
			if err != nil {
				log.Printf("  FAILED: %v", err)
				continue
			}
			d := filepath.Join(cfg.out, sc.ID, fmt.Sprintf("run%d", r))
			os.MkdirAll(d, 0o755)
			writeBins(filepath.Join(d, "bins.csv"), bins)
			writeJSON(filepath.Join(d, "run.json"), res)
			all = append(all, res)
			rec := "never (until healed)"
			if res.RecoveryMs >= 0 {
				rec = fmt.Sprintf("%.0f ms", res.RecoveryMs)
			}
			log.Printf("  victims %v | fault-window availability %.4f%% p99 %.1f ms (baseline %.1f) | recovery %s | disrupted %.1f s%s",
				res.Victims, res.Fault.Availability*100, res.Fault.P99, res.Baseline.P99, rec, res.DisruptedMs/1000,
				map[bool]string{true: " | ERROR " + res.Error, false: ""}[res.Error != ""])
			if err := writeJSON(filepath.Join(cfg.out, "runs.json"), all); err != nil {
				log.Fatal(err)
			}
		}
	}
	log.Printf("done: %d runs in %s", len(all), cfg.out)
}
