// Command distkv-demo runs a complete DistKV playground on one machine:
//
//   - N real distkv server processes (default 5) with persistent data dirs
//   - a built-in traffic generator with an adjustable rate
//   - the live dashboard at http://localhost:8080 with chaos controls:
//     kill -9, graceful stop, start, network partition, packet loss
//   - optionally the Python historian (SQLite-backed metrics history)
//
// Everything is free and local: no Docker, no cloud account.
//
//	go build -o bin/ ./cmd/... && bin/distkv-demo
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/UDogg/DistKV/internal/api"
	"github.com/UDogg/DistKV/internal/histogram"
	"github.com/UDogg/DistKV/internal/transport"
	"github.com/UDogg/DistKV/internal/ui"
	"github.com/UDogg/DistKV/pkg/client"
)

type config struct {
	nodes, shards, rf int
	basePort          int
	listen            string
	dir               string
	rate              int
	readRatio         float64
	fresh             bool
	historian         bool
	pythonPath        string
	distkvBin         string
	electionTimeout   time.Duration
}

func main() {
	var cfg config
	flag.IntVar(&cfg.nodes, "nodes", 5, "number of server processes")
	flag.IntVar(&cfg.shards, "shards", 24, "number of shards (Raft groups)")
	flag.IntVar(&cfg.rf, "replication", 3, "replicas per shard")
	flag.IntVar(&cfg.basePort, "base-port", 7101, "first node port")
	flag.StringVar(&cfg.listen, "listen", "127.0.0.1:8080", "dashboard address")
	flag.StringVar(&cfg.dir, "dir", ".demo", "directory for node data, logs and the history database")
	flag.IntVar(&cfg.rate, "rate", 800, "initial generated load (ops/s); adjustable from the dashboard")
	flag.Float64Var(&cfg.readRatio, "read-ratio", 0.8, "fraction of generated operations that are reads")
	flag.BoolVar(&cfg.fresh, "fresh", false, "wipe the data directory first")
	flag.BoolVar(&cfg.historian, "historian", true, "run the Python historian if python3 is available")
	flag.StringVar(&cfg.pythonPath, "python-path", "clients/python", "path to the DistKV Python package")
	flag.StringVar(&cfg.distkvBin, "distkv", "", "path to the distkv binary (default: next to this binary)")
	flag.DurationVar(&cfg.electionTimeout, "election-timeout", 300*time.Millisecond, "Raft election timeout for demo nodes")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(cfg, logger); err != nil {
		logger.Error("demo failed", "err", err)
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// Node supervision

type nodeState string

const (
	stateRunning nodeState = "running"
	stateStopped nodeState = "stopped" // graceful
	stateKilled  nodeState = "killed"  // SIGKILL
	stateExited  nodeState = "exited"  // died on its own
)

type node struct {
	ID      string    `json:"id"`
	Addr    string    `json:"addr"`
	PID     int       `json:"pid"`
	State   nodeState `json:"state"`
	Since   time.Time `json:"since"`
	Starts  int       `json:"starts"`
	cmd     *exec.Cmd
	exited  chan struct{}
	logFile *os.File
}

type supervisor struct {
	cfg     config
	logger  *slog.Logger
	bin     string
	members string

	mu    sync.Mutex
	nodes []*node

	rate      atomic.Int64
	readRatio atomic.Uint64 // float64 bits
	historian string        // base URL, if running
	clientStats
}

// clientStats is what the demo's traffic generator, a real client with
// retries, actually experienced: the user-visible availability.
type clientStats struct {
	okTotal, failTotal atomic.Uint64
	okSec, failSec     atomic.Uint64
	statsMu            sync.Mutex
	window             []clientSecond // last 60 seconds
	lat                *histogram.Histogram
}

type clientSecond struct {
	OK     uint64  `json:"ok"`
	Failed uint64  `json:"failed"`
	P99Ms  float64 `json:"p99_ms"`
}

// ClientReport is exposed in /demo/info.
type ClientReport struct {
	OKTotal      uint64         `json:"ok_total"`
	FailedTotal  uint64         `json:"failed_total"`
	Availability float64        `json:"availability"`     // since start
	Window       float64        `json:"availability_60s"` // last 60 s
	LastSecond   clientSecond   `json:"last_second"`
	Recent       []clientSecond `json:"recent"`
}

func (cs *clientStats) record(err error, d time.Duration) {
	if err != nil && !errors.Is(err, client.ErrNotFound) {
		cs.failTotal.Add(1)
		cs.failSec.Add(1)
		return
	}
	cs.okTotal.Add(1)
	cs.okSec.Add(1)
	cs.statsMu.Lock()
	cs.lat.Record(d)
	cs.statsMu.Unlock()
}

func (cs *clientStats) tick() {
	cs.statsMu.Lock()
	defer cs.statsMu.Unlock()
	sec := clientSecond{OK: cs.okSec.Swap(0), Failed: cs.failSec.Swap(0), P99Ms: float64(cs.lat.Quantile(0.99).Microseconds()) / 1000}
	cs.lat = histogram.New()
	cs.window = append(cs.window, sec)
	if len(cs.window) > 60 {
		cs.window = cs.window[1:]
	}
}

func (cs *clientStats) report() ClientReport {
	cs.statsMu.Lock()
	defer cs.statsMu.Unlock()
	r := ClientReport{OKTotal: cs.okTotal.Load(), FailedTotal: cs.failTotal.Load(), Availability: 1, Window: 1,
		Recent: append([]clientSecond(nil), cs.window...)}
	if t := r.OKTotal + r.FailedTotal; t > 0 {
		r.Availability = float64(r.OKTotal) / float64(t)
	}
	var ok, failed uint64
	for _, s := range cs.window {
		ok += s.OK
		failed += s.Failed
	}
	if ok+failed > 0 {
		r.Window = float64(ok) / float64(ok+failed)
	}
	if n := len(cs.window); n > 0 {
		r.LastSecond = cs.window[n-1]
	}
	return r
}

func run(cfg config, logger *slog.Logger) error {
	bin := cfg.distkvBin
	if bin == "" {
		exe, err := os.Executable()
		if err != nil {
			return err
		}
		bin = filepath.Join(filepath.Dir(exe), "distkv")
	}
	if _, err := os.Stat(bin); err != nil {
		return fmt.Errorf("distkv binary not found at %s (build with: go build -o bin/ ./cmd/...)", bin)
	}
	if cfg.fresh {
		os.RemoveAll(cfg.dir)
	}
	if err := os.MkdirAll(cfg.dir, 0o755); err != nil {
		return err
	}
	sv := &supervisor{cfg: cfg, logger: logger, bin: bin}
	sv.lat = histogram.New()
	sv.rate.Store(int64(cfg.rate))
	sv.setReadRatio(cfg.readRatio)

	var parts []string
	for i := 1; i <= cfg.nodes; i++ {
		id := fmt.Sprintf("n%d", i)
		addr := fmt.Sprintf("http://127.0.0.1:%d", cfg.basePort+i-1)
		parts = append(parts, id+"="+addr)
		sv.nodes = append(sv.nodes, &node{ID: id, Addr: addr, State: stateStopped})
	}
	sv.members = strings.Join(parts, ",")
	for _, n := range sv.nodes {
		if err := sv.start(n); err != nil {
			sv.shutdown()
			return err
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := sv.waitReady(ctx, 30*time.Second); err != nil {
		sv.shutdown()
		return err
	}
	if cfg.historian {
		sv.startHistorian(ctx)
	}

	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		sv.shutdown()
		return err
	}
	srv := &http.Server{Handler: sv.routes()}
	go srv.Serve(ln)
	go sv.generateLoad(ctx)

	url := "http://" + ln.Addr().String()
	if host, port, _ := net.SplitHostPort(ln.Addr().String()); host == "0.0.0.0" || host == "::" {
		url = "http://localhost:" + port // listening on all interfaces (e.g. in Docker)
	}
	fmt.Printf(`
  ╔══════════════════════════════════════════════════════════════╗
  ║  DistKV demo is running                                      ║
  ║                                                              ║
  ║  Dashboard:  %-47s ║
  ║  Nodes:      %-47s ║
  ║  Load:       %-47s ║
  ║                                                              ║
  ║  Try the chaos buttons on the node cards. Ctrl-C to stop.    ║
  ╚══════════════════════════════════════════════════════════════╝

`, url, fmt.Sprintf("%d processes, %d shards, RF=%d (logs in %s)", cfg.nodes, cfg.shards, cfg.rf, cfg.dir),
		fmt.Sprintf("%d ops/s, %.0f%% reads", cfg.rate, cfg.readRatio*100))

	<-ctx.Done()
	fmt.Println("\nstopping demo...")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(sctx)
	sv.shutdown()
	return nil
}

func (sv *supervisor) setReadRatio(v float64) {
	sv.readRatio.Store(uint64(v * 1e6))
}

func (sv *supervisor) getReadRatio() float64 { return float64(sv.readRatio.Load()) / 1e6 }

func (sv *supervisor) start(n *node) error {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	if n.State == stateRunning {
		return nil
	}
	dir := filepath.Join(sv.cfg.dir, n.ID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	logFile, err := os.OpenFile(filepath.Join(dir, "node.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	et := sv.cfg.electionTimeout
	cmd := exec.Command(sv.bin,
		"--id", n.ID, "--members", sv.members,
		"--shards", strconv.Itoa(sv.cfg.shards), "--replication", strconv.Itoa(sv.cfg.rf),
		"--data-dir", filepath.Join(dir, "data"),
		"--election-timeout", et.String(), "--heartbeat", (et / 6).String(),
		"--rebalance-interval", "3s", "--snapshot-threshold", "5000",
		"--enable-fault-injection", "--cors-origin", "*",
	)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = childAttrs()
	if err := cmd.Start(); err != nil {
		logFile.Close()
		return err
	}
	n.cmd, n.PID, n.State, n.Since, n.logFile = cmd, cmd.Process.Pid, stateRunning, time.Now(), logFile
	n.Starts++
	n.exited = make(chan struct{})
	go func(n *node, cmd *exec.Cmd, done chan struct{}) {
		cmd.Wait()
		sv.mu.Lock()
		if n.cmd == cmd && n.State == stateRunning {
			n.State, n.Since = stateExited, time.Now()
		}
		n.logFile.Close()
		sv.mu.Unlock()
		close(done)
	}(n, cmd, n.exited)
	sv.logger.Info("node started", "node", n.ID, "pid", n.PID)
	return nil
}

func (sv *supervisor) stop(n *node, sig syscall.Signal) {
	sv.mu.Lock()
	if n.State != stateRunning {
		sv.mu.Unlock()
		return
	}
	cmd, done := n.cmd, n.exited
	if sig == syscall.SIGKILL {
		n.State = stateKilled
	} else {
		n.State = stateStopped
	}
	n.Since = time.Now()
	sv.mu.Unlock()
	cmd.Process.Signal(sig)
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		cmd.Process.Kill()
		<-done
	}
	sv.logger.Info("node stopped", "node", n.ID, "signal", sig.String())
}

func (sv *supervisor) shutdown() {
	var wg sync.WaitGroup
	for _, n := range sv.nodes {
		wg.Add(1)
		go func(n *node) {
			defer wg.Done()
			sv.stop(n, syscall.SIGTERM)
		}(n)
	}
	wg.Wait()
}

func (sv *supervisor) find(id string) *node {
	for _, n := range sv.nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

func (sv *supervisor) waitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	c := &http.Client{Timeout: time.Second}
	for time.Now().Before(deadline) {
		ready := 0
		for _, n := range sv.nodes {
			if resp, err := c.Get(n.Addr + "/readyz"); err == nil {
				resp.Body.Close()
				if resp.StatusCode == 200 {
					ready++
				}
			}
		}
		if ready == len(sv.nodes) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return errors.New("cluster did not become ready; see node.log files")
}

// liveNodes returns running nodes that are not partitioned, in random order.
func (sv *supervisor) liveNodes() []*node {
	sv.mu.Lock()
	defer sv.mu.Unlock()
	var out []*node
	for _, n := range sv.nodes {
		if n.State == stateRunning {
			out = append(out, n)
		}
	}
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

// ---------------------------------------------------------------------------
// Historian (Python, SQLite)

func (sv *supervisor) startHistorian(ctx context.Context) {
	py, err := exec.LookPath("python3")
	if err != nil {
		sv.logger.Warn("python3 not found; history panel disabled")
		return
	}
	pkg, _ := filepath.Abs(sv.cfg.pythonPath)
	if _, err := os.Stat(filepath.Join(pkg, "distkv", "historian.py")); err != nil {
		sv.logger.Warn("historian not found; history panel disabled", "path", pkg)
		return
	}
	port := sv.cfg.basePort + 90
	var eps []string
	for _, n := range sv.nodes {
		eps = append(eps, n.Addr)
	}
	logFile, _ := os.Create(filepath.Join(sv.cfg.dir, "historian.log"))
	dbPath, _ := filepath.Abs(filepath.Join(sv.cfg.dir, "history.sqlite3"))
	cmd := exec.CommandContext(ctx, py, "-m", "distkv.historian",
		"--endpoints", strings.Join(eps, ","), "--db", dbPath, "--listen", fmt.Sprintf("127.0.0.1:%d", port))
	cmd.Env = append(os.Environ(), "PYTHONPATH="+pkg)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		sv.logger.Warn("starting historian failed", "err", err)
		return
	}
	go cmd.Wait()
	sv.historian = fmt.Sprintf("http://127.0.0.1:%d", port)
	sv.logger.Info("historian started", "url", sv.historian, "db", dbPath)
}

// ---------------------------------------------------------------------------
// Traffic generation

func (sv *supervisor) generateLoad(ctx context.Context) {
	var eps []string
	for _, n := range sv.nodes {
		eps = append(eps, n.Addr)
	}
	c, err := client.New(ctx, client.Config{Endpoints: eps, Timeout: 5 * time.Second})
	if err != nil {
		sv.logger.Error("load generator could not connect", "err", err)
		return
	}
	sem := make(chan struct{}, 256)
	val := []byte(strings.Repeat("v", 96))
	const tick = 20 * time.Millisecond
	t := time.NewTicker(tick)
	defer t.Stop()
	var carry float64
	secTick := time.NewTicker(time.Second)
	defer secTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-secTick.C:
			sv.clientStats.tick()
			continue
		case <-t.C:
		}
		carry += float64(sv.rate.Load()) * tick.Seconds()
		n := int(carry)
		carry -= float64(n)
		rr := sv.getReadRatio()
		for i := 0; i < n; i++ {
			select {
			case sem <- struct{}{}:
			default:
				continue // saturated: shed load rather than queue forever
			}
			go func() {
				defer func() { <-sem }()
				key := fmt.Sprintf("demo:%05d", rand.Intn(20000))
				opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				start := time.Now()
				var err error
				if rand.Float64() < rr {
					_, err = c.Get(opCtx, key)
				} else if rand.Intn(10) == 0 {
					_, err = c.Put(opCtx, key, val, client.WithTTL(time.Duration(30+rand.Intn(90))*time.Second))
				} else {
					_, err = c.Put(opCtx, key, val)
				}
				if ctx.Err() == nil {
					sv.clientStats.record(err, time.Since(start))
				}
			}()
		}
	}
}

// ---------------------------------------------------------------------------
// HTTP: dashboard, control API, proxy to the cluster

type demoInfo struct {
	Demo      bool         `json:"demo"`
	Nodes     []*node      `json:"nodes"`
	Rate      int64        `json:"rate"`
	ReadRatio float64      `json:"read_ratio"`
	Historian bool         `json:"historian"`
	Shards    int          `json:"shards"`
	Client    ClientReport `json:"client"`
}

func (sv *supervisor) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", ui.Handler())
	mux.HandleFunc("GET /demo/info", func(w http.ResponseWriter, r *http.Request) {
		sv.mu.Lock()
		nodes := make([]*node, len(sv.nodes))
		for i, n := range sv.nodes {
			cp := *n
			nodes[i] = &cp
		}
		sv.mu.Unlock()
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
		writeJSON(w, demoInfo{Demo: true, Nodes: nodes, Rate: sv.rate.Load(), ReadRatio: sv.getReadRatio(),
			Historian: sv.historian != "", Shards: sv.cfg.shards, Client: sv.clientStats.report()})
	})
	mux.HandleFunc("POST /demo/nodes/{id}/{action}", func(w http.ResponseWriter, r *http.Request) {
		n := sv.find(r.PathValue("id"))
		if n == nil {
			http.Error(w, "unknown node", http.StatusNotFound)
			return
		}
		switch action := r.PathValue("action"); action {
		case "kill":
			go sv.stop(n, syscall.SIGKILL)
		case "stop":
			go sv.stop(n, syscall.SIGTERM)
		case "start":
			if err := sv.start(n); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		case "faults":
			body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
			resp, err := http.Post(n.Addr+"/v1/admin/faults", "application/json", strings.NewReader(string(body)))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadGateway)
				return
			}
			defer resp.Body.Close()
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			return
		default:
			http.Error(w, "unknown action "+action, http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]string{"node": n.ID, "status": "ok"})
	})
	// Incremental node logs for the dashboard's log console: pass back the
	// returned offset to receive only new lines (-1 = the last few lines).
	mux.HandleFunc("GET /demo/nodes/{id}/logs", func(w http.ResponseWriter, r *http.Request) {
		n := sv.find(r.PathValue("id"))
		if n == nil {
			http.Error(w, "unknown node", http.StatusNotFound)
			return
		}
		off, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		if err != nil {
			off = -1
		}
		lines, next, err := readLog(filepath.Join(sv.cfg.dir, n.ID, "node.log"), off, 16<<10, 256<<10)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"node": n.ID, "offset": next, "lines": lines})
	})
	mux.HandleFunc("POST /demo/load", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Rate      *int64   `json:"rate"`
			ReadRatio *float64 `json:"read_ratio"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if req.Rate != nil {
			sv.rate.Store(min(max(*req.Rate, 0), 20000))
		}
		if req.ReadRatio != nil {
			sv.setReadRatio(min(max(*req.ReadRatio, 0), 1))
		}
		writeJSON(w, map[string]any{"rate": sv.rate.Load(), "read_ratio": sv.getReadRatio()})
	})
	mux.Handle("/historian/", http.StripPrefix("/historian", http.HandlerFunc(sv.proxyHistorian)))
	mux.Handle("/v1/", sv.clusterProxy())
	return mux
}

func (sv *supervisor) proxyHistorian(w http.ResponseWriter, r *http.Request) {
	if sv.historian == "" {
		http.Error(w, "historian not running", http.StatusNotFound)
		return
	}
	u, _ := url.Parse(sv.historian)
	httputil.NewSingleHostReverseProxy(u).ServeHTTP(w, r)
}

// clusterProxy forwards API calls to a healthy node, so the dashboard
// keeps working whichever nodes are killed or partitioned.
func (sv *supervisor) clusterProxy() http.Handler {
	hc := transport.NewHTTPClient(nil)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(r.Body, 2<<20))
		var lastErr error
		for _, n := range sv.liveNodes() {
			if isPartitioned(hc, n) {
				continue
			}
			target, _ := url.Parse(n.Addr)
			if r.URL.Path == "/v1/events" {
				// Stream: hand over to a reverse proxy with immediate flushing.
				rp := httputil.NewSingleHostReverseProxy(target)
				rp.FlushInterval = -1
				rp.ServeHTTP(w, r)
				return
			}
			req, _ := http.NewRequestWithContext(r.Context(), r.Method, n.Addr+r.URL.RequestURI(), strings.NewReader(string(body)))
			req.Header = r.Header.Clone()
			resp, err := hc.Do(req)
			if err != nil {
				lastErr = err
				continue
			}
			for k, vs := range resp.Header {
				w.Header()[k] = vs
			}
			w.WriteHeader(resp.StatusCode)
			io.Copy(w, resp.Body)
			resp.Body.Close()
			return
		}
		msg := "no reachable node"
		if lastErr != nil {
			msg += ": " + lastErr.Error()
		}
		writeJSONStatus(w, http.StatusServiceUnavailable, api.ErrorResponse{Error: msg, Code: api.CodeUnavailable})
	})
}

func isPartitioned(hc *http.Client, n *node) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, n.Addr+"/v1/admin/faults", nil)
	resp, err := hc.Do(req)
	if err != nil {
		return true
	}
	defer resp.Body.Close()
	var f api.Faults
	json.NewDecoder(resp.Body).Decode(&f)
	return f.Isolated
}

func writeJSON(w http.ResponseWriter, v any) { writeJSONStatus(w, http.StatusOK, v) }

func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

// readLog returns the complete lines of the file at path that start at byte
// offset off, and the offset to continue from. A negative offset, or one past
// the end (the file was recreated), returns the last tail bytes instead. At
// most limit bytes are read per call.
func readLog(path string, off, tail, limit int64) ([]string, int64, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return []string{}, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := st.Size()
	start, partial := off, false
	if off < 0 || off > size {
		start = max(0, size-tail)
		partial = start > 0
	}
	buf := make([]byte, min(limit, size-start))
	n, err := f.ReadAt(buf, start)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, 0, err
	}
	buf = buf[:n]
	end := strings.LastIndexByte(string(buf), '\n') + 1
	if end == 0 && int64(n) == limit {
		end = n // a single line longer than limit: deliver it in pieces
	}
	next := start + int64(end)
	text := string(buf[:end])
	if partial {
		// Started mid-line: drop the fragment.
		if i := strings.IndexByte(text, '\n'); i >= 0 {
			text = text[i+1:]
		}
	}
	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	if text == "" {
		lines = []string{}
	}
	return lines, next, nil
}
