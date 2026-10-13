// Package server wires DistKV together: one Raft group per locally hosted
// shard, the HTTP client API, request routing/forwarding, leader balancing,
// observability endpoints and graceful shutdown.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/UDogg/DistKV/internal/api"
	"github.com/UDogg/DistKV/internal/cluster"
	"github.com/UDogg/DistKV/internal/kv"
	"github.com/UDogg/DistKV/internal/raft"
	"github.com/UDogg/DistKV/internal/transport"
	"github.com/UDogg/DistKV/internal/ui"
)

// Config configures a Server.
type Config struct {
	// NodeID is this server's identity; it must be a member of Spec.
	NodeID cluster.NodeID
	// Spec is the static cluster definition, identical on all nodes.
	Spec cluster.Spec

	// DataDir holds one subdirectory per hosted shard. When empty, state
	// is kept in memory only (pure cache mode: a restarted node rebuilds
	// from its peers via snapshot).
	DataDir string
	// NoSync disables fsync (survives process crashes, not power loss).
	NoSync bool

	HeartbeatInterval time.Duration
	ElectionTimeout   time.Duration
	SnapshotThreshold uint64
	// ReadMode selects how linearizable reads confirm leadership:
	// ReadModeLease (default) or ReadModeReadIndex.
	ReadMode string

	// Cache bounds applied to every shard.
	MaxKeysPerShard  int
	MaxBytesPerShard int64

	// RequestTimeout bounds each client request end to end.
	RequestTimeout time.Duration
	// MaxValueBytes rejects larger values with 413.
	MaxValueBytes int64
	// MaxKeyBytes rejects longer keys with 400.
	MaxKeyBytes int

	// RebalanceInterval controls how often leaders hand shards back to
	// their preferred replica. Zero disables balancing.
	RebalanceInterval time.Duration
	// ExpiryInterval controls how often leaders advance the replicated
	// clock on idle shards so expired keys are reclaimed.
	ExpiryInterval time.Duration

	// TLS, if set, serves HTTPS. If TLS.ClientCAs is set, the Raft and
	// admin endpoints additionally require a verified client certificate
	// (mutual TLS between nodes).
	TLS *tls.Config
	// PeerTLS configures outbound connections to other nodes (Raft RPCs,
	// forwarding, status aggregation). Member addresses must use https://.
	PeerTLS *tls.Config

	// EnableFaultInjection exposes /v1/admin/faults for partition, packet
	// loss and delay injection (demos and chaos testing only).
	EnableFaultInjection bool
	// CORSOrigin, if set, is sent as Access-Control-Allow-Origin so that a
	// dashboard served from elsewhere can call the API.
	CORSOrigin string
	// DisableUI turns off the embedded dashboard at /ui/.
	DisableUI bool

	EnablePprof bool
	Logger      *slog.Logger
}

func (c *Config) setDefaults() {
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 50 * time.Millisecond
	}
	if c.ElectionTimeout <= 0 {
		c.ElectionTimeout = 500 * time.Millisecond
	}
	if c.ReadMode == "" {
		c.ReadMode = ReadModeLease
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 20_000
	}
	if c.RequestTimeout <= 0 {
		c.RequestTimeout = 3 * time.Second
	}
	if c.MaxValueBytes <= 0 {
		c.MaxValueBytes = 1 << 20
	}
	if c.MaxKeyBytes <= 0 {
		c.MaxKeyBytes = 1024
	}
	if c.ExpiryInterval <= 0 {
		c.ExpiryInterval = time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

// Read modes.
const (
	// ReadModeLease serves linearizable reads locally while the leader
	// holds a quorum lease, falling back to ReadIndex otherwise.
	ReadModeLease = "lease"
	// ReadModeReadIndex confirms leadership with a heartbeat round for
	// every batch of reads; it does not depend on clock rates at all.
	ReadModeReadIndex = "readindex"
)

// Server is a DistKV node.
type Server struct {
	cfg    Config
	topo   *cluster.Topology
	self   cluster.Member
	logger *slog.Logger

	groups    map[cluster.ShardID]*group
	engine    *raft.LogEngine // shared WAL for all local shards (nil in memory mode)
	transport *transport.HTTP
	client    *http.Client // for forwarding and status aggregation

	// leaderHints caches the last known leader of shards not hosted here.
	hintsMu     sync.RWMutex
	leaderHints map[cluster.ShardID]cluster.NodeID

	handler http.Handler
	httpSrv *http.Server
	m       *serverMetrics
	live    *liveStats
	faults  faults

	// lifeMu makes Serve's start-up and the start of Shutdown/Kill mutually
	// exclusive: a stop either follows a complete start or prevents it.
	lifeMu   sync.Mutex
	started  atomic.Bool
	draining atomic.Bool
	stopBg   chan struct{}
	bgWG     sync.WaitGroup
}

type group struct {
	shard    cluster.ShardID
	replicas []cluster.NodeID
	node     *raft.Node
	store    *kv.Store
	storage  raft.Storage
}

// New builds a server and recovers local state. It does not start network
// activity; call Serve.
func New(cfg Config) (*Server, error) {
	cfg.setDefaults()
	if cfg.ReadMode != ReadModeLease && cfg.ReadMode != ReadModeReadIndex {
		return nil, fmt.Errorf("server: unknown read mode %q", cfg.ReadMode)
	}
	topo, err := cluster.New(cfg.Spec)
	if err != nil {
		return nil, err
	}
	self, ok := topo.Member(cfg.NodeID)
	if !ok {
		return nil, fmt.Errorf("server: node %q is not a cluster member", cfg.NodeID)
	}
	s := &Server{
		cfg:         cfg,
		topo:        topo,
		self:        self,
		logger:      cfg.Logger.With("node", string(cfg.NodeID)),
		groups:      make(map[cluster.ShardID]*group),
		client:      transport.NewHTTPClient(cfg.PeerTLS),
		leaderHints: make(map[cluster.ShardID]cluster.NodeID),
		stopBg:      make(chan struct{}),
		live:        newLiveStats(),
	}
	s.transport = transport.NewHTTP(func(id raft.NodeID) (string, bool) {
		m, ok := topo.Member(cluster.NodeID(id))
		return m.Addr, ok
	}, cfg.PeerTLS)
	if cfg.EnableFaultInjection {
		s.transport.Hook = s.faults.outbound
	}

	if cfg.DataDir != "" {
		if err := s.checkFingerprint(); err != nil {
			return nil, err
		}
		eng, err := raft.OpenLogEngine(filepath.Join(cfg.DataDir, "raft"), raft.LogEngineOptions{NoSync: cfg.NoSync})
		if err != nil {
			return nil, fmt.Errorf("server: opening log engine: %w", err)
		}
		s.engine = eng
	}
	for _, shard := range topo.ShardsOn(cfg.NodeID) {
		g, err := s.openGroup(shard)
		if err != nil {
			s.closeGroups()
			return nil, err
		}
		s.groups[shard] = g
	}
	s.m = newServerMetrics(s)
	s.handler = s.routes()
	s.httpSrv = &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(s.logger.Handler(), slog.LevelDebug),
	}
	s.logger.Info("node initialised", "shards_hosted", len(s.groups), "total_shards", topo.NumShards(),
		"replication_factor", topo.Spec().ReplicationFactor, "fingerprint", topo.Fingerprint())
	return s, nil
}

func (s *Server) openGroup(shard cluster.ShardID) (*group, error) {
	replicas := s.topo.Replicas(shard)
	peers := make([]raft.NodeID, len(replicas))
	for i, r := range replicas {
		peers[i] = raft.NodeID(r)
	}
	var storage raft.Storage
	if s.engine == nil {
		storage = raft.NewMemoryStorage()
	} else {
		storage = s.engine.Storage(uint64(shard))
	}
	store := kv.NewStore(kv.Options{MaxKeys: s.cfg.MaxKeysPerShard, MaxBytes: s.cfg.MaxBytesPerShard})
	node, err := raft.NewNode(raft.Config{
		ID:                raft.NodeID(s.cfg.NodeID),
		Peers:             peers,
		HeartbeatInterval: s.cfg.HeartbeatInterval,
		ElectionTimeout:   s.cfg.ElectionTimeout,
		SnapshotThreshold: s.cfg.SnapshotThreshold,
		Storage:           storage,
		Transport:         s.transport.ForShard(shard),
		StateMachine:      store,
		Logger:            s.logger.With("shard", int(shard)),
	})
	if err != nil {
		storage.Close()
		return nil, fmt.Errorf("server: shard %d: %w", shard, err)
	}
	return &group{shard: shard, replicas: replicas, node: node, store: store, storage: storage}, nil
}

// checkFingerprint refuses to start if the data directory was created
// with a different placement-relevant configuration.
func (s *Server) checkFingerprint() error {
	if err := os.MkdirAll(s.cfg.DataDir, 0o755); err != nil {
		return err
	}
	path := filepath.Join(s.cfg.DataDir, "CLUSTER")
	want := fmt.Sprintf("node=%s fingerprint=%s\n", s.cfg.NodeID, s.topo.Fingerprint())
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.WriteFile(path, []byte(want), 0o644)
	}
	if err != nil {
		return err
	}
	if string(data) != want {
		return fmt.Errorf("server: data dir %s belongs to a different cluster configuration (%q, expected %q); "+
			"refusing to start", s.cfg.DataDir, strings.TrimSpace(string(data)), strings.TrimSpace(want))
	}
	return nil
}

// Handler returns the root HTTP handler (useful for embedding and tests).
func (s *Server) Handler() http.Handler { return s.handler }

// Topology returns the cluster topology.
func (s *Server) Topology() *cluster.Topology { return s.topo }

// Serve starts Raft and serves HTTP on l until Shutdown is called.
func (s *Server) Serve(l net.Listener) error {
	if !s.started.CompareAndSwap(false, true) {
		return errors.New("server: already started")
	}
	s.lifeMu.Lock()
	if s.draining.Load() {
		s.lifeMu.Unlock()
		l.Close()
		return nil
	}
	if s.cfg.TLS != nil {
		l = tls.NewListener(l, s.cfg.TLS)
	}
	for _, g := range s.groups {
		g.node.Start()
	}
	s.startBackground()
	s.lifeMu.Unlock()
	s.logger.Info("serving", "addr", l.Addr().String(), "advertise", s.self.Addr)
	err := s.httpSrv.Serve(l)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// ListenAndServe listens on addr and calls Serve.
func (s *Server) ListenAndServe(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(l)
}

// Shutdown gracefully stops the node: it hands off every shard it leads to
// a healthy peer (so clients see no election gap), drains in-flight
// requests and closes storage.
func (s *Server) Shutdown(ctx context.Context) error {
	if !s.beginStop() {
		return nil
	}
	s.logger.Info("shutting down: transferring leadership")
	s.transferAllLeadership(ctx)

	close(s.stopBg)
	s.bgWG.Wait()

	err := s.httpSrv.Shutdown(ctx)
	s.closeGroups()
	s.transport.Close()
	s.client.CloseIdleConnections()
	s.logger.Info("shutdown complete")
	return err
}

// Kill stops the node abruptly without handing off leadership (used by
// fault-injection tests to simulate a crash).
func (s *Server) Kill() {
	if !s.beginStop() {
		return
	}
	close(s.stopBg)
	s.httpSrv.Close()
	s.bgWG.Wait()
	s.closeGroups()
	s.transport.Close()
	s.client.CloseIdleConnections()
}

// beginStop marks the node as draining; it reports false if it already was.
// It waits for an in-progress Serve start-up to finish.
func (s *Server) beginStop() bool {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	return s.draining.CompareAndSwap(false, true)
}

func (s *Server) closeGroups() {
	var wg sync.WaitGroup
	for _, g := range s.groups {
		wg.Add(1)
		go func(g *group) {
			defer wg.Done()
			g.node.Stop()
			if err := g.storage.Close(); err != nil {
				s.logger.Error("closing storage", "shard", int(g.shard), "err", err)
			}
		}(g)
	}
	wg.Wait()
	if s.engine != nil {
		if err := s.engine.Close(); err != nil {
			s.logger.Error("closing log engine", "err", err)
		}
	}
}

func (s *Server) transferAllLeadership(ctx context.Context) {
	var wg sync.WaitGroup
	for _, g := range s.groups {
		if !g.node.IsLeader() {
			continue
		}
		target, ok := s.bestTransferTarget(g, "")
		if !ok {
			continue
		}
		wg.Add(1)
		go func(g *group, target raft.NodeID) {
			defer wg.Done()
			if err := g.node.TransferLeadership(ctx, target); err != nil {
				s.logger.Warn("leadership transfer failed", "shard", int(g.shard), "target", string(target), "err", err)
			}
		}(g, target)
	}
	wg.Wait()
}

// bestTransferTarget picks the most up-to-date peer that has been in
// recent contact, preferring prefer if it qualifies.
func (s *Server) bestTransferTarget(g *group, prefer raft.NodeID) (raft.NodeID, bool) {
	st := g.node.Status()
	if st.State != raft.Leader {
		return "", false
	}
	healthy := func(p raft.PeerStatus) bool {
		return p.LastContact < s.cfg.ElectionTimeout && st.LastIndex-p.Match < 1000
	}
	if prefer != "" {
		if p, ok := st.Peers[prefer]; ok && healthy(p) {
			return prefer, true
		}
		return "", false
	}
	var best raft.NodeID
	var bestMatch uint64
	for id, p := range st.Peers {
		if healthy(p) && (best == "" || p.Match > bestMatch || (p.Match == bestMatch && id < best)) {
			best, bestMatch = id, p.Match
		}
	}
	return best, best != ""
}

func (s *Server) startBackground() {
	if s.cfg.RebalanceInterval > 0 {
		s.bgWG.Add(1)
		go s.rebalanceLoop()
	}
	s.bgWG.Add(2)
	go s.expiryLoop()
	go s.statsLoop()
}

func (s *Server) statsLoop() {
	defer s.bgWG.Done()
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-s.stopBg:
			return
		case <-t.C:
			s.live.tick()
		}
	}
}

// rebalanceLoop moves leadership of each shard back to its preferred
// replica (the first in rendezvous order). This spreads leaders evenly
// across the cluster after failures and restarts, which matters because
// all writes and linearizable reads of a shard are served by its leader.
func (s *Server) rebalanceLoop() {
	defer s.bgWG.Done()
	t := time.NewTicker(s.cfg.RebalanceInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stopBg:
			return
		case <-t.C:
		}
		if s.draining.Load() {
			return
		}
		for _, g := range s.groups {
			preferred := raft.NodeID(g.replicas[0])
			if preferred == raft.NodeID(s.cfg.NodeID) || !g.node.IsLeader() {
				continue
			}
			if _, ok := s.bestTransferTarget(g, preferred); !ok {
				continue
			}
			s.logger.Info("rebalancing leadership", "shard", int(g.shard), "to", string(preferred))
			ctx, cancel := context.WithTimeout(context.Background(), 4*s.cfg.ElectionTimeout)
			err := g.node.TransferLeadership(ctx, preferred)
			cancel()
			if err != nil {
				s.logger.Warn("rebalance transfer failed", "shard", int(g.shard), "err", err)
			}
			s.m.rebalances.With().Inc()
			break // at most one transfer per tick keeps churn low
		}
	}
}

// expiryLoop proposes no-op commands on shards whose leader is this node
// so the replicated clock advances and expired keys are physically
// reclaimed even when a shard receives no writes.
func (s *Server) expiryLoop() {
	defer s.bgWG.Done()
	t := time.NewTicker(s.cfg.ExpiryInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stopBg:
			return
		case <-t.C:
		}
		now := time.Now().UnixNano()
		for _, g := range s.groups {
			if !g.node.IsLeader() || !g.store.HasExpired(now) {
				continue
			}
			cmd := kv.Command{Op: kv.OpNoop, Now: now}
			ctx, cancel := context.WithTimeout(context.Background(), s.cfg.RequestTimeout)
			g.node.Propose(ctx, cmd.Encode())
			cancel()
		}
	}
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/kv/{key...}", s.instrument("get", s.handleGet))
	mux.HandleFunc("PUT /v1/kv/{key...}", s.instrument("put", s.handlePut))
	mux.HandleFunc("DELETE /v1/kv/{key...}", s.instrument("delete", s.handleDelete))
	mux.HandleFunc("GET /v1/cluster", s.handleCluster)
	mux.HandleFunc("GET /v1/events", s.handleEvents)
	mux.HandleFunc("GET /v1/status", s.handleLocalStatus)
	mux.HandleFunc("POST /v1/admin/shards/{shard}/transfer-leader", s.peerOnly(s.handleTransfer))
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", s.m.reg.Handler())
	raftHandler := transport.NewHandler(func(shard cluster.ShardID) (*raft.Node, bool) {
		g, ok := s.groups[shard]
		if !ok {
			return nil, false
		}
		return g.node, true
	})
	raftHTTP := s.peerOnly(raftHandler.ServeHTTP)
	if s.cfg.EnableFaultInjection {
		raftHTTP = s.faults.inbound(raftHTTP)
		mux.HandleFunc("GET /v1/admin/faults", s.peerOnly(s.handleFaults))
		mux.HandleFunc("POST /v1/admin/faults", s.peerOnly(s.handleFaults))
	}
	mux.Handle(transport.Pattern, raftHTTP)
	if !s.cfg.DisableUI {
		mux.Handle("GET /ui/", http.StripPrefix("/ui/", ui.Handler()))
		mux.Handle("GET /{$}", http.RedirectHandler("/ui/", http.StatusFound))
	}
	if s.cfg.EnablePprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	if s.cfg.CORSOrigin != "" {
		return withCORS(mux, s.cfg.CORSOrigin)
	}
	return mux
}

// withCORS allows a browser dashboard on another origin to use the API.
func withCORS(h http.Handler, origin string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hd := w.Header()
		hd.Set("Access-Control-Allow-Origin", origin)
		hd.Set("Access-Control-Expose-Headers", "X-Distkv-Version, X-Distkv-Shard, X-Distkv-Leader, X-Distkv-Served-By, X-Distkv-Expires-At")
		if r.Method == http.MethodOptions {
			hd.Set("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS")
			hd.Set("Access-Control-Allow-Headers", "Content-Type, X-Distkv-Client-Id, X-Distkv-Seq, X-Distkv-Ack-Below")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		h.ServeHTTP(w, r)
	})
}

// peerOnly restricts a handler to callers holding a certificate signed by
// the cluster CA when mutual TLS is configured. Without a client CA it is
// a no-op (protect such deployments at the network layer).
func (s *Server) peerOnly(h http.HandlerFunc) http.HandlerFunc {
	if s.cfg.TLS == nil || s.cfg.TLS.ClientCAs == nil {
		return h
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 {
			writeError(w, http.StatusForbidden, api.ErrorResponse{
				Error: "this endpoint requires a client certificate signed by the cluster CA", Code: "forbidden"})
			return
		}
		h(w, r)
	}
}

// IsShardLeader reports whether this node currently leads shard.
func (s *Server) IsShardLeader(shard cluster.ShardID) bool {
	g, ok := s.groups[shard]
	return ok && g.node.IsLeader()
}
