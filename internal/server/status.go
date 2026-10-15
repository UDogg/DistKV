package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/UDogg/DistKV/internal/api"
	"github.com/UDogg/DistKV/internal/cluster"
	"github.com/UDogg/DistKV/internal/metrics"
	"github.com/UDogg/DistKV/internal/raft"
	"github.com/UDogg/DistKV/internal/version"
)

var processStart = time.Now()

// localStatus describes every shard hosted on this node.
func (s *Server) localStatus() api.NodeStatus {
	st := api.NodeStatus{
		Node:        string(s.cfg.NodeID),
		Addr:        s.self.Addr,
		Version:     version.Version,
		Fingerprint: s.topo.Fingerprint(),
		Ready:       s.ready(),
		UptimeSec:   time.Since(processStart).Seconds(),
		Load:        s.live.snapshot(),
	}
	if s.cfg.EnableFaultInjection {
		f := s.faults.get()
		st.Faults = &f
	}
	for _, g := range s.sortedGroups() {
		rs := g.node.Status()
		ks := g.store.Stats()
		gs := api.GroupStatus{
			Shard:         int(g.shard),
			Replicas:      nodeStrings(g.replicas),
			State:         rs.State.String(),
			Term:          rs.Term,
			Leader:        string(rs.Leader),
			CommitIndex:   rs.CommitIndex,
			AppliedIndex:  rs.AppliedIndex,
			LastIndex:     rs.LastIndex,
			SnapshotIndex: rs.SnapshotIndex,
			Keys:          ks.Keys,
			Bytes:         ks.Bytes,
		}
		if rs.State == raft.Leader {
			gs.Followers = map[string]api.Follow{}
			for id, p := range rs.Peers {
				gs.Followers[string(id)] = api.Follow{
					Match:         p.Match,
					Lag:           rs.LastIndex - min(p.Match, rs.LastIndex),
					LastContactMS: float64(p.LastContact.Microseconds()) / 1000,
				}
			}
		}
		st.Groups = append(st.Groups, gs)
	}
	return st
}

func (s *Server) handleLocalStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.localStatus())
}

// handleCluster aggregates every node's status into a cluster-wide view:
// which node leads each shard and whether each shard has a healthy leader.
func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.clusterStatus(r.Context()))
}

func (s *Server) clusterStatus(ctx context.Context) api.ClusterStatus {
	members := s.topo.Members()
	statuses := make([]*api.NodeStatus, len(members))
	var wg sync.WaitGroup
	for i, m := range members {
		if m.ID == s.cfg.NodeID {
			ls := s.localStatus()
			statuses[i] = &ls
			continue
		}
		wg.Add(1)
		go func(i int, m cluster.Member) {
			defer wg.Done()
			if s.cfg.EnableFaultInjection && s.faults.isolated.Load() {
				return // partitioned from peers
			}
			ctx, cancel := context.WithTimeout(ctx, 750*time.Millisecond)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, m.Addr+"/v1/status", nil)
			resp, err := s.client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			var ns api.NodeStatus
			if resp.StatusCode == http.StatusOK && json.NewDecoder(resp.Body).Decode(&ns) == nil {
				statuses[i] = &ns
			}
		}(i, m)
	}
	wg.Wait()

	spec := s.topo.Spec()
	out := api.ClusterStatus{
		Spec: api.Spec{
			Shards:            spec.Shards,
			ReplicationFactor: spec.ReplicationFactor,
			VirtualNodes:      spec.VirtualNodes,
		},
		Fingerprint: s.topo.Fingerprint(),
		Timestamp:   time.Now().UnixMilli(),
		ObservedBy:  string(s.cfg.NodeID),
	}
	for _, m := range members {
		out.Spec.Members = append(out.Spec.Members, api.Member{ID: string(m.ID), Addr: m.Addr})
	}

	type best struct {
		term   uint64
		leader string
		commit uint64
		keys   int
		lag    map[string]uint64
	}
	shards := make([]best, spec.Shards)
	for i, m := range members {
		ns := api.NodeSummary{ID: string(m.ID), Addr: m.Addr, ShardsHeld: len(s.topo.ShardsOn(m.ID))}
		if st := statuses[i]; st != nil {
			ns.Reachable, ns.Ready, ns.Version, ns.UptimeSec = true, st.Ready, st.Version, st.UptimeSec
			load := st.Load
			ns.Load, ns.Faults = &load, st.Faults
			out.Load.ReadsPerSec += load.ReadsPerSec
			out.Load.WritesPerSec += load.WritesPerSec
			out.Load.ErrorsPerSec += load.ErrorsPerSec
			out.Load.ForwardedPerSec += load.ForwardedPerSec
			out.Load.P50Ms = max(out.Load.P50Ms, load.P50Ms)
			out.Load.P99Ms = max(out.Load.P99Ms, load.P99Ms)
			out.Load.P999Ms = max(out.Load.P999Ms, load.P999Ms)
			for _, g := range st.Groups {
				if g.State == raft.Leader.String() {
					ns.ShardsLed++
					if g.Term >= shards[g.Shard].term {
						lag := map[string]uint64{}
						for f, fs := range g.Followers {
							lag[f] = fs.Lag
						}
						shards[g.Shard] = best{term: g.Term, leader: st.Node, commit: g.CommitIndex, keys: g.Keys, lag: lag}
					}
				}
			}
		}
		out.Nodes = append(out.Nodes, ns)
	}
	for i, b := range shards {
		sum := api.ShardSummary{
			Shard:       i,
			Replicas:    nodeStrings(s.topo.Replicas(cluster.ShardID(i))),
			Leader:      b.leader,
			Term:        b.term,
			CommitIndex: b.commit,
			Keys:        b.keys,
			Healthy:     b.leader != "",
			Lag:         b.lag,
		}
		if m, ok := s.topo.Member(cluster.NodeID(b.leader)); ok {
			sum.LeaderAddr = m.Addr
		}
		if sum.Healthy {
			out.HealthyShards++
		}
		out.Shards = append(out.Shards, sum)
	}
	return out
}

// handleEvents streams cluster snapshots as Server-Sent Events
// (GET /v1/events?interval=1s). The dashboard subscribes to it.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	interval := time.Second
	if v := r.URL.Query().Get("interval"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 250*time.Millisecond {
			interval = d
		}
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	// If this node dies, browsers reconnect (to another node, via a proxy or
	// a load balancer) after 500 ms instead of the EventSource default of ~3 s.
	fmt.Fprint(w, "retry: 500\n\n")
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		data, _ := json.Marshal(s.clusterStatus(r.Context()))
		if _, err := fmt.Fprintf(w, "event: cluster\ndata: %s\n\n", data); err != nil {
			return
		}
		flusher.Flush()
		select {
		case <-r.Context().Done():
			return
		case <-s.stopBg:
			return
		case <-t.C:
		}
	}
}

// handleTransfer serves POST /v1/admin/shards/{shard}/transfer-leader?to=ID.
func (s *Server) handleTransfer(w http.ResponseWriter, r *http.Request) {
	n, err := strconv.ParseUint(r.PathValue("shard"), 10, 32)
	g, ok := s.groups[cluster.ShardID(n)]
	if err != nil || !ok {
		writeError(w, http.StatusNotFound, api.ErrorResponse{Error: "shard not hosted on this node", Code: api.CodeNotFound})
		return
	}
	target := raft.NodeID(r.URL.Query().Get("to"))
	if target == "" {
		var ok bool
		if target, ok = s.bestTransferTarget(g, ""); !ok {
			writeError(w, http.StatusServiceUnavailable, api.ErrorResponse{Error: "no healthy transfer target", Code: api.CodeUnavailable})
			return
		}
	}
	if err := g.node.TransferLeadership(r.Context(), target); err != nil {
		code := http.StatusServiceUnavailable
		if err == raft.ErrNotLeader {
			code = http.StatusMisdirectedRequest
		}
		writeError(w, code, api.ErrorResponse{Error: err.Error(), Code: api.CodeUnavailable})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"shard": n, "leader": string(target)})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "node": string(s.cfg.NodeID)})
}

// ready reports whether every locally hosted shard has a known leader.
func (s *Server) ready() bool {
	if s.draining.Load() {
		return false
	}
	for _, g := range s.groups {
		if g.node.Leader() == "" {
			return false
		}
	}
	return true
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if !s.ready() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) sortedGroups() []*group {
	out := make([]*group, 0, len(s.groups))
	for _, g := range s.groups {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].shard < out[j].shard })
	return out
}

func nodeStrings(ids []cluster.NodeID) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = string(id)
	}
	return out
}

// ---------------------------------------------------------------------------
// Metrics

type serverMetrics struct {
	reg        *metrics.Registry
	requests   *metrics.CounterVec
	latency    *metrics.HistogramVec
	forwarded  *metrics.CounterVec
	rebalances *metrics.CounterVec
}

func newServerMetrics(s *Server) *serverMetrics {
	reg := metrics.NewRegistry()
	m := &serverMetrics{
		reg:        reg,
		requests:   reg.NewCounterVec("distkv_requests_total", "Client requests by operation and HTTP status.", "op", "status"),
		latency:    reg.NewHistogramVec("distkv_request_duration_seconds", "Client request latency.", nil, "op"),
		forwarded:  reg.NewCounterVec("distkv_forwarded_requests_total", "Requests proxied to another node.", "method"),
		rebalances: reg.NewCounterVec("distkv_leader_rebalances_total", "Leadership transfers initiated by the balancer."),
	}
	reg.NewGaugeVec("distkv_build_info", "Build information.", "version", "commit").With(version.Version, version.Commit).Set(1)
	reg.RegisterCollector(func(w *metrics.Writer) {
		node := string(s.cfg.NodeID)
		w.Gauge("distkv_ready", "Whether every local shard has a leader.", b2f(s.ready()), "node", node)
		for _, g := range s.sortedGroups() {
			shard := strconv.Itoa(int(g.shard))
			rs := g.node.Status()
			ks := g.store.Stats()
			w.Gauge("distkv_raft_term", "Current Raft term.", float64(rs.Term), "shard", shard)
			w.Gauge("distkv_raft_is_leader", "1 if this node leads the shard.", b2f(rs.State == raft.Leader), "shard", shard)
			w.Gauge("distkv_raft_has_leader", "1 if the shard has a known leader.", b2f(rs.Leader != ""), "shard", shard)
			w.Gauge("distkv_raft_commit_index", "Raft commit index.", float64(rs.CommitIndex), "shard", shard)
			w.Gauge("distkv_raft_applied_index", "Raft applied index.", float64(rs.AppliedIndex), "shard", shard)
			w.Gauge("distkv_raft_last_index", "Last Raft log index.", float64(rs.LastIndex), "shard", shard)
			w.Gauge("distkv_raft_snapshot_index", "Index of the latest snapshot.", float64(rs.SnapshotIndex), "shard", shard)
			w.Counter("distkv_raft_elections_total", "Elections started by this node.", float64(rs.Stats.ElectionsStarted), "shard", shard)
			w.Counter("distkv_raft_leaderships_total", "Times this node became leader.", float64(rs.Stats.LeadershipsWon), "shard", shard)
			w.Counter("distkv_raft_snapshots_taken_total", "Snapshots taken locally.", float64(rs.Stats.SnapshotsTaken), "shard", shard)
			w.Counter("distkv_raft_snapshots_installed_total", "Snapshots received from a leader.", float64(rs.Stats.SnapshotsInstalled), "shard", shard)
			w.Counter("distkv_raft_read_index_rounds_total", "ReadIndex heartbeat rounds.", float64(rs.Stats.ReadIndexRounds), "shard", shard)
			w.Counter("distkv_raft_lease_reads_total", "Reads served under the leader lease.", float64(rs.Stats.LeaseReads), "shard", shard)
			for id, p := range rs.Peers {
				w.Gauge("distkv_raft_follower_lag_entries", "Entries a follower is behind the leader.",
					float64(rs.LastIndex-min(p.Match, rs.LastIndex)), "shard", shard, "follower", string(id))
			}
			w.Gauge("distkv_kv_keys", "Live keys in the shard.", float64(ks.Keys), "shard", shard)
			w.Gauge("distkv_kv_bytes", "Approximate bytes used by the shard.", float64(ks.Bytes), "shard", shard)
			w.Counter("distkv_kv_evictions_total", "Keys evicted by capacity limits.", float64(ks.Evictions), "shard", shard)
			w.Counter("distkv_kv_expirations_total", "Keys removed by TTL expiry.", float64(ks.Expirations), "shard", shard)
			w.Counter("distkv_kv_dedup_hits_total", "Retried writes answered from the session cache.", float64(ks.DedupHits), "shard", shard)
		}
	})
	return m
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *Server) instrument(op string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		h(rec, r)
		d := time.Since(start)
		s.m.latency.With(op).Observe(d.Seconds())
		s.m.requests.With(op, strconv.Itoa(rec.status)).Inc()
		if r.Header.Get(api.HeaderHops) == "" { // count requests where they entered the cluster
			s.live.record(op == "get", d, rec.status)
		}
	}
}
