package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/UDogg/DistKV/internal/api"
	"github.com/UDogg/DistKV/internal/cluster"
	"github.com/UDogg/DistKV/internal/kv"
	"github.com/UDogg/DistKV/internal/raft"
)

// maxHops bounds server-side forwarding (follower -> leader -> stop).
const maxHops = 2

// handleGet serves GET /v1/kv/{key}.
//
// Linearizable reads (the default) are served by the shard leader once it
// has confirmed its leadership, either via its quorum lease or a ReadIndex
// heartbeat round; stale reads are served by any replica from local state.
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key, ok := s.keyParam(w, r)
	if !ok {
		return
	}
	shard := s.topo.ShardForKey(key)
	g := s.groups[shard]
	stale := r.URL.Query().Get(api.QueryConsistency) == api.Stale

	if stale {
		if g == nil {
			s.routeElsewhere(w, r, shard, nil, nil)
			return
		}
		s.writeItem(w, g, key)
		return
	}
	if g == nil || !g.node.IsLeader() {
		s.routeElsewhere(w, r, shard, g, nil)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	read := g.node.LeaseRead
	if s.cfg.ReadMode == ReadModeReadIndex {
		read = g.node.LinearizableRead
	}
	if err := read(ctx); err != nil {
		s.raftError(w, r, shard, g, nil, err)
		return
	}
	s.writeItem(w, g, key)
}

func (s *Server) writeItem(w http.ResponseWriter, g *group, key string) {
	s.setRoutingHeaders(w, g.shard, g)
	item, ok := g.store.Get(key, time.Now().UnixNano())
	if !ok {
		writeError(w, http.StatusNotFound, api.ErrorResponse{Error: "key not found", Code: api.CodeNotFound})
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set(api.HeaderVersion, strconv.FormatUint(item.Version, 10))
	if item.ExpireAt != 0 {
		h.Set(api.HeaderExpiresAt, time.Unix(0, item.ExpireAt).UTC().Format(time.RFC3339Nano))
	}
	h.Set("Content-Length", strconv.Itoa(len(item.Value)))
	w.WriteHeader(http.StatusOK)
	w.Write(item.Value)
}

// handlePut serves PUT /v1/kv/{key}?ttl=30s&if_version=N.
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key, ok := s.keyParam(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxValueBytes))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, api.ErrorResponse{Error: "value too large", Code: api.CodeTooLarge})
			return
		}
		writeError(w, http.StatusBadRequest, api.ErrorResponse{Error: "reading body: " + err.Error(), Code: api.CodeBadRequest})
		return
	}
	cmd := kv.Command{Op: kv.OpPut, Key: key, Value: body}
	q := r.URL.Query()
	if v := q.Get(api.QueryTTL); v != "" {
		ttl, err := time.ParseDuration(v)
		if err != nil || ttl <= 0 {
			writeError(w, http.StatusBadRequest, api.ErrorResponse{Error: "invalid ttl", Code: api.CodeBadRequest})
			return
		}
		cmd.ExpireAt = time.Now().Add(ttl).UnixNano()
	}
	if v := q.Get(api.QueryIfVersion); v != "" {
		ver, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, api.ErrorResponse{Error: "invalid if_version", Code: api.CodeBadRequest})
			return
		}
		cmd.Op = kv.OpCAS
		cmd.ExpectVersion = ver
	}
	s.propose(w, r, key, &cmd, body)
}

// handleDelete serves DELETE /v1/kv/{key}.
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key, ok := s.keyParam(w, r)
	if !ok {
		return
	}
	s.propose(w, r, key, &kv.Command{Op: kv.OpDelete, Key: key}, nil)
}

func (s *Server) propose(w http.ResponseWriter, r *http.Request, key string, cmd *kv.Command, body []byte) {
	shard := s.topo.ShardForKey(key)
	g := s.groups[shard]
	if g == nil || !g.node.IsLeader() {
		s.routeElsewhere(w, r, shard, g, body)
		return
	}
	if err := readSession(r, cmd); err != nil {
		writeError(w, http.StatusBadRequest, api.ErrorResponse{Error: err.Error(), Code: api.CodeBadRequest})
		return
	}
	cmd.Now = time.Now().UnixNano()
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	out, err := g.node.Propose(ctx, cmd.Encode())
	if err != nil {
		s.raftError(w, r, shard, g, body, err)
		return
	}
	res := out.(kv.Result)
	s.setRoutingHeaders(w, shard, g)
	switch res.Code {
	case kv.CodeOK:
		writeJSON(w, http.StatusOK, api.WriteResponse{Key: key, Version: res.Version, Shard: int(shard), Deleted: cmd.Op == kv.OpDelete})
	case kv.CodeNotFound:
		writeError(w, http.StatusNotFound, api.ErrorResponse{Error: "key not found", Code: api.CodeNotFound})
	case kv.CodeConflict:
		writeError(w, http.StatusConflict, api.ErrorResponse{Error: "version mismatch", Code: api.CodeConflict, CurrentVersion: res.Version})
	case kv.CodeStale:
		writeError(w, http.StatusBadRequest, api.ErrorResponse{Error: "request sequence already acknowledged", Code: api.CodeStale})
	default:
		writeError(w, http.StatusInternalServerError, api.ErrorResponse{Error: "command rejected: " + res.Code.String(), Code: api.CodeInternal})
	}
}

func readSession(r *http.Request, cmd *kv.Command) error {
	h := r.Header
	if v := h.Get(api.HeaderClientID); v != "" {
		id, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return errors.New("invalid client id header")
		}
		seq, err := strconv.ParseUint(h.Get(api.HeaderSeq), 10, 64)
		if err != nil || seq == 0 {
			return errors.New("invalid or missing sequence header")
		}
		ack, _ := strconv.ParseUint(h.Get(api.HeaderAckBelow), 10, 64)
		cmd.ClientID, cmd.Seq, cmd.AckBelow = id, seq, ack
	}
	return nil
}

// raftError maps consensus-layer failures to HTTP responses.
func (s *Server) raftError(w http.ResponseWriter, r *http.Request, shard cluster.ShardID, g *group, body []byte, err error) {
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		s.routeElsewhere(w, r, shard, g, body)
	case errors.Is(err, raft.ErrLeadershipLost):
		s.setRoutingHeaders(w, shard, g)
		writeError(w, http.StatusServiceUnavailable, api.ErrorResponse{
			Error: "leader changed before the write committed; retry with the same sequence number", Code: api.CodeOutcomeUnsure})
	case errors.Is(err, raft.ErrStopped):
		writeError(w, http.StatusServiceUnavailable, api.ErrorResponse{Error: "node shutting down", Code: api.CodeShuttingDown})
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, raft.ErrTimeout):
		writeError(w, http.StatusGatewayTimeout, api.ErrorResponse{Error: "timed out waiting for quorum", Code: api.CodeTimeout})
	case errors.Is(err, context.Canceled):
		// Client went away; nothing useful to send.
		w.WriteHeader(499)
	default:
		writeError(w, http.StatusInternalServerError, api.ErrorResponse{Error: err.Error(), Code: api.CodeInternal})
	}
}

// routeElsewhere handles a request this node cannot serve: either it
// answers 421 with a leader hint (smart clients) or it proxies the request
// to the node most likely to be the shard leader.
func (s *Server) routeElsewhere(w http.ResponseWriter, r *http.Request, shard cluster.ShardID, g *group, body []byte) {
	leader := s.leaderFor(shard, g)
	hops, _ := strconv.Atoi(r.Header.Get(api.HeaderHops))
	shardNum := int(shard)

	if r.Header.Get(api.HeaderNoForward) != "" || hops >= maxHops {
		resp := api.ErrorResponse{Error: "not the leader for this shard", Code: api.CodeNotLeader, Shard: &shardNum}
		status := http.StatusMisdirectedRequest
		if leader != "" && leader != s.cfg.NodeID {
			resp.Leader = string(leader)
			if m, ok := s.topo.Member(leader); ok {
				resp.LeaderAddr = m.Addr
			}
		} else if g != nil {
			// We host the shard and know of no other leader: an
			// election is in progress.
			resp.Code, resp.Error = api.CodeNoLeader, "shard has no leader (election in progress)"
			status = http.StatusServiceUnavailable
			w.Header().Set("Retry-After", "1")
		}
		s.setRoutingHeaders(w, shard, g)
		writeError(w, status, resp)
		return
	}

	var targets []cluster.NodeID
	if leader != "" && leader != s.cfg.NodeID {
		targets = append(targets, leader)
	} else if g != nil {
		writeError(w, http.StatusServiceUnavailable, api.ErrorResponse{
			Error: "shard has no leader (election in progress)", Code: api.CodeNoLeader, Shard: &shardNum})
		return
	}
	for _, rep := range s.topo.Replicas(shard) {
		if rep != s.cfg.NodeID && rep != leader {
			targets = append(targets, rep)
		}
	}
	for _, target := range targets {
		if s.forward(w, r, shard, target, body, hops) {
			return
		}
	}
	writeError(w, http.StatusServiceUnavailable, api.ErrorResponse{
		Error: "no replica of the shard is reachable", Code: api.CodeUnavailable, Shard: &shardNum})
}

// forward proxies r to target. It returns false only if the request
// certainly did not reach the target (connection failure), in which case
// the caller may try another replica.
func (s *Server) forward(w http.ResponseWriter, r *http.Request, shard cluster.ShardID, target cluster.NodeID, body []byte, hops int) bool {
	m, ok := s.topo.Member(target)
	if !ok {
		return false
	}
	var rd io.Reader = http.NoBody
	if body != nil {
		rd = bytes.NewReader(body)
	}
	if s.cfg.EnableFaultInjection && s.faults.isolated.Load() {
		return false // partitioned: peers are unreachable
	}
	if hops == 0 {
		s.live.forwarded.Add(1)
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.RequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, r.Method, m.Addr+r.URL.RequestURI(), rd)
	if err != nil {
		return false
	}
	for _, h := range []string{api.HeaderClientID, api.HeaderSeq, api.HeaderAckBelow, "Content-Type"} {
		if v := r.Header.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	req.Header.Set(api.HeaderHops, strconv.Itoa(hops+1))
	s.m.forwarded.With(r.Method).Inc()
	resp, err := s.client.Do(req)
	if err != nil {
		if isDialError(err) {
			return false
		}
		writeError(w, http.StatusBadGateway, api.ErrorResponse{Error: "forwarding to " + string(target) + ": " + err.Error(), Code: api.CodeUnavailable})
		return true
	}
	defer resp.Body.Close()
	if l := resp.Header.Get(api.HeaderLeader); l != "" {
		s.rememberLeader(shard, cluster.NodeID(l))
	}
	for k, vs := range resp.Header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	return true
}

// isDialError reports whether err happened while establishing the
// connection, i.e. the request was never sent.
func isDialError(err error) bool {
	var oe *net.OpError
	return errors.As(err, &oe) && oe.Op == "dial"
}

func (s *Server) leaderFor(shard cluster.ShardID, g *group) cluster.NodeID {
	if g != nil {
		return cluster.NodeID(g.node.Leader())
	}
	s.hintsMu.RLock()
	defer s.hintsMu.RUnlock()
	return s.leaderHints[shard]
}

func (s *Server) rememberLeader(shard cluster.ShardID, leader cluster.NodeID) {
	if _, local := s.groups[shard]; local {
		return
	}
	s.hintsMu.Lock()
	s.leaderHints[shard] = leader
	s.hintsMu.Unlock()
}

func (s *Server) setRoutingHeaders(w http.ResponseWriter, shard cluster.ShardID, g *group) {
	h := w.Header()
	h.Set(api.HeaderShard, strconv.Itoa(int(shard)))
	h.Set(api.HeaderServedBy, string(s.cfg.NodeID))
	if l := s.leaderFor(shard, g); l != "" {
		h.Set(api.HeaderLeader, string(l))
	}
}

func (s *Server) keyParam(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, api.ErrorResponse{Error: "empty key", Code: api.CodeBadRequest})
		return "", false
	}
	if len(key) > s.cfg.MaxKeyBytes {
		writeError(w, http.StatusBadRequest, api.ErrorResponse{Error: "key too long", Code: api.CodeBadRequest})
		return "", false
	}
	return key, true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, e api.ErrorResponse) {
	writeJSON(w, status, e)
}
