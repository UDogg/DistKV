// Package transport carries Raft RPCs between DistKV nodes over HTTP.
//
// All shards (Raft groups) hosted by a node share a single listener and
// connection pool: requests are addressed as
//
//	POST /internal/raft/{shard}/{rpc}
//
// with bodies in a compact binary encoding (see codec.go). HTTP/1.1 keep-alive connections are pooled per
// peer, so steady-state replication involves no connection setup.
package transport

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/UDogg/DistKV/internal/cluster"
	"github.com/UDogg/DistKV/internal/raft"
)

// PathPrefix is the URL prefix for Raft RPCs.
const PathPrefix = "/internal/raft/"

const contentType = "application/x-distkv-raft"

// RPC names.
const (
	rpcVote     = "vote"
	rpcAppend   = "append"
	rpcSnapshot = "snapshot"
	rpcTimeout  = "timeout-now"
)

// Resolver maps a node ID to its base URL.
type Resolver func(id raft.NodeID) (string, bool)

// HTTP is a Raft transport shared by all local shards.
type HTTP struct {
	client  *http.Client
	resolve Resolver
	bufs    sync.Pool
}

// NewHTTPClient returns an http.Client tuned for low-latency intra-cluster
// traffic. tlsCfg may be nil for plain HTTP.
func NewHTTPClient(tlsCfg *tls.Config) *http.Client {
	tr := &http.Transport{
		TLSClientConfig: tlsCfg,
		Proxy:           nil,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        4096,
		MaxIdleConnsPerHost: 1024,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
	}
	return &http.Client{Transport: tr}
}

// NewHTTP returns a transport that resolves peers with resolve. tlsCfg
// (nil for plain HTTP) should carry this node's certificate when peers
// require mutual TLS.
func NewHTTP(resolve Resolver, tlsCfg *tls.Config) *HTTP {
	return &HTTP{
		client:  NewHTTPClient(tlsCfg),
		resolve: resolve,
		bufs:    sync.Pool{New: func() any { b := make([]byte, 0, 4096); return &b }},
	}
}

// ForShard returns a raft.Transport bound to one shard.
func (t *HTTP) ForShard(shard cluster.ShardID) raft.Transport {
	return &shardTransport{t: t, shard: strconv.FormatUint(uint64(shard), 10)}
}

// Close releases pooled connections.
func (t *HTTP) Close() {
	t.client.CloseIdleConnections()
}

type shardTransport struct {
	t     *HTTP
	shard string
}

func (s *shardTransport) RequestVote(ctx context.Context, to raft.NodeID, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	resp := new(raft.RequestVoteResponse)
	return resp, s.t.call(ctx, to, s.shard, rpcVote, req, resp)
}

func (s *shardTransport) AppendEntries(ctx context.Context, to raft.NodeID, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	resp := new(raft.AppendEntriesResponse)
	return resp, s.t.call(ctx, to, s.shard, rpcAppend, req, resp)
}

func (s *shardTransport) InstallSnapshot(ctx context.Context, to raft.NodeID, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	resp := new(raft.InstallSnapshotResponse)
	return resp, s.t.call(ctx, to, s.shard, rpcSnapshot, req, resp)
}

func (s *shardTransport) TimeoutNow(ctx context.Context, to raft.NodeID, req *raft.TimeoutNowRequest) (*raft.TimeoutNowResponse, error) {
	resp := new(raft.TimeoutNowResponse)
	return resp, s.t.call(ctx, to, s.shard, rpcTimeout, req, resp)
}

func (t *HTTP) call(ctx context.Context, to raft.NodeID, shard, rpc string, req, resp any) error {
	base, ok := t.resolve(to)
	if !ok {
		return fmt.Errorf("transport: unknown peer %q", to)
	}
	bp := t.bufs.Get().(*[]byte)
	defer t.bufs.Put(bp)
	body, err := marshal((*bp)[:0], req)
	if err != nil {
		return err
	}
	*bp = body
	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+PathPrefix+shard+"/"+rpc, bytes.NewReader(body))
	if err != nil {
		return err
	}
	hreq.Header.Set("Content-Type", contentType)
	hresp, err := t.client.Do(hreq)
	if err != nil {
		return err
	}
	defer hresp.Body.Close()
	if hresp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(hresp.Body, 512))
		io.Copy(io.Discard, hresp.Body)
		return fmt.Errorf("transport: %s %s/%s: %s: %s", to, shard, rpc, hresp.Status, bytes.TrimSpace(msg))
	}
	data, err := io.ReadAll(hresp.Body)
	if err != nil {
		return err
	}
	if err := unmarshal(data, resp); err != nil {
		return fmt.Errorf("transport: decode %s response: %w", rpc, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Server side

// Lookup returns the local Raft node for a shard.
type Lookup func(shard cluster.ShardID) (*raft.Node, bool)

// Handler serves inbound Raft RPCs.
type Handler struct {
	lookup Lookup
	// MaxBodyBytes bounds request bodies (snapshots can be large).
	MaxBodyBytes int64
}

// NewHandler returns an inbound RPC handler.
func NewHandler(lookup Lookup) *Handler {
	return &Handler{lookup: lookup, MaxBodyBytes: 1 << 30}
}

// ErrNoShard is reported when a peer addresses a shard this node does not host.
var ErrNoShard = errors.New("transport: shard not hosted here")

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	shardStr, rpc := r.PathValue("shard"), r.PathValue("rpc")
	shardNum, err := strconv.ParseUint(shardStr, 10, 32)
	if err != nil {
		http.Error(w, "bad shard", http.StatusBadRequest)
		return
	}
	node, ok := h.lookup(cluster.ShardID(shardNum))
	if !ok {
		http.Error(w, ErrNoShard.Error(), http.StatusNotFound)
		return
	}
	// The body is read into a fresh buffer: decoded entries alias it and
	// are retained by the Raft log.
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.MaxBodyBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resp any
	switch rpc {
	case rpcVote:
		var req raft.RequestVoteRequest
		if err = unmarshal(data, &req); err == nil {
			resp, err = node.HandleRequestVote(&req)
		}
	case rpcAppend:
		var req raft.AppendEntriesRequest
		if err = unmarshal(data, &req); err == nil {
			resp, err = node.HandleAppendEntries(&req)
		}
	case rpcSnapshot:
		var req raft.InstallSnapshotRequest
		if err = unmarshal(data, &req); err == nil {
			resp, err = node.HandleInstallSnapshot(&req)
		}
	case rpcTimeout:
		var req raft.TimeoutNowRequest
		if err = unmarshal(data, &req); err == nil {
			resp, err = node.HandleTimeoutNow(&req)
		}
	default:
		http.Error(w, "unknown rpc", http.StatusNotFound)
		return
	}
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, raft.ErrStopped) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, err.Error(), status)
		return
	}
	out, err := marshal(make([]byte, 0, 64), resp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(out)))
	w.Write(out)
}

// Pattern is the ServeMux pattern the Handler expects to be mounted at.
const Pattern = "POST " + PathPrefix + "{shard}/{rpc}"
