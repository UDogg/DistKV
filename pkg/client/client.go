// Package client is the Go client for DistKV.
//
// The client is topology-aware: it downloads the cluster specification
// once, computes the shard of every key locally (exactly as servers do) and
// sends each request straight to that shard's leader, learning leaders from
// "421 Misdirected Request" hints. Failed requests are retried with
// exponential backoff until the context expires.
//
// Writes are exactly-once by default: every client has a random 64-bit
// session ID and numbers its writes, so a write retried after a leader
// failure is applied at most once even if the first attempt committed.
//
//	c, err := client.New(ctx, client.Config{Endpoints: []string{"http://127.0.0.1:7001"}})
//	ver, err := c.Put(ctx, "user:42", []byte("alice"), client.WithTTL(time.Minute))
//	item, err := c.Get(ctx, "user:42")
package client

import (
	"bytes"
	"context"
	cryptorand "crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/UDogg/DistKV/internal/api"
	"github.com/UDogg/DistKV/internal/cluster"
	"github.com/UDogg/DistKV/internal/transport"
)

var (
	// ErrNotFound is returned when a key does not exist (or has expired).
	ErrNotFound = errors.New("distkv: key not found")
	// ErrConflict is returned (wrapped in *ConflictError) when a
	// compare-and-set precondition fails.
	ErrConflict = errors.New("distkv: version conflict")
	// ErrUnavailable is returned when the request could not be completed
	// before the context expired.
	ErrUnavailable = errors.New("distkv: unavailable")
)

// ConflictError reports the key's actual version after a failed CAS.
type ConflictError struct {
	Key            string
	CurrentVersion uint64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("distkv: version conflict on %q (current version %d)", e.Key, e.CurrentVersion)
}

// Is makes errors.Is(err, ErrConflict) work.
func (e *ConflictError) Is(target error) bool { return target == ErrConflict }

// Config configures a Client.
type Config struct {
	// Endpoints are seed node URLs used to discover the cluster.
	Endpoints []string
	// Timeout applies to operations whose context has no deadline.
	Timeout time.Duration
	// DisableSmartRouting sends every request to the seed endpoints and
	// lets servers forward it. Use when member addresses advertised by the
	// servers are not reachable from the client (e.g. behind NAT).
	DisableSmartRouting bool
	// DisableSessions turns off exactly-once write deduplication. Writes
	// are then only retried when the request certainly was not sent.
	DisableSessions bool
	// TLS configures HTTPS (endpoints must then use https://). Set
	// Certificates for clusters that require client certificates.
	TLS *tls.Config
	// HTTPClient overrides the default tuned client (TLS is then ignored).
	HTTPClient *http.Client
}

// Client is safe for concurrent use.
type Client struct {
	cfg  Config
	http *http.Client
	topo *cluster.Topology

	mu      sync.RWMutex
	leaders map[cluster.ShardID]string // shard -> base URL

	clientID uint64
	sess     sessionTracker
	rr       atomic.Uint64
}

// Item is a value read from the store.
type Item struct {
	Key       string
	Value     []byte
	Version   uint64
	ExpiresAt time.Time // zero if no TTL
	Shard     int
	ServedBy  string
}

// New connects to the cluster and downloads its topology.
func New(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("distkv: no endpoints")
	}
	for i, e := range cfg.Endpoints {
		if !strings.Contains(e, "://") {
			e = "http://" + e
		}
		cfg.Endpoints[i] = strings.TrimRight(e, "/")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	c := &Client{cfg: cfg, http: cfg.HTTPClient, leaders: map[cluster.ShardID]string{}}
	if c.http == nil {
		c.http = transport.NewHTTPClient(cfg.TLS)
	}
	if !cfg.DisableSessions {
		var b [8]byte
		if _, err := cryptorand.Read(b[:]); err != nil {
			return nil, err
		}
		c.clientID = binary.LittleEndian.Uint64(b[:])>>1 | 1
		c.sess.next = 1
		c.sess.inflight = map[uint64]struct{}{}
	}
	st, err := c.Cluster(ctx)
	if err != nil {
		return nil, fmt.Errorf("distkv: discovering cluster: %w", err)
	}
	spec := cluster.Spec{Shards: st.Spec.Shards, ReplicationFactor: st.Spec.ReplicationFactor, VirtualNodes: st.Spec.VirtualNodes}
	for _, m := range st.Spec.Members {
		spec.Members = append(spec.Members, cluster.Member{ID: cluster.NodeID(m.ID), Addr: m.Addr})
	}
	if c.topo, err = cluster.New(spec); err != nil {
		return nil, err
	}
	for _, s := range st.Shards {
		if s.LeaderAddr != "" {
			c.leaders[cluster.ShardID(s.Shard)] = s.LeaderAddr
		}
	}
	return c, nil
}

// Close releases idle connections.
func (c *Client) Close() { c.http.CloseIdleConnections() }

// ShardForKey returns the shard that owns key.
func (c *Client) ShardForKey(key string) int { return int(c.topo.ShardForKey(key)) }

// ReadOption customises Get.
type ReadOption func(*readOpts)

type readOpts struct{ stale bool }

// Stale allows the read to be served by any replica from local state. It is
// faster and available during elections, but may return out-of-date data.
func Stale() ReadOption { return func(o *readOpts) { o.stale = true } }

// WriteOption customises Put.
type WriteOption func(*writeOpts)

type writeOpts struct {
	ttl       time.Duration
	ifVersion *uint64
}

// WithTTL expires the key after d.
func WithTTL(d time.Duration) WriteOption { return func(o *writeOpts) { o.ttl = d } }

// IfVersion makes the write conditional on the key's current version
// (compare-and-set). Version 0 means "only if the key does not exist".
func IfVersion(v uint64) WriteOption { return func(o *writeOpts) { o.ifVersion = &v } }

// Get reads a key. It returns ErrNotFound if the key is absent.
func (c *Client) Get(ctx context.Context, key string, opts ...ReadOption) (*Item, error) {
	var o readOpts
	for _, f := range opts {
		f(&o)
	}
	q := url.Values{}
	if o.stale {
		q.Set(api.QueryConsistency, api.Stale)
	}
	resp, err := c.do(ctx, http.MethodGet, key, q, nil, o.stale)
	if err != nil {
		return nil, err
	}
	if resp.status == http.StatusNotFound {
		return nil, ErrNotFound
	}
	if resp.status != http.StatusOK {
		return nil, resp.err(key)
	}
	it := &Item{Key: key, Value: resp.body, ServedBy: resp.header.Get(api.HeaderServedBy)}
	it.Version, _ = strconv.ParseUint(resp.header.Get(api.HeaderVersion), 10, 64)
	it.Shard, _ = strconv.Atoi(resp.header.Get(api.HeaderShard))
	if v := resp.header.Get(api.HeaderExpiresAt); v != "" {
		it.ExpiresAt, _ = time.Parse(time.RFC3339Nano, v)
	}
	return it, nil
}

// Put writes a key and returns its new version.
func (c *Client) Put(ctx context.Context, key string, value []byte, opts ...WriteOption) (uint64, error) {
	var o writeOpts
	for _, f := range opts {
		f(&o)
	}
	q := url.Values{}
	if o.ttl > 0 {
		q.Set(api.QueryTTL, o.ttl.String())
	}
	if o.ifVersion != nil {
		q.Set(api.QueryIfVersion, strconv.FormatUint(*o.ifVersion, 10))
	}
	if value == nil {
		value = []byte{}
	}
	resp, err := c.do(ctx, http.MethodPut, key, q, value, false)
	if err != nil {
		return 0, err
	}
	switch resp.status {
	case http.StatusOK:
		var wr api.WriteResponse
		if err := json.Unmarshal(resp.body, &wr); err != nil {
			return 0, err
		}
		return wr.Version, nil
	case http.StatusConflict:
		return 0, &ConflictError{Key: key, CurrentVersion: resp.apiErr().CurrentVersion}
	default:
		return 0, resp.err(key)
	}
}

// CompareAndSwap writes value only if the key's version equals expect
// (0 = must not exist).
func (c *Client) CompareAndSwap(ctx context.Context, key string, expect uint64, value []byte, opts ...WriteOption) (uint64, error) {
	return c.Put(ctx, key, value, append(opts, IfVersion(expect))...)
}

// Delete removes a key. It returns ErrNotFound if the key did not exist.
func (c *Client) Delete(ctx context.Context, key string) error {
	resp, err := c.do(ctx, http.MethodDelete, key, nil, nil, false)
	if err != nil {
		return err
	}
	switch resp.status {
	case http.StatusOK:
		return nil
	case http.StatusNotFound:
		return ErrNotFound
	default:
		return resp.err(key)
	}
}

// Cluster returns the aggregated cluster status from any reachable node.
func (c *Client) Cluster(ctx context.Context) (*api.ClusterStatus, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	var lastErr error
	for _, ep := range c.cfg.Endpoints {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, ep+"/v1/cluster", nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		var st api.ClusterStatus
		err = json.NewDecoder(resp.Body).Decode(&st)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		return &st, nil
	}
	return nil, lastErr
}

// ---------------------------------------------------------------------------
// Request execution

type response struct {
	status int
	header http.Header
	body   []byte
}

func (r *response) apiErr() api.ErrorResponse {
	var e api.ErrorResponse
	json.Unmarshal(r.body, &e)
	return e
}

func (r *response) err(key string) error {
	e := r.apiErr()
	if e.Error == "" {
		e.Error = http.StatusText(r.status)
	}
	return fmt.Errorf("distkv: %s %q: %s (%s, HTTP %d)", "request", key, e.Error, e.Code, r.status)
}

func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.cfg.Timeout)
}

func (c *Client) do(ctx context.Context, method, key string, q url.Values, body []byte, anyReplica bool) (*response, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	shard := c.topo.ShardForKey(key)
	write := method != http.MethodGet

	var seq uint64
	if write && c.clientID != 0 {
		seq = c.sess.begin()
		defer c.sess.end(seq)
	}
	path := "/v1/kv/" + escapeKey(key)
	if len(q) > 0 {
		path += "?" + q.Encode()
	}

	var lastErr error
	redirects := 0
	for attempt := 0; ; attempt++ {
		base := c.pickTarget(shard, attempt, anyReplica)
		var rd io.Reader
		if body != nil {
			rd = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, base+path, rd)
		if err != nil {
			return nil, err
		}
		if !c.cfg.DisableSmartRouting {
			req.Header.Set(api.HeaderNoForward, "1")
		}
		if seq != 0 {
			req.Header.Set(api.HeaderClientID, strconv.FormatUint(c.clientID, 10))
			req.Header.Set(api.HeaderSeq, strconv.FormatUint(seq, 10))
			req.Header.Set(api.HeaderAckBelow, strconv.FormatUint(c.sess.ackBelow(), 10))
		}

		hresp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			c.forgetLeader(shard, base)
			if ctx.Err() != nil {
				break
			}
			// Without sessions, a write that may have reached the server
			// cannot be retried safely.
			if write && seq == 0 && !isDialError(err) {
				return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
			}
			if !sleep(ctx, backoff(attempt)) {
				break
			}
			continue
		}
		data, err := io.ReadAll(hresp.Body)
		hresp.Body.Close()
		if err != nil {
			lastErr = err
			if !sleep(ctx, backoff(attempt)) {
				break
			}
			continue
		}
		r := &response{status: hresp.StatusCode, header: hresp.Header, body: data}

		switch r.status {
		case http.StatusMisdirectedRequest:
			e := r.apiErr()
			if e.LeaderAddr != "" && e.LeaderAddr != base {
				c.setLeader(shard, e.LeaderAddr)
				if redirects++; redirects <= 3 {
					attempt--
					continue // follow the hint immediately
				}
				lastErr = errors.New(e.Error)
				break
			}
			c.forgetLeader(shard, base)
			lastErr = errors.New(e.Error)
		case http.StatusServiceUnavailable, http.StatusBadGateway, http.StatusGatewayTimeout:
			e := r.apiErr()
			lastErr = fmt.Errorf("%s (%s)", e.Error, e.Code)
			// This node cannot serve the shard right now (it may be
			// partitioned from its peers): probe the other replicas.
			if e.Code == api.CodeShuttingDown || e.Code == api.CodeUnavailable || e.Code == api.CodeNoLeader {
				c.forgetLeader(shard, base)
			}
			if write && seq == 0 && e.Code != api.CodeNoLeader && e.Code != api.CodeShuttingDown {
				return r, nil // ambiguous outcome without sessions: surface it
			}
		default:
			if !anyReplica && r.status < 500 {
				c.setLeader(shard, base)
			}
			return r, nil
		}
		if !sleep(ctx, backoff(attempt)) {
			break
		}
	}
	if lastErr == nil {
		lastErr = ctx.Err()
	}
	return nil, fmt.Errorf("%w: %v", ErrUnavailable, lastErr)
}

// pickTarget chooses the node to contact for shard.
func (c *Client) pickTarget(shard cluster.ShardID, attempt int, anyReplica bool) string {
	if c.cfg.DisableSmartRouting {
		eps := c.cfg.Endpoints
		return eps[(int(c.rr.Add(1))+attempt)%len(eps)]
	}
	reps := c.topo.Replicas(shard)
	if anyReplica {
		m, _ := c.topo.Member(reps[int(c.rr.Add(1))%len(reps)])
		return m.Addr
	}
	c.mu.RLock()
	leader, ok := c.leaders[shard]
	c.mu.RUnlock()
	if ok {
		return leader
	}
	// Unknown leader: probe replicas, starting with the preferred one.
	m, _ := c.topo.Member(reps[attempt%len(reps)])
	return m.Addr
}

func (c *Client) setLeader(shard cluster.ShardID, addr string) {
	c.mu.RLock()
	cur := c.leaders[shard]
	c.mu.RUnlock()
	if cur == addr {
		return
	}
	c.mu.Lock()
	c.leaders[shard] = addr
	c.mu.Unlock()
}

func (c *Client) forgetLeader(shard cluster.ShardID, addr string) {
	c.mu.Lock()
	if c.leaders[shard] == addr {
		delete(c.leaders, shard)
	}
	c.mu.Unlock()
}

func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

func backoff(attempt int) time.Duration {
	d := 5 * time.Millisecond << min(attempt, 6) // 5ms .. 320ms
	return d/2 + time.Duration(rand.Int63n(int64(d)))
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// ---------------------------------------------------------------------------
// Session sequence tracking

// sessionTracker hands out write sequence numbers and computes the
// acknowledgement watermark: every sequence below it has completed.
type sessionTracker struct {
	mu       sync.Mutex
	next     uint64
	inflight map[uint64]struct{}
}

func (s *sessionTracker) begin() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.next
	s.next++
	s.inflight[seq] = struct{}{}
	return seq
}

func (s *sessionTracker) end(seq uint64) {
	s.mu.Lock()
	delete(s.inflight, seq)
	s.mu.Unlock()
}

func (s *sessionTracker) ackBelow() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	low := s.next
	for seq := range s.inflight {
		low = min(low, seq)
	}
	return low
}
