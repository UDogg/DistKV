// Package testcluster runs complete DistKV clusters inside one process on
// loopback ports, with real HTTP transport and real storage, for
// integration, linearizability and fault-injection tests.
package testcluster

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/UDogg/DistKV/internal/cluster"
	"github.com/UDogg/DistKV/internal/server"
	"github.com/UDogg/DistKV/pkg/client"
)

// Options configures a test cluster.
type Options struct {
	Nodes             int
	Shards            int
	ReplicationFactor int
	// Persistent uses FileStorage under t.TempDir() (with NoSync for
	// speed); otherwise state is in memory.
	Persistent        bool
	HeartbeatInterval time.Duration
	ElectionTimeout   time.Duration
	SnapshotThreshold uint64
	RebalanceInterval time.Duration
}

// Cluster is a running in-process cluster.
type Cluster struct {
	t       testing.TB
	opts    Options
	spec    cluster.Spec
	dataDir string

	mu      sync.Mutex
	servers map[cluster.NodeID]*server.Server
	addrs   map[cluster.NodeID]string // host:port
	done    map[cluster.NodeID]chan struct{}
}

// Start launches a cluster and waits until every shard has a leader.
func Start(t testing.TB, opts Options) *Cluster {
	t.Helper()
	if opts.Nodes == 0 {
		opts.Nodes = 3
	}
	if opts.Shards == 0 {
		opts.Shards = 8
	}
	if opts.HeartbeatInterval == 0 {
		opts.HeartbeatInterval = 25 * time.Millisecond
	}
	if opts.ElectionTimeout == 0 {
		opts.ElectionTimeout = 250 * time.Millisecond
	}
	c := &Cluster{
		t:       t,
		opts:    opts,
		dataDir: t.TempDir(),
		servers: map[cluster.NodeID]*server.Server{},
		addrs:   map[cluster.NodeID]string{},
		done:    map[cluster.NodeID]chan struct{}{},
	}
	listeners := map[cluster.NodeID]net.Listener{}
	for i := 1; i <= opts.Nodes; i++ {
		id := cluster.NodeID(fmt.Sprintf("n%d", i))
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id] = l
		c.addrs[id] = l.Addr().String()
		c.spec.Members = append(c.spec.Members, cluster.Member{ID: id, Addr: "http://" + l.Addr().String()})
	}
	c.spec.Shards = opts.Shards
	c.spec.ReplicationFactor = opts.ReplicationFactor
	for id, l := range listeners {
		c.launch(id, l)
	}
	t.Cleanup(c.Stop)
	c.WaitHealthy(10 * time.Second)
	return c
}

func (c *Cluster) launch(id cluster.NodeID, l net.Listener) {
	c.t.Helper()
	level := slog.LevelError
	if os.Getenv("DISTKV_TEST_DEBUG") != "" {
		level = slog.LevelInfo
	}
	cfg := server.Config{
		NodeID:            id,
		Spec:              c.spec,
		HeartbeatInterval: c.opts.HeartbeatInterval,
		ElectionTimeout:   c.opts.ElectionTimeout,
		SnapshotThreshold: c.opts.SnapshotThreshold,
		RebalanceInterval: c.opts.RebalanceInterval,
		RequestTimeout:    2 * time.Second,
		Logger:            slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})),
	}
	if c.opts.Persistent {
		cfg.DataDir = filepath.Join(c.dataDir, string(id))
		cfg.NoSync = true
	}
	s, err := server.New(cfg)
	if err != nil {
		c.t.Fatalf("starting %s: %v", id, err)
	}
	done := make(chan struct{})
	c.mu.Lock()
	c.servers[id] = s
	c.done[id] = done
	c.mu.Unlock()
	go func() {
		defer close(done)
		s.Serve(l)
	}()
}

// IDs returns the node IDs.
func (c *Cluster) IDs() []cluster.NodeID {
	var out []cluster.NodeID
	for _, m := range c.spec.Members {
		out = append(out, m.ID)
	}
	return out
}

// Endpoints returns every node's URL.
func (c *Cluster) Endpoints() []string {
	var out []string
	for _, m := range c.spec.Members {
		out = append(out, m.Addr)
	}
	return out
}

// Server returns a running server, or nil.
func (c *Cluster) Server(id cluster.NodeID) *server.Server {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.servers[id]
}

// Client returns a smart client connected to the cluster.
func (c *Cluster) Client() *client.Client {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl, err := client.New(ctx, client.Config{Endpoints: c.Endpoints(), Timeout: 5 * time.Second})
	if err != nil {
		c.t.Fatalf("client: %v", err)
	}
	c.t.Cleanup(cl.Close)
	return cl
}

// Kill crashes a node abruptly (no leadership hand-off).
func (c *Cluster) Kill(id cluster.NodeID) {
	c.mu.Lock()
	s, done := c.servers[id], c.done[id]
	delete(c.servers, id)
	c.mu.Unlock()
	if s == nil {
		return
	}
	s.Kill()
	<-done
}

// Shutdown stops a node gracefully (leadership is handed off first).
func (c *Cluster) Shutdown(id cluster.NodeID) {
	c.mu.Lock()
	s, done := c.servers[id], c.done[id]
	delete(c.servers, id)
	c.mu.Unlock()
	if s == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.Shutdown(ctx)
	<-done
}

// Restart brings a stopped node back on its original address.
func (c *Cluster) Restart(id cluster.NodeID) {
	c.t.Helper()
	var l net.Listener
	var err error
	for i := 0; i < 50; i++ {
		if l, err = net.Listen("tcp", c.addrs[id]); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		c.t.Fatalf("relisten %s: %v", id, err)
	}
	c.launch(id, l)
}

// LeaderOf returns the node currently leading shard, or "".
func (c *Cluster) LeaderOf(shard int) cluster.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, s := range c.servers {
		if s.IsShardLeader(cluster.ShardID(shard)) {
			return id
		}
	}
	return ""
}

// WaitHealthy waits until every shard has a leader among running nodes.
func (c *Cluster) WaitHealthy(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for s := 0; s < c.spec.Shards; s++ {
			if c.LeaderOf(s) == "" {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("cluster did not become healthy within %v", timeout)
}

// Stop shuts down every node.
func (c *Cluster) Stop() {
	for _, id := range c.IDs() {
		c.Kill(id)
	}
}
