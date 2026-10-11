package raft

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// In-memory network with fault injection

var errUnreachable = errors.New("network: unreachable")

type network struct {
	mu          sync.Mutex
	nodes       map[NodeID]*Node
	cut         map[[2]NodeID]bool // directed links that drop traffic
	unreliable  bool
	rng         *rand.Rand
	rpcCount    int
	longReorder bool
}

func newNetwork() *network {
	return &network{
		nodes: map[NodeID]*Node{},
		cut:   map[[2]NodeID]bool{},
		rng:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (nw *network) linkUp(from, to NodeID) bool {
	return !nw.cut[[2]NodeID{from, to}]
}

// deliver simulates one RPC: request leg, handler, response leg.
func deliver[Req any, Resp any](nw *network, ctx context.Context, from, to NodeID, req Req,
	handle func(*Node, Req) (Resp, error)) (Resp, error) {
	var zero Resp
	nw.mu.Lock()
	nw.rpcCount++
	target := nw.nodes[to]
	up := nw.linkUp(from, to)
	unreliable := nw.unreliable
	var delay time.Duration
	drop := false
	if unreliable {
		delay = time.Duration(nw.rng.Intn(27)) * time.Millisecond
		drop = nw.rng.Intn(10) == 0
	}
	nw.mu.Unlock()

	if target == nil || !up || drop {
		// Simulate a timeout-ish failure without blocking forever.
		select {
		case <-time.After(time.Duration(rand.Intn(10)) * time.Millisecond):
		case <-ctx.Done():
		}
		return zero, errUnreachable
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
	resp, err := handle(target, req)
	if err != nil {
		return zero, err
	}
	nw.mu.Lock()
	up = nw.linkUp(to, from) && nw.nodes[to] == target
	drop = nw.unreliable && nw.rng.Intn(10) == 0
	nw.mu.Unlock()
	if !up || drop {
		return zero, errUnreachable
	}
	return resp, nil
}

type memTransport struct {
	nw *network
	id NodeID
}

// roundTrip deep-copies messages through gob so that tests catch any
// accidental sharing of state between nodes, as a real network would.
func roundTrip[T any](v T) T {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		panic(err)
	}
	var out T
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		panic(err)
	}
	return out
}

func (t *memTransport) RequestVote(ctx context.Context, to NodeID, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	return deliver(t.nw, ctx, t.id, to, roundTrip(req), (*Node).HandleRequestVote)
}

func (t *memTransport) AppendEntries(ctx context.Context, to NodeID, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return deliver(t.nw, ctx, t.id, to, roundTrip(req), (*Node).HandleAppendEntries)
}

func (t *memTransport) InstallSnapshot(ctx context.Context, to NodeID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	return deliver(t.nw, ctx, t.id, to, roundTrip(req), (*Node).HandleInstallSnapshot)
}

func (t *memTransport) TimeoutNow(ctx context.Context, to NodeID, req *TimeoutNowRequest) (*TimeoutNowResponse, error) {
	return deliver(t.nw, ctx, t.id, to, roundTrip(req), (*Node).HandleTimeoutNow)
}

// ---------------------------------------------------------------------------
// Test state machine that cross-checks every applied entry

type testFSM struct {
	c       *cluster
	id      NodeID
	mu      sync.Mutex
	applied []string // applied[i] is the command at log index lastIncluded+i+1
	base    uint64   // index covered by the last restored snapshot
	last    uint64
}

type fsmSnapshot struct {
	Applied []string
	Base    uint64
	Last    uint64
}

func (f *testFSM) Apply(e Entry) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Indices may skip: leader no-op entries are not delivered.
	if e.Index <= f.last {
		f.c.fail(fmt.Errorf("node %s applied index %d after %d (out of order)", f.id, e.Index, f.last))
	}
	f.last = e.Index
	cmd := string(e.Data)
	f.applied = append(f.applied, cmd)
	f.c.recordApply(f.id, e.Index, cmd)
	return cmd
}

func (f *testFSM) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(fsmSnapshot{Applied: f.applied, Base: f.base, Last: f.last})
	return buf.Bytes(), err
}

func (f *testFSM) Restore(data []byte) error {
	var s fsmSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&s); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applied, f.base, f.last = s.Applied, s.Base, s.Last
	return nil
}

func (f *testFSM) lastApplied() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.last
}

// ---------------------------------------------------------------------------
// Cluster harness

type clusterOpts struct {
	snapshotThreshold uint64
	snapshotTrailing  uint64
	disablePreVote    bool
}

type cluster struct {
	t        *testing.T
	nw       *network
	ids      []NodeID
	opts     clusterOpts
	storages map[NodeID]*MemoryStorage
	fsms     map[NodeID]*testFSM

	mu        sync.Mutex
	committed map[uint64]string // global index -> command, for safety checks
	errs      []error
}

func newCluster(t *testing.T, n int, opts clusterOpts) *cluster {
	t.Helper()
	c := &cluster{
		t:         t,
		nw:        newNetwork(),
		opts:      opts,
		storages:  map[NodeID]*MemoryStorage{},
		fsms:      map[NodeID]*testFSM{},
		committed: map[uint64]string{},
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, NodeID(fmt.Sprintf("n%d", i)))
	}
	for _, id := range c.ids {
		c.storages[id] = NewMemoryStorage()
		c.start(id)
	}
	t.Cleanup(c.shutdown)
	return c
}

func testLogger() *slog.Logger {
	level := slog.LevelError
	if os.Getenv("RAFT_DEBUG") != "" {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

func (c *cluster) start(id NodeID) {
	c.t.Helper()
	fsm := &testFSM{c: c, id: id}
	node, err := NewNode(Config{
		ID:                id,
		Peers:             c.ids,
		HeartbeatInterval: 20 * time.Millisecond,
		ElectionTimeout:   150 * time.Millisecond,
		TickInterval:      5 * time.Millisecond,
		SnapshotThreshold: c.opts.snapshotThreshold,
		SnapshotTrailing:  c.opts.snapshotTrailing,
		DisablePreVote:    c.opts.disablePreVote,
		Storage:           c.storages[id],
		Transport:         &memTransport{nw: c.nw, id: id},
		StateMachine:      fsm,
		Logger:            testLogger(),
	})
	if err != nil {
		c.t.Fatalf("start %s: %v", id, err)
	}
	c.nw.mu.Lock()
	c.fsms[id] = fsm
	c.nw.nodes[id] = node
	c.nw.mu.Unlock()
	node.Start()
}

// crash stops a node and discards any unsynced writes.
func (c *cluster) crash(id NodeID) {
	c.nw.mu.Lock()
	node := c.nw.nodes[id]
	delete(c.nw.nodes, id)
	c.nw.mu.Unlock()
	if node != nil {
		node.Stop()
	}
	c.storages[id].DropUnsynced()
}

func (c *cluster) restart(id NodeID) {
	c.crash(id)
	c.start(id)
}

func (c *cluster) node(id NodeID) *Node {
	c.nw.mu.Lock()
	defer c.nw.mu.Unlock()
	return c.nw.nodes[id]
}

func (c *cluster) shutdown() {
	for _, id := range c.ids {
		if n := c.node(id); n != nil {
			n.Stop()
		}
	}
	c.checkErrors()
}

func (c *cluster) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
}

func (c *cluster) recordApply(id NodeID, index uint64, cmd string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if prev, ok := c.committed[index]; ok && prev != cmd {
		c.errs = append(c.errs, fmt.Errorf("SAFETY VIOLATION: node %s applied %q at index %d, others applied %q", id, cmd, index, prev))
		return
	}
	c.committed[index] = cmd
}

func (c *cluster) checkErrors() {
	c.t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, err := range c.errs {
		c.t.Error(err)
	}
	c.errs = nil
}

// disconnect isolates id from every other node (both directions).
func (c *cluster) disconnect(id NodeID) {
	c.nw.mu.Lock()
	defer c.nw.mu.Unlock()
	for _, other := range c.ids {
		if other != id {
			c.nw.cut[[2]NodeID{id, other}] = true
			c.nw.cut[[2]NodeID{other, id}] = true
		}
	}
}

func (c *cluster) connect(id NodeID) {
	c.nw.mu.Lock()
	defer c.nw.mu.Unlock()
	for _, other := range c.ids {
		if other != id {
			delete(c.nw.cut, [2]NodeID{id, other})
			delete(c.nw.cut, [2]NodeID{other, id})
		}
	}
}

// partition splits the cluster into the given groups.
func (c *cluster) partition(groups ...[]NodeID) {
	c.nw.mu.Lock()
	defer c.nw.mu.Unlock()
	c.nw.cut = map[[2]NodeID]bool{}
	group := map[NodeID]int{}
	for gi, g := range groups {
		for _, id := range g {
			group[id] = gi
		}
	}
	for _, a := range c.ids {
		for _, b := range c.ids {
			if a != b && group[a] != group[b] {
				c.nw.cut[[2]NodeID{a, b}] = true
			}
		}
	}
}

func (c *cluster) heal() {
	c.nw.mu.Lock()
	defer c.nw.mu.Unlock()
	c.nw.cut = map[[2]NodeID]bool{}
}

func (c *cluster) setUnreliable(v bool) {
	c.nw.mu.Lock()
	defer c.nw.mu.Unlock()
	c.nw.unreliable = v
}

func (c *cluster) connected(id NodeID) bool {
	c.nw.mu.Lock()
	defer c.nw.mu.Unlock()
	if c.nw.nodes[id] == nil {
		return false
	}
	for _, other := range c.ids {
		if other != id && !c.nw.cut[[2]NodeID{id, other}] {
			return true
		}
	}
	return len(c.ids) == 1
}

// checkOneLeader waits for exactly one leader among the connected nodes
// (per term) and returns it.
func (c *cluster) checkOneLeader() NodeID {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		leaders := map[uint64][]NodeID{}
		for _, id := range c.ids {
			n := c.node(id)
			if n == nil || !c.connected(id) {
				continue
			}
			st := n.Status()
			if st.State == Leader {
				leaders[st.Term] = append(leaders[st.Term], id)
			}
		}
		var lastTerm uint64
		for term, ls := range leaders {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
			lastTerm = max(lastTerm, term)
		}
		if len(leaders) > 0 {
			return leaders[lastTerm][0]
		}
	}
	c.t.Fatalf("expected one leader, got none")
	return ""
}

func (c *cluster) checkNoLeader() {
	c.t.Helper()
	for _, id := range c.ids {
		n := c.node(id)
		if n != nil && c.connected(id) && n.IsLeader() {
			c.t.Fatalf("expected no leader among connected nodes, but %s claims leadership", id)
		}
	}
}

func (c *cluster) checkTerms() uint64 {
	c.t.Helper()
	var term uint64
	for _, id := range c.ids {
		if n := c.node(id); n != nil && c.connected(id) {
			t := n.Status().Term
			if term == 0 {
				term = t
			} else if term != t {
				c.t.Fatalf("servers disagree on term: %d vs %d", term, t)
			}
		}
	}
	return term
}

// nCommitted returns how many nodes have applied index and the command.
func (c *cluster) nCommitted(index uint64) (int, string) {
	c.mu.Lock()
	cmd, ok := c.committed[index]
	c.mu.Unlock()
	if !ok {
		return 0, ""
	}
	count := 0
	for _, id := range c.ids {
		c.nw.mu.Lock()
		f := c.fsms[id]
		alive := c.nw.nodes[id] != nil
		c.nw.mu.Unlock()
		if alive && f.lastApplied() >= index {
			count++
		}
	}
	return count, cmd
}

// propose submits cmd to whichever node is leader, retrying until it is
// committed on at least expected nodes. It returns the log index.
func (c *cluster) propose(cmd string, expected int, retry bool) uint64 {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, id := range c.ids {
			n := c.node(id)
			if n == nil || !c.connected(id) || !n.IsLeader() {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_, err := n.Propose(ctx, []byte(cmd))
			cancel()
			if err != nil {
				continue
			}
			// Find the index.
			end := time.Now().Add(2 * time.Second)
			for time.Now().Before(end) {
				if idx := c.indexOf(cmd); idx > 0 {
					if cnt, _ := c.nCommitted(idx); cnt >= expected {
						return idx
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("propose(%q) failed to reach %d nodes", cmd, expected)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatalf("propose(%q) failed to reach agreement", cmd)
	return 0
}

func (c *cluster) indexOf(cmd string) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	for idx, v := range c.committed {
		if v == cmd {
			return idx
		}
	}
	return 0
}

// waitConverged waits until all live nodes have applied the same index.
func (c *cluster) waitConverged(timeout time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var idx []uint64
		for _, id := range c.ids {
			if n := c.node(id); n != nil {
				idx = append(idx, n.Status().AppliedIndex)
			}
		}
		same := true
		for _, v := range idx {
			if v != idx[0] {
				same = false
			}
		}
		if same && len(idx) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("cluster failed to converge within %v", timeout)
}
