package raft

import (
	"context"
	"testing"
	"time"
)

// gatedStorage blocks Sync while armed, so a test can interleave other work
// with an in-flight sync.
type gatedStorage struct {
	*MemoryStorage
	armed   chan struct{} // closed: the next Sync blocks
	entered chan struct{}
	release chan struct{}
}

func (g *gatedStorage) Sync() error {
	select {
	case <-g.armed:
		select {
		case g.entered <- struct{}{}:
			<-g.release
		default:
		}
	default:
	}
	return g.MemoryStorage.Sync()
}

// Regression test: the leader's sync loop skipped advancing the commit
// index when a compaction (which also syncs) had already raised
// durableIndex past the sync's target. Nothing else advances the commit on
// a single-node group, so the proposal hung forever; with followers it
// stalled until the next acknowledgement.
func TestCompactionDuringLeaderSyncStillCommits(t *testing.T) {
	g := &gatedStorage{MemoryStorage: NewMemoryStorage(), armed: make(chan struct{}), entered: make(chan struct{}), release: make(chan struct{})}
	n, err := NewNode(Config{
		ID: "n1", Peers: []NodeID{"n1"}, HeartbeatInterval: 10 * time.Millisecond, ElectionTimeout: 50 * time.Millisecond,
		Storage: g, Transport: &memTransport{nw: newNetwork(), id: "n1"}, StateMachine: &testFSM{c: &cluster{t: t, committed: map[uint64]string{}}, id: "n1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	defer n.Stop()
	waitLeader(t, n)
	if _, err := n.Propose(t.Context(), []byte("warm-up")); err != nil {
		t.Fatal(err)
	}

	close(g.armed)
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := n.Propose(ctx, []byte("x"))
		done <- err
	}()
	select {
	case <-g.entered: // the sync loop is now syncing the new entry
	case <-time.After(3 * time.Second):
		t.Fatal("the sync loop never synced the proposal")
	}
	// A snapshot's compaction syncs everything and raises durableIndex.
	n.mu.Lock()
	n.compactStorage()
	n.mu.Unlock()
	close(g.release)

	if err := <-done; err != nil {
		t.Fatalf("proposal did not commit: %v", err)
	}
}
