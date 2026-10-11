package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func openEngine(t *testing.T, dir string, opts LogEngineOptions) *LogEngine {
	t.Helper()
	e, err := OpenLogEngine(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func initial(t *testing.T, s Storage) PersistentState {
	t.Helper()
	st, err := s.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestEngineMultiGroupRoundTrip(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir, LogEngineOptions{})
	a, b := e.Storage(1), e.Storage(2)
	must(t, a.SaveHardState(HardState{Term: 2, VotedFor: "n1", Commit: 3}))
	must(t, b.SaveHardState(HardState{Term: 9, VotedFor: "n3"}))
	must(t, a.Append(ents(1, 10, 2)))
	must(t, b.Append(ents(1, 4, 9)))
	must(t, a.Append(ents(8, 9, 3))) // overwrite a's suffix
	must(t, a.Sync())
	must(t, e.Close())

	e = openEngine(t, dir, LogEngineOptions{})
	defer e.Close()
	sa, sb := initial(t, e.Storage(1)), initial(t, e.Storage(2))
	if sa.HardState != (HardState{Term: 2, VotedFor: "n1", Commit: 3}) || sb.HardState.Term != 9 {
		t.Fatalf("hard states: %+v / %+v", sa.HardState, sb.HardState)
	}
	if len(sa.Entries) != 9 || sa.Entries[7].Term != 3 || sa.Entries[6].Term != 2 {
		t.Fatalf("group 1 entries wrong: %+v", sa.Entries)
	}
	if len(sb.Entries) != 4 {
		t.Fatalf("group 2 has %d entries", len(sb.Entries))
	}
	if st := initial(t, e.Storage(3)); len(st.Entries) != 0 || st.HardState.Term != 0 {
		t.Fatalf("unknown group not empty: %+v", st)
	}
}

func TestEngineCompactAndSnapshot(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir, LogEngineOptions{})
	s := e.Storage(7)
	must(t, s.Append(ents(1, 50, 1)))
	must(t, s.SaveSnapshot(Snapshot{Meta: SnapshotMeta{Index: 40, Term: 1}, Data: []byte("s40")}))
	must(t, s.Compact(HardState{Term: 1, Commit: 45}, 30, 1, ents(31, 50, 1)))
	must(t, s.Append(ents(51, 52, 1)))
	must(t, e.Close())

	e = openEngine(t, dir, LogEngineOptions{})
	defer e.Close()
	st := initial(t, e.Storage(7))
	if st.Snapshot == nil || string(st.Snapshot.Data) != "s40" {
		t.Fatalf("snapshot = %+v", st.Snapshot)
	}
	if st.SentinelIndex != 30 || len(st.Entries) != 22 || st.Entries[21].Index != 52 || st.HardState.Commit != 45 {
		t.Fatalf("state after compaction: sentinel=%d entries=%d hs=%+v", st.SentinelIndex, len(st.Entries), st.HardState)
	}
}

func TestEngineTornTail(t *testing.T) {
	dir := t.TempDir()
	e := openEngine(t, dir, LogEngineOptions{SegmentSize: 1 << 20})
	s := e.Storage(1)
	must(t, s.Append(ents(1, 5, 1)))
	must(t, s.Sync())
	off := e.off
	must(t, e.Close())

	// Scribble a partial record after the last valid one.
	seg := filepath.Join(dir, "wal", fmt.Sprintf("%020d.seg", 1))
	f, err := os.OpenFile(seg, os.O_WRONLY, 0)
	must(t, err)
	f.WriteAt([]byte{0x40, 0, 0, 0, 1, 2, 3, 4, 2, 9, 9}, off)
	f.Close()

	e = openEngine(t, dir, LogEngineOptions{SegmentSize: 1 << 20})
	s = e.Storage(1)
	if st := initial(t, s); len(st.Entries) != 5 {
		t.Fatalf("got %d entries after torn tail", len(st.Entries))
	}
	must(t, s.Append(ents(6, 6, 1)))
	must(t, e.Close())
	e = openEngine(t, dir, LogEngineOptions{SegmentSize: 1 << 20})
	defer e.Close()
	if st := initial(t, e.Storage(1)); len(st.Entries) != 6 {
		t.Fatalf("got %d entries after re-append", len(st.Entries))
	}
}

// With tiny segments, rotation and GC run constantly. Idle groups that
// pin old segments must be checkpointed forward, and nothing may be lost.
func TestEngineRotationAndGC(t *testing.T) {
	dir := t.TempDir()
	opts := LogEngineOptions{SegmentSize: 32 << 10, MaxSegments: 3}
	e := openEngine(t, dir, opts)
	idle := e.Storage(99)
	must(t, idle.SaveHardState(HardState{Term: 5, VotedFor: "x", Commit: 2}))
	must(t, idle.Append(ents(1, 3, 5)))

	busy := e.Storage(1)
	payload := make([]byte, 512)
	var next uint64 = 1
	for round := 0; round < 40; round++ {
		var batch []Entry
		for i := 0; i < 20; i++ {
			batch = append(batch, Entry{Index: next, Term: 1, Data: payload})
			next++
		}
		must(t, busy.Append(batch))
		must(t, busy.Sync())
		if round%10 == 9 {
			// Simulate a snapshot + compaction of the busy group.
			last := next - 1
			must(t, busy.SaveSnapshot(Snapshot{Meta: SnapshotMeta{Index: last - 5, Term: 1}, Data: []byte("x")}))
			e.mu.Lock()
			keep := append([]Entry(nil), e.groups[1].entries[len(e.groups[1].entries)-5:]...)
			e.mu.Unlock()
			must(t, busy.Compact(HardState{Term: 1, Commit: last}, last-5, 1, keep))
		}
	}
	if n := e.Segments(); n > opts.MaxSegments+1 {
		t.Fatalf("GC did not bound segments: %d", n)
	}
	must(t, e.Close())

	e = openEngine(t, dir, opts)
	defer e.Close()
	if st := initial(t, e.Storage(99)); st.HardState.VotedFor != "x" || len(st.Entries) != 3 {
		t.Fatalf("idle group lost state after GC: %+v", st)
	}
	st := initial(t, e.Storage(1))
	if got := st.Entries[len(st.Entries)-1].Index; got != next-1 {
		t.Fatalf("busy group last index %d, want %d", got, next-1)
	}
	segs, _ := os.ReadDir(filepath.Join(dir, "wal"))
	if len(segs) > opts.MaxSegments+1 {
		t.Fatalf("%d segment files on disk", len(segs))
	}
}

// Concurrent syncs from many groups must be coalesced.
func TestEngineGroupCommit(t *testing.T) {
	e := openEngine(t, t.TempDir(), LogEngineOptions{})
	defer e.Close()
	const groups, perGroup = 16, 50
	var syncs int
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := time.Now()
	for g := 1; g <= groups; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s := e.Storage(uint64(g))
			for i := 1; i <= perGroup; i++ {
				must(t, s.Append([]Entry{{Index: uint64(i), Term: 1, Data: []byte("v")}}))
				must(t, s.Sync())
				mu.Lock()
				syncs++
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	t.Logf("%d durable appends across %d groups in %v", syncs, groups, time.Since(start))
	for g := 1; g <= groups; g++ {
		if st := initial(t, e.Storage(uint64(g))); len(st.Entries) != perGroup {
			t.Fatalf("group %d has %d entries", g, len(st.Entries))
		}
	}
}

// Full Raft clusters on top of a shared engine per node, with restarts.
func TestRaftOnLogEngine(t *testing.T) {
	dirs := map[NodeID]string{}
	for _, id := range []NodeID{"n1", "n2", "n3"} {
		dirs[id] = t.TempDir()
	}
	c := &cluster{t: t, nw: newNetwork(), ids: []NodeID{"n1", "n2", "n3"}, committed: map[uint64]string{}, fsms: map[NodeID]*testFSM{}}
	engines := map[NodeID]*LogEngine{}
	start := func(id NodeID) {
		e := openEngine(t, dirs[id], LogEngineOptions{SegmentSize: 64 << 10, MaxSegments: 2})
		engines[id] = e
		fsm := &testFSM{c: c, id: id}
		n, err := NewNode(Config{
			ID: id, Peers: c.ids, HeartbeatInterval: 20 * time.Millisecond, ElectionTimeout: 150 * time.Millisecond,
			SnapshotThreshold: 30, SnapshotTrailing: 5,
			Storage: e.Storage(42), Transport: &memTransport{nw: c.nw, id: id}, StateMachine: fsm, Logger: testLogger(),
		})
		must(t, err)
		c.nw.mu.Lock()
		c.nw.nodes[id] = n
		c.fsms[id] = fsm
		c.nw.mu.Unlock()
		n.Start()
	}
	stop := func(id NodeID) {
		c.nw.mu.Lock()
		n := c.nw.nodes[id]
		delete(c.nw.nodes, id)
		c.nw.mu.Unlock()
		n.Stop()
		must(t, engines[id].Close())
	}
	for _, id := range c.ids {
		start(id)
	}
	for i := 0; i < 150; i++ {
		c.propose(fmt.Sprintf("x%d", i), 2, true)
		if i%50 == 25 {
			victim := c.ids[i%3]
			stop(victim)
			start(victim)
		}
	}
	c.waitConverged(5 * time.Second)
	ids := append([]NodeID(nil), c.ids...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		stop(id)
	}
	c.checkErrors()
}
