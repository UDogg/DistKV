package raft

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openFS(t *testing.T, dir string) (*FileStorage, PersistentState) {
	t.Helper()
	s, err := OpenFileStorage(dir, FileStorageOptions{})
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	return s, st
}

func ents(from, to, term uint64) []Entry {
	var out []Entry
	for i := from; i <= to; i++ {
		out = append(out, Entry{Index: i, Term: term, Data: []byte{byte(i)}})
	}
	return out
}

func TestFileStorageRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, st := openFS(t, dir)
	if len(st.Entries) != 0 || st.HardState.Term != 0 {
		t.Fatalf("fresh storage not empty: %+v", st)
	}
	must(t, s.SaveHardState(HardState{Term: 3, VotedFor: "n2", Commit: 1}))
	must(t, s.Append(ents(1, 10, 3)))
	must(t, s.Sync())
	// Overwrite a suffix (follower conflict resolution).
	must(t, s.Append(ents(6, 8, 4)))
	must(t, s.Sync())
	must(t, s.Close())

	_, st = openFS(t, dir)
	if st.HardState != (HardState{Term: 3, VotedFor: "n2", Commit: 1}) {
		t.Fatalf("hard state = %+v", st.HardState)
	}
	if len(st.Entries) != 8 {
		t.Fatalf("got %d entries, want 8", len(st.Entries))
	}
	if st.Entries[4].Term != 3 || st.Entries[5].Term != 4 || st.Entries[7].Index != 8 {
		t.Fatalf("overwrite not honoured: %+v", st.Entries)
	}
}

func TestFileStorageTornTail(t *testing.T) {
	dir := t.TempDir()
	s, _ := openFS(t, dir)
	must(t, s.Append(ents(1, 5, 1)))
	must(t, s.Close())

	// Simulate a crash in the middle of writing a record.
	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	must(t, err)
	f.Write([]byte{0x10, 0, 0, 0, 0xde, 0xad})
	f.Close()

	s, st := openFS(t, dir)
	if len(st.Entries) != 5 {
		t.Fatalf("got %d entries after torn tail, want 5", len(st.Entries))
	}
	// Further appends must work after truncation.
	must(t, s.Append(ents(6, 6, 1)))
	must(t, s.Close())
	_, st = openFS(t, dir)
	if len(st.Entries) != 6 {
		t.Fatalf("got %d entries, want 6", len(st.Entries))
	}
}

func TestFileStorageCorruptRecordTruncates(t *testing.T) {
	dir := t.TempDir()
	s, _ := openFS(t, dir)
	must(t, s.Append(ents(1, 3, 1)))
	must(t, s.Close())
	path := filepath.Join(dir, walFileName)
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xff // flip a payload bit in the last record
	must(t, os.WriteFile(path, data, 0o644))
	_, st := openFS(t, dir)
	if len(st.Entries) != 2 {
		t.Fatalf("got %d entries, want 2 (corrupt last record dropped)", len(st.Entries))
	}
}

func TestFileStorageSnapshotAndCompact(t *testing.T) {
	dir := t.TempDir()
	s, _ := openFS(t, dir)
	must(t, s.Append(ents(1, 100, 2)))
	must(t, s.Sync())
	snap := Snapshot{Meta: SnapshotMeta{Index: 90, Term: 2}, Data: []byte("state@90")}
	must(t, s.SaveSnapshot(snap))
	// Older snapshots must never replace newer ones.
	must(t, s.SaveSnapshot(Snapshot{Meta: SnapshotMeta{Index: 50, Term: 2}, Data: []byte("old")}))
	must(t, s.Compact(HardState{Term: 2, Commit: 95}, 80, 2, ents(81, 100, 2)))
	must(t, s.Append(ents(101, 105, 2)))
	must(t, s.Close())

	_, st := openFS(t, dir)
	if st.Snapshot == nil || !bytes.Equal(st.Snapshot.Data, []byte("state@90")) {
		t.Fatalf("snapshot = %+v", st.Snapshot)
	}
	if st.SentinelIndex != 80 || st.SentinelTerm != 2 {
		t.Fatalf("sentinel = %d/%d", st.SentinelIndex, st.SentinelTerm)
	}
	if len(st.Entries) != 25 || st.Entries[0].Index != 81 || st.Entries[24].Index != 105 {
		t.Fatalf("entries after compaction: %d", len(st.Entries))
	}
	if st.HardState.Commit != 95 {
		t.Fatalf("commit = %d", st.HardState.Commit)
	}
}

func TestFileStorageCorruptSnapshotDetected(t *testing.T) {
	dir := t.TempDir()
	s, _ := openFS(t, dir)
	must(t, s.SaveSnapshot(Snapshot{Meta: SnapshotMeta{Index: 1, Term: 1}, Data: []byte("hello")}))
	must(t, s.Close())
	path := filepath.Join(dir, snapshotFileName)
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xff
	must(t, os.WriteFile(path, data, 0o644))
	s2, err := OpenFileStorage(dir, FileStorageOptions{})
	must(t, err)
	if _, err := s2.InitialState(); err == nil {
		t.Fatalf("corrupt snapshot was not detected")
	}
}

// A node backed by FileStorage must recover its full state machine after
// a restart.
func TestNodeRecoversFromFileStorage(t *testing.T) {
	dir := t.TempDir()
	c := &cluster{t: t, committed: map[uint64]string{}}
	newNode := func() (*Node, *testFSM, *FileStorage) {
		s, err := OpenFileStorage(dir, FileStorageOptions{})
		must(t, err)
		fsm := &testFSM{c: c, id: "n1"}
		n, err := NewNode(Config{
			ID: "n1", Peers: []NodeID{"n1"},
			HeartbeatInterval: 10e6, ElectionTimeout: 50e6,
			SnapshotThreshold: 10, SnapshotTrailing: 3,
			Storage: s, Transport: &memTransport{nw: newNetwork(), id: "n1"}, StateMachine: fsm,
		})
		must(t, err)
		n.Start()
		return n, fsm, s
	}
	n, _, s := newNode()
	waitLeader(t, n)
	for i := 0; i < 25; i++ {
		if _, err := n.Propose(t.Context(), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	n.Stop()
	must(t, s.Close())

	n, fsm, s := newNode()
	defer func() { n.Stop(); s.Close() }()
	waitLeader(t, n)
	must(t, n.LinearizableRead(t.Context()))
	fsm.mu.Lock()
	got := len(fsm.applied)
	fsm.mu.Unlock()
	if got != 25 {
		t.Fatalf("recovered state machine has %d commands, want 25", got)
	}
	if n.Status().SnapshotIndex == 0 {
		t.Fatalf("expected a snapshot to have been taken")
	}
}

func waitLeader(t *testing.T, n *Node) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if n.IsLeader() {
			return
		}
		sleepMs(10)
	}
	t.Fatalf("node never became leader")
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func sleepMs(ms int) { time.Sleep(time.Duration(ms) * time.Millisecond) }
