package raft

import "sync"

// PersistentState is everything a node recovers on restart.
type PersistentState struct {
	HardState HardState
	// Snapshot is the most recent durable snapshot, or nil.
	Snapshot *Snapshot
	// SentinelIndex/SentinelTerm describe the entry immediately preceding
	// Entries. They are at most Snapshot.Meta.Index because DistKV retains
	// a tail of already-snapshotted entries for slow followers.
	SentinelIndex uint64
	SentinelTerm  uint64
	Entries       []Entry
}

// Storage persists Raft state. Implementations must be safe for concurrent
// use: the leader appends under its state lock while a background syncer
// flushes concurrently.
//
// Append has "overwrite" semantics: if the first entry's index is not
// directly after the last stored entry, all stored entries with index >=
// entries[0].Index are logically discarded first. Append may buffer; data
// is only guaranteed durable after Sync or SaveHardState returns.
type Storage interface {
	InitialState() (PersistentState, error)
	SaveHardState(hs HardState) error
	Append(entries []Entry) error
	Sync() error
	// SaveSnapshot durably stores snap unless a snapshot with a higher
	// index is already stored, in which case it is a no-op.
	SaveSnapshot(snap Snapshot) error
	LoadSnapshot() (*Snapshot, error)
	// Compact rewrites the log so that it holds exactly the given hard
	// state, sentinel and entries.
	Compact(hs HardState, sentinelIndex, sentinelTerm uint64, entries []Entry) error
	Close() error
}

// MemoryStorage is an in-memory Storage used by tests and by ephemeral
// (cache-only) deployments. It models durability faithfully: appended
// entries are staged until Sync, and DropUnsynced simulates a crash that
// loses everything not yet synced.
type MemoryStorage struct {
	mu            sync.Mutex
	hs            HardState
	snap          *Snapshot
	sentinelIndex uint64
	sentinelTerm  uint64
	durable       []Entry
	stagedOps     [][]Entry
}

// NewMemoryStorage returns an empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) InitialState() (PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := PersistentState{
		HardState:     m.hs,
		SentinelIndex: m.sentinelIndex,
		SentinelTerm:  m.sentinelTerm,
		Entries:       append([]Entry(nil), m.durable...),
	}
	if m.snap != nil {
		s := *m.snap
		st.Snapshot = &s
	}
	return st, nil
}

func (m *MemoryStorage) SaveHardState(hs HardState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncLocked()
	m.hs = hs
	return nil
}

func (m *MemoryStorage) Append(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stagedOps = append(m.stagedOps, append([]Entry(nil), entries...))
	return nil
}

func (m *MemoryStorage) Sync() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.syncLocked()
	return nil
}

func (m *MemoryStorage) syncLocked() {
	for _, op := range m.stagedOps {
		first := op[0].Index
		// Drop everything at or after first, then append.
		keep := 0
		for keep < len(m.durable) && m.durable[keep].Index < first {
			keep++
		}
		m.durable = append(m.durable[:keep:keep], op...)
	}
	m.stagedOps = nil
}

// DropUnsynced discards entries that were appended but never synced,
// simulating a machine crash.
func (m *MemoryStorage) DropUnsynced() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stagedOps = nil
}

func (m *MemoryStorage) SaveSnapshot(snap Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snap != nil && m.snap.Meta.Index >= snap.Meta.Index {
		return nil
	}
	s := snap
	s.Data = append([]byte(nil), snap.Data...)
	m.snap = &s
	return nil
}

func (m *MemoryStorage) LoadSnapshot() (*Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snap == nil {
		return nil, nil
	}
	s := *m.snap
	return &s, nil
}

func (m *MemoryStorage) Compact(hs HardState, sentinelIndex, sentinelTerm uint64, entries []Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hs = hs
	m.sentinelIndex = sentinelIndex
	m.sentinelTerm = sentinelTerm
	m.durable = append([]Entry(nil), entries...)
	m.stagedOps = nil
	return nil
}

func (m *MemoryStorage) Close() error { return nil }
