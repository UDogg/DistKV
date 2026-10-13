package kv

import (
	"bytes"
	"container/heap"
	"container/list"
	"encoding/gob"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/UDogg/DistKV/internal/raft"
)

// Code classifies the outcome of a command.
type Code uint8

const (
	CodeOK Code = iota
	CodeNotFound
	// CodeConflict reports a failed compare-and-set.
	CodeConflict
	// CodeStale reports a duplicate of a request whose response the client
	// has already acknowledged.
	CodeStale
	CodeBadCommand
)

func (c Code) String() string {
	return [...]string{"ok", "not_found", "conflict", "stale", "bad_command"}[c]
}

// Result is returned (via raft.Node.Propose) for every applied command.
type Result struct {
	Code Code
	// Version is the new version on success, or the current version of
	// the key on CodeConflict (0 if absent).
	Version uint64
}

// Item is a cached value.
type Item struct {
	Key      string
	Value    []byte
	Version  uint64
	ExpireAt int64 // Unix nanoseconds, 0 = never
}

// Options bounds the store.
type Options struct {
	// MaxKeys caps the number of keys; least-recently-written keys are
	// evicted first. 0 means unlimited.
	MaxKeys int
	// MaxBytes caps the approximate memory used by keys and values.
	MaxBytes int64
	// SessionTTL is how long an idle client session is remembered for
	// deduplication (measured on the replicated logical clock).
	SessionTTL time.Duration
	// MaxResultsPerSession bounds the dedup cache per client.
	MaxResultsPerSession int
}

const itemOverhead = 64

// Store is the replicated cache. It implements raft.StateMachine.
type Store struct {
	opts Options

	mu       sync.RWMutex
	items    map[string]*entry
	lru      *list.List // front = most recently written
	exp      expHeap
	bytes    int64
	clock    int64 // replicated logical clock (max Command.Now seen)
	applied  uint64
	sessions map[uint64]*session

	evictions   atomic.Uint64
	expirations atomic.Uint64
	dedupHits   atomic.Uint64
}

type entry struct {
	key      string
	value    []byte
	version  uint64
	expireAt int64
	elem     *list.Element
	heapIdx  int // -1 when not in the expiry heap
}

type session struct {
	ackBelow uint64
	lastSeen int64
	results  map[uint64]Result
}

// NewStore returns an empty store.
func NewStore(opts Options) *Store {
	if opts.SessionTTL <= 0 {
		opts.SessionTTL = 10 * time.Minute
	}
	if opts.MaxResultsPerSession <= 0 {
		opts.MaxResultsPerSession = 4096
	}
	s := &Store{opts: opts}
	s.reset()
	return s
}

func (s *Store) reset() {
	s.items = make(map[string]*entry)
	s.lru = list.New()
	s.exp = nil
	s.bytes = 0
	s.clock = 0
	s.applied = 0
	s.sessions = make(map[uint64]*session)
}

// Get returns the live value for key as observed at wall-clock time now
// (Unix nanoseconds). The returned Value must not be modified.
func (s *Store) Get(key string, now int64) (Item, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.items[key]
	if !ok || (e.expireAt != 0 && now >= e.expireAt) {
		return Item{}, false
	}
	return Item{Key: e.key, Value: e.value, Version: e.version, ExpireAt: e.expireAt}, true
}

// Stats describes the store's contents.
type Stats struct {
	Keys        int
	Bytes       int64
	Sessions    int
	Evictions   uint64
	Expirations uint64
	DedupHits   uint64
}

// Stats returns current counters.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Stats{
		Keys:        len(s.items),
		Bytes:       s.bytes,
		Sessions:    len(s.sessions),
		Evictions:   s.evictions.Load(),
		Expirations: s.expirations.Load(),
		DedupHits:   s.dedupHits.Load(),
	}
}

// Apply implements raft.StateMachine.
func (s *Store) Apply(e raft.Entry) any {
	cmd, err := DecodeCommand(e.Data)
	if err != nil {
		return Result{Code: CodeBadCommand}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.apply(&cmd, e.Index)
}

func (s *Store) apply(cmd *Command, index uint64) Result {
	if cmd.Now > s.clock {
		s.clock = cmd.Now
		s.purgeExpired()
	}
	s.applied++
	if s.applied%1024 == 0 {
		s.expireSessions()
	}

	var sess *session
	if cmd.ClientID != 0 {
		sess = s.sessions[cmd.ClientID]
		if sess == nil {
			sess = &session{results: make(map[uint64]Result)}
			s.sessions[cmd.ClientID] = sess
		}
		sess.lastSeen = s.clock
		if cmd.Seq < sess.ackBelow {
			return Result{Code: CodeStale}
		}
		if r, ok := sess.results[cmd.Seq]; ok {
			s.dedupHits.Add(1)
			return r
		}
	}

	r := s.execute(cmd, index)

	if sess != nil {
		sess.results[cmd.Seq] = r
		if cmd.AckBelow > sess.ackBelow {
			sess.ackBelow = cmd.AckBelow
			for seq := range sess.results {
				if seq < sess.ackBelow {
					delete(sess.results, seq)
				}
			}
		}
		for len(sess.results) > s.opts.MaxResultsPerSession {
			// Deterministic: drop the lowest sequence number.
			lowest := ^uint64(0)
			for seq := range sess.results {
				lowest = min(lowest, seq)
			}
			delete(sess.results, lowest)
			sess.ackBelow = max(sess.ackBelow, lowest+1)
		}
	}
	return r
}

func (s *Store) execute(cmd *Command, index uint64) Result {
	switch cmd.Op {
	case OpNoop:
		return Result{Code: CodeOK}
	case OpPut:
		s.set(cmd.Key, cmd.Value, index, cmd.ExpireAt)
		return Result{Code: CodeOK, Version: index}
	case OpCAS:
		var cur uint64
		if e, ok := s.items[cmd.Key]; ok {
			cur = e.version
		}
		if cur != cmd.ExpectVersion {
			return Result{Code: CodeConflict, Version: cur}
		}
		s.set(cmd.Key, cmd.Value, index, cmd.ExpireAt)
		return Result{Code: CodeOK, Version: index}
	case OpDelete:
		e, ok := s.items[cmd.Key]
		if !ok {
			return Result{Code: CodeNotFound}
		}
		s.remove(e)
		return Result{Code: CodeOK, Version: index}
	default:
		return Result{Code: CodeBadCommand}
	}
}

func (s *Store) set(key string, value []byte, version uint64, expireAt int64) {
	e, ok := s.items[key]
	if ok {
		s.bytes -= int64(len(e.value))
		e.value = value
		e.version = version
		s.bytes += int64(len(value))
		s.lru.MoveToFront(e.elem)
		if e.heapIdx >= 0 {
			heap.Remove(&s.exp, e.heapIdx)
		}
	} else {
		e = &entry{key: key, value: value, version: version, heapIdx: -1}
		e.elem = s.lru.PushFront(e)
		s.items[key] = e
		s.bytes += int64(len(key)+len(value)) + itemOverhead
	}
	e.expireAt = expireAt
	if expireAt != 0 {
		heap.Push(&s.exp, e)
	}
	s.purgeExpired()
	s.enforceCapacity()
}

func (s *Store) remove(e *entry) {
	s.lru.Remove(e.elem)
	if e.heapIdx >= 0 {
		heap.Remove(&s.exp, e.heapIdx)
	}
	delete(s.items, e.key)
	s.bytes -= int64(len(e.key)+len(e.value)) + itemOverhead
}

func (s *Store) purgeExpired() {
	for len(s.exp) > 0 && s.exp[0].expireAt <= s.clock {
		s.remove(s.exp[0])
		s.expirations.Add(1)
	}
}

func (s *Store) enforceCapacity() {
	for s.lru.Len() > 1 &&
		((s.opts.MaxKeys > 0 && len(s.items) > s.opts.MaxKeys) ||
			(s.opts.MaxBytes > 0 && s.bytes > s.opts.MaxBytes)) {
		s.remove(s.lru.Back().Value.(*entry))
		s.evictions.Add(1)
	}
}

func (s *Store) expireSessions() {
	cutoff := s.clock - int64(s.opts.SessionTTL)
	for id, sess := range s.sessions {
		if sess.lastSeen < cutoff {
			delete(s.sessions, id)
		}
	}
}

// ---------------------------------------------------------------------------
// Snapshots

type snapItem struct {
	Key      string
	Value    []byte
	Version  uint64
	ExpireAt int64
}

type snapSession struct {
	ID       uint64
	AckBelow uint64
	LastSeen int64
	Seqs     []uint64
	Results  []Result
}

type snapState struct {
	Clock    int64
	Applied  uint64
	Items    []snapItem // least recently written first
	Sessions []snapSession
}

// Snapshot implements raft.StateMachine.
func (s *Store) Snapshot() ([]byte, error) {
	s.mu.RLock()
	st := snapState{Clock: s.clock, Applied: s.applied, Items: make([]snapItem, 0, len(s.items))}
	for el := s.lru.Back(); el != nil; el = el.Prev() {
		e := el.Value.(*entry)
		st.Items = append(st.Items, snapItem{Key: e.key, Value: e.value, Version: e.version, ExpireAt: e.expireAt})
	}
	for id, sess := range s.sessions {
		ss := snapSession{ID: id, AckBelow: sess.ackBelow, LastSeen: sess.lastSeen}
		for seq := range sess.results {
			ss.Seqs = append(ss.Seqs, seq)
		}
		sort.Slice(ss.Seqs, func(i, j int) bool { return ss.Seqs[i] < ss.Seqs[j] })
		for _, seq := range ss.Seqs {
			ss.Results = append(ss.Results, sess.results[seq])
		}
		st.Sessions = append(st.Sessions, ss)
	}
	s.mu.RUnlock()
	// Canonical ordering makes snapshots byte-for-byte comparable.
	sort.Slice(st.Sessions, func(i, j int) bool { return st.Sessions[i].ID < st.Sessions[j].ID })

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&st); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Restore implements raft.StateMachine.
func (s *Store) Restore(data []byte) error {
	var st snapState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&st); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reset()
	s.clock = st.Clock
	s.applied = st.Applied
	for _, it := range st.Items {
		e := &entry{key: it.Key, value: it.Value, version: it.Version, expireAt: it.ExpireAt, heapIdx: -1}
		e.elem = s.lru.PushFront(e)
		s.items[it.Key] = e
		s.bytes += int64(len(it.Key)+len(it.Value)) + itemOverhead
		if it.ExpireAt != 0 {
			heap.Push(&s.exp, e)
		}
	}
	for _, ss := range st.Sessions {
		sess := &session{ackBelow: ss.AckBelow, lastSeen: ss.LastSeen, results: make(map[uint64]Result, len(ss.Seqs))}
		for i, seq := range ss.Seqs {
			sess.results[seq] = ss.Results[i]
		}
		s.sessions[ss.ID] = sess
	}
	return nil
}

// ---------------------------------------------------------------------------
// Expiry heap

type expHeap []*entry

func (h expHeap) Len() int { return len(h) }
func (h expHeap) Less(i, j int) bool {
	if h[i].expireAt != h[j].expireAt {
		return h[i].expireAt < h[j].expireAt
	}
	return h[i].key < h[j].key // deterministic tie-break
}
func (h expHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}
func (h *expHeap) Push(x any) {
	e := x.(*entry)
	e.heapIdx = len(*h)
	*h = append(*h, e)
}
func (h *expHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.heapIdx = -1
	*h = old[:n-1]
	return e
}

// HasExpired reports whether any key's deadline is at or before now, i.e.
// whether advancing the replicated clock would reclaim memory.
func (s *Store) HasExpired(now int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.exp) > 0 && s.exp[0].expireAt <= now
}
