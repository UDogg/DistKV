package raft

// raftLog is the in-memory view of the replicated log. entries[0] is a
// sentinel describing the last compacted entry (index and term only); the
// real entries start at entries[0].Index+1.
//
// Entries are treated as immutable once appended, which lets callers hand
// out sub-slices without copying the payloads.
type raftLog struct {
	entries []Entry
}

func newRaftLog(sentinelIndex, sentinelTerm uint64, entries []Entry) *raftLog {
	l := &raftLog{entries: make([]Entry, 1, len(entries)+1)}
	l.entries[0] = Entry{Index: sentinelIndex, Term: sentinelTerm}
	l.entries = append(l.entries, entries...)
	return l
}

// sentinel returns the index and term of the compaction point.
func (l *raftLog) sentinel() (uint64, uint64) {
	return l.entries[0].Index, l.entries[0].Term
}

func (l *raftLog) firstIndex() uint64 { return l.entries[0].Index + 1 }

func (l *raftLog) lastIndex() uint64 { return l.entries[len(l.entries)-1].Index }

func (l *raftLog) lastTerm() uint64 { return l.entries[len(l.entries)-1].Term }

// term returns the term of the entry at index i. ok is false when i lies
// outside [sentinel, lastIndex].
func (l *raftLog) term(i uint64) (uint64, bool) {
	base := l.entries[0].Index
	if i < base || i > l.lastIndex() {
		return 0, false
	}
	return l.entries[i-base].Term, true
}

// entry returns the entry at index i, which must be in [firstIndex, lastIndex].
func (l *raftLog) entry(i uint64) Entry {
	return l.entries[i-l.entries[0].Index]
}

// slice returns entries in [lo, hi). The result aliases the log's backing
// array and must not be modified.
func (l *raftLog) slice(lo, hi uint64) []Entry {
	base := l.entries[0].Index
	return l.entries[lo-base : hi-base]
}

// window returns up to maxEntries entries starting at lo, also bounded by
// maxBytes of payload (at least one entry is always returned when
// available). The result is a fresh slice.
func (l *raftLog) window(lo uint64, maxEntries int, maxBytes int) []Entry {
	last := l.lastIndex()
	if lo > last {
		return nil
	}
	hi := last + 1
	if maxEntries > 0 && hi-lo > uint64(maxEntries) {
		hi = lo + uint64(maxEntries)
	}
	src := l.slice(lo, hi)
	size := 0
	for i, e := range src {
		size += len(e.Data) + 32
		if maxBytes > 0 && size > maxBytes && i > 0 {
			src = src[:i]
			break
		}
	}
	out := make([]Entry, len(src))
	copy(out, src)
	return out
}

// truncateFrom removes every entry with index >= i.
func (l *raftLog) truncateFrom(i uint64) {
	base := l.entries[0].Index
	if i <= base {
		panic("raft: truncating compacted entries")
	}
	if i > l.lastIndex() {
		return
	}
	// Clear the tail so payloads can be garbage collected.
	tail := l.entries[i-base:]
	for k := range tail {
		tail[k] = Entry{}
	}
	l.entries = l.entries[:i-base]
}

func (l *raftLog) append(es ...Entry) {
	l.entries = append(l.entries, es...)
}

// compact discards entries up to and including index i, which becomes the
// new sentinel. i must be within the current log.
func (l *raftLog) compact(i uint64) {
	base := l.entries[0].Index
	if i <= base {
		return
	}
	t, ok := l.term(i)
	if !ok {
		panic("raft: compacting beyond last index")
	}
	remaining := l.entries[i-base+1:]
	fresh := make([]Entry, 1, len(remaining)+1)
	fresh[0] = Entry{Index: i, Term: t}
	l.entries = append(fresh, remaining...)
}

// reset discards the entire log and installs a new sentinel.
func (l *raftLog) reset(index, term uint64) {
	l.entries = []Entry{{Index: index, Term: term}}
}

// firstIndexOfTerm returns the first index in the log whose term equals t,
// scanning backwards from 'from'. Used to compute conflict hints.
func (l *raftLog) firstIndexOfTerm(t uint64, from uint64) uint64 {
	base := l.entries[0].Index
	i := from
	for i > base+1 {
		if pt, _ := l.term(i - 1); pt != t {
			break
		}
		i--
	}
	return i
}

// lastIndexOfTerm returns the last index whose term equals t, or 0.
func (l *raftLog) lastIndexOfTerm(t uint64) uint64 {
	for k := len(l.entries) - 1; k > 0; k-- {
		et := l.entries[k].Term
		if et == t {
			return l.entries[k].Index
		}
		if et < t {
			break
		}
	}
	return 0
}

// isUpToDate implements the election restriction of Raft §5.4.1.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	myTerm := l.lastTerm()
	return lastTerm > myTerm || (lastTerm == myTerm && lastIndex >= l.lastIndex())
}
