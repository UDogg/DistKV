package raft

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// LogEngine is a write-ahead log shared by many Raft groups on one node
// (the "multi-Raft" storage layout used by systems such as TiKV and
// CockroachDB).
//
// Why share a log? Raft requires an fsync before acknowledging entries. With
// one file per group, a node hosting G groups issues up to G concurrent
// fsyncs, which contend on the device and inflate tail latency. LogEngine
// appends every group's records to a single log and performs *group
// commit*: concurrent Sync calls from different groups are coalesced into
// one fdatasync.
//
// Layout of the engine directory:
//
//	wal/00000000000000000001.seg   preallocated, append-only log segments
//	snap/group-<id>.snap           latest snapshot of each group
//
// Records use the same CRC-protected framing as FileStorage, with the group
// ID prefixed to each payload. Compaction is logical: Compact writes a
// "reset" record followed by the group's retained hard state and entries.
// A segment can be deleted once every group with records in it has been
// reset in a later segment; groups that pin old segments (e.g. idle ones)
// are checkpointed forward when the segment count exceeds a bound.
type LogEngine struct {
	dir  string
	opts LogEngineOptions

	mu       sync.Mutex
	segs     []*segment // oldest first; last is active
	w        *bufio.Writer
	off      int64 // write offset in the active segment
	writeSeq uint64
	groups   map[uint64]*engineGroup
	buf      []byte
	closed   bool
	failed   error

	syncMu    sync.Mutex
	syncCond  *sync.Cond
	syncing   bool
	syncedSeq uint64
}

// LogEngineOptions configures a LogEngine.
type LogEngineOptions struct {
	// SegmentSize is the preallocated size of each segment (default 64 MiB).
	SegmentSize int64
	// MaxSegments triggers garbage collection when exceeded (default 4).
	MaxSegments int
	// NoSync skips fdatasync (see FileStorageOptions.NoSync).
	NoSync bool
}

type segment struct {
	seq    uint64
	path   string
	f      *os.File
	groups map[uint64]struct{} // groups with records in this segment
}

// engineGroup mirrors one group's durable log state. It is what replay
// reconstructs and what a checkpoint rewrites.
type engineGroup struct {
	hs            HardState
	sentinelIndex uint64
	sentinelTerm  uint64
	entries       []Entry
	resetSeg      uint64 // segment holding the group's latest reset record

	snapMu    sync.Mutex
	snapIndex uint64
}

const (
	recReset byte = 4 // sentinel + implicit truncation of the group's log
)

// OpenLogEngine opens or creates an engine in dir and replays it.
func OpenLogEngine(dir string, opts LogEngineOptions) (*LogEngine, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 64 << 20
	}
	if opts.MaxSegments <= 1 {
		opts.MaxSegments = 4
	}
	for _, sub := range []string{"wal", "snap"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, err
		}
	}
	e := &LogEngine{dir: dir, opts: opts, groups: map[uint64]*engineGroup{}}
	e.syncCond = sync.NewCond(&e.syncMu)
	if err := e.replay(); err != nil {
		e.closeFiles()
		return nil, err
	}
	return e, nil
}

func (e *LogEngine) segPath(seq uint64) string {
	return filepath.Join(e.dir, "wal", fmt.Sprintf("%020d.seg", seq))
}

func (e *LogEngine) replay() error {
	names, err := os.ReadDir(filepath.Join(e.dir, "wal"))
	if err != nil {
		return err
	}
	var seqs []uint64
	for _, n := range names {
		if !strings.HasSuffix(n.Name(), ".seg") {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(n.Name(), ".seg"), 10, 64)
		if err != nil {
			continue
		}
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })

	for i, seq := range seqs {
		last := i == len(seqs)-1
		f, err := os.OpenFile(e.segPath(seq), os.O_RDWR, 0o644)
		if err != nil {
			return err
		}
		seg := &segment{seq: seq, path: e.segPath(seq), f: f, groups: map[uint64]struct{}{}}
		e.segs = append(e.segs, seg)
		end, clean, err := e.replaySegment(seg)
		if err != nil {
			return err
		}
		if !clean && !last {
			return fmt.Errorf("%w: corrupt record in sealed segment %s at offset %d", ErrCorrupt, seg.path, end)
		}
		if last {
			// Zero everything after the last valid record so that a torn
			// write can never be mistaken for data later.
			if err := f.Truncate(end); err != nil {
				return err
			}
			preallocate(f, e.opts.SegmentSize)
			e.off = end
		}
	}
	if len(e.segs) == 0 {
		if err := e.newSegment(1); err != nil {
			return err
		}
	} else {
		e.w = bufio.NewWriterSize(&offsetWriter{f: e.active().f, off: &e.off}, 256<<10)
	}

	// Load snapshot metadata.
	for id, g := range e.groups {
		if snap, err := readSnapshotFile(e.snapPath(id)); err == nil && snap != nil {
			g.snapIndex = snap.Meta.Index
		} else if err != nil {
			return err
		}
	}
	return nil
}

// replaySegment applies every valid record of seg. It returns the offset
// after the last valid record and whether the segment ended cleanly (EOF
// or zeroed preallocation) rather than at a corrupt record.
func (e *LogEngine) replaySegment(seg *segment) (int64, bool, error) {
	r := bufio.NewReaderSize(seg.f, 1<<20)
	hdr := make([]byte, recordHeaderSize)
	var off int64
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return off, err == io.EOF, nil
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		typ := hdr[8]
		if n == 0 && sum == 0 && typ == 0 {
			return off, true, nil // preallocated zero region: end of log
		}
		if n > maxRecordSize {
			return off, false, nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return off, false, nil
		}
		if crc32.Update(crc32.Checksum([]byte{typ}, crcTable), crcTable, payload) != sum {
			return off, false, nil
		}
		id, k := binary.Uvarint(payload)
		if k <= 0 {
			return off, false, nil
		}
		if err := e.applyRecord(seg, id, typ, payload[k:]); err != nil {
			return off, false, err
		}
		off += int64(recordHeaderSize) + int64(n)
	}
}

func (e *LogEngine) group(id uint64) *engineGroup {
	g := e.groups[id]
	if g == nil {
		g = &engineGroup{}
		e.groups[id] = g
	}
	return g
}

// applyRecord decodes a replayed record into the in-memory mirror.
func (e *LogEngine) applyRecord(seg *segment, id uint64, typ byte, p []byte) error {
	g := e.group(id)
	seg.groups[id] = struct{}{}
	switch typ {
	case recHardState:
		hs, err := decodeHardState(p)
		if err != nil {
			return err
		}
		g.hs = hs
	case recReset:
		if len(p) != 16 {
			return ErrCorrupt
		}
		g.reset(binary.LittleEndian.Uint64(p[0:8]), binary.LittleEndian.Uint64(p[8:16]), seg.seq)
	case recEntry:
		ent, err := decodeEntry(p)
		if err != nil {
			return err
		}
		return g.appendEntry(ent)
	default:
		return fmt.Errorf("%w: unknown record type %d", ErrCorrupt, typ)
	}
	return nil
}

func (g *engineGroup) reset(index, term, seg uint64) {
	g.sentinelIndex, g.sentinelTerm = index, term
	clear(g.entries)
	g.entries = g.entries[:0]
	g.resetSeg = seg
}

func (g *engineGroup) appendEntry(ent Entry) error {
	if ent.Index <= g.sentinelIndex {
		return nil
	}
	if n := len(g.entries); n > 0 && g.entries[n-1].Index >= ent.Index {
		keep := int(ent.Index - g.sentinelIndex - 1)
		clear(g.entries[keep:])
		g.entries = g.entries[:keep]
	}
	if want := g.sentinelIndex + uint64(len(g.entries)) + 1; ent.Index != want {
		return fmt.Errorf("%w: log gap: expected index %d, found %d", ErrCorrupt, want, ent.Index)
	}
	g.entries = append(g.entries, ent)
	return nil
}

func (e *LogEngine) active() *segment { return e.segs[len(e.segs)-1] }

func (e *LogEngine) newSegment(seq uint64) error {
	f, err := os.OpenFile(e.segPath(seq), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	preallocate(f, e.opts.SegmentSize)
	if err := syncDir(filepath.Join(e.dir, "wal"), e.opts.NoSync); err != nil {
		f.Close()
		return err
	}
	e.segs = append(e.segs, &segment{seq: seq, path: e.segPath(seq), f: f, groups: map[uint64]struct{}{}})
	e.off = 0
	e.w = bufio.NewWriterSize(&offsetWriter{f: f, off: &e.off}, 256<<10)
	return nil
}

// write appends one record for group id and marks the active segment as
// holding data for it. Callers update the group's mirror themselves (with
// the original values, avoiding a decode/copy). Called with e.mu held.
func (e *LogEngine) write(id uint64, typ byte, payload []byte) error {
	if e.failed != nil {
		return e.failed
	}
	var hdr [recordHeaderSize]byte
	rec := binary.AppendUvarint(make([]byte, 0, 10+len(payload)), id)
	rec = append(rec, payload...)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(rec)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.Update(crc32.Checksum([]byte{typ}, crcTable), crcTable, rec))
	hdr[8] = typ
	if _, err := e.w.Write(hdr[:]); err != nil {
		e.failed = err
		return err
	}
	if _, err := e.w.Write(rec); err != nil {
		e.failed = err
		return err
	}
	e.writeSeq++
	e.active().groups[id] = struct{}{}
	return nil
}

// maybeRotate seals the active segment when it is full and garbage
// collects old ones. Called with e.mu held, after a batch of writes.
func (e *LogEngine) maybeRotate() error {
	if e.off+int64(e.w.Buffered()) < e.opts.SegmentSize {
		return nil
	}
	if err := e.w.Flush(); err != nil {
		return err
	}
	old := e.active()
	if err := fdatasync(old.f, e.opts.NoSync); err != nil {
		return err
	}
	if err := e.newSegment(old.seq + 1); err != nil {
		return err
	}
	return e.gcLocked()
}

// gcLocked deletes segments that no group needs, checkpointing groups that
// pin the oldest segment when there are too many.
func (e *LogEngine) gcLocked() error {
	for len(e.segs) > 1 {
		oldest := e.segs[0]
		var pinned []uint64
		for id := range oldest.groups {
			if e.groups[id].resetSeg <= oldest.seq {
				pinned = append(pinned, id)
			}
		}
		if len(pinned) > 0 {
			if len(e.segs) <= e.opts.MaxSegments {
				return nil
			}
			sort.Slice(pinned, func(i, j int) bool { return pinned[i] < pinned[j] })
			for _, id := range pinned {
				if err := e.checkpointLocked(id); err != nil {
					return err
				}
			}
			if err := e.w.Flush(); err != nil {
				return err
			}
			if err := fdatasync(e.active().f, e.opts.NoSync); err != nil {
				return err
			}
		}
		oldest.f.Close()
		if err := os.Remove(oldest.path); err != nil {
			return err
		}
		e.segs = e.segs[1:]
	}
	return nil
}

// checkpointLocked rewrites a group's full log state into the active
// segment so that older segments no longer hold anything it needs.
func (e *LogEngine) checkpointLocked(id uint64) error {
	g := e.groups[id]
	return e.resetLocked(id, g.hs, g.sentinelIndex, g.sentinelTerm, append([]Entry(nil), g.entries...))
}

// Checkpoint forces every group's state into the active segment and
// garbage-collects all older segments. Exposed for tests and tooling.
func (e *LogEngine) Checkpoint() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for id := range e.groups {
		if err := e.checkpointLocked(id); err != nil {
			return err
		}
	}
	if err := e.w.Flush(); err != nil {
		return err
	}
	if err := fdatasync(e.active().f, e.opts.NoSync); err != nil {
		return err
	}
	return e.gcLocked()
}

func (e *LogEngine) resetLocked(id uint64, hs HardState, sentIdx, sentTerm uint64, entries []Entry) error {
	var sent [16]byte
	binary.LittleEndian.PutUint64(sent[0:8], sentIdx)
	binary.LittleEndian.PutUint64(sent[8:16], sentTerm)
	if err := e.write(id, recReset, sent[:]); err != nil {
		return err
	}
	g := e.groups[id]
	g.reset(sentIdx, sentTerm, e.active().seq)
	e.buf = encodeHardState(e.buf[:0], hs)
	if err := e.write(id, recHardState, e.buf); err != nil {
		return err
	}
	g.hs = hs
	return e.appendLocked(id, g, entries)
}

func (e *LogEngine) appendLocked(id uint64, g *engineGroup, entries []Entry) error {
	for _, ent := range entries {
		e.buf = encodeEntry(e.buf[:0], ent)
		if err := e.write(id, recEntry, e.buf); err != nil {
			return err
		}
		if err := g.appendEntry(ent); err != nil {
			return err
		}
	}
	return nil
}

// Sync makes every record written before the call durable. Concurrent
// callers share fdatasync calls (group commit).
func (e *LogEngine) Sync() error {
	e.mu.Lock()
	target := e.writeSeq
	if e.failed != nil {
		e.mu.Unlock()
		return e.failed
	}
	e.mu.Unlock()

	e.syncMu.Lock()
	defer e.syncMu.Unlock()
	for e.syncedSeq < target {
		if e.syncing {
			e.syncCond.Wait()
			continue
		}
		e.syncing = true
		e.syncMu.Unlock()
		err := e.syncOnce()
		e.syncMu.Lock()
		e.syncing = false
		e.syncCond.Broadcast()
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *LogEngine) syncOnce() error {
	e.mu.Lock()
	if err := e.w.Flush(); err != nil {
		e.failed = err
		e.mu.Unlock()
		return err
	}
	upTo := e.writeSeq
	f := e.active().f
	if err := e.maybeRotate(); err != nil {
		e.failed = err
		e.mu.Unlock()
		return err
	}
	rotated := e.active().f != f
	e.mu.Unlock()

	// fdatasync outside e.mu so other groups keep appending meanwhile. If
	// the segment was rotated, maybeRotate already synced it.
	if !rotated {
		if err := fdatasync(f, e.opts.NoSync); err != nil {
			return err
		}
	}
	e.syncMu.Lock()
	if upTo > e.syncedSeq {
		e.syncedSeq = upTo
	}
	e.syncMu.Unlock()
	return nil
}

// Close flushes and closes the engine.
func (e *LogEngine) Close() error {
	err := e.Sync()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	e.failed = errors.New("raft: log engine closed")
	e.closeFiles()
	return err
}

func (e *LogEngine) closeFiles() {
	for _, s := range e.segs {
		s.f.Close()
	}
}

// Segments returns the number of live segments (for tests and metrics).
func (e *LogEngine) Segments() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.segs)
}

func (e *LogEngine) snapPath(id uint64) string {
	return filepath.Join(e.dir, "snap", fmt.Sprintf("group-%d.snap", id))
}

// Storage returns the Storage view of one Raft group. Closing the view is
// a no-op; close the engine instead.
func (e *LogEngine) Storage(group uint64) Storage {
	e.mu.Lock()
	e.group(group)
	e.mu.Unlock()
	return &engineStorage{e: e, id: group}
}

type engineStorage struct {
	e  *LogEngine
	id uint64
}

func (s *engineStorage) InitialState() (PersistentState, error) {
	e := s.e
	e.mu.Lock()
	g := e.groups[s.id]
	st := PersistentState{
		HardState:     g.hs,
		SentinelIndex: g.sentinelIndex,
		SentinelTerm:  g.sentinelTerm,
		Entries:       append([]Entry(nil), g.entries...),
	}
	e.mu.Unlock()
	snap, err := readSnapshotFile(e.snapPath(s.id))
	if err != nil {
		return st, err
	}
	st.Snapshot = snap
	if snap == nil && st.SentinelIndex > 0 {
		return st, fmt.Errorf("%w: group %d compacted to %d but has no snapshot", ErrCorrupt, s.id, st.SentinelIndex)
	}
	if snap != nil && st.SentinelIndex == 0 && len(st.Entries) == 0 {
		st.SentinelIndex, st.SentinelTerm = snap.Meta.Index, snap.Meta.Term
	}
	return st, nil
}

func (s *engineStorage) SaveHardState(hs HardState) error {
	e := s.e
	e.mu.Lock()
	e.buf = encodeHardState(e.buf[:0], hs)
	err := e.write(s.id, recHardState, e.buf)
	if err == nil {
		e.groups[s.id].hs = hs
	}
	e.mu.Unlock()
	if err != nil {
		return err
	}
	return e.Sync()
}

func (s *engineStorage) Append(entries []Entry) error {
	e := s.e
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.appendLocked(s.id, e.groups[s.id], entries)
}

func (s *engineStorage) Sync() error { return s.e.Sync() }

func (s *engineStorage) Compact(hs HardState, sentinelIndex, sentinelTerm uint64, entries []Entry) error {
	e := s.e
	e.mu.Lock()
	err := e.resetLocked(s.id, hs, sentinelIndex, sentinelTerm, entries)
	e.mu.Unlock()
	if err != nil {
		return err
	}
	return e.Sync()
}

func (s *engineStorage) SaveSnapshot(snap Snapshot) error {
	e := s.e
	e.mu.Lock()
	g := e.groups[s.id]
	e.mu.Unlock()
	g.snapMu.Lock()
	defer g.snapMu.Unlock()
	if snap.Meta.Index <= g.snapIndex {
		return nil
	}
	if err := writeSnapshotFile(e.snapPath(s.id), snap, e.opts.NoSync); err != nil {
		return err
	}
	g.snapIndex = snap.Meta.Index
	return nil
}

func (s *engineStorage) LoadSnapshot() (*Snapshot, error) {
	e := s.e
	e.mu.Lock()
	g := e.groups[s.id]
	e.mu.Unlock()
	g.snapMu.Lock()
	defer g.snapMu.Unlock()
	return readSnapshotFile(e.snapPath(s.id))
}

func (s *engineStorage) Close() error { return nil }

// offsetWriter writes sequentially at *off using WriteAt, which keeps
// writes inside the preallocated region (no file size change, so
// fdatasync need not flush metadata).
type offsetWriter struct {
	f   *os.File
	off *int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(p, *w.off)
	*w.off += int64(n)
	return n, err
}
