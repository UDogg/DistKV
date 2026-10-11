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
	"sync"
)

// On-disk layout of a FileStorage directory:
//
//	raft.wal      append-only write-ahead log of CRC-protected records
//	snapshot.dat  most recent state machine snapshot (atomically replaced)
//
// WAL record framing (little endian):
//
//	+-----------+-----------+--------+-------------------+
//	| len (u32) | crc (u32) | type u8| payload (len B)   |
//	+-----------+-----------+--------+-------------------+
//
// The CRC (Castagnoli) covers the type byte and the payload. A torn or
// corrupt record at the tail of the WAL (the only place a crash can leave
// one) is detected on open and truncated away.
const (
	walFileName      = "raft.wal"
	snapshotFileName = "snapshot.dat"

	recHardState byte = 1
	recEntry     byte = 2
	recSentinel  byte = 3

	recordHeaderSize = 9
	maxRecordSize    = 64 << 20

	snapshotMagic uint32 = 0x444b5653 // "DKVS"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt is returned when durable state fails validation.
var ErrCorrupt = errors.New("raft: storage corrupt")

// FileStorageOptions configures a FileStorage.
type FileStorageOptions struct {
	// NoSync skips fsync calls. Data still reaches the OS page cache and
	// therefore survives process crashes, but not power loss. Suitable for
	// pure caching workloads; defaults to false (fully durable).
	NoSync bool
}

// FileStorage is a durable Storage backed by a write-ahead log file and a
// snapshot file.
type FileStorage struct {
	dir  string
	opts FileStorageOptions

	mu  sync.Mutex
	wal *os.File
	w   *bufio.Writer
	buf []byte

	snapMu    sync.Mutex
	snapIndex uint64
}

// OpenFileStorage opens (creating if necessary) a FileStorage in dir.
func OpenFileStorage(dir string, opts FileStorageOptions) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: create data dir: %w", err)
	}
	s := &FileStorage{dir: dir, opts: opts}
	return s, nil
}

// InitialState replays the WAL and loads the snapshot. It must be called
// exactly once, before any other method.
func (s *FileStorage) InitialState() (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st PersistentState

	snap, err := s.readSnapshotFile()
	if err != nil {
		return st, err
	}
	if snap != nil {
		st.Snapshot = snap
		st.SentinelIndex, st.SentinelTerm = snap.Meta.Index, snap.Meta.Term
		s.snapIndex = snap.Meta.Index
	}

	path := filepath.Join(s.dir, walFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return st, fmt.Errorf("raft: open wal: %w", err)
	}
	good, err := s.replay(f, &st)
	if err != nil {
		f.Close()
		return st, err
	}
	// Drop any torn tail left by a crash mid-write.
	if err := f.Truncate(good); err != nil {
		f.Close()
		return st, fmt.Errorf("raft: truncate wal: %w", err)
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return st, err
	}
	s.wal = f
	s.w = bufio.NewWriterSize(f, 256<<10)

	// A compacted WAL without a snapshot cannot rebuild the state machine.
	if snap == nil && st.SentinelIndex > 0 {
		return st, fmt.Errorf("%w: wal compacted to %d but no snapshot", ErrCorrupt, st.SentinelIndex)
	}
	return st, nil
}

// replay reads every valid record and returns the offset just past the
// last valid one.
func (s *FileStorage) replay(f *os.File, st *PersistentState) (int64, error) {
	r := bufio.NewReaderSize(f, 256<<10)
	var off int64
	hdr := make([]byte, recordHeaderSize)
	for {
		if _, err := io.ReadFull(r, hdr); err != nil {
			return off, nil // EOF or torn header
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		typ := hdr[8]
		if n > maxRecordSize {
			return off, nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return off, nil
		}
		crc := crc32.Update(crc32.Checksum([]byte{typ}, crcTable), crcTable, payload)
		if crc != sum {
			return off, nil
		}
		switch typ {
		case recHardState:
			hs, err := decodeHardState(payload)
			if err != nil {
				return off, nil
			}
			st.HardState = hs
		case recSentinel:
			if len(payload) != 16 {
				return off, nil
			}
			idx := binary.LittleEndian.Uint64(payload[0:8])
			term := binary.LittleEndian.Uint64(payload[8:16])
			// The node reconciles the WAL sentinel with the snapshot on
			// startup (see Node.restore).
			st.SentinelIndex, st.SentinelTerm = idx, term
			st.Entries = st.Entries[:0]
		case recEntry:
			e, err := decodeEntry(payload)
			if err != nil {
				return off, nil
			}
			if e.Index <= st.SentinelIndex {
				break
			}
			// Overwrite semantics: drop the conflicting suffix.
			for len(st.Entries) > 0 && st.Entries[len(st.Entries)-1].Index >= e.Index {
				st.Entries = st.Entries[:len(st.Entries)-1]
			}
			expected := st.SentinelIndex + uint64(len(st.Entries)) + 1
			if e.Index != expected {
				return off, fmt.Errorf("%w: wal gap: expected index %d, found %d", ErrCorrupt, expected, e.Index)
			}
			st.Entries = append(st.Entries, e)
		default:
			return off, fmt.Errorf("%w: unknown wal record type %d", ErrCorrupt, typ)
		}
		off += int64(recordHeaderSize) + int64(n)
	}
}

func (s *FileStorage) writeRecord(w io.Writer, typ byte, payload []byte) error {
	var hdr [recordHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	crc := crc32.Update(crc32.Checksum([]byte{typ}, crcTable), crcTable, payload)
	binary.LittleEndian.PutUint32(hdr[4:8], crc)
	hdr[8] = typ
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

// SaveHardState appends a hard state record and syncs.
func (s *FileStorage) SaveHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = encodeHardState(s.buf[:0], hs)
	if err := s.writeRecord(s.w, recHardState, s.buf); err != nil {
		return err
	}
	return s.syncLocked()
}

// Append buffers entries; call Sync to make them durable.
func (s *FileStorage) Append(entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		s.buf = encodeEntry(s.buf[:0], e)
		if err := s.writeRecord(s.w, recEntry, s.buf); err != nil {
			return err
		}
	}
	return nil
}

// Sync flushes buffered records and fsyncs the WAL.
func (s *FileStorage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncLocked()
}

func (s *FileStorage) syncLocked() error {
	if err := s.w.Flush(); err != nil {
		return err
	}
	if s.opts.NoSync {
		return nil
	}
	return s.wal.Sync()
}

// Compact atomically replaces the WAL with one containing only the given
// sentinel, hard state and entries.
func (s *FileStorage) Compact(hs HardState, sentinelIndex, sentinelTerm uint64, entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tmpPath := filepath.Join(s.dir, walFileName+".tmp")
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 256<<10)
	var sent [16]byte
	binary.LittleEndian.PutUint64(sent[0:8], sentinelIndex)
	binary.LittleEndian.PutUint64(sent[8:16], sentinelTerm)
	err = s.writeRecord(w, recSentinel, sent[:])
	if err == nil {
		s.buf = encodeHardState(s.buf[:0], hs)
		err = s.writeRecord(w, recHardState, s.buf)
	}
	for _, e := range entries {
		if err != nil {
			break
		}
		s.buf = encodeEntry(s.buf[:0], e)
		err = s.writeRecord(w, recEntry, s.buf)
	}
	if err == nil {
		err = w.Flush()
	}
	if err == nil && !s.opts.NoSync {
		err = f.Sync()
	}
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("raft: write compacted wal: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(s.dir, walFileName)); err != nil {
		f.Close()
		return fmt.Errorf("raft: install compacted wal: %w", err)
	}
	if err := s.syncDir(); err != nil {
		f.Close()
		return err
	}
	// Discard whatever was buffered for the old file: every entry in it is
	// either included in 'entries' or was deliberately dropped.
	s.wal.Close()
	s.wal = f
	s.w = bufio.NewWriterSize(f, 256<<10)
	return nil
}

// SaveSnapshot atomically writes snap unless a newer snapshot exists.
func (s *FileStorage) SaveSnapshot(snap Snapshot) error {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	if snap.Meta.Index <= s.snapIndex {
		return nil
	}
	if err := writeSnapshotFile(filepath.Join(s.dir, snapshotFileName), snap, s.opts.NoSync); err != nil {
		return err
	}
	s.snapIndex = snap.Meta.Index
	return nil
}

// LoadSnapshot reads the current snapshot, or returns nil if none exists.
func (s *FileStorage) LoadSnapshot() (*Snapshot, error) {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	return s.readSnapshotFile()
}

func (s *FileStorage) readSnapshotFile() (*Snapshot, error) {
	return readSnapshotFile(filepath.Join(s.dir, snapshotFileName))
}

func (s *FileStorage) syncDir() error { return syncDir(s.dir, s.opts.NoSync) }

// writeSnapshotFile atomically replaces path with snap (write to a
// temporary file, fsync, rename, fsync the directory).
func writeSnapshotFile(path string, snap Snapshot, noSync bool) error {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	var hdr [32]byte
	binary.LittleEndian.PutUint32(hdr[0:4], snapshotMagic)
	binary.LittleEndian.PutUint64(hdr[4:12], snap.Meta.Index)
	binary.LittleEndian.PutUint64(hdr[12:20], snap.Meta.Term)
	binary.LittleEndian.PutUint64(hdr[20:28], uint64(len(snap.Data)))
	binary.LittleEndian.PutUint32(hdr[28:32], crc32.Checksum(snap.Data, crcTable))
	w := bufio.NewWriterSize(f, 1<<20)
	_, err = w.Write(hdr[:])
	if err == nil {
		_, err = w.Write(snap.Data)
	}
	if err == nil {
		err = w.Flush()
	}
	if err == nil && !noSync {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("raft: write snapshot: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("raft: install snapshot: %w", err)
	}
	return syncDir(filepath.Dir(path), noSync)
}

// readSnapshotFile loads and validates a snapshot; nil if absent.
func readSnapshotFile(path string) (*Snapshot, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) < 32 || binary.LittleEndian.Uint32(data[0:4]) != snapshotMagic {
		return nil, fmt.Errorf("%w: bad snapshot header in %s", ErrCorrupt, path)
	}
	n := binary.LittleEndian.Uint64(data[20:28])
	if uint64(len(data)-32) != n {
		return nil, fmt.Errorf("%w: snapshot length mismatch in %s", ErrCorrupt, path)
	}
	body := data[32:]
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(data[28:32]) {
		return nil, fmt.Errorf("%w: snapshot checksum mismatch in %s", ErrCorrupt, path)
	}
	return &Snapshot{
		Meta: SnapshotMeta{
			Index: binary.LittleEndian.Uint64(data[4:12]),
			Term:  binary.LittleEndian.Uint64(data[12:20]),
		},
		Data: body,
	}, nil
}

func syncDir(dir string, noSync bool) error {
	if noSync {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Close flushes and closes the WAL.
func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return nil
	}
	err := s.syncLocked()
	if cerr := s.wal.Close(); err == nil {
		err = cerr
	}
	s.wal = nil
	return err
}

func encodeHardState(b []byte, hs HardState) []byte {
	b = binary.LittleEndian.AppendUint64(b, hs.Term)
	b = binary.LittleEndian.AppendUint64(b, hs.Commit)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(hs.VotedFor)))
	return append(b, hs.VotedFor...)
}

func decodeHardState(b []byte) (HardState, error) {
	if len(b) < 18 {
		return HardState{}, ErrCorrupt
	}
	hs := HardState{
		Term:   binary.LittleEndian.Uint64(b[0:8]),
		Commit: binary.LittleEndian.Uint64(b[8:16]),
	}
	n := int(binary.LittleEndian.Uint16(b[16:18]))
	if len(b) != 18+n {
		return HardState{}, ErrCorrupt
	}
	hs.VotedFor = NodeID(b[18:])
	return hs, nil
}

func encodeEntry(b []byte, e Entry) []byte {
	b = binary.LittleEndian.AppendUint64(b, e.Index)
	b = binary.LittleEndian.AppendUint64(b, e.Term)
	b = append(b, byte(e.Type))
	return append(b, e.Data...)
}

func decodeEntry(b []byte) (Entry, error) {
	if len(b) < 17 {
		return Entry{}, ErrCorrupt
	}
	e := Entry{
		Index: binary.LittleEndian.Uint64(b[0:8]),
		Term:  binary.LittleEndian.Uint64(b[8:16]),
		Type:  EntryType(b[16]),
	}
	if len(b) > 17 {
		e.Data = append([]byte(nil), b[17:]...)
	}
	return e, nil
}
