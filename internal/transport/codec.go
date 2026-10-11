package transport

import (
	"encoding/binary"
	"errors"

	"github.com/UDogg/DistKV/internal/raft"
)

// A compact, allocation-light binary codec for Raft messages. Every message
// starts with a one-byte codec version so the format can evolve. Integers
// are uvarints; strings and byte slices are length-prefixed.
//
// (An earlier revision used encoding/gob; constructing a gob decoder per
// request dominated CPU profiles at 10k ops/s.)

const codecVersion = 1

var errCodec = errors.New("transport: malformed message")

type wbuf []byte

func (b wbuf) u(v uint64) wbuf        { return binary.AppendUvarint(b, v) }
func (b wbuf) bool(v bool) wbuf       { return append(b, b2u(v)) }
func (b wbuf) str(s string) wbuf      { return append(b.u(uint64(len(s))), s...) }
func (b wbuf) bytes(p []byte) wbuf    { return append(b.u(uint64(len(p))), p...) }
func (b wbuf) id(id raft.NodeID) wbuf { return b.str(string(id)) }

func b2u(v bool) byte {
	if v {
		return 1
	}
	return 0
}

type rbuf struct {
	b   []byte
	err bool
}

func (r *rbuf) u() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *rbuf) bool() bool {
	if len(r.b) < 1 {
		r.err = true
		return false
	}
	v := r.b[0] == 1
	r.b = r.b[1:]
	return v
}

func (r *rbuf) raw() []byte {
	n := r.u()
	if r.err || n > uint64(len(r.b)) {
		r.err = true
		return nil
	}
	v := r.b[:n:n]
	r.b = r.b[n:]
	return v
}

func (r *rbuf) id() raft.NodeID { return raft.NodeID(r.raw()) }

func (r *rbuf) done() error {
	if r.err || len(r.b) != 0 {
		return errCodec
	}
	return nil
}

func header(b []byte) wbuf { return append(b, codecVersion) }

func open(data []byte) *rbuf {
	if len(data) < 1 || data[0] != codecVersion {
		return &rbuf{err: true}
	}
	return &rbuf{b: data[1:]}
}

// marshal appends the encoding of msg to b.
func marshal(b []byte, msg any) ([]byte, error) {
	w := header(b)
	switch m := msg.(type) {
	case *raft.RequestVoteRequest:
		w = w.u(m.Term).id(m.CandidateID).u(m.LastLogIndex).u(m.LastLogTerm).bool(m.PreVote).bool(m.LeadershipTransfer)
	case *raft.RequestVoteResponse:
		w = w.u(m.Term).bool(m.Granted)
	case *raft.AppendEntriesRequest:
		w = w.u(m.Term).id(m.LeaderID).u(m.PrevLogIndex).u(m.PrevLogTerm).u(m.LeaderCommit).u(uint64(len(m.Entries)))
		for i := range m.Entries {
			e := &m.Entries[i]
			w = w.u(e.Index).u(e.Term).u(uint64(e.Type)).bytes(e.Data)
		}
	case *raft.AppendEntriesResponse:
		w = w.u(m.Term).bool(m.Success).u(m.MatchIndex).u(m.ConflictTerm).u(m.ConflictIndex)
	case *raft.InstallSnapshotRequest:
		w = w.u(m.Term).id(m.LeaderID).u(m.Meta.Index).u(m.Meta.Term).bytes(m.Data)
	case *raft.InstallSnapshotResponse:
		w = w.u(m.Term)
	case *raft.TimeoutNowRequest:
		w = w.u(m.Term).id(m.LeaderID)
	case *raft.TimeoutNowResponse:
		w = w.u(m.Term)
	default:
		return nil, errors.New("transport: cannot marshal message type")
	}
	return w, nil
}

// unmarshal decodes data into msg (a pointer to one of the raft message
// types). Byte slices in the result alias data.
func unmarshal(data []byte, msg any) error {
	r := open(data)
	switch m := msg.(type) {
	case *raft.RequestVoteRequest:
		m.Term, m.CandidateID, m.LastLogIndex, m.LastLogTerm = r.u(), r.id(), r.u(), r.u()
		m.PreVote, m.LeadershipTransfer = r.bool(), r.bool()
	case *raft.RequestVoteResponse:
		m.Term, m.Granted = r.u(), r.bool()
	case *raft.AppendEntriesRequest:
		m.Term, m.LeaderID, m.PrevLogIndex, m.PrevLogTerm, m.LeaderCommit = r.u(), r.id(), r.u(), r.u(), r.u()
		n := r.u()
		if n > uint64(len(r.b)) { // each entry needs at least one byte
			return errCodec
		}
		if n > 0 {
			m.Entries = make([]raft.Entry, n)
			for i := range m.Entries {
				e := &m.Entries[i]
				e.Index, e.Term, e.Type = r.u(), r.u(), raft.EntryType(r.u())
				if d := r.raw(); len(d) > 0 {
					e.Data = d
				}
			}
		}
	case *raft.AppendEntriesResponse:
		m.Term, m.Success, m.MatchIndex, m.ConflictTerm, m.ConflictIndex = r.u(), r.bool(), r.u(), r.u(), r.u()
	case *raft.InstallSnapshotRequest:
		m.Term, m.LeaderID, m.Meta.Index, m.Meta.Term = r.u(), r.id(), r.u(), r.u()
		m.Data = r.raw()
	case *raft.InstallSnapshotResponse:
		m.Term = r.u()
	case *raft.TimeoutNowRequest:
		m.Term, m.LeaderID = r.u(), r.id()
	case *raft.TimeoutNowResponse:
		m.Term = r.u()
	default:
		return errors.New("transport: cannot unmarshal message type")
	}
	return r.done()
}
