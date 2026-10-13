// Package raft implements the Raft consensus algorithm as described in
// "In Search of an Understandable Consensus Algorithm (Extended Version)"
// by Ongaro and Ousterhout, together with the production extensions from
// Ongaro's PhD thesis that DistKV relies on:
//
//   - Pre-vote (§9.6) so that a partitioned node rejoining the cluster cannot
//     disrupt a healthy leader by inflating the term.
//   - Leader stickiness / check-quorum (§6.2): followers ignore vote requests
//     while they hear from a live leader, and a leader that cannot reach a
//     quorum steps down on its own.
//   - ReadIndex linearizable reads (§6.4) batched per heartbeat round.
//   - Log compaction with snapshots and InstallSnapshot (§5).
//   - Leadership transfer via TimeoutNow (§3.10).
//   - Fast log back-tracking using conflict term/index hints.
//
// The package is transport- and storage-agnostic: callers supply a Transport
// (DistKV uses HTTP, tests use an in-memory network that can partition and
// drop messages) and a Storage (file-backed WAL or in-memory).
package raft

import (
	"errors"
	"fmt"
)

// NodeID uniquely identifies a member of a Raft group.
type NodeID string

// State is the role a node currently plays in the group.
type State int

const (
	Follower State = iota
	PreCandidate
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case PreCandidate:
		return "pre-candidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("state(%d)", int(s))
	}
}

// EntryType distinguishes client commands from internal entries.
type EntryType uint8

const (
	// EntryNormal carries an opaque state machine command.
	EntryNormal EntryType = iota
	// EntryNoop is appended by every new leader so that entries from
	// previous terms can be committed (Raft §5.4.2) and so that ReadIndex
	// has a committed entry from the current term.
	EntryNoop
)

// Entry is a single record in the replicated log.
type Entry struct {
	Index uint64
	Term  uint64
	Type  EntryType
	Data  []byte
}

// HardState is the state that must be durable before a node responds to
// any RPC.
type HardState struct {
	Term     uint64
	VotedFor NodeID
	Commit   uint64
}

// SnapshotMeta identifies the last log entry covered by a snapshot.
type SnapshotMeta struct {
	Index uint64
	Term  uint64
}

// Snapshot is a point-in-time image of the state machine.
type Snapshot struct {
	Meta SnapshotMeta
	Data []byte
}

// RequestVoteRequest is sent by (pre-)candidates to gather votes.
type RequestVoteRequest struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
	// PreVote marks a pre-vote probe. Pre-votes never mutate the voter's
	// term or vote; they only report whether a real vote would be granted.
	PreVote bool
	// LeadershipTransfer is set when the election was triggered by
	// TimeoutNow; voters then disregard leader stickiness.
	LeadershipTransfer bool
}

// RequestVoteResponse answers a RequestVoteRequest.
type RequestVoteResponse struct {
	Term    uint64
	Granted bool
}

// AppendEntriesRequest replicates log entries and doubles as a heartbeat.
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64
}

// AppendEntriesResponse answers an AppendEntriesRequest.
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
	// MatchIndex is the index of the last entry known to match the
	// leader's log when Success is true.
	MatchIndex uint64
	// ConflictTerm/ConflictIndex accelerate log back-tracking when Success
	// is false. ConflictTerm is zero when the follower's log is too short.
	ConflictTerm  uint64
	ConflictIndex uint64
}

// InstallSnapshotRequest ships a full snapshot to a follower that has
// fallen behind the leader's compacted log.
type InstallSnapshotRequest struct {
	Term     uint64
	LeaderID NodeID
	Meta     SnapshotMeta
	Data     []byte
}

// InstallSnapshotResponse answers an InstallSnapshotRequest.
type InstallSnapshotResponse struct {
	Term uint64
}

// TimeoutNowRequest asks a follower to start an election immediately. It
// is the final step of a leadership transfer.
type TimeoutNowRequest struct {
	Term     uint64
	LeaderID NodeID
}

// TimeoutNowResponse answers a TimeoutNowRequest.
type TimeoutNowResponse struct {
	Term uint64
}

var (
	// ErrNotLeader is returned when an operation requires leadership.
	ErrNotLeader = errors.New("raft: not the leader")
	// ErrStopped is returned after Stop has been called.
	ErrStopped = errors.New("raft: node stopped")
	// ErrLeadershipLost is returned to proposers whose entry was appended
	// but whose leader stepped down before it committed. The entry may or
	// may not eventually commit; callers should retry idempotently.
	ErrLeadershipLost = errors.New("raft: leadership lost; outcome unknown")
	// ErrProposalDropped is returned when the proposed entry was
	// overwritten by a different leader and will never be applied.
	ErrProposalDropped = errors.New("raft: proposal dropped")
	// ErrTransferInProgress is returned by TransferLeadership when another
	// transfer is already running.
	ErrTransferInProgress = errors.New("raft: leadership transfer in progress")
	// ErrTimeout is returned when an internal deadline elapses.
	ErrTimeout = errors.New("raft: timed out")
)
