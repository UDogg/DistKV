package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"
)

// StateMachine is the application driven by the replicated log.
//
// Apply is invoked sequentially, in log order, from a single goroutine, for
// every committed EntryNormal entry. Its return value is delivered to the
// proposer (if the proposal originated on this node). Snapshot and Restore
// are invoked from the same goroutine, so implementations need no
// additional synchronisation between them and Apply.
type StateMachine interface {
	Apply(e Entry) any
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// Transport delivers RPCs to other members of the group.
type Transport interface {
	RequestVote(ctx context.Context, to NodeID, req *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(ctx context.Context, to NodeID, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, to NodeID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
	TimeoutNow(ctx context.Context, to NodeID, req *TimeoutNowRequest) (*TimeoutNowResponse, error)
}

// Config configures a Node.
type Config struct {
	// ID of this node. Must appear in Peers.
	ID NodeID
	// Peers lists every voting member of the group, including ID.
	Peers []NodeID

	// HeartbeatInterval is how often an idle leader contacts followers.
	HeartbeatInterval time.Duration
	// ElectionTimeout is the base election timeout; the effective timeout
	// is randomised in [ElectionTimeout, 2*ElectionTimeout).
	ElectionTimeout time.Duration
	// TickInterval is the resolution of the election timer.
	TickInterval time.Duration

	// MaxEntriesPerMsg and MaxBytesPerMsg bound AppendEntries batches.
	MaxEntriesPerMsg int
	MaxBytesPerMsg   int

	// SnapshotThreshold is the number of applied entries since the last
	// snapshot that triggers a new one. Zero disables snapshotting.
	SnapshotThreshold uint64
	// SnapshotTrailing is the number of already-snapshotted entries kept
	// in the log so that slightly lagging followers can catch up without a
	// full snapshot transfer.
	SnapshotTrailing uint64

	// DisablePreVote turns off the pre-vote phase (not recommended).
	DisablePreVote bool
	// DisableCheckQuorum stops leaders from stepping down when they lose
	// contact with a quorum (not recommended).
	DisableCheckQuorum bool

	Storage      Storage
	Transport    Transport
	StateMachine StateMachine
	Logger       *slog.Logger
}

func (c *Config) withDefaults() (Config, error) {
	cfg := *c
	if cfg.ID == "" {
		return cfg, errors.New("raft: config: ID is required")
	}
	found := false
	seen := map[NodeID]bool{}
	for _, p := range cfg.Peers {
		if seen[p] {
			return cfg, fmt.Errorf("raft: config: duplicate peer %q", p)
		}
		seen[p] = true
		if p == cfg.ID {
			found = true
		}
	}
	if !found {
		return cfg, fmt.Errorf("raft: config: ID %q not in peers", cfg.ID)
	}
	if cfg.Storage == nil || cfg.Transport == nil || cfg.StateMachine == nil {
		return cfg, errors.New("raft: config: Storage, Transport and StateMachine are required")
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.ElectionTimeout <= 0 {
		cfg.ElectionTimeout = 10 * cfg.HeartbeatInterval
	}
	if cfg.ElectionTimeout < 2*cfg.HeartbeatInterval {
		return cfg, errors.New("raft: config: ElectionTimeout must be at least 2x HeartbeatInterval")
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = cfg.HeartbeatInterval / 5
		if cfg.TickInterval < time.Millisecond {
			cfg.TickInterval = time.Millisecond
		}
	}
	if cfg.MaxEntriesPerMsg <= 0 {
		cfg.MaxEntriesPerMsg = 1024
	}
	if cfg.MaxBytesPerMsg <= 0 {
		cfg.MaxBytesPerMsg = 4 << 20
	}
	if cfg.SnapshotTrailing == 0 {
		cfg.SnapshotTrailing = 1024
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return cfg, nil
}
