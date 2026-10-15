package raft

import (
	"context"
	"time"
)

// replicate is the per-follower replication goroutine. It sends at most one
// RPC at a time; batching (rather than pipelining) amortises round trips
// under load because every RPC carries all entries appended since the last
// one.
func (n *Node) replicate(peer NodeID, p *progress, stop <-chan struct{}, term uint64) {
	defer n.wg.Done()
	hb := time.NewTimer(0) // establish authority immediately
	defer hb.Stop()
	backoff := time.NewTimer(0)
	<-backoff.C
	defer backoff.Stop()

	for {
		select {
		case <-stop:
			return
		case <-n.stopCh:
			return
		case <-p.trigger:
		case <-hb.C:
		}
		for {
			ok, more := n.replicateOnce(peer, p, term)
			if !ok {
				// Peer unreachable: avoid hammering it, but stay
				// responsive to step-down.
				backoff.Reset(n.cfg.HeartbeatInterval)
				select {
				case <-stop:
					return
				case <-n.stopCh:
					return
				case <-backoff.C:
				}
				break
			}
			if !more {
				break
			}
			select {
			case <-stop:
				return
			default:
			}
		}
		hb.Reset(n.cfg.HeartbeatInterval)
	}
}

// replicateOnce sends a single AppendEntries (or InstallSnapshot) to peer.
// ok is false if the RPC failed; more is true if the follower still lags.
func (n *Node) replicateOnce(peer NodeID, p *progress, term uint64) (ok bool, more bool) {
	n.mu.Lock()
	if n.stopped || n.state != Leader || n.term != term {
		n.mu.Unlock()
		return true, false
	}
	round := n.readRound
	if sent, _ := n.rlog.sentinel(); p.next <= sent {
		n.mu.Unlock()
		return n.sendSnapshot(peer, p, term, round)
	}
	prev := p.next - 1
	prevTerm, _ := n.rlog.term(prev)
	req := &AppendEntriesRequest{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      n.rlog.window(p.next, n.cfg.MaxEntriesPerMsg, n.cfg.MaxBytesPerMsg),
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	sentAt := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
	resp, err := n.transport.AppendEntries(ctx, peer, req)
	cancel()
	if err != nil {
		return false, false
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, "")
		return true, false
	}
	if n.state != Leader || n.term != term {
		return true, false
	}
	n.recordAck(p, round, sentAt)
	if resp.Success {
		if resp.MatchIndex > p.match {
			p.match = resp.MatchIndex
			n.maybeAdvanceCommit()
		}
		p.next = p.match + 1
	} else {
		next := resp.ConflictIndex
		if resp.ConflictTerm != 0 {
			if li := n.rlog.lastIndexOfTerm(resp.ConflictTerm); li > 0 {
				next = li + 1
			}
		}
		next = max(next, p.match+1, 1)
		next = min(next, n.rlog.lastIndex()+1)
		p.next = next
		return true, true
	}
	return true, p.next <= n.rlog.lastIndex()
}

func (n *Node) sendSnapshot(peer NodeID, p *progress, term, round uint64) (bool, bool) {
	snap, err := n.storage.LoadSnapshot()
	if err != nil || snap == nil {
		n.logger.Error("cannot load snapshot for follower", "peer", string(peer), "err", err)
		return false, false
	}
	req := &InstallSnapshotRequest{Term: term, LeaderID: n.id, Meta: snap.Meta, Data: snap.Data}
	sentAt := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 20*n.cfg.ElectionTimeout)
	resp, err := n.transport.InstallSnapshot(ctx, peer, req)
	cancel()
	if err != nil {
		n.logger.Warn("install snapshot failed", "peer", string(peer), "err", err)
		return false, false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, "")
		return true, false
	}
	if n.state != Leader || n.term != term {
		return true, false
	}
	n.recordAck(p, round, sentAt)
	if snap.Meta.Index > p.match {
		p.match = snap.Meta.Index
		n.maybeAdvanceCommit()
	}
	p.next = p.match + 1
	n.logger.Info("sent snapshot to follower", "peer", string(peer), "index", snap.Meta.Index)
	return true, p.next <= n.rlog.lastIndex()
}

// HandleAppendEntries processes an AppendEntries RPC from a leader.
func (n *Node) HandleAppendEntries(req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	resp := &AppendEntriesResponse{}
	if req.Term < n.term {
		resp.Term = n.term
		return resp, nil
	}
	if req.Term > n.term || n.state != Follower {
		n.becomeFollower(req.Term, req.LeaderID)
	}
	n.leader = req.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionTimer()
	resp.Term = n.term

	prev := req.PrevLogIndex
	entries := req.Entries
	if sent, _ := n.rlog.sentinel(); prev < sent {
		// Everything up to the sentinel is committed and therefore
		// identical to the leader's log; skip it.
		skip := sent - prev
		if uint64(len(entries)) <= skip {
			entries = nil
		} else {
			entries = entries[skip:]
		}
		prev = sent
	} else {
		if prev > n.rlog.lastIndex() {
			resp.ConflictIndex = n.rlog.lastIndex() + 1
			return resp, nil
		}
		if t, _ := n.rlog.term(prev); t != req.PrevLogTerm {
			resp.ConflictTerm = t
			resp.ConflictIndex = n.rlog.firstIndexOfTerm(t, prev)
			return resp, nil
		}
	}

	var toAppend []Entry
	for i, e := range entries {
		if e.Index > n.rlog.lastIndex() {
			toAppend = entries[i:]
			break
		}
		if t, _ := n.rlog.term(e.Index); t != e.Term {
			if e.Index <= n.commitIndex {
				n.fatal(errCommittedConflict(e.Index))
			}
			n.rlog.truncateFrom(e.Index)
			n.logEpoch++
			n.durableIndex = min(n.durableIndex, e.Index-1)
			toAppend = entries[i:]
			break
		}
	}
	if len(toAppend) > 0 {
		n.rlog.append(toAppend...)
		if err := n.storage.Append(toAppend); err != nil {
			n.fatal(err)
		}
	}
	lastNew := prev + uint64(len(entries))

	// Make everything we are about to acknowledge durable. The sync runs
	// without holding mu so that other RPCs for this group (and the
	// applier) are not stalled behind the disk; the storage layer
	// coalesces concurrent syncs from all groups into one fdatasync.
	for lastNew > n.durableIndex {
		epoch, target, term := n.logEpoch, n.rlog.lastIndex(), n.term
		n.mu.Unlock()
		err := n.storage.Sync()
		n.mu.Lock()
		if err != nil {
			n.fatal(err)
		}
		if n.stopped {
			return nil, ErrStopped
		}
		if n.term != term {
			// A newer leader appeared while we were syncing; let the
			// old one learn about it.
			return &AppendEntriesResponse{Term: n.term, ConflictIndex: n.rlog.lastIndex() + 1}, nil
		}
		if n.logEpoch == epoch && target > n.durableIndex {
			n.durableIndex = target
		}
	}
	if len(entries) > 0 {
		if t, ok := n.rlog.term(lastNew); !ok || t != entries[len(entries)-1].Term {
			return &AppendEntriesResponse{Term: n.term, ConflictIndex: n.rlog.lastIndex() + 1}, nil
		}
	}

	if req.LeaderCommit > n.commitIndex {
		if c := min(req.LeaderCommit, lastNew); c > n.commitIndex {
			n.commitIndex = c
			n.applyCond.Signal()
		}
	}
	resp.Success = true
	resp.MatchIndex = lastNew
	return resp, nil
}

type errCommittedConflict uint64

func (e errCommittedConflict) Error() string {
	return "leader attempted to overwrite committed entry " + itoa(uint64(e))
}

func itoa(v uint64) string {
	var b [20]byte
	i := len(b)
	for {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
		if v == 0 {
			return string(b[i:])
		}
	}
}

// HandleRequestVote processes a (pre-)vote request.
func (n *Node) HandleRequestVote(req *RequestVoteRequest) (*RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	// Leader stickiness: while we believe a leader is alive, refuse to help
	// depose it unless it asked us to (leadership transfer).
	now := time.Now()
	inLease := !n.cfg.DisableCheckQuorum && !req.LeadershipTransfer &&
		(n.state == Leader || now.Before(n.voteFreezeUntil) ||
			(n.leader != "" && now.Sub(n.lastLeaderContact) < n.cfg.ElectionTimeout))
	upToDate := n.rlog.isUpToDate(req.LastLogIndex, req.LastLogTerm)

	if req.PreVote {
		return &RequestVoteResponse{
			Term:    n.term,
			Granted: req.Term > n.term && upToDate && !inLease,
		}, nil
	}
	if inLease || req.Term < n.term {
		return &RequestVoteResponse{Term: n.term}, nil
	}
	if req.Term > n.term {
		n.becomeFollower(req.Term, "")
	}
	if (n.votedFor == "" || n.votedFor == req.CandidateID) && upToDate {
		n.votedFor = req.CandidateID
		n.persistHardState()
		n.resetElectionTimer()
		return &RequestVoteResponse{Term: n.term, Granted: true}, nil
	}
	return &RequestVoteResponse{Term: n.term}, nil
}

// HandleInstallSnapshot processes an InstallSnapshot RPC.
func (n *Node) HandleInstallSnapshot(req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if req.Term < n.term {
		return &InstallSnapshotResponse{Term: n.term}, nil
	}
	if req.Term > n.term || n.state != Follower {
		n.becomeFollower(req.Term, req.LeaderID)
	}
	n.leader = req.LeaderID
	n.lastLeaderContact = time.Now()
	n.resetElectionTimer()
	resp := &InstallSnapshotResponse{Term: n.term}
	if req.Meta.Index <= n.commitIndex {
		return resp, nil
	}
	snap := Snapshot{Meta: req.Meta, Data: req.Data}
	if err := n.storage.SaveSnapshot(snap); err != nil {
		n.fatal(err)
	}
	if t, ok := n.rlog.term(snap.Meta.Index); ok && t == snap.Meta.Term {
		n.rlog.compact(snap.Meta.Index)
	} else {
		n.rlog.reset(snap.Meta.Index, snap.Meta.Term)
		n.logEpoch++
	}
	n.snapIndex, n.snapTerm = snap.Meta.Index, snap.Meta.Term
	n.commitIndex = snap.Meta.Index
	n.compactStorage()
	n.pendingSnapshot = &snap
	n.stats.SnapshotsInstalled++
	n.applyCond.Signal()
	return resp, nil
}

// HandleTimeoutNow makes this node campaign immediately (leadership
// transfer target).
func (n *Node) HandleTimeoutNow(req *TimeoutNowRequest) (*TimeoutNowResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return nil, ErrStopped
	}
	if req.Term < n.term {
		return &TimeoutNowResponse{Term: n.term}, nil
	}
	if req.Term > n.term {
		n.becomeFollower(req.Term, req.LeaderID)
	}
	if n.state != Leader {
		n.logger.Info("received timeout-now; campaigning", "from", string(req.LeaderID), "term", n.term)
		n.campaign(false, true)
	}
	return &TimeoutNowResponse{Term: n.term}, nil
}

// maybeSnapshot takes a snapshot when enough entries have been applied
// since the last one. Runs on the applier goroutine so the state machine
// is quiescent at lastApplied.
func (n *Node) maybeSnapshot() {
	n.mu.Lock()
	if n.cfg.SnapshotThreshold == 0 || n.pendingSnapshot != nil ||
		n.lastApplied-n.snapIndex < n.cfg.SnapshotThreshold {
		n.mu.Unlock()
		return
	}
	idx := n.lastApplied
	t, ok := n.rlog.term(idx)
	n.mu.Unlock()
	if !ok {
		return
	}
	data, err := n.fsm.Snapshot()
	if err != nil {
		n.logger.Error("state machine snapshot failed", "err", err)
		return
	}
	snap := Snapshot{Meta: SnapshotMeta{Index: idx, Term: t}, Data: data}
	if err := n.storage.SaveSnapshot(snap); err != nil {
		n.fatal(err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if idx <= n.snapIndex {
		return // a newer snapshot was installed meanwhile
	}
	n.snapIndex, n.snapTerm = idx, t
	n.stats.SnapshotsTaken++
	if idx > n.cfg.SnapshotTrailing {
		if compactTo := idx - n.cfg.SnapshotTrailing; compactTo > n.rlog.entries[0].Index {
			n.rlog.compact(compactTo)
			n.compactStorage()
		}
	}
	n.logger.Debug("took snapshot", "index", idx, "bytes", len(data))
}

// compactStorage rewrites the durable log to match the in-memory one.
// Compact syncs, so afterwards the entire log is durable.
func (n *Node) compactStorage() {
	sentIdx, sentTerm := n.rlog.sentinel()
	entries := n.rlog.slice(n.rlog.firstIndex(), n.rlog.lastIndex()+1)
	if err := n.storage.Compact(n.hardState(), sentIdx, sentTerm, entries); err != nil {
		n.fatal(err)
	}
	n.durableIndex = n.rlog.lastIndex()
	// The leader counts its own durable entries towards the quorum, and this
	// may be what completes one (a concurrent sync loop that finds nothing
	// left to sync relies on it).
	n.maybeAdvanceCommit()
}
