package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"slices"
	"sync"
	"time"
)

// Node is a single member of a Raft group.
//
// Concurrency model: all consensus state is guarded by mu. Network I/O is
// never performed while holding mu. Background goroutines:
//
//   - ticker:      drives election timeouts and check-quorum
//   - applier:     applies committed entries / snapshots to the StateMachine
//   - syncer:      leader-side group commit (batched fsync of the local log)
//   - readLoop:    batches ReadIndex requests into heartbeat rounds
//   - replicators: one per follower while leader; pipelines AppendEntries
type Node struct {
	cfg       Config
	id        NodeID
	others    []NodeID
	quorum    int
	logger    *slog.Logger
	storage   Storage
	transport Transport
	fsm       StateMachine

	mu          sync.Mutex
	state       State
	term        uint64
	votedFor    NodeID
	leader      NodeID
	rlog        *raftLog
	commitIndex uint64
	lastApplied uint64
	// durableIndex is the highest local log index known to be synced. The
	// leader counts itself towards a quorum only up to this index, and a
	// follower never acknowledges entries beyond it.
	durableIndex uint64
	// logEpoch increments whenever entries are truncated, so code that
	// synced storage without holding mu can tell whether what it synced is
	// still the log.
	logEpoch  uint64
	snapIndex uint64
	snapTerm  uint64

	electionDeadline  time.Time
	lastLeaderContact time.Time
	electionSeq       uint64
	rng               *rand.Rand

	// Leader-only state.
	progress        map[NodeID]*progress
	leaderStop      chan struct{}
	leaderReady     chan struct{}
	readyClosed     bool
	noopIndex       uint64
	lastQuorumCheck time.Time
	transferee      NodeID
	// leaseRevoked is set once this leader has sent TimeoutNow: the
	// transfer target may be elected without waiting out our lease.
	leaseRevoked bool
	// voteFreezeUntil: after (re)starting we do not remember when we last
	// heard from a leader, so we refuse votes for one election timeout to
	// preserve any lease we may have granted before the restart.
	voteFreezeUntil time.Time

	// ReadIndex state.
	readRound    uint64
	pendingReads []*readRequest
	roundWait    uint64
	roundWaitCh  chan struct{}

	waiters         map[uint64]*proposal
	appliedCh       chan struct{}
	applyCond       *sync.Cond
	pendingSnapshot *Snapshot

	readTrigger chan struct{}
	syncTrigger chan struct{}
	started     bool
	stopped     bool
	stopCh      chan struct{}
	wg          sync.WaitGroup

	stats Stats
}

type progress struct {
	next        uint64
	match       uint64
	lastContact time.Time
	// ackedSentAt is the send time of the most recent RPC this follower
	// acknowledged in the current term; it anchors the leader lease.
	ackedSentAt time.Time
	readAck     uint64
	trigger     chan struct{}
}

type proposal struct {
	index uint64
	term  uint64
	done  chan proposalResult
}

type proposalResult struct {
	value any
	err   error
}

type readRequest struct {
	done chan error
}

// Stats are cumulative counters exposed for monitoring.
type Stats struct {
	ElectionsStarted   uint64
	LeadershipsWon     uint64
	SnapshotsTaken     uint64
	SnapshotsInstalled uint64
	ProposalsCommitted uint64
	ReadIndexRounds    uint64
	LeaseReads         uint64
}

// PeerStatus is the leader's view of a follower.
type PeerStatus struct {
	Match       uint64
	Next        uint64
	LastContact time.Duration
}

// Status is a point-in-time summary of a node.
type Status struct {
	ID            NodeID
	State         State
	Term          uint64
	Leader        NodeID
	FirstIndex    uint64
	LastIndex     uint64
	CommitIndex   uint64
	AppliedIndex  uint64
	SnapshotIndex uint64
	Peers         map[NodeID]PeerStatus
	Stats         Stats
}

// NewNode creates a node and recovers its durable state. Call Start to
// begin participating in the group.
func NewNode(c Config) (*Node, error) {
	cfg, err := c.withDefaults()
	if err != nil {
		return nil, err
	}
	st, err := cfg.Storage.InitialState()
	if err != nil {
		return nil, fmt.Errorf("raft: load state: %w", err)
	}
	n := &Node{
		cfg:         cfg,
		id:          cfg.ID,
		quorum:      len(cfg.Peers)/2 + 1,
		logger:      cfg.Logger.With("node", string(cfg.ID)),
		storage:     cfg.Storage,
		transport:   cfg.Transport,
		fsm:         cfg.StateMachine,
		state:       Follower,
		term:        st.HardState.Term,
		votedFor:    st.HardState.VotedFor,
		rlog:        newRaftLog(st.SentinelIndex, st.SentinelTerm, st.Entries),
		rng:         rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(len(cfg.ID))<<32)),
		waiters:     make(map[uint64]*proposal),
		appliedCh:   make(chan struct{}),
		readTrigger: make(chan struct{}, 1),
		syncTrigger: make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
	n.applyCond = sync.NewCond(&n.mu)
	for _, p := range cfg.Peers {
		if p != cfg.ID {
			n.others = append(n.others, p)
		}
	}

	if snap := st.Snapshot; snap != nil {
		if sent, _ := n.rlog.sentinel(); sent > snap.Meta.Index {
			return nil, fmt.Errorf("%w: log compacted to %d beyond snapshot %d", ErrCorrupt, sent, snap.Meta.Index)
		}
		// Keep the log only if it is consistent with the snapshot (it
		// may not be if we crashed while installing a leader's snapshot).
		if t, ok := n.rlog.term(snap.Meta.Index); !ok || t != snap.Meta.Term {
			n.rlog.reset(snap.Meta.Index, snap.Meta.Term)
		}
		if err := n.fsm.Restore(snap.Data); err != nil {
			return nil, fmt.Errorf("raft: restore snapshot: %w", err)
		}
		n.snapIndex, n.snapTerm = snap.Meta.Index, snap.Meta.Term
		n.lastApplied = snap.Meta.Index
		n.commitIndex = snap.Meta.Index
	}
	if c := min(st.HardState.Commit, n.rlog.lastIndex()); c > n.commitIndex {
		n.commitIndex = c
	}
	n.durableIndex = n.rlog.lastIndex()
	return n, nil
}

// Start launches the node's background goroutines.
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started || n.stopped {
		return
	}
	n.started = true
	n.resetElectionTimer()
	n.voteFreezeUntil = time.Now().Add(n.cfg.ElectionTimeout)
	n.wg.Add(4)
	go n.tickLoop()
	go n.applyLoop()
	go n.syncLoop()
	go n.readLoop()
	n.logger.Info("raft node started", "term", n.term, "last_index", n.rlog.lastIndex(),
		"commit", n.commitIndex, "snapshot", n.snapIndex, "peers", len(n.cfg.Peers))
}

// Stop halts the node and waits for its goroutines to exit. It does not
// close the Storage.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	if n.state == Leader {
		n.stepDownLeader(ErrStopped)
	}
	n.state = Follower
	for idx, w := range n.waiters {
		w.done <- proposalResult{err: ErrStopped}
		delete(n.waiters, idx)
	}
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
}

// ID returns the node's identifier.
func (n *Node) ID() NodeID { return n.id }

// Leader returns the current leader, or "" if unknown.
func (n *Node) Leader() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// IsLeader reports whether this node currently believes it is the leader.
func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader
}

// Status returns a snapshot of the node's state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := Status{
		ID:            n.id,
		State:         n.state,
		Term:          n.term,
		Leader:        n.leader,
		FirstIndex:    n.rlog.firstIndex(),
		LastIndex:     n.rlog.lastIndex(),
		CommitIndex:   n.commitIndex,
		AppliedIndex:  n.lastApplied,
		SnapshotIndex: n.snapIndex,
		Stats:         n.stats,
	}
	if n.state == Leader {
		now := time.Now()
		s.Peers = make(map[NodeID]PeerStatus, len(n.progress))
		for id, p := range n.progress {
			s.Peers[id] = PeerStatus{Match: p.match, Next: p.next, LastContact: now.Sub(p.lastContact)}
		}
	}
	return s
}

// Propose replicates data through the log and returns the value produced
// by StateMachine.Apply once the entry is committed and applied locally.
func (n *Node) Propose(ctx context.Context, data []byte) (any, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, ErrStopped
	}
	if n.state != Leader || n.transferee != "" {
		n.mu.Unlock()
		return nil, ErrNotLeader
	}
	e := Entry{Index: n.rlog.lastIndex() + 1, Term: n.term, Type: EntryNormal, Data: data}
	n.rlog.append(e)
	if err := n.storage.Append([]Entry{e}); err != nil {
		n.fatal(err)
	}
	p := &proposal{index: e.Index, term: e.Term, done: make(chan proposalResult, 1)}
	n.waiters[e.Index] = p
	n.signal(n.syncTrigger)
	n.triggerReplication()
	n.mu.Unlock()

	select {
	case r := <-p.done:
		return r.value, r.err
	case <-ctx.Done():
		n.mu.Lock()
		if n.waiters[e.Index] == p {
			delete(n.waiters, e.Index)
		}
		n.mu.Unlock()
		return nil, ctx.Err()
	}
}

// ReadIndex returns a commit index that is safe to serve linearizable
// reads from once the local state machine has applied it. It confirms
// leadership with a quorum heartbeat round (batched across callers).
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	for {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return 0, ErrStopped
		}
		if n.state != Leader {
			n.mu.Unlock()
			return 0, ErrNotLeader
		}
		if n.readyClosed {
			break
		}
		// The leader must have committed an entry from its own term
		// before its commit index is known to be up to date.
		ready, stop := n.leaderReady, n.leaderStop
		n.mu.Unlock()
		select {
		case <-ready:
		case <-stop:
			return 0, ErrNotLeader
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	idx := n.commitIndex
	if n.quorum == 1 {
		n.mu.Unlock()
		return idx, nil
	}
	req := &readRequest{done: make(chan error, 1)}
	n.pendingReads = append(n.pendingReads, req)
	n.mu.Unlock()
	n.signal(n.readTrigger)

	select {
	case err := <-req.done:
		if err != nil {
			return 0, err
		}
		return idx, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.stopCh:
		return 0, ErrStopped
	}
}

// WaitApplied blocks until the state machine has applied index.
func (n *Node) WaitApplied(ctx context.Context, index uint64) error {
	for {
		n.mu.Lock()
		if n.lastApplied >= index {
			n.mu.Unlock()
			return nil
		}
		if n.stopped {
			n.mu.Unlock()
			return ErrStopped
		}
		ch := n.appliedCh
		n.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stopCh:
			return ErrStopped
		}
	}
}

// LinearizableRead blocks until it is safe to read the local state
// machine with linearizable semantics.
func (n *Node) LinearizableRead(ctx context.Context) error {
	idx, err := n.ReadIndex(ctx)
	if err != nil {
		return err
	}
	return n.WaitApplied(ctx, idx)
}

// LeaseRead is like LinearizableRead but avoids a network round trip while
// the leader holds a valid lease (Raft thesis §6.4.1): a quorum has
// acknowledged a heartbeat sent less than LeaseDuration ago, and those
// followers will not vote for another candidate until a full election
// timeout has passed since they received it. Correctness assumes bounded
// clock *rate* drift between nodes (not synchronised clocks); the lease is
// 10% shorter than the election timeout to absorb drift. Falls back to
// ReadIndex when no lease is held.
func (n *Node) LeaseRead(ctx context.Context) error {
	n.mu.Lock()
	if n.state == Leader && n.readyClosed && n.transferee == "" && !n.leaseRevoked &&
		!n.cfg.DisableCheckQuorum && n.leaseValid(time.Now()) {
		idx := n.commitIndex
		n.stats.LeaseReads++
		n.mu.Unlock()
		return n.WaitApplied(ctx, idx)
	}
	n.mu.Unlock()
	return n.LinearizableRead(ctx)
}

// leaseValid reports whether a quorum acknowledged an RPC sent within the
// lease duration. Called with mu held.
func (n *Node) leaseValid(now time.Time) bool {
	if n.quorum == 1 {
		return true
	}
	sent := make([]time.Time, 0, len(n.progress))
	for _, p := range n.progress {
		sent = append(sent, p.ackedSentAt)
	}
	// The leader counts itself; we need quorum-1 followers.
	slices.SortFunc(sent, func(a, b time.Time) int { return b.Compare(a) })
	anchor := sent[n.quorum-2]
	return !anchor.IsZero() && now.Sub(anchor) < n.leaseDuration()
}

func (n *Node) leaseDuration() time.Duration { return n.cfg.ElectionTimeout * 9 / 10 }

// TransferLeadership hands leadership to target: new proposals are
// refused, target is brought fully up to date, and is then told to start
// an election immediately.
func (n *Node) TransferLeadership(ctx context.Context, target NodeID) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	if target == n.id {
		n.mu.Unlock()
		return nil
	}
	p, ok := n.progress[target]
	if !ok {
		n.mu.Unlock()
		return fmt.Errorf("raft: unknown transfer target %q", target)
	}
	if n.transferee != "" {
		n.mu.Unlock()
		return ErrTransferInProgress
	}
	n.transferee = target
	term := n.term
	n.mu.Unlock()
	n.logger.Info("leadership transfer started", "target", string(target), "term", term)

	defer func() {
		n.mu.Lock()
		if n.state == Leader && n.term == term && n.transferee == target {
			n.transferee = ""
		}
		n.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(ctx, 4*n.cfg.ElectionTimeout)
	defer cancel()
	tick := time.NewTicker(n.cfg.TickInterval)
	defer tick.Stop()

	stillLeader := func() bool {
		n.mu.Lock()
		defer n.mu.Unlock()
		return n.state == Leader && n.term == term
	}

	// Phase 1: wait for the target to catch up.
	for {
		n.mu.Lock()
		if n.state != Leader || n.term != term {
			n.mu.Unlock()
			return nil
		}
		caught := p.match == n.rlog.lastIndex()
		if !caught {
			n.signal(p.trigger)
		}
		n.mu.Unlock()
		if caught {
			break
		}
		select {
		case <-tick.C:
		case <-ctx.Done():
			return ErrTimeout
		}
	}

	// Phase 2: tell the target to campaign. Its election bypasses leader
	// stickiness, so our lease is void from here on.
	n.mu.Lock()
	n.leaseRevoked = true
	n.mu.Unlock()
	resp, err := n.transport.TimeoutNow(ctx, target, &TimeoutNowRequest{Term: term, LeaderID: n.id})
	if err != nil {
		return fmt.Errorf("raft: timeout-now: %w", err)
	}
	n.mu.Lock()
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, "")
	}
	n.mu.Unlock()

	// Phase 3: wait until we have stepped down.
	for stillLeader() {
		select {
		case <-tick.C:
		case <-ctx.Done():
			return ErrTimeout
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Background loops

func (n *Node) tickLoop() {
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
			n.tick()
		}
	}
}

func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return
	}
	now := time.Now()
	if n.state == Leader {
		if !n.cfg.DisableCheckQuorum && now.Sub(n.lastQuorumCheck) >= n.cfg.ElectionTimeout {
			n.lastQuorumCheck = now
			if !n.quorumActive(now) {
				n.logger.Warn("leader lost contact with quorum; stepping down", "term", n.term)
				n.becomeFollower(n.term, "")
			}
		}
		return
	}
	if now.After(n.electionDeadline) {
		n.campaign(!n.cfg.DisablePreVote, false)
	}
}

func (n *Node) quorumActive(now time.Time) bool {
	active := 1
	for _, p := range n.progress {
		if now.Sub(p.lastContact) < n.cfg.ElectionTimeout {
			active++
		}
	}
	return active >= n.quorum
}

func (n *Node) applyLoop() {
	defer n.wg.Done()
	const maxBatch = 512
	for {
		n.mu.Lock()
		for !n.stopped && n.pendingSnapshot == nil && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		if snap := n.pendingSnapshot; snap != nil {
			n.pendingSnapshot = nil
			n.mu.Unlock()
			if err := n.fsm.Restore(snap.Data); err != nil {
				n.fatal(fmt.Errorf("restore snapshot %d: %w", snap.Meta.Index, err))
			}
			n.mu.Lock()
			if snap.Meta.Index > n.lastApplied {
				n.lastApplied = snap.Meta.Index
			}
			n.notifyApplied()
			n.mu.Unlock()
			n.logger.Info("installed snapshot", "index", snap.Meta.Index, "term", snap.Meta.Term)
			continue
		}
		start := n.lastApplied + 1
		end := min(n.commitIndex, start+maxBatch-1)
		entries := n.rlog.slice(start, end+1)
		n.mu.Unlock()

		results := make([]any, len(entries))
		for i, e := range entries {
			if e.Type == EntryNormal {
				results[i] = n.fsm.Apply(e)
			}
		}

		n.mu.Lock()
		n.lastApplied = end
		for i, e := range entries {
			if e.Type == EntryNormal {
				n.stats.ProposalsCommitted++
			}
			w, ok := n.waiters[e.Index]
			if !ok {
				continue
			}
			delete(n.waiters, e.Index)
			if w.term == e.Term {
				w.done <- proposalResult{value: results[i]}
			} else {
				w.done <- proposalResult{err: ErrProposalDropped}
			}
		}
		n.notifyApplied()
		n.mu.Unlock()

		n.maybeSnapshot()
	}
}

func (n *Node) syncLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.syncTrigger:
		}
		n.mu.Lock()
		if n.state != Leader || n.stopped {
			n.mu.Unlock()
			continue
		}
		term, target := n.term, n.rlog.lastIndex()
		n.mu.Unlock()

		if err := n.storage.Sync(); err != nil {
			n.fatal(err)
		}

		n.mu.Lock()
		if n.state == Leader && n.term == term {
			n.durableIndex = max(n.durableIndex, target)
			// Always re-evaluate: something else (a compaction) may already
			// have raised durableIndex past target without committing.
			n.maybeAdvanceCommit()
		}
		n.mu.Unlock()
	}
}

func (n *Node) readLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.readTrigger:
		}
		n.mu.Lock()
		batch := n.pendingReads
		n.pendingReads = nil
		if len(batch) == 0 {
			n.mu.Unlock()
			continue
		}
		if n.state != Leader {
			n.mu.Unlock()
			failReads(batch, ErrNotLeader)
			continue
		}
		n.readRound++
		n.stats.ReadIndexRounds++
		ch := make(chan struct{})
		n.roundWait, n.roundWaitCh = n.readRound, ch
		stop := n.leaderStop
		n.triggerReplication()
		n.mu.Unlock()

		timer := time.NewTimer(n.cfg.ElectionTimeout)
		var err error
		select {
		case <-ch:
		case <-stop:
			err = ErrNotLeader
		case <-timer.C:
			err = ErrTimeout
		case <-n.stopCh:
			err = ErrStopped
		}
		timer.Stop()
		if err != nil {
			n.mu.Lock()
			if n.roundWaitCh == ch {
				n.roundWaitCh = nil
			}
			n.mu.Unlock()
		}
		failReads(batch, err)
		// Requests that queued up during this round get the next one.
		n.mu.Lock()
		if len(n.pendingReads) > 0 {
			n.signal(n.readTrigger)
		}
		n.mu.Unlock()
	}
}

func failReads(batch []*readRequest, err error) {
	for _, r := range batch {
		r.done <- err
	}
}

// ---------------------------------------------------------------------------
// Role transitions (all called with mu held)

func (n *Node) resetElectionTimer() {
	base := n.cfg.ElectionTimeout
	n.electionDeadline = time.Now().Add(base + time.Duration(n.rng.Int63n(int64(base))))
}

func (n *Node) campaign(pre, transfer bool) {
	n.electionSeq++
	seq := n.electionSeq
	n.resetElectionTimer()
	n.leader = ""
	var reqTerm uint64
	if pre {
		n.state = PreCandidate
		reqTerm = n.term + 1
	} else {
		n.state = Candidate
		n.term++
		n.votedFor = n.id
		n.persistHardState()
		reqTerm = n.term
		n.stats.ElectionsStarted++
		n.logger.Info("starting election", "term", n.term, "transfer", transfer)
	}
	if n.quorum == 1 {
		if pre {
			n.campaign(false, transfer)
		} else {
			n.becomeLeader()
		}
		return
	}
	req := RequestVoteRequest{
		Term:               reqTerm,
		CandidateID:        n.id,
		LastLogIndex:       n.rlog.lastIndex(),
		LastLogTerm:        n.rlog.lastTerm(),
		PreVote:            pre,
		LeadershipTransfer: transfer,
	}
	granted := 1
	for _, peer := range n.others {
		go n.solicitVote(peer, req, seq, &granted)
	}
}

func (n *Node) solicitVote(peer NodeID, req RequestVoteRequest, seq uint64, granted *int) {
	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.ElectionTimeout)
	defer cancel()
	resp, err := n.transport.RequestVote(ctx, peer, &req)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || n.electionSeq != seq {
		return
	}
	if resp.Term > n.term && !resp.Granted {
		n.becomeFollower(resp.Term, "")
		return
	}
	if !resp.Granted {
		return
	}
	*granted++
	if *granted != n.quorum {
		return
	}
	if req.PreVote {
		n.campaign(false, req.LeadershipTransfer)
	} else {
		n.becomeLeader()
	}
}

func (n *Node) becomeFollower(term uint64, leader NodeID) {
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.persistHardState()
	}
	wasLeader := n.state == Leader
	n.state = Follower
	n.leader = leader
	n.electionSeq++
	if wasLeader {
		n.stepDownLeader(ErrLeadershipLost)
		n.resetElectionTimer()
		n.logger.Info("stepped down", "term", n.term)
	}
}

func (n *Node) stepDownLeader(err error) {
	close(n.leaderStop)
	n.leaderStop = nil
	n.leaderReady = nil
	n.readyClosed = false
	n.progress = nil
	n.transferee = ""
	n.roundWaitCh = nil
	for idx, w := range n.waiters {
		w.done <- proposalResult{err: err}
		delete(n.waiters, idx)
	}
	failReads(n.pendingReads, ErrNotLeader)
	n.pendingReads = nil
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.leader = n.id
	n.electionSeq++
	n.stats.LeadershipsWon++
	now := time.Now()
	n.leaderStop = make(chan struct{})
	n.leaderReady = make(chan struct{})
	n.readyClosed = false
	n.lastQuorumCheck = now
	n.transferee = ""
	n.leaseRevoked = false

	// Entries from earlier terms may still be buffered; make them durable
	// so the leader can count itself towards every quorum.
	if err := n.storage.Sync(); err != nil {
		n.fatal(err)
	}
	n.durableIndex = n.rlog.lastIndex()

	n.progress = make(map[NodeID]*progress, len(n.others))
	for _, peer := range n.others {
		n.progress[peer] = &progress{
			next:        n.rlog.lastIndex() + 1,
			lastContact: now,
			trigger:     make(chan struct{}, 1),
		}
	}
	noop := Entry{Index: n.rlog.lastIndex() + 1, Term: n.term, Type: EntryNoop}
	n.rlog.append(noop)
	if err := n.storage.Append([]Entry{noop}); err != nil {
		n.fatal(err)
	}
	n.noopIndex = noop.Index
	n.signal(n.syncTrigger)
	for peer, p := range n.progress {
		n.wg.Add(1)
		go n.replicate(peer, p, n.leaderStop, n.term)
	}
	n.logger.Info("became leader", "term", n.term, "last_index", n.rlog.lastIndex())
}

// maybeAdvanceCommit recomputes the commit index from the quorum's match
// indices. Only entries from the current term are committed by counting
// replicas (Raft §5.4.2).
func (n *Node) maybeAdvanceCommit() {
	if n.state != Leader {
		return
	}
	matches := make([]uint64, 0, len(n.progress)+1)
	matches = append(matches, n.durableIndex)
	for _, p := range n.progress {
		matches = append(matches, p.match)
	}
	slices.Sort(matches)
	N := matches[len(matches)-n.quorum]
	if N <= n.commitIndex {
		return
	}
	if t, _ := n.rlog.term(N); t != n.term {
		return
	}
	n.commitIndex = N
	n.applyCond.Signal()
	if !n.readyClosed && N >= n.noopIndex {
		n.readyClosed = true
		close(n.leaderReady)
	}
}

func (n *Node) recordAck(p *progress, round uint64, sentAt time.Time) {
	p.lastContact = time.Now()
	if sentAt.After(p.ackedSentAt) {
		p.ackedSentAt = sentAt
	}
	if round > p.readAck {
		p.readAck = round
	}
	if n.roundWaitCh == nil {
		return
	}
	acks := 1
	for _, q := range n.progress {
		if q.readAck >= n.roundWait {
			acks++
		}
	}
	if acks >= n.quorum {
		close(n.roundWaitCh)
		n.roundWaitCh = nil
	}
}

func (n *Node) triggerReplication() {
	for _, p := range n.progress {
		n.signal(p.trigger)
	}
}

func (n *Node) signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (n *Node) notifyApplied() {
	close(n.appliedCh)
	n.appliedCh = make(chan struct{})
}

func (n *Node) hardState() HardState {
	return HardState{Term: n.term, VotedFor: n.votedFor, Commit: n.commitIndex}
}

func (n *Node) persistHardState() {
	if err := n.storage.SaveHardState(n.hardState()); err != nil {
		n.fatal(err)
	}
}

// fatal aborts the process on unrecoverable storage errors. Continuing
// after a failed write could violate Raft's safety guarantees.
func (n *Node) fatal(err error) {
	n.logger.Error("unrecoverable raft storage error", "err", err)
	panic(fmt.Sprintf("raft[%s]: unrecoverable storage error: %v", n.id, err))
}
