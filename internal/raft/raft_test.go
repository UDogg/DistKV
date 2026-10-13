package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{})
	c.checkOneLeader()
	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, expected >= 1", term1)
	}
	// Without failures the term must stay stable.
	time.Sleep(600 * time.Millisecond)
	if term2 := c.checkTerms(); term1 != term2 {
		t.Fatalf("term changed from %d to %d without failures", term1, term2)
	}
	c.checkOneLeader()
}

func TestSingleNodeCluster(t *testing.T) {
	c := newCluster(t, 1, clusterOpts{})
	c.checkOneLeader()
	for i := 0; i < 10; i++ {
		c.propose(fmt.Sprintf("cmd-%d", i), 1, false)
	}
	n := c.node("n1")
	if err := n.LinearizableRead(context.Background()); err != nil {
		t.Fatalf("read: %v", err)
	}
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{})
	leader1 := c.checkOneLeader()

	// Leader disconnects: a new one must be elected.
	c.disconnect(leader1)
	c.checkOneLeader()

	// Old leader rejoins: must not disturb the new leader.
	c.connect(leader1)
	leader2 := c.checkOneLeader()

	// No quorum: no leader.
	c.disconnect(leader2)
	other := c.ids[0]
	if other == leader2 {
		other = c.ids[1]
	}
	c.disconnect(other)
	time.Sleep(600 * time.Millisecond)
	c.checkNoLeader()

	// Quorum restored.
	c.connect(other)
	c.checkOneLeader()
	c.connect(leader2)
	c.checkOneLeader()
}

func TestManyElections(t *testing.T) {
	c := newCluster(t, 7, clusterOpts{})
	c.checkOneLeader()
	rng := rand.New(rand.NewSource(1))
	for iter := 0; iter < 10; iter++ {
		perm := rng.Perm(7)
		down := []NodeID{c.ids[perm[0]], c.ids[perm[1]], c.ids[perm[2]]}
		for _, id := range down {
			c.disconnect(id)
		}
		c.checkOneLeader()
		for _, id := range down {
			c.connect(id)
		}
	}
	c.checkOneLeader()
}

// A node partitioned away keeps timing out. With pre-vote it must not
// inflate the term and depose the healthy leader when it returns.
func TestPreVotePreventsDisruption(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{})
	leader := c.checkOneLeader()
	termBefore := c.node(leader).Status().Term

	var victim NodeID
	for _, id := range c.ids {
		if id != leader {
			victim = id
			break
		}
	}
	c.disconnect(victim)
	time.Sleep(1500 * time.Millisecond) // many election timeouts
	if got := c.node(victim).Status().Term; got != termBefore {
		t.Fatalf("partitioned node inflated term from %d to %d despite pre-vote", termBefore, got)
	}
	c.connect(victim)
	time.Sleep(500 * time.Millisecond)
	if l := c.checkOneLeader(); l != leader {
		t.Fatalf("leader changed from %s to %s after partition healed", leader, l)
	}
	if got := c.node(leader).Status().Term; got != termBefore {
		t.Fatalf("leader term changed from %d to %d", termBefore, got)
	}
}

func TestCheckQuorumStepDown(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{})
	leader := c.checkOneLeader()
	c.disconnect(leader)
	deadline := time.Now().Add(2 * time.Second)
	for c.node(leader).IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("isolated leader did not step down")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestBasicAgree(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{})
	c.checkOneLeader()
	for i := 1; i <= 20; i++ {
		idx := c.propose(fmt.Sprintf("cmd-%d", i), 5, false)
		if idx == 0 {
			t.Fatalf("no index")
		}
	}
}

func TestFollowerFailure(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{})
	c.propose("a", 3, false)
	leader := c.checkOneLeader()
	follower := c.ids[0]
	if follower == leader {
		follower = c.ids[1]
	}
	c.disconnect(follower)
	c.propose("b", 2, false)
	time.Sleep(200 * time.Millisecond)
	c.propose("c", 2, false)
	c.connect(follower)
	c.propose("d", 3, true)
	c.waitConverged(3 * time.Second)
}

func TestNoAgreementWithoutQuorum(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{})
	c.propose("x", 5, false)
	leader := c.checkOneLeader()
	var down []NodeID
	for _, id := range c.ids {
		if id != leader && len(down) < 3 {
			down = append(down, id)
			c.disconnect(id)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := c.node(leader).Propose(ctx, []byte("never"))
	if err == nil {
		t.Fatalf("proposal committed without a quorum")
	}
	for _, id := range down {
		c.connect(id)
	}
	c.propose("after", 5, true)
}

// TestLogBackup is modelled on the classic "Backup" test: a leader and one
// follower accumulate many uncommitted entries in a minority partition
// while the majority moves on; on healing the stale entries must be
// discarded quickly via conflict hints.
func TestLogBackup(t *testing.T) {
	c := newCluster(t, 5, clusterOpts{})
	c.propose("init", 5, false)
	leader1 := c.checkOneLeader()
	var others []NodeID
	for _, id := range c.ids {
		if id != leader1 {
			others = append(others, id)
		}
	}
	// Minority: leader1 + others[0].
	c.partition([]NodeID{leader1, others[0]}, others[1:])
	ln := c.node(leader1)
	for i := 0; i < 50; i++ {
		go func(i int) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			ln.Propose(ctx, []byte(fmt.Sprintf("lost-%d", i)))
		}(i)
	}
	time.Sleep(300 * time.Millisecond)

	// Majority makes progress.
	for i := 0; i < 50; i++ {
		c.propose(fmt.Sprintf("maj-%d", i), 3, true)
	}
	c.heal()
	c.propose("final", 5, true)
	c.waitConverged(5 * time.Second)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, v := range c.committed {
		if len(v) > 5 && v[:5] == "lost-" {
			t.Fatalf("entry %q from minority partition was committed", v)
		}
	}
}

func TestPersistence(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{})
	c.propose("p1", 3, false)
	for _, id := range c.ids {
		c.restart(id)
	}
	c.checkOneLeader()
	c.propose("p2", 3, true)

	leader := c.checkOneLeader()
	c.restart(leader)
	c.propose("p3", 2, true)
	c.waitConverged(3 * time.Second)

	for _, id := range c.ids {
		if got := c.node(id).Status().AppliedIndex; got < 3 {
			t.Fatalf("node %s applied only %d entries after restart", id, got)
		}
	}
}

func TestSnapshotInstallToLaggingFollower(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{snapshotThreshold: 20, snapshotTrailing: 5})
	c.propose("start", 3, false)
	leader := c.checkOneLeader()
	lagger := c.ids[0]
	if lagger == leader {
		lagger = c.ids[1]
	}
	c.disconnect(lagger)
	for i := 0; i < 100; i++ {
		c.propose(fmt.Sprintf("v%d", i), 2, true)
	}
	if st := c.node(leader).Status(); st.SnapshotIndex == 0 || st.FirstIndex <= 1 {
		t.Fatalf("leader did not snapshot/compact: %+v", st)
	}
	c.connect(lagger)
	c.propose("after", 3, true)
	c.waitConverged(5 * time.Second)
	if st := c.node(lagger).Status(); st.Stats.SnapshotsInstalled == 0 {
		t.Fatalf("lagging follower caught up without a snapshot install: %+v", st)
	}

	// Restart the follower: it must recover from its installed snapshot.
	c.restart(lagger)
	c.propose("after-restart", 3, true)
	c.waitConverged(5 * time.Second)
}

func TestReadIndex(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{})
	c.propose("w1", 3, false)
	leader := c.checkOneLeader()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.node(leader).LinearizableRead(ctx); err != nil {
		t.Fatalf("read on healthy leader: %v", err)
	}
	if _, err := c.node(other(c, leader)).ReadIndex(ctx); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("ReadIndex on follower returned %v, want ErrNotLeader", err)
	}

	// A deposed leader in a minority must not serve linearizable reads.
	c.disconnect(leader)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel2()
	if err := c.node(leader).LinearizableRead(ctx2); err == nil {
		t.Fatalf("isolated leader served a linearizable read")
	}
	c.connect(leader)
	c.checkOneLeader()
}

func TestConcurrentReadIndexBatching(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{})
	c.propose("w1", 3, false)
	leader := c.node(c.checkOneLeader())
	before := leader.Status().Stats.ReadIndexRounds
	var wg sync.WaitGroup
	var failures atomic.Int32
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := leader.LinearizableRead(ctx); err != nil {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()
	if failures.Load() > 0 {
		t.Fatalf("%d reads failed", failures.Load())
	}
	rounds := leader.Status().Stats.ReadIndexRounds - before
	if rounds >= 200 {
		t.Fatalf("expected ReadIndex batching, got %d rounds for 200 reads", rounds)
	}
}

func TestLeadershipTransfer(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{})
	c.propose("a", 3, false)
	leader := c.checkOneLeader()
	target := other(c, leader)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.node(leader).TransferLeadership(ctx, target); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for !c.node(target).IsLeader() {
		if time.Now().After(deadline) {
			t.Fatalf("target %s did not become leader", target)
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.propose("b", 3, true)
}

// TestUnreliableChurn is a randomized stress test in the spirit of the
// "Figure 8 (unreliable)" test: random crashes, restarts, partitions and
// message loss, while clients keep proposing. The harness checks that no two
// nodes ever apply different commands at the same index; at the end the
// cluster must recover and converge.
func TestUnreliableChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}
	c := newCluster(t, 5, clusterOpts{snapshotThreshold: 50, snapshotTrailing: 10})
	c.setUnreliable(true)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	var seq atomic.Int64
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for _, id := range c.ids {
					n := c.node(id)
					if n == nil || !n.IsLeader() {
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
					n.Propose(ctx, []byte(fmt.Sprintf("c%d", seq.Add(1))))
					cancel()
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}

	crashed := map[NodeID]bool{}
	for iter := 0; iter < 40; iter++ {
		time.Sleep(time.Duration(50+rng.Intn(150)) * time.Millisecond)
		id := c.ids[rng.Intn(len(c.ids))]
		switch rng.Intn(4) {
		case 0:
			if !crashed[id] && len(crashed) < 2 {
				c.crash(id)
				crashed[id] = true
			}
		case 1:
			if crashed[id] {
				c.start(id)
				delete(crashed, id)
			}
		case 2:
			c.disconnect(id)
		case 3:
			c.connect(id)
		}
	}
	close(stop)
	wg.Wait()

	c.setUnreliable(false)
	c.heal()
	for id := range crashed {
		c.start(id)
	}
	c.propose("final-barrier", 5, true)
	c.waitConverged(10 * time.Second)
	c.checkErrors()
}

func other(c *cluster, not NodeID) NodeID {
	for _, id := range c.ids {
		if id != not {
			return id
		}
	}
	return ""
}

func TestLeaseRead(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{})
	c.propose("w1", 3, false)
	leader := c.checkOneLeader()
	ln := c.node(leader)
	time.Sleep(100 * time.Millisecond) // let heartbeats establish the lease
	before := ln.Status().Stats.LeaseReads
	for i := 0; i < 50; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := ln.LeaseRead(ctx); err != nil {
			t.Fatalf("lease read on healthy leader: %v", err)
		}
		cancel()
	}
	if got := ln.Status().Stats.LeaseReads - before; got < 45 {
		t.Fatalf("only %d of 50 reads used the lease", got)
	}
}

// The safety property behind lease reads: once a new leader exists, the
// old (partitioned) leader's lease must already have expired.
func TestLeaseExpiresBeforeNewLeaderElected(t *testing.T) {
	for iter := 0; iter < 5; iter++ {
		c := newCluster(t, 3, clusterOpts{})
		c.propose("w", 3, false)
		old := c.checkOneLeader()
		on := c.node(old)
		time.Sleep(60 * time.Millisecond)
		c.disconnect(old)

		// Poll the old leader's lease until a new leader appears.
		for {
			newLeader := ""
			for _, id := range c.ids {
				if id != old && c.node(id).IsLeader() {
					newLeader = string(id)
				}
			}
			on.mu.Lock()
			valid := on.state == Leader && on.leaseValid(time.Now())
			on.mu.Unlock()
			if newLeader != "" {
				if valid {
					t.Fatalf("old leader %s still holds a lease after %s was elected", old, newLeader)
				}
				break
			}
			time.Sleep(time.Millisecond)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		if err := on.LeaseRead(ctx); err == nil {
			t.Fatalf("deposed leader served a lease read")
		}
		cancel()
		c.shutdown()
	}
}

func TestRestartedNodeFreezesVotes(t *testing.T) {
	c := newCluster(t, 3, clusterOpts{})
	leader := c.checkOneLeader()
	f := other(c, leader)
	c.restart(f)
	resp, err := c.node(f).HandleRequestVote(&RequestVoteRequest{Term: 100, CandidateID: "n9", LastLogIndex: 1 << 40, LastLogTerm: 99})
	if err != nil || resp.Granted {
		t.Fatalf("freshly restarted node granted a vote: %+v %v", resp, err)
	}
}
