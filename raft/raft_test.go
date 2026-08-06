package raft

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// proposeOnLeader writes a command through the current leader, retrying across
// an election if leadership moves mid-write.
func proposeOnLeader(c *testCluster, command string) uint64 {
	c.t.Helper()
	return proposeOn(c, c.alive(), command)
}

// proposeOn writes through whichever of the given nodes will accept it, which
// matters during a partition where only one side can make progress.
func proposeOn(c *testCluster, nodes []*Node, command string) uint64 {
	c.t.Helper()

	var index uint64
	c.waitFor(fmt.Sprintf("command %q to be accepted", command), func() bool {
		for _, n := range nodes {
			idx, _, err := n.Propose([]byte(command))
			if err == nil {
				index = idx
				return true
			}
		}
		return false
	})
	return index
}

func (c *testCluster) node(id NodeID) *Node { return c.nodes[id] }

// waitForCommands blocks until the node's state machine holds exactly want.
func (c *testCluster) waitForCommands(id NodeID, want []string) {
	c.t.Helper()

	sm := c.sms[id]
	c.waitFor(fmt.Sprintf("%s to hold %v", id, want), func() bool {
		return reflect.DeepEqual(sm.commands(), want)
	})
}

// Correctness test 1 of 5 (PRD.md Section 9).
//
// Raft's central safety property is that a term has at most one leader. Two
// nodes both believing they are leader would each accept writes, and the logs
// would diverge irreconcilably.
func TestExactlyOneLeaderIsElectedInHealthyCluster(t *testing.T) {
	c := newTestCluster(t, 3)

	leader := c.waitForLeader()

	if got := len(c.leaders()); got != 1 {
		t.Fatalf("leaders = %d, want exactly 1", got)
	}

	// Leadership must also be stable: a cluster that re-elects continuously has
	// technically had "one leader" at every instant while being useless.
	term := leader.Term()
	time.Sleep(10 * testHeartbeat)

	if got := len(c.leaders()); got != 1 {
		t.Fatalf("after settling, leaders = %d, want exactly 1", got)
	}
	if got := c.leaders()[0].ID(); got != leader.ID() {
		t.Errorf("leadership moved from %s to %s while the cluster was healthy", leader.ID(), got)
	}
	if got := leader.Term(); got != term {
		t.Errorf("term advanced from %d to %d with no failure to trigger it", term, got)
	}

	// Every follower must agree on who leads and in which term.
	for _, n := range c.alive() {
		if n.ID() == leader.ID() {
			continue
		}
		if n.Role() != Follower {
			t.Errorf("%s: role = %v, want Follower", n.ID(), n.Role())
		}
		if n.Term() != term {
			t.Errorf("%s: term = %d, want %d", n.ID(), n.Term(), term)
		}
		if n.LeaderID() != leader.ID() {
			t.Errorf("%s: follows %q, want %q", n.ID(), n.LeaderID(), leader.ID())
		}
	}
}

// Correctness test 2 of 5 (PRD.md Section 9).
//
// If a follower accepted writes directly, the cluster would have two sources of
// truth and the logs could never be reconciled. A follower must refuse, and say
// who to talk to instead.
func TestFollowerRejectsDirectWrites(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader()

	// The redirect can only name the leader once the follower has heard from it.
	c.waitForFollowersToLearnLeader(leader)

	for _, n := range c.alive() {
		if n.ID() == leader.ID() {
			continue
		}

		_, _, err := n.Propose([]byte("write-via-follower"))
		if err == nil {
			t.Fatalf("%s: follower accepted a direct write", n.ID())
		}

		var notLeader *NotLeaderError
		if !errors.As(err, &notLeader) {
			t.Fatalf("%s: error = %v, want a NotLeaderError", n.ID(), err)
		}
		// The refusal must be useful: point the caller at the real leader.
		if notLeader.Leader != leader.ID() {
			t.Errorf("%s: redirected to %q, want %q", n.ID(), notLeader.Leader, leader.ID())
		}
	}

	// The rejected writes must not have reached anyone's state machine.
	for id, sm := range c.sms {
		if got := sm.len(); got != 0 {
			t.Errorf("%s: applied %d entries, want 0 (a rejected write was replicated)", id, got)
		}
	}

	// The leader still accepts writes normally.
	if _, _, err := leader.Propose([]byte("write-via-leader")); err != nil {
		t.Errorf("leader %s rejected a write: %v", leader.ID(), err)
	}
}

// Committed entries must reach every node's state machine, in the same order.
func TestEntriesReplicateToAllNodes(t *testing.T) {
	c := newTestCluster(t, 3)
	c.waitForLeader()

	want := []string{"set-a", "set-b", "set-c"}
	for _, cmd := range want {
		proposeOnLeader(c, cmd)
	}

	for _, n := range c.alive() {
		id := n.ID()
		sm := c.sms[id]
		c.waitFor(fmt.Sprintf("%s to apply %d entries", id, len(want)), func() bool {
			return sm.len() == len(want)
		})
		if got := sm.commands(); !reflect.DeepEqual(got, want) {
			t.Errorf("%s applied %v, want %v", id, got, want)
		}
	}
}

// Correctness test 3 of 5 (PRD.md Section 9).
//
// This is the promise the whole project rests on: once a write is acknowledged,
// killing the machine that accepted it must not lose it.
func TestCommittedEntrySurvivesLeaderFailure(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader()

	proposeOnLeader(c, "survive-me")

	// Wait for the entry to be committed and applied everywhere, so it is
	// genuinely acknowledged rather than merely accepted by the leader.
	for _, n := range c.alive() {
		sm := c.sms[n.ID()]
		c.waitFor(fmt.Sprintf("%s to apply the committed entry", n.ID()), func() bool {
			return sm.len() == 1
		})
	}

	oldTerm := leader.Term()
	c.kill(leader.ID())

	// The survivors must elect a replacement.
	newLeader := c.waitForLeader()
	if newLeader.ID() == leader.ID() {
		t.Fatal("the killed node is still reported as leader")
	}
	if newLeader.Term() <= oldTerm {
		t.Errorf("new leader term = %d, want greater than the failed leader's %d",
			newLeader.Term(), oldTerm)
	}

	// The committed value must still be there, and still be the only one.
	for _, n := range c.alive() {
		got := c.sms[n.ID()].commands()
		if !reflect.DeepEqual(got, []string{"survive-me"}) {
			t.Errorf("%s holds %v after failover, want [survive-me]", n.ID(), got)
		}
	}

	// And the new leader must accept new writes.
	proposeOnLeader(c, "after-failover")
	for _, n := range c.alive() {
		id := n.ID()
		sm := c.sms[id]
		c.waitFor(fmt.Sprintf("%s to apply the post-failover write", id), func() bool {
			return sm.len() == 2
		})
		if got := sm.commands(); !reflect.DeepEqual(got, []string{"survive-me", "after-failover"}) {
			t.Errorf("%s holds %v, want [survive-me after-failover]", id, got)
		}
	}
}

// Correctness test 4 of 5 (PRD.md Section 9).
//
// A node cut off from the cluster must not be able to crown itself. If it could,
// the partition would heal into two leaders with conflicting histories - the
// classic split-brain that consensus exists to prevent.
func TestMinorityPartitionCannotElectLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader()

	var victim *Node
	for _, n := range c.alive() {
		if n.ID() != leader.ID() {
			victim = n
			break
		}
	}

	c.nw.isolate(victim.ID())

	// Give it many election timeouts to try, and keep checking throughout rather
	// than only at the end - becoming leader even briefly is a safety violation.
	deadline := time.Now().Add(8 * testElectionMax)
	for time.Now().Before(deadline) {
		if victim.Role() == Leader {
			t.Fatalf("isolated node %s became leader on its own vote (term %d)",
				victim.ID(), victim.Term())
		}
		time.Sleep(2 * time.Millisecond)
	}

	// It should be campaigning fruitlessly, not sitting idle: repeated failed
	// elections are the expected behaviour, and the rising term proves it tried.
	if victim.Term() <= leader.Term() {
		t.Errorf("isolated node term = %d, expected it to exceed the leader's %d from repeated attempts",
			victim.Term(), leader.Term())
	}

	// Meanwhile the majority side must be unaffected.
	majorityLeaders := 0
	for _, n := range c.alive() {
		if n.ID() != victim.ID() && n.Role() == Leader {
			majorityLeaders++
		}
	}
	if majorityLeaders != 1 {
		t.Errorf("majority side has %d leaders, want exactly 1", majorityLeaders)
	}

	// The majority can still make progress while the minority is cut off.
	proposeOnLeader(c, "quorum-still-works")
}

// Correctness test 5 of 5 (PRD.md Section 9).
//
// The strongest of the five: after writes interrupted by a leader failure, every
// surviving node must hold byte-identical state. Anything else means replicas
// have silently diverged, which is the failure mode that makes a cluster worse
// than a single machine.
func TestReplicatedStateMachinesRemainIdentical(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader()

	var want []string
	for i := 0; i < 3; i++ {
		cmd := fmt.Sprintf("before-%d", i)
		proposeOnLeader(c, cmd)
		want = append(want, cmd)
	}

	for _, n := range c.alive() {
		sm := c.sms[n.ID()]
		c.waitFor(fmt.Sprintf("%s to apply the pre-failure writes", n.ID()), func() bool {
			return sm.len() == len(want)
		})
	}

	// Fail the leader in the middle of the sequence.
	c.kill(leader.ID())
	c.waitForLeader()

	for i := 0; i < 3; i++ {
		cmd := fmt.Sprintf("after-%d", i)
		proposeOnLeader(c, cmd)
		want = append(want, cmd)
	}

	survivors := c.alive()
	for _, n := range survivors {
		sm := c.sms[n.ID()]
		c.waitFor(fmt.Sprintf("%s to apply all %d writes", n.ID(), len(want)), func() bool {
			return sm.len() == len(want)
		})
	}

	// Every survivor must match the expected history exactly...
	for _, n := range survivors {
		if got := c.sms[n.ID()].commands(); !reflect.DeepEqual(got, want) {
			t.Errorf("%s applied %v, want %v", n.ID(), got, want)
		}
	}

	// ...and, stated directly, must match each other.
	reference := c.sms[survivors[0].ID()].commands()
	for _, n := range survivors[1:] {
		if got := c.sms[n.ID()].commands(); !reflect.DeepEqual(got, reference) {
			t.Errorf("replicas diverged:\n %s = %v\n %s = %v",
				survivors[0].ID(), reference, n.ID(), got)
		}
	}
}

// An isolated leader is a subtler case than an isolated follower: Raft lets it
// keep believing it leads, because it cannot know it has been cut off. What it
// must never do is commit anything, since it can no longer reach a majority.
func TestIsolatedLeaderCannotCommit(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader()

	c.nw.isolate(leader.ID())

	committedBefore := leader.CommitIndex()
	if _, _, err := leader.Propose([]byte("written-while-cut-off")); err != nil {
		t.Fatalf("isolated leader refused the write outright: %v", err)
	}

	time.Sleep(6 * testElectionMax)

	if got := leader.CommitIndex(); got != committedBefore {
		t.Errorf("isolated leader advanced commit index from %d to %d without a majority",
			committedBefore, got)
	}

	// The majority must have moved on without it.
	var newLeader *Node
	for _, n := range c.alive() {
		if n.ID() != leader.ID() && n.Role() == Leader {
			newLeader = n
		}
	}
	if newLeader == nil {
		t.Fatal("the majority side failed to elect a replacement leader")
	}
	if newLeader.Term() <= leader.Term() {
		t.Errorf("replacement term = %d, want greater than the isolated leader's %d",
			newLeader.Term(), leader.Term())
	}

	// The write the isolated leader accepted must never reach anyone.
	for _, n := range c.alive() {
		if n.ID() == leader.ID() {
			continue
		}
		for _, cmd := range c.sms[n.ID()].commands() {
			if cmd == "written-while-cut-off" {
				t.Errorf("%s applied a command that was never committed", n.ID())
			}
		}
	}
}

// A deposed leader's uncommitted writes must be discarded, not resurrected.
//
// This is the case the earlier tests never reached: every one of them killed a
// node cleanly from a converged cluster, so no log ever actually disagreed with
// another. The rules that handle disagreement - log matching, and refusing a
// vote to a candidate with a stale log - were never exercised at all.
func TestDeposedLeaderDiscardsUncommittedWrites(t *testing.T) {
	c := newTestCluster(t, 3)
	oldLeader := c.waitForLeader()

	proposeOnLeader(c, "committed-before-split")
	for _, n := range c.alive() {
		c.waitForCommands(n.ID(), []string{"committed-before-split"})
	}

	// Cut the leader off from the other two.
	var majorityIDs []NodeID
	var majority []*Node
	for _, n := range c.alive() {
		if n.ID() != oldLeader.ID() {
			majorityIDs = append(majorityIDs, n.ID())
			majority = append(majority, n)
		}
	}
	c.nw.partition([]NodeID{oldLeader.ID()}, majorityIDs)

	// The stranded leader keeps accepting writes it can never commit.
	for i := 0; i < 3; i++ {
		if _, _, err := oldLeader.Propose([]byte(fmt.Sprintf("orphaned-%d", i))); err != nil {
			t.Fatalf("stranded leader refused a write: %v", err)
		}
	}

	// The majority elects a replacement and makes real progress.
	want := []string{"committed-before-split"}
	for i := 0; i < 2; i++ {
		cmd := fmt.Sprintf("real-%d", i)
		proposeOn(c, majority, cmd)
		want = append(want, cmd)
	}
	for _, n := range majority {
		c.waitForCommands(n.ID(), want)
	}

	c.nw.heal()

	// The old leader must throw away its orphaned entries and adopt the
	// majority's history. Every node ends up identical.
	for _, n := range c.alive() {
		c.waitForCommands(n.ID(), want)
	}

	for _, n := range c.alive() {
		for _, cmd := range c.sms[n.ID()].commands() {
			if len(cmd) >= 8 && cmd[:8] == "orphaned" {
				t.Errorf("%s applied %q, which was never committed", n.ID(), cmd)
			}
		}
	}
}

// A minority that can still confer must not be able to elect a leader.
//
// Two isolated nodes can vote for each other, so this catches an incorrect
// majority calculation in a way that isolating a single node cannot - a lone
// node never collects a second vote whether the threshold is right or wrong.
func TestMinorityOfTwoCannotElectLeader(t *testing.T) {
	c := newTestCluster(t, 5)
	leader := c.waitForLeader()

	// Keep the sitting leader on the majority side, so a node that merely
	// retains leadership it already held cannot be mistaken for a new election.
	majority := []NodeID{leader.ID()}
	var minority []NodeID
	for _, id := range c.ids {
		if id == leader.ID() {
			continue
		}
		if len(minority) < 2 {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	c.nw.partition(minority, majority)

	deadline := time.Now().Add(8 * testElectionMax)
	for time.Now().Before(deadline) {
		for _, id := range minority {
			if c.node(id).Role() == Leader {
				t.Fatalf("node %s led a minority of 2 out of 5 (term %d)", id, c.node(id).Term())
			}
		}
		time.Sleep(2 * time.Millisecond)
	}

	// The majority side must still be able to commit.
	var majorityNodes []*Node
	for _, id := range majority {
		majorityNodes = append(majorityNodes, c.node(id))
	}
	proposeOn(c, majorityNodes, "majority-still-commits")
	for _, id := range majority {
		c.waitForCommands(id, []string{"majority-still-commits"})
	}
}

// A leader must not commit an entry from an earlier term just because it now
// sits on a majority of nodes (Raft paper, Figure 8).
//
// Such an entry can still be overwritten by a future leader, so treating it as
// committed would let an acknowledged write disappear. Only committing an entry
// from the leader's own term makes it, and everything before it, permanent.
//
// Driven directly against the commit rule rather than through a live cluster:
// the scenario needs a precise arrangement of terms that is impractical to
// orchestrate reliably through timing alone.
func TestLeaderDoesNotCommitEntriesFromEarlierTerms(t *testing.T) {
	n := NewNode(Config{
		ID:           "node-1",
		Peers:        []NodeID{"node-2", "node-3"},
		StateMachine: &memStateMachine{},
	})

	n.mu.Lock()
	n.role = Leader
	n.currentTerm = 2
	// One entry, left over from term 1, already stored on both followers.
	n.log = []LogEntry{{}, {Term: 1, Index: 1, Command: []byte("from-old-term")}}
	n.matchIndex = map[NodeID]uint64{"node-2": 1, "node-3": 1}
	n.advanceCommitLocked()
	afterOldTerm := n.commitIndex
	n.mu.Unlock()

	if afterOldTerm != 0 {
		t.Errorf("commit index = %d, want 0: an earlier-term entry was committed on replica count alone",
			afterOldTerm)
	}

	// Once an entry from the leader's own term is replicated to a majority, it
	// commits - and carries the earlier entry with it.
	n.mu.Lock()
	n.log = append(n.log, LogEntry{Term: 2, Index: 2, Command: []byte("from-current-term")})
	n.matchIndex = map[NodeID]uint64{"node-2": 2, "node-3": 2}
	n.advanceCommitLocked()
	afterCurrentTerm := n.commitIndex
	n.mu.Unlock()

	if afterCurrentTerm != 2 {
		t.Errorf("commit index = %d, want 2: a current-term entry on a majority must commit",
			afterCurrentTerm)
	}
}
