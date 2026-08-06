package raft

import (
	"fmt"
	"testing"
)

// The tests in this file pin down individual safety rules from Figure 2.
//
// They exist because the cluster-level tests in raft_test.go proved insufficient:
// deliberately breaking these rules left every one of those tests passing. A
// live cluster only exposes some rules when nodes disagree in a specific way,
// and randomised timing means it may take thousands of runs to stumble into it.
// Driving the rule directly makes the check deterministic.

// A node that missed committed entries must never win an election.
//
// This is Raft's Leader Completeness property, and the scenario the cluster
// tests all skipped: a node isolated while the cluster made progress campaigns
// continuously, so its term climbs far above everyone else's. On rejoining, that
// high term forces the others to stand down. If it could also win the election,
// its shorter log would overwrite entries that were already acknowledged - an
// acknowledged write silently vanishing, the worst failure a datastore can have.
// A high term must not be enough; the log has to be current too.
func TestNodeThatMissedEntriesCannotWinElection(t *testing.T) {
	c := newTestCluster(t, 3)
	leader := c.waitForLeader()

	var laggard *Node
	var majority []*Node
	var majorityIDs []NodeID
	for _, n := range c.alive() {
		if n.ID() == leader.ID() {
			continue
		}
		if laggard == nil {
			laggard = n
			continue
		}
		majority = append(majority, n)
		majorityIDs = append(majorityIDs, n.ID())
	}
	majority = append(majority, leader)
	majorityIDs = append(majorityIDs, leader.ID())

	c.nw.partition([]NodeID{laggard.ID()}, majorityIDs)

	var want []string
	for i := 0; i < 3; i++ {
		cmd := fmt.Sprintf("committed-%d", i)
		proposeOn(c, majority, cmd)
		want = append(want, cmd)
	}
	for _, n := range majority {
		c.waitForCommands(n.ID(), want)
	}

	// While cut off it keeps calling elections it cannot win, so its term races
	// ahead of the rest of the cluster.
	c.waitFor("the isolated node's term to overtake the cluster", func() bool {
		return laggard.Term() > leader.Term()
	})

	c.nw.heal()

	// Every committed entry must still be present everywhere, including on the
	// node that rejoined with the highest term and the emptiest log.
	for _, n := range c.alive() {
		c.waitForCommands(n.ID(), want)
	}
}

// A node must not vote twice in one term. Two votes can produce two leaders in
// the same term, each accepting writes the other never sees.
func TestNodeVotesAtMostOncePerTerm(t *testing.T) {
	n := NewNode(Config{ID: "node-1", Peers: []NodeID{"node-2", "node-3"}})

	first := n.HandleRequestVote(&RequestVoteArgs{
		Term: 1, CandidateID: "node-2", LastLogIndex: 0, LastLogTerm: 0,
	})
	if !first.VoteGranted {
		t.Fatal("the first candidate in a new term was refused")
	}

	second := n.HandleRequestVote(&RequestVoteArgs{
		Term: 1, CandidateID: "node-3", LastLogIndex: 0, LastLogTerm: 0,
	})
	if second.VoteGranted {
		t.Error("granted a second vote in the same term, which permits two leaders at once")
	}

	// Re-asking with the same candidate must be idempotent, not a second vote.
	repeat := n.HandleRequestVote(&RequestVoteArgs{
		Term: 1, CandidateID: "node-2", LastLogIndex: 0, LastLogTerm: 0,
	})
	if !repeat.VoteGranted {
		t.Error("refused to reconfirm the vote already granted to this candidate")
	}

	// A later term is a fresh decision, so voting again is correct there.
	next := n.HandleRequestVote(&RequestVoteArgs{
		Term: 2, CandidateID: "node-3", LastLogIndex: 0, LastLogTerm: 0,
	})
	if !next.VoteGranted {
		t.Error("refused a vote in a new term, where the previous vote no longer applies")
	}
}

// A vote must be refused to a candidate whose log is behind ours.
func TestVoteDeniedToCandidateWithStaleLog(t *testing.T) {
	n := NewNode(Config{ID: "node-1", Peers: []NodeID{"node-2", "node-3"}})

	n.mu.Lock()
	n.currentTerm = 5
	n.log = []LogEntry{{}, {Term: 5, Index: 1, Command: []byte("held-here")}}
	n.mu.Unlock()

	// Higher term, but an empty log: exactly the disruptive rejoining node.
	stale := n.HandleRequestVote(&RequestVoteArgs{
		Term: 6, CandidateID: "node-2", LastLogIndex: 0, LastLogTerm: 0,
	})
	if stale.VoteGranted {
		t.Error("voted for a candidate missing an entry we hold; it would overwrite it")
	}

	// The same candidate with an up-to-date log is a legitimate winner.
	current := n.HandleRequestVote(&RequestVoteArgs{
		Term: 6, CandidateID: "node-2", LastLogIndex: 1, LastLogTerm: 5,
	})
	if !current.VoteGranted {
		t.Error("refused a candidate whose log is at least as current as ours")
	}
}

// AppendEntries must be refused when the entry just before the new ones does not
// match, because anything built on a mismatched prefix is wrong.
func TestAppendEntriesRejectsMismatchedPrefix(t *testing.T) {
	n := NewNode(Config{ID: "node-1", Peers: []NodeID{"node-2", "node-3"}})

	n.mu.Lock()
	n.currentTerm = 2
	n.log = []LogEntry{
		{},
		{Term: 1, Index: 1, Command: []byte("a")},
		{Term: 1, Index: 2, Command: []byte("b")},
	}
	n.mu.Unlock()

	// The leader believes position 2 holds a term-2 entry; ours is from term 1.
	reply := n.HandleAppendEntries(&AppendEntriesArgs{
		Term:         2,
		LeaderID:     "node-2",
		PrevLogIndex: 2,
		PrevLogTerm:  2,
		Entries:      []LogEntry{{Term: 2, Index: 3, Command: []byte("c")}},
	})
	if reply.Success {
		t.Fatal("accepted entries onto a prefix that does not match the leader's")
	}
	// The rejection must send the leader back past the whole conflicting run,
	// not just one entry, or convergence takes a round trip per entry.
	if reply.ConflictTerm != 1 || reply.ConflictIndex != 1 {
		t.Errorf("conflict hint = term %d index %d, want term 1 index 1",
			reply.ConflictTerm, reply.ConflictIndex)
	}

	// The mismatched entry must not have been applied to our log either.
	n.mu.Lock()
	logLen := len(n.log)
	n.mu.Unlock()
	if logLen != 3 {
		t.Errorf("log length = %d, want 3: a rejected AppendEntries still modified the log", logLen)
	}

	// A matching prefix is accepted.
	ok := n.HandleAppendEntries(&AppendEntriesArgs{
		Term:         2,
		LeaderID:     "node-2",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 2, Index: 3, Command: []byte("c")}},
	})
	if !ok.Success {
		t.Error("refused entries onto a prefix that does match")
	}
}
