package fault

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/AbeerDas/quorum/raft"
)

// These tests run three real Raft nodes over an in-memory network so the fault
// controls can be judged by what the cluster actually does, not by what the
// injector reports about itself. The consensus code here is unmodified - that
// is the property being checked.

// memNetwork delivers RPCs between nodes in-process. Delivery consults the
// destination's injector, so a faulted node is unreachable from the outside in
// exactly the way it would be over a real socket.
type memNetwork struct {
	mu        sync.RWMutex
	handlers  map[raft.NodeID]raft.RPCHandler
	injectors map[raft.NodeID]*Injector
}

func newMemNetwork() *memNetwork {
	return &memNetwork{
		handlers:  make(map[raft.NodeID]raft.RPCHandler),
		injectors: make(map[raft.NodeID]*Injector),
	}
}

func (n *memNetwork) register(id raft.NodeID, h raft.RPCHandler, inj *Injector) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
	n.injectors[id] = inj
}

var errNoPeer = errors.New("memnetwork: no such peer")

// arrive is the receiving end of the wire: it applies the destination's fault
// before the message is handed to that node's consensus code.
func (n *memNetwork) arrive(ctx context.Context, to raft.NodeID) (raft.RPCHandler, error) {
	n.mu.RLock()
	h, ok := n.handlers[to]
	inj := n.injectors[to]
	n.mu.RUnlock()

	if !ok {
		return nil, errNoPeer
	}
	if err := inj.Gate(ctx); err != nil {
		return nil, err
	}
	return h, nil
}

// memTransport is one node's outbound side of the in-memory network.
type memTransport struct{ net *memNetwork }

func (t *memTransport) RequestVote(ctx context.Context, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	h, err := t.net.arrive(ctx, to)
	if err != nil {
		return nil, err
	}
	return h.HandleRequestVote(args), nil
}

func (t *memTransport) AppendEntries(ctx context.Context, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	h, err := t.net.arrive(ctx, to)
	if err != nil {
		return nil, err
	}
	return h.HandleAppendEntries(args), nil
}

type nopStateMachine struct{}

func (nopStateMachine) Apply(raft.LogEntry) {}

type testCluster struct {
	nodes     map[raft.NodeID]*raft.Node
	injectors map[raft.NodeID]*Injector
	ids       []raft.NodeID
}

// newTestCluster starts three nodes, each with its own fault injector supplying
// both its transport wrapper and its clock.
func newTestCluster(t *testing.T) *testCluster {
	t.Helper()

	net := newMemNetwork()
	c := &testCluster{
		nodes:     make(map[raft.NodeID]*raft.Node),
		injectors: make(map[raft.NodeID]*Injector),
		ids:       []raft.NodeID{"node-1", "node-2", "node-3"},
	}

	for _, id := range c.ids {
		var peers []raft.NodeID
		for _, other := range c.ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		inj := New()
		node := raft.NewNode(raft.Config{
			ID:           id,
			Peers:        peers,
			Transport:    NewTransport(&memTransport{net: net}, inj),
			StateMachine: nopStateMachine{},
			Clock:        inj.Clock(),
			// Short enough to keep the test quick, wide enough that the three
			// nodes rarely time out together and split the vote.
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  40 * time.Millisecond,
			Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		})

		net.register(id, node, inj)
		c.nodes[id] = node
		c.injectors[id] = inj
	}

	for _, id := range c.ids {
		c.nodes[id].Start()
	}

	t.Cleanup(func() {
		for _, id := range c.ids {
			c.nodes[id].Stop()
			c.injectors[id].Close()
		}
	})

	return c
}

// leaders returns every node that currently believes it is the leader, ignoring
// any that are frozen - a suspended machine's opinion is stale by definition.
func (c *testCluster) leaders() []raft.NodeID {
	var found []raft.NodeID
	for _, id := range c.ids {
		if c.injectors[id].Down() {
			continue
		}
		if c.nodes[id].Role() == raft.Leader {
			found = append(found, id)
		}
	}
	return found
}

// awaitLeader waits for exactly one live leader and returns it.
func (c *testCluster) awaitLeader(t *testing.T, within time.Duration) raft.NodeID {
	t.Helper()

	deadline := time.Now().Add(within)
	var last []raft.NodeID
	for time.Now().Before(deadline) {
		last = c.leaders()
		if len(last) == 1 {
			return last[0]
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("no single leader after %v; leaders were %v", within, last)
	return ""
}

// anyFollower returns a live node that is not the given leader.
//
// Which node is faulted matters more than it looks. A leader in this
// implementation never starts an election, so freezing a leader's clock proves
// nothing about whether the freeze works; only a follower has a timer that
// would fire and raise its term.
func (c *testCluster) anyFollower(t *testing.T, leader raft.NodeID) raft.NodeID {
	t.Helper()

	for _, id := range c.ids {
		if id != leader {
			return id
		}
	}

	t.Fatalf("no follower alongside leader %s", leader)
	return ""
}

// awaitConvergence waits for the live nodes to agree - exactly one leader, and
// every live node on the same term - and returns that term.
func (c *testCluster) awaitConvergence(t *testing.T, within time.Duration) uint64 {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if len(c.leaders()) == 1 {
			terms := make(map[uint64]struct{})
			for _, id := range c.ids {
				if !c.injectors[id].Down() {
					terms[c.nodes[id].Term()] = struct{}{}
				}
			}
			if len(terms) == 1 {
				return c.maxTerm()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("cluster did not converge within %v; leaders were %v", within, c.leaders())
	return 0
}

// settleAfterFreeze waits out an election that was already under way when the
// node was frozen.
//
// Freezing stops a node from *starting* an election; it cannot un-start one
// that had already begun microseconds earlier. A node killed in that window
// freezes one term ahead, which is legitimate - a real machine can crash just
// after calling an election too. The tests therefore read a frozen node's term
// after this settling period rather than racing the campaign it may be in.
const settleAfterFreeze = 300 * time.Millisecond

// maxTerm is the highest term any live node has reached.
func (c *testCluster) maxTerm() uint64 {
	var highest uint64
	for _, id := range c.ids {
		if c.injectors[id].Down() {
			continue
		}
		if term := c.nodes[id].Term(); term > highest {
			highest = term
		}
	}
	return highest
}

// Killing the leader must produce a new one from the two survivors. This is the
// dashboard's headline action, driven entirely through the fault controls.
func TestKilledLeaderTriggersFailover(t *testing.T) {
	c := newTestCluster(t)

	first := c.awaitLeader(t, 3*time.Second)
	c.injectors[first].Kill()

	second := c.awaitLeader(t, 5*time.Second)
	if second == first {
		t.Fatalf("leader is still %s after it was killed", first)
	}
}

// A killed node must go quiet, not go rogue. If its clock kept running it would
// campaign against peers it cannot reach and climb a term per election, and the
// longer it stayed "down" the more damage it would do on the way back.
//
// This has to be a follower. A leader has no election timer to fire, so killing
// one would leave its term still whether or not the freeze worked at all.
func TestKilledNodeStopsCampaigning(t *testing.T) {
	c := newTestCluster(t)

	leader := c.awaitLeader(t, 3*time.Second)
	victim := c.anyFollower(t, leader)

	c.injectors[victim].Kill()
	time.Sleep(settleAfterFreeze)
	termAtDeath := c.nodes[victim].Term()

	// Comfortably longer than several election timeouts: an unfrozen isolated
	// node would have raised its term many times over by now.
	time.Sleep(2 * time.Second)

	if got := c.nodes[victim].Term(); got != termAtDeath {
		t.Fatalf("killed follower's term moved from %d to %d while it was down; "+
			"its clock is still running and it is campaigning against nobody",
			termAtDeath, got)
	}
}

// The disruption this whole design exists to prevent: a node that was away for
// a while must not come back with a term far ahead of everyone else's.
//
// The guarantee is a bound, not equality. A node frozen at the instant it began
// campaigning keeps that one extra term, so rejoining can cost at most one
// election. What the frozen clock rules out is the unbounded case: without it a
// node climbs a term per election timeout for as long as it is away, so the
// damage grows with the length of the outage. Measured with the freeze removed,
// two seconds away was enough to return at term 10 against a cluster at term 1.
func TestRevivedFollowerDoesNotDisruptTheCluster(t *testing.T) {
	c := newTestCluster(t)

	leader := c.awaitLeader(t, 3*time.Second)
	victim := c.anyFollower(t, leader)

	c.injectors[victim].Kill()
	time.Sleep(settleAfterFreeze)

	termWhileAway := c.maxTerm()
	frozenTerm := c.nodes[victim].Term()
	if frozenTerm > termWhileAway+1 {
		t.Fatalf("node froze at term %d against a cluster at term %d, "+
			"more than the one term a mid-campaign freeze can cost", frozenTerm, termWhileAway)
	}

	// Several election timeouts' worth of downtime. This is the part that
	// matters: a node whose clock kept running would climb a term throughout.
	time.Sleep(2 * time.Second)

	if got := c.nodes[victim].Term(); got != frozenTerm {
		t.Fatalf("term climbed from %d to %d during the outage; the node is still campaigning",
			frozenTerm, got)
	}

	c.injectors[victim].Revive()

	// The cluster must settle back to exactly one leader with every node on the
	// same term - that is what awaitConvergence checks - and it must get there
	// within one election of where it started.
	//
	// The ceiling is the higher of the two terms, not the cluster's. A node
	// frozen mid-campaign is already a term ahead, and campaigning once on the
	// way back takes it one further. That is the whole cost, and crucially it
	// does not grow with how long the node was away.
	ceiling := termWhileAway
	if frozenTerm > ceiling {
		ceiling = frozenTerm
	}

	settled := c.awaitConvergence(t, 10*time.Second)
	if settled > ceiling+1 {
		t.Fatalf("rejoining took the cluster from term %d to %d, more than the "+
			"single election a bounded rejoin can cost", ceiling, settled)
	}
}

// The payoff: reviving a node must not disturb a cluster that has moved on
// without it. It should discover the current leader, step down, and rejoin - no
// new election, no change of leadership.
func TestRevivedNodeRejoinsWithoutDisruption(t *testing.T) {
	c := newTestCluster(t)

	first := c.awaitLeader(t, 3*time.Second)
	c.injectors[first].Kill()

	second := c.awaitLeader(t, 5*time.Second)
	termAfterFailover := c.maxTerm()

	c.injectors[first].Revive()

	// Give the revived node time to hear a heartbeat and settle.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if c.nodes[first].Role() == raft.Follower && c.nodes[first].Term() == termAfterFailover {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if role := c.nodes[first].Role(); role != raft.Follower {
		t.Fatalf("revived node came back as %v, want follower", role)
	}
	if got := c.nodes[first].Term(); got != termAfterFailover {
		t.Fatalf("revived node settled at term %d, want the cluster's term %d", got, termAfterFailover)
	}
	if now := c.nodes[second].Role(); now != raft.Leader {
		t.Fatalf("leader %s was displaced by the revived node, now %v", second, now)
	}
	if got := c.maxTerm(); got != termAfterFailover {
		t.Fatalf("reviving a node forced the cluster's term from %d to %d, "+
			"so the rejoin was disruptive", termAfterFailover, got)
	}
}

// Killing two of three must leave the survivor unable to lead. This is the
// check that the fault controls produce a real partition rather than a cosmetic
// one: a killed node that still answered votes would keep making quorum, and
// the cluster would carry on with a single live machine deciding everything.
func TestKillingTwoOfThreeLeavesNoLeader(t *testing.T) {
	c := newTestCluster(t)

	first := c.awaitLeader(t, 3*time.Second)
	c.injectors[first].Kill()

	second := c.awaitLeader(t, 5*time.Second)
	c.injectors[second].Kill()

	// Long enough for the survivor to have timed out and campaigned repeatedly.
	time.Sleep(2 * time.Second)

	if got := c.leaders(); len(got) != 0 {
		t.Fatalf("%v claims leadership with only one node alive, so the killed "+
			"nodes are still voting", got)
	}
}

// A paused node is a hung machine rather than a dead one. The cluster must
// still route around it, which is what proves pause is a genuine fault and not
// just a slower kill.
func TestPausedLeaderTriggersFailover(t *testing.T) {
	c := newTestCluster(t)

	first := c.awaitLeader(t, 3*time.Second)
	c.injectors[first].Pause()

	second := c.awaitLeader(t, 5*time.Second)
	if second == first {
		t.Fatalf("leader is still %s after it was paused", first)
	}
}

// Delay leaves a node in the cluster. A delay well under the election timeout
// must therefore change nothing at all - that is what makes the control useful
// for showing where the breaking point is.
func TestModestDelayKeepsTheLeader(t *testing.T) {
	c := newTestCluster(t)

	leader := c.awaitLeader(t, 3*time.Second)
	for _, id := range c.ids {
		c.injectors[id].SetDelay(20 * time.Millisecond)
	}

	time.Sleep(time.Second)

	if got := c.leaders(); len(got) != 1 || got[0] != leader {
		t.Fatalf("leadership changed to %v under a 20ms delay, want %s to hold", got, leader)
	}
}
