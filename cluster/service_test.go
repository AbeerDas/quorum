package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/AbeerDas/quorum/limiter"
	"github.com/AbeerDas/quorum/raft"
)

// A compact in-memory network, enough to run a real cluster in-process and kill
// nodes at will. The raft package has its own richer version for partition
// tests; this one only needs reachability and death.
type memNet struct {
	mu       sync.RWMutex
	handlers map[raft.NodeID]raft.RPCHandler
	dead     map[raft.NodeID]bool
}

func newMemNet() *memNet {
	return &memNet{
		handlers: make(map[raft.NodeID]raft.RPCHandler),
		dead:     make(map[raft.NodeID]bool),
	}
}

func (n *memNet) register(id raft.NodeID, h raft.RPCHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

func (n *memNet) kill(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dead[id] = true
}

func (n *memNet) route(from, to raft.NodeID) (raft.RPCHandler, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.dead[from] || n.dead[to] {
		return nil, false
	}
	h, ok := n.handlers[to]
	return h, ok
}

type memTransport struct {
	net  *memNet
	from raft.NodeID
}

var errUnreachable = errors.New("unreachable")

func (t *memTransport) RequestVote(ctx context.Context, to raft.NodeID, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	h, ok := t.net.route(t.from, to)
	if !ok {
		return nil, errUnreachable
	}
	cp := *a
	return h.HandleRequestVote(&cp), nil
}

func (t *memTransport) AppendEntries(ctx context.Context, to raft.NodeID, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	h, ok := t.net.route(t.from, to)
	if !ok {
		return nil, errUnreachable
	}
	cp := *a
	cp.Entries = append([]raft.LogEntry(nil), a.Entries...)
	return h.HandleAppendEntries(&cp), nil
}

type testNode struct {
	id   raft.NodeID
	node *raft.Node
	fsm  *FSM
	svc  *Service
}

type testCluster struct {
	t     *testing.T
	net   *memNet
	ids   []raft.NodeID
	nodes map[raft.NodeID]*testNode
}

func newTestCluster(t *testing.T, n int, cfg limiter.Config) *testCluster {
	t.Helper()

	c := &testCluster{t: t, net: newMemNet(), nodes: make(map[raft.NodeID]*testNode)}
	for i := 0; i < n; i++ {
		c.ids = append(c.ids, raft.NodeID(fmt.Sprintf("node-%d", i+1)))
	}

	for _, id := range c.ids {
		var peers []raft.NodeID
		for _, other := range c.ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		fsm := NewFSM(limiter.New(cfg))
		node := raft.NewNode(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          &memTransport{net: c.net, from: id},
			StateMachine:       fsm,
			ElectionTimeoutMin: 60 * time.Millisecond,
			ElectionTimeoutMax: 120 * time.Millisecond,
			HeartbeatInterval:  15 * time.Millisecond,
			Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		})

		c.nodes[id] = &testNode{id: id, node: node, fsm: fsm, svc: NewService(node, fsm)}
		c.net.register(id, node)
	}

	for _, id := range c.ids {
		c.nodes[id].node.Start()
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.node.Stop()
		}
	})

	return c
}

func (c *testCluster) alive() []*testNode {
	c.net.mu.RLock()
	defer c.net.mu.RUnlock()

	var out []*testNode
	for _, id := range c.ids {
		if !c.net.dead[id] {
			out = append(out, c.nodes[id])
		}
	}
	return out
}

func (c *testCluster) waitFor(what string, cond func() bool) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for %s", what)
}

func (c *testCluster) waitForLeader() *testNode {
	c.t.Helper()
	var leader *testNode
	c.waitFor("a leader", func() bool {
		var found []*testNode
		for _, n := range c.alive() {
			if n.node.Role() == raft.Leader {
				found = append(found, n)
			}
		}
		if len(found) != 1 {
			return false
		}
		leader = found[0]
		return true
	})
	return leader
}

func (c *testCluster) killLeader() raft.NodeID {
	c.t.Helper()
	leader := c.waitForLeader()
	c.net.kill(leader.id)
	leader.node.Stop()
	return leader.id
}

// check sends a request to whichever live node will accept it, mirroring what
// the API layer does when a request lands on a follower.
func (c *testCluster) check(callerID string, at time.Time) (limiter.Decision, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var lastErr error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.alive() {
			d, err := n.svc.Check(ctx, callerID, at)
			if err == nil {
				return d, nil
			}
			lastErr = err
		}
		time.Sleep(2 * time.Millisecond)
	}
	return limiter.Decision{}, lastErr
}

func TestCheckThroughLeaderReplicatesToEveryNode(t *testing.T) {
	c := newTestCluster(t, 3, limiter.Config{Limit: 10, Window: time.Minute})
	c.waitForLeader()

	for i := 0; i < 3; i++ {
		if _, err := c.check("alice", t0); err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
	}

	// The count must be visible on every node, not just the one that took it.
	for _, n := range c.alive() {
		id := n.id
		fsm := n.fsm
		c.waitFor(fmt.Sprintf("%s to hold alice's count", id), func() bool {
			return fsm.Snapshot()["alice"].Tokens == 7
		})
	}
}

func TestFollowerRefusesAndNamesTheLeader(t *testing.T) {
	c := newTestCluster(t, 3, limiter.Config{Limit: 10, Window: time.Minute})
	leader := c.waitForLeader()

	// Wait until followers have heard a heartbeat, or they cannot name anyone.
	c.waitFor("followers to learn the leader", func() bool {
		for _, n := range c.alive() {
			if n.id != leader.id && n.node.LeaderID() != leader.id {
				return false
			}
		}
		return true
	})

	ctx := context.Background()
	for _, n := range c.alive() {
		if n.id == leader.id {
			continue
		}
		_, err := n.svc.Check(ctx, "alice", t0)
		if err == nil {
			t.Fatalf("%s: follower accepted a write directly", n.id)
		}

		var notLeader *raft.NotLeaderError
		if !errors.As(err, &notLeader) {
			t.Fatalf("%s: error = %v, want NotLeaderError", n.id, err)
		}
		if notLeader.Leader != leader.id {
			t.Errorf("%s: named %q as leader, want %q", n.id, notLeader.Leader, leader.id)
		}
	}
}

// PRD.md Stage 4 validation: a count made through the leader is still present
// on a follower after that leader fails.
func TestCountSurvivesLeaderFailure(t *testing.T) {
	c := newTestCluster(t, 3, limiter.Config{Limit: 10, Window: time.Minute})
	c.waitForLeader()

	for i := 0; i < 4; i++ {
		if _, err := c.check("alice", t0); err != nil {
			t.Fatalf("pre-failure request %d: %v", i+1, err)
		}
	}
	for _, n := range c.alive() {
		fsm := n.fsm
		c.waitFor("the count to replicate before the failure", func() bool {
			return fsm.Snapshot()["alice"].Tokens == 6
		})
	}

	c.killLeader()
	c.waitForLeader()

	// The survivors must still hold the count taken by the dead leader.
	for _, n := range c.alive() {
		if got := n.fsm.Snapshot()["alice"].Tokens; got != 6 {
			t.Errorf("%s: tokens = %v after failover, want 6", n.id, got)
		}
	}

	// And counting must continue from where it left off, not restart.
	if _, err := c.check("alice", t0); err != nil {
		t.Fatalf("post-failover request: %v", err)
	}
	for _, n := range c.alive() {
		id := n.id
		fsm := n.fsm
		c.waitFor(fmt.Sprintf("%s to record the post-failover count", id), func() bool {
			return fsm.Snapshot()["alice"].Tokens == 5
		})
	}
}

// PRD.md Stage 4 validation, and the strongest claim the project makes: killing
// the leader mid-count neither loses a request that was acknowledged nor counts
// one twice.
//
// Every request carries the same fixed instant, so no tokens refill during the
// test and the arithmetic is exact: tokens spent must equal the number of
// requests the client was told were allowed. Not approximately - exactly.
func TestKillingLeaderMidCountNeitherLosesNorDoubleCounts(t *testing.T) {
	const (
		limit = 500
		total = 60
	)
	c := newTestCluster(t, 3, limiter.Config{Limit: limit, Window: time.Hour})
	c.waitForLeader()

	observedAllowed := 0
	for i := 0; i < total; i++ {
		if i == total/2 {
			c.killLeader()
		}
		d, err := c.check("alice", t0)
		if err != nil {
			// A request that errored was never acknowledged, so it must not
			// appear in the replicated state either.
			continue
		}
		if d.Allowed {
			observedAllowed++
		}
	}

	if observedAllowed == 0 {
		t.Fatal("no request succeeded; the test proved nothing")
	}

	wantTokens := float64(limit - observedAllowed)
	for _, n := range c.alive() {
		id := n.id
		fsm := n.fsm
		c.waitFor(fmt.Sprintf("%s to settle on the final count", id), func() bool {
			return fsm.Snapshot()["alice"].Tokens == wantTokens
		})
	}

	for _, n := range c.alive() {
		got := n.fsm.Snapshot()["alice"].Tokens
		if got != wantTokens {
			t.Errorf("%s: tokens = %v, want %v (client was told %d requests were allowed)",
				n.id, got, wantTokens, observedAllowed)
		}
	}

	// The survivors must also agree with each other exactly.
	survivors := c.alive()
	reference := survivors[0].fsm.Snapshot()
	for _, n := range survivors[1:] {
		if !reflect.DeepEqual(n.fsm.Snapshot(), reference) {
			t.Errorf("replicas diverged after failover:\n %s = %+v\n %s = %+v",
				survivors[0].id, reference, n.id, n.fsm.Snapshot())
		}
	}
}

func TestConfigChangeReplicatesToEveryNode(t *testing.T) {
	c := newTestCluster(t, 3, limiter.Config{Limit: 10, Window: time.Minute})
	leader := c.waitForLeader()

	ctx := context.Background()
	if err := leader.svc.SetLimit(ctx, 42, 20*time.Second, t0); err != nil {
		t.Fatalf("SetLimit: %v", err)
	}

	for _, n := range c.alive() {
		id := n.id
		fsm := n.fsm
		c.waitFor(fmt.Sprintf("%s to adopt the new limit", id), func() bool {
			cfg := fsm.Config()
			return cfg.Limit == 42 && cfg.Window == 20*time.Second
		})
	}
}
