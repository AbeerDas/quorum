package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// Test timings. Short enough that tests finish quickly, wide enough that the
// randomised election timeouts still spread out and avoid split votes.
const (
	testElectionMin = 60 * time.Millisecond
	testElectionMax = 120 * time.Millisecond
	testHeartbeat   = 15 * time.Millisecond

	// Ceiling for condition-based waits. Generous on purpose: tests poll until
	// the condition holds, so a high ceiling costs nothing when things work and
	// only prevents false failures on a loaded machine.
	settleTimeout = 3 * time.Second
)

var errUnreachable = errors.New("raft: peer unreachable")

// memNetwork is an in-memory stand-in for the network between nodes. Unlike real
// sockets, any link can be severed instantly and precisely, which is what makes
// partition tests deterministic rather than a firewall-manipulation exercise.
type memNetwork struct {
	mu       sync.RWMutex
	handlers map[NodeID]RPCHandler
	isolated map[NodeID]bool
	dead     map[NodeID]bool
	frozen   map[NodeID]bool
	// group splits the cluster into sides that cannot talk to each other. Zero
	// means "not partitioned", so an empty map leaves everyone connected.
	group map[NodeID]int
	delay time.Duration
}

func newMemNetwork() *memNetwork {
	return &memNetwork{
		handlers: make(map[NodeID]RPCHandler),
		isolated: make(map[NodeID]bool),
		dead:     make(map[NodeID]bool),
		frozen:   make(map[NodeID]bool),
		group:    make(map[NodeID]int),
	}
}

// partition splits the cluster into sides. Nodes within a side still reach each
// other, which is what makes it a real partition rather than a set of isolated
// machines - a minority that can still confer is the interesting case.
func (nw *memNetwork) partition(sides ...[]NodeID) {
	nw.mu.Lock()
	defer nw.mu.Unlock()

	nw.group = make(map[NodeID]int)
	for i, side := range sides {
		for _, id := range side {
			nw.group[id] = i + 1
		}
	}
}

func (nw *memNetwork) register(id NodeID, h RPCHandler) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.handlers[id] = h
}

// isolate cuts every link to and from id, simulating a network partition where
// the node is still running but can no longer be reached.
func (nw *memNetwork) isolate(id NodeID) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.isolated[id] = true
}

func (nw *memNetwork) heal() {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.isolated = make(map[NodeID]bool)
	nw.group = make(map[NodeID]int)
}

// kill marks a node permanently unreachable. Per the project's no-disk-persistence
// rule, a killed node does not come back: it has no durable record of what it
// voted for, and reviving it could let the cluster elect two leaders at once.
func (nw *memNetwork) kill(id NodeID) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.dead[id] = true
}

// freeze simulates a hung process: the node is still on the network but stops
// answering. Distinct from kill, because a frozen node keeps its memory and can
// legitimately resume.
func (nw *memNetwork) freeze(id NodeID) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.frozen[id] = true
}

func (nw *memNetwork) unfreeze(id NodeID) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	delete(nw.frozen, id)
}

func (nw *memNetwork) setDelay(d time.Duration) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	nw.delay = d
}

// route returns the handler for `to`, or false if the message cannot get there.
func (nw *memNetwork) route(from, to NodeID) (RPCHandler, time.Duration, bool) {
	nw.mu.RLock()
	defer nw.mu.RUnlock()

	if nw.dead[from] || nw.dead[to] {
		return nil, 0, false
	}
	if nw.frozen[from] || nw.frozen[to] {
		return nil, 0, false
	}
	if nw.isolated[from] || nw.isolated[to] {
		return nil, 0, false
	}
	if nw.group[from] != nw.group[to] {
		return nil, 0, false
	}

	h, ok := nw.handlers[to]
	return h, nw.delay, ok
}

func (nw *memNetwork) transportFor(id NodeID) Transport {
	return &memTransport{nw: nw, from: id}
}

type memTransport struct {
	nw   *memNetwork
	from NodeID
}

func (t *memTransport) RequestVote(ctx context.Context, to NodeID, args *RequestVoteArgs) (*RequestVoteReply, error) {
	h, delay, ok := t.nw.route(t.from, to)
	if !ok {
		return nil, errUnreachable
	}
	if err := sleepCtx(ctx, delay); err != nil {
		return nil, err
	}
	// Copy the request so a handler cannot mutate the sender's memory, which a
	// real network would never allow.
	cp := *args
	return h.HandleRequestVote(&cp), nil
}

func (t *memTransport) AppendEntries(ctx context.Context, to NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	h, delay, ok := t.nw.route(t.from, to)
	if !ok {
		return nil, errUnreachable
	}
	if err := sleepCtx(ctx, delay); err != nil {
		return nil, err
	}
	cp := *args
	cp.Entries = append([]LogEntry(nil), args.Entries...)
	return h.HandleAppendEntries(&cp), nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// memStateMachine records every command applied to it, in order, so tests can
// compare what different nodes ended up with.
type memStateMachine struct {
	mu      sync.Mutex
	applied []LogEntry
}

func (m *memStateMachine) Apply(e LogEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = append(m.applied, e)
}

func (m *memStateMachine) commands() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.applied))
	for _, e := range m.applied {
		out = append(out, string(e.Command))
	}
	return out
}

func (m *memStateMachine) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.applied)
}

// testCluster wires n nodes onto one in-memory network.
type testCluster struct {
	t     *testing.T
	nw    *memNetwork
	ids   []NodeID
	nodes map[NodeID]*Node
	sms   map[NodeID]*memStateMachine
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()

	c := &testCluster{
		t:     t,
		nw:    newMemNetwork(),
		nodes: make(map[NodeID]*Node),
		sms:   make(map[NodeID]*memStateMachine),
	}

	for i := 0; i < n; i++ {
		c.ids = append(c.ids, NodeID(fmt.Sprintf("node-%d", i+1)))
	}

	for _, id := range c.ids {
		var peers []NodeID
		for _, other := range c.ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		sm := &memStateMachine{}
		node := NewNode(Config{
			ID:                 id,
			Peers:              peers,
			Transport:          c.nw.transportFor(id),
			StateMachine:       sm,
			ElectionTimeoutMin: testElectionMin,
			ElectionTimeoutMax: testElectionMax,
			HeartbeatInterval:  testHeartbeat,
			Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		})

		c.nodes[id] = node
		c.sms[id] = sm
		c.nw.register(id, node)
	}

	for _, id := range c.ids {
		c.nodes[id].Start()
	}

	t.Cleanup(c.stop)
	return c
}

func (c *testCluster) stop() {
	for _, n := range c.nodes {
		n.Stop()
	}
}

// alive lists nodes that have not been killed.
func (c *testCluster) alive() []*Node {
	c.nw.mu.RLock()
	defer c.nw.mu.RUnlock()

	var out []*Node
	for _, id := range c.ids {
		if !c.nw.dead[id] && !c.nw.frozen[id] {
			out = append(out, c.nodes[id])
		}
	}
	return out
}

// leaders returns every node currently claiming leadership. More than one at the
// same term would be a safety violation.
func (c *testCluster) leaders() []*Node {
	var out []*Node
	for _, n := range c.alive() {
		if n.Role() == Leader {
			out = append(out, n)
		}
	}
	return out
}

// waitForLeader polls until exactly one leader exists, rather than sleeping a
// fixed guess. A fixed sleep either wastes time or fails on a slow machine.
func (c *testCluster) waitForLeader() *Node {
	c.t.Helper()

	var leader *Node
	c.waitFor("a single leader to be elected", func() bool {
		ls := c.leaders()
		if len(ls) != 1 {
			return false
		}
		leader = ls[0]
		return true
	})
	return leader
}

// waitForFollowersToLearnLeader blocks until every other live node has heard
// from the leader.
//
// waitForLeader only guarantees the winner knows it won. A follower does not
// learn who leads by voting - the candidate it voted for might still lose - it
// finds out when the first heartbeat arrives. Any assertion about what a
// follower knows must wait for that, or it races the heartbeat.
func (c *testCluster) waitForFollowersToLearnLeader(leader *Node) {
	c.t.Helper()

	c.waitFor(fmt.Sprintf("every follower to recognise %s as leader", leader.ID()), func() bool {
		for _, n := range c.alive() {
			if n.ID() == leader.ID() {
				continue
			}
			if n.LeaderID() != leader.ID() {
				return false
			}
		}
		return true
	})
}

func (c *testCluster) waitFor(what string, cond func() bool) {
	c.t.Helper()

	deadline := time.Now().Add(settleTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	c.t.Fatalf("timed out after %v waiting for %s", settleTimeout, what)
}

// kill stops a node and removes it from the network for good.
func (c *testCluster) kill(id NodeID) {
	c.nw.kill(id)
	c.nodes[id].Stop()
}
