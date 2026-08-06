package grpctransport_test

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/AbeerDas/quorum/raft"
	"github.com/AbeerDas/quorum/raft/grpctransport"
)

// These tests exercise the real wire. The five correctness proofs in the raft
// package run against a simulated network so partitions can be created exactly;
// the job here is narrower but essential - proving that separate processes
// really do talk to each other, and that nothing is lost translating Raft's
// types to protobuf and back.

const (
	electionMin = 80 * time.Millisecond
	electionMax = 160 * time.Millisecond
	heartbeat   = 20 * time.Millisecond
	settle      = 10 * time.Second
)

type recordingStateMachine struct {
	mu      sync.Mutex
	applied []string
}

func (m *recordingStateMachine) Apply(e raft.LogEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = append(m.applied, string(e.Command))
}

func (m *recordingStateMachine) commands() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.applied...)
}

type grpcCluster struct {
	t     *testing.T
	ids   []raft.NodeID
	nodes map[raft.NodeID]*raft.Node
	sms   map[raft.NodeID]*recordingStateMachine
}

// newGRPCCluster starts n nodes, each with its own gRPC server on a real
// loopback port.
func newGRPCCluster(t *testing.T, n int) *grpcCluster {
	t.Helper()

	c := &grpcCluster{
		t:     t,
		nodes: make(map[raft.NodeID]*raft.Node),
		sms:   make(map[raft.NodeID]*recordingStateMachine),
	}

	// Listeners come first so every node knows where the others will be. Port 0
	// lets the OS pick a free port, so parallel test runs cannot collide.
	listeners := make(map[raft.NodeID]net.Listener)
	addrs := make(map[raft.NodeID]string)
	for i := 0; i < n; i++ {
		id := raft.NodeID(fmt.Sprintf("node-%d", i+1))
		c.ids = append(c.ids, id)

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen for %s: %v", id, err)
		}
		listeners[id] = lis
		addrs[id] = lis.Addr().String()
	}

	var servers []*grpc.Server
	var transports []*grpctransport.Transport

	for _, id := range c.ids {
		var peers []raft.NodeID
		for _, other := range c.ids {
			if other != id {
				peers = append(peers, other)
			}
		}

		transport := grpctransport.New(addrs)
		transports = append(transports, transport)

		sm := &recordingStateMachine{}
		node := raft.NewNode(raft.Config{
			ID:                 id,
			Peers:              peers,
			Transport:          transport,
			StateMachine:       sm,
			ElectionTimeoutMin: electionMin,
			ElectionTimeoutMax: electionMax,
			HeartbeatInterval:  heartbeat,
			Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
		})

		c.nodes[id] = node
		c.sms[id] = sm

		server := grpc.NewServer()
		grpctransport.Register(server, node)
		servers = append(servers, server)

		go func(lis net.Listener) {
			_ = server.Serve(lis)
		}(listeners[id])
	}

	for _, id := range c.ids {
		c.nodes[id].Start()
	}

	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
		for _, tr := range transports {
			_ = tr.Close()
		}
		for _, s := range servers {
			s.Stop()
		}
	})

	return c
}

func (c *grpcCluster) waitFor(what string, cond func() bool) {
	c.t.Helper()

	deadline := time.Now().Add(settle)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("timed out after %v waiting for %s", settle, what)
}

func (c *grpcCluster) leaders() []*raft.Node {
	var out []*raft.Node
	for _, id := range c.ids {
		if c.nodes[id].Role() == raft.Leader {
			out = append(out, c.nodes[id])
		}
	}
	return out
}

func (c *grpcCluster) waitForLeader() *raft.Node {
	c.t.Helper()

	var leader *raft.Node
	c.waitFor("a single leader over gRPC", func() bool {
		ls := c.leaders()
		if len(ls) != 1 {
			return false
		}
		leader = ls[0]
		return true
	})
	return leader
}

// Three real servers, real TCP connections, real protobuf on the wire.
func TestClusterElectsLeaderOverGRPC(t *testing.T) {
	c := newGRPCCluster(t, 3)

	leader := c.waitForLeader()

	// Every follower must learn who leads, which only happens once heartbeats
	// have actually crossed the network.
	c.waitFor("followers to recognise the leader", func() bool {
		for _, id := range c.ids {
			if id == leader.ID() {
				continue
			}
			if c.nodes[id].LeaderID() != leader.ID() || c.nodes[id].Role() != raft.Follower {
				return false
			}
		}
		return true
	})

	if got := len(c.leaders()); got != 1 {
		t.Errorf("leaders = %d, want exactly 1", got)
	}
}

// Commands must survive the round trip through protobuf and reach every node.
func TestEntriesReplicateOverGRPC(t *testing.T) {
	c := newGRPCCluster(t, 3)
	c.waitForLeader()

	want := []string{"alpha", "beta", "gamma"}
	for _, cmd := range want {
		c.waitFor(fmt.Sprintf("%q to be accepted", cmd), func() bool {
			for _, id := range c.ids {
				if _, _, err := c.nodes[id].Propose([]byte(cmd)); err == nil {
					return true
				}
			}
			return false
		})
	}

	for _, id := range c.ids {
		sm := c.sms[id]
		c.waitFor(fmt.Sprintf("%s to apply all entries", id), func() bool {
			return reflect.DeepEqual(sm.commands(), want)
		})
	}
}

// A node that stops answering must not stall the cluster: the remaining two
// still form a majority. This is the same failover the correctness tests prove
// against a simulated network, repeated here against real sockets, where a dead
// peer surfaces as a connection error rather than a clean signal.
func TestClusterSurvivesNodeFailureOverGRPC(t *testing.T) {
	c := newGRPCCluster(t, 3)
	leader := c.waitForLeader()

	c.waitFor("a command to be committed before the failure", func() bool {
		_, _, err := leader.Propose([]byte("before-failure"))
		return err == nil
	})
	for _, id := range c.ids {
		sm := c.sms[id]
		c.waitFor(fmt.Sprintf("%s to apply the pre-failure command", id), func() bool {
			return reflect.DeepEqual(sm.commands(), []string{"before-failure"})
		})
	}

	leader.Stop()

	// The survivors must elect a replacement and keep accepting writes.
	var survivors []raft.NodeID
	for _, id := range c.ids {
		if id != leader.ID() {
			survivors = append(survivors, id)
		}
	}

	c.waitFor("the survivors to elect a new leader", func() bool {
		for _, id := range survivors {
			if c.nodes[id].Role() == raft.Leader {
				return true
			}
		}
		return false
	})

	c.waitFor("a command to be accepted after the failure", func() bool {
		for _, id := range survivors {
			if _, _, err := c.nodes[id].Propose([]byte("after-failure")); err == nil {
				return true
			}
		}
		return false
	})

	want := []string{"before-failure", "after-failure"}
	for _, id := range survivors {
		sm := c.sms[id]
		c.waitFor(fmt.Sprintf("%s to apply both commands", id), func() bool {
			return reflect.DeepEqual(sm.commands(), want)
		})
	}
}
