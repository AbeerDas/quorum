package raft

import "time"

// Metrics receives measurements from a node.
//
// It is an interface, and the consensus core depends on nothing else, so Raft
// stays free of any monitoring library. Implementations are called from the
// replication hot path and while the node's lock is held, so they must be safe
// for concurrent use and must not block - a slow collector would slow down
// consensus itself.
type Metrics interface {
	// ElectionSettled reports how long an election took and whether this node
	// won it. Duration is measured from becoming a candidate.
	ElectionSettled(term uint64, took time.Duration, won bool)

	// RoleChanged reports a transition between follower, candidate and leader.
	RoleChanged(role Role, term uint64)

	// ReplicationLag reports how long a peer took to apply an entry after the
	// leader committed it. Only a leader can measure this.
	ReplicationLag(peer NodeID, lag time.Duration)

	// CommitIndexAdvanced reports the leader's newly committed position.
	CommitIndexAdvanced(index uint64)
}

// nopMetrics is used when no collector is configured, so instrumentation calls
// need no nil checks at the call sites.
type nopMetrics struct{}

func (nopMetrics) ElectionSettled(uint64, time.Duration, bool) {}
func (nopMetrics) RoleChanged(Role, uint64)                    {}
func (nopMetrics) ReplicationLag(NodeID, time.Duration)        {}
func (nopMetrics) CommitIndexAdvanced(uint64)                  {}
