package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/AbeerDas/quorum/limiter"
	"github.com/AbeerDas/quorum/raft"
)

// These tests pin the rules that decide when a failed request may be retried.
// Getting them wrong is how a rate limiter double-counts, and they only execute
// when leadership changes during a single request - a window a live cluster will
// not hit on demand. Mutation testing showed both rules could be deleted
// outright with every other test still passing, so they are driven directly.

// fakeNode stands in for a Raft node so a proposal's fate can be scripted.
type fakeNode struct {
	mu        sync.Mutex
	proposals int
	lastIndex uint64

	term uint64
	err  error

	// onPropose runs after each accepted proposal, letting a test decide what
	// lands in the log at that position.
	onPropose func(index, term uint64)
}

func (f *fakeNode) Propose(command []byte) (uint64, uint64, error) {
	f.mu.Lock()
	f.proposals++
	if f.err != nil {
		err := f.err
		f.mu.Unlock()
		return 0, 0, err
	}
	f.lastIndex++
	index, term := f.lastIndex, f.term
	hook := f.onPropose
	f.mu.Unlock()

	if hook != nil {
		go hook(index, term)
	}
	return index, term, nil
}

func (f *fakeNode) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.proposals
}

func (f *fakeNode) ID() raft.NodeID       { return "node-1" }
func (f *fakeNode) Role() raft.Role       { return raft.Leader }
func (f *fakeNode) Term() uint64          { return f.term }
func (f *fakeNode) LeaderID() raft.NodeID { return "node-1" }
func (f *fakeNode) CommitIndex() uint64   { return f.lastIndex }
func (f *fakeNode) PeerLastContact() map[raft.NodeID]time.Time {
	return map[raft.NodeID]time.Time{}
}

func mustEncode(t *testing.T, c Command) []byte {
	t.Helper()
	b, err := c.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

// A proposal displaced by a later leader must be reported as such, never
// returned as a successful decision. Reporting it as success would tell the
// caller a request was counted when it never happened.
func TestSupersededProposalIsNotReportedAsSuccess(t *testing.T) {
	fsm := NewFSM(limiter.New(limiter.Config{Limit: 10, Window: time.Minute}))
	node := &fakeNode{term: 3}

	// Whatever this node proposes, a later leader's entry occupies that
	// position instead.
	node.onPropose = func(index, term uint64) {
		fsm.Apply(raft.LogEntry{
			Index:   index,
			Term:    term + 1,
			Command: mustEncode(t, ConsumeCommand("someone-else", 1, t0)),
		})
	}

	svc := NewService(node, fsm)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := svc.Check(ctx, "alice", t0)
	if err == nil {
		t.Fatal("a superseded proposal was reported as a successful decision")
	}
	if !errors.Is(err, ErrProposalSuperseded) {
		t.Errorf("error = %v, want ErrProposalSuperseded", err)
	}

	// Alice must not have been charged for a command that never took effect.
	if _, charged := fsm.Snapshot()["alice"]; charged {
		t.Error("alice was charged for a proposal that was discarded")
	}
}

// A superseded proposal is known not to have committed, so retrying it is safe
// and expected - that is what makes a failover look seamless.
func TestSupersededProposalIsRetried(t *testing.T) {
	fsm := NewFSM(limiter.New(limiter.Config{Limit: 10, Window: time.Minute}))
	node := &fakeNode{term: 3}
	node.onPropose = func(index, term uint64) {
		fsm.Apply(raft.LogEntry{
			Index:   index,
			Term:    term + 1,
			Command: mustEncode(t, ConsumeCommand("someone-else", 1, t0)),
		})
	}

	svc := NewService(node, fsm)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, _ = svc.Check(ctx, "alice", t0)

	if got := node.attempts(); got < 2 {
		t.Errorf("proposed %d time(s); a proposal known not to have committed should be retried", got)
	}
}

// The rule that prevents double-counting: when a request's outcome is unknown,
// it must not be retried.
//
// A timeout means the command may have committed and simply not been observed.
// Sending it again would spend a second token for one request - the exact
// failure this project claims not to have.
func TestOutcomeUnknownFailureIsNeverRetried(t *testing.T) {
	fsm := NewFSM(limiter.New(limiter.Config{Limit: 10, Window: time.Minute}))
	// Proposals are accepted but nothing ever applies, so the caller times out
	// without ever learning what happened.
	node := &fakeNode{term: 1}

	svc := NewService(node, fsm)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	_, err := svc.Check(ctx, "alice", t0)
	if err == nil {
		t.Fatal("expected the request to fail")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want a deadline error", err)
	}

	if got := node.attempts(); got != 1 {
		t.Errorf("proposed %d times when the outcome was unknown, want exactly 1: "+
			"retrying a request that may already have committed double-counts it", got)
	}
}

// A node that is not the leader must refuse at once so the API layer can
// forward, rather than spinning on a node that cannot help.
func TestNotLeaderIsReturnedImmediately(t *testing.T) {
	fsm := NewFSM(limiter.New(limiter.Config{Limit: 10, Window: time.Minute}))
	node := &fakeNode{term: 1, err: &raft.NotLeaderError{Leader: "node-2"}}

	svc := NewService(node, fsm)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, err := svc.Check(ctx, "alice", t0)

	var notLeader *raft.NotLeaderError
	if !errors.As(err, &notLeader) {
		t.Fatalf("error = %v, want NotLeaderError", err)
	}
	if notLeader.Leader != "node-2" {
		t.Errorf("named %q as leader, want node-2", notLeader.Leader)
	}
	if got := node.attempts(); got != 1 {
		t.Errorf("attempted %d proposals, want exactly 1: a follower cannot become "+
			"the leader by being asked repeatedly", got)
	}
}
