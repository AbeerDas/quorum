package cluster

import (
	"sync"

	"github.com/AbeerDas/quorum/limiter"
	"github.com/AbeerDas/quorum/raft"
)

// retainedResults bounds how many applied results are kept for callers that
// have not collected them yet. Results are normally picked up within
// milliseconds; this only covers callers that gave up or were slow, and stops
// an abandoned request from leaking memory forever.
const retainedResults = 1024

// Result is the outcome of applying one command.
//
// Term identifies which log entry produced it. A caller that proposed at a
// given index must check it, because a leader change can overwrite an
// uncommitted entry: seeing a different term at your index means your command
// was discarded rather than executed.
type Result struct {
	Index    uint64
	Term     uint64
	Decision limiter.Decision
	Err      error
}

// FSM is the replicated state machine: the rate limiter, driven exclusively by
// committed Raft entries.
//
// Every node runs one. Because Raft delivers the same entries in the same order
// to all of them, and because each entry carries the instant it should be
// applied at, all nodes reach identical state (PRD.md Section 14).
type FSM struct {
	mu       sync.Mutex
	lim      *limiter.Limiter
	waiters  map[uint64]chan Result
	results  map[uint64]Result
	lastSeen uint64
}

// NewFSM wraps a limiter as a replicated state machine.
func NewFSM(l *limiter.Limiter) *FSM {
	return &FSM{
		lim:     l,
		waiters: make(map[uint64]chan Result),
		results: make(map[uint64]Result),
	}
}

// Apply executes one committed entry. Raft calls this on every node.
func (f *FSM) Apply(e raft.LogEntry) {
	res := Result{Index: e.Index, Term: e.Term}

	cmd, err := DecodeCommand(e.Command)
	if err != nil {
		// A command this node cannot read must not stop the log or crash the
		// process: the entry is committed and every other node has it too.
		// Report it and move on, leaving state untouched.
		res.Err = err
		f.publish(res)
		return
	}

	switch cmd.Type {
	case CommandConsume:
		res.Decision = f.lim.AllowN(cmd.CallerID, cmd.Amount, cmd.At())
	case CommandConfig:
		cfg := f.lim.Config()
		cfg.Limit = cmd.Limit
		cfg.Window = cmd.Window()
		f.lim.SetConfig(cfg)
	}

	f.publish(res)
}

// Wait returns a channel delivering the result of the entry at index. If that
// entry has already been applied the result is delivered immediately, which
// matters because a caller can only register after its proposal returns an
// index - by which time the entry may already be committed and applied.
func (f *FSM) Wait(index uint64) <-chan Result {
	f.mu.Lock()
	defer f.mu.Unlock()

	ch := make(chan Result, 1)

	if res, ok := f.results[index]; ok {
		delete(f.results, index)
		ch <- res
		return ch
	}

	f.waiters[index] = ch
	return ch
}

// publish hands a result to whoever is waiting, or retains it briefly.
func (f *FSM) publish(res Result) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if res.Index > f.lastSeen {
		f.lastSeen = res.Index
	}

	if ch, ok := f.waiters[res.Index]; ok {
		delete(f.waiters, res.Index)
		ch <- res
		return
	}

	f.results[res.Index] = res
	f.pruneLocked()
}

// pruneLocked drops results old enough that nobody is coming back for them.
func (f *FSM) pruneLocked() {
	if len(f.results) <= retainedResults {
		return
	}
	cutoff := f.lastSeen - retainedResults
	for index := range f.results {
		if index <= cutoff {
			delete(f.results, index)
		}
	}
}

// Snapshot copies the replicated limiter state, for comparing nodes.
func (f *FSM) Snapshot() map[string]limiter.BucketState {
	return f.lim.Snapshot()
}

// Config is the limit currently in force.
func (f *FSM) Config() limiter.Config {
	return f.lim.Config()
}

// TrackedCallers is how many callers currently have state.
func (f *FSM) TrackedCallers() int {
	return f.lim.Len()
}
