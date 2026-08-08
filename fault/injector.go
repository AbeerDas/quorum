// Package fault injects the failure conditions the dashboard demonstrates:
// a crashed node, a hung node, and a slow network between nodes.
//
// It is deliberately a layer around the consensus core rather than a feature
// inside it. Raft (PRD.md Section 6) is the part of this build that has to be
// correct, and the way to keep it correct is to not reach into it for a demo
// affordance. Everything here works by sitting on the wire - the messages a
// node sends, the messages it receives, and the clock it reads - so a faulted
// node is indistinguishable from a broken machine to the rest of the cluster
// while the consensus code runs completely unmodified.
//
// These controls are for demonstration. They are registered only when the node
// is started with -demo-controls, which is off by default.
package fault

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Mode is what is currently wrong with this node.
type Mode string

const (
	// Healthy is a node behaving normally.
	Healthy Mode = "healthy"

	// Killed models a crashed machine. Traffic in both directions fails
	// instantly, the way a connection to a dead host is refused rather than
	// left hanging, so peers notice as fast as their election timeout allows.
	Killed Mode = "killed"

	// Paused models a hung process: the machine is still there and still
	// accepts messages, it just never answers. Peers cannot tell it apart from
	// a slow network until their own RPC timeout expires, which makes a hung
	// node measurably more disruptive to a leader than a dead one.
	Paused Mode = "paused"
)

var (
	// ErrNodeDown is returned for traffic that a killed node would never have
	// handled. Raft treats an unreachable peer as an ordinary condition, so
	// this surfaces to the consensus code as nothing more interesting than a
	// peer that did not answer.
	ErrNodeDown = errors.New("fault: node is down")

	// ErrClosed releases anything parked on a fault when the node shuts down.
	ErrClosed = errors.New("fault: injector closed")
)

// State is the injector's current condition, as reported on /status.
type State struct {
	Mode    Mode  `json:"mode"`
	DelayMS int64 `json:"delay_ms"`
	// SinceMS is how long this node has been frozen, so the dashboard can say
	// "down for 12s" rather than just "down". Zero while healthy.
	SinceMS int64 `json:"since_ms"`
}

// Injector holds one node's fault state.
//
// The zero value is not usable; call New.
type Injector struct {
	mu    sync.RWMutex
	mode  Mode
	delay time.Duration

	// running is closed while the node is healthy and open while it is frozen,
	// so anything waiting on the node to come back just reads from it. It is
	// replaced on each freeze, never reused, so a revive cannot be missed by a
	// caller that arrived a moment too late.
	running chan struct{}

	// frozenAt is the instant the clock stopped. A frozen node reads this as
	// "now" for as long as it stays frozen.
	frozenAt time.Time

	done      chan struct{}
	closeOnce sync.Once
}

// New returns a healthy injector.
func New() *Injector {
	running := make(chan struct{})
	close(running)
	return &Injector{
		mode:    Healthy,
		running: running,
		done:    make(chan struct{}),
	}
}

// Kill freezes the node and makes its traffic fail instantly.
func (i *Injector) Kill() { i.freeze(Killed) }

// Pause freezes the node and makes its traffic hang instead.
func (i *Injector) Pause() { i.freeze(Paused) }

func (i *Injector) freeze(mode Mode) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Only the first freeze creates a new channel. Going straight from killed
	// to paused must not strand callers already waiting on the old one.
	if i.mode == Healthy {
		i.running = make(chan struct{})
		i.frozenAt = time.Now()
	}
	i.mode = mode
}

// Revive returns the node to normal: it unfreezes and clears any delay, so one
// action in the dashboard genuinely restores the node rather than leaving it
// quietly crippled by a setting the user has forgotten about.
func (i *Injector) Revive() {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.mode != Healthy {
		close(i.running)
		i.mode = Healthy
		i.frozenAt = time.Time{}
	}
	i.delay = 0
}

// SetDelay slows every message crossing this node's boundary. Unlike a freeze
// this leaves the node in the cluster, which is the point: it makes elections
// harder and slower rather than simply causing one.
func (i *Injector) SetDelay(d time.Duration) {
	if d < 0 {
		d = 0
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	i.delay = d
}

// State reports what is currently wrong with the node.
func (i *Injector) State() State {
	i.mu.RLock()
	defer i.mu.RUnlock()

	st := State{Mode: i.mode, DelayMS: i.delay.Milliseconds()}
	if i.mode != Healthy {
		st.SinceMS = time.Since(i.frozenAt).Milliseconds()
	}
	return st
}

// Down reports whether the node is pretending to be off. The HTTP API uses it
// to refuse rate-limit decisions: a machine that has crashed does not answer
// its clients either.
func (i *Injector) Down() bool {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.mode != Healthy
}

// Gate applies the current fault to one message crossing the node boundary. A
// non-nil error means the message must not be delivered.
//
// This runs on every Raft RPC in both directions, so the healthy path is two
// atomic-ish reads and a return; a feature nobody switched on must not cost
// the cluster anything.
func (i *Injector) Gate(ctx context.Context) error {
	i.mu.RLock()
	mode, delay, running := i.mode, i.delay, i.running
	i.mu.RUnlock()

	switch mode {
	case Killed:
		return ErrNodeDown
	case Paused:
		select {
		case <-running:
		case <-ctx.Done():
			return ctx.Err()
		case <-i.done:
			return ErrClosed
		}
	}

	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-i.done:
			return ErrClosed
		}
	}
	return nil
}

// Close releases every caller parked on a fault, so shutting a node down does
// not hang on a freeze somebody left switched on.
func (i *Injector) Close() {
	i.closeOnce.Do(func() { close(i.done) })
}

// waitRunning blocks until the node is unfrozen. It reports false if the
// injector was closed first.
func (i *Injector) waitRunning() bool {
	i.mu.RLock()
	running := i.running
	i.mu.RUnlock()

	select {
	case <-running:
		return true
	case <-i.done:
		return false
	}
}

// Clock returns the clock this node's Raft instance should read.
//
// This is how a node is frozen without a single change to the consensus code.
// Raft decides to call an election because its clock told it too much time had
// passed; a machine that is suspended has no clock advancing, so it never
// reaches that decision. Freezing the network alone would not be enough - the
// node would sit there holding elections against peers it cannot reach, and
// every one of those elections raises its term. It would then come back after
// a minute "away" with a term far ahead of the cluster's and force a pointless
// election among three healthy nodes. Stopping its clock means it wakes up
// exactly where it left off and rejoins as a follower.
//
// The returned type satisfies raft.Clock structurally; this package does not
// import raft, so the consensus core stays free of any knowledge of faults.
func (i *Injector) Clock() *Clock { return &Clock{inj: i} }

// Clock is a clock that stops while its node is frozen.
type Clock struct{ inj *Injector }

// Now reports the current time, or the instant the node froze.
func (c *Clock) Now() time.Time {
	c.inj.mu.RLock()
	defer c.inj.mu.RUnlock()

	if c.inj.mode != Healthy {
		return c.inj.frozenAt
	}
	return time.Now()
}

// After delivers a tick once the duration has elapsed *and* the node is
// running. A tick that comes due while the node is frozen is held, not
// dropped, so a revived node resumes rather than sitting inert.
func (c *Clock) After(d time.Duration) <-chan time.Time {
	// Buffered so an abandoned tick - Raft routinely stops waiting when a
	// heartbeat arrives first - never parks this goroutine forever.
	ch := make(chan time.Time, 1)

	go func() {
		timer := time.NewTimer(d)
		defer timer.Stop()

		select {
		case <-timer.C:
		case <-c.inj.done:
			return
		}

		if !c.inj.waitRunning() {
			return
		}
		ch <- time.Now()
	}()

	return ch
}
