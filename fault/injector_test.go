package fault

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A healthy injector must be invisible. Every message crossing a node boundary
// runs through Gate, so anything slower than "return immediately" here would
// tax the whole cluster for a feature nobody switched on.
func TestHealthyGateIsTransparent(t *testing.T) {
	inj := New()
	defer inj.Close()

	start := time.Now()
	if err := inj.Gate(context.Background()); err != nil {
		t.Fatalf("healthy Gate returned %v, want nil", err)
	}
	if took := time.Since(start); took > 5*time.Millisecond {
		t.Fatalf("healthy Gate took %v, want it to be immediate", took)
	}
	if got := inj.State().Mode; got != Healthy {
		t.Fatalf("mode is %q, want %q", got, Healthy)
	}
}

// A killed node models a crashed machine: peers find out instantly, the way a
// connection to a dead host is refused rather than left hanging.
func TestKilledGateFailsImmediately(t *testing.T) {
	inj := New()
	defer inj.Close()
	inj.Kill()

	start := time.Now()
	err := inj.Gate(context.Background())
	took := time.Since(start)

	if !errors.Is(err, ErrNodeDown) {
		t.Fatalf("killed Gate returned %v, want ErrNodeDown", err)
	}
	if took > 5*time.Millisecond {
		t.Fatalf("killed Gate took %v, want it to fail immediately", took)
	}
}

// A paused node models a hung process: it accepts the message and then never
// answers. That is the difference from a kill, and it is the whole point of
// having both - peers must wait out their own timeout to notice.
func TestPausedGateBlocksUntilRevived(t *testing.T) {
	inj := New()
	defer inj.Close()
	inj.Pause()

	done := make(chan error, 1)
	go func() { done <- inj.Gate(context.Background()) }()

	select {
	case err := <-done:
		t.Fatalf("paused Gate returned %v immediately, want it to block", err)
	case <-time.After(50 * time.Millisecond):
	}

	inj.Revive()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Gate after Revive returned %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Gate stayed blocked after Revive")
	}
}

// A caller that gave up must not be held forever by a paused node, or every
// pause would leak a goroutine per in-flight RPC.
func TestPausedGateRespectsContextDeadline(t *testing.T) {
	inj := New()
	defer inj.Close()
	inj.Pause()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := inj.Gate(ctx)
	took := time.Since(start)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("paused Gate returned %v, want context.DeadlineExceeded", err)
	}
	if took > 500*time.Millisecond {
		t.Fatalf("paused Gate took %v to honour a 40ms deadline", took)
	}
}

// Delay slows the network without taking the node out of the cluster, which is
// what makes elections visibly harder rather than simply triggering one.
func TestDelayHoldsMessageThenDelivers(t *testing.T) {
	inj := New()
	defer inj.Close()
	inj.SetDelay(60 * time.Millisecond)

	start := time.Now()
	err := inj.Gate(context.Background())
	took := time.Since(start)

	if err != nil {
		t.Fatalf("delayed Gate returned %v, want nil", err)
	}
	if took < 50*time.Millisecond {
		t.Fatalf("delayed Gate took %v, want at least the configured 60ms", took)
	}
	if got := inj.State().DelayMS; got != 60 {
		t.Fatalf("State reports %dms of delay, want 60", got)
	}
}

func TestDelayRespectsContextDeadline(t *testing.T) {
	inj := New()
	defer inj.Close()
	inj.SetDelay(2 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := inj.Gate(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("delayed Gate returned %v, want context.DeadlineExceeded", err)
	}
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("delayed Gate took %v to honour a 40ms deadline", took)
	}
}

// Revive must clear a delay as well as a freeze, so one button in the UI puts
// the node back to genuinely normal rather than quietly leaving it crippled.
func TestReviveClearsDelay(t *testing.T) {
	inj := New()
	defer inj.Close()
	inj.SetDelay(500 * time.Millisecond)
	inj.Revive()

	start := time.Now()
	if err := inj.Gate(context.Background()); err != nil {
		t.Fatalf("Gate after Revive returned %v, want nil", err)
	}
	if took := time.Since(start); took > 50*time.Millisecond {
		t.Fatalf("Gate still delayed by %v after Revive", took)
	}
	if got := inj.State().DelayMS; got != 0 {
		t.Fatalf("State reports %dms of delay after Revive, want 0", got)
	}
}

// The clock is how a frozen node is stopped without touching the consensus
// code: its election timer simply never fires. If this leaked a tick, a killed
// node would campaign while "down" and come back with an inflated term.
func TestFrozenClockDoesNotFire(t *testing.T) {
	inj := New()
	defer inj.Close()
	clock := inj.Clock()

	inj.Kill()
	ticked := clock.After(20 * time.Millisecond)

	select {
	case <-ticked:
		t.Fatal("clock fired while the node was killed, so it would keep campaigning")
	case <-time.After(150 * time.Millisecond):
	}
}

// ...and the tick must be delivered once the node is back, or a revived node
// would sit inert forever instead of rejoining.
func TestFrozenClockFiresAfterRevive(t *testing.T) {
	inj := New()
	defer inj.Close()
	clock := inj.Clock()

	inj.Pause()
	ticked := clock.After(10 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	inj.Revive()

	select {
	case <-ticked:
	case <-time.After(time.Second):
		t.Fatal("clock never fired after Revive, so the node would never rejoin")
	}
}

// A frozen node's clock stands still, which is what "suspended machine" means.
// Raft reads Now for its own bookkeeping, so it must not jump while frozen.
func TestFrozenClockNowStandsStill(t *testing.T) {
	inj := New()
	defer inj.Close()
	clock := inj.Clock()

	inj.Kill()
	first := clock.Now()
	time.Sleep(60 * time.Millisecond)
	second := clock.Now()

	if !first.Equal(second) {
		t.Fatalf("frozen clock advanced from %v to %v", first, second)
	}

	inj.Revive()
	if third := clock.Now(); !third.After(second) {
		t.Fatalf("clock did not resume after Revive: %v is not after %v", third, second)
	}
}

// Closing must release anything parked in Gate, so shutting a node down does
// not hang on a fault somebody left switched on.
func TestCloseReleasesBlockedCallers(t *testing.T) {
	inj := New()
	inj.Pause()

	done := make(chan error, 1)
	go func() { done <- inj.Gate(context.Background()) }()
	time.Sleep(30 * time.Millisecond)

	inj.Close()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Gate returned nil after Close, want an error")
		}
	case <-time.After(time.Second):
		t.Fatal("Gate stayed blocked after Close")
	}
}
