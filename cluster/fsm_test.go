package cluster

import (
	"reflect"
	"testing"
	"time"

	"github.com/AbeerDas/quorum/limiter"
	"github.com/AbeerDas/quorum/raft"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func testConfig() limiter.Config {
	return limiter.Config{Limit: 5, Window: time.Minute}
}

// entry wraps a command as the Raft log entry the FSM will receive.
func entry(t *testing.T, index, term uint64, cmd Command) raft.LogEntry {
	t.Helper()
	encoded, err := cmd.Encode()
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}
	return raft.LogEntry{Index: index, Term: term, Command: encoded}
}

func TestConsumeCommandSpendsTokens(t *testing.T) {
	f := NewFSM(limiter.New(testConfig()))

	for i := 1; i <= 5; i++ {
		f.Apply(entry(t, uint64(i), 1, ConsumeCommand("alice", 1, t0)))
	}

	got := f.Snapshot()["alice"].Tokens
	if got != 0 {
		t.Errorf("tokens = %v, want 0 after spending the whole limit", got)
	}
}

func TestConfigCommandChangesLimit(t *testing.T) {
	f := NewFSM(limiter.New(testConfig()))

	f.Apply(entry(t, 1, 1, ConfigCommand(50, 10*time.Second, t0)))

	cfg := f.Config()
	if cfg.Limit != 50 {
		t.Errorf("limit = %d, want 50", cfg.Limit)
	}
	if cfg.Window != 10*time.Second {
		t.Errorf("window = %v, want 10s", cfg.Window)
	}
}

// The property the whole stage rests on: the same log produces the same state,
// on any node, in any order of real time.
func TestApplyingTheSameLogProducesIdenticalState(t *testing.T) {
	commands := []Command{
		ConsumeCommand("alice", 1, t0),
		ConsumeCommand("bob", 2, t0.Add(time.Second)),
		ConsumeCommand("alice", 1, t0.Add(2*time.Second)),
		ConfigCommand(10, 30*time.Second, t0.Add(3*time.Second)),
		ConsumeCommand("carol", 3, t0.Add(20*time.Second)),
		ConsumeCommand("alice", 4, t0.Add(45*time.Second)),
	}

	leader := NewFSM(limiter.New(testConfig()))
	follower := NewFSM(limiter.New(testConfig()))

	for i, cmd := range commands {
		leader.Apply(entry(t, uint64(i+1), 1, cmd))
	}
	// The follower applies the same log later, as it would after catching up.
	for i, cmd := range commands {
		follower.Apply(entry(t, uint64(i+1), 1, cmd))
	}

	if !reflect.DeepEqual(leader.Snapshot(), follower.Snapshot()) {
		t.Errorf("replicas diverged:\n leader   = %+v\n follower = %+v",
			leader.Snapshot(), follower.Snapshot())
	}
	if !reflect.DeepEqual(leader.Config(), follower.Config()) {
		t.Errorf("configs diverged: leader = %+v, follower = %+v", leader.Config(), follower.Config())
	}
}

// Applying an entry must depend on the timestamp carried in the command, never
// on when the node happens to get around to applying it. A follower catching up
// minutes late must reach exactly the state the leader did.
func TestResultDependsOnCommandTimestampNotWallClock(t *testing.T) {
	f := NewFSM(limiter.New(limiter.Config{Limit: 2, Window: time.Minute}))
	g := NewFSM(limiter.New(limiter.Config{Limit: 2, Window: time.Minute}))

	cmds := []Command{
		ConsumeCommand("alice", 1, t0),
		ConsumeCommand("alice", 1, t0.Add(30*time.Second)),
	}

	for i, c := range cmds {
		f.Apply(entry(t, uint64(i+1), 1, c))
	}

	// Real time passes before the second replica applies the identical log.
	time.Sleep(25 * time.Millisecond)
	for i, c := range cmds {
		g.Apply(entry(t, uint64(i+1), 1, c))
	}

	if !reflect.DeepEqual(f.Snapshot(), g.Snapshot()) {
		t.Errorf("a delayed replica reached different state:\n first  = %+v\n second = %+v",
			f.Snapshot(), g.Snapshot())
	}
}

// A time.Time carries its zone, so two nodes in different regions would produce
// snapshots that compare as different while describing the same instant.
func TestAppliedTimestampsAreNormalisedToUTC(t *testing.T) {
	f := NewFSM(limiter.New(testConfig()))
	f.Apply(entry(t, 1, 1, ConsumeCommand("alice", 1, t0)))

	got := f.Snapshot()["alice"].LastRefill
	if got.Location() != time.UTC {
		t.Errorf("LastRefill location = %v, want UTC: replicas in different zones would not compare equal",
			got.Location())
	}
}

// The decision must come back to the caller that proposed the command, since it
// is only known once the entry is applied.
func TestApplyReportsTheDecisionToAWaiter(t *testing.T) {
	f := NewFSM(limiter.New(limiter.Config{Limit: 1, Window: time.Minute}))

	first := f.Wait(1)
	f.Apply(entry(t, 1, 1, ConsumeCommand("alice", 1, t0)))
	got := <-first
	if !got.Decision.Allowed {
		t.Error("first request: allowed = false, want true")
	}

	second := f.Wait(2)
	f.Apply(entry(t, 2, 1, ConsumeCommand("alice", 1, t0)))
	got = <-second
	if got.Decision.Allowed {
		t.Error("second request: allowed = true, want false (limit is 1)")
	}
}

// A caller may register only after its entry has already been applied, because
// proposing and waiting cannot be one atomic step. The result must still be
// available rather than lost.
func TestWaitFindsAResultThatWasAlreadyApplied(t *testing.T) {
	f := NewFSM(limiter.New(testConfig()))

	f.Apply(entry(t, 1, 1, ConsumeCommand("alice", 1, t0)))

	select {
	case got := <-f.Wait(1):
		if !got.Decision.Allowed {
			t.Error("allowed = false, want true")
		}
		if got.Term != 1 {
			t.Errorf("term = %d, want 1", got.Term)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting for an already-applied entry blocked; the result was dropped")
	}
}

// A malformed entry must not take the node down or stall the log.
func TestUndecodableEntryIsReportedNotFatal(t *testing.T) {
	f := NewFSM(limiter.New(testConfig()))

	w := f.Wait(1)
	f.Apply(raft.LogEntry{Index: 1, Term: 1, Command: []byte("not json")})

	got := <-w
	if got.Err == nil {
		t.Error("err = nil, want a decode error reported back to the caller")
	}
	if n := len(f.Snapshot()); n != 0 {
		t.Errorf("snapshot has %d entries, want 0: a bad command changed state", n)
	}
}
