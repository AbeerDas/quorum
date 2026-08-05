package limiter

import (
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// t0 is a fixed reference instant. Every test drives time explicitly by passing
// `now` into Allow, so tests are deterministic and never sleep.
var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestAllowsUpToLimitThenBlocks(t *testing.T) {
	l := New(Config{Limit: 5, Window: time.Minute})

	for i := 1; i <= 5; i++ {
		if d := l.Allow("alice", t0); !d.Allowed {
			t.Fatalf("request %d: got blocked, want allowed", i)
		}
	}

	if d := l.Allow("alice", t0); d.Allowed {
		t.Fatal("request 6: got allowed, want blocked")
	}
}

func TestRefillsOverTime(t *testing.T) {
	l := New(Config{Limit: 10, Window: time.Minute})

	for i := 0; i < 10; i++ {
		l.Allow("alice", t0)
	}
	if d := l.Allow("alice", t0); d.Allowed {
		t.Fatal("bucket should be drained")
	}

	// Half a window later, half the tokens are back.
	half := t0.Add(30 * time.Second)
	for i := 1; i <= 5; i++ {
		if d := l.Allow("alice", half); !d.Allowed {
			t.Fatalf("refilled request %d: got blocked, want allowed", i)
		}
	}
	if d := l.Allow("alice", half); d.Allowed {
		t.Fatal("got allowed after spending the refill, want blocked")
	}
}

func TestRefillDoesNotExceedLimit(t *testing.T) {
	l := New(Config{Limit: 3, Window: time.Minute})
	l.Allow("alice", t0)

	// Idle for ten windows. The bucket must cap at Limit, not accumulate.
	later := t0.Add(10 * time.Minute)
	for i := 1; i <= 3; i++ {
		if d := l.Allow("alice", later); !d.Allowed {
			t.Fatalf("request %d: got blocked, want allowed", i)
		}
	}
	if d := l.Allow("alice", later); d.Allowed {
		t.Fatal("got allowed: bucket accumulated beyond Limit")
	}
}

// The core fairness property: one caller exhausting its budget must not affect
// any other caller.
func TestCallersAreIsolated(t *testing.T) {
	l := New(Config{Limit: 2, Window: time.Minute})

	l.Allow("noisy", t0)
	l.Allow("noisy", t0)
	if d := l.Allow("noisy", t0); d.Allowed {
		t.Fatal("noisy caller: got allowed, want blocked")
	}

	if d := l.Allow("quiet", t0); !d.Allowed {
		t.Fatal("quiet caller: got blocked, want allowed (noisy must not affect it)")
	}
}

func TestRemainingCountsDown(t *testing.T) {
	l := New(Config{Limit: 3, Window: time.Minute})

	for i, want := range []int{2, 1, 0} {
		d := l.Allow("alice", t0)
		if d.Remaining != want {
			t.Errorf("request %d: Remaining = %d, want %d", i+1, d.Remaining, want)
		}
	}
}

func TestRetryAfterReportsTimeToNextToken(t *testing.T) {
	// One token per minute: after draining, a full minute must pass.
	l := New(Config{Limit: 1, Window: time.Minute})
	l.Allow("alice", t0)

	d := l.Allow("alice", t0)
	if d.Allowed {
		t.Fatal("got allowed, want blocked")
	}
	if d.RetryAfter != time.Minute {
		t.Errorf("RetryAfter = %v, want %v", d.RetryAfter, time.Minute)
	}
}

func TestAllowNConsumesRequestedAmount(t *testing.T) {
	l := New(Config{Limit: 10, Window: time.Minute})

	if d := l.AllowN("alice", 4, t0); !d.Allowed || d.Remaining != 6 {
		t.Fatalf("AllowN(4) = {allowed:%v remaining:%d}, want {true 6}", d.Allowed, d.Remaining)
	}

	// Only 6 tokens left, so a request for 7 must be refused outright.
	if d := l.AllowN("alice", 7, t0); d.Allowed {
		t.Fatal("AllowN(7) with 6 tokens left: got allowed, want blocked")
	}

	// A refused request must not have consumed anything.
	if d := l.AllowN("alice", 6, t0); !d.Allowed {
		t.Fatal("AllowN(6): got blocked, want allowed (refused request consumed tokens)")
	}
}

func TestClockSkewDoesNotMintTokens(t *testing.T) {
	l := New(Config{Limit: 2, Window: time.Minute})
	l.Allow("alice", t0)
	l.Allow("alice", t0)

	// A clock jumping backwards must not be treated as elapsed time.
	if d := l.Allow("alice", t0.Add(-time.Hour)); d.Allowed {
		t.Fatal("backwards clock: got allowed, want blocked")
	}
	// ...and must not have corrupted the bucket's timestamp either.
	if d := l.Allow("alice", t0); d.Allowed {
		t.Fatal("after backwards clock: got allowed, want blocked")
	}
}

func TestIdleFullBucketIsEvicted(t *testing.T) {
	l := New(Config{Limit: 2, Window: time.Minute, IdleTTL: 5 * time.Minute, SweepThreshold: 1})
	l.Allow("alice", t0)
	l.Allow("bob", t0)

	// An hour later both are long since refilled and untouched, so they carry
	// no information and can be dropped.
	l.Allow("carol", t0.Add(time.Hour))

	if got := l.Len(); got != 1 {
		t.Errorf("tracked buckets = %d, want 1 (only carol)", got)
	}
}

// Eviction must never hand back tokens a caller has not earned.
func TestPartiallyDrainedBucketIsNotEvicted(t *testing.T) {
	// SweepThreshold 0 means every call sweeps, so the eviction predicate is
	// genuinely exercised rather than skipped for want of entries.
	l := New(Config{Limit: 10, Window: time.Hour, IdleTTL: time.Minute, SweepThreshold: 0})
	for i := 0; i < 10; i++ {
		l.Allow("alice", t0)
	}

	// Two minutes is well past IdleTTL, but with a one-hour window alice has
	// only earned a fraction of a token back. Dropping her would reset her to
	// a full bucket for free.
	l.Allow("bob", t0.Add(2*time.Minute))

	if _, ok := l.Snapshot()["alice"]; !ok {
		t.Fatal("alice was evicted while still rate-limited")
	}
}

// Dropping a full, idle bucket is indistinguishable from keeping it, so
// eviction settings must not change any decision the limiter makes.
func TestEvictionDoesNotChangeDecisions(t *testing.T) {
	base := Config{Limit: 3, Window: time.Minute}
	evicting := base
	evicting.IdleTTL = time.Minute
	evicting.SweepThreshold = 1

	withEviction := New(evicting)
	withoutEviction := New(base)

	steps := []struct {
		caller string
		at     time.Time
	}{
		{"alice", t0},
		{"alice", t0},
		{"bob", t0},
		{"alice", t0.Add(10 * time.Second)},
		{"alice", t0.Add(10 * time.Second)},
		{"carol", t0.Add(time.Hour)},
		{"alice", t0.Add(time.Hour)},
		{"bob", t0.Add(2 * time.Hour)},
	}

	for i, s := range steps {
		got := withEviction.Allow(s.caller, s.at)
		want := withoutEviction.Allow(s.caller, s.at)
		if got != want {
			t.Errorf("step %d (%s): with eviction = %+v, without = %+v", i, s.caller, got, want)
		}
	}
}

// Refill must round the same way on every CPU architecture, or nodes applying
// an identical Raft log compute different balances and diverge.
//
// arm64 can fold `tokens + Limit*(d/Window)` into one fused multiply-add, which
// rounds once; amd64 emits a separate multiply and add, which rounds twice. The
// limiter pins the arithmetic to the two-rounding form so both agree. The chosen
// values are ones where the two forms genuinely disagree, so removing the pin
// changes the result. This test can only fail on an architecture that fuses
// (arm64); on amd64 it passes either way.
func TestRefillArithmeticIsNotFused(t *testing.T) {
	const (
		limit   = 1002
		elapsed = 42 * time.Millisecond
		window  = time.Second
	)

	l := New(Config{Limit: limit, Window: window})
	l.AllowN("alice", 100.25, t0) // leaves exactly 901.75 tokens
	start := l.Snapshot()["alice"].Tokens

	ratio := float64(elapsed) / float64(window)
	// The explicit conversion pins the product's rounding, matching the limiter.
	want := start + float64(float64(limit)*ratio)
	fused := math.FMA(float64(limit), ratio, start)

	if want == fused {
		t.Fatalf("test values no longer distinguish fused from split arithmetic "+
			"(start=%v ratio=%v); pick new ones or this test guards nothing", start, ratio)
	}

	// Consume nothing, so the only thing that moves the balance is the refill.
	l.AllowN("alice", 0, t0.Add(elapsed))
	got := l.Snapshot()["alice"].Tokens

	if got != want {
		t.Errorf("refill = %.20f, want %.20f\nfused multiply-add would give %.20f - "+
			"the pin in accrued() was likely removed", got, want, fused)
	}
}

// The property Raft replication depends on: the limiter is a deterministic
// state machine. Replaying one ordered sequence of commands on a second,
// independent instance must reproduce byte-identical state (PRD.md Section 14).
func TestReplayProducesIdenticalState(t *testing.T) {
	cfg := Config{Limit: 5, Window: time.Minute, IdleTTL: 10 * time.Minute, SweepThreshold: 2}

	steps := []struct {
		caller string
		amount float64
		at     time.Time
	}{
		{"alice", 1, t0},
		{"bob", 2, t0.Add(time.Second)},
		{"alice", 1, t0.Add(2 * time.Second)},
		{"alice", 3, t0.Add(3 * time.Second)},
		{"carol", 1, t0.Add(20 * time.Second)},
		{"bob", 4, t0.Add(45 * time.Second)},
		{"alice", 2, t0.Add(time.Minute)},
		{"dave", 5, t0.Add(30 * time.Minute)},
	}

	leader := New(cfg)
	follower := New(cfg)
	for _, s := range steps {
		leader.AllowN(s.caller, s.amount, s.at)
	}
	for _, s := range steps {
		follower.AllowN(s.caller, s.amount, s.at)
	}

	if !reflect.DeepEqual(leader.Snapshot(), follower.Snapshot()) {
		t.Errorf("replicas diverged:\n leader   = %+v\n follower = %+v",
			leader.Snapshot(), follower.Snapshot())
	}
}

// Under concurrency the limiter must hand out exactly Limit tokens, never more.
// Run with -race this also proves there is no data race on the store.
func TestConcurrentAllowNeverExceedsLimit(t *testing.T) {
	const (
		limit    = 100
		contend  = 500
		theCalle = "alice"
	)
	l := New(Config{Limit: limit, Window: time.Hour})

	var allowed int64
	var wg sync.WaitGroup
	for i := 0; i < contend; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Allow(theCalle, t0).Allowed {
				atomic.AddInt64(&allowed, 1)
			}
		}()
	}
	wg.Wait()

	if allowed != limit {
		t.Errorf("allowed = %d, want exactly %d (tokens were double-spent or lost)", allowed, limit)
	}
}

func TestSetConfigAppliesNewLimit(t *testing.T) {
	l := New(Config{Limit: 1, Window: time.Minute})
	l.Allow("alice", t0)

	l.SetConfig(Config{Limit: 5, Window: time.Minute})

	if got := l.Config().Limit; got != 5 {
		t.Errorf("Config().Limit = %d, want 5", got)
	}

	// A caller seen after the change gets the new capacity.
	for i := 1; i <= 5; i++ {
		if d := l.Allow("bob", t0); !d.Allowed {
			t.Fatalf("bob request %d: got blocked, want allowed under new limit", i)
		}
	}
	if d := l.Allow("bob", t0); d.Allowed {
		t.Fatal("bob exceeded the new limit")
	}
}

// Lowering the limit must immediately cap callers already holding more tokens
// than the new capacity allows.
func TestLoweringLimitCapsExistingBucket(t *testing.T) {
	l := New(Config{Limit: 10, Window: time.Minute})
	l.Allow("alice", t0) // alice now holds 9 tokens

	l.SetConfig(Config{Limit: 2, Window: time.Minute})

	for i := 1; i <= 2; i++ {
		if d := l.Allow("alice", t0); !d.Allowed {
			t.Fatalf("alice request %d: got blocked, want allowed", i)
		}
	}
	if d := l.Allow("alice", t0); d.Allowed {
		t.Fatal("alice kept tokens above the lowered limit")
	}
}
