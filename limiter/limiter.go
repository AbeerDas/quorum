package limiter

import (
	"sync"
	"time"
)

// Config describes the rate limit applied to every caller independently.
type Config struct {
	// Limit is the bucket capacity: the most requests a caller can make in a
	// burst, and the number of tokens restored over one full Window.
	Limit int
	// Window is how long a caller takes to refill from empty to Limit.
	Window time.Duration

	// IdleTTL is how long a caller that has refilled to capacity is kept before
	// being dropped. A full bucket is indistinguishable from a bucket that was
	// never seen, so dropping it cannot change any future decision - it only
	// reclaims memory. Zero disables eviction entirely.
	IdleTTL time.Duration
	// SweepThreshold is the number of tracked callers tolerated before a sweep
	// for evictable buckets runs. Zero sweeps on every call. Ignored unless
	// IdleTTL is set.
	SweepThreshold int
}

// Decision is the result of a single rate-limit check.
type Decision struct {
	Allowed bool
	// Remaining is the number of whole requests the caller can still make.
	Remaining int
	// RetryAfter is how long until the caller has enough tokens for the request
	// it just made. It is only meaningful when Allowed is false.
	RetryAfter time.Duration
}

// BucketState is an exported copy of one caller's bucket, used to compare state
// across nodes.
type BucketState struct {
	Tokens     float64
	LastRefill time.Time
}

// bucket is one caller's token bucket. Tokens are fractional so they accrue
// smoothly rather than in window-sized steps (PRD.md Section 14).
type bucket struct {
	tokens     float64
	lastRefill time.Time
}

// Limiter is an in-memory token-bucket rate limiter, safe for concurrent use.
//
// Time is never read from the clock inside the limiter: every call takes an
// explicit `now`. That keeps the limiter a deterministic state machine, so
// replaying the same sequence of calls on another node produces byte-identical
// state, which is what Raft replication requires (PRD.md Section 14). For the
// same reason eviction is driven off the caller-supplied `now` and never a
// background goroutine, which would sweep at a different instant on each node
// and pull replicas apart.
type Limiter struct {
	mu      sync.Mutex
	cfg     Config
	buckets map[string]*bucket
}

// New creates a Limiter enforcing cfg.
func New(cfg Config) *Limiter {
	return &Limiter{
		cfg:     cfg,
		buckets: make(map[string]*bucket),
	}
}

// Allow consumes one token for callerID at instant now and reports whether the
// request is permitted. An unknown caller starts with a full bucket.
func (l *Limiter) Allow(callerID string, now time.Time) Decision {
	return l.AllowN(callerID, 1, now)
}

// AllowN consumes amount tokens for callerID at instant now. If the caller has
// fewer than amount tokens the request is refused and nothing is consumed.
func (l *Limiter) AllowN(callerID string, amount float64, now time.Time) Decision {
	l.mu.Lock()
	defer l.mu.Unlock()

	capacity := float64(l.cfg.Limit)

	if l.cfg.IdleTTL > 0 && len(l.buckets) > l.cfg.SweepThreshold {
		l.sweep(now, capacity)
	}

	b, ok := l.buckets[callerID]
	if !ok {
		b = &bucket{tokens: capacity, lastRefill: now}
		l.buckets[callerID] = b
	} else {
		l.refill(b, now, capacity)
	}

	if b.tokens < amount {
		return Decision{
			Allowed:    false,
			Remaining:  int(b.tokens),
			RetryAfter: l.timeToAccrue(amount - b.tokens),
		}
	}

	b.tokens -= amount
	return Decision{Allowed: true, Remaining: int(b.tokens)}
}

// Config returns the limit currently in force.
func (l *Limiter) Config() Config {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.cfg
}

// SetConfig replaces the limit in force. Callers already holding more tokens
// than the new capacity allows are capped immediately, so lowering the limit
// takes effect at once rather than after the next refill.
func (l *Limiter) SetConfig(cfg Config) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.cfg = cfg
	capacity := float64(cfg.Limit)
	for _, b := range l.buckets {
		if b.tokens > capacity {
			b.tokens = capacity
		}
	}
}

// Len is the number of callers currently tracked.
func (l *Limiter) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// Snapshot copies the full limiter state. Two nodes that have applied the same
// ordered sequence of commands must produce equal snapshots.
func (l *Limiter) Snapshot() map[string]BucketState {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make(map[string]BucketState, len(l.buckets))
	for id, b := range l.buckets {
		out[id] = BucketState{Tokens: b.tokens, LastRefill: b.lastRefill}
	}
	return out
}

// refill credits the tokens accrued since the bucket was last touched, capped
// at capacity.
func (l *Limiter) refill(b *bucket, now time.Time, capacity float64) {
	// A clock that moves backwards must never mint tokens, and must not drag
	// lastRefill backwards either.
	if !now.After(b.lastRefill) {
		return
	}

	b.tokens += l.accrued(now.Sub(b.lastRefill))
	if b.tokens > capacity {
		b.tokens = capacity
	}
	b.lastRefill = now
}

// sweep drops every bucket that has sat at capacity for longer than IdleTTL.
//
// The predicate is evaluated independently per entry, so the resulting map does
// not depend on Go's randomized map iteration order - every node sweeping at the
// same `now` deletes exactly the same set.
func (l *Limiter) sweep(now time.Time, capacity float64) {
	for id, b := range l.buckets {
		if l.evictable(b, now, capacity) {
			delete(l.buckets, id)
		}
	}
}

// evictable reports whether b currently carries no information: it has been
// untouched for at least IdleTTL and has refilled to capacity in that time.
func (l *Limiter) evictable(b *bucket, now time.Time, capacity float64) bool {
	idle := now.Sub(b.lastRefill)
	if idle < l.cfg.IdleTTL {
		return false
	}
	return b.tokens+l.accrued(idle) >= capacity
}

// accrued is how many tokens build up over d.
//
// The outer float64 conversion is load-bearing and must not be removed. Without
// it, Go is free to compile `tokens + Limit*(d/Window)` at the call sites into a
// single fused multiply-add. arm64 has that instruction and uses it; amd64 does
// not and emits a separate multiply and add. Fusing rounds once where the split
// form rounds twice, so the two architectures compute measurably different token
// balances - they disagree on roughly a fifth of realistic inputs. Nodes on
// different CPUs would then drift apart while applying an identical Raft log,
// breaking the determinism the replicated state machine depends on (PRD.md
// Section 14) and the "all nodes identical" correctness test (PRD.md Section 9).
// An explicit conversion forces the product to be rounded before the addition,
// so every architecture performs the same two roundings and agrees exactly.
// TestRefillArithmeticIsNotFused guards this.
//
// Limit * (d / Window) is preferred over d * (Limit / Window) only because the
// ratio form stays exact for clean fractions of a window, which keeps the tests
// readable. Neither form is uniformly more accurate than the other.
func (l *Limiter) accrued(d time.Duration) float64 {
	return float64(float64(l.cfg.Limit) * (float64(d) / float64(l.cfg.Window)))
}

// timeToAccrue is the inverse of accrued: how long until `tokens` build up.
func (l *Limiter) timeToAccrue(tokens float64) time.Duration {
	return time.Duration(tokens / float64(l.cfg.Limit) * float64(l.cfg.Window))
}
