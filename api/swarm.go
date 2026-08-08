package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// The swarm is the node's own load generator. PRD.md Section 8 requires the
// dashboard to produce traffic on command, so that watching a limiter allow
// then block does not depend on the viewer having vegeta installed.
//
// Requests are dispatched straight into this node's own router rather than
// over a loopback socket. That keeps the generator from spending the machine's
// file descriptors on itself, and it still exercises the real request path -
// including, on a follower, being forwarded to the leader and replicated
// through Raft.

// swarmRequest is the body of POST /swarm.
type swarmRequest struct {
	Rate       int    `json:"rate"`
	DurationMS int64  `json:"duration_ms"`
	CallerMix  string `json:"caller_mix"`
}

// swarmCaller is one simulated client's tally.
type swarmCaller struct {
	CallerID string `json:"caller_id"`
	Abusive  bool   `json:"abusive"`
	Allowed  uint64 `json:"allowed"`
	Blocked  uint64 `json:"blocked"`
	Failed   uint64 `json:"failed"`
}

// swarmStatus is what the dashboard polls while a swarm runs.
type swarmStatus struct {
	Running     bool   `json:"running"`
	Rate        int    `json:"rate"`
	CallerMix   string `json:"caller_mix"`
	RemainingMS int64  `json:"remaining_ms"`

	Sent    uint64 `json:"sent"`
	Allowed uint64 `json:"allowed"`
	Blocked uint64 `json:"blocked"`
	Failed  uint64 `json:"failed"`

	// Dropped counts requests the generator could not issue because it fell
	// behind its own target rate. Reported separately from Failed on purpose:
	// this is the load generator running out of room, not the cluster refusing
	// anything, and conflating the two would make the node look worse than it
	// is (see the Stage 8 note on benchmarks that measure their own harness).
	Dropped uint64 `json:"dropped"`

	Callers []swarmCaller `json:"callers"`
}

const (
	mixManyFair   = "many_fair"
	mixOneAbusive = "one_abusive"
)

// callerPlan is the set of simulated clients and how traffic is split between
// them. When abusive is set, ids[0] is the badly behaved caller.
type callerPlan struct {
	ids     []string
	abusive bool
}

// abusiveShare is how much of the traffic the one abusive caller sends. Nine
// in ten is enough to burn its own budget quickly while still leaving the
// well-behaved callers visibly unaffected, which is the point being made.
const abusiveShare = 10

func newCallerPlan(mix string) (callerPlan, error) {
	switch mix {
	case mixManyFair:
		ids := make([]string, 0, 20)
		for i := 1; i <= 20; i++ {
			ids = append(ids, fmt.Sprintf("fair-%02d", i))
		}
		return callerPlan{ids: ids}, nil

	case mixOneAbusive:
		ids := []string{"abusive-1"}
		for i := 1; i <= 5; i++ {
			ids = append(ids, fmt.Sprintf("fair-%02d", i))
		}
		return callerPlan{ids: ids, abusive: true}, nil

	default:
		return callerPlan{}, fmt.Errorf("caller_mix must be %q or %q", mixManyFair, mixOneAbusive)
	}
}

// pick chooses which caller sends request n.
func (p callerPlan) pick(n uint64) int {
	if !p.abusive {
		return int(n % uint64(len(p.ids)))
	}
	if n%abusiveShare != 0 {
		return 0
	}
	return 1 + int((n/abusiveShare)%uint64(len(p.ids)-1))
}

// swarmCounts is one caller's running tally.
type swarmCounts struct {
	allowed atomic.Uint64
	blocked atomic.Uint64
	failed  atomic.Uint64
}

// swarmRun is one execution of the load generator.
type swarmRun struct {
	rate   int
	mix    string
	plan   callerPlan
	endsAt time.Time

	cancel   context.CancelFunc
	done     chan struct{}
	finished atomic.Bool

	sent    atomic.Uint64
	allowed atomic.Uint64
	blocked atomic.Uint64
	failed  atomic.Uint64
	dropped atomic.Uint64

	counts []*swarmCounts
}

func (r *swarmRun) status() swarmStatus {
	st := swarmStatus{
		Running:   !r.finished.Load(),
		Rate:      r.rate,
		CallerMix: r.mix,
		Sent:      r.sent.Load(),
		Allowed:   r.allowed.Load(),
		Blocked:   r.blocked.Load(),
		Failed:    r.failed.Load(),
		Dropped:   r.dropped.Load(),
		Callers:   make([]swarmCaller, 0, len(r.plan.ids)),
	}

	if st.Running {
		if remaining := time.Until(r.endsAt).Milliseconds(); remaining > 0 {
			st.RemainingMS = remaining
		}
	}

	for i, id := range r.plan.ids {
		st.Callers = append(st.Callers, swarmCaller{
			CallerID: id,
			Abusive:  r.plan.abusive && i == 0,
			Allowed:  r.counts[i].allowed.Load(),
			Blocked:  r.counts[i].blocked.Load(),
			Failed:   r.counts[i].failed.Load(),
		})
	}

	return st
}

// handleSwarm starts, reports on, and stops the built-in load generator.
func (s *Server) handleSwarm(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		s.startSwarm(w, r)
	case http.MethodGet:
		s.writeJSON(w, http.StatusOK, s.swarmStatus())
	case http.MethodDelete:
		s.StopSwarm()
		s.writeJSON(w, http.StatusOK, s.swarmStatus())
	default:
		s.writeError(w, http.StatusMethodNotAllowed, "method not allowed, use POST, GET or DELETE")
	}
}

func (s *Server) startSwarm(w http.ResponseWriter, r *http.Request) {
	var req swarmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.writeError(w, http.StatusBadRequest,
			"body must be JSON of the form {\"rate\": 500, \"duration_ms\": 10000, \"caller_mix\": \"many_fair\"}")
		return
	}
	if req.Rate <= 0 {
		s.writeError(w, http.StatusBadRequest, "rate must be greater than zero")
		return
	}
	if req.DurationMS <= 0 {
		s.writeError(w, http.StatusBadRequest, "duration_ms must be greater than zero")
		return
	}

	plan, err := newCallerPlan(req.CallerMix)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// The dashboard drives this from a slider, so a second start means "change
	// the rate", not "run two swarms at once".
	s.StopSwarm()

	duration := time.Duration(req.DurationMS) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), duration)

	run := &swarmRun{
		rate:   req.Rate,
		mix:    req.CallerMix,
		plan:   plan,
		endsAt: time.Now().Add(duration),
		cancel: cancel,
		done:   make(chan struct{}),
		counts: make([]*swarmCounts, len(plan.ids)),
	}
	for i := range run.counts {
		run.counts[i] = &swarmCounts{}
	}

	s.swarmMu.Lock()
	s.swarm = run
	s.swarmMu.Unlock()

	s.logger.Info("swarm started", "rate", req.Rate, "duration_ms", req.DurationMS, "caller_mix", req.CallerMix)
	go s.runSwarm(ctx, run)

	s.writeJSON(w, http.StatusAccepted, run.status())
}

// swarmStatus reports the current or most recent run.
func (s *Server) swarmStatus() swarmStatus {
	s.swarmMu.Lock()
	run := s.swarm
	s.swarmMu.Unlock()

	if run == nil {
		return swarmStatus{Callers: []swarmCaller{}}
	}
	return run.status()
}

// StopSwarm halts the load generator and waits for it to finish, so a node
// shutting down does not leave traffic being fired at a closing server.
func (s *Server) StopSwarm() {
	s.swarmMu.Lock()
	run := s.swarm
	s.swarmMu.Unlock()

	if run == nil {
		return
	}

	run.cancel()
	<-run.done
}

// runSwarm paces the load and hands each request to a pool of senders.
func (s *Server) runSwarm(ctx context.Context, run *swarmRun) {
	defer func() {
		run.finished.Store(true)
		run.cancel() // release the context's timer even when the duration ran out
		close(run.done)
		s.logger.Info("swarm finished",
			"sent", run.sent.Load(), "allowed", run.allowed.Load(),
			"blocked", run.blocked.Load(), "failed", run.failed.Load(),
			"dropped", run.dropped.Load())
	}()

	// A ticker cannot resolve much below a millisecond, so above 1000 req/s the
	// rate is met by sending a small batch on each tick instead.
	interval := time.Second / time.Duration(run.rate)
	perTick := 1
	if interval < time.Millisecond {
		interval = time.Millisecond
		perTick = run.rate / 1000
		if perTick < 1 {
			perTick = 1
		}
	}

	// Enough senders to cover the in-flight requests at the target rate, capped
	// so a careless slider setting cannot spawn an unbounded number.
	workers := run.rate/50 + 4
	if workers > 128 {
		workers = 128
	}

	jobs := make(chan uint64, 1024)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := range jobs {
				s.fireOne(ctx, run, n)
			}
		}()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var seq uint64
	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return

		case <-ticker.C:
			for i := 0; i < perTick; i++ {
				select {
				case jobs <- seq:
					seq++
				default:
					// Every sender is busy. Counted rather than blocked on: a
					// pacer that waits here would silently lower the rate and
					// report a target it never actually hit.
					run.dropped.Add(1)
				}
			}
		}
	}
}

// fireOne issues a single /check through this node's own router.
func (s *Server) fireOne(ctx context.Context, run *swarmRun, n uint64) {
	idx := run.plan.pick(n)

	body, err := json.Marshal(checkRequest{CallerID: run.plan.ids[idx]})
	if err != nil {
		return
	}

	// The host is ignored by the router; only the path is matched. An absolute
	// URL is required purely because http.NewRequest insists on one.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://127.0.0.1/check", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	rec := &statusOnlyWriter{}
	s.mux.ServeHTTP(rec, req)

	run.sent.Add(1)
	switch rec.status {
	case http.StatusOK:
		run.allowed.Add(1)
		run.counts[idx].allowed.Add(1)
	case http.StatusTooManyRequests:
		run.blocked.Add(1)
		run.counts[idx].blocked.Add(1)
	default:
		run.failed.Add(1)
		run.counts[idx].failed.Add(1)
	}
}

// statusOnlyWriter captures a response's status code and discards its body.
// The generator only ever needs to know allowed, blocked, or failed, and at
// several thousand requests a second the bodies are not worth allocating.
type statusOnlyWriter struct {
	status int
	header http.Header
}

func (w *statusOnlyWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *statusOnlyWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return len(b), nil
}

func (w *statusOnlyWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}
