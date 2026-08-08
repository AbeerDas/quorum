package api

import (
	"net/http"
	"testing"
	"time"

	"github.com/AbeerDas/quorum/fault"
	"github.com/AbeerDas/quorum/limiter"
)

// newDemoServer is a node with the demo controls switched on. The limiter's
// clock is frozen, so tokens never refill and a caller's budget is exactly the
// limit - which keeps the swarm assertions below deterministic.
func newDemoServer(t *testing.T, cfg limiter.Config) (*Server, *fault.Injector) {
	t.Helper()

	inj := fault.New()
	t.Cleanup(inj.Close)

	clock := &fakeClock{t: t0}
	srv := NewServer(ServerConfig{
		Backend: NewSingleNodeBackend(limiter.New(cfg), "node-test"),
		NodeID:  "node-test",
		Now:     clock.Now,
		Faults:  inj,
	})
	t.Cleanup(srv.StopSwarm)

	return srv, inj
}

// The controls let anyone stop a node over plain HTTP, so they must not exist
// unless the operator asked for them.
func TestDemoControlsAreOffByDefault(t *testing.T) {
	srv, _ := newTestServer(t, limiter.Config{Limit: 10, Window: time.Minute})

	for _, path := range []string{"/admin/kill", "/admin/pause", "/admin/delay", "/admin/revive", "/swarm"} {
		if rec := do(t, srv, http.MethodPost, path, nil); rec.Code != http.StatusNotFound {
			t.Errorf("POST %s returned %d on a node without demo controls, want 404", path, rec.Code)
		}
	}

	st := decode[statusResponse](t, do(t, srv, http.MethodGet, "/status", nil))
	if st.DemoControls {
		t.Error("/status advertises demo controls on a node that has none")
	}
}

func TestAdminKillReportsFaultState(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 10, Window: time.Minute})

	rec := do(t, srv, http.MethodPost, "/admin/kill", nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /admin/kill returned %d, want 200", rec.Code)
	}
	if got := decode[adminResponse](t, rec).Fault.Mode; got != fault.Killed {
		t.Fatalf("kill reported mode %q, want %q", got, fault.Killed)
	}

	st := decode[statusResponse](t, do(t, srv, http.MethodGet, "/status", nil))
	if st.Fault.Mode != fault.Killed {
		t.Fatalf("/status reports mode %q after a kill, want %q", st.Fault.Mode, fault.Killed)
	}
	if !st.DemoControls {
		t.Error("/status does not advertise demo controls on a node that has them")
	}
}

// A machine that has crashed does not serve its clients either. If a killed
// node kept answering /check, the dashboard would show a node marked down that
// was still quietly doing the work.
func TestKilledNodeRefusesChecks(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 10, Window: time.Minute})

	do(t, srv, http.MethodPost, "/admin/kill", nil)

	rec := do(t, srv, http.MethodPost, "/check", checkRequest{CallerID: "alice"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST /check on a killed node returned %d, want 503", rec.Code)
	}
}

// A frozen node's Raft state is whatever it was at the moment it stopped, so a
// killed leader goes on describing itself as the leader. Left uncorrected the
// dashboard would draw two leaders at once, and a viewer counting them would
// conclude the consensus protocol had failed. PRD.md Section 8 gives the third
// state a node can be drawn in: down.
func TestDownNodeReportsRoleDown(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 10, Window: time.Minute})

	before := decode[statusResponse](t, do(t, srv, http.MethodGet, "/status", nil))
	if before.Role == "down" {
		t.Fatal("a healthy node already reports itself down")
	}

	for _, control := range []string{"/admin/kill", "/admin/pause"} {
		do(t, srv, http.MethodPost, control, nil)

		st := decode[statusResponse](t, do(t, srv, http.MethodGet, "/status", nil))
		if st.Role != "down" {
			t.Errorf("after %s /status reports role %q, want \"down\"", control, st.Role)
		}

		do(t, srv, http.MethodPost, "/admin/revive", nil)
	}

	after := decode[statusResponse](t, do(t, srv, http.MethodGet, "/status", nil))
	if after.Role == "down" {
		t.Fatal("node still reports itself down after being revived")
	}
}

func TestPausedNodeRefusesChecks(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 10, Window: time.Minute})

	do(t, srv, http.MethodPost, "/admin/pause", nil)

	rec := do(t, srv, http.MethodPost, "/check", checkRequest{CallerID: "alice"})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST /check on a paused node returned %d, want 503", rec.Code)
	}
}

// Revive is the button that makes the demo repeatable. If it did not fully
// restore the node, every run of the demo would leave the cluster a little
// more broken than the last.
func TestAdminReviveRestoresService(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 10, Window: time.Minute})

	do(t, srv, http.MethodPost, "/admin/kill", nil)
	rec := do(t, srv, http.MethodPost, "/admin/revive", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("POST /admin/revive returned %d, want 200", rec.Code)
	}
	if got := decode[adminResponse](t, rec).Fault.Mode; got != fault.Healthy {
		t.Fatalf("revive reported mode %q, want %q", got, fault.Healthy)
	}
	if rec := do(t, srv, http.MethodPost, "/check", checkRequest{CallerID: "alice"}); rec.Code != http.StatusOK {
		t.Fatalf("POST /check after revive returned %d, want 200", rec.Code)
	}
}

func TestAdminDelaySetsAndClears(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 10, Window: time.Minute})

	rec := do(t, srv, http.MethodPost, "/admin/delay", delayRequest{DelayMS: 120})
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /admin/delay returned %d, want 200", rec.Code)
	}
	if got := decode[adminResponse](t, rec).Fault.DelayMS; got != 120 {
		t.Fatalf("delay reported %dms, want 120", got)
	}

	// A delayed node is slow, not down: it must still serve traffic.
	if rec := do(t, srv, http.MethodPost, "/check", checkRequest{CallerID: "alice"}); rec.Code != http.StatusOK {
		t.Fatalf("POST /check on a delayed node returned %d, want 200", rec.Code)
	}

	do(t, srv, http.MethodPost, "/admin/revive", nil)
	st := decode[statusResponse](t, do(t, srv, http.MethodGet, "/status", nil))
	if st.Fault.DelayMS != 0 {
		t.Fatalf("/status reports %dms of delay after revive, want 0", st.Fault.DelayMS)
	}
}

func TestAdminDelayRejectsBadInput(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 10, Window: time.Minute})

	for _, body := range []any{delayRequest{DelayMS: -1}, "not json"} {
		if rec := do(t, srv, http.MethodPost, "/admin/delay", body); rec.Code != http.StatusBadRequest {
			t.Errorf("POST /admin/delay with %v returned %d, want 400", body, rec.Code)
		}
	}
}

func TestDemoEndpointsRejectWrongMethod(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 10, Window: time.Minute})

	for _, path := range []string{"/admin/kill", "/admin/pause", "/admin/delay", "/admin/revive"} {
		if rec := do(t, srv, http.MethodGet, path, nil); rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("GET %s returned %d, want 405", path, rec.Code)
		}
	}
}

// awaitSwarmDone polls until the generator has finished, so tests assert on a
// settled result rather than racing a run that is still in flight.
func awaitSwarmDone(t *testing.T, srv *Server, within time.Duration) swarmStatus {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		st := decode[swarmStatus](t, do(t, srv, http.MethodGet, "/swarm", nil))
		if !st.Running {
			return st
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("swarm still running after %v", within)
	return swarmStatus{}
}

// The point of the built-in generator is that the demo needs no terminal, so
// it has to produce real traffic through the node's own request path.
func TestSwarmGeneratesRealTraffic(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 1_000_000, Window: time.Minute})

	rec := do(t, srv, http.MethodPost, "/swarm", swarmRequest{
		Rate: 200, DurationMS: 300, CallerMix: "many_fair",
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /swarm returned %d, want 202", rec.Code)
	}

	final := awaitSwarmDone(t, srv, 5*time.Second)
	if final.Sent == 0 {
		t.Fatal("swarm finished having sent nothing")
	}
	if final.Allowed == 0 {
		t.Fatalf("swarm sent %d requests but none were allowed", final.Sent)
	}

	st := decode[statusResponse](t, do(t, srv, http.MethodGet, "/status", nil))
	if st.AllowedTotal == 0 {
		t.Fatal("swarm traffic did not reach the node's own counters")
	}
}

// The fairness point the dashboard exists to make: one caller burning through
// its budget must not cost the well-behaved callers anything.
func TestSwarmAbusiveCallerDoesNotAffectOthers(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 5, Window: time.Minute})

	rec := do(t, srv, http.MethodPost, "/swarm", swarmRequest{
		Rate: 200, DurationMS: 400, CallerMix: "one_abusive",
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("POST /swarm returned %d, want 202", rec.Code)
	}

	final := awaitSwarmDone(t, srv, 5*time.Second)

	var abusiveBlocked, fairAllowed, fairBlocked uint64
	for _, c := range final.Callers {
		if c.Abusive {
			abusiveBlocked += c.Blocked
			continue
		}
		fairAllowed += c.Allowed
		fairBlocked += c.Blocked
	}

	if abusiveBlocked == 0 {
		t.Fatalf("the abusive caller was never blocked against a limit of 5; callers were %+v", final.Callers)
	}
	if fairAllowed == 0 {
		t.Fatalf("no well-behaved caller got through; callers were %+v", final.Callers)
	}
	if fairBlocked != 0 {
		t.Fatalf("%d well-behaved requests were blocked by another caller's abuse; callers were %+v",
			fairBlocked, final.Callers)
	}
}

func TestSwarmStopsOnDelete(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 1_000_000, Window: time.Minute})

	do(t, srv, http.MethodPost, "/swarm", swarmRequest{
		Rate: 100, DurationMS: 60_000, CallerMix: "many_fair",
	})

	if rec := do(t, srv, http.MethodDelete, "/swarm", nil); rec.Code != http.StatusOK {
		t.Fatalf("DELETE /swarm returned %d, want 200", rec.Code)
	}

	st := awaitSwarmDone(t, srv, 2*time.Second)
	if st.Running {
		t.Fatal("swarm still running after DELETE")
	}
}

// The dashboard drives this from a slider, so changing the rate mid-run has to
// replace the swarm rather than stack a second one on top of it.
func TestSwarmRestartReplacesTheRunningOne(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 1_000_000, Window: time.Minute})

	do(t, srv, http.MethodPost, "/swarm", swarmRequest{
		Rate: 100, DurationMS: 60_000, CallerMix: "many_fair",
	})
	rec := do(t, srv, http.MethodPost, "/swarm", swarmRequest{
		Rate: 250, DurationMS: 300, CallerMix: "one_abusive",
	})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("restarting the swarm returned %d, want 202", rec.Code)
	}

	running := decode[swarmStatus](t, do(t, srv, http.MethodGet, "/swarm", nil))
	if running.Rate != 250 {
		t.Fatalf("swarm reports rate %d after a restart, want 250", running.Rate)
	}
	if running.CallerMix != "one_abusive" {
		t.Fatalf("swarm reports mix %q after a restart, want one_abusive", running.CallerMix)
	}

	awaitSwarmDone(t, srv, 5*time.Second)
}

func TestSwarmRejectsBadInput(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 10, Window: time.Minute})

	cases := []struct {
		name string
		body any
	}{
		{"zero rate", swarmRequest{Rate: 0, DurationMS: 100, CallerMix: "many_fair"}},
		{"negative rate", swarmRequest{Rate: -5, DurationMS: 100, CallerMix: "many_fair"}},
		{"zero duration", swarmRequest{Rate: 10, DurationMS: 0, CallerMix: "many_fair"}},
		{"unknown mix", swarmRequest{Rate: 10, DurationMS: 100, CallerMix: "chaos"}},
		{"not json", "{"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if rec := do(t, srv, http.MethodPost, "/swarm", tc.body); rec.Code != http.StatusBadRequest {
				t.Fatalf("POST /swarm returned %d, want 400", rec.Code)
			}
		})
	}
}

// A swarm aimed at a node that has been killed should fail loudly rather than
// silently reporting success, so the dashboard can show the traffic dying with
// the machine that was generating it.
func TestSwarmOnKilledNodeRecordsFailures(t *testing.T) {
	srv, _ := newDemoServer(t, limiter.Config{Limit: 1_000_000, Window: time.Minute})

	do(t, srv, http.MethodPost, "/admin/kill", nil)
	do(t, srv, http.MethodPost, "/swarm", swarmRequest{
		Rate: 100, DurationMS: 200, CallerMix: "many_fair",
	})

	final := awaitSwarmDone(t, srv, 5*time.Second)
	if final.Failed == 0 {
		t.Fatalf("swarm against a killed node recorded no failures: %+v", final)
	}
	if final.Allowed != 0 {
		t.Fatalf("swarm against a killed node had %d requests allowed", final.Allowed)
	}
}
