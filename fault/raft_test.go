package fault

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/AbeerDas/quorum/raft"
)

// recordingTransport stands in for the real gRPC transport so the tests can
// assert not just what the caller saw, but whether the message was sent at all.
type recordingTransport struct{ calls atomic.Int64 }

func (r *recordingTransport) RequestVote(context.Context, raft.NodeID, *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	r.calls.Add(1)
	return &raft.RequestVoteReply{Term: 7, VoteGranted: true}, nil
}

func (r *recordingTransport) AppendEntries(context.Context, raft.NodeID, *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	r.calls.Add(1)
	return &raft.AppendEntriesReply{Term: 7, Success: true}, nil
}

func TestTransportDeliversWhenHealthy(t *testing.T) {
	inner := &recordingTransport{}
	inj := New()
	defer inj.Close()

	tr := NewTransport(inner, inj)

	reply, err := tr.RequestVote(context.Background(), "node-2", &raft.RequestVoteArgs{Term: 7})
	if err != nil {
		t.Fatalf("RequestVote returned %v, want nil", err)
	}
	if !reply.VoteGranted {
		t.Fatal("vote was not granted, so the reply did not come from the inner transport")
	}
	if got := inner.calls.Load(); got != 1 {
		t.Fatalf("inner transport called %d times, want 1", got)
	}
}

// A killed node must not merely fail to be heard - it must not speak at all.
// If the message still went out, the node would keep voting and heartbeating
// while the dashboard showed it as down.
func TestTransportBlocksOutboundWhenKilled(t *testing.T) {
	inner := &recordingTransport{}
	inj := New()
	defer inj.Close()
	inj.Kill()

	tr := NewTransport(inner, inj)

	if _, err := tr.RequestVote(context.Background(), "node-2", &raft.RequestVoteArgs{}); !errors.Is(err, ErrNodeDown) {
		t.Fatalf("RequestVote returned %v, want ErrNodeDown", err)
	}
	if _, err := tr.AppendEntries(context.Background(), "node-2", &raft.AppendEntriesArgs{}); !errors.Is(err, ErrNodeDown) {
		t.Fatalf("AppendEntries returned %v, want ErrNodeDown", err)
	}
	if got := inner.calls.Load(); got != 0 {
		t.Fatalf("inner transport was called %d times by a killed node, want 0", got)
	}
}

func TestTransportDelaysOutbound(t *testing.T) {
	inner := &recordingTransport{}
	inj := New()
	defer inj.Close()
	inj.SetDelay(60 * time.Millisecond)

	tr := NewTransport(inner, inj)

	start := time.Now()
	if _, err := tr.AppendEntries(context.Background(), "node-2", &raft.AppendEntriesArgs{}); err != nil {
		t.Fatalf("AppendEntries returned %v, want nil", err)
	}
	if took := time.Since(start); took < 50*time.Millisecond {
		t.Fatalf("delayed AppendEntries took %v, want at least the configured 60ms", took)
	}
}

// unaryInfo is the minimum gRPC metadata the interceptor is handed.
var unaryInfo = &grpc.UnaryServerInfo{FullMethod: "/raft.Raft/AppendEntries"}

func TestInterceptorPassesThroughWhenHealthy(t *testing.T) {
	inj := New()
	defer inj.Close()

	var served atomic.Int64
	handler := func(context.Context, any) (any, error) {
		served.Add(1)
		return "ok", nil
	}

	got, err := inj.UnaryServerInterceptor()(context.Background(), nil, unaryInfo, handler)
	if err != nil {
		t.Fatalf("interceptor returned %v, want nil", err)
	}
	if got != "ok" {
		t.Fatalf("interceptor returned %v, want the handler's reply", got)
	}
	if served.Load() != 1 {
		t.Fatal("handler was not reached on a healthy node")
	}
}

// A crashed host refuses the connection. Reporting Unavailable is what makes a
// killed node look like a dead machine to its peers rather than a live node
// that happens to be saying no.
func TestInterceptorRejectsInboundWhenKilled(t *testing.T) {
	inj := New()
	defer inj.Close()
	inj.Kill()

	var served atomic.Int64
	handler := func(context.Context, any) (any, error) {
		served.Add(1)
		return "ok", nil
	}

	_, err := inj.UnaryServerInterceptor()(context.Background(), nil, unaryInfo, handler)
	if err == nil {
		t.Fatal("interceptor accepted a request on a killed node")
	}
	if code := status.Code(err); code != codes.Unavailable {
		t.Fatalf("interceptor returned code %v, want %v", code, codes.Unavailable)
	}
	if served.Load() != 0 {
		t.Fatal("handler ran on a killed node")
	}
}

// The difference between a kill and a pause lives here: a paused node accepts
// the call and then goes silent, so the caller burns its own timeout.
func TestInterceptorHangsWhenPaused(t *testing.T) {
	inj := New()
	defer inj.Close()
	inj.Pause()

	var served atomic.Int64
	handler := func(context.Context, any) (any, error) {
		served.Add(1)
		return "ok", nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := inj.UnaryServerInterceptor()(ctx, nil, unaryInfo, handler)
	took := time.Since(start)

	if err == nil {
		t.Fatal("interceptor answered on a paused node")
	}
	if took < 50*time.Millisecond {
		t.Fatalf("paused interceptor gave up after %v, want it to hang until the caller's deadline", took)
	}
	if served.Load() != 0 {
		t.Fatal("handler ran on a paused node")
	}
}

// A pause that ends must let queued traffic through rather than having failed
// it, otherwise reviving a hung node would still look broken for a full round.
func TestInterceptorServesAfterRevive(t *testing.T) {
	inj := New()
	defer inj.Close()
	inj.Pause()

	handler := func(context.Context, any) (any, error) { return "ok", nil }

	type result struct {
		val any
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := inj.UnaryServerInterceptor()(context.Background(), nil, unaryInfo, handler)
		done <- result{v, err}
	}()

	time.Sleep(40 * time.Millisecond)
	inj.Revive()

	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("interceptor returned %v after Revive, want nil", got.err)
		}
		if got.val != "ok" {
			t.Fatalf("interceptor returned %v after Revive, want the handler's reply", got.val)
		}
	case <-time.After(time.Second):
		t.Fatal("interceptor stayed blocked after Revive")
	}
}
