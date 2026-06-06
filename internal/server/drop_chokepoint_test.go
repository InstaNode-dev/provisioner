package server

// drop_chokepoint_test.go — unit tests for the sanctioned customer-data drop
// chokepoint (guardedDrop). These assert the truehomie-incident invariant: every
// drop the provisioner performs is recorded (metric + audit log) and attributed,
// and the metric outcome label is correct for ok / error / breaker-open.

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/peer"

	commonv1 "instant.dev/proto/common/v1"
	provisionerv1 "instant.dev/proto/provisioner/v1"

	"instant.dev/provisioner/internal/circuit"
)

// freshBreaker returns a closed breaker with a high threshold so a single
// failure in a test never trips it (we drive the open state explicitly).
func freshBreaker() *circuit.Breaker {
	return circuit.NewBreaker("test", 1000, time.Minute)
}

func dropReq(token string) *provisionerv1.DeprovisionRequest {
	return &provisionerv1.DeprovisionRequest{
		Token:              token,
		ProviderResourceId: "local:0",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		RequestId:          "req-test",
	}
}

func TestGuardedDrop_Success_IncrementsOkOutcome(t *testing.T) {
	s := &Server{breakers: circuit.NewBreakers()}
	before := testutil.ToFloat64(dropTotal.WithLabelValues("RESOURCE_TYPE_POSTGRES", "shared", "ok"))

	called := false
	err := s.guardedDrop(context.Background(), dropReq("tok-ok"), dropBackendShared, freshBreaker(), func() error {
		called = true
		return nil
	})
	if err != nil {
		t.Fatalf("guardedDrop returned error: %v", err)
	}
	if !called {
		t.Fatal("backend fn was not invoked")
	}
	after := testutil.ToFloat64(dropTotal.WithLabelValues("RESOURCE_TYPE_POSTGRES", "shared", "ok"))
	if after != before+1 {
		t.Fatalf("ok counter: got %v want %v", after, before+1)
	}
}

func TestGuardedDrop_BackendError_IncrementsErrorOutcome_AndPropagates(t *testing.T) {
	s := &Server{breakers: circuit.NewBreakers()}
	before := testutil.ToFloat64(dropTotal.WithLabelValues("RESOURCE_TYPE_POSTGRES", "shared", "error"))

	sentinel := errors.New("boom")
	err := s.guardedDrop(context.Background(), dropReq("tok-err"), dropBackendShared, freshBreaker(), func() error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error to propagate, got %v", err)
	}
	after := testutil.ToFloat64(dropTotal.WithLabelValues("RESOURCE_TYPE_POSTGRES", "shared", "error"))
	if after != before+1 {
		t.Fatalf("error counter: got %v want %v", after, before+1)
	}
}

func TestGuardedDrop_BreakerOpen_DoesNotInvokeBackend_AndRecordsBreakerOpen(t *testing.T) {
	s := &Server{breakers: circuit.NewBreakers()}
	before := testutil.ToFloat64(dropTotal.WithLabelValues("RESOURCE_TYPE_POSTGRES", "shared", "breaker_open"))

	// Trip a fresh breaker: threshold 1, so one recorded failure opens it.
	br := circuit.NewBreaker("test-open", 1, time.Minute)
	br.Record(errors.New("trip"))
	if br.Allow() {
		t.Fatal("breaker should be open after exceeding threshold")
	}

	called := false
	err := s.guardedDrop(context.Background(), dropReq("tok-open"), dropBackendShared, br, func() error {
		called = true
		return nil
	})
	if !errors.Is(err, circuit.ErrOpen) {
		t.Fatalf("expected circuit.ErrOpen, got %v", err)
	}
	if called {
		t.Fatal("backend fn must NOT be invoked when breaker is open — a drop must never reach the backend through an open breaker")
	}
	after := testutil.ToFloat64(dropTotal.WithLabelValues("RESOURCE_TYPE_POSTGRES", "shared", "breaker_open"))
	if after != before+1 {
		t.Fatalf("breaker_open counter: got %v want %v", after, before+1)
	}
}

func TestCallerFromContext_WithPeer_ReturnsAddr(t *testing.T) {
	ctx := peer.NewContext(context.Background(), &peer.Peer{
		Addr: &net.TCPAddr{IP: net.IPv4(10, 109, 3, 201), Port: 54422},
	})
	got := callerFromContext(ctx)
	if got != "10.109.3.201:54422" {
		t.Fatalf("callerFromContext: got %q want %q", got, "10.109.3.201:54422")
	}
}

func TestCallerFromContext_NoPeer_ReturnsUnknown(t *testing.T) {
	if got := callerFromContext(context.Background()); got != "unknown" {
		t.Fatalf("callerFromContext: got %q want %q", got, "unknown")
	}
}

func TestDropOutcome_Mapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "ok"},
		{"breaker", circuit.ErrOpen, "breaker_open"},
		{"other", errors.New("x"), "error"},
	}
	for _, c := range cases {
		if got := dropOutcome(c.err); got != c.want {
			t.Errorf("%s: dropOutcome=%q want %q", c.name, got, c.want)
		}
	}
}
