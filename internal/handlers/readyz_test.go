package handlers_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	"instant.dev/common/readiness"
	"instant.dev/provisioner/internal/handlers"
)

type fakePool struct{ err error }

func (f fakePool) Ping(ctx context.Context) error { return f.err }

type fakeCircuit struct {
	name string
	open bool
}

func (f fakeCircuit) Name() string { return f.name }
func (f fakeCircuit) IsOpen() bool { return f.open }

// TestReadyz_AllOK — happy path: pool pings ok, no circuits, 200/ok.
func TestReadyz_AllOK(t *testing.T) {
	h := handlers.NewReadyzHandler(handlers.Config{
		PoolPinger: fakePool{err: nil},
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.Get(rr, req)

	if rr.Code != 200 {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var got readiness.Response
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("body not valid JSON: %v", err)
	}
	if got.Service != "instant-provisioner" {
		t.Fatalf("want service=instant-provisioner, got %q", got.Service)
	}
	if got.Overall != readiness.StatusOK {
		t.Fatalf("want overall=ok, got %q", got.Overall)
	}
}

// TestReadyz_PoolDisabled_Is200 — when the operator intentionally
// runs without a PROVISIONER_DATABASE_URL (hot pool feature off),
// main.go passes PoolPinger=nil to signal "skip platform_db
// entirely." The provisioner serves gRPC fine without the pool, so
// /readyz must return 200/ok rather than 503-ing the pod out of
// the Service endpoints (which would also blackout /metrics
// scraping → silently dead NR alerts). See BugBash B14-P0-F2.
//
// This replaces the prior TestReadyz_PoolNotConfigured_Is503 which
// encoded the broken semantics — a nil PoolPinger no longer means
// "failed", it means "not a registered check at all."
func TestReadyz_PoolDisabled_Is200(t *testing.T) {
	h := handlers.NewReadyzHandler(handlers.Config{
		PoolPinger: nil,
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.Get(rr, req)

	if rr.Code != 200 {
		t.Fatalf("nil PoolPinger (pool intentionally disabled) must yield 200, got %d", rr.Code)
	}
	var got readiness.Response
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("body not valid JSON: %v", err)
	}
	if got.Overall != readiness.StatusOK {
		t.Fatalf("want overall=ok, got %q", got.Overall)
	}
	// platform_db must NOT appear in the checks list when the pool
	// is disabled — registering an always-ok or always-failed dummy
	// check would re-introduce the false-alarm pattern.
	for _, c := range got.Checks {
		if c.Name == "platform_db" {
			t.Fatalf("platform_db check must be omitted when pool is disabled; found %+v", c)
		}
	}
}

// TestReadyz_PoolEnabledButNotReady_Is503 — guards the failure
// mode the new nil semantics could hide. When PROVISIONER_DATABASE_URL
// is set (so main wires a real poolProbe adapter), but the lazy
// poolHolder.Set() has not fired yet because the DB was unreachable
// at boot, platform_db must surface as failed-critical → 503 so the
// operator sees the wire signal. The fake adapter returns the same
// "pool_not_ready" error path the production lazy adapter uses.
func TestReadyz_PoolEnabledButNotReady_Is503(t *testing.T) {
	h := handlers.NewReadyzHandler(handlers.Config{
		PoolPinger: fakePool{err: errors.New("pool_not_ready")},
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.Get(rr, req)
	if rr.Code != 503 {
		t.Fatalf("pool wired but unset must yield 503, got %d", rr.Code)
	}
}

// TestReadyz_PoolPingFailed_Is503 — pool is wired but the ping fails
// (network blip, password rotation). Still critical → 503.
func TestReadyz_PoolPingFailed_Is503(t *testing.T) {
	h := handlers.NewReadyzHandler(handlers.Config{
		PoolPinger: fakePool{err: errors.New("connection refused")},
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.Get(rr, req)
	if rr.Code != 503 {
		t.Fatalf("pool ping failure must yield 503, got %d", rr.Code)
	}
}

// TestReadyz_OpenCircuit_Degrades — an open backend circuit surfaces
// as degraded (not failed). The provisioner stays in rotation; the
// circuit's own half-open recovery handles the actual fix.
func TestReadyz_OpenCircuit_Degrades(t *testing.T) {
	h := handlers.NewReadyzHandler(handlers.Config{
		PoolPinger: fakePool{err: nil},
		Circuits: []handlers.CircuitInspector{
			fakeCircuit{name: "postgres", open: true},
			fakeCircuit{name: "redis", open: false},
		},
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.Get(rr, req)

	if rr.Code != 200 {
		t.Fatalf("open circuit (non-critical) must NOT return 503, got %d", rr.Code)
	}
	var got readiness.Response
	_ = json.Unmarshal(rr.Body.Bytes(), &got)
	if got.Overall != readiness.StatusDegraded {
		t.Fatalf("want overall=degraded, got %q", got.Overall)
	}
	// Spot-check the named checks landed.
	names := map[string]readiness.Status{}
	for _, c := range got.Checks {
		names[c.Name] = c.Status
	}
	if names["backend_postgres"] != readiness.StatusDegraded {
		t.Fatalf("expected backend_postgres=degraded, got %q", names["backend_postgres"])
	}
	if names["backend_redis"] != readiness.StatusOK {
		t.Fatalf("expected backend_redis=ok, got %q", names["backend_redis"])
	}
}

// TestReadyz_ContentType_AndNoStore — the response is JSON and tagged
// no-store so probes don't stale.
func TestReadyz_ContentType_AndNoStore(t *testing.T) {
	h := handlers.NewReadyzHandler(handlers.Config{
		PoolPinger: fakePool{err: nil},
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/readyz", nil)
	h.Get(rr, req)
	if rr.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("want Content-Type=application/json, got %q", rr.Header().Get("Content-Type"))
	}
	if rr.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("want Cache-Control=no-store, got %q", rr.Header().Get("Cache-Control"))
	}
}
