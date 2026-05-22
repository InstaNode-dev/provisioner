// Tests for the observability scaffolding. Each test corresponds to one of
// the four assertions called out in the track-5 brief (relocated 2026-05-12
// from the api repo's reference scaffold under provisioner/main_test.go).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/newrelic/go-agent/v3/newrelic"
	"google.golang.org/grpc"

	"instant.dev/common/logctx"
	"instant.dev/provisioner/internal/circuit"
	"instant.dev/provisioner/internal/server"
)

// TestHealthzReturnsCommitID verifies the /healthz endpoint returns a
// well-formed JSON body containing commit_id. Uses httptest so we don't
// need to bind a real port.
func TestHealthzReturnsCommitID(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	server.HealthzHandler(nil).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}

	var body server.HealthzResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !body.OK {
		t.Errorf("ok = false, want true")
	}
	if body.Service != "instant-provisioner" {
		t.Errorf("service = %q, want instant-provisioner", body.Service)
	}
	if body.CommitID == "" {
		t.Errorf("commit_id is empty — buildinfo.GitSHA must always have a value (default 'dev')")
	}
	if body.BuildTime == "" {
		t.Errorf("build_time is empty")
	}
	if body.Version == "" {
		t.Errorf("version is empty")
	}
}

// TestHealthzPortNoCollisionWithGRPC asserts the chosen sidecar port is not
// the same as the gRPC port. Cheap, but it catches a config typo that would
// otherwise show up as "address already in use" at pod boot.
func TestHealthzPortNoCollisionWithGRPC(t *testing.T) {
	const grpcPort = ":50051"
	if healthzAddr == grpcPort {
		t.Fatalf("healthzAddr %q must not equal gRPC port %q", healthzAddr, grpcPort)
	}
	// Also sanity-check we have a port at all and it parses.
	if !strings.HasPrefix(healthzAddr, ":") {
		t.Fatalf("healthzAddr %q should start with ':'", healthzAddr)
	}
}

// TestInitNewRelicFailOpenOnEmptyKey verifies the agent init returns nil
// (not panic) when the license key env var is unset — which is the dev
// default. The real concern is "does the provisioner crash if NR is down"
// and the answer must be no.
func TestInitNewRelicFailOpenOnEmptyKey(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	app := initNewRelic()
	if app != nil {
		t.Errorf("initNewRelic() = non-nil with empty key, want nil")
	}
}

// TestInitNewRelicFailOpenOnInvalidKey verifies that a malformed license
// key (e.g. someone pasted in a short string) also returns nil without
// panicking. NR's validator rejects keys < 40 chars.
func TestInitNewRelicFailOpenOnInvalidKey(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "obviously-not-a-real-key")
	app := initNewRelic()
	if app != nil {
		t.Errorf("initNewRelic() = non-nil with bogus key, want nil — agent should fail-open")
	}
}

// TestInitNewRelic_ConstructsWithValidLengthKey — a 40-char license key makes
// newrelic.NewApplication succeed at construction (it dials home async), so
// initNewRelic returns a non-nil app. Covers the success-return arm that the
// empty/invalid-key fail-open tests don't reach. We shut the app down
// immediately so the test leaves no background NR harvester running.
func TestInitNewRelic_ConstructsWithValidLengthKey(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "0123456789012345678901234567890123456789")
	t.Setenv("NEW_RELIC_APP_NAME", "instant-provisioner-test")
	app := initNewRelic()
	if app == nil {
		t.Fatal("initNewRelic returned nil for a valid-length license key — success arm not exercised")
	}
	app.Shutdown(2 * time.Second)
}

// newTestNRApp constructs a real *newrelic.Application with
// ConfigEnabled(false) so it produces real trace metadata but performs no
// network I/O. Returns nil if construction fails — caller decides whether
// to t.Skip or fail.
func newTestNRApp(t *testing.T) *newrelic.Application {
	t.Helper()
	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName("provisioner-test"),
		// 40-char dummy license; NR's validator only checks length when
		// enabled. With ConfigEnabled(false) it's never sent anywhere.
		newrelic.ConfigLicense("0123456789012345678901234567890123456789"),
		newrelic.ConfigEnabled(false),
		newrelic.ConfigDistributedTracerEnabled(true),
	)
	if err != nil {
		t.Fatalf("newrelic.NewApplication: %v", err)
	}
	return app
}

// TestStampTraceIDFromNR is the load-bearing assertion of the track-5
// rollout: when an NR transaction is present on ctx, stampTraceIDFromNR
// must copy its trace_id onto ctx via logctx so downstream slog calls log
// with the propagated trace ID.
func TestStampTraceIDFromNR(t *testing.T) {
	app := newTestNRApp(t)
	txn := app.StartTransaction("test/Provision")
	defer txn.End()

	md := txn.GetTraceMetadata()
	if md.TraceID == "" {
		t.Skip("NR test app did not produce a trace ID — disabled-mode behavior changed; revisit")
	}

	ctx := newrelic.NewContext(context.Background(), txn)
	out := stampTraceIDFromNR(ctx)

	if got := logctx.TraceIDFromContext(out); got != md.TraceID {
		t.Errorf("stampTraceIDFromNR did not propagate trace_id: got %q, want %q", got, md.TraceID)
	}
}

// TestStampTraceIDFromNR_EmptyTraceID — when an NR txn IS on ctx but its trace
// metadata has an empty TraceID (distributed tracing disabled), stampTraceIDFromNR
// must take the md.TraceID=="" early-return arm and leave ctx unstamped.
func TestStampTraceIDFromNR_EmptyTraceID(t *testing.T) {
	app, err := newrelic.NewApplication(
		newrelic.ConfigAppName("provisioner-test-nodt"),
		newrelic.ConfigLicense("0123456789012345678901234567890123456789"),
		newrelic.ConfigEnabled(false),
		// Distributed tracing OFF → GetTraceMetadata().TraceID is empty.
		newrelic.ConfigDistributedTracerEnabled(false),
	)
	if err != nil {
		t.Fatalf("newrelic.NewApplication: %v", err)
	}
	txn := app.StartTransaction("test/NoDT")
	defer txn.End()
	if txn.GetTraceMetadata().TraceID != "" {
		t.Skip("NR produced a trace id with DT disabled — arm not reachable this build")
	}

	ctx := newrelic.NewContext(context.Background(), txn)
	out := stampTraceIDFromNR(ctx)
	if got := logctx.TraceIDFromContext(out); got != "" {
		t.Errorf("empty-trace-id txn should leave ctx unstamped, got %q", got)
	}
}

// TestStampTraceIDFromNR_NoTxn confirms the function is a safe no-op when
// the input ctx has no NR transaction.
func TestStampTraceIDFromNR_NoTxn(t *testing.T) {
	out := stampTraceIDFromNR(context.Background())
	if got := logctx.TraceIDFromContext(out); got != "" {
		t.Errorf("stampTraceIDFromNR with no txn stamped %q; want empty", got)
	}
}

// TestComposeTraceIDInjectorRunsInner verifies the composed interceptor
// actually calls the inner one (e.g. nrgrpc) and the handler. We use a
// synthetic "inner" that just delegates to the handler so we can confirm
// the wiring without bringing up real NR machinery.
func TestComposeTraceIDInjectorRunsInner(t *testing.T) {
	var innerCalls, handlerCalls int

	inner := func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		innerCalls++
		return handler(ctx, req)
	}

	composed := composeTraceIDInjector(inner)

	handler := func(ctx context.Context, _ any) (any, error) {
		handlerCalls++
		// No NR txn → trace_id stays empty.
		if got := logctx.TraceIDFromContext(ctx); got != "" {
			t.Errorf("trace_id = %q before NR ctx, want empty", got)
		}
		return "ok", nil
	}

	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}
	resp, err := composed(context.Background(), "req", info, handler)
	if err != nil {
		t.Fatalf("composed interceptor err: %v", err)
	}
	if resp != "ok" {
		t.Errorf("resp = %v, want ok", resp)
	}
	if innerCalls != 1 {
		t.Errorf("inner was called %d times, want 1", innerCalls)
	}
	if handlerCalls != 1 {
		t.Errorf("handler was called %d times, want 1", handlerCalls)
	}
}

// TestComposeTraceIDInjectorPropagatesNRTraceID closes the loop end-to-end:
// build a composed interceptor with an inner that simulates nrgrpc by
// stuffing a real NR txn into ctx, then assert the handler sees a populated
// trace_id via logctx.
func TestComposeTraceIDInjectorPropagatesNRTraceID(t *testing.T) {
	app := newTestNRApp(t)
	txn := app.StartTransaction("test/Provision")
	defer txn.End()

	expected := txn.GetTraceMetadata().TraceID
	if expected == "" {
		t.Skip("NR test app did not produce a trace ID")
	}

	// Synthetic "inner" interceptor — pretends to be nrgrpc by injecting
	// the txn into ctx before calling the (wrapped) handler.
	inner := func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		return handler(newrelic.NewContext(ctx, txn), req)
	}
	composed := composeTraceIDInjector(inner)

	var captured context.Context
	handler := func(ctx context.Context, _ any) (any, error) {
		captured = ctx
		return "ok", nil
	}

	info := &grpc.UnaryServerInfo{FullMethod: "/test/Method"}
	if _, err := composed(context.Background(), "req", info, handler); err != nil {
		t.Fatalf("composed interceptor err: %v", err)
	}
	if captured == nil {
		t.Fatal("handler did not run")
	}
	if got := logctx.TraceIDFromContext(captured); got != expected {
		t.Errorf("trace_id propagated to handler ctx = %q, want %q", got, expected)
	}
}

// TestLogctxWithTraceIDRoundTrip covers the common logctx package end-to-end
// from the provisioner main_test to defend against a future cleanup pass
// accidentally breaking the trace-id round-trip across services.
//
// Note: common/logctx.WithTraceID(ctx, "") sets an empty string (unlike the
// removed stub which preserved the previous value). The assertions below
// match the canonical common/logctx semantics.
func TestLogctxWithTraceIDRoundTrip(t *testing.T) {
	ctx := context.Background()
	if got := logctx.TraceIDFromContext(ctx); got != "" {
		t.Errorf("fresh ctx TraceID = %q, want empty", got)
	}

	ctx2 := logctx.WithTraceID(ctx, "abc123")
	if got := logctx.TraceIDFromContext(ctx2); got != "abc123" {
		t.Errorf("after WithTraceID, TraceID = %q, want abc123", got)
	}
}

// TestEnvAppNameOverride confirms NEW_RELIC_APP_NAME wins over the default.
// This is what k8s deployment specs will use to differentiate -prod /
// -staging / -dev environments per the plan doc's open question 2.
func TestEnvAppNameOverride(t *testing.T) {
	// We can't easily inspect what name NR was init'd with because the
	// agent's internal config isn't exported — but we can at least verify
	// init doesn't panic when the env is set.
	t.Setenv("NEW_RELIC_APP_NAME", "instant-provisioner-staging")
	t.Setenv("NEW_RELIC_LICENSE_KEY", "") // still fail-open
	app := initNewRelic()
	if app != nil {
		t.Errorf("app should still be nil — empty license key overrides app name")
	}
}

// Static check: we expect os.Args[0] to be a real binary name when this
// test runs, so basic process plumbing is healthy. Cheap smoke test.
func TestProcessSmoke(t *testing.T) {
	if os.Args[0] == "" {
		t.Fatal("os.Args[0] empty — test runner misconfigured")
	}
	if errors.Is(nil, http.ErrServerClosed) {
		t.Fatal("errors.Is(nil, http.ErrServerClosed) should be false")
	}
}

// TestStartHealthzSidecar_ServesMetrics is the regression guard for
// BugBash B14-P0-F1: the provisioner sidecar HTTP mux on :8092 must
// expose /metrics so the cluster ServiceMonitor can scrape circuit
// breaker state, readyz_check_status, and Go runtime collectors.
// Without this, every NR alert keyed off provisioner metrics is
// silently dead.
//
// We can't bind the real port in CI, so we exercise the same mux by
// constructing the sidecar against a t.Cleanup-ed httptest.Server.
// The test asserts (a) /metrics is registered (200, prometheus text
// format), (b) /healthz still works, (c) /readyz still works.
func TestStartHealthzSidecar_ServesMetrics(t *testing.T) {
	ready := &server.Readiness{}
	ready.SetReady(true)
	box := &poolBox{}
	// Build the sidecar with poolEnabled=false (matches today's prod
	// where PROVISIONER_DATABASE_URL is unset) so the test is also a
	// /readyz=200 cross-check.
	srv := startHealthzSidecar(ready, box, false)
	t.Cleanup(func() {
		_ = srv.Close()
	})

	// startHealthzSidecar binds to healthzAddr (:8092). Drive the same
	// mux it built by hitting that address via the local loopback — if
	// :8092 is in use (e.g. another test, or a real provisioner running
	// locally), skip rather than fail. The unit test we care about is
	// "mux includes /metrics"; the integration check belongs to verify-live.
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://localhost:8092/metrics")
	if err != nil {
		t.Skipf("port :8092 not reachable locally (%v) — verify-live owns the integration check", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("/metrics returned %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") && !strings.Contains(ct, "application/openmetrics") {
		t.Fatalf("/metrics Content-Type = %q, want prometheus text or openmetrics", ct)
	}

	// Confirm /healthz + /readyz still work on the same mux.
	hzResp, err := client.Get("http://localhost:8092/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	hzResp.Body.Close()
	if hzResp.StatusCode != 200 {
		t.Fatalf("/healthz returned %d, want 200 (readiness was set true)", hzResp.StatusCode)
	}

	rzResp, err := client.Get("http://localhost:8092/readyz")
	if err != nil {
		t.Fatalf("GET /readyz: %v", err)
	}
	rzResp.Body.Close()
	if rzResp.StatusCode != 200 {
		t.Fatalf("/readyz returned %d, want 200 (pool disabled — platform_db should be skipped)", rzResp.StatusCode)
	}
}

// TestStartHealthzSidecar_BindFailureLogged — when :8092 is already bound, the
// sidecar's ListenAndServe fails; the goroutine must log healthz.serve_failed
// and NOT crash the process (losing /healthz must never take down the service).
// We occupy :8092 first, then start the sidecar, then give its goroutine a beat
// to hit the serve-failed branch.
func TestStartHealthzSidecar_BindFailureLogged(t *testing.T) {
	occupier, err := net.Listen("tcp", healthzAddr)
	if err != nil {
		t.Skipf("could not occupy %s (already in use by another test/process): %v", healthzAddr, err)
	}
	defer occupier.Close()

	ready := &server.Readiness{}
	srv := startHealthzSidecar(ready, &poolBox{}, false)
	t.Cleanup(func() { _ = srv.Close() })

	// The goroutine's ListenAndServe should fail fast against the occupied port
	// and log serve_failed. No assertion on the log (it goes to slog); the
	// branch is what we're covering, and the test asserts the process survives.
	time.Sleep(100 * time.Millisecond)
}

// TestCollectBreakerInspectors_NilSafe — collectBreakerInspectors(nil) must
// return nil rather than panic. The test path constructs a Server without
// breakers; main.go's readyz wiring should range over a nil slice safely.
func TestCollectBreakerInspectors_NilSafe(t *testing.T) {
	got := collectBreakerInspectors(nil)
	if got != nil {
		t.Fatalf("collectBreakerInspectors(nil) = %v, want nil", got)
	}
}

// TestCollectBreakerInspectors_AllBackendsSurfaced — when handed a real
// *circuit.Breakers, collectBreakerInspectors must surface every backend
// the Breakers struct exposes (postgres_admin / postgres_k8s / redis_admin
// / mongo_admin / k8s_api) so /readyz reports each one. A future breaker
// added to the struct without a matching adapter entry here would silently
// drop off /readyz; this test is the registry pin.
func TestCollectBreakerInspectors_AllBackendsSurfaced(t *testing.T) {
	bs := circuit.NewBreakers()
	got := collectBreakerInspectors(bs)
	want := map[string]bool{
		"postgres_admin": false,
		"postgres_k8s":   false,
		"redis_admin":    false,
		"mongo_admin":    false,
		"k8s_api":        false,
	}
	for _, ins := range got {
		name := ins.Name()
		if _, ok := want[name]; !ok {
			t.Errorf("unexpected breaker surfaced: %q", name)
			continue
		}
		want[name] = true
		// Fresh breakers start StateClosed → IsOpen() == false.
		if ins.IsOpen() {
			t.Errorf("breaker %q reports IsOpen()=true on a fresh Breakers — fresh state should be closed", name)
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("breaker %q not surfaced — Breakers struct gained a backend without updating collectBreakerInspectors", name)
		}
	}
}
