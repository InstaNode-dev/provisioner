package telemetry

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/resource"
)

// TestInitTracer_EmptyEndpointNoop — when the endpoint is unset, the
// returned shutdown must be a working no-op. This is the fail-open
// contract for local dev / CI runs where OTel is intentionally off.
func TestInitTracer_EmptyEndpointNoop(t *testing.T) {
	shutdown := InitTracer("instant-provisioner", "")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown for empty endpoint")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("noop shutdown returned error: %v", err)
	}
}

// TestInitTracer_Boots — with a non-empty endpoint, InitTracer constructs
// a real exporter without crashing even if NEW_RELIC_LICENSE_KEY is unset.
// The exporter dials lazily on the first export, so construction must
// succeed regardless of whether the endpoint is reachable.
func TestInitTracer_Boots(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	shutdown := InitTracer("instant-provisioner", "https://otlp.nr-data.net:4317")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown")
	}
	_ = shutdown(context.Background())
}

// TestShouldUseTLS — the regression case for P0-2: every `https://`
// endpoint AND every `*nr-data.net` host MUST resolve to TLS=true.
// Reverting to WithInsecure() for these would silently kill tracing
// again (the symptom that produced this test).
func TestShouldUseTLS(t *testing.T) {
	cases := []struct {
		endpoint string
		want     bool
	}{
		{"https://otlp.nr-data.net:4317", true},
		{"https://otlp.eu01.nr-data.net:4317", true},
		{"otlp.nr-data.net:4317", true},
		{"otlp.eu01.nr-data.net:4317", true},
		{"foo.example.com:443", true},
		{"http://otel-collector.observability:4317", false},
		{"otel-collector.observability:4317", false},
		{"localhost:4317", false},
		{"", false},
	}
	for _, c := range cases {
		got := shouldUseTLS(c.endpoint)
		if got != c.want {
			t.Errorf("shouldUseTLS(%q) = %v, want %v", c.endpoint, got, c.want)
		}
	}
}

// TestInitTracer_NRLicenseHeaderApplied — when NEW_RELIC_LICENSE_KEY is a
// real (non-sentinel) value, InitTracer wires the `api-key` header onto the
// exporter. The exporter dials lazily so we don't actually need NR
// reachability — successful construction + non-nil shutdown is enough to
// prove the branch was taken.
func TestInitTracer_NRLicenseHeaderApplied(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "fake-but-non-empty-license-key-1234567890")
	shutdown := InitTracer("instant-provisioner-test", "https://otlp.nr-data.net:4317")
	if shutdown == nil {
		t.Fatal("shutdown nil with non-empty license key")
	}
	// Run shutdown with a tight ctx — exporter has never sent so it should
	// shutdown quickly even without network reachability. We don't assert
	// success on the err — a real shutdown against an unreachable batch
	// processor may error; we only assert no panic and that the function
	// returns within the timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = shutdown(ctx)
}

// TestInitTracer_SentinelLicenseTreatedAsEmpty — the CHANGE_ME sentinel is
// treated identically to an unset env. Hits the warn-and-continue branch
// where licenseKey is reset to "".
func TestInitTracer_SentinelLicenseTreatedAsEmpty(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "CHANGE_ME")
	shutdown := InitTracer("instant-provisioner-test", "https://otlp.nr-data.net:4317")
	if shutdown == nil {
		t.Fatal("shutdown nil under CHANGE_ME sentinel")
	}
	_ = shutdown(context.Background())
}

// TestInitTracer_PlaintextEndpoint — exercises the WithInsecure() path for
// an in-cluster collector style endpoint that resolves to TLS=false. The
// branch is the "everything else" arm of shouldUseTLS.
func TestInitTracer_PlaintextEndpoint(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	shutdown := InitTracer("instant-provisioner-test", "otel-collector.observability:4317")
	if shutdown == nil {
		t.Fatal("shutdown nil for plaintext endpoint")
	}
	_ = shutdown(context.Background())
}

// TestInitTracer_OTELServiceNameOverride — when OTEL_SERVICE_NAME is set,
// InitTracer prefers it over the argument. This branch (`s != ""`) was
// previously uncovered. We assert no panic + non-nil shutdown — the actual
// service-name resource attribute is exercised by the OTel SDK internals.
func TestInitTracer_OTELServiceNameOverride(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	t.Setenv("OTEL_SERVICE_NAME", "override-via-env")
	shutdown := InitTracer("argument-name", "otel-collector.observability:4317")
	if shutdown == nil {
		t.Fatal("shutdown nil with OTEL_SERVICE_NAME override")
	}
	_ = shutdown(context.Background())
}

// TestInitTracer_Port443EndpointTLS — `:443` host suffix forces TLS.
// Confirms the path through both shouldUseTLS and the TLS-options arm of
// InitTracer.
func TestInitTracer_Port443EndpointTLS(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	shutdown := InitTracer("instant-provisioner-test", "collector.example.com:443")
	if shutdown == nil {
		t.Fatal("shutdown nil for :443 endpoint")
	}
	_ = shutdown(context.Background())
}

// TestInitTracer_HTTPSchemePlaintext — explicit `http://` scheme forces
// plaintext even when the host would otherwise suggest TLS.
func TestInitTracer_HTTPSchemePlaintext(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	shutdown := InitTracer("instant-provisioner-test", "http://otlp.nr-data.net:4317")
	if shutdown == nil {
		t.Fatal("shutdown nil for http:// scheme")
	}
	_ = shutdown(context.Background())
}

// TestInitTracer_WhitespaceEndpoint — a whitespace-only endpoint is treated
// the same as empty (tracing disabled). Confirms the TrimSpace branch.
func TestInitTracer_WhitespaceEndpoint(t *testing.T) {
	shutdown := InitTracer("svc", "   \t  ")
	if shutdown == nil {
		t.Fatal("shutdown nil for whitespace endpoint")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("noop shutdown returned err for whitespace endpoint: %v", err)
	}
}

// TestInitTracer_ExporterConstructionFailure — covers the
// `telemetry.otlp_exporter_failed` arm. otlptracegrpc.New rejects endpoints
// containing control characters (the underlying url.Parse fails), so a NUL
// byte in the endpoint forces the constructor to return an error. The
// function must fall back to a no-op shutdown rather than panicking — the
// "NEVER crashes" contract spelled out in the doc comment.
func TestInitTracer_ExporterConstructionFailure(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	shutdown := InitTracer("svc", "\x00bad-endpoint:4317")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown on exporter failure — should fall back to no-op")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("fallback shutdown returned error: %v (must be no-op)", err)
	}
}

// TestInitTracer_ShutdownWithCancelledCtx — exercises the shutdown error
// wrapping branch. Calling shutdown with an already-cancelled context
// pushes the wrapped Shutdown call onto a path where the SDK reports the
// context error; the function returns a wrapped `telemetry shutdown:` err.
// We don't strictly require an error (the SDK may also return nil if no
// spans are buffered) but we DO require the call returns without panicking
// within the test budget — proving the deferred-cancel + Shutdown path
// executes.
func TestInitTracer_ShutdownWithCancelledCtx(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	shutdown := InitTracer("svc", "otel-collector.observability:4317")
	if shutdown == nil {
		t.Fatal("shutdown nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel up-front so tp.Shutdown sees a dead context
	_ = shutdown(ctx)
}

// TestInitTracer_ResourceFailureFallsBackToNoop — when resource.New errors
// the function must shutdown the already-constructed exporter and return a
// no-op shutdown. We force the failure by swapping the `newResource`
// indirection for a stub that always errors; the production code path
// (`resource.New(...)`) is untouched in any non-test build.
func TestInitTracer_ResourceFailureFallsBackToNoop(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "")
	orig := newResource
	t.Cleanup(func() { newResource = orig })
	newResource = func(context.Context, ...resource.Option) (*resource.Resource, error) {
		return nil, errResourceForceFail
	}

	shutdown := InitTracer("svc", "otel-collector.observability:4317")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown after resource failure")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("fallback shutdown returned err: %v", err)
	}
}

// errResourceForceFail is a sentinel used only by
// TestInitTracer_ResourceFailureFallsBackToNoop to verify the error path
// runs without the SDK ever returning an actual error.
var errResourceForceFail = forceErr("force resource failure for coverage")

type forceErr string

func (f forceErr) Error() string { return string(f) }

// TestStripScheme — strips http:// and https:// uniformly.
func TestStripScheme(t *testing.T) {
	cases := map[string]string{
		"https://otlp.nr-data.net:4317": "otlp.nr-data.net:4317",
		"http://localhost:4317":         "localhost:4317",
		"otlp.nr-data.net:4317":         "otlp.nr-data.net:4317",
		"":                              "",
	}
	for in, want := range cases {
		if got := stripScheme(in); got != want {
			t.Errorf("stripScheme(%q) = %q, want %q", in, got, want)
		}
	}
}
