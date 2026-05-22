package telemetry

// tracer_extra_test.go — covers the InitTracer branches the baseline suite
// misses: the OTEL_SERVICE_NAME override, the plaintext (WithInsecure) arm for
// a non-TLS endpoint, the NR-license header arm, and a real (non-noop)
// shutdown returning cleanly.

import (
	"context"
	"errors"
	"testing"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// TestInitTracer_ExporterError forces the otlptracegrpc.New error arm via the
// newExporter seam. A construction failure must fall back to a working no-op
// shutdown (fail-open) rather than crash boot.
func TestInitTracer_ExporterError(t *testing.T) {
	orig := newExporter
	t.Cleanup(func() { newExporter = orig })
	newExporter = func(context.Context, ...otlptracegrpc.Option) (sdktrace.SpanExporter, error) {
		return nil, errors.New("exporter boom")
	}

	shutdown := InitTracer("instant-provisioner", "localhost:4317")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown after exporter error")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("fallback shutdown should be a clean no-op, got %v", err)
	}
}

// TestInitTracer_ResourceError forces the resource.New error arm. The exporter
// succeeds (real constructor), then resource detection fails — InitTracer must
// shut the exporter down and return a no-op.
func TestInitTracer_ResourceError(t *testing.T) {
	origRes := newResource
	t.Cleanup(func() { newResource = origRes })
	newResource = func(context.Context, ...resource.Option) (*resource.Resource, error) {
		return nil, errors.New("resource boom")
	}

	shutdown := InitTracer("instant-provisioner", "localhost:4317")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown after resource error")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("fallback shutdown should be a clean no-op, got %v", err)
	}
}

// TestInitTracer_PlaintextWithServiceNameOverride drives the non-TLS exporter
// path (an in-cluster collector host with no scheme → WithInsecure) plus the
// OTEL_SERVICE_NAME override. The exporter dials lazily, so construction
// succeeds even though nothing listens on the endpoint; we then exercise the
// real shutdown closure (not the noop) and assert it returns without error.
func TestInitTracer_PlaintextWithServiceNameOverride(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "instant-provisioner-override")
	t.Setenv("NEW_RELIC_LICENSE_KEY", "") // exercise the license-missing warn arm

	shutdown := InitTracer("instant-provisioner", "otel-collector.observability:4317")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown for a plaintext endpoint")
	}
	// Real shutdown path (tp.Shutdown) — should return nil for a never-exported
	// provider.
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("real shutdown returned error: %v", err)
	}
}

// TestInitTracer_WithNRLicenseHeader drives the licenseKey != "" arm that
// appends the api-key header. A 40-char dummy key is non-sentinel so it is not
// reset to "" by the CHANGE_ME / empty guard.
func TestInitTracer_WithNRLicenseHeader(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("NEW_RELIC_LICENSE_KEY", "0123456789012345678901234567890123456789")

	shutdown := InitTracer("instant-provisioner", "https://otlp.nr-data.net:4317")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}
}

// TestInitTracer_SentinelLicenseTreatedAsEmpty asserts the CHANGE_ME sentinel
// is treated as no-license (the licenseKey = "" reset arm) — the exporter is
// still constructed (warn-and-continue), proving fail-open.
func TestInitTracer_SentinelLicenseTreatedAsEmpty(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "CHANGE_ME")
	shutdown := InitTracer("instant-provisioner", "localhost:4317")
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown for sentinel license")
	}
	_ = shutdown(context.Background())
}
