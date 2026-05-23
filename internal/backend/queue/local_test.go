package queue

// local_test.go — unit tests for LocalBackend.
//
// LocalBackend provisions NATS credentials on the shared cluster. NATS itself
// runs without authentication on the shared backend, so per-user state is
// never created on the server side. The interesting behaviour is:
//
//   1. Constructor defaults: empty natsHost falls back to "localhost"; the
//      monitor port defaults to 8222 (the NATS HTTP monitor port).
//   2. Provision health-check: it hits http://<host>:<monitorPort>/healthz and:
//        - returns Credentials on 200,
//        - returns an error on any non-2xx,
//        - returns an error if the dial itself fails (host unreachable),
//        - returns an error if NewRequestWithContext fails (malformed host).
//   3. The returned SubjectPrefix is the canonical full-token derivation, so
//      two tokens sharing an 8-hex-char prefix never share a subject namespace
//      (the truncation-bug class fixed in subjident.go).
//   4. The returned URL is "nats://<host>:4222".
//   5. Deprovision is a no-op — no per-user state, never errors.
//
// We use httptest.Server bound to a random port and the test-only
// `monitorPort` knob on LocalBackend so the tests never collide with the
// docker daemon, a real NATS, or another developer's pod that happens to be
// bound to :8222.

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newHealthTestServer starts an httptest.Server on a random loopback port that
// answers GET /healthz with the configured status. Returns the host:port pair
// and ensures cleanup at test end. The httptest server already binds to
// 127.0.0.1:<random> so collision with other listeners is impossible.
func newHealthTestServer(t *testing.T, status int) (host string, port int) {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// srv.URL is e.g. http://127.0.0.1:54321 — split out host and port.
	u := strings.TrimPrefix(srv.URL, "http://")
	hostStr, portStr, err := net.SplitHostPort(u)
	if err != nil {
		t.Fatalf("split httptest URL %q: %v", srv.URL, err)
	}
	var p int
	if _, err := fmt.Sscanf(portStr, "%d", &p); err != nil {
		t.Fatalf("parse port %q: %v", portStr, err)
	}
	return hostStr, p
}

// TestNewLocalBackend_Defaults asserts the constructor defaults: empty
// natsHost is replaced by "localhost", monitorPort defaults to 8222, and
// the http client is initialised.
func TestNewLocalBackend_Defaults(t *testing.T) {
	b := newLocalBackend("")
	if b.natsHost != "localhost" {
		t.Errorf("natsHost = %q; want %q", b.natsHost, "localhost")
	}
	if b.monitorPort != 8222 {
		t.Errorf("monitorPort = %d; want 8222 (NATS HTTP monitor default)", b.monitorPort)
	}
	if b.httpClient == nil {
		t.Error("httpClient must be initialised")
	}
}

// TestNewLocalBackend_NonEmptyHostPreserved asserts an explicit natsHost is
// preserved verbatim (no normalisation, no port munging).
func TestNewLocalBackend_NonEmptyHostPreserved(t *testing.T) {
	b := newLocalBackend("nats.example.com")
	if b.natsHost != "nats.example.com" {
		t.Errorf("natsHost = %q; want preserved", b.natsHost)
	}
}

// TestLocalBackend_Provision_Happy spins up an httptest server, points the
// LocalBackend's monitor port at it, and asserts that Provision:
//   - returns a non-nil Credentials struct,
//   - URL is "nats://<natsHost>:4222",
//   - SubjectPrefix is the canonical full-token derivation,
//   - ProviderResourceID is empty (shared backend has no per-resource state).
func TestLocalBackend_Provision_Happy(t *testing.T) {
	host, port := newHealthTestServer(t, http.StatusOK)
	b := newLocalBackend(host)
	b.monitorPort = port

	const token = "abc12345deadbeefcafef00d00112233"
	creds, err := b.Provision(context.Background(), token, "anonymous")
	if err != nil {
		t.Fatalf("Provision returned error: %v", err)
	}
	if creds == nil {
		t.Fatal("Provision returned nil Credentials")
	}
	wantURL := fmt.Sprintf("nats://%s:4222", host)
	if creds.URL != wantURL {
		t.Errorf("URL = %q; want %q", creds.URL, wantURL)
	}
	wantPrefix := canonicalSubjectPrefix(token)
	if creds.SubjectPrefix != wantPrefix {
		t.Errorf("SubjectPrefix = %q; want %q", creds.SubjectPrefix, wantPrefix)
	}
	if creds.ProviderResourceID != "" {
		t.Errorf("ProviderResourceID = %q; want empty (shared backend)", creds.ProviderResourceID)
	}
}

// TestLocalBackend_Provision_UnhealthyStatus asserts that any non-2xx from the
// NATS monitor surfaces as a clear "NATS unhealthy" error — silent success on
// a 503 would let a broken NATS look healthy to the customer.
func TestLocalBackend_Provision_UnhealthyStatus(t *testing.T) {
	host, port := newHealthTestServer(t, http.StatusServiceUnavailable)
	b := newLocalBackend(host)
	b.monitorPort = port

	_, err := b.Provision(context.Background(), "tok", "anonymous")
	if err == nil {
		t.Fatal("Provision must return an error when NATS returns non-2xx")
	}
	if !strings.Contains(err.Error(), "unhealthy") {
		t.Errorf("error must mention 'unhealthy' for non-2xx response; got: %v", err)
	}
}

// TestLocalBackend_Provision_UnreachableHost asserts that a dial failure
// (host doesn't resolve) surfaces as a health-check error, NOT a silent
// success or a panic. We use a hostname under the RFC 2606 `.invalid` TLD
// so DNS resolution is guaranteed to fail on every runner.
func TestLocalBackend_Provision_UnreachableHost(t *testing.T) {
	b := newLocalBackend("nonexistent-host-for-queue-test.invalid")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, perr := b.Provision(ctx, "tok", "anonymous")
	if perr == nil {
		t.Fatal("Provision must return an error when NATS monitor is unreachable")
	}
	if !strings.Contains(perr.Error(), "health check failed") {
		t.Errorf("error must mention 'health check failed' for dial failure; got: %v", perr)
	}
}

// TestLocalBackend_Provision_BuildRequestError asserts that a malformed
// natsHost surfaces as a "build health request" error from
// http.NewRequestWithContext — never silently swallowed. A control character
// makes the URL invalid.
func TestLocalBackend_Provision_BuildRequestError(t *testing.T) {
	b := newLocalBackend("bad\x7fhost")
	_, err := b.Provision(context.Background(), "tok", "anonymous")
	if err == nil {
		t.Fatal("Provision must surface an error for a malformed natsHost")
	}
	if !strings.Contains(err.Error(), "build health request") {
		t.Errorf("error must mention 'build health request'; got: %v", err)
	}
}

// TestLocalBackend_Deprovision_NoOp asserts the shared backend's Deprovision
// is a no-op: no error, no side effect, regardless of token or PRID values
// (including empty strings).
func TestLocalBackend_Deprovision_NoOp(t *testing.T) {
	b := newLocalBackend("nats.example.com")
	if err := b.Deprovision(context.Background(), "any-token", "any-prid"); err != nil {
		t.Errorf("Deprovision must be a no-op; got error: %v", err)
	}
	if err := b.Deprovision(context.Background(), "", ""); err != nil {
		t.Errorf("Deprovision must accept empty token/PRID; got error: %v", err)
	}
}

// TestLocalBackend_Provision_SubjectPrefix_NoCollision is the cross-cutting
// regression guard: provisioning two tokens that share an 8-hex-char prefix
// must yield distinct SubjectPrefix values (the truncation-bug class fixed
// in subjident.go). On the shared NATS backend the SubjectPrefix is the
// ONLY tenant isolation boundary, so a collision would let one tenant
// publish/subscribe inside another's subject namespace.
func TestLocalBackend_Provision_SubjectPrefix_NoCollision(t *testing.T) {
	host, port := newHealthTestServer(t, http.StatusOK)
	b := newLocalBackend(host)
	b.monitorPort = port

	tokA := "abc12345deadbeefcafef00d00112233"
	tokB := "abc12345111122223333444455556666"
	credsA, err := b.Provision(context.Background(), tokA, "anonymous")
	if err != nil {
		t.Fatalf("Provision(tokA) error: %v", err)
	}
	credsB, err := b.Provision(context.Background(), tokB, "anonymous")
	if err != nil {
		t.Fatalf("Provision(tokB) error: %v", err)
	}
	if credsA.SubjectPrefix == credsB.SubjectPrefix {
		t.Errorf("SubjectPrefix collision for tokens sharing 8-char prefix: both = %q", credsA.SubjectPrefix)
	}
}
