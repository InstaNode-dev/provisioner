package queue

// backend_test.go — unit tests for the public Backend factory `NewBackend` and
// its tiny aliases `goredisParseURL` / `goredisNewClient`.
//
// `NewBackend` selects between K8sBackend (when backendType == "k8s") and
// LocalBackend (default). The k8s path reads multiple env vars to wire up the
// kubeconfig, storage class, NATS image, external host, public host, and a
// Redis-backed route registry. The factory must:
//
//   1. Fall back to the local backend on any k8s init failure (kubeconfig
//      missing, in-cluster config not available, etc.) — never panic.
//   2. Return a LocalBackend for the default branch ("", "local", any other
//      string).
//   3. Drive the Redis-backed route registry only when REDIS_URL_FOR_ROUTES /
//      REDIS_URL is set AND parses cleanly.
//   4. Skip route registry wiring when the URL is bad (don't crash, just warn).
//
// The k8s success branch is exercised indirectly by k8s_test.go (Provision
// against a fake clientset); here we only care about backend selection and
// the env-var fallback ladder.

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// writeTestKubeconfig writes a minimal kubeconfig that
// clientcmd.BuildConfigFromFlags will parse cleanly without dialing
// anything. The k8s clientset is constructed but never used in this test
// (NewBackend doesn't call any API on it); we only need it to populate the
// K8sBackend struct so we can exercise the route-registry-wiring branch.
func writeTestKubeconfig(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "kubeconfig")
	content := `apiVersion: v1
kind: Config
clusters:
- name: fake-cluster
  cluster:
    server: https://127.0.0.1:1
contexts:
- name: fake-context
  context:
    cluster: fake-cluster
    user: fake-user
current-context: fake-context
users:
- name: fake-user
  user:
    token: fake-token
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return path
}

// TestNewBackend_DefaultBranchReturnsLocal verifies that any non-"k8s"
// backendType falls into the LocalBackend branch.
func TestNewBackend_DefaultBranchReturnsLocal(t *testing.T) {
	for _, bt := range []string{"", "local", "garbage", "K8S" /* case-sensitive switch */} {
		t.Run(bt, func(t *testing.T) {
			got := NewBackend(bt, "nats.example.com")
			lb, ok := got.(*LocalBackend)
			if !ok {
				t.Fatalf("NewBackend(%q) = %T; want *LocalBackend", bt, got)
			}
			if lb.natsHost != "nats.example.com" {
				t.Errorf("natsHost = %q; want %q", lb.natsHost, "nats.example.com")
			}
		})
	}
}

// TestNewBackend_K8sFallsBackToLocalOnInitFailure verifies the safety net: if
// k8s client init fails (no kubeconfig, no in-cluster config), the factory
// returns a LocalBackend instead of crashing. We force the failure by setting
// K8S_KUBECONFIG to a path that does not exist.
func TestNewBackend_K8sFallsBackToLocalOnInitFailure(t *testing.T) {
	t.Setenv("K8S_KUBECONFIG", "/nonexistent/kubeconfig-for-unit-test")
	// Clear REDIS_URL so the route-registry wiring is a no-op even if the k8s
	// path had succeeded. Belt + suspenders.
	t.Setenv("REDIS_URL", "")
	t.Setenv("REDIS_URL_FOR_ROUTES", "")
	got := NewBackend("k8s", "nats.example.com")
	if _, ok := got.(*LocalBackend); !ok {
		t.Fatalf("NewBackend(k8s) with bad kubeconfig = %T; want fallback *LocalBackend", got)
	}
}

// TestGoredisHelpers verifies the tiny aliases compile and round-trip a sane
// URL. A bad URL must surface an error.
func TestGoredisHelpers(t *testing.T) {
	opt, err := goredisParseURL("redis://localhost:6379/0")
	if err != nil {
		t.Fatalf("goredisParseURL(valid) error: %v", err)
	}
	if opt == nil {
		t.Fatal("goredisParseURL(valid) returned nil *Options")
	}
	c := goredisNewClient(opt)
	if c == nil {
		t.Fatal("goredisNewClient returned nil")
	}
	_ = c.Close()

	if _, err := goredisParseURL("not-a-valid-redis-url-scheme"); err == nil {
		t.Error("goredisParseURL must return an error for a malformed URL")
	}
}

// TestK8sEnv_FallbackLadder verifies the env-var helper used throughout the
// k8s constructor: set value wins; empty falls back to default. The factory
// reads K8S_NATS_PUBLIC_HOST, K8S_STORAGE_CLASS, K8S_NATS_IMAGE,
// K8S_EXTERNAL_HOST, NATS_PROXY_ROUTE_PREFIX, NATS_PROXY_TOKEN_ROUTE_PREFIX —
// all via this helper.
func TestK8sEnv_FallbackLadder(t *testing.T) {
	const key = "QUEUE_TEST_K8S_ENV_FALLBACK_KEY"
	_ = os.Unsetenv(key)
	if got := k8sEnv(key, "default"); got != "default" {
		t.Errorf("k8sEnv(unset, default) = %q; want %q", got, "default")
	}
	t.Setenv(key, "set-value")
	if got := k8sEnv(key, "default"); got != "set-value" {
		t.Errorf("k8sEnv(set, default) = %q; want %q", got, "set-value")
	}
	// Empty env value is treated as unset — falls back to default. This is
	// the explicit contract: `if v := os.Getenv(key); v != ""`.
	t.Setenv(key, "")
	if got := k8sEnv(key, "default"); got != "default" {
		t.Errorf("k8sEnv(empty, default) = %q; want default fallback (\"\" treated as unset)", got)
	}
}

// TestNatsK8sBoolPtr_RoundTrip — the bool pointer helper must round-trip both
// true and false without aliasing (each call returns a fresh *bool).
func TestNatsK8sBoolPtr_RoundTrip(t *testing.T) {
	tp := natsK8sBoolPtr(true)
	fp := natsK8sBoolPtr(false)
	if tp == nil || *tp != true {
		t.Errorf("natsK8sBoolPtr(true) = %v; want &true", tp)
	}
	if fp == nil || *fp != false {
		t.Errorf("natsK8sBoolPtr(false) = %v; want &false", fp)
	}
	if tp == fp {
		t.Error("natsK8sBoolPtr must allocate a fresh pointer each call (no aliasing)")
	}
}

// TestNewBackend_K8sSucceeds_WithRouteRegistry exercises the k8s success
// branch in NewBackend: kubeconfig parses, K8sBackend is constructed,
// SetPublicHost is called, and the Redis-backed route registry is wired
// (because REDIS_URL_FOR_ROUTES is set to a syntactically valid URL — the
// underlying Redis is never dialled at construction time).
func TestNewBackend_K8sSucceeds_WithRouteRegistry(t *testing.T) {
	kcfg := writeTestKubeconfig(t)
	t.Setenv("K8S_KUBECONFIG", kcfg)
	t.Setenv("K8S_NATS_PUBLIC_HOST", "nats.test.invalid")
	t.Setenv("K8S_STORAGE_CLASS", "test-sc")
	t.Setenv("K8S_NATS_IMAGE", "nats:test")
	t.Setenv("K8S_EXTERNAL_HOST", "node.test.invalid")
	t.Setenv("REDIS_URL_FOR_ROUTES", "redis://127.0.0.1:1/0")
	t.Setenv("NATS_PROXY_ROUTE_PREFIX", "test_route:")
	t.Setenv("NATS_PROXY_TOKEN_ROUTE_PREFIX", "test_route_by_token:")

	got := NewBackend("k8s", "ignored.host")
	kb, ok := got.(*K8sBackend)
	if !ok {
		t.Fatalf("NewBackend(k8s) = %T; want *K8sBackend", got)
	}
	if kb.publicHost != "nats.test.invalid" {
		t.Errorf("publicHost = %q; want nats.test.invalid", kb.publicHost)
	}
	if kb.storageClass != "test-sc" {
		t.Errorf("storageClass = %q; want test-sc", kb.storageClass)
	}
	if kb.image != "nats:test" {
		t.Errorf("image = %q; want nats:test", kb.image)
	}
	if kb.externalHost != "node.test.invalid" {
		t.Errorf("externalHost = %q; want node.test.invalid", kb.externalHost)
	}
	if kb.rdb == nil {
		t.Error("rdb must be wired when REDIS_URL_FOR_ROUTES parses cleanly")
	}
	if kb.tokenPrefix != "test_route:" {
		t.Errorf("tokenPrefix = %q; want test_route:", kb.tokenPrefix)
	}
	if kb.routePrefix != "test_route_by_token:" {
		t.Errorf("routePrefix = %q; want test_route_by_token:", kb.routePrefix)
	}
	// Close the embedded redis client so it doesn't leak a goroutine into
	// subsequent tests.
	if kb.rdb != nil {
		_ = kb.rdb.Close()
	}
}

// TestNewBackend_K8sSucceeds_NoRedisURL — when neither REDIS_URL_FOR_ROUTES
// nor REDIS_URL is set, the route registry is left disabled (rdb is nil).
// The k8s backend still works — the proxy just won't have a route record
// for resources provisioned in this mode.
func TestNewBackend_K8sSucceeds_NoRedisURL(t *testing.T) {
	kcfg := writeTestKubeconfig(t)
	t.Setenv("K8S_KUBECONFIG", kcfg)
	t.Setenv("REDIS_URL_FOR_ROUTES", "")
	t.Setenv("REDIS_URL", "")

	got := NewBackend("k8s", "ignored.host")
	kb, ok := got.(*K8sBackend)
	if !ok {
		t.Fatalf("NewBackend(k8s) = %T; want *K8sBackend", got)
	}
	if kb.rdb != nil {
		t.Error("rdb must be nil when no Redis URL is configured")
		_ = kb.rdb.Close()
	}
}

// TestNewBackend_K8sSucceeds_BadRedisURL — when REDIS_URL_FOR_ROUTES is set
// but malformed, the route registry is silently disabled (slog.Warn only).
// This is the documented behaviour: a bad Redis URL must not crash the boot
// of the entire backend.
func TestNewBackend_K8sSucceeds_BadRedisURL(t *testing.T) {
	kcfg := writeTestKubeconfig(t)
	t.Setenv("K8S_KUBECONFIG", kcfg)
	t.Setenv("REDIS_URL_FOR_ROUTES", "not-a-valid-redis-url-scheme")
	t.Setenv("REDIS_URL", "")

	got := NewBackend("k8s", "ignored.host")
	kb, ok := got.(*K8sBackend)
	if !ok {
		t.Fatalf("NewBackend(k8s) = %T; want *K8sBackend", got)
	}
	if kb.rdb != nil {
		t.Error("rdb must be nil when REDIS_URL_FOR_ROUTES is malformed")
		_ = kb.rdb.Close()
	}
}

// TestNewBackend_K8sSucceeds_FallsBackToRedisURL — if REDIS_URL_FOR_ROUTES is
// empty but REDIS_URL is set, the latter is used as the route-registry URL.
func TestNewBackend_K8sSucceeds_FallsBackToRedisURL(t *testing.T) {
	kcfg := writeTestKubeconfig(t)
	t.Setenv("K8S_KUBECONFIG", kcfg)
	t.Setenv("REDIS_URL_FOR_ROUTES", "")
	t.Setenv("REDIS_URL", "redis://127.0.0.1:1/0")

	got := NewBackend("k8s", "ignored.host")
	kb, ok := got.(*K8sBackend)
	if !ok {
		t.Fatalf("NewBackend(k8s) = %T; want *K8sBackend", got)
	}
	if kb.rdb == nil {
		t.Error("rdb must be wired when REDIS_URL is set as the fallback")
	}
	if kb.rdb != nil {
		_ = kb.rdb.Close()
	}
}

// TestNewK8sBackend_DefaultStorageClassAndImage — when storageClass and image
// are empty, newK8sBackend fills in the documented defaults (gp3 and
// nats:2.10-alpine).
func TestNewK8sBackend_DefaultStorageClassAndImage(t *testing.T) {
	kcfg := writeTestKubeconfig(t)
	b, err := newK8sBackend(kcfg, "", "", "")
	if err != nil {
		t.Fatalf("newK8sBackend: %v", err)
	}
	if b.image != "nats:2.10-alpine" {
		t.Errorf("default image = %q; want nats:2.10-alpine", b.image)
	}
	if b.storageClass != "gp3" {
		t.Errorf("default storageClass = %q; want gp3", b.storageClass)
	}
}

// TestSizingForTier_AllKnownTiers — the tier→sizing table must:
//   - return distinct sizings for anonymous/hobby/pro/team-or-growth,
//   - return zero pvcMi for anonymous (memory-only JetStream),
//   - return non-zero pvcMi for every paid tier,
//   - fall back to hobby sizing for any unknown tier (no panic, no zero sizing).
func TestSizingForTier_AllKnownTiers(t *testing.T) {
	cases := []struct {
		tier      string
		wantPVCMi int
	}{
		{"anonymous", 0},
		{"hobby", 1024},
		{"pro", 10240},
		{"team", 51200},
		{"growth", 51200},
	}
	for _, tc := range cases {
		t.Run(tc.tier, func(t *testing.T) {
			sz := sizingForTier(tc.tier)
			if sz.pvcMi != tc.wantPVCMi {
				t.Errorf("sizingForTier(%q).pvcMi = %d; want %d", tc.tier, sz.pvcMi, tc.wantPVCMi)
			}
			if sz.cpuReq == "" || sz.memReq == "" || sz.cpuLim == "" || sz.memLim == "" {
				t.Errorf("sizingForTier(%q) returned empty cpu/mem fields: %+v", tc.tier, sz)
			}
			if sz.qCPURequests == "" || sz.qMemRequests == "" || sz.qCPULimits == "" || sz.qMemLimits == "" {
				t.Errorf("sizingForTier(%q) returned empty namespace-quota fields: %+v", tc.tier, sz)
			}
		})
	}

	// Unknown tier → hobby fallback (not a zero struct).
	if got, hobby := sizingForTier("unknown_tier_xyz"), sizingForTier("hobby"); !reflect.DeepEqual(got, hobby) {
		t.Errorf("sizingForTier(unknown) = %+v; want hobby fallback %+v", got, hobby)
	}
}
