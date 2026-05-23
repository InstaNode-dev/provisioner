package mongo

// backend_test.go — covers the factory helpers in backend.go.
// All tests are pure unit and require no live infra.

import (
	"os"
	"testing"
)

// TestK8sEnv_FallbackAndOverride exercises both branches of k8sEnv.
func TestK8sEnv_FallbackAndOverride(t *testing.T) {
	const key = "INSTANT_MONGO_TEST_KEY"
	t.Setenv(key, "")
	if got := k8sEnv(key, "fallback"); got != "fallback" {
		t.Errorf("empty env: got %q, want fallback", got)
	}
	t.Setenv(key, "explicit")
	if got := k8sEnv(key, "fallback"); got != "explicit" {
		t.Errorf("explicit env: got %q, want explicit", got)
	}
	// Unset via OS so we cover the os.Getenv == "" branch directly.
	os.Unsetenv(key)
	if got := k8sEnv(key, "fb2"); got != "fb2" {
		t.Errorf("unset env: got %q, want fb2", got)
	}
}

// TestK8sEnvInt_FallbackParseAndBad covers fallback / good parse / bad parse.
func TestK8sEnvInt_FallbackParseAndBad(t *testing.T) {
	const key = "INSTANT_MONGO_TEST_INT"
	t.Setenv(key, "")
	if got := k8sEnvInt(key, 7); got != 7 {
		t.Errorf("empty env: got %d, want 7", got)
	}
	t.Setenv(key, "42")
	if got := k8sEnvInt(key, 7); got != 42 {
		t.Errorf("explicit: got %d, want 42", got)
	}
	// Non-numeric falls back.
	t.Setenv(key, "notanint")
	if got := k8sEnvInt(key, 7); got != 7 {
		t.Errorf("bad parse: got %d, want 7", got)
	}
}

// TestGoredisHelpers asserts the narrow aliases used by NewBackend really
// proxy through to goredis. ParseURL on a bad URL must error; on a good URL
// produce Options whose Addr matches; NewClient must return non-nil.
func TestGoredisHelpers(t *testing.T) {
	if _, err := goredisParseURL("://not-a-url"); err == nil {
		t.Error("goredisParseURL on bad URL: want error, got nil")
	}
	opts, err := goredisParseURL("redis://127.0.0.1:6379/0")
	if err != nil {
		t.Fatalf("goredisParseURL: %v", err)
	}
	if opts.Addr != "127.0.0.1:6379" {
		t.Errorf("Addr = %q, want 127.0.0.1:6379", opts.Addr)
	}
	c := goredisNewClient(opts)
	if c == nil {
		t.Fatal("goredisNewClient: got nil")
	}
	_ = c.Close()
}

// TestNewBackend_LocalDefault covers the default-case branch.
func TestNewBackend_LocalDefault(t *testing.T) {
	b := NewBackend("", "mongodb://x:y@h:1", "h:1")
	if _, ok := b.(*LocalBackend); !ok {
		t.Fatalf("default backend type: got %T, want *LocalBackend", b)
	}
	// Unknown type also routes to local (default case).
	b2 := NewBackend("unknown", "", "")
	if _, ok := b2.(*LocalBackend); !ok {
		t.Fatalf("unknown backend type: got %T, want *LocalBackend", b2)
	}
}

// TestNewBackend_K8sFallsBackToLocalWithoutKubeconfig hits the "k8s init failed,
// fall back to local" branch. We deliberately point K8S_KUBECONFIG at a path
// that doesn't exist so newK8sBackend returns an error; NewBackend must catch
// it and return a LocalBackend rather than panic / return nil.
func TestNewBackend_K8sFallsBackToLocalWithoutKubeconfig(t *testing.T) {
	t.Setenv("K8S_KUBECONFIG", "/dev/null/does-not-exist")
	t.Setenv("REDIS_URL_FOR_ROUTES", "") // ensure route registry path stays off
	t.Setenv("REDIS_URL", "")

	b := NewBackend("k8s", "mongodb://x:y@h:1", "h:1")
	if _, ok := b.(*LocalBackend); !ok {
		t.Fatalf("k8s init failure: got %T, want fallback to *LocalBackend", b)
	}
}

// TestNewK8sDedicatedBackend_BadKubeconfig covers the exported entry point's
// error path. A missing kubeconfig file must surface an error instead of a
// panic / nil-deref.
func TestNewK8sDedicatedBackend_BadKubeconfig(t *testing.T) {
	if _, err := NewK8sDedicatedBackend("/dev/null/does-not-exist", "", "", "", 0); err == nil {
		t.Fatal("NewK8sDedicatedBackend: want error on bad kubeconfig, got nil")
	}
}

// writeFakeKubeconfig writes a minimal-but-valid kubeconfig to a temp dir and
// returns its path. clientcmd.BuildConfigFromFlags only parses; it does not
// dial the cluster, so a stub URL is fine.
func writeFakeKubeconfig(t *testing.T) string {
	t.Helper()
	const yaml = `
apiVersion: v1
kind: Config
clusters:
- name: stub
  cluster:
    server: https://127.0.0.1:6443
contexts:
- name: stub
  context:
    cluster: stub
    user: stub
current-context: stub
users:
- name: stub
  user:
    token: stubtoken
`
	dir := t.TempDir()
	path := dir + "/kubeconfig"
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	return path
}

// TestNewK8sDedicatedBackend_HappyPath covers the success branch of
// newK8sBackend: a parseable kubeconfig produces a non-nil backend whose
// defaults are applied (image=mongo:7, storageClass=gp3, storageSizeGi=50).
func TestNewK8sDedicatedBackend_HappyPath(t *testing.T) {
	cfg := writeFakeKubeconfig(t)
	b, err := NewK8sDedicatedBackend(cfg, "", "", "host.example", 0)
	if err != nil {
		t.Fatalf("NewK8sDedicatedBackend: %v", err)
	}
	kb, ok := b.(*K8sBackend)
	if !ok {
		t.Fatalf("got %T, want *K8sBackend", b)
	}
	if kb.image != "mongo:7" {
		t.Errorf("default image = %q, want mongo:7", kb.image)
	}
	if kb.storageClass != "gp3" {
		t.Errorf("default storageClass = %q, want gp3", kb.storageClass)
	}
	if kb.storageSizeGi != 50 {
		t.Errorf("default storageSizeGi = %d, want 50", kb.storageSizeGi)
	}
	if kb.externalHost != "host.example" {
		t.Errorf("externalHost = %q", kb.externalHost)
	}
}

// TestNewBackend_K8sSuccessPath_AppliesPublicHostAndRouteRegistry covers the
// k8s-success branch of NewBackend, including the redis route-registry wiring
// when REDIS_URL_FOR_ROUTES is set.
func TestNewBackend_K8sSuccessPath_AppliesPublicHostAndRouteRegistry(t *testing.T) {
	cfg := writeFakeKubeconfig(t)
	t.Setenv("K8S_KUBECONFIG", cfg)
	t.Setenv("K8S_STORAGE_CLASS", "do-block-storage")
	t.Setenv("K8S_MONGO_IMAGE", "mongo:6")
	t.Setenv("K8S_EXTERNAL_HOST", "ext.example")
	t.Setenv("K8S_MONGO_PUBLIC_HOST", "mongo.example.com")
	t.Setenv("K8S_MONGO_STORAGE_GI", "200")
	t.Setenv("REDIS_URL_FOR_ROUTES", "redis://127.0.0.1:6379/0")
	t.Setenv("MONGO_PROXY_ROUTE_PREFIX", "mongo_route:")
	t.Setenv("MONGO_PROXY_USER_ROUTE_PREFIX", "mongo_route_by_user:")

	b := NewBackend("k8s", "", "")
	kb, ok := b.(*K8sBackend)
	if !ok {
		t.Fatalf("k8s backend: got %T, want *K8sBackend", b)
	}
	if kb.publicHost != "mongo.example.com" {
		t.Errorf("publicHost = %q", kb.publicHost)
	}
	if kb.image != "mongo:6" {
		t.Errorf("image = %q", kb.image)
	}
	if kb.storageClass != "do-block-storage" {
		t.Errorf("storageClass = %q", kb.storageClass)
	}
	if kb.storageSizeGi != 200 {
		t.Errorf("storageSizeGi = %d", kb.storageSizeGi)
	}
	if kb.rdb == nil {
		t.Errorf("route registry not enabled despite REDIS_URL_FOR_ROUTES set")
	}
	if kb.routePrefix == "" || kb.userPrefix == "" {
		t.Errorf("route prefixes empty: route=%q user=%q", kb.routePrefix, kb.userPrefix)
	}
}

// TestNewBackend_K8sSuccessPath_BadRedisURLSkipsRouteRegistry covers the
// "REDIS_URL_FOR_ROUTES is set but unparseable" branch — the backend still
// initialises, just without route registry.
func TestNewBackend_K8sSuccessPath_BadRedisURLSkipsRouteRegistry(t *testing.T) {
	cfg := writeFakeKubeconfig(t)
	t.Setenv("K8S_KUBECONFIG", cfg)
	t.Setenv("REDIS_URL_FOR_ROUTES", "://not-a-url")

	b := NewBackend("k8s", "", "")
	kb, ok := b.(*K8sBackend)
	if !ok {
		t.Fatalf("k8s backend: got %T, want *K8sBackend", b)
	}
	if kb.rdb != nil {
		t.Errorf("route registry enabled with unparseable URL")
	}
}
