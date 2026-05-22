package postgres

// backend_factory_test.go — unit tests for the NewBackend / NewDedicatedBackend
// factory in backend.go. These functions are pure construction (no live
// dependencies) so the tests run in every coverage mode.

import (
	"os"
	"path/filepath"
	"testing"
)

func TestK8sEnv(t *testing.T) {
	t.Setenv("K8S_PROBE_TEST_SET", "found")
	if got := k8sEnv("K8S_PROBE_TEST_SET", "fallback"); got != "found" {
		t.Errorf("k8sEnv(set) = %q; want found", got)
	}
	os.Unsetenv("K8S_PROBE_TEST_UNSET")
	if got := k8sEnv("K8S_PROBE_TEST_UNSET", "fallback"); got != "fallback" {
		t.Errorf("k8sEnv(unset) = %q; want fallback", got)
	}
}

func TestK8sEnvInt(t *testing.T) {
	t.Setenv("K8S_PROBE_INT_SET", "42")
	if got := k8sEnvInt("K8S_PROBE_INT_SET", 9); got != 42 {
		t.Errorf("k8sEnvInt(set) = %d; want 42", got)
	}
	t.Setenv("K8S_PROBE_INT_GARBAGE", "not-a-number")
	if got := k8sEnvInt("K8S_PROBE_INT_GARBAGE", 9); got != 9 {
		t.Errorf("k8sEnvInt(garbage) = %d; want fallback 9", got)
	}
	os.Unsetenv("K8S_PROBE_INT_UNSET")
	if got := k8sEnvInt("K8S_PROBE_INT_UNSET", 9); got != 9 {
		t.Errorf("k8sEnvInt(unset) = %d; want fallback 9", got)
	}
}

func TestNewBackend_DefaultIsLocal(t *testing.T) {
	b := NewBackend("", "postgres://u:p@h/d", "", "", "")
	if _, ok := b.(*LocalBackend); !ok {
		t.Errorf("NewBackend(\"\") = %T; want *LocalBackend", b)
	}
}

func TestNewBackend_UnknownTypeIsLocal(t *testing.T) {
	b := NewBackend("nonsense", "postgres://u:p@h/d", "", "", "")
	if _, ok := b.(*LocalBackend); !ok {
		t.Errorf("NewBackend(\"nonsense\") = %T; want *LocalBackend", b)
	}
}

func TestNewBackend_Neon(t *testing.T) {
	b := NewBackend("neon", "", "", "api-key", "us-east-1")
	if _, ok := b.(*NeonBackend); !ok {
		t.Errorf("NewBackend(\"neon\") = %T; want *NeonBackend", b)
	}
}

func TestNewBackend_MultiCluster(t *testing.T) {
	b := NewBackend("", "", "postgres://a/x, postgres://b/y , ", "", "")
	lb, ok := b.(*LocalBackend)
	if !ok {
		t.Fatalf("NewBackend(multi) = %T; want *LocalBackend", b)
	}
	if got := len(lb.router.adminURLs); got != 2 {
		t.Errorf("router has %d clusters; want 2 (trailing/empty entries filtered)", got)
	}
}

func TestNewBackend_MultiCluster_AllEmptyFallsBack(t *testing.T) {
	b := NewBackend("", "postgres://u:p@h/d", " , , ", "", "")
	lb, ok := b.(*LocalBackend)
	if !ok {
		t.Fatalf("NewBackend(all-empty cluster URLs) = %T; want *LocalBackend", b)
	}
	if got := len(lb.router.adminURLs); got != 1 {
		t.Errorf("router has %d clusters; want 1 single-customer URL fallback", got)
	}
}

func TestNewBackend_K8s_FallsBackToLocal_WhenOutOfCluster(t *testing.T) {
	// Without a valid kubeconfig or in-cluster service account, newK8sBackend
	// errors and NewBackend logs + falls back to LocalBackend. We verify the
	// fallback type so the gRPC server doesn't crash on dev machines.
	t.Setenv("K8S_KUBECONFIG", "/nonexistent/kubeconfig-for-test")
	b := NewBackend("k8s", "postgres://u:p@h/d", "", "", "")
	if _, ok := b.(*LocalBackend); !ok {
		t.Errorf("NewBackend(\"k8s\") with bad kubeconfig = %T; want LocalBackend fallback", b)
	}
}

// TestNewBackend_K8s_RouteRegistry covers the k8s success branch of NewBackend:
// a valid kubeconfig lets newK8sBackend succeed, and REDIS_URL_FOR_ROUTES drives
// the route-registry enablement block (goredisParseURL → goredisNewClient →
// EnableRouteRegistry). Returns a *K8sBackend, not the LocalBackend fallback.
func TestNewBackend_K8s_RouteRegistry(t *testing.T) {
	dir := t.TempDir()
	kc := filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(kc, []byte(validKubeconfig), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	t.Setenv("K8S_KUBECONFIG", kc)
	t.Setenv("REDIS_URL_FOR_ROUTES", "redis://127.0.0.1:6379/0")
	t.Setenv("PG_PROXY_ROUTE_PREFIX", "test_pg_route:")
	b := NewBackend("k8s", "postgres://u:p@h/d", "", "", "")
	kb, ok := b.(*K8sBackend)
	if !ok {
		t.Fatalf("NewBackend(k8s, valid kubeconfig) = %T; want *K8sBackend", b)
	}
	if kb.rdb == nil {
		t.Error("route registry not enabled; rdb is nil")
	}
	if kb.routePrefix != "test_pg_route:" {
		t.Errorf("routePrefix = %q; want test_pg_route:", kb.routePrefix)
	}
}

// TestNewBackend_K8s_BadRedisURL covers the route-registry-disabled branch:
// newK8sBackend succeeds but the REDIS URL fails to parse, so route registry is
// left disabled (the goredisParseURL error arm) and a *K8sBackend is still
// returned.
func TestNewBackend_K8s_BadRedisURL(t *testing.T) {
	dir := t.TempDir()
	kc := filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(kc, []byte(validKubeconfig), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	t.Setenv("K8S_KUBECONFIG", kc)
	t.Setenv("REDIS_URL_FOR_ROUTES", "::not a redis url::")
	b := NewBackend("k8s", "postgres://u:p@h/d", "", "", "")
	kb, ok := b.(*K8sBackend)
	if !ok {
		t.Fatalf("NewBackend(k8s, bad redis URL) = %T; want *K8sBackend", b)
	}
	if kb.rdb != nil {
		t.Error("route registry enabled despite bad redis URL; rdb should be nil")
	}
}

func TestNewDedicatedBackend(t *testing.T) {
	b := NewDedicatedBackend("postgres://a/x", "")
	if b == nil {
		t.Fatal("NewDedicatedBackend returned nil")
	}
	if _, ok := b.(*DedicatedProvider); !ok {
		t.Errorf("NewDedicatedBackend = %T; want *DedicatedProvider", b)
	}
}

func TestNewK8sDedicatedBackend_BadConfig(t *testing.T) {
	_, err := NewK8sDedicatedBackend("/nonexistent/kubeconfig", "", "", "", 0)
	if err == nil {
		t.Error("NewK8sDedicatedBackend(bad path) returned nil; want error")
	}
}
