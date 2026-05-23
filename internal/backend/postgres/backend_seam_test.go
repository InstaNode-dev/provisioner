package postgres

// backend_seam_test.go — coverage for the NewBackend factory, the goredis
// helper aliases, k8sEnv/k8sEnvInt, and the cluster-router paths the existing
// tests don't reach (at-capacity Pick, refreshCounts/dbCount, pollLoop,
// ProviderResourceID).

import (
	"context"
	"os"
	"testing"
	"time"
)

// osWriteFileBackend writes a kubeconfig fixture for the NewBackend k8s tests.
func osWriteFileBackend(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

func TestK8sEnv_Seam(t *testing.T) {
	t.Setenv("K8S_TEST_KEY", "v")
	if k8sEnv("K8S_TEST_KEY", "fb") != "v" {
		t.Error("should return env value")
	}
	if k8sEnv("K8S_UNSET_KEY_XYZ", "fb") != "fb" {
		t.Error("should return fallback")
	}
}

func TestK8sEnvInt_Seam(t *testing.T) {
	t.Setenv("K8S_INT_KEY", "42")
	if k8sEnvInt("K8S_INT_KEY", 7) != 42 {
		t.Error("should parse env int")
	}
	t.Setenv("K8S_INT_BAD", "notanint")
	if k8sEnvInt("K8S_INT_BAD", 7) != 7 {
		t.Error("bad int should fall back")
	}
	if k8sEnvInt("K8S_INT_UNSET_XYZ", 9) != 9 {
		t.Error("unset should fall back")
	}
}

func TestGoredisAliases_Seam(t *testing.T) {
	if _, err := goredisParseURL("not-a-redis-url"); err == nil {
		t.Error("expected parse error")
	}
	opt, err := goredisParseURL("redis://127.0.0.1:6379")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if c := goredisNewClient(opt); c == nil {
		t.Error("nil client")
	}
}

func TestNewBackend_Neon_Seam(t *testing.T) {
	b := NewBackend("neon", "", "", "apikey", "region")
	if _, ok := b.(*NeonBackend); !ok {
		t.Errorf("want *NeonBackend, got %T", b)
	}
}

func TestNewBackend_DefaultLocal(t *testing.T) {
	b := NewBackend("", "postgres://u@h/db", "", "", "")
	if _, ok := b.(*LocalBackend); !ok {
		t.Errorf("want *LocalBackend, got %T", b)
	}
}

func TestNewBackend_ClusterURLs(t *testing.T) {
	b := NewBackend("", "", "url0,url1, ,url2,", "", "")
	lb, ok := b.(*LocalBackend)
	if !ok {
		t.Fatalf("want *LocalBackend, got %T", b)
	}
	// trailing empty + whitespace entries filtered → 3 clusters
	if len(lb.router.adminURLs) != 3 {
		t.Errorf("adminURLs = %v; want 3 filtered", lb.router.adminURLs)
	}
}

func TestNewBackend_ClusterURLs_AllEmpty_FallsBack(t *testing.T) {
	b := NewBackend("", "cust", " , ,", "", "")
	lb := b.(*LocalBackend)
	if len(lb.router.adminURLs) != 1 {
		t.Errorf("all-empty cluster list should fall back to single; got %v", lb.router.adminURLs)
	}
}

// k8s backend with no kubeconfig + no in-cluster config → newK8sBackend fails →
// NewBackend falls back to local. Covers the fallback branch in the factory.
func TestNewBackend_K8s_FallbackToLocal(t *testing.T) {
	t.Setenv("K8S_KUBECONFIG", "/nonexistent/kubeconfig-path")
	b := NewBackend("k8s", "cust-url", "", "", "")
	if _, ok := b.(*LocalBackend); !ok {
		t.Errorf("k8s init failure should fall back to *LocalBackend, got %T", b)
	}
}

// NewBackend("k8s") with a valid kubeconfig + a parseable REDIS_URL_FOR_ROUTES
// exercises the route-registry-enabled block (the only sub-95 gap in NewBackend).
func TestNewBackend_K8s_RouteRegistryEnabled(t *testing.T) {
	dir := t.TempDir()
	kc := dir + "/kubeconfig"
	if err := osWriteFileBackend(kc, minimalKubeconfig); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	t.Setenv("K8S_KUBECONFIG", kc)
	t.Setenv("REDIS_URL_FOR_ROUTES", "redis://127.0.0.1:6379")
	b := NewBackend("k8s", "cust", "", "", "")
	kb, ok := b.(*K8sBackend)
	if !ok {
		t.Fatalf("want *K8sBackend, got %T", b)
	}
	if kb.rdb == nil {
		t.Error("route registry should be enabled when REDIS_URL_FOR_ROUTES parses")
	}
}

// NewBackend("k8s") with a valid kubeconfig but an UNPARSEABLE route Redis URL
// exercises the route-registry-disabled (warn) branch.
func TestNewBackend_K8s_RouteRegistryBadURL(t *testing.T) {
	dir := t.TempDir()
	kc := dir + "/kubeconfig"
	if err := osWriteFileBackend(kc, minimalKubeconfig); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	t.Setenv("K8S_KUBECONFIG", kc)
	t.Setenv("REDIS_URL_FOR_ROUTES", "::::not-a-redis-url")
	b := NewBackend("k8s", "cust", "", "", "")
	kb, ok := b.(*K8sBackend)
	if !ok {
		t.Fatalf("want *K8sBackend, got %T", b)
	}
	if kb.rdb != nil {
		t.Error("route registry should stay disabled when the URL fails to parse")
	}
}

func TestNewDedicatedBackend_Seam(t *testing.T) {
	b := NewDedicatedBackend("dsn", "")
	if _, ok := b.(*DedicatedProvider); !ok {
		t.Errorf("want *DedicatedProvider, got %T", b)
	}
}

// --- cluster_router uncovered paths ---

func TestProviderResourceID(t *testing.T) {
	r := newClusterRouter([]string{"u0", "u1"}, 0)
	if r.ProviderResourceID(1) != "local:1" {
		t.Errorf("got %q", r.ProviderResourceID(1))
	}
}

func TestPick_AllAtCapacity_FallsBackToZero(t *testing.T) {
	r := newClusterRouter([]string{"u0", "u1"}, 1)
	// Saturate both clusters' counts so headroom <= 0 everywhere.
	r.mu.Lock()
	r.counts[0] = 5
	r.counts[1] = 5
	r.mu.Unlock()
	idx, url, err := r.Pick()
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if idx != 0 || url != "u0" {
		t.Errorf("at-capacity Pick should fall back to index 0; got %d/%q", idx, url)
	}
}

func TestPick_AllURLsEmpty_BestNegativeFallback(t *testing.T) {
	// Non-empty slice but every URL blank → loop never sets best → best<0 path.
	r := newClusterRouter([]string{"", ""}, 0)
	idx, _, err := r.Pick()
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if idx != 0 {
		t.Errorf("best<0 fallback should pick index 0; got %d", idx)
	}
}

func TestPick_NoClusters_Error(t *testing.T) {
	r := newClusterRouter(nil, 0)
	if _, _, err := r.Pick(); err == nil {
		t.Error("expected no-clusters error")
	}
}

func TestRefreshCounts_ConnectFails_KeepsPrevious(t *testing.T) {
	// Unreachable admin URL → dbCount errors → previous count retained.
	r := newClusterRouter([]string{"postgres://x@127.0.0.1:1/none", ""}, 0)
	r.mu.Lock()
	r.counts[0] = 3
	r.mu.Unlock()
	r.refreshCounts(context.Background())
	r.mu.RLock()
	got := r.counts[0]
	r.mu.RUnlock()
	if got != 3 {
		t.Errorf("count after failed poll = %d; want previous 3", got)
	}
}

func TestDbCount_ConnectError(t *testing.T) {
	r := newClusterRouter([]string{"x"}, 0)
	if _, err := r.dbCount(context.Background(), "postgres://x@127.0.0.1:1/none"); err == nil {
		t.Error("expected connect error")
	}
}

func TestDbCount_Success_ViaSeam(t *testing.T) {
	fc := &fakePGConn{scanInt64: 11}
	withPGXConnect(t, fc, nil)
	r := newClusterRouter([]string{"u0"}, 0)
	n, err := r.dbCount(context.Background(), "u0")
	if err != nil || n != 11 {
		t.Errorf("dbCount = %d, %v", n, err)
	}
}

func TestDbCount_ScanError_ViaSeam(t *testing.T) {
	fc := &fakePGConn{queryRowErr: errSeam}
	withPGXConnect(t, fc, nil)
	r := newClusterRouter([]string{"u0"}, 0)
	if _, err := r.dbCount(context.Background(), "u0"); err == nil {
		t.Error("expected scan error")
	}
}

// pollLoop ticker branch: shrink the poll interval so ticker.C fires and the
// periodic refreshCounts runs, then cancel.
func TestPollLoop_TickerFires(t *testing.T) {
	fc := &fakePGConn{scanInt64: 1}
	withPGXConnect(t, fc, nil)
	r := newClusterRouter([]string{"u0"}, 0)
	r.pollInterval = 5 * time.Millisecond // per-instance, no shared-global race
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.pollLoop(ctx); close(done) }()
	time.Sleep(40 * time.Millisecond) // let several ticks fire
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pollLoop did not return after cancel")
	}
}

// pollLoop with a non-positive pollInterval falls back to the default — covers
// the interval<=0 guard. We cancel immediately after start so the default-60s
// ticker never actually fires (we only need the guard line executed).
func TestPollLoop_ZeroIntervalFallsBackToDefault(t *testing.T) {
	fc := &fakePGConn{scanInt64: 1}
	withPGXConnect(t, fc, nil)
	r := newClusterRouter([]string{"u0"}, 0)
	r.pollInterval = 0 // → guard sets interval = defaultClusterPollInterval
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel: after the immediate refresh + ticker setup, return
	done := make(chan struct{})
	go func() { r.pollLoop(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pollLoop did not return")
	}
}

// pollLoop ctx.Done() return path: call pollLoop directly with an
// already-cancelled context and a fresh (never-closed) done channel so the
// select can only fire on ctx.Done(). Deterministic — no race with done.
func TestPollLoop_CtxDoneReturns(t *testing.T) {
	fc := &fakePGConn{scanInt64: 1}
	withPGXConnect(t, fc, nil)
	r := newClusterRouter([]string{"u0"}, 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel: after the immediate refresh, select hits ctx.Done()
	done := make(chan struct{})
	go func() { r.pollLoop(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pollLoop did not return on ctx.Done()")
	}
}

// pollLoop done-channel return path: drive directly and signal exit via
// Shutdown (closes done), joining the goroutine so it can't leak.
func TestPollLoop_ShutdownReturns(t *testing.T) {
	fc := &fakePGConn{scanInt64: 1}
	withPGXConnect(t, fc, nil)
	r := newClusterRouter([]string{"u0"}, 0)
	done := make(chan struct{})
	go func() { r.pollLoop(context.Background()); close(done) }()
	// Wait until the poller is up, then Shutdown (closes r.done) → return.
	deadline := time.Now().Add(2 * time.Second)
	for r.pollStarts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	r.Shutdown()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pollLoop did not return after Shutdown")
	}
}
