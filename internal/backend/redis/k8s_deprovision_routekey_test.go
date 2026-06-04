package redis

// k8s_deprovision_routekey_test.go — regression coverage for the SWEEP #10 fix:
// Deprovision must delete BOTH route-registry keys best-effort even when the
// namespace Delete errors early (apiserver / RBAC / transient failure). Before
// the fix a non-NotFound Delete error returned immediately, stranding the
// password-route key — which for a paid/permanent resource carries no TTL and
// would route a dead password through the proxy forever.
//
// These tests use a real Redis (CUSTOMER_REDIS_URL / localhost:6379 — the
// coverage CI job provisions a redis:7-alpine service container) so the actual
// DEL against the route registry is exercised. They skip cleanly when no Redis
// is reachable locally.

import (
	"context"
	"fmt"
	"testing"

	goredis "github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// newRouteRegistryBackend builds a K8sBackend wired to a real Redis route
// registry with unique key prefixes per test, plus the supplied fake clientset.
func newRouteRegistryBackend(t *testing.T, cs *fake.Clientset) (*K8sBackend, *goredis.Client, string, string) {
	t.Helper()
	addr := liveRedisAddr(t) // skips if no Redis
	rdb := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { _ = rdb.Close() })

	suffix, err := redisK8sRandHex(6)
	if err != nil {
		t.Fatalf("redisK8sRandHex: %v", err)
	}
	routePrefix := "test_redis_route_" + suffix + ":"
	passwordPrefix := "test_redis_route_by_password_" + suffix + ":"

	b := &K8sBackend{cs: cs, rdb: rdb, routePrefix: routePrefix, passwordPrefix: passwordPrefix}
	return b, rdb, routePrefix, passwordPrefix
}

// secretWithPassword returns a redis-auth Secret fixture carrying the given
// password so Deprovision can recover the password-route key before teardown.
func secretWithPassword(ns, password string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-auth", Namespace: ns},
		Data:       map[string][]byte{"REDIS_PASSWORD": []byte(password)},
	}
}

// TestDeprovision_DeletesRouteKeys_OnNamespaceDeleteError is the core SWEEP #10
// regression guard. With a non-NotFound namespace Delete error, Deprovision must
// STILL delete both the token-route key and the password-route key before
// returning the error, instead of stranding them.
func TestDeprovision_DeletesRouteKeys_OnNamespaceDeleteError(t *testing.T) {
	const token = "tok-routekey-deleteerr"
	const password = "p4ssw0rd-deleteerr"
	ns := redisK8sNsPrefix + token

	cs := fake.NewClientset(secretWithPassword(ns, password))
	// Inject a transient (non-NotFound) failure on namespace Delete.
	cs.PrependReactor("delete", "namespaces", func(_ ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("etcd: leader changed")
	})

	b, rdb, routePrefix, passwordPrefix := newRouteRegistryBackend(t, cs)
	ctx := context.Background()

	// Seed both route keys as a real Provision would (no TTL — paid resource).
	tokenKey := routePrefix + token
	pwKey := passwordPrefix + password
	if err := rdb.Set(ctx, tokenKey, "redis."+ns+".svc.cluster.local:6379", 0).Err(); err != nil {
		t.Fatalf("seed token route key: %v", err)
	}
	if err := rdb.Set(ctx, pwKey, "redis."+ns+".svc.cluster.local:6379", 0).Err(); err != nil {
		t.Fatalf("seed password route key: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Del(context.Background(), tokenKey, pwKey).Err() })

	err := b.Deprovision(ctx, token, ns)
	if err == nil {
		t.Fatal("Deprovision must surface the namespace Delete error for caller retry")
	}

	// The fix: both keys must be gone despite the Delete error.
	if n, _ := rdb.Exists(ctx, tokenKey).Result(); n != 0 {
		t.Errorf("token route key %q leaked after Delete error (n=%d)", tokenKey, n)
	}
	if n, _ := rdb.Exists(ctx, pwKey).Result(); n != 0 {
		t.Errorf("password route key %q leaked after Delete error (n=%d) — the SWEEP #10 bug", pwKey, n)
	}
}

// TestDeprovision_DeletesRouteKeys_OnSuccess is the happy-path control: a clean
// namespace Delete still removes both route keys (the post-fix normal path).
func TestDeprovision_DeletesRouteKeys_OnSuccess(t *testing.T) {
	const token = "tok-routekey-ok"
	const password = "p4ssw0rd-ok"
	ns := redisK8sNsPrefix + token

	cs := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
		secretWithPassword(ns, password),
	)

	b, rdb, routePrefix, passwordPrefix := newRouteRegistryBackend(t, cs)
	ctx := context.Background()

	tokenKey := routePrefix + token
	pwKey := passwordPrefix + password
	if err := rdb.Set(ctx, tokenKey, "fqdn:6379", 0).Err(); err != nil {
		t.Fatalf("seed token route key: %v", err)
	}
	if err := rdb.Set(ctx, pwKey, "fqdn:6379", 0).Err(); err != nil {
		t.Fatalf("seed password route key: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Del(context.Background(), tokenKey, pwKey).Err() })

	if err := b.Deprovision(ctx, token, ns); err != nil {
		t.Fatalf("Deprovision (success path): %v", err)
	}
	if n, _ := rdb.Exists(ctx, tokenKey).Result(); n != 0 {
		t.Errorf("token route key %q not deleted on success (n=%d)", tokenKey, n)
	}
	if n, _ := rdb.Exists(ctx, pwKey).Result(); n != 0 {
		t.Errorf("password route key %q not deleted on success (n=%d)", pwKey, n)
	}
}

// TestDeprovision_RouteKeyCleanup_DelError exercises the best-effort warn
// branches inside the route-key cleanup closure: when the Redis DEL itself
// fails (here, the client is closed before Deprovision runs) the closure logs a
// warning per key and continues — the namespace teardown outcome is unchanged.
func TestDeprovision_RouteKeyCleanup_DelError(t *testing.T) {
	const token = "tok-delerr"
	const password = "p4ssw0rd-delerr"
	ns := redisK8sNsPrefix + token

	cs := fake.NewClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}},
		secretWithPassword(ns, password),
	)

	b, rdb, _, _ := newRouteRegistryBackend(t, cs)
	// Close the client so every subsequent DEL returns an error, driving the
	// route_unregister_failed / password_route_unregister_failed warn branches.
	_ = rdb.Close()

	if err := b.Deprovision(context.Background(), token, ns); err != nil {
		t.Fatalf("Deprovision must remain success when only the route-key DEL fails: %v", err)
	}
}

// TestDeprovision_AlreadyGone_StillDeletesRouteKeys covers the NotFound branch:
// the namespace is already gone (no-op log), and the route keys are still cleaned
// up via the post-switch routeKeys() call.
func TestDeprovision_AlreadyGone_StillDeletesRouteKeys(t *testing.T) {
	const token = "tok-already-gone"
	const password = "p4ssw0rd-gone"
	ns := redisK8sNsPrefix + token

	// No namespace object and no secret → Delete returns NotFound; password
	// cannot be recovered, so only the token-route key is removable.
	cs := fake.NewClientset()

	b, rdb, routePrefix, _ := newRouteRegistryBackend(t, cs)
	ctx := context.Background()

	tokenKey := routePrefix + token
	if err := rdb.Set(ctx, tokenKey, "fqdn:6379", 0).Err(); err != nil {
		t.Fatalf("seed token route key: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Del(context.Background(), tokenKey).Err() })

	if err := b.Deprovision(ctx, token, ns); err != nil {
		t.Fatalf("Deprovision must succeed when namespace is already gone: %v", err)
	}
	if n, _ := rdb.Exists(ctx, tokenKey).Result(); n != 0 {
		t.Errorf("token route key %q not deleted on already-gone path (n=%d)", tokenKey, n)
	}
}

// TestDeprovision_RouteKeyCleanup_NilRDB exercises the b.rdb == nil short-circuit
// inside the route-key cleanup closure on the Delete-error path. This runs in any
// environment (no real Redis needed) so the new branch lines are always covered.
func TestDeprovision_RouteKeyCleanup_NilRDB(t *testing.T) {
	const token = "tok-nilrdb"
	ns := redisK8sNsPrefix + token

	cs := fake.NewClientset()
	cs.PrependReactor("delete", "namespaces", func(_ ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("apiserver unavailable")
	})

	// rdb intentionally nil — route-key cleanup must be a no-op, error still surfaced.
	b := &K8sBackend{cs: cs}
	if err := b.Deprovision(context.Background(), token, ns); err == nil {
		t.Fatal("Deprovision must surface the namespace Delete error even with rdb=nil")
	}
}
