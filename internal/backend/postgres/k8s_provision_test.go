package postgres

// k8s_provision_test.go — coverage for K8sBackend.Provision orchestration.
//
// Strategy: a PrependReactor injects a Pod with PodReady=true into the fake
// clientset's response to the waitPodReady LIST, so the function can progress
// past the readiness wait. The subsequent initDatabase call dials the fake
// Service's ClusterIP (empty), so pgx.Connect fails fast and Provision rolls
// back — but the orchestration path (apply* helpers, the wait loop, route
// registry branches) is now covered.

import (
	"context"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// preloadReadyPod inserts a Pod with the app=postgres label and Ready=True
// status condition into the fake tracker BEFORE Provision runs. waitPodReady's
// first List call returns it and the loop exits immediately.
//
// Pod must carry the label selector that K8sBackend.waitPodReady passes
// (app=postgres) so the fake tracker's label filter includes it in the result.
func preloadReadyPod(t *testing.T, cs *fake.Clientset, ns string) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "postgres-ready",
			Namespace: ns,
			Labels:    map[string]string{"app": "postgres"},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	if _, err := cs.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("preload ready pod: %v", err)
	}
}

// TestK8sBackend_Provision_RollsBack_OnInitDatabaseFailure drives Provision
// end-to-end with a fake clientset; initDatabase will fail because the fake
// Service has no real Postgres on its ClusterIP. The orchestration up to that
// point is exercised, then rollback fires.
func TestK8sBackend_Provision_RollsBack_OnInitDatabaseFailure(t *testing.T) {
	cs := fake.NewSimpleClientset()
	ns := k8sNsPrefix + "abc123"
	preloadReadyPod(t, cs, ns)

	b := &K8sBackend{
		cs:            cs,
		storageClass:  "do-block-storage",
		image:         "pgvector/pgvector:pg16",
		externalHost:  "127.0.0.1",
		storageSizeGi: 50,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := b.Provision(ctx, "abc123", "hobby", 5)
	if err == nil {
		t.Fatal("Provision returned nil; expected init-database failure → rollback")
	}
	// Sanity: rollback should have removed the namespace.
	if _, getErr := cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{}); getErr == nil {
		t.Errorf("namespace %q still present after rollback; want gone", ns)
	}
}

// TestK8sBackend_Provision_AnonymousSkipsPVC drives the pvcGi==0 emptyDir
// branch — anonymous tier skips applyPVC, so the function takes the
// "if sz.pvcGi > 0" false path.
func TestK8sBackend_Provision_AnonymousSkipsPVC(t *testing.T) {
	cs := fake.NewSimpleClientset()
	ns := k8sNsPrefix + "anonprov"
	preloadReadyPod(t, cs, ns)

	b := &K8sBackend{cs: cs, image: "img", externalHost: "h"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, _ = b.Provision(ctx, "anonprov", "anonymous", -1)
	// PVC must not have been created (anonymous → pvcGi=0 → emptyDir).
	pvcs, _ := cs.CoreV1().PersistentVolumeClaims(ns).List(ctx, metav1.ListOptions{})
	if len(pvcs.Items) != 0 {
		t.Errorf("anonymous tier created %d PVCs; want 0 (emptyDir)", len(pvcs.Items))
	}
}

// testRedisAddr returns the local Redis address for route-registry coverage,
// or "" if no Redis is reachable.
func testRedisAddr() string {
	if v := os.Getenv("TEST_REDIS_ADDR"); v != "" {
		return v
	}
	return "localhost:6379"
}

// TestK8sBackend_Provision_WithRouteRegistry covers the rdb-not-nil branch of
// Provision — the Redis route SET happens before initDatabase fails. We use
// the live test-redis on :6379 (or TEST_REDIS_ADDR override).
func TestK8sBackend_Provision_WithRouteRegistry(t *testing.T) {
	addr := testRedisAddr()
	rdb := goredis.NewClient(&goredis.Options{Addr: addr, DialTimeout: 2 * time.Second})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("redis not reachable at %s: %v", addr, err)
	}
	defer rdb.Close() //nolint:errcheck

	cs := fake.NewSimpleClientset()
	ns := k8sNsPrefix + "rt123"
	preloadReadyPod(t, cs, ns)

	b := &K8sBackend{cs: cs, image: "img", externalHost: "127.0.0.1"}
	b.EnableRouteRegistry(rdb, "test_pg_route:")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, _ = b.Provision(ctx, "rt123", "hobby", 4)
	// After Provision the route Set fired; even though the lifecycle ended
	// in rollback, the route may have been left behind in Redis. Deprovision
	// (which iterates legacyK8sDBNames) cleans it up; exercise that branch.
	if err := b.Deprovision(ctx, "rt123", ns); err != nil {
		t.Errorf("Deprovision with route registry: %v", err)
	}
}
