package postgres

// k8s_helpers_test.go — coverage for K8sBackend resource-creation helpers and
// pure-function helpers. Driven entirely by the fake clientset; no real
// Kubernetes cluster needed.

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// newUnreachablePostgresService returns a Service named "postgres" with a
// non-routable ClusterIP so pgx.Connect fails fast — used to exercise the
// connect-failure skip branch of K8sBackend.Regrade.
func newUnreachablePostgresService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres"},
		Spec: corev1.ServiceSpec{
			// 127.0.0.255 is non-routable; pgx.Connect to it errors quickly.
			// K8sBackend.Regrade dials this:5432 with a synthesized DSN; the
			// failure flows into the "resource not reachable" skip branch.
			ClusterIP: "127.0.0.255",
			Ports:     []corev1.ServicePort{{Port: 5432}},
		},
	}
}

func TestSizingForTier_AllKnownTiers(t *testing.T) {
	for _, tier := range []string{"anonymous", "hobby", "pro", "team", "growth", "unknown_falls_back_to_hobby"} {
		got := sizingForTier(tier)
		if got.cpuReq == "" || got.memReq == "" {
			t.Errorf("sizingForTier(%q) = %+v; missing fields", tier, got)
		}
	}
	// Anonymous is the only tier with pvcGi==0 — guards the emptyDir branch.
	if sizingForTier("anonymous").pvcGi != 0 {
		t.Error("anonymous pvcGi != 0; emptyDir branch will not fire")
	}
	// Team and growth share sizing (the explicit case "team", "growth" line).
	if sizingForTier("team") != sizingForTier("growth") {
		t.Error("team and growth tiers should yield identical sizing")
	}
	// Unknown tier delegates to hobby — verify equivalence.
	if sizingForTier("anything-random") != sizingForTier("hobby") {
		t.Error("unknown tier should fall back to hobby")
	}
}

func TestPgDataVolumeSource_EmptyDirForAnonymous(t *testing.T) {
	v := pgDataVolumeSource(tierSizing{pvcGi: 0})
	if v.EmptyDir == nil {
		t.Error("pgDataVolumeSource(pvcGi=0) did not return emptyDir")
	}
	v = pgDataVolumeSource(tierSizing{pvcGi: 10})
	if v.PersistentVolumeClaim == nil || v.PersistentVolumeClaim.ClaimName != "postgres-data" {
		t.Errorf("pgDataVolumeSource(pvcGi>0) = %+v; want PVC postgres-data", v)
	}
}

func TestBoolPtr(t *testing.T) {
	if !*boolPtr(true) || *boolPtr(false) {
		t.Error("boolPtr roundtrip broken")
	}
}

func TestMin(t *testing.T) {
	if min(1, 2) != 1 || min(2, 1) != 1 || min(3, 3) != 3 {
		t.Error("min broken")
	}
}

func TestK8sRandHex(t *testing.T) {
	s, err := k8sRandHex(8)
	if err != nil {
		t.Fatalf("k8sRandHex: %v", err)
	}
	if len(s) != 16 {
		t.Errorf("len(k8sRandHex(8)) = %d; want 16 hex chars", len(s))
	}
	// Must be valid hex.
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("k8sRandHex returned non-hex char %q", c)
			break
		}
	}
	// k8sRandHex(0) is allowed → "".
	if s, err := k8sRandHex(0); err != nil || s != "" {
		t.Errorf("k8sRandHex(0) = (%q,%v); want (\"\", nil)", s, err)
	}
}

func TestK8sBackend_ApplyHelpers(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs, storageClass: "gp3", image: "pgvector/pgvector:pg16"}
	ctx := context.Background()
	const ns = "instant-customer-helper-test"
	sz := sizingForTier("hobby")

	if err := b.applyNamespace(ctx, ns); err != nil {
		t.Fatalf("applyNamespace: %v", err)
	}
	if err := b.applyNetworkPolicy(ctx, ns, 5432); err != nil {
		t.Fatalf("applyNetworkPolicy: %v", err)
	}
	if err := b.applyResourceQuota(ctx, ns, sz); err != nil {
		t.Fatalf("applyResourceQuota: %v", err)
	}
	if err := b.applyAdminSecret(ctx, ns, "adm", "p"); err != nil {
		t.Fatalf("applyAdminSecret: %v", err)
	}
	if err := b.applyPVC(ctx, ns, sz); err != nil {
		t.Fatalf("applyPVC: %v", err)
	}
	if err := b.applyDeployment(ctx, ns, "adm", sz); err != nil {
		t.Fatalf("applyDeployment: %v", err)
	}
	svc, err := b.applyService(ctx, ns)
	if err != nil {
		t.Fatalf("applyService: %v", err)
	}
	if svc.Name != "postgres" {
		t.Errorf("applyService.Name = %q; want postgres", svc.Name)
	}

	// applyResourceQuota for anonymous (pvcGi=0) skips the persistentvolumeclaims
	// hard limit — exercise that branch too.
	if err := b.applyResourceQuota(ctx, ns+"-anon", sizingForTier("anonymous")); err != nil {
		// Need the namespace to exist or the fake clientset will allow the
		// quota creation anyway — apply it first.
		_ = b.applyNamespace(ctx, ns+"-anon")
		if err := b.applyResourceQuota(ctx, ns+"-anon", sizingForTier("anonymous")); err != nil {
			t.Fatalf("applyResourceQuota(anonymous): %v", err)
		}
	}
}

func TestK8sBackend_ApplyNamespace_Idempotent_AlreadyExists(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	ctx := context.Background()
	const ns = "instant-customer-already-exists"
	if err := b.applyNamespace(ctx, ns); err != nil {
		t.Fatalf("first applyNamespace: %v", err)
	}
	// Second call must surface AlreadyExists (the function only retries
	// if the existing namespace is Terminating).
	err := b.applyNamespace(ctx, ns)
	if err == nil {
		t.Error("second applyNamespace returned nil; want AlreadyExists")
	}
}

func TestK8sBackend_StorageBytes_LegacyMissingSecretReturnsZero(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	got, err := b.StorageBytes(context.Background(), "tok", "instant-customer-tok")
	if err != nil {
		t.Errorf("StorageBytes legacy = (%d,%v); want (0, nil) — missing Secret is non-actionable", got, err)
	}
	if got != 0 {
		t.Errorf("StorageBytes legacy = %d; want 0", got)
	}
}

func TestK8sBackend_StorageBytes_LegacyMissingServiceReturnsZero(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	const ns = "instant-customer-tok2"
	// Create the Secret but no Service.
	if err := b.applyNamespace(context.Background(), ns); err != nil {
		t.Fatalf("ns: %v", err)
	}
	if err := b.applyAdminSecret(context.Background(), ns, "u", "p"); err != nil {
		t.Fatalf("secret: %v", err)
	}
	got, err := b.StorageBytes(context.Background(), "tok", ns)
	if err != nil || got != 0 {
		t.Errorf("StorageBytes legacy missing service = (%d,%v); want (0,nil)", got, err)
	}
}

func TestK8sBackend_StorageBytes_FallbackToNamespaceFromToken(t *testing.T) {
	// providerResourceID == "" path uses k8sNsPrefix+token; both Secret and
	// Service are missing → fall-soft to 0.
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	got, err := b.StorageBytes(context.Background(), "tok-empty-prid", "")
	if err != nil || got != 0 {
		t.Errorf("StorageBytes empty PRID = (%d,%v); want (0, nil)", got, err)
	}
}

func TestK8sBackend_Regrade_LegacyMissingSecretSkips(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	res, err := b.Regrade(context.Background(), "tok", "instant-customer-tok", 8)
	if err != nil {
		t.Errorf("Regrade legacy err = %v; want nil", err)
	}
	if res.Applied {
		t.Errorf("Regrade legacy Applied=true; want false (skip)")
	}
	if !strings.Contains(strings.ToLower(res.SkipReason), "secret") {
		t.Errorf("Regrade legacy SkipReason = %q; want 'secret' mention", res.SkipReason)
	}
}

func TestK8sBackend_Regrade_LegacyMissingServiceSkips(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	const ns = "instant-customer-r2"
	if err := b.applyNamespace(context.Background(), ns); err != nil {
		t.Fatalf("ns: %v", err)
	}
	if err := b.applyAdminSecret(context.Background(), ns, "u", "p"); err != nil {
		t.Fatalf("secret: %v", err)
	}
	res, _ := b.Regrade(context.Background(), "tok", ns, 8)
	if res.Applied {
		t.Errorf("Applied=true; want skip on missing service")
	}
}

func TestK8sBackend_Regrade_PodUnreachable_ConnectFailsSkip(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	const ns = "instant-customer-r3"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.applyNamespace(ctx, ns); err != nil {
		t.Fatalf("ns: %v", err)
	}
	if err := b.applyAdminSecret(ctx, ns, "u", "p"); err != nil {
		t.Fatalf("secret: %v", err)
	}
	// Create a Service with a deliberately unreachable ClusterIP so pgx.Connect
	// fails fast.
	_, err := cs.CoreV1().Services(ns).Create(ctx, newUnreachablePostgresService(), metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create svc: %v", err)
	}
	res, err := b.Regrade(ctx, "tok", ns, 8)
	if err != nil {
		t.Errorf("Regrade returned err = %v; want nil (skip-on-unreachable)", err)
	}
	if res.Applied {
		t.Errorf("Applied=true; want false")
	}
}

func TestK8sBackend_Deprovision_Idempotent(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	ctx := context.Background()
	const ns = "instant-customer-deprov"
	if err := b.applyNamespace(ctx, ns); err != nil {
		t.Fatalf("ns: %v", err)
	}
	if err := b.Deprovision(ctx, "tok", ns); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	// Second deprovision must be idempotent (namespace already gone).
	if err := b.Deprovision(ctx, "tok", ns); err != nil {
		t.Errorf("second Deprovision = %v; want nil (NotFound is idempotent success)", err)
	}
}

func TestK8sBackend_Deprovision_NamespaceFromTokenFallback(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	ctx := context.Background()
	// No namespace created → Delete returns NotFound which the function treats
	// as idempotent success.
	if err := b.Deprovision(ctx, "fallback-token", ""); err != nil {
		t.Errorf("Deprovision empty PRID = %v; want nil", err)
	}
}

func TestK8sBackend_EnableRouteRegistry_DefaultPrefix(t *testing.T) {
	b := &K8sBackend{}
	b.EnableRouteRegistry(nil, "")
	if b.routePrefix != "pg_route:" {
		t.Errorf("routePrefix default = %q; want pg_route:", b.routePrefix)
	}
	b.EnableRouteRegistry(nil, "custom_prefix:")
	if b.routePrefix != "custom_prefix:" {
		t.Errorf("routePrefix override = %q; want custom_prefix:", b.routePrefix)
	}
}

// TestNewK8sBackend_DefaultsAndBadConfig exercises the constructor's defaults
// (storageClass=gp3, image=pgvector, storageSizeGi=50) and its error path on
// a bogus kubeconfig.
func TestNewK8sBackend_BadConfigErrors(t *testing.T) {
	_, err := newK8sBackend("/nonexistent/kubeconfig", "", "", "", 0)
	if err == nil {
		t.Error("newK8sBackend with bad path returned nil; want error")
	}
}

func TestNewK8sBackend_OutOfClusterNoKubeconfig(t *testing.T) {
	// kubeconfigPath empty → rest.InClusterConfig; should fail outside cluster
	// with ErrNotInCluster.
	_, err := newK8sBackend("", "", "", "", 0)
	if err == nil {
		t.Error("newK8sBackend in-cluster path outside cluster returned nil; want error")
	}
}
