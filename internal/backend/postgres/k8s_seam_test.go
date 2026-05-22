package postgres

// k8s_seam_test.go — seam-driven coverage for k8s.go. Uses fake.Clientset for
// the apiserver, the pgConn seam for the in-pod SQL, shrunk poll/timeout vars,
// and a preloaded Ready pod so the full Provision happy path runs in ms.

import (
	"context"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"instant.dev/provisioner/internal/ctxkeys"
)

var nsGroupResource = schema.GroupResource{Group: "", Resource: "namespaces"}

func k8sNotFound(name string) error      { return apierrors.NewNotFound(nsGroupResource, name) }
func k8sAlreadyExists(name string) error { return apierrors.NewAlreadyExists(nsGroupResource, name) }

// shrinkK8sTimers shrinks the pod-ready and namespace-terminate timers so
// timeout branches are reached in milliseconds.
func shrinkK8sTimers(t *testing.T) {
	t.Helper()
	rt, ri := k8sReadyTimeout, k8sReadyInterval
	nt, np := k8sNsTerminateTimeout, k8sNsTerminatePoll
	k8sReadyTimeout, k8sReadyInterval = 80*time.Millisecond, 5*time.Millisecond
	k8sNsTerminateTimeout, k8sNsTerminatePoll = 80*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() {
		k8sReadyTimeout, k8sReadyInterval = rt, ri
		k8sNsTerminateTimeout, k8sNsTerminatePoll = nt, np
	})
}

// readyPod returns a pod in the given namespace with the app=postgres label and
// a PodReady=True condition.
func readyPostgresPod(ns string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-1", Namespace: ns, Labels: map[string]string{"app": "postgres"}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

func TestSizingForTier_AllTiers(t *testing.T) {
	for _, tier := range []string{"anonymous", "hobby", "pro", "team", "growth", "unknown-x"} {
		sz := sizingForTier(tier)
		if sz.qCPURequests == "" {
			t.Errorf("tier %q: empty sizing", tier)
		}
	}
}

func TestK8sRandHex_Success(t *testing.T) {
	s, err := k8sRandHex(16)
	if err != nil || len(s) != 32 {
		t.Errorf("k8sRandHex = %q, %v", s, err)
	}
}

func TestK8sRandHex_RandError(t *testing.T) {
	orig := randRead
	randRead = func([]byte) (int, error) { return 0, errSeam }
	t.Cleanup(func() { randRead = orig })
	if _, err := k8sRandHex(16); err == nil {
		t.Error("expected rand error")
	}
}

func TestBoolPtr_Seam(t *testing.T) {
	if *boolPtr(true) != true || *boolPtr(false) != false {
		t.Error("boolPtr broken")
	}
}

func TestMin_Seam(t *testing.T) {
	if min(2, 5) != 2 || min(5, 2) != 2 {
		t.Error("min broken")
	}
}

func TestPgDataVolumeSource(t *testing.T) {
	if pgDataVolumeSource(tierSizing{pvcGi: 10}).PersistentVolumeClaim == nil {
		t.Error("pvc>0 should use PVC source")
	}
	if pgDataVolumeSource(tierSizing{pvcGi: 0}).EmptyDir == nil {
		t.Error("pvc==0 should use emptyDir")
	}
}

func TestNewK8sDedicatedBackend_BadKubeconfig(t *testing.T) {
	if _, err := NewK8sDedicatedBackend("/nonexistent/kubeconfig", "", "", "", 0); err == nil {
		t.Error("expected build-config error for missing kubeconfig")
	}
}

// minimalKubeconfig writes a syntactically valid kubeconfig pointing at a dummy
// API host. BuildConfigFromFlags + NewForConfig both succeed without contacting
// the server, so newK8sBackend's success + default-fill branches are exercised.
const minimalKubeconfig = `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:6443
  name: test
contexts:
- context:
    cluster: test
    user: test
  name: test
current-context: test
users:
- name: test
  user:
    token: abc
`

func TestNewK8sBackend_Success_FillsDefaults(t *testing.T) {
	dir := t.TempDir()
	kc := dir + "/kubeconfig"
	if err := os.WriteFile(kc, []byte(minimalKubeconfig), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	// Empty storageClass/image + storageSizeGi<=0 → all default-fill branches run.
	b, err := newK8sBackend(kc, "", "", "ext.host", 0)
	if err != nil {
		t.Fatalf("newK8sBackend: %v", err)
	}
	if b.storageClass != "gp3" {
		t.Errorf("storageClass default = %q; want gp3", b.storageClass)
	}
	if b.image != "pgvector/pgvector:pg16" {
		t.Errorf("image default = %q", b.image)
	}
	if b.storageSizeGi != 50 {
		t.Errorf("storageSizeGi default = %d; want 50", b.storageSizeGi)
	}
}

// A kubeconfig with an unknown auth-provider parses fine (BuildConfigFromFlags
// succeeds) but kubernetes.NewForConfig rejects it — covers the NewForConfig
// error branch in newK8sBackend.
const badAuthKubeconfig = `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:6443
  name: test
contexts:
- context:
    cluster: test
    user: test
  name: test
current-context: test
users:
- name: test
  user:
    auth-provider:
      name: this-provider-does-not-exist
`

func TestNewK8sBackend_NewForConfigError(t *testing.T) {
	dir := t.TempDir()
	kc := dir + "/kubeconfig"
	if err := os.WriteFile(kc, []byte(badAuthKubeconfig), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	if _, err := newK8sBackend(kc, "", "", "", 0); err == nil {
		t.Error("expected NewForConfig error for unknown auth-provider")
	}
}

func TestNewK8sBackend_InClusterConfigError(t *testing.T) {
	// Empty kubeconfig → rest.InClusterConfig is used. Outside a pod it fails.
	// Skip if the test happens to run inside a real pod (in-cluster config valid).
	if os.Getenv("KUBERNETES_SERVICE_HOST") != "" {
		t.Skip("running inside a k8s pod — in-cluster config would succeed")
	}
	if _, err := newK8sBackend("", "", "", "", 0); err == nil {
		t.Error("expected in-cluster config error outside a pod")
	}
}

func TestK8sBackend_EnableRouteRegistry(t *testing.T) {
	b := &K8sBackend{}
	b.EnableRouteRegistry(&goredis.Client{}, "")
	if b.routePrefix != "pg_route:" {
		t.Errorf("default prefix = %q", b.routePrefix)
	}
	b.EnableRouteRegistry(&goredis.Client{}, "custom:")
	if b.routePrefix != "custom:" {
		t.Errorf("prefix = %q", b.routePrefix)
	}
}

// Provision happy path with PVC (hobby tier) — exercises every apply* helper,
// waitPodReady success, and initDatabase via the pgConn seam.
func TestK8sBackend_Provision_HappyPath_WithPVC(t *testing.T) {
	shrinkK8sTimers(t)
	const token = "happytoken"
	ns := k8sNsPrefix + token
	cs := fake.NewClientset(readyPostgresPod(ns))
	// Stamp a ClusterIP on the Service when it's created (fake doesn't auto-assign).
	cs.PrependReactor("create", "services", func(a k8stesting.Action) (bool, runtime.Object, error) {
		svc := a.(k8stesting.CreateAction).GetObject().(*corev1.Service)
		svc.Spec.ClusterIP = "10.0.0.5"
		if len(svc.Spec.Ports) > 0 {
			svc.Spec.Ports[0].NodePort = 30111
		}
		return false, nil, nil // let the default reactor store it
	})
	fc := &fakePGConn{}
	withPGXConnect(t, fc, nil)

	b := &K8sBackend{cs: cs, storageClass: "local-path", image: "pgvector/pgvector:pg16", externalHost: "127.0.0.1", storageSizeGi: 50}
	creds, err := b.Provision(context.Background(), token, "hobby", 5)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if creds.DatabaseName != k8sDBName(token) || creds.ProviderResourceID != ns {
		t.Errorf("creds = %+v", creds)
	}
}

// Provision anonymous tier (pvcGi==0 → no PVC, emptyDir) + route registry on.
func TestK8sBackend_Provision_Anonymous_RouteRegistry(t *testing.T) {
	shrinkK8sTimers(t)
	const token = "anontoken"
	ns := k8sNsPrefix + token
	cs := fake.NewClientset(readyPostgresPod(ns))
	cs.PrependReactor("create", "services", func(a k8stesting.Action) (bool, runtime.Object, error) {
		svc := a.(k8stesting.CreateAction).GetObject().(*corev1.Service)
		svc.Spec.ClusterIP = "10.0.0.6"
		if len(svc.Spec.Ports) > 0 {
			svc.Spec.Ports[0].NodePort = 30222
		}
		return false, nil, nil
	})
	fc := &fakePGConn{}
	withPGXConnect(t, fc, nil)

	// rdb pointed at a closed port: Set will fail → exercises the route-register
	// failure (non-fatal) branch without a real Redis.
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	b := &K8sBackend{cs: cs, storageClass: "local-path", externalHost: "proxy.host", rdb: rdb, routePrefix: "pg_route:"}
	ctx := context.WithValue(context.Background(), ctxkeys.TeamIDKey, "team-xyz")
	creds, err := b.Provision(ctx, token, "anonymous", 2)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if creds.URL == "" {
		t.Error("empty URL")
	}
}

func TestK8sBackend_Provision_RandError(t *testing.T) {
	orig := randRead
	randRead = func([]byte) (int, error) { return 0, errSeam }
	t.Cleanup(func() { randRead = orig })
	b := &K8sBackend{cs: fake.NewClientset()}
	if _, err := b.Provision(context.Background(), "t", "hobby", 5); err == nil {
		t.Error("expected rand error from admin pass")
	}
}

// Provision app-pass rand error: first k8sRandHex (admin) succeeds, second
// (app) fails — covers the second rand-error branch (k8s.go:236).
func TestK8sBackend_Provision_AppPassRandError(t *testing.T) {
	orig := randRead
	var n int
	randRead = func(b []byte) (int, error) {
		n++
		if n >= 2 {
			return 0, errSeam
		}
		return orig(b)
	}
	t.Cleanup(func() { randRead = orig })
	b := &K8sBackend{cs: fake.NewClientset()}
	if _, err := b.Provision(context.Background(), "t", "hobby", 5); err == nil {
		t.Error("expected rand error from app pass")
	}
}

// Each apply* step failing must trigger the rollback path (namespace delete +
// wrapped error). Driven via PrependReactor on the matching resource create.
func TestK8sBackend_Provision_RollbackBranches(t *testing.T) {
	cases := []struct {
		name     string
		resource string
	}{
		{"network_policy", "networkpolicies"},
		{"resource_quota", "resourcequotas"},
		{"admin_secret", "secrets"},
		{"pvc", "persistentvolumeclaims"},
		{"deployment", "deployments"},
		{"service", "services"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			shrinkK8sTimers(t)
			cs := fake.NewClientset()
			cs.PrependReactor("create", tc.resource, func(k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, errSeam
			})
			// hobby tier has pvcGi>0 so the PVC step runs.
			b := &K8sBackend{cs: cs, storageClass: "local-path", externalHost: "127.0.0.1", storageSizeGi: 10}
			if _, err := b.Provision(context.Background(), "rb"+tc.name, "hobby", 5); err == nil {
				t.Errorf("expected rollback error when %q create fails", tc.resource)
			}
		})
	}
}

// applyNamespace failing (not via rollback — it's the first step) returns the
// namespace error directly.
func TestK8sBackend_Provision_NamespaceError(t *testing.T) {
	cs := fake.NewClientset()
	cs.PrependReactor("create", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errSeam
	})
	b := &K8sBackend{cs: cs, storageClass: "local-path", externalHost: "127.0.0.1", storageSizeGi: 10}
	if _, err := b.Provision(context.Background(), "nserr", "hobby", 5); err == nil {
		t.Error("expected namespace error")
	}
}

func TestK8sBackend_Provision_WaitReadyTimeout_Rollback(t *testing.T) {
	shrinkK8sTimers(t)
	const token = "neverready"
	cs := fake.NewClientset() // no ready pod → waitPodReady times out
	cs.PrependReactor("create", "services", func(a k8stesting.Action) (bool, runtime.Object, error) {
		svc := a.(k8stesting.CreateAction).GetObject().(*corev1.Service)
		svc.Spec.ClusterIP = "10.0.0.7"
		if len(svc.Spec.Ports) > 0 {
			svc.Spec.Ports[0].NodePort = 30333
		}
		return false, nil, nil
	})
	b := &K8sBackend{cs: cs, storageClass: "local-path", externalHost: "127.0.0.1", storageSizeGi: 10}
	if _, err := b.Provision(context.Background(), token, "anonymous", 2); err == nil {
		t.Error("expected wait-ready timeout error")
	}
}

func TestK8sBackend_Provision_InitDBError_Rollback(t *testing.T) {
	shrinkK8sTimers(t)
	const token = "initfail"
	ns := k8sNsPrefix + token
	cs := fake.NewClientset(readyPostgresPod(ns))
	cs.PrependReactor("create", "services", func(a k8stesting.Action) (bool, runtime.Object, error) {
		svc := a.(k8stesting.CreateAction).GetObject().(*corev1.Service)
		svc.Spec.ClusterIP = "10.0.0.8"
		if len(svc.Spec.Ports) > 0 {
			svc.Spec.Ports[0].NodePort = 30444
		}
		return false, nil, nil
	})
	withPGXConnect(t, nil, errSeam) // initDatabase connect fails
	b := &K8sBackend{cs: cs, storageClass: "local-path", externalHost: "127.0.0.1", storageSizeGi: 10}
	if _, err := b.Provision(context.Background(), token, "anonymous", 2); err == nil {
		t.Error("expected init-database error")
	}
}

func TestK8sBackend_initDatabase_Success(t *testing.T) {
	fc := &fakePGConn{}
	withPGXConnect(t, fc, nil)
	b := &K8sBackend{}
	if err := b.initDatabase(context.Background(), "postgres://a:b@h:5432/postgres?sslmode=disable", "db_x", "usr_x", "p", 5); err != nil {
		t.Fatalf("initDatabase: %v", err)
	}
}

func TestK8sBackend_initDatabase_ConnectError(t *testing.T) {
	withPGXConnect(t, nil, errSeam)
	b := &K8sBackend{}
	if err := b.initDatabase(context.Background(), "dsn", "db_x", "usr_x", "p", 5); err == nil {
		t.Error("expected connect error")
	}
}

func TestK8sBackend_initDatabase_ExecError(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{"CREATE USER": errSeam}}
	withPGXConnect(t, fc, nil)
	b := &K8sBackend{}
	if err := b.initDatabase(context.Background(), "dsn", "db_x", "usr_x", "p", 5); err == nil {
		t.Error("expected exec error")
	}
}

// initDatabase second connection (to the new DB for vector ext) failing is
// non-fatal — covered by returning a conn for the first connect, error for the
// second.
func TestK8sBackend_initDatabase_VectorExtConnFail_NonFatal(t *testing.T) {
	var calls int
	fc := &fakePGConn{}
	withPGXConnectFunc(t, func(ctx context.Context, dsn string) (pgConn, error) {
		calls++
		if calls == 2 {
			return nil, errSeam
		}
		return fc, nil
	})
	b := &K8sBackend{}
	if err := b.initDatabase(context.Background(), "postgres://a@h/postgres?x", "db_x", "usr_x", "p", -1); err != nil {
		t.Errorf("vector-ext connect failure must be non-fatal: %v", err)
	}
}

func TestK8sBackend_waitPodReady_ListError(t *testing.T) {
	shrinkK8sTimers(t)
	cs := fake.NewClientset()
	cs.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errSeam
	})
	b := &K8sBackend{cs: cs}
	if err := b.waitPodReady(context.Background(), "ns", "app=postgres"); err == nil {
		t.Error("expected list error")
	}
}

func TestK8sBackend_waitPodReady_CtxCancel(t *testing.T) {
	rt, ri := k8sReadyTimeout, k8sReadyInterval
	k8sReadyTimeout, k8sReadyInterval = time.Minute, 50*time.Millisecond
	t.Cleanup(func() { k8sReadyTimeout, k8sReadyInterval = rt, ri })
	cs := fake.NewClientset() // pod never ready
	b := &K8sBackend{cs: cs}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := b.waitPodReady(ctx, "ns", "app=postgres"); err == nil {
		t.Error("expected ctx-cancel error")
	}
}

// --- StorageBytes ---

func TestK8sBackend_StorageBytes_Success(t *testing.T) {
	const ns = "instant-customer-sb"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-admin", Namespace: ns},
		Data:       map[string][]byte{"POSTGRES_USER": []byte("pgadmin"), "POSTGRES_PASSWORD": []byte("pw")},
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres", Namespace: ns},
		Spec:       corev1.ServiceSpec{ClusterIP: "10.0.0.9"},
	}
	cs := fake.NewClientset(secret, svc)
	fc := &fakePGConn{scanInt64: 1234}
	withPGXConnect(t, fc, nil)
	b := &K8sBackend{cs: cs}
	got, err := b.StorageBytes(context.Background(), "sb", ns)
	if err != nil || got != 1234 {
		t.Errorf("StorageBytes = %d, %v", got, err)
	}
}

func TestK8sBackend_StorageBytes_LegacyMissingSecret(t *testing.T) {
	cs := fake.NewClientset() // secret not found
	b := &K8sBackend{cs: cs}
	got, err := b.StorageBytes(context.Background(), "tok", "instant-customer-tok")
	if err != nil || got != 0 {
		t.Errorf("missing secret should fail-soft to 0,nil; got %d, %v", got, err)
	}
}

func TestK8sBackend_StorageBytes_MissingService(t *testing.T) {
	const ns = "instant-customer-nosvc"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-admin", Namespace: ns},
		Data:       map[string][]byte{"POSTGRES_USER": []byte("u"), "POSTGRES_PASSWORD": []byte("p")},
	}
	cs := fake.NewClientset(secret) // no service
	b := &K8sBackend{cs: cs}
	got, err := b.StorageBytes(context.Background(), "tok", ns)
	if err != nil || got != 0 {
		t.Errorf("missing service should fail-soft to 0,nil; got %d, %v", got, err)
	}
}

func TestK8sBackend_StorageBytes_DefaultNamespace(t *testing.T) {
	// providerResourceID empty → derive ns from token; missing secret fail-soft.
	cs := fake.NewClientset()
	b := &K8sBackend{cs: cs}
	if _, err := b.StorageBytes(context.Background(), "tok", ""); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// Non-NotFound get errors propagate (they are NOT the legacy fail-soft path).
func TestK8sBackend_StorageBytes_SecretGetError(t *testing.T) {
	cs := fake.NewClientset()
	cs.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errSeam // generic error, not NotFound
	})
	b := &K8sBackend{cs: cs}
	if _, err := b.StorageBytes(context.Background(), "tok", "instant-customer-tok"); err == nil {
		t.Error("expected non-NotFound secret-get error to propagate")
	}
}

func TestK8sBackend_StorageBytes_ServiceGetError(t *testing.T) {
	const ns = "instant-customer-svcerr"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-admin", Namespace: ns},
		Data:       map[string][]byte{"POSTGRES_USER": []byte("u"), "POSTGRES_PASSWORD": []byte("p")},
	}
	cs := fake.NewClientset(secret)
	cs.PrependReactor("get", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errSeam
	})
	b := &K8sBackend{cs: cs}
	if _, err := b.StorageBytes(context.Background(), "tok", ns); err == nil {
		t.Error("expected non-NotFound service-get error to propagate")
	}
}

func TestK8sBackend_StorageBytes_ConnectError_Seam(t *testing.T) {
	const ns = "instant-customer-ce"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-admin", Namespace: ns},
		Data:       map[string][]byte{"POSTGRES_USER": []byte("u"), "POSTGRES_PASSWORD": []byte("p")},
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "postgres", Namespace: ns}, Spec: corev1.ServiceSpec{ClusterIP: "1.2.3.4"}}
	cs := fake.NewClientset(secret, svc)
	withPGXConnect(t, nil, errSeam)
	b := &K8sBackend{cs: cs}
	if _, err := b.StorageBytes(context.Background(), "tok", ns); err == nil {
		t.Error("expected connect error")
	}
}

func TestK8sBackend_StorageBytes_AllCandidatesMiss(t *testing.T) {
	const ns = "instant-customer-miss"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-admin", Namespace: ns},
		Data:       map[string][]byte{"POSTGRES_USER": []byte("u"), "POSTGRES_PASSWORD": []byte("p")},
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "postgres", Namespace: ns}, Spec: corev1.ServiceSpec{ClusterIP: "1.2.3.4"}}
	cs := fake.NewClientset(secret, svc)
	fc := &fakePGConn{queryRowErr: errSeam}
	withPGXConnect(t, fc, nil)
	b := &K8sBackend{cs: cs}
	if _, err := b.StorageBytes(context.Background(), "tok", ns); err == nil {
		t.Error("expected all-candidates-miss error")
	}
}

// --- Deprovision ---

func TestK8sBackend_Deprovision_Success(t *testing.T) {
	const ns = "instant-customer-dp"
	nsObj := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	cs := fake.NewClientset(nsObj)
	b := &K8sBackend{cs: cs}
	if err := b.Deprovision(context.Background(), "dp", ns); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
}

func TestK8sBackend_Deprovision_AlreadyGone(t *testing.T) {
	cs := fake.NewClientset() // namespace not found → idempotent success
	b := &K8sBackend{cs: cs}
	if err := b.Deprovision(context.Background(), "dp", "instant-customer-gone"); err != nil {
		t.Errorf("already-gone should be success: %v", err)
	}
}

func TestK8sBackend_Deprovision_DeleteError_Seam(t *testing.T) {
	cs := fake.NewClientset()
	cs.PrependReactor("delete", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errSeam
	})
	b := &K8sBackend{cs: cs}
	if err := b.Deprovision(context.Background(), "dp", "instant-customer-x"); err == nil {
		t.Error("expected delete error")
	}
}

func TestK8sBackend_Deprovision_RouteUnregister(t *testing.T) {
	const ns = "instant-customer-route"
	cs := fake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"}) // Del fails, non-fatal
	b := &K8sBackend{cs: cs, rdb: rdb, routePrefix: "pg_route:"}
	if err := b.Deprovision(context.Background(), "route", ns); err != nil {
		t.Errorf("route-unregister failure must be non-fatal: %v", err)
	}
}

func TestK8sBackend_Deprovision_DefaultNamespace(t *testing.T) {
	cs := fake.NewClientset()
	b := &K8sBackend{cs: cs}
	if err := b.Deprovision(context.Background(), "tok", ""); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- Regrade ---

func k8sRegradeFixture(ns string) (*corev1.Secret, *corev1.Service) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-admin", Namespace: ns},
		Data:       map[string][]byte{"POSTGRES_USER": []byte("u"), "POSTGRES_PASSWORD": []byte("p")},
	}
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "postgres", Namespace: ns}, Spec: corev1.ServiceSpec{ClusterIP: "1.2.3.4"}}
	return secret, svc
}

func TestK8sBackend_Regrade_Success(t *testing.T) {
	const ns = "instant-customer-rg"
	secret, svc := k8sRegradeFixture(ns)
	cs := fake.NewClientset(secret, svc)
	fc := &fakePGConn{}
	withPGXConnect(t, fc, nil)
	b := &K8sBackend{cs: cs}
	res, err := b.Regrade(context.Background(), "rg", ns, 20)
	if err != nil || !res.Applied || res.AppliedConnLimit != 20 {
		t.Errorf("Regrade = %+v, %v", res, err)
	}
}

func TestK8sBackend_Regrade_MissingSecret_Skip(t *testing.T) {
	cs := fake.NewClientset()
	b := &K8sBackend{cs: cs}
	res, err := b.Regrade(context.Background(), "tok", "instant-customer-tok", 5)
	if err != nil || res.Applied {
		t.Errorf("missing secret should skip without error; got %+v, %v", res, err)
	}
}

func TestK8sBackend_Regrade_MissingService_Skip(t *testing.T) {
	const ns = "instant-customer-rgnosvc"
	secret, _ := k8sRegradeFixture(ns)
	cs := fake.NewClientset(secret)
	b := &K8sBackend{cs: cs}
	res, err := b.Regrade(context.Background(), "tok", ns, 5)
	if err != nil || res.Applied {
		t.Errorf("missing service should skip; got %+v, %v", res, err)
	}
}

// Non-NotFound get errors → skip with a "not reachable" reason (no error).
func TestK8sBackend_Regrade_SecretGetError_Skip(t *testing.T) {
	cs := fake.NewClientset()
	cs.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errSeam
	})
	b := &K8sBackend{cs: cs}
	res, err := b.Regrade(context.Background(), "tok", "instant-customer-tok", 5)
	if err != nil || res.Applied {
		t.Errorf("secret-get error should skip without error; got %+v, %v", res, err)
	}
}

func TestK8sBackend_Regrade_ServiceGetError_Skip(t *testing.T) {
	const ns = "instant-customer-rgsvcerr"
	secret, _ := k8sRegradeFixture(ns)
	cs := fake.NewClientset(secret)
	cs.PrependReactor("get", "services", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errSeam
	})
	b := &K8sBackend{cs: cs}
	res, err := b.Regrade(context.Background(), "tok", ns, 5)
	if err != nil || res.Applied {
		t.Errorf("service-get error should skip without error; got %+v, %v", res, err)
	}
}

func TestK8sBackend_Regrade_ConnectError_Skip(t *testing.T) {
	const ns = "instant-customer-rgconn"
	secret, svc := k8sRegradeFixture(ns)
	cs := fake.NewClientset(secret, svc)
	withPGXConnect(t, nil, errSeam)
	b := &K8sBackend{cs: cs}
	res, err := b.Regrade(context.Background(), "tok", ns, 5)
	if err != nil || res.Applied {
		t.Errorf("connect error should skip; got %+v, %v", res, err)
	}
}

func TestK8sBackend_Regrade_AlterRoleAllMiss_Skip(t *testing.T) {
	const ns = "instant-customer-rgalter"
	secret, svc := k8sRegradeFixture(ns)
	cs := fake.NewClientset(secret, svc)
	fc := &fakePGConn{execErrFor: map[string]error{"ALTER ROLE": errSeam}}
	withPGXConnect(t, fc, nil)
	b := &K8sBackend{cs: cs}
	res, err := b.Regrade(context.Background(), "tok", ns, 5)
	if err != nil || res.Applied {
		t.Errorf("alter-role miss should skip; got %+v, %v", res, err)
	}
}

func TestK8sBackend_Regrade_DefaultNamespace(t *testing.T) {
	cs := fake.NewClientset() // missing secret → skip path, ns derived from token
	b := &K8sBackend{cs: cs}
	if _, err := b.Regrade(context.Background(), "tok", "", 5); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- applyNamespace edge paths ---

func TestApplyNamespace_AlreadyExists_NotTerminating(t *testing.T) {
	const ns = "instant-customer-exists"
	existing := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	}
	cs := fake.NewClientset(existing)
	b := &K8sBackend{cs: cs}
	if err := b.applyNamespace(context.Background(), ns); err == nil {
		t.Error("expected AlreadyExists error surfaced for active namespace")
	}
}

func TestApplyNamespace_Terminating_TimesOut(t *testing.T) {
	shrinkK8sTimers(t)
	const ns = "instant-customer-term"
	existing := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceTerminating},
	}
	cs := fake.NewClientset(existing)
	b := &K8sBackend{cs: cs}
	if err := b.applyNamespace(context.Background(), ns); err == nil {
		t.Error("expected still-terminating timeout error")
	}
}

// Terminating namespace that disappears (Get → NotFound) on the next poll →
// applyNamespace recreates it successfully. Covers the recreate-success branch.
func TestApplyNamespace_Terminating_RecreatesAfterGone(t *testing.T) {
	shrinkK8sTimers(t)
	const ns = "instant-customer-recreate"
	existing := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceTerminating},
	}
	cs := fake.NewClientset()

	var gets int
	cs.PrependReactor("get", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		gets++
		if gets == 1 {
			// First Get (AlreadyExists branch) sees the terminating namespace.
			return true, existing, nil
		}
		// Subsequent Get (poll loop) → NotFound so it recreates.
		return true, nil, k8sNotFound(ns)
	})
	var creates int
	cs.PrependReactor("create", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		creates++
		if creates == 1 {
			// First create → AlreadyExists so we enter the terminating path.
			return true, nil, k8sAlreadyExists(ns)
		}
		// Recreate after gone → success (reactor fully handles it).
		return true, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, nil
	})

	b := &K8sBackend{cs: cs}
	if err := b.applyNamespace(context.Background(), ns); err != nil {
		t.Errorf("recreate-after-gone should succeed: %v", err)
	}
}

func TestApplyNamespace_CreateError(t *testing.T) {
	cs := fake.NewClientset()
	cs.PrependReactor("create", "namespaces", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errSeam
	})
	b := &K8sBackend{cs: cs}
	if err := b.applyNamespace(context.Background(), "instant-customer-cerr"); err == nil {
		t.Error("expected create error")
	}
}
