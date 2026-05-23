package postgres

// k8s_orchestration_test.go — coverage for K8sBackend orchestration error and
// retry branches that the happy-path / fake-clientset tests don't reach:
//   - applyNamespace: non-AlreadyExists create error, Terminating-namespace
//     wait+recreate, and the still-terminating timeout.
//   - waitPodReady: List error and ctx-cancellation while not ready.
//   - newK8sBackend: the post-config defaults block (storageClass/image/sizeGi)
//     reached via a syntactically valid kubeconfig.
//
// All driven by the fake clientset + PrependReactors; no real cluster needed.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// newClosedRedis returns a goredis client pointed at a port with no listener so
// every command (including Del) errors quickly — used to exercise the non-fatal
// route-unregister warn branch of Deprovision.
func newClosedRedis(t *testing.T) *goredis.Client {
	t.Helper()
	cl := goredis.NewClient(&goredis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: time.Second,
	})
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

// TestApplyNamespace_NonAlreadyExistsError covers the branch where Create fails
// with an error that is NOT AlreadyExists — the function surfaces it verbatim.
func TestApplyNamespace_NonAlreadyExistsError(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "namespaces", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver boom")
	})
	b := &K8sBackend{cs: cs}
	err := b.applyNamespace(context.Background(), "instant-customer-boom")
	if err == nil || err.Error() != "apiserver boom" {
		t.Fatalf("applyNamespace err = %v; want raw 'apiserver boom'", err)
	}
}

// TestApplyNamespace_TerminatingThenRecreated covers the Terminating-namespace
// wait loop: the first Create returns AlreadyExists, the Get reports
// Terminating, and a reactor then makes the next Get return NotFound so the
// recreate path fires and succeeds.
func TestApplyNamespace_TerminatingThenRecreated(t *testing.T) {
	cs := fake.NewSimpleClientset()
	const ns = "instant-customer-terminating"

	createCalls := 0
	cs.PrependReactor("create", "namespaces", func(clienttesting.Action) (bool, runtime.Object, error) {
		createCalls++
		if createCalls == 1 {
			// First create → AlreadyExists so we enter the terminating branch.
			return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "namespaces"}, ns)
		}
		// Recreate after termination → succeed.
		return true, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, nil
	})
	getCalls := 0
	cs.PrependReactor("get", "namespaces", func(clienttesting.Action) (bool, runtime.Object, error) {
		getCalls++
		if getCalls == 1 {
			// First Get (right after AlreadyExists) → Terminating.
			return true, &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: ns},
				Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceTerminating},
			}, nil
		}
		// Subsequent Get (inside the loop) → NotFound → recreate.
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "namespaces"}, ns)
	})

	b := &K8sBackend{cs: cs}
	// k8sReadyInterval-style wait inside is 3s; give the loop room.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.applyNamespace(ctx, ns); err != nil {
		t.Fatalf("applyNamespace(terminating→recreated): %v", err)
	}
}

// TestApplyNamespace_TerminatingCtxCanceled covers the ctx.Done() arm of the
// terminating wait loop: the namespace stays Terminating and the context is
// canceled while the loop sleeps.
func TestApplyNamespace_TerminatingCtxCanceled(t *testing.T) {
	cs := fake.NewSimpleClientset()
	const ns = "instant-customer-term-cancel"
	cs.PrependReactor("create", "namespaces", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewAlreadyExists(schema.GroupResource{Resource: "namespaces"}, ns)
	})
	cs.PrependReactor("get", "namespaces", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: ns},
			Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceTerminating},
		}, nil
	})
	b := &K8sBackend{cs: cs}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel; the loop's select hits ctx.Done() on the first iteration
	err := b.applyNamespace(ctx, ns)
	if err == nil {
		t.Fatal("applyNamespace with canceled ctx returned nil; want ctx error")
	}
}

// TestWaitPodReady_ListError covers the List-error branch of waitPodReady.
func TestWaitPodReady_ListError(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("list pods failed")
	})
	b := &K8sBackend{cs: cs}
	err := b.waitPodReady(context.Background(), "instant-customer-x", "app=postgres")
	if err == nil || err.Error() != "list pods failed" {
		t.Fatalf("waitPodReady err = %v; want 'list pods failed'", err)
	}
}

// TestWaitPodReady_CtxCanceledWhileNotReady covers the ctx.Done() arm: the pod
// list returns a not-ready pod, so the loop sleeps and the canceled context
// fires.
func TestWaitPodReady_CtxCanceledWhileNotReady(t *testing.T) {
	cs := fake.NewSimpleClientset()
	const ns = "instant-customer-notready"
	// A pod that exists but is NOT ready (no PodReady=True condition).
	_, _ = cs.CoreV1().Pods(ns).Create(context.Background(), &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-0", Namespace: ns, Labels: map[string]string{"app": "postgres"}},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse}},
		},
	}, metav1.CreateOptions{})

	b := &K8sBackend{cs: cs}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel: the not-ready loop's select hits ctx.Done()
	err := b.waitPodReady(ctx, ns, "app=postgres")
	if err == nil {
		t.Fatal("waitPodReady with canceled ctx returned nil; want ctx error")
	}
}

// TestK8sBackend_Provision_RollbackPerStep drives Provision to a failure at each
// apply step in turn via a PrependReactor, exercising every rollback branch
// (network policy / resource quota / admin secret / pvc / deployment / service)
// and asserting the namespace is torn down. The first step (namespace) has no
// rollback — a namespace failure returns directly — and is covered separately.
func TestK8sBackend_Provision_RollbackPerStep(t *testing.T) {
	type step struct {
		verb     string
		resource string
		wantMsg  string
	}
	steps := []step{
		{"create", "networkpolicies", "network policy"},
		{"create", "resourcequotas", "resource quota"},
		{"create", "secrets", "admin secret"},
		{"create", "persistentvolumeclaims", "pvc"},
		{"create", "deployments", "deployment"},
		{"create", "services", "service"},
	}
	for _, s := range steps {
		s := s
		t.Run(s.resource, func(t *testing.T) {
			cs := fake.NewSimpleClientset()
			ns := k8sNsPrefix + "rb" + s.resource
			cs.PrependReactor(s.verb, s.resource, func(clienttesting.Action) (bool, runtime.Object, error) {
				return true, nil, errors.New("injected " + s.resource + " failure")
			})
			b := &K8sBackend{cs: cs, image: "img", externalHost: "h", storageClass: "sc", storageSizeGi: 10}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			// "hobby" tier has pvcGi > 0 so the pvc step is reached.
			_, err := b.Provision(ctx, "rb"+s.resource, "hobby", 4)
			if err == nil {
				t.Fatalf("Provision expected to fail at %s step", s.resource)
			}
			if !contains(err.Error(), s.wantMsg) {
				t.Errorf("err = %v; want mention of %q", err, s.wantMsg)
			}
			// Rollback deletes the namespace (best-effort). For the network-policy
			// step onward, rollback() runs; assert the namespace is gone.
			if _, getErr := cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{}); getErr == nil {
				t.Errorf("namespace %q still present after rollback at %s", ns, s.resource)
			}
		})
	}
}

// TestK8sBackend_Provision_NamespaceStepFails covers the first-step (namespace)
// failure: Provision returns "namespace: ..." without invoking rollback.
func TestK8sBackend_Provision_NamespaceStepFails(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "namespaces", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("ns boom")
	})
	b := &K8sBackend{cs: cs, image: "img", externalHost: "h"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := b.Provision(ctx, "nsfail", "hobby", 4)
	if err == nil || !contains(err.Error(), "namespace") {
		t.Fatalf("Provision err = %v; want 'namespace' wrap", err)
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOfStr(s, sub) >= 0)
}

func indexOfStr(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

// TestK8sBackend_StorageBytes_GetSecretError covers the non-NotFound Get-secret
// error branch (335): a transient apiserver error propagates, not fail-soft.
func TestK8sBackend_StorageBytes_GetSecretError(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("get", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver unavailable")
	})
	b := &K8sBackend{cs: cs}
	if _, err := b.StorageBytes(context.Background(), "tok", "instant-customer-tok"); err == nil ||
		!contains(err.Error(), "get secret") {
		t.Fatalf("StorageBytes get-secret err = %v; want 'get secret' wrap", err)
	}
}

// TestK8sBackend_StorageBytes_GetServiceError covers the non-NotFound Get-service
// error branch (344).
func TestK8sBackend_StorageBytes_GetServiceError(t *testing.T) {
	cs := fake.NewSimpleClientset()
	const ns = "instant-customer-svcerr"
	b := &K8sBackend{cs: cs}
	if err := b.applyNamespace(context.Background(), ns); err != nil {
		t.Fatalf("ns: %v", err)
	}
	if _, err := b.cs.CoreV1().Secrets(ns).Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-admin"},
		Data:       map[string][]byte{"POSTGRES_USER": []byte("u"), "POSTGRES_PASSWORD": []byte("p")},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("secret: %v", err)
	}
	cs.PrependReactor("get", "services", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("svc apiserver boom")
	})
	if _, err := b.StorageBytes(context.Background(), "tok", ns); err == nil ||
		!contains(err.Error(), "get service") {
		t.Fatalf("StorageBytes get-service err = %v; want 'get service' wrap", err)
	}
}

// TestK8sBackend_StorageBytes_ConnectError covers the pgx.Connect failure branch
// (352): a valid Secret + a Service with an unreachable ClusterIP.
func TestK8sBackend_StorageBytes_ConnectError(t *testing.T) {
	cs := fake.NewSimpleClientset()
	const ns = "instant-customer-sbconn"
	b := &K8sBackend{cs: cs}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.applyNamespace(ctx, ns); err != nil {
		t.Fatalf("ns: %v", err)
	}
	if _, err := b.cs.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-admin"},
		Data:       map[string][]byte{"POSTGRES_USER": []byte("u"), "POSTGRES_PASSWORD": []byte("p")},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("secret: %v", err)
	}
	if _, err := cs.CoreV1().Services(ns).Create(ctx, newUnreachablePostgresService(), metav1.CreateOptions{}); err != nil {
		t.Fatalf("svc: %v", err)
	}
	if _, err := b.StorageBytes(ctx, "tok", ns); err == nil || !contains(err.Error(), "connect") {
		t.Fatalf("StorageBytes connect err = %v; want 'connect' wrap", err)
	}
}

// TestK8sBackend_Deprovision_DeleteError covers the non-NotFound namespace-delete
// error branch (389): a transient apiserver error propagates.
func TestK8sBackend_Deprovision_DeleteError(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("delete", "namespaces", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("delete apiserver boom")
	})
	b := &K8sBackend{cs: cs}
	if err := b.Deprovision(context.Background(), "tok", "instant-customer-tok"); err == nil ||
		!contains(err.Error(), "delete namespace") {
		t.Fatalf("Deprovision delete err = %v; want 'delete namespace' wrap", err)
	}
}

// TestK8sBackend_Deprovision_RouteDelError covers the route-unregister branch
// (402): rdb is set but Del fails (closed redis), logged and ignored — the
// overall Deprovision still returns nil.
func TestK8sBackend_Deprovision_RouteDelError(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	// Point at a redis that immediately fails so Del errors into the warn branch.
	rdb := newClosedRedis(t)
	b.EnableRouteRegistry(rdb, "test_route:")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Namespace doesn't exist (NotFound → treated as success), then route Del runs.
	if err := b.Deprovision(ctx, "tok", "instant-customer-rtdel"); err != nil {
		t.Errorf("Deprovision with failing route Del = %v; want nil (Del error is non-fatal)", err)
	}
}

// TestK8sBackend_Regrade_EmptyPRID covers the empty-providerResourceID branch
// (424): ns falls back to k8sNsPrefix+token, then the legacy-missing-secret skip.
func TestK8sBackend_Regrade_EmptyPRID(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	res, err := b.Regrade(context.Background(), "emptyprid", "", 4)
	if err != nil {
		t.Fatalf("Regrade empty PRID err = %v; want nil (skip)", err)
	}
	if res.Applied {
		t.Errorf("Applied=true; want false (no secret at derived ns)")
	}
}

// TestK8sBackend_Regrade_GetSecretError covers the non-NotFound Get-secret error
// skip branch (436).
func TestK8sBackend_Regrade_GetSecretError(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("get", "secrets", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("secret apiserver boom")
	})
	b := &K8sBackend{cs: cs}
	res, err := b.Regrade(context.Background(), "tok", "instant-customer-tok", 4)
	if err != nil {
		t.Fatalf("Regrade err = %v; want nil (skip)", err)
	}
	if res.Applied || !contains(res.SkipReason, "get secret") {
		t.Errorf("res = %+v; want skip with 'get secret'", res)
	}
}

// TestK8sBackend_Regrade_GetServiceError covers the non-NotFound Get-service
// error skip branch (443).
func TestK8sBackend_Regrade_GetServiceError(t *testing.T) {
	cs := fake.NewSimpleClientset()
	const ns = "instant-customer-rgsvc"
	b := &K8sBackend{cs: cs}
	if err := b.applyNamespace(context.Background(), ns); err != nil {
		t.Fatalf("ns: %v", err)
	}
	if _, err := b.cs.CoreV1().Secrets(ns).Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-admin"},
		Data:       map[string][]byte{"POSTGRES_USER": []byte("u"), "POSTGRES_PASSWORD": []byte("p")},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("secret: %v", err)
	}
	cs.PrependReactor("get", "services", func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("svc apiserver boom")
	})
	res, err := b.Regrade(context.Background(), "tok", ns, 4)
	if err != nil {
		t.Fatalf("Regrade err = %v; want nil (skip)", err)
	}
	if res.Applied || !contains(res.SkipReason, "get service") {
		t.Errorf("res = %+v; want skip with 'get service'", res)
	}
}

// validKubeconfig is a minimal but syntactically valid kubeconfig pointing at a
// dummy apiserver. clientcmd.BuildConfigFromFlags parses it without contacting
// the server, so newK8sBackend proceeds past config-building into the defaults
// block (storageClass/image/storageSizeGi) — the part the bad-path tests skip.
const validKubeconfig = `apiVersion: v1
kind: Config
clusters:
- name: dummy
  cluster:
    server: https://127.0.0.1:6443
contexts:
- name: dummy
  context:
    cluster: dummy
    user: dummy
current-context: dummy
users:
- name: dummy
  user:
    token: abc
`

// TestNewK8sBackend_ValidKubeconfig_AppliesDefaults covers the post-config
// defaults block: empty storageClass→"gp3", empty image→pgvector, sizeGi<=0→50.
func TestNewK8sBackend_ValidKubeconfig_AppliesDefaults(t *testing.T) {
	dir := t.TempDir()
	kc := filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(kc, []byte(validKubeconfig), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	b, err := newK8sBackend(kc, "", "", "ext-host", 0)
	if err != nil {
		t.Fatalf("newK8sBackend(valid kubeconfig): %v", err)
	}
	if b.storageClass != "gp3" {
		t.Errorf("storageClass = %q; want gp3 default", b.storageClass)
	}
	if b.image != "pgvector/pgvector:pg16" {
		t.Errorf("image = %q; want pgvector default", b.image)
	}
	if b.storageSizeGi != 50 {
		t.Errorf("storageSizeGi = %d; want 50 default", b.storageSizeGi)
	}
}

// TestNewK8sBackend_ValidKubeconfig_HonoursExplicit covers the non-default arm:
// explicit storageClass/image/sizeGi are kept as-is.
func TestNewK8sBackend_ValidKubeconfig_HonoursExplicit(t *testing.T) {
	dir := t.TempDir()
	kc := filepath.Join(dir, "kubeconfig")
	if err := os.WriteFile(kc, []byte(validKubeconfig), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	b, err := newK8sBackend(kc, "do-block-storage", "custom:img", "h", 25)
	if err != nil {
		t.Fatalf("newK8sBackend: %v", err)
	}
	if b.storageClass != "do-block-storage" || b.image != "custom:img" || b.storageSizeGi != 25 {
		t.Errorf("got sc=%q img=%q gi=%d; want explicit values", b.storageClass, b.image, b.storageSizeGi)
	}
}
