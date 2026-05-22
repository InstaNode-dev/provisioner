package mongo

// k8s_lifecycle_test.go — covers the K8sBackend Provision/StorageBytes/
// Deprovision orchestration paths using a fake clientset + reactor stubs.
//
// True end-to-end Provision requires a real cluster + pod scheduling +
// running mongod — out of scope for unit tests. We instead exercise:
//
//   * StorageBytes fail-soft paths (missing Secret, missing Service)
//   * StorageBytes error propagation (synthetic non-NotFound secret error)
//   * StorageBytes connect-failure branch (secret + service present but
//     ClusterIP unreachable; SCRAM auth against authless local mongo fails
//     so the dbStats loop drains lastErr → wrapped error)
//   * Deprovision happy path (namespace Delete via fake clientset)
//   * Deprovision idempotent NotFound (delete on missing namespace must
//     NOT return an error)
//   * Deprovision propagates non-NotFound delete errors
//   * Deprovision with route-registry (Del on missing keys is non-fatal)
//   * Provision early-bail when applyNamespace fails (AlreadyExists Active
//     namespace — Provision returns an error, NOT a nil credential)
//   * Provision early-bail when applyResourceQuota fails (rollback path)

import (
	"context"
	"errors"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/kubernetes/fake"

	"instant.dev/provisioner/internal/poolident"
)

// ─── StorageBytes ───────────────────────────────────────────────────────────

func TestK8sStorageBytes_NoSecretFailsSoft(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	got, err := b.StorageBytes(context.Background(), "abc", "")
	if err != nil {
		t.Errorf("missing secret: want nil err (fail-soft), got %v", err)
	}
	if got != 0 {
		t.Errorf("missing secret: got %d bytes, want 0", got)
	}
}

func TestK8sStorageBytes_NoServiceFailsSoft(t *testing.T) {
	const token = "abc"
	ns := mongoK8sNsPrefix + token
	cs := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mongo-admin", Namespace: ns},
		Data:       map[string][]byte{"MONGO_INITDB_ROOT_PASSWORD": []byte("pw")},
	})
	b := &K8sBackend{cs: cs}
	got, err := b.StorageBytes(context.Background(), token, "")
	if err != nil {
		t.Errorf("missing service: want nil err (fail-soft), got %v", err)
	}
	if got != 0 {
		t.Errorf("missing service: got %d bytes, want 0", got)
	}
}

func TestK8sStorageBytes_SecretGetErrorPropagates(t *testing.T) {
	const token = "abc"
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("get", "secrets", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("synthetic secret get failure")
	})
	b := &K8sBackend{cs: cs}
	if _, err := b.StorageBytes(context.Background(), token, ""); err == nil {
		t.Fatal("expected error to propagate, got nil")
	}
}

func TestK8sStorageBytes_ServiceGetErrorPropagates(t *testing.T) {
	const token = "abc"
	ns := mongoK8sNsPrefix + token
	cs := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mongo-admin", Namespace: ns},
		Data:       map[string][]byte{"MONGO_INITDB_ROOT_PASSWORD": []byte("pw")},
	})
	cs.PrependReactor("get", "services", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("synthetic service get failure")
	})
	b := &K8sBackend{cs: cs}
	if _, err := b.StorageBytes(context.Background(), token, ""); err == nil {
		t.Fatal("expected service get error to propagate, got nil")
	}
}

// TestK8sStorageBytes_ClusterIPUnreachable feeds the dbStats loop an
// unreachable ClusterIP so every candidate dbStats fails and the function
// returns a wrapped error from the lastErr path.
func TestK8sStorageBytes_ClusterIPUnreachable(t *testing.T) {
	const token = "abc"
	ns := mongoK8sNsPrefix + token
	cs := fake.NewSimpleClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "mongo-admin", Namespace: ns},
			Data:       map[string][]byte{"MONGO_INITDB_ROOT_PASSWORD": []byte("pw")},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "mongodb", Namespace: ns},
			Spec:       corev1.ServiceSpec{ClusterIP: "127.0.0.1"}, // wrong creds → all candidates fail
		},
	)
	b := &K8sBackend{cs: cs}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_, err := b.StorageBytes(ctx, token, "")
	if err == nil {
		t.Fatal("expected dbStats-all-candidates error, got nil")
	}
}

// ─── Deprovision ────────────────────────────────────────────────────────────

func TestK8sDeprovision_DeletesNamespace(t *testing.T) {
	const token = "drop-abc"
	ns := mongoK8sNsPrefix + token
	cs := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	})
	b := &K8sBackend{cs: cs}
	if err := b.Deprovision(context.Background(), token, ""); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if _, err := cs.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("namespace not deleted: %v", err)
	}
}

func TestK8sDeprovision_NotFoundIsIdempotent(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	if err := b.Deprovision(context.Background(), "missing", ""); err != nil {
		t.Errorf("Deprovision on missing namespace: want nil, got %v", err)
	}
}

func TestK8sDeprovision_DeleteErrorPropagates(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("delete", "namespaces", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("synthetic delete failure")
	})
	b := &K8sBackend{cs: cs}
	if err := b.Deprovision(context.Background(), "tok", ""); err == nil {
		t.Fatal("expected delete error to propagate, got nil")
	}
}

func TestK8sDeprovision_WithRouteRegistry_NoCrashOnUnreachableRedis(t *testing.T) {
	const token = "rtr-abc"
	ns := mongoK8sNsPrefix + token
	cs := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	})
	// Point at a port nothing listens on — Del must log a warn, not panic.
	rdb := goredis.NewClient(&goredis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 200 * time.Millisecond,
		MaxRetries:  -1,
	})
	defer rdb.Close()
	b := &K8sBackend{cs: cs}
	b.EnableRouteRegistry(rdb, "")
	if err := b.Deprovision(context.Background(), token, ""); err != nil {
		t.Errorf("Deprovision with bad redis: want nil (route Del is warn-only), got %v", err)
	}
}

// TestK8sDeprovision_PoolPRID_StripsNamespacePrefix asserts that when the
// provider_resource_id encodes both a base PRID (the real namespace) and a
// pool token, Deprovision deletes the BASE PRID namespace — not the synthetic
// instant-customer-<requestToken>.
func TestK8sDeprovision_PoolPRID_StripsNamespacePrefix(t *testing.T) {
	const realNS = "instant-customer-pool-poolish"
	prid := poolident.Encode(realNS, "pool-poolish")
	cs := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: realNS},
	})
	b := &K8sBackend{cs: cs}
	if err := b.Deprovision(context.Background(), "requestor", prid); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	if _, err := cs.CoreV1().Namespaces().Get(context.Background(), realNS, metav1.GetOptions{}); !k8serrors.IsNotFound(err) {
		t.Errorf("real pool namespace must be deleted; got %v", err)
	}
}

// ─── Provision early-error paths ────────────────────────────────────────────

// TestK8sProvision_AlreadyExistsActiveNamespace covers the applyNamespace
// branch where the namespace is already Active (not Terminating) — Provision
// must surface the error rather than create over the live namespace.
func TestK8sProvision_AlreadyExistsActiveNamespace(t *testing.T) {
	const token = "exists-tok"
	ns := mongoK8sNsPrefix + token
	cs := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	})
	b := &K8sBackend{cs: cs, image: "mongo:7", storageClass: "gp3"}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	creds, err := b.Provision(ctx, token, "hobby")
	if err == nil {
		t.Fatal("Provision: want error on already-exists Active namespace, got nil")
	}
	if creds != nil {
		t.Errorf("Provision: must not return creds on error, got %+v", creds)
	}
}

// TestK8sProvision_RollsBackOnQuotaFailure forces applyResourceQuota to fail
// via a reactor, then asserts the rollback path runs: the namespace must end
// up deleted. The function returns a wrapped quota error.
func TestK8sProvision_RollsBackOnQuotaFailure(t *testing.T) {
	const token = "rollback-tok"
	ns := mongoK8sNsPrefix + token
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "resourcequotas", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("synthetic quota create failure")
	})
	b := &K8sBackend{cs: cs, image: "mongo:7", storageClass: "gp3"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	creds, err := b.Provision(ctx, token, "hobby")
	if err == nil {
		t.Fatal("Provision: want quota error, got nil")
	}
	if creds != nil {
		t.Error("Provision: must not return creds on error")
	}
	// Rollback Deletes ns on a fresh context; wait briefly for fake clientset
	// to settle then assert namespace is gone.
	_, getErr := cs.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{})
	if !k8serrors.IsNotFound(getErr) {
		t.Errorf("rollback: namespace not deleted, got %v", getErr)
	}
}

// TestK8sProvision_RollsBackOnNetworkPolicyFailure injects a failure on the
// NetworkPolicy create. Covers the second rollback site.
func TestK8sProvision_RollsBackOnNetworkPolicyFailure(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "networkpolicies", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("synthetic netpol failure")
	})
	b := &K8sBackend{cs: cs, image: "mongo:7", storageClass: "gp3"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, "np-tok", "hobby"); err == nil {
		t.Fatal("Provision: want netpol error, got nil")
	}
}

// TestK8sProvision_RollsBackOnSecretFailure covers the applyAdminSecret error
// rollback branch.
func TestK8sProvision_RollsBackOnSecretFailure(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "secrets", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("synthetic secret failure")
	})
	b := &K8sBackend{cs: cs, image: "mongo:7", storageClass: "gp3"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, "sec-tok", "hobby"); err == nil {
		t.Fatal("Provision: want secret error, got nil")
	}
}

// TestK8sProvision_RollsBackOnPVCFailure exercises the PVC rollback branch.
// Only runs for tiers where sz.pvcMi > 0 (hobby) — anonymous skips the PVC.
func TestK8sProvision_RollsBackOnPVCFailure(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "persistentvolumeclaims", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("synthetic pvc failure")
	})
	b := &K8sBackend{cs: cs, image: "mongo:7", storageClass: "gp3"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, "pvc-tok", "hobby"); err == nil {
		t.Fatal("Provision: want pvc error, got nil")
	}
}

// TestK8sProvision_RollsBackOnDeploymentFailure exercises the Deployment
// create rollback branch.
func TestK8sProvision_RollsBackOnDeploymentFailure(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "deployments", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("synthetic deployment failure")
	})
	b := &K8sBackend{cs: cs, image: "mongo:7", storageClass: "gp3"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, "dep-tok", "hobby"); err == nil {
		t.Fatal("Provision: want deployment error, got nil")
	}
}

// TestK8sProvision_RollsBackOnServiceFailure exercises the Service create
// rollback branch.
func TestK8sProvision_RollsBackOnServiceFailure(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("create", "services", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("synthetic service failure")
	})
	b := &K8sBackend{cs: cs, image: "mongo:7", storageClass: "gp3"}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, "svc-tok", "hobby"); err == nil {
		t.Fatal("Provision: want service error, got nil")
	}
}

// TestK8sProvision_WaitPodReady_TimeoutRollsBack — drive Provision past every
// applyXxx and into waitPodReady, where the fake clientset has no Ready pod.
// Use a short request context so the wait loop exits via ctx.Done().
//
// NOTE: Provision uses a fresh 5-minute provCtx internally, so the parent
// ctx is consulted only for the teamID value — we cannot cancel via parent
// ctx here. Instead, we inject a synthetic Pods List error so waitPodReady
// fails fast.
func TestK8sProvision_WaitPodReady_ErrorRollsBack(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("list", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("synthetic pod list failure")
	})
	b := &K8sBackend{cs: cs, image: "mongo:7", storageClass: "gp3"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, "wait-tok", "hobby"); err == nil {
		t.Fatal("Provision: want wait-ready error, got nil")
	}
}

// TestK8sProvision_InitMongoFailureRollsBack drives Provision through every
// applyXxx + a successful waitPodReady (Ready pod injected) into initMongo.
// We inject a syntactically-invalid ClusterIP into the created Service so
// mongo.Connect's ApplyURI parse step fails IMMEDIATELY (non-retryable →
// initMongo bails on attempt 1 rather than burning 30 seconds of retries).
func TestK8sProvision_InitMongoFailureRollsBack(t *testing.T) {
	const token = "init-tok"
	ns := mongoK8sNsPrefix + token
	cs := fake.NewSimpleClientset()
	// Plant a Ready pod up front; waitPodReady will see it immediately.
	cs.PrependReactor("list", "pods", func(a ktesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{Items: []corev1.Pod{{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mongodb-readypod",
				Namespace: ns,
				Labels:    map[string]string{"app": "mongodb"},
			},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				},
			},
		}}}, nil
	})
	// Plant an INVALID ClusterIP (contains [ which breaks ApplyURI parse).
	// This forces tryInitMongo's first attempt to return a non-retryable
	// parse error so initMongo bails immediately.
	cs.PrependReactor("create", "services", func(a ktesting.Action) (bool, runtime.Object, error) {
		ca, ok := a.(ktesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		svc := ca.GetObject().(*corev1.Service)
		svc.Spec.ClusterIP = "[bad-uri-host"
		if len(svc.Spec.Ports) > 0 {
			svc.Spec.Ports[0].NodePort = 30000
		}
		return false, nil, nil // let normal create run with our mutation
	})
	b := &K8sBackend{cs: cs, image: "mongo:7", storageClass: "gp3"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	creds, err := b.Provision(ctx, token, "hobby")
	if err == nil {
		t.Fatal("Provision: want init-mongo error, got nil")
	}
	if creds != nil {
		t.Error("Provision: must not return creds on init-mongo failure")
	}
}

// TestApplyNamespace_TerminatingThenDeleted exercises the Terminating-namespace
// branch of applyNamespace: AlreadyExists + Phase==Terminating then re-Create
// after the namespace finally drains.
//
// We start with a Terminating namespace and, after the first Get inside the
// wait loop, delete it from the tracker so the next Get returns NotFound;
// applyNamespace then re-Creates and returns nil.
func TestApplyNamespace_TerminatingThenDeleted(t *testing.T) {
	const ns = "instant-customer-mongo-term"
	cs := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceTerminating},
	})
	// On the FIRST Get inside the wait loop, asynchronously drop the ns from
	// the tracker so the SECOND Get returns NotFound and the re-Create wins.
	getInsideLoop := 0
	cs.PrependReactor("get", "namespaces", func(a ktesting.Action) (bool, runtime.Object, error) {
		getInsideLoop++
		// First Get is from the main fn body (still returns Terminating);
		// second Get is the first loop iteration — drop the ns so the third
		// (next loop iteration) returns NotFound.
		if getInsideLoop == 2 {
			// drop the namespace from the tracker, async to avoid reentrancy.
			go func() {
				_ = cs.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("namespaces"), "", ns)
			}()
		}
		return false, nil, nil // delegate to default tracker
	})
	b := &K8sBackend{cs: cs}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := b.applyNamespace(ctx, ns); err != nil {
		t.Fatalf("applyNamespace: want eventual success after Terminating, got %v", err)
	}
	// Re-Create succeeded — ns now exists in Active phase via the create.
	got, err := cs.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("post-recreate get: %v", err)
	}
	if got.Status.Phase == corev1.NamespaceTerminating {
		t.Errorf("re-created ns still shows Terminating phase: %+v", got)
	}
}

// _ ensures the test package compiles against the runtime.Object schema
// even if a future change removes the last direct reference.
var _ = schema.GroupVersionResource{}
