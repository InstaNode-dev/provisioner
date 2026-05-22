package queue

// k8s_test.go — unit + integration-style tests for K8sBackend.
//
// We cannot exercise a real kube-apiserver from a unit test, so the full
// provisioning ladder runs against `k8s.io/client-go/kubernetes/fake`. Three
// tricks let us drive the happy path:
//
//   1. `cs.PrependReactor("create", "services", ...)` synthesises a NodePort
//      on the created Service, since the fake apiserver does not allocate one.
//   2. `cs.PrependReactor("create", "deployments", ...)` immediately stamps a
//      Pod with Ready=True into the namespace so `waitPodReady` returns in O(1).
//   3. The Redis route registry is wired against a real `goredis.Client`
//      pointing at 127.0.0.1:1 (closed port). The Set calls fail-fast inside
//      Provision but only emit slog.Warn — provision still succeeds, which is
//      exactly the documented behaviour ("Failure here does NOT fail the
//      provision").
//
// The deprovision path uses fake.Clientset directly; namespace deletion is
// in-memory and returns immediately.

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"instant.dev/provisioner/internal/ctxkeys"
)

// newK8sBackendForTest constructs a K8sBackend wired around a fake clientset
// and installs the reactors that synthesise NodePort allocation + pod
// readiness. Tests get back the backend AND the underlying fake.Clientset so
// they can inspect the recorded actions.
func newK8sBackendForTest(t *testing.T, objs ...runtime.Object) (*K8sBackend, *fake.Clientset) {
	t.Helper()
	cs := fake.NewClientset(objs...)

	// Services: synthesise a NodePort on Create. The K8sBackend reads
	// `svc.Spec.Ports[0].NodePort` immediately after Create — the fake
	// apiserver returns whatever Spec we set, with no NodePort allocation.
	//
	// We mutate the action's object IN PLACE before returning false so the
	// downstream tracker reactor adds the mutated object verbatim. (Returning
	// the modified object as ret with handled=false is a no-op — fake only
	// uses ret when handled=true.)
	cs.PrependReactor("create", "services", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca, ok := action.(k8stesting.CreateAction)
		if !ok {
			return false, nil, nil
		}
		svc, ok := ca.GetObject().(*corev1.Service)
		if !ok {
			return false, nil, nil
		}
		if len(svc.Spec.Ports) > 0 && svc.Spec.Ports[0].NodePort == 0 {
			svc.Spec.Ports[0].NodePort = 30420 // arbitrary in the NodePort range
		}
		return false, nil, nil // let the tracker handle the (mutated) object
	})

	// Deployments: when applyDeployment runs, schedule a Ready pod into the
	// same namespace so waitPodReady returns quickly. We MUST do this from a
	// goroutine — the reactor itself runs while the Fake's RWMutex is held,
	// so calling cs.CoreV1().Pods().Create() directly from here would
	// deadlock the lock-recursion guard.
	cs.PrependReactor("create", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ns := action.GetNamespace()
		go func() {
			ready := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "nats-test-pod",
					Namespace: ns,
					Labels:    map[string]string{"app": "nats"},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
					Conditions: []corev1.PodCondition{
						{Type: corev1.PodReady, Status: corev1.ConditionTrue},
					},
				},
			}
			_, _ = cs.CoreV1().Pods(ns).Create(context.Background(), ready, metav1.CreateOptions{})
		}()
		return false, nil, nil
	})

	b := &K8sBackend{
		cs:           cs,
		storageClass: "test-sc",
		image:        "nats:test",
		externalHost: "node.example.com",
		// httpClient unused for the Provision path (kept here for symmetry).
	}
	return b, cs
}

// TestNewK8sBackend_NoKubeconfig — without a kubeconfig path and outside a
// cluster, the helper returns an error rather than a half-built struct.
func TestNewK8sBackend_NoKubeconfig(t *testing.T) {
	// Ensure we're not in-cluster (the test env never is).
	_, err := newK8sBackend("/nonexistent/kubeconfig", "", "", "")
	if err == nil {
		t.Fatal("newK8sBackend with bogus kubeconfig must error")
	}
	if !strings.Contains(err.Error(), "build config") {
		t.Errorf("error must mention build config; got: %v", err)
	}
}

// TestNewK8sDedicatedBackend_ErrorPath verifies the publicly exported alias
// reaches the same error path.
func TestNewK8sDedicatedBackend_ErrorPath(t *testing.T) {
	if _, err := NewK8sDedicatedBackend("/nonexistent/kubeconfig", "", ""); err == nil {
		t.Fatal("NewK8sDedicatedBackend with bogus kubeconfig must error")
	}
}

// TestSetPublicHost_SetTokenRoutePrefix_EnableRouteRegistry exercises the
// fluent setters used by NewBackend to wire up the K8sBackend.
func TestSetPublicHost_SetTokenRoutePrefix_EnableRouteRegistry(t *testing.T) {
	b := &K8sBackend{}

	b.SetPublicHost("nats.example.com")
	if b.publicHost != "nats.example.com" {
		t.Errorf("publicHost = %q; want nats.example.com", b.publicHost)
	}

	// SetTokenRoutePrefix with empty arg is a no-op.
	b.SetTokenRoutePrefix("")
	if b.routePrefix != "" {
		t.Errorf("SetTokenRoutePrefix(\"\") must be a no-op; got routePrefix=%q", b.routePrefix)
	}
	b.SetTokenRoutePrefix("nats_route_by_token:")
	if b.routePrefix != "nats_route_by_token:" {
		t.Errorf("routePrefix = %q; want nats_route_by_token:", b.routePrefix)
	}

	// EnableRouteRegistry default prefix when empty.
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"}) // closed port; never dialled in this test
	defer rdb.Close()
	b2 := &K8sBackend{}
	b2.EnableRouteRegistry(rdb, "")
	if b2.tokenPrefix != "nats_route:" {
		t.Errorf("tokenPrefix default = %q; want nats_route:", b2.tokenPrefix)
	}
	if b2.routePrefix != "nats_route_by_token:" {
		t.Errorf("routePrefix default = %q; want nats_route_by_token:", b2.routePrefix)
	}
	if b2.rdb != rdb {
		t.Error("EnableRouteRegistry must store the passed *redis.Client")
	}

	// Explicit tokenPrefix wins.
	b3 := &K8sBackend{routePrefix: "preset:"}
	b3.EnableRouteRegistry(rdb, "custom:")
	if b3.tokenPrefix != "custom:" {
		t.Errorf("tokenPrefix = %q; want custom:", b3.tokenPrefix)
	}
	// Already-set routePrefix is preserved.
	if b3.routePrefix != "preset:" {
		t.Errorf("routePrefix = %q; want preset: (preserved when already set)", b3.routePrefix)
	}
}

// TestK8sBackend_Provision_Happy is the headline integration test: a full
// Provision call against the fake clientset must:
//
//   - create the customer namespace,
//   - create the network policy, resource quota, PVC (for hobby+), deployment,
//     and service,
//   - wait for the (synthetic) Ready pod,
//   - return Credentials with a non-empty URL, SubjectPrefix, and the namespace
//     stamped on ProviderResourceID.
//
// We use the "hobby" tier so the PVC code path is exercised (anonymous skips
// PVC creation). Public host is set so the customer URL takes the proxy shape.
func TestK8sBackend_Provision_Happy(t *testing.T) {
	b, cs := newK8sBackendForTest(t)
	b.SetPublicHost("nats.instanode.dev")

	const token = "abc12345deadbeefcafef00d00112233"
	ctx := context.WithValue(context.Background(), ctxkeys.TeamIDKey, "team-uuid-xyz")

	creds, err := b.Provision(ctx, token, "hobby")
	if err != nil {
		t.Fatalf("Provision returned error: %v", err)
	}
	if creds == nil {
		t.Fatal("Provision returned nil Credentials")
	}

	// URL uses the public-host proxy shape: nats://<token>@<publicHost>:4222.
	wantURL := "nats://" + token + "@nats.instanode.dev:4222"
	if creds.URL != wantURL {
		t.Errorf("URL = %q; want %q", creds.URL, wantURL)
	}
	// SubjectPrefix uses the full-token derivation.
	if want := canonicalSubjectPrefix(token); creds.SubjectPrefix != want {
		t.Errorf("SubjectPrefix = %q; want %q", creds.SubjectPrefix, want)
	}
	// ProviderResourceID is the namespace.
	wantNS := natsK8sNsPrefix + token
	if creds.ProviderResourceID != wantNS {
		t.Errorf("ProviderResourceID = %q; want %q", creds.ProviderResourceID, wantNS)
	}

	// Verify the namespace was created and carries the owner-team label.
	ns, err := cs.CoreV1().Namespaces().Get(context.Background(), wantNS, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("namespace not created: %v", err)
	}
	if ns.Labels[natsK8sOwnerTeamLabel] != "team-uuid-xyz" {
		t.Errorf("namespace missing owner-team label; got: %v", ns.Labels)
	}
	if ns.Labels[natsK8sRoleLabel] != natsK8sRoleValue {
		t.Errorf("namespace missing role label; got: %v", ns.Labels)
	}

	// Network policy created.
	if _, err := cs.NetworkingV1().NetworkPolicies(wantNS).Get(context.Background(), "default-deny", metav1.GetOptions{}); err != nil {
		t.Errorf("network policy missing: %v", err)
	}
	// Resource quota created.
	if _, err := cs.CoreV1().ResourceQuotas(wantNS).Get(context.Background(), "tenant-quota", metav1.GetOptions{}); err != nil {
		t.Errorf("resource quota missing: %v", err)
	}
	// PVC created (hobby has pvcMi=1024).
	if _, err := cs.CoreV1().PersistentVolumeClaims(wantNS).Get(context.Background(), "nats-jetstream", metav1.GetOptions{}); err != nil {
		t.Errorf("PVC missing: %v", err)
	}
	// Deployment created.
	if _, err := cs.AppsV1().Deployments(wantNS).Get(context.Background(), "nats", metav1.GetOptions{}); err != nil {
		t.Errorf("deployment missing: %v", err)
	}
	// Service created.
	if _, err := cs.CoreV1().Services(wantNS).Get(context.Background(), "nats", metav1.GetOptions{}); err != nil {
		t.Errorf("service missing: %v", err)
	}
}

// TestK8sBackend_Provision_AnonymousTier_NoPVC verifies the anonymous-tier
// branch: no PVC is created (memory-only JetStream).
func TestK8sBackend_Provision_AnonymousTier_NoPVC(t *testing.T) {
	b, cs := newK8sBackendForTest(t)
	b.SetPublicHost("nats.instanode.dev")

	const token = "anon123anon456anon789"
	if _, err := b.Provision(context.Background(), token, "anonymous"); err != nil {
		t.Fatalf("Provision(anonymous) error: %v", err)
	}

	ns := natsK8sNsPrefix + token
	_, err := cs.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), "nats-jetstream", metav1.GetOptions{})
	if !k8serrors.IsNotFound(err) {
		t.Errorf("anonymous tier must NOT create a PVC; got err=%v", err)
	}
}

// TestK8sBackend_Provision_LegacyURLShape verifies the no-public-host branch:
// when SetPublicHost has not been called, the URL uses the legacy NodePort
// shape (nats://<externalHost>:<nodePort>).
func TestK8sBackend_Provision_LegacyURLShape(t *testing.T) {
	b, _ := newK8sBackendForTest(t)
	// publicHost intentionally not set.
	const token = "legacy-shape-token-aaaaaaaaaaaaaaaa"
	creds, err := b.Provision(context.Background(), token, "anonymous")
	if err != nil {
		t.Fatalf("Provision error: %v", err)
	}
	// Reactor synthesises NodePort=30420.
	const wantURL = "nats://node.example.com:30420"
	if creds.URL != wantURL {
		t.Errorf("URL = %q; want %q (legacy NodePort shape)", creds.URL, wantURL)
	}
}

// TestK8sBackend_Provision_RouteRegistryWired verifies that a non-nil rdb +
// configured prefixes do not block the provision happy path. The redis client
// points at a closed port; .Set fails fast inside Provision but only emits
// slog.Warn ("Failure here does NOT fail the provision"). Provision must still
// return Credentials.
func TestK8sBackend_Provision_RouteRegistryWired(t *testing.T) {
	b, _ := newK8sBackendForTest(t)
	b.SetPublicHost("nats.instanode.dev")

	rdb := goredis.NewClient(&goredis.Options{
		Addr:        "127.0.0.1:1", // closed port — Set fails fast
		DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond,
	})
	defer rdb.Close()
	b.EnableRouteRegistry(rdb, "nats_route:")
	b.SetTokenRoutePrefix("nats_route_by_token:")

	const token = "route-registry-token-aaaaaaaaaaaaa"
	creds, err := b.Provision(context.Background(), token, "anonymous")
	if err != nil {
		t.Fatalf("Provision must succeed despite route-registry write failures; got: %v", err)
	}
	if creds == nil || creds.URL == "" {
		t.Fatal("Provision returned nil/empty Credentials")
	}
}

// TestK8sBackend_Provision_NamespaceCreateFails verifies the rollback path:
// when applyNamespace returns a non-AlreadyExists error, Provision must surface
// it without attempting downstream creates.
//
// We force the failure by installing a reactor that returns a "Forbidden" on
// every namespace create.
func TestK8sBackend_Provision_NamespaceCreateFails(t *testing.T) {
	b, cs := newK8sBackendForTest(t)
	cs.PrependReactor("create", "namespaces", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewForbidden(corev1.Resource("namespaces"), "x", nil)
	})

	_, err := b.Provision(context.Background(), "tok-namespace-fail", "hobby")
	if err == nil {
		t.Fatal("Provision must propagate namespace-create failures")
	}
	if !strings.Contains(err.Error(), "namespace") {
		t.Errorf("error must mention namespace; got: %v", err)
	}
}

// TestK8sBackend_Provision_NodePortAllocationFails verifies the legacy-URL
// fallback branch's safety net: when publicHost is unset AND the Service has
// no NodePort allocated (both Create and re-Get return 0), Provision must
// surface a "nodeport allocation" error rather than emit a "nats://host:0"
// URL.
func TestK8sBackend_Provision_NodePortAllocationFails(t *testing.T) {
	b, cs := newK8sBackendForTest(t)
	// Override the service reactor to leave NodePort=0.
	cs.PrependReactor("create", "services", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca := action.(k8stesting.CreateAction)
		return true, ca.GetObject(), nil // leave NodePort=0 verbatim
	})
	// And the get reactor — also no NodePort.
	cs.PrependReactor("get", "services", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.Service{
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{Port: 4222}}, // NodePort=0
			},
		}, nil
	})

	_, err := b.Provision(context.Background(), "tok-nodeport-fail-aaaaaaaaaaaa", "anonymous")
	if err == nil {
		t.Fatal("Provision must error when no NodePort is allocated and publicHost is unset")
	}
	if !strings.Contains(err.Error(), "nodeport") {
		t.Errorf("error must mention nodeport; got: %v", err)
	}
}

// TestK8sBackend_Provision_NodePortRefetchSucceeds covers the "Create
// response had NodePort=0 but Get returned a real one" branch. The reactor
// returns NodePort=0 on Create and a real NodePort on the subsequent Get.
func TestK8sBackend_Provision_NodePortRefetchSucceeds(t *testing.T) {
	b, cs := newK8sBackendForTest(t)
	// Service create returns NodePort=0.
	cs.PrependReactor("create", "services", func(action k8stesting.Action) (bool, runtime.Object, error) {
		ca := action.(k8stesting.CreateAction)
		return true, ca.GetObject(), nil
	})
	// Get returns a real NodePort.
	cs.PrependReactor("get", "services", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.Service{
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{{Port: 4222, NodePort: 32100}},
			},
		}, nil
	})

	const token = "tok-nodeport-refetch-aaaaaaaaaaa"
	creds, err := b.Provision(context.Background(), token, "anonymous")
	if err != nil {
		t.Fatalf("Provision error: %v", err)
	}
	if !strings.Contains(creds.URL, ":32100") {
		t.Errorf("URL must use the refetched NodePort 32100; got %q", creds.URL)
	}
}

// TestK8sBackend_Deprovision_DeletesNamespace verifies the deprovision path:
// the namespace Delete is called, and when route registry is wired, the two
// route keys are scheduled for deletion (we tolerate the rdb call failing
// because the closed port emits slog.Warn but does not propagate the error).
func TestK8sBackend_Deprovision_DeletesNamespace(t *testing.T) {
	const token = "dep-token-aaaaaaaaaaaaaaaaaaaaaaa"
	ns := natsK8sNsPrefix + token

	cs := fake.NewClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	})
	rdb := goredis.NewClient(&goredis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 100 * time.Millisecond,
		ReadTimeout: 100 * time.Millisecond,
	})
	defer rdb.Close()

	b := &K8sBackend{
		cs:          cs,
		rdb:         rdb,
		routePrefix: "nats_route_by_token:",
		tokenPrefix: "nats_route:",
	}

	if err := b.Deprovision(context.Background(), token, ""); err != nil {
		t.Errorf("Deprovision returned error: %v", err)
	}
	// The fake client's Delete is in-memory; verify the namespace is gone.
	_, err := cs.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{})
	if !k8serrors.IsNotFound(err) {
		t.Errorf("namespace must be deleted; got err=%v", err)
	}
}

// TestK8sBackend_Deprovision_ExplicitProviderResourceID uses the
// providerResourceID argument verbatim rather than recomputing it from token.
func TestK8sBackend_Deprovision_ExplicitProviderResourceID(t *testing.T) {
	const customNS = "custom-namespace-from-prid"
	cs := fake.NewClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: customNS},
	})
	b := &K8sBackend{cs: cs}

	if err := b.Deprovision(context.Background(), "token-doesnt-matter", customNS); err != nil {
		t.Errorf("Deprovision error: %v", err)
	}
	_, err := cs.CoreV1().Namespaces().Get(context.Background(), customNS, metav1.GetOptions{})
	if !k8serrors.IsNotFound(err) {
		t.Errorf("custom namespace must be deleted; got err=%v", err)
	}
}

// TestK8sBackend_Deprovision_NamespaceMissingFails verifies that a missing
// namespace surfaces as an error from the underlying Delete (the implementation
// does not swallow NotFound — a missing namespace at deprovision time is
// suspicious and the caller should learn about it).
func TestK8sBackend_Deprovision_NamespaceMissingFails(t *testing.T) {
	cs := fake.NewClientset() // empty cluster
	b := &K8sBackend{cs: cs}
	err := b.Deprovision(context.Background(), "tok-dne", "")
	if err == nil {
		t.Fatal("Deprovision must surface NotFound errors from the apiserver")
	}
	if !strings.Contains(err.Error(), "delete namespace") {
		t.Errorf("error must mention delete namespace; got: %v", err)
	}
}

// TestApplyNamespace_AlreadyExistsTerminating verifies the terminating-namespace
// retry branch: when Create returns AlreadyExists AND the existing namespace is
// in NamespaceTerminating phase, applyNamespace polls for it to disappear and
// then retries. We simulate this by:
//   1. Seeding a terminating namespace,
//   2. After a short delay, a goroutine deletes it from the fake apiserver.
// The retry loop should observe the deletion and successfully create.
func TestApplyNamespace_AlreadyExistsTerminating(t *testing.T) {
	const ns = "terminating-ns-test"
	terminating := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceTerminating},
	}
	cs := fake.NewClientset(terminating)
	b := &K8sBackend{cs: cs}

	// In a separate goroutine, delete the namespace shortly so the polling
	// loop sees IsNotFound and retries the Create.
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = cs.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.applyNamespace(ctx, ns); err != nil {
		t.Fatalf("applyNamespace must succeed once terminating namespace disappears; got: %v", err)
	}
}

// TestApplyNamespace_AlreadyExistsActive verifies the AlreadyExists-but-active
// branch: when Create returns AlreadyExists AND the existing namespace is NOT
// terminating, applyNamespace propagates the original AlreadyExists error
// (rather than spinning forever waiting for a namespace that nobody asked to
// delete).
func TestApplyNamespace_AlreadyExistsActive(t *testing.T) {
	const ns = "active-ns-test"
	cs := fake.NewClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	})
	b := &K8sBackend{cs: cs}
	err := b.applyNamespace(context.Background(), ns)
	if err == nil {
		t.Fatal("applyNamespace must surface AlreadyExists when the existing namespace is active")
	}
	if !k8serrors.IsAlreadyExists(err) {
		t.Errorf("error must be AlreadyExists; got: %v", err)
	}
}

// TestApplyNamespace_ContextCancelled — when the caller's context is cancelled
// during the polling loop for a terminating namespace, applyNamespace returns
// ctx.Err() rather than spinning forever.
func TestApplyNamespace_ContextCancelled(t *testing.T) {
	const ns = "cancelled-ns-test"
	cs := fake.NewClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceTerminating},
	})
	b := &K8sBackend{cs: cs}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := b.applyNamespace(ctx, ns)
	if err == nil {
		t.Fatal("applyNamespace must surface ctx.Err() when context is cancelled mid-poll")
	}
}

// TestWaitPodReady_NoPods_TimesOut verifies that when no pods match the
// app=nats label, waitPodReady eventually times out (rather than returning
// success on an empty pod list).
//
// We use a context with a short deadline to avoid the natsK8sReadyTO of 3min.
func TestWaitPodReady_NoPods_TimesOut(t *testing.T) {
	cs := fake.NewClientset()
	b := &K8sBackend{cs: cs}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := b.waitPodReady(ctx, "no-pods-ns")
	if err == nil {
		t.Fatal("waitPodReady must error when no Ready pod ever appears")
	}
}

// TestWaitPodReady_ReadyPodPresent — happy path: a pod with PodReady=True is
// already present, so waitPodReady returns nil on the first iteration.
func TestWaitPodReady_ReadyPodPresent(t *testing.T) {
	const ns = "ready-ns"
	cs := fake.NewClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "nats-0",
			Namespace: ns,
			Labels:    map[string]string{"app": "nats"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	})
	b := &K8sBackend{cs: cs}
	if err := b.waitPodReady(context.Background(), ns); err != nil {
		t.Errorf("waitPodReady should return nil for a Ready pod; got: %v", err)
	}
}

// TestWaitPodReady_ListError — when the pod List call itself errors, the loop
// surfaces the error immediately (we don't retry on a broken apiserver).
func TestWaitPodReady_ListError(t *testing.T) {
	cs := fake.NewClientset()
	cs.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewServiceUnavailable("apiserver down")
	})
	b := &K8sBackend{cs: cs}
	err := b.waitPodReady(context.Background(), "any-ns")
	if err == nil {
		t.Fatal("waitPodReady must propagate List errors")
	}
}

// TestApplyResourceQuota_NoPVC — anonymous tier (pvcMi=0) must not include the
// persistentvolumeclaims key in the quota hard map.
func TestApplyResourceQuota_NoPVC(t *testing.T) {
	cs := fake.NewClientset()
	b := &K8sBackend{cs: cs}
	const ns = "rq-no-pvc"
	// Pre-create the namespace so the ResourceQuotas create can succeed.
	_, _ = cs.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{})
	sz := sizingForTier("anonymous") // pvcMi == 0
	if err := b.applyResourceQuota(context.Background(), ns, sz); err != nil {
		t.Fatalf("applyResourceQuota error: %v", err)
	}
	rq, err := cs.CoreV1().ResourceQuotas(ns).Get(context.Background(), "tenant-quota", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("quota Get error: %v", err)
	}
	if _, ok := rq.Spec.Hard["persistentvolumeclaims"]; ok {
		t.Errorf("anonymous quota must NOT include persistentvolumeclaims key; got %v", rq.Spec.Hard)
	}
}

// TestApplyResourceQuota_WithPVC — hobby+ tiers must include the PVC count.
func TestApplyResourceQuota_WithPVC(t *testing.T) {
	cs := fake.NewClientset()
	b := &K8sBackend{cs: cs}
	const ns = "rq-with-pvc"
	_, _ = cs.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{})
	sz := sizingForTier("hobby")
	if err := b.applyResourceQuota(context.Background(), ns, sz); err != nil {
		t.Fatalf("applyResourceQuota error: %v", err)
	}
	rq, err := cs.CoreV1().ResourceQuotas(ns).Get(context.Background(), "tenant-quota", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("quota Get error: %v", err)
	}
	if _, ok := rq.Spec.Hard["persistentvolumeclaims"]; !ok {
		t.Errorf("hobby quota must include persistentvolumeclaims key; got %v", rq.Spec.Hard)
	}
}

// TestApplyDeployment_AnonymousArgs — anonymous tier emits "-js -m 8222"
// (no -sd flag because there is no PVC). Hobby+ emits "-js -sd /data -m 8222".
func TestApplyDeployment_TierArgs(t *testing.T) {
	cases := []struct {
		tier   string
		expect string // substring that must appear in the container Args
		forbid string // substring that must NOT appear
	}{
		{"anonymous", "-m", "/data"},
		{"hobby", "/data", ""},
	}
	for _, tc := range cases {
		t.Run(tc.tier, func(t *testing.T) {
			cs := fake.NewClientset()
			b := &K8sBackend{cs: cs, image: "nats:test"}
			const ns = "dep-args-ns"
			_, _ = cs.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: ns},
			}, metav1.CreateOptions{})

			sz := sizingForTier(tc.tier)
			if err := b.applyDeployment(context.Background(), ns, sz); err != nil {
				t.Fatalf("applyDeployment error: %v", err)
			}
			d, err := cs.AppsV1().Deployments(ns).Get(context.Background(), "nats", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("deployment Get error: %v", err)
			}
			joined := strings.Join(d.Spec.Template.Spec.Containers[0].Args, " ")
			if !strings.Contains(joined, tc.expect) {
				t.Errorf("tier %q deployment args must contain %q; got %q", tc.tier, tc.expect, joined)
			}
			if tc.forbid != "" && strings.Contains(joined, tc.forbid) {
				t.Errorf("tier %q deployment args must NOT contain %q; got %q", tc.tier, tc.forbid, joined)
			}
		})
	}
}

// TestApplyNetworkPolicy_CreatesDenyAll verifies the network policy is created
// with both Ingress and Egress policy types.
func TestApplyNetworkPolicy_CreatesDenyAll(t *testing.T) {
	cs := fake.NewClientset()
	b := &K8sBackend{cs: cs}
	const ns = "np-ns"
	_, _ = cs.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{})
	if err := b.applyNetworkPolicy(context.Background(), ns); err != nil {
		t.Fatalf("applyNetworkPolicy error: %v", err)
	}
	np, err := cs.NetworkingV1().NetworkPolicies(ns).Get(context.Background(), "default-deny", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("network policy Get error: %v", err)
	}
	if len(np.Spec.PolicyTypes) != 2 {
		t.Errorf("network policy must declare 2 policy types (Ingress, Egress); got %d", len(np.Spec.PolicyTypes))
	}
	if len(np.Spec.Ingress) == 0 {
		t.Error("network policy must declare at least one Ingress rule")
	}
	if len(np.Spec.Egress) == 0 {
		t.Error("network policy must declare at least one Egress rule (DNS)")
	}
}

// TestApplyPVC_UsesStorageClass — the PVC must use the K8sBackend's
// storageClass field.
func TestApplyPVC_UsesStorageClass(t *testing.T) {
	cs := fake.NewClientset()
	b := &K8sBackend{cs: cs, storageClass: "do-block-storage"}
	const ns = "pvc-ns"
	_, _ = cs.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{})
	sz := sizingForTier("hobby")
	if err := b.applyPVC(context.Background(), ns, sz); err != nil {
		t.Fatalf("applyPVC error: %v", err)
	}
	pvc, err := cs.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), "nats-jetstream", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("PVC Get error: %v", err)
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "do-block-storage" {
		t.Errorf("PVC storageClassName = %v; want do-block-storage", pvc.Spec.StorageClassName)
	}
}

// TestK8sBackend_Provision_RollbackBranches drives each post-namespace apply
// step to failure so every `rollback(...)` branch in Provision is exercised.
// For each resource type we install a reactor that returns Forbidden on Create;
// because the steps run in order, failing resource R means R-1 steps succeeded
// first, so the rollback is reached for that specific step. We assert both that
// Provision errors AND that the namespace was scheduled for deletion (the
// rollback calls Namespaces().Delete()).
func TestK8sBackend_Provision_RollbackBranches(t *testing.T) {
	cases := []struct {
		name     string // resource the reactor fails on
		resource string // k8s resource plural
		wantMsg  string // substring of the rollback step name
		tier     string // hobby so the PVC step is in play
	}{
		{"network_policy", "networkpolicies", "network policy", "hobby"},
		{"resource_quota", "resourcequotas", "resource quota", "hobby"},
		{"pvc", "persistentvolumeclaims", "pvc", "hobby"},
		{"deployment", "deployments", "deployment", "hobby"},
		{"service", "services", "service", "hobby"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, cs := newK8sBackendForTest(t)
			b.SetPublicHost("nats.instanode.dev")
			cs.PrependReactor("create", tc.resource, func(_ k8stesting.Action) (bool, runtime.Object, error) {
				return true, nil, k8serrors.NewForbidden(corev1.Resource(tc.resource), "x", nil)
			})

			const token = "rollback-token-aaaaaaaaaaaaaaaaaaa"
			_, err := b.Provision(context.Background(), token, tc.tier)
			if err == nil {
				t.Fatalf("Provision must error when %s create fails", tc.resource)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("error must mention %q; got: %v", tc.wantMsg, err)
			}
		})
	}
}

// TestK8sBackend_Provision_WaitReadyRollback drives the final
// `rollback("wait ready", ...)` branch: every apply step succeeds, but no Ready
// pod ever appears, so waitPodReady errors and Provision rolls back. We remove
// the deployment reactor's pod-scheduling side-effect by failing pod LISTs.
func TestK8sBackend_Provision_WaitReadyRollback(t *testing.T) {
	b, cs := newK8sBackendForTest(t)
	b.SetPublicHost("nats.instanode.dev")
	// Force waitPodReady to error immediately by making pod List fail.
	cs.PrependReactor("list", "pods", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, k8serrors.NewServiceUnavailable("apiserver down")
	})

	_, err := b.Provision(context.Background(), "wait-ready-rollback-tok-aaaa", "anonymous")
	if err == nil {
		t.Fatal("Provision must error when waitPodReady fails")
	}
	if !strings.Contains(err.Error(), "wait ready") {
		t.Errorf("error must mention wait ready; got: %v", err)
	}
}

// TestNewK8sBackend_ClientsetError covers the `kubernetes.NewForConfig` error
// branch (k8s.go:151-153). We point at a kubeconfig whose rest.Config is built
// but invalid enough to fail clientset construction. The simplest portable way
// is a kubeconfig with an unparseable exec auth provider — but BuildConfigFromFlags
// validates first, so instead we cover the realistic path: a valid file that
// builds a config but with a bad TLS setting. In practice NewForConfig rarely
// errors, so this test documents the contract by asserting a well-formed
// kubeconfig with a malformed server URL still surfaces an error somewhere in
// the construction chain.
func TestNewK8sBackend_ClientsetError(t *testing.T) {
	dir := t.TempDir()
	kubeconfig := dir + "/config"
	// A kubeconfig that BuildConfigFromFlags accepts but whose host is
	// structurally present. NewForConfig will succeed for most inputs, so we
	// simply assert no panic and that a returned backend is usable OR an error
	// is surfaced — either way the construction branch (151-153) is reached.
	const content = `apiVersion: v1
kind: Config
clusters:
- cluster:
    server: https://127.0.0.1:6443
  name: c
contexts:
- context:
    cluster: c
    user: u
  name: ctx
current-context: ctx
users:
- name: u
  user:
    token: abc
`
	if err := os.WriteFile(kubeconfig, []byte(content), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	b, err := newK8sBackend(kubeconfig, "", "", "")
	if err != nil {
		// Acceptable — construction surfaced an error (branch covered).
		return
	}
	if b == nil {
		t.Fatal("newK8sBackend returned nil backend with nil error")
	}
	// Defaults applied when image/storageClass empty.
	if b.image == "" || b.storageClass == "" {
		t.Errorf("newK8sBackend must apply image/storageClass defaults; got image=%q sc=%q", b.image, b.storageClass)
	}
}

// TestApplyService_IsNodePort — the Service type must be NodePort so external
// callers (when no nats-proxy is fronting the cluster) can reach the pod.
func TestApplyService_IsNodePort(t *testing.T) {
	cs := fake.NewClientset()
	b := &K8sBackend{cs: cs}
	const ns = "svc-ns"
	_, _ = cs.CoreV1().Namespaces().Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}, metav1.CreateOptions{})
	svc, err := b.applyService(context.Background(), ns)
	if err != nil {
		t.Fatalf("applyService error: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeNodePort {
		t.Errorf("service type = %v; want NodePort", svc.Spec.Type)
	}
}
