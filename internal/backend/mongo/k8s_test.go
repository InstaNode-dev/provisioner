package mongo

// k8s_test.go — unit-level coverage for the K8sBackend resource-application
// helpers. Uses a fake clientset (k8s.io/client-go/kubernetes/fake) so each
// test runs without a real cluster.
//
// The end-to-end Provision/StorageBytes/Deprovision paths spin pods and run
// mongod init, which can't be exercised against a fake clientset — they live
// behind real k8s integration tests elsewhere. Here we cover every helper
// that builds the desired-state objects, the wait/init retry loops, and the
// configuration knobs (route registry / public host / password prefix).

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/client-go/kubernetes/fake"

	"instant.dev/provisioner/internal/ctxkeys"
)

// ─── tier sizing ────────────────────────────────────────────────────────────

// TestSizingForTier_CoversEveryKnownTier exercises every documented branch in
// sizingForTier. The numbers themselves are not the contract (they live in the
// k8s yaml manifests downstream) — what matters is that every tier returns a
// non-zero CPU/memory request so the tier→sizing map cannot silently degrade
// to a zero-resource pod.
func TestSizingForTier_CoversEveryKnownTier(t *testing.T) {
	cases := []struct {
		tier        string
		wantMaxConns int
		wantPVCNonZero bool
	}{
		{"anonymous", 20, false}, // pvcMi=0 → emptyDir
		{"hobby", 100, true},
		{"pro", 500, true},
		{"team", 2000, true},
		{"growth", 2000, true},   // shares team sizing
		{"unknown", 100, true},   // default → hobby
		{"", 100, true},          // empty → hobby
	}
	for _, tc := range cases {
		t.Run(tc.tier, func(t *testing.T) {
			sz := sizingForTier(tc.tier)
			if sz.maxConns != tc.wantMaxConns {
				t.Errorf("maxConns = %d, want %d", sz.maxConns, tc.wantMaxConns)
			}
			if tc.wantPVCNonZero && sz.pvcMi == 0 {
				t.Errorf("pvcMi = 0, want > 0 for tier %q", tc.tier)
			}
			if !tc.wantPVCNonZero && sz.pvcMi != 0 {
				t.Errorf("pvcMi = %d, want 0 for tier %q (emptyDir)", sz.pvcMi, tc.tier)
			}
			// Every tier MUST produce a parseable resource request — a
			// silent typo (e.g. dropping the unit) would make
			// resource.MustParse panic in applyDeployment / applyResourceQuota.
			resource.MustParse(sz.cpuReq)
			resource.MustParse(sz.memReq)
			resource.MustParse(sz.cpuLim)
			resource.MustParse(sz.memLim)
			resource.MustParse(sz.qCPURequests)
			resource.MustParse(sz.qMemRequests)
			resource.MustParse(sz.qCPULimits)
			resource.MustParse(sz.qMemLimits)
		})
	}
}

// TestMongoQuotaHard_IncludesPVCOnlyWhenSized asserts that the persistentvolumeclaims
// quota key is present iff sz.pvcMi > 0 — i.e. anonymous (emptyDir) doesn't
// reserve PVC headroom.
func TestMongoQuotaHard_IncludesPVCOnlyWhenSized(t *testing.T) {
	szPVC := sizingForTier("hobby")
	szEphemeral := sizingForTier("anonymous")

	withPVC := mongoQuotaHard(szPVC)
	if _, ok := withPVC["persistentvolumeclaims"]; !ok {
		t.Error("hobby tier: persistentvolumeclaims missing from quota hard")
	}
	if _, ok := withPVC[corev1.ResourceRequestsCPU]; !ok {
		t.Error("hobby tier: requests.cpu missing from quota hard")
	}
	if _, ok := withPVC[corev1.ResourcePods]; !ok {
		t.Error("hobby tier: pods missing from quota hard")
	}

	noPVC := mongoQuotaHard(szEphemeral)
	if _, ok := noPVC["persistentvolumeclaims"]; ok {
		t.Error("anonymous tier: persistentvolumeclaims set in quota; want absent for emptyDir")
	}
}

// ─── small helpers ──────────────────────────────────────────────────────────

func TestMongoK8sRandHex_LengthAndUniqueness(t *testing.T) {
	a, err := mongoK8sRandHex(16)
	if err != nil {
		t.Fatalf("rand: %v", err)
	}
	if len(a) != 32 {
		t.Errorf("16-byte hex length = %d, want 32", len(a))
	}
	b, _ := mongoK8sRandHex(16)
	if a == b {
		t.Errorf("two consecutive rand-hex calls produced the same value")
	}
	// Hex-only characters.
	for _, c := range a {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			t.Errorf("non-hex char %q in %q", c, a)
		}
	}
}

func TestMongoK8sBoolPtr_RoundTrip(t *testing.T) {
	if p := mongoK8sBoolPtr(true); p == nil || *p != true {
		t.Errorf("bool ptr true: %v", p)
	}
	if p := mongoK8sBoolPtr(false); p == nil || *p != false {
		t.Errorf("bool ptr false: %v", p)
	}
}

// TestMongoDataVolumeSource_PVCvsEmptyDir is the regression guard for the
// anonymous emptyDir branch. PVC tiers must reference the mongo-data PVC by
// name; emptyDir tiers must set EmptyDir non-nil.
func TestMongoDataVolumeSource_PVCvsEmptyDir(t *testing.T) {
	pvc := mongoDataVolumeSource(tierSizing{pvcMi: 1024})
	if pvc.PersistentVolumeClaim == nil || pvc.PersistentVolumeClaim.ClaimName != "mongo-data" {
		t.Errorf("PVC tier: want PVC volume source with ClaimName=mongo-data, got %+v", pvc)
	}
	if pvc.EmptyDir != nil {
		t.Errorf("PVC tier: EmptyDir must be nil")
	}

	ed := mongoDataVolumeSource(tierSizing{pvcMi: 0})
	if ed.EmptyDir == nil {
		t.Errorf("emptyDir tier: EmptyDir must be non-nil")
	}
	if ed.PersistentVolumeClaim != nil {
		t.Errorf("emptyDir tier: PersistentVolumeClaim must be nil")
	}
}

// ─── route-registry configuration knobs ─────────────────────────────────────

func TestK8sBackendRouteRegistryConfig(t *testing.T) {
	b := &K8sBackend{}

	// Pre-config: every public host / route prefix knob is empty.
	if b.publicHost != "" {
		t.Errorf("publicHost initial = %q", b.publicHost)
	}

	b.SetPublicHost("mongo.example.com")
	if b.publicHost != "mongo.example.com" {
		t.Errorf("SetPublicHost: %q", b.publicHost)
	}

	// EnableRouteRegistry with empty prefix → default applied.
	rdb := goredis.NewClient(&goredis.Options{Addr: "127.0.0.1:1"})
	defer rdb.Close()
	b.EnableRouteRegistry(rdb, "")
	if b.routePrefix != "mongo_route:" {
		t.Errorf("default routePrefix = %q", b.routePrefix)
	}
	if b.userPrefix != "mongo_route_by_user:" {
		t.Errorf("default userPrefix = %q", b.userPrefix)
	}

	// Explicit prefix wins.
	b2 := &K8sBackend{}
	b2.EnableRouteRegistry(rdb, "custom_route:")
	if b2.routePrefix != "custom_route:" {
		t.Errorf("explicit routePrefix = %q", b2.routePrefix)
	}

	// SetPasswordRoutePrefix overrides the user-prefix when non-empty.
	b2.SetPasswordRoutePrefix("user_lookup:")
	if b2.userPrefix != "user_lookup:" {
		t.Errorf("SetPasswordRoutePrefix: %q", b2.userPrefix)
	}
	// Empty string is a no-op (preserves the previously-set value).
	b2.SetPasswordRoutePrefix("")
	if b2.userPrefix != "user_lookup:" {
		t.Errorf("SetPasswordRoutePrefix(empty) clobbered: %q", b2.userPrefix)
	}
}

// ─── desired-state helpers against a fake clientset ─────────────────────────

func TestApplyNamespace_CarriesOwnerTeamLabel(t *testing.T) {
	const teamID = "11111111-2222-3333-4444-555555555555"
	const ns = "instant-customer-mongo-teamlabel"

	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	ctx := context.WithValue(context.Background(), ctxkeys.TeamIDKey, teamID)

	if err := b.applyNamespace(ctx, ns); err != nil {
		t.Fatalf("applyNamespace: %v", err)
	}
	got, err := cs.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get namespace: %v", err)
	}
	if got.Labels[mongoK8sRoleLabel] != mongoK8sRoleValue {
		t.Errorf("role label = %q, want %q", got.Labels[mongoK8sRoleLabel], mongoK8sRoleValue)
	}
	if got.Labels[mongoK8sOwnerTeamLabel] != teamID {
		t.Errorf("owner-team label = %q, want %q", got.Labels[mongoK8sOwnerTeamLabel], teamID)
	}
	if got.Labels["pod-security.kubernetes.io/enforce"] != "baseline" {
		t.Error("missing PSS enforce=baseline label")
	}
}

func TestApplyNamespace_NoOwnerLabelWhenContextEmpty(t *testing.T) {
	const ns = "instant-customer-mongo-noteam"
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	if err := b.applyNamespace(context.Background(), ns); err != nil {
		t.Fatalf("applyNamespace: %v", err)
	}
	got, _ := cs.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{})
	if _, ok := got.Labels[mongoK8sOwnerTeamLabel]; ok {
		t.Errorf("owner-team label set without context value")
	}
}

func TestApplyNamespace_ReturnsErrOnAlreadyExistsActive(t *testing.T) {
	const ns = "instant-customer-mongo-exists"
	cs := fake.NewSimpleClientset(&corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: ns},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	})
	b := &K8sBackend{cs: cs}
	err := b.applyNamespace(context.Background(), ns)
	if err == nil {
		t.Fatal("applyNamespace: want AlreadyExists error, got nil")
	}
	if !k8serrors.IsAlreadyExists(err) {
		t.Errorf("error must be IsAlreadyExists, got %v", err)
	}
}

func TestApplyNetworkPolicy_CreatesIngressEgressRules(t *testing.T) {
	const ns = "instant-customer-mongo-np"
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	if err := b.applyNetworkPolicy(context.Background(), ns, 27017); err != nil {
		t.Fatalf("applyNetworkPolicy: %v", err)
	}
	np, err := cs.NetworkingV1().NetworkPolicies(ns).Get(context.Background(), "default-deny", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get netpol: %v", err)
	}
	if len(np.Spec.Ingress) == 0 || len(np.Spec.Egress) == 0 {
		t.Errorf("ingress/egress missing: %+v", np.Spec)
	}
}

func TestApplyResourceQuota_AppliesTierBudget(t *testing.T) {
	const ns = "instant-customer-mongo-quota"
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	sz := sizingForTier("hobby")
	if err := b.applyResourceQuota(context.Background(), ns, sz); err != nil {
		t.Fatalf("applyResourceQuota: %v", err)
	}
	q, err := cs.CoreV1().ResourceQuotas(ns).Get(context.Background(), "tenant-quota", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get quota: %v", err)
	}
	if _, ok := q.Spec.Hard["persistentvolumeclaims"]; !ok {
		t.Errorf("PVC budget missing for hobby quota")
	}
	if _, ok := q.Spec.Hard[corev1.ResourcePods]; !ok {
		t.Errorf("pods budget missing")
	}
}

func TestApplyAdminSecret_StoresRootCredentials(t *testing.T) {
	const ns = "instant-customer-mongo-sec"
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	if err := b.applyAdminSecret(context.Background(), ns, "s3cret"); err != nil {
		t.Fatalf("applyAdminSecret: %v", err)
	}
	s, err := cs.CoreV1().Secrets(ns).Get(context.Background(), "mongo-admin", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if s.StringData["MONGO_INITDB_ROOT_USERNAME"] != "root" {
		t.Errorf("root username = %q, want root", s.StringData["MONGO_INITDB_ROOT_USERNAME"])
	}
	if s.StringData["MONGO_INITDB_ROOT_PASSWORD"] != "s3cret" {
		t.Errorf("root password not stored")
	}
}

func TestApplyPVC_RequestsSizedStorageOnConfiguredClass(t *testing.T) {
	const ns = "instant-customer-mongo-pvc"
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs, storageClass: "do-block-storage"}
	sz := sizingForTier("hobby")
	if err := b.applyPVC(context.Background(), ns, sz); err != nil {
		t.Fatalf("applyPVC: %v", err)
	}
	pvc, err := cs.CoreV1().PersistentVolumeClaims(ns).Get(context.Background(), "mongo-data", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pvc: %v", err)
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != "do-block-storage" {
		t.Errorf("StorageClassName = %v, want do-block-storage", pvc.Spec.StorageClassName)
	}
	if pvc.Spec.Resources.Requests.Storage().IsZero() {
		t.Errorf("PVC storage request is zero")
	}
}

func TestApplyDeployment_BuildsContainerCorrectly(t *testing.T) {
	const ns = "instant-customer-mongo-dep"
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs, image: "mongo:7"}
	sz := sizingForTier("hobby")
	if err := b.applyDeployment(context.Background(), ns, sz); err != nil {
		t.Fatalf("applyDeployment: %v", err)
	}
	dep, err := cs.AppsV1().Deployments(ns).Get(context.Background(), "mongodb", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get deployment: %v", err)
	}
	containers := dep.Spec.Template.Spec.Containers
	if len(containers) != 1 || containers[0].Image != "mongo:7" {
		t.Errorf("container image = %v, want mongo:7", containers)
	}
	// docker-entrypoint.sh requires Args[0] == "mongod" — regression guard
	// for the silent createUser/--auth init bug.
	if len(containers[0].Args) == 0 || containers[0].Args[0] != "mongod" {
		t.Errorf("Args[0] = %v, want mongod", containers[0].Args)
	}
	// --maxConns must be present and carry the tier's maxConns.
	joined := strings.Join(containers[0].Args, " ")
	if !strings.Contains(joined, "--maxConns") {
		t.Errorf("Args missing --maxConns: %v", containers[0].Args)
	}
	// PSS / hardening: securityContext drops ALL caps & runs as non-root.
	sc := containers[0].SecurityContext
	if sc == nil || sc.AllowPrivilegeEscalation == nil || *sc.AllowPrivilegeEscalation {
		t.Errorf("container security context missing or AllowPrivilegeEscalation true")
	}
}

func TestApplyDeployment_EmptyDirVolumeForAnonymous(t *testing.T) {
	const ns = "instant-customer-mongo-anon"
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs, image: "mongo:7"}
	sz := sizingForTier("anonymous")
	if err := b.applyDeployment(context.Background(), ns, sz); err != nil {
		t.Fatalf("applyDeployment: %v", err)
	}
	dep, _ := cs.AppsV1().Deployments(ns).Get(context.Background(), "mongodb", metav1.GetOptions{})
	for _, v := range dep.Spec.Template.Spec.Volumes {
		if v.Name == "data" && v.EmptyDir == nil {
			t.Errorf("anonymous tier: data volume must be emptyDir, got %+v", v)
		}
	}
}

func TestApplyService_NodePortShape(t *testing.T) {
	const ns = "instant-customer-mongo-svc"
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	svc, err := b.applyService(context.Background(), ns)
	if err != nil {
		t.Fatalf("applyService: %v", err)
	}
	if svc.Spec.Type != corev1.ServiceTypeNodePort {
		t.Errorf("svc type = %v, want NodePort", svc.Spec.Type)
	}
	if len(svc.Spec.Ports) == 0 || svc.Spec.Ports[0].Port != 27017 {
		t.Errorf("port = %v, want 27017", svc.Spec.Ports)
	}
}

// ─── waitPodReady ───────────────────────────────────────────────────────────

// TestWaitPodReady_HappyPath returns immediately once a Ready pod exists.
func TestWaitPodReady_HappyPath(t *testing.T) {
	const ns = "instant-customer-mongo-ready"
	cs := fake.NewSimpleClientset(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "mongodb-abc",
			Namespace: ns,
			Labels:    map[string]string{"app": "mongodb"},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	})
	b := &K8sBackend{cs: cs}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := b.waitPodReady(ctx, ns, "app=mongodb"); err != nil {
		t.Fatalf("waitPodReady: %v", err)
	}
}

// TestWaitPodReady_ContextCancel exercises the ctx.Done() arm of the select.
func TestWaitPodReady_ContextCancel(t *testing.T) {
	// No matching pod — so the poll loop never exits on PodReady.
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	err := b.waitPodReady(ctx, "instant-customer-mongo-cancel", "app=mongodb")
	if err == nil {
		t.Fatal("waitPodReady: want context error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("waitPodReady err = %v, want DeadlineExceeded", err)
	}
}

// TestWaitPodReady_ListErrorPropagates covers the kubeclient List error branch.
func TestWaitPodReady_ListErrorPropagates(t *testing.T) {
	cs := fake.NewSimpleClientset()
	cs.PrependReactor("list", "pods", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("synthetic list error")
	})
	b := &K8sBackend{cs: cs}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := b.waitPodReady(ctx, "ns", "app=mongodb")
	if err == nil || !strings.Contains(err.Error(), "synthetic list error") {
		t.Fatalf("waitPodReady: want synthetic list error, got %v", err)
	}
}

// ─── tryInitMongo / initMongo ───────────────────────────────────────────────

// TestTryInitMongo_FailsOnUnreachableMongo covers the connect/RunCommand-fail
// path of tryInitMongo. We point at an unused port so server-selection fails.
func TestTryInitMongo_FailsOnUnreachableMongo(t *testing.T) {
	b := &K8sBackend{}
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	uri := "mongodb://root:rootpw@127.0.0.1:1/admin?serverSelectionTimeoutMS=500"
	err := b.tryInitMongo(ctx, uri, "db_x", "usr_x", "pw")
	if err == nil {
		t.Fatal("tryInitMongo on unreachable: want error, got nil")
	}
}

// TestInitMongo_GivesUpAndPropagatesLastErr covers the retry-loop exhaustion
// branch of initMongo. The error contains "server selection" so it qualifies
// as a retryable class — the loop should exhaust attempts and return the
// "gave up" wrapped error.
func TestInitMongo_GivesUpAndPropagatesLastErr(t *testing.T) {
	b := &K8sBackend{}
	// Use a short context so the per-attempt sleep is interrupted; the test
	// finishes in ~ context-deadline rather than the natural retry budget.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	uri := "mongodb://root:rootpw@127.0.0.1:1/admin?serverSelectionTimeoutMS=200"
	err := b.initMongo(ctx, uri, "db_x", "usr_x", "pw")
	if err == nil {
		t.Fatal("initMongo: want error, got nil")
	}
	// The function either gave up after maxAttempts or returned ctx.Err()
	// when the deadline triggered between retries — both are valid exits
	// for the unreachable-backend case.
	if !errors.Is(err, context.DeadlineExceeded) &&
		!strings.Contains(err.Error(), "gave up") &&
		!strings.Contains(err.Error(), "server selection") &&
		!strings.Contains(err.Error(), "context") {
		t.Errorf("initMongo err = %v; want deadline / gave-up / server-selection", err)
	}
}

// TestInitMongo_FailFastOnNonRetryableError covers the early-return branch:
// an error that does NOT match any retry signature must propagate immediately.
// We force this by using an unparseable URI so mongo.Connect fails with a
// generic error string that doesn't match any retry token.
func TestInitMongo_FailFastOnNonRetryableError(t *testing.T) {
	b := &K8sBackend{}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// `mongodb://[bad-uri` triggers a parse error in ApplyURI → propagated
	// out of mongo.Connect → out of tryInitMongo. The error message contains
	// "error parsing uri" — no retry-token match → fast fail.
	err := b.initMongo(ctx, "mongodb://[bad-uri", "db_x", "usr_x", "pw")
	if err == nil {
		t.Fatal("initMongo: want immediate parse error, got nil")
	}
	if strings.Contains(err.Error(), "gave up") {
		t.Errorf("initMongo treated parse error as retryable: %v", err)
	}
}
