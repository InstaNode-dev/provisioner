package mongo

// k8s.go — K8sBackend provisions a dedicated MongoDB pod per token in its own namespace.
// Security model mirrors postgres/k8s.go — see that file for architecture notes.
//
// Configuration env vars:
//   K8S_EXTERNAL_HOST       — legacy NodePort hostname (kept for back-compat / fallback URL)
//   K8S_MONGO_PUBLIC_HOST   — hostname embedded in customer URLs when mongo-proxy is fronting
//                             the cluster (default "mongo.instanode.dev")
//   K8S_MONGO_IMAGE         — container image, default "mongo:7"
//   K8S_STORAGE_CLASS       — PVC storage class, default "gp3"
//   K8S_MONGO_STORAGE_GI    — PVC size in GiB, default 50 (overridden by tier sizing)
//   K8S_KUBECONFIG          — path to kubeconfig; empty = in-cluster
//
// # External access model
//
// Customer connection URLs are of the form
// `mongodb://<user>:<pass>@<publicHost>:27017/<db>?authSource=<db>`. The
// mongo-proxy (see mongo-proxy/) listens on :27017 in the cluster, reads
// enough of the client's pre-auth handshake to extract the SCRAM username
// from saslStart, looks up `mongo_route_by_user:<user>` in Redis to find the
// dedicated pod, and forwards bytes transparently. The backend performs the
// real SCRAM check.
//
// When K8S_MONGO_PUBLIC_HOST is empty, the URL falls back to the legacy
// `mongodb://<user>:<pass>@<K8S_EXTERNAL_HOST>:<NodePort>/<db>?authSource=<db>`
// shape so resources remain reachable in environments without the proxy.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	mongoclient "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	goredis "github.com/redis/go-redis/v9"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"instant.dev/provisioner/internal/ctxkeys"
)

const (
	mongoK8sNsPrefix  = "instant-customer-"
	mongoK8sRoleLabel = "instant.dev/role"
	mongoK8sRoleValue = "customer-resource"
	mongoK8sReadyTO   = 3 * time.Minute
	mongoK8sReadyPoll = 3 * time.Second

	// mongoK8sOwnerTeamLabel is applied to dedicated customer namespaces.
	// Mirrors the constant in postgres/k8s.go — must stay in sync.
	// Pentest fix: 2026-05-16.
	mongoK8sOwnerTeamLabel = "instant.dev/owner-team"

	// anonRouteKeyTTL is the expiry applied to the mongo-proxy route-registry
	// keys (<routePrefix><dbName> and <userPrefix><appUser>) for ANONYMOUS
	// resources only. Deprovision deletes these explicitly, but that delete is
	// best-effort — if the admin Secret read fails, the user-route key would
	// otherwise leak forever. A TTL lets orphans self-heal.
	//
	// 365 days is deliberately far longer than the 24h anonymous resource
	// lifetime, so an in-use anonymous route is never expired out from under a
	// running pod before its own 24h teardown.
	//
	// P1-A fix (2026-05-17): paid/permanent resources MUST NOT carry this TTL.
	// They are long-lived and are only ever re-Set on Provision; a paid resource
	// that is never re-provisioned would silently lose its proxy route at 1 year
	// and become unreachable. routeKeyTTLForTier returns persistRouteKey (no
	// expiry) for every non-anonymous tier — matching the postgres and queue
	// backends, which already write their route keys with TTL 0.
	anonRouteKeyTTL = 365 * 24 * time.Hour

	// persistRouteKey is the TTL value (go-redis: 0 == no expiry) used for
	// paid/permanent resources so their proxy route never expires while the
	// resource is alive. Deprovision still explicitly deletes the key.
	persistRouteKey = time.Duration(0)

	// anonymousTier is the billing tier string for 24h-TTL trial resources.
	anonymousTier = "anonymous"
)

// routeKeyTTLForTier returns the route-registry key TTL for a given billing
// tier. Anonymous resources get a long self-healing TTL (anonRouteKeyTTL);
// every paid/permanent tier gets persistRouteKey (no expiry) so a long-lived
// resource can never lose its proxy route. An empty/unknown tier is treated
// as paid (persistent) — failing safe toward "never lose a live route".
func routeKeyTTLForTier(tier string) time.Duration {
	if tier == anonymousTier {
		return anonRouteKeyTTL
	}
	return persistRouteKey
}

// tierSizing maps a billing tier to k8s resource sizing for the provisioned Mongo pod.
// Anonymous (24h trial) gets the smallest viable pod — still a real, dedicated Mongo,
// just configured for low cost so the free tier scales. Each step up gives more
// headroom; team is large enough for real production workloads.
//
// Mongo doesn't have a per-user connection limit, only --maxConns at the pod level.
// Because each pod is dedicated to one customer this is functionally a per-customer
// cap — we pass it via the container command. CPU/RAM limits + maxConns together
// provide noisy-neighbor protection without needing per-user enforcement.
type tierSizing struct {
	cpuReq, memReq string
	cpuLim, memLim string
	pvcMi          int // PVC size in MiB (lets us go below 1Gi for anonymous)
	// quotaRequests / quotaLimits cap the whole namespace as defense-in-depth.
	qCPURequests, qMemRequests string
	qCPULimits, qMemLimits     string
	// maxConns is the pod-wide cap on simultaneous client connections. Mongo
	// has no per-user equivalent; this is informational at the tier level but
	// load-bearing as a pod-wide DoS guard.
	maxConns int
}

func sizingForTier(tier string) tierSizing {
	switch tier {
	case "anonymous":
		// Anonymous trial: smallest practical pod.
		// Memory limit MUST be > 256Mi or the docker-entrypoint.sh init phase
		// gets OOM-killed: the entrypoint briefly runs TWO mongod processes
		// (a temp 127.0.0.1 instance to seed the root user + the real one),
		// and WiredTiger's default cache_size is 256MB, so 256Mi total is
		// instant OOM. 384Mi is the smallest size that survives init + serves
		// a low-traffic anonymous workload reliably.
		// pvcMi=0 → emptyDir: skips the 5-10s DOKS block-storage attach on
		// cold provision. Anonymous data is 24h TTL so ephemeral is fine.
		return tierSizing{
			cpuReq: "50m", memReq: "192Mi",
			cpuLim: "250m", memLim: "384Mi",
			pvcMi:        0,
			qCPURequests: "100m", qMemRequests: "384Mi",
			qCPULimits: "500m", qMemLimits: "640Mi",
			maxConns: 20,
		}
	case "hobby":
		return tierSizing{
			cpuReq: "100m", memReq: "256Mi",
			cpuLim: "500m", memLim: "1Gi",
			pvcMi:        1024, // 1Gi
			qCPURequests: "200m", qMemRequests: "512Mi",
			qCPULimits: "1", qMemLimits: "2Gi",
			maxConns: 100,
		}
	case "pro":
		return tierSizing{
			cpuReq: "250m", memReq: "1Gi",
			cpuLim: "2", memLim: "2Gi",
			pvcMi:        10240, // 10Gi
			qCPURequests: "500m", qMemRequests: "2Gi",
			qCPULimits: "4", qMemLimits: "4Gi",
			maxConns: 500,
		}
	case "team", "growth":
		return tierSizing{
			cpuReq: "500m", memReq: "2Gi",
			cpuLim: "4", memLim: "4Gi",
			pvcMi:        51200, // 50Gi
			qCPURequests: "1", qMemRequests: "4Gi",
			qCPULimits: "8", qMemLimits: "8Gi",
			maxConns: 2000,
		}
	default:
		return sizingForTier("hobby")
	}
}

// K8sBackend provisions a dedicated MongoDB pod per token.
type K8sBackend struct {
	cs            *kubernetes.Clientset
	storageClass  string // K8S_STORAGE_CLASS
	image         string // K8S_MONGO_IMAGE
	externalHost  string // K8S_EXTERNAL_HOST (legacy NodePort host; kept for back-compat)
	publicHost    string // K8S_MONGO_PUBLIC_HOST (e.g. mongo.instanode.dev) — preferred URL host when set
	storageSizeGi int    // K8S_MONGO_STORAGE_GI (legacy ceiling; tier sizing overrides per-resource)

	// Route registry — written on every successful Provision so the Mongo
	// routing proxy (mongo-proxy/) can demux. Two key families are written:
	//   <routePrefix><dbName>           → <service-fqdn>:27017  (debugging / future db-based routing)
	//   <userPrefix><appUser>           → <service-fqdn>:27017  (consumed by the proxy)
	// When rdb == nil, no route records are written.
	rdb         *goredis.Client
	routePrefix string
	userPrefix  string
}

func newK8sBackend(kubeconfigPath, storageClass, image, externalHost string, storageSizeGi int) (*K8sBackend, error) {
	var rc *rest.Config
	var err error
	if kubeconfigPath != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("k8s mongo: build config: %w", err)
	}
	slog.Info("k8s.mongo.init", "api_host", rc.Host)
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("k8s mongo: new clientset: %w", err)
	}
	if storageClass == "" {
		storageClass = "gp3"
	}
	if image == "" {
		image = "mongo:7"
	}
	if storageSizeGi <= 0 {
		storageSizeGi = 50
	}
	return &K8sBackend{cs: cs, storageClass: storageClass, image: image, externalHost: externalHost, storageSizeGi: storageSizeGi}, nil
}

// EnableRouteRegistry tells the K8sBackend to publish routing records to Redis
// after every successful Provision so the mongo-proxy can forward client
// traffic to the dedicated pod. Two key families are written per resource:
//
//	<prefix><dbName>                — debugging / future db-based lookup
//	<userPrefix><appUser>           — consumed by mongo-proxy to demux by SCRAM username
//
// Safe to call once at startup; subsequent calls overwrite. Passing rdb=nil
// disables route registration (default).
func (b *K8sBackend) EnableRouteRegistry(rdb *goredis.Client, prefix string) {
	if prefix == "" {
		prefix = "mongo_route:"
	}
	b.rdb = rdb
	b.routePrefix = prefix
	if b.userPrefix == "" {
		b.userPrefix = "mongo_route_by_user:"
	}
}

// SetPasswordRoutePrefix overrides the user→backend key family. Default
// "mongo_route_by_user:" matches the mongo-proxy default. The name mirrors
// the redis backend's SetPasswordRoutePrefix even though Mongo routes by
// username (not password) — keeping a single naming convention for the
// "route by auth identity" knob across both backends.
func (b *K8sBackend) SetPasswordRoutePrefix(prefix string) {
	if prefix == "" {
		return
	}
	b.userPrefix = prefix
}

// SetPublicHost sets the hostname embedded in customer connection URLs when
// the mongo-proxy is fronting the cluster. Empty value keeps the legacy
// K8S_EXTERNAL_HOST + NodePort URL shape.
func (b *K8sBackend) SetPublicHost(host string) { b.publicHost = host }

// Provision creates a dedicated MongoDB instance with a restricted app user.
func (b *K8sBackend) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	ns := mongoK8sNsPrefix + token
	// Canonical, collision-free names (see naming.go). The pre-fix scheme used
	// mongoK8sShort, which truncated the token to 12 chars — any two tokens
	// sharing the first 12 hex digits collided onto the same DB and user.
	dbName := mongoDBName(token)
	adminPass, err := mongoK8sRandHex(16)
	if err != nil {
		return nil, fmt.Errorf("k8s mongo: rand admin pass: %w", err)
	}
	appUser := mongoUserName(token)
	appPass, err := mongoK8sRandHex(16)
	if err != nil {
		return nil, fmt.Errorf("k8s mongo: rand app pass: %w", err)
	}

	rollback := func(step string, cause error) error {
		slog.Error("k8s.mongo.provision.rollback", "step", step, "namespace", ns, "error", cause)
		_ = b.cs.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
		return fmt.Errorf("k8s mongo: %s: %w", step, cause)
	}

	// Use a fresh background context — pod startup can take minutes, far exceeding
	// any gRPC request deadline on the incoming ctx.
	// Carry the teamID value forward so applyNamespace can label the namespace
	// with instant.dev/owner-team (pentest 2026-05-16 fix).
	provCtx, provCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer provCancel()
	if teamID, ok := ctx.Value(ctxkeys.TeamIDKey).(string); ok && teamID != "" {
		provCtx = context.WithValue(provCtx, ctxkeys.TeamIDKey, teamID)
	}

	sz := sizingForTier(tier)

	if err := b.applyNamespace(provCtx, ns); err != nil {
		return nil, fmt.Errorf("k8s mongo: namespace: %w", err)
	}
	if err := b.applyNetworkPolicy(provCtx, ns, 27017); err != nil {
		return nil, rollback("network policy", err)
	}
	if err := b.applyResourceQuota(provCtx, ns, sz); err != nil {
		return nil, rollback("resource quota", err)
	}
	if err := b.applyAdminSecret(provCtx, ns, adminPass); err != nil {
		return nil, rollback("admin secret", err)
	}
	if sz.pvcMi > 0 {
		if err := b.applyPVC(provCtx, ns, sz); err != nil {
			return nil, rollback("pvc", err)
		}
	}
	if err := b.applyDeployment(provCtx, ns, sz); err != nil {
		return nil, rollback("deployment", err)
	}
	svc, err := b.applyService(provCtx, ns)
	if err != nil {
		return nil, rollback("service", err)
	}

	if err := b.waitPodReady(provCtx, ns, "app=mongodb"); err != nil {
		return nil, rollback("wait ready", err)
	}

	clusterIP := svc.Spec.ClusterIP
	nodePort := int(svc.Spec.Ports[0].NodePort)

	// Force SCRAM-SHA-256: the mongo:7 image only initialises the root user
	// with SHA-256, but the Go driver's negotiator can pick SHA-1 first which
	// the server then rejects. Pinning the mechanism removes the race.
	adminURI := fmt.Sprintf("mongodb://root:%s@%s:27017/admin?authMechanism=SCRAM-SHA-256", adminPass, clusterIP)
	if err := b.initMongo(provCtx, adminURI, dbName, appUser, appPass); err != nil {
		return nil, rollback("init mongo", err)
	}

	// Customer-facing URL.
	//   With publicHost set (typical prod):
	//     mongodb://<user>:<pass>@mongo.instanode.dev:27017/<db>?authSource=<db>
	//     — the mongo-proxy demuxes by SCRAM username (extracted from saslStart)
	//       and forwards to the right pod.
	//   Without publicHost (legacy / dev without the proxy): falls back to the
	//     NodePort URL so the resource is still reachable from outside the cluster.
	var connURL string
	if b.publicHost != "" {
		connURL = fmt.Sprintf("mongodb://%s:%s@%s:27017/%s?authSource=%s",
			appUser, appPass, b.publicHost, dbName, dbName)
	} else {
		connURL = fmt.Sprintf("mongodb://%s:%s@%s:%d/%s?authSource=%s",
			appUser, appPass, b.externalHost, nodePort, dbName, dbName)
	}

	// Route records consumed by mongo-proxy. Failure here does NOT fail the
	// provision — the pod is functional over its NodePort, and customers using
	// the public URL will get a clean network error at the proxy if the lookup
	// fails. Worth surfacing via slog.Warn.
	if b.rdb != nil {
		serviceFQDN := fmt.Sprintf("mongodb.%s.svc.cluster.local:27017", ns)
		regCtx, regCancel := context.WithTimeout(context.Background(), 3*time.Second)
		// P1-A: anonymous resources get a long self-healing TTL; paid/permanent
		// resources get persistRouteKey (no expiry) so a long-lived resource
		// that is never re-provisioned cannot lose its proxy route.
		routeTTL := routeKeyTTLForTier(tier)
		if err := b.rdb.Set(regCtx, b.routePrefix+dbName, serviceFQDN, routeTTL).Err(); err != nil {
			slog.Warn("k8s.mongo.route_register_failed", "db", dbName, "error", err)
		} else {
			slog.Info("k8s.mongo.route_registered", "db", dbName, "backend", serviceFQDN)
		}
		// The proxy consumes THIS key — it's the one that actually matters
		// for external connectivity through mongo.instanode.dev.
		if err := b.rdb.Set(regCtx, b.userPrefix+appUser, serviceFQDN, routeTTL).Err(); err != nil {
			slog.Warn("k8s.mongo.user_route_register_failed", "user", appUser, "error", err)
		} else {
			slog.Info("k8s.mongo.user_route_registered", "user", appUser, "backend", serviceFQDN)
		}
		regCancel()
	}

	slog.Info("k8s.mongo.provisioned", "namespace", ns, "node_port", nodePort, "max_conns", sz.maxConns, "public_host", b.publicHost)
	return &Credentials{URL: connURL, DatabaseName: dbName, ProviderResourceID: ns}, nil
}

// StorageBytes returns the storageSize from dbStats for the customer database.
func (b *K8sBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	ns := providerResourceID
	if ns == "" {
		ns = mongoK8sNsPrefix + token
	}

	// Fail-soft when the customer namespace exists but is missing the
	// modern mongo-admin Secret or mongodb Service — these are legacy
	// rows in the platform DB whose pods are gone; nothing actionable
	// for the worker to retry. Other Get failures still propagate.
	secret, err := b.cs.CoreV1().Secrets(ns).Get(ctx, "mongo-admin", metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			slog.Debug("k8s mongo.StorageBytes: legacy resource without mongo-admin secret",
				"namespace", ns, "token", token)
			return 0, nil
		}
		return 0, fmt.Errorf("k8s mongo.StorageBytes: get secret: %w", err)
	}
	svc, err := b.cs.CoreV1().Services(ns).Get(ctx, "mongodb", metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			slog.Debug("k8s mongo.StorageBytes: legacy resource without mongodb service",
				"namespace", ns, "token", token)
			return 0, nil
		}
		return 0, fmt.Errorf("k8s mongo.StorageBytes: get service: %w", err)
	}

	adminPass := string(secret.Data["MONGO_INITDB_ROOT_PASSWORD"])
	uri := fmt.Sprintf("mongodb://root:%s@%s:27017/admin", adminPass, svc.Spec.ClusterIP)

	clientOpts := options.Client().ApplyURI(uri).SetServerSelectionTimeout(5 * time.Second)
	client, err := mongoclient.Connect(ctx, clientOpts)
	if err != nil {
		return 0, fmt.Errorf("k8s mongo.StorageBytes: connect: %w", err)
	}
	defer client.Disconnect(ctx)

	// Try the canonical DB name first, then every legacy scheme. A pod
	// provisioned before the P0-5 naming fix holds its data under the legacy
	// 12-char-truncated name; probing only the canonical name would read 0
	// bytes and silently un-enforce the customer's Mongo quota.
	var lastErr error
	for _, dbName := range legacyMongoDBNames(token) {
		var result bson.M
		if err := client.Database(dbName).RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&result); err != nil {
			lastErr = err
			slog.Debug("k8s mongo.StorageBytes: dbStats miss for candidate", "namespace", ns, "db", dbName, "error", err)
			continue
		}
		if v, ok := result["storageSize"]; ok {
			switch n := v.(type) {
			case int32:
				return int64(n), nil
			case int64:
				return n, nil
			case float64:
				return int64(n), nil
			}
		}
		return 0, nil
	}
	if lastErr != nil {
		return 0, fmt.Errorf("k8s mongo.StorageBytes: dbStats (all candidates): %w", lastErr)
	}
	return 0, nil
}

// Deprovision deletes the customer namespace (cascading GC of all resources).
// When route registration is enabled, both route records are removed so the
// mongo-proxy fails fast on a stale username instead of hitting a non-existent
// pod.
func (b *K8sBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	ns := providerResourceID
	if ns == "" {
		ns = mongoK8sNsPrefix + token
	}
	if err := b.cs.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		if !k8serrors.IsNotFound(err) {
			return fmt.Errorf("k8s mongo.Deprovision: delete namespace %s: %w", ns, err)
		}
		slog.Info("k8s.mongo.deprovision.namespace_already_gone", "namespace", ns)
	}
	if b.rdb != nil {
		// Delete route keys for the canonical name AND every legacy scheme —
		// the pod was registered under whichever scheme was current when it
		// was provisioned, and a stale route key would leave the mongo-proxy
		// forwarding to a deleted pod.
		delCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for _, dbName := range legacyMongoDBNames(token) {
			if err := b.rdb.Del(delCtx, b.routePrefix+dbName).Err(); err != nil {
				slog.Warn("k8s.mongo.route_unregister_failed", "db", dbName, "error", err)
			}
		}
		for _, appUser := range legacyMongoUserNames(token) {
			if err := b.rdb.Del(delCtx, b.userPrefix+appUser).Err(); err != nil {
				slog.Warn("k8s.mongo.user_route_unregister_failed", "user", appUser, "error", err)
			}
		}
	}
	slog.Info("k8s.mongo.deprovisioned", "namespace", ns)
	return nil
}

// --- private resource creators ---

func (b *K8sBackend) applyNamespace(ctx context.Context, ns string) error {
	labels := map[string]string{
		mongoK8sRoleLabel:                    mongoK8sRoleValue,
		"pod-security.kubernetes.io/enforce": "baseline",
		"pod-security.kubernetes.io/warn":    "restricted",
	}
	// SECURITY FIX (pentest 2026-05-16): label the namespace with the owning
	// team UUID when provided. The deploy-side NetworkPolicy combines this label
	// with role=customer-resource to scope DB-port egress per-team.
	if teamID, ok := ctx.Value(ctxkeys.TeamIDKey).(string); ok && teamID != "" {
		labels[mongoK8sOwnerTeamLabel] = teamID
	}
	nsObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   ns,
			Labels: labels,
		},
	}
	_, err := b.cs.CoreV1().Namespaces().Create(ctx, nsObj, metav1.CreateOptions{})
	if err == nil {
		return nil
	}
	if !k8serrors.IsAlreadyExists(err) {
		return err
	}
	existing, getErr := b.cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	if getErr != nil || existing.Status.Phase != corev1.NamespaceTerminating {
		return err
	}
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
		_, getErr = b.cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
		if k8serrors.IsNotFound(getErr) {
			_, err = b.cs.CoreV1().Namespaces().Create(ctx, nsObj, metav1.CreateOptions{})
			return err
		}
	}
	return fmt.Errorf("namespace %s still terminating after 2 minutes", ns)
}

func (b *K8sBackend) applyNetworkPolicy(ctx context.Context, ns string, dbPort int) error {
	proto := corev1.ProtocolTCP
	udp := corev1.ProtocolUDP
	dbP := intstr.FromInt32(int32(dbPort))
	dns := intstr.FromInt32(53)
	_, err := b.cs.NetworkingV1().NetworkPolicies(ns).Create(ctx, &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "default-deny"},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{Ports: []networkingv1.NetworkPolicyPort{{Protocol: &proto, Port: &dbP}}},
			},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{
					Ports: []networkingv1.NetworkPolicyPort{
						{Protocol: &proto, Port: &dns},
						{Protocol: &udp, Port: &dns},
					},
					To: []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{}}},
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyResourceQuota(ctx context.Context, ns string, sz tierSizing) error {
	_, err := b.cs.CoreV1().ResourceQuotas(ns).Create(ctx, &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-quota"},
		Spec:       corev1.ResourceQuotaSpec{Hard: mongoQuotaHard(sz)},
	}, metav1.CreateOptions{})
	return err
}

func mongoQuotaHard(sz tierSizing) corev1.ResourceList {
	hard := corev1.ResourceList{
		corev1.ResourceRequestsCPU:    resource.MustParse(sz.qCPURequests),
		corev1.ResourceRequestsMemory: resource.MustParse(sz.qMemRequests),
		corev1.ResourceLimitsCPU:      resource.MustParse(sz.qCPULimits),
		corev1.ResourceLimitsMemory:   resource.MustParse(sz.qMemLimits),
		corev1.ResourcePods:           resource.MustParse("3"),
	}
	if sz.pvcMi > 0 {
		hard["persistentvolumeclaims"] = resource.MustParse("2")
	}
	return hard
}

func (b *K8sBackend) applyAdminSecret(ctx context.Context, ns, adminPass string) error {
	_, err := b.cs.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mongo-admin"},
		StringData: map[string]string{
			"MONGO_INITDB_ROOT_USERNAME": "root",
			"MONGO_INITDB_ROOT_PASSWORD": adminPass,
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyPVC(ctx context.Context, ns string, sz tierSizing) error {
	sc := b.storageClass
	_, err := b.cs.CoreV1().PersistentVolumeClaims(ns).Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "mongo-data"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &sc,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(fmt.Sprintf("%dMi", sz.pvcMi)),
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyDeployment(ctx context.Context, ns string, sz tierSizing) error {
	replicas := int32(1)
	noPrivEsc := false
	runAsUser := int64(999) // mongodb UID in the official mongo:7 image
	fsGroup := int64(999)   // mongodb GID

	_, err := b.cs.AppsV1().Deployments(ns).Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "mongodb"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "mongodb"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "mongodb"}},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: mongoK8sBoolPtr(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsUser:  &runAsUser,
						RunAsGroup: &fsGroup,
						FSGroup:    &fsGroup,
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Name:  "mongodb",
						Image: b.image,
						// docker-entrypoint.sh requires the first arg to be literally "mongod"
						// for it to run its initialisation logic (which creates the
						// MONGO_INITDB_ROOT_USERNAME root user from the secret env vars).
						// If the first arg is anything else (e.g. "--bind_ip_all"), the
						// entrypoint just execs the args directly and never creates the
						// root user — leaving --auth turned on with no users, which is
						// exactly the AuthenticationFailed loop we hit before this fix.
						// See https://github.com/docker-library/mongo/blob/master/docker-entrypoint.sh
						// (the `if [ "$originalArgOne" = 'mongod' ]` check.)
						Args: []string{
							"mongod",
							"--bind_ip_all",
							"--auth",
							"--maxConns", fmt.Sprintf("%d", sz.maxConns),
						},
						Ports: []corev1.ContainerPort{{ContainerPort: 27017, Protocol: corev1.ProtocolTCP}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(27017)},
							},
							InitialDelaySeconds: 10,
							PeriodSeconds:       3,
							FailureThreshold:    30,
						},
						EnvFrom: []corev1.EnvFromSource{{
							SecretRef: &corev1.SecretEnvSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: "mongo-admin"},
							},
						}},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &noPrivEsc,
							RunAsNonRoot:             mongoK8sBoolPtr(true),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse(sz.cpuReq),
								corev1.ResourceMemory: resource.MustParse(sz.memReq),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse(sz.cpuLim),
								corev1.ResourceMemory: resource.MustParse(sz.memLim),
							},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/data/db"},
							{Name: "tmp", MountPath: "/tmp"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "data", VolumeSource: mongoDataVolumeSource(sz)},
						{Name: "tmp", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyService(ctx context.Context, ns string) (*corev1.Service, error) {
	return b.cs.CoreV1().Services(ns).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "mongodb"},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: map[string]string{"app": "mongodb"},
			Ports:    []corev1.ServicePort{{Port: 27017, TargetPort: intstr.FromInt32(27017), Protocol: corev1.ProtocolTCP}},
		},
	}, metav1.CreateOptions{})
}

func (b *K8sBackend) waitPodReady(ctx context.Context, ns, labelSelector string) error {
	deadline := time.Now().Add(mongoK8sReadyTO)
	for time.Now().Before(deadline) {
		pods, err := b.cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: labelSelector})
		if err != nil {
			return err
		}
		for i := range pods.Items {
			for _, cond := range pods.Items[i].Status.Conditions {
				if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(mongoK8sReadyPoll):
		}
	}
	return fmt.Errorf("mongodb pod not ready after %s", mongoK8sReadyTO)
}

// initMongo connects as admin and creates a restricted app user.
// The user is created IN the customer database (not admin), so the connection URL
// can use authSource=dbName — the user's authenticating database matches where they live.
//
// Retry rationale: the official mongo image's entrypoint script creates the
// MONGO_INITDB_ROOT_USERNAME *after* the server starts accepting TCP connections.
// Our k8s readiness probe only checks port 27017 is open, so initMongo can race
// the init script. Retry on AuthenticationFailed for up to ~30s to ride out that
// window. Other errors fail fast.
func (b *K8sBackend) initMongo(ctx context.Context, adminURI, dbName, appUser, appPass string) error {
	const (
		maxAttempts = 15
		retryDelay  = 2 * time.Second
	)
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := b.tryInitMongo(ctx, adminURI, dbName, appUser, appPass)
		if err == nil {
			return nil
		}
		lastErr = err
		msg := err.Error()
		// Retry on auth races (root user not yet created by entrypoint script)
		// and topology/handshake transients.
		if !strings.Contains(msg, "AuthenticationFailed") &&
			!strings.Contains(msg, "auth error") &&
			!strings.Contains(msg, "server selection") &&
			!strings.Contains(msg, "connection refused") {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
		}
	}
	return fmt.Errorf("initMongo: gave up after %d attempts: %w", maxAttempts, lastErr)
}

func (b *K8sBackend) tryInitMongo(ctx context.Context, adminURI, dbName, appUser, appPass string) error {
	clientOpts := options.Client().ApplyURI(adminURI).SetServerSelectionTimeout(5 * time.Second)
	client, err := mongoclient.Connect(ctx, clientOpts)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer client.Disconnect(ctx)

	// Create the user in dbName (not admin). This way authSource=dbName works in the
	// connection URL. MongoDB creates the database implicitly on first write.
	customerDB := client.Database(dbName)
	cmd := bson.D{
		{Key: "createUser", Value: appUser},
		{Key: "pwd", Value: appPass},
		{Key: "roles", Value: bson.A{
			bson.D{
				{Key: "role", Value: "readWrite"},
				{Key: "db", Value: dbName},
			},
		}},
	}
	if err := customerDB.RunCommand(ctx, cmd).Err(); err != nil {
		return fmt.Errorf("createUser: %w", err)
	}
	return nil
}

// NOTE: the former mongoK8sShort helper (truncate dash-stripped token to 12
// chars) was removed in the P0-5 fix. It was collision-prone — see naming.go,
// which now owns all DB/user name derivation for both backends.

func mongoK8sRandHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func mongoK8sBoolPtr(b bool) *bool { return &b }

// mongoDataVolumeSource returns the right volume source for /data/db.
// Anonymous tier (pvcMi == 0) uses emptyDir — skips DOKS block-storage attach.
// WiredTiger writes still work; data is lost on pod restart, fine for 24h TTL.
func mongoDataVolumeSource(sz tierSizing) corev1.VolumeSource {
	if sz.pvcMi > 0 {
		return corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "mongo-data"},
		}
	}
	return corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}
}
