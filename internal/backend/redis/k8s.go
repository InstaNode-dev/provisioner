package redis

// k8s.go — K8sBackend provisions a dedicated Redis pod per token in its own namespace.
// Security model and architecture mirrors the postgres K8sBackend — see postgres/k8s.go.
//
// Configuration env vars:
//   K8S_EXTERNAL_HOST       — legacy NodePort hostname (kept for back-compat / fallback URL)
//   K8S_REDIS_PUBLIC_HOST   — hostname embedded in customer URLs when redis-proxy is fronting
//                             the cluster (default "redis.instanode.dev")
//   K8S_REDIS_IMAGE         — container image, default "redis:7-alpine"
//   K8S_STORAGE_CLASS       — PVC storage class, default "gp3"
//   K8S_REDIS_STORAGE_GI    — PVC size in GiB, default 10 (overridden by tier sizing)
//   K8S_KUBECONFIG          — path to kubeconfig file; empty = in-cluster
//
// # External access model
//
// Customer connection URLs are of the form `redis://:<pass>@<publicHost>:6379/0`. The
// redis-proxy (see redis-proxy/) listens on :6379 in the cluster, reads the client's
// first command (AUTH or HELLO AUTH), looks up `redis_route_by_password:<pass>` in
// Redis to find the dedicated pod, and forwards bytes transparently. The backend
// performs the real AUTH check.
//
// When K8S_REDIS_PUBLIC_HOST is empty, the URL falls back to the legacy
// `redis://:<pass>@<K8S_EXTERNAL_HOST>:<NodePort>/0` shape so resources remain
// reachable in environments without the proxy.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

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
)

const (
	redisK8sNsPrefix  = "instant-customer-"
	redisK8sRoleLabel = "instant.dev/role"
	redisK8sRoleValue = "customer-resource"
	redisK8sReadyTO   = 3 * time.Minute
	redisK8sReadyPoll = 3 * time.Second
)

// tierSizing maps a billing tier to k8s resource sizing for the provisioned Redis pod.
// Anonymous (24h trial) gets the smallest viable pod — still a real, dedicated Redis,
// just configured for low cost so the free tier scales. Each step up gives more
// headroom; team is large enough to satisfy real production workloads.
//
// maxClients is Redis's pod-wide cap on simultaneous client connections (CONFIG SET
// maxclients <N>). Per-user ACL maxconn would be cleaner but our connection URL uses
// the default user (no ACL), so pod-level is the natural lever. Since each pod is
// dedicated to one customer this is functionally a per-customer cap.
//
// maxmemoryMB is the Redis --maxmemory flag value in MB. A value of -1 means
// unlimited (the flag is omitted entirely). This enforces the per-resource memory
// limit advertised in plans.yaml at the Redis level. allkeys-lru eviction is used
// so Redis behaves as a cache (evicts old keys) rather than returning write errors
// when full. Values mirror plans.yaml redis_memory_mb:
//
//	anonymous: 5 MB, hobby: 50 MB, pro: 512 MB, growth: 1024 MB, team: unlimited (-1)
type tierSizing struct {
	cpuReq, memReq string
	cpuLim, memLim string
	pvcMi          int // PVC size in MiB (Redis pods are small relative to postgres)
	// quotaRequests / quotaLimits cap the whole namespace as defense-in-depth.
	qCPURequests, qMemRequests string
	qCPULimits, qMemLimits     string
	// maxClients is the Redis-level maxclients config applied via --maxclients flag.
	// Bounds total simultaneous TCP clients connected to this pod.
	maxClients int
	// maxmemoryMB is the Redis --maxmemory limit in MB applied at pod start.
	// -1 means unlimited (flag omitted). Mirrors plans.yaml redis_memory_mb.
	// allkeys-lru eviction policy is applied alongside this limit.
	maxmemoryMB int
}

func sizingForTier(tier string) tierSizing {
	switch tier {
	case "anonymous":
		// Anonymous trial: smallest practical pod.
		// pvcMi=0 → emptyDir: redis stays in-memory only, skipping the
		// 5-10s DOKS block-storage attach on cold provision.
		return tierSizing{
			cpuReq: "50m", memReq: "64Mi",
			cpuLim: "200m", memLim: "128Mi",
			pvcMi:        0,
			qCPURequests: "100m", qMemRequests: "128Mi",
			qCPULimits: "400m", qMemLimits: "256Mi",
			maxClients:  10,
			maxmemoryMB: 5, // plans.yaml: anonymous redis_memory_mb = 5
		}
	case "hobby":
		return tierSizing{
			cpuReq: "100m", memReq: "128Mi",
			cpuLim: "500m", memLim: "512Mi",
			pvcMi:        1024, // 1Gi
			qCPURequests: "200m", qMemRequests: "256Mi",
			qCPULimits: "1", qMemLimits: "1Gi",
			maxClients:  50,
			maxmemoryMB: 50, // plans.yaml: hobby redis_memory_mb = 50
		}
	case "pro":
		return tierSizing{
			cpuReq: "250m", memReq: "512Mi",
			cpuLim: "2", memLim: "2Gi",
			pvcMi:        10240, // 10Gi
			qCPURequests: "500m", qMemRequests: "1Gi",
			qCPULimits: "4", qMemLimits: "4Gi",
			maxClients:  200,
			maxmemoryMB: 512, // plans.yaml: pro redis_memory_mb = 512
		}
	case "growth":
		return tierSizing{
			cpuReq: "500m", memReq: "1Gi",
			cpuLim: "4", memLim: "4Gi",
			pvcMi:        51200, // 50Gi
			qCPURequests: "1", qMemRequests: "2Gi",
			qCPULimits: "8", qMemLimits: "8Gi",
			maxClients:  1000,
			maxmemoryMB: 1024, // plans.yaml: growth redis_memory_mb = 1024
		}
	case "team":
		return tierSizing{
			cpuReq: "500m", memReq: "1Gi",
			cpuLim: "4", memLim: "4Gi",
			pvcMi:        51200, // 50Gi
			qCPURequests: "1", qMemRequests: "2Gi",
			qCPULimits: "8", qMemLimits: "8Gi",
			maxClients:  1000,
			maxmemoryMB: -1, // unlimited — team dedicated pods have no memory cap
		}
	default:
		// Unknown tier → conservative hobby-equivalent sizing.
		return sizingForTier("hobby")
	}
}

// K8sBackend provisions a dedicated Redis pod per token.
type K8sBackend struct {
	cs            *kubernetes.Clientset
	storageClass  string // K8S_STORAGE_CLASS
	image         string // K8S_REDIS_IMAGE
	externalHost  string // K8S_EXTERNAL_HOST (legacy NodePort host; kept for back-compat)
	publicHost    string // K8S_REDIS_PUBLIC_HOST (e.g. redis.instanode.dev) — preferred URL host when set
	storageSizeGi int    // K8S_REDIS_STORAGE_GI (legacy ceiling; tier sizing overrides per-resource)

	// Route registry — written on every successful Provision so the Redis
	// routing proxy (redis-proxy/) can demux. Two key families are written:
	//   <routePrefix><token>            → <service-fqdn>:6379  (debugging / future token-based routing)
	//   <passwordPrefix><password>      → <service-fqdn>:6379  (consumed by the proxy)
	// When rdb == nil, no route records are written.
	rdb            *goredis.Client
	routePrefix    string
	passwordPrefix string
}

// newK8sBackend constructs a K8sBackend. publicHost (K8S_REDIS_PUBLIC_HOST) is
// the hostname embedded in customer URLs when the routing proxy is in front of
// the cluster — defaults via SetPublicHost / EnableRouteRegistry from the
// factory. externalHost is the legacy NodePort hostname; we still record it
// for any caller that needs the per-pod NodePort URL.
func newK8sBackend(kubeconfigPath, storageClass, image, externalHost string, storageSizeGi int) (*K8sBackend, error) {
	var rc *rest.Config
	var err error
	if kubeconfigPath != "" {
		rc, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	} else {
		rc, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("k8s redis: build config: %w", err)
	}
	slog.Info("k8s.redis.init", "api_host", rc.Host)
	cs, err := kubernetes.NewForConfig(rc)
	if err != nil {
		return nil, fmt.Errorf("k8s redis: new clientset: %w", err)
	}
	if storageClass == "" {
		storageClass = "gp3"
	}
	if image == "" {
		image = "redis:7-alpine"
	}
	if storageSizeGi <= 0 {
		storageSizeGi = 10
	}
	return &K8sBackend{cs: cs, storageClass: storageClass, image: image, externalHost: externalHost, storageSizeGi: storageSizeGi}, nil
}

// EnableRouteRegistry tells the K8sBackend to publish routing records to Redis
// after every successful Provision so the redis-proxy can forward client
// traffic to the dedicated pod. Two key families are written per resource:
//
//	<prefix><token>                 — debugging / future token-based lookup
//	<passwordPrefix><password>      — consumed by redis-proxy to demux by AUTH
//
// Safe to call once at startup; subsequent calls overwrite. Passing rdb=nil
// disables route registration (default).
func (b *K8sBackend) EnableRouteRegistry(rdb *goredis.Client, prefix string) {
	if prefix == "" {
		prefix = "redis_route:"
	}
	b.rdb = rdb
	b.routePrefix = prefix
	if b.passwordPrefix == "" {
		b.passwordPrefix = "redis_route_by_password:"
	}
}

// SetPasswordRoutePrefix overrides the password→backend key family. Default
// "redis_route_by_password:" matches the redis-proxy default. Must be called
// before any Provision to take effect.
func (b *K8sBackend) SetPasswordRoutePrefix(prefix string) {
	if prefix == "" {
		return
	}
	b.passwordPrefix = prefix
}

// SetPublicHost sets the hostname embedded in customer connection URLs when
// the redis-proxy is fronting the cluster. Empty value keeps the legacy
// K8S_EXTERNAL_HOST + NodePort URL shape.
func (b *K8sBackend) SetPublicHost(host string) { b.publicHost = host }

// Provision creates a dedicated Redis instance. The pod is started with --requirepass
// and --maxclients injected via the container command. No post-start init step needed
// (unlike Postgres).
func (b *K8sBackend) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	ns := redisK8sNsPrefix + token
	password, err := redisK8sRandHex(16)
	if err != nil {
		return nil, fmt.Errorf("k8s redis: rand pass: %w", err)
	}

	rollback := func(step string, cause error) error {
		slog.Error("k8s.redis.provision.rollback", "step", step, "namespace", ns, "error", cause)
		_ = b.cs.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
		return fmt.Errorf("k8s redis: %s: %w", step, cause)
	}

	// Use a fresh background context — pod startup can take minutes, far exceeding
	// any gRPC request deadline on the incoming ctx.
	provCtx, provCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer provCancel()

	sz := sizingForTier(tier)

	if err := b.applyNamespace(provCtx, ns); err != nil {
		return nil, fmt.Errorf("k8s redis: namespace: %w", err)
	}
	if err := b.applyNetworkPolicy(provCtx, ns, 6379); err != nil {
		return nil, rollback("network policy", err)
	}
	if err := b.applyResourceQuota(provCtx, ns, sz); err != nil {
		return nil, rollback("resource quota", err)
	}
	if err := b.applySecret(provCtx, ns, password); err != nil {
		return nil, rollback("secret", err)
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

	if err := b.waitPodReady(provCtx, ns, "app=redis"); err != nil {
		return nil, rollback("wait ready", err)
	}

	nodePort := int(svc.Spec.Ports[0].NodePort)

	// Customer-facing URL.
	//   With publicHost set (typical prod): redis://:<pass>@redis.instanode.dev:6379/0
	//     — the redis-proxy demuxes by AUTH password and forwards to the right pod.
	//   Without publicHost (legacy / dev without the proxy): falls back to the
	//     NodePort URL so the resource is still reachable from outside the cluster.
	var connURL string
	if b.publicHost != "" {
		connURL = fmt.Sprintf("redis://:%s@%s/0", password, b.publicHost)
	} else {
		connURL = fmt.Sprintf("redis://:%s@%s:%d/0", password, b.externalHost, nodePort)
	}

	// Route records consumed by redis-proxy. Failure here does NOT fail the
	// provision — the pod is functional over its NodePort, and customers using
	// the public URL will get a clear WRONGPASS at the proxy if the lookup
	// fails. Worth surfacing via slog.Warn.
	if b.rdb != nil {
		serviceFQDN := fmt.Sprintf("redis.%s.svc.cluster.local:6379", ns)
		regCtx, regCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := b.rdb.Set(regCtx, b.routePrefix+token, serviceFQDN, 0).Err(); err != nil {
			slog.Warn("k8s.redis.route_register_failed", "token", token, "error", err)
		} else {
			slog.Info("k8s.redis.route_registered", "token", token, "backend", serviceFQDN)
		}
		// The proxy consumes THIS key — it's the one that actually matters for
		// external connectivity through redis.instanode.dev.
		if err := b.rdb.Set(regCtx, b.passwordPrefix+password, serviceFQDN, 0).Err(); err != nil {
			slog.Warn("k8s.redis.password_route_register_failed", "token", token, "error", err)
		} else {
			slog.Info("k8s.redis.password_route_registered", "token", token, "backend", serviceFQDN)
		}
		regCancel()
	}

	slog.Info("k8s.redis.provisioned", "namespace", ns, "node_port", nodePort, "max_clients", sz.maxClients, "public_host", b.publicHost)
	return &Credentials{URL: connURL, KeyPrefix: "", ProviderResourceID: ns}, nil
}

// StorageBytes returns used_memory from the Redis INFO command.
//
// Returns (0, nil) when the customer namespace exists but is missing the
// modern `redis-auth` Secret or the `redis` Service. These are legacy
// resources provisioned before the platform standardised on those names —
// the resource is no longer reachable from this backend, but it's not an
// operational error worth a worker retry, just a stale row in the
// platform DB. Worker logs at debug level; user-facing storage_bytes
// stays at its last known value (or 0) for that resource.
//
// Any other Get error (network, RBAC, transient apiserver failure)
// propagates so the worker can retry.
func (b *K8sBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	ns := providerResourceID
	if ns == "" {
		ns = redisK8sNsPrefix + token
	}

	secret, err := b.cs.CoreV1().Secrets(ns).Get(ctx, "redis-auth", metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			slog.Debug("k8s redis.StorageBytes: legacy resource without redis-auth secret",
				"namespace", ns, "token", token)
			return 0, nil
		}
		return 0, fmt.Errorf("k8s redis.StorageBytes: get secret: %w", err)
	}
	svc, err := b.cs.CoreV1().Services(ns).Get(ctx, "redis", metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			slog.Debug("k8s redis.StorageBytes: legacy resource without redis service",
				"namespace", ns, "token", token)
			return 0, nil
		}
		return 0, fmt.Errorf("k8s redis.StorageBytes: get service: %w", err)
	}

	password := string(secret.Data["REDIS_PASSWORD"])
	rdb := goredis.NewClient(&goredis.Options{
		Addr:     fmt.Sprintf("%s:6379", svc.Spec.ClusterIP),
		Password: password,
	})
	defer rdb.Close()

	info, err := rdb.Info(ctx, "memory").Result()
	if err != nil {
		return 0, fmt.Errorf("k8s redis.StorageBytes: INFO memory: %w", err)
	}
	return parseUsedMemory(info), nil
}

// Deprovision deletes the customer namespace (cascading GC of all resources).
// When route registration is enabled, both route records are removed so the
// redis-proxy fails fast on a stale password instead of hitting a non-existent
// pod.
func (b *K8sBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	ns := providerResourceID
	if ns == "" {
		ns = redisK8sNsPrefix + token
	}

	// Read the password BEFORE deleting the namespace so we can clean up the
	// password→route key. Best-effort — if the secret is already gone (manual
	// cleanup / replay) we skip that key; a stale entry will fail-close at the
	// proxy when the pod disappears anyway.
	var password string
	if b.rdb != nil {
		if sec, err := b.cs.CoreV1().Secrets(ns).Get(ctx, "redis-auth", metav1.GetOptions{}); err == nil {
			password = string(sec.Data["REDIS_PASSWORD"])
		}
	}

	if err := b.cs.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		if !k8serrors.IsNotFound(err) {
			return fmt.Errorf("k8s redis.Deprovision: delete namespace %s: %w", ns, err)
		}
		slog.Info("k8s.redis.deprovision.namespace_already_gone", "namespace", ns)
	}
	if b.rdb != nil {
		delCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := b.rdb.Del(delCtx, b.routePrefix+token).Err(); err != nil {
			slog.Warn("k8s.redis.route_unregister_failed", "token", token, "error", err)
		}
		if password != "" {
			if err := b.rdb.Del(delCtx, b.passwordPrefix+password).Err(); err != nil {
				slog.Warn("k8s.redis.password_route_unregister_failed", "token", token, "error", err)
			}
		}
	}
	slog.Info("k8s.redis.deprovisioned", "namespace", ns)
	return nil
}

// RegradeResult is returned by Regrade to indicate what happened.
type RegradeResult struct {
	Applied          bool
	AppliedMaxmemory int64  // actual maxmemory set in bytes (0 = unlimited); 0 when Applied=false
	SkipReason       string // populated when Applied=false
}

// Regrade connects to the dedicated Redis pod for this resource and ensures
// its maxmemory matches the tier cap encoded in targetMaxmemoryMB:
//
//   - targetMaxmemoryMB > 0  → set maxmemory to that many MB + allkeys-lru policy,
//     then CONFIG REWRITE so the cap survives a pod restart.
//   - targetMaxmemoryMB <= 0 → unlimited tier (team/growth): set maxmemory to 0
//     (Redis "no cap") + CONFIG REWRITE so it explicitly overrides any leftover cap.
//
// Idempotent: reads CONFIG GET maxmemory first and short-circuits if the value
// already matches, returning Applied=false + SkipReason="already correct".
//
// Only k8s-backed (dedicated) pods are supported. The caller must NOT pass
// shared-backend resources here — they are identified by their provider_resource_id
// prefix "instant-customer-". Shared pods (local backend) have no per-tenant cap
// lever and are skipped by the server before this method is ever reached.
func (b *K8sBackend) Regrade(ctx context.Context, token, providerResourceID string, targetMaxmemoryMB int) (RegradeResult, error) {
	ns := providerResourceID
	if ns == "" {
		ns = redisK8sNsPrefix + token
	}

	// Read the password from the pod's auth Secret.
	secret, err := b.cs.CoreV1().Secrets(ns).Get(ctx, "redis-auth", metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			// Legacy resource without the redis-auth Secret (pre-standardisation).
			// Can't connect → treat as a soft skip so the reconciler logs + moves on.
			slog.Warn("k8s.redis.Regrade: redis-auth secret not found — skipping legacy resource",
				"namespace", ns, "token", token)
			return RegradeResult{Applied: false, SkipReason: "redis-auth secret not found (legacy resource)"}, nil
		}
		return RegradeResult{Applied: false}, fmt.Errorf("k8s redis.Regrade: get secret: %w", err)
	}

	svc, err := b.cs.CoreV1().Services(ns).Get(ctx, "redis", metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			slog.Warn("k8s.redis.Regrade: redis service not found — skipping legacy resource",
				"namespace", ns, "token", token)
			return RegradeResult{Applied: false, SkipReason: "redis service not found (legacy resource)"}, nil
		}
		return RegradeResult{Applied: false}, fmt.Errorf("k8s redis.Regrade: get service: %w", err)
	}

	password := string(secret.Data["REDIS_PASSWORD"])
	rdb := goredis.NewClient(&goredis.Options{
		Addr:     fmt.Sprintf("%s:6379", svc.Spec.ClusterIP),
		Password: password,
	})
	defer rdb.Close()

	// Compute the target in bytes (what Redis uses internally).
	// targetMaxmemoryMB <= 0 means unlimited → targetBytes = 0.
	var targetBytes int64
	if targetMaxmemoryMB > 0 {
		targetBytes = int64(targetMaxmemoryMB) * 1024 * 1024
	}

	// Read current maxmemory to detect whether a CONFIG SET is needed.
	vals, err := rdb.ConfigGet(ctx, "maxmemory").Result()
	if err != nil {
		return RegradeResult{Applied: false}, fmt.Errorf("k8s redis.Regrade: CONFIG GET maxmemory: %w", err)
	}
	var currentBytes int64
	if v, ok := vals["maxmemory"]; ok {
		fmt.Sscanf(v, "%d", &currentBytes)
	}

	if currentBytes == targetBytes {
		slog.Debug("k8s.redis.Regrade: maxmemory already correct — no-op",
			"namespace", ns, "token", token,
			"maxmemory_bytes", currentBytes,
			"target_bytes", targetBytes,
		)
		return RegradeResult{Applied: false, SkipReason: "already correct"}, nil
	}

	// Apply the new maxmemory. Use the Redis byte value directly for precision.
	// Redis accepts "0" as "unlimited" (no cap). CONFIG SET maxmemory accepts
	// an integer string (byte count). We avoid the "<N>mb" suffix so there is
	// no rounding ambiguity.
	maxmemStr := fmt.Sprintf("%d", targetBytes)
	if err := rdb.ConfigSet(ctx, "maxmemory", maxmemStr).Err(); err != nil {
		return RegradeResult{Applied: false}, fmt.Errorf("k8s redis.Regrade: CONFIG SET maxmemory: %w", err)
	}

	// Apply allkeys-lru policy for capped tiers so Redis evicts old keys
	// gracefully instead of returning write errors when full. For unlimited
	// tiers (targetBytes == 0) set the policy to noeviction (the Redis
	// default) to avoid accidental eviction of important data.
	policy := "noeviction"
	if targetBytes > 0 {
		policy = "allkeys-lru"
	}
	if err := rdb.ConfigSet(ctx, "maxmemory-policy", policy).Err(); err != nil {
		// Non-fatal — the cap is applied; policy mismatch only affects eviction
		// behaviour. Log and continue; the next reconciler tick will retry.
		slog.Warn("k8s.redis.Regrade: CONFIG SET maxmemory-policy failed (non-fatal)",
			"namespace", ns, "token", token, "policy", policy, "error", err)
	}

	// CONFIG REWRITE persists the change to the redis.conf file so the cap
	// survives a pod restart. Fail-soft: if there is no config file (pod
	// started without --save or no config mount), REWRITE errors but the
	// in-memory cap is still enforced until the next restart.
	if err := rdb.ConfigRewrite(ctx).Err(); err != nil {
		slog.Warn("k8s.redis.Regrade: CONFIG REWRITE failed (non-fatal, in-memory cap is active)",
			"namespace", ns, "token", token, "error", err)
	}

	slog.Info("k8s.redis.Regrade: applied",
		"namespace", ns, "token", token,
		"old_maxmemory_bytes", currentBytes,
		"new_maxmemory_bytes", targetBytes,
		"target_maxmemory_mb", targetMaxmemoryMB,
		"policy", policy,
	)
	return RegradeResult{Applied: true, AppliedMaxmemory: targetBytes}, nil
}

// --- private resource creators ---

func (b *K8sBackend) applyNamespace(ctx context.Context, ns string) error {
	nsObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
			Labels: map[string]string{
				redisK8sRoleLabel:                    redisK8sRoleValue,
				"pod-security.kubernetes.io/enforce": "baseline",
				"pod-security.kubernetes.io/warn":    "restricted",
			},
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
	_, err := b.cs.CoreV1().ResourceQuotas(ns).Create(ctx, &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-quota"},
		Spec:       corev1.ResourceQuotaSpec{Hard: hard},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applySecret(ctx context.Context, ns, password string) error {
	_, err := b.cs.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-auth"},
		StringData: map[string]string{"REDIS_PASSWORD": password},
	}, metav1.CreateOptions{})
	return err
}

func (b *K8sBackend) applyPVC(ctx context.Context, ns string, sz tierSizing) error {
	sc := b.storageClass
	_, err := b.cs.CoreV1().PersistentVolumeClaims(ns).Create(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-data"},
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
	runAsUser := int64(999) // redis user in redis:7-alpine
	fsGroup := int64(999)

	_, err := b.cs.AppsV1().Deployments(ns).Create(ctx, &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "redis"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "redis"}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "redis"}},
				Spec: corev1.PodSpec{
					AutomountServiceAccountToken: boolPtrR(false),
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: boolPtrR(true),
						RunAsUser:    &runAsUser,
						FSGroup:      &fsGroup,
						SeccompProfile: &corev1.SeccompProfile{
							Type: corev1.SeccompProfileTypeRuntimeDefault,
						},
					},
					Containers: []corev1.Container{{
						Name:  "redis",
						Image: b.image,
						// Tier-specific maxclients and maxmemory passed at start.
						// redis-server respects these flags before any client can connect,
						// so they enforce the caps without needing a post-start CONFIG SET.
						// allkeys-lru eviction means Redis behaves as a cache (evicts old
						// keys) rather than returning write errors when full — appropriate
						// because this platform positions Redis as a cache service.
						Command: func() []string {
							cmd := []string{
								"redis-server",
								"--requirepass", "$(REDIS_PASSWORD)",
								"--appendonly", "yes",
								"--dir", "/data",
								"--maxclients", fmt.Sprintf("%d", sz.maxClients),
							}
							// Only add --maxmemory when the tier has a defined cap.
							// -1 means unlimited (team/growth) — omit the flag so Redis
							// uses its default (no cap). This matches plans.yaml semantics
							// where -1 = unlimited.
							if sz.maxmemoryMB > 0 {
								cmd = append(cmd,
									"--maxmemory", fmt.Sprintf("%dmb", sz.maxmemoryMB),
									"--maxmemory-policy", "allkeys-lru",
								)
							}
							return cmd
						}(),
						Env: []corev1.EnvVar{{
							Name: "REDIS_PASSWORD",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{Name: "redis-auth"},
									Key:                  "REDIS_PASSWORD",
								},
							},
						}},
						Ports: []corev1.ContainerPort{{ContainerPort: 6379, Protocol: corev1.ProtocolTCP}},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(6379)},
							},
							InitialDelaySeconds: 2,
							PeriodSeconds:       2,
							FailureThreshold:    30,
						},
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &noPrivEsc,
							ReadOnlyRootFilesystem:   boolPtrR(true),
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
							{Name: "data", MountPath: "/data"},
							{Name: "tmp", MountPath: "/tmp"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "data", VolumeSource: redisDataVolumeSource(sz)},
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
		ObjectMeta: metav1.ObjectMeta{Name: "redis"},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: map[string]string{"app": "redis"},
			Ports:    []corev1.ServicePort{{Port: 6379, TargetPort: intstr.FromInt32(6379), Protocol: corev1.ProtocolTCP}},
		},
	}, metav1.CreateOptions{})
}

func (b *K8sBackend) waitPodReady(ctx context.Context, ns, labelSelector string) error {
	deadline := time.Now().Add(redisK8sReadyTO)
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
		case <-time.After(redisK8sReadyPoll):
		}
	}
	return fmt.Errorf("redis pod not ready after %s", redisK8sReadyTO)
}

// parseUsedMemory extracts used_memory from Redis INFO memory output.
func parseUsedMemory(info string) int64 {
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "used_memory:") {
			var n int64
			fmt.Sscanf(strings.TrimPrefix(line, "used_memory:"), "%d", &n)
			return n
		}
	}
	return 0
}

func redisK8sRandHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func boolPtrR(b bool) *bool { return &b }

// redisDataVolumeSource returns the right volume source for the data dir.
// Anonymous tier (pvcMi == 0) uses emptyDir: redis is in-memory only and
// data is ephemeral, so the DOKS block-storage attach is wasteful.
func redisDataVolumeSource(sz tierSizing) corev1.VolumeSource {
	if sz.pvcMi > 0 {
		return corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "redis-data"},
		}
	}
	return corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}
}
