package redis

// k8s.go — K8sBackend provisions a dedicated Redis pod per token in its own namespace.
// Security model and architecture mirrors the postgres K8sBackend — see postgres/k8s.go.
//
// Configuration env vars:
//
//	K8S_EXTERNAL_HOST       — legacy NodePort hostname (kept for back-compat / fallback URL)
//	K8S_REDIS_PUBLIC_HOST   — hostname embedded in customer URLs when redis-proxy is fronting
//	                          the cluster (default "redis.instanode.dev")
//	K8S_REDIS_IMAGE         — container image, default "redis:7-alpine"
//	K8S_STORAGE_CLASS       — PVC storage class, default "gp3"
//	K8S_REDIS_STORAGE_GI    — PVC size in GiB, default 10 (overridden by tier sizing)
//	K8S_KUBECONFIG          — path to kubeconfig file; empty = in-cluster
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
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/http"
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
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"instant.dev/provisioner/internal/ctxkeys"
)

// PodExecor is an abstraction over the k8s pod exec transport, injected into
// K8sBackend so that Regrade's exec-fallback path can be exercised in unit
// tests without a live cluster. The production implementation delegates to
// client-go's SPDY remotecommand executor.
//
// ExecInPod runs the given command inside the named container of a pod, writing
// stdout/stderr to the provided buffers. Returns an error if the exec transport
// fails or the remote command exits non-zero.
type PodExecor interface {
	ExecInPod(ctx context.Context, namespace, podName, containerName string, cmd []string, stdout, stderr *bytes.Buffer) error
}

const (
	redisK8sNsPrefix  = "instant-customer-"
	redisK8sRoleLabel = "instant.dev/role"
	redisK8sRoleValue = "customer-resource"
	redisK8sReadyTO   = 3 * time.Minute
	redisK8sReadyPoll = 3 * time.Second

	// redisMaxmemoryPolicyCapped is the maxmemory-policy for capped (non-unlimited)
	// dedicated Redis tiers. "noeviction" makes writes fail loudly with an OOM
	// error at the memory cap so the agent/customer sees it and can upgrade —
	// instead of "allkeys-lru" silently evicting the customer's oldest keys.
	// Silent eviction also contradicts --appendonly yes (durability). See P1-C.
	redisMaxmemoryPolicyCapped = "noeviction"
	// redisMaxmemoryPolicyUnlimited is the policy used when a tier resolves to the
	// "no cap" sentinel (maxmemoryMB <= 0): no cap, so eviction never triggers;
	// "noeviction" is Redis's default. Post the strict-80 margin redesign
	// (2026-06-05) every tier's redis_memory_mb is finite, so no current tier uses
	// this policy — it is retained for the sentinel contract.
	redisMaxmemoryPolicyUnlimited = "noeviction"

	// redisK8sOwnerTeamLabel is applied to dedicated customer namespaces.
	// Mirrors the constant in postgres/k8s.go — must stay in sync.
	// Pentest fix: 2026-05-16.
	redisK8sOwnerTeamLabel = "instant.dev/owner-team"

	// anonRouteKeyTTL is the expiry applied to the redis-proxy route-registry
	// keys (<routePrefix><token> and <passwordPrefix><password>) for ANONYMOUS
	// resources only. Deprovision deletes these explicitly, but that delete is
	// best-effort — if the redis-auth Secret read fails, the password-route key
	// would otherwise leak forever. A TTL lets orphans self-heal.
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
// maxmemoryMB is the Redis --maxmemory flag value in MB. A value of <= 0 is the
// "no cap" sentinel (the flag is omitted entirely). This enforces the per-resource
// memory limit at the Redis level. The noeviction policy is used (see
// redisMaxmemoryPolicyCapped) so writes fail loudly with an OOM error at the cap
// rather than silently evicting customer data. These pod-start values are the
// dedicated-pod sizing defaults; the runtime-enforced cap is reconciled to the
// plans registry by RegradeResource (server.go) on provision and plan change.
//
//	anonymous: 5 MB, hobby: 50 MB, pro: 512 MB, growth: 1024 MB, team: -1 (no flag)
//
// NOTE: the registry's redis_memory_mb is finite for every tier post the strict-80
// margin redesign (2026-06-05; team=1536), so the team -1 below is a pod-start
// default that Regrade overrides — it no longer mirrors plans.yaml.
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
	// <= 0 is the "no cap" sentinel (flag omitted). The pod-start value is a sizing
	// default; the runtime cap is reconciled to the plans registry by Regrade. The
	// registry's redis_memory_mb is finite for every tier post strict-80 (2026-06-05).
	// The noeviction maxmemory-policy is applied alongside this limit (P1-C).
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
	case "hobby", "hobby_yearly":
		// hobby_yearly mirrors hobby (plans.yaml: identical limits, annual billing).
		return tierSizing{
			cpuReq: "100m", memReq: "128Mi",
			cpuLim: "500m", memLim: "512Mi",
			pvcMi:        1024, // 1Gi
			qCPURequests: "200m", qMemRequests: "256Mi",
			qCPULimits: "1", qMemLimits: "1Gi",
			maxClients:  50,
			maxmemoryMB: 50, // plans.yaml: hobby redis_memory_mb = 50
		}
	case "hobby_plus", "hobby_plus_yearly":
		// hobby_plus (W11 mid-tier insertion 2026-05-13). Redis memory cap
		// matches hobby (50MB) per plans.yaml; the upsell over hobby is on
		// postgres/mongo/storage, not redis. F1 fix (2026-05-21): explicit
		// case so this tier no longer falls through to the default → hobby
		// path, which by coincidence had the same maxmemoryMB but would
		// silently drift the moment hobby_plus diverged from hobby.
		return tierSizing{
			cpuReq: "100m", memReq: "128Mi",
			cpuLim: "500m", memLim: "512Mi",
			pvcMi:        1024, // 1Gi (matches hobby)
			qCPURequests: "200m", qMemRequests: "256Mi",
			qCPULimits: "1", qMemLimits: "1Gi",
			maxClients:  50,
			maxmemoryMB: 50, // plans.yaml: hobby_plus redis_memory_mb = 50
		}
	case "pro", "pro_yearly":
		// pro_yearly mirrors pro (plans.yaml: identical limits, annual billing).
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
	case "team", "team_yearly":
		// team_yearly mirrors team (identical limits, annual billing).
		return tierSizing{
			cpuReq: "500m", memReq: "1Gi",
			cpuLim: "4", memLim: "4Gi",
			pvcMi:        51200, // 50Gi
			qCPURequests: "1", qMemRequests: "2Gi",
			qCPULimits: "8", qMemLimits: "8Gi",
			maxClients: 1000,
			// "no cap" pod-start default; Regrade reconciles to the registry value
			// (plans.yaml: team redis_memory_mb = 1536, finite post strict-80).
			maxmemoryMB: -1,
		}
	default:
		// Unknown tier → conservative hobby-equivalent sizing.
		return sizingForTier("hobby")
	}
}

// K8sBackend provisions a dedicated Redis pod per token.
type K8sBackend struct {
	cs            kubernetes.Interface // kubernetes.Interface allows fake.Clientset in tests
	restCfg       *rest.Config         // stored for pod-exec transport construction
	storageClass  string               // K8S_STORAGE_CLASS
	image         string               // K8S_REDIS_IMAGE
	externalHost  string               // K8S_EXTERNAL_HOST (legacy NodePort host; kept for back-compat)
	publicHost    string               // K8S_REDIS_PUBLIC_HOST (e.g. redis.instanode.dev) — preferred URL host when set
	storageSizeGi int                  // K8S_REDIS_STORAGE_GI (legacy ceiling; tier sizing overrides per-resource)

	// execor is the pod-exec transport used by the Regrade legacy fallback path.
	// nil at construction; replaced by spdyPodExecor in production and by a
	// fakeExecor in tests. Populated lazily on first use so we don't open
	// network connections during backend init.
	execor PodExecor

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
	return &K8sBackend{
		cs:            cs,
		restCfg:       rc,
		storageClass:  storageClass,
		image:         image,
		externalHost:  externalHost,
		storageSizeGi: storageSizeGi,
	}, nil
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

// spdyPodExecor is the production PodExecor implementation. It uses client-go's
// SPDY remotecommand executor to run a command inside a container via the k8s
// API server. The rest.Config must have its TLS / auth already populated (i.e.
// come from rest.InClusterConfig or clientcmd.BuildConfigFromFlags).
type spdyPodExecor struct {
	cs      kubernetes.Interface
	restCfg *rest.Config
}

// ExecInPod implements PodExecor using the k8s SPDY exec sub-resource.
// The command is run in the named container; stdout and stderr are written to
// the supplied buffers. The pod's environment variables (including REDIS_PASSWORD)
// are available to the command via the shell — we NEVER pass the password as
// a literal argv string.
func (e *spdyPodExecor) ExecInPod(ctx context.Context, namespace, podName, containerName string, cmd []string, stdout, stderr *bytes.Buffer) error {
	req := e.cs.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: containerName,
			Command:   cmd,
			Stdout:    true,
			Stderr:    true,
			Stdin:     false,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(e.restCfg, http.MethodPost, req.URL())
	if err != nil {
		return fmt.Errorf("redis exec: build SPDY executor: %w", err)
	}
	return exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: stdout,
		Stderr: stderr,
	})
}

// podExecor returns the PodExecor for this backend, constructing the
// production spdyPodExecor on first call. Tests may replace b.execor before
// calling Regrade to inject a fake.
func (b *K8sBackend) podExecor() PodExecor {
	if b.execor != nil {
		return b.execor
	}
	b.execor = &spdyPodExecor{cs: b.cs, restCfg: b.restCfg}
	return b.execor
}

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
	// Carry the teamID value forward so applyNamespace can label the namespace
	// with instant.dev/owner-team (pentest 2026-05-16 fix).
	provCtx, provCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer provCancel()
	if teamID, ok := ctx.Value(ctxkeys.TeamIDKey).(string); ok && teamID != "" {
		provCtx = context.WithValue(provCtx, ctxkeys.TeamIDKey, teamID)
	}

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
		// P1-A: anonymous resources get a long self-healing TTL; paid/permanent
		// resources get persistRouteKey (no expiry) so a long-lived resource
		// that is never re-provisioned cannot lose its proxy route.
		routeTTL := routeKeyTTLForTier(tier)
		if err := b.rdb.Set(regCtx, b.routePrefix+token, serviceFQDN, routeTTL).Err(); err != nil {
			slog.Warn("k8s.redis.route_register_failed", "token", token, "error", err)
		} else {
			slog.Info("k8s.redis.route_registered", "token", token, "backend", serviceFQDN)
		}
		// The proxy consumes THIS key — it's the one that actually matters for
		// external connectivity through redis.instanode.dev.
		if err := b.rdb.Set(regCtx, b.passwordPrefix+password, serviceFQDN, routeTTL).Err(); err != nil {
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
	defer func() { _ = rdb.Close() }()

	info, err := rdb.Info(ctx, "memory").Result()
	if err != nil {
		return 0, fmt.Errorf("k8s redis.StorageBytes: INFO memory: %w", err)
	}
	used, err := parseUsedMemory(info)
	if err != nil {
		// A malformed INFO body must not be reported as 0 bytes — that would
		// silently under-report quota usage. Surface the error so the worker
		// skips the tick (fail-open quota convention).
		return 0, fmt.Errorf("k8s redis.StorageBytes: %w", err)
	}
	return used, nil
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

	// Delete BOTH route-registry keys regardless of the namespace-Delete
	// outcome. The keys were captured above (password from the Secret, token
	// from the request) BEFORE any early return, so a Secret-read failure or a
	// namespace Delete error can no longer strand the password-route key. For a
	// paid/permanent resource that key carries no TTL (persistRouteKey), so
	// leaking it leaves the proxy routing a dead password forever — worse than
	// a leaked namespace, which the orphan sweep eventually reaps. routeKeys is
	// idempotent: deleting an already-absent key is a no-op.
	routeKeys := func() {
		if b.rdb == nil {
			return
		}
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

	if err := b.cs.CoreV1().Namespaces().Delete(ctx, ns, metav1.DeleteOptions{}); err != nil {
		if !k8serrors.IsNotFound(err) {
			// Namespace Delete failed (apiserver error, RBAC, transient). Still
			// clean up the route keys best-effort so the proxy stops routing a
			// dead password before we surface the error for the caller to retry.
			routeKeys()
			return fmt.Errorf("k8s redis.Deprovision: delete namespace %s: %w", ns, err)
		}
		slog.Info("k8s.redis.deprovision.namespace_already_gone", "namespace", ns)
	}
	routeKeys()
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
//   - targetMaxmemoryMB > 0  → set maxmemory to that many MB + noeviction policy
//     (P1-C: writes fail loudly at the cap, no silent eviction), then CONFIG
//     REWRITE so the cap survives a pod restart.
//   - targetMaxmemoryMB <= 0 → "no cap" sentinel: set maxmemory to 0 (Redis "no
//     cap") + CONFIG REWRITE so it explicitly overrides any leftover cap. No current
//     tier resolves here post strict-80 (2026-06-05) — every redis_memory_mb is finite.
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

	// ── Orphaned-resource short-circuit ───────────────────────────────────────
	//
	// If the namespace itself is gone, the resource row is orphaned: deprovision
	// raced ahead of platform-DB cleanup, the namespace was force-deleted, or a
	// test cluster was wiped. Without this guard the reconciler hits the
	// Secrets.Get → IsNotFound → exec-fallback → Pods.List → empty path on every
	// sweep forever, logging WARN twice per orphan per ~5min tick (verified
	// 2026-05-30 in prod: 2 orphaned namespaces emitting 576 WARN/day combined).
	//
	// Quiet skip: log at INFO (one line per tick is acceptable; debug-only would
	// hide a real cluster-wide namespace outage) and return a distinct SkipReason
	// so operators can differentiate orphaned rows ("namespace not found") from
	// genuine legacy-pod drift ("exec fallback: no pod found"). Also saves two
	// kube-apiserver round-trips (Secrets.Get + Pods.List) per orphaned row.
	if _, err := b.cs.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			slog.Info("k8s.redis.Regrade: namespace not found — orphaned resource, skip",
				"namespace", ns, "token", token)
			return RegradeResult{Applied: false, SkipReason: "namespace not found (resource orphaned)"}, nil
		}
		// Other errors (permission, transport): surface so the caller can retry.
		return RegradeResult{Applied: false}, fmt.Errorf("k8s redis.Regrade: get namespace: %w", err)
	}

	// ── Secret-based path (modern resources) ──────────────────────────────────
	//
	// Modern resources (provisioned after the redis-auth Secret convention) store
	// the password in a k8s Secret so we can open a direct Redis connection from
	// the provisioner pod. This is the fast, low-latency path.
	secret, err := b.cs.CoreV1().Secrets(ns).Get(ctx, "redis-auth", metav1.GetOptions{})
	if err != nil {
		if !k8serrors.IsNotFound(err) {
			return RegradeResult{Applied: false}, fmt.Errorf("k8s redis.Regrade: get secret: %w", err)
		}

		// ── Exec-based fallback (legacy resources) ─────────────────────────────
		//
		// Legacy pods (provisioned before the redis-auth Secret convention) don't
		// have a k8s Secret, but the password still lives inside the pod as the
		// REDIS_PASSWORD env var feeding --requirepass. We reach it via k8s pod
		// exec, which never exposes the password in argv or logs — the shell
		// interpolates $REDIS_PASSWORD inside the pod's own environment.
		//
		// RBAC requirement: provisioner ClusterRole must include
		//   resources: ["pods/exec"]  verbs: ["create"]
		// (infra/k8s/provisioner/rbac.yaml — added by this PR).
		slog.Info("k8s.redis.Regrade: redis-auth secret absent — attempting exec-based legacy fallback",
			"namespace", ns, "token", token)
		return b.regradeViaExec(ctx, ns, token, targetMaxmemoryMB)
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
	defer func() { _ = rdb.Close() }()

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
		n, perr := fmt.Sscanf(v, "%d", &currentBytes)
		if n != 1 || perr != nil {
			// A non-integer maxmemory means we cannot tell whether the cap is
			// already correct. Soft-skip rather than risk a spurious CONFIG SET
			// (or, worse, treating it as 0 = unlimited).
			slog.Warn("k8s.redis.Regrade: CONFIG GET maxmemory returned non-integer — soft skip",
				"namespace", ns, "token", token, "raw_value", v, "error", perr)
			return RegradeResult{Applied: false, SkipReason: "maxmemory value unparseable"}, nil
		}
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

	// Apply noeviction policy for capped tiers so writes fail loudly with an OOM
	// error at the cap (the customer sees it and can upgrade) instead of
	// allkeys-lru silently evicting their oldest keys. Unlimited tiers
	// (targetBytes == 0) also use noeviction — with no cap, eviction never
	// triggers anyway. See P1-C.
	policy := redisMaxmemoryPolicyUnlimited
	if targetBytes > 0 {
		policy = redisMaxmemoryPolicyCapped
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

// regradeViaExec is the exec-based fallback for Regrade when the redis-auth
// Secret is absent (legacy pods). It:
//  1. Lists pods in the namespace to find the running Redis pod.
//  2. Execs into the pod and runs CONFIG GET maxmemory to check the current value.
//  3. If an update is needed, execs CONFIG SET maxmemory + CONFIG SET maxmemory-policy
//     + CONFIG REWRITE — each step using $REDIS_PASSWORD inside the pod's own
//     environment so the secret never appears in argv or logs.
//
// Fail-soft: if the pod is absent, exec fails, or CONFIG SET fails, the error is
// logged and a soft-skip result is returned so the reconciler sweep continues.
// NEVER logs or prints the password value.
func (b *K8sBackend) regradeViaExec(ctx context.Context, ns, token string, targetMaxmemoryMB int) (RegradeResult, error) {
	// Find the running Redis pod in the namespace.
	pods, err := b.cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{LabelSelector: "app=redis"})
	if err != nil {
		slog.Warn("k8s.redis.Regrade.exec: pod list failed — soft skip",
			"namespace", ns, "token", token, "error", err)
		return RegradeResult{Applied: false, SkipReason: "exec fallback: pod list failed"}, nil
	}
	if len(pods.Items) == 0 {
		slog.Warn("k8s.redis.Regrade.exec: no Redis pod found — soft skip",
			"namespace", ns, "token", token)
		return RegradeResult{Applied: false, SkipReason: "exec fallback: no pod found"}, nil
	}

	// Use the first Running pod; skip non-Running ones (e.g. CrashLoopBackOff).
	var podName string
	for i := range pods.Items {
		if pods.Items[i].Status.Phase == corev1.PodRunning {
			podName = pods.Items[i].Name
			break
		}
	}
	if podName == "" {
		slog.Warn("k8s.redis.Regrade.exec: no Running Redis pod — soft skip",
			"namespace", ns, "token", token)
		return RegradeResult{Applied: false, SkipReason: "exec fallback: no Running pod"}, nil
	}

	exec := b.podExecor()

	// ── Step 1: read current maxmemory via CONFIG GET ──────────────────────
	//
	// We use 'sh -c' so $REDIS_PASSWORD is expanded by the pod's own shell,
	// keeping the literal value out of the argv that the API server logs.
	// The output format is "maxmemory\n<value>\n" (redis-cli --no-auth-warning
	// suppresses the auth-warning line on stderr so we get clean stdout).
	getCmd := []string{"sh", "-c",
		`redis-cli --no-auth-warning -a "$REDIS_PASSWORD" CONFIG GET maxmemory`}
	var getOut, getErr bytes.Buffer
	if err := exec.ExecInPod(ctx, ns, podName, "redis", getCmd, &getOut, &getErr); err != nil {
		slog.Warn("k8s.redis.Regrade.exec: CONFIG GET maxmemory failed — soft skip",
			"namespace", ns, "token", token, "error", err)
		return RegradeResult{Applied: false, SkipReason: "exec fallback: CONFIG GET failed"}, nil
	}

	currentBytes, err := parseConfigGetMaxmemory(getOut.String())
	if err != nil {
		// An unparseable CONFIG GET body must not be read as 0 = "unlimited" —
		// that would silently skip the tier cap. Soft-skip this Regrade.
		slog.Warn("k8s.redis.Regrade.exec: CONFIG GET maxmemory output unparseable — soft skip",
			"namespace", ns, "token", token, "error", err)
		return RegradeResult{Applied: false, SkipReason: "exec fallback: CONFIG GET output unparseable"}, nil
	}

	// Compute target bytes (targetMaxmemoryMB <= 0 → 0 bytes = Redis "no cap").
	var targetBytes int64
	if targetMaxmemoryMB > 0 {
		targetBytes = int64(targetMaxmemoryMB) * 1024 * 1024
	}

	if currentBytes == targetBytes {
		slog.Debug("k8s.redis.Regrade.exec: maxmemory already correct — no-op",
			"namespace", ns, "token", token,
			"maxmemory_bytes", currentBytes,
			"target_bytes", targetBytes,
		)
		return RegradeResult{Applied: false, SkipReason: "already correct"}, nil
	}

	// ── Step 2: CONFIG SET maxmemory ──────────────────────────────────────
	//
	// targetBytes is a plain integer (never user-controlled string) so printf
	// interpolation is safe. $REDIS_PASSWORD is expanded inside the pod's shell.
	setMemCmd := []string{"sh", "-c",
		fmt.Sprintf(`redis-cli --no-auth-warning -a "$REDIS_PASSWORD" CONFIG SET maxmemory %d`, targetBytes)}
	var setMemOut, setMemErr bytes.Buffer
	if err := exec.ExecInPod(ctx, ns, podName, "redis", setMemCmd, &setMemOut, &setMemErr); err != nil {
		slog.Warn("k8s.redis.Regrade.exec: CONFIG SET maxmemory failed — soft skip",
			"namespace", ns, "token", token, "error", err)
		return RegradeResult{Applied: false, SkipReason: "exec fallback: CONFIG SET maxmemory failed"}, nil
	}

	// ── Step 3: CONFIG SET maxmemory-policy ──────────────────────────────
	// Capped tiers use noeviction so writes fail loudly at the cap rather than
	// silently evicting customer keys (see P1-C).
	policy := redisMaxmemoryPolicyUnlimited
	if targetBytes > 0 {
		policy = redisMaxmemoryPolicyCapped
	}
	// policy is one of two known constants, safe to interpolate.
	setPolicyCmd := []string{"sh", "-c",
		fmt.Sprintf(`redis-cli --no-auth-warning -a "$REDIS_PASSWORD" CONFIG SET maxmemory-policy %s`, policy)}
	var setPolicyOut, setPolicyErr bytes.Buffer
	if err := exec.ExecInPod(ctx, ns, podName, "redis", setPolicyCmd, &setPolicyOut, &setPolicyErr); err != nil {
		// Non-fatal — the memory cap is applied; policy mismatch only affects
		// eviction behaviour. Log and continue.
		slog.Warn("k8s.redis.Regrade.exec: CONFIG SET maxmemory-policy failed (non-fatal)",
			"namespace", ns, "token", token, "policy", policy, "error", err)
	}

	// ── Step 4: CONFIG REWRITE ────────────────────────────────────────────
	// Persists the new cap to redis.conf so it survives a pod restart.
	// Legacy pods may have been started without a config file — REWRITE will
	// fail in that case. Non-fatal: the in-memory cap is still enforced.
	rewriteCmd := []string{"sh", "-c",
		`redis-cli --no-auth-warning -a "$REDIS_PASSWORD" CONFIG REWRITE`}
	var rwOut, rwErr bytes.Buffer
	if err := exec.ExecInPod(ctx, ns, podName, "redis", rewriteCmd, &rwOut, &rwErr); err != nil {
		slog.Warn("k8s.redis.Regrade.exec: CONFIG REWRITE failed (non-fatal, in-memory cap is active)",
			"namespace", ns, "token", token, "error", err)
	}

	slog.Info("k8s.redis.Regrade.exec: applied via pod exec",
		"namespace", ns, "token", token,
		"old_maxmemory_bytes", currentBytes,
		"new_maxmemory_bytes", targetBytes,
		"target_maxmemory_mb", targetMaxmemoryMB,
		"policy", policy,
	)
	return RegradeResult{Applied: true, AppliedMaxmemory: targetBytes}, nil
}

// parseConfigGetMaxmemory extracts the maxmemory value in bytes from the output
// of `redis-cli CONFIG GET maxmemory`. The output format is:
//
//	maxmemory
//	<integer>
//
// It returns an error when the "maxmemory" line is absent or its value does not
// parse as an integer. A silent 0 here would read as "unlimited" and silently
// skip the tier cap, so the caller must distinguish a genuine 0 from a parse
// failure rather than guessing.
func parseConfigGetMaxmemory(output string) (int64, error) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) == "maxmemory" && i+1 < len(lines) {
			raw := strings.TrimSpace(lines[i+1])
			var v int64
			n, err := fmt.Sscanf(raw, "%d", &v)
			if n != 1 || err != nil {
				return 0, fmt.Errorf("parseConfigGetMaxmemory: maxmemory value %q is not an integer: %w", raw, err)
			}
			return v, nil
		}
	}
	return 0, fmt.Errorf("parseConfigGetMaxmemory: no maxmemory line in output")
}

// --- private resource creators ---

func (b *K8sBackend) applyNamespace(ctx context.Context, ns string) error {
	labels := map[string]string{
		redisK8sRoleLabel:                    redisK8sRoleValue,
		"pod-security.kubernetes.io/enforce": "baseline",
		"pod-security.kubernetes.io/warn":    "restricted",
	}
	// SECURITY FIX (pentest 2026-05-16): label the namespace with the owning
	// team UUID when provided. The deploy-side NetworkPolicy combines this label
	// with role=customer-resource to scope DB-port egress per-team.
	if teamID, ok := ctx.Value(ctxkeys.TeamIDKey).(string); ok && teamID != "" {
		labels[redisK8sOwnerTeamLabel] = teamID
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
						// Capped tiers use noeviction (redisMaxmemoryPolicyCapped): at the
						// memory cap, writes fail loudly with an OOM error so the customer
						// sees it and can upgrade — rather than allkeys-lru silently
						// evicting their oldest keys (which also contradicts --appendonly
						// yes durability). See P1-C.
						Command: func() []string {
							cmd := []string{
								"redis-server",
								"--requirepass", "$(REDIS_PASSWORD)",
								"--appendonly", "yes",
								"--dir", "/data",
								"--maxclients", fmt.Sprintf("%d", sz.maxClients),
							}
							// Only add --maxmemory when the sizing has a defined cap.
							// sz.maxmemoryMB <= 0 is the "no cap" sentinel — omit the
							// flag so Redis uses its default (no cap). This is a pod-start
							// default; Regrade later reconciles the cap to the registry
							// value, which is finite for every tier post strict-80.
							if sz.maxmemoryMB > 0 {
								cmd = append(cmd,
									"--maxmemory", fmt.Sprintf("%dmb", sz.maxmemoryMB),
									"--maxmemory-policy", redisMaxmemoryPolicyCapped,
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

// parseUsedMemory extracts used_memory from Redis INFO memory output. It
// returns an error when the "used_memory:" line is absent or its value does not
// parse as an integer — a silent 0 would under-report quota usage, so the
// caller must not be left guessing whether 0 is genuine.
func parseUsedMemory(info string) (int64, error) {
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "used_memory:") {
			raw := strings.TrimSpace(strings.TrimPrefix(line, "used_memory:"))
			var n int64
			cnt, err := fmt.Sscanf(raw, "%d", &n)
			if cnt != 1 || err != nil {
				return 0, fmt.Errorf("parseUsedMemory: used_memory value %q is not an integer: %w", raw, err)
			}
			return n, nil
		}
	}
	return 0, fmt.Errorf("parseUsedMemory: no used_memory line in INFO output")
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
