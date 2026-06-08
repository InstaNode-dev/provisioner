package redis

import (
	"context"
	"log/slog"
	"os"
	"strconv"

	goredis "github.com/redis/go-redis/v9"
)

// goredisParseURL / goredisNewClient — narrow aliases so we don't import the
// goredis package directly in the factory body. Keeps the call sites readable
// and the dependency obvious in this file alone.
func goredisParseURL(s string) (*goredis.Options, error)  { return goredis.ParseURL(s) }
func goredisNewClient(o *goredis.Options) *goredis.Client { return goredis.NewClient(o) }

func k8sEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func k8sEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

// Backend is the interface every Redis provisioning backend must implement.
type Backend interface {
	Provision(ctx context.Context, token, tier string) (*Credentials, error)
	StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error)
	Deprovision(ctx context.Context, token, providerResourceID string) error
}

// Regrader is implemented by backends that support post-provision maxmemory
// adjustment. Only the k8s backend supports this — shared/local and
// dedicated-upstash backends have no per-tenant maxmemory lever at the Redis
// level and do not implement this interface.
//
// The server uses a type assertion (b.redisBackend.(redis.Regrader)) to check
// whether the active backend supports regrade; when the assertion fails it
// returns {applied:false, skip_reason:"backend does not support redis regrade"}
// without error, and the reconciler leaves the row for the next sweep.
type Regrader interface {
	// Regrade connects to the dedicated Redis pod and adjusts maxmemory to
	// match targetMaxmemoryMB. targetMaxmemoryMB <= 0 is the "no cap" sentinel
	// (maxmemory 0); every tier's redis_memory_mb is finite post strict-80
	// (2026-06-05), so the registry does not pass the sentinel today.
	// Returns RegradeResult.Applied=false + SkipReason when the pod is already
	// correctly configured (idempotent no-op) or unreachable (soft skip).
	Regrade(ctx context.Context, token, providerResourceID string, targetMaxmemoryMB int) (RegradeResult, error)
}

// Credentials holds the Redis connection details returned after provisioning.
type Credentials struct {
	// URL is the redis:// connection string the caller can use immediately.
	// For local backend with ACL: redis://usr_{short}:{password}@{host}:6379/0
	// For local backend without ACL: redis://{host}:6379/0
	URL string

	// KeyPrefix is the key namespace for local backend without ACL.
	// Clients must prefix all keys with this value to stay in their namespace.
	// Empty when ACL-based isolation is used.
	KeyPrefix string

	// ProviderResourceID is an opaque identifier for the provisioned resource.
	// For the k8s backend this is the namespace name (e.g. "instant-customer-{token}").
	// Empty for shared backends that have no per-resource identifier.
	ProviderResourceID string
}

// NewBackend creates a Backend using the given backend type string.
// "k8s" → K8sBackend (dedicated pod per token, every tier).
// "local" (default) → LocalBackend (ACL user on shared cluster).
func NewBackend(backendType, redisHost string) Backend {
	switch backendType {
	case "k8s":
		// Dedicated-pod-per-resource backend for every tier. Each /cache/new
		// provisions a real Redis pod in its own namespace; sizing is driven
		// by `tier` inside Provision (see sizingForTier).
		//
		// Env-var driven so we don't have to thread a Config object through
		// the existing factory signature.
		kubeconfig := os.Getenv("K8S_KUBECONFIG")
		storageClass := k8sEnv("K8S_STORAGE_CLASS", "do-block-storage")
		image := k8sEnv("K8S_REDIS_IMAGE", "")
		externalHost := k8sEnv("K8S_EXTERNAL_HOST", "")
		// K8S_REDIS_PUBLIC_HOST is the hostname embedded in customer URLs when
		// the redis-proxy is fronting the cluster (typical prod). Default to
		// `redis.instanode.dev` since that's where DNS + the LB will point
		// for the production cluster; ops can override per environment.
		publicHost := k8sEnv("K8S_REDIS_PUBLIC_HOST", "redis.instanode.dev")
		// storageSizeGi from env is a legacy ceiling; the actual PVC is
		// tier-sized via sizingForTier (uses MiB for Redis). Kept to avoid
		// breaking the constructor signature.
		storageSizeGi := k8sEnvInt("K8S_REDIS_STORAGE_GI", 10)
		b, err := newK8sBackend(kubeconfig, storageClass, image, externalHost, storageSizeGi)
		if err != nil {
			slog.Error("redis.k8s_backend_init_failed_fallback_to_local", "error", err)
			return newLocalBackend(redisHost)
		}
		b.SetPublicHost(publicHost)
		// Route registry — writes route records per provision so the
		// redis-proxy can demux client connections by AUTH password.
		routeRedisURL := k8sEnv("REDIS_URL_FOR_ROUTES", os.Getenv("REDIS_URL"))
		if routeRedisURL != "" {
			if opt, perr := goredisParseURL(routeRedisURL); perr == nil {
				rdb := goredisNewClient(opt)
				b.EnableRouteRegistry(rdb, k8sEnv("REDIS_PROXY_ROUTE_PREFIX", "redis_route:"))
				b.SetPasswordRoutePrefix(k8sEnv("REDIS_PROXY_PASSWORD_ROUTE_PREFIX", "redis_route_by_password:"))
				slog.Info("redis.route_registry_enabled", "redis_url_set", true)
			} else {
				slog.Warn("redis.route_registry_disabled_bad_redis_url", "error", perr)
			}
		}
		slog.Info("redis.backend_selected", "backend", "k8s", "external_host", externalHost, "public_host", publicHost)
		return b
	default:
		return newLocalBackend(redisHost)
	}
}

// NewSharedCarveBackend creates a LocalBackend: an ACL user + key-prefix carve
// on a SHARED Redis instance (many tenants per pod). It is the non-Team side of
// tier-aware routing (see TierDispatchBackend). redisHost is "host:port".
func NewSharedCarveBackend(redisHost string) Backend {
	return newLocalBackend(redisHost)
}

// NewDedicatedBackend creates a DedicatedProvider for Team-tier Redis provisioning.
// adminRedisURL must point to a dedicated Redis instance (separate from the shared cluster).
// upstashAPIKey is optional; when set the Upstash API path is used instead.
func NewDedicatedBackend(adminRedisURL, upstashAPIKey string) Backend {
	return NewDedicatedProvider(adminRedisURL, upstashAPIKey)
}

// NewK8sDedicatedBackend creates a K8sBackend for Team-tier Redis provisioning.
// Each token gets its own k8s namespace with a dedicated Redis pod.
// All parameters map directly to env vars (see config.Config for names).
func NewK8sDedicatedBackend(kubeconfigPath, storageClass, image, externalHost string, storageSizeGi int) (Backend, error) {
	return newK8sBackend(kubeconfigPath, storageClass, image, externalHost, storageSizeGi)
}
