package postgres

import (
	"context"
	"log/slog"
	"os"
	"strconv"
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

// goredisParseURL / goredisNewClient — narrow aliases so we don't import the
// goredis package directly in the factory body. Keeps the call sites readable
// and the dependency obvious in this file alone.
func goredisParseURL(s string) (*goredis.Options, error) { return goredis.ParseURL(s) }
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

// Backend is the interface every Postgres provisioning backend must implement.
type Backend interface {
	Provision(ctx context.Context, token, tier string) (*Credentials, error)
	StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error)
	Deprovision(ctx context.Context, token, providerResourceID string) error
	// Regrade re-applies the tier's per-role CONNECTION LIMIT to an already
	// provisioned resource (e.g. after a plan upgrade). Idempotent.
	//
	// connLimit is the connection cap to apply (-1 = unlimited). Backends that
	// own a dedicated pod per resource (k8s) ALTER ROLE in place. The shared
	// local/dedicated/neon backends set no per-role cap at provision time, so
	// they return RegradeResult{Applied:false, SkipReason:"..."} without error.
	//
	// A non-error RegradeResult{Applied:false} means "nothing to do / not
	// reachable" — the caller can safely retry on the next sweep. An error is
	// reserved for unexpected failures.
	Regrade(ctx context.Context, token, providerResourceID string, connLimit int) (RegradeResult, error)
}

// RegradeResult is the outcome of a Backend.Regrade call.
type RegradeResult struct {
	Applied          bool   // true if the new connection cap was applied
	AppliedConnLimit int    // the cap that is now in effect (-1 = unlimited)
	SkipReason       string // populated when Applied is false
}

// Credentials returned by Provision.
type Credentials struct {
	URL                string // postgres://usr_{token}:{pass}@host/db_{token}
	DatabaseName       string // db_{token}
	Username           string // usr_{token}
	ProviderResourceID string // Neon project ID, empty for local
}

// NewBackend creates a Backend using the given backend type string.
// "neon" → NeonBackend; default → LocalBackend.
// When clusterURLs is a comma-separated list of admin DSNs, the ClusterRouter
// distributes provisions across them. When empty, customersURL is used alone.
func NewBackend(backendType, customersURL, clusterURLs, neonAPIKey, neonRegionID string) Backend {
	switch backendType {
	case "neon":
		return newNeonBackend(neonAPIKey, neonRegionID)
	case "k8s":
		// Dedicated-pod-per-resource backend for every tier. Each /db/new
		// provisions a real Postgres pod in its own namespace; sizing is
		// driven by `tier` inside Provision (see sizingForTier).
		//
		// Env-var driven so we don't have to thread a Config object through
		// the existing factory signature.
		kubeconfig := os.Getenv("K8S_KUBECONFIG")
		storageClass := k8sEnv("K8S_STORAGE_CLASS", "do-block-storage")
		image := k8sEnv("K8S_POSTGRES_IMAGE", "")
		externalHost := k8sEnv("K8S_EXTERNAL_HOST", "")
		// storageSizeGi from env is now a per-resource ceiling; the actual PVC
		// is tier-sized via sizingForTier. Kept as a no-op default to avoid
		// breaking the constructor signature.
		storageSizeGi := k8sEnvInt("K8S_POSTGRES_STORAGE_GI", 50)
		b, err := newK8sBackend(kubeconfig, storageClass, image, externalHost, storageSizeGi)
		if err != nil {
			slog.Error("postgres.k8s_backend_init_failed_fallback_to_local", "error", err)
			return newLocalBackend(customersURL)
		}
		// Route registry: when REDIS_URL_FOR_ROUTES is set (or REDIS_URL),
		// every successful Provision publishes a route to Redis so the
		// pg-proxy in front of pg.instanode.dev:5432 can demux by db name.
		routeRedisURL := k8sEnv("REDIS_URL_FOR_ROUTES", os.Getenv("REDIS_URL"))
		if routeRedisURL != "" {
			if opt, perr := goredisParseURL(routeRedisURL); perr == nil {
				rdb := goredisNewClient(opt)
				b.EnableRouteRegistry(rdb, k8sEnv("PG_PROXY_ROUTE_PREFIX", "pg_route:"))
				slog.Info("postgres.route_registry_enabled", "redis_url_set", true)
			} else {
				slog.Warn("postgres.route_registry_disabled_bad_redis_url", "error", perr)
			}
		}
		slog.Info("postgres.backend_selected", "backend", "k8s", "external_host", externalHost)
		return b
	default:
		if clusterURLs != "" {
			urls := strings.Split(clusterURLs, ",")
			// Filter empty entries (trailing comma, etc.).
			var filtered []string
			for _, u := range urls {
				if u = strings.TrimSpace(u); u != "" {
					filtered = append(filtered, u)
				}
			}
			if len(filtered) > 0 {
				return newLocalBackendMulti(filtered)
			}
		}
		return newLocalBackend(customersURL)
	}
}

// NewDedicatedBackend creates a DedicatedProvider for Team-tier provisioning.
// When neonAPIKey is non-empty the Neon API is used; otherwise a local admin DSN
// (adminDSN) is used to simulate dedicated isolation.
func NewDedicatedBackend(adminDSN, neonAPIKey string) Backend {
	return NewDedicatedProvider(adminDSN, neonAPIKey)
}

// NewK8sDedicatedBackend creates a K8sBackend for Team-tier provisioning.
// Each token gets its own k8s namespace with a dedicated Postgres pod.
// All parameters map directly to env vars (see config.Config for names).
func NewK8sDedicatedBackend(kubeconfigPath, storageClass, image, externalHost string, storageSizeGi int) (Backend, error) {
	return newK8sBackend(kubeconfigPath, storageClass, image, externalHost, storageSizeGi)
}
