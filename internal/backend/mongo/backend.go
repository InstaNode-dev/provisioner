package mongo

import (
	"context"
	"log/slog"
	"os"
	"strconv"

	goredis "github.com/redis/go-redis/v9"
)

// goredisParseURL / goredisNewClient — narrow aliases so we don't import the
// goredis package directly in the factory body.
func goredisParseURL(s string) (*goredis.Options, error)   { return goredis.ParseURL(s) }
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

// Backend is the interface every MongoDB provisioning backend must implement.
type Backend interface {
	Provision(ctx context.Context, token, tier string) (*Credentials, error)
	StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error)
	Deprovision(ctx context.Context, token, providerResourceID string) error
}

// Credentials holds the MongoDB connection details returned after provisioning.
type Credentials struct {
	// URL is the mongodb:// connection string the caller can use immediately.
	// Format: mongodb://usr_{token}:{password}@{host}/db_{token}
	URL string

	// DatabaseName is the name of the provisioned database.
	DatabaseName string

	// ProviderResourceID is an opaque identifier for the provisioned resource.
	// For the k8s backend this is the namespace name (e.g. "instant-customer-{token}").
	// Empty for shared backends that have no per-resource identifier.
	ProviderResourceID string
}

// NewBackend creates a Backend using the given backend type string.
// "k8s" → K8sBackend (dedicated pod per token, every tier).
// "local" (default) → LocalBackend (CREATE USER on shared cluster).
func NewBackend(backendType, adminURI, mongoHost string) Backend {
	switch backendType {
	case "k8s":
		// Dedicated-pod-per-resource backend for every tier. Each /nosql/new
		// provisions a real Mongo pod in its own namespace; sizing is driven
		// by `tier` inside Provision (see sizingForTier).
		kubeconfig := os.Getenv("K8S_KUBECONFIG")
		storageClass := k8sEnv("K8S_STORAGE_CLASS", "do-block-storage")
		image := k8sEnv("K8S_MONGO_IMAGE", "")
		externalHost := k8sEnv("K8S_EXTERNAL_HOST", "")
		// K8S_MONGO_PUBLIC_HOST is the hostname embedded in customer URLs when
		// the mongo-proxy is fronting the cluster (typical prod). Default to
		// `mongo.instanode.dev` since that's where DNS + the LB will point for
		// the production cluster; ops can override per environment.
		publicHost := k8sEnv("K8S_MONGO_PUBLIC_HOST", "mongo.instanode.dev")
		// storageSizeGi from env is a legacy ceiling; the actual PVC is
		// tier-sized via sizingForTier. Kept to avoid breaking the constructor.
		storageSizeGi := k8sEnvInt("K8S_MONGO_STORAGE_GI", 50)
		b, err := newK8sBackend(kubeconfig, storageClass, image, externalHost, storageSizeGi)
		if err != nil {
			slog.Error("mongo.k8s_backend_init_failed_fallback_to_local", "error", err)
			return newLocalBackend(adminURI, mongoHost)
		}
		b.SetPublicHost(publicHost)
		// Route registry — writes route records per provision so the
		// mongo-proxy can demux client connections by SCRAM username.
		routeRedisURL := k8sEnv("REDIS_URL_FOR_ROUTES", os.Getenv("REDIS_URL"))
		if routeRedisURL != "" {
			if opt, perr := goredisParseURL(routeRedisURL); perr == nil {
				rdb := goredisNewClient(opt)
				b.EnableRouteRegistry(rdb, k8sEnv("MONGO_PROXY_ROUTE_PREFIX", "mongo_route:"))
				b.SetPasswordRoutePrefix(k8sEnv("MONGO_PROXY_USER_ROUTE_PREFIX", "mongo_route_by_user:"))
				slog.Info("mongo.route_registry_enabled", "redis_url_set", true)
			} else {
				slog.Warn("mongo.route_registry_disabled_bad_redis_url", "error", perr)
			}
		}
		slog.Info("mongo.backend_selected", "backend", "k8s", "external_host", externalHost, "public_host", publicHost)
		return b
	default:
		return newLocalBackend(adminURI, mongoHost)
	}
}

// NewK8sDedicatedBackend creates a K8sBackend for Team-tier MongoDB provisioning.
// Each token gets its own k8s namespace with a dedicated MongoDB pod.
// All parameters map directly to env vars (see config.Config for names).
func NewK8sDedicatedBackend(kubeconfigPath, storageClass, image, externalHost string, storageSizeGi int) (Backend, error) {
	return newK8sBackend(kubeconfigPath, storageClass, image, externalHost, storageSizeGi)
}
