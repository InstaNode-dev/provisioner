package queue

import (
	"context"
	"log/slog"
	"os"

	goredis "github.com/redis/go-redis/v9"
)

// goredisParseURL / goredisNewClient — narrow aliases so we don't import goredis
// directly in the factory body. Mirrors redis/backend.go.
func goredisParseURL(s string) (*goredis.Options, error) { return goredis.ParseURL(s) }
func goredisNewClient(o *goredis.Options) *goredis.Client { return goredis.NewClient(o) }

// Backend is the interface every NATS queue provisioning backend must implement.
type Backend interface {
	Provision(ctx context.Context, token, tier string) (*Credentials, error)
	Deprovision(ctx context.Context, token, providerResourceID string) error
}

// Credentials holds the NATS connection details returned after provisioning.
type Credentials struct {
	// URL is the nats:// connection string. Format: nats://{host}:4222
	URL string

	// SubjectPrefix is the subject namespace for this resource.
	// Callers must use subjects of the form "{SubjectPrefix}{event-name}".
	SubjectPrefix string

	// ProviderResourceID is the k8s namespace name for dedicated backends.
	// Empty for shared backends.
	ProviderResourceID string
}

// NewBackend creates a Backend using the given backend type string.
// "k8s" → K8sBackend (dedicated pod per token, every tier).
// "local" (default) → LocalBackend (shared NATS cluster, subject-prefix isolation).
func NewBackend(backendType, natsHost string) Backend {
	switch backendType {
	case "k8s":
		// Dedicated-pod-per-resource backend for every tier. Each /queue/new
		// provisions a real NATS pod in its own namespace; sizing is driven
		// by `tier` inside Provision (see sizingForTier).
		//
		// Env-var driven so we don't have to thread a Config object through
		// the existing factory signature.
		kubeconfig := os.Getenv("K8S_KUBECONFIG")
		storageClass := k8sEnv("K8S_STORAGE_CLASS", "do-block-storage")
		image := k8sEnv("K8S_NATS_IMAGE", "")
		externalHost := k8sEnv("K8S_EXTERNAL_HOST", "")
		// K8S_NATS_PUBLIC_HOST is the hostname embedded in customer URLs when
		// the nats-proxy is fronting the cluster (typical prod). Default to
		// `nats.instanode.dev` since that's where DNS + the LB will point
		// for the production cluster; ops can override per environment.
		publicHost := k8sEnv("K8S_NATS_PUBLIC_HOST", "nats.instanode.dev")
		b, err := newK8sBackend(kubeconfig, storageClass, image, externalHost)
		if err != nil {
			slog.Error("queue.k8s_backend_init_failed_fallback_to_local", "error", err)
			return newLocalBackend(natsHost)
		}
		b.SetPublicHost(publicHost)
		// Route registry — writes route records per provision so the
		// nats-proxy can demux client connections by CONNECT auth_token.
		routeRedisURL := k8sEnv("REDIS_URL_FOR_ROUTES", os.Getenv("REDIS_URL"))
		if routeRedisURL != "" {
			if opt, perr := goredisParseURL(routeRedisURL); perr == nil {
				rdb := goredisNewClient(opt)
				b.EnableRouteRegistry(rdb, k8sEnv("NATS_PROXY_ROUTE_PREFIX", "nats_route:"))
				b.SetTokenRoutePrefix(k8sEnv("NATS_PROXY_TOKEN_ROUTE_PREFIX", "nats_route_by_token:"))
				slog.Info("queue.route_registry_enabled", "redis_url_set", true)
			} else {
				slog.Warn("queue.route_registry_disabled_bad_redis_url", "error", perr)
			}
		}
		slog.Info("queue.backend_selected", "backend", "k8s", "external_host", externalHost, "public_host", publicHost)
		return b
	default:
		return newLocalBackend(natsHost)
	}
}
