package redis

import "context"

// Backend is the interface every Redis provisioning backend must implement.
type Backend interface {
	Provision(ctx context.Context, token, tier string) (*Credentials, error)
	StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error)
	Deprovision(ctx context.Context, token, providerResourceID string) error
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
// "local" (default) → LocalBackend.
func NewBackend(backendType, redisHost string) Backend {
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
