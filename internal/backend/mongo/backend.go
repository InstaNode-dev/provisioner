package mongo

import "context"

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

// NewBackend creates a Backend using the given configuration.
func NewBackend(adminURI, mongoHost string) Backend {
	return newLocalBackend(adminURI, mongoHost)
}

// NewK8sDedicatedBackend creates a K8sBackend for Team-tier MongoDB provisioning.
// Each token gets its own k8s namespace with a dedicated MongoDB pod.
// All parameters map directly to env vars (see config.Config for names).
func NewK8sDedicatedBackend(kubeconfigPath, storageClass, image, externalHost string, storageSizeGi int) (Backend, error) {
	return newK8sBackend(kubeconfigPath, storageClass, image, externalHost, storageSizeGi)
}
