package postgres

import (
	"context"
	"strings"
)

// Backend is the interface every Postgres provisioning backend must implement.
type Backend interface {
	Provision(ctx context.Context, token, tier string) (*Credentials, error)
	StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error)
	Deprovision(ctx context.Context, token, providerResourceID string) error
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
