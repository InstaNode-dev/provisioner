package queue

import "context"

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
