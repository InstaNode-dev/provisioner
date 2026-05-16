// Package ctxkeys defines context keys shared across the provisioner packages.
// Using a typed key prevents collisions with other packages that also store
// values in the context.
package ctxkeys

// contextKey is an unexported type for context keys in this package.
type contextKey int

const (
	// TeamIDKey carries the owning team UUID from the gRPC server handler
	// down to the k8s provisioning backends.  The backends use it to label
	// dedicated customer-resource namespaces with instant.dev/owner-team so
	// the deploy-side NetworkPolicy can scope DB-port egress per-team.
	//
	// Value type: string — empty string means anonymous (no label applied).
	TeamIDKey contextKey = iota
)
