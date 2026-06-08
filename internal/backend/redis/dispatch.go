package redis

// dispatch.go — tier-aware Redis backend routing.
//
// # Why this exists (dedicated-pod-per-token capacity problem)
//
// The shared `redisBackend` the server holds is built from REDIS_PROVISION_BACKEND.
// In production that value is "k8s", so the K8sBackend (one Redis pod + namespace
// per token) serves EVERY non-dedicated tier — anonymous, free, hobby, hobby_plus.
// A 5MB anonymous cache therefore costs a whole pod, PVC, Service and NetworkPolicy.
// That does not scale: pod count grows linearly with /cache/new calls.
//
// TierDispatchBackend is the durable fix. It routes per tier:
//   - non-Team tiers (capped 5MB–512MB caches) → the SHARED-carve backend
//     (LocalBackend: an ACL user + key-prefix namespace on a shared Redis), so
//     many tenants share one pod.
//   - Team tier (unlimited, justifies dedicated infra) → the DEDICATED backend
//     (the configured K8sBackend: one pod per token).
//
// # Flag-gated, default OFF
//
// This type is ONLY constructed when REDIS_TIER_AWARE_ROUTING_ENABLED=true (see
// config.Config.RedisTierAwareRoutingEnabled and server.New). When the flag is
// off or unset the server keeps using the single configured backend for every
// tier — behaviour is byte-for-byte identical to today. The dispatcher changes
// nothing in production until an operator flips the flag.

import (
	"context"
	"log/slog"
	"strings"
)

// dedicatedTier is the single tier that receives a dedicated Redis pod under
// tier-aware routing. Every other tier is routed to the shared-carve backend.
//
// This is deliberately a named constant (not an inline "team" literal) and is
// narrower than the server's isDedicatedTier (pro/team/growth): the whole point
// of tier-aware routing is to move the high-volume capped tiers OFF dedicated
// pods, and only Team's unlimited promise justifies a pod per token. If product
// later decides another tier earns a dedicated pod, change it here.
const dedicatedTier = "team"

// dedicatedNamespacePrefix is the provider_resource_id / namespace prefix the
// dedicated (k8s) backend stamps on every resource it provisions. It is defined
// as an alias of redisK8sNsPrefix (the constant the k8s backend actually uses in
// k8s.go), so the two can never drift: a rename of redisK8sNsPrefix flows
// through here automatically.
//
// Lifecycle RPCs (Deprovision / StorageBytes) do not carry the tier, so the
// dispatcher routes them by this prefix: a PRID that starts with it was
// provisioned on the dedicated backend; anything else (empty PRID, a pool-token
// marker, etc.) lives on the shared-carve backend.
const dedicatedNamespacePrefix = redisK8sNsPrefix

// isDedicatedRoutingTier reports whether a tier should be served by the
// dedicated backend under tier-aware routing.
func isDedicatedRoutingTier(tier string) bool {
	return tier == dedicatedTier
}

// hasDedicatedProviderID reports whether a provider_resource_id was produced by
// the dedicated backend (a k8s namespace). Used to route lifecycle RPCs that do
// not carry the tier.
func hasDedicatedProviderID(providerResourceID string) bool {
	return strings.HasPrefix(providerResourceID, dedicatedNamespacePrefix)
}

// TierDispatchBackend routes Redis lifecycle calls to one of two backends based
// on tier (Provision) or provider_resource_id prefix (Deprovision/StorageBytes).
//
// It satisfies redis.Backend and — when the dedicated backend does — redis.Regrader.
type TierDispatchBackend struct {
	// sharedCarve serves all non-Team tiers: an ACL carve on a shared Redis.
	sharedCarve Backend
	// dedicated serves the Team tier: a dedicated pod per token (k8s backend).
	dedicated Backend
}

// Compile-time check: the dispatcher is a Backend.
var _ Backend = (*TierDispatchBackend)(nil)

// NewTierDispatchBackend builds a tier-aware dispatcher. Both backends must be
// non-nil; the caller (server.New) only constructs the dispatcher once it has
// both a shared-carve and a dedicated backend in hand.
func NewTierDispatchBackend(sharedCarve, dedicated Backend) *TierDispatchBackend {
	return &TierDispatchBackend{sharedCarve: sharedCarve, dedicated: dedicated}
}

// Provision routes by tier: Team → dedicated pod, everyone else → shared carve.
func (b *TierDispatchBackend) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	if isDedicatedRoutingTier(tier) {
		slog.Info("redis.dispatch.provision", "route", "dedicated", "token", token, "tier", tier)
		return b.dedicated.Provision(ctx, token, tier)
	}
	slog.Info("redis.dispatch.provision", "route", "shared_carve", "token", token, "tier", tier)
	return b.sharedCarve.Provision(ctx, token, tier)
}

// Deprovision routes by provider_resource_id: a dedicated (k8s namespace) PRID
// → dedicated backend; anything else → shared-carve backend. The tier is not
// available on this call, so the PRID prefix is the only reliable signal — the
// same convention server.go already uses to split dedicated vs shared teardown.
func (b *TierDispatchBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	if hasDedicatedProviderID(providerResourceID) {
		slog.Info("redis.dispatch.deprovision", "route", "dedicated", "token", token, "provider_resource_id", providerResourceID)
		return b.dedicated.Deprovision(ctx, token, providerResourceID)
	}
	slog.Info("redis.dispatch.deprovision", "route", "shared_carve", "token", token, "provider_resource_id", providerResourceID)
	return b.sharedCarve.Deprovision(ctx, token, providerResourceID)
}

// StorageBytes routes by provider_resource_id, mirroring Deprovision.
func (b *TierDispatchBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	if hasDedicatedProviderID(providerResourceID) {
		return b.dedicated.StorageBytes(ctx, token, providerResourceID)
	}
	return b.sharedCarve.StorageBytes(ctx, token, providerResourceID)
}

// Regrade delegates to the dedicated backend's Regrader, if it implements one.
// Only dedicated (k8s) pods have a per-tenant maxmemory lever to adjust; the
// shared-carve backend has no per-user maxmemory at the Redis level (Redis ACLs
// cannot cap memory per user — maxmemory is server-wide), so a shared-carve
// resource is simply not regradeable here and returns a soft skip.
//
// Routing uses the same PRID-prefix signal as Deprovision/StorageBytes: only a
// dedicated (k8s namespace) PRID is forwarded to the dedicated Regrader; a
// shared-carve PRID short-circuits to a soft skip without touching the shared
// pod (whose maxmemory is shared by every tenant on it).
func (b *TierDispatchBackend) Regrade(ctx context.Context, token, providerResourceID string, targetMaxmemoryMB int) (RegradeResult, error) {
	regrader, ok := b.dedicated.(Regrader)
	if !ok {
		return RegradeResult{Applied: false, SkipReason: "dedicated backend does not support redis regrade"}, nil
	}
	if !hasDedicatedProviderID(providerResourceID) {
		// A shared-carve resource: no per-tenant maxmemory to set. Soft skip —
		// never CONFIG SET maxmemory on the shared pod (that would cap EVERY
		// tenant sharing it).
		return RegradeResult{Applied: false, SkipReason: "shared-carve redis has no per-tenant maxmemory lever"}, nil
	}
	return regrader.Regrade(ctx, token, providerResourceID, targetMaxmemoryMB)
}
