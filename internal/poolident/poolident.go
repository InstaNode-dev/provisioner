// Package poolident encodes and decodes the *canonical naming token* of a
// pool-claimed resource inside the gRPC ProvisionResponse.provider_resource_id
// field.
//
// # Why this package exists (P0-2)
//
// The hot-pool manager pre-provisions Redis / Mongo / Postgres resources under
// a synthetic token "pool-<uuid>". The shared (local) backends derive every
// backing-infra name FROM that token:
//
//	postgres → db_pool-<uuid> / usr_pool-<uuid>
//	redis    → ACL user usr_pool-<uuid>, keyspace pool-<uuid>:*
//	mongo    → db_pool-<uuid> / usr_pool-<uuid>
//
// When the pool item is later handed to a real /db/new /cache/new /nosql/new
// caller, the api resource row records the caller's REAL token — but the
// backing infra keeps its pool-token-derived names. Every later lifecycle RPC
// (Deprovision / StorageBytes / Regrade) re-derives the name from the real
// token, so:
//
//   - Deprovision DROPs db_<real> / usr_<real> — a no-op; the pool-token infra
//     leaks forever.
//   - StorageBytes measures db_<real> — returns 0; quota never enforced.
//   - Regrade ALTERs usr_<real> — a no-op; tier caps never applied.
//
// # The fix: a canonical naming token carried on provider_resource_id
//
// On a pool hit the server stamps the pool token into provider_resource_id
// using Encode(). The api persists provider_resource_id verbatim (it already
// does). Every lookup path (Deprovision / StorageBytes / Regrade) then resolves
// the naming token with NamingToken(): the pool token when provider_resource_id
// carries one, the request token otherwise (live-provisioned and legacy rows).
//
// provider_resource_id is otherwise:
//   - empty for shared (local) Redis / Mongo resources, so a bare pool-token
//     marker is unambiguous there;
//   - "local:<N>" for shared Postgres (cluster index — see cluster_router.go),
//     so the pool-token marker is appended after a ';' separator.
//
// The marker is therefore always self-describing ("pooltok:<token>") and never
// collides with the "local:" cluster prefix or the "instant-customer-" k8s
// namespace prefix.
package poolident

import "strings"

const (
	// Marker is the self-describing key that introduces a pool naming token
	// inside a provider_resource_id value. Example encodings:
	//
	//	redis / mongo (no other prid content): "pooltok:pool-<uuid>"
	//	postgres (cluster index present):      "local:0;pooltok:pool-<uuid>"
	Marker = "pooltok:"

	// segSep separates independent segments of a provider_resource_id value
	// (currently only the Postgres "local:<N>" cluster segment and the
	// pool-token segment). A ';' cannot appear in a "local:<N>" string or in a
	// UUID-based pool token, so splitting on it is unambiguous.
	segSep = ";"

	// k8sNamespacePrefix is the provider_resource_id form used by every
	// dedicated (k8s-backed) resource: "instant-customer-<token>". A k8s pool
	// item's namespace name ALREADY embeds the pool token, so it is itself the
	// canonical naming identifier — Encode is a deliberate no-op for it. Only
	// the shared (local) backends, which derive names from the request token
	// rather than from a namespace, need the pool-token marker.
	k8sNamespacePrefix = "instant-customer-"
)

// Encode returns the provider_resource_id value for a pool-claimed resource.
//
//	basePRID  — the provider_resource_id the backend would otherwise return
//	            ("" for shared Redis/Mongo, "local:<N>" for shared Postgres).
//	poolToken — the synthetic "pool-<uuid>" token the infra was provisioned
//	            under.
//
// When poolToken is empty Encode returns basePRID unchanged (live-provision
// path — no marker needed).
//
// Encode is also a no-op when basePRID is a k8s namespace
// ("instant-customer-<token>"): a dedicated pool item's namespace name already
// embeds the pool token, so the k8s backends resolve the correct infra from
// the namespace alone and must not receive the marker (it would corrupt the
// namespace string they use verbatim).
func Encode(basePRID, poolToken string) string {
	if poolToken == "" {
		return basePRID
	}
	if strings.HasPrefix(basePRID, k8sNamespacePrefix) {
		return basePRID
	}
	marker := Marker + poolToken
	if basePRID == "" {
		return marker
	}
	return basePRID + segSep + marker
}

// PoolToken extracts the pool naming token previously stored by Encode, or ""
// when providerResourceID carries no pool-token marker (live-provisioned and
// legacy rows). It tolerates the marker appearing as the whole value or as a
// ';'-separated segment, in any position.
func PoolToken(providerResourceID string) string {
	if providerResourceID == "" {
		return ""
	}
	for _, seg := range strings.Split(providerResourceID, segSep) {
		if strings.HasPrefix(seg, Marker) {
			return strings.TrimPrefix(seg, Marker)
		}
	}
	return ""
}

// BasePRID strips any pool-token marker from providerResourceID and returns the
// remaining base value (the "local:<N>" Postgres cluster segment, or "").
// Backends that interpret provider_resource_id for routing — notably the
// Postgres ClusterRouter — call this so a pool-claimed resource still routes to
// the correct cluster.
func BasePRID(providerResourceID string) string {
	if providerResourceID == "" {
		return ""
	}
	segs := strings.Split(providerResourceID, segSep)
	kept := make([]string, 0, len(segs))
	for _, seg := range segs {
		if strings.HasPrefix(seg, Marker) {
			continue
		}
		kept = append(kept, seg)
	}
	return strings.Join(kept, segSep)
}

// NamingToken resolves the canonical token used to derive backing-infra names
// (db_<token>, usr_<token>, keyspace <token>:*) for a lifecycle RPC.
//
// It returns the pool token encoded in providerResourceID when present, and
// falls back to requestToken otherwise. The fallback covers both
// live-provisioned resources (no pool involvement) and legacy rows written
// before this fix shipped — for those the request token IS the naming token,
// so behaviour is unchanged.
func NamingToken(requestToken, providerResourceID string) string {
	if pt := PoolToken(providerResourceID); pt != "" {
		return pt
	}
	return requestToken
}
