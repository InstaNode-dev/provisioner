package postgres

// naming.go — single source of truth for deriving the dedicated-Postgres
// (K8sBackend) database name and app-role name from a provisioning token.
//
// # Why this file exists (P1-W5-05)
//
// The K8sBackend used to derive both identifiers via k8sShort(): strip dashes
// from the token, then TRUNCATE to the first 12 hex characters:
//
//	dbName  → db_{token[:12]}    appUser → usr_{token[:12]}    (dashes stripped)
//
// A 12-hex-char prefix has only 2^48 possibilities, so any two Team-tier tokens
// that share their first 12 dash-stripped hex digits collide onto the SAME
// Postgres database AND the SAME app role. `CREATE USER`/`CREATE DATABASE`
// would then fail (or, worse, a later StorageBytes/Deprovision/Regrade would
// resolve the wrong tenant's DB+role). A lossy truncation can never be the
// canonical scheme.
//
// There is NO length reason to truncate: a Postgres identifier may be 63 bytes,
// and `db_`/`usr_` plus a 32-hex dash-stripped token is only 35/36 chars — well
// under the limit. This is the same defect mongo/naming.go (P0-5) and
// redis/dedident.go fixed for their backends; postgres-k8s was the one backend
// the 2026-05-17 token-truncation sweep missed.
//
// # Canonical scheme
//
// New provisions use the canonical name: the prefix plus the FULL token with
// dashes removed. No truncation, no hashing, no collisions.
//
// # Backward compatibility
//
// Dedicated-Postgres pods provisioned before this fix hold their database and
// role under the legacy db_/usr_<token[:12]> names. The DeprovisionRequest /
// StorageRequest protos carry only token + provider_resource_id (no stored
// database_name), so lookup paths must re-derive the identifier — a blind
// rename to the canonical scheme would orphan those pre-fix resources
// (StorageBytes silently reads 0 bytes, Regrade ALTERs a non-existent role).
//
// The fix therefore keeps lookups working by trying every historic scheme:
// legacyK8sDBNames / legacyK8sRoleNames return the canonical name first, then
// the legacy 12-char-truncated form. StorageBytes / Deprovision / Regrade walk
// that list so a database/role created under the old scheme is still resolved.

import "strings"

const (
	// k8sDBPrefix / k8sRolePrefix are the fixed prefixes for the dedicated
	// customer database and its scoped app role.
	k8sDBPrefix   = "db_"
	k8sRolePrefix = "usr_"

	// legacyK8sShortLen is the truncation length the pre-fix K8sBackend used
	// (k8sShort). Retained ONLY so lookup paths can still resolve databases and
	// roles provisioned before the canonical scheme shipped. Never used for new
	// provisions.
	legacyK8sShortLen = 12
)

// k8sCanonicalToken returns the token with dashes removed. UUID tokens become
// 32 hex chars; "pool-"-prefixed pool tokens keep the literal "pool" plus 32
// hex chars. This is the basis for every canonical identifier.
func k8sCanonicalToken(token string) string {
	return strings.ReplaceAll(token, "-", "")
}

// k8sDBName returns the canonical, collision-free database name for a token.
// Used by K8sBackend.Provision for all new provisions.
func k8sDBName(token string) string {
	return k8sDBPrefix + k8sCanonicalToken(token)
}

// k8sRoleName returns the canonical, collision-free app-role name for a token.
// Used by K8sBackend.Provision for all new provisions.
func k8sRoleName(token string) string {
	return k8sRolePrefix + k8sCanonicalToken(token)
}

// legacyK8sDBNames returns every database name a token may live under, most
// likely first: the canonical (full-token) name, then the legacy 12-char-
// truncated name. Lookup paths (StorageBytes, Deprovision) iterate this list so
// a database created under either historic scheme is still found after the fix.
// Duplicates are elided (a token <= 12 dash-stripped chars yields one entry).
func legacyK8sDBNames(token string) []string {
	return distinctK8sNames(k8sDBPrefix, token)
}

// legacyK8sRoleNames is the app-role analogue of legacyK8sDBNames.
func legacyK8sRoleNames(token string) []string {
	return distinctK8sNames(k8sRolePrefix, token)
}

// distinctK8sNames builds the ordered, de-duplicated list of historic
// identifiers for the given prefix: [canonical, legacy-12-char-truncated].
func distinctK8sNames(prefix, token string) []string {
	canonical := k8sCanonicalToken(token)

	legacyShort := canonical
	if len(legacyShort) > legacyK8sShortLen {
		legacyShort = legacyShort[:legacyK8sShortLen]
	}

	candidates := []string{
		prefix + canonical,   // canonical (post-fix scheme)
		prefix + legacyShort, // legacy K8sBackend k8sShort scheme
	}

	seen := make(map[string]struct{}, len(candidates))
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if _, dup := seen[c]; dup {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out
}
