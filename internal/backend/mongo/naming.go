package mongo

// naming.go — single source of truth for deriving MongoDB database and user
// names from a provisioning token.
//
// # Why this file exists (P0-5)
//
// Before this file, the two backends derived names incompatibly:
//
//	LocalBackend  → db_{raw-token}            usr_{raw-token}            (dashes kept)
//	K8sBackend    → db_{token[:12]}           usr_{token[:12]}           (dashes stripped, TRUNCATED to 12)
//
// The k8s 12-char truncation is a correctness bug: any two tokens that share
// their first 12 hex digits collide onto the same database and user. A lossy
// truncation can never be the canonical scheme.
//
// # Canonical scheme
//
// Both backends now provision with the canonical name: the prefix plus the
// FULL token with dashes removed. Tokens are UUIDs (optionally "pool-"
// prefixed), so a dash-stripped token is at most ~37 characters; with the
// "db_"/"usr_" prefix the identifier stays well under MongoDB's 64-byte
// database-name limit and the practical username limit. No truncation, no
// hashing, no collisions.
//
// # Backward compatibility
//
// Live customer databases already exist under the two pre-fix schemes. A blind
// rename would orphan them — a later StorageBytes/Deprovision would compute the
// new canonical name and miss the real database, silently reporting 0 bytes
// (quota un-enforced) or failing to drop it. The proto's StorageRequest /
// DeprovisionRequest carry only token + provider_resource_id (no stored
// database_name), so a lookup cannot read a persisted name — it must re-derive.
//
// The fix therefore keeps lookups working by trying every historic scheme:
// legacyMongoDBNames / legacyMongoUserNames return the canonical name first,
// then each legacy form. StorageBytes and Deprovision walk that list.

import "strings"

const (
	// mongoDBPrefix / mongoUserPrefix are the fixed prefixes for the customer
	// database and its scoped readWrite user.
	mongoDBPrefix   = "db_"
	mongoUserPrefix = "usr_"

	// legacyShortLen is the truncation length the pre-fix K8sBackend used
	// (mongoK8sShort). Kept ONLY so lookup paths can still resolve databases
	// provisioned before the canonical scheme. Never used for new provisions.
	legacyShortLen = 12
)

// mongoCanonicalToken returns the token with dashes removed. UUID tokens become
// 32 hex chars; "pool-"-prefixed pool tokens keep the literal "pool" plus 32
// hex chars. This is the basis for every canonical identifier.
func mongoCanonicalToken(token string) string {
	return strings.ReplaceAll(token, "-", "")
}

// mongoDBName returns the canonical, collision-free database name for a token.
// Used by both LocalBackend and K8sBackend for all new provisions.
func mongoDBName(token string) string {
	return mongoDBPrefix + mongoCanonicalToken(token)
}

// mongoUserName returns the canonical, collision-free user name for a token.
// Used by both LocalBackend and K8sBackend for all new provisions.
func mongoUserName(token string) string {
	return mongoUserPrefix + mongoCanonicalToken(token)
}

// legacyMongoDBNames returns every database name a token may live under, most
// likely first: the canonical name, then the legacy k8s 12-char-truncated
// name, then the legacy LocalBackend raw-token (dashes kept) name.
//
// Lookup paths (StorageBytes, Deprovision) iterate this list so a database
// created under any historic scheme is still found after the fix. Duplicates
// are elided (e.g. a token <= 12 chars with no dashes yields one entry).
func legacyMongoDBNames(token string) []string {
	return distinctNames(mongoDBPrefix, token)
}

// legacyMongoUserNames is the user-name analogue of legacyMongoDBNames.
func legacyMongoUserNames(token string) []string {
	return distinctNames(mongoUserPrefix, token)
}

// distinctNames builds the ordered, de-duplicated list of historic identifiers
// for the given prefix: [canonical, legacy-short, legacy-raw-token].
func distinctNames(prefix, token string) []string {
	canonical := mongoCanonicalToken(token)

	legacyShort := canonical
	if len(legacyShort) > legacyShortLen {
		legacyShort = legacyShort[:legacyShortLen]
	}

	candidates := []string{
		prefix + canonical, // canonical (post-fix scheme)
		prefix + legacyShort, // legacy K8sBackend mongoK8sShort scheme
		prefix + token, // legacy LocalBackend raw-token scheme (dashes kept)
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
