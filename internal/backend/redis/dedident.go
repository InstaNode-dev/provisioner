package redis

// dedident.go — canonical identifier helper for the DEDICATED (local-admin)
// Redis backend's ACL username.
//
// # Why this exists (token-truncation class, P1 BUGHUNT-REPORT-2026-05-17-round2)
//
// The dedicated backend used to derive its ACL username by truncating the
// token to its first 8 hex characters:
//
//	short := token; if len(short) > 8 { short = short[:8] }
//	username := "ded_" + short
//
// An 8-hex-char prefix has only 2^32 possibilities, so two distinct dedicated-
// Redis tokens that happen to share their first 8 characters map to the SAME
// ACL user. `ACL SETUSER` is an upsert, so provisioning tenant B clobbers
// tenant A's password/keyspace grant; a later `ACL DELUSER` for either tenant
// kills the survivor; the worker's quota-suspend `ACL SETUSER <user> off`
// targets the wrong tenant.
//
// # The fix: full-token username, stored at provision time, never re-derived
//
// New provisions create the ACL user under dedicatedACLUsername(token) — the
// FULL token, which collides only on a genuine token collision (cryptographic
// improbability). The provisioner stamps the canonical username it actually
// created into ProviderResourceID, the api persists it on the resource row,
// and every lifecycle RPC (StorageBytes / Deprovision) resolves the username
// via resolveDedicatedACLUsername(): the STORED value when present, the
// full-token derivation otherwise.
//
// Legacy rows (provisioned before this fix, provider_resource_id empty/NULL)
// have their ACL user under the old ded_<token[:8]> name. resolveDedicated-
// ACLUsername falls back to legacyDedicatedACLUsername() for them, and
// Deprovision probes BOTH names, so existing dedicated-Redis pods keep working
// unchanged — no pod recycle, no ACL-user migration required.

// dedicatedACLUserPrefix is the fixed prefix for every ACL user the dedicated
// (local-admin) Redis backend creates.
const dedicatedACLUserPrefix = "ded_"

// legacyDedicatedACLUserShortLen is the truncation length used by the pre-fix
// dedicated-username scheme (dedicatedACLUserPrefix + token[:8]). Retained ONLY
// so an ACL user created under the old truncated scheme can still be located
// and deleted. New provisions never use it.
const legacyDedicatedACLUserShortLen = 8

// dedicatedACLUsername returns the canonical ACL username for a dedicated-Redis
// token: the FULL token, not a truncated prefix, so two tokens can never
// collide on the same ACL user.
func dedicatedACLUsername(token string) string {
	return dedicatedACLUserPrefix + token
}

// legacyDedicatedACLUsername returns the pre-fix 8-char-prefix ACL username for
// a token, or "" when the token is too short to have been truncated (in which
// case the canonical name already equals the legacy name and no extra probe is
// needed). Deprovision probes this so dedicated-Redis ACL users created before
// the truncation fix shipped are still cleaned up.
func legacyDedicatedACLUsername(token string) string {
	if len(token) <= legacyDedicatedACLUserShortLen {
		return ""
	}
	return dedicatedACLUserPrefix + token[:legacyDedicatedACLUserShortLen]
}

// resolveDedicatedACLUsername returns the ACL username a lifecycle RPC
// (StorageBytes / Deprovision) must target for a dedicated-Redis resource.
//
// It prefers the providerResourceID stamped on the resource row at provision
// time — that is the EXACT username the provisioner created, so no re-
// derivation can drift. It falls back to the canonical full-token derivation
// when providerResourceID is empty, which covers two cases:
//   - rows provisioned before this fix shipped (provider_resource_id NULL); for
//     those the ACL user is actually under the legacy 8-char name, but callers
//     that need teardown probe legacyDedicatedACLUsername() in addition;
//   - rows provisioned by a build that has this fix but where the caller did
//     not thread providerResourceID through.
func resolveDedicatedACLUsername(token, providerResourceID string) string {
	if providerResourceID != "" {
		return providerResourceID
	}
	return dedicatedACLUsername(token)
}
