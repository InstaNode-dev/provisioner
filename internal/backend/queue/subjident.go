package queue

// subjident.go — canonical identifier helper for a queue resource's NATS
// SubjectPrefix.
//
// # Why this exists (token-truncation class, the highest-recurrence bug class
// in this codebase — see redis/dedident.go, poolident, postgres/local.go)
//
// Both queue backends used to derive the SubjectPrefix by truncating the token
// to its first 8 hex characters:
//
//	prefix := token; if len(prefix) > 8 { prefix = prefix[:8] }
//	SubjectPrefix = prefix + "."
//
// On the SHARED NATS backend (local.go) the SubjectPrefix is the ONLY tenant
// isolation boundary — NATS runs without authentication, so two tokens that
// happen to share their first 8 hex characters share a subject namespace and
// can publish/subscribe to each other's events. An 8-hex-char prefix has only
// 2^32 possibilities. On the dedicated k8s backend the pod is the isolation
// boundary, so the impact there is lower, but the truncation is the same class
// and is fixed identically for consistency.
//
// # The fix: full-token-derived subject prefix for NEW provisions
//
// canonicalSubjectPrefix(token) derives the prefix from the FULL token. NATS
// subject tokens permit ASCII alphanumerics but NOT '.' (the subject separator)
// or '*'/'>' (wildcards); a UUID's dashes are also not valid subject-token
// characters, so they are stripped. A dash-stripped resource token is a plain
// alphanumeric string and is therefore a valid single NATS subject token.
//
// # Backward compatibility
//
// The SubjectPrefix is part of the customer connection contract: queues already
// provisioned under the old token[:8] scheme must keep working. Mirroring the
// dedident.go / poolident.go legacy-fallback pattern:
//
//   - NEW provisions use canonicalSubjectPrefix(token).
//   - resolveSubjectPrefix(token, providerResourceID) resolves the prefix a
//     lifecycle path must use for an EXISTING resource — the value stamped on
//     provider_resource_id at provision time when present, the canonical
//     full-token derivation otherwise.
//   - legacySubjectPrefix(token) reproduces the pre-fix token[:8] prefix so a
//     route-lookup / teardown for a pre-fix resource can still locate it.
//
// The shared (local) backend has no per-user server state, so its Deprovision
// is a no-op and never needs to resolve a prefix; the helpers are nonetheless
// provided so any future route-lookup path resolves prefixes uniformly.

const (
	// subjectPrefixSep terminates a SubjectPrefix so callers form subjects of
	// the shape "<prefix><event-name>".
	subjectPrefixSep = "."

	// legacySubjectShortLen is the truncation length of the pre-fix
	// SubjectPrefix scheme (token[:8] + "."). Retained ONLY so a prefix created
	// under the old truncated scheme can still be located. New provisions never
	// use it.
	legacySubjectShortLen = 8
)

// stripDashes removes '-' characters so a UUID-style token becomes a single
// valid NATS subject token (NATS subject tokens permit alphanumerics but not
// '.', '*', '>' — and a dash-stripped UUID is plain alphanumeric).
func stripDashes(token string) string {
	out := make([]byte, 0, len(token))
	for i := 0; i < len(token); i++ {
		if token[i] != '-' {
			out = append(out, token[i])
		}
	}
	return string(out)
}

// canonicalSubjectPrefix returns the canonical SubjectPrefix for a queue token:
// the FULL token (dashes stripped) followed by the subject separator. Two
// tokens can collide on this prefix only on a genuine token collision
// (cryptographic improbability), unlike the pre-fix 8-char truncation.
func canonicalSubjectPrefix(token string) string {
	return stripDashes(token) + subjectPrefixSep
}

// legacySubjectPrefix returns the pre-fix 8-char-truncated SubjectPrefix for a
// token, or "" when the token is too short to have ever been truncated (in
// which case the canonical prefix already equals the legacy prefix and no
// extra probe is needed). The token is dash-stripped first so the slice is
// taken over the same character space the legacy code truncated.
func legacySubjectPrefix(token string) string {
	stripped := stripDashes(token)
	if len(stripped) <= legacySubjectShortLen {
		return ""
	}
	return stripped[:legacySubjectShortLen] + subjectPrefixSep
}

// resolveSubjectPrefix returns the SubjectPrefix a lifecycle path must target
// for an EXISTING queue resource.
//
// It prefers providerResourceID — the value stamped at provision time — so no
// re-derivation can drift. It falls back to the canonical full-token derivation
// when providerResourceID carries no prefix, which covers both rows provisioned
// before this fix shipped and rows where the caller did not thread
// providerResourceID through. (The shared NATS backend has no per-user state,
// so its Deprovision never needs this; it exists for uniform resolution by any
// future route-lookup path.)
func resolveSubjectPrefix(token, providerResourceID string) string {
	if providerResourceID != "" {
		return providerResourceID
	}
	return canonicalSubjectPrefix(token)
}
