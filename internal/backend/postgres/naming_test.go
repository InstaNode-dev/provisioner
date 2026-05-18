package postgres

import (
	"strings"
	"testing"
)

// TestK8sName_CanonicalStripsDashesNoTruncation is the core P1-W5-05 regression
// guard: the canonical DB/role name must use the FULL dash-stripped token. A
// return to the truncating k8sShort scheme would re-introduce the collision.
func TestK8sName_CanonicalStripsDashesNoTruncation(t *testing.T) {
	token := "550e8400-e29b-41d4-a716-446655440000"
	if got, want := k8sDBName(token), "db_550e8400e29b41d4a716446655440000"; got != want {
		t.Fatalf("k8sDBName(%q) = %q, want %q", token, got, want)
	}
	if got, want := k8sRoleName(token), "usr_550e8400e29b41d4a716446655440000"; got != want {
		t.Fatalf("k8sRoleName(%q) = %q, want %q", token, got, want)
	}
	// No truncation: the hex body must be the full 32 chars.
	if body := strings.TrimPrefix(k8sDBName(token), k8sDBPrefix); len(body) != 32 {
		t.Fatalf("canonical DB body length = %d, want 32 (no truncation)", len(body))
	}
	// db_ + 32-hex = 35 chars, well under the Postgres 63-byte identifier limit.
	if len(k8sDBName(token)) > 63 || len(k8sRoleName(token)) > 63 {
		t.Fatalf("canonical identifier exceeds Postgres 63-byte limit: db=%d role=%d",
			len(k8sDBName(token)), len(k8sRoleName(token)))
	}
}

// TestK8sName_NoCollisionOnSharedPrefix proves the bug P1-W5-05 fixes: two
// Team-tier tokens sharing their first 12 dash-stripped hex digits must NOT map
// to the same Postgres database OR the same app role.
func TestK8sName_NoCollisionOnSharedPrefix(t *testing.T) {
	a := "abcdef012345-1111-1111-1111-111111111111"
	b := "abcdef012345-2222-2222-2222-222222222222"

	// Sanity: both tokens DO share the first 12 dash-stripped hex chars, so the
	// pre-fix k8sShort(token[:12]) scheme would have collided them.
	if k8sCanonicalToken(a)[:legacyK8sShortLen] != k8sCanonicalToken(b)[:legacyK8sShortLen] {
		t.Fatalf("test setup invalid: tokens do not share a %d-char prefix", legacyK8sShortLen)
	}

	if k8sDBName(a) == k8sDBName(b) {
		t.Fatalf("DB collision: k8sDBName(%q) == k8sDBName(%q) == %q", a, b, k8sDBName(a))
	}
	if k8sRoleName(a) == k8sRoleName(b) {
		t.Fatalf("role collision: k8sRoleName(%q) == k8sRoleName(%q) == %q", a, b, k8sRoleName(a))
	}
}

// TestLegacyK8sDBNames_ResolverFindsTruncatedResource verifies the lookup
// fallback list contains the canonical name plus the legacy 12-char-truncated
// name (canonical first), so StorageBytes/Deprovision/Regrade still resolve a
// resource provisioned under the pre-fix k8sShort scheme.
func TestLegacyK8sDBNames_ResolverFindsTruncatedResource(t *testing.T) {
	token := "550e8400-e29b-41d4-a716-446655440000"

	canonical := "db_550e8400e29b41d4a716446655440000" // post-fix scheme
	legacyShort := "db_550e8400e29b"                   // pre-fix k8sShort (token[:12])

	got := legacyK8sDBNames(token)
	if len(got) != 2 {
		t.Fatalf("legacyK8sDBNames returned %d names, want 2: %v", len(got), got)
	}
	if got[0] != canonical {
		t.Fatalf("canonical name must be first; got[0]=%q want %q", got[0], canonical)
	}
	if got[1] != legacyShort {
		t.Fatalf("legacy truncated name must be second; got[1]=%q want %q", got[1], legacyShort)
	}

	// The resolver must contain the legacy name a pre-fix resource lives under.
	var foundLegacy bool
	for _, n := range got {
		if n == legacyShort {
			foundLegacy = true
		}
	}
	if !foundLegacy {
		t.Fatalf("resolver %v does not include legacy truncated name %q", got, legacyShort)
	}

	// Role-name resolver mirrors the DB-name resolver.
	gotRoles := legacyK8sRoleNames(token)
	if len(gotRoles) != 2 || gotRoles[0] != "usr_550e8400e29b41d4a716446655440000" || gotRoles[1] != "usr_550e8400e29b" {
		t.Fatalf("legacyK8sRoleNames(%q) = %v, want [canonical, legacy-12-char]", token, gotRoles)
	}
}

// TestLegacyK8sDBNames_DeduplicatesShortTokens ensures a token already <= 12
// dash-stripped chars does not produce duplicate candidate names (canonical ==
// legacy-short for such tokens).
func TestLegacyK8sDBNames_DeduplicatesShortTokens(t *testing.T) {
	token := "abc123" // 6 chars, no dashes: canonical == legacy-short
	got := legacyK8sDBNames(token)
	if len(got) != 1 {
		t.Fatalf("legacyK8sDBNames(%q) = %v, want exactly 1 deduplicated name", token, got)
	}
	if got[0] != "db_abc123" {
		t.Fatalf("got[0] = %q, want db_abc123", got[0])
	}
}
