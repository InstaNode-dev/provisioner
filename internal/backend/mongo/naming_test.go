package mongo

import (
	"strings"
	"testing"
)

// TestMongoDBName_CanonicalStripsDashesNoTruncation is the core P0-5 regression
// guard: the canonical name must use the FULL dash-stripped token. A return to
// the truncating scheme would re-introduce the collision bug.
func TestMongoDBName_CanonicalStripsDashesNoTruncation(t *testing.T) {
	token := "550e8400-e29b-41d4-a716-446655440000"
	want := "db_550e8400e29b41d4a716446655440000"
	if got := mongoDBName(token); got != want {
		t.Fatalf("mongoDBName(%q) = %q, want %q", token, got, want)
	}
	if got := mongoUserName(token); got != "usr_550e8400e29b41d4a716446655440000" {
		t.Fatalf("mongoUserName(%q) = %q, want usr_...", token, got)
	}
	// No truncation: the hex body must be the full 32 chars.
	if body := strings.TrimPrefix(mongoDBName(token), mongoDBPrefix); len(body) != 32 {
		t.Fatalf("canonical body length = %d, want 32 (no truncation)", len(body))
	}
}

// TestMongoDBName_NoCollisionOnSharedPrefix proves the bug P0-5 fixes: two
// tokens sharing their first 12 hex digits must NOT map to the same DB name.
func TestMongoDBName_NoCollisionOnSharedPrefix(t *testing.T) {
	a := "abcdef012345-1111-1111-1111-111111111111"
	b := "abcdef012345-2222-2222-2222-222222222222"
	if mongoDBName(a) == mongoDBName(b) {
		t.Fatalf("collision: mongoDBName(%q) == mongoDBName(%q) == %q", a, b, mongoDBName(a))
	}
	if mongoUserName(a) == mongoUserName(b) {
		t.Fatalf("collision: mongoUserName(%q) == mongoUserName(%q)", a, b)
	}
}

// TestLegacyMongoDBNames_IncludesAllSchemes verifies the lookup fallback list
// contains the canonical name plus both legacy schemes, canonical first.
func TestLegacyMongoDBNames_IncludesAllSchemes(t *testing.T) {
	token := "550e8400-e29b-41d4-a716-446655440000"
	got := legacyMongoDBNames(token)

	canonical := "db_550e8400e29b41d4a716446655440000" // post-fix scheme
	legacyShort := "db_550e8400e29b"                   // pre-fix K8sBackend (12-char truncation)
	legacyRaw := "db_" + token                         // pre-fix LocalBackend (dashes kept)

	if len(got) != 3 {
		t.Fatalf("legacyMongoDBNames returned %d names, want 3: %v", len(got), got)
	}
	if got[0] != canonical {
		t.Fatalf("canonical name must be first; got[0]=%q want %q", got[0], canonical)
	}
	want := map[string]bool{canonical: true, legacyShort: true, legacyRaw: true}
	for _, n := range got {
		if !want[n] {
			t.Fatalf("unexpected name %q in %v", n, got)
		}
	}
}

// TestLegacyMongoDBNames_DeduplicatesShortTokens ensures a token that is
// already short / dashless does not produce duplicate candidate names.
func TestLegacyMongoDBNames_DeduplicatesShortTokens(t *testing.T) {
	token := "abc123" // 6 chars, no dashes: canonical == short == raw
	got := legacyMongoDBNames(token)
	if len(got) != 1 {
		t.Fatalf("legacyMongoDBNames(%q) = %v, want exactly 1 deduplicated name", token, got)
	}
	if got[0] != "db_abc123" {
		t.Fatalf("got[0] = %q, want db_abc123", got[0])
	}
}

// TestLegacyMongoUserNames_MirrorsDBNames sanity-checks the user-name list uses
// the usr_ prefix and the same scheme set as the DB-name list.
func TestLegacyMongoUserNames_MirrorsDBNames(t *testing.T) {
	token := "550e8400-e29b-41d4-a716-446655440000"
	got := legacyMongoUserNames(token)
	if len(got) != 3 {
		t.Fatalf("legacyMongoUserNames returned %d names, want 3: %v", len(got), got)
	}
	for _, n := range got {
		if !strings.HasPrefix(n, mongoUserPrefix) {
			t.Fatalf("user name %q missing %q prefix", n, mongoUserPrefix)
		}
	}
}
