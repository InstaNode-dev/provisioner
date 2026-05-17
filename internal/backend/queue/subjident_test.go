package queue

import (
	"strings"
	"testing"
)

// fullToken / collidingToken deliberately share their first 8 hex characters so
// the tests exercise the truncation-collision the fix closes. dashToken carries
// dashes, exercising the NATS-subject-token dash stripping.
const (
	fullToken      = "abc12345deadbeefcafef00d00112233"
	collidingToken = "abc12345111122223333444455556666" // shares "abc12345" prefix with fullToken
	dashToken      = "6fffcc21-dead-beef-cafe-f00d00112233"
	shortToken     = "abc"      // shorter than the 8-char legacy slice
	eightCharToken = "abc12345" // exactly the legacy slice length
)

// TestCanonicalSubjectPrefix_UsesFullToken is the core fix: on the shared NATS
// backend the SubjectPrefix is the ONLY tenant isolation, so it must derive
// from the FULL token. Two tokens sharing an 8-char prefix must NOT collide.
func TestCanonicalSubjectPrefix_UsesFullToken(t *testing.T) {
	a := canonicalSubjectPrefix(fullToken)
	b := canonicalSubjectPrefix(collidingToken)

	if want := fullToken + subjectPrefixSep; a != want {
		t.Errorf("canonicalSubjectPrefix(fullToken) = %q; want %q", a, want)
	}
	if a == b {
		t.Errorf("canonicalSubjectPrefix collided for two tokens sharing an 8-char prefix: both = %q", a)
	}
}

// TestCanonicalSubjectPrefix_StripsDashes — a dash-stripped UUID is a single
// valid NATS subject token; the canonical prefix must contain no '-'.
func TestCanonicalSubjectPrefix_StripsDashes(t *testing.T) {
	got := canonicalSubjectPrefix(dashToken)
	if strings.Contains(strings.TrimSuffix(got, subjectPrefixSep), "-") {
		t.Errorf("canonicalSubjectPrefix(dashToken) = %q; must not contain '-' (invalid NATS subject token)", got)
	}
	if want := stripDashes(dashToken) + subjectPrefixSep; got != want {
		t.Errorf("canonicalSubjectPrefix(dashToken) = %q; want %q", got, want)
	}
}

// TestLegacySubjectPrefix_8CharSlice — the legacy probe form is exactly
// token[:8] + ".", and "" for tokens too short to have ever been truncated.
// The 8-char-prefix collision is the historical bug being closed.
func TestLegacySubjectPrefix_8CharSlice(t *testing.T) {
	if got, want := legacySubjectPrefix(fullToken), fullToken[:legacySubjectShortLen]+subjectPrefixSep; got != want {
		t.Errorf("legacySubjectPrefix(fullToken) = %q; want %q", got, want)
	}
	if legacySubjectPrefix(fullToken) != legacySubjectPrefix(collidingToken) {
		t.Error("expected the legacy 8-char scheme to collide for fullToken/collidingToken (the bug being fixed)")
	}
	if got := legacySubjectPrefix(shortToken); got != "" {
		t.Errorf("legacySubjectPrefix(shortToken) = %q; want \"\" (too short to truncate)", got)
	}
	if got := legacySubjectPrefix(eightCharToken); got != "" {
		t.Errorf("legacySubjectPrefix(eightCharToken) = %q; want \"\" (len == slice, no truncation)", got)
	}
}

// TestResolveSubjectPrefix_PrefersStoredPRID — a lifecycle path must use the
// prefix STORED at provision time, never re-derive it.
func TestResolveSubjectPrefix_PrefersStoredPRID(t *testing.T) {
	stored := canonicalSubjectPrefix(fullToken)
	if got := resolveSubjectPrefix(fullToken, stored); got != stored {
		t.Errorf("resolveSubjectPrefix with stored PRID = %q; want %q", got, stored)
	}
	// A non-derivable stored value must be honoured verbatim.
	if got := resolveSubjectPrefix(fullToken, "legacy.prefix."); got != "legacy.prefix." {
		t.Errorf("resolveSubjectPrefix must honour the stored PRID verbatim; got %q", got)
	}
}

// TestResolveSubjectPrefix_LegacyFallback — the coverage test for the legacy
// path: a resource with an empty provider_resource_id (provisioned before this
// fix shipped) must still resolve to a usable canonical prefix, and the legacy
// 8-char form must remain derivable. Fails if a future change drops either.
func TestResolveSubjectPrefix_LegacyFallback(t *testing.T) {
	if got, want := resolveSubjectPrefix(fullToken, ""), canonicalSubjectPrefix(fullToken); got != want {
		t.Errorf("resolveSubjectPrefix(fullToken, \"\") = %q; want canonical derivation %q", got, want)
	}
	if got := legacySubjectPrefix(fullToken); got == "" {
		t.Error("legacy 8-char prefix must remain derivable for route-lookup of pre-fix resources")
	}
}
