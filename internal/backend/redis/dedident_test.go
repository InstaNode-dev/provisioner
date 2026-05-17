package redis

import "testing"

// fullToken is a representative 32-hex-char resource token. Two test tokens
// below deliberately share their first 8 hex characters to exercise the
// truncation-collision the fix closes.
const (
	tokenA          = "abc12345deadbeefcafef00d00112233"
	tokenB          = "abc12345111122223333444455556666" // shares "abc12345" prefix with tokenA
	shortToken      = "abc"                              // shorter than the 8-char legacy slice
	exactEightToken = "abc12345"                         // exactly the legacy slice length
)

// TestDedicatedACLUsername_UsesFullToken — the core fix: the canonical ACL
// username embeds the FULL token so two tokens sharing an 8-char prefix never
// collide on one ACL user.
func TestDedicatedACLUsername_UsesFullToken(t *testing.T) {
	gotA := dedicatedACLUsername(tokenA)
	gotB := dedicatedACLUsername(tokenB)

	if want := dedicatedACLUserPrefix + tokenA; gotA != want {
		t.Errorf("dedicatedACLUsername(tokenA) = %q; want %q", gotA, want)
	}
	if gotA == gotB {
		t.Errorf("dedicatedACLUsername collided for two distinct tokens sharing an 8-char prefix: both = %q", gotA)
	}
}

// TestLegacyDedicatedACLUsername_8CharSlice verifies the legacy probe form is
// exactly ded_<token[:8]> for a long token, and "" for tokens too short to
// have ever been truncated.
func TestLegacyDedicatedACLUsername_8CharSlice(t *testing.T) {
	if got, want := legacyDedicatedACLUsername(tokenA), dedicatedACLUserPrefix+tokenA[:legacyDedicatedACLUserShortLen]; got != want {
		t.Errorf("legacyDedicatedACLUsername(tokenA) = %q; want %q", got, want)
	}
	// tokenA and tokenB share their 8-char prefix — this IS the historical
	// collision the fix closes.
	if legacyDedicatedACLUsername(tokenA) != legacyDedicatedACLUsername(tokenB) {
		t.Error("expected the legacy 8-char scheme to collide for tokenA/tokenB (the bug being fixed)")
	}
	if got := legacyDedicatedACLUsername(shortToken); got != "" {
		t.Errorf("legacyDedicatedACLUsername(shortToken) = %q; want \"\" (too short to truncate)", got)
	}
	if got := legacyDedicatedACLUsername(exactEightToken); got != "" {
		t.Errorf("legacyDedicatedACLUsername(exactEightToken) = %q; want \"\" (len == slice, no truncation)", got)
	}
}

// TestResolveDedicatedACLUsername_PrefersStoredPRID — a lifecycle RPC must use
// the username STORED at provision time, never re-derive it.
func TestResolveDedicatedACLUsername_PrefersStoredPRID(t *testing.T) {
	stored := dedicatedACLUsername(tokenA)
	if got := resolveDedicatedACLUsername(tokenA, stored); got != stored {
		t.Errorf("resolveDedicatedACLUsername with stored PRID = %q; want %q", got, stored)
	}
	// Even a non-derivable PRID value must be honoured verbatim — the stored
	// value is authoritative.
	if got := resolveDedicatedACLUsername(tokenA, "ded_custom_name"); got != "ded_custom_name" {
		t.Errorf("resolveDedicatedACLUsername must honour the stored PRID verbatim; got %q", got)
	}
}

// TestResolveDedicatedACLUsername_LegacyFallback — the coverage test for the
// legacy path: a row with an empty provider_resource_id (provisioned before
// this fix shipped) must still resolve to a usable canonical username, and the
// legacy 8-char form must still be derivable for teardown. This test fails if
// a future change drops the empty-PRID fallback.
func TestResolveDedicatedACLUsername_LegacyFallback(t *testing.T) {
	// Empty provider_resource_id → canonical full-token derivation.
	if got, want := resolveDedicatedACLUsername(tokenA, ""), dedicatedACLUsername(tokenA); got != want {
		t.Errorf("resolveDedicatedACLUsername(tokenA, \"\") = %q; want full-token derivation %q", got, want)
	}
	// The legacy 8-char username for an old row is still derivable so
	// Deprovision can probe it.
	if got := legacyDedicatedACLUsername(tokenA); got == "" {
		t.Error("legacy 8-char username must remain derivable for teardown of pre-fix rows")
	}
}
