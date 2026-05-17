package redis

import (
	"testing"

	"instant.dev/provisioner/internal/poolident"
)

// TestACLUsername_FullToken verifies P1-D: the canonical ACL username is
// derived from the FULL token, so two tokens that share an 8-char prefix can
// never collide on the same ACL user (the pre-fix bug that let one tenant's
// ACL SETUSER overwrite another's credentials).
func TestACLUsername_FullToken(t *testing.T) {
	// Two distinct tokens sharing their first 8 hex chars.
	tokA := "abcd1234aaaaaaaa"
	tokB := "abcd1234bbbbbbbb"

	uA := aclUsername(tokA)
	uB := aclUsername(tokB)

	if uA == uB {
		t.Fatalf("aclUsername collision: %q == %q — full-token derivation is broken (P1-D)", uA, uB)
	}
	if uA != aclUserPrefix+tokA {
		t.Errorf("aclUsername(%q) = %q, want %q", tokA, uA, aclUserPrefix+tokA)
	}
}

// TestLegacyACLUsername verifies the backward-compat fallback used by
// Deprovision: for a long token it returns the pre-P1-D 8-char-prefix form so
// users provisioned before the fix are still cleaned up; for a short token it
// returns "" because the canonical name already equals the legacy name (no
// extra DELUSER probe needed).
func TestLegacyACLUsername(t *testing.T) {
	cases := []struct {
		name  string
		token string
		want  string
	}{
		{"long token truncates to 8 chars", "abcd1234ZZZZZZ", aclUserPrefix + "abcd1234"},
		{"exactly 8 chars → no legacy form", "abcd1234", ""},
		{"short token → no legacy form", "abc", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := legacyACLUsername(tc.token); got != tc.want {
				t.Errorf("legacyACLUsername(%q) = %q, want %q", tc.token, got, tc.want)
			}
		})
	}
}

// TestDeprovisionUsernameCandidates verifies that Deprovision probes both the
// canonical full-token username and the legacy 8-char form, so an ACL user
// created before P1-D is still deleted while new users use the full-token name.
func TestDeprovisionUsernameCandidates(t *testing.T) {
	token := "abcd1234deadbeef"

	// Mirror the candidate list Deprovision iterates.
	candidates := []string{aclUsername(token), legacyACLUsername(token)}

	wantCanonical := aclUserPrefix + token
	wantLegacy := aclUserPrefix + "abcd1234"

	if candidates[0] != wantCanonical {
		t.Errorf("canonical candidate = %q, want %q", candidates[0], wantCanonical)
	}
	if candidates[1] != wantLegacy {
		t.Errorf("legacy candidate = %q, want %q", candidates[1], wantLegacy)
	}
	if candidates[0] == candidates[1] {
		t.Error("canonical and legacy candidates must differ for a long token")
	}
}

// TestPoolClaimedRedisNamesDeriveFromPoolToken is the P0-2 regression guard for
// the shared Redis backend: a pool-claimed cache's ACL user and keyspace are
// named from the pool token. Deprovision/StorageBytes resolve the canonical
// naming token from provider_resource_id (carrying a poolident marker) — if
// they fell back to the request token, DELUSER/SCAN would target a name that
// was never created and the real usr_pool-<uuid>/keyspace would leak.
func TestPoolClaimedRedisNamesDeriveFromPoolToken(t *testing.T) {
	const (
		realToken = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
		poolToken = "pool-12345678-90ab-cdef-1234-567890abcdef"
	)
	prid := poolident.Encode("", poolToken)

	// Exact expressions Deprovision / StorageBytes use.
	gotUser := aclUsername(poolident.NamingToken(realToken, prid))
	gotKeyspace := poolident.NamingToken(realToken, prid) + ":*"

	if want := aclUsername(poolToken); gotUser != want {
		t.Errorf("pool-claimed ACL user = %q, want %q (would leak otherwise)", gotUser, want)
	}
	if want := poolToken + ":*"; gotKeyspace != want {
		t.Errorf("pool-claimed keyspace = %q, want %q", gotKeyspace, want)
	}

	// A live (non-pool) provision (empty provider_resource_id) keeps deriving
	// from the request token — the normal path must be unchanged.
	if got := aclUsername(poolident.NamingToken(realToken, "")); got != aclUsername(realToken) {
		t.Errorf("live ACL user = %q, want %q (non-pool path must be unchanged)", got, aclUsername(realToken))
	}
}
