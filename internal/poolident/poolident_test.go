package poolident

import "testing"

// TestEncode_NamingToken_RoundTrip is the P0-2 coverage test: it asserts that
// for every shape of provider_resource_id a pool-claimed resource can have, the
// pool token Encode() stamps in is the one NamingToken() resolves back out —
// and that a non-pool (live-provisioned / legacy) resource always resolves to
// the request token. A regression that drops the marker, mangles the segment
// separator, or fails to round-trip will fail here.
func TestEncode_NamingToken_RoundTrip(t *testing.T) {
	const (
		realToken = "11111111-2222-3333-4444-555555555555"
		poolToken = "pool-99999999-8888-7777-6666-555555555555"
	)

	cases := []struct {
		name        string
		basePRID    string // what the backend would return without pool involvement
		poolToken   string // "" for a live (non-pool) provision
		wantNaming  string // expected NamingToken result
		wantBase    string // expected BasePRID result
		wantPoolTok string // expected PoolToken result
	}{
		{
			name:        "redis/mongo local pool hit (empty base prid)",
			basePRID:    "",
			poolToken:   poolToken,
			wantNaming:  poolToken,
			wantBase:    "",
			wantPoolTok: poolToken,
		},
		{
			name:        "postgres local pool hit (local:N cluster segment)",
			basePRID:    "local:0",
			poolToken:   poolToken,
			wantNaming:  poolToken,
			wantBase:    "local:0",
			wantPoolTok: poolToken,
		},
		{
			name:        "postgres local pool hit on a non-zero cluster",
			basePRID:    "local:3",
			poolToken:   poolToken,
			wantNaming:  poolToken,
			wantBase:    "local:3",
			wantPoolTok: poolToken,
		},
		{
			name:        "live (non-pool) redis/mongo provision — empty prid",
			basePRID:    "",
			poolToken:   "",
			wantNaming:  realToken,
			wantBase:    "",
			wantPoolTok: "",
		},
		{
			name:        "live (non-pool) postgres provision — local:N only",
			basePRID:    "local:1",
			poolToken:   "",
			wantNaming:  realToken,
			wantBase:    "local:1",
			wantPoolTok: "",
		},
		{
			name:        "k8s pool item — namespace already embeds the pool token, no marker",
			basePRID:    "instant-customer-" + poolToken,
			poolToken:   poolToken,
			wantNaming:  realToken, // NamingToken falls through: backend uses the namespace itself
			wantBase:    "instant-customer-" + poolToken,
			wantPoolTok: "",
		},
		{
			name:        "legacy row — k8s namespace, no pool involvement",
			basePRID:    "instant-customer-" + realToken,
			poolToken:   "",
			wantNaming:  realToken,
			wantBase:    "instant-customer-" + realToken,
			wantPoolTok: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			prid := Encode(tc.basePRID, tc.poolToken)

			if got := NamingToken(realToken, prid); got != tc.wantNaming {
				t.Errorf("NamingToken(%q, %q) = %q, want %q", realToken, prid, got, tc.wantNaming)
			}
			if got := BasePRID(prid); got != tc.wantBase {
				t.Errorf("BasePRID(%q) = %q, want %q", prid, got, tc.wantBase)
			}
			if got := PoolToken(prid); got != tc.wantPoolTok {
				t.Errorf("PoolToken(%q) = %q, want %q", prid, got, tc.wantPoolTok)
			}
		})
	}
}

// TestEncode_K8sNamespace_IsNeverMarked guards the invariant that a k8s
// namespace base prid is returned verbatim by Encode. The k8s backends use the
// provider_resource_id directly as a namespace name; appending a ";pooltok:…"
// suffix would produce an invalid namespace and break Deprovision.
func TestEncode_K8sNamespace_IsNeverMarked(t *testing.T) {
	ns := "instant-customer-pool-abc"
	if got := Encode(ns, "pool-abc"); got != ns {
		t.Fatalf("Encode must not mark a k8s namespace: got %q, want %q", got, ns)
	}
}

// TestPoolToken_NoMarker returns "" for values that carry no marker, so the
// fallback to the request token is taken for live-provisioned and legacy rows.
func TestPoolToken_NoMarker(t *testing.T) {
	for _, prid := range []string{"", "local:0", "instant-customer-xyz", "garbage"} {
		if got := PoolToken(prid); got != "" {
			t.Errorf("PoolToken(%q) = %q, want empty", prid, got)
		}
	}
}
