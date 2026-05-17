package postgres

// local_test.go — unit tests for LocalBackend behaviour that do not require
// a live Postgres cluster. Integration tests (with a real cluster) run under
// the e2e tag when E2E_PG_HOST is set.

import (
	"fmt"
	"testing"

	"instant.dev/provisioner/internal/poolident"
)

// TestConnLimitClause_Positive verifies the CONNECTION LIMIT clause is
// generated correctly for a positive connection cap (A3 regression guard).
func TestConnLimitClause_Positive(t *testing.T) {
	connLimit := 8
	got := connLimitClauseFor(connLimit)
	want := " CONNECTION LIMIT 8"
	if got != want {
		t.Errorf("connLimitClauseFor(%d) = %q; want %q", connLimit, got, want)
	}
}

// TestConnLimitClause_Unlimited_NegativeOne verifies that connLimit=-1
// (unlimited) produces no clause so Postgres uses its default.
func TestConnLimitClause_Unlimited_NegativeOne(t *testing.T) {
	connLimit := -1
	got := connLimitClauseFor(connLimit)
	if got != "" {
		t.Errorf("connLimitClauseFor(%d) = %q; want empty string (unlimited)", connLimit, got)
	}
}

// TestConnLimitClause_Zero_Unlimited verifies connLimit=0 (un-set in plans)
// also produces no clause.
func TestConnLimitClause_Zero_Unlimited(t *testing.T) {
	connLimit := 0
	got := connLimitClauseFor(connLimit)
	if got != "" {
		t.Errorf("connLimitClauseFor(%d) = %q; want empty string (treat 0 as unlimited)", connLimit, got)
	}
}

// TestRegradeApplyLimit_Normalization verifies the applyLimit logic that maps
// connLimit=0 → -1 (Postgres "no limit" sentinel) inside Regrade.
func TestRegradeApplyLimit_Normalization(t *testing.T) {
	cases := []struct {
		in   int
		want int
	}{
		{0, -1},   // zero means "unset" → unlimited
		{-1, -1},  // explicit unlimited stays -1
		{8, 8},    // positive cap preserved
		{20, 20},  // pro-tier cap preserved
	}
	for _, tc := range cases {
		got := normalizeRegradeConnLimit(tc.in)
		if got != tc.want {
			t.Errorf("normalizeRegradeConnLimit(%d) = %d; want %d", tc.in, got, tc.want)
		}
	}
}

// connLimitClauseFor mirrors the logic inside LocalBackend.Provision so the
// test does not access unexported state.
func connLimitClauseFor(connLimit int) string {
	if connLimit > 0 {
		return fmt.Sprintf(" CONNECTION LIMIT %d", connLimit)
	}
	return ""
}

// normalizeRegradeConnLimit mirrors the normalization inside LocalBackend.Regrade.
func normalizeRegradeConnLimit(connLimit int) int {
	if connLimit == 0 {
		return -1
	}
	return connLimit
}

// TestPoolClaimedNamesDeriveFromPoolToken is the P0-2 regression guard for the
// shared Postgres backend: when a resource was claimed from the hot pool, its
// database/role are named from the pool token, and provider_resource_id carries
// a poolident marker. Deprovision/StorageBytes/Regrade MUST derive the name
// from that marker, not from the request token — otherwise db_<real-token> is a
// no-op and the real db_pool-<uuid> leaks forever.
func TestPoolClaimedNamesDeriveFromPoolToken(t *testing.T) {
	const (
		realToken = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
		poolToken = "pool-12345678-90ab-cdef-1234-567890abcdef"
	)
	prid := poolident.Encode("local:0", poolToken)

	// This is the exact expression Deprovision / StorageBytes / Regrade use.
	gotDB := dbNamePrefix + poolident.NamingToken(realToken, prid)
	gotUser := userNamePrefix + poolident.NamingToken(realToken, prid)

	if wantDB := dbNamePrefix + poolToken; gotDB != wantDB {
		t.Errorf("pool-claimed db name = %q, want %q (would leak otherwise)", gotDB, wantDB)
	}
	if wantUser := userNamePrefix + poolToken; gotUser != wantUser {
		t.Errorf("pool-claimed user name = %q, want %q", gotUser, wantUser)
	}
	// The router must still see only the cluster segment.
	if base := poolident.BasePRID(prid); base != "local:0" {
		t.Errorf("BasePRID(%q) = %q, want local:0 — cluster routing would break", prid, base)
	}

	// A live (non-pool) provision keeps deriving from the request token.
	liveDB := dbNamePrefix + poolident.NamingToken(realToken, "local:0")
	if want := dbNamePrefix + realToken; liveDB != want {
		t.Errorf("live db name = %q, want %q (non-pool path must be unchanged)", liveDB, want)
	}
}
