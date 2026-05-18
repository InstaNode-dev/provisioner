package postgres

// local_deprovision_test.go — regression test for the BugBash-2026-05-18 P2-07
// fix: LocalBackend.Deprovision was not idempotent. It dropped the database
// first and returned Internal on ANY failure, so a transient "database is being
// accessed by other users" race aborted the RPC before DROP USER ran, leaking
// the role forever. The fix: retry the in-use race, and run DROP USER
// unconditionally. isDatabaseInUseErr is the predicate that decides what to
// retry — pinning it here guards the retry/terminal classification.

import (
	"errors"
	"testing"
)

func TestIsDatabaseInUseErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil error", nil, false},
		{
			"the in-use race — retryable",
			errors.New(`db.local.Deprovision: DROP DATABASE: ERROR: database "db_tok" is being accessed by other users (SQLSTATE 55006)`),
			true,
		},
		{
			"in-use race, mixed case",
			errors.New("database is BEING ACCESSED BY OTHER USERS"),
			true,
		},
		{
			"permission denied — terminal, must NOT retry",
			errors.New(`ERROR: must be owner of database "db_tok" (SQLSTATE 42501)`),
			false,
		},
		{
			"connection failure — terminal for the retry loop",
			errors.New("failed to connect: connection refused"),
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isDatabaseInUseErr(tc.err); got != tc.want {
				t.Errorf("isDatabaseInUseErr(%v) = %v; want %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestDeprovisionDropDBAttempts_Sane — the retry budget must be > 1 (a single
// attempt is exactly the non-idempotent pre-fix behaviour) and bounded.
func TestDeprovisionDropDBAttempts_Sane(t *testing.T) {
	if deprovisionDropDBAttempts < 2 {
		t.Errorf("deprovisionDropDBAttempts = %d; want >= 2 so the in-use race is actually retried", deprovisionDropDBAttempts)
	}
	if deprovisionDropDBAttempts > 10 {
		t.Errorf("deprovisionDropDBAttempts = %d; unreasonably high", deprovisionDropDBAttempts)
	}
}
