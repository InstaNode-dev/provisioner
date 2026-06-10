package postgres

// dropcheck_test.go — unit tests for the name-convention guard on every DROP
// DATABASE / DROP USER site in this package (truehomie hardening, task D3).
// The invariant: a wrong-name bug must never reach a destructive statement,
// and legitimate per-tenant names must pass untouched.

import (
	"context"
	"errors"
	"testing"

	"instant.dev/provisioner/internal/dropguard"
)

func TestValidateDropTargets_OK(t *testing.T) {
	if err := validateDropTargets("test.site", "tok",
		"db_96edf9eed8ed42929036b63298ec5b2b",
		"usr_96edf9eed8ed42929036b63298ec5b2b"); err != nil {
		t.Fatalf("validateDropTargets: unexpected refusal: %v", err)
	}
}

func TestValidateDropTargets_RefusesBadDatabase(t *testing.T) {
	err := validateDropTargets("test.site", "tok", "instant_customers", "usr_ok123")
	if !errors.Is(err, dropguard.ErrRefused) {
		t.Fatalf("expected ErrRefused for system database, got %v", err)
	}
}

func TestValidateDropTargets_RefusesBadUser(t *testing.T) {
	// Database valid, user invalid — exercises the second check independently.
	err := validateDropTargets("test.site", "tok", "db_ok123", "instanode_admin")
	if !errors.Is(err, dropguard.ErrRefused) {
		t.Fatalf("expected ErrRefused for admin role, got %v", err)
	}
}

// TestLocalDeprovision_RefusedToken_NeverConnects asserts the guard refuses a
// reserved/system naming token BEFORE any admin connection or DROP statement.
// The backend is constructed with a nil router: if the guard let the call
// through, the router deref would panic — passing proves the early return.
func TestLocalDeprovision_RefusedToken_NeverConnects(t *testing.T) {
	b := &LocalBackend{}
	for _, tok := range []string{"postgres", "instant_customers", "", "x y"} {
		err := b.Deprovision(context.Background(), tok, "")
		if !errors.Is(err, dropguard.ErrRefused) {
			t.Fatalf("Deprovision(%q): expected ErrRefused, got %v", tok, err)
		}
	}
}

func TestCleanupProvisionPartial_RefusedNames_ExecutesNothing(t *testing.T) {
	conn := &fakePGConn{}
	cleanupProvisionPartial(conn, "instant_customers", "usr_ok123")
	if len(conn.execCalls) != 0 {
		t.Fatalf("rollback executed %d statements for a refused database name: %v", len(conn.execCalls), conn.execCalls)
	}
	cleanupProvisionPartial(conn, "db_ok123", "instanode_admin")
	if len(conn.execCalls) != 0 {
		t.Fatalf("rollback executed %d statements for a refused user name: %v", len(conn.execCalls), conn.execCalls)
	}
}

func TestCleanupProvisionPartial_ValidNames_StillExecutesDrops(t *testing.T) {
	// Behaviour preservation: the legitimate rollback path still runs both
	// IF EXISTS drops.
	conn := &fakePGConn{}
	cleanupProvisionPartial(conn, "db_ok123", "usr_ok123")
	if len(conn.execCalls) != 2 {
		t.Fatalf("rollback: want 2 statements (DROP DATABASE + DROP USER), got %d: %v", len(conn.execCalls), conn.execCalls)
	}
}

// TestDedicatedDeprovisionLocal_RefusedToken_NeverConnects mirrors the local
// test for the dedicated provider: a reserved token is refused before the
// admin DSN is ever dialed (empty adminDSN would otherwise attempt a real
// connection to the default customers URL).
func TestDedicatedDeprovisionLocal_RefusedToken_NeverConnects(t *testing.T) {
	p := &DedicatedProvider{}
	for _, tok := range []string{"postgres", "", "a b"} {
		err := p.deprovisionLocal(context.Background(), tok)
		if !errors.Is(err, dropguard.ErrRefused) {
			t.Fatalf("deprovisionLocal(%q): expected ErrRefused, got %v", tok, err)
		}
	}
}
