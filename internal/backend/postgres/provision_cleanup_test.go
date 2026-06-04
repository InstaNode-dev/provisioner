package postgres

// provision_cleanup_test.go — regression coverage for SWEEP #9: a post-CREATE
// DDL failure in Provision / provisionLocal must roll back the partially-created
// database (and user) instead of stranding db_<token> / usr_<token> on the
// shared cluster with no reaper.
//
// These use the seam-driven fakePGConn (no live Postgres) so they run in the
// mock-only coverage CI job. execCalls records every SQL statement issued, so we
// assert the rollback DROPs are present after an injected failure.

import (
	"context"
	"strings"
	"testing"
)

// hasExec reports whether any recorded Exec statement contains sub.
func hasExec(calls []string, sub string) bool {
	for _, c := range calls {
		if strings.Contains(c, sub) {
			return true
		}
	}
	return false
}

// TestLocalBackend_Provision_CreateUserFailure_RollsBackDatabase asserts that
// when CREATE USER fails after CREATE DATABASE succeeded, Provision issues a
// best-effort DROP DATABASE (the user does not exist yet, but DROP USER IF
// EXISTS is harmless and also issued).
func TestLocalBackend_Provision_CreateUserFailure_RollsBackDatabase(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{"CREATE USER": errSeam}}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")

	if _, err := b.Provision(context.Background(), "tok", "hobby", 5); err == nil {
		t.Fatal("expected CREATE USER error")
	}
	if !hasExec(fc.execCalls, "DROP DATABASE IF EXISTS") {
		t.Errorf("CREATE USER failure must roll back the database; execCalls=%v", fc.execCalls)
	}
	if !hasExec(fc.execCalls, "DROP USER IF EXISTS") {
		t.Errorf("rollback should also issue DROP USER IF EXISTS (idempotent); execCalls=%v", fc.execCalls)
	}
}

// TestLocalBackend_Provision_GrantFailure_RollsBackBoth asserts that when GRANT
// fails after both CREATE DATABASE and CREATE USER succeeded, Provision drops
// BOTH the database and the user.
func TestLocalBackend_Provision_GrantFailure_RollsBackBoth(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{"GRANT ALL PRIVILEGES ON DATABASE": errSeam}}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")

	if _, err := b.Provision(context.Background(), "tok", "hobby", -1); err == nil {
		t.Fatal("expected GRANT DATABASE error")
	}
	if !hasExec(fc.execCalls, "DROP DATABASE IF EXISTS") {
		t.Errorf("GRANT failure must roll back the database; execCalls=%v", fc.execCalls)
	}
	if !hasExec(fc.execCalls, "DROP USER IF EXISTS") {
		t.Errorf("GRANT failure must roll back the user; execCalls=%v", fc.execCalls)
	}
}

// TestLocalBackend_Provision_RollbackDropErrors_NonFatal exercises the
// best-effort error-log branches inside cleanupProvisionPartial: even when both
// rollback DROPs themselves fail, Provision still returns the original DDL
// error (the rollback failure is swallowed, not surfaced).
func TestLocalBackend_Provision_RollbackDropErrors_NonFatal(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{
		"CREATE USER":             errSeam,
		"DROP DATABASE IF EXISTS": errSeam,
		"DROP USER IF EXISTS":     errSeam,
	}}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")

	_, err := b.Provision(context.Background(), "tok", "hobby", -1)
	if err == nil {
		t.Fatal("expected CREATE USER error")
	}
	if !strings.Contains(err.Error(), "CREATE USER") {
		t.Errorf("returned error should be the original CREATE USER failure, not a rollback error; got %v", err)
	}
}

// TestDedicated_ProvisionLocal_CreateUserFailure_RollsBackDatabase mirrors the
// local-backend rollback assertion for the dedicated provider's local path.
func TestDedicated_ProvisionLocal_CreateUserFailure_RollsBackDatabase(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{"CREATE USER": errSeam}}
	withPGXConnect(t, fc, nil)
	p := &DedicatedProvider{adminDSN: "x"}

	if _, err := p.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Fatal("expected CREATE USER error")
	}
	if !hasExec(fc.execCalls, "DROP DATABASE IF EXISTS") {
		t.Errorf("dedicated CREATE USER failure must roll back the database; execCalls=%v", fc.execCalls)
	}
	if !hasExec(fc.execCalls, "DROP USER IF EXISTS") {
		t.Errorf("dedicated rollback should also issue DROP USER IF EXISTS; execCalls=%v", fc.execCalls)
	}
}

// TestDedicated_ProvisionLocal_GrantFailure_RollsBackBoth mirrors the GRANT
// rollback assertion for the dedicated provider's local path.
func TestDedicated_ProvisionLocal_GrantFailure_RollsBackBoth(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{"GRANT ALL PRIVILEGES ON DATABASE": errSeam}}
	withPGXConnect(t, fc, nil)
	p := &DedicatedProvider{adminDSN: "x"}

	if _, err := p.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Fatal("expected GRANT DATABASE error")
	}
	if !hasExec(fc.execCalls, "DROP DATABASE IF EXISTS") {
		t.Errorf("dedicated GRANT failure must roll back the database; execCalls=%v", fc.execCalls)
	}
	if !hasExec(fc.execCalls, "DROP USER IF EXISTS") {
		t.Errorf("dedicated GRANT failure must roll back the user; execCalls=%v", fc.execCalls)
	}
}
