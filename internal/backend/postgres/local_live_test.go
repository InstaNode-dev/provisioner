package postgres

// local_live_test.go — integration coverage for LocalBackend.Provision /
// StorageBytes / Deprovision / Regrade against a real Postgres cluster.
//
// Skipped unless TEST_POSTGRES_ADMIN_DSN (or CUSTOMER_POSTGRES_DSN) points at
// an admin DSN capable of CREATE DATABASE / CREATE USER. CI's coverage.yml
// wires a docker postgres for this purpose; the local docker `test-pg`
// container (postgres://postgres:postgres@localhost:5432/postgres) works for
// developer runs.

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"instant.dev/provisioner/internal/poolident"
)

// testAdminDSN returns the admin DSN to drive live-Postgres tests, or "" when
// no admin DSN is configured (caller MUST t.Skip in that case).
func testAdminDSN() string {
	for _, k := range []string{"TEST_POSTGRES_ADMIN_DSN", "CUSTOMER_POSTGRES_DSN", "TEST_POSTGRES_CUSTOMERS_URL"} {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// uniqueToken returns a short, unique-per-test-run token. Postgres role/db
// names must fit 63 bytes and accept underscores but not dashes well inside
// %q-quoted identifiers; the prefix "tok" + the nanosecond clock keeps it
// short and clearly test-scoped.
func uniqueToken(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("tok%d_%s", time.Now().UnixNano(), strings.ReplaceAll(t.Name(), "/", "_"))
}

func cleanupPGObjects(t *testing.T, adminDSN string, dbs, roles []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Logf("cleanup: connect: %v", err)
		return
	}
	defer conn.Close(ctx) //nolint:errcheck
	for _, db := range dbs {
		_, _ = conn.Exec(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, db))
	}
	for _, role := range roles {
		_, _ = conn.Exec(ctx, fmt.Sprintf(`DROP USER IF EXISTS %q`, role))
	}
}

func TestLocalBackend_Provision_StorageBytes_Deprovision_Regrade_LiveCluster(t *testing.T) {
	adminDSN := testAdminDSN()
	if adminDSN == "" {
		t.Skip("TEST_POSTGRES_ADMIN_DSN/CUSTOMER_POSTGRES_DSN unset — skipping live-Postgres LocalBackend test")
	}
	b := newLocalBackend(adminDSN)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	token := uniqueToken(t)
	dbName := dbNamePrefix + token
	username := userNamePrefix + token
	t.Cleanup(func() { cleanupPGObjects(t, adminDSN, []string{dbName}, []string{username}) })

	creds, err := b.Provision(ctx, token, "pro", 8)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if creds == nil || creds.DatabaseName != dbName || creds.Username != username {
		t.Fatalf("Provision returned %+v; want db=%q user=%q", creds, dbName, username)
	}
	if !strings.HasPrefix(creds.URL, "postgres://") {
		t.Errorf("Credentials.URL = %q; want postgres:// prefix", creds.URL)
	}
	if creds.ProviderResourceID != "local:0" {
		t.Errorf("ProviderResourceID = %q; want local:0", creds.ProviderResourceID)
	}

	// StorageBytes should return a non-error int >= 0 for a freshly-created DB.
	size, err := b.StorageBytes(ctx, token, creds.ProviderResourceID)
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	if size <= 0 {
		t.Errorf("StorageBytes = %d; want > 0 for an existing DB", size)
	}

	// Regrade should ALTER ROLE successfully — both a positive cap and the
	// -1/0 unlimited normalization path.
	res, err := b.Regrade(ctx, token, creds.ProviderResourceID, 16)
	if err != nil {
		t.Fatalf("Regrade(16): %v", err)
	}
	if !res.Applied || res.AppliedConnLimit != 16 {
		t.Errorf("Regrade(16) = %+v; want Applied=true cap=16", res)
	}
	res, err = b.Regrade(ctx, token, creds.ProviderResourceID, 0)
	if err != nil {
		t.Fatalf("Regrade(0): %v", err)
	}
	if !res.Applied || res.AppliedConnLimit != -1 {
		t.Errorf("Regrade(0) = %+v; want Applied=true cap=-1 (zero coerced)", res)
	}
	res, err = b.Regrade(ctx, token, creds.ProviderResourceID, -1)
	if err != nil {
		t.Fatalf("Regrade(-1): %v", err)
	}
	if !res.Applied || res.AppliedConnLimit != -1 {
		t.Errorf("Regrade(-1) = %+v; want Applied=true cap=-1", res)
	}

	// Deprovision drops the database and user idempotently.
	if err := b.Deprovision(ctx, token, creds.ProviderResourceID); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	// Idempotency: second deprovision should still succeed (DROP IF EXISTS).
	if err := b.Deprovision(ctx, token, creds.ProviderResourceID); err != nil {
		t.Errorf("second Deprovision (idempotent) returned %v; want nil", err)
	}

	// StorageBytes after deprovision must error (database gone).
	if _, err := b.StorageBytes(ctx, token, creds.ProviderResourceID); err == nil {
		t.Errorf("StorageBytes on dropped DB returned nil error; want pg_database_size error")
	}
}

// TestLocalBackend_Provision_UnlimitedConnLimit covers the connLimit<=0 branch
// of Provision (no CONNECTION LIMIT clause appended).
func TestLocalBackend_Provision_UnlimitedConnLimit(t *testing.T) {
	adminDSN := testAdminDSN()
	if adminDSN == "" {
		t.Skip("admin DSN unset")
	}
	b := newLocalBackend(adminDSN)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	token := uniqueToken(t)
	dbName := dbNamePrefix + token
	username := userNamePrefix + token
	t.Cleanup(func() { cleanupPGObjects(t, adminDSN, []string{dbName}, []string{username}) })

	if _, err := b.Provision(ctx, token, "hobby", -1); err != nil {
		t.Fatalf("Provision(connLimit=-1): %v", err)
	}
	if err := b.Deprovision(ctx, token, "local:0"); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
}

// TestLocalBackend_Provision_DuplicateFails — a second Provision under the
// same token must fail at CREATE DATABASE (already exists).
func TestLocalBackend_Provision_DuplicateFails(t *testing.T) {
	adminDSN := testAdminDSN()
	if adminDSN == "" {
		t.Skip("admin DSN unset")
	}
	b := newLocalBackend(adminDSN)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	token := uniqueToken(t)
	dbName := dbNamePrefix + token
	username := userNamePrefix + token
	t.Cleanup(func() { cleanupPGObjects(t, adminDSN, []string{dbName}, []string{username}) })

	if _, err := b.Provision(ctx, token, "hobby", 4); err != nil {
		t.Fatalf("first Provision: %v", err)
	}
	_, err := b.Provision(ctx, token, "hobby", 4)
	if err == nil {
		t.Fatalf("second Provision returned nil; want CREATE DATABASE error")
	}
	if !strings.Contains(err.Error(), "CREATE DATABASE") {
		t.Errorf("second Provision error = %v; want CREATE DATABASE error", err)
	}
}

// TestLocalBackend_StorageBytes_ConnectError exercises the connection-failure
// branch of StorageBytes — the function must return a wrapped error, not panic.
func TestLocalBackend_StorageBytes_ConnectError(t *testing.T) {
	// Point at a port no Postgres listens on.
	b := newLocalBackend("postgres://u:p@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := b.StorageBytes(ctx, "any", "local:0")
	if err == nil {
		t.Fatal("StorageBytes on dead admin URL returned nil; want connect error")
	}
	if !strings.Contains(err.Error(), "connect") {
		t.Errorf("err = %v; want 'connect' wrapping", err)
	}
}

// TestLocalBackend_Deprovision_ConnectError exercises Deprovision's connect
// failure branch.
func TestLocalBackend_Deprovision_ConnectError(t *testing.T) {
	b := newLocalBackend("postgres://u:p@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := b.Deprovision(ctx, "tok", "local:0")
	if err == nil {
		t.Fatal("Deprovision on dead admin URL returned nil; want connect error")
	}
}

// TestLocalBackend_Regrade_ConnectError exercises Regrade's connect-failure
// branch (RegradeResult{Applied:false} + non-nil err wrapping "connect").
func TestLocalBackend_Regrade_ConnectError(t *testing.T) {
	b := newLocalBackend("postgres://u:p@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := b.Regrade(ctx, "tok", "local:0", 8)
	if err == nil {
		t.Fatal("Regrade on dead admin URL returned nil error; want connect error")
	}
	if res.Applied {
		t.Errorf("Regrade.Applied = true after connect failure; want false")
	}
}

// TestLocalBackend_Provision_ConnectError covers Provision's pgx.Connect
// failure branch.
func TestLocalBackend_Provision_ConnectError(t *testing.T) {
	b := newLocalBackend("postgres://u:p@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := b.Provision(ctx, "tok", "pro", 8)
	if err == nil {
		t.Fatal("Provision on dead admin URL returned nil; want connect error")
	}
}

// TestLocalBackend_StartShutdown covers the optional Starter interface and
// Shutdown — Start spawns a polling goroutine on the in-cluster router; Shutdown
// stops it. Double-Shutdown must be safe (the close-once guard).
func TestLocalBackend_StartShutdown(t *testing.T) {
	b := newLocalBackend("postgres://u:p@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1")
	ctx, cancel := context.WithCancel(context.Background())
	b.Start(ctx)
	b.Shutdown()
	b.Shutdown() // second call must not panic
	cancel()
}

// TestLocalBackend_Provision_PoolToken_NamesFromPool — confirms the poolident
// path: when provider_resource_id carries a pooltok marker the StorageBytes /
// Deprovision / Regrade name resolution uses the pool token.
func TestLocalBackend_Provision_PoolToken_NamesFromPool(t *testing.T) {
	adminDSN := testAdminDSN()
	if adminDSN == "" {
		t.Skip("admin DSN unset")
	}
	b := newLocalBackend(adminDSN)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Provision a "pool" resource under a pool token, then Deprovision using
	// the *real* request token with a poolident-encoded PRID.
	poolTok := "pool_" + uniqueToken(t)
	realTok := "real_" + uniqueToken(t)
	dbName := dbNamePrefix + poolTok
	username := userNamePrefix + poolTok
	t.Cleanup(func() { cleanupPGObjects(t, adminDSN, []string{dbName}, []string{username}) })

	if _, err := b.Provision(ctx, poolTok, "hobby", 4); err != nil {
		t.Fatalf("Provision(poolToken): %v", err)
	}

	prid := poolident.Encode("local:0", poolTok)

	// StorageBytes via the request token + pool PRID must hit the pool DB.
	if _, err := b.StorageBytes(ctx, realTok, prid); err != nil {
		t.Fatalf("StorageBytes(realTok, pool PRID): %v — name resolution didn't follow the pool marker", err)
	}
	// Regrade via the same path must succeed (the pool role exists).
	res, err := b.Regrade(ctx, realTok, prid, 6)
	if err != nil {
		t.Fatalf("Regrade(realTok, pool PRID): %v", err)
	}
	if !res.Applied {
		t.Errorf("Regrade(pool PRID).Applied = false; want true")
	}
	// Deprovision via the same path drops the pool-owned DB/user.
	if err := b.Deprovision(ctx, realTok, prid); err != nil {
		t.Fatalf("Deprovision(realTok, pool PRID): %v", err)
	}
}

// TestIsDatabaseInUseErr_AdditionalCases extends the existing TestIsDatabaseInUseErr
// to cover the strings.ToLower path with mixed-case markers and the
// errors-wrapped retry classification.
func TestIsDatabaseInUseErr_WrappedError(t *testing.T) {
	wrapped := fmt.Errorf("layer 1: %w", errors.New("DATABASE \"X\" IS BEING ACCESSED BY OTHER USERS"))
	if !isDatabaseInUseErr(wrapped) {
		t.Errorf("isDatabaseInUseErr(wrapped uppercase) = false; want true")
	}
}
