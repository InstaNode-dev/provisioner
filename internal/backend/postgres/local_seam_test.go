package postgres

// local_seam_test.go — seam-driven coverage for local.go: generatePassword,
// Provision (success + every Exec-error branch), StorageBytes, Deprovision
// (success, retry loop, terminal error, ctx cancel), Regrade, and the small
// URL-building helpers.

import (
	"context"
	"errors"
	"io"
	"math/big"
	"testing"
	"time"
)

func TestGeneratePassword_Success(t *testing.T) {
	got, err := generatePassword(16)
	if err != nil {
		t.Fatalf("generatePassword: %v", err)
	}
	if len(got) != 16 {
		t.Errorf("len = %d; want 16", len(got))
	}
}

func TestGeneratePassword_RandError(t *testing.T) {
	orig := randInt
	randInt = func(_ io.Reader, _ *big.Int) (*big.Int, error) { return nil, errSeam }
	t.Cleanup(func() { randInt = orig })

	if _, err := generatePassword(8); err == nil {
		t.Error("expected error when randInt fails")
	}
}

func TestLocalBackend_Provision_Success(t *testing.T) {
	fc := &fakePGConn{}
	withPGXConnect(t, fc, nil)

	b := newLocalBackend("postgres://u:p@h:5432/postgres?sslmode=disable")
	creds, err := b.Provision(context.Background(), "tok-123", "hobby", 8)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if creds.DatabaseName != "db_tok-123" || creds.Username != "usr_tok-123" {
		t.Errorf("creds = %+v", creds)
	}
	if creds.ProviderResourceID != "local:0" {
		t.Errorf("PRID = %q; want local:0", creds.ProviderResourceID)
	}
}

func TestLocalBackend_Provision_GenPasswordError(t *testing.T) {
	orig := randInt
	randInt = func(_ io.Reader, _ *big.Int) (*big.Int, error) { return nil, errSeam }
	t.Cleanup(func() { randInt = orig })
	b := newLocalBackend("")
	if _, err := b.Provision(context.Background(), "t", "hobby", -1); err == nil {
		t.Error("expected generatePassword error")
	}
}

func TestLocalBackend_Provision_PickError(t *testing.T) {
	// A router with no clusters → Pick returns an error.
	b := &LocalBackend{router: newClusterRouter(nil, 0)}
	if _, err := b.Provision(context.Background(), "t", "hobby", -1); err == nil {
		t.Error("expected Pick error when no clusters configured")
	}
}

func TestLocalBackend_Provision_ConnectError_Seam(t *testing.T) {
	withPGXConnect(t, nil, errSeam)
	b := newLocalBackend("")
	if _, err := b.Provision(context.Background(), "t", "hobby", -1); err == nil {
		t.Error("expected connect error")
	}
}

func TestLocalBackend_Provision_ExecErrorBranches(t *testing.T) {
	cases := []struct {
		name string
		sub  string
	}{
		{"create_database", "CREATE DATABASE"},
		{"create_user", "CREATE USER"},
		{"grant_database", "GRANT ALL PRIVILEGES ON DATABASE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fc := &fakePGConn{execErrFor: map[string]error{tc.sub: errSeam}}
			withPGXConnect(t, fc, nil)
			b := newLocalBackend("")
			if _, err := b.Provision(context.Background(), "t", "hobby", 5); err == nil {
				t.Errorf("expected error when %q fails", tc.sub)
			}
		})
	}
}

// Non-fatal branches: REVOKE CONNECT and (post-new-db) GRANT SCHEMA / CREATE
// EXTENSION failures are logged but Provision still succeeds.
func TestLocalBackend_Provision_NonFatalBranches(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{
		"REVOKE CONNECT":      errSeam,
		"GRANT ALL ON SCHEMA": errSeam,
		"CREATE EXTENSION":    errSeam,
	}}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	if _, err := b.Provision(context.Background(), "t", "hobby", -1); err != nil {
		t.Errorf("non-fatal failures should not fail Provision: %v", err)
	}
}

// When the second connection (to the new DB for schema grant) fails, Provision
// logs and continues — the schema grant is best-effort.
func TestLocalBackend_Provision_NewDBConnectError_NonFatal(t *testing.T) {
	var calls int
	fc := &fakePGConn{}
	withPGXConnectFunc(t, func(ctx context.Context, dsn string) (pgConn, error) {
		calls++
		if calls == 2 { // second connect = new-db schema grant
			return nil, errSeam
		}
		return fc, nil
	})
	b := newLocalBackend("")
	if _, err := b.Provision(context.Background(), "t", "hobby", -1); err != nil {
		t.Errorf("new-db connect failure must be non-fatal: %v", err)
	}
}

func TestLocalBackend_Provision_CloseError_NonFatal(t *testing.T) {
	fc := &fakePGConn{closeErr: errSeam}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	if _, err := b.Provision(context.Background(), "t", "hobby", -1); err != nil {
		t.Errorf("Close error must be non-fatal: %v", err)
	}
	if fc.closeCalls == 0 {
		t.Error("Close was never called")
	}
}

func TestLocalBackend_StorageBytes_Success(t *testing.T) {
	fc := &fakePGConn{scanInt64: 4242}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	got, err := b.StorageBytes(context.Background(), "tok", "local:0")
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	if got != 4242 {
		t.Errorf("got %d; want 4242", got)
	}
}

func TestLocalBackend_StorageBytes_ConnectError_Seam(t *testing.T) {
	withPGXConnect(t, nil, errSeam)
	b := newLocalBackend("")
	if _, err := b.StorageBytes(context.Background(), "tok", ""); err == nil {
		t.Error("expected connect error")
	}
}

func TestLocalBackend_StorageBytes_ScanError(t *testing.T) {
	fc := &fakePGConn{queryRowErr: errSeam}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	if _, err := b.StorageBytes(context.Background(), "tok", ""); err == nil {
		t.Error("expected scan error")
	}
}

func TestLocalBackend_Deprovision_Success(t *testing.T) {
	fc := &fakePGConn{}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	if err := b.Deprovision(context.Background(), "tok", "local:0"); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
}

func TestLocalBackend_Deprovision_ConnectError_Seam(t *testing.T) {
	withPGXConnect(t, nil, errSeam)
	b := newLocalBackend("")
	if err := b.Deprovision(context.Background(), "tok", ""); err == nil {
		t.Error("expected connect error")
	}
}

// Non-fatal: REVOKE CONNECT, terminate, DROP USER all log-and-continue.
func TestLocalBackend_Deprovision_NonFatalBranches(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{
		"REVOKE CONNECT":       errSeam,
		"pg_terminate_backend": errSeam,
		"DROP USER":            errSeam,
	}}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	if err := b.Deprovision(context.Background(), "tok", ""); err != nil {
		t.Errorf("non-fatal failures should not fail Deprovision: %v", err)
	}
}

// Terminal DROP DATABASE error (not the in-use race) breaks the loop on attempt 1.
func TestLocalBackend_Deprovision_DropDBTerminalError(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{"DROP DATABASE": errors.New("permission denied")}}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	if err := b.Deprovision(context.Background(), "tok", ""); err == nil {
		t.Error("expected terminal DROP DATABASE error")
	}
}

// The in-use race retries deprovisionDropDBAttempts times, then returns the error.
func TestLocalBackend_Deprovision_DropDBInUseRetries(t *testing.T) {
	orig := deprovisionDropDBRetryDelay
	deprovisionDropDBRetryDelay = time.Millisecond
	t.Cleanup(func() { deprovisionDropDBRetryDelay = orig })

	fc := &fakePGConn{execErrFor: map[string]error{
		"DROP DATABASE": errors.New("database is being accessed by other users"),
	}}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	if err := b.Deprovision(context.Background(), "tok", ""); err == nil {
		t.Error("expected in-use error after exhausting retries")
	}
}

// ctx cancellation inside the retry loop breaks out via ctx.Done().
func TestLocalBackend_Deprovision_CtxCancelDuringRetry(t *testing.T) {
	orig := deprovisionDropDBRetryDelay
	deprovisionDropDBRetryDelay = time.Second
	t.Cleanup(func() { deprovisionDropDBRetryDelay = orig })

	fc := &fakePGConn{execErrFor: map[string]error{
		"DROP DATABASE": errors.New("database is being accessed by other users"),
	}}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled → loop's select hits ctx.Done immediately
	if err := b.Deprovision(ctx, "tok", ""); err == nil {
		t.Error("expected error after ctx cancel")
	}
}

func TestLocalBackend_Regrade_Success(t *testing.T) {
	fc := &fakePGConn{}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	res, err := b.Regrade(context.Background(), "tok", "local:0", 20)
	if err != nil {
		t.Fatalf("Regrade: %v", err)
	}
	if !res.Applied || res.AppliedConnLimit != 20 {
		t.Errorf("res = %+v; want Applied with 20", res)
	}
}

func TestLocalBackend_Regrade_ZeroNormalizedToUnlimited(t *testing.T) {
	fc := &fakePGConn{}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	res, err := b.Regrade(context.Background(), "tok", "", 0)
	if err != nil {
		t.Fatalf("Regrade: %v", err)
	}
	if res.AppliedConnLimit != -1 {
		t.Errorf("0 should normalize to -1; got %d", res.AppliedConnLimit)
	}
}

func TestLocalBackend_Regrade_ConnectError_Seam(t *testing.T) {
	withPGXConnect(t, nil, errSeam)
	b := newLocalBackend("")
	if _, err := b.Regrade(context.Background(), "tok", "", 5); err == nil {
		t.Error("expected connect error")
	}
}

func TestLocalBackend_Regrade_AlterRoleError(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{"ALTER ROLE": errSeam}}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	if _, err := b.Regrade(context.Background(), "tok", "", 5); err == nil {
		t.Error("expected ALTER ROLE error")
	}
}

// Close-error branches in the deferred disconnect of StorageBytes and Regrade.
func TestLocalBackend_StorageBytes_CloseErrorLogged(t *testing.T) {
	fc := &fakePGConn{scanInt64: 1, closeErr: errSeam}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	if _, err := b.StorageBytes(context.Background(), "tok", ""); err != nil {
		t.Errorf("Close error must be non-fatal: %v", err)
	}
}

func TestLocalBackend_Regrade_CloseErrorLogged(t *testing.T) {
	fc := &fakePGConn{closeErr: errSeam}
	withPGXConnect(t, fc, nil)
	b := newLocalBackend("")
	if _, err := b.Regrade(context.Background(), "tok", "", 5); err != nil {
		t.Errorf("Close error must be non-fatal: %v", err)
	}
}

func TestLocalBackend_StartShutdown_Seam(t *testing.T) {
	// Install a fast, deterministic seam so the immediate first refreshCounts
	// poll returns at once rather than dialing a real (absent) Postgres for the
	// full 5s connect timeout. Shutdown joins the poll goroutine, so this also
	// guarantees no router goroutine survives the test to race the global seam.
	withPGXConnect(t, &fakePGConn{}, nil)
	b := newLocalBackend("")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	b.Start(ctx)
	b.Shutdown()
}

func TestBuildDBURL_PublicHostPort(t *testing.T) {
	t.Setenv("POSTGRES_PUBLIC_HOST_PORT", "pg.example.com:6543")
	got := buildDBURL("postgres://a:b@internal:5432/postgres", "u", "p", "db_x")
	if got != "postgres://u:p@pg.example.com:6543/db_x?sslmode=disable" {
		t.Errorf("got %q", got)
	}
}

func TestBuildDBURL_PublicHostDefaultPort(t *testing.T) {
	t.Setenv("POSTGRES_PUBLIC_HOST_PORT", "")
	t.Setenv("POSTGRES_PUBLIC_HOST", "pg.example.com")
	t.Setenv("POSTGRES_PUBLIC_PORT", "")
	got := buildDBURL("postgres://a:b@internal:5432/postgres", "u", "p", "db_x")
	if got != "postgres://u:p@pg.example.com:5432/db_x?sslmode=disable" {
		t.Errorf("got %q", got)
	}
}

func TestBuildDBURL_PublicHostExplicitPort(t *testing.T) {
	t.Setenv("POSTGRES_PUBLIC_HOST_PORT", "")
	t.Setenv("POSTGRES_PUBLIC_HOST", "pg.example.com")
	t.Setenv("POSTGRES_PUBLIC_PORT", "7777")
	got := buildDBURL("postgres://a:b@internal:5432/postgres", "u", "p", "db_x")
	if got != "postgres://u:p@pg.example.com:7777/db_x?sslmode=disable" {
		t.Errorf("got %q", got)
	}
}

func TestBuildDBURL_FallbackToAdminHost(t *testing.T) {
	t.Setenv("POSTGRES_PUBLIC_HOST_PORT", "")
	t.Setenv("POSTGRES_PUBLIC_HOST", "")
	got := buildDBURL("postgres://a:b@internal-host:5432/postgres", "u", "p", "db_x")
	if got != "postgres://u:p@internal-host:5432/db_x?sslmode=disable" {
		t.Errorf("got %q", got)
	}
}

func TestBuildAdminNewDBURL_Seam(t *testing.T) {
	if got := buildAdminNewDBURL("postgres://a:b@h:5432/postgres?x=1", "db_y"); got != "postgres://a:b@h:5432/db_y" {
		t.Errorf("got %q", got)
	}
	// No slash → appends.
	if got := buildAdminNewDBURL("noslash", "db_y"); got != "noslash/db_y" {
		t.Errorf("got %q", got)
	}
}

func TestExtractHost_Seam(t *testing.T) {
	cases := map[string]string{
		"postgres://u:p@h:5432/db": "h:5432",
		"postgres://u:p@h/db":      "h",
		"postgres://h:5432":        "h:5432",
		"h-no-prefix":              "h-no-prefix",
	}
	for in, want := range cases {
		if got := extractHost(in); got != want {
			t.Errorf("extractHost(%q) = %q; want %q", in, got, want)
		}
	}
}

func TestIndexOf_Seam(t *testing.T) {
	if indexOf("abc@def", '@') != 3 {
		t.Error("indexOf @ wrong")
	}
	if indexOf("abc", '@') != -1 {
		t.Error("indexOf missing should be -1")
	}
}

func TestPublicHostPort_Empty(t *testing.T) {
	t.Setenv("POSTGRES_PUBLIC_HOST_PORT", "")
	t.Setenv("POSTGRES_PUBLIC_HOST", "")
	if got := publicHostPort(); got != "" {
		t.Errorf("got %q; want empty", got)
	}
}
