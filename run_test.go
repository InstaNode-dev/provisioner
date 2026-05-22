package main

// run_test.go — exercises the run() boot seam extracted from main(). run wires
// every dependency main used to wire inline and blocks until its ctx is
// cancelled or the gRPC server errors. Driving run() directly covers the boot
// path, the ordered teardown, and the os.Exit-class error arms (now returned
// errors) without spawning a process or sending real signals.
//
// The happy-path test binds the gRPC listener on an ephemeral port (Port "0")
// with the hot pool disabled (no PROVISIONER_DATABASE_URL) so no real DB is
// needed; it cancels the ctx to trigger graceful shutdown and asserts run
// returns nil. The error tests assert the fail-closed / fail-fast arms.

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"instant.dev/provisioner/internal/config"
)

// minimalRunConfig returns a config that boots run() with the hot pool disabled
// and local (lazy, non-dialing) backends. Port "0" → ephemeral gRPC port.
func minimalRunConfig() *config.Config {
	return &config.Config{
		Port:                     "0",
		ProvisionerSecret:        "test-secret-not-empty",
		PostgresProvisionBackend: "local",
		RedisProvisionBackend:    "local",
		MongoProvisionBackend:    "local",
		QueueProvisionBackend:    "local",
		RedisProvisionHost:       "127.0.0.1:6379",
		MongoAdminURI:            "mongodb://root:root@127.0.0.1:27017",
		MongoHost:                "127.0.0.1:27017",
		NATSHost:                 "127.0.0.1",
		// ProvisionerDatabaseURL + AESKey deliberately empty → pool disabled.
	}
}

// TestRealMain_CleanShutdownReturnsZero — realMain loads config, boots, and
// returns exit code 0 when the ctx cancels cleanly. Driven with a quickly
// cancelled ctx + env-configured minimal pool-disabled boot so config.Load
// yields a runnable config. This is the testable core main() delegates to.
func TestRealMain_CleanShutdownReturnsZero(t *testing.T) {
	t.Setenv("PROVISIONER_SECRET", "test-secret-not-empty")
	t.Setenv("PROVISIONER_PORT", "0")
	// Ensure the pool stays disabled regardless of ambient env.
	t.Setenv("PROVISIONER_DATABASE_URL", "")
	t.Setenv("AES_KEY", "")

	ctx, cancel := context.WithCancel(context.Background())
	codeCh := make(chan int, 1)
	go func() { codeCh <- realMain(ctx) }()

	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case code := <-codeCh:
		if code != 0 {
			t.Fatalf("realMain clean shutdown exit code = %d, want 0", code)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("realMain did not return within 15s of ctx cancel")
	}
}

// TestRealMain_BootErrorReturnsOne — a fatal boot error (empty secret →
// fail-closed) must make realMain return exit code 1.
func TestRealMain_BootErrorReturnsOne(t *testing.T) {
	t.Setenv("PROVISIONER_SECRET", "")
	t.Setenv("PROVISIONER_DATABASE_URL", "")
	t.Setenv("AES_KEY", "")
	if code := realMain(context.Background()); code != 1 {
		t.Fatalf("realMain with empty secret exit code = %d, want 1", code)
	}
}

// TestSignalContext_CancelsOnStop — signalContext returns a live context plus a
// stop func; calling stop cancels the context. This is the signal-wiring main
// uses, exercised without delivering a real OS signal.
func TestSignalContext_CancelsOnStop(t *testing.T) {
	ctx, stop := signalContext()
	select {
	case <-ctx.Done():
		t.Fatal("signalContext returned an already-cancelled context")
	default:
	}
	stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("stop() did not cancel the signal context")
	}
}

// TestRun_ListenFailure_WithPool — pool enabled (so poolMgr is non-nil) plus an
// already-bound gRPC port: run must surface the listen error AND tear the pool
// Manager down on the way out (the listen-failure cleanup arm). Gated on
// TEST_PROVISIONER_DATABASE_URL.
func TestRun_ListenFailure_WithPool(t *testing.T) {
	dsn := os.Getenv("TEST_PROVISIONER_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_PROVISIONER_DATABASE_URL not set — skipping pool listen-failure test")
	}

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}

	cfg := minimalRunConfig()
	cfg.Port = port
	cfg.AESKey = validAESKeyHex
	cfg.ProvisionerDatabaseURL = dsn
	cfg.PoolPostgresSize = 0 // no refill churn; pool Manager still starts

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := run(ctx, cfg); err == nil {
		t.Fatal("run should return a listen error even with the pool enabled")
	}
}

// TestBootstrap_InstallsLoggerAndRuns — bootstrap installs the global slog
// logger then runs the service. Driven with a cancellable ctx + minimal config
// so the logger-install side effect and the run happy path are both covered;
// this is the testable core that main() delegates to.
func TestBootstrap_InstallsLoggerAndRuns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- bootstrap(ctx, minimalRunConfig()) }()

	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("bootstrap returned error on clean shutdown: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("bootstrap did not return within 15s of ctx cancel")
	}
}

// TestRun_BootsAndShutsDownCleanly — the full happy path: run boots the sidecar
// + gRPC server, becomes ready, then returns nil when the ctx is cancelled.
// A valid-length NEW_RELIC_LICENSE_KEY is set so initNewRelic returns a non-nil
// app and the deferred nrApp.Shutdown arm runs on teardown.
func TestRun_BootsAndShutsDownCleanly(t *testing.T) {
	t.Setenv("NEW_RELIC_LICENSE_KEY", "0123456789012345678901234567890123456789")
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx, minimalRunConfig()) }()

	// Give run a beat to bind the listener + flip readiness, then ask it to
	// shut down. The GracefulStop + 5s healthz drain are bounded, so the whole
	// thing must complete well inside the test deadline.
	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run returned error on clean shutdown: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not return within 15s of ctx cancel")
	}
}

// TestRun_PoolEnabled_BootsAndShutsDown — the hot-pool-enabled boot path. With
// a reachable PROVISIONER_DATABASE_URL + a valid AES key, run must build the
// bounded pgxpool, ping it successfully, surface it on /readyz via the box,
// start the pool Manager, serve gRPC, then tear all of it down (including
// poolMgr.Shutdown) when the ctx cancels. This is the single biggest uncovered
// block of run(); gated on TEST_PROVISIONER_DATABASE_URL.
func TestRun_PoolEnabled_BootsAndShutsDown(t *testing.T) {
	dsn := os.Getenv("TEST_PROVISIONER_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_PROVISIONER_DATABASE_URL not set — skipping pool-enabled run test")
	}

	cfg := minimalRunConfig()
	cfg.ProvisionerDatabaseURL = dsn
	// 64 hex chars = 32 bytes = a valid AES-256 key.
	cfg.AESKey = "0000000000000000000000000000000000000000000000000000000000000000"
	// Keep the pool tiny so refill goroutines are cheap; the backends are the
	// lazy local ones, so a refused Provision just logs — boot still succeeds.
	cfg.PoolPostgresSize = 1

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx, cfg) }()

	time.Sleep(500 * time.Millisecond) // let pool connect + Manager.Start run
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("pool-enabled run returned error on clean shutdown: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("pool-enabled run did not return within 20s of ctx cancel")
	}
}

const validAESKeyHex = "0000000000000000000000000000000000000000000000000000000000000000"

// TestRun_PoolDSNParseError — pool enabled with a valid AES key but a malformed
// PROVISIONER_DATABASE_URL must surface the pgxpool config-parse error before
// binding gRPC.
func TestRun_PoolDSNParseError(t *testing.T) {
	cfg := minimalRunConfig()
	cfg.AESKey = validAESKeyHex
	cfg.ProvisionerDatabaseURL = "::::not-a-dsn::::"
	if err := run(context.Background(), cfg); err == nil {
		t.Fatal("run should fail on a malformed pool DSN")
	}
}

// TestRun_PoolPingFailsButBootsAnyway — pool enabled, valid AES key, parseable
// DSN pointing at an unreachable host: the ping-retry loop exhausts and logs
// "pool disabled", but run must still boot the gRPC server (pool failure is
// non-fatal) and shut down cleanly on ctx cancel. Exercises the ping-failure
// arm (the `if pingErr != nil` branch) without a real DB.
func TestRun_PoolPingFailsButBootsAnyway(t *testing.T) {
	if testing.Short() {
		t.Skip("ping-retry backoff takes ~7.5s — skipped under -short")
	}
	cfg := minimalRunConfig()
	cfg.AESKey = validAESKeyHex
	// Parseable DSN, but 127.0.0.1:1 has nothing listening → every ping fails.
	cfg.ProvisionerDatabaseURL = "postgres://nobody@127.0.0.1:1/x?sslmode=disable&connect_timeout=1"

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx, cfg) }()

	// The ping loop runs ~7.5s of backoff before "pool disabled"; wait past it
	// so gRPC has bound, then cancel.
	time.Sleep(9 * time.Second)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run should boot despite an unreachable pool DB; got %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not return within 20s of ctx cancel")
	}
}

// TestRun_PoolStartError — pool enabled, DB reachable (ping OK) but the pool
// migrate (CREATE TABLE pool_items) fails because the connecting role lacks
// CREATE on the schema → run must surface the pool_start_failed error. We mint
// a privilege-stripped role on the throwaway DB so migrate fails for a real,
// in-band reason. Gated on TEST_PROVISIONER_DATABASE_URL.
func TestRun_PoolStartError(t *testing.T) {
	dsn := os.Getenv("TEST_PROVISIONER_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_PROVISIONER_DATABASE_URL not set — skipping pool-start-error test")
	}

	admin, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	defer admin.Close()

	role := fmt.Sprintf("noddl_%d", time.Now().UnixNano())
	ctx := context.Background()
	// Create a login role with NO schema-create privilege.
	if _, err := admin.Exec(ctx, fmt.Sprintf(
		`CREATE ROLE %s LOGIN PASSWORD 'pw'`, role)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), fmt.Sprintf(`DROP ROLE IF EXISTS %s`, role))
	})
	// Revoke CREATE on the public schema so CREATE TABLE in migrate fails.
	if _, err := admin.Exec(ctx, `REVOKE CREATE ON SCHEMA public FROM PUBLIC`); err != nil {
		t.Fatalf("revoke create: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `GRANT CREATE ON SCHEMA public TO PUBLIC`)
	})

	// Build a DSN for the stripped role pointed at the same DB.
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	u.User = url.UserPassword(role, "pw")
	roleDSN := u.String()

	cfg := minimalRunConfig()
	cfg.AESKey = validAESKeyHex
	cfg.ProvisionerDatabaseURL = roleDSN

	err = run(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "pool start") {
		t.Fatalf("run should fail with a pool-start error; got %v", err)
	}
}

// TestRun_ServeError — if grpcServer.Serve returns an error before the ctx is
// cancelled, run must take the serveErr select arm (log serve_failed) and still
// tear down cleanly, returning nil. We inject a listener via the netListen seam
// and close it once run is serving, which makes Serve return an error.
func TestRun_ServeError(t *testing.T) {
	origListen := netListen
	t.Cleanup(func() { netListen = origListen })

	lisCh := make(chan net.Listener, 1)
	netListen = func(network, addr string) (net.Listener, error) {
		l, err := origListen(network, addr)
		if err == nil {
			lisCh <- l
		}
		return l, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx, minimalRunConfig()) }()

	// Once run has created+served on the listener, close it to force Serve to
	// return an error → run hits the serveErr arm.
	select {
	case l := <-lisCh:
		time.Sleep(150 * time.Millisecond) // let Serve get going
		_ = l.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("run never created a listener via the netListen seam")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run should return nil after a serve error + teardown; got %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not return after the gRPC serve error")
	}
}

// TestRun_EmptySecret_FailsClosed — an empty PROVISIONER_SECRET must make run
// return an auth-misconfigured error (the fail-closed contract), not boot.
func TestRun_EmptySecret_FailsClosed(t *testing.T) {
	cfg := minimalRunConfig()
	cfg.ProvisionerSecret = ""
	err := run(context.Background(), cfg)
	if err == nil {
		t.Fatal("run with empty secret should fail closed")
	}
}

// TestRun_BadAESKey_FailsFast — pool enabled (DB url + AES key set) but the AES
// key is unparseable → run must return a key-parse error before binding gRPC.
func TestRun_BadAESKey_FailsFast(t *testing.T) {
	cfg := minimalRunConfig()
	cfg.ProvisionerDatabaseURL = "postgres://nobody@127.0.0.1:1/x?sslmode=disable"
	cfg.AESKey = "not-a-valid-hex-aes-key"
	err := run(context.Background(), cfg)
	if err == nil {
		t.Fatal("run with a malformed AES key should fail fast")
	}
}

// TestRun_ListenFailure — an already-bound gRPC port must surface a listen
// error from run. We grab an ephemeral port on the SAME all-interfaces address
// run uses (":port" → 0.0.0.0), hold it, and hand run that exact port so
// net.Listen genuinely collides (binding 127.0.0.1:port would NOT conflict
// with 0.0.0.0:port on macOS, leaving run to serve and block forever — so the
// reservation must also be on the wildcard address). A bounded ctx is a
// belt-and-suspenders guard against a hang if the bind ever did succeed.
func TestRun_ListenFailure(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer l.Close()
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}

	cfg := minimalRunConfig()
	cfg.Port = port // already taken by l on the same wildcard addr → bind fails

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := run(ctx, cfg); err == nil {
		t.Fatal("run should return a listen error when its port is already bound")
	}
}
