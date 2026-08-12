package server_test

// server_live_roundtrip_test.go — REAL-BACKEND integration coverage for the
// gRPC server layer's Provision → Regrade → Deprovision lifecycle.
//
// Why this file exists (the truehomie-db DROP incident class, 2026-06-03):
// every existing server_test.go / server_coverage_test.go test injects a *fake*
// backend, so the actual DROP DATABASE / DROP USER / ALTER ROLE DDL has never
// run through the real gRPC handler path (breaker wrapping, tier→connLimit
// routing, mapError, response shaping, idempotent re-deprovision). High
// statement coverage from mocks does NOT prove the destroy/regrade DDL is
// correct end-to-end. These tests drive the genuine RPC handlers
// (server.ProvisionResource / RegradeResource / DeprovisionResource) against a
// real Postgres and a real Redis, and assert the backing infra is actually
// created, regraded, and torn down — and that a second Deprovision is a clean
// idempotent no-op (the #9 DROP IF EXISTS fix).
//
// Env-gated: skips cleanly when the backend URL is unset, so `go test -short`
// in CI without a backend stays green; runs for real when the backend is
// present (local dev Postgres at localhost:5432, Redis at localhost:6379, and
// CI's coverage.yml docker services).

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	goredis "github.com/redis/go-redis/v9"

	commonv1 "instant.dev/proto/common/v1"
	provisionerv1 "instant.dev/proto/provisioner/v1"
	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/redis"
	"instant.dev/provisioner/internal/circuit"
	"instant.dev/provisioner/internal/config"
	"instant.dev/provisioner/internal/server"
)

// livePostgresAdminDSN returns an admin DSN capable of CREATE/DROP DATABASE,
// or "" when none is configured (caller MUST t.Skip). Mirrors the env-var
// resolution used by the backend/postgres live tests so a single env wires
// both layers.
func livePostgresAdminDSN() string {
	for _, k := range []string{"TEST_POSTGRES_CUSTOMERS_URL", "TEST_POSTGRES_ADMIN_DSN", "CUSTOMER_POSTGRES_DSN"} {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// liveRedisURL returns a redis:// URL for the provision pool, or "" when unset.
func liveRedisURL() string {
	for _, k := range []string{"TEST_REDIS_URL", "CUSTOMER_REDIS_URL"} {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// liveServerWithRealPostgres builds a Server wired to a REAL LocalBackend
// Postgres (shared-cluster admin DSN) and fresh per-test breakers. No pool, no
// dedicated backend, so every RPC takes the live shared-cluster path.
func liveServerWithRealPostgres(adminDSN string) *server.Server {
	return server.NewWithBackends(
		&config.Config{},
		postgres.NewBackend("", adminDSN, "", "", ""), // "" → LocalBackend(adminDSN)
		nil, nil, nil, nil, // redis/mongo/queue/storage unused on this path
		nil, nil, nil, nil, // no dedicated backends
		nil, // no pool → live provision path
	).SetBreakers(circuit.NewBreakers())
}

// liveServerWithRealRedis builds a Server wired to a REAL Redis LocalBackend.
func liveServerWithRealRedis(redisAddr string) *server.Server {
	return server.NewWithBackends(
		&config.Config{},
		nil,
		// backendType "" → LocalBackend; adminURL "" → the credential-less
		// REDIS_PROVISION_HOST Addr form, which is what the test's local Redis wants.
		redis.NewBackend("", "", redisAddr),
		nil, nil, nil,
		nil, nil, nil, nil,
		nil,
	).SetBreakers(circuit.NewBreakers())
}

// liveToken returns a short, unique, test-scoped token safe as a Postgres
// db_/usr_ identifier and a Redis key prefix.
func liveToken(t *testing.T) string {
	t.Helper()
	clean := strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	if len(clean) > 24 {
		clean = clean[:24]
	}
	return fmt.Sprintf("tok%d%s", time.Now().UnixNano(), clean)
}

// pgConnLimit queries the actual rolconnlimit for usr_<token> on the live
// cluster, or returns (0, err) if the role does not exist.
func pgConnLimit(t *testing.T, adminDSN, username string) (int, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("pgConnLimit connect: %v", err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	var lim int
	err = conn.QueryRow(ctx, "SELECT rolconnlimit FROM pg_roles WHERE rolname=$1", username).Scan(&lim)
	if err == pgx.ErrNoRows {
		return 0, false
	}
	if err != nil {
		t.Fatalf("pgConnLimit query: %v", err)
	}
	return lim, true
}

// pgDatabaseExists reports whether db_<token> exists on the live cluster.
func pgDatabaseExists(t *testing.T, adminDSN, dbName string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("pgDatabaseExists connect: %v", err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	var n int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM pg_database WHERE datname=$1", dbName).Scan(&n); err != nil {
		t.Fatalf("pgDatabaseExists query: %v", err)
	}
	return n > 0
}

// cleanupPG drops db_<token>/usr_<token> best-effort so repeated runs and
// failed assertions never leak objects on the shared cluster.
func cleanupPG(t *testing.T, adminDSN, dbName, username string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Logf("cleanupPG connect: %v", err)
		return
	}
	defer conn.Close(ctx) //nolint:errcheck
	_, _ = conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %q WITH (FORCE)", dbName))
	_, _ = conn.Exec(ctx, fmt.Sprintf("DROP USER IF EXISTS %q", username))
}

// TestServer_Postgres_Provision_Regrade_Deprovision_LiveRoundTrip is the
// truehomie-DROP-class integration test for the gRPC server layer: it drives
// the real RPC handlers against a real Postgres and asserts the backing
// db_/usr_ are CREATED by ProvisionResource, the role CONNECTION LIMIT is
// adjusted by RegradeResource, the db_/usr_ are DROPped by DeprovisionResource,
// and a second DeprovisionResource is a clean idempotent no-op (DROP IF EXISTS).
func TestServer_Postgres_Provision_Regrade_Deprovision_LiveRoundTrip(t *testing.T) {
	adminDSN := livePostgresAdminDSN()
	if adminDSN == "" {
		t.Skip("TEST_POSTGRES_CUSTOMERS_URL/TEST_POSTGRES_ADMIN_DSN unset — skipping live-Postgres gRPC round-trip")
	}
	srv := liveServerWithRealPostgres(adminDSN)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	token := liveToken(t)
	dbName := "db_" + token
	username := "usr_" + token
	t.Cleanup(func() { cleanupPG(t, adminDSN, dbName, username) })

	// --- Provision (hobby tier → a positive CONNECTION LIMIT) ---
	provResp, err := srv.ProvisionResource(ctx, &provisionerv1.ProvisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		Tier:         "hobby",
	})
	if err != nil {
		t.Fatalf("ProvisionResource(postgres, hobby): %v", err)
	}
	if provResp.DatabaseName != dbName || provResp.Username != username {
		t.Fatalf("ProvisionResource returned db=%q user=%q; want db=%q user=%q",
			provResp.DatabaseName, provResp.Username, dbName, username)
	}
	if !strings.HasPrefix(provResp.ConnectionUrl, "postgres://") {
		t.Errorf("ConnectionUrl = %q; want postgres:// prefix", provResp.ConnectionUrl)
	}
	if !pgDatabaseExists(t, adminDSN, dbName) {
		t.Fatalf("after ProvisionResource, %q does not exist on the live cluster", dbName)
	}
	hobbyLimit, ok := pgConnLimit(t, adminDSN, username)
	if !ok {
		t.Fatalf("after ProvisionResource, role %q does not exist", username)
	}
	if hobbyLimit <= 0 {
		t.Errorf("hobby role connection limit = %d; want a positive cap applied at CREATE USER", hobbyLimit)
	}

	// --- Regrade (pro tier → a different positive cap; assert the real ALTER ROLE took) ---
	regResp, err := srv.RegradeResource(ctx, &provisionerv1.RegradeRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		Tier:         "pro",
	})
	if err != nil {
		t.Fatalf("RegradeResource(postgres, pro): %v", err)
	}
	if !regResp.Applied {
		t.Errorf("RegradeResource(pro).Applied = false; want true")
	}
	proLimit, ok := pgConnLimit(t, adminDSN, username)
	if !ok {
		t.Fatalf("role %q vanished after Regrade", username)
	}
	if int(regResp.AppliedConnLimit) != proLimit {
		t.Errorf("pg_roles.rolconnlimit = %d but RegradeResponse.AppliedConnLimit = %d; the ALTER ROLE did not match the reported cap",
			proLimit, regResp.AppliedConnLimit)
	}
	if proLimit == hobbyLimit {
		t.Errorf("pro connection limit (%d) equals hobby (%d); the Regrade did not change the live cap", proLimit, hobbyLimit)
	}

	// --- Deprovision (the DROP DATABASE / DROP USER path — truehomie class) ---
	depResp, err := srv.DeprovisionResource(ctx, &provisionerv1.DeprovisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("DeprovisionResource(postgres): %v", err)
	}
	if !depResp.Deprovisioned {
		t.Errorf("DeprovisionResource.Deprovisioned = false; want true")
	}
	if pgDatabaseExists(t, adminDSN, dbName) {
		t.Errorf("after DeprovisionResource, %q still exists — DROP DATABASE did not run", dbName)
	}
	if _, ok := pgConnLimit(t, adminDSN, username); ok {
		t.Errorf("after DeprovisionResource, role %q still exists — DROP USER did not run", username)
	}

	// --- Idempotency: a second Deprovision must be a clean no-op (DROP IF EXISTS, #9) ---
	depResp2, err := srv.DeprovisionResource(ctx, &provisionerv1.DeprovisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Errorf("second DeprovisionResource (idempotent) returned %v; want nil — DROP IF EXISTS must no-op cleanly", err)
	}
	if depResp2 != nil && !depResp2.Deprovisioned {
		t.Errorf("second DeprovisionResource.Deprovisioned = false; want true (idempotent success)")
	}
}

// TestServer_Postgres_Reprovision_AfterDeprovision_LiveRoundTrip asserts the
// (re)Provision leg of the round-trip: after a full teardown the SAME token can
// be provisioned again with no "already exists" collision — i.e. Deprovision
// truly removed every object Provision created. This is the regression guard
// for a partial-DROP leak that would block re-provisioning.
func TestServer_Postgres_Reprovision_AfterDeprovision_LiveRoundTrip(t *testing.T) {
	adminDSN := livePostgresAdminDSN()
	if adminDSN == "" {
		t.Skip("postgres admin DSN unset — skipping reprovision round-trip")
	}
	srv := liveServerWithRealPostgres(adminDSN)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	token := liveToken(t)
	dbName := "db_" + token
	username := "usr_" + token
	t.Cleanup(func() { cleanupPG(t, adminDSN, dbName, username) })

	req := &provisionerv1.ProvisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		Tier:         "hobby",
	}
	depReq := &provisionerv1.DeprovisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	}

	if _, err := srv.ProvisionResource(ctx, req); err != nil {
		t.Fatalf("first ProvisionResource: %v", err)
	}
	if _, err := srv.DeprovisionResource(ctx, depReq); err != nil {
		t.Fatalf("DeprovisionResource: %v", err)
	}
	// Re-provision the same token: must succeed (no orphaned db_/usr_ blocking it).
	if _, err := srv.ProvisionResource(ctx, req); err != nil {
		t.Fatalf("re-ProvisionResource after Deprovision: %v — teardown leaked an object that blocks reuse", err)
	}
	if !pgDatabaseExists(t, adminDSN, dbName) {
		t.Errorf("re-provisioned %q missing", dbName)
	}
	// Final teardown.
	if _, err := srv.DeprovisionResource(ctx, depReq); err != nil {
		t.Errorf("final DeprovisionResource: %v", err)
	}
}

// TestServer_Redis_Provision_Deprovision_LiveRoundTrip drives the real Redis
// LocalBackend through the gRPC handlers: ProvisionResource creates an ACL
// user, DeprovisionResource removes the ACL user and namespace keys, and a
// second Deprovision is a clean idempotent no-op. (Redis LocalBackend has no
// Regrade — only the k8s backend implements redis.Regrader.)
func TestServer_Redis_Provision_Deprovision_LiveRoundTrip(t *testing.T) {
	redisURL := liveRedisURL()
	if redisURL == "" {
		t.Skip("TEST_REDIS_URL/CUSTOMER_REDIS_URL unset — skipping live-Redis gRPC round-trip")
	}
	opt, err := goredis.ParseURL(redisURL)
	if err != nil {
		t.Skipf("redis URL %q does not parse: %v", redisURL, err)
	}
	// Probe so we skip (not fail) when nothing is listening.
	probe := goredis.NewClient(opt)
	pctx, pcancel := context.WithTimeout(context.Background(), time.Second)
	defer pcancel()
	if perr := probe.Ping(pctx).Err(); perr != nil {
		_ = probe.Close()
		t.Skipf("redis not reachable at %s: %v", opt.Addr, perr)
	}

	srv := liveServerWithRealRedis(opt.Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	token := liveToken(t)
	username := "usr_" + token
	t.Cleanup(func() {
		// Best-effort: drop the ACL user and any namespace keys directly.
		_ = probe.Do(context.Background(), "ACL", "DELUSER", username).Err()
		if keys, _, kerr := probe.Scan(context.Background(), 0, token+":*", 100).Result(); kerr == nil && len(keys) > 0 {
			_ = probe.Del(context.Background(), keys...).Err()
		}
		_ = probe.Close()
	})

	// --- Provision: the gRPC handler must create the ACL user on the live pod ---
	provResp, err := srv.ProvisionResource(ctx, &provisionerv1.ProvisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
		Tier:         "hobby",
	})
	if err != nil {
		t.Fatalf("ProvisionResource(redis, hobby): %v", err)
	}
	if !strings.HasPrefix(provResp.ConnectionUrl, "redis://") {
		t.Errorf("ConnectionUrl = %q; want redis:// prefix", provResp.ConnectionUrl)
	}
	if provResp.KeyPrefix != token+":" {
		t.Errorf("KeyPrefix = %q; want %q", provResp.KeyPrefix, token+":")
	}
	// Assert the ACL user actually exists on the live Redis.
	if gerr := probe.Do(ctx, "ACL", "GETUSER", username).Err(); gerr != nil {
		t.Fatalf("ACL user %q not created on live Redis after ProvisionResource: %v", username, gerr)
	}
	// Write a namespace key so Deprovision has keys to reap.
	if serr := probe.Set(ctx, token+":k1", "v1", 0).Err(); serr != nil {
		t.Fatalf("seed key: %v", serr)
	}

	// --- Deprovision: removes the ACL user and the namespace keys ---
	depResp, err := srv.DeprovisionResource(ctx, &provisionerv1.DeprovisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	if err != nil {
		t.Fatalf("DeprovisionResource(redis): %v", err)
	}
	if !depResp.Deprovisioned {
		t.Errorf("DeprovisionResource.Deprovisioned = false; want true")
	}
	if gerr := probe.Do(ctx, "ACL", "GETUSER", username).Err(); gerr == nil {
		t.Errorf("ACL user %q still exists after DeprovisionResource — DELUSER did not run", username)
	}
	if n, eerr := probe.Exists(ctx, token+":k1").Result(); eerr != nil {
		t.Fatalf("EXISTS after deprovision: %v", eerr)
	} else if n != 0 {
		t.Errorf("namespace key survived DeprovisionResource — SCAN+DEL did not reap it")
	}

	// --- Idempotency: a second Deprovision is a clean no-op ---
	if _, err := srv.DeprovisionResource(ctx, &provisionerv1.DeprovisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	}); err != nil {
		t.Errorf("second DeprovisionResource (idempotent) returned %v; want nil", err)
	}
}
