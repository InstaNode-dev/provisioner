package server_test

// server_multitenant_scoping_test.go — REGRESSION TEST for the 2026-06-03
// truehomie-db DROP incident class.
//
// On 2026-06-03 an active Pro customer's database AND role were dropped by a
// non-audited path while a *co-resident* tenant shared the same cluster. The
// failure mode this class represents is an UNSCOPED / OVER-BROAD deprovision:
// tearing down tenant A reaches beyond A's own db_/usr_ and takes out a
// neighbor's database, role, or data.
//
// The PR #45 round-trips already prove that deprovisioning a SINGLE tenant
// drops that tenant's db_/usr_ and is idempotent. They do NOT prove SCOPING —
// that the DROP is confined to the target tenant. That is the new value here.
//
// These tests provision TWO co-resident tenants (A and B) through the genuine
// gRPC ProvisionResource handler against a real Postgres / real Redis, seed
// data into each, then DeprovisionResource(A) and assert:
//   - A's database + role are gone (DROP ran), AND
//   - B's database + role still EXIST, B's seeded row is INTACT, and B can
//     still CONNECT with its own credentials (the neighbor survives).
//
// If a future change made the postgres DROP DATABASE / DROP USER (or the redis
// ACL DELUSER / namespace SCAN+DEL) match more than the target token, exactly
// this assertion fails — which is the assertion that would have caught the
// truehomie incident before it reached prod.
//
// Env-gated identically to server_live_roundtrip_test.go: skips cleanly when
// the backend URL is unset (so `go test -short` in CI without a backend stays
// green) and runs for real against local dev Postgres (localhost:5432) / Redis
// (localhost:6379) or CI's coverage.yml docker services.

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	goredis "github.com/redis/go-redis/v9"

	commonv1 "instant.dev/proto/common/v1"
	provisionerv1 "instant.dev/proto/provisioner/v1"
)

// pgTenantCanConnectAndRead opens a connection with the tenant's OWN
// credentials (the gRPC-returned ConnectionUrl) and reads back the single
// seeded row, asserting both connectivity and data integrity survive a
// neighbor's deprovision. Fails the test on any error.
func pgTenantCanConnectAndRead(t *testing.T, tenantConnURL, wantVal string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, tenantConnURL)
	if err != nil {
		t.Fatalf("tenant B can no longer CONNECT with its own credentials after neighbor deprovision: %v", err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	var got string
	if err := conn.QueryRow(ctx, "SELECT v FROM scoping_probe WHERE id = 1").Scan(&got); err != nil {
		t.Fatalf("tenant B seeded row unreadable after neighbor deprovision: %v", err)
	}
	if got != wantVal {
		t.Errorf("tenant B seeded data corrupted: got %q want %q", got, wantVal)
	}
}

// pgSeedTenant connects with the tenant's own ConnectionUrl, creates a probe
// table, and inserts a single sentinel row. Mirrors what a real customer app
// would do immediately after provisioning.
func pgSeedTenant(t *testing.T, tenantConnURL, val string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, tenantConnURL)
	if err != nil {
		t.Fatalf("seed connect (tenant own creds): %v", err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	if _, err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS scoping_probe (id int PRIMARY KEY, v text)"); err != nil {
		t.Fatalf("seed CREATE TABLE: %v", err)
	}
	if _, err := conn.Exec(ctx,
		"INSERT INTO scoping_probe (id, v) VALUES (1, $1) ON CONFLICT (id) DO UPDATE SET v = EXCLUDED.v", val,
	); err != nil {
		t.Fatalf("seed INSERT: %v", err)
	}
}

// TestServer_Postgres_Deprovision_IsScopedToTargetTenant is the truehomie-DROP
// regression: deprovisioning tenant A must drop ONLY A's db_/usr_ and leave a
// CO-RESIDENT tenant B's database, role, AND seeded data fully intact and
// connectable. This is the assertion the 2026-06-03 incident lacked.
func TestServer_Postgres_Deprovision_IsScopedToTargetTenant(t *testing.T) {
	adminDSN := livePostgresAdminDSN()
	if adminDSN == "" {
		t.Skip("TEST_POSTGRES_CUSTOMERS_URL/TEST_POSTGRES_ADMIN_DSN unset — skipping multi-tenant Postgres scoping test")
	}
	srv := liveServerWithRealPostgres(adminDSN)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Two distinct co-resident tenants on the SAME cluster.
	tokenA := liveToken(t) + "a"
	tokenB := liveToken(t) + "b"
	dbA, usrA := "db_"+tokenA, "usr_"+tokenA
	dbB, usrB := "db_"+tokenB, "usr_"+tokenB
	t.Cleanup(func() { cleanupPG(t, adminDSN, dbA, usrA) })
	t.Cleanup(func() { cleanupPG(t, adminDSN, dbB, usrB) })

	// --- Provision A and B through the genuine gRPC handler ---
	provA, err := srv.ProvisionResource(ctx, &provisionerv1.ProvisionRequest{
		Token:        tokenA,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		Tier:         "hobby",
	})
	if err != nil {
		t.Fatalf("ProvisionResource(A): %v", err)
	}
	provB, err := srv.ProvisionResource(ctx, &provisionerv1.ProvisionRequest{
		Token:        tokenB,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		Tier:         "hobby",
	})
	if err != nil {
		t.Fatalf("ProvisionResource(B): %v", err)
	}

	// Sanity: both databases + roles exist before any teardown.
	if !pgDatabaseExists(t, adminDSN, dbA) {
		t.Fatalf("precondition: A's database %q missing after provision", dbA)
	}
	if !pgDatabaseExists(t, adminDSN, dbB) {
		t.Fatalf("precondition: B's database %q missing after provision", dbB)
	}
	if _, ok := pgConnLimit(t, adminDSN, usrA); !ok {
		t.Fatalf("precondition: A's role %q missing after provision", usrA)
	}
	if _, ok := pgConnLimit(t, adminDSN, usrB); !ok {
		t.Fatalf("precondition: B's role %q missing after provision", usrB)
	}

	// Seed real data into each tenant using ITS OWN credentials.
	pgSeedTenant(t, provA.ConnectionUrl, "tenant-A-data")
	pgSeedTenant(t, provB.ConnectionUrl, "tenant-B-data")

	// --- Deprovision ONLY tenant A ---
	depA, err := srv.DeprovisionResource(ctx, &provisionerv1.DeprovisionRequest{
		Token:        tokenA,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("DeprovisionResource(A): %v", err)
	}
	if !depA.Deprovisioned {
		t.Errorf("DeprovisionResource(A).Deprovisioned = false; want true")
	}

	// --- A is gone (DROP actually ran) ---
	if pgDatabaseExists(t, adminDSN, dbA) {
		t.Errorf("after Deprovision(A), A's database %q still exists — DROP DATABASE did not run", dbA)
	}
	if _, ok := pgConnLimit(t, adminDSN, usrA); ok {
		t.Errorf("after Deprovision(A), A's role %q still exists — DROP USER did not run", usrA)
	}

	// --- B SURVIVES: the truehomie regression assertion ---
	if !pgDatabaseExists(t, adminDSN, dbB) {
		t.Fatalf("REGRESSION (truehomie class): deprovisioning A dropped co-resident B's database %q", dbB)
	}
	if _, ok := pgConnLimit(t, adminDSN, usrB); !ok {
		t.Fatalf("REGRESSION (truehomie class): deprovisioning A dropped co-resident B's role %q", usrB)
	}
	// B's data is intact AND B can still connect with its own credentials.
	pgTenantCanConnectAndRead(t, provB.ConnectionUrl, "tenant-B-data")
}

// TestServer_Redis_Deprovision_IsScopedToTargetTenant is the redis analogue:
// deprovisioning tenant A removes A's ACL user + A's namespace keys, and leaves
// co-resident tenant B's ACL user and namespace keys fully intact.
func TestServer_Redis_Deprovision_IsScopedToTargetTenant(t *testing.T) {
	redisURL := liveRedisURL()
	if redisURL == "" {
		t.Skip("TEST_REDIS_URL/CUSTOMER_REDIS_URL unset — skipping multi-tenant Redis scoping test")
	}
	opt, err := goredis.ParseURL(redisURL)
	if err != nil {
		t.Skipf("redis URL %q does not parse: %v", redisURL, err)
	}
	probe := goredis.NewClient(opt)
	pctx, pcancel := context.WithTimeout(context.Background(), time.Second)
	defer pcancel()
	if perr := probe.Ping(pctx).Err(); perr != nil {
		_ = probe.Close()
		t.Skipf("redis not reachable at %s: %v", opt.Addr, perr)
	}

	srv := liveServerWithRealRedis(opt.Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tokenA := liveToken(t) + "a"
	tokenB := liveToken(t) + "b"
	usrA := "usr_" + tokenA
	usrB := "usr_" + tokenB
	t.Cleanup(func() {
		for _, u := range []string{usrA, usrB} {
			_ = probe.Do(context.Background(), "ACL", "DELUSER", u).Err()
		}
		for _, tok := range []string{tokenA, tokenB} {
			if keys, _, kerr := probe.Scan(context.Background(), 0, tok+":*", 100).Result(); kerr == nil && len(keys) > 0 {
				_ = probe.Del(context.Background(), keys...).Err()
			}
		}
		_ = probe.Close()
	})

	// --- Provision A and B through the genuine gRPC handler ---
	if _, err := srv.ProvisionResource(ctx, &provisionerv1.ProvisionRequest{
		Token:        tokenA,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
		Tier:         "hobby",
	}); err != nil {
		t.Fatalf("ProvisionResource(redis A): %v", err)
	}
	if _, err := srv.ProvisionResource(ctx, &provisionerv1.ProvisionRequest{
		Token:        tokenB,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
		Tier:         "hobby",
	}); err != nil {
		t.Fatalf("ProvisionResource(redis B): %v", err)
	}

	// Both ACL users exist; seed a namespace key into each.
	if gerr := probe.Do(ctx, "ACL", "GETUSER", usrA).Err(); gerr != nil {
		t.Fatalf("precondition: A's ACL user %q missing: %v", usrA, gerr)
	}
	if gerr := probe.Do(ctx, "ACL", "GETUSER", usrB).Err(); gerr != nil {
		t.Fatalf("precondition: B's ACL user %q missing: %v", usrB, gerr)
	}
	if serr := probe.Set(ctx, tokenA+":k1", "vA", 0).Err(); serr != nil {
		t.Fatalf("seed A key: %v", serr)
	}
	if serr := probe.Set(ctx, tokenB+":k1", "vB", 0).Err(); serr != nil {
		t.Fatalf("seed B key: %v", serr)
	}

	// --- Deprovision ONLY tenant A ---
	depA, err := srv.DeprovisionResource(ctx, &provisionerv1.DeprovisionRequest{
		Token:        tokenA,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	if err != nil {
		t.Fatalf("DeprovisionResource(redis A): %v", err)
	}
	if !depA.Deprovisioned {
		t.Errorf("DeprovisionResource(redis A).Deprovisioned = false; want true")
	}

	// --- A is gone ---
	if gerr := probe.Do(ctx, "ACL", "GETUSER", usrA).Err(); gerr == nil {
		t.Errorf("after Deprovision(A), A's ACL user %q still exists — DELUSER did not run", usrA)
	}
	if n, eerr := probe.Exists(ctx, tokenA+":k1").Result(); eerr != nil {
		t.Fatalf("EXISTS A key after deprovision: %v", eerr)
	} else if n != 0 {
		t.Errorf("after Deprovision(A), A's namespace key survived — SCAN+DEL did not reap it")
	}

	// --- B SURVIVES: the truehomie regression assertion (redis) ---
	if gerr := probe.Do(ctx, "ACL", "GETUSER", usrB).Err(); gerr != nil {
		t.Fatalf("REGRESSION (truehomie class): deprovisioning A removed co-resident B's ACL user %q: %v", usrB, gerr)
	}
	if n, eerr := probe.Exists(ctx, tokenB+":k1").Result(); eerr != nil {
		t.Fatalf("EXISTS B key after A deprovision: %v", eerr)
	} else if n != 1 {
		t.Fatalf("REGRESSION (truehomie class): deprovisioning A reaped co-resident B's namespace key %q", tokenB+":k1")
	}
	if v, gerr := probe.Get(ctx, tokenB+":k1").Result(); gerr != nil || v != "vB" {
		t.Errorf("tenant B key value after A deprovision = %q (err %v); want %q", v, gerr, "vB")
	}
}
