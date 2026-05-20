package server_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"

	commonv1 "instant.dev/proto/common/v1"
	provisionerv1 "instant.dev/proto/provisioner/v1"

	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/redis"
	"instant.dev/provisioner/internal/circuit"
	"instant.dev/provisioner/internal/config"
	"instant.dev/provisioner/internal/server"
)

// server_circuit_test.go — wire-level regression for the per-backend
// circuit breakers introduced for audit P0-3. Verifies that:
//
//  1. After enough backend failures, the breaker trips and the next
//     ProvisionResource gRPC call returns codes.Unavailable WITHOUT
//     invoking the backend mock (the whole point of the breaker is to
//     fast-fail without hitting the downstream).
//  2. context.Canceled / context.DeadlineExceeded returns from the backend
//     mock DO NOT count toward the threshold — the breaker stays closed
//     no matter how many times a caller times out.
//  3. The breakers are isolated per-backend: tripping postgres_admin does
//     not affect redis_admin or mongo_admin.
//
// Tests construct a fresh circuit.Breakers via NewBreakers() and inject
// it via Server.SetBreakers() so each test owns its breaker state.

// freshBreakers — every test gets its own breaker set so prior test
// failures don't bleed in via the package-level circuit.Default.
func freshBreakers() *circuit.Breakers {
	return circuit.NewBreakers()
}

// newTestServerWithBackend constructs a Server with one specified
// postgresBackend mock and a fresh per-backend breaker set. All other
// backends are unconfigured (nil dedicated, default mocks for shared).
func newTestServerWithPostgres(pg postgres.Backend) *server.Server {
	srv := server.NewWithBackends(
		&config.Config{},
		pg,
		&mockRedisBackend{},
		&mockMongoBackend{},
		&mockQueueBackend{},
		nil, nil, nil, nil, nil, // storage + dedicated
		nil,                     // pool
	)
	srv.SetBreakers(freshBreakers())
	return srv
}

// TestServer_CircuitTripsOnRepeatedBackendFailures asserts that after the
// configured threshold of consecutive non-deadline failures, the next
// Provision call returns Unavailable without invoking the backend.
func TestServer_CircuitTripsOnRepeatedBackendFailures(t *testing.T) {
	calls := 0
	failingBackend := &mockPostgresBackend{
		provision: func(_ context.Context, _, _ string, _ int) (*postgres.Credentials, error) {
			calls++
			// "permission denied" is a non-retryable Internal error in
			// mapError — so each failure counts cleanly toward the
			// breaker threshold without also being classified as
			// retryable Unavailable from the mapError path.
			return nil, errors.New("permission denied: cannot create role")
		},
	}
	srv := newTestServerWithPostgres(failingBackend)

	// Drive 5 failures (default threshold) to trip the postgres_admin
	// breaker. Each call returns Internal from mapError; that's fine —
	// the breaker only cares about err != nil.
	for i := 0; i < 5; i++ {
		_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
			Token:        "tok",
			ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
			Tier:         "hobby",
		})
		if err == nil {
			t.Fatalf("attempt %d: expected backend error, got nil", i+1)
		}
	}
	if calls != 5 {
		t.Fatalf("backend should have been called 5 times before tripping, got %d", calls)
	}

	// Next call MUST short-circuit with Unavailable — the breaker is open
	// and the mock should NOT be invoked.
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		Tier:         "hobby",
	})
	assertCode(t, err, codes.Unavailable)
	if calls != 5 {
		t.Fatalf("breaker should have short-circuited without invoking backend; backend was called %d times (expected 5)", calls)
	}
}

// TestServer_CircuitDoesNotTripOnCallerDeadline asserts the audit-
// mandated caller-deadline filter: context.Canceled and
// context.DeadlineExceeded returns from the backend never count toward
// the breaker threshold.
func TestServer_CircuitDoesNotTripOnCallerDeadline(t *testing.T) {
	calls := 0
	deadlineBackend := &mockPostgresBackend{
		provision: func(_ context.Context, _, _ string, _ int) (*postgres.Credentials, error) {
			calls++
			// Alternate between Canceled and DeadlineExceeded.
			if calls%2 == 0 {
				return nil, context.DeadlineExceeded
			}
			return nil, context.Canceled
		},
	}
	srv := newTestServerWithPostgres(deadlineBackend)

	// 50 caller-deadline failures — 10× the threshold. If any counts,
	// the breaker would trip and the 51st call would short-circuit.
	for i := 0; i < 50; i++ {
		_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
			Token:        "tok",
			ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
			Tier:         "hobby",
		})
		if err == nil {
			t.Fatalf("attempt %d: expected backend deadline error, got nil", i+1)
		}
	}

	// 51st call MUST still reach the backend (breaker still closed).
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		Tier:         "hobby",
	})
	if err == nil {
		t.Fatal("expected backend error on 51st call")
	}
	if calls != 51 {
		t.Fatalf("breaker tripped on caller-deadline (backend called only %d times, expected 51) — filter is broken", calls)
	}
}

// TestServer_CircuitIsolation_RedisFailureDoesNotAffectPostgres asserts the
// brief's headline invariant: a Redis outage MUST NOT trip the Postgres
// breaker. We hammer the Redis backend until its breaker would trip if it
// were shared, then verify a Postgres provision still works.
func TestServer_CircuitIsolation_RedisFailureDoesNotAffectPostgres(t *testing.T) {
	redisCalls := 0
	failingRedis := &mockRedisBackend{
		provision: func(_ context.Context, _, _ string) (*redis.Credentials, error) {
			redisCalls++
			return nil, errors.New("permission denied: redis ACL setup failed")
		},
	}
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, // healthy postgres
		failingRedis,
		&mockMongoBackend{},
		&mockQueueBackend{},
		nil, nil, nil, nil, nil,
		nil,
	)
	srv.SetBreakers(freshBreakers())

	// Trip the redis_admin breaker (5 consecutive failures).
	for i := 0; i < 5; i++ {
		_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
			Token:        "tok",
			ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
			Tier:         "hobby",
		})
		if err == nil {
			t.Fatalf("redis attempt %d: expected backend error", i+1)
		}
	}
	// Confirm the redis breaker is open.
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
		Tier:         "hobby",
	})
	assertCode(t, err, codes.Unavailable)

	// Postgres provision must still succeed — postgres_admin is unaffected.
	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		Tier:         "hobby",
	})
	if err != nil {
		t.Fatalf("postgres provision should succeed despite Redis outage — per-backend isolation broken: %v", err)
	}
	if resp == nil || resp.ConnectionUrl == "" {
		t.Fatal("postgres provision returned empty response")
	}
}
