package server_test

// server_p2_test.go — regression tests for the BugBash-2026-05-18 provisioner
// P2 fixes that live in internal/server/server.go:
//
//   - GetStorageBytes RESOURCE_TYPE_QUEUE case (P2-W2-05): queues have no
//     StorageBytes method; the switch must return 0, not InvalidArgument.
//   - GetStorageBytes pool-claimed routing (P1-W3-12 / P2-W2-06): a pool-claimed
//     SHARED resource has a non-empty provider_resource_id carrying the
//     "pooltok:" marker — it must be measured by the SHARED backend, never
//     mis-routed to the dedicated backend.

import (
	"context"
	"testing"

	commonv1 "instant.dev/proto/common/v1"
	provisionerv1 "instant.dev/proto/provisioner/v1"

	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/redis"
	"instant.dev/provisioner/internal/config"
	"instant.dev/provisioner/internal/server"
)

// --- P2-W2-05: GetStorageBytes QUEUE case ---

func TestGetStorageBytes_Queue_ReturnsZeroNotInvalidArgument(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "queue-tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	if err != nil {
		t.Fatalf("GetStorageBytes(QUEUE) returned error %v; want nil — queues meter by "+
			"messages-stored, not bytes, so the case must return 0", err)
	}
	if resp.StorageBytes != 0 {
		t.Fatalf("GetStorageBytes(QUEUE) = %d bytes; want 0", resp.StorageBytes)
	}
	if resp.MeasuredAt == 0 {
		t.Fatal("GetStorageBytes(QUEUE): expected non-zero MeasuredAt")
	}
}

// --- P1-W3-12 / P2-W2-06: pool-claimed routing ---

// callRecordingPGBackend records the provider_resource_id passed to StorageBytes
// so a test can assert which backend a request was routed to.
type callRecordingPGBackend struct {
	gotStorageBytesPRID string
	called              bool
}

func (m *callRecordingPGBackend) Provision(context.Context, string, string, int) (*postgres.Credentials, error) {
	return &postgres.Credentials{}, nil
}
func (m *callRecordingPGBackend) StorageBytes(_ context.Context, _, prid string) (int64, error) {
	m.called = true
	m.gotStorageBytesPRID = prid
	return 7777, nil
}
func (m *callRecordingPGBackend) Deprovision(context.Context, string, string) error { return nil }
func (m *callRecordingPGBackend) Regrade(context.Context, string, string, int) (postgres.RegradeResult, error) {
	return postgres.RegradeResult{}, nil
}

type callRecordingRedisBackend struct {
	called bool
}

func (m *callRecordingRedisBackend) Provision(context.Context, string, string) (*redis.Credentials, error) {
	return &redis.Credentials{}, nil
}
func (m *callRecordingRedisBackend) StorageBytes(context.Context, string, string) (int64, error) {
	m.called = true
	return 9999, nil
}
func (m *callRecordingRedisBackend) Deprovision(context.Context, string, string) error { return nil }

// serverWithDedicated builds a Server whose shared backends are call-recording
// and whose dedicated backends are call-recording, so a test can assert routing.
func serverWithDedicated(sharedPG, dedPG postgres.Backend, sharedRedis, dedRedis redis.Backend) *server.Server {
	return server.NewWithBackends(
		&config.Config{},
		sharedPG,
		sharedRedis,
		&mockMongoBackend{},
		&mockQueueBackend{},
		nil, // storageBackend
		dedPG,
		dedRedis,
		nil, // dedicatedMongoBackend
		nil, // dedicatedQueueBackend
		nil, // pool
	)
}

func TestGetStorageBytes_Postgres_PoolClaimed_RoutesToSharedBackend(t *testing.T) {
	shared := &callRecordingPGBackend{}
	dedicated := &callRecordingPGBackend{}
	srv := serverWithDedicated(shared, dedicated, &mockRedisBackend{}, &mockRedisBackend{})

	// A pool-claimed shared Postgres resource: cluster segment + pooltok marker.
	const poolPRID = "local:0;pooltok:pool-11111111-2222-3333-4444-555555555555"
	resp, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:              "real-token",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		ProviderResourceId: poolPRID,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dedicated.called {
		t.Errorf("pool-claimed shared Postgres was mis-routed to the DEDICATED backend "+
			"(prid=%q) — it must go to the shared backend", poolPRID)
	}
	if !shared.called {
		t.Error("pool-claimed shared Postgres was not routed to the shared backend")
	}
	if shared.gotStorageBytesPRID != poolPRID {
		t.Errorf("shared backend got prid %q; want %q", shared.gotStorageBytesPRID, poolPRID)
	}
	if resp.StorageBytes != 7777 {
		t.Errorf("StorageBytes = %d; want 7777 (from shared backend)", resp.StorageBytes)
	}
}

func TestGetStorageBytes_Postgres_LocalClusterPRID_RoutesToSharedBackend(t *testing.T) {
	shared := &callRecordingPGBackend{}
	dedicated := &callRecordingPGBackend{}
	srv := serverWithDedicated(shared, dedicated, &mockRedisBackend{}, &mockRedisBackend{})

	// A live-provisioned shared Postgres resource: "local:<N>" only.
	_, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:              "real-token",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		ProviderResourceId: "local:2",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dedicated.called {
		t.Error("a 'local:2' shared-cluster PRID was mis-routed to the dedicated backend")
	}
	if !shared.called {
		t.Error("a 'local:2' shared-cluster PRID was not routed to the shared backend")
	}
}

func TestGetStorageBytes_Postgres_DedicatedPRID_RoutesToDedicatedBackend(t *testing.T) {
	shared := &callRecordingPGBackend{}
	dedicated := &callRecordingPGBackend{}
	srv := serverWithDedicated(shared, dedicated, &mockRedisBackend{}, &mockRedisBackend{})

	// A genuine dedicated (Neon) project ID — no "local:" prefix, no marker.
	_, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:              "real-token",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		ProviderResourceId: "neon-project-abc123",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dedicated.called {
		t.Error("a genuine dedicated PRID was not routed to the dedicated backend")
	}
	if shared.called {
		t.Error("a genuine dedicated PRID was wrongly routed to the shared backend")
	}
}

func TestGetStorageBytes_Redis_PoolClaimed_RoutesToSharedBackend(t *testing.T) {
	shared := &callRecordingRedisBackend{}
	dedicated := &callRecordingRedisBackend{}
	srv := serverWithDedicated(&mockPostgresBackend{}, &mockPostgresBackend{}, shared, dedicated)

	// Pool-claimed shared Redis: bare "pooltok:" marker (no cluster segment).
	_, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:              "real-token",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_REDIS,
		ProviderResourceId: "pooltok:pool-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dedicated.called {
		t.Error("pool-claimed shared Redis was mis-routed to the dedicated backend")
	}
	if !shared.called {
		t.Error("pool-claimed shared Redis was not routed to the shared backend")
	}
}
