package server_test

// server_coverage_test.go — coverage-driven tests for the uncovered branches
// in server.go: dedicated-backend dispatch on every RPC, queue provisioning
// (no-pool path), DeprovisionResource error mappings, GetStorageBytes
// dedicated routing, accessor methods, teamIDFromContext via metadata, and
// the end-to-end gRPC pipe via bufconn. Pairs with server_test.go (shape
// happy-paths) and server_p2_test.go (PRID routing) to push the server
// package past 95% statement coverage.

import (
	"context"
	"errors"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"instant.dev/provisioner/internal/backend/mongo"
	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/queue"
	"instant.dev/provisioner/internal/backend/redis"
	"instant.dev/provisioner/internal/circuit"
	"instant.dev/provisioner/internal/config"
	"instant.dev/provisioner/internal/pool"
	"instant.dev/provisioner/internal/server"
	commonv1 "instant.dev/proto/common/v1"
	provisionerv1 "instant.dev/proto/provisioner/v1"
)

// --- fake pool claimer for exercising the s.pool != nil branches ---

type fakePoolClaimer struct {
	items      map[string]*pool.Item // per resource_type
	err        error                 // when set, Claim returns this error
	discarded  []string              // ids passed to Discard
	discardErr error                 // when set, Discard returns this error
}

func (f *fakePoolClaimer) Claim(_ context.Context, resourceType string) (*pool.Item, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.items == nil {
		return nil, nil
	}
	return f.items[resourceType], nil
}

func (f *fakePoolClaimer) Discard(_ context.Context, item *pool.Item) error {
	if item != nil {
		f.discarded = append(f.discarded, item.ID)
	}
	return f.discardErr
}

// --- failing mocks (return error on every op) ---

type failingPGBackend struct{ err error }

func (f *failingPGBackend) Provision(context.Context, string, string, int) (*postgres.Credentials, error) {
	return nil, f.err
}
func (f *failingPGBackend) StorageBytes(context.Context, string, string) (int64, error) {
	return 0, f.err
}
func (f *failingPGBackend) Deprovision(context.Context, string, string) error { return f.err }
func (f *failingPGBackend) Regrade(context.Context, string, string, int) (postgres.RegradeResult, error) {
	return postgres.RegradeResult{}, f.err
}

type failingRedisBackend struct{ err error }

func (f *failingRedisBackend) Provision(context.Context, string, string) (*redis.Credentials, error) {
	return nil, f.err
}
func (f *failingRedisBackend) StorageBytes(context.Context, string, string) (int64, error) {
	return 0, f.err
}
func (f *failingRedisBackend) Deprovision(context.Context, string, string) error { return f.err }

type failingMongoBackend struct{ err error }

func (f *failingMongoBackend) Provision(context.Context, string, string) (*mongo.Credentials, error) {
	return nil, f.err
}
func (f *failingMongoBackend) StorageBytes(context.Context, string, string) (int64, error) {
	return 0, f.err
}
func (f *failingMongoBackend) Deprovision(context.Context, string, string) error { return f.err }

type failingQueueBackend struct{ err error }

func (f *failingQueueBackend) Provision(context.Context, string, string) (*queue.Credentials, error) {
	return nil, f.err
}
func (f *failingQueueBackend) Deprovision(context.Context, string, string) error { return f.err }

// dedicated stubs that succeed and tag the response so the test can assert
// the dedicated path was actually taken.

type dedicatedPGBackend struct {
	provisionCalled, deprovisionCalled, storageCalled bool
}

func (d *dedicatedPGBackend) Provision(context.Context, string, string, int) (*postgres.Credentials, error) {
	d.provisionCalled = true
	return &postgres.Credentials{
		URL:                "postgres://ded:p@host/db_d",
		DatabaseName:       "db_d",
		Username:           "usr_d",
		ProviderResourceID: "neon-project-xyz",
	}, nil
}
func (d *dedicatedPGBackend) StorageBytes(context.Context, string, string) (int64, error) {
	d.storageCalled = true
	return 9001, nil
}
func (d *dedicatedPGBackend) Deprovision(context.Context, string, string) error {
	d.deprovisionCalled = true
	return nil
}
func (d *dedicatedPGBackend) Regrade(context.Context, string, string, int) (postgres.RegradeResult, error) {
	return postgres.RegradeResult{Applied: true, AppliedConnLimit: 20}, nil
}

type dedicatedRedisBackend struct {
	provisionCalled, deprovisionCalled, storageCalled bool
}

func (d *dedicatedRedisBackend) Provision(context.Context, string, string) (*redis.Credentials, error) {
	d.provisionCalled = true
	return &redis.Credentials{
		URL:                "redis://ded:p@host/0",
		KeyPrefix:          "ded:",
		ProviderResourceID: "upstash-db-xyz",
	}, nil
}
func (d *dedicatedRedisBackend) StorageBytes(context.Context, string, string) (int64, error) {
	d.storageCalled = true
	return 8001, nil
}
func (d *dedicatedRedisBackend) Deprovision(context.Context, string, string) error {
	d.deprovisionCalled = true
	return nil
}

type dedicatedMongoBackend struct {
	provisionCalled, deprovisionCalled bool
}

func (d *dedicatedMongoBackend) Provision(context.Context, string, string) (*mongo.Credentials, error) {
	d.provisionCalled = true
	return &mongo.Credentials{
		URL:                "mongodb://ded:p@host/db_d",
		DatabaseName:       "db_d",
		ProviderResourceID: "atlas-cluster-xyz",
	}, nil
}
func (d *dedicatedMongoBackend) StorageBytes(context.Context, string, string) (int64, error) {
	return 7001, nil
}
func (d *dedicatedMongoBackend) Deprovision(context.Context, string, string) error {
	d.deprovisionCalled = true
	return nil
}

type dedicatedQueueBackend struct {
	provisionCalled, deprovisionCalled bool
}

func (d *dedicatedQueueBackend) Provision(context.Context, string, string) (*queue.Credentials, error) {
	d.provisionCalled = true
	return &queue.Credentials{
		URL:                "nats://ded:4222",
		SubjectPrefix:      "ded.",
		ProviderResourceID: "nats-pod-xyz",
	}, nil
}
func (d *dedicatedQueueBackend) Deprovision(context.Context, string, string) error {
	d.deprovisionCalled = true
	return nil
}

// newServerWithDedicated builds a Server with shared + dedicated backends so
// pro-tier requests go through the dedicated dispatch.
func newServerWithDedicated(t *testing.T) (
	*server.Server,
	*dedicatedPGBackend, *dedicatedRedisBackend, *dedicatedMongoBackend, *dedicatedQueueBackend,
) {
	t.Helper()
	dedPG := &dedicatedPGBackend{}
	dedRedis := &dedicatedRedisBackend{}
	dedMongo := &dedicatedMongoBackend{}
	dedQueue := &dedicatedQueueBackend{}
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{},
		&mockRedisBackend{},
		&mockMongoBackend{},
		&mockQueueBackend{},
		nil, // storageBackend
		dedPG,
		dedRedis,
		dedMongo,
		dedQueue,
		nil, // pool
	)
	return srv, dedPG, dedRedis, dedMongo, dedQueue
}

// --- ProvisionResource: dedicated path for each resource type ---

func TestProvisionResource_Postgres_ProTier_UsesDedicated(t *testing.T) {
	srv, dedPG, _, _, _ := newServerWithDedicated(t)
	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok-pro",
		Tier:         "pro",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dedPG.provisionCalled {
		t.Fatal("dedicated postgres backend was not called for pro tier")
	}
	if resp.ProviderResourceId != "neon-project-xyz" {
		t.Errorf("expected dedicated PRID, got %q", resp.ProviderResourceId)
	}
}

func TestProvisionResource_Redis_TeamTier_UsesDedicated(t *testing.T) {
	srv, _, dedRedis, _, _ := newServerWithDedicated(t)
	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok-team",
		Tier:         "team",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dedRedis.provisionCalled {
		t.Fatal("dedicated redis backend was not called for team tier")
	}
	if resp.KeyPrefix != "ded:" {
		t.Errorf("expected ded: prefix, got %q", resp.KeyPrefix)
	}
}

func TestProvisionResource_Mongo_GrowthTier_UsesDedicated(t *testing.T) {
	srv, _, _, dedMongo, _ := newServerWithDedicated(t)
	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok-growth",
		Tier:         "growth",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dedMongo.provisionCalled {
		t.Fatal("dedicated mongo backend was not called for growth tier")
	}
	if resp.DatabaseName != "db_d" {
		t.Errorf("expected db_d, got %q", resp.DatabaseName)
	}
}

func TestProvisionResource_Queue_ProTier_UsesDedicated(t *testing.T) {
	srv, _, _, _, dedQueue := newServerWithDedicated(t)
	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok-q-pro",
		Tier:         "pro",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dedQueue.provisionCalled {
		t.Fatal("dedicated queue backend was not called for pro tier")
	}
	if resp.KeyPrefix != "ded." {
		t.Errorf("expected SubjectPrefix=ded. echoed as KeyPrefix, got %q", resp.KeyPrefix)
	}
	if resp.ProviderResourceId != "nats-pod-xyz" {
		t.Errorf("expected dedicated queue PRID, got %q", resp.ProviderResourceId)
	}
}

// Queue provisioning, shared path (no pool, no dedicated backend).
func TestProvisionResource_Queue_AnonymousTier_UsesShared(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "queue-tok",
		Tier:         "anonymous",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ConnectionUrl == "" {
		t.Fatal("expected non-empty ConnectionUrl from shared queue backend")
	}
}

// Queue shared provisioning failure must map to a gRPC error.
func TestProvisionResource_Queue_SharedFailure_ReturnsError(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{},
		&mockRedisBackend{},
		&mockMongoBackend{},
		&failingQueueBackend{err: errors.New("nats unreachable: connection refused")},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		Tier:         "anonymous",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	assertCode(t, err, codes.Unavailable)
}

// Queue dedicated provisioning failure path.
func TestProvisionResource_Queue_DedicatedFailure_ReturnsError(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil,
		&failingQueueBackend{err: errors.New("invalid: missing operator key")},
		nil,
	)
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		Tier:         "pro",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	assertCode(t, err, codes.InvalidArgument)
}

// Dedicated provision error → mapError'd.
func TestProvisionResource_Redis_DedicatedFailure_ReturnsError(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil,
		nil,
		&failingRedisBackend{err: errors.New("dial tcp: connection refused")},
		nil, nil, nil,
	)
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		Tier:         "pro",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	assertCode(t, err, codes.Unavailable)
}

func TestProvisionResource_Mongo_DedicatedFailure_ReturnsError(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil,
		&failingMongoBackend{err: errors.New("already exists: namespace")},
		nil, nil,
	)
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		Tier:         "pro",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	assertCode(t, err, codes.AlreadyExists)
}

func TestProvisionResource_Postgres_DedicatedFailure_ReturnsError(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil,
		&failingPGBackend{err: errors.New("k8s ns creation failed: timeout")},
		nil, nil, nil, nil,
	)
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		Tier:         "pro",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.Unavailable)
}

// Shared provision error paths (redis/mongo) for mapError coverage.
func TestProvisionResource_Redis_SharedFailure_ReturnsError(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{},
		&failingRedisBackend{err: errors.New("redis acl already exists")},
		&mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token: "tok", Tier: "hobby",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	assertCode(t, err, codes.AlreadyExists)
}

func TestProvisionResource_Mongo_SharedFailure_ReturnsError(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{},
		&failingMongoBackend{err: errors.New("mongo dial timeout")},
		&mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token: "tok", Tier: "hobby",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	assertCode(t, err, codes.Unavailable)
}

// Unknown resource type defaults to InvalidArgument.
func TestProvisionResource_UnknownResourceType_ReturnsInvalidArgument(t *testing.T) {
	srv := newTestServer()
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType(9999),
	})
	assertCode(t, err, codes.InvalidArgument)
}

// --- DeprovisionResource: dedicated + queue branches + error paths ---

func TestDeprovisionResource_Postgres_DedicatedNeonPRID_UsesDedicated(t *testing.T) {
	srv, dedPG, _, _, _ := newServerWithDedicated(t)
	resp, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:              "tok",
		ProviderResourceId: "neon-project-xyz",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dedPG.deprovisionCalled {
		t.Fatal("dedicated postgres deprovision not called for neon-style PRID")
	}
	if !resp.Deprovisioned {
		t.Fatal("expected Deprovisioned=true")
	}
}

// k8s-style PRID ("instant-customer-*") must go through SHARED backend so the
// route-registry connection unregisters the key.
func TestDeprovisionResource_Postgres_K8sPRID_UsesShared(t *testing.T) {
	srv, dedPG, _, _, _ := newServerWithDedicated(t)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:              "tok",
		ProviderResourceId: "instant-customer-tok",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dedPG.deprovisionCalled {
		t.Fatal("dedicated postgres deprovision was wrongly called for k8s-style PRID")
	}
}

func TestDeprovisionResource_Redis_DedicatedPRID_UsesDedicated(t *testing.T) {
	srv, _, dedRedis, _, _ := newServerWithDedicated(t)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:              "tok",
		ProviderResourceId: "upstash-db-xyz",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dedRedis.deprovisionCalled {
		t.Fatal("dedicated redis deprovision not called")
	}
}

func TestDeprovisionResource_Mongo_DedicatedPRID_UsesDedicated(t *testing.T) {
	srv, _, _, dedMongo, _ := newServerWithDedicated(t)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:              "tok",
		ProviderResourceId: "atlas-cluster-xyz",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dedMongo.deprovisionCalled {
		t.Fatal("dedicated mongo deprovision not called")
	}
}

func TestDeprovisionResource_Queue_DedicatedPRID_UsesDedicated(t *testing.T) {
	srv, _, _, _, dedQueue := newServerWithDedicated(t)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:              "tok",
		ProviderResourceId: "nats-pod-xyz",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dedQueue.deprovisionCalled {
		t.Fatal("dedicated queue deprovision not called")
	}
}

func TestDeprovisionResource_Queue_Shared(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Deprovisioned {
		t.Fatal("expected Deprovisioned=true")
	}
}

func TestDeprovisionResource_Queue_K8sPRID_UsesShared(t *testing.T) {
	srv, _, _, _, dedQueue := newServerWithDedicated(t)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:              "tok",
		ProviderResourceId: "instant-customer-tok",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if dedQueue.deprovisionCalled {
		t.Fatal("dedicated queue deprovision was wrongly called for k8s-style PRID")
	}
}

// Deprovision error mappings.
func TestDeprovisionResource_Postgres_DedicatedFailure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil,
		&failingPGBackend{err: errors.New("dial tcp: timeout")},
		nil, nil, nil, nil,
	)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:              "tok",
		ProviderResourceId: "neon-project-xyz",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.Unavailable)
}

func TestDeprovisionResource_Postgres_SharedFailure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&failingPGBackend{err: errors.New("kaniko build failed")},
		&mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.Internal)
}

func TestDeprovisionResource_Redis_DedicatedFailure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil,
		&failingRedisBackend{err: errors.New("connection refused")},
		nil, nil, nil,
	)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:              "tok",
		ProviderResourceId: "upstash-db-xyz",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	assertCode(t, err, codes.Unavailable)
}

func TestDeprovisionResource_Redis_SharedFailure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{},
		&failingRedisBackend{err: errors.New("kaniko: exit 1")},
		&mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	assertCode(t, err, codes.Internal)
}

func TestDeprovisionResource_Mongo_DedicatedFailure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil,
		&failingMongoBackend{err: errors.New("connection refused")},
		nil, nil,
	)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:              "tok",
		ProviderResourceId: "atlas-cluster-xyz",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	assertCode(t, err, codes.Unavailable)
}

func TestDeprovisionResource_Mongo_SharedFailure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{},
		&failingMongoBackend{err: errors.New("kaniko fail")},
		&mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	assertCode(t, err, codes.Internal)
}

func TestDeprovisionResource_Queue_DedicatedFailure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil,
		&failingQueueBackend{err: errors.New("invalid: bad cred")},
		nil,
	)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:              "tok",
		ProviderResourceId: "nats-pod-xyz",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	assertCode(t, err, codes.InvalidArgument)
}

func TestDeprovisionResource_Queue_SharedFailure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{},
		&failingQueueBackend{err: errors.New("kaniko fail")},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	assertCode(t, err, codes.Internal)
}

func TestDeprovisionResource_UnspecifiedType(t *testing.T) {
	srv := newTestServer()
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED,
	})
	assertCode(t, err, codes.InvalidArgument)
}

func TestDeprovisionResource_UnknownType(t *testing.T) {
	srv := newTestServer()
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType(9999),
	})
	assertCode(t, err, codes.InvalidArgument)
}

// --- GetStorageBytes: dedicated paths + mongo + storage + queue + errors ---

func TestGetStorageBytes_Postgres_DedicatedFailure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil,
		&failingPGBackend{err: errors.New("dial timeout")},
		nil, nil, nil, nil,
	)
	_, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:              "tok",
		ProviderResourceId: "neon-project-xyz",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.Unavailable)
}

func TestGetStorageBytes_Redis_DedicatedPRID_UsesDedicated(t *testing.T) {
	srv, _, dedRedis, _, _ := newServerWithDedicated(t)
	resp, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:              "tok",
		ProviderResourceId: "upstash-db-xyz",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !dedRedis.storageCalled {
		t.Fatal("dedicated redis StorageBytes was not called")
	}
	if resp.StorageBytes != 8001 {
		t.Errorf("expected 8001, got %d", resp.StorageBytes)
	}
}

func TestGetStorageBytes_Redis_DedicatedFailure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil,
		&failingRedisBackend{err: errors.New("invalid command")},
		nil, nil, nil,
	)
	_, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:              "tok",
		ProviderResourceId: "upstash-db-xyz",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	assertCode(t, err, codes.InvalidArgument)
}

func TestGetStorageBytes_Mongo_ReturnsBytes(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StorageBytes != 256 {
		t.Errorf("expected 256, got %d", resp.StorageBytes)
	}
}

func TestGetStorageBytes_Mongo_Failure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{},
		&failingMongoBackend{err: errors.New("dial timeout")},
		&mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	assertCode(t, err, codes.Unavailable)
}

func TestGetStorageBytes_Postgres_SharedFailure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&failingPGBackend{err: errors.New("kaniko fail")},
		&mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.Internal)
}

func TestGetStorageBytes_Redis_SharedFailure(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{},
		&failingRedisBackend{err: errors.New("kaniko fail")},
		&mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	assertCode(t, err, codes.Internal)
}

func TestGetStorageBytes_UnspecifiedType(t *testing.T) {
	srv := newTestServer()
	_, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED,
	})
	assertCode(t, err, codes.InvalidArgument)
}

func TestGetStorageBytes_UnknownType(t *testing.T) {
	srv := newTestServer()
	_, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType(9999),
	})
	assertCode(t, err, codes.InvalidArgument)
}

// --- RegradeResource error path: dedicated regrade backend errors ---

func TestRegradeResource_Postgres_BackendError_ReturnsError(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&failingPGBackend{err: errors.New("invalid: bad tier")},
		&mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.RegradeResource(context.Background(), &provisionerv1.RegradeRequest{
		Token:        "tok",
		Tier:         "pro",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.InvalidArgument)
}

// regradePostgres dedicated routing: a non-k8s PRID should be routed to dedicatedPostgresBackend
// when present. This exercises the `backend = s.dedicatedPostgresBackend` branch.
func TestRegradeResource_Postgres_DedicatedPRID_UsesDedicatedBackend(t *testing.T) {
	srv, dedPG, _, _, _ := newServerWithDedicated(t)
	resp, err := srv.RegradeResource(context.Background(), &provisionerv1.RegradeRequest{
		Token:              "tok",
		Tier:               "pro",
		ProviderResourceId: "neon-project-xyz",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// dedicatedPGBackend.Regrade returns Applied=true with conn_limit=20.
	if !resp.Applied {
		t.Fatal("expected Applied=true from dedicated regrade")
	}
	if resp.AppliedConnLimit != 20 {
		t.Errorf("AppliedConnLimit = %d; want 20", resp.AppliedConnLimit)
	}
	_ = dedPG // dedicated backend exercised
}

// regradeRedis error path: the Regrade call returns an error.
func TestRegradeResource_Redis_BackendError_ReturnsError(t *testing.T) {
	regrader := &mockRegraderRedisBackend{
		regrade: func(_ context.Context, _, _ string, _ int) (redis.RegradeResult, error) {
			return redis.RegradeResult{}, errors.New("invalid: nope")
		},
	}
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, regrader, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.RegradeResource(context.Background(), &provisionerv1.RegradeRequest{
		Token:              "tok",
		Tier:               "pro",
		ProviderResourceId: "instant-customer-tok",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	assertCode(t, err, codes.InvalidArgument)
}

// regradeRedis with pool-token marker on PRID: BasePRID strips it, then the
// bare-token path constructs the namespace.
func TestRegradeResource_Redis_PoolTokenMarker_StrippedAndNamespaceConstructed(t *testing.T) {
	const realToken = "real-tok"
	var capturedPRID string
	regrader := &mockRegraderRedisBackend{
		regrade: func(_ context.Context, _, id string, _ int) (redis.RegradeResult, error) {
			capturedPRID = id
			return redis.RegradeResult{Applied: true, AppliedMaxmemory: 512 * 1024 * 1024}, nil
		},
	}
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, regrader, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	// Pool-token marker — BasePRID will strip back to "".
	resp, err := srv.RegradeResource(context.Background(), &provisionerv1.RegradeRequest{
		Token:              realToken,
		Tier:               "pro",
		ProviderResourceId: "pooltok:pool-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Applied {
		t.Fatal("expected Applied=true")
	}
	wantPRID := "instant-customer-" + realToken
	if capturedPRID != wantPRID {
		t.Errorf("PRID passed to Regrade = %q; want %q (pool marker should have been stripped)",
			capturedPRID, wantPRID)
	}
}

// --- teamIDFromContext via gRPC metadata: passes through ProvisionResource ---

func TestProvisionResource_TeamIDMetadata_ThreadedIntoContext(t *testing.T) {
	// We can't directly observe the injected value without a peek, but we can
	// verify that a provision with team-id metadata completes successfully
	// (covering the teamIDFromContext branch where the key is present).
	md := metadata.New(map[string]string{"x-instant-team-id": "team-uuid-123"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	srv := newTestServer()
	resp, err := srv.ProvisionResource(ctx, &provisionerv1.ProvisionRequest{
		Token:        "tok",
		Tier:         "anonymous",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ConnectionUrl == "" {
		t.Fatal("expected non-empty ConnectionUrl")
	}
}

// teamIDFromContext: empty metadata key path (Get returns no values).
func TestProvisionResource_TeamIDMetadata_KeyAbsent_StillSucceeds(t *testing.T) {
	md := metadata.New(map[string]string{"unrelated-key": "value"})
	ctx := metadata.NewIncomingContext(context.Background(), md)

	srv := newTestServer()
	resp, err := srv.ProvisionResource(ctx, &provisionerv1.ProvisionRequest{
		Token:        "tok",
		Tier:         "anonymous",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ConnectionUrl == "" {
		t.Fatal("expected non-empty ConnectionUrl")
	}
}

// --- accessor methods: PostgresBackend, Breakers ---

func TestServer_PostgresBackend_ReturnsConfigured(t *testing.T) {
	pg := &mockPostgresBackend{}
	srv := server.NewWithBackends(
		&config.Config{},
		pg, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	if got := srv.PostgresBackend(); got != pg {
		t.Errorf("PostgresBackend() = %v, want %v", got, pg)
	}
}

func TestServer_Breakers_ReturnsConfigured(t *testing.T) {
	srv := newTestServer()
	br := circuit.NewBreakers()
	srv.SetBreakers(br)
	if got := srv.Breakers(); got != br {
		t.Errorf("Breakers() = %v, want %v", got, br)
	}
}

// --- mapError: invalid-argument substring path ---

// Triggered by Provision returning a non-pg error whose message contains
// "invalid" — mapError must classify it as InvalidArgument.
func TestProvisionResource_InvalidArgErrorPath(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&failingPGBackend{err: errors.New("invalid tier name: floob")},
		&mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		Tier:         "hobby",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.InvalidArgument)
}

// --- New(): exercise the config-driven constructor with a minimal config ---

// New() takes a *config.Config and constructs all the wired backends. With
// every dedicated/k8s field zero-valued, it falls through to NewWithBackends
// without touching the k8s API or external services. This covers the all-
// defaults branch.
func TestNew_MinimalConfig_DoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("server.New panicked on minimal config: %v", r)
		}
	}()
	// Pick a non-DSN backend that doesn't try to dial:
	cfg := &config.Config{
		PostgresProvisionBackend: "local",
		RedisProvisionBackend:    "local",
		MongoProvisionBackend:    "local",
		QueueProvisionBackend:    "local",
	}
	srv := server.New(cfg, nil)
	if srv == nil {
		t.Fatal("server.New returned nil")
	}
}

// K8sDedicatedBackend=true but no K8sExternalHost → logs error, leaves dedicated nil.
func TestNew_K8sDedicatedBackend_NoExternalHost_LogsAndContinues(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("server.New panicked: %v", r)
		}
	}()
	cfg := &config.Config{
		PostgresProvisionBackend: "local",
		RedisProvisionBackend:    "local",
		MongoProvisionBackend:    "local",
		QueueProvisionBackend:    "local",
		K8sDedicatedBackend:      true,
		// K8sExternalHost intentionally empty
	}
	srv := server.New(cfg, nil)
	if srv == nil {
		t.Fatal("server.New returned nil")
	}
}

// Dedicated DSN-style config: hits NewDedicatedBackend constructors.
func TestNew_DedicatedDSNs_ConstructsDedicatedBackends(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("server.New panicked: %v", r)
		}
	}()
	cfg := &config.Config{
		PostgresProvisionBackend: "local",
		RedisProvisionBackend:    "local",
		MongoProvisionBackend:    "local",
		QueueProvisionBackend:    "local",
		DedicatedPostgresDSN:     "postgres://x:y@host/db",
		DedicatedRedisURL:        "redis://host:6379",
	}
	srv := server.New(cfg, nil)
	if srv == nil {
		t.Fatal("server.New returned nil")
	}
}

// MinIO endpoint set: hits the storage.New constructor branch.
func TestNew_MinIOEndpoint_ConstructsStorageBackend(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("server.New panicked: %v", r)
		}
	}()
	cfg := &config.Config{
		PostgresProvisionBackend: "local",
		RedisProvisionBackend:    "local",
		MongoProvisionBackend:    "local",
		QueueProvisionBackend:    "local",
		MinioEndpoint:            "localhost:9000",
		MinioRootUser:            "minio",
		MinioRootPassword:        "minio12345",
		MinioBucketName:          "test-bucket",
	}
	srv := server.New(cfg, nil)
	if srv == nil {
		t.Fatal("server.New returned nil")
	}
}

// --- end-to-end via bufconn: round-trip a real gRPC call ---

// bufconnDialer registers Server on a bufconn listener and returns a client.
func newBufconnClient(t *testing.T, srv *server.Server) (provisionerv1.ProvisionerServiceClient, func()) {
	t.Helper()
	const bufSize = 1024 * 1024
	lis := bufconn.Listen(bufSize)
	gs := grpc.NewServer()
	provisionerv1.RegisterProvisionerServiceServer(gs, srv)
	go func() {
		_ = gs.Serve(lis)
	}()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("bufconn dial: %v", err)
	}
	client := provisionerv1.NewProvisionerServiceClient(conn)
	return client, func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	}
}

func TestBufconn_ProvisionResource_E2E(t *testing.T) {
	srv := newTestServer()
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	resp, err := client.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "e2e-tok",
		Tier:         "anonymous",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("e2e ProvisionResource: %v", err)
	}
	if resp.ConnectionUrl == "" {
		t.Fatal("e2e: expected non-empty ConnectionUrl")
	}
}

func TestBufconn_DeprovisionResource_E2E(t *testing.T) {
	srv := newTestServer()
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	resp, err := client.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "e2e-tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("e2e DeprovisionResource: %v", err)
	}
	if !resp.Deprovisioned {
		t.Fatal("e2e: expected Deprovisioned=true")
	}
}

func TestBufconn_GetStorageBytes_E2E(t *testing.T) {
	srv := newTestServer()
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	resp, err := client.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "e2e-tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("e2e GetStorageBytes: %v", err)
	}
	if resp.StorageBytes != 1024 {
		t.Errorf("e2e: StorageBytes = %d, want 1024", resp.StorageBytes)
	}
}

// --- Pool branches: provisionPostgres/Redis/Mongo/Queue with fake claimer ---

// Pool claim error: Claim returns an error → falls back to live provision.
func TestProvisionPostgres_PoolError_FallsBackToLive(t *testing.T) {
	pg := &mockPostgresBackend{}
	srv := server.NewWithBackends(
		&config.Config{},
		pg, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	srv.SetPool(&fakePoolClaimer{err: errors.New("db down")})

	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token: "tok", Tier: "hobby",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ConnectionUrl == "" {
		t.Fatal("expected fallback to live provision")
	}
}

// Pool hit with PoolToken: Regrade succeeds, server returns item creds.
func TestProvisionPostgres_PoolHit_WithToken_AppliesRegrade(t *testing.T) {
	var regradedConnLimit int
	pg := &mockPostgresBackend{
		regrade: func(_ context.Context, _, _ string, connLimit int) (postgres.RegradeResult, error) {
			regradedConnLimit = connLimit
			return postgres.RegradeResult{Applied: true, AppliedConnLimit: connLimit}, nil
		},
	}
	srv := server.NewWithBackends(
		&config.Config{},
		pg, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	srv.SetPool(&fakePoolClaimer{items: map[string]*pool.Item{
		"postgres": {
			ID:                 "pool-id",
			ConnectionURL:      "postgres://pooled:secret@host/db_pooled",
			ProviderResourceID: "local:0",
			DatabaseName:       "db_pooled",
			Username:           "usr_pooled",
			PoolToken:          "pool-aaaa-bbbb-cccc-dddd",
		},
	}})

	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token: "real-tok", Tier: "hobby",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ConnectionUrl != "postgres://pooled:secret@host/db_pooled" {
		t.Errorf("expected pooled connection URL, got %q", resp.ConnectionUrl)
	}
	if regradedConnLimit == 0 {
		t.Error("Regrade was not called on the pool item")
	}
	// PRID should encode the pool token.
	if resp.ProviderResourceId == "" {
		t.Error("expected non-empty provider_resource_id")
	}
}

// Pool hit with PoolToken but Regrade fails → fall back to live.
func TestProvisionPostgres_PoolHit_RegradeFailure_FallsBack(t *testing.T) {
	var provisionCalled bool
	pg := &mockPostgresBackend{
		regrade: func(context.Context, string, string, int) (postgres.RegradeResult, error) {
			return postgres.RegradeResult{}, errors.New("regrade failed: connection lost")
		},
		provision: func(_ context.Context, token, _ string, _ int) (*postgres.Credentials, error) {
			provisionCalled = true
			return &postgres.Credentials{URL: "postgres://live:p@host/db_live", DatabaseName: "db_live", Username: "usr_live"}, nil
		},
	}
	srv := server.NewWithBackends(
		&config.Config{},
		pg, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	srv.SetPool(&fakePoolClaimer{items: map[string]*pool.Item{
		"postgres": {
			ID:            "pool-id",
			ConnectionURL: "postgres://pooled:secret@host/db_pooled",
			PoolToken:     "pool-xxxxxxxx",
		},
	}})

	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token: "real-tok", Tier: "hobby",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !provisionCalled {
		t.Fatal("expected fallback to live provision after regrade failure")
	}
	if resp.DatabaseName != "db_live" {
		t.Errorf("expected db_live, got %q", resp.DatabaseName)
	}
}

// Pool hit but PoolToken missing → fall back to live.
func TestProvisionPostgres_PoolHit_MissingToken_FallsBack(t *testing.T) {
	var provisionCalled bool
	pg := &mockPostgresBackend{
		provision: func(context.Context, string, string, int) (*postgres.Credentials, error) {
			provisionCalled = true
			return &postgres.Credentials{URL: "postgres://live:p@host/db_live", DatabaseName: "db_live"}, nil
		},
	}
	srv := server.NewWithBackends(
		&config.Config{},
		pg, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	srv.SetPool(&fakePoolClaimer{items: map[string]*pool.Item{
		"postgres": {
			ID:            "pool-id",
			ConnectionURL: "postgres://pooled:secret@host/db_pooled",
			PoolToken:     "", // missing — no namespace to target
		},
	}})

	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token: "real-tok", Tier: "hobby",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !provisionCalled {
		t.Fatal("expected fallback to live provision when PoolToken empty")
	}
}

// Redis pool hit: returns pooled credentials with encoded PRID.
func TestProvisionRedis_PoolHit(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	srv.SetPool(&fakePoolClaimer{items: map[string]*pool.Item{
		"redis": {
			ID:                 "pool-r",
			ConnectionURL:      "redis://pooled:p@host/0",
			KeyPrefix:          "pool-x:",
			ProviderResourceID: "",
			PoolToken:          "pool-xxxx-yyyy",
		},
	}})
	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token: "real-tok", Tier: "hobby",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.KeyPrefix != "pool-x:" {
		t.Errorf("expected pooled key_prefix, got %q", resp.KeyPrefix)
	}
}

// Redis pool claim error → falls back to live.
func TestProvisionRedis_PoolError_FallsBackToLive(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	srv.SetPool(&fakePoolClaimer{err: errors.New("db down")})

	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token: "tok", Tier: "hobby",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ConnectionUrl == "" {
		t.Fatal("expected fallback live provision")
	}
}

// Mongo pool hit.
func TestProvisionMongo_PoolHit(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	srv.SetPool(&fakePoolClaimer{items: map[string]*pool.Item{
		"mongodb": {
			ID:            "pool-m",
			ConnectionURL: "mongodb://pooled:p@host/db_pooled",
			DatabaseName:  "db_pooled",
			PoolToken:     "pool-mongo-1",
		},
	}})
	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token: "tok", Tier: "hobby",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.DatabaseName != "db_pooled" {
		t.Errorf("expected db_pooled, got %q", resp.DatabaseName)
	}
}

// Mongo pool claim error → falls back.
func TestProvisionMongo_PoolError_FallsBackToLive(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	srv.SetPool(&fakePoolClaimer{err: errors.New("db down")})

	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token: "tok", Tier: "hobby",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.DatabaseName == "" {
		t.Fatal("expected fallback live provision")
	}
}

// Queue pool hit: returns pooled URL.
func TestProvisionQueue_PoolHit(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	srv.SetPool(&fakePoolClaimer{items: map[string]*pool.Item{
		"queue": {
			ID:                 "pool-q",
			ConnectionURL:      "nats://pooled:4222",
			ProviderResourceID: "instant-customer-queue-pool",
		},
	}})
	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token: "tok", Tier: "anonymous",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ConnectionUrl != "nats://pooled:4222" {
		t.Errorf("expected pooled NATS URL, got %q", resp.ConnectionUrl)
	}
}

// Queue pool claim error → falls back to live.
func TestProvisionQueue_PoolError_FallsBackToLive(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	srv.SetPool(&fakePoolClaimer{err: errors.New("db down")})

	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token: "tok", Tier: "anonymous",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ConnectionUrl == "" {
		t.Fatal("expected fallback live provision")
	}
}

// --- callBackendVoid breaker-open path via DeprovisionResource ---

// Trip the postgres_admin breaker via 5 failing provisions, then issue a
// Deprovision and verify it short-circuits with Unavailable WITHOUT calling
// the backend mock — exercising the `!b.Allow()` branch in callBackendVoid.
func TestDeprovisionResource_BreakerOpen_ReturnsUnavailable(t *testing.T) {
	deprovisionCalls := 0
	pg := &mockPostgresBackend{
		provision: func(context.Context, string, string, int) (*postgres.Credentials, error) {
			return nil, errors.New("permission denied: cannot create role")
		},
		deprovision: func(context.Context, string, string) error {
			deprovisionCalls++
			return nil
		},
	}
	srv := server.NewWithBackends(
		&config.Config{},
		pg, &mockRedisBackend{}, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	srv.SetBreakers(freshBreakers())

	// Trip postgres_admin breaker.
	for i := 0; i < 5; i++ {
		_, _ = srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
			Token:        "tok",
			Tier:         "hobby",
			ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		})
	}

	// Deprovision must now short-circuit without invoking the mock.
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.Unavailable)
	if deprovisionCalls != 0 {
		t.Fatalf("breaker should have prevented backend call, but deprovision was called %d times", deprovisionCalls)
	}
}

// --- GetStorageBytes: storage backend non-nil but ListObjects error ---

// We can't easily inject a failing MinIO backend (storage.MinIOBackend is a
// concrete struct), but we can construct one pointing at an unreachable
// endpoint so that BucketExists fails — exercising the error path inside
// GetStorageBytes for RESOURCE_TYPE_STORAGE.
func TestGetStorageBytes_Storage_BackendError_ReturnsError(t *testing.T) {
	// Use the same New() path New(config) follows so we exercise that
	// constructor branch too: MinIO endpoint set, but pointing at 127.0.0.1:1
	// which refuses connections.
	cfg := &config.Config{
		PostgresProvisionBackend: "local",
		RedisProvisionBackend:    "local",
		MongoProvisionBackend:    "local",
		QueueProvisionBackend:    "local",
		MinioEndpoint:            "127.0.0.1:1",
		MinioRootUser:            "minio",
		MinioRootPassword:        "minio12345",
		MinioBucketName:          "test-bucket",
	}
	srv := server.New(cfg, nil)
	// Drive a Storage StorageBytes — BucketExists will fail to dial.
	_, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "a1b2c3d4",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_STORAGE,
	})
	if err == nil {
		t.Fatal("expected error from unreachable MinIO backend")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Fatalf("expected Unavailable (connect failure), got %v: %v", st, err)
	}
}

// --- New() k8s branch with K8sExternalHost set ---

func TestNew_K8sDedicatedBackend_WithExternalHost_ConstructsBackends(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("server.New panicked: %v", r)
		}
	}()
	cfg := &config.Config{
		PostgresProvisionBackend: "local",
		RedisProvisionBackend:    "local",
		MongoProvisionBackend:    "local",
		QueueProvisionBackend:    "local",
		K8sDedicatedBackend:      true,
		K8sExternalHost:          "127.0.0.1",
		K8sStorageClass:          "local-path",
		K8sPostgresImage:         "postgres:16",
		K8sRedisImage:            "redis:7-alpine",
		K8sMongoImage:            "mongo:7",
		K8sNatsImage:             "nats:2.10-alpine",
		K8sPostgresStorageGi:     10,
		K8sRedisStorageGi:        5,
		K8sMongoStorageGi:        10,
	}
	srv := server.New(cfg, nil)
	if srv == nil {
		t.Fatal("server.New returned nil")
	}
}

// --- isSharedBackendProviderID extra branches ---

// regradeRedis with a "local:N" PRID — BasePRID leaves it untouched, then the
// branch "doesn't have instant-customer- prefix → construct from non-empty
// effectivePRID" walks through the bareToken != "" branch.
func TestRegradeResource_Redis_LocalPRID_ConstructsNamespaceFromIt(t *testing.T) {
	var capturedPRID string
	regrader := &mockRegraderRedisBackend{
		regrade: func(_ context.Context, _, id string, _ int) (redis.RegradeResult, error) {
			capturedPRID = id
			return redis.RegradeResult{Applied: true, AppliedMaxmemory: 0}, nil
		},
	}
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{}, regrader, &mockMongoBackend{}, &mockQueueBackend{},
		nil, nil, nil, nil, nil, nil,
	)
	_, err := srv.RegradeResource(context.Background(), &provisionerv1.RegradeRequest{
		Token:              "real-tok",
		Tier:               "team",
		ProviderResourceId: "local:0", // not k8s-prefixed
		ResourceType:       commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The bareToken branch took "local:0" and prefixed it; not ideal but
	// exercises the construction branch faithfully.
	want := "instant-customer-local:0"
	if capturedPRID != want {
		t.Errorf("captured PRID = %q, want %q", capturedPRID, want)
	}
}

// --- bufconn empty-token path (already added) ---

func TestBufconn_EmptyToken_ReturnsInvalidArgument(t *testing.T) {
	srv := newTestServer()
	client, cleanup := newBufconnClient(t, srv)
	defer cleanup()

	_, err := client.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err == nil {
		t.Fatal("expected error for empty token")
	}
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
}
