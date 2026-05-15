package server_test

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "instant.dev/proto/common/v1"
	provisionerv1 "instant.dev/proto/provisioner/v1"

	"instant.dev/provisioner/internal/backend/mongo"
	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/queue"
	"instant.dev/provisioner/internal/backend/redis"
	"instant.dev/provisioner/internal/config"
	"instant.dev/provisioner/internal/server"
)

// --- mock backends ---

type mockPostgresBackend struct {
	provision    func(ctx context.Context, token, tier string) (*postgres.Credentials, error)
	storageBytes func(ctx context.Context, token, providerResourceID string) (int64, error)
	deprovision  func(ctx context.Context, token, providerResourceID string) error
	regrade      func(ctx context.Context, token, providerResourceID string, connLimit int) (postgres.RegradeResult, error)
}

func (m *mockPostgresBackend) Provision(ctx context.Context, token, tier string) (*postgres.Credentials, error) {
	if m.provision != nil {
		return m.provision(ctx, token, tier)
	}
	return &postgres.Credentials{
		URL:          "postgres://usr_tok:pass@host/db_tok",
		DatabaseName: "db_tok",
		Username:     "usr_tok",
	}, nil
}

func (m *mockPostgresBackend) StorageBytes(ctx context.Context, token, id string) (int64, error) {
	if m.storageBytes != nil {
		return m.storageBytes(ctx, token, id)
	}
	return 1024, nil
}

func (m *mockPostgresBackend) Deprovision(ctx context.Context, token, id string) error {
	if m.deprovision != nil {
		return m.deprovision(ctx, token, id)
	}
	return nil
}

func (m *mockPostgresBackend) Regrade(ctx context.Context, token, id string, connLimit int) (postgres.RegradeResult, error) {
	if m.regrade != nil {
		return m.regrade(ctx, token, id, connLimit)
	}
	return postgres.RegradeResult{Applied: false, SkipReason: "backend has no per-role connection cap"}, nil
}

type mockRedisBackend struct {
	provision    func(ctx context.Context, token, tier string) (*redis.Credentials, error)
	storageBytes func(ctx context.Context, token, providerResourceID string) (int64, error)
	deprovision  func(ctx context.Context, token, providerResourceID string) error
}

func (m *mockRedisBackend) Provision(ctx context.Context, token, tier string) (*redis.Credentials, error) {
	if m.provision != nil {
		return m.provision(ctx, token, tier)
	}
	return &redis.Credentials{URL: "redis://usr:pass@host/0", KeyPrefix: ""}, nil
}

func (m *mockRedisBackend) StorageBytes(ctx context.Context, token, id string) (int64, error) {
	if m.storageBytes != nil {
		return m.storageBytes(ctx, token, id)
	}
	return 512, nil
}

func (m *mockRedisBackend) Deprovision(ctx context.Context, token, id string) error {
	if m.deprovision != nil {
		return m.deprovision(ctx, token, id)
	}
	return nil
}

type mockMongoBackend struct {
	provision    func(ctx context.Context, token, tier string) (*mongo.Credentials, error)
	storageBytes func(ctx context.Context, token, providerResourceID string) (int64, error)
	deprovision  func(ctx context.Context, token, providerResourceID string) error
}

func (m *mockMongoBackend) Provision(ctx context.Context, token, tier string) (*mongo.Credentials, error) {
	if m.provision != nil {
		return m.provision(ctx, token, tier)
	}
	return &mongo.Credentials{URL: "mongodb://usr:pass@host/db_tok", DatabaseName: "db_tok"}, nil
}

func (m *mockMongoBackend) StorageBytes(ctx context.Context, token, id string) (int64, error) {
	if m.storageBytes != nil {
		return m.storageBytes(ctx, token, id)
	}
	return 256, nil
}

func (m *mockMongoBackend) Deprovision(ctx context.Context, token, id string) error {
	if m.deprovision != nil {
		return m.deprovision(ctx, token, id)
	}
	return nil
}

type mockQueueBackend struct{}

func (m *mockQueueBackend) Provision(_ context.Context, token, _ string) (*queue.Credentials, error) {
	prefix := token
	if len(prefix) > 8 {
		prefix = prefix[:8]
	}
	return &queue.Credentials{URL: "nats://host:4222", SubjectPrefix: prefix + "."}, nil
}

func (m *mockQueueBackend) Deprovision(_ context.Context, _, _ string) error { return nil }

// newTestServer creates a Server with mock backends and no pool.
func newTestServer() *server.Server {
	return server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{},
		&mockRedisBackend{},
		&mockMongoBackend{},
		&mockQueueBackend{},
		nil, // storageBackend
		nil, // dedicatedPostgresBackend
		nil, // dedicatedRedisBackend
		nil, // dedicatedMongoBackend
		nil, // dedicatedQueueBackend
		nil, // pool
	)
}

// --- ProvisionResource tests ---

func TestProvisionResource_EmptyToken_ReturnsInvalidArgument(t *testing.T) {
	srv := newTestServer()
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.InvalidArgument)
}

func TestProvisionResource_UnspecifiedType_ReturnsInvalidArgument(t *testing.T) {
	srv := newTestServer()
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "abc",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_UNSPECIFIED,
	})
	assertCode(t, err, codes.InvalidArgument)
}

func TestProvisionResource_Postgres_Success(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok1",
		Tier:         "anonymous",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ConnectionUrl == "" {
		t.Fatal("expected non-empty ConnectionUrl")
	}
	if resp.DatabaseName == "" {
		t.Fatal("expected non-empty DatabaseName")
	}
}

func TestProvisionResource_Redis_Success(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok2",
		Tier:         "anonymous",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.ConnectionUrl == "" {
		t.Fatal("expected non-empty ConnectionUrl")
	}
}

func TestProvisionResource_MongoDB_Success(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok3",
		Tier:         "anonymous",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.DatabaseName == "" {
		t.Fatal("expected non-empty DatabaseName")
	}
}

func TestProvisionResource_BackendConnectError_ReturnsUnavailable(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{
			provision: func(_ context.Context, _, _ string) (*postgres.Credentials, error) {
				return nil, errors.New("connection refused: cannot reach postgres")
			},
		},
		&mockRedisBackend{},
		&mockMongoBackend{},
		&mockQueueBackend{},
		nil, nil, nil, nil, nil, // storage + dedicated backends
		nil,                     // pool
	)
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.Unavailable)
}

func TestProvisionResource_AlreadyExists_ReturnsAlreadyExists(t *testing.T) {
	srv := server.NewWithBackends(
		&config.Config{},
		&mockPostgresBackend{
			provision: func(_ context.Context, _, _ string) (*postgres.Credentials, error) {
				return nil, errors.New("already exists: database already exists")
			},
		},
		&mockRedisBackend{},
		&mockMongoBackend{},
		&mockQueueBackend{},
		nil, nil, nil, nil, nil, // storage + dedicated backends
		nil,                     // pool
	)
	_, err := srv.ProvisionResource(context.Background(), &provisionerv1.ProvisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.AlreadyExists)
}

// --- DeprovisionResource tests ---

func TestDeprovisionResource_EmptyToken_ReturnsInvalidArgument(t *testing.T) {
	srv := newTestServer()
	_, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.InvalidArgument)
}

func TestDeprovisionResource_Postgres_Success(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Deprovisioned {
		t.Fatal("expected Deprovisioned=true")
	}
}

func TestDeprovisionResource_Redis_Success(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Deprovisioned {
		t.Fatal("expected Deprovisioned=true")
	}
}

func TestDeprovisionResource_MongoDB_Success(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.DeprovisionResource(context.Background(), &provisionerv1.DeprovisionRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Deprovisioned {
		t.Fatal("expected Deprovisioned=true")
	}
}

// --- GetStorageBytes tests ---

func TestGetStorageBytes_EmptyToken_ReturnsInvalidArgument(t *testing.T) {
	srv := newTestServer()
	_, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	assertCode(t, err, codes.InvalidArgument)
}

func TestGetStorageBytes_Postgres_ReturnsBytes(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StorageBytes != 1024 {
		t.Fatalf("expected 1024, got %d", resp.StorageBytes)
	}
	if resp.MeasuredAt == 0 {
		t.Fatal("expected non-zero MeasuredAt")
	}
}

func TestGetStorageBytes_Redis_ReturnsBytes(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "tok",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StorageBytes != 512 {
		t.Fatalf("expected 512, got %d", resp.StorageBytes)
	}
}

func TestGetStorageBytes_Storage_NilMinIOBackend_ReturnsZero(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.GetStorageBytes(context.Background(), &provisionerv1.StorageRequest{
		Token:        "a1b2c3d4-0000-0000-0000-000000000001",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_STORAGE,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.StorageBytes != 0 {
		t.Fatalf("expected 0 when MinIO backend is not configured, got %d", resp.StorageBytes)
	}
	if resp.MeasuredAt == 0 {
		t.Fatal("expected non-zero MeasuredAt")
	}
}

// --- RegradeResource tests ---

func TestRegradeResource_EmptyToken_ReturnsInvalidArgument(t *testing.T) {
	srv := newTestServer()
	_, err := srv.RegradeResource(context.Background(), &provisionerv1.RegradeRequest{
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		Tier:         "pro",
	})
	assertCode(t, err, codes.InvalidArgument)
}

func TestRegradeResource_NonPostgres_SkipsWithReason(t *testing.T) {
	srv := newTestServer()
	resp, err := srv.RegradeResource(context.Background(), &provisionerv1.RegradeRequest{
		Token:        "tok-123",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_REDIS,
		Tier:         "pro",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Applied {
		t.Fatal("expected applied=false for non-postgres resource")
	}
	if resp.SkipReason != "unsupported resource type for regrade" {
		t.Fatalf("unexpected skip_reason: %q", resp.SkipReason)
	}
}

func TestRegradeResource_NonK8sBackend_SkipsWithReason(t *testing.T) {
	// newTestServer wires the shared mockPostgresBackend, which is not a
	// *postgres.K8sBackend — the server should skip without touching it.
	srv := newTestServer()
	resp, err := srv.RegradeResource(context.Background(), &provisionerv1.RegradeRequest{
		Token:        "tok-123",
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_POSTGRES,
		Tier:         "pro",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Applied {
		t.Fatal("expected applied=false for non-k8s backend")
	}
	if resp.SkipReason != "backend has no per-role connection cap" {
		t.Fatalf("unexpected skip_reason: %q", resp.SkipReason)
	}
}

// --- helper ---

func assertCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error with code %v, got nil", want)
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %T: %v", err, err)
	}
	if st.Code() != want {
		t.Fatalf("expected code %v, got %v: %s", want, st.Code(), st.Message())
	}
}
