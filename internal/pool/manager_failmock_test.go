package pool

// manager_failmock_test.go — failing-backend coverage for the provision-error
// arms of provisionOneItemBackend (one per resource type) plus the
// provisionItemsConcurrently error-log branch and the provisionOneItem INSERT
// error arm. These run hermetically (no DB) except the INSERT-error test, which
// is gated on TEST_PROVISIONER_DATABASE_URL via newDBManager.

import (
	"context"
	"errors"
	"testing"

	"instant.dev/provisioner/internal/backend/mongo"
	"instant.dev/provisioner/internal/backend/postgres"
	"instant.dev/provisioner/internal/backend/queue"
	"instant.dev/provisioner/internal/backend/redis"
)

var errBackendDown = errors.New("backend down")

// failing backends — each Provision returns errBackendDown so the matching arm
// of provisionOneItemBackend takes its error branch.
type failPostgres struct{}

func (failPostgres) Provision(context.Context, string, string, int) (*postgres.Credentials, error) {
	return nil, errBackendDown
}
func (failPostgres) StorageBytes(context.Context, string, string) (int64, error) { return 0, nil }
func (failPostgres) Deprovision(context.Context, string, string) error           { return nil }
func (failPostgres) Regrade(context.Context, string, string, int) (postgres.RegradeResult, error) {
	return postgres.RegradeResult{}, nil
}

type failRedis struct{}

func (failRedis) Provision(context.Context, string, string) (*redis.Credentials, error) {
	return nil, errBackendDown
}
func (failRedis) StorageBytes(context.Context, string, string) (int64, error) { return 0, nil }
func (failRedis) Deprovision(context.Context, string, string) error           { return nil }

type failMongo struct{}

func (failMongo) Provision(context.Context, string, string) (*mongo.Credentials, error) {
	return nil, errBackendDown
}
func (failMongo) StorageBytes(context.Context, string, string) (int64, error) { return 0, nil }
func (failMongo) Deprovision(context.Context, string, string) error           { return nil }

type failQueue struct{}

func (failQueue) Provision(context.Context, string, string) (*queue.Credentials, error) {
	return nil, errBackendDown
}
func (failQueue) Deprovision(context.Context, string, string) error { return nil }

var (
	_ postgres.Backend = failPostgres{}
	_ redis.Backend    = failRedis{}
	_ mongo.Backend    = failMongo{}
	_ queue.Backend    = failQueue{}
)

// TestProvisionOneItemBackend_ProvisionError — every resource type's backend
// Provision error must propagate out of provisionOneItemBackend. Iterates all
// four arms so a new resource type can't slip through with a silent success.
func TestProvisionOneItemBackend_ProvisionError(t *testing.T) {
	m := &Manager{
		aesKey:    make([]byte, 32),
		postgresB: failPostgres{},
		redisB:    failRedis{},
		mongoB:    failMongo{},
		queueB:    failQueue{},
	}
	for _, rt := range []string{"postgres", "redis", "mongodb", "queue"} {
		t.Run(rt, func(t *testing.T) {
			_, err := m.provisionOneItemBackend(context.Background(), rt)
			if !errors.Is(err, errBackendDown) {
				t.Fatalf("%s: err = %v, want wrap of errBackendDown", rt, err)
			}
		})
	}
}

// TestProvisionItemsConcurrently_AllFail — when every backend provision fails,
// provisionItemsConcurrently must still return (logging each failure via the
// error-log branch) rather than block. No DB needed; provisionOneItem fails at
// the backend phase before any INSERT.
func TestProvisionItemsConcurrently_AllFail(t *testing.T) {
	m := &Manager{
		aesKey:    make([]byte, 32),
		postgresB: failPostgres{},
		redisB:    failRedis{},
		mongoB:    failMongo{},
		queueB:    failQueue{},
	}
	done := make(chan struct{})
	go func() {
		m.provisionItemsConcurrently(context.Background(), "postgres", 5)
		close(done)
	}()
	select {
	case <-done:
	default:
		// give it a moment via the test's own deadline; if it blocks the test
		// times out — that's the failure signal.
		<-done
	}
}

// TestProvisionOneItem_InsertError — a successful backend provision followed by
// a closed pool must surface the INSERT error arm of provisionOneItem.
func TestProvisionOneItem_InsertError(t *testing.T) {
	m, pool, _ := newDBManager(t, Config{})
	pool.Close() // INSERT will now fail; backend (mock) still succeeds

	err := m.provisionOneItem(context.Background(), "postgres")
	if err == nil {
		t.Fatal("provisionOneItem should error when the INSERT fails (closed pool)")
	}
}
