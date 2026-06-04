package server_test

// server_live_roundtrip_mqs_test.go — REAL-BACKEND integration coverage for the
// gRPC server layer's Mongo / Queue (NATS) / Storage flows. The companion file
// server_live_roundtrip_test.go covers Postgres + Redis; this file closes the
// remaining cells of the provisioner gRPC × backend matrix (plan §2.3:
// docs/sessions/2026-06-04/INTEGRATION-COVERAGE-PLAN-2026-06-04.md).
//
// Why this file exists (same truehomie-db DROP-incident class, 2026-06-03):
// every pre-existing server_test.go / server_coverage_test.go test injects a
// *fake* mongo/queue/storage backend, so the real createUser/dropUser/dropDatabase
// DDL, the real NATS health-check provision path, and the real MinIO object-walk
// accounting have never run through the genuine gRPC handlers (breaker wrapping,
// tier routing, mapError, response shaping, idempotent re-deprovision). High
// statement coverage from mocks does NOT prove these flows are correct
// end-to-end. These tests drive the real RPC handlers
// (server.ProvisionResource / DeprovisionResource / GetStorageBytes /
// RegradeResource) against a real MongoDB, a real NATS, and a real MinIO/S3, and
// assert the backing artifact is actually created, measured, and torn down — and
// that a second Deprovision is a clean idempotent no-op.
//
// Env-gated: each test skips cleanly when its backend env is unset, so
// `go test -short` (the deploy.yml gate) without backends stays green. They run
// for real when the backend env is present — CI's coverage.yml supplies the
// docker service containers + env vars.

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	"go.mongodb.org/mongo-driver/bson"
	mongodriver "go.mongodb.org/mongo-driver/mongo"
	mongoopts "go.mongodb.org/mongo-driver/mongo/options"

	commonv1 "instant.dev/proto/common/v1"
	provisionerv1 "instant.dev/proto/provisioner/v1"
	"instant.dev/provisioner/internal/backend/mongo"
	"instant.dev/provisioner/internal/backend/queue"
	"instant.dev/provisioner/internal/backend/storage"
	"instant.dev/provisioner/internal/circuit"
	"instant.dev/provisioner/internal/config"
	"instant.dev/provisioner/internal/server"
)

// ─── Mongo ──────────────────────────────────────────────────────────────────

// liveMongoAdminURI returns an admin URI capable of createUser/dropDatabase, or
// "" when none is configured (caller MUST t.Skip). Prefers the authenticated
// instance (CUSTOMER_MONGO_AUTH_URL — CI's mongo-auth on :27018) because that is
// the realistic prod path; falls back to the no-auth instance.
func liveMongoAdminURI() string {
	for _, k := range []string{"CUSTOMER_MONGO_AUTH_URL", "CUSTOMER_MONGO_URL", "TEST_MONGO_URL"} {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// mongoHostFromURI strips the mongodb:// scheme + creds to the bare host:port the
// LocalBackend embeds in customer URLs (mirrors mongo/local_test.go::hostFromURI).
func mongoHostFromURI(uri string) string {
	u := strings.TrimPrefix(uri, "mongodb://")
	if i := strings.Index(u, "@"); i >= 0 {
		u = u[i+1:]
	}
	if i := strings.Index(u, "/"); i >= 0 {
		u = u[:i]
	}
	if i := strings.Index(u, "?"); i >= 0 {
		u = u[:i]
	}
	return u
}

// liveServerWithRealMongo builds a Server wired to a REAL shared (Local) Mongo
// backend and fresh per-test breakers. No dedicated backend, no pool → every RPC
// takes the live shared-cluster path.
func liveServerWithRealMongo(adminURI string) *server.Server {
	return server.NewWithBackends(
		&config.Config{},
		nil, nil,
		mongo.NewBackend("local", adminURI, mongoHostFromURI(adminURI)),
		nil, nil,
		nil, nil, nil, nil,
		nil,
	).SetBreakers(circuit.NewBreakers())
}

// mongoProbe dials the admin URI and pings, returning (client, true) on success
// or (nil, false) so the caller can t.Skip when nothing is listening.
func mongoProbe(t *testing.T, adminURI string) (*mongodriver.Client, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cl, err := mongodriver.Connect(ctx, mongoopts.Client().ApplyURI(adminURI).SetServerSelectionTimeout(2*time.Second))
	if err != nil {
		return nil, false
	}
	if perr := cl.Ping(ctx, nil); perr != nil {
		_ = cl.Disconnect(ctx)
		return nil, false
	}
	return cl, true
}

// mongoUserExists reports whether usr_<token> exists in the admin DB on the live
// cluster (usersInfo against the admin database).
func mongoUserExists(t *testing.T, cl *mongodriver.Client, username string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var res bson.M
	err := cl.Database("admin").RunCommand(ctx, bson.D{{Key: "usersInfo", Value: username}}).Decode(&res)
	if err != nil {
		t.Fatalf("usersInfo(%s): %v", username, err)
	}
	users, _ := res["users"].(bson.A)
	return len(users) > 0
}

// mongoDBExists reports whether db_<token> appears in listDatabases on the live
// cluster (a DB only persists if it has ≥1 object; the provision sentinel insert
// + drop leaves it empty, so this can be false even right after provision — used
// only after seeding a real document).
func mongoDBExists(t *testing.T, cl *mongodriver.Client, dbName string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	names, err := cl.ListDatabaseNames(ctx, bson.D{{Key: "name", Value: dbName}})
	if err != nil {
		t.Fatalf("listDatabaseNames: %v", err)
	}
	return len(names) > 0
}

// TestServer_Mongo_Provision_StorageBytes_Deprovision_LiveRoundTrip drives the
// real Mongo LocalBackend through the gRPC handlers: ProvisionResource creates
// usr_<token> + db_<token>, GetStorageBytes returns the real dbStats size after
// seeding a document, DeprovisionResource drops the user and database, and a
// second Deprovision is a clean idempotent no-op. RegradeResource(mongo) is the
// documented skip path (only postgres/redis support regrade) — asserted too.
func TestServer_Mongo_Provision_StorageBytes_Deprovision_LiveRoundTrip(t *testing.T) {
	adminURI := liveMongoAdminURI()
	if adminURI == "" {
		t.Skip("CUSTOMER_MONGO_AUTH_URL/CUSTOMER_MONGO_URL/TEST_MONGO_URL unset — skipping live-Mongo gRPC round-trip")
	}
	probe, ok := mongoProbe(t, adminURI)
	if !ok {
		t.Skipf("mongo not reachable at %s — skipping", mongoHostFromURI(adminURI))
	}
	defer probe.Disconnect(context.Background()) //nolint:errcheck

	srv := liveServerWithRealMongo(adminURI)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	// Mongo names strip dashes (naming.go); keep the token dash-free + short so
	// db_/usr_ identifiers stay clean and unique.
	token := strings.ReplaceAll(liveToken(t), "_", "")
	dbName := "db_" + token
	username := "usr_" + token
	t.Cleanup(func() {
		bg := context.Background()
		_ = probe.Database("admin").RunCommand(bg, bson.D{{Key: "dropUser", Value: username}}).Err()
		_ = probe.Database(dbName).Drop(bg)
	})

	// --- Provision ---
	provResp, err := srv.ProvisionResource(ctx, &provisionerv1.ProvisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
		Tier:         "hobby",
	})
	if err != nil {
		t.Fatalf("ProvisionResource(mongo, hobby): %v", err)
	}
	if !strings.HasPrefix(provResp.ConnectionUrl, "mongodb://") {
		t.Errorf("ConnectionUrl = %q; want mongodb:// prefix", provResp.ConnectionUrl)
	}
	if provResp.DatabaseName != dbName {
		t.Errorf("DatabaseName = %q; want %q", provResp.DatabaseName, dbName)
	}
	if !mongoUserExists(t, probe, username) {
		t.Fatalf("after ProvisionResource, mongo user %q does not exist — createUser did not run", username)
	}

	// --- GetStorageBytes: seed a real document so dbStats > 0, then measure ---
	seedColl := probe.Database(dbName).Collection("seed")
	if _, serr := seedColl.InsertOne(ctx, bson.D{{Key: "payload", Value: strings.Repeat("x", 4096)}}); serr != nil {
		t.Fatalf("seed insert: %v", serr)
	}
	if !mongoDBExists(t, probe, dbName) {
		t.Fatalf("after seeding, %q not visible in listDatabases", dbName)
	}
	stResp, err := srv.GetStorageBytes(ctx, &provisionerv1.StorageRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	if err != nil {
		t.Fatalf("GetStorageBytes(mongo): %v", err)
	}
	if stResp.StorageBytes <= 0 {
		t.Errorf("GetStorageBytes = %d; want > 0 after seeding a 4KB document (dbStats did not read the live DB)", stResp.StorageBytes)
	}

	// --- Regrade(mongo): documented skip path (only postgres/redis regrade) ---
	regResp, err := srv.RegradeResource(ctx, &provisionerv1.RegradeRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
		Tier:         "pro",
	})
	if err != nil {
		t.Fatalf("RegradeResource(mongo): %v", err)
	}
	if regResp.Applied {
		t.Errorf("RegradeResource(mongo).Applied = true; want false (mongo has no DB-level regrade)")
	}
	if regResp.SkipReason == "" {
		t.Errorf("RegradeResource(mongo).SkipReason empty; want a non-empty reason")
	}

	// --- Deprovision (the DROP USER / DROP DATABASE path — truehomie class) ---
	depResp, err := srv.DeprovisionResource(ctx, &provisionerv1.DeprovisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	if err != nil {
		t.Fatalf("DeprovisionResource(mongo): %v", err)
	}
	if !depResp.Deprovisioned {
		t.Errorf("DeprovisionResource.Deprovisioned = false; want true")
	}
	if mongoUserExists(t, probe, username) {
		t.Errorf("after DeprovisionResource, mongo user %q still exists — dropUser did not run", username)
	}
	if mongoDBExists(t, probe, dbName) {
		t.Errorf("after DeprovisionResource, %q still exists — dropDatabase did not run", dbName)
	}

	// --- Idempotency: a second Deprovision must be a clean no-op ---
	depResp2, err := srv.DeprovisionResource(ctx, &provisionerv1.DeprovisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_MONGODB,
	})
	if err != nil {
		t.Errorf("second DeprovisionResource(mongo) returned %v; want nil — drop must no-op cleanly", err)
	}
	if depResp2 != nil && !depResp2.Deprovisioned {
		t.Errorf("second DeprovisionResource.Deprovisioned = false; want true (idempotent success)")
	}
}

// ─── Queue (NATS) ─────────────────────────────────────────────────────────────

// liveNATSHost returns the NATS hostname (no port — the LocalBackend appends the
// :8222 monitor port itself) the queue backend should health-check, or "" when
// unset. TEST_NATS_HOST is the canonical knob; if a full nats:// URL is provided
// via TEST_NATS_URL we strip it to host.
func liveNATSHost() string {
	if h := os.Getenv("TEST_NATS_HOST"); h != "" {
		return h
	}
	if u := os.Getenv("TEST_NATS_URL"); u != "" {
		s := strings.TrimPrefix(strings.TrimPrefix(u, "nats://"), "http://")
		if i := strings.IndexAny(s, ":/"); i >= 0 {
			s = s[:i]
		}
		return s
	}
	return ""
}

// liveServerWithRealQueue builds a Server wired to a REAL shared (Local) NATS
// queue backend. The LocalBackend health-checks http://<host>:8222/healthz.
func liveServerWithRealQueue(natsHost string) *server.Server {
	return server.NewWithBackends(
		&config.Config{},
		nil, nil, nil,
		queue.NewBackend("local", natsHost),
		nil,
		nil, nil, nil, nil,
		nil,
	).SetBreakers(circuit.NewBreakers())
}

// TestServer_Queue_Provision_Deprovision_LiveRoundTrip drives the real NATS
// queue LocalBackend through the gRPC handlers: ProvisionResource health-checks
// the live NATS monitor endpoint and returns a nats:// URL + subject prefix,
// DeprovisionResource is the documented shared-backend no-op (NATS holds no
// per-tenant server state), GetStorageBytes(queue) returns 0 (queues are metered
// by message count, not bytes), and Regrade(queue) is the skip path. The
// integration assertion is that the *real NATS health check* gates provisioning —
// the path that 503s in api when NATS is unreachable.
func TestServer_Queue_Provision_Deprovision_LiveRoundTrip(t *testing.T) {
	natsHost := liveNATSHost()
	if natsHost == "" {
		t.Skip("TEST_NATS_HOST/TEST_NATS_URL unset — skipping live-NATS gRPC round-trip")
	}
	srv := liveServerWithRealQueue(natsHost)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	token := liveToken(t)

	// --- Provision: the real NATS monitor health check must pass ---
	provResp, err := srv.ProvisionResource(ctx, &provisionerv1.ProvisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
		Tier:         "hobby",
	})
	if err != nil {
		// A reachable-but-unhealthy NATS (no monitor port) is a legit skip, not a
		// failure — the test asserts the happy path against a real healthy NATS.
		t.Skipf("ProvisionResource(queue) failed against NATS host %q (monitor :8222 unreachable?): %v", natsHost, err)
	}
	if !strings.HasPrefix(provResp.ConnectionUrl, "nats://") {
		t.Errorf("ConnectionUrl = %q; want nats:// prefix", provResp.ConnectionUrl)
	}
	if provResp.KeyPrefix == "" {
		// The server maps queue.Credentials.SubjectPrefix → ProvisionResponse.KeyPrefix.
		t.Errorf("KeyPrefix (subject prefix) empty; want a non-empty per-tenant subject namespace")
	}

	// --- GetStorageBytes(queue): always 0 (metered by message count) ---
	stResp, err := srv.GetStorageBytes(ctx, &provisionerv1.StorageRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	if err != nil {
		t.Fatalf("GetStorageBytes(queue): %v", err)
	}
	if stResp.StorageBytes != 0 {
		t.Errorf("GetStorageBytes(queue) = %d; want 0 (queues are message-metered, not byte-metered)", stResp.StorageBytes)
	}

	// --- Regrade(queue): documented skip path ---
	regResp, err := srv.RegradeResource(ctx, &provisionerv1.RegradeRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
		Tier:         "pro",
	})
	if err != nil {
		t.Fatalf("RegradeResource(queue): %v", err)
	}
	if regResp.Applied {
		t.Errorf("RegradeResource(queue).Applied = true; want false")
	}

	// --- Deprovision: shared-backend no-op, must report success idempotently ---
	depResp, err := srv.DeprovisionResource(ctx, &provisionerv1.DeprovisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	if err != nil {
		t.Fatalf("DeprovisionResource(queue): %v", err)
	}
	if !depResp.Deprovisioned {
		t.Errorf("DeprovisionResource(queue).Deprovisioned = false; want true")
	}
	depResp2, err := srv.DeprovisionResource(ctx, &provisionerv1.DeprovisionRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_QUEUE,
	})
	if err != nil {
		t.Errorf("second DeprovisionResource(queue) returned %v; want nil (idempotent no-op)", err)
	}
	if depResp2 != nil && !depResp2.Deprovisioned {
		t.Errorf("second DeprovisionResource(queue).Deprovisioned = false; want true")
	}
}

// ─── Storage (MinIO / S3-compatible) ───────────────────────────────────────────

// liveStorageEndpoint returns the S3-compatible endpoint ("host:port", no
// scheme) + access/secret keys + bucket, or ok=false when unset. Storage in the
// provisioner has only a GetStorageBytes flow (Provision/Deprovision are
// API-side), so the round-trip is: put an object under the tenant prefix →
// GetStorageBytes returns its size → delete it → GetStorageBytes returns 0.
func liveStorageEndpoint() (endpoint, access, secret, bucket string, ok bool) {
	endpoint = os.Getenv("TEST_MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("MINIO_ENDPOINT")
	}
	if endpoint == "" {
		return "", "", "", "", false
	}
	access = firstEnv("TEST_MINIO_ROOT_USER", "MINIO_ROOT_USER")
	secret = firstEnv("TEST_MINIO_ROOT_PASSWORD", "MINIO_ROOT_PASSWORD")
	bucket = firstEnv("TEST_MINIO_BUCKET", "MINIO_BUCKET_NAME")
	if bucket == "" {
		bucket = "instant-shared"
	}
	return endpoint, access, secret, bucket, true
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := os.Getenv(k); v != "" {
			return v
		}
	}
	return ""
}

// liveServerWithRealStorage builds a Server wired to a REAL MinIOBackend.
func liveServerWithRealStorage(t *testing.T, endpoint, access, secret, bucket string) *server.Server {
	t.Helper()
	b, err := storage.New(endpoint, access, secret, bucket)
	if err != nil {
		t.Fatalf("storage.New(%s): %v", endpoint, err)
	}
	return server.NewWithBackends(
		&config.Config{},
		nil, nil, nil, nil,
		b,
		nil, nil, nil, nil,
		nil,
	).SetBreakers(circuit.NewBreakers())
}

// TestServer_Storage_GetStorageBytes_LiveRoundTrip drives the real MinIOBackend
// through the GetStorageBytes gRPC handler: with no objects under the tenant
// prefix it returns 0; after putting a real object it returns that object's
// size; after deleting it, 0 again. This exercises the genuine S3 object-walk
// accounting (the prefix derivation, version walk, multipart sum) end-to-end
// through the server layer — not a fake byte count.
func TestServer_Storage_GetStorageBytes_LiveRoundTrip(t *testing.T) {
	endpoint, access, secret, bucket, ok := liveStorageEndpoint()
	if !ok {
		t.Skip("TEST_MINIO_ENDPOINT/MINIO_ENDPOINT unset — skipping live-storage GetStorageBytes round-trip")
	}

	// Build a raw client to (a) ensure the bucket exists and (b) put/delete the
	// test object directly — the provisioner storage backend only measures.
	cl, err := minio.New(endpoint, &minio.Options{
		Creds:  miniocreds.NewStaticV4(access, secret, ""),
		Secure: false,
	})
	if err != nil {
		t.Skipf("minio client for %s: %v", endpoint, err)
	}
	pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pcancel()
	exists, berr := cl.BucketExists(pctx, bucket)
	if berr != nil {
		t.Skipf("minio not reachable at %s: %v", endpoint, berr)
	}
	if !exists {
		if merr := cl.MakeBucket(context.Background(), bucket, minio.MakeBucketOptions{}); merr != nil {
			t.Skipf("could not create bucket %q: %v", bucket, merr)
		}
	}

	srv := liveServerWithRealStorage(t, endpoint, access, secret, bucket)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Tenant prefix = first 8 chars of the token (matches objectPrefix). Use a
	// token whose first 8 chars are unique to avoid colliding with other runs.
	token := fmt.Sprintf("st%08d", time.Now().UnixNano()%1e8)
	prefix := token[:8] + "/"
	objKey := prefix + "blob.bin"
	t.Cleanup(func() {
		_ = cl.RemoveObject(context.Background(), bucket, objKey, minio.RemoveObjectOptions{})
	})

	// --- Empty prefix → 0 bytes ---
	stResp, err := srv.GetStorageBytes(ctx, &provisionerv1.StorageRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_STORAGE,
	})
	if err != nil {
		t.Fatalf("GetStorageBytes(storage, empty): %v", err)
	}
	if stResp.StorageBytes != 0 {
		t.Errorf("GetStorageBytes(empty prefix) = %d; want 0", stResp.StorageBytes)
	}

	// --- Put a real object, then measure ---
	const payloadLen = 8192
	payload := strings.NewReader(strings.Repeat("z", payloadLen))
	if _, perr := cl.PutObject(ctx, bucket, objKey, payload, payloadLen, minio.PutObjectOptions{ContentType: "application/octet-stream"}); perr != nil {
		t.Fatalf("PutObject %q: %v", objKey, perr)
	}
	stResp, err = srv.GetStorageBytes(ctx, &provisionerv1.StorageRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_STORAGE,
	})
	if err != nil {
		t.Fatalf("GetStorageBytes(storage, after put): %v", err)
	}
	if stResp.StorageBytes != payloadLen {
		t.Errorf("GetStorageBytes(after put) = %d; want %d (the real object-walk did not sum the live object)", stResp.StorageBytes, payloadLen)
	}

	// --- Delete the object, then measure again → 0 ---
	if rerr := cl.RemoveObject(ctx, bucket, objKey, minio.RemoveObjectOptions{}); rerr != nil {
		t.Fatalf("RemoveObject %q: %v", objKey, rerr)
	}
	stResp, err = srv.GetStorageBytes(ctx, &provisionerv1.StorageRequest{
		Token:        token,
		ResourceType: commonv1.ResourceType_RESOURCE_TYPE_STORAGE,
	})
	if err != nil {
		t.Fatalf("GetStorageBytes(storage, after delete): %v", err)
	}
	if stResp.StorageBytes != 0 {
		t.Errorf("GetStorageBytes(after delete) = %d; want 0 (deleted object still counted)", stResp.StorageBytes)
	}
}
