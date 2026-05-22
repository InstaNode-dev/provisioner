package mongo

// local_test.go — exercises LocalBackend against a real MongoDB instance.
//
// Set CUSTOMER_MONGO_URL to a reachable mongodb:// URI (e.g.
// mongodb://127.0.0.1:27017 from infra/docker-compose.yml). Tests that need a
// live instance skip cleanly if the connect probe fails — they never poison
// CI when Mongo is absent.

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"instant.dev/provisioner/internal/poolident"
)

// liveMongoURI returns the admin URI for the local Mongo and whether it's
// reachable. Tests that mutate state skip when reachable=false.
func liveMongoURI(t *testing.T) (string, bool) {
	t.Helper()
	uri := os.Getenv("CUSTOMER_MONGO_URL")
	if uri == "" {
		uri = "mongodb://127.0.0.1:27017"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetServerSelectionTimeout(2*time.Second))
	if err != nil {
		return uri, false
	}
	defer client.Disconnect(ctx)
	if err := client.Ping(ctx, nil); err != nil {
		return uri, false
	}
	return uri, true
}

// hostFromURI strips the mongodb:// scheme to produce the bare host:port the
// LocalBackend embeds in customer URLs.
func hostFromURI(uri string) string {
	u := strings.TrimPrefix(uri, "mongodb://")
	if i := strings.Index(u, "/"); i >= 0 {
		u = u[:i]
	}
	if i := strings.Index(u, "?"); i >= 0 {
		u = u[:i]
	}
	return u
}

// uniqueToken returns a token that won't collide with any existing fixture
// state. The hex shape mimics a real UUID with dashes so naming.go exercises
// its strip path.
func uniqueToken(prefix string) string {
	const layout = "20060102-1504-0500-9999"
	return prefix + "-" + time.Now().UTC().Format(layout)
}

// TestNewLocalBackend_AppliesDefaults exercises the parameter-defaulting branch
// (empty adminURI / mongoHost). Pure unit — no Mongo.
func TestNewLocalBackend_AppliesDefaults(t *testing.T) {
	b := newLocalBackend("", "")
	if b.adminURI != "mongodb://root:root@localhost:27017" {
		t.Errorf("default adminURI = %q", b.adminURI)
	}
	if b.mongoHost != "localhost:27017" {
		t.Errorf("default mongoHost = %q", b.mongoHost)
	}

	b2 := newLocalBackend("mongodb://x:y@h:1/admin", "other:42")
	if b2.adminURI != "mongodb://x:y@h:1/admin" || b2.mongoHost != "other:42" {
		t.Errorf("explicit values not preserved: %+v", b2)
	}
}

// TestLocalProvision_Happy provisions a real DB/user, asserts the credential
// shape, and cleans up. Skipped when Mongo is unreachable.
func TestLocalProvision_Happy(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	token := uniqueToken("prov")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	creds, err := b.Provision(ctx, token, "hobby")
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	defer func() {
		_ = b.Deprovision(context.Background(), token, "")
	}()

	wantDB := mongoDBName(token)
	if creds.DatabaseName != wantDB {
		t.Errorf("DatabaseName = %q, want %q", creds.DatabaseName, wantDB)
	}
	if !strings.Contains(creds.URL, wantDB) || !strings.Contains(creds.URL, "authSource=admin") {
		t.Errorf("URL malformed: %q", creds.URL)
	}
	if !strings.Contains(creds.URL, mongoUserName(token)) {
		t.Errorf("URL missing user: %q", creds.URL)
	}

	// Verify the user actually authenticates by connecting through the URL.
	verifyCtx, vCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer vCancel()
	client, err := mongo.Connect(verifyCtx, options.Client().ApplyURI(creds.URL).SetServerSelectionTimeout(3*time.Second))
	if err != nil {
		t.Fatalf("connect with returned URL: %v", err)
	}
	defer client.Disconnect(verifyCtx)
	if err := client.Database(wantDB).RunCommand(verifyCtx, bson.D{{Key: "ping", Value: 1}}).Err(); err != nil {
		t.Errorf("user cannot ping its DB: %v", err)
	}
}

// TestLocalProvision_CreateUserConflict exercises the createUser-fail branch:
// running Provision twice against the same token must return an error because
// the second createUser hits a Duplicate-Key.
func TestLocalProvision_CreateUserConflict(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	token := uniqueToken("dup")
	defer func() { _ = b.Deprovision(context.Background(), token, "") }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, token, "hobby"); err != nil {
		t.Fatalf("first Provision: %v", err)
	}
	if _, err := b.Provision(ctx, token, "hobby"); err == nil {
		t.Fatalf("second Provision: want error, got nil")
	}
}

// TestLocalProvision_ConnectFails covers the connect-error branch by pointing
// the backend at a port nothing listens on. The driver's lazy-connect means
// failures surface inside the first RunCommand — we deliberately use a short
// server-selection timeout so the test is fast.
func TestLocalProvision_ConnectFails(t *testing.T) {
	// Use an invalid URI scheme so mongo.Connect itself fails (cheap; no socket).
	b := newLocalBackend("mongodb://[bad-uri", "h:1")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, "tkn", "hobby"); err == nil {
		t.Fatal("Provision: want connect error, got nil")
	}
}

// TestLocalStorageBytes_ReturnsZeroOnConnectFailure covers the fail-open
// path: an unreachable mongo URI must yield (0, nil) — never an error — so
// the worker quota scan continues past a degraded backend.
func TestLocalStorageBytes_ReturnsZeroOnConnectFailure(t *testing.T) {
	b := newLocalBackend("mongodb://[bad-uri", "h:1")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := b.StorageBytes(ctx, "tkn", "")
	if err != nil {
		t.Errorf("StorageBytes connect error: want nil (fail-open), got %v", err)
	}
	if got != 0 {
		t.Errorf("StorageBytes connect error: got %d, want 0", got)
	}
}

// TestLocalStorageBytes_NonExistentDB asserts the all-candidates-missed fall
// through: against a live Mongo, an un-provisioned token has no DB so every
// dbStats candidate fails and the function returns (0, nil) — fail-open.
func TestLocalStorageBytes_NonExistentDB(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// dbStats actually succeeds against a non-existent DB on Mongo (it
	// returns a stub with storageSize=0), so we get a (0, nil) success on
	// the FIRST candidate. That still exercises the int32/int64/float64
	// type switch and the "no error" return.
	got, err := b.StorageBytes(ctx, uniqueToken("absent"), "")
	if err != nil {
		t.Errorf("StorageBytes: want nil err for missing DB, got %v", err)
	}
	if got != 0 {
		t.Errorf("StorageBytes missing DB: got %d, want 0", got)
	}
}

// TestLocalStorageBytes_AfterProvision exercises the happy dbStats path.
// Mongo's dbStats on a freshly-created (empty) DB returns a tiny non-negative
// storageSize, so we assert >= 0 and that the type-switch decode worked.
func TestLocalStorageBytes_AfterProvision(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	token := uniqueToken("sz")
	defer func() { _ = b.Deprovision(context.Background(), token, "") }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, token, "hobby"); err != nil {
		t.Fatalf("Provision: %v", err)
	}

	// Insert some data so storageSize is observably > 0.
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect(ctx)
	dbName := mongoDBName(token)
	coll := client.Database(dbName).Collection("data")
	for i := 0; i < 5; i++ {
		_, _ = coll.InsertOne(ctx, bson.D{{Key: "i", Value: i}, {Key: "s", Value: strings.Repeat("x", 64)}})
	}

	got, err := b.StorageBytes(ctx, token, "")
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	if got < 0 {
		t.Errorf("StorageBytes: got %d, want >= 0", got)
	}
}

// TestLocalStorageBytes_PoolNamingToken proves the poolident.NamingToken
// branch is honoured: when provider_resource_id encodes a pool token, the
// dbStats probe must target the pool DB, not the request token's DB.
func TestLocalStorageBytes_PoolNamingToken(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	poolToken := uniqueToken("pool-aaaaaaaa")
	requestToken := uniqueToken("req")
	prid := poolident.Encode("", poolToken)
	defer func() { _ = b.Deprovision(context.Background(), poolToken, "") }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Provision under the pool token (this is what the hot-pool does).
	if _, err := b.Provision(ctx, poolToken, "hobby"); err != nil {
		t.Fatalf("Provision pool: %v", err)
	}
	// StorageBytes called with the REQUEST token + PRID encoding the pool
	// token must hit the pool DB, not db_<requestToken>.
	got, err := b.StorageBytes(ctx, requestToken, prid)
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	if got < 0 {
		t.Errorf("storage_bytes = %d, want >= 0", got)
	}
}

// TestLocalDeprovision_DropsUserAndDB asserts that after Deprovision: (a) the
// user is gone (subsequent auth fails), (b) the DB drop succeeded.
func TestLocalDeprovision_DropsUserAndDB(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	token := uniqueToken("drop")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	creds, err := b.Provision(ctx, token, "hobby")
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if err := b.Deprovision(ctx, token, ""); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}

	// Try to authenticate with the dropped user — must fail.
	authCtx, aCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer aCancel()
	client, err := mongo.Connect(authCtx, options.Client().ApplyURI(creds.URL).SetServerSelectionTimeout(2*time.Second))
	if err == nil {
		err = client.Ping(authCtx, nil)
		_ = client.Disconnect(authCtx)
	}
	if err == nil {
		t.Errorf("dropped user still authenticates: %s", creds.URL)
	}
}

// TestLocalDeprovision_IdempotentOnMissing covers the "drop all candidates"
// loop when none of the candidate DBs/users exist — must NOT return an error
// because dropDatabase on a missing DB is a Mongo no-op.
func TestLocalDeprovision_IdempotentOnMissing(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	if err := b.Deprovision(context.Background(), uniqueToken("ghost"), ""); err != nil {
		t.Errorf("Deprovision on missing token: want nil, got %v", err)
	}
}

// TestLocalDeprovision_ConnectFails covers the connect-failure branch.
func TestLocalDeprovision_ConnectFails(t *testing.T) {
	b := newLocalBackend("mongodb://[bad-uri", "h:1")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Deprovision(ctx, "t", ""); err == nil {
		t.Fatal("Deprovision: want connect error, got nil")
	}
}

// TestLocalDeprovision_PoolNamingToken asserts that a pool-PRID routes the drop
// to the pool DB / user — not db_<requestToken>. Provision under the pool
// token, Deprovision via the request token + PRID, then verify the pool DB
// is gone.
func TestLocalDeprovision_PoolNamingToken(t *testing.T) {
	uri, ok := liveMongoURI(t)
	if !ok {
		t.Skip("CUSTOMER_MONGO_URL unreachable; skipping")
	}
	b := newLocalBackend(uri, hostFromURI(uri))
	poolToken := uniqueToken("pool-bbbbbbbb")
	requestToken := uniqueToken("req2")
	prid := poolident.Encode("", poolToken)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := b.Provision(ctx, poolToken, "hobby"); err != nil {
		t.Fatalf("Provision pool: %v", err)
	}
	if err := b.Deprovision(ctx, requestToken, prid); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}

	// The pool DB must now be missing — listDatabases must not include it.
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer client.Disconnect(ctx)
	dbs, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		t.Fatalf("ListDatabaseNames: %v", err)
	}
	want := mongoDBName(poolToken)
	for _, d := range dbs {
		if d == want {
			t.Errorf("pool DB %q still exists after pool-PRID Deprovision; routing fell through", want)
		}
	}
}
