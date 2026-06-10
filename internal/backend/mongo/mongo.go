package mongo

// Package mongo handles MongoDB database provisioning.
// Supports local MongoDB instances running in the k8s cluster.
// Each provisioned token gets its own database and a dedicated user with
// readWrite access scoped to that DB only. DB/user names are derived from the
// token by naming.go — the canonical scheme is db_{token}/usr_{token} with the
// token's dashes stripped (collision-free; see naming.go for the full rationale
// and the legacy-scheme backward-compatibility fallback).

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"instant.dev/provisioner/internal/dropguard"
	"instant.dev/provisioner/internal/poolident"
)

// connectTimeout is the maximum time to wait for a MongoDB server to be found.
// Short to fail-fast in tests and when MongoDB is not reachable.
const connectTimeout = 3 * time.Second

// decodeStorageSize extracts the dbStats storageSize from a decoded result,
// tolerating every BSON numeric encoding the server may use across versions
// (int32 / int64 / float64). Any missing or non-numeric value yields 0 — the
// fail-open contract for the quota scanner. Extracted as a standalone helper so
// the per-type decode arms are unit-testable without a live server returning a
// specific BSON type (real dbStats emits float64, leaving the integer arms
// otherwise unexercised).
func decodeStorageSize(result bson.M) int64 {
	switch v := result["storageSize"].(type) {
	case int32:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	default:
		return 0
	}
}

// LocalBackend provisions MongoDB databases on a local instance.
type LocalBackend struct {
	adminURI  string // admin connection URI, e.g. mongodb://root:root@localhost:27017
	mongoHost string // host for building connection strings, e.g. localhost:27017

	// connectFn is the seam between the LocalBackend methods and
	// mongo.Connect, mirroring K8sBackend.initMongoFn. The real mongo driver
	// almost never returns an error from Connect itself (a malformed URI
	// surfaces lazily on the first RunCommand), so the connect-error branches
	// are otherwise unreachable in tests; substituting connectFn lets a test
	// drive those branches deterministically. Never overridden in prod paths.
	connectFn func(ctx context.Context, opts ...*options.ClientOptions) (*mongo.Client, error)
}

// connect dials Mongo via the (overridable) connectFn seam, defaulting to the
// real mongo.Connect.
func (b *LocalBackend) connect(ctx context.Context, opts ...*options.ClientOptions) (*mongo.Client, error) {
	if b.connectFn != nil {
		return b.connectFn(ctx, opts...)
	}
	return mongo.Connect(ctx, opts...)
}

// newLocalBackend creates a LocalBackend.
func newLocalBackend(adminURI, mongoHost string) *LocalBackend {
	if adminURI == "" {
		adminURI = "mongodb://root:root@localhost:27017"
	}
	if mongoHost == "" {
		mongoHost = "localhost:27017"
	}
	return &LocalBackend{adminURI: adminURI, mongoHost: mongoHost}
}

// Provision creates a MongoDB database and user for the given token.
// Database: db_{token}
// User: usr_{token} with readWrite role scoped to db_{token}
// Returns credentials the caller can use immediately.
func (b *LocalBackend) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	client, err := b.connect(ctx, options.Client().ApplyURI(b.adminURI).
		SetServerSelectionTimeout(connectTimeout))
	if err != nil {
		return nil, fmt.Errorf("nosql.Provision: connect: %w", err)
	}
	defer func() {
		if discErr := client.Disconnect(ctx); discErr != nil {
			slog.Error("nosql.Provision: disconnect", "error", discErr)
		}
	}()

	// Generate random 16-byte password.
	pwBytes := make([]byte, 16)
	if _, err := rand.Read(pwBytes); err != nil {
		return nil, fmt.Errorf("nosql.Provision: generate password: %w", err)
	}
	password := hex.EncodeToString(pwBytes)

	// Canonical, collision-free names (see naming.go). New provisions ALWAYS
	// use the canonical scheme; lookup paths below additionally tolerate the
	// legacy schemes for databases provisioned before this fix.
	dbName := mongoDBName(token)
	username := mongoUserName(token)

	// Create the user in the admin database with readWrite role scoped to the token DB.
	adminDB := client.Database("admin")
	result := adminDB.RunCommand(ctx, bson.D{
		{Key: "createUser", Value: username},
		{Key: "pwd", Value: password},
		{Key: "roles", Value: bson.A{
			bson.D{
				{Key: "role", Value: "readWrite"},
				{Key: "db", Value: dbName},
			},
		}},
	})
	if result.Err() != nil {
		return nil, fmt.Errorf("nosql.Provision: createUser: %w", result.Err())
	}

	// MongoDB creates the database implicitly on first insert. We insert and delete
	// a sentinel document to ensure the database exists and the user has access.
	tokenDB := client.Database(dbName)
	coll := tokenDB.Collection("_init")
	_, insertErr := coll.InsertOne(ctx, bson.D{{Key: "init", Value: true}})
	if insertErr != nil {
		slog.Error("nosql.Provision: init insert failed (non-fatal)", "token", token, "error", insertErr)
	} else {
		// Clean up the sentinel document — the DB will persist even when empty.
		_ = coll.Drop(ctx)
	}

	// User is created in the admin database; include authSource so clients authenticate correctly.
	url := fmt.Sprintf("mongodb://%s:%s@%s/%s?authSource=admin", username, password, b.mongoHost, dbName)
	slog.Info("nosql.Provision: provisioned",
		"token", token,
		"db", dbName,
		"user", username,
		"tier", tier,
	)

	return &Credentials{
		URL:          url,
		DatabaseName: dbName,
	}, nil
}

// StorageBytes returns the storage size in bytes used by db_{token}.
// Runs dbStats on the token database. Returns 0 on any error (fail-open).
func (b *LocalBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	client, err := b.connect(ctx, options.Client().ApplyURI(b.adminURI).
		SetServerSelectionTimeout(connectTimeout))
	if err != nil {
		slog.Error("nosql.StorageBytes: connect", "token", token, "error", err)
		return 0, nil
	}
	defer func() {
		if discErr := client.Disconnect(ctx); discErr != nil {
			slog.Error("nosql.StorageBytes: disconnect", "error", discErr)
		}
	}()

	// P0-2: a pool-claimed database is named from the pool token (db_pool-<uuid>),
	// not the request token. poolident.NamingToken resolves the canonical naming
	// token from provider_resource_id so quota is measured against the real
	// database; without it dbStats would miss and silently report 0 bytes.
	namingToken := poolident.NamingToken(token, providerResourceID)

	// Try the canonical name first, then every legacy scheme. A database
	// provisioned before the P0-5 naming fix lives under a legacy name; if we
	// only probed the canonical name we would read 0 bytes and silently
	// un-enforce its quota. The first DB whose dbStats succeeds wins.
	for _, dbName := range legacyMongoDBNames(namingToken) {
		var result bson.M
		err = client.Database(dbName).RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&result)
		if err != nil {
			slog.Debug("nosql.StorageBytes: dbStats miss for candidate", "token", token, "db", dbName, "error", err)
			continue
		}
		return decodeStorageSize(result), nil
	}

	// No candidate database exists yet — fail open.
	slog.Error("nosql.StorageBytes: dbStats failed for all name candidates", "token", token)
	return 0, nil
}

// Deprovision drops the user and database for the given token.
// Drops user first, then drops the database.
func (b *LocalBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	// P0-2: a pool-claimed database/user are named from the pool token, not
	// the request token. Resolve the canonical naming token from
	// provider_resource_id so dropUser/dropDatabase target the real infra
	// instead of no-op'ing on db_<real-token>, which would leak it forever.
	namingToken := poolident.NamingToken(token, providerResourceID)

	// Name-convention guard (truehomie hardening, task D3): every candidate
	// user/database name below derives from this token; refuse the whole
	// deprovision — before even connecting — when the token is empty,
	// malformed, or a reserved system identifier. Per-candidate names are
	// additionally validated in the loops so a future naming.go refactor
	// stays covered.
	if guardErr := dropguard.CheckNamingToken(namingToken); guardErr != nil {
		slog.Error("provisioner.drop.refused",
			"event", "provisioner.drop.refused", "site", "nosql.Deprovision",
			"token", token, "naming_token", namingToken, "error", guardErr)
		return fmt.Errorf("nosql.Deprovision: %w", guardErr)
	}

	client, err := b.connect(ctx, options.Client().ApplyURI(b.adminURI).
		SetServerSelectionTimeout(connectTimeout))
	if err != nil {
		return fmt.Errorf("nosql.Deprovision: connect: %w", err)
	}
	defer func() {
		if discErr := client.Disconnect(ctx); discErr != nil {
			slog.Error("nosql.Deprovision: disconnect", "error", discErr)
		}
	}()

	// Drop every candidate user. The resource was provisioned under exactly
	// one scheme, but we cannot know which without a stored name, so we drop
	// the canonical name and every legacy form. dropUser on a non-existent
	// user is harmless (logged, non-fatal).
	adminDB := client.Database("admin")
	for _, username := range legacyMongoUserNames(namingToken) {
		if guardErr := dropguard.CheckUserName(username); guardErr != nil {
			slog.Error("provisioner.drop.refused",
				"event", "provisioner.drop.refused", "site", "nosql.Deprovision",
				"token", token, "user", username, "error", guardErr)
			continue
		}
		if r := adminDB.RunCommand(ctx, bson.D{{Key: "dropUser", Value: username}}); r.Err() != nil {
			slog.Debug("nosql.Deprovision: dropUser miss (continuing)", "token", token, "user", username, "error", r.Err())
		}
	}

	// Drop every candidate database. Dropping a non-existent database is a
	// MongoDB no-op, so iterating all schemes is safe; a real drop error on
	// the canonical name still propagates.
	canonicalDB := mongoDBName(namingToken)
	for _, dbName := range legacyMongoDBNames(namingToken) {
		if guardErr := dropguard.CheckDatabaseName(dbName); guardErr != nil {
			slog.Error("provisioner.drop.refused",
				"event", "provisioner.drop.refused", "site", "nosql.Deprovision",
				"token", token, "db", dbName, "error", guardErr)
			if dbName == canonicalDB {
				return fmt.Errorf("nosql.Deprovision: %w", guardErr)
			}
			continue
		}
		if dropErr := client.Database(dbName).Drop(ctx); dropErr != nil {
			if dbName == canonicalDB {
				return fmt.Errorf("nosql.Deprovision: drop database %s: %w", dbName, dropErr)
			}
			slog.Debug("nosql.Deprovision: legacy drop miss (continuing)", "token", token, "db", dbName, "error", dropErr)
		}
	}

	slog.Info("nosql.Deprovision: deprovisioned", "token", token, "db", canonicalDB)
	return nil
}
