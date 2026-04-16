package mongo

// Package mongo handles MongoDB database provisioning.
// Supports local MongoDB instances running in the k8s cluster.
// Each provisioned token gets its own database (db_{token}) and
// a dedicated user (usr_{token}) with readWrite access scoped to that DB only.

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
)

// connectTimeout is the maximum time to wait for a MongoDB server to be found.
// Short to fail-fast in tests and when MongoDB is not reachable.
const connectTimeout = 3 * time.Second

// LocalBackend provisions MongoDB databases on a local instance.
type LocalBackend struct {
	adminURI  string // admin connection URI, e.g. mongodb://root:root@localhost:27017
	mongoHost string // host for building connection strings, e.g. localhost:27017
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
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(b.adminURI).
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

	dbName := "db_" + token
	username := "usr_" + token

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
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(b.adminURI).
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

	dbName := "db_" + token
	var result bson.M
	err = client.Database(dbName).RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&result)
	if err != nil {
		// Database may not exist yet — fail open.
		slog.Error("nosql.StorageBytes: dbStats failed", "token", token, "db", dbName, "error", err)
		return 0, nil
	}

	switch v := result["storageSize"].(type) {
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		return int64(v), nil
	}

	return 0, nil
}

// Deprovision drops the user and database for the given token.
// Drops user first, then drops the database.
func (b *LocalBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(b.adminURI).
		SetServerSelectionTimeout(connectTimeout))
	if err != nil {
		return fmt.Errorf("nosql.Deprovision: connect: %w", err)
	}
	defer func() {
		if discErr := client.Disconnect(ctx); discErr != nil {
			slog.Error("nosql.Deprovision: disconnect", "error", discErr)
		}
	}()

	dbName := "db_" + token
	username := "usr_" + token

	// Drop the user from the admin database.
	adminDB := client.Database("admin")
	dropUserResult := adminDB.RunCommand(ctx, bson.D{{Key: "dropUser", Value: username}})
	if dropUserResult.Err() != nil {
		slog.Error("nosql.Deprovision: dropUser failed (continuing)", "token", token, "error", dropUserResult.Err())
	}

	// Drop the database.
	if dropErr := client.Database(dbName).Drop(ctx); dropErr != nil {
		return fmt.Errorf("nosql.Deprovision: drop database %s: %w", dbName, dropErr)
	}

	slog.Info("nosql.Deprovision: deprovisioned", "token", token, "db", dbName, "user", username)
	return nil
}
