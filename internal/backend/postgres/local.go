package postgres

// local.go — LocalBackend provisions Postgres databases on the shared postgres-customers pod.
// Connects via POSTGRES_CUSTOMERS_URL (default postgres://postgres:postgres@postgres-customers:5432/postgres).
// Each provisioned token gets its own database (db_{token}) and user (usr_{token}).
//
// Multi-cluster: when POSTGRES_CLUSTER_URLS is set to a comma-separated list of
// admin DSNs, the ClusterRouter selects the least-loaded cluster per provision.
// The cluster index is stored in providerResourceID ("local:0", "local:1", …)
// so that StorageBytes and Deprovision can reconnect to the right cluster.

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"math/big"
	"os"

	"github.com/jackc/pgx/v5"
)

const defaultCustomersURL = "postgres://instant_cust:instant_cust@postgres-customers:5432/instant_customers?sslmode=disable"

// alphanumChars is the charset for generated passwords.
const alphanumChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// LocalBackend provisions databases on one or more shared postgres-customers instances.
// When multiple admin URLs are provided, the ClusterRouter distributes provisions
// across them based on available capacity.
type LocalBackend struct {
	router *ClusterRouter
}

// newLocalBackend creates a LocalBackend using a single admin connection URL.
func newLocalBackend(customersURL string) *LocalBackend {
	if customersURL == "" {
		customersURL = defaultCustomersURL
	}
	return &LocalBackend{router: newClusterRouter([]string{customersURL}, 0)}
}

// newLocalBackendMulti creates a LocalBackend that distributes provisions across
// multiple shared Postgres clusters. adminURLs must be non-empty.
func newLocalBackendMulti(adminURLs []string) *LocalBackend {
	return &LocalBackend{router: newClusterRouter(adminURLs, 0)}
}

// Start begins background cluster-capacity polling. Implement the optional
// Starter interface so callers can kick off the poll loop without changing the
// Backend interface.
func (b *LocalBackend) Start(ctx context.Context) {
	b.router.Start(ctx)
}

// Shutdown stops the background polling goroutine.
func (b *LocalBackend) Shutdown() {
	b.router.Shutdown()
}

// generatePassword returns a cryptographically random alphanumeric string of length n.
func generatePassword(n int) (string, error) {
	buf := make([]byte, n)
	charsetLen := big.NewInt(int64(len(alphanumChars)))
	for i := range buf {
		idx, err := rand.Int(rand.Reader, charsetLen)
		if err != nil {
			return "", fmt.Errorf("generatePassword: %w", err)
		}
		buf[i] = alphanumChars[idx.Int64()]
	}
	return string(buf), nil
}

// Provision creates a Postgres database and user for the given token.
func (b *LocalBackend) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	dbName := "db_" + token
	username := "usr_" + token

	pass, err := generatePassword(16)
	if err != nil {
		return nil, fmt.Errorf("db.local.Provision: %w", err)
	}

	// Pick the least-loaded cluster for this provision.
	clusterIdx, adminURL, err := b.router.Pick()
	if err != nil {
		return nil, fmt.Errorf("db.local.Provision: pick cluster: %w", err)
	}

	// Connect as admin.
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return nil, fmt.Errorf("db.local.Provision: connect: %w", err)
	}
	defer func() {
		if discErr := conn.Close(ctx); discErr != nil {
			slog.Error("db.local.Provision: disconnect", "error", discErr)
		}
	}()

	// CREATE DATABASE — identifiers cannot be parameterised.
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", dbName)); err != nil {
		return nil, fmt.Errorf("db.local.Provision: CREATE DATABASE: %w", err)
	}

	// CREATE USER with password.
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE USER %q WITH PASSWORD '%s'", username, pass)); err != nil {
		return nil, fmt.Errorf("db.local.Provision: CREATE USER: %w", err)
	}

	// REVOKE CONNECT from PUBLIC so only the provisioned user can connect.
	// PostgreSQL grants CONNECT to PUBLIC by default; without this, any role
	// that knows the password of another user could connect to their database.
	if _, err := conn.Exec(ctx, fmt.Sprintf("REVOKE CONNECT ON DATABASE %q FROM PUBLIC", dbName)); err != nil {
		slog.Error("db.local.Provision: REVOKE CONNECT (non-fatal)", "token", token, "error", err)
	}

	// GRANT ALL PRIVILEGES ON DATABASE to the new user.
	if _, err := conn.Exec(ctx, fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %q TO %q", dbName, username)); err != nil {
		return nil, fmt.Errorf("db.local.Provision: GRANT DATABASE: %w", err)
	}

	// Connect to the new database to grant schema privileges.
	// Build the new DB URL by substituting the database name in the admin URL.
	newDBURL := buildDBURL(adminURL, username, pass, dbName)
	adminNewDB, err := pgx.Connect(ctx, buildAdminNewDBURL(adminURL, dbName))
	if err != nil {
		slog.Error("db.local.Provision: connect new db for schema grant (non-fatal)", "error", err)
	} else {
		defer func() {
			if discErr := adminNewDB.Close(ctx); discErr != nil {
				slog.Error("db.local.Provision: disconnect new db", "error", discErr)
			}
		}()
		if _, err := adminNewDB.Exec(ctx, fmt.Sprintf("GRANT ALL ON SCHEMA public TO %q", username)); err != nil {
			slog.Error("db.local.Provision: GRANT SCHEMA (non-fatal)", "token", token, "error", err)
		}
		// Install pgvector so users can create vector columns immediately.
		if _, err := adminNewDB.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS vector"); err != nil {
			slog.Error("db.local.Provision: CREATE EXTENSION vector (non-fatal)", "token", token, "error", err)
		}
	}

	slog.Info("db.local.Provision: provisioned",
		"token", token,
		"db", dbName,
		"user", username,
		"tier", tier,
	)

	return &Credentials{
		URL:                newDBURL,
		DatabaseName:       dbName,
		Username:           username,
		ProviderResourceID: b.router.ProviderResourceID(clusterIdx),
	}, nil
}

// StorageBytes returns the size of db_{token} in bytes using pg_database_size.
func (b *LocalBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	adminURL := b.router.AdminURLForResource(providerResourceID)
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return 0, fmt.Errorf("db.local.StorageBytes: connect: %w", err)
	}
	defer func() {
		if discErr := conn.Close(ctx); discErr != nil {
			slog.Error("db.local.StorageBytes: disconnect", "error", discErr)
		}
	}()

	dbName := "db_" + token
	var size int64
	if err := conn.QueryRow(ctx, "SELECT pg_database_size($1)", dbName).Scan(&size); err != nil {
		return 0, fmt.Errorf("db.local.StorageBytes: pg_database_size: %w", err)
	}
	return size, nil
}

// Deprovision terminates active connections, drops the database and user for the token.
func (b *LocalBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	dbName := "db_" + token
	username := "usr_" + token

	adminURL := b.router.AdminURLForResource(providerResourceID)
	conn, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		return fmt.Errorf("db.local.Deprovision: connect: %w", err)
	}
	defer func() {
		if discErr := conn.Close(ctx); discErr != nil {
			slog.Error("db.local.Deprovision: disconnect", "error", discErr)
		}
	}()

	// Terminate active connections before dropping.
	_, err = conn.Exec(ctx,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()",
		dbName,
	)
	if err != nil {
		slog.Error("db.local.Deprovision: terminate connections (continuing)", "token", token, "error", err)
	}

	// DROP DATABASE IF EXISTS.
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %q", dbName)); err != nil {
		return fmt.Errorf("db.local.Deprovision: DROP DATABASE: %w", err)
	}

	// DROP USER IF EXISTS.
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP USER IF EXISTS %q", username)); err != nil {
		slog.Error("db.local.Deprovision: DROP USER (continuing)", "token", token, "error", err)
	}

	slog.Info("db.local.Deprovision: deprovisioned", "token", token, "db", dbName, "user", username)
	return nil
}

// buildDBURL constructs the user-facing connection URL for the provisioned database.
// sslmode=disable is explicit because the shared postgres-customers cluster does not
// have SSL configured. Without it, lib/pq defaults to sslmode=prefer and fails with
// "SSL is not enabled on the server" when the migrator's Verify step connects.
//
// Host resolution order:
//  1. POSTGRES_PUBLIC_HOST_PORT (e.g. "pg.instanode.dev:5432") — explicit override
//  2. POSTGRES_PUBLIC_HOST + POSTGRES_PUBLIC_PORT (port defaults to 5432)
//  3. host extracted from adminURL (cluster-internal — only useful from inside the cluster)
//
// The returned URL is what clients use to connect. Admin operations (CREATE DATABASE,
// CREATE USER) still use adminURL directly via pgx.Connect — those run from inside the
// cluster against the in-cluster postgres-customers Service.
func buildDBURL(adminURL, username, password, dbName string) string {
	host := publicHostPort()
	if host == "" {
		host = extractHost(adminURL)
	}
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", username, password, host, dbName)
}

// publicHostPort returns the host:port that should be embedded in user-facing
// connection URLs, or "" if no public host is configured (caller falls back to
// the cluster-internal host from the admin URL).
func publicHostPort() string {
	if hp := os.Getenv("POSTGRES_PUBLIC_HOST_PORT"); hp != "" {
		return hp
	}
	host := os.Getenv("POSTGRES_PUBLIC_HOST")
	if host == "" {
		return ""
	}
	port := os.Getenv("POSTGRES_PUBLIC_PORT")
	if port == "" {
		port = "5432"
	}
	return host + ":" + port
}

// buildAdminNewDBURL builds an admin connection URL targeting a specific database.
func buildAdminNewDBURL(adminURL, dbName string) string {
	for i := len(adminURL) - 1; i >= 0; i-- {
		if adminURL[i] == '/' {
			return adminURL[:i+1] + dbName
		}
	}
	return adminURL + "/" + dbName
}

// extractHost returns the host:port portion of a postgres:// URL.
func extractHost(rawURL string) string {
	// postgres://user:pass@host:port/db  or  postgres://user:pass@host/db
	// Find "@" then take up to the next "/".
	const prefix = "postgres://"
	s := rawURL
	if len(s) > len(prefix) {
		s = s[len(prefix):]
	}
	// skip user:pass@
	if at := indexOf(s, '@'); at >= 0 {
		s = s[at+1:]
	}
	// take up to first "/"
	if slash := indexOf(s, '/'); slash >= 0 {
		return s[:slash]
	}
	return s
}

func indexOf(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
