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
	"strings"
	"time"

	"instant.dev/provisioner/internal/poolident"
)

const (
	// dbNamePrefix / userNamePrefix are the fixed prefixes for a customer
	// database and its scoped role on the shared cluster.
	dbNamePrefix   = "db_"
	userNamePrefix = "usr_"
)

const defaultCustomersURL = "postgres://instant_cust:instant_cust@postgres-customers:5432/instant_customers?sslmode=disable"

// alphanumChars is the charset for generated passwords.
const alphanumChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// sqlCreateExtensionVector installs pgvector on a freshly provisioned database.
// Applied unconditionally — see the call site in Provision for the rationale.
const sqlCreateExtensionVector = "CREATE EXTENSION IF NOT EXISTS vector"

// deprovisionDropDBAttempts is the number of DROP DATABASE tries in Deprovision.
// A concurrent connection that reconnects between pg_terminate_backend and
// DROP DATABASE produces "is being accessed by other users"; a short retry loop
// rides out that TOCTOU window. WITH (FORCE) (Postgres 13+) terminates stragglers
// itself, so one or two attempts is normally enough.
const deprovisionDropDBAttempts = 3

// deprovisionDropDBRetryDelay is the pause between DROP DATABASE attempts. It is
// a package var (not a const) only so tests can shrink it to avoid a real
// 500ms*N wait while still exercising the retry loop. Production value unchanged.
var deprovisionDropDBRetryDelay = 500 * time.Millisecond

// pgDatabaseInUseMarker is the Postgres error-message fragment for SQLSTATE
// 55006 (object_in_use) raised by DROP DATABASE when a backend is still
// connected. It is the ONLY DROP DATABASE failure Deprovision retries.
const pgDatabaseInUseMarker = "being accessed by other users"

// isDatabaseInUseErr reports whether a DROP DATABASE error is the transient
// "database is being accessed by other users" race that the Deprovision retry
// loop should ride out. Any other error is terminal.
func isDatabaseInUseErr(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), pgDatabaseInUseMarker)
}

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
// multiple shared Postgres clusters. An empty adminURLs slice would leave the
// ClusterRouter with no clusters — every Pick / AdminURLForResource would then
// have to fail soft — so it falls back to the single default customers URL,
// matching newLocalBackend's behaviour.
func newLocalBackendMulti(adminURLs []string) *LocalBackend {
	if len(adminURLs) == 0 {
		adminURLs = []string{defaultCustomersURL}
	}
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
		idx, err := randInt(rand.Reader, charsetLen)
		if err != nil {
			return "", fmt.Errorf("generatePassword: %w", err)
		}
		buf[i] = alphanumChars[idx.Int64()]
	}
	return string(buf), nil
}

// Provision creates a Postgres database and user for the given token.
// connLimit is the CONNECTION LIMIT to apply to the role; -1 means unlimited
// (omits the clause). This value comes from the API handler via plans.Registry
// so the provisioner stays a dumb executor and the API remains the policy owner.
func (b *LocalBackend) Provision(ctx context.Context, token, tier string, connLimit int) (*Credentials, error) {
	dbName := dbNamePrefix + token
	username := userNamePrefix + token

	pass, err := generatePassword(16)
	if err != nil {
		return nil, fmt.Errorf("db.local.Provision: %w", err)
	}

	// Pick the least-loaded cluster for this provision. Pick increments the
	// cluster's in-flight count so concurrent provisions between two 60s
	// capacity polls don't all stampede the same cluster; ReleasePick (deferred)
	// decrements it once this provision settles, success or failure.
	clusterIdx, adminURL, err := b.router.Pick()
	if err != nil {
		return nil, fmt.Errorf("db.local.Provision: pick cluster: %w", err)
	}
	defer b.router.ReleasePick(clusterIdx)

	// Connect as admin.
	conn, err := pgxConnect(ctx, adminURL)
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

	// CREATE USER with password and optional CONNECTION LIMIT.
	// connLimit <= 0 means unlimited — omit the clause so Postgres uses its
	// default of -1 (unlimited). We only apply the cap when connLimit > 0.
	connLimitClause := ""
	if connLimit > 0 {
		connLimitClause = fmt.Sprintf(" CONNECTION LIMIT %d", connLimit)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE USER %q WITH PASSWORD '%s'%s", username, pass, connLimitClause)); err != nil {
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
	adminNewDB, err := pgxConnect(ctx, buildAdminNewDBURL(adminURL, dbName))
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
		// Install pgvector unconditionally on every provisioned database.
		//
		// This is intentional, not request-driven. The gRPC ProvisionRequest
		// proto carries no extensions field, so the provisioner cannot know
		// which extensions a caller asked for — and `vector` is the *only*
		// extension the platform allows anyway (see the API's ValidateExtensions
		// allowlist). Installing it for everyone means an agent can create
		// vector columns immediately without a second round-trip, at the cost
		// of one always-present extension.
		//
		// Consequence: the API-side ValidateExtensions allowlist is effectively
		// cosmetic on this (production gRPC) path — it only gated the retired
		// API-local backend. If extension selection ever needs to be
		// caller-driven, add an `extensions` field to the proto ProvisionRequest
		// and route it through here; do NOT widen this hardcoded call.
		if _, err := adminNewDB.Exec(ctx, sqlCreateExtensionVector); err != nil {
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
//
// P0-2: when this resource was claimed from the hot pool, the database is named
// from the pool token (db_pool-<uuid>), not the request token. poolident
// resolves the canonical naming token from provider_resource_id; the cluster
// segment is stripped before the router parses it.
func (b *LocalBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	adminURL := b.router.AdminURLForResource(poolident.BasePRID(providerResourceID))
	conn, err := pgxConnect(ctx, adminURL)
	if err != nil {
		return 0, fmt.Errorf("db.local.StorageBytes: connect: %w", err)
	}
	defer func() {
		if discErr := conn.Close(ctx); discErr != nil {
			slog.Error("db.local.StorageBytes: disconnect", "error", discErr)
		}
	}()

	dbName := dbNamePrefix + poolident.NamingToken(token, providerResourceID)
	var size int64
	if err := conn.QueryRow(ctx, "SELECT pg_database_size($1)", dbName).Scan(&size); err != nil {
		return 0, fmt.Errorf("db.local.StorageBytes: pg_database_size: %w", err)
	}
	return size, nil
}

// Deprovision terminates active connections, drops the database and user for the token.
//
// P0-2: a pool-claimed resource's database/user are named from the pool token,
// not the request token; poolident.NamingToken resolves the correct name from
// provider_resource_id so DROP DATABASE actually destroys the backing infra
// rather than no-op'ing on a db_<real-token> that was never created.
//
// P2-07: idempotency. The pre-fix code dropped the database first and returned
// Internal on any failure — so a transient "database is being accessed by other
// users" (a client reconnected in the TOCTOU window after pg_terminate_backend)
// aborted the whole RPC and the role was never dropped, leaking it forever.
// Now DROP USER runs unconditionally regardless of the DROP DATABASE outcome,
// and DROP DATABASE uses WITH (FORCE) inside a short retry loop so it can ride
// out the reconnect race itself.
func (b *LocalBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	namingToken := poolident.NamingToken(token, providerResourceID)
	dbName := dbNamePrefix + namingToken
	username := userNamePrefix + namingToken

	adminURL := b.router.AdminURLForResource(poolident.BasePRID(providerResourceID))
	conn, err := pgxConnect(ctx, adminURL)
	if err != nil {
		return fmt.Errorf("db.local.Deprovision: connect: %w", err)
	}
	defer func() {
		if discErr := conn.Close(ctx); discErr != nil {
			slog.Error("db.local.Deprovision: disconnect", "error", discErr)
		}
	}()

	// Revoke CONNECT so no new connections can be established mid-teardown, then
	// terminate the active ones. Both are best-effort — DROP DATABASE WITH
	// (FORCE) below will terminate any straggler itself.
	if _, err := conn.Exec(ctx, fmt.Sprintf("REVOKE CONNECT ON DATABASE %q FROM PUBLIC", dbName)); err != nil {
		slog.Debug("db.local.Deprovision: REVOKE CONNECT (continuing)", "token", token, "error", err)
	}
	if _, err := conn.Exec(ctx,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()",
		dbName,
	); err != nil {
		slog.Error("db.local.Deprovision: terminate connections (continuing)", "token", token, "error", err)
	}

	// DROP DATABASE IF EXISTS WITH (FORCE): FORCE (Postgres 13+) terminates any
	// remaining backends rather than failing with "is being accessed by other
	// users". The retry loop covers a client that reconnects between FORCE's
	// own terminate and the drop.
	var dropDBErr error
	for attempt := 1; attempt <= deprovisionDropDBAttempts; attempt++ {
		_, dropDBErr = conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %q WITH (FORCE)", dbName))
		if dropDBErr == nil {
			break
		}
		// Only retry the in-use race; any other error is terminal.
		if !isDatabaseInUseErr(dropDBErr) {
			break
		}
		slog.Warn("db.local.Deprovision: DROP DATABASE in-use, retrying",
			"token", token, "db", dbName, "attempt", attempt, "error", dropDBErr)
		select {
		case <-ctx.Done():
			dropDBErr = ctx.Err()
		case <-time.After(deprovisionDropDBRetryDelay):
		}
		if ctx.Err() != nil {
			break
		}
	}

	// DROP USER runs unconditionally — even if DROP DATABASE failed above. A
	// leaked role is worse than a leaked database: the role is a credential, the
	// database is dead weight. Dropping the role last means a failed DROP
	// DATABASE no longer leaves the credential behind.
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP USER IF EXISTS %q", username)); err != nil {
		slog.Error("db.local.Deprovision: DROP USER (continuing)", "token", token, "error", err)
	}

	if dropDBErr != nil {
		return fmt.Errorf("db.local.Deprovision: DROP DATABASE: %w", dropDBErr)
	}

	slog.Info("db.local.Deprovision: deprovisioned", "token", token, "db", dbName, "user", username)
	return nil
}

// Regrade re-applies the tier's per-role CONNECTION LIMIT to an already-provisioned
// user on the shared local Postgres cluster. This is called by the entitlement
// reconciler on plan upgrades/downgrades to ensure the role cap matches the new tier.
//
// connLimit <= 0 means unlimited (pass -1 from plans.Registry). Postgres uses -1
// internally for "no limit"; ALTER ROLE with CONNECTION LIMIT -1 removes any cap.
func (b *LocalBackend) Regrade(ctx context.Context, token, providerResourceID string, connLimit int) (RegradeResult, error) {
	// P0-2: a pool-claimed role is named from the pool token; resolve it from
	// provider_resource_id so ALTER ROLE targets the role that actually exists.
	username := userNamePrefix + poolident.NamingToken(token, providerResourceID)

	adminURL := b.router.AdminURLForResource(poolident.BasePRID(providerResourceID))
	conn, err := pgxConnect(ctx, adminURL)
	if err != nil {
		return RegradeResult{Applied: false}, fmt.Errorf("db.local.Regrade: connect: %w", err)
	}
	defer func() {
		if discErr := conn.Close(ctx); discErr != nil {
			slog.Error("db.local.Regrade: disconnect", "error", discErr)
		}
	}()

	// connLimit of -1 means unlimited; apply as-is so Postgres removes any cap.
	// connLimit of 0 is treated as unlimited (plans.Registry returns 0 for
	// tiers with no connection limit field set — safer than blocking all connections).
	applyLimit := connLimit
	if applyLimit == 0 {
		applyLimit = -1
	}

	if _, err := conn.Exec(ctx, fmt.Sprintf("ALTER ROLE %q CONNECTION LIMIT %d", username, applyLimit)); err != nil {
		return RegradeResult{Applied: false}, fmt.Errorf("db.local.Regrade: ALTER ROLE: %w", err)
	}

	slog.Info("db.local.Regrade: applied", "token", token, "conn_limit", applyLimit)
	return RegradeResult{Applied: true, AppliedConnLimit: applyLimit}, nil
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
