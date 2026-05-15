package postgres

// dedicated.go — DedicatedProvider provisions isolated Postgres instances for the Team tier.
//
// Two modes:
//  1. Neon API mode (neonAPIKey != ""): calls POST /api/v2/projects to create a new
//     Neon project.  Each project is a fully isolated Postgres cluster with its own
//     compute, storage and connection string.
//  2. Local admin mode (neonAPIKey == ""): creates a database + user on a separate
//     "dedicated" Postgres cluster pointed to by adminDSN.  This simulates dedicated
//     isolation in dev/test without requiring an external API.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5"
)

const dedicatedNeonRegion = "aws-us-east-2"

// DedicatedProvider provisions dedicated Postgres instances.
// For local/dev: creates a separate Postgres database on a "dedicated" admin DSN.
// For production: calls the Neon API to create a new project.
type DedicatedProvider struct {
	adminDSN    string // postgres://admin@host/postgres — separate from shared cluster
	neonAPIKey  string // optional: if set, use Neon API instead of direct admin DSN
	neonBaseURL string // "https://console.neon.tech/api/v2"
	httpClient  *http.Client
}

// NewDedicatedProvider creates a DedicatedProvider.
// adminDSN is used when neonAPIKey is empty (local/dev simulation).
// neonAPIKey triggers the real Neon API path.
func NewDedicatedProvider(adminDSN, neonAPIKey string) *DedicatedProvider {
	return &DedicatedProvider{
		adminDSN:    adminDSN,
		neonAPIKey:  neonAPIKey,
		neonBaseURL: neonAPIBase, // reuse the constant from neon.go
		httpClient:  &http.Client{},
	}
}

// Provision creates a dedicated database instance.
// If neonAPIKey is set: calls Neon API POST /projects.
// Otherwise: creates a new database + role on adminDSN (local dedicated simulation).
func (p *DedicatedProvider) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	if p.neonAPIKey != "" {
		return p.provisionNeon(ctx, token, tier)
	}
	return p.provisionLocal(ctx, token, tier)
}

// StorageBytes returns storage used by the dedicated instance.
// For Neon: calls GET /projects/{id}.
// For local: uses pg_database_size on the admin connection.
func (p *DedicatedProvider) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	if p.neonAPIKey != "" {
		// Delegate to NeonBackend's StorageBytes logic via a local helper.
		return p.neonStorageBytes(ctx, providerResourceID)
	}
	return p.localStorageBytes(ctx, token)
}

// Deprovision tears down the dedicated instance.
// For Neon: DELETE /projects/{id}.
// For local: drops database and user on adminDSN.
func (p *DedicatedProvider) Deprovision(ctx context.Context, token, providerResourceID string) error {
	if p.neonAPIKey != "" {
		return p.deprovisionNeon(ctx, token, providerResourceID)
	}
	return p.deprovisionLocal(ctx, token)
}

// Regrade is a no-op for the dedicated provider. The Neon path manages
// connection limits through the Neon project plan, not a per-role
// CONNECTION LIMIT; the local-admin path sets no per-role cap at provision
// time. Either way there is no cap to re-apply, so a skip result is returned.
func (p *DedicatedProvider) Regrade(ctx context.Context, token, providerResourceID string, connLimit int) (RegradeResult, error) {
	return RegradeResult{Applied: false, SkipReason: "backend has no per-role connection cap"}, nil
}

// --- Neon API path ---

func (p *DedicatedProvider) provisionNeon(ctx context.Context, token, tier string) (*Credentials, error) {
	// Use a short prefix so project names don't exceed Neon's 64-char limit.
	tokenPrefix := token
	if len(tokenPrefix) > 16 {
		tokenPrefix = tokenPrefix[:16]
	}

	body := map[string]any{
		"project": map[string]any{
			"name":       "instant-" + tokenPrefix,
			"pg_version": 16,
			"region_id":  dedicatedNeonRegion,
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("db.dedicated.provisionNeon: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		p.neonBaseURL+"/projects", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("db.dedicated.provisionNeon: new request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.neonAPIKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("db.dedicated.provisionNeon: http: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("db.dedicated.provisionNeon: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("db.dedicated.provisionNeon: status %d: %s", resp.StatusCode, string(respBytes))
	}

	var result struct {
		Project struct {
			ID string `json:"id"`
		} `json:"project"`
		ConnectionURIs []struct {
			ConnectionURI string `json:"connection_uri"`
		} `json:"connection_uris"`
	}
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return nil, fmt.Errorf("db.dedicated.provisionNeon: unmarshal: %w", err)
	}
	if result.Project.ID == "" {
		return nil, fmt.Errorf("db.dedicated.provisionNeon: empty project ID in response")
	}
	if len(result.ConnectionURIs) == 0 || result.ConnectionURIs[0].ConnectionURI == "" {
		return nil, fmt.Errorf("db.dedicated.provisionNeon: no connection URI in response")
	}

	slog.Info("db.dedicated.provisionNeon: provisioned",
		"token", token,
		"project_id", result.Project.ID,
		"tier", tier,
	)
	return &Credentials{
		URL:                result.ConnectionURIs[0].ConnectionURI,
		DatabaseName:       "neondb",
		Username:           "",
		ProviderResourceID: result.Project.ID,
	}, nil
}

func (p *DedicatedProvider) neonStorageBytes(ctx context.Context, providerResourceID string) (int64, error) {
	if providerResourceID == "" {
		return 0, fmt.Errorf("db.dedicated.neonStorageBytes: empty providerResourceID")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		p.neonBaseURL+"/projects/"+providerResourceID, nil)
	if err != nil {
		return 0, fmt.Errorf("db.dedicated.neonStorageBytes: new request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.neonAPIKey)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("db.dedicated.neonStorageBytes: http: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("db.dedicated.neonStorageBytes: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("db.dedicated.neonStorageBytes: status %d: %s", resp.StatusCode, string(respBytes))
	}

	var result struct {
		Project struct {
			Usage struct {
				DataStorageBytesHour int64 `json:"data_storage_bytes_hour"`
			} `json:"usage"`
		} `json:"project"`
	}
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return 0, fmt.Errorf("db.dedicated.neonStorageBytes: unmarshal: %w", err)
	}
	return result.Project.Usage.DataStorageBytesHour, nil
}

func (p *DedicatedProvider) deprovisionNeon(ctx context.Context, token, providerResourceID string) error {
	if providerResourceID == "" {
		return fmt.Errorf("db.dedicated.deprovisionNeon: empty providerResourceID")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		p.neonBaseURL+"/projects/"+providerResourceID, nil)
	if err != nil {
		return fmt.Errorf("db.dedicated.deprovisionNeon: new request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+p.neonAPIKey)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("db.dedicated.deprovisionNeon: http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("db.dedicated.deprovisionNeon: status %d: %s", resp.StatusCode, string(body))
	}
	slog.Info("db.dedicated.deprovisionNeon: deprovisioned", "token", token, "project_id", providerResourceID)
	return nil
}

// --- Local admin path (dev/test) ---

// localAdminDSN returns the configured adminDSN or falls back to the shared customers URL
// with a "dedicated_" prefix on the database name component.
func (p *DedicatedProvider) localAdminDSN() string {
	if p.adminDSN != "" {
		return p.adminDSN
	}
	return defaultCustomersURL
}

func (p *DedicatedProvider) provisionLocal(ctx context.Context, token, tier string) (*Credentials, error) {
	// Use dedicated_ prefix to distinguish from shared-cluster databases.
	dbName := "dedicated_db_" + token
	username := "dedicated_usr_" + token

	pass, err := generatePassword(16)
	if err != nil {
		return nil, fmt.Errorf("db.dedicated.provisionLocal: %w", err)
	}

	adminDSN := p.localAdminDSN()
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return nil, fmt.Errorf("db.dedicated.provisionLocal: connect: %w", err)
	}
	defer func() {
		if discErr := conn.Close(ctx); discErr != nil {
			slog.Error("db.dedicated.provisionLocal: disconnect", "error", discErr)
		}
	}()

	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", dbName)); err != nil {
		return nil, fmt.Errorf("db.dedicated.provisionLocal: CREATE DATABASE: %w", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE USER %q WITH PASSWORD '%s'", username, pass)); err != nil {
		return nil, fmt.Errorf("db.dedicated.provisionLocal: CREATE USER: %w", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %q TO %q", dbName, username)); err != nil {
		return nil, fmt.Errorf("db.dedicated.provisionLocal: GRANT DATABASE: %w", err)
	}

	// Grant schema privileges on the new database.
	adminNewDB, err := pgx.Connect(ctx, buildAdminNewDBURL(adminDSN, dbName))
	if err != nil {
		slog.Error("db.dedicated.provisionLocal: connect new db for schema grant (non-fatal)", "error", err)
	} else {
		defer func() {
			if discErr := adminNewDB.Close(ctx); discErr != nil {
				slog.Error("db.dedicated.provisionLocal: disconnect new db", "error", discErr)
			}
		}()
		if _, err := adminNewDB.Exec(ctx, fmt.Sprintf("GRANT ALL ON SCHEMA public TO %q", username)); err != nil {
			slog.Error("db.dedicated.provisionLocal: GRANT SCHEMA (non-fatal)", "token", token, "error", err)
		}
	}

	host := extractHost(adminDSN)
	newDBURL := fmt.Sprintf("postgres://%s:%s@%s/%s", username, pass, host, dbName)

	slog.Info("db.dedicated.provisionLocal: provisioned",
		"token", token,
		"db", dbName,
		"user", username,
		"tier", tier,
	)
	return &Credentials{
		URL:                newDBURL,
		DatabaseName:       dbName,
		Username:           username,
		ProviderResourceID: "",
	}, nil
}

func (p *DedicatedProvider) localStorageBytes(ctx context.Context, token string) (int64, error) {
	adminDSN := p.localAdminDSN()
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return 0, fmt.Errorf("db.dedicated.localStorageBytes: connect: %w", err)
	}
	defer func() {
		if discErr := conn.Close(ctx); discErr != nil {
			slog.Error("db.dedicated.localStorageBytes: disconnect", "error", discErr)
		}
	}()

	dbName := "dedicated_db_" + token
	var size int64
	if err := conn.QueryRow(ctx, "SELECT pg_database_size($1)", dbName).Scan(&size); err != nil {
		return 0, fmt.Errorf("db.dedicated.localStorageBytes: pg_database_size: %w", err)
	}
	return size, nil
}

func (p *DedicatedProvider) deprovisionLocal(ctx context.Context, token string) error {
	dbName := "dedicated_db_" + token
	username := "dedicated_usr_" + token

	adminDSN := p.localAdminDSN()
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return fmt.Errorf("db.dedicated.deprovisionLocal: connect: %w", err)
	}
	defer func() {
		if discErr := conn.Close(ctx); discErr != nil {
			slog.Error("db.dedicated.deprovisionLocal: disconnect", "error", discErr)
		}
	}()

	// Terminate active connections before dropping.
	_, err = conn.Exec(ctx,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid<>pg_backend_pid()",
		dbName,
	)
	if err != nil {
		slog.Error("db.dedicated.deprovisionLocal: terminate connections (continuing)", "token", token, "error", err)
	}

	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %q", dbName)); err != nil {
		return fmt.Errorf("db.dedicated.deprovisionLocal: DROP DATABASE: %w", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("DROP USER IF EXISTS %q", username)); err != nil {
		slog.Error("db.dedicated.deprovisionLocal: DROP USER (continuing)", "token", token, "error", err)
	}

	slog.Info("db.dedicated.deprovisionLocal: deprovisioned", "token", token, "db", dbName, "user", username)
	return nil
}
