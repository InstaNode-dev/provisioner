package postgres

// neon.go — NeonBackend calls the Neon Management API to provision isolated Postgres projects.
// Uses net/http only — no external SDK dependency.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

const (
	neonAPIBase       = "https://console.neon.tech/api/v2"
	defaultNeonRegion = "aws-us-east-1"

	// neonProjectNamePrefix is the deterministic project-name prefix. Provision
	// derives the name as neonProjectNamePrefix+token so a retried Provision can
	// look the project up by name and reuse it instead of creating a duplicate.
	neonProjectNamePrefix = "instant-"

	// neonHTTPTimeout bounds every Neon Management API call. Without it the
	// default http.Client has NO timeout — a hung Neon API connection would
	// block the provisioning gRPC handler (and a worker storage tick) forever.
	neonHTTPTimeout = 30 * time.Second
)

// NeonBackend provisions Postgres via the Neon Management API.
type NeonBackend struct {
	apiKey   string
	regionID string
	client   *http.Client
	// apiBase is the Neon Management API root. Defaults to neonAPIBase; a test
	// overrides it to point at an httptest server.
	apiBase string
}

// newNeonBackend creates a NeonBackend.
func newNeonBackend(apiKey, regionID string) *NeonBackend {
	if regionID == "" {
		regionID = defaultNeonRegion
	}
	return &NeonBackend{
		apiKey:   apiKey,
		regionID: regionID,
		client:   &http.Client{Timeout: neonHTTPTimeout},
		apiBase:  neonAPIBase,
	}
}

// base returns the Neon API root, defaulting to neonAPIBase when unset (so a
// NeonBackend constructed via a struct literal still works).
func (b *NeonBackend) base() string {
	if b.apiBase == "" {
		return neonAPIBase
	}
	return b.apiBase
}

// Provision creates a new Neon project for the given token.
// connLimit is accepted but not applied — Neon projects are fully isolated and
// do not share a Postgres role with other tenants, so there is no shared role
// to cap. Neon enforces compute quotas at the project level via the Neon API.
// POST https://console.neon.tech/api/v2/projects
//
// Idempotency (P2, W5 T2): project creation is NOT naturally idempotent — a
// retried Provision (gRPC deadline expiry, worker re-dispatch) would create a
// SECOND Neon project for the same token, leaking a paid project and orphaning
// its connection URL. The project name is deterministic (neonProjectNamePrefix
// + token), so Provision first looks for an existing project by that name and
// reuses it when found. A new connection-URI cannot be re-derived for an
// existing project (Neon only returns connection_uris on the create call), so
// the reuse path returns the project ID with an empty URL — the caller already
// holds the URL from the first (successful-but-unacknowledged) attempt.
func (b *NeonBackend) Provision(ctx context.Context, token, tier string, connLimit int) (*Credentials, error) {
	projectName := neonProjectNamePrefix + token

	// Reuse an existing project for this token if one is already present.
	if existingID, err := b.findProjectByName(ctx, projectName); err != nil {
		slog.Warn("db.neon.Provision: pre-create lookup failed (continuing to create)",
			"token", token, "error", err)
	} else if existingID != "" {
		slog.Info("db.neon.Provision: reusing existing project (idempotent retry)",
			"token", token, "project_id", existingID)
		return &Credentials{
			DatabaseName:       "neondb",
			Username:           "",
			ProviderResourceID: existingID,
		}, nil
	}

	body := map[string]any{
		"project": map[string]any{
			"name":       projectName,
			"region_id":  b.regionID,
			"pg_version": 16,
		},
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("db.neon.Provision: marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.base()+"/projects", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("db.neon.Provision: new request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+b.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("db.neon.Provision: http: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("db.neon.Provision: read body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("db.neon.Provision: unexpected status %d: %s", resp.StatusCode, string(respBytes))
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
		return nil, fmt.Errorf("db.neon.Provision: unmarshal: %w", err)
	}

	if result.Project.ID == "" {
		return nil, fmt.Errorf("db.neon.Provision: empty project ID in response")
	}
	if len(result.ConnectionURIs) == 0 || result.ConnectionURIs[0].ConnectionURI == "" {
		return nil, fmt.Errorf("db.neon.Provision: no connection URI in response")
	}

	slog.Info("db.neon.Provision: provisioned",
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

// StorageBytes returns data_storage_bytes_hour for the Neon project.
// GET https://console.neon.tech/api/v2/projects/{providerResourceID}
func (b *NeonBackend) StorageBytes(ctx context.Context, token, providerResourceID string) (int64, error) {
	if providerResourceID == "" {
		return 0, fmt.Errorf("db.neon.StorageBytes: empty providerResourceID")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		b.base()+"/projects/"+providerResourceID, nil)
	if err != nil {
		return 0, fmt.Errorf("db.neon.StorageBytes: new request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+b.apiKey)

	resp, err := b.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("db.neon.StorageBytes: http: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("db.neon.StorageBytes: read body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("db.neon.StorageBytes: unexpected status %d: %s", resp.StatusCode, string(respBytes))
	}

	var result struct {
		Project struct {
			Usage struct {
				DataStorageBytesHour int64 `json:"data_storage_bytes_hour"`
			} `json:"usage"`
		} `json:"project"`
	}
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return 0, fmt.Errorf("db.neon.StorageBytes: unmarshal: %w", err)
	}

	return result.Project.Usage.DataStorageBytesHour, nil
}

// Deprovision deletes the Neon project.
// DELETE https://console.neon.tech/api/v2/projects/{providerResourceID}
func (b *NeonBackend) Deprovision(ctx context.Context, token, providerResourceID string) error {
	if providerResourceID == "" {
		return fmt.Errorf("db.neon.Deprovision: empty providerResourceID")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		b.base()+"/projects/"+providerResourceID, nil)
	if err != nil {
		return fmt.Errorf("db.neon.Deprovision: new request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+b.apiKey)

	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("db.neon.Deprovision: http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("db.neon.Deprovision: unexpected status %d: %s", resp.StatusCode, string(body))
	}

	slog.Info("db.neon.Deprovision: deprovisioned", "token", token, "project_id", providerResourceID)
	return nil
}

// Regrade is a no-op for the Neon backend: connection limits are governed by
// the Neon project plan, not a per-role CONNECTION LIMIT, so there is nothing
// to re-apply on a plan upgrade.
func (b *NeonBackend) Regrade(ctx context.Context, token, providerResourceID string, connLimit int) (RegradeResult, error) {
	return RegradeResult{Applied: false, SkipReason: "backend has no per-role connection cap"}, nil
}

// findProjectByName returns the ID of an existing Neon project whose name
// exactly matches projectName, or "" when no such project exists. It supports
// the idempotency check in Provision. A lookup failure is returned as an error
// so the caller can decide whether to proceed with a create.
// GET https://console.neon.tech/api/v2/projects
func (b *NeonBackend) findProjectByName(ctx context.Context, projectName string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.base()+"/projects", nil)
	if err != nil {
		return "", fmt.Errorf("db.neon.findProjectByName: new request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+b.apiKey)

	resp, err := b.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("db.neon.findProjectByName: http: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("db.neon.findProjectByName: read body: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("db.neon.findProjectByName: unexpected status %d: %s", resp.StatusCode, string(respBytes))
	}

	var result struct {
		Projects []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"projects"`
	}
	if err := json.Unmarshal(respBytes, &result); err != nil {
		return "", fmt.Errorf("db.neon.findProjectByName: unmarshal: %w", err)
	}
	for _, p := range result.Projects {
		if p.Name == projectName {
			return p.ID, nil
		}
	}
	return "", nil
}
