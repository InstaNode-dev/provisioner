package postgres

// neon_more2_test.go — additional httptest coverage for NeonBackend success and
// http-error branches, plus a K8sBackend Provision path that carries a team ID
// in context.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/kubernetes/fake"

	"instant.dev/provisioner/internal/ctxkeys"
)

// TestNeonBackend_Provision_OK covers the full success path of NeonBackend
// Provision: pre-create lookup (empty), POST /projects, parse project+conn URI.
func TestNeonBackend_Provision_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/projects":
			// findProjectByName: no existing project.
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []any{}})
		case r.Method == http.MethodPost && r.URL.Path == "/projects":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"project":         map[string]string{"id": "proj-new"},
				"connection_uris": []map[string]string{{"connection_uri": "postgres://neon/db"}},
			})
		default:
			http.Error(w, "unexpected", http.StatusNotFound)
		}
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "key", regionID: "r", client: srv.Client(), apiBase: srv.URL}
	creds, err := b.Provision(context.Background(), "tok", "team", -1)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if creds.ProviderResourceID != "proj-new" || creds.URL != "postgres://neon/db" {
		t.Errorf("creds = %+v; want proj-new / postgres://neon/db", creds)
	}
}

// TestNeonBackend_Provision_ReuseExisting covers the idempotent-reuse branch:
// findProjectByName returns an existing match so Provision returns it without
// POSTing.
func TestNeonBackend_Provision_ReuseExisting(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			t.Error("Provision POSTed despite an existing project")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"projects": []map[string]string{
				{"id": "proj-existing", "name": neonProjectNamePrefix + "tok"},
			},
		})
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "key", regionID: "r", client: srv.Client(), apiBase: srv.URL}
	creds, err := b.Provision(context.Background(), "tok", "team", -1)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if creds.ProviderResourceID != "proj-existing" {
		t.Errorf("ProviderResourceID = %q; want proj-existing (reuse)", creds.ProviderResourceID)
	}
}

// TestNeonBackend_Provision_HTTPError covers the create-POST http transport
// error branch (the server is closed so the POST Do() fails).
func TestNeonBackend_Provision_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	srv.Close()
	b := &NeonBackend{apiKey: "key", regionID: "r", client: srv.Client(), apiBase: srv.URL}
	if _, err := b.Provision(context.Background(), "tok", "team", -1); err == nil ||
		!strings.Contains(err.Error(), "http") {
		t.Fatalf("Provision http err = %v; want 'http' wrap", err)
	}
}

// TestK8sBackend_Provision_CarriesTeamIDContext covers the teamID-in-context
// branch of Provision (the provCtx value-propagation arm). Provision still fails
// at initDatabase against the fake Service, but the context arm is exercised.
func TestK8sBackend_Provision_CarriesTeamIDContext(t *testing.T) {
	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs, image: "img", externalHost: "h", storageClass: "sc", storageSizeGi: 10}
	// Preload a ready pod so waitPodReady returns immediately (otherwise Provision
	// blocks for the full k8sReadyTimeout before failing at initDatabase).
	preloadReadyPod(t, cs, k8sNsPrefix+"teamidtok")
	ctx := context.WithValue(context.Background(), ctxkeys.TeamIDKey, "team-uuid-123")
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	// Fails downstream (no real pod), but the teamID context-propagation arm and
	// the applyNamespace owner-team-label path run. We only assert it returns an
	// error — rollback deletes the namespace so a post-check would race.
	if _, err := b.Provision(ctx, "teamidtok", "hobby", 4); err == nil {
		t.Fatal("Provision returned nil; expected downstream failure")
	}
}
