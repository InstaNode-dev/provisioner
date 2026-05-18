package postgres

// neon_test.go — regression tests for the BugBash-2026-05-18 P2 fixes in the
// Neon backend:
//   - the HTTP client must carry a timeout (a hung Neon API call must not block
//     the provisioning handler forever);
//   - Provision must be idempotent — a retry for a token whose project already
//     exists must reuse it, never create a duplicate (paid) project.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNeonBackend_HTTPClientHasTimeout(t *testing.T) {
	b := newNeonBackend("key", "")
	if b.client.Timeout != neonHTTPTimeout {
		t.Errorf("neon HTTP client timeout = %v; want %v — a hung Neon API call "+
			"must not block the provisioner indefinitely", b.client.Timeout, neonHTTPTimeout)
	}
	if neonHTTPTimeout == 0 {
		t.Error("neonHTTPTimeout is 0 — that is exactly the no-timeout bug")
	}
}

// TestNeonBackend_Provision_Idempotent_ReusesExistingProject — when a project
// named neonProjectNamePrefix+token already exists, Provision must reuse it and
// NOT POST a new /projects create.
func TestNeonBackend_Provision_Idempotent_ReusesExistingProject(t *testing.T) {
	const token = "abc-123"
	wantName := neonProjectNamePrefix + token

	var createCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/projects":
			// List: report the project as already present.
			_ = json.NewEncoder(w).Encode(map[string]any{
				"projects": []map[string]string{
					{"id": "proj-existing-999", "name": wantName},
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/projects":
			createCalls++
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"project":         map[string]string{"id": "proj-NEW-should-not-happen"},
				"connection_uris": []map[string]string{{"connection_uri": "postgres://x"}},
			})
		default:
			http.Error(w, "unexpected", http.StatusNotFound)
		}
	}))
	defer srv.Close()

	b := &NeonBackend{apiKey: "key", regionID: defaultNeonRegion, client: srv.Client(), apiBase: srv.URL}

	creds, err := b.Provision(context.Background(), token, "pro", -1)
	if err != nil {
		t.Fatalf("Provision returned error: %v", err)
	}
	if createCalls != 0 {
		t.Errorf("Provision created %d new project(s) for an already-existing token "+
			"— it must reuse, not duplicate", createCalls)
	}
	if creds.ProviderResourceID != "proj-existing-999" {
		t.Errorf("Provision ProviderResourceID = %q; want the existing project ID", creds.ProviderResourceID)
	}
}

// TestNeonBackend_Provision_NoExistingProject_Creates — when no project exists,
// Provision falls through to the create call as before.
func TestNeonBackend_Provision_NoExistingProject_Creates(t *testing.T) {
	var createCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/projects":
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []map[string]string{}})
		case r.Method == http.MethodPost && r.URL.Path == "/projects":
			createCalls++
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"project":         map[string]string{"id": "proj-fresh-1"},
				"connection_uris": []map[string]string{{"connection_uri": "postgres://fresh"}},
			})
		default:
			http.Error(w, "unexpected", http.StatusNotFound)
		}
	}))
	defer srv.Close()

	b := &NeonBackend{apiKey: "key", regionID: defaultNeonRegion, client: srv.Client(), apiBase: srv.URL}

	creds, err := b.Provision(context.Background(), "new-token", "pro", -1)
	if err != nil {
		t.Fatalf("Provision returned error: %v", err)
	}
	if createCalls != 1 {
		t.Errorf("Provision made %d create calls; want exactly 1 for a fresh token", createCalls)
	}
	if creds.ProviderResourceID != "proj-fresh-1" || creds.URL != "postgres://fresh" {
		t.Errorf("Provision creds = %+v; want fresh project id + URL", creds)
	}
}
