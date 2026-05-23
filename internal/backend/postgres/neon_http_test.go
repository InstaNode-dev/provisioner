package postgres

// neon_http_test.go — httptest-driven coverage for the NeonBackend and the
// DedicatedProvider's Neon-mode code path. No real Neon API calls.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---- NeonBackend ----

func TestNeonBackend_StorageBytes_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/projects/") {
			http.Error(w, "unexpected", http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer key" {
			http.Error(w, "bad auth", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project": map[string]any{
				"usage": map[string]any{"data_storage_bytes_hour": 12345},
			},
		})
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "key", regionID: "r", client: srv.Client(), apiBase: srv.URL}
	got, err := b.StorageBytes(context.Background(), "tok", "proj-1")
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	if got != 12345 {
		t.Errorf("StorageBytes = %d; want 12345", got)
	}
}

func TestNeonBackend_StorageBytes_EmptyPRID(t *testing.T) {
	b := newNeonBackend("k", "")
	_, err := b.StorageBytes(context.Background(), "t", "")
	if err == nil || !strings.Contains(err.Error(), "empty providerResourceID") {
		t.Errorf("StorageBytes(\"\") err = %v; want empty providerResourceID", err)
	}
}

func TestNeonBackend_StorageBytes_BadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	_, err := b.StorageBytes(context.Background(), "t", "proj-x")
	if err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Errorf("StorageBytes bad status err = %v", err)
	}
}

func TestNeonBackend_StorageBytes_BadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{not json"))
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	_, err := b.StorageBytes(context.Background(), "t", "proj-x")
	if err == nil || !strings.Contains(err.Error(), "unmarshal") {
		t.Errorf("StorageBytes bad json err = %v", err)
	}
}

func TestNeonBackend_StorageBytes_HTTPError(t *testing.T) {
	// Use a closed server so client.Do fails.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	_, err := b.StorageBytes(context.Background(), "t", "proj-x")
	if err == nil {
		t.Error("expected http error on closed server")
	}
}

func TestNeonBackend_Deprovision_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || !strings.HasPrefix(r.URL.Path, "/projects/") {
			http.Error(w, "unexpected", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	if err := b.Deprovision(context.Background(), "t", "proj-1"); err != nil {
		t.Errorf("Deprovision: %v", err)
	}
}

func TestNeonBackend_Deprovision_EmptyPRID(t *testing.T) {
	b := newNeonBackend("k", "")
	if err := b.Deprovision(context.Background(), "t", ""); err == nil {
		t.Error("Deprovision(\"\") err = nil; want empty providerResourceID")
	}
}

func TestNeonBackend_Deprovision_BadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "denied", http.StatusForbidden)
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	err := b.Deprovision(context.Background(), "t", "proj-x")
	if err == nil || !strings.Contains(err.Error(), "status 403") {
		t.Errorf("Deprovision bad status err = %v", err)
	}
}

func TestNeonBackend_Deprovision_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	if err := b.Deprovision(context.Background(), "t", "proj-x"); err == nil {
		t.Error("expected http error")
	}
}

func TestNeonBackend_Regrade_NoOp(t *testing.T) {
	b := newNeonBackend("k", "")
	res, err := b.Regrade(context.Background(), "t", "proj-1", 8)
	if err != nil {
		t.Fatalf("Regrade: %v", err)
	}
	if res.Applied {
		t.Errorf("Regrade.Applied = true; want false (neon backend has no per-role cap)")
	}
	if res.SkipReason == "" {
		t.Errorf("Regrade.SkipReason is empty; want a reason")
	}
}

func TestNeonBackend_Provision_BadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []map[string]string{}})
			return
		}
		http.Error(w, "fail", http.StatusBadRequest)
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	_, err := b.Provision(context.Background(), "tok", "pro", -1)
	if err == nil || !strings.Contains(err.Error(), "status 400") {
		t.Errorf("Provision bad status err = %v", err)
	}
}

func TestNeonBackend_Provision_EmptyProjectID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []map[string]string{}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project":         map[string]string{"id": ""},
			"connection_uris": []map[string]string{{"connection_uri": "postgres://x"}},
		})
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	_, err := b.Provision(context.Background(), "tok", "pro", -1)
	if err == nil || !strings.Contains(err.Error(), "empty project ID") {
		t.Errorf("Provision err = %v; want 'empty project ID'", err)
	}
}

func TestNeonBackend_Provision_NoConnURI(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []map[string]string{}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project":         map[string]string{"id": "proj-1"},
			"connection_uris": []map[string]string{},
		})
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	_, err := b.Provision(context.Background(), "tok", "pro", -1)
	if err == nil || !strings.Contains(err.Error(), "connection URI") {
		t.Errorf("Provision err = %v; want missing connection URI", err)
	}
}

func TestNeonBackend_Provision_BadResponseJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []map[string]string{}})
			return
		}
		_, _ = w.Write([]byte("{not json"))
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	_, err := b.Provision(context.Background(), "tok", "pro", -1)
	if err == nil || !strings.Contains(err.Error(), "unmarshal") {
		t.Errorf("Provision err = %v; want unmarshal", err)
	}
}

func TestNeonBackend_FindProjectByName_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	if _, err := b.findProjectByName(context.Background(), "instant-x"); err == nil {
		t.Error("expected http error on closed server")
	}
}

func TestNeonBackend_FindProjectByName_BadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "down", http.StatusBadGateway)
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	if _, err := b.findProjectByName(context.Background(), "instant-x"); err == nil {
		t.Error("expected status 502 error")
	}
}

func TestNeonBackend_FindProjectByName_BadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{not json"))
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	if _, err := b.findProjectByName(context.Background(), "instant-x"); err == nil {
		t.Error("expected unmarshal error")
	}
}

func TestNeonBackend_FindProjectByName_NoMatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"projects": []map[string]string{{"id": "p1", "name": "instant-other"}},
		})
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	id, err := b.findProjectByName(context.Background(), "instant-missing")
	if err != nil {
		t.Fatalf("findProjectByName: %v", err)
	}
	if id != "" {
		t.Errorf("findProjectByName(missing) = %q; want \"\"", id)
	}
}

func TestNeonBackend_Provision_PreLookupFails_StillCreates(t *testing.T) {
	// GET returns 500, POST must still succeed (Provision logs + continues).
	var postHit bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			http.Error(w, "down", http.StatusInternalServerError)
		case http.MethodPost:
			postHit = true
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"project":         map[string]string{"id": "proj-after-list-fail"},
				"connection_uris": []map[string]string{{"connection_uri": "postgres://x"}},
			})
		}
	}))
	defer srv.Close()
	b := &NeonBackend{apiKey: "k", client: srv.Client(), apiBase: srv.URL}
	creds, err := b.Provision(context.Background(), "tok", "pro", -1)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if !postHit {
		t.Error("POST was not made despite list lookup failure")
	}
	if creds.ProviderResourceID != "proj-after-list-fail" {
		t.Errorf("Provision PRID = %q; want proj-after-list-fail", creds.ProviderResourceID)
	}
}

// TestNeonBackend_Base_DefaultsWhenEmpty exercises the apiBase fallback.
func TestNeonBackend_Base_DefaultsWhenEmpty(t *testing.T) {
	b := &NeonBackend{}
	if got := b.base(); got != neonAPIBase {
		t.Errorf("base() = %q; want %q", got, neonAPIBase)
	}
}

// ---- DedicatedProvider Neon path ----

func TestDedicatedProvider_NeonProvision_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/projects" {
			http.Error(w, "unexpected", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project":         map[string]string{"id": "ded-1"},
			"connection_uris": []map[string]string{{"connection_uri": "postgres://x"}},
		})
	}))
	defer srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	creds, err := p.Provision(context.Background(), "long-token-truncated-to-16", "team", -1)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if creds.ProviderResourceID != "ded-1" {
		t.Errorf("ProviderResourceID = %q; want ded-1", creds.ProviderResourceID)
	}
}

func TestDedicatedProvider_NeonProvision_BadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "fail", http.StatusInternalServerError)
	}))
	defer srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	_, err := p.Provision(context.Background(), "tok", "team", -1)
	if err == nil || !strings.Contains(err.Error(), "status 500") {
		t.Errorf("Provision err = %v; want status 500", err)
	}
}

func TestDedicatedProvider_NeonProvision_EmptyID(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project":         map[string]string{"id": ""},
			"connection_uris": []map[string]string{{"connection_uri": "postgres://x"}},
		})
	}))
	defer srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	if _, err := p.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("Provision empty id returned nil; want error")
	}
}

func TestDedicatedProvider_NeonProvision_NoURI(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project":         map[string]string{"id": "ded-1"},
			"connection_uris": []map[string]string{},
		})
	}))
	defer srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	if _, err := p.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("Provision no URI returned nil; want error")
	}
}

func TestDedicatedProvider_NeonProvision_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	if _, err := p.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("expected http error")
	}
}

func TestDedicatedProvider_NeonStorageBytes_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project": map[string]any{"usage": map[string]any{"data_storage_bytes_hour": 9000}},
		})
	}))
	defer srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	n, err := p.StorageBytes(context.Background(), "tok", "ded-1")
	if err != nil || n != 9000 {
		t.Errorf("StorageBytes = (%d,%v); want (9000,nil)", n, err)
	}
}

func TestDedicatedProvider_NeonStorageBytes_EmptyPRID(t *testing.T) {
	p := &DedicatedProvider{neonAPIKey: "k"}
	if _, err := p.StorageBytes(context.Background(), "tok", ""); err == nil {
		t.Error("StorageBytes(\"\") returned nil; want error")
	}
}

func TestDedicatedProvider_NeonStorageBytes_BadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "fail", http.StatusInternalServerError)
	}))
	defer srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	if _, err := p.StorageBytes(context.Background(), "tok", "ded-1"); err == nil {
		t.Error("StorageBytes bad status returned nil; want error")
	}
}

func TestDedicatedProvider_NeonStorageBytes_BadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{not json"))
	}))
	defer srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	if _, err := p.StorageBytes(context.Background(), "tok", "ded-1"); err == nil {
		t.Error("StorageBytes bad json returned nil; want error")
	}
}

func TestDedicatedProvider_NeonStorageBytes_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	if _, err := p.StorageBytes(context.Background(), "tok", "ded-1"); err == nil {
		t.Error("expected http error")
	}
}

func TestDedicatedProvider_NeonDeprovision_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			http.Error(w, "unexpected", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	if err := p.Deprovision(context.Background(), "tok", "ded-1"); err != nil {
		t.Errorf("Deprovision: %v", err)
	}
}

func TestDedicatedProvider_NeonDeprovision_EmptyPRID(t *testing.T) {
	p := &DedicatedProvider{neonAPIKey: "k"}
	if err := p.Deprovision(context.Background(), "tok", ""); err == nil {
		t.Error("Deprovision(\"\") returned nil; want error")
	}
}

func TestDedicatedProvider_NeonDeprovision_BadStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "denied", http.StatusForbidden)
	}))
	defer srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	if err := p.Deprovision(context.Background(), "tok", "ded-1"); err == nil {
		t.Error("Deprovision bad status returned nil; want error")
	}
}

func TestDedicatedProvider_NeonDeprovision_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	if err := p.Deprovision(context.Background(), "tok", "ded-1"); err == nil {
		t.Error("expected http error")
	}
}

func TestDedicatedProvider_Regrade_NoOp(t *testing.T) {
	p := NewDedicatedProvider("", "")
	res, err := p.Regrade(context.Background(), "t", "p", 8)
	if err != nil || res.Applied {
		t.Errorf("Regrade = (%+v,%v); want Applied=false err=nil", res, err)
	}
}

func TestDedicatedProvider_LocalAdminDSN_DefaultsFallback(t *testing.T) {
	p := &DedicatedProvider{}
	if got := p.localAdminDSN(); got != defaultCustomersURL {
		t.Errorf("localAdminDSN(empty) = %q; want default %q", got, defaultCustomersURL)
	}
	p.adminDSN = "postgres://override/x"
	if got := p.localAdminDSN(); got != "postgres://override/x" {
		t.Errorf("localAdminDSN(set) = %q; want override", got)
	}
}
