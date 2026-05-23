package postgres

// neon_seam_test.go — coverage for neon.go paths the existing idempotency tests
// don't reach: StorageBytes, Deprovision, findProjectByName, the base() default,
// and the marshal / new-request / read-body / non-2xx / unmarshal error wraps
// (driven via the json/http/io seams).

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func neonTestServer(t *testing.T, h http.HandlerFunc) *NeonBackend {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &NeonBackend{apiKey: "key", regionID: defaultNeonRegion, client: srv.Client(), apiBase: srv.URL}
}

func TestNeon_base_DefaultsWhenEmpty(t *testing.T) {
	b := &NeonBackend{}
	if b.base() != neonAPIBase {
		t.Errorf("base() = %q; want %q", b.base(), neonAPIBase)
	}
}

func TestNewNeonBackend_DefaultRegion(t *testing.T) {
	b := newNeonBackend("k", "")
	if b.regionID != defaultNeonRegion {
		t.Errorf("regionID = %q; want default", b.regionID)
	}
	b2 := newNeonBackend("k", "eu-x")
	if b2.regionID != "eu-x" {
		t.Errorf("regionID = %q; want eu-x", b2.regionID)
	}
}

func TestNeon_Provision_CreateSuccess_FullDecode(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []any{}})
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project":         map[string]string{"id": "p1"},
			"connection_uris": []map[string]string{{"connection_uri": "postgres://c"}},
		})
	})
	creds, err := b.Provision(context.Background(), "tok", "team", -1)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if creds.ProviderResourceID != "p1" || creds.URL != "postgres://c" {
		t.Errorf("creds = %+v", creds)
	}
}

func TestNeon_Provision_MarshalError(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"projects": []any{}})
	})
	orig := jsonMarshal
	jsonMarshal = func(any) ([]byte, error) { return nil, errSeam }
	t.Cleanup(func() { jsonMarshal = orig })
	if _, err := b.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("expected marshal error")
	}
}

func TestNeon_Provision_NewRequestError(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"projects": []any{}})
	})
	orig := httpNewRequestWithContext
	httpNewRequestWithContext = func(context.Context, string, string, io.Reader) (*http.Request, error) {
		return nil, errSeam
	}
	t.Cleanup(func() { httpNewRequestWithContext = orig })
	if _, err := b.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("expected new-request error")
	}
}

func TestNeon_Provision_HTTPError(t *testing.T) {
	// Point at an unroutable base so client.Do fails.
	b := &NeonBackend{apiKey: "k", client: &http.Client{}, apiBase: "http://127.0.0.1:1"}
	if _, err := b.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("expected http error")
	}
}

func TestNeon_Provision_Non2xx(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []any{}})
			return
		}
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	if _, err := b.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("expected non-2xx error")
	}
}

func TestNeon_Provision_ReadBodyError(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []any{}})
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("{}"))
	})
	orig := ioReadAll
	var n int
	ioReadAll = func(r io.Reader) ([]byte, error) {
		n++
		if n >= 2 { // first ReadAll is the GET list; fail on the POST create body
			return nil, errSeam
		}
		return io.ReadAll(r)
	}
	t.Cleanup(func() { ioReadAll = orig })
	if _, err := b.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("expected read-body error")
	}
}

func TestNeon_Provision_UnmarshalError(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []any{}})
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("not-json"))
	})
	if _, err := b.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("expected unmarshal error")
	}
}

func TestNeon_Provision_EmptyProjectID(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []any{}})
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"project": map[string]string{"id": ""}})
	})
	if _, err := b.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("expected empty-project-id error")
	}
}

func TestNeon_Provision_NoConnectionURI(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_ = json.NewEncoder(w).Encode(map[string]any{"projects": []any{}})
			return
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"project": map[string]string{"id": "p1"}})
	})
	if _, err := b.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("expected no-connection-uri error")
	}
}

func TestNeon_StorageBytes_Success(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project": map[string]any{"usage": map[string]any{"data_storage_bytes_hour": 9999}},
		})
	})
	got, err := b.StorageBytes(context.Background(), "tok", "p1")
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	if got != 9999 {
		t.Errorf("got %d; want 9999", got)
	}
}

func TestNeon_StorageBytes_EmptyPRID(t *testing.T) {
	b := &NeonBackend{}
	if _, err := b.StorageBytes(context.Background(), "tok", ""); err == nil {
		t.Error("expected empty-PRID error")
	}
}

func TestNeon_StorageBytes_NewRequestError(t *testing.T) {
	orig := httpNewRequestWithContext
	httpNewRequestWithContext = func(context.Context, string, string, io.Reader) (*http.Request, error) {
		return nil, errSeam
	}
	t.Cleanup(func() { httpNewRequestWithContext = orig })
	b := &NeonBackend{apiKey: "k", client: &http.Client{}, apiBase: "http://x"}
	if _, err := b.StorageBytes(context.Background(), "tok", "p1"); err == nil {
		t.Error("expected new-request error")
	}
}

func TestNeon_StorageBytes_HTTPError(t *testing.T) {
	b := &NeonBackend{apiKey: "k", client: &http.Client{}, apiBase: "http://127.0.0.1:1"}
	if _, err := b.StorageBytes(context.Background(), "tok", "p1"); err == nil {
		t.Error("expected http error")
	}
}

func TestNeon_StorageBytes_Non2xx(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusNotFound)
	})
	if _, err := b.StorageBytes(context.Background(), "tok", "p1"); err == nil {
		t.Error("expected non-2xx error")
	}
}

func TestNeon_StorageBytes_ReadBodyError(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("{}")) })
	orig := ioReadAll
	ioReadAll = func(io.Reader) ([]byte, error) { return nil, errSeam }
	t.Cleanup(func() { ioReadAll = orig })
	if _, err := b.StorageBytes(context.Background(), "tok", "p1"); err == nil {
		t.Error("expected read-body error")
	}
}

func TestNeon_StorageBytes_UnmarshalError(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("xx")) })
	if _, err := b.StorageBytes(context.Background(), "tok", "p1"); err == nil {
		t.Error("expected unmarshal error")
	}
}

func TestNeon_Deprovision_Success(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	if err := b.Deprovision(context.Background(), "tok", "p1"); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
}

func TestNeon_Deprovision_EmptyPRID(t *testing.T) {
	b := &NeonBackend{}
	if err := b.Deprovision(context.Background(), "tok", ""); err == nil {
		t.Error("expected empty-PRID error")
	}
}

func TestNeon_Deprovision_NewRequestError(t *testing.T) {
	orig := httpNewRequestWithContext
	httpNewRequestWithContext = func(context.Context, string, string, io.Reader) (*http.Request, error) {
		return nil, errSeam
	}
	t.Cleanup(func() { httpNewRequestWithContext = orig })
	b := &NeonBackend{apiKey: "k", client: &http.Client{}, apiBase: "http://x"}
	if err := b.Deprovision(context.Background(), "tok", "p1"); err == nil {
		t.Error("expected new-request error")
	}
}

func TestNeon_Deprovision_HTTPError(t *testing.T) {
	b := &NeonBackend{apiKey: "k", client: &http.Client{}, apiBase: "http://127.0.0.1:1"}
	if err := b.Deprovision(context.Background(), "tok", "p1"); err == nil {
		t.Error("expected http error")
	}
}

func TestNeon_Deprovision_Non2xx(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusBadRequest)
	})
	if err := b.Deprovision(context.Background(), "tok", "p1"); err == nil {
		t.Error("expected non-2xx error")
	}
}

func TestNeon_Regrade_NoOp(t *testing.T) {
	b := &NeonBackend{}
	res, err := b.Regrade(context.Background(), "tok", "p1", 5)
	if err != nil || res.Applied {
		t.Errorf("Regrade = %+v, %v; want no-op skip", res, err)
	}
}

func TestNeon_findProjectByName_NewRequestError(t *testing.T) {
	orig := httpNewRequestWithContext
	httpNewRequestWithContext = func(context.Context, string, string, io.Reader) (*http.Request, error) {
		return nil, errSeam
	}
	t.Cleanup(func() { httpNewRequestWithContext = orig })
	b := &NeonBackend{apiKey: "k", client: &http.Client{}, apiBase: "http://x"}
	if _, err := b.findProjectByName(context.Background(), "n"); err == nil {
		t.Error("expected new-request error")
	}
}

func TestNeon_findProjectByName_HTTPError(t *testing.T) {
	b := &NeonBackend{apiKey: "k", client: &http.Client{}, apiBase: "http://127.0.0.1:1"}
	if _, err := b.findProjectByName(context.Background(), "n"); err == nil {
		t.Error("expected http error")
	}
}

func TestNeon_findProjectByName_Non2xx(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusForbidden)
	})
	if _, err := b.findProjectByName(context.Background(), "n"); err == nil {
		t.Error("expected non-2xx error")
	}
}

func TestNeon_findProjectByName_ReadBodyError(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("{}")) })
	orig := ioReadAll
	ioReadAll = func(io.Reader) ([]byte, error) { return nil, errSeam }
	t.Cleanup(func() { ioReadAll = orig })
	if _, err := b.findProjectByName(context.Background(), "n"); err == nil {
		t.Error("expected read-body error")
	}
}

func TestNeon_findProjectByName_UnmarshalError(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("xx")) })
	if _, err := b.findProjectByName(context.Background(), "n"); err == nil {
		t.Error("expected unmarshal error")
	}
}

func TestNeon_findProjectByName_NotFoundReturnsEmpty(t *testing.T) {
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"projects": []map[string]string{{"id": "other", "name": "instant-zzz"}},
		})
	})
	id, err := b.findProjectByName(context.Background(), "instant-target")
	if err != nil || id != "" {
		t.Errorf("findProjectByName = %q, %v; want empty, nil", id, err)
	}
}

// Provision lookup-error path: findProjectByName errors → Provision logs and
// proceeds to create.
func TestNeon_Provision_LookupError_ProceedsToCreate(t *testing.T) {
	var created bool
	b := neonTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			http.Error(w, "list-fail", http.StatusInternalServerError)
			return
		}
		created = true
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project":         map[string]string{"id": "p9"},
			"connection_uris": []map[string]string{{"connection_uri": "postgres://u"}},
		})
	})
	if _, err := b.Provision(context.Background(), "tok", "team", -1); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if !created {
		t.Error("expected create to proceed after lookup error")
	}
}

var _ = errors.New
