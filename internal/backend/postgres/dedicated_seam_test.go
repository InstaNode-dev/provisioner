package postgres

// dedicated_seam_test.go — coverage for dedicated.go: the Neon-API path
// (provision/storage/deprovision + every error wrap) and the local-admin path
// (driven via the pgConn seam).

import (
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
)

func dedicatedNeonProvider(t *testing.T, h http.HandlerFunc) *DedicatedProvider {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &DedicatedProvider{neonAPIKey: "key", neonBaseURL: srv.URL, httpClient: srv.Client()}
}

func TestNewDedicatedProvider_Defaults(t *testing.T) {
	p := NewDedicatedProvider("dsn", "key")
	if p.adminDSN != "dsn" || p.neonAPIKey != "key" || p.neonBaseURL != neonAPIBase || p.httpClient == nil {
		t.Errorf("provider = %+v", p)
	}
}

func TestDedicated_Provision_NeonSuccess(t *testing.T) {
	p := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project":         map[string]string{"id": "dp1"},
			"connection_uris": []map[string]string{{"connection_uri": "postgres://d"}},
		})
	})
	creds, err := p.Provision(context.Background(), "this-is-a-very-long-token-1234567890", "team", -1)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if creds.ProviderResourceID != "dp1" {
		t.Errorf("PRID = %q", creds.ProviderResourceID)
	}
}

func TestDedicated_ProvisionNeon_MarshalError(t *testing.T) {
	p := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) {})
	orig := jsonMarshal
	jsonMarshal = func(any) ([]byte, error) { return nil, errSeam }
	t.Cleanup(func() { jsonMarshal = orig })
	if _, err := p.Provision(context.Background(), "t", "team", -1); err == nil {
		t.Error("expected marshal error")
	}
}

func TestDedicated_ProvisionNeon_NewRequestError(t *testing.T) {
	p := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) {})
	orig := httpNewRequestWithContext
	httpNewRequestWithContext = func(context.Context, string, string, io.Reader) (*http.Request, error) {
		return nil, errSeam
	}
	t.Cleanup(func() { httpNewRequestWithContext = orig })
	if _, err := p.Provision(context.Background(), "t", "team", -1); err == nil {
		t.Error("expected new-request error")
	}
}

func TestDedicated_ProvisionNeon_HTTPError(t *testing.T) {
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: "http://127.0.0.1:1", httpClient: &http.Client{}}
	if _, err := p.Provision(context.Background(), "t", "team", -1); err == nil {
		t.Error("expected http error")
	}
}

func TestDedicated_ProvisionNeon_ReadBodyError(t *testing.T) {
	p := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("{}"))
	})
	orig := ioReadAll
	ioReadAll = func(io.Reader) ([]byte, error) { return nil, errSeam }
	t.Cleanup(func() { ioReadAll = orig })
	if _, err := p.Provision(context.Background(), "t", "team", -1); err == nil {
		t.Error("expected read-body error")
	}
}

func TestDedicated_ProvisionNeon_Non2xx(t *testing.T) {
	p := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	if _, err := p.Provision(context.Background(), "t", "team", -1); err == nil {
		t.Error("expected non-2xx error")
	}
}

func TestDedicated_ProvisionNeon_UnmarshalError(t *testing.T) {
	p := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("xx"))
	})
	if _, err := p.Provision(context.Background(), "t", "team", -1); err == nil {
		t.Error("expected unmarshal error")
	}
}

func TestDedicated_ProvisionNeon_EmptyProjectID(t *testing.T) {
	p := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"project": map[string]string{"id": ""}})
	})
	if _, err := p.Provision(context.Background(), "t", "team", -1); err == nil {
		t.Error("expected empty-id error")
	}
}

func TestDedicated_ProvisionNeon_NoConnectionURI(t *testing.T) {
	p := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{"project": map[string]string{"id": "x"}})
	})
	if _, err := p.Provision(context.Background(), "t", "team", -1); err == nil {
		t.Error("expected no-uri error")
	}
}

func TestDedicated_StorageBytes_Neon(t *testing.T) {
	p := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"project": map[string]any{"usage": map[string]any{"data_storage_bytes_hour": 7}},
		})
	})
	got, err := p.StorageBytes(context.Background(), "tok", "p1")
	if err != nil || got != 7 {
		t.Errorf("StorageBytes = %d, %v", got, err)
	}
}

func TestDedicated_neonStorageBytes_ErrorBranches(t *testing.T) {
	// empty PRID
	p := &DedicatedProvider{neonAPIKey: "k", httpClient: &http.Client{}, neonBaseURL: "http://x"}
	if _, err := p.neonStorageBytes(context.Background(), ""); err == nil {
		t.Error("expected empty-PRID error")
	}

	// new-request error
	orig := httpNewRequestWithContext
	httpNewRequestWithContext = func(context.Context, string, string, io.Reader) (*http.Request, error) {
		return nil, errSeam
	}
	if _, err := p.neonStorageBytes(context.Background(), "p1"); err == nil {
		t.Error("expected new-request error")
	}
	httpNewRequestWithContext = orig

	// http error
	p2 := &DedicatedProvider{neonAPIKey: "k", httpClient: &http.Client{}, neonBaseURL: "http://127.0.0.1:1"}
	if _, err := p2.neonStorageBytes(context.Background(), "p1"); err == nil {
		t.Error("expected http error")
	}

	// non-2xx
	p3 := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusNotFound)
	})
	if _, err := p3.neonStorageBytes(context.Background(), "p1"); err == nil {
		t.Error("expected non-2xx error")
	}

	// read-body error
	p4 := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("{}")) })
	io2 := ioReadAll
	ioReadAll = func(io.Reader) ([]byte, error) { return nil, errSeam }
	if _, err := p4.neonStorageBytes(context.Background(), "p1"); err == nil {
		t.Error("expected read-body error")
	}
	ioReadAll = io2

	// unmarshal error
	p5 := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("xx")) })
	if _, err := p5.neonStorageBytes(context.Background(), "p1"); err == nil {
		t.Error("expected unmarshal error")
	}
}

func TestDedicated_Deprovision_Neon(t *testing.T) {
	p := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	if err := p.Deprovision(context.Background(), "tok", "p1"); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
}

func TestDedicated_deprovisionNeon_ErrorBranches(t *testing.T) {
	p := &DedicatedProvider{neonAPIKey: "k", httpClient: &http.Client{}, neonBaseURL: "http://x"}
	if err := p.deprovisionNeon(context.Background(), "tok", ""); err == nil {
		t.Error("expected empty-PRID error")
	}

	orig := httpNewRequestWithContext
	httpNewRequestWithContext = func(context.Context, string, string, io.Reader) (*http.Request, error) {
		return nil, errSeam
	}
	if err := p.deprovisionNeon(context.Background(), "tok", "p1"); err == nil {
		t.Error("expected new-request error")
	}
	httpNewRequestWithContext = orig

	p2 := &DedicatedProvider{neonAPIKey: "k", httpClient: &http.Client{}, neonBaseURL: "http://127.0.0.1:1"}
	if err := p2.deprovisionNeon(context.Background(), "tok", "p1"); err == nil {
		t.Error("expected http error")
	}

	p3 := dedicatedNeonProvider(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no", http.StatusBadRequest)
	})
	if err := p3.deprovisionNeon(context.Background(), "tok", "p1"); err == nil {
		t.Error("expected non-2xx error")
	}
}

func TestDedicated_Regrade_NoOp(t *testing.T) {
	p := &DedicatedProvider{}
	res, err := p.Regrade(context.Background(), "tok", "p1", 5)
	if err != nil || res.Applied {
		t.Errorf("Regrade = %+v, %v", res, err)
	}
}

// --- local-admin path (neonAPIKey == "") ---

func TestDedicated_localAdminDSN_Fallback(t *testing.T) {
	p := &DedicatedProvider{}
	if p.localAdminDSN() != defaultCustomersURL {
		t.Errorf("localAdminDSN = %q; want default", p.localAdminDSN())
	}
	p2 := &DedicatedProvider{adminDSN: "postgres://a@h/x"}
	if p2.localAdminDSN() != "postgres://a@h/x" {
		t.Errorf("localAdminDSN = %q", p2.localAdminDSN())
	}
}

func TestDedicated_ProvisionLocal_Success(t *testing.T) {
	fc := &fakePGConn{}
	withPGXConnect(t, fc, nil)
	p := &DedicatedProvider{adminDSN: "postgres://a:b@h:5432/postgres"}
	creds, err := p.Provision(context.Background(), "tok", "team", -1)
	if err != nil {
		t.Fatalf("Provision local: %v", err)
	}
	if creds.DatabaseName != "dedicated_db_tok" || creds.Username != "dedicated_usr_tok" {
		t.Errorf("creds = %+v", creds)
	}
}

func TestDedicated_ProvisionLocal_GenPasswordError(t *testing.T) {
	origRI := randInt
	randInt = func(_ io.Reader, _ *big.Int) (*big.Int, error) { return nil, errSeam }
	t.Cleanup(func() { randInt = origRI })
	p := &DedicatedProvider{adminDSN: "x"}
	if _, err := p.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("expected generatePassword error")
	}
}

func TestDedicated_ProvisionLocal_CloseErrors(t *testing.T) {
	// closeErr on both the admin conn and the new-db conn → both deferred-close
	// error-log branches run; Provision still succeeds.
	fc := &fakePGConn{closeErr: errSeam}
	withPGXConnect(t, fc, nil)
	p := &DedicatedProvider{adminDSN: "x"}
	if _, err := p.Provision(context.Background(), "tok", "team", -1); err != nil {
		t.Errorf("Close errors must be non-fatal: %v", err)
	}
}

func TestDedicated_localStorageBytes_CloseError(t *testing.T) {
	fc := &fakePGConn{scanInt64: 1, closeErr: errSeam}
	withPGXConnect(t, fc, nil)
	p := &DedicatedProvider{adminDSN: "x"}
	if _, err := p.StorageBytes(context.Background(), "tok", ""); err != nil {
		t.Errorf("Close error must be non-fatal: %v", err)
	}
}

func TestDedicated_DeprovisionLocal_CloseError(t *testing.T) {
	fc := &fakePGConn{closeErr: errSeam}
	withPGXConnect(t, fc, nil)
	p := &DedicatedProvider{adminDSN: "x"}
	if err := p.Deprovision(context.Background(), "tok", ""); err != nil {
		t.Errorf("Close error must be non-fatal: %v", err)
	}
}

func TestDedicated_ProvisionLocal_ConnectError(t *testing.T) {
	withPGXConnect(t, nil, errSeam)
	p := &DedicatedProvider{adminDSN: "x"}
	if _, err := p.Provision(context.Background(), "tok", "team", -1); err == nil {
		t.Error("expected connect error")
	}
}

func TestDedicated_ProvisionLocal_ExecErrorBranches(t *testing.T) {
	for _, sub := range []string{"CREATE DATABASE", "CREATE USER", "GRANT ALL PRIVILEGES ON DATABASE"} {
		fc := &fakePGConn{execErrFor: map[string]error{sub: errSeam}}
		withPGXConnect(t, fc, nil)
		p := &DedicatedProvider{adminDSN: "x"}
		if _, err := p.Provision(context.Background(), "tok", "team", -1); err == nil {
			t.Errorf("expected error when %q fails", sub)
		}
	}
}

func TestDedicated_ProvisionLocal_SchemaGrantNonFatal(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{"GRANT ALL ON SCHEMA": errSeam}}
	withPGXConnect(t, fc, nil)
	p := &DedicatedProvider{adminDSN: "x"}
	if _, err := p.Provision(context.Background(), "tok", "team", -1); err != nil {
		t.Errorf("schema-grant failure must be non-fatal: %v", err)
	}
}

func TestDedicated_ProvisionLocal_NewDBConnectError_NonFatal(t *testing.T) {
	var calls int
	fc := &fakePGConn{}
	withPGXConnectFunc(t, func(ctx context.Context, dsn string) (pgConn, error) {
		calls++
		if calls == 2 {
			return nil, errSeam
		}
		return fc, nil
	})
	p := &DedicatedProvider{adminDSN: "x"}
	if _, err := p.Provision(context.Background(), "tok", "team", -1); err != nil {
		t.Errorf("new-db connect failure must be non-fatal: %v", err)
	}
}

func TestDedicated_localStorageBytes(t *testing.T) {
	fc := &fakePGConn{scanInt64: 555}
	withPGXConnect(t, fc, nil)
	p := &DedicatedProvider{adminDSN: "x"}
	got, err := p.StorageBytes(context.Background(), "tok", "")
	if err != nil || got != 555 {
		t.Errorf("StorageBytes = %d, %v", got, err)
	}
}

func TestDedicated_localStorageBytes_ConnectError(t *testing.T) {
	withPGXConnect(t, nil, errSeam)
	p := &DedicatedProvider{adminDSN: "x"}
	if _, err := p.StorageBytes(context.Background(), "tok", ""); err == nil {
		t.Error("expected connect error")
	}
}

func TestDedicated_localStorageBytes_ScanError(t *testing.T) {
	fc := &fakePGConn{queryRowErr: errSeam}
	withPGXConnect(t, fc, nil)
	p := &DedicatedProvider{adminDSN: "x"}
	if _, err := p.StorageBytes(context.Background(), "tok", ""); err == nil {
		t.Error("expected scan error")
	}
}

func TestDedicated_DeprovisionLocal_Success(t *testing.T) {
	fc := &fakePGConn{}
	withPGXConnect(t, fc, nil)
	p := &DedicatedProvider{adminDSN: "x"}
	if err := p.Deprovision(context.Background(), "tok", ""); err != nil {
		t.Fatalf("Deprovision local: %v", err)
	}
}

func TestDedicated_DeprovisionLocal_ConnectError(t *testing.T) {
	withPGXConnect(t, nil, errSeam)
	p := &DedicatedProvider{adminDSN: "x"}
	if err := p.Deprovision(context.Background(), "tok", ""); err == nil {
		t.Error("expected connect error")
	}
}

func TestDedicated_DeprovisionLocal_TerminateAndDropUserNonFatal(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{
		"pg_terminate_backend": errSeam,
		"DROP USER":            errSeam,
	}}
	withPGXConnect(t, fc, nil)
	p := &DedicatedProvider{adminDSN: "x"}
	if err := p.Deprovision(context.Background(), "tok", ""); err != nil {
		t.Errorf("terminate/drop-user failures must be non-fatal: %v", err)
	}
}

func TestDedicated_DeprovisionLocal_DropDBError(t *testing.T) {
	fc := &fakePGConn{execErrFor: map[string]error{"DROP DATABASE": errSeam}}
	withPGXConnect(t, fc, nil)
	p := &DedicatedProvider{adminDSN: "x"}
	if err := p.Deprovision(context.Background(), "tok", ""); err == nil {
		t.Error("expected DROP DATABASE error")
	}
}
