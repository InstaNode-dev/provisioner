package postgres

// dedicated_live_test.go — coverage for the DedicatedProvider's local-admin
// (non-Neon) path. Skipped unless a real Postgres admin DSN is configured.

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDedicatedProvider_Local_FullLifecycle(t *testing.T) {
	adminDSN := testAdminDSN()
	if adminDSN == "" {
		t.Skip("admin DSN unset — skipping dedicated local lifecycle")
	}
	p := NewDedicatedProvider(adminDSN, "")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	token := fmt.Sprintf("ded%d", time.Now().UnixNano())
	dbName := "dedicated_db_" + token
	username := "dedicated_usr_" + token
	t.Cleanup(func() { cleanupPGObjects(t, adminDSN, []string{dbName}, []string{username}) })

	creds, err := p.Provision(ctx, token, "team", -1)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if creds.DatabaseName != dbName || creds.Username != username {
		t.Errorf("Provision creds = %+v; want db=%q user=%q", creds, dbName, username)
	}
	if creds.ProviderResourceID != "" {
		t.Errorf("ProviderResourceID = %q; want empty for local-admin path", creds.ProviderResourceID)
	}

	size, err := p.StorageBytes(ctx, token, "")
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	if size <= 0 {
		t.Errorf("StorageBytes = %d; want > 0", size)
	}

	if err := p.Deprovision(ctx, token, ""); err != nil {
		t.Fatalf("Deprovision: %v", err)
	}
	// StorageBytes after deprovision must error (DB gone).
	if _, err := p.StorageBytes(ctx, token, ""); err == nil {
		t.Errorf("StorageBytes after deprovision returned nil; want error")
	}
}

func TestDedicatedProvider_Local_ConnectErrors(t *testing.T) {
	p := NewDedicatedProvider("postgres://u:p@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1", "")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := p.Provision(ctx, "tok", "team", -1); err == nil {
		t.Error("Provision connect-fail returned nil; want error")
	}
	if _, err := p.StorageBytes(ctx, "tok", ""); err == nil {
		t.Error("StorageBytes connect-fail returned nil; want error")
	}
	if err := p.Deprovision(ctx, "tok", ""); err == nil {
		t.Error("Deprovision connect-fail returned nil; want error")
	}
}

func TestDedicatedProvider_Local_DuplicateProvisionFails(t *testing.T) {
	adminDSN := testAdminDSN()
	if adminDSN == "" {
		t.Skip("admin DSN unset")
	}
	p := NewDedicatedProvider(adminDSN, "")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	token := fmt.Sprintf("ded%d", time.Now().UnixNano())
	dbName := "dedicated_db_" + token
	username := "dedicated_usr_" + token
	t.Cleanup(func() { cleanupPGObjects(t, adminDSN, []string{dbName}, []string{username}) })

	if _, err := p.Provision(ctx, token, "team", -1); err != nil {
		t.Fatalf("first Provision: %v", err)
	}
	_, err := p.Provision(ctx, token, "team", -1)
	if err == nil || !strings.Contains(err.Error(), "CREATE DATABASE") {
		t.Errorf("second Provision err = %v; want CREATE DATABASE", err)
	}
}
