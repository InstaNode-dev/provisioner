package postgres

// dedicated_error_test.go — coverage for DedicatedProvider error branches not
// reached by the happy-path lifecycle / neon_http tests.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestDedicatedProvider_NeonProvision_BadJSON covers provisionNeon's unmarshal
// error branch (2xx status but malformed body).
func TestDedicatedProvider_NeonProvision_BadJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{not json"))
	}))
	defer srv.Close()
	p := &DedicatedProvider{neonAPIKey: "k", neonBaseURL: srv.URL, httpClient: srv.Client()}
	if _, err := p.Provision(context.Background(), "tok", "team", -1); err == nil ||
		!strings.Contains(err.Error(), "unmarshal") {
		t.Errorf("Provision bad-json err = %v; want unmarshal wrap", err)
	}
}

// TestDedicatedProvider_Local_CreateUserFails covers provisionLocal's CREATE
// USER error branch: pre-create the role so the second statement fails.
func TestDedicatedProvider_Local_CreateUserFails(t *testing.T) {
	adminDSN := testAdminDSN()
	if adminDSN == "" {
		t.Skip("admin DSN unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	token := fmt.Sprintf("dederr%d", time.Now().UnixNano())
	dbName := "dedicated_db_" + token
	username := "dedicated_usr_" + token
	t.Cleanup(func() { cleanupPGObjects(t, adminDSN, []string{dbName}, []string{username}) })

	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("setup connect: %v", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE USER %q WITH PASSWORD 'x'", username)); err != nil {
		conn.Close(ctx) //nolint:errcheck
		t.Fatalf("pre-create role: %v", err)
	}
	conn.Close(ctx) //nolint:errcheck

	p := NewDedicatedProvider(adminDSN, "")
	if _, err := p.Provision(ctx, token, "team", -1); err == nil ||
		!strings.Contains(err.Error(), "CREATE USER") {
		t.Errorf("Provision err = %v; want CREATE USER wrap", err)
	}
}

// TestDedicatedProvider_Local_DeprovisionRoleOnly covers deprovisionLocal when
// the database never existed but the role does: DROP DATABASE IF EXISTS no-ops,
// DROP USER runs, returns nil.
func TestDedicatedProvider_Local_DeprovisionRoleOnly(t *testing.T) {
	adminDSN := testAdminDSN()
	if adminDSN == "" {
		t.Skip("admin DSN unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	token := fmt.Sprintf("dedro%d", time.Now().UnixNano())
	username := "dedicated_usr_" + token
	t.Cleanup(func() { cleanupPGObjects(t, adminDSN, nil, []string{username}) })

	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("setup connect: %v", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE USER %q WITH PASSWORD 'x'", username)); err != nil {
		conn.Close(ctx) //nolint:errcheck
		t.Fatalf("pre-create role: %v", err)
	}
	conn.Close(ctx) //nolint:errcheck

	p := NewDedicatedProvider(adminDSN, "")
	if err := p.Deprovision(ctx, token, ""); err != nil {
		t.Errorf("Deprovision role-only = %v; want nil", err)
	}
}

// TestDedicatedProvider_Local_DeprovisionPermissionDenied covers deprovisionLocal's
// terminate-error and DROP DATABASE permission-denied branches: a non-superuser
// "warden" admin cannot terminate backends on, or DROP, a database it doesn't own.
func TestDedicatedProvider_Local_DeprovisionPermissionDenied(t *testing.T) {
	adminDSN := testAdminDSN()
	if adminDSN == "" {
		t.Skip("admin DSN unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer admin.Close(ctx) //nolint:errcheck

	ts := time.Now().UnixNano()
	token := fmt.Sprintf("dedpriv%d", ts)
	dbName := "dedicated_db_" + token
	username := "dedicated_usr_" + token
	warden := fmt.Sprintf("dedwarden%d", ts)

	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), adminDSN)
		if err != nil {
			return
		}
		defer c.Close(context.Background()) //nolint:errcheck
		_, _ = c.Exec(context.Background(), fmt.Sprintf(`DROP DATABASE IF EXISTS %q WITH (FORCE)`, dbName))
		_, _ = c.Exec(context.Background(), fmt.Sprintf(`DROP USER IF EXISTS %q`, username))
		_, _ = c.Exec(context.Background(), fmt.Sprintf(`DROP USER IF EXISTS %q`, warden))
	})

	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", dbName)); err != nil {
		t.Fatalf("create db: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE USER %q WITH PASSWORD 'x'", username)); err != nil {
		t.Fatalf("create role: %v", err)
	}
	wDSN := wardenDSN(t, ctx, admin, adminDSN, warden, "wardenpw")

	p := NewDedicatedProvider(wDSN, "")
	if err := p.Deprovision(ctx, token, ""); err == nil ||
		!strings.Contains(err.Error(), "DROP DATABASE") {
		t.Errorf("Deprovision as warden err = %v; want DROP DATABASE permission wrap", err)
	}
}
