package postgres

// local_privilege_test.go — coverage for LocalBackend Deprovision / Regrade
// terminal-error branches that require the admin role to LACK privilege:
//   - DROP DATABASE permission-denied (terminal, non-in-use) → break + return wrap
//   - DROP USER permission-denied → non-fatal log branch
//   - ALTER ROLE permission-denied → Regrade error wrap
//
// Setup: the configured superuser creates a target db + role owned by a third
// role, plus a NON-superuser "warden" admin role that can connect but cannot
// DROP/ALTER them. A LocalBackend pointed at the warden DSN then drives the
// privilege-denied branches against real Postgres.
//
// Skipped unless CUSTOMER_POSTGRES_DSN points at a superuser-capable admin DSN.

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// wardenDSN builds a DSN for a freshly-created non-superuser role on the same
// host/db as the configured admin DSN. Returns the warden DSN, the warden role
// name (for cleanup), and ok=false if no admin DSN is set.
func wardenDSN(t *testing.T, ctx context.Context, adminConn *pgx.Conn, adminDSN, warden, pass string) string {
	t.Helper()
	if _, err := adminConn.Exec(ctx, fmt.Sprintf("CREATE USER %q WITH PASSWORD '%s' LOGIN", warden, pass)); err != nil {
		t.Fatalf("create warden role: %v", err)
	}
	u, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse admin DSN: %v", err)
	}
	u.User = url.UserPassword(warden, pass)
	return u.String()
}

func TestLocalBackend_Deprovision_PermissionDenied_Terminal(t *testing.T) {
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
	token := fmt.Sprintf("priv%d", ts)
	dbName := dbNamePrefix + token
	username := userNamePrefix + token
	warden := fmt.Sprintf("warden%d", ts)

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

	// Superuser creates the target DB + role that the warden cannot touch.
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", dbName)); err != nil {
		t.Fatalf("create target db: %v", err)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE USER %q WITH PASSWORD 'x'", username)); err != nil {
		t.Fatalf("create target role: %v", err)
	}

	wDSN := wardenDSN(t, ctx, admin, adminDSN, warden, "wardenpw")

	// LocalBackend driven by the non-superuser warden. DROP DATABASE on a DB it
	// doesn't own → permission denied (NOT the in-use marker) → terminal branch
	// (313 break, 336 return). DROP USER on a role it can't drop → 332 log.
	b := newLocalBackend(wDSN)
	err = b.Deprovision(ctx, token, "local:0")
	if err == nil {
		t.Fatal("Deprovision as non-privileged warden returned nil; want DROP DATABASE permission error")
	}
	if !strings.Contains(err.Error(), "DROP DATABASE") {
		t.Errorf("err = %v; want DROP DATABASE wrap", err)
	}
}

func TestLocalBackend_Regrade_PermissionDenied(t *testing.T) {
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
	token := fmt.Sprintf("privrg%d", ts)
	username := userNamePrefix + token
	warden := fmt.Sprintf("wardenrg%d", ts)

	t.Cleanup(func() {
		c, err := pgx.Connect(context.Background(), adminDSN)
		if err != nil {
			return
		}
		defer c.Close(context.Background()) //nolint:errcheck
		_, _ = c.Exec(context.Background(), fmt.Sprintf(`DROP USER IF EXISTS %q`, username))
		_, _ = c.Exec(context.Background(), fmt.Sprintf(`DROP USER IF EXISTS %q`, warden))
	})

	if _, err := admin.Exec(ctx, fmt.Sprintf("CREATE USER %q WITH PASSWORD 'x'", username)); err != nil {
		t.Fatalf("create target role: %v", err)
	}
	wDSN := wardenDSN(t, ctx, admin, adminDSN, warden, "wardenpw")

	// ALTER ROLE on a role the warden lacks privilege to modify → error wrap (374).
	b := newLocalBackend(wDSN)
	res, err := b.Regrade(ctx, token, "local:0", 8)
	if err == nil {
		t.Fatal("Regrade as non-privileged warden returned nil; want ALTER ROLE permission error")
	}
	if res.Applied {
		t.Errorf("Applied=true after permission error; want false")
	}
	if !strings.Contains(err.Error(), "ALTER ROLE") {
		t.Errorf("err = %v; want ALTER ROLE wrap", err)
	}
}
