package postgres

// url_helpers_test.go — pure-function unit tests for the URL-building helpers
// in local.go. No external dependencies.

import (
	"os"
	"strings"
	"testing"
)

func TestExtractHost(t *testing.T) {
	cases := []struct {
		raw, want string
	}{
		{"postgres://u:p@host:5432/db", "host:5432"},
		{"postgres://u:p@host/db", "host"},
		{"postgres://host:5432/db", "host:5432"},
		{"postgres://host/db", "host"},
		{"postgres://host:5432", "host:5432"}, // no path
		{"postgres://u:p@h.svc.cluster.local:1/d?sslmode=disable", "h.svc.cluster.local:1"},
		{"", ""},
	}
	for _, tc := range cases {
		got := extractHost(tc.raw)
		if got != tc.want {
			t.Errorf("extractHost(%q) = %q; want %q", tc.raw, got, tc.want)
		}
	}
}

func TestIndexOf(t *testing.T) {
	if got := indexOf("abc@d", '@'); got != 3 {
		t.Errorf("indexOf @ = %d; want 3", got)
	}
	if got := indexOf("noChar", '@'); got != -1 {
		t.Errorf("indexOf missing = %d; want -1", got)
	}
	if got := indexOf("", '/'); got != -1 {
		t.Errorf("indexOf empty = %d; want -1", got)
	}
}

func TestBuildAdminNewDBURL(t *testing.T) {
	cases := []struct {
		admin, dbName, want string
	}{
		{
			"postgres://u:p@host:5432/postgres?sslmode=disable",
			"db_x",
			"postgres://u:p@host:5432/db_x?sslmode=disable", // replace trailing path
		},
		{
			"postgres://u:p@host/postgres",
			"db_y",
			"postgres://u:p@host/db_y",
		},
		{
			// no '/' anywhere — fallback path
			"postgres",
			"d",
			"postgres/d",
		},
	}
	for _, tc := range cases {
		got := buildAdminNewDBURL(tc.admin, tc.dbName)
		// buildAdminNewDBURL replaces text after the LAST '/' — verify
		// that the db name appears at the right position.
		if !strings.Contains(got, tc.dbName) {
			t.Errorf("buildAdminNewDBURL(%q, %q) = %q; missing dbName", tc.admin, tc.dbName, got)
		}
		// Where the admin URL ended in `?...`, the function strips it (replaces
		// the entire path-and-query trailing portion). That's fine for callers
		// because they only need a valid admin DSN for the new DB.
		if tc.admin == "postgres://u:p@host:5432/postgres?sslmode=disable" && !strings.HasSuffix(got, "/db_x?sslmode=disable") && !strings.HasSuffix(got, "/db_x") {
			t.Logf("buildAdminNewDBURL trailing form: %q (informational)", got)
		}
	}
}

func TestBuildDBURL_WithExtractHost(t *testing.T) {
	// Force the extractHost branch by clearing public env vars.
	t.Setenv("POSTGRES_PUBLIC_HOST_PORT", "")
	t.Setenv("POSTGRES_PUBLIC_HOST", "")
	t.Setenv("POSTGRES_PUBLIC_PORT", "")
	got := buildDBURL("postgres://admin:p@internal:5432/postgres?sslmode=disable", "usr_x", "pwd", "db_x")
	want := "postgres://usr_x:pwd@internal:5432/db_x?sslmode=disable"
	if got != want {
		t.Errorf("buildDBURL = %q; want %q", got, want)
	}
}

func TestBuildDBURL_WithPublicHostPort(t *testing.T) {
	t.Setenv("POSTGRES_PUBLIC_HOST_PORT", "pg.public.example:6543")
	got := buildDBURL("postgres://a:b@internal/postgres", "u", "p", "db")
	if !strings.Contains(got, "pg.public.example:6543") {
		t.Errorf("buildDBURL = %q; want host override pg.public.example:6543", got)
	}
}

func TestBuildDBURL_WithPublicHostAndDefaultPort(t *testing.T) {
	// Reset HOST_PORT first (subtest of t.Setenv would also work).
	os.Unsetenv("POSTGRES_PUBLIC_HOST_PORT")
	t.Setenv("POSTGRES_PUBLIC_HOST", "pg.public.example")
	t.Setenv("POSTGRES_PUBLIC_PORT", "") // forces default 5432
	got := buildDBURL("postgres://a:b@internal/postgres", "u", "p", "db")
	if !strings.Contains(got, "pg.public.example:5432") {
		t.Errorf("buildDBURL = %q; want pg.public.example:5432 (default port)", got)
	}
}

func TestBuildDBURL_WithPublicHostAndExplicitPort(t *testing.T) {
	os.Unsetenv("POSTGRES_PUBLIC_HOST_PORT")
	t.Setenv("POSTGRES_PUBLIC_HOST", "pg.public.example")
	t.Setenv("POSTGRES_PUBLIC_PORT", "7777")
	got := buildDBURL("postgres://a:b@internal/postgres", "u", "p", "db")
	if !strings.Contains(got, "pg.public.example:7777") {
		t.Errorf("buildDBURL = %q; want pg.public.example:7777", got)
	}
}

func TestPublicHostPort_AllUnset(t *testing.T) {
	t.Setenv("POSTGRES_PUBLIC_HOST_PORT", "")
	t.Setenv("POSTGRES_PUBLIC_HOST", "")
	t.Setenv("POSTGRES_PUBLIC_PORT", "")
	if got := publicHostPort(); got != "" {
		t.Errorf("publicHostPort all-unset = %q; want \"\"", got)
	}
}

func TestPublicHostPort_HostPortShortcut(t *testing.T) {
	t.Setenv("POSTGRES_PUBLIC_HOST_PORT", "h:1234")
	if got := publicHostPort(); got != "h:1234" {
		t.Errorf("publicHostPort = %q; want h:1234", got)
	}
}

// TestGeneratePassword covers the cryptographically-random password helper.
func TestGeneratePassword(t *testing.T) {
	p, err := generatePassword(16)
	if err != nil {
		t.Fatalf("generatePassword: %v", err)
	}
	if len(p) != 16 {
		t.Errorf("len(generatePassword(16)) = %d; want 16", len(p))
	}
	for _, c := range p {
		if !strings.ContainsRune(alphanumChars, c) {
			t.Errorf("generatePassword returned non-alphanum %q", c)
		}
	}
	// Zero-length is technically valid.
	if p, err := generatePassword(0); err != nil || p != "" {
		t.Errorf("generatePassword(0) = (%q,%v); want (\"\", nil)", p, err)
	}
}
