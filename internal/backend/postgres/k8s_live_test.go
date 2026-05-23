package postgres

// k8s_live_test.go — deep-path coverage for K8sBackend.initDatabase /
// StorageBytes / Regrade against a REAL Postgres reachable at the address the
// fake Service's ClusterIP points at.
//
// The fake clientset can model the Namespace/Secret/Service objects but cannot
// stand up a Postgres pod. We close that gap by pointing the fake "postgres"
// Service's ClusterIP at the developer/CI Postgres on 127.0.0.1 and seeding the
// "postgres-admin" Secret with that cluster's real admin credentials. The
// synthesized DSN K8sBackend builds (postgres://USER:PASS@CLUSTERIP:5432/postgres)
// then connects for real, exercising the query/exec bodies that the
// unreachable-ClusterIP tests can only stub up to the connect call.
//
// Skipped unless CUSTOMER_POSTGRES_DSN (or TEST_POSTGRES_ADMIN_DSN) is set.

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// parseAdminDSN splits the configured admin DSN into the pieces K8sBackend needs
// to synthesize its own DSN: the host:port (used as the fake Service ClusterIP +
// port) and the user/password (seeded into the fake Secret). Returns ok=false
// when no admin DSN is configured.
func parseAdminDSN(t *testing.T) (host, port, user, pass string, ok bool) {
	t.Helper()
	dsn := testAdminDSN()
	if dsn == "" {
		return "", "", "", "", false
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse admin DSN %q: %v", dsn, err)
	}
	host = u.Hostname()
	port = u.Port()
	if port == "" {
		port = "5432"
	}
	user = u.User.Username()
	pass, _ = u.User.Password()
	return host, port, user, pass, true
}

// newReachablePostgresService returns a "postgres" Service whose ClusterIP is the
// real Postgres host. K8sBackend dials ClusterIP:5432, so the configured port is
// only honoured when it is the conventional 5432; for non-5432 dev ports we fold
// the port into the ClusterIP via the standard host string and let pgx parse it
// — but K8sBackend hardcodes :5432, so this test family requires Postgres on
// 5432 (the documented CUSTOMER_POSTGRES_DSN). We assert that precondition.
func newReachablePostgresService(host string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres"},
		Spec: corev1.ServiceSpec{
			ClusterIP: host,
			Ports:     []corev1.ServicePort{{Port: 5432}},
		},
	}
}

// seedK8sResource creates the Namespace + postgres-admin Secret + reachable
// postgres Service that StorageBytes/Regrade resolve before connecting.
func seedK8sResource(t *testing.T, b *K8sBackend, ns, host, user, pass string) {
	t.Helper()
	ctx := context.Background()
	if err := b.applyNamespace(ctx, ns); err != nil {
		t.Fatalf("applyNamespace: %v", err)
	}
	// Seed the Secret via Data (raw bytes) rather than applyAdminSecret's
	// StringData: the fake clientset — unlike a real apiserver — does NOT convert
	// StringData→Data on write, and StorageBytes/Regrade read secret.Data. Using
	// StringData here would leave Data nil and the synthesized DSN would carry an
	// empty user/password (auth would fail against real Postgres).
	if _, err := b.cs.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "postgres-admin"},
		Data: map[string][]byte{
			"POSTGRES_USER":     []byte(user),
			"POSTGRES_PASSWORD": []byte(pass),
		},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create postgres-admin secret: %v", err)
	}
	if _, err := b.cs.CoreV1().Services(ns).Create(ctx, newReachablePostgresService(host), metav1.CreateOptions{}); err != nil {
		t.Fatalf("create postgres service: %v", err)
	}
}

// requirePort5432 skips the test when the configured Postgres is not on 5432,
// because K8sBackend hardcodes :5432 in its synthesized DSN.
func requirePort5432(t *testing.T, port string) {
	t.Helper()
	if port != "5432" {
		t.Skipf("K8sBackend hardcodes :5432; configured Postgres port is %s — skipping", port)
	}
}

// TestK8sBackend_InitDatabase_Live drives initDatabase end-to-end against a real
// Postgres: it CREATE USERs, CREATE DATABASEs (OWNER), REVOKEs, GRANTs, and the
// best-effort pgvector step. Then we verify the role + DB actually exist.
func TestK8sBackend_InitDatabase_Live(t *testing.T) {
	host, port, user, pass, ok := parseAdminDSN(t)
	if !ok {
		t.Skip("admin DSN unset — skipping live initDatabase test")
	}
	adminDSN := testAdminDSN()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := &K8sBackend{cs: fake.NewSimpleClientset()}
	tok := uniqueToken(t)
	dbName := "db_k8slive_" + sanitizeForDB(tok)
	appUser := "usr_k8slive_" + sanitizeForDB(tok)
	t.Cleanup(func() { cleanupPGObjects(t, adminDSN, []string{dbName}, []string{appUser}) })

	if err := b.initDatabase(ctx, adminDSN, dbName, appUser, "pw_"+sanitizeForDB(tok), 7); err != nil {
		t.Fatalf("initDatabase: %v", err)
	}

	// Verify the role exists with the connection cap, and the DB exists.
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	var rolconnlimit int
	if err := conn.QueryRow(ctx, "SELECT rolconnlimit FROM pg_roles WHERE rolname=$1", appUser).Scan(&rolconnlimit); err != nil {
		t.Fatalf("role lookup: %v", err)
	}
	if rolconnlimit != 7 {
		t.Errorf("role conn limit = %d; want 7", rolconnlimit)
	}
	var dbExists bool
	if err := conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname=$1)", dbName).Scan(&dbExists); err != nil {
		t.Fatalf("db lookup: %v", err)
	}
	if !dbExists {
		t.Errorf("database %q not created", dbName)
	}

	_ = host
	_ = port
	_ = user
	_ = pass
}

// TestK8sBackend_InitDatabase_ConnectError covers the connect-failure branch of
// initDatabase (the pgx.Connect error wrap), no live cluster needed.
func TestK8sBackend_InitDatabase_ConnectError(t *testing.T) {
	b := &K8sBackend{cs: fake.NewSimpleClientset()}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := b.initDatabase(ctx, "postgres://u:p@127.0.0.1:1/postgres?sslmode=disable&connect_timeout=1", "db_x", "usr_x", "pw", 5)
	if err == nil || !strings.Contains(err.Error(), "connect") {
		t.Fatalf("initDatabase on dead DSN err = %v; want connect wrap", err)
	}
}

// TestK8sBackend_InitDatabase_ExecError covers the per-statement exec-failure
// branch: a duplicate CREATE USER fails because the role already exists.
func TestK8sBackend_InitDatabase_ExecError(t *testing.T) {
	host, port, _, _, ok := parseAdminDSN(t)
	if !ok {
		t.Skip("admin DSN unset")
	}
	requirePort5432(t, port)
	_ = host
	adminDSN := testAdminDSN()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b := &K8sBackend{cs: fake.NewSimpleClientset()}

	tok := uniqueToken(t)
	dbName := "db_k8serr_" + sanitizeForDB(tok)
	appUser := "usr_k8serr_" + sanitizeForDB(tok)
	t.Cleanup(func() { cleanupPGObjects(t, adminDSN, []string{dbName}, []string{appUser}) })

	// Pre-create the role so the first CREATE USER statement fails.
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("setup connect: %v", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE USER %q WITH PASSWORD 'x'", appUser)); err != nil {
		conn.Close(ctx) //nolint:errcheck
		t.Fatalf("pre-create role: %v", err)
	}
	conn.Close(ctx) //nolint:errcheck

	if err := b.initDatabase(ctx, adminDSN, dbName, appUser, "pw", 5); err == nil {
		t.Fatal("initDatabase with duplicate role returned nil; want exec error")
	}
}

// TestK8sBackend_StorageBytes_Live drives the StorageBytes happy path: resolves
// the Secret + Service, connects to the real Postgres at the ClusterIP, and runs
// pg_database_size against the candidate db names.
func TestK8sBackend_StorageBytes_Live(t *testing.T) {
	host, port, user, pass, ok := parseAdminDSN(t)
	if !ok {
		t.Skip("admin DSN unset")
	}
	requirePort5432(t, port)
	adminDSN := testAdminDSN()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := &K8sBackend{cs: fake.NewSimpleClientset()}
	// The token must produce a canonical k8sDBName that actually exists. Create
	// that exact DB so pg_database_size resolves on the first candidate.
	tok := "k8ssb" + sanitizeForDB(uniqueToken(t))
	canonicalDB := k8sDBName(tok)
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("setup connect: %v", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %q", canonicalDB)); err != nil {
		conn.Close(ctx) //nolint:errcheck
		t.Fatalf("create canonical db %q: %v", canonicalDB, err)
	}
	conn.Close(ctx) //nolint:errcheck
	t.Cleanup(func() { cleanupPGObjects(t, adminDSN, []string{canonicalDB}, nil) })

	ns := k8sNsPrefix + tok
	seedK8sResource(t, b, ns, host, user, pass)

	size, err := b.StorageBytes(ctx, tok, ns)
	if err != nil {
		t.Fatalf("StorageBytes: %v", err)
	}
	if size <= 0 {
		t.Errorf("StorageBytes = %d; want > 0 for an existing DB", size)
	}
}

// TestK8sBackend_StorageBytes_Live_AllCandidatesMiss covers the
// "all candidates errored" terminal branch: the DB does not exist under any
// candidate name, so pg_database_size errors and StorageBytes returns the wrap.
func TestK8sBackend_StorageBytes_Live_AllCandidatesMiss(t *testing.T) {
	host, port, user, pass, ok := parseAdminDSN(t)
	if !ok {
		t.Skip("admin DSN unset")
	}
	requirePort5432(t, port)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := &K8sBackend{cs: fake.NewSimpleClientset()}
	tok := "k8smiss" + sanitizeForDB(uniqueToken(t)) // no DB created for this token
	ns := k8sNsPrefix + tok
	seedK8sResource(t, b, ns, host, user, pass)

	if _, err := b.StorageBytes(ctx, tok, ns); err == nil {
		t.Fatal("StorageBytes for nonexistent DB returned nil; want all-candidates error")
	}
}

// TestK8sBackend_Regrade_Live drives the Regrade happy path: resolves
// Secret+Service, connects, and ALTER ROLEs the candidate role. We pre-create
// the canonical role and assert the cap is applied.
func TestK8sBackend_Regrade_Live(t *testing.T) {
	host, port, user, pass, ok := parseAdminDSN(t)
	if !ok {
		t.Skip("admin DSN unset")
	}
	requirePort5432(t, port)
	adminDSN := testAdminDSN()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := &K8sBackend{cs: fake.NewSimpleClientset()}
	tok := "k8srg" + sanitizeForDB(uniqueToken(t))
	canonicalRole := k8sRoleName(tok)
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("setup connect: %v", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE USER %q WITH PASSWORD 'x'", canonicalRole)); err != nil {
		conn.Close(ctx) //nolint:errcheck
		t.Fatalf("create canonical role: %v", err)
	}
	conn.Close(ctx) //nolint:errcheck
	t.Cleanup(func() { cleanupPGObjects(t, adminDSN, nil, []string{canonicalRole}) })

	ns := k8sNsPrefix + tok
	seedK8sResource(t, b, ns, host, user, pass)

	res, err := b.Regrade(ctx, tok, ns, 12)
	if err != nil {
		t.Fatalf("Regrade: %v", err)
	}
	if !res.Applied || res.AppliedConnLimit != 12 {
		t.Errorf("Regrade = %+v; want Applied=true cap=12", res)
	}

	// Verify the cap landed.
	vconn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	defer vconn.Close(ctx) //nolint:errcheck
	var cap int
	if err := vconn.QueryRow(ctx, "SELECT rolconnlimit FROM pg_roles WHERE rolname=$1", canonicalRole).Scan(&cap); err != nil {
		t.Fatalf("cap lookup: %v", err)
	}
	if cap != 12 {
		t.Errorf("role conn limit = %d; want 12", cap)
	}
}

// TestK8sBackend_Regrade_Live_RoleMissingSkips covers the "all role candidates
// errored" branch: the role does not exist, so ALTER ROLE fails for every
// candidate and Regrade returns Applied=false with a skip reason (no error).
func TestK8sBackend_Regrade_Live_RoleMissingSkips(t *testing.T) {
	host, port, user, pass, ok := parseAdminDSN(t)
	if !ok {
		t.Skip("admin DSN unset")
	}
	requirePort5432(t, port)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	b := &K8sBackend{cs: fake.NewSimpleClientset()}
	tok := "k8srgmiss" + sanitizeForDB(uniqueToken(t)) // no role created
	ns := k8sNsPrefix + tok
	seedK8sResource(t, b, ns, host, user, pass)

	res, err := b.Regrade(ctx, tok, ns, 5)
	if err != nil {
		t.Fatalf("Regrade returned err = %v; want nil (skip)", err)
	}
	if res.Applied {
		t.Errorf("Applied=true; want false (role missing on live pod is non-actionable)")
	}
	if !strings.Contains(strings.ToLower(res.SkipReason), "alter role") {
		t.Errorf("SkipReason = %q; want 'alter role' mention", res.SkipReason)
	}
}

// sanitizeForDB lowercases and strips characters that don't belong in a Postgres
// identifier the test builds by hand (the production naming code does its own
// canonicalisation; here we just need a stable, valid suffix).
func sanitizeForDB(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '_':
			b.WriteRune(r)
		}
	}
	out := b.String()
	if len(out) > 40 {
		out = out[:40]
	}
	return out
}
