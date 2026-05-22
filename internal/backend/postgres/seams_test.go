package postgres

// seams_test.go — shared test doubles for the seam-driven coverage tests.
//
// fakePGConn implements the pgConn interface so the SQL happy paths and each
// individual Exec/QueryRow error branch can be exercised in CI without a live
// Postgres cluster (the provisioner coverage job is mock-only). withPGXConnect
// and the rand/json/http overrides install/restore the package seams.

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakePGConn is a programmable pgConn double.
type fakePGConn struct {
	// execErrFor returns a non-nil error when the executed SQL contains the
	// substring key. The first matching key wins. nil → success.
	execErrFor map[string]error
	// queryRowErr is returned by the Row.Scan of any QueryRow call.
	queryRowErr error
	// scanInt64 is written into the first *int64 Scan destination on success.
	scanInt64 int64
	// closeErr is returned by Close.
	closeErr error

	execCalls  []string
	queryCalls []string
	closeCalls int
}

func (f *fakePGConn) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	f.execCalls = append(f.execCalls, sql)
	for sub, err := range f.execErrFor {
		if err != nil && strings.Contains(sql, sub) {
			return pgconn.CommandTag{}, err
		}
	}
	return pgconn.CommandTag{}, nil
}

func (f *fakePGConn) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	f.queryCalls = append(f.queryCalls, sql)
	return &fakeRow{err: f.queryRowErr, v: f.scanInt64}
}

func (f *fakePGConn) Close(_ context.Context) error {
	f.closeCalls++
	return f.closeErr
}

// fakeRow implements pgx.Row.
type fakeRow struct {
	err error
	v   int64
}

func (r *fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) > 0 {
		switch p := dest[0].(type) {
		case *int64:
			*p = r.v
		case *int:
			*p = int(r.v)
		}
	}
	return nil
}

// withPGXConnect installs a pgxConnect seam returning the given conn (or err)
// and restores the original on test cleanup.
func withPGXConnect(t *testing.T, conn pgConn, err error) {
	t.Helper()
	orig := pgxConnect
	pgxConnect = func(context.Context, string) (pgConn, error) {
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
	t.Cleanup(func() { pgxConnect = orig })
}

// withPGXConnectFunc installs a fully custom pgxConnect seam (e.g. to vary the
// returned conn by call order) and restores it on cleanup.
func withPGXConnectFunc(t *testing.T, fn func(ctx context.Context, dsn string) (pgConn, error)) {
	t.Helper()
	orig := pgxConnect
	pgxConnect = fn
	t.Cleanup(func() { pgxConnect = orig })
}

var errSeam = errors.New("seam-induced error")
