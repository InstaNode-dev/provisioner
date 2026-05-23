package postgres

// seams.go — test seams for the postgres backends.
//
// These package-level function variables and the narrow pgConn interface let
// the test suite drive the error and success branches of code paths that would
// otherwise require a live Postgres cluster, a working crypto/rand, or a
// successful net/http construction. In production every seam defaults to the
// real implementation, so behaviour is identical to calling pgx.Connect /
// rand.Read / json.Marshal / http.NewRequestWithContext directly.
//
// Why a seam instead of a live cluster: the provisioner's coverage CI job is
// mock-only (no service containers — see .github/workflows/coverage.yml). The
// redis backend reaches ≥95% the same way: fakes + connection-failure branches.
// For postgres the SQL happy path (CREATE DATABASE / CREATE USER success) is
// the bulk of the statements, so a fake pgConn is the only way to execute those
// lines deterministically in CI without a real database.

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"math/big"
	"net/http"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// pgConn is the narrow subset of *pgx.Conn that the postgres backends use.
// *pgx.Conn satisfies it; tests inject a fake.
type pgConn interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Close(ctx context.Context) error
}

// pgxConnect is the seam for pgx.Connect. Production wraps the real connection
// so it satisfies the pgConn interface. A test overrides this var to return a
// fake pgConn (success path) or an error (connect-failure path).
var pgxConnect = func(ctx context.Context, connString string) (pgConn, error) {
	c, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// randRead is the seam for crypto/rand.Read. Overriding it to return an error
// covers the rand-failure branch in k8sRandHex without weakening production
// (which uses the real CSPRNG).
var randRead = rand.Read

// randInt is the seam for crypto/rand.Int, used by generatePassword. A test
// overrides it to force the rand-failure branch.
var randInt func(rand io.Reader, max *big.Int) (*big.Int, error) = rand.Int

// jsonMarshal is the seam for encoding/json.Marshal, used by the Neon/dedicated
// request bodies. Overriding it forces the marshal-error wrap branch.
var jsonMarshal = json.Marshal

// httpNewRequestWithContext is the seam for http.NewRequestWithContext, used by
// every Neon/dedicated API call. Overriding it forces the construction-error
// wrap branch (otherwise unreachable — a valid method + URL never errors).
var httpNewRequestWithContext = http.NewRequestWithContext

// ioReadAll is the seam for io.ReadAll on an HTTP response body. Overriding it
// forces the read-body-error wrap branch.
var ioReadAll = io.ReadAll
