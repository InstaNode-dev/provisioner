package server

// maperror_test.go — regression test for the BugBash-2026-05-18 P1-02 fix:
// mapError must classify a typed *pgconn.PgError by its SQLSTATE, NOT by
// substring-matching the message text. A real CREATE DATABASE failure whose
// message happens to contain "connection" must surface as Internal (the worker
// must not retry it), while a genuine class-08 connection error surfaces as
// Unavailable.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMapError_PgError_ClassifiesBySQLSTATE(t *testing.T) {
	cases := []struct {
		name    string
		sqlCode string
		msg     string
		want    codes.Code
	}{
		{
			// The exact misclassification P1-02 describes: a permission error
			// whose message text contains the word "connection".
			name:    "permission denied mentioning connection → Internal",
			sqlCode: "42501",
			msg:     "permission denied to create database connection pool",
			want:    codes.Internal,
		},
		{
			name:    "genuine connection-exception class 08 → Unavailable",
			sqlCode: "08006",
			msg:     "server closed the connection unexpectedly",
			want:    codes.Unavailable,
		},
		{
			name:    "cannot_connect_now 57P03 → Unavailable",
			sqlCode: "57P03",
			msg:     "the database system is starting up",
			want:    codes.Unavailable,
		},
		{
			name:    "duplicate_database 42P04 → AlreadyExists",
			sqlCode: "42P04",
			msg:     `database "db_tok" already exists`,
			want:    codes.AlreadyExists,
		},
		{
			name:    "duplicate_object 42710 → AlreadyExists",
			sqlCode: "42710",
			msg:     `role "usr_tok" already exists`,
			want:    codes.AlreadyExists,
		},
		{
			name:    "syntax error 42601 → Internal",
			sqlCode: "42601",
			msg:     "syntax error at or near \"CONNECT\"",
			want:    codes.Internal,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := mapError("op", &pgconn.PgError{Code: tc.sqlCode, Message: tc.msg})
			if got := status.Code(err); got != tc.want {
				t.Errorf("mapError(PgError{Code:%s}) = %v; want %v", tc.sqlCode, got, tc.want)
			}
		})
	}
}

func TestMapError_NonPgError_FallsBackToSubstring(t *testing.T) {
	// Non-Postgres backends (redis/mongo/k8s) have no SQLSTATE — the substring
	// heuristic still applies.
	if got := status.Code(mapError("op", errors.New("dial tcp: connection refused"))); got != codes.Unavailable {
		t.Errorf("non-pg connect error: got %v; want Unavailable", got)
	}
	if got := status.Code(mapError("op", errors.New("namespace already exists"))); got != codes.AlreadyExists {
		t.Errorf("non-pg already-exists error: got %v; want AlreadyExists", got)
	}
	if got := status.Code(mapError("op", errors.New("kaniko build failed: exit 1"))); got != codes.Internal {
		t.Errorf("non-pg opaque error: got %v; want Internal", got)
	}
	if mapError("op", nil) != nil {
		t.Error("mapError(nil) must return nil")
	}
}

// TestMapError_ContextErrors_AreRetryable guards fix/redis-pro-provision-hang.
//
// When a backend honours the caller's deadline and bails (the redis k8s Provision
// fast-fail on a stalled Pro-tier PVC attach), the error wraps context.DeadlineExceeded
// or context.Canceled. mapError MUST classify these via errors.Is — NOT a fragile
// message substring — so the api treats them as soft/retryable (soft-delete + 503)
// rather than a hard Internal/500. A wrapped context.DeadlineExceeded must map to
// DeadlineExceeded even if the message does not contain the word "timeout".
func TestMapError_ContextErrors_AreRetryable(t *testing.T) {
	// The exact wrapped form returned by waitPodReady — note the message says
	// "cancelled/timed out", which does NOT contain the substring "timeout" the
	// old heuristic looked for. errors.Is is what makes this robust.
	deadlineErr := fmt.Errorf("redis pod not ready (caller cancelled/timed out): %w", context.DeadlineExceeded)
	if got := status.Code(mapError("ProvisionResource.redis.dedicated", deadlineErr)); got != codes.DeadlineExceeded {
		t.Errorf("wrapped context.DeadlineExceeded → %v; want DeadlineExceeded (retryable, not Internal)", got)
	}

	canceledErr := fmt.Errorf("redis pod not ready (caller cancelled/timed out): %w", context.Canceled)
	if got := status.Code(mapError("ProvisionResource.redis.dedicated", canceledErr)); got != codes.Unavailable {
		t.Errorf("wrapped context.Canceled → %v; want Unavailable (retryable, not Internal)", got)
	}

	// Bare (unwrapped) context errors are also classified.
	if got := status.Code(mapError("op", context.DeadlineExceeded)); got != codes.DeadlineExceeded {
		t.Errorf("bare context.DeadlineExceeded → %v; want DeadlineExceeded", got)
	}
	if got := status.Code(mapError("op", context.Canceled)); got != codes.Unavailable {
		t.Errorf("bare context.Canceled → %v; want Unavailable", got)
	}
}
