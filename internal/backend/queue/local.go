package queue

// local.go — LocalBackend provisions NATS credentials on the shared NATS cluster.
// NATS runs without authentication — no per-user state is created on the server.
// The SubjectPrefix provides logical isolation via naming convention.
//
// Configuration env vars:
//   NATS_HOST — hostname:port or just hostname of shared NATS (default "localhost")

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// LocalBackend provisions NATS on the shared cluster.
type LocalBackend struct {
	natsHost   string
	httpClient *http.Client
	// monitorPort is the NATS monitor port; defaults to 8222 in newLocalBackend.
	// Exposed as a struct field rather than a constant so tests can drive the
	// health-check codepath against an httptest.Server on a random port without
	// colliding with a docker daemon / real NATS / another developer's pod
	// already bound to :8222 on the same loopback.
	monitorPort int
}

func newLocalBackend(natsHost string) *LocalBackend {
	if natsHost == "" {
		natsHost = "localhost"
	}
	return &LocalBackend{
		natsHost:    natsHost,
		httpClient:  &http.Client{Timeout: 5 * time.Second},
		monitorPort: 8222,
	}
}

// Provision verifies NATS is reachable and returns a connection URL + subject prefix.
func (b *LocalBackend) Provision(ctx context.Context, token, tier string) (*Credentials, error) {
	monitorURL := fmt.Sprintf("http://%s:%d/healthz", b.natsHost, b.monitorPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, monitorURL, nil)
	if err != nil {
		return nil, fmt.Errorf("queue.local.Provision: build health request: %w", err)
	}
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("queue.local.Provision: NATS health check failed (%s): %w", monitorURL, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("queue.local.Provision: NATS unhealthy (HTTP %d from %s)", resp.StatusCode, monitorURL)
	}

	// SubjectPrefix is the ONLY tenant-isolation boundary on the shared NATS
	// backend (NATS runs unauthenticated). Derive it from the FULL token — see
	// subjident.go — so two tokens sharing an 8-hex-char prefix can never share
	// a subject namespace. Pre-fix queues keep their token[:8] prefix; the
	// legacy resolver in subjident.go covers them.
	prefix := canonicalSubjectPrefix(token)

	slog.Info("queue.local.provisioned", "token", token, "subject_prefix", prefix)
	return &Credentials{
		URL:           fmt.Sprintf("nats://%s:4222", b.natsHost),
		SubjectPrefix: prefix,
	}, nil
}

// Deprovision is a no-op for the shared backend — NATS has no per-user state.
func (b *LocalBackend) Deprovision(_ context.Context, token, _ string) error {
	slog.Info("queue.local.deprovision: noop (shared NATS has no per-user state)", "token", token)
	return nil
}
