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
	"os"
	"time"
)

// natsClientPort is the NATS client-protocol port embedded in customer URLs.
// Matches the port the k8s backend hardcodes in its own customer URLs (k8s.go)
// and the port the nats-proxy listens on.
const natsClientPort = "4222"

// buildNATSURL constructs the user-facing NATS URL. Mirrors
// postgres.buildDBURL / mongo.buildMongoURL: the public host wins when
// configured, otherwise clusterHost — the in-cluster admin address, which is
// only resolvable from inside the cluster. clusterHost carries no port (config
// NATS_HOST is a bare hostname), so the client port is appended.
//
// This is the fix for the leak of internal cluster DNS into customer
// connection strings: before it, /queue/new handed out
// nats://nats.instant-data.svc.cluster.local:4222 on the shared backend,
// because the public host was applied only in the "k8s" branch of NewBackend
// and the cluster runs QUEUE_PROVISION_BACKEND=local.
func buildNATSURL(clusterHost string) string {
	host := publicHostPort()
	if host == "" {
		host = clusterHost + ":" + natsClientPort
	}
	return "nats://" + host
}

// publicHostPort returns the host:port to embed in user-facing NATS URLs, or ""
// when no public host is configured (the caller then falls back to the
// cluster-internal natsHost).
//
// Identical mechanism to postgres.publicHostPort (backend/postgres/local.go),
// redis.publicHostPort (backend/redis/local.go) and mongo.publicHostPort
// (backend/mongo/mongo.go) — env-resolved at Provision time so the shared/local
// backend and the dedicated k8s backend agree on the customer-facing hostname.
//
// Resolution order:
//  1. NATS_PUBLIC_HOST_PORT (e.g. "nats.instanode.dev:4222")
//  2. NATS_PUBLIC_HOST + NATS_PUBLIC_PORT (port defaults to 4222)
//  3. K8S_NATS_PUBLIC_HOST + NATS_PUBLIC_PORT — the env the k8s branch of
//     NewBackend already reads, so a cluster that already advertises
//     nats.instanode.dev for dedicated pods advertises it for shared ones too.
//  4. "" — caller falls back to the in-cluster natsHost.
//
// Deliberately NO built-in default (the k8s branch defaults to
// "nats.instanode.dev"): a dev box running the shared backend against localhost
// must keep emitting localhost, not a production hostname.
func publicHostPort() string {
	if hp := os.Getenv("NATS_PUBLIC_HOST_PORT"); hp != "" {
		return hp
	}
	host := os.Getenv("NATS_PUBLIC_HOST")
	if host == "" {
		host = os.Getenv("K8S_NATS_PUBLIC_HOST")
	}
	if host == "" {
		return ""
	}
	port := os.Getenv("NATS_PUBLIC_PORT")
	if port == "" {
		port = natsClientPort
	}
	return host + ":" + port
}

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
		// The health check above deliberately keeps using b.natsHost: the
		// monitor port is cluster-internal. Only the customer-facing URL is
		// rewritten to the public host.
		URL:           buildNATSURL(b.natsHost),
		SubjectPrefix: prefix,
	}, nil
}

// Deprovision is a no-op for the shared backend — NATS has no per-user state.
func (b *LocalBackend) Deprovision(_ context.Context, token, _ string) error {
	slog.Info("queue.local.deprovision: noop (shared NATS has no per-user state)", "token", token)
	return nil
}
