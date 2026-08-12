package queue

// public_host_test.go — the customer-facing hostname in /queue/new connection
// strings.
//
// COVERAGE BLOCK (CLAUDE.md rule 17):
//
//	Symptom:        /queue/new returned
//	                nats://nats.instant-data.svc.cluster.local:4222 — internal
//	                cluster DNS no customer can resolve. The public host was
//	                applied only inside the `case "k8s"` branch of NewBackend
//	                (backend.go), and the cluster runs
//	                QUEUE_PROVISION_BACKEND=local.
//	Enumeration:    rg -F 'nats://' / 'b.natsHost' / 'K8S_NATS_PUBLIC_HOST'
//	Sites found:    1 customer-URL emitter on the shared path (local.go
//	                Provision) + 1 on the k8s path (k8s.go, already correct).
//	Sites touched:  the shared emitter, via buildNATSURL — the same
//	                helper+publicHostPort shape as postgres.buildDBURL.
//	Coverage test:  TestBuildNATSURL below; the unset row pins the fallback to
//	                the in-cluster host:4222 (never a bare "nats://" or an
//	                empty host).

import (
	"strings"
	"testing"
)

// natsPublicHostEnvKeys is every env var publicHostPort consults. Tests clear
// all of them so a developer's ambient shell env cannot perturb the "unset"
// rows. A new source added to publicHostPort must be added here.
var natsPublicHostEnvKeys = []string{
	"NATS_PUBLIC_HOST_PORT",
	"NATS_PUBLIC_HOST",
	"NATS_PUBLIC_PORT",
	"K8S_NATS_PUBLIC_HOST",
}

func clearNATSPublicHostEnv(t *testing.T) {
	t.Helper()
	for _, k := range natsPublicHostEnvKeys {
		t.Setenv(k, "")
	}
}

// TestPublicHostPort_NATS exercises every resolution branch of the helper.
func TestPublicHostPort_NATS(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "nothing set — empty so the caller falls back to the admin host",
			env:  map[string]string{},
			want: "",
		},
		{
			name: "NATS_PUBLIC_HOST_PORT wins over everything",
			env: map[string]string{
				"NATS_PUBLIC_HOST_PORT": "nats.instanode.dev:4230",
				"NATS_PUBLIC_HOST":      "ignored.example.com",
				"NATS_PUBLIC_PORT":      "1111",
				"K8S_NATS_PUBLIC_HOST":  "also-ignored.example.com",
			},
			want: "nats.instanode.dev:4230",
		},
		{
			name: "NATS_PUBLIC_HOST with the default port",
			env:  map[string]string{"NATS_PUBLIC_HOST": "nats.instanode.dev"},
			want: "nats.instanode.dev:" + natsClientPort,
		},
		{
			name: "NATS_PUBLIC_HOST with an explicit port",
			env: map[string]string{
				"NATS_PUBLIC_HOST": "nats.instanode.dev",
				"NATS_PUBLIC_PORT": "4299",
			},
			want: "nats.instanode.dev:4299",
		},
		{
			name: "NATS_PUBLIC_HOST wins over K8S_NATS_PUBLIC_HOST",
			env: map[string]string{
				"NATS_PUBLIC_HOST":     "explicit.example.com",
				"K8S_NATS_PUBLIC_HOST": "k8s.example.com",
			},
			want: "explicit.example.com:" + natsClientPort,
		},
		{
			// The env the cluster ALREADY sets. Honouring it is what makes the
			// fix a pure code change with no ops change.
			name: "K8S_NATS_PUBLIC_HOST alone — the already-configured prod env",
			env:  map[string]string{"K8S_NATS_PUBLIC_HOST": "nats.instanode.dev"},
			want: "nats.instanode.dev:" + natsClientPort,
		},
		{
			name: "K8S_NATS_PUBLIC_HOST with an explicit port",
			env: map[string]string{
				"K8S_NATS_PUBLIC_HOST": "nats.instanode.dev",
				"NATS_PUBLIC_PORT":     "4298",
			},
			want: "nats.instanode.dev:4298",
		},
		{
			name: "port set but no host — still empty (a port alone addresses nothing)",
			env:  map[string]string{"NATS_PUBLIC_PORT": "4299"},
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearNATSPublicHostEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := publicHostPort(); got != tc.want {
				t.Errorf("publicHostPort() = %q; want %q", got, tc.want)
			}
		})
	}
}

// TestBuildNATSURL asserts the customer URL uses the public host when one is
// configured and the in-cluster admin host otherwise — never an empty host.
func TestBuildNATSURL(t *testing.T) {
	const clusterHost = "nats.instant-data.svc.cluster.local"

	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "public host unset — falls back to clusterHost:4222, NOT an empty host",
			env:  map[string]string{},
			want: "nats://" + clusterHost + ":4222",
		},
		{
			name: "public host set via K8S_NATS_PUBLIC_HOST (prod today)",
			env:  map[string]string{"K8S_NATS_PUBLIC_HOST": "nats.instanode.dev"},
			want: "nats://nats.instanode.dev:4222",
		},
		{
			name: "public host set via NATS_PUBLIC_HOST_PORT",
			env:  map[string]string{"NATS_PUBLIC_HOST_PORT": "nats.instanode.dev:4230"},
			want: "nats://nats.instanode.dev:4230",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearNATSPublicHostEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			got := buildNATSURL(clusterHost)
			if got != tc.want {
				t.Errorf("buildNATSURL() = %q; want %q", got, tc.want)
			}
			if got == "nats://" || strings.HasSuffix(got, "://:4222") {
				t.Errorf("buildNATSURL() = %q has an empty host", got)
			}
		})
	}
}

// TestBuildNATSURL_NeverLeaksClusterDNSWhenPublicHostSet is the regression pin:
// with the public host configured, the internal service DNS must be gone from
// the customer's connection string entirely.
func TestBuildNATSURL_NeverLeaksClusterDNSWhenPublicHostSet(t *testing.T) {
	clearNATSPublicHostEnv(t)
	t.Setenv("K8S_NATS_PUBLIC_HOST", "nats.instanode.dev")

	got := buildNATSURL("nats.instant-data.svc.cluster.local")
	if strings.Contains(got, "svc.cluster.local") {
		t.Errorf("customer URL still contains internal cluster DNS: %q", got)
	}
}

// TestLocalBackend_Provision_UsesPublicHost closes the loop through Provision
// itself: the health check must still hit the in-cluster monitor address while
// the returned URL advertises the public host.
func TestLocalBackend_Provision_UsesPublicHost(t *testing.T) {
	clearNATSPublicHostEnv(t)
	t.Setenv("K8S_NATS_PUBLIC_HOST", "nats.instanode.dev")

	host, port := newHealthTestServer(t, 200)
	b := newLocalBackend(host)
	b.monitorPort = port

	creds, err := b.Provision(t.Context(), "abc12345deadbeefcafef00d00112233", "anonymous")
	if err != nil {
		t.Fatalf("Provision returned error: %v", err)
	}
	if creds.URL != "nats://nats.instanode.dev:4222" {
		t.Errorf("URL = %q; want nats://nats.instanode.dev:4222", creds.URL)
	}
}
