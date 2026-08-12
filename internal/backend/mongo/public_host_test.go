package mongo

// public_host_test.go — the customer-facing hostname in /nosql/new connection
// strings.
//
// COVERAGE BLOCK (CLAUDE.md rule 17):
//
//	Symptom:        /nosql/new returned
//	                mongodb://usr_…:…@mongodb.instant-data.svc.cluster.local:27017/…
//	                — internal cluster DNS no customer can resolve. The public
//	                host was applied only inside the `case "k8s"` branch of
//	                NewBackend (backend.go), and the cluster runs
//	                MONGO_PROVISION_BACKEND=local.
//	Enumeration:    rg -F 'mongodb://' / 'b.mongoHost' / 'K8S_MONGO_PUBLIC_HOST'
//	Sites found:    1 customer-URL emitter on the shared path
//	                (mongo.go Provision) + 1 on the k8s path (k8s.go, already
//	                correct) + admin URIs (not customer-facing).
//	Sites touched:  the shared emitter, via buildMongoURL — the same
//	                helper+publicHostPort shape as postgres.buildDBURL.
//	Coverage test:  TestBuildMongoURL below; the unset row pins the fallback to
//	                the in-cluster host (never an empty host).

import (
	"strings"
	"testing"
)

// mongoPublicHostEnvKeys is every env var publicHostPort consults. Tests clear
// all of them so a developer's ambient shell env cannot perturb the "unset"
// rows. A new source added to publicHostPort must be added here.
var mongoPublicHostEnvKeys = []string{
	"MONGO_PUBLIC_HOST_PORT",
	"MONGO_PUBLIC_HOST",
	"MONGO_PUBLIC_PORT",
	"K8S_MONGO_PUBLIC_HOST",
}

func clearMongoPublicHostEnv(t *testing.T) {
	t.Helper()
	for _, k := range mongoPublicHostEnvKeys {
		t.Setenv(k, "")
	}
}

// TestPublicHostPort_Mongo exercises every resolution branch of the helper.
func TestPublicHostPort_Mongo(t *testing.T) {
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
			name: "MONGO_PUBLIC_HOST_PORT wins over everything",
			env: map[string]string{
				"MONGO_PUBLIC_HOST_PORT": "mongo.instanode.dev:27020",
				"MONGO_PUBLIC_HOST":      "ignored.example.com",
				"MONGO_PUBLIC_PORT":      "1111",
				"K8S_MONGO_PUBLIC_HOST":  "also-ignored.example.com",
			},
			want: "mongo.instanode.dev:27020",
		},
		{
			name: "MONGO_PUBLIC_HOST with the default port",
			env:  map[string]string{"MONGO_PUBLIC_HOST": "mongo.instanode.dev"},
			want: "mongo.instanode.dev:" + defaultMongoPort,
		},
		{
			name: "MONGO_PUBLIC_HOST with an explicit port",
			env: map[string]string{
				"MONGO_PUBLIC_HOST": "mongo.instanode.dev",
				"MONGO_PUBLIC_PORT": "27099",
			},
			want: "mongo.instanode.dev:27099",
		},
		{
			name: "MONGO_PUBLIC_HOST wins over K8S_MONGO_PUBLIC_HOST",
			env: map[string]string{
				"MONGO_PUBLIC_HOST":     "explicit.example.com",
				"K8S_MONGO_PUBLIC_HOST": "k8s.example.com",
			},
			want: "explicit.example.com:" + defaultMongoPort,
		},
		{
			// The env the cluster ALREADY sets. Honouring it is what makes the
			// fix a pure code change with no ops change.
			name: "K8S_MONGO_PUBLIC_HOST alone — the already-configured prod env",
			env:  map[string]string{"K8S_MONGO_PUBLIC_HOST": "mongo.instanode.dev"},
			want: "mongo.instanode.dev:" + defaultMongoPort,
		},
		{
			name: "K8S_MONGO_PUBLIC_HOST with an explicit port",
			env: map[string]string{
				"K8S_MONGO_PUBLIC_HOST": "mongo.instanode.dev",
				"MONGO_PUBLIC_PORT":     "27098",
			},
			want: "mongo.instanode.dev:27098",
		},
		{
			name: "port set but no host — still empty (a port alone addresses nothing)",
			env:  map[string]string{"MONGO_PUBLIC_PORT": "27099"},
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearMongoPublicHostEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if got := publicHostPort(); got != tc.want {
				t.Errorf("publicHostPort() = %q; want %q", got, tc.want)
			}
		})
	}
}

// TestBuildMongoURL asserts the customer URL uses the public host when one is
// configured and the in-cluster admin host otherwise — never an empty host.
func TestBuildMongoURL(t *testing.T) {
	const (
		clusterHost = "mongodb.instant-data.svc.cluster.local:27017"
		user        = "usr_abc"
		pass        = "pw123"
		db          = "db_abc"
	)

	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "public host unset — falls back to the cluster host, NOT an empty host",
			env:  map[string]string{},
			want: "mongodb://usr_abc:pw123@" + clusterHost + "/db_abc?authSource=admin",
		},
		{
			name: "public host set via K8S_MONGO_PUBLIC_HOST (prod today)",
			env:  map[string]string{"K8S_MONGO_PUBLIC_HOST": "mongo.instanode.dev"},
			want: "mongodb://usr_abc:pw123@mongo.instanode.dev:27017/db_abc?authSource=admin",
		},
		{
			name: "public host set via MONGO_PUBLIC_HOST_PORT",
			env:  map[string]string{"MONGO_PUBLIC_HOST_PORT": "mongo.instanode.dev:27020"},
			want: "mongodb://usr_abc:pw123@mongo.instanode.dev:27020/db_abc?authSource=admin",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearMongoPublicHostEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			got := buildMongoURL(clusterHost, user, pass, db)
			if got != tc.want {
				t.Errorf("buildMongoURL() = %q; want %q", got, tc.want)
			}
			if strings.Contains(got, "@/") {
				t.Errorf("buildMongoURL() = %q has an empty host", got)
			}
		})
	}
}

// TestBuildMongoURL_NeverLeaksClusterDNSWhenPublicHostSet is the regression pin:
// with the public host configured, the internal service DNS must be gone from
// the customer's connection string entirely.
func TestBuildMongoURL_NeverLeaksClusterDNSWhenPublicHostSet(t *testing.T) {
	clearMongoPublicHostEnv(t)
	t.Setenv("K8S_MONGO_PUBLIC_HOST", "mongo.instanode.dev")

	got := buildMongoURL("mongodb.instant-data.svc.cluster.local:27017", "usr_x", "pw", "db_x")
	if strings.Contains(got, "svc.cluster.local") {
		t.Errorf("customer URL still contains internal cluster DNS: %q", got)
	}
}
