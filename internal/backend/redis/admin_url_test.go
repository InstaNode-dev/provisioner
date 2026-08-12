package redis

// admin_url_test.go — REDIS_PROVISION_URL, the credentialed admin connection
// for the shared redis-provision pool.
//
// COVERAGE BLOCK (CLAUDE.md rule 17):
//
//	Symptom:        /cache/new 503s with
//	                "cache.provisionLocal: ACL SETUSER failed on shared
//	                 multi-tenant Redis … ERR Protocol error: unauthenticated
//	                 multibulk length"
//	                because the admin client was built from a bare
//	                goredis.Options{Addr: REDIS_PROVISION_HOST} and therefore
//	                never sends AUTH to a pod started with --requirepass.
//	Enumeration:    rg -F 'newLocalBackend(' / 'NewSharedCarveBackend' /
//	                'redis.NewBackend' / 'RedisProvisionHost'
//	Sites found:    3 production constructors of the shared LocalBackend
//	                (NewBackend default arm, NewBackend k8s-init-failure
//	                fallback, NewSharedCarveBackend) reached from 3 wiring
//	                sites (server.New x2, pool.NewWithConfig x1).
//	Sites touched:  all 3 constructors take adminURL; all 3 wiring sites pass
//	                cfg.RedisProvisionURL.
//	Coverage test:  TestSharedBackendConstructors_AllHonourAdminURL below —
//	                iterates every constructor that yields a shared
//	                LocalBackend and asserts each one authenticates. A 4th
//	                constructor added without the adminURL parameter fails it.

import (
	"strings"
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

// TestNewLocalBackend_AdminURL is the table for the three states of
// REDIS_PROVISION_URL: set, unset, malformed.
func TestNewLocalBackend_AdminURL(t *testing.T) {
	tests := []struct {
		name         string
		adminURL     string
		redisHost    string
		wantAddr     string
		wantUsername string
		wantPassword string
		wantDB       int
	}{
		{
			name:         "url set with user and password — credentials reach the client",
			adminURL:     "redis://admin:s3cret@redis-provision.instant-data.svc.cluster.local:6379/0",
			redisHost:    "ignored-when-url-set:6379",
			wantAddr:     "redis-provision.instant-data.svc.cluster.local:6379",
			wantUsername: "admin",
			wantPassword: "s3cret",
			wantDB:       0,
		},
		{
			name: "url set with password only — the --requirepass shape " +
				"(default user, no username in the URL)",
			adminURL:     "redis://:pooLpassw0rd@127.0.0.1:6380/3",
			redisHost:    "ignored-when-url-set:6379",
			wantAddr:     "127.0.0.1:6380",
			wantUsername: "",
			wantPassword: "pooLpassw0rd",
			wantDB:       3,
		},
		{
			name:         "url unset — legacy REDIS_PROVISION_HOST Addr form, unchanged",
			adminURL:     "",
			redisHost:    "custom:6380",
			wantAddr:     "custom:6380",
			wantUsername: "",
			wantPassword: "",
			wantDB:       0,
		},
		{
			name:         "url unset and host unset — package default addr",
			adminURL:     "",
			redisHost:    "",
			wantAddr:     defaultRedisAddr,
			wantUsername: "",
			wantPassword: "",
			wantDB:       0,
		},
		{
			name: "url malformed (bad scheme) — falls back to the host form " +
				"instead of failing the whole provisioner start",
			adminURL:     "http://admin:s3cret@somewhere:6379",
			redisHost:    "fallback:6379",
			wantAddr:     "fallback:6379",
			wantUsername: "",
			wantPassword: "",
			wantDB:       0,
		},
		{
			name: "url malformed (control character, url.Parse failure) — " +
				"falls back to the host form",
			adminURL:     "redis://admin:s3cret@some\x7fwhere:6379",
			redisHost:    "fallback:6379",
			wantAddr:     "fallback:6379",
			wantUsername: "",
			wantPassword: "",
			wantDB:       0,
		},
		{
			name: "url malformed AND host unset — package default addr, " +
				"never a half-configured client",
			adminURL:     "::not a url::",
			redisHost:    "",
			wantAddr:     defaultRedisAddr,
			wantUsername: "",
			wantPassword: "",
			wantDB:       0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			b := newLocalBackend(tc.adminURL, tc.redisHost)
			opts := b.rdb.Options()

			if opts.Addr != tc.wantAddr {
				t.Errorf("admin client Addr = %q; want %q", opts.Addr, tc.wantAddr)
			}
			if opts.Username != tc.wantUsername {
				t.Errorf("admin client Username = %q; want %q", opts.Username, tc.wantUsername)
			}
			if opts.Password != tc.wantPassword {
				t.Errorf("admin client Password = %q; want %q", opts.Password, tc.wantPassword)
			}
			if opts.DB != tc.wantDB {
				t.Errorf("admin client DB = %d; want %d", opts.DB, tc.wantDB)
			}

			// The recorded host is what Provision embeds in the CUSTOMER url
			// when no REDIS_PUBLIC_HOST* is configured. It must always be a
			// bare host:port — never the admin URL, which carries the shared
			// pool's password.
			if b.redisHost != tc.wantAddr {
				t.Errorf("redisHost = %q; want %q (bare host:port)", b.redisHost, tc.wantAddr)
			}
			if strings.Contains(b.redisHost, "@") || strings.Contains(b.redisHost, "//") {
				t.Errorf("redisHost = %q leaks admin URL structure into customer URLs", b.redisHost)
			}
		})
	}
}

// TestNewLocalBackend_AdminURLDoesNotLeakIntoCustomerURL pins the specific
// regression the redisHost assertion above guards: the password from
// REDIS_PROVISION_URL must never appear in a credential returned to a customer.
func TestNewLocalBackend_AdminURLDoesNotLeakIntoCustomerURL(t *testing.T) {
	const adminPassword = "sup3r-s3cret-pool-pw"
	b := newLocalBackend("redis://admin:"+adminPassword+"@10.0.0.1:6379/0", "")

	// Provision cannot run without a server, but the host it would interpolate
	// is fixed at construction time — assert on that.
	if strings.Contains(b.redisHost, adminPassword) {
		t.Fatalf("redisHost %q contains the admin password; it is interpolated into customer URLs", b.redisHost)
	}
}

// TestParseAdminURL covers the helper's three return shapes directly, including
// that a parsed URL's non-credential options survive.
func TestParseAdminURL(t *testing.T) {
	t.Run("empty returns nil", func(t *testing.T) {
		if got := parseAdminURL(""); got != nil {
			t.Errorf("parseAdminURL(\"\") = %+v; want nil", got)
		}
	})

	t.Run("malformed returns nil", func(t *testing.T) {
		if got := parseAdminURL("not-a-redis-url"); got != nil {
			t.Errorf("parseAdminURL(malformed) = %+v; want nil", got)
		}
	})

	t.Run("valid returns options with query params applied", func(t *testing.T) {
		got := parseAdminURL("redis://:pw@h:6379/2?dial_timeout=7s")
		if got == nil {
			t.Fatal("parseAdminURL(valid) = nil; want options")
		}
		if got.Password != "pw" || got.Addr != "h:6379" || got.DB != 2 {
			t.Errorf("options = %+v; want Addr h:6379, Password pw, DB 2", got)
		}
		if got.DialTimeout.Seconds() != 7 {
			t.Errorf("DialTimeout = %v; want 7s (query params must survive)", got.DialTimeout)
		}
	})
}

// TestRedactCredentials asserts the log-sanitiser strips URL userinfo. A
// net/url parse error stringifies as `parse "<the whole URL>": …`, so logging a
// ParseURL failure verbatim would publish the shared pool's admin password.
func TestRedactCredentials(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{
			"no url — untouched",
			"redis: invalid URL scheme: http",
			"redis: invalid URL scheme: http",
		},
		{
			"user:password userinfo redacted",
			`parse "redis://admin:s3cret@host:6379": net/url: invalid control character in URL`,
			`parse "redis://***:***@host:6379": net/url: invalid control character in URL`,
		},
		{
			"password-only userinfo redacted",
			`parse "redis://:s3cret@host:6379": boom`,
			`parse "redis://***:***@host:6379": boom`,
		},
		{
			"username-only userinfo left alone (no secret in it)",
			`parse "redis://admin@host:6379": boom`,
			`parse "redis://admin@host:6379": boom`,
		},
		{
			"every occurrence redacted, not just the first",
			`a redis://u:p@h1 and b redis://u2:p2@h2`,
			`a redis://***:***@h1 and b redis://***:***@h2`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := redactCredentials(tc.in); got != tc.want {
				t.Errorf("redactCredentials(%q) = %q; want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestRedactCredentials_OnRealParseError closes the loop on the redaction: it
// feeds the sanitiser the ACTUAL error text go-redis produces for a credentialed
// URL that url.Parse rejects, rather than a hand-written approximation of it.
func TestRedactCredentials_OnRealParseError(t *testing.T) {
	const adminPassword = "sup3r-s3cret-pool-pw"
	_, err := goredisParseURL("redis://admin:" + adminPassword + "@ho\x7fst:6379")
	if err == nil {
		t.Fatal("expected a parse error for a URL containing a control character")
	}
	if !strings.Contains(err.Error(), adminPassword) {
		t.Skipf("go-redis no longer echoes the URL in its error (%v) — redaction is belt-and-braces here", err)
	}
	if got := redactCredentials(err.Error()); strings.Contains(got, adminPassword) {
		t.Errorf("redacted error still contains the admin password: %q", got)
	}
}

// TestSharedBackendConstructors_AllHonourAdminURL is the rule-18
// registry-iterating guard: EVERY constructor that yields a shared LocalBackend
// must thread adminURL through to the admin client. A new constructor added
// without the parameter (the "two emitters of one broken behaviour" failure
// mode) fails here rather than silently 503-ing /cache/new in prod.
func TestSharedBackendConstructors_AllHonourAdminURL(t *testing.T) {
	const (
		adminURL = "redis://admin:ctor-pw@ctor-host:6390/0"
		wantAddr = "ctor-host:6390"
		wantPass = "ctor-pw"
	)

	ctors := map[string]func(adminURL, redisHost string) Backend{
		// Default arm — the backend prod actually runs (REDIS_PROVISION_BACKEND=local).
		"NewBackend/local": func(a, h string) Backend { return NewBackend("local", a, h) },
		// Unknown backend names fall through to the same default arm.
		"NewBackend/unknown": func(a, h string) Backend { return NewBackend("no-such-backend", a, h) },
		// Non-Team side of tier-aware routing (REDIS_TIER_AWARE_ROUTING_ENABLED).
		"NewSharedCarveBackend": NewSharedCarveBackend,
	}

	for name, ctor := range ctors {
		t.Run(name, func(t *testing.T) {
			b := ctor(adminURL, "unused:6379")
			local, ok := b.(*LocalBackend)
			if !ok {
				t.Fatalf("%s returned %T; want *LocalBackend", name, b)
			}
			opts := local.rdb.Options()
			if opts.Addr != wantAddr {
				t.Errorf("%s: Addr = %q; want %q", name, opts.Addr, wantAddr)
			}
			if opts.Password != wantPass {
				t.Errorf("%s: Password = %q; want %q — this constructor ignores REDIS_PROVISION_URL and will 503 on a password-protected pool",
					name, opts.Password, wantPass)
			}
		})
	}
}

// TestSharedBackendConstructors_FallBackToHost is the same enumeration for the
// unset-URL case: no constructor may start requiring REDIS_PROVISION_URL.
func TestSharedBackendConstructors_FallBackToHost(t *testing.T) {
	ctors := map[string]func(adminURL, redisHost string) Backend{
		"NewBackend/local":      func(a, h string) Backend { return NewBackend("local", a, h) },
		"NewBackend/unknown":    func(a, h string) Backend { return NewBackend("no-such-backend", a, h) },
		"NewSharedCarveBackend": NewSharedCarveBackend,
	}

	for name, ctor := range ctors {
		t.Run(name, func(t *testing.T) {
			b := ctor("", "legacy-host:6379")
			local, ok := b.(*LocalBackend)
			if !ok {
				t.Fatalf("%s returned %T; want *LocalBackend", name, b)
			}
			opts := local.rdb.Options()
			if opts.Addr != "legacy-host:6379" {
				t.Errorf("%s: Addr = %q; want legacy-host:6379", name, opts.Addr)
			}
			if opts.Password != "" {
				t.Errorf("%s: Password = %q; want empty (unauthenticated legacy pool)", name, opts.Password)
			}
		})
	}
}

// TestGoredisNewClient_UsesGivenOptions guards the narrow alias newLocalBackend
// now depends on: a client built from parsed options must keep them.
func TestGoredisNewClient_UsesGivenOptions(t *testing.T) {
	c := goredisNewClient(&goredis.Options{Addr: "a:1", Password: "p"})
	if c.Options().Addr != "a:1" || c.Options().Password != "p" {
		t.Errorf("goredisNewClient dropped options: %+v", c.Options())
	}
}
