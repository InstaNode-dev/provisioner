// Package dropguard validates the TARGET IDENTITY of a customer-data
// destruction before it executes.
//
// # Why this package exists (truehomie-db DROP incident, 2026-06-03)
//
// An active Pro customer's Postgres database + role were dropped on the shared
// postgres-customers cluster by an unidentified path. The guardedDrop
// chokepoint (internal/server/drop_chokepoint.go) made every sanctioned drop
// AUDITABLE; this package makes a *mis-targeted* drop UNEXECUTABLE: a bug that
// constructs the wrong identifier (an empty token, a system database such as
// "postgres"/"instant_customers", an admin role such as "instanode_admin", or
// an arbitrary non-tenant-shaped name) is refused BEFORE the destructive
// statement runs.
//
// # What the convention is
//
// Every per-tenant identifier the provisioner ever destroys is a fixed prefix
// plus a platform-issued naming token:
//
//	postgres shared    → db_<token>            usr_<token>
//	postgres dedicated → dedicated_db_<token>  dedicated_usr_<token>
//	mongo              → db_<token-form>       usr_<token-form>   (canonical + legacy forms)
//	redis ACL          → usr_<token>           usr_<token[:8]>    (legacy)
//
// Tokens are UUIDs (with or without dashes), optionally "pool-"-prefixed for
// hot-pool items; e2e/internal cohorts may use other shapes. The guard is
// therefore deliberately CHARSET-AND-DENYLIST based, not format-strict:
// refusing a legitimate legacy form would wedge a deprovision in prod (this
// repo auto-deploys), while the catastrophic mis-target class — system
// databases, admin roles, empty/garbage names — is exactly what the charset +
// denylist refuse. Loosening NEVER happens implicitly: every accepted name
// still has to carry a per-tenant prefix.
//
// Refusals are surfaced by the callers as a structured
// `provisioner.drop.refused` log event; through the guardedDrop chokepoint
// they also increment instant_provisioner_drop_total{outcome="refused"}.
package dropguard

import (
	"errors"
	"fmt"
	"strings"
)

// ErrRefused is the sentinel wrapped by every refusal so callers and tests can
// errors.Is() a dropguard refusal regardless of the message text.
var ErrRefused = errors.New("dropguard: destructive target refused")

// maxIdentifierLen bounds every validated identifier. Postgres truncates
// identifiers at 63 bytes; nothing legitimate is longer.
const maxIdentifierLen = 63

// reservedTokens are naming tokens that must never be destroyed even when the
// charset would otherwise allow them (e.g. a bug that flows a database or role
// name INTO the token field). Compared case-insensitively.
var reservedTokens = map[string]bool{
	"postgres":          true,
	"template0":         true,
	"template1":         true,
	"admin":             true,
	"default":           true,
	"root":              true,
	"instant_customers": true,
	"instant_platform":  true,
	"instant_cust":      true,
	"instanode_admin":   true,
	"doadmin":           true,
}

// systemDatabases are database names that must never be dropped. The per-tenant
// prefix requirement already excludes them; this denylist is belt-and-suspenders
// for any future caller that validates a name without a prefix expectation.
var systemDatabases = map[string]bool{
	"postgres":          true,
	"template0":         true,
	"template1":         true,
	"instant_customers": true,
	"instant_platform":  true,
	// Mongo system databases.
	"admin":  true,
	"local":  true,
	"config": true,
}

// systemRoles are role/user names that must never be dropped or DELUSER'd.
var systemRoles = map[string]bool{
	"postgres":        true,
	"instant_cust":    true,
	"instanode_admin": true,
	"doadmin":         true,
	"admin":           true,
	"root":            true,
	"default":         true, // the Redis ACL admin user — DELUSER default bricks the pod
	"replication":     true,
}

// dbPrefixes / userPrefixes are the only prefixes a destroyable per-tenant
// identifier may carry. Longest-first so "dedicated_db_x" never strips as
// "db_" with tail "dedicated_..." (it can't — prefix match is from the start —
// but the order keeps the loop's intent obvious).
var (
	dbPrefixes = []string{"dedicated_db_", "db_"}
	// "ded_" is the dedicated-Redis ACL user prefix (redis/dedident.go);
	// "dedicated_usr_" the dedicated-Postgres role prefix; "usr_" everything else.
	userPrefixes = []string{"dedicated_usr_", "usr_", "ded_"}
)

// validTokenChars reports whether every byte of tok is in the conservative
// identifier charset [a-z0-9._-] (case-insensitive). UUIDs (dashed or dashless),
// "pool-" forms, and e2e cohort tokens all fit; SQL metacharacters, quotes,
// spaces, '%' and '*' wildcards do not.
func validTokenChars(tok string) bool {
	for i := 0; i < len(tok); i++ {
		c := tok[i]
		switch {
		case c >= 'a' && c <= 'z':
		case c >= 'A' && c <= 'Z':
		case c >= '0' && c <= '9':
		case c == '-' || c == '_' || c == '.':
		default:
			return false
		}
	}
	return true
}

// CheckNamingToken validates the canonical naming token of a resource about to
// be destroyed (the resolved poolident.NamingToken — pool token or request
// token). It refuses empty, over-long, bad-charset, and reserved tokens.
func CheckNamingToken(token string) error {
	switch {
	case token == "":
		return fmt.Errorf("%w: empty naming token", ErrRefused)
	case len(token) > maxIdentifierLen:
		return fmt.Errorf("%w: naming token %q exceeds %d bytes", ErrRefused, token, maxIdentifierLen)
	case !validTokenChars(token):
		return fmt.Errorf("%w: naming token %q contains characters outside [A-Za-z0-9._-]", ErrRefused, token)
	case reservedTokens[strings.ToLower(token)]:
		return fmt.Errorf("%w: naming token %q is a reserved system identifier", ErrRefused, token)
	}
	return nil
}

// CheckDatabaseName validates a database name about to be passed to
// DROP DATABASE (or a Mongo dropDatabase). The name must carry a per-tenant
// db prefix, have a valid non-reserved tail, and never be a system database.
func CheckDatabaseName(name string) error {
	return checkPrefixed("database", name, dbPrefixes, systemDatabases)
}

// CheckUserName validates a role/user name about to be passed to DROP USER /
// DROP ROLE / Mongo dropUser / Redis ACL DELUSER. The name must carry a
// per-tenant usr prefix, have a valid non-reserved tail, and never be a
// system role.
func CheckUserName(name string) error {
	return checkPrefixed("user", name, userPrefixes, systemRoles)
}

// checkPrefixed is the shared prefix + tail + denylist validation.
func checkPrefixed(kind, name string, prefixes []string, denylist map[string]bool) error {
	if denylist[strings.ToLower(name)] {
		return fmt.Errorf("%w: %s %q is a protected system identifier", ErrRefused, kind, name)
	}
	if len(name) > maxIdentifierLen {
		return fmt.Errorf("%w: %s %q exceeds %d bytes", ErrRefused, kind, name, maxIdentifierLen)
	}
	for _, p := range prefixes {
		tail, ok := strings.CutPrefix(name, p)
		if !ok {
			continue
		}
		if tail == "" {
			return fmt.Errorf("%w: %s %q has an empty token after prefix %q", ErrRefused, kind, name, p)
		}
		if !validTokenChars(tail) {
			return fmt.Errorf("%w: %s %q has characters outside [A-Za-z0-9._-] after prefix %q", ErrRefused, kind, name, p)
		}
		if reservedTokens[strings.ToLower(tail)] {
			return fmt.Errorf("%w: %s %q embeds a reserved system identifier", ErrRefused, kind, name)
		}
		return nil
	}
	return fmt.Errorf("%w: %s %q does not carry a per-tenant prefix (%s)", ErrRefused, kind, name, strings.Join(prefixes, ", "))
}
