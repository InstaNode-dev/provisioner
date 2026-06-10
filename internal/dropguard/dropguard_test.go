package dropguard

import (
	"errors"
	"strings"
	"testing"
)

func TestCheckNamingToken_AcceptsEveryLegitimateForm(t *testing.T) {
	valid := []string{
		"96edf9eed8ed42929036b63298ec5b2b",          // dashless 32-hex (observed prod form)
		"96edf9ee-d8ed-4292-9036-b63298ec5b2b",      // uuid.String() with dashes
		"pool-96edf9ee-d8ed-4292-9036-b63298ec5b2b", // hot-pool synthetic token
		"pool96edf9eed8ed42929036b63298ec5b2b",      // mongo canonical (dash-stripped pool token)
		"a1b2c3d4",                                  // legacy short hex
		"tok",                                       // unit-test / internal short token
		"e2e-tok",                                   // e2e cohort shape
		"E2E-Cohort.1",                              // mixed case + dot
	}
	for _, tok := range valid {
		if err := CheckNamingToken(tok); err != nil {
			t.Errorf("CheckNamingToken(%q): unexpected refusal: %v", tok, err)
		}
	}
}

func TestCheckNamingToken_RefusesDangerousForms(t *testing.T) {
	invalid := []string{
		"",                       // empty — would derive db_ / a *:* scan prefix
		"postgres",               // system database
		"template0",              // system database
		"template1",              // system database
		"instant_customers",      // the shared customer cluster's own database
		"instant_platform",       // the platform database
		"INSTANODE_ADMIN",        // admin role, case-insensitive
		"instant_cust",           // provisioner admin role
		"doadmin",                // DO managed-PG admin
		"admin",                  // mongo system db / generic admin
		"default",                // redis ACL admin user
		"root",                   // generic admin
		"db_*",                   // wildcard
		"tok; DROP DATABASE x",   // SQL metacharacters
		"a b",                    // whitespace
		`a"b`,                    // quote
		"%",                      // redis SCAN wildcard material
		strings.Repeat("a", 129), // over-long (absurdity bound)
	}
	for _, tok := range invalid {
		err := CheckNamingToken(tok)
		if err == nil {
			t.Errorf("CheckNamingToken(%q): expected refusal, got nil", tok)
			continue
		}
		if !errors.Is(err, ErrRefused) {
			t.Errorf("CheckNamingToken(%q): refusal not wrapped in ErrRefused: %v", tok, err)
		}
	}
}

func TestCheckDatabaseName_AcceptsPerTenantForms(t *testing.T) {
	valid := []string{
		"db_96edf9eed8ed42929036b63298ec5b2b",
		"db_96edf9ee-d8ed-4292-9036-b63298ec5b2b",
		"db_pool-96edf9ee-d8ed-4292-9036-b63298ec5b2b",
		"db_pool96edf9eed8ed42929036b63298ec5b2b", // mongo canonical pool form
		"db_96edf9eed8ed",                         // mongo legacy 12-char form
		"dedicated_db_96edf9eed8ed42929036b63298ec5b2b",
		"db_tok", // unit-test shape
	}
	for _, name := range valid {
		if err := CheckDatabaseName(name); err != nil {
			t.Errorf("CheckDatabaseName(%q): unexpected refusal: %v", name, err)
		}
	}
}

func TestCheckDatabaseName_RefusesSystemAndMisshapenNames(t *testing.T) {
	invalid := []string{
		"postgres", // THE truehomie nightmare targets
		"template0",
		"template1",
		"instant_customers",
		"Instant_Customers", // case-insensitive denylist
		"instant_platform",
		"admin", // mongo system dbs
		"local",
		"config",
		"",            // empty
		"db_",         // prefix with empty token
		"db_postgres", // reserved token behind a valid prefix
		"db_instant_customers",
		"96edf9eed8ed42929036b63298ec5b2b", // missing prefix entirely
		"customers",                        // arbitrary non-tenant name
		`db_x"; DROP DATABASE "postgres`,   // injection-shaped
		"db_" + strings.Repeat("a", 129),   // over-long (absurdity bound)
		"dedicated_db_",                    // dedicated prefix, empty token
	}
	for _, name := range invalid {
		err := CheckDatabaseName(name)
		if err == nil {
			t.Errorf("CheckDatabaseName(%q): expected refusal, got nil", name)
			continue
		}
		if !errors.Is(err, ErrRefused) {
			t.Errorf("CheckDatabaseName(%q): refusal not wrapped in ErrRefused: %v", name, err)
		}
	}
}

func TestCheckUserName_AcceptsPerTenantForms(t *testing.T) {
	valid := []string{
		"usr_96edf9eed8ed42929036b63298ec5b2b",
		"usr_96edf9ee-d8ed-4292-9036-b63298ec5b2b",
		"usr_pool-96edf9ee-d8ed-4292-9036-b63298ec5b2b",
		"usr_96edf9ee", // redis legacy 8-char ACL form
		"dedicated_usr_96edf9eed8ed42929036b63298ec5b2b",
		"usr_tok",
	}
	for _, name := range valid {
		if err := CheckUserName(name); err != nil {
			t.Errorf("CheckUserName(%q): unexpected refusal: %v", name, err)
		}
	}
}

func TestCheckUserName_RefusesSystemAndMisshapenNames(t *testing.T) {
	invalid := []string{
		"postgres",
		"instanode_admin", // the confirmed truehomie vector role
		"instant_cust",
		"doadmin",
		"default", // redis ACL admin — DELUSER default bricks the shared pod
		"admin",
		"root",
		"replication",
		"",
		"usr_",
		"usr_postgres",
		"usr_instanode_admin",
		"96edf9eed8ed42929036b63298ec5b2b", // missing prefix
		"usr_a b",
		"usr_" + strings.Repeat("a", 129),
		"dedicated_usr_",
	}
	for _, name := range invalid {
		err := CheckUserName(name)
		if err == nil {
			t.Errorf("CheckUserName(%q): expected refusal, got nil", name)
			continue
		}
		if !errors.Is(err, ErrRefused) {
			t.Errorf("CheckUserName(%q): refusal not wrapped in ErrRefused: %v", name, err)
		}
	}
}

// TestRegistryDenylistsAreSubsetsOfReservedTokens guards the rule-18 invariant:
// any identifier protected as a bare system database/role is ALSO refused when
// it appears as a naming-token (so a bug that flows the name into the token
// field is refused at the chokepoint too). Iterates the live registries, not a
// hand-typed list.
func TestRegistryDenylistsAreSubsetsOfReservedTokens(t *testing.T) {
	for name := range systemDatabases {
		// Mongo system dbs (admin/local/config) are protected as database
		// names; admin is additionally a reserved token. local/config are
		// harmless as tokens (db_local is tenant-shaped), so only assert the
		// postgres-side names.
		if name == "local" || name == "config" {
			continue
		}
		if !reservedTokens[name] {
			t.Errorf("systemDatabases[%q] is not in reservedTokens — a bug flowing it into the token field would not be refused at the chokepoint", name)
		}
	}
	for name := range systemRoles {
		if name == "replication" {
			continue // not a plausible token; protected at the name layer
		}
		if !reservedTokens[name] {
			t.Errorf("systemRoles[%q] is not in reservedTokens", name)
		}
	}
}
