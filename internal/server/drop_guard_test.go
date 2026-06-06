package server

// drop_guard_test.go — the CI GUARD that makes a NEW un-audited customer-data
// drop path UNMERGEABLE (rule 18: AST-iterating, not a hand-typed allowlist of
// line numbers).
//
// # WHY (truehomie-db DROP incident, 2026-06-03)
//
// An active customer's DB+role were dropped by an UNIDENTIFIED path with no audit
// trail. The durable fix for "an unidentified path dropped it" is structural: the
// ONLY sanctioned customer-data destruction is through server.guardedDrop ->
// DeprovisionResource -> backend.Deprovision. This test walks the provisioner
// source with go/parser and FAILS if a destructive operation
// (DROP DATABASE / DROP ROLE / DROP USER as a SQL literal, an `ACL DELUSER`
// literal, a mongo `dropDatabase`/`dropUser` command literal, or a
// `*mongo.Database.Drop(...)` call) appears inside a function that is NOT one of
// the sanctioned deprovision/rollback functions reached through guardedDrop.
//
// A new admin/dev/internal handler that drops a customer DB directly (the exact
// failure surface of the incident) lands its DROP in a non-sanctioned function
// and trips this test. To add a legitimate new destruction site, route it through
// guardedDrop and (if it is a new backend deprovision) add the function to
// sanctionedDropFuncs below — a deliberate, reviewed act, not a silent drift.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// sanctionedDropFuncs is the allowlist of function names permitted to contain a
// raw customer-data destruction. Every one of these is reached ONLY through
// server.guardedDrop (the audited chokepoint) — see DeprovisionResource. Adding
// a name here is a reviewed decision; it is the visible cost of introducing a new
// drop site.
//
// Keyed by simple function/method name (the AST gives us the *ast.FuncDecl name).
var sanctionedDropFuncs = map[string]bool{
	// Postgres shared cluster — the primary prod path.
	"Deprovision":             true, // (*LocalBackend).Deprovision (pg/redis/mongo), (*K8sBackend).Deprovision, (*DedicatedProvider).Deprovision, (*NeonBackend).Deprovision
	"cleanupProvisionPartial": true, // rollback of a just-created db on a failed provision (same request)
	"deprovisionLocal":        true, // (*DedicatedProvider).deprovisionLocal — dedicated local-admin DSN teardown
}

// dangerousSQLFragments are the destructive SQL fragments that may appear ONLY in
// a sanctioned function. Matched case-insensitively against string literals
// parsed from the AST (so comments never match — go/parser does not surface
// comments as expressions).
var dangerousSQLFragments = []string{
	"drop database",
	"drop role",
	"drop user",
	"drop schema",
}

// scanRoots are the package directories (relative to the provisioner module root)
// the guard walks. The whole backend tree plus the server package — i.e.
// everywhere a drop could be introduced.
var scanRoots = []string{
	"internal/backend",
	"internal/server",
	"internal/pool",
	"internal/handlers",
}

// moduleRoot returns the provisioner module root (two levels up from
// internal/server). Resolved from the test's working directory, which `go test`
// sets to the package dir.
func moduleRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	// .../provisioner/internal/server -> .../provisioner
	root := filepath.Dir(filepath.Dir(wd))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("could not locate provisioner module root from %s (looked at %s): %v", wd, root, err)
	}
	return root
}

// dropViolation is a single un-sanctioned destructive site.
type dropViolation struct {
	file string
	line int
	fn   string
	what string
}

func TestNoUnsanctionedDropSites(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()

	var violations []dropViolation
	// sawSanctioned proves the guard's allowlist is live, not vacuous: if the
	// known sanctioned drops disappear (refactor moved them, or the parser
	// silently matched nothing), this stays false and the test fails — a guard
	// that matches nothing is a broken guard.
	sawSanctioned := false

	for _, rel := range scanRoots {
		dir := filepath.Join(root, rel)
		if _, err := os.Stat(dir); err != nil {
			continue // package dir may not exist (e.g. handlers); skip silently
		}
		walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				t.Fatalf("parse %s: %v", path, perr)
			}
			inspectFileForDrops(fset, file, path, &violations, &sawSanctioned)
			return nil
		})
		if walkErr != nil {
			t.Fatalf("walk %s: %v", dir, walkErr)
		}
	}

	if !sawSanctioned {
		t.Fatal("CI guard found ZERO sanctioned drop sites — the guard matched nothing, " +
			"which means its matchers are broken (the provisioner backends DO contain " +
			"DROP DATABASE / DROP USER). A guard that matches nothing protects nothing.")
	}

	if len(violations) > 0 {
		var b strings.Builder
		b.WriteString("UN-SANCTIONED customer-data DROP site(s) found — every customer-data " +
			"destruction MUST route through server.guardedDrop (the audited chokepoint). " +
			"See docs/ci/DATA-INTEGRITY-DROP-PATH-AUDIT.md and the truehomie-db incident.\n")
		for _, v := range violations {
			b.WriteString("  " + v.file + ":" + itoa(v.line) + " in func " + v.fn + " — " + v.what + "\n")
		}
		b.WriteString("If this is a legitimate new deprovision path, route it through guardedDrop " +
			"and add its function name to sanctionedDropFuncs in drop_guard_test.go (a reviewed decision).")
		t.Fatal(b.String())
	}
}

// TestDropGuard_FlagsUnsanctionedSite proves the guard is NOT vacuous: a DROP
// DATABASE in a non-sanctioned function MUST be flagged. Without this, a guard
// whose matcher silently broke would pass (rule 18: a guard that matches nothing
// protects nothing). We parse a synthetic source string and run the same
// inspection used in production.
func TestDropGuard_FlagsUnsanctionedSite(t *testing.T) {
	const src = `package x
import "context"
func adminNukeDB(ctx context.Context, conn execer, db string) error {
	_, err := conn.Exec(ctx, "DROP DATABASE "+db)   // un-audited admin path
	return err
}
func collectionCleanup(c coll) { _ = c.Drop(ctx) } // mongo COLLECTION drop — must NOT be flagged
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "synthetic.go", src, 0)
	if err != nil {
		t.Fatalf("parse synthetic: %v", err)
	}
	var violations []dropViolation
	saw := false
	inspectFileForDrops(fset, file, "provisioner/synthetic.go", &violations, &saw)

	if len(violations) != 1 {
		t.Fatalf("expected exactly 1 violation (the un-sanctioned DROP DATABASE), got %d: %+v", len(violations), violations)
	}
	if violations[0].fn != "adminNukeDB" {
		t.Fatalf("expected violation in adminNukeDB, got %q", violations[0].fn)
	}
	if !strings.Contains(violations[0].what, "DROP DATABASE") {
		t.Fatalf("expected DROP DATABASE description, got %q", violations[0].what)
	}
}

// TestDropGuard_IgnoresComments proves a DROP DATABASE in a COMMENT is not
// flagged (go/parser does not emit comments as expressions) — so the heavily
// commented server.go / drop_chokepoint.go do not produce false positives.
func TestDropGuard_IgnoresComments(t *testing.T) {
	const src = `package x
// This function would DROP DATABASE if misused, but the comment must not match.
func benign() {}
`
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "synthetic_comment.go", src, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var violations []dropViolation
	saw := false
	inspectFileForDrops(fset, file, "provisioner/synthetic_comment.go", &violations, &saw)
	if len(violations) != 0 {
		t.Fatalf("comment must not be flagged, got %+v", violations)
	}
}

// inspectFileForDrops walks the AST of one file, attributing every destructive
// site to its enclosing function and recording a violation when that function is
// not sanctioned.
func inspectFileForDrops(fset *token.FileSet, file *ast.File, path string, violations *[]dropViolation, sawSanctioned *bool) {
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		fnName := fn.Name.Name
		ast.Inspect(fn, func(n ast.Node) bool {
			what, isDrop := destructiveNode(n)
			if !isDrop {
				return true
			}
			if sanctionedDropFuncs[fnName] {
				*sawSanctioned = true
				return true
			}
			pos := fset.Position(n.Pos())
			*violations = append(*violations, dropViolation{
				file: shortPath(path),
				line: pos.Line,
				fn:   fnName,
				what: what,
			})
			return true
		})
	}
}

// destructiveNode reports whether an AST node is a customer-data destruction and
// a short description of what it is. It matches:
//   - basic string literals containing a dangerous SQL fragment (DROP DATABASE/…)
//   - string literals "DELUSER" (the Redis ACL user-delete command)
//   - string literals "dropDatabase"/"dropUser" (Mongo admin commands)
//   - a call expression of the form `<x>.Database(<y>).Drop(...)` (Mongo db drop)
//
// Comments are never matched — go/parser does not emit comments as expressions,
// so a `// DROP DATABASE …` doc line is invisible here (verified by the test that
// the guard does NOT flag the heavily-commented server.go / drop_chokepoint.go).
func destructiveNode(n ast.Node) (string, bool) {
	switch v := n.(type) {
	case *ast.BasicLit:
		if v.Kind != token.STRING {
			return "", false
		}
		lit := strings.ToLower(v.Value)
		for _, frag := range dangerousSQLFragments {
			if strings.Contains(lit, frag) {
				return "SQL literal containing " + strings.ToUpper(frag), true
			}
		}
		if strings.Contains(lit, "deluser") {
			return "Redis ACL DELUSER literal", true
		}
		if strings.Contains(lit, "dropdatabase") || strings.Contains(lit, "dropuser") {
			return "Mongo drop command literal", true
		}
		return "", false
	case *ast.CallExpr:
		if isDatabaseDropCall(v) {
			return "Mongo Database(...).Drop(...) call", true
		}
		return "", false
	default:
		return "", false
	}
}

// isDatabaseDropCall matches `<expr>.Database(<args>).Drop(<args>)` — a Mongo
// database drop. It deliberately does NOT match `<coll>.Drop(...)` (a Mongo
// COLLECTION drop, used to clean up the provision sentinel doc — not customer
// data destruction).
func isDatabaseDropCall(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Drop" {
		return false
	}
	// The receiver of .Drop must itself be a call to .Database(...).
	inner, ok := sel.X.(*ast.CallExpr)
	if !ok {
		return false
	}
	innerSel, ok := inner.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	return innerSel.Sel.Name == "Database"
}

// shortPath trims everything up to and including "provisioner/" so failure
// messages are stable across machines.
func shortPath(p string) string {
	const marker = "provisioner/"
	if i := strings.LastIndex(p, marker); i >= 0 {
		return p[i+len(marker):]
	}
	return p
}

// itoa is a tiny strconv.Itoa to avoid importing strconv just for messages.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
