package main

// coverage_registry_test.go — Wave 2 (2026-05-20) registry-iterating
// regression test per CLAUDE.md rule 18.
//
// What the existing TestCollectBreakerInspectors_AllBackendsSurfaced
// (main_test.go) covers:
//   - given a real *circuit.Breakers, every name in the hand-typed
//     `want` map is surfaced by collectBreakerInspectors.
//
// What it does NOT catch: a new field added to circuit.Breakers
// without (a) the collectBreakerInspectors slice being extended AND
// (b) the test's hand-typed `want` map being extended. Either omission
// drops the new backend off /readyz silently AND the test still
// passes — the hand-typed `want` list cannot detect what it doesn't
// enumerate.
//
// This test closes the gap by walking the circuit.Breakers struct via
// reflection. The Breakers struct IS the registry; reflection turns it
// into the iteration source. A new field added to the struct
// automatically becomes a required entry in collectBreakerInspectors
// — caught at PR time, not after a customer-facing degradation.

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"instant.dev/provisioner/internal/circuit"
)

// TestBreakers_EveryStructFieldHasAnInspector — reflection-driven
// gate. The circuit.Breakers struct is the registry; every *Breaker
// field on it MUST be returned by collectBreakerInspectors. A new
// field added to the struct without an update to
// collectBreakerInspectors fails this test.
//
// COVERAGE BLOCK (rule 17):
//   Symptom:       a new circuit-protected backend is added to
//                  *circuit.Breakers (new field, e.g. NATSAdmin) but
//                  collectBreakerInspectors in main.go is not
//                  extended. The new backend's circuit state never
//                  surfaces on /readyz. A future tripped breaker
//                  doesn't show up as a degraded check, the NR alert
//                  filter (which keys on `circuit.opened` log lines)
//                  fires but operators have no /readyz signal to
//                  cross-reference.
//   Enumeration:   reflect over the *circuit.Breakers struct fields
//                  whose type is *circuit.Breaker. This IS the
//                  registry.
//   Sites found:   N fields on Breakers (currently 5: PostgresAdmin,
//                  PostgresK8s, RedisAdmin, MongoAdmin, K8sAPI).
//   Sites touched: each must be findable in the inspector slice
//                  collectBreakerInspectors returns. The match is
//                  by .Name() — every breaker carries the backend
//                  name as its identity.
//   Coverage test: missing-from-inspectors and missing-from-struct
//                  both fail.
//   Live verified: provisioner /readyz output today shows
//                  backend_postgres_admin / backend_postgres_k8s /
//                  backend_redis_admin / backend_mongo_admin /
//                  backend_k8s_api — five degraded checks. This test
//                  pins that count to whatever the struct says.
func TestBreakers_EveryStructFieldHasAnInspector(t *testing.T) {
	bs := circuit.NewBreakers()
	got := collectBreakerInspectors(bs)

	// Pull breaker names out of the inspector slice — these are
	// what /readyz uses as the `backend_<name>` check label.
	inspectorNames := map[string]bool{}
	for _, ins := range got {
		inspectorNames[ins.Name()] = true
	}

	// Enumerate Breakers struct fields via reflection. The struct
	// is `circuit.Breakers`; we look at the type information of a
	// dereferenced *Breakers value.
	bsVal := reflect.ValueOf(bs).Elem()
	bsType := bsVal.Type()

	// Map from struct field name (Go identifier) → expected breaker
	// name string. The breaker carries its name via Name() — that
	// name is set at NewBreaker time using one of the BackendXxx
	// constants in internal/circuit/breakers.go. We enumerate them
	// here so we can cross-check, but we DON'T hardcode the field
	// list — the struct walk drives it.
	expected := map[string]string{
		"PostgresK8s":   circuit.BackendPostgresK8s,
		"PostgresAdmin": circuit.BackendPostgresAdmin,
		"RedisAdmin":    circuit.BackendRedisAdmin,
		"MongoAdmin":    circuit.BackendMongoAdmin,
		"K8sAPI":        circuit.BackendK8sAPI,
	}

	// Forward: every struct field whose type is *circuit.Breaker
	// must be surfaced. A new field that has the right type but
	// is missing from `expected` above + collectBreakerInspectors
	// in main.go shows up here.
	var missingFromInspectors []string
	var missingFromExpectedMap []string
	breakerPtrType := reflect.TypeOf((*circuit.Breaker)(nil))
	for i := 0; i < bsType.NumField(); i++ {
		field := bsType.Field(i)
		if field.Type != breakerPtrType {
			continue
		}
		wantName, ok := expected[field.Name]
		if !ok {
			missingFromExpectedMap = append(missingFromExpectedMap, field.Name)
			continue
		}
		if !inspectorNames[wantName] {
			missingFromInspectors = append(missingFromInspectors,
				field.Name+" (expected backend name "+wantName+")")
		}
	}

	sort.Strings(missingFromExpectedMap)
	sort.Strings(missingFromInspectors)
	if len(missingFromExpectedMap) > 0 {
		t.Errorf("the following struct fields on circuit.Breakers are *circuit.Breaker but not in this test's `expected` map — a new breaker landed without updating BOTH the inspector wiring (main.go:collectBreakerInspectors) AND this test:\n  %s\n\nAdd an entry mapping the Go field name → the circuit.BackendXxx constant so /readyz can surface the new backend.",
			strings.Join(missingFromExpectedMap, "\n  "))
	}
	if len(missingFromInspectors) > 0 {
		t.Errorf("the following breakers are declared on circuit.Breakers but NOT returned by collectBreakerInspectors — they will not appear on /readyz as backend_<name> checks:\n  %s\n\nFix: extend the slice literal in main.go:collectBreakerInspectors with `breakerAdapter{b: bs.<Field>}`.",
			strings.Join(missingFromInspectors, "\n  "))
	}

	// Reverse: every entry in `expected` must correspond to a real
	// struct field. Catches stale entries from a renamed/removed
	// breaker.
	var staleExpected []string
	for fieldName := range expected {
		if _, ok := bsType.FieldByName(fieldName); !ok {
			staleExpected = append(staleExpected, fieldName)
		}
	}
	sort.Strings(staleExpected)
	if len(staleExpected) > 0 {
		t.Errorf("the following entries in `expected` refer to struct fields that no longer exist on circuit.Breakers — remove the stale entry:\n  %s",
			strings.Join(staleExpected, "\n  "))
	}

	// Reverse-reverse: every inspector name must correspond to a
	// known backend (a sanity check — if the inspector wiring
	// surfaces a name that isn't in our constants, there's a
	// rogue adapter somewhere).
	knownBackendNames := map[string]bool{}
	for _, v := range expected {
		knownBackendNames[v] = true
	}
	var rogueNames []string
	for name := range inspectorNames {
		if !knownBackendNames[name] {
			rogueNames = append(rogueNames, name)
		}
	}
	sort.Strings(rogueNames)
	if len(rogueNames) > 0 {
		t.Errorf("the following inspector names are surfaced but not mapped to a known backend — possibly a leftover adapter from a deleted breaker:\n  %s",
			strings.Join(rogueNames, "\n  "))
	}
}
