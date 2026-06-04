package server_test

// server_rpc_coverage_guard_test.go — the DRIFT GUARD for the provisioner gRPC
// surface (done-bar §4, rule 18: registry-iterating, not hand-typed).
//
// Why this file exists: the real-backend round-trip coverage in
// server_live_roundtrip_test.go + server_live_roundtrip_mqs_test.go proves each
// of today's RPCs (ProvisionResource / DeprovisionResource / GetStorageBytes /
// RegradeResource) is exercised end-to-end against a genuine backend. But that
// guarantee is a snapshot: the day someone adds a new RPC to
// proto/provisioner/v1/provisioner.proto, the round-trip suite would NOT fail —
// the new method would simply ship with zero real-backend coverage, silently
// (the silent-untested-RPC class).
//
// This test closes that hole. It iterates the proto-GENERATED service descriptor
// (ProvisionerService_ServiceDesc.Methods — the single source of truth for which
// RPCs exist) and asserts every RPC maps to a maintained round-trip test that
// actually exists in this package's source. It fails CI if:
//   - a new RPC is added to the proto without a mapping here (unmapped RPC), or
//   - a mapping points at a test that no longer exists (deleted/renamed test), or
//   - a mapping is removed while its RPC still exists in the descriptor.
//
// It is a pure descriptor + source-scan test: no backends, no env — it runs in
// the `go test -short` deploy.yml gate and never skips. That is deliberate: the
// drift guard must be unconditional, or it cannot catch the drift it exists for.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	provisionerv1 "instant.dev/proto/provisioner/v1"
)

// rpcCoverage maps each gRPC RPC (by its proto MethodName) to the round-trip
// test(s) that drive it against a real backend. At least one mapped test must
// exist in this package's source for every RPC in the service descriptor.
//
// To exempt an RPC from round-trip coverage, give it an empty slice AND add a
// justification row to INTEGRATION-COVERAGE-EXCLUSIONS.md (the test enforces the
// empty-slice case is intentional via the exemptedRPCs set below).
var rpcCoverage = map[string][]string{
	"ProvisionResource": {
		"TestServer_Postgres_Provision_Regrade_Deprovision_LiveRoundTrip",
		"TestServer_Redis_Provision_Deprovision_LiveRoundTrip",
		"TestServer_Mongo_Provision_StorageBytes_Deprovision_LiveRoundTrip",
		"TestServer_Queue_Provision_Deprovision_LiveRoundTrip",
	},
	"DeprovisionResource": {
		"TestServer_Postgres_Provision_Regrade_Deprovision_LiveRoundTrip",
		"TestServer_Postgres_Reprovision_AfterDeprovision_LiveRoundTrip",
		"TestServer_Redis_Provision_Deprovision_LiveRoundTrip",
		"TestServer_Mongo_Provision_StorageBytes_Deprovision_LiveRoundTrip",
		"TestServer_Queue_Provision_Deprovision_LiveRoundTrip",
	},
	"GetStorageBytes": {
		"TestServer_Mongo_Provision_StorageBytes_Deprovision_LiveRoundTrip",
		"TestServer_Queue_Provision_Deprovision_LiveRoundTrip",
		"TestServer_Storage_GetStorageBytes_LiveRoundTrip",
	},
	"RegradeResource": {
		"TestServer_Postgres_Provision_Regrade_Deprovision_LiveRoundTrip",
		"TestServer_Mongo_Provision_StorageBytes_Deprovision_LiveRoundTrip",
		"TestServer_Queue_Provision_Deprovision_LiveRoundTrip",
	},
}

// exemptedRPCs is the set of RPCs intentionally mapped to zero round-trip tests.
// An RPC may appear here ONLY if it also has a justification row in
// INTEGRATION-COVERAGE-EXCLUSIONS.md. Empty today — every RPC has a round-trip.
var exemptedRPCs = map[string]bool{}

// TestGRPCSurface_EveryRPCHasRoundTripTest is the registry-iterating drift guard.
// It walks the proto-generated service descriptor and asserts each RPC has a
// maintained, existing round-trip test (or a justified exemption).
func TestGRPCSurface_EveryRPCHasRoundTripTest(t *testing.T) {
	// (1) The authoritative RPC list: the proto-generated service descriptor.
	descRPCs := descriptorRPCNames()
	if len(descRPCs) == 0 {
		t.Fatal("ProvisionerService_ServiceDesc.Methods is empty — descriptor introspection broke; the drift guard cannot run")
	}

	// (2) The set of test function names that actually exist in this package.
	existing := existingTestFuncs(t)

	descSet := make(map[string]bool, len(descRPCs))
	for _, rpc := range descRPCs {
		descSet[rpc] = true
	}

	// (3) Every RPC in the descriptor must be mapped (or exempted) AND its
	// mapped tests must really exist.
	for _, rpc := range descRPCs {
		mapped, ok := rpcCoverage[rpc]
		if !ok {
			t.Errorf("RPC %q exists in ProvisionerService_ServiceDesc but has NO entry in rpcCoverage. "+
				"A new RPC shipped without a real-backend round-trip test. Add a round-trip test "+
				"(reuse the harness in server_live_roundtrip_mqs_test.go) and map it here, or add an "+
				"exemption to exemptedRPCs + a justification row in INTEGRATION-COVERAGE-EXCLUSIONS.md.", rpc)
			continue
		}
		if len(mapped) == 0 {
			if !exemptedRPCs[rpc] {
				t.Errorf("RPC %q maps to zero round-trip tests but is not in exemptedRPCs. "+
					"Either add a round-trip test or mark it exempt (with a justification row in "+
					"INTEGRATION-COVERAGE-EXCLUSIONS.md).", rpc)
			}
			continue
		}
		for _, name := range mapped {
			if !existing[name] {
				t.Errorf("RPC %q maps to test %q, but no such test function exists in package server_test. "+
					"A round-trip test was deleted or renamed without updating rpcCoverage — the RPC is now "+
					"silently untested. Restore the test or fix the mapping.", rpc, name)
			}
		}
	}

	// (4) No stale mappings: every key in rpcCoverage must be a real RPC. This
	// catches a mapping left behind after an RPC was removed from the proto.
	for rpc := range rpcCoverage {
		if !descSet[rpc] {
			t.Errorf("rpcCoverage maps %q, which is NOT an RPC in ProvisionerService_ServiceDesc. "+
				"Remove the stale mapping (the RPC was renamed/removed in the proto).", rpc)
		}
	}
	for rpc := range exemptedRPCs {
		if !descSet[rpc] {
			t.Errorf("exemptedRPCs lists %q, which is NOT an RPC in ProvisionerService_ServiceDesc. "+
				"Remove the stale exemption.", rpc)
		}
	}
}

// descriptorRPCNames returns the sorted list of RPC MethodNames from the
// proto-generated grpc.ServiceDesc — the single source of truth.
func descriptorRPCNames() []string {
	names := make([]string, 0, len(provisionerv1.ProvisionerService_ServiceDesc.Methods))
	for _, m := range provisionerv1.ProvisionerService_ServiceDesc.Methods {
		names = append(names, m.MethodName)
	}
	// Streaming RPCs (none today) would also need coverage; include them so a
	// future streaming RPC is caught too.
	for _, s := range provisionerv1.ProvisionerService_ServiceDesc.Streams {
		names = append(names, s.StreamName)
	}
	sort.Strings(names)
	return names
}

// existingTestFuncs parses every *_test.go file in this directory and returns
// the set of top-level `func Test...` names. This is what makes a deleted or
// renamed round-trip test fail the guard: the mapping references a name that no
// longer parses out of the source.
func existingTestFuncs(t *testing.T) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, perr := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", name, perr)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil {
				continue
			}
			if strings.HasPrefix(fn.Name.Name, "Test") {
				out[fn.Name.Name] = true
			}
		}
	}
	return out
}
