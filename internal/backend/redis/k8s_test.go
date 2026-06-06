package redis

// k8s_test.go — unit + integration-style tests for K8sBackend.
//
// # Test categories
//
//  1. TestSizingForTier_* — regression guards for tier→maxmemory mapping
//     (A4 fix; no k8s cluster required).
//
//  2. TestParseConfigGetMaxmemory — unit tests for the helper that parses
//     redis-cli CONFIG GET output.
//
//  3. TestExecCommandConstruction_* — table-driven tests that verify the exact
//     shell commands issued by regradeViaExec for every tier. Asserts that:
//     - $REDIS_PASSWORD is referenced as a shell variable (never interpolated as a literal).
//     - The correct maxmemory byte value and policy are used per tier.
//     - The team tier (unlimited) produces targetBytes=0 and policy=noeviction.
//
//  4. TestRegrade_SecretPresent_UsesSecretPath — integration-style test using a
//     fakeK8s stub. Asserts that when the redis-auth Secret IS present, Regrade
//     does NOT fall through to the exec path.
//
//  5. TestRegrade_SecretAbsent_UsesExecPath — integration-style test (A4 regression
//     guard). Asserts that when the redis-auth Secret is ABSENT, Regrade falls back
//     to the exec path and issues CONFIG SET — NOT a silent skip. This test MUST
//     FAIL if a future change makes the Secret-absent case silently skip again.
//
//  6. TestRegrade_ExecFails_SoftSkip — asserts that a failing exec returns a
//     soft-skip result (no error propagated) so the reconciler sweep continues.
//
//  7. TestRegrade_NoPodRunning_SoftSkip — asserts that when no Running pod exists
//     the exec fallback returns a soft-skip result.

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

// ─── Tier sizing regression guards ──────────────────────────────────────────

// TestSizingForTier_MaxmemoryMB_MatchesPlansYAML verifies that maxmemoryMB for
// each tier matches the redis_memory_mb value in plans.yaml (A4 regression guard).
// If plans.yaml is updated and these values are not kept in sync, the test fails.
func TestSizingForTier_MaxmemoryMB_MatchesPlansYAML(t *testing.T) {
	cases := []struct {
		tier        string
		wantMB      int  // expected maxmemoryMB (mirrors plans.yaml redis_memory_mb)
		expectLimit bool // true if --maxmemory flag should be applied
	}{
		{"anonymous", 5, true},   // plans.yaml: anonymous redis_memory_mb = 5
		{"hobby", 50, true},      // plans.yaml: hobby redis_memory_mb = 50
		{"hobby_yearly", 50, true}, // plans.yaml: hobby_yearly mirrors hobby
		{"hobby_plus", 50, true}, // plans.yaml: hobby_plus redis_memory_mb = 50
		{"hobby_plus_yearly", 50, true}, // plans.yaml: hobby_plus_yearly mirrors hobby_plus
		{"pro", 512, true},       // plans.yaml: pro redis_memory_mb = 512
		{"pro_yearly", 512, true},// plans.yaml: pro_yearly mirrors pro
		{"team", -1, false},      // "no cap" pod-start default — flag omitted; Regrade reconciles to registry (team=1536, finite post strict-80)
		{"team_yearly", -1, false}, // team_yearly mirrors team's pod-start sizing default
		{"growth", 1024, true},   // plans.yaml: growth redis_memory_mb = 1024
		{"unknown", 50, true},    // unknown → hobby fallback
	}
	for _, tc := range cases {
		t.Run(tc.tier, func(t *testing.T) {
			sz := sizingForTier(tc.tier)
			if sz.maxmemoryMB != tc.wantMB {
				t.Errorf("sizingForTier(%q).maxmemoryMB = %d; want %d (plans.yaml redis_memory_mb)",
					tc.tier, sz.maxmemoryMB, tc.wantMB)
			}
			// Verify the flag would be applied (or omitted) correctly.
			wouldApply := sz.maxmemoryMB > 0
			if wouldApply != tc.expectLimit {
				t.Errorf("sizingForTier(%q): maxmemoryMB=%d → expectLimit=%v but got wouldApply=%v",
					tc.tier, sz.maxmemoryMB, tc.expectLimit, wouldApply)
			}
		})
	}
}

// TestSizingForTier_MaxmemoryFlag_InCommand verifies that the Redis server
// command includes --maxmemory / --maxmemory-policy for limited tiers and
// omits them for the unlimited tier (team).
func TestSizingForTier_MaxmemoryFlag_InCommand(t *testing.T) {
	// Build the command slice the same way applyDeployment does.
	buildCmd := func(sz tierSizing) []string {
		cmd := []string{
			"redis-server",
			"--requirepass", "$(REDIS_PASSWORD)",
			"--appendonly", "yes",
			"--dir", "/data",
			"--maxclients", fmt.Sprintf("%d", sz.maxClients),
		}
		if sz.maxmemoryMB > 0 {
			cmd = append(cmd,
				"--maxmemory", fmt.Sprintf("%dmb", sz.maxmemoryMB),
				"--maxmemory-policy", redisMaxmemoryPolicyCapped,
			)
		}
		return cmd
	}

	containsFlag := func(cmd []string, flag string) bool {
		for _, arg := range cmd {
			if arg == flag {
				return true
			}
		}
		return false
	}

	// flagValue returns the argument immediately after flag, or "".
	flagValue := func(cmd []string, flag string) string {
		for i, arg := range cmd {
			if arg == flag && i+1 < len(cmd) {
				return cmd[i+1]
			}
		}
		return ""
	}

	limitedTiers := []string{"anonymous", "hobby", "hobby_yearly", "hobby_plus", "hobby_plus_yearly", "pro", "pro_yearly", "growth"}
	for _, tier := range limitedTiers {
		t.Run("limited/"+tier, func(t *testing.T) {
			sz := sizingForTier(tier)
			cmd := buildCmd(sz)
			if !containsFlag(cmd, "--maxmemory") {
				t.Errorf("tier %q: --maxmemory flag missing from Redis command (maxmemoryMB=%d)", tier, sz.maxmemoryMB)
			}
			if !containsFlag(cmd, "--maxmemory-policy") {
				t.Errorf("tier %q: --maxmemory-policy flag missing from Redis command", tier)
			}
			// P1-C: capped tiers must use noeviction so writes fail loudly at
			// the cap rather than silently evicting customer keys.
			if got := flagValue(cmd, "--maxmemory-policy"); got != "noeviction" {
				t.Errorf("tier %q: --maxmemory-policy = %q, want noeviction (P1-C)", tier, got)
			}
		})
	}

	unlimitedTiers := []string{"team", "team_yearly"}
	for _, tier := range unlimitedTiers {
		t.Run("unlimited/"+tier, func(t *testing.T) {
			sz := sizingForTier(tier)
			cmd := buildCmd(sz)
			if containsFlag(cmd, "--maxmemory") {
				t.Errorf("tier %q: --maxmemory flag should be absent for unlimited tier", tier)
			}
		})
	}
}

// ─── P1-A: route-key TTL per tier ───────────────────────────────────────────

// TestRouteKeyTTLForTier_PaidTiersNeverExpire is the P1-A regression guard.
//
// CONTRACT: route-registry keys for paid/permanent resources MUST be written
// with no expiry (persistRouteKey == 0). The provisioner only re-Sets these
// keys on Provision; a long-lived paid Redis that is never re-provisioned would
// silently lose its proxy route — and become unreachable — if it carried the
// 365-day TTL.
//
// Anonymous resources (24h lifetime) keep a long self-healing TTL so an
// orphaned key from a failed Deprovision eventually disappears.
//
// If a future change reintroduces a TTL for any paid tier, this test fails.
func TestRouteKeyTTLForTier_PaidTiersNeverExpire(t *testing.T) {
	if persistRouteKey != 0 {
		t.Fatalf("persistRouteKey must be 0 (go-redis: no expiry); got %v", persistRouteKey)
	}
	cases := []struct {
		tier    string
		wantTTL time.Duration
	}{
		{"anonymous", anonRouteKeyTTL}, // only anonymous gets a TTL
		{"hobby", persistRouteKey},
		{"hobby_plus", persistRouteKey},
		{"pro", persistRouteKey},
		{"growth", persistRouteKey},
		{"team", persistRouteKey},
		{"", persistRouteKey},        // empty/unknown → fail safe to persistent
		{"some_future_tier", persistRouteKey},
	}
	for _, tc := range cases {
		t.Run(tc.tier, func(t *testing.T) {
			got := routeKeyTTLForTier(tc.tier)
			if got != tc.wantTTL {
				t.Errorf("routeKeyTTLForTier(%q) = %v; want %v", tc.tier, got, tc.wantTTL)
			}
			// Every non-anonymous tier MUST be persistent (TTL 0). A live paid
			// route must never expire out from under a running pod.
			if tc.tier != anonymousTier && got != 0 {
				t.Errorf("P1-A REGRESSION: routeKeyTTLForTier(%q) = %v; paid/permanent "+
					"resources must have NO route-key expiry (got non-zero TTL)", tc.tier, got)
			}
		})
	}
}

// ─── parseConfigGetMaxmemory unit tests ─────────────────────────────────────

func TestParseConfigGetMaxmemory(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		wantMB  int64 // expected bytes (direct); ignored when wantErr
		wantErr bool  // a parse failure must surface, NOT silently return 0
	}{
		{
			name:   "standard redis-cli output",
			input:  "maxmemory\n52428800\n",
			wantMB: 52428800, // 50 MB
		},
		{
			name:   "zero means unlimited",
			input:  "maxmemory\n0\n",
			wantMB: 0,
		},
		{
			name:   "with trailing whitespace",
			input:  "maxmemory\n536870912 \n",
			wantMB: 536870912, // 512 MB
		},
		{
			name:   "with auth warning prefix line",
			input:  "Warning: Using a password with '-a' or '-u' option on the command line interface may not be safe.\nmaxmemory\n5242880\n",
			wantMB: 5242880, // 5 MB
		},
		{
			name:    "empty output errors (must not read as 0 = unlimited)",
			input:   "",
			wantErr: true,
		},
		{
			name:    "no maxmemory line errors",
			input:   "somekey\n123\n",
			wantErr: true,
		},
		{
			name:    "non-integer maxmemory value errors",
			input:   "maxmemory\nNaN\n",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseConfigGetMaxmemory(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseConfigGetMaxmemory(%q) = (%d, nil); want an error", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Errorf("parseConfigGetMaxmemory(%q) unexpected error: %v", tc.input, err)
			}
			if got != tc.wantMB {
				t.Errorf("parseConfigGetMaxmemory(%q) = %d; want %d", tc.input, got, tc.wantMB)
			}
		})
	}
}

// TestParseUsedMemory — a malformed INFO body must surface an error rather than
// be reported as 0 bytes (which would silently under-report quota usage).
func TestParseUsedMemory(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{name: "standard INFO memory", input: "# Memory\r\nused_memory:1048576\r\nused_memory_human:1.00M\r\n", want: 1048576},
		{name: "no trailing CR", input: "used_memory:524288\n", want: 524288},
		{name: "missing used_memory line errors", input: "# Memory\r\nmaxmemory:0\r\n", wantErr: true},
		{name: "empty input errors", input: "", wantErr: true},
		{name: "non-integer value errors", input: "used_memory:notanumber\n", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseUsedMemory(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseUsedMemory(%q) = (%d, nil); want an error", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Errorf("parseUsedMemory(%q) unexpected error: %v", tc.input, err)
			}
			if got != tc.want {
				t.Errorf("parseUsedMemory(%q) = %d; want %d", tc.input, got, tc.want)
			}
		})
	}
}

// ─── Exec command construction tests ────────────────────────────────────────

// execRecord records a call to fakeExecor.ExecInPod.
type execRecord struct {
	namespace     string
	podName       string
	containerName string
	cmd           []string
}

// fakeExecor implements PodExecor for testing. It records every call and
// returns the configured responses.
type fakeExecor struct {
	// responses is a queue of (stdout, err) pairs returned in order.
	// If the queue is exhausted, subsequent calls return ("", nil).
	responses []fakeExecResponse
	calls     []execRecord
}

type fakeExecResponse struct {
	stdout string
	err    error
}

func (f *fakeExecor) ExecInPod(_ context.Context, namespace, podName, containerName string, cmd []string, stdout, _ *bytes.Buffer) error {
	f.calls = append(f.calls, execRecord{
		namespace:     namespace,
		podName:       podName,
		containerName: containerName,
		cmd:           cmd,
	})
	if len(f.responses) == 0 {
		stdout.WriteString("OK\n")
		return nil
	}
	resp := f.responses[0]
	f.responses = f.responses[1:]
	stdout.WriteString(resp.stdout)
	return resp.err
}

// TestExecCommandConstruction verifies that regradeViaExec sends the correct
// shell commands for each tier. Key assertions:
//  1. $REDIS_PASSWORD is a shell variable reference, NEVER an interpolated literal.
//  2. The CONFIG SET maxmemory value matches sizingForTier(tier).maxmemoryMB * 1024 * 1024.
//  3. team tier → maxmemory=0, policy=noeviction.
//  4. Limited tiers → maxmemory>0, policy=noeviction (P1-C: writes fail loudly
//     at the cap instead of silently evicting customer keys).
func TestExecCommandConstruction(t *testing.T) {
	cases := []struct {
		tier           string
		targetMB       int
		wantTargetBytes int64
		wantPolicy     string
		wantMaxmem     string // expected substring in the CONFIG SET maxmemory command
	}{
		{"anonymous", 5, 5 * 1024 * 1024, "noeviction", "5242880"},
		{"hobby", 50, 50 * 1024 * 1024, "noeviction", "52428800"},
		{"pro", 512, 512 * 1024 * 1024, "noeviction", "536870912"},
		{"growth", 1024, 1024 * 1024 * 1024, "noeviction", "1073741824"},
		{"team", -1, 0, "noeviction", "maxmemory 0"},  // unlimited — check full "maxmemory 0" substring
	}

	for _, tc := range cases {
		t.Run(tc.tier, func(t *testing.T) {
			// Pod present and running, secret absent → exec path.
			pod := runningPod("instant-customer-tok", "redis-abc", "redis")
			cs := fake.NewClientset(pod)

			// For the team tier (unlimited, targetBytes=0) we seed the current
			// value with a non-zero value (e.g. 50 MB) so the idempotency check
			// doesn't short-circuit. Other tiers use current=0 which is always
			// less than their positive target.
			currentMaxmemStr := "0"
			if tc.targetMB <= 0 {
				currentMaxmemStr = "52428800" // 50 MB — differs from target (0)
			}

			fe := &fakeExecor{
				responses: []fakeExecResponse{
					// CONFIG GET response: current differs from target, so update is needed.
					{stdout: fmt.Sprintf("maxmemory\n%s\n", currentMaxmemStr)},
					// CONFIG SET maxmemory → OK
					{stdout: "+OK\n"},
					// CONFIG SET maxmemory-policy → OK
					{stdout: "+OK\n"},
					// CONFIG REWRITE → OK
					{stdout: "+OK\n"},
				},
			}

			b := &K8sBackend{cs: cs, execor: fe}
			// Simulate missing redis-auth secret: fake client returns NotFound automatically
			// since we didn't add the secret to the fake clientset.

			result, err := b.regradeViaExec(context.Background(), "instant-customer-tok", "tok", tc.targetMB)
			if err != nil {
				t.Fatalf("regradeViaExec returned error: %v", err)
			}
			if !result.Applied {
				t.Fatalf("expected Applied=true for tier %q (current=0, target=%d bytes), got SkipReason=%q",
					tc.tier, tc.wantTargetBytes, result.SkipReason)
			}
			if result.AppliedMaxmemory != tc.wantTargetBytes {
				t.Errorf("AppliedMaxmemory = %d; want %d", result.AppliedMaxmemory, tc.wantTargetBytes)
			}

			// Verify at least 2 exec calls were made (CONFIG GET + CONFIG SET maxmemory).
			if len(fe.calls) < 2 {
				t.Fatalf("expected at least 2 exec calls, got %d", len(fe.calls))
			}

			// Call 0: CONFIG GET maxmemory
			getCall := fe.calls[0]
			if !strings.Contains(strings.Join(getCall.cmd, " "), "CONFIG GET maxmemory") {
				t.Errorf("call[0] should be CONFIG GET maxmemory, got: %v", getCall.cmd)
			}
			// PASSWORD must appear as $REDIS_PASSWORD (shell variable), never as a literal.
			// The password variable itself is fine; what we forbid is any hardcoded
			// secret string. Since we never inject one, this check ensures the pattern
			// `$REDIS_PASSWORD` appears in every redis-cli command.
			for i, call := range fe.calls {
				cmdStr := strings.Join(call.cmd, " ")
				if !strings.Contains(cmdStr, `$REDIS_PASSWORD`) {
					t.Errorf("call[%d] cmd does not reference $REDIS_PASSWORD: %v", i, call.cmd)
				}
			}

			// Call 1: CONFIG SET maxmemory <bytes>
			setMemCall := fe.calls[1]
			setMemStr := strings.Join(setMemCall.cmd, " ")
			if !strings.Contains(setMemStr, "CONFIG SET maxmemory") {
				t.Errorf("call[1] should be CONFIG SET maxmemory, got: %v", setMemCall.cmd)
			}
			if !strings.Contains(setMemStr, tc.wantMaxmem) {
				t.Errorf("call[1] CONFIG SET maxmemory should contain %q (bytes), got: %v", tc.wantMaxmem, setMemCall.cmd)
			}

			// Call 2: CONFIG SET maxmemory-policy <policy>
			if len(fe.calls) >= 3 {
				setPolicyStr := strings.Join(fe.calls[2].cmd, " ")
				if !strings.Contains(setPolicyStr, tc.wantPolicy) {
					t.Errorf("call[2] CONFIG SET maxmemory-policy should contain %q, got: %v", tc.wantPolicy, fe.calls[2].cmd)
				}
			}
		})
	}
}

// TestExecCommandConstruction_AlreadyCorrect verifies that when the current
// maxmemory already matches the target, regradeViaExec returns Applied=false
// without issuing a CONFIG SET.
func TestExecCommandConstruction_AlreadyCorrect(t *testing.T) {
	const targetMB = 50
	const targetBytes = 50 * 1024 * 1024 // 52428800

	pod := runningPod("instant-customer-tok", "redis-abc", "redis")
	cs := fake.NewClientset(pod)

	fe := &fakeExecor{
		responses: []fakeExecResponse{
			// CONFIG GET returns the target value — already correct.
			{stdout: fmt.Sprintf("maxmemory\n%d\n", targetBytes)},
		},
	}

	b := &K8sBackend{cs: cs, execor: fe}
	result, err := b.regradeViaExec(context.Background(), "instant-customer-tok", "tok", targetMB)
	if err != nil {
		t.Fatalf("regradeViaExec returned error: %v", err)
	}
	if result.Applied {
		t.Errorf("Applied should be false when maxmemory is already correct")
	}
	if result.SkipReason != "already correct" {
		t.Errorf("SkipReason = %q; want %q", result.SkipReason, "already correct")
	}
	// Only the CONFIG GET call should have been made.
	if len(fe.calls) != 1 {
		t.Errorf("expected exactly 1 exec call (CONFIG GET only), got %d", len(fe.calls))
	}
}

// ─── Integration-style tests: Regrade routing ────────────────────────────────

// nsObject returns a minimal corev1.Namespace fixture for use with fake.NewClientset.
// Required after the orphaned-resource short-circuit (k8s.go) — Regrade now returns
// SkipReason="namespace not found (resource orphaned)" if the namespace is absent,
// so every Regrade test that exercises the existing code paths must include it.
func nsObject(name string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

// TestRegrade_SecretPresent_DoesNotUseExec asserts that when the redis-auth
// Secret IS present, Regrade uses the direct-connection path and never calls
// the exec fallback.
//
// We cannot fully exercise the direct-connection path without a real Redis
// server. This test verifies the routing decision by using a deliberately
// short context deadline to abort the Redis dial quickly, then asserting that
// the fakeExecor received zero calls — proving the exec path was NOT taken.
func TestRegrade_SecretPresent_DoesNotUseExec(t *testing.T) {
	const ns = "instant-customer-tok"
	const token = "tok"

	// Secret present in the namespace.
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "redis-auth", Namespace: ns},
		Data:       map[string][]byte{"REDIS_PASSWORD": []byte("s3cr3t")},
	}
	// Service present (needed for the direct-connection path after secret lookup).
	// Use a loopback address with a closed port so the dial fails fast.
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "redis", Namespace: ns},
		Spec:       corev1.ServiceSpec{ClusterIP: "127.0.0.1"},
	}
	cs := fake.NewClientset(nsObject(ns), secret, svc)

	fe := &fakeExecor{}
	b := &K8sBackend{cs: cs, execor: fe}

	// Short deadline so the Redis dial fails fast without blocking the test suite.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// We expect the direct-connection path to attempt a Redis dial and fail
	// (no real Redis running). The important assertion is that the fakeExecor
	// received no calls — proving the exec path was NOT taken.
	_, _ = b.Regrade(ctx, token, ns, 50)

	if len(fe.calls) != 0 {
		t.Errorf("fakeExecor should NOT be called when redis-auth secret is present; got %d calls", len(fe.calls))
	}
}

// TestRegrade_SecretAbsent_UsesExecPath is the A4 regression guard.
//
// CONTRACT: when the redis-auth Secret is absent (legacy resource), Regrade MUST
// fall back to exec and issue CONFIG SET — it must NOT silently skip.
//
// If a future refactor makes the Secret-absent case silently skip again, this
// test will fail on the Applied=false assertion, surfacing the regression.
func TestRegrade_SecretAbsent_UsesExecPath(t *testing.T) {
	const ns = "instant-customer-tok"
	const token = "tok"

	// No secret — secret lookup returns NotFound.
	// Pod is present and Running so exec can proceed.
	pod := runningPod(ns, "redis-aaa", "redis")
	cs := fake.NewClientset(nsObject(ns), pod)

	fe := &fakeExecor{
		responses: []fakeExecResponse{
			// CONFIG GET: current=0 so update is needed.
			{stdout: "maxmemory\n0\n"},
			// CONFIG SET maxmemory → OK.
			{stdout: "+OK\n"},
			// CONFIG SET maxmemory-policy → OK.
			{stdout: "+OK\n"},
			// CONFIG REWRITE → OK.
			{stdout: "+OK\n"},
		},
	}
	b := &K8sBackend{cs: cs, execor: fe}

	result, err := b.Regrade(context.Background(), token, ns, 50)
	if err != nil {
		t.Fatalf("Regrade returned error: %v", err)
	}

	// ── Core A4 regression assertion ──────────────────────────────────────
	// Applied MUST be true: a legacy pod without a Secret must receive the
	// CONFIG SET via exec, not be silently skipped.
	if !result.Applied {
		t.Fatalf("A4 REGRESSION: Regrade returned Applied=false for a Secret-absent resource.\n"+
			"SkipReason=%q\n"+
			"This means the exec fallback did not run — the legacy pod never received CONFIG SET.\n"+
			"Expected the exec path to be used and Applied=true.",
			result.SkipReason)
	}

	const wantTargetBytes = 50 * 1024 * 1024
	if result.AppliedMaxmemory != wantTargetBytes {
		t.Errorf("AppliedMaxmemory = %d; want %d", result.AppliedMaxmemory, wantTargetBytes)
	}

	// The exec fallback must have been used (at least CONFIG GET + CONFIG SET).
	if len(fe.calls) < 2 {
		t.Errorf("expected at least 2 exec calls via fallback, got %d", len(fe.calls))
	}
}

// TestRegrade_ExecFails_SoftSkip asserts that an exec failure during the CONFIG GET
// step returns a soft-skip result (Applied=false, no error propagated). This ensures
// the reconciler sweep continues even when a single pod is unreachable.
func TestRegrade_ExecFails_SoftSkip(t *testing.T) {
	const ns = "instant-customer-tok"
	const token = "tok"

	pod := runningPod(ns, "redis-aaa", "redis")
	cs := fake.NewClientset(nsObject(ns), pod)

	fe := &fakeExecor{
		responses: []fakeExecResponse{
			{stdout: "", err: fmt.Errorf("exec: connection refused")},
		},
	}
	b := &K8sBackend{cs: cs, execor: fe}

	result, err := b.Regrade(context.Background(), token, ns, 50)
	if err != nil {
		t.Fatalf("Regrade must not propagate exec errors (fail-soft); got: %v", err)
	}
	if result.Applied {
		t.Errorf("Applied should be false on exec failure")
	}
	if result.SkipReason == "" {
		t.Errorf("SkipReason should be non-empty on exec failure")
	}
}

// TestRegrade_NoPodRunning_SoftSkip asserts that when no Running pod exists
// in the namespace, the exec fallback returns a soft-skip (no exec attempted,
// no error propagated).
func TestRegrade_NoPodRunning_SoftSkip(t *testing.T) {
	const ns = "instant-customer-tok"
	const token = "tok"

	// Pod exists but is in Pending state (not Running).
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-aaa",
			Namespace: ns,
			Labels:    map[string]string{"app": "redis"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending},
	}
	cs := fake.NewClientset(nsObject(ns), pod)

	fe := &fakeExecor{}
	b := &K8sBackend{cs: cs, execor: fe}

	result, err := b.Regrade(context.Background(), token, ns, 50)
	if err != nil {
		t.Fatalf("Regrade must not propagate errors when no Running pod; got: %v", err)
	}
	if result.Applied {
		t.Errorf("Applied should be false when no Running pod")
	}
	if len(fe.calls) != 0 {
		t.Errorf("fakeExecor must not be called when no Running pod; got %d calls", len(fe.calls))
	}
}

// TestRegrade_NoPodsAtAll_SoftSkip asserts that when the namespace EXISTS but has
// no pods at all (e.g. a legacy pod was deleted but the namespace lingers), the
// exec fallback returns a soft-skip.
func TestRegrade_NoPodsAtAll_SoftSkip(t *testing.T) {
	const ns = "instant-customer-tok"
	const token = "tok"

	// Namespace exists, but no secret + no pods → exec fallback → no pod found.
	cs := fake.NewClientset(nsObject(ns))

	fe := &fakeExecor{}
	b := &K8sBackend{cs: cs, execor: fe}

	result, err := b.Regrade(context.Background(), token, ns, 50)
	if err != nil {
		t.Fatalf("Regrade must not propagate errors when no pods exist; got: %v", err)
	}
	if result.Applied {
		t.Errorf("Applied should be false when namespace has no pods")
	}
	// Distinct skip reason: not the orphan-namespace path.
	if result.SkipReason == "namespace not found (resource orphaned)" {
		t.Errorf("SkipReason should be exec-fallback reason, not orphan-ns reason; got %q", result.SkipReason)
	}
}

// TestRegrade_NamespaceMissing_QuietSkip asserts the orphaned-resource short-circuit:
// when the namespace is gone (deprovisioned tenant, force-deleted ns, wiped cluster),
// Regrade returns a quiet skip with the orphan SkipReason and makes NO Secrets.Get
// or Pods.List call — verified by checking the fakeExecor.calls count stays zero AND
// the SkipReason exactly matches the orphan sentinel string.
//
// Regression guard: prevents the WARN-spam-per-tick behaviour observed in prod on
// 2026-05-30 (2 orphaned namespaces emitting ~576 WARN/day combined).
func TestRegrade_NamespaceMissing_QuietSkip(t *testing.T) {
	const ns = "instant-customer-gone"
	const token = "gone"

	// Empty cluster — no namespace exists. The pre-fix code would have
	// hit Secrets.Get → IsNotFound → exec-fallback → Pods.List → WARN.
	cs := fake.NewClientset()

	fe := &fakeExecor{}
	b := &K8sBackend{cs: cs, execor: fe}

	result, err := b.Regrade(context.Background(), token, ns, 50)
	if err != nil {
		t.Fatalf("Regrade must not propagate errors for missing namespace; got: %v", err)
	}
	if result.Applied {
		t.Errorf("Applied should be false when namespace is missing")
	}
	const wantSkip = "namespace not found (resource orphaned)"
	if result.SkipReason != wantSkip {
		t.Errorf("SkipReason = %q; want %q (distinct from exec-fallback reasons so operators can differentiate orphans from real legacy drift)",
			result.SkipReason, wantSkip)
	}
	// Critical: the exec path MUST NOT be entered for orphans. That was the
	// pre-fix bug — fanout to Pods.List per orphan per tick.
	if len(fe.calls) != 0 {
		t.Errorf("fakeExecor must not be called when namespace is missing; got %d calls", len(fe.calls))
	}
}

// TestRegrade_NamespaceLookupTransientError_PropagatesError asserts that a
// non-IsNotFound error from the namespace lookup (e.g. permission denied,
// apiserver transport failure) is surfaced to the caller — NOT swallowed as a
// silent skip. The reconciler needs the error to schedule a retry; treating it
// as "orphaned" would cause skipped grades the customer pays for.
func TestRegrade_NamespaceLookupTransientError_PropagatesError(t *testing.T) {
	const ns = "instant-customer-tok"
	const token = "tok"

	cs := fake.NewClientset(nsObject(ns))
	// Inject a transient error on the namespace GET reactor.
	cs.PrependReactor("get", "namespaces", func(_ ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("etcd: leader changed")
	})

	fe := &fakeExecor{}
	b := &K8sBackend{cs: cs, execor: fe}

	result, err := b.Regrade(context.Background(), token, ns, 50)
	if err == nil {
		t.Fatalf("Regrade must propagate non-IsNotFound errors so the reconciler retries; got nil error, result=%+v", result)
	}
	if result.Applied {
		t.Errorf("Applied should be false on namespace lookup error")
	}
	if len(fe.calls) != 0 {
		t.Errorf("fakeExecor must not be called on namespace lookup error; got %d calls", len(fe.calls))
	}
}

// TestRegrade_TeamTier_Unlimited verifies that the team tier (unlimited, targetMB=-1)
// correctly sets maxmemory=0 and policy=noeviction via the exec path.
func TestRegrade_TeamTier_Unlimited(t *testing.T) {
	const ns = "instant-customer-tok"
	const token = "tok"

	pod := runningPod(ns, "redis-aaa", "redis")
	cs := fake.NewClientset(pod)

	fe := &fakeExecor{
		responses: []fakeExecResponse{
			// CONFIG GET: current non-zero → update needed.
			{stdout: "maxmemory\n52428800\n"},
			// CONFIG SET maxmemory 0 → OK.
			{stdout: "+OK\n"},
			// CONFIG SET maxmemory-policy noeviction → OK.
			{stdout: "+OK\n"},
			// CONFIG REWRITE → OK.
			{stdout: "+OK\n"},
		},
	}
	b := &K8sBackend{cs: cs, execor: fe}

	result, err := b.regradeViaExec(context.Background(), ns, token, -1)
	if err != nil {
		t.Fatalf("regradeViaExec returned error: %v", err)
	}
	if !result.Applied {
		t.Fatalf("Applied should be true for team tier update; SkipReason=%q", result.SkipReason)
	}
	if result.AppliedMaxmemory != 0 {
		t.Errorf("team tier AppliedMaxmemory = %d; want 0 (unlimited)", result.AppliedMaxmemory)
	}

	// Verify the CONFIG SET maxmemory command uses 0 (not -1 or any other value).
	if len(fe.calls) < 2 {
		t.Fatalf("expected at least 2 exec calls, got %d", len(fe.calls))
	}
	setMemStr := strings.Join(fe.calls[1].cmd, " ")
	if !strings.Contains(setMemStr, "CONFIG SET maxmemory 0") {
		t.Errorf("team tier CONFIG SET maxmemory command should contain '0' (unlimited); got: %v", fe.calls[1].cmd)
	}
	// Verify noeviction policy.
	if len(fe.calls) >= 3 {
		setPolicyStr := strings.Join(fe.calls[2].cmd, " ")
		if !strings.Contains(setPolicyStr, "noeviction") {
			t.Errorf("team tier CONFIG SET maxmemory-policy should be noeviction; got: %v", fe.calls[2].cmd)
		}
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

// runningPod creates a minimal Running pod for use in fake.Clientset.
func runningPod(namespace, name, containerName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"app": "redis"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: containerName, Image: "redis:7-alpine"}},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// notFoundErr returns a k8s NotFound error for use in fakeExecor responses.
// Retained for completeness; use fake.NewClientset without the object instead.
func notFoundErr(resource, name string) error {
	return k8serrors.NewNotFound(schema.GroupResource{Resource: resource}, name)
}

// Ensure notFoundErr is used (avoids "declared and not used" compile error).
var _ = notFoundErr

// Ensure the corev1/runtime imports stay referenced.
var _ runtime.Object = (*corev1.Pod)(nil)
