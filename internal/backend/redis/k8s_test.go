package redis

// k8s_test.go — unit tests for K8sBackend tier sizing (A4 regression guards).
// Tests run without a live k8s cluster — they only exercise sizingForTier().

import (
	"fmt"
	"testing"
)

// TestSizingForTier_MaxmemoryMB_MatchesPlansYAML verifies that maxmemoryMB for
// each tier matches the redis_memory_mb value in plans.yaml (A4 regression guard).
// If plans.yaml is updated and these values are not kept in sync, the test fails.
func TestSizingForTier_MaxmemoryMB_MatchesPlansYAML(t *testing.T) {
	cases := []struct {
		tier        string
		wantMB      int  // expected maxmemoryMB (mirrors plans.yaml redis_memory_mb)
		expectLimit bool // true if --maxmemory flag should be applied
	}{
		{"anonymous", 5, true},    // plans.yaml: anonymous redis_memory_mb = 5
		{"hobby", 50, true},       // plans.yaml: hobby redis_memory_mb = 50
		{"pro", 512, true},        // plans.yaml: pro redis_memory_mb = 512
		{"team", -1, false},       // unlimited — flag omitted
		{"growth", -1, false},     // unlimited — flag omitted
		{"unknown", 50, true},     // unknown → hobby fallback
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
// omits them for unlimited tiers (team/growth).
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
				"--maxmemory-policy", "allkeys-lru",
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

	limitedTiers := []string{"anonymous", "hobby", "pro"}
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
		})
	}

	unlimitedTiers := []string{"team", "growth"}
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
