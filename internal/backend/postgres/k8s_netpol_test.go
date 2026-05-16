package postgres

// k8s_netpol_test.go — security regression tests for the namespace-labeling fix.
//
// Pentest 2026-05-16 finding: customer-resource namespaces were not labelled with
// instant.dev/owner-team, so the deploy-side NetworkPolicy DB-egress selector
// (which used only instant.dev/role=customer-resource) selected ALL customer
// namespaces — not just the requesting team's.
//
// Fix: applyNamespace reads ctxkeys.TeamIDKey from the context and, when non-empty,
// applies instant.dev/owner-team=<teamID> to the namespace.  The tests below
// guard this invariant.

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"instant.dev/provisioner/internal/ctxkeys"
)

// ctxWithTeamID returns a context carrying the given teamID under ctxkeys.TeamIDKey.
func ctxWithTeamID(teamID string) context.Context {
	return context.WithValue(context.Background(), ctxkeys.TeamIDKey, teamID)
}

// TestApplyNamespace_CarriesOwnerTeamLabel verifies that when the context carries
// a team ID, the created namespace is labelled with instant.dev/owner-team=<teamID>.
//
// This is the primary provisioner regression guard for the pentest 2026-05-16 fix.
// If this test fails, the deploy-side NetworkPolicy would fall back to role-only
// selection, enabling cross-tenant DB access.
func TestApplyNamespace_CarriesOwnerTeamLabel(t *testing.T) {
	const teamID = "aaaaaaaa-1111-2222-3333-444444444444"
	const ns = "instant-customer-tok-teamlabel"

	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}

	ctx := ctxWithTeamID(teamID)
	if err := b.applyNamespace(ctx, ns); err != nil {
		t.Fatalf("applyNamespace: %v", err)
	}

	got, err := cs.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get namespace: %v", err)
	}

	// Verify role label (unchanged).
	if got.Labels[k8sRoleLabel] != k8sRoleValue {
		t.Errorf("namespace label %s = %q; want %q", k8sRoleLabel, got.Labels[k8sRoleLabel], k8sRoleValue)
	}

	// Core assertion: owner-team label must be present.
	gotOwner, hasOwner := got.Labels[k8sOwnerTeamLabel]
	if !hasOwner {
		t.Errorf("namespace is missing label %s — cross-tenant NetworkPolicy scoping will be broken; labels=%v",
			k8sOwnerTeamLabel, got.Labels)
	} else if gotOwner != teamID {
		t.Errorf("namespace label %s = %q; want %q", k8sOwnerTeamLabel, gotOwner, teamID)
	}
}

// TestApplyNamespace_NoOwnerTeamLabel_WhenContextEmpty verifies that when the
// context does NOT carry a team ID (anonymous / missing), the namespace is NOT
// labelled with instant.dev/owner-team. This is the acceptable anonymous fallback.
func TestApplyNamespace_NoOwnerTeamLabel_WhenContextEmpty(t *testing.T) {
	const ns = "instant-customer-tok-anon"

	cs := fake.NewSimpleClientset()
	b := &K8sBackend{cs: cs}

	// No team ID in context.
	if err := b.applyNamespace(context.Background(), ns); err != nil {
		t.Fatalf("applyNamespace: %v", err)
	}

	got, err := cs.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get namespace: %v", err)
	}

	if v, has := got.Labels[k8sOwnerTeamLabel]; has {
		t.Errorf("anonymous namespace unexpectedly has label %s=%q; should be absent when no team ID in ctx",
			k8sOwnerTeamLabel, v)
	}
}

// TestApplyNamespace_OwnerTeamLabel_TableDriven is a table-driven extension of the
// above tests, covering multiple team IDs to confirm the label is set correctly for
// each.
func TestApplyNamespace_OwnerTeamLabel_TableDriven(t *testing.T) {
	cases := []struct {
		name          string
		teamID        string
		wantLabel     bool
		wantLabelValue string
	}{
		{
			name:           "team_A",
			teamID:         "team-A-uuid-0001",
			wantLabel:      true,
			wantLabelValue: "team-A-uuid-0001",
		},
		{
			name:           "team_B",
			teamID:         "team-B-uuid-0002",
			wantLabel:      true,
			wantLabelValue: "team-B-uuid-0002",
		},
		{
			name:      "anonymous_empty_string",
			teamID:    "",
			wantLabel: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cs := fake.NewSimpleClientset()
			b := &K8sBackend{cs: cs}

			ns := "instant-customer-tok-" + tc.name

			var ctx context.Context
			if tc.teamID != "" {
				ctx = ctxWithTeamID(tc.teamID)
			} else {
				ctx = context.Background()
			}

			if err := b.applyNamespace(ctx, ns); err != nil {
				t.Fatalf("applyNamespace: %v", err)
			}

			got, err := cs.CoreV1().Namespaces().Get(context.Background(), ns, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get namespace: %v", err)
			}

			gotOwner, hasOwner := got.Labels[k8sOwnerTeamLabel]
			if tc.wantLabel {
				if !hasOwner {
					t.Errorf("team=%q: namespace missing label %s; labels=%v", tc.teamID, k8sOwnerTeamLabel, got.Labels)
				} else if gotOwner != tc.wantLabelValue {
					t.Errorf("team=%q: label %s = %q; want %q", tc.teamID, k8sOwnerTeamLabel, gotOwner, tc.wantLabelValue)
				}
			} else {
				if hasOwner {
					t.Errorf("anon: namespace unexpectedly has label %s=%q", k8sOwnerTeamLabel, gotOwner)
				}
			}
		})
	}
}
