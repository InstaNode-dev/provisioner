package main

import (
	"testing"
	"time"
)

// TestEnvInt32_FallsBackOnBadValues — guard against a typo'd env var
// silently disabling the pgxpool ceiling.
func TestEnvInt32_FallsBackOnBadValues(t *testing.T) {
	cases := []struct {
		raw  string
		want int32
	}{
		{"", 25},
		{"not-a-number", 25},
		{"-1", 25}, // negative → fallback
		{"0", 25},  // zero → fallback
		{"15", 15},
	}
	for _, tc := range cases {
		t.Setenv("__TEST_PROV_PG_ENVINT32", tc.raw)
		got := envInt32("__TEST_PROV_PG_ENVINT32", 25)
		if got != tc.want {
			t.Errorf("envInt32(%q): want %d, got %d", tc.raw, tc.want, got)
		}
	}
}

// TestEnvDuration_FallsBackOnBadValues — guard against a typo'd env
// var silently disabling the connection-lifetime knob.
func TestEnvDuration_FallsBackOnBadValues(t *testing.T) {
	cases := []struct {
		raw  string
		want time.Duration
	}{
		{"", 7 * time.Minute},
		{"not-a-duration", 7 * time.Minute},
		{"-1s", 7 * time.Minute},
		{"0", 7 * time.Minute},
		{"5m", 5 * time.Minute},
		{"30s", 30 * time.Second},
	}
	for _, tc := range cases {
		t.Setenv("__TEST_PROV_PG_ENVDURATION", tc.raw)
		got := envDuration("__TEST_PROV_PG_ENVDURATION", 7*time.Minute)
		if got != tc.want {
			t.Errorf("envDuration(%q): want %v, got %v", tc.raw, tc.want, got)
		}
	}
}

// TestNewBoundedPgxPoolConfig_AppliesDefaults — asserts the bounded
// config applies the documented defaults when no env vars are set.
// Regression contract for the Wave-3 chaos verify (2026-05-21) finding:
// provisioner default was pgxpool.New (MaxConns = max(4, runtime.NumCPU()))
// — a high-CPU node could grab > 50 connections under load, multiplying
// the same DO Managed Postgres exhaustion that took out worker.
func TestNewBoundedPgxPoolConfig_AppliesDefaults(t *testing.T) {
	dsn := "postgres://nobody@127.0.0.1:1/postgres?sslmode=disable"
	cfg, err := newBoundedPgxPoolConfig(dsn)
	if err != nil {
		t.Fatalf("newBoundedPgxPoolConfig: %v", err)
	}
	if cfg.MaxConns != defaultProvisionerPGMaxConns {
		t.Errorf("MaxConns: want %d, got %d", defaultProvisionerPGMaxConns, cfg.MaxConns)
	}
	if cfg.MinConns != defaultProvisionerPGMinConns {
		t.Errorf("MinConns: want %d, got %d", defaultProvisionerPGMinConns, cfg.MinConns)
	}
	if cfg.MaxConnLifetime != defaultProvisionerPGConnMaxLife {
		t.Errorf("MaxConnLifetime: want %v, got %v", defaultProvisionerPGConnMaxLife, cfg.MaxConnLifetime)
	}
	if cfg.MaxConnIdleTime != defaultProvisionerPGConnMaxIdle {
		t.Errorf("MaxConnIdleTime: want %v, got %v", defaultProvisionerPGConnMaxIdle, cfg.MaxConnIdleTime)
	}
}

// TestNewBoundedPgxPoolConfig_RespectsEnv — asserts env vars override
// the defaults so operators can raise the ceiling without redeploying.
func TestNewBoundedPgxPoolConfig_RespectsEnv(t *testing.T) {
	t.Setenv("PROVISIONER_PG_MAX_CONNS", "30")
	t.Setenv("PROVISIONER_PG_MIN_CONNS", "5")
	t.Setenv("PROVISIONER_PG_CONN_MAX_LIFETIME", "2m")
	t.Setenv("PROVISIONER_PG_CONN_MAX_IDLE_TIME", "45s")

	dsn := "postgres://nobody@127.0.0.1:1/postgres?sslmode=disable"
	cfg, err := newBoundedPgxPoolConfig(dsn)
	if err != nil {
		t.Fatalf("newBoundedPgxPoolConfig: %v", err)
	}
	if cfg.MaxConns != 30 {
		t.Errorf("MaxConns: want 30, got %d", cfg.MaxConns)
	}
	if cfg.MinConns != 5 {
		t.Errorf("MinConns: want 5, got %d", cfg.MinConns)
	}
	if cfg.MaxConnLifetime != 2*time.Minute {
		t.Errorf("MaxConnLifetime: want 2m, got %v", cfg.MaxConnLifetime)
	}
	if cfg.MaxConnIdleTime != 45*time.Second {
		t.Errorf("MaxConnIdleTime: want 45s, got %v", cfg.MaxConnIdleTime)
	}
}
