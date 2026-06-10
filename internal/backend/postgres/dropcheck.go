package postgres

// dropcheck.go — the postgres-package seam for the dropguard name-convention
// guard (truehomie hardening, task D3). Every DROP DATABASE / DROP USER this
// package executes validates its FINAL constructed identifiers here first, so
// a bug constructing a wrong target (empty token, system database, admin role)
// is refused before the destructive statement runs.

import (
	"log/slog"

	"instant.dev/provisioner/internal/dropguard"
)

// validateDropTargets refuses a (dbName, username) pair that does not match the
// per-tenant naming convention. On refusal it emits the structured
// `provisioner.drop.refused` audit event (same event name as the server
// chokepoint, so one NR/grep surface covers every refusal site) and returns the
// dropguard error for the caller to wrap. site names the calling function.
func validateDropTargets(site, token, dbName, username string) error {
	err := dropguard.CheckDatabaseName(dbName)
	if err == nil {
		err = dropguard.CheckUserName(username)
	}
	if err != nil {
		slog.Error("provisioner.drop.refused",
			"event", "provisioner.drop.refused", "site", site,
			"token", token, "db", dbName, "user", username, "error", err)
	}
	return err
}
