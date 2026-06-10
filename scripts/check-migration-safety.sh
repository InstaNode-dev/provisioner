#!/usr/bin/env bash
# check-migration-safety.sh — CI gate that flags DESTRUCTIVE DDL in migration
# files (truehomie-db DROP incident hardening, task D3, 2026-06-11).
#
# WHAT IT CATCHES
#   DROP TABLE / DROP DATABASE / DROP SCHEMA / DROP COLUMN / DROP ROLE /
#   DROP USER / TRUNCATE, and DELETE FROM statements with no WHERE clause —
#   in any *.sql file under a migrations directory.
#
# HOW TO SHIP A LEGITIMATE DESTRUCTIVE MIGRATION
#   Add an explicit acknowledgement marker comment ANYWHERE in the file:
#       -- destructive: acknowledged <reason>
#   e.g. -- destructive: acknowledged dropping legacy column replaced by mig 063
#   The marker forces the destruction to be a reviewed, deliberate act — the
#   reviewer sees the reason next to the DDL — instead of a silent drift.
#
# PORTABILITY
#   Self-contained bash + grep + awk; no repo-specific assumptions. To adopt in
#   another repo, copy this file and add the two CI steps:
#       bash scripts/check-migration-safety.sh --self-test
#       bash scripts/check-migration-safety.sh
#   Override the scan root with MIGRATION_SAFETY_ROOT (default: repo root).
#
# EXIT CODES: 0 = clean (or all destructive files acknowledged); 1 = violation;
#             2 = self-test failure.

set -euo pipefail

MARKER_REGEX='--[[:space:]]*destructive:[[:space:]]*acknowledged[[:space:]]+[^[:space:]]'

# scan_file <file> — prints a violation line per un-acknowledged destructive
# statement; returns 0 when the file is safe.
scan_file() {
  local f="$1"
  local violations=""

  # Acknowledged files pass wholesale: the marker is the reviewed waiver.
  if grep -qiE -e "$MARKER_REGEX" "$f"; then
    return 0
  fi

  # Plain destructive DDL patterns (case-insensitive, line-based; SQL comments
  # starting with -- are stripped first so prose about DROP doesn't trip it).
  local stripped
  stripped="$(sed 's/--.*$//' "$f")"

  local pat
  for pat in 'DROP[[:space:]]+TABLE' 'DROP[[:space:]]+DATABASE' 'DROP[[:space:]]+SCHEMA' \
             'DROP[[:space:]]+COLUMN' 'DROP[[:space:]]+ROLE' 'DROP[[:space:]]+USER' \
             'TRUNCATE'; do
    if printf '%s' "$stripped" | grep -qiE "$pat"; then
      violations="${violations}  - $(printf '%s' "$pat" | sed 's/\[\[:space:\]\]+/ /g')\n"
    fi
  done

  # DELETE FROM without a WHERE in the same statement (statements split on ';').
  # awk joins the comment-stripped file into one line, splits on ';', and flags
  # any statement that contains DELETE FROM but no WHERE.
  if printf '%s' "$stripped" | awk '
      { gsub(/\r/, ""); buf = buf " " $0 }
      END {
        n = split(buf, stmts, ";")
        for (i = 1; i <= n; i++) {
          s = toupper(stmts[i])
          if (s ~ /DELETE[ \t]+FROM/ && s !~ /WHERE/) { exit 0 }
        }
        exit 1
      }'; then
    violations="${violations}  - DELETE FROM without WHERE\n"
  fi

  if [ -n "$violations" ]; then
    printf 'VIOLATION %s\n%b' "$f" "$violations"
    return 1
  fi
  return 0
}

run_scan() {
  local root="${MIGRATION_SAFETY_ROOT:-.}"
  local failed=0 scanned=0 f

  # Every *.sql under any directory whose path contains "migrations".
  while IFS= read -r f; do
    scanned=$((scanned + 1))
    if ! scan_file "$f"; then
      failed=1
    fi
  done < <(find "$root" -type f -name '*.sql' -path '*migrations*' \
             -not -path '*/vendor/*' -not -path '*/node_modules/*' -not -path '*/.git/*' | sort)

  if [ "$failed" -ne 0 ]; then
    echo ""
    echo "Migration-safety gate FAILED: destructive DDL found without an acknowledgement marker."
    echo "If the destruction is intentional and reviewed, add to the migration file:"
    echo "    -- destructive: acknowledged <reason>"
    echo "(truehomie-db DROP incident hardening — see provisioner/internal/dropguard)"
    return 1
  fi
  echo "migration-safety: OK ($scanned migration file(s) scanned, 0 unacknowledged destructive statements)"
  return 0
}

self_test() {
  local tmp
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' RETURN
  mkdir -p "$tmp/migrations"

  local rc=0

  # 1. Destructive without marker → must FAIL.
  cat > "$tmp/migrations/001_bad.sql" <<'SQL'
ALTER TABLE foo ADD COLUMN bar TEXT;
DROP TABLE legacy_widgets;
SQL
  if MIGRATION_SAFETY_ROOT="$tmp" run_scan >/dev/null 2>&1; then
    echo "self-test FAIL: unacknowledged DROP TABLE was not flagged"; rc=2
  fi

  # 2. Destructive WITH marker → must PASS.
  cat > "$tmp/migrations/001_bad.sql" <<'SQL'
-- destructive: acknowledged legacy_widgets replaced by widgets_v2 in mig 000
DROP TABLE legacy_widgets;
SQL
  if ! MIGRATION_SAFETY_ROOT="$tmp" run_scan >/dev/null 2>&1; then
    echo "self-test FAIL: acknowledged DROP TABLE was flagged"; rc=2
  fi

  # 3. DELETE FROM without WHERE → must FAIL.
  cat > "$tmp/migrations/001_bad.sql" <<'SQL'
DELETE FROM sessions;
SQL
  if MIGRATION_SAFETY_ROOT="$tmp" run_scan >/dev/null 2>&1; then
    echo "self-test FAIL: DELETE FROM without WHERE was not flagged"; rc=2
  fi

  # 4. DELETE FROM with WHERE → must PASS.
  cat > "$tmp/migrations/001_bad.sql" <<'SQL'
DELETE FROM sessions WHERE expires_at < now();
SQL
  if ! MIGRATION_SAFETY_ROOT="$tmp" run_scan >/dev/null 2>&1; then
    echo "self-test FAIL: DELETE FROM with WHERE was flagged"; rc=2
  fi

  # 5. Non-destructive migration → must PASS (incl. prose comments about DROP).
  cat > "$tmp/migrations/001_bad.sql" <<'SQL'
-- this migration does NOT drop table anything; see DROP DATABASE docs.
CREATE TABLE widgets (id INT);
ALTER TABLE widgets ADD COLUMN name TEXT;
SQL
  if ! MIGRATION_SAFETY_ROOT="$tmp" run_scan >/dev/null 2>&1; then
    echo "self-test FAIL: non-destructive migration was flagged"; rc=2
  fi

  # 6. TRUNCATE without marker → must FAIL.
  cat > "$tmp/migrations/001_bad.sql" <<'SQL'
TRUNCATE sessions;
SQL
  if MIGRATION_SAFETY_ROOT="$tmp" run_scan >/dev/null 2>&1; then
    echo "self-test FAIL: TRUNCATE was not flagged"; rc=2
  fi

  if [ "$rc" -eq 0 ]; then
    echo "migration-safety: self-test OK (6/6 fixtures behaved)"
  fi
  return "$rc"
}

case "${1:-}" in
  --self-test) self_test ;;
  *) run_scan ;;
esac
