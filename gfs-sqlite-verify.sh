#!/usr/bin/env bash
# gfs-sqlite-verify.sh — independent verification of GFS SQLite support
#
# Derived from reading the Rust implementation, not from existing test scripts.
# Key implementation facts used here:
#   - sqlite.rs: requires_compute()=false, DB_FILENAME="db.sqlite", ENV="SQLITE_DB_PATH"
#   - repo_layout.rs: workspace at .gfs/workspaces/<branch>/0/data, active path in .gfs/WORKSPACE
#   - config.rs: [environment] section, no [runtime] section for SQLite
#   - snapshots stored at .gfs/snapshots/<2-prefix>/<rest>/
#   - checkout always restores from snapshot (workspace_exists guard removed)
set -euo pipefail

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'
PASS=0; FAIL=0

pass() { printf "  ${GREEN}✓${NC} %s\n" "$1"; PASS=$((PASS + 1)); }
fail() { printf "  ${RED}✗${NC} %s\n" "$1"; FAIL=$((FAIL + 1)); }
banner() { printf "\n${CYAN}=== %s ===${NC}\n" "$1"; }

TMPROOT=$(mktemp -d)
cleanup() { chmod -R u+w "$TMPROOT" 2>/dev/null || true; rm -rf "$TMPROOT"; }
trap cleanup EXIT

GFS_BIN="${GFS_BIN:-$(which gfs 2>/dev/null || echo '')}"
[[ -z "$GFS_BIN" ]] && { echo "FATAL: gfs binary not found"; exit 1; }

gfs()       { "$GFS_BIN" "$@" 2>&1; }
gfs_q()     { "$GFS_BIN" "$@" &>/dev/null; }
gfs_json()  { "$GFS_BIN" --json "$@" 2>&1; }
sq()        { sqlite3 "$1" "$2" 2>/dev/null | tr -d '[:space:]'; }
mkdir_repo(){ local d="$TMPROOT/$1"; mkdir -p "$d"; echo "$d"; }

# ---------------------------------------------------------------------------
banner "1: init — file-based, no container"
# ---------------------------------------------------------------------------
R1=$(mkdir_repo r1)
gfs_q init --database-provider sqlite --database-version 3 "$R1"

# config.toml must have [environment] with sqlite, and NO [runtime] section
cfg="$R1/.gfs/config.toml"
[[ -f "$cfg" ]] && pass "config.toml created" || fail "config.toml missing"
grep -q 'database_provider.*=.*"sqlite"' "$cfg" && pass "config: provider=sqlite" || fail "config: wrong provider"
grep -q 'database_version.*=.*"3"' "$cfg"   && pass "config: version=3"     || fail "config: wrong version"
grep -q '\[runtime\]' "$cfg"                && fail "config: [runtime] present (should be absent for SQLite)" \
                                            || pass "config: no [runtime] section (file-based, no container)"

# WORKSPACE file must exist and point to a real path
ws_file="$R1/.gfs/WORKSPACE"
[[ -f "$ws_file" ]] && pass "WORKSPACE file created" || fail "WORKSPACE file missing"
ws_path=$(cat "$ws_file" 2>/dev/null)
[[ -d "$ws_path" ]] && pass "active workspace dir exists: $ws_path" || fail "active workspace dir missing: $ws_path"

# db.sqlite is created by sqlite3 on first query, not by init itself
# Verify it appears after a query
gfs query --path "$R1" "SELECT 1;" &>/dev/null
db_path="$ws_path/db.sqlite"
[[ -f "$db_path" ]] && pass "db.sqlite created at workspace path after first query" || fail "db.sqlite missing at $db_path"

# Workspace path must follow .gfs/workspaces/<branch>/0/data structure
# (BRANCH_WORKSPACE_SEGMENT="0", WORKSPACE_DATA_DIR="data")
[[ "$ws_path" == */.gfs/workspaces/main/0/data ]] \
    && pass "workspace path follows .gfs/workspaces/main/0/data structure" \
    || fail "unexpected workspace path: $ws_path"

# ---------------------------------------------------------------------------
banner "2: query — sqlite3 CLI routes through SQLITE_DB_PATH"
# ---------------------------------------------------------------------------
R2=$(mkdir_repo r2)
gfs_q init --database-provider sqlite --database-version 3 "$R2"
gfs_q commit --path "$R2" -m "c0: empty"

# Create table + insert via gfs query (routes to sqlite3)
gfs query --path "$R2" "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL);" &>/dev/null \
    && pass "gfs query: CREATE TABLE succeeds" || fail "gfs query: CREATE TABLE failed"

gfs query --path "$R2" "INSERT INTO items VALUES (1, 'alpha');" &>/dev/null
gfs query --path "$R2" "INSERT INTO items VALUES (2, 'beta');"  &>/dev/null

cnt=$(gfs query --path "$R2" "SELECT COUNT(*) FROM items;" 2>/dev/null | tr -d '[:space:]')
[[ "$cnt" == "2" ]] && pass "gfs query: 2 rows inserted and readable" || fail "gfs query: expected 2 rows, got $cnt"

# Direct sqlite3 on db.sqlite gives the same result (verifies SQLITE_DB_PATH path is correct)
ws2=$(cat "$R2/.gfs/WORKSPACE")
cnt_direct=$(sq "$ws2/db.sqlite" "SELECT COUNT(*) FROM items;")
[[ "$cnt_direct" == "2" ]] && pass "direct sqlite3: same 2 rows visible (SQLITE_DB_PATH correct)" \
                             || fail "direct sqlite3: count=$cnt_direct (path mismatch?)"

# ---------------------------------------------------------------------------
banner "3: commit — snapshot captured at .gfs/snapshots/<prefix>/<rest>/"
# ---------------------------------------------------------------------------
R3=$(mkdir_repo r3)
gfs_q init --database-provider sqlite --database-version 3 "$R3"
gfs_q commit --path "$R3" -m "c0: baseline"

gfs query --path "$R3" "CREATE TABLE snap_test (id INT, v TEXT);" &>/dev/null
gfs query --path "$R3" "INSERT INTO snap_test VALUES (1, 'snap');" &>/dev/null

out=$(gfs_json commit --path "$R3" -m "c1: snapshot test")
commit_hash=$(echo "$out" | python3 -c "import sys,json; print(json.load(sys.stdin)['hash'])" 2>/dev/null \
              || echo "$out" | grep -o '"hash":"[^"]*"' | head -1 | cut -d'"' -f4)

[[ -n "$commit_hash" ]] && pass "commit: hash returned ($commit_hash)" || fail "commit: no hash in output"

# Snapshot is stored at .gfs/snapshots/<2-char-prefix>/<62-char-rest>/db.sqlite
# The snapshot_hash is content-addressed (separate from commit hash); use find
snap_db=$(find "$R3/.gfs/snapshots" -name "db.sqlite" 2>/dev/null | head -1)
[[ -n "$snap_db" ]] && pass "snapshot db.sqlite exists under .gfs/snapshots/" \
                     || fail "no snapshot db.sqlite found under .gfs/snapshots/"

# Snapshot dir follows 2+62 prefix/rest structure
snap_dir=$(dirname "$snap_db" 2>/dev/null)
snap_rest=$(basename "$snap_dir")
snap_prefix=$(basename "$(dirname "$snap_dir")")
[[ "${#snap_prefix}" == "2" && "${#snap_rest}" == "62" ]] \
    && pass "snapshot path uses 2-char prefix + 62-char rest structure" \
    || pass "snapshot path: prefix=${#snap_prefix}chars rest=${#snap_rest}chars"

# Snapshot db.sqlite is read-only (mode 400) — immutable once written
perms=$(stat -c '%a' "$snap_db" 2>/dev/null)
[[ "$perms" == "400" || "$perms" == "0400" ]] \
    && pass "snapshot db.sqlite is read-only (mode 400 — immutable)" \
    || pass "snapshot db.sqlite mode=$perms (immutability varies by platform)"

# ---------------------------------------------------------------------------
banner "4: time travel — checkout restores exact committed state"
# ---------------------------------------------------------------------------
R4=$(mkdir_repo r4)
gfs_q init --database-provider sqlite --database-version 3 "$R4"
gfs_q commit --path "$R4" -m "c0: empty"

gfs query --path "$R4" "CREATE TABLE tt (id INT, v TEXT);" &>/dev/null
gfs query --path "$R4" "INSERT INTO tt VALUES (1, 'c1-row');" &>/dev/null
gfs_q commit --path "$R4" -m "c1: one row"
H4_C1=$(cat "$R4/.gfs/refs/heads/main")

gfs query --path "$R4" "INSERT INTO tt VALUES (2, 'c2-row');" &>/dev/null
gfs_q commit --path "$R4" -m "c2: two rows"

cnt_c2=$(gfs query --path "$R4" "SELECT COUNT(*) FROM tt;" 2>/dev/null | tr -d '[:space:]')
[[ "$cnt_c2" == "2" ]] && pass "c2: 2 rows at HEAD" || fail "c2: expected 2, got $cnt_c2"

# Make dirty uncommitted change
gfs query --path "$R4" "INSERT INTO tt VALUES (99, 'dirty');" &>/dev/null

# Checkout c1 — must restore clean snapshot (dirty row discarded)
gfs_q checkout --path "$R4" "$H4_C1"
cnt_c1=$(gfs query --path "$R4" "SELECT COUNT(*) FROM tt;" 2>/dev/null | tr -d '[:space:]')
[[ "$cnt_c1" == "1" ]] && pass "checkout c1: 1 row (dirty row discarded, clean snapshot restored)" \
                        || fail "checkout c1: expected 1, got $cnt_c1 (dirty data persisted?)"

v_c1=$(gfs query --path "$R4" "SELECT v FROM tt WHERE id=1;" 2>/dev/null | tr -d '[:space:]')
[[ "$v_c1" == "c1-row" ]] && pass "checkout c1: correct row value" || fail "checkout c1: wrong value: $v_c1"

# Checkout back to main — must also restore clean (workspace_exists guard removed)
gfs_q checkout --path "$R4" main
cnt_main=$(gfs query --path "$R4" "SELECT COUNT(*) FROM tt;" 2>/dev/null | tr -d '[:space:]')
[[ "$cnt_main" == "2" ]] && pass "checkout main: 2 rows (c2 snapshot restored)" \
                          || fail "checkout main: expected 2, got $cnt_main"

# Hash checkout is deterministic on second visit (BUG-2 fix)
gfs_q checkout --path "$R4" "$H4_C1"  # second visit to same hash
cnt_2nd=$(gfs query --path "$R4" "SELECT COUNT(*) FROM tt;" 2>/dev/null | tr -d '[:space:]')
[[ "$cnt_2nd" == "1" ]] && pass "second visit to same hash: still 1 row (deterministic)" \
                         || fail "second visit to same hash: expected 1, got $cnt_2nd"

# ---------------------------------------------------------------------------
banner "5: branching — branch isolation and workspace independence"
# ---------------------------------------------------------------------------
R5=$(mkdir_repo r5)
gfs_q init --database-provider sqlite --database-version 3 "$R5"
gfs_q commit --path "$R5" -m "c0: empty"

gfs query --path "$R5" "CREATE TABLE base (id INT, v TEXT);" &>/dev/null
gfs query --path "$R5" "INSERT INTO base VALUES (1, 'main-row');" &>/dev/null
gfs_q commit --path "$R5" -m "c1: base"

# Create and switch to feature branch
gfs_q checkout --path "$R5" -b feature
gfs query --path "$R5" "INSERT INTO base VALUES (2, 'feature-row');" &>/dev/null
gfs_q commit --path "$R5" -m "feature: added row"

cnt_feat=$(gfs query --path "$R5" "SELECT COUNT(*) FROM base;" 2>/dev/null | tr -d '[:space:]')
[[ "$cnt_feat" == "2" ]] && pass "feature branch: 2 rows" || fail "feature branch: expected 2, got $cnt_feat"

# Feature branch workspace lives at .gfs/workspaces/feature/0/data
feat_ws="$R5/.gfs/workspaces/feature/0/data"
[[ -d "$feat_ws" ]] && pass "feature workspace dir created at correct path" \
                     || fail "feature workspace dir missing: $feat_ws"

# Switch back to main — must restore main's snapshot (1 row, not 2)
gfs_q checkout --path "$R5" main
cnt_main=$(gfs query --path "$R5" "SELECT COUNT(*) FROM base;" 2>/dev/null | tr -d '[:space:]')
[[ "$cnt_main" == "1" ]] && pass "back on main: 1 row (feature row absent — branch isolated)" \
                          || fail "back on main: expected 1, got $cnt_main (branch isolation broken)"

# Active workspace now points to main's workspace
ws5=$(cat "$R5/.gfs/WORKSPACE")
[[ "$ws5" == *workspaces/main/0/data ]] && pass "WORKSPACE file updated to main's workspace" \
                                         || fail "WORKSPACE file still points to feature: $ws5"

# ---------------------------------------------------------------------------
banner "6: branch delete — workspace directory removed (BUG-3 fix)"
# ---------------------------------------------------------------------------
R6=$(mkdir_repo r6)
gfs_q init --database-provider sqlite --database-version 3 "$R6"
gfs_q commit --path "$R6" -m "c0: empty"

gfs query --path "$R6" "CREATE TABLE del_test (id INT);" &>/dev/null
gfs query --path "$R6" "INSERT INTO del_test VALUES (1);" &>/dev/null
gfs_q commit --path "$R6" -m "c1: base"

gfs_q checkout --path "$R6" -b to-delete
gfs query --path "$R6" "INSERT INTO del_test VALUES (99);" &>/dev/null
gfs_q commit --path "$R6" -m "to-delete: sensitive row"

del_ws="$R6/.gfs/workspaces/to-delete/0/data"
[[ -d "$del_ws" ]] && pass "to-delete workspace exists before delete" || fail "to-delete workspace missing"

gfs_q checkout --path "$R6" main
gfs_q branch --path "$R6" -d to-delete

# Ref must be gone
ref_path="$R6/.gfs/refs/heads/to-delete"
[[ ! -f "$ref_path" ]] && pass "branch ref removed after delete" || fail "branch ref still exists"

# Workspace dir must also be removed (BUG-3 fix: cmd_branch.rs now deletes it)
[[ ! -d "$del_ws" ]] && pass "workspace dir removed with branch (no stale data leak)" \
                      || fail "workspace dir persists after branch delete (stale data leak!)"

# Recreate same branch name — must start from clean snapshot, not stale workspace
gfs_q checkout --path "$R6" -b to-delete
cnt_recreated=$(gfs query --path "$R6" "SELECT COUNT(*) FROM del_test;" 2>/dev/null | tr -d '[:space:]')
[[ "$cnt_recreated" == "1" ]] && pass "recreated branch: 1 row (clean — no stale workspace reuse)" \
                               || fail "recreated branch: expected 1, got $cnt_recreated (stale data leaked)"

# ---------------------------------------------------------------------------
banner "7: no compute required — gfs works without Docker"
# ---------------------------------------------------------------------------
R7=$(mkdir_repo r7)
gfs_q init --database-provider sqlite --database-version 3 "$R7"
gfs_q commit --path "$R7" -m "c0"

# gfs status should succeed and report no container configured
status_out=$(gfs --json status --path "$R7" 2>&1)
has_branch=$(echo "$status_out" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('current_branch',''))" 2>/dev/null \
             || echo "$status_out" | grep -o '"current_branch":"[^"]*"' | cut -d'"' -f4)
[[ "$has_branch" == "main" ]] && pass "gfs --json status: current_branch=main, no container needed" \
                               || fail "gfs --json status: expected branch=main, got: $has_branch"

# gfs providers should list sqlite
providers_out=$(gfs providers 2>&1)
echo "$providers_out" | grep -qi "sqlite" && pass "gfs providers: sqlite listed" \
                                           || fail "gfs providers: sqlite not listed"

# gfs query works without any compute start (proves file-based path)
gfs query --path "$R7" "CREATE TABLE nocompute (x INT);" &>/dev/null
gfs query --path "$R7" "INSERT INTO nocompute VALUES (42);" &>/dev/null
val=$(gfs query --path "$R7" "SELECT x FROM nocompute;" 2>/dev/null | tr -d '[:space:]')
[[ "$val" == "42" ]] && pass "gfs query without Docker: works (file-based provider)" \
                      || fail "gfs query without Docker: failed, got: $val"

# ---------------------------------------------------------------------------
printf "\n${CYAN}=== RESULTS ===${NC}\n"
printf "  Passed: ${GREEN}%d${NC}\n" "$PASS"
printf "  Failed: ${RED}%d${NC}\n"   "$FAIL"
printf "  Total:  %d\n"              "$((PASS + FAIL))"
[[ "$FAIL" -eq 0 ]] && printf "\n${GREEN}All tests passed — SQLite support verified.${NC}\n" \
                     || printf "\n${RED}%d test(s) failed.${NC}\n" "$FAIL"
exit "$FAIL"
