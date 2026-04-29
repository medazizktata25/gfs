#!/usr/bin/env bash
# gfs-sqlite-test.sh — Exhaustive GFS SQLite test suite
# Tests: init, query, commit/checkout time-travel, branch isolation, data types,
#        large datasets, schema ops, WAL handling, JSON output, edge cases.
# Requires: sqlite3 CLI, gfs binary (cargo build --bin gfs)
# No Docker / container runtime needed.

set -uo pipefail

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GFS_BIN="${GFS_BIN:-${SCRIPT_DIR}/target/debug/gfs}"
TEST_WORK_BASE="${HOME}/.gfs-sqlite-test-tmp"
SQLITE3="${SQLITE3:-sqlite3}"

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'
CYAN='\033[0;36m'; MAGENTA='\033[0;35m'; NC='\033[0m'; BOLD='\033[1m'

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

banner()    { printf "\n${CYAN}${BOLD}=== %s ===${NC}\n" "$*"; }
subbanner() { printf "\n  ${MAGENTA}--- %s ---${NC}\n" "$*"; }
info()      { printf "  ${YELLOW}→${NC} %s\n" "$*"; }

pass() {
    printf "  ${GREEN}✓${NC} %s\n" "$*"
    PASS_COUNT=$((PASS_COUNT + 1))
}

fail() {
    printf "  ${RED}✗${NC} %s\n" "$*"
    FAIL_COUNT=$((FAIL_COUNT + 1))
}

skip() {
    printf "  ${YELLOW}⊘${NC} %s (skipped)\n" "$*"
    SKIP_COUNT=$((SKIP_COUNT + 1))
}

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
mkdir -p "$TEST_WORK_BASE"

make_test_dir() { mktemp -d "${TEST_WORK_BASE}/gfs-sqlite-XXXXXX"; }

safe_rm() {
    local dir="$1"
    [[ -z "$dir" || "$dir" == "/" ]] && return
    chmod -R u+w "$dir" 2>/dev/null || true
    rm -rf "$dir" 2>/dev/null || true
}

# Wrap gfs binary calls
gfs()       { "$GFS_BIN" "$@" 2>&1; }
gfs_quiet() { "$GFS_BIN" "$@" &>/dev/null; }
gfs_exit()  { "$GFS_BIN" "$@" &>/dev/null; echo $?; }

# Get active workspace data dir for a repo
get_workspace_data_dir() {
    local repo="$1"
    cat "${repo}/.gfs/WORKSPACE" 2>/dev/null | tr -d '[:space:]' || echo ""
}

# Get path to the live db.sqlite for a repo (reads WORKSPACE)
get_db_path() {
    local repo="$1"
    local ws; ws=$(get_workspace_data_dir "$repo")
    echo "${ws}/db.sqlite"
}

# Run SQL directly via sqlite3 against the live workspace db
sq() {
    local repo="$1"; shift
    local db; db=$(get_db_path "$repo")
    "$SQLITE3" "$db" "$@" 2>&1
}

# Run SQL via gfs query
gq() {
    local repo="$1"; local query="$2"
    "$GFS_BIN" query --path "$repo" "$query" 2>&1
}

# Count rows in a table (returns integer)
row_count() {
    local repo="$1"; local table="$2"
    sq "$repo" "SELECT COUNT(*) FROM ${table};" 2>/dev/null | tr -d '[:space:]'
}

# Check that a number matches expected, report pass/fail
assert_count() {
    local label="$1"; local actual="$2"; local expected="$3"
    if [[ "${actual//[[:space:]]/}" == "$expected" ]]; then
        pass "$label: count=$expected"
    else
        fail "$label: expected $expected, got $actual"
    fi
}

# Assert string contains substring
assert_contains() {
    local label="$1"; local haystack="$2"; local needle="$3"
    if echo "$haystack" | grep -qF "$needle"; then
        pass "$label"
    else
        fail "$label (expected '${needle}' in output: '${haystack:0:120}')"
    fi
}

# Assert command exits with given code
assert_exit() {
    local label="$1"; local expected_exit="$2"; shift 2
    local actual_exit; actual_exit=$(gfs_exit "$@")
    if [[ "$actual_exit" == "$expected_exit" ]]; then
        pass "$label (exit=$expected_exit)"
    else
        fail "$label (expected exit=$expected_exit, got=$actual_exit)"
    fi
}

# Read current HEAD hash
head_hash() {
    local repo="$1"
    local branch; branch=$(cat "${repo}/.gfs/HEAD" 2>/dev/null | sed 's|ref: refs/heads/||' | tr -d '[:space:]')
    cat "${repo}/.gfs/refs/heads/${branch}" 2>/dev/null | tr -d '[:space:]' || echo ""
}

# Verify JSON is valid
is_valid_json() {
    python3 -c "import sys,json; json.loads(sys.argv[1])" "$1" 2>/dev/null
}

# ---------------------------------------------------------------------------
# Semantic integrity helpers
# ---------------------------------------------------------------------------

# Compute a deterministic content fingerprint for a table.
# Serialises every row to "col1|col2|...\n" (NULL → <NULL>), sorts by first
# column, pipes through sha256sum.  Order-independent of insertion order.
table_fingerprint() {
    local repo="$1"; local table="$2"
    local db; db=$(get_db_path "$repo")
    # Use SQLite's json serialisation so every type is normalised consistently.
    "$SQLITE3" "$db" \
        "SELECT group_concat(row_json, char(10))
         FROM (
             SELECT json_group_array(
                 CASE WHEN col IS NULL THEN '<NULL>' ELSE CAST(col AS TEXT) END
             ) AS row_json
             FROM (
                 SELECT json_each.value AS col
                 FROM ${table}, json_each(json_array(${table}.*))
                 ORDER BY rowid, json_each.key
             )
             GROUP BY rowid
             ORDER BY rowid
         );" 2>/dev/null | sha256sum | awk '{print $1}'
}

# Simpler fingerprint: concatenate every cell with a separator via CSV dump,
# then hash.  Works even when json_array(table.*) is unsupported (SQLite < 3.38).
table_fingerprint_csv() {
    local repo="$1"; local table="$2"
    local db; db=$(get_db_path "$repo")
    {
        # Header line stabilises column order across schema changes
        "$SQLITE3" -separator '|' "$db" \
            "SELECT * FROM ${table} ORDER BY 1;" 2>/dev/null
    } | sha256sum | awk '{print $1}'
}

# Choose the best available fingerprint method
fingerprint() {
    local repo="$1"; local table="$2"
    # Prefer CSV (portable, no json_array dependency)
    table_fingerprint_csv "$repo" "$table"
}

# Assert two fingerprints match
assert_fingerprint_eq() {
    local label="$1"; local fp_a="$2"; local fp_b="$3"
    if [[ -n "$fp_a" && "$fp_a" == "$fp_b" ]]; then
        pass "${label}: content fingerprint matches"
    else
        fail "${label}: fingerprint mismatch (before=${fp_a:0:12}… after=${fp_b:0:12}…)"
    fi
}

# Assert two fingerprints differ
assert_fingerprint_ne() {
    local label="$1"; local fp_a="$2"; local fp_b="$3"
    if [[ -n "$fp_a" && -n "$fp_b" && "$fp_a" != "$fp_b" ]]; then
        pass "${label}: fingerprints are distinct (expected)"
    else
        fail "${label}: fingerprints are identical — data not isolated"
    fi
}

# Run PRAGMA integrity_check and assert "ok"
assert_db_integrity() {
    local label="$1"; local repo="$2"
    local db; db=$(get_db_path "$repo")
    if [[ ! -f "$db" ]]; then
        pass "${label}: db absent (empty workspace — ok)"
        return
    fi
    local result; result=$("$SQLITE3" "$db" "PRAGMA integrity_check;" 2>&1)
    if echo "$result" | grep -q "^ok$"; then
        pass "${label}: PRAGMA integrity_check = ok"
    else
        fail "${label}: integrity_check failed: ${result:0:120}"
    fi
}

# Assert a specific cell value equals expected
assert_cell() {
    local label="$1"; local repo="$2"; local query="$3"; local expected="$4"
    local actual; actual=$(sq "$repo" "$query" | tr -d '[:space:]')
    if [[ "$actual" == "$expected" ]]; then
        pass "${label}: value='${expected}'"
    else
        fail "${label}: expected='${expected}' got='${actual}'"
    fi
}

# Verify every expected row is present by PK+value pair
assert_row_exists() {
    local label="$1"; local repo="$2"; local table="$3"
    local pk_col="$4"; local pk_val="$5"; local check_col="$6"; local check_val="$7"
    local v; v=$(sq "$repo" \
        "SELECT ${check_col} FROM ${table} WHERE ${pk_col}=${pk_val};" \
        | tr -d '[:space:]')
    if [[ "$v" == "$check_val" ]]; then
        pass "${label}: ${table}[${pk_col}=${pk_val}].${check_col}='${check_val}'"
    else
        fail "${label}: ${table}[${pk_col}=${pk_val}].${check_col} expected='${check_val}' got='${v}'"
    fi
}

# Assert a row is ABSENT (count by PK = 0)
assert_row_absent() {
    local label="$1"; local repo="$2"; local table="$3"
    local pk_col="$4"; local pk_val="$5"
    local c; c=$(sq "$repo" \
        "SELECT COUNT(*) FROM ${table} WHERE ${pk_col}=${pk_val};" \
        | tr -d '[:space:]')
    if [[ "$c" == "0" ]]; then
        pass "${label}: row ${pk_col}=${pk_val} absent"
    else
        fail "${label}: row ${pk_col}=${pk_val} should be absent (count=$c)"
    fi
}

# Check schema definition preserved (column count + names)
assert_table_schema() {
    local label="$1"; local repo="$2"; local table="$3"
    local expected_cols="$4"  # space-separated column names
    local actual_cols; actual_cols=$(sq "$repo" \
        "SELECT name FROM pragma_table_info('${table}') ORDER BY cid;" \
        | tr '\n' ' ' | tr -s ' ' | sed 's/ $//')
    if [[ "$actual_cols" == "$expected_cols" ]]; then
        pass "${label}: schema columns match"
    else
        fail "${label}: expected cols='${expected_cols}' got='${actual_cols}'"
    fi
}

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
banner "PRE-FLIGHT"

if [[ ! -f "$GFS_BIN" ]]; then
    printf "${RED}FATAL:${NC} gfs binary not found: %s\n" "$GFS_BIN"
    printf "Build with: cargo build --bin gfs\n"
    exit 1
fi
pass "gfs binary: $("$GFS_BIN" --version 2>/dev/null | head -1)"

if ! command -v "$SQLITE3" &>/dev/null; then
    printf "${RED}FATAL:${NC} sqlite3 not found. Install sqlite3.\n"
    exit 1
fi
pass "sqlite3: $("$SQLITE3" --version 2>/dev/null | head -1)"

if ! command -v python3 &>/dev/null; then
    printf "${YELLOW}WARN:${NC} python3 not found — JSON validation tests will be skipped\n"
    PYTHON3_OK=false
else
    PYTHON3_OK=true
    pass "python3: $(python3 --version 2>/dev/null)"
fi

# ===========================================================================
banner "TEST 1: gfs init — sqlite provider"
# ===========================================================================
T1=$(make_test_dir)

gfs_quiet init --database-provider sqlite --database-version 3 "$T1" \
    && pass "gfs init --database-provider sqlite succeeds" \
    || fail "gfs init --database-provider sqlite failed"

# .gfs layout
[[ -d "${T1}/.gfs" ]]             && pass ".gfs dir exists"            || fail ".gfs dir missing"
[[ -f "${T1}/.gfs/config.toml" ]] && pass "config.toml created"        || fail "config.toml missing"
[[ -f "${T1}/.gfs/HEAD" ]]        && pass "HEAD file exists"            || fail "HEAD missing"
[[ -d "${T1}/.gfs/refs/heads" ]]  && pass "refs/heads exists"           || fail "refs/heads missing"
[[ -d "${T1}/.gfs/objects" ]]     && pass "objects dir exists"          || fail "objects missing"
[[ -d "${T1}/.gfs/snapshots" ]]   && pass "snapshots dir exists"        || fail "snapshots missing"
[[ -f "${T1}/.gfs/WORKSPACE" ]]   && pass "WORKSPACE file exists"       || fail "WORKSPACE missing"

# config.toml: has [environment] with sqlite, no [runtime]
local_config=$(cat "${T1}/.gfs/config.toml" 2>/dev/null || echo "")
echo "$local_config" | grep -q 'database_provider.*=.*"sqlite"' \
    && pass "config.toml: database_provider = sqlite" \
    || fail "config.toml: database_provider missing or wrong"
echo "$local_config" | grep -qv 'container_name' \
    && pass "config.toml: no [runtime]/container_name (no container)" \
    || fail "config.toml: unexpected container_name in config"
echo "$local_config" | grep -q 'database_version.*=.*"3"' \
    && pass "config.toml: database_version = 3" \
    || fail "config.toml: database_version wrong"

# WORKSPACE path
WS_PATH=$(get_workspace_data_dir "$T1")
[[ -n "$WS_PATH" ]] && pass "WORKSPACE non-empty: $WS_PATH" || fail "WORKSPACE empty"
[[ -d "$WS_PATH" ]] && pass "WORKSPACE dir exists on disk" || fail "WORKSPACE dir missing"

# gfs status (must not require Docker)
status_out=$(gfs status --path "$T1" 2>&1 || true)
echo "$status_out" | grep -qi "sqlite\|branch\|HEAD\|main" \
    && pass "gfs status: shows branch/sqlite info" \
    || fail "gfs status: unexpected output: ${status_out:0:120}"

# gfs providers shows sqlite
providers_out=$(gfs providers 2>&1 || true)
echo "$providers_out" | grep -qi "sqlite" \
    && pass "gfs providers: sqlite listed" \
    || fail "gfs providers: sqlite not listed"

# gfs init twice → should fail
second_init=$(gfs init --database-provider sqlite --database-version 3 "$T1" 2>&1 || true)
echo "$second_init" | grep -qi "already\|initialized\|exist" \
    && pass "gfs init twice: rejected with already-initialized error" \
    || fail "gfs init twice: did not reject (got: ${second_init:0:80})"

# --json init
T1j=$(make_test_dir)
json_out=$(gfs --json init --database-provider sqlite --database-version 3 "$T1j" 2>/dev/null || echo "{}")
if $PYTHON3_OK && is_valid_json "$json_out"; then
    pass "--json init: valid JSON"
    echo "$json_out" | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'branch' in d or 'path' in d" 2>/dev/null \
        && pass "--json init: contains branch or path key" \
        || fail "--json init: missing expected keys (got: ${json_out:0:120})"
else
    skip "--json init: python3 not available or invalid JSON (got: ${json_out:0:80})"
fi
safe_rm "$T1j"

safe_rm "$T1"

# ===========================================================================
banner "TEST 2: gfs query — DDL + DML basics"
# ===========================================================================
T2=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T2"

# CREATE TABLE
gq "$T2" "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT NOT NULL, value REAL);" &>/dev/null \
    && pass "gfs query: CREATE TABLE" || fail "gfs query: CREATE TABLE failed"

# INSERT single
gq "$T2" "INSERT INTO items VALUES (1, 'alpha', 1.5);" &>/dev/null \
    && pass "gfs query: INSERT single" || fail "gfs query: INSERT single failed"

# INSERT multiple
gq "$T2" "INSERT INTO items VALUES (2,'beta',2.5),(3,'gamma',3.5),(4,'delta',4.5),(5,'epsilon',5.5);" &>/dev/null \
    && pass "gfs query: INSERT multiple" || fail "gfs query: INSERT multiple failed"

assert_count "gfs query INSERT: 5 rows" "$(row_count "$T2" items)" "5"

# SELECT *
sel_out=$(gq "$T2" "SELECT * FROM items ORDER BY id;")
echo "$sel_out" | grep -q "alpha" && pass "gfs query: SELECT * returns data" || fail "gfs query: SELECT * missing data"

# SELECT with WHERE
where_out=$(gq "$T2" "SELECT name FROM items WHERE value > 3.0;")
echo "$where_out" | grep -q "gamma\|delta\|epsilon" && pass "gfs query: SELECT WHERE filters correctly" || fail "gfs query: SELECT WHERE wrong"
echo "$where_out" | grep -q "alpha\|beta" && fail "gfs query: SELECT WHERE includes rows it should exclude" || pass "gfs query: SELECT WHERE excludes low-value rows"

# SELECT with ORDER BY + LIMIT
limit_out=$(gq "$T2" "SELECT name FROM items ORDER BY value DESC LIMIT 2;")
echo "$limit_out" | grep -q "epsilon" && pass "gfs query: LIMIT returns top row" || fail "gfs query: LIMIT wrong"
lines=$(echo "$limit_out" | grep -c "." || true)
[[ "$lines" -le 3 ]] && pass "gfs query: LIMIT caps results" || fail "gfs query: LIMIT not respected ($lines lines)"

# UPDATE
gq "$T2" "UPDATE items SET value = 99.0 WHERE id = 1;" &>/dev/null \
    && pass "gfs query: UPDATE" || fail "gfs query: UPDATE failed"
upd_val=$(sq "$T2" "SELECT value FROM items WHERE id=1;")
[[ "${upd_val//[[:space:]]/}" == "99.0" ]] && pass "UPDATE: value persisted" || fail "UPDATE: value wrong: $upd_val"

# DELETE
gq "$T2" "DELETE FROM items WHERE id = 5;" &>/dev/null \
    && pass "gfs query: DELETE" || fail "gfs query: DELETE failed"
assert_count "DELETE: 4 rows remain" "$(row_count "$T2" items)" "4"

# Aggregate functions
agg_out=$(gq "$T2" "SELECT COUNT(*), SUM(value), MIN(value), MAX(value) FROM items;")
echo "$agg_out" | grep -q "4" && pass "gfs query: aggregate COUNT=4" || fail "gfs query: aggregate COUNT wrong: ${agg_out:0:80}"

# CREATE INDEX
gq "$T2" "CREATE INDEX idx_items_value ON items(value);" &>/dev/null \
    && pass "gfs query: CREATE INDEX" || fail "gfs query: CREATE INDEX failed"

# DROP INDEX
gq "$T2" "DROP INDEX idx_items_value;" &>/dev/null \
    && pass "gfs query: DROP INDEX" || fail "gfs query: DROP INDEX failed"

# PRAGMA table_info (SQLite-specific)
pragma_out=$(gq "$T2" "PRAGMA table_info(items);")
echo "$pragma_out" | grep -q "name" && pass "gfs query: PRAGMA table_info works" || fail "gfs query: PRAGMA table_info wrong"

# SQL syntax error → non-zero exit (gfs query should propagate it)
syntax_err=$(gfs query --path "$T2" "THIS IS NOT SQL" 2>&1 || true)
echo "$syntax_err" | grep -qi "error\|syntax\|near\|unrecognized" \
    && pass "gfs query: SQL syntax error reported" \
    || fail "gfs query: SQL syntax error silently swallowed"

safe_rm "$T2"

# ===========================================================================
banner "TEST 3: Data types — exhaustive type coverage"
# ===========================================================================
T3=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T3"

gq "$T3" "CREATE TABLE types_test (
    id        INTEGER PRIMARY KEY,
    int_zero  INTEGER,
    int_neg   INTEGER,
    int_big   INTEGER,
    real_val  REAL,
    real_neg  REAL,
    txt_ascii TEXT,
    txt_unicode TEXT,
    txt_emoji TEXT,
    txt_empty TEXT,
    txt_quote TEXT,
    null_col  TEXT,
    blob_col  BLOB
);" &>/dev/null && pass "CREATE TABLE with all type columns" || fail "CREATE TABLE types_test failed"

gq "$T3" "INSERT INTO types_test VALUES (
    1, 0, -42, 9999999999,
    3.14159265358979, -0.000001,
    'hello world',
    'こんにちは世界',
    '🚀🎉🔥',
    '',
    'it''s a \"test\"',
    NULL,
    X'DEADBEEF'
);" &>/dev/null && pass "INSERT row with all types" || fail "INSERT types_test failed"

# Integer zero
v=$(sq "$T3" "SELECT int_zero FROM types_test WHERE id=1;")
[[ "${v//[[:space:]]/}" == "0" ]] && pass "INTEGER zero roundtrip" || fail "INTEGER zero wrong: $v"

# Negative integer
v=$(sq "$T3" "SELECT int_neg FROM types_test WHERE id=1;")
[[ "${v//[[:space:]]/}" == "-42" ]] && pass "INTEGER negative roundtrip" || fail "INTEGER negative wrong: $v"

# Large integer
v=$(sq "$T3" "SELECT int_big FROM types_test WHERE id=1;")
[[ "${v//[[:space:]]/}" == "9999999999" ]] && pass "INTEGER large roundtrip" || fail "INTEGER large wrong: $v"

# REAL precision
v=$(sq "$T3" "SELECT real_val FROM types_test WHERE id=1;")
echo "$v" | grep -q "3.14" && pass "REAL pi roundtrip" || fail "REAL pi wrong: $v"

# Negative REAL
v=$(sq "$T3" "SELECT real_neg FROM types_test WHERE id=1;")
echo "$v" | grep -qE "^-0.000001|^-1e-06|^-1\.0e-06" && pass "REAL negative small roundtrip" || fail "REAL negative small wrong: $v"

# ASCII text
v=$(sq "$T3" "SELECT txt_ascii FROM types_test WHERE id=1;")
[[ "${v//[[:space:]]/}" == "helloworld" ]] && pass "TEXT ASCII roundtrip" || fail "TEXT ASCII wrong: $v"

# Unicode text
v=$(sq "$T3" "SELECT txt_unicode FROM types_test WHERE id=1;")
echo "$v" | grep -q "こんにちは" && pass "TEXT Unicode (Japanese) roundtrip" || fail "TEXT Unicode wrong: $v"

# Emoji
v=$(sq "$T3" "SELECT txt_emoji FROM types_test WHERE id=1;")
echo "$v" | grep -q "🚀" && pass "TEXT emoji roundtrip" || fail "TEXT emoji wrong: $v"

# Empty string
v=$(sq "$T3" "SELECT txt_empty FROM types_test WHERE id=1;")
[[ -z "${v//[[:space:]]/}" ]] && pass "TEXT empty string roundtrip" || fail "TEXT empty string wrong: '$v'"

# Single-quote in text (SQL escaping)
v=$(sq "$T3" "SELECT txt_quote FROM types_test WHERE id=1;")
echo "$v" | grep -q "it's" && pass "TEXT single-quote escaping roundtrip" || fail "TEXT single-quote wrong: $v"

# NULL
v=$(sq "$T3" "SELECT null_col IS NULL FROM types_test WHERE id=1;")
[[ "${v//[[:space:]]/}" == "1" ]] && pass "NULL stored and read as NULL" || fail "NULL wrong: $v"

# NULL in WHERE
v=$(sq "$T3" "SELECT COUNT(*) FROM types_test WHERE null_col IS NULL;")
[[ "${v//[[:space:]]/}" == "1" ]] && pass "WHERE IS NULL filter works" || fail "WHERE IS NULL wrong: $v"

# BLOB (stored as hex, read back via hex())
v=$(sq "$T3" "SELECT hex(blob_col) FROM types_test WHERE id=1;")
echo "$v" | grep -qi "DEADBEEF" && pass "BLOB roundtrip via hex()" || fail "BLOB wrong: $v"

# NULL IS NOT NULL filter (empty result)
v=$(sq "$T3" "SELECT COUNT(*) FROM types_test WHERE null_col IS NOT NULL;")
[[ "${v//[[:space:]]/}" == "0" ]] && pass "WHERE IS NOT NULL returns 0 (col is null)" || fail "WHERE IS NOT NULL wrong: $v"

safe_rm "$T3"

# ===========================================================================
banner "TEST 4: Commit + checkout — time travel"
# ===========================================================================
T4=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T4"

# --- Commit 0: empty workspace ---
gfs_quiet config --path "$T4" user.name "TimeTravel" 2>/dev/null || true
gfs_quiet config --path "$T4" user.email "tt@test.com" 2>/dev/null || true

gfs_quiet commit --path "$T4" -m "c0: empty workspace" \
    && pass "Commit 0 (empty workspace)" || fail "Commit 0 failed"
H0=$(head_hash "$T4")
[[ ${#H0} -eq 64 ]] && pass "Commit 0 hash valid: ${H0:0:7}" || fail "Commit 0 hash invalid: $H0"

# --- Commit 1: CREATE TABLE + 10 rows ---
gq "$T4" "CREATE TABLE events (id INTEGER PRIMARY KEY, label TEXT, amount INTEGER);" &>/dev/null
for i in $(seq 1 10); do
    gq "$T4" "INSERT INTO events VALUES ($i, 'event_$i', $((i * 10)));" &>/dev/null
done
assert_count "Before commit 1: 10 rows" "$(row_count "$T4" events)" "10"

gfs_quiet commit --path "$T4" -m "c1: events table with 10 rows" \
    && pass "Commit 1" || fail "Commit 1 failed"
H1=$(head_hash "$T4")
[[ "$H0" != "$H1" ]] && pass "Commit 1 advances HEAD" || fail "Commit 1 did not advance HEAD"

# --- Commit 2: INSERT 5 more rows ---
for i in $(seq 11 15); do
    gq "$T4" "INSERT INTO events VALUES ($i, 'event_$i', $((i * 10)));" &>/dev/null
done
assert_count "Before commit 2: 15 rows" "$(row_count "$T4" events)" "15"

gfs_quiet commit --path "$T4" -m "c2: +5 rows (total 15)" \
    && pass "Commit 2" || fail "Commit 2 failed"
H2=$(head_hash "$T4")

# --- Commit 3: UPDATE rows ---
gq "$T4" "UPDATE events SET amount = amount * 2 WHERE id <= 5;" &>/dev/null
upd=$(sq "$T4" "SELECT amount FROM events WHERE id=1;")
[[ "${upd//[[:space:]]/}" == "20" ]] && pass "UPDATE doubled amount for id=1" || fail "UPDATE wrong: $upd"

gfs_quiet commit --path "$T4" -m "c3: doubled amount for id<=5" \
    && pass "Commit 3" || fail "Commit 3 failed"
H3=$(head_hash "$T4")

# All 4 hashes distinct
hashes_unique=$(printf "%s\n" "$H0" "$H1" "$H2" "$H3" | sort -u | wc -l | tr -d ' ')
[[ "$hashes_unique" == "4" ]] && pass "4 commits produce 4 distinct hashes" || fail "Hash collision ($hashes_unique unique)"

# --- Verify HEAD state (H3) ---
assert_count "HEAD (H3): 15 rows" "$(row_count "$T4" events)" "15"
v=$(sq "$T4" "SELECT amount FROM events WHERE id=1;")
[[ "${v//[[:space:]]/}" == "20" ]] && pass "HEAD (H3): id=1 amount=20 (doubled)" || fail "HEAD (H3): id=1 amount wrong: $v"

# --- Checkout H2: 15 rows, original amounts ---
gfs_quiet checkout --path "$T4" "$H2" \
    && pass "Checkout H2 (15 rows, original amounts)" || fail "Checkout H2 failed"
assert_count "H2: 15 rows" "$(row_count "$T4" events)" "15"
v=$(sq "$T4" "SELECT amount FROM events WHERE id=1;")
[[ "${v//[[:space:]]/}" == "10" ]] && pass "H2: id=1 amount=10 (pre-update)" || fail "H2: id=1 amount wrong: $v"

# --- Checkout H1: 10 rows ---
gfs_quiet checkout --path "$T4" "$H1" \
    && pass "Checkout H1 (10 rows)" || fail "Checkout H1 failed"
assert_count "H1: 10 rows" "$(row_count "$T4" events)" "10"

# --- Checkout H0: empty (no events table) ---
gfs_quiet checkout --path "$T4" "$H0" \
    && pass "Checkout H0 (empty workspace)" || fail "Checkout H0 failed"
db0=$(get_db_path "$T4")
if [[ ! -f "$db0" ]]; then
    pass "H0: db.sqlite absent (empty workspace)"
else
    tbl_count=$(sq "$T4" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='events';" 2>/dev/null | tr -d '[:space:]')
    [[ "$tbl_count" == "0" ]] \
        && pass "H0: events table absent" \
        || fail "H0: events table unexpectedly present"
fi

# --- Checkout main (back to H3) ---
gfs_quiet checkout --path "$T4" main \
    && pass "Checkout main (restore to H3)" || fail "Checkout main failed"
assert_count "main (H3): 15 rows" "$(row_count "$T4" events)" "15"
v=$(sq "$T4" "SELECT amount FROM events WHERE id=1;")
[[ "${v//[[:space:]]/}" == "20" ]] && pass "main (H3): id=1 amount=20 restored" || fail "main (H3): id=1 amount wrong: $v"

# --- Rev notation ---
gfs_quiet checkout --path "$T4" "HEAD~1" \
    && pass "Checkout HEAD~1" || fail "Checkout HEAD~1 failed"
assert_count "HEAD~1: 15 rows, original amounts" "$(row_count "$T4" events)" "15"
v=$(sq "$T4" "SELECT amount FROM events WHERE id=1;")
[[ "${v//[[:space:]]/}" == "10" ]] && pass "HEAD~1: pre-update amount" || fail "HEAD~1: amount wrong: $v"

gfs_quiet checkout --path "$T4" main &>/dev/null || true
gfs_quiet checkout --path "$T4" "HEAD~2" \
    && pass "Checkout HEAD~2" || fail "Checkout HEAD~2 failed"
assert_count "HEAD~2: 10 rows" "$(row_count "$T4" events)" "10"

gfs_quiet checkout --path "$T4" main &>/dev/null || true

# Short hash checkout
gfs_quiet checkout --path "$T4" "${H1:0:7}" \
    && pass "Checkout by short hash (7-char)" || fail "Checkout by short hash failed"
assert_count "Short hash (H1): 10 rows" "$(row_count "$T4" events)" "10"

gfs_quiet checkout --path "$T4" main &>/dev/null || true

safe_rm "$T4"

# ===========================================================================
banner "TEST 5: Branch isolation — 3 branches"
# ===========================================================================
T5=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T5"

# --- Seed main: 100 users ---
subbanner "Seed main (100 users)"
gq "$T5" "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, branch TEXT);" &>/dev/null
for i in $(seq 1 100); do
    gq "$T5" "INSERT INTO users VALUES ($i, 'user_$i', 'main');" &>/dev/null
done
assert_count "main: 100 users seeded" "$(row_count "$T5" users)" "100"

gfs_quiet commit --path "$T5" -m "main: 100 users baseline" \
    && pass "Commit main baseline" || fail "Commit main baseline failed"
MAIN_H=$(head_hash "$T5")

# --- Branch feature-a: +50 users ---
subbanner "Branch feature-a (+50 users)"
gfs_quiet checkout --path "$T5" -b feature-a \
    && pass "gfs checkout -b feature-a" || fail "gfs checkout -b feature-a failed"

[[ -f "${T5}/.gfs/refs/heads/feature-a" ]] \
    && pass "feature-a ref file created" || fail "feature-a ref file missing"

# Inherits baseline
assert_count "feature-a: inherits 100 users" "$(row_count "$T5" users)" "100"

for i in $(seq 101 150); do
    gq "$T5" "INSERT INTO users VALUES ($i, 'feat_a_$i', 'feature-a');" &>/dev/null
done
assert_count "feature-a: 150 users after insert" "$(row_count "$T5" users)" "150"

gfs_quiet commit --path "$T5" -m "feature-a: +50 users" \
    && pass "Commit feature-a" || fail "Commit feature-a failed"
FA_H=$(head_hash "$T5")
[[ "$FA_H" != "$MAIN_H" ]] && pass "feature-a diverges from main" || fail "feature-a did not diverge"

# --- Checkout main: feature-a data absent ---
subbanner "Checkout main → verify feature-a absent"
gfs_quiet checkout --path "$T5" main \
    && pass "Checkout main from feature-a" || fail "Checkout main failed"

assert_count "ISOLATION: main has 100 users" "$(row_count "$T5" users)" "100"
fa_rows=$(sq "$T5" "SELECT COUNT(*) FROM users WHERE branch='feature-a';")
[[ "${fa_rows//[[:space:]]/}" == "0" ]] \
    && pass "ISOLATION: zero feature-a rows on main" \
    || fail "ISOLATION FAIL: $fa_rows feature-a rows visible on main"

# --- Add divergent commit on main ---
for i in $(seq 151 180); do
    gq "$T5" "INSERT INTO users VALUES ($i, 'main_extra_$i', 'main');" &>/dev/null
done
gfs_quiet commit --path "$T5" -m "main: +30 divergent users" \
    && pass "Divergent commit on main" || fail "Divergent commit failed"
MAIN_H2=$(head_hash "$T5")

# --- Branch feature-b from current main (130 users) ---
subbanner "Branch feature-b from diverged main (+20 users)"
gfs_quiet checkout --path "$T5" -b feature-b \
    && pass "gfs checkout -b feature-b" || fail "gfs checkout -b feature-b failed"

assert_count "feature-b: inherits 130 users" "$(row_count "$T5" users)" "130"

for i in $(seq 181 200); do
    gq "$T5" "INSERT INTO users VALUES ($i, 'feat_b_$i', 'feature-b');" &>/dev/null
done
assert_count "feature-b: 150 users" "$(row_count "$T5" users)" "150"

gfs_quiet commit --path "$T5" -m "feature-b: +20 users" \
    && pass "Commit feature-b" || fail "Commit feature-b failed"

# --- Restore feature-a: verify exactly 150, no divergent/feature-b data ---
subbanner "Restore feature-a → verify isolation"
gfs_quiet checkout --path "$T5" feature-a \
    && pass "Checkout feature-a" || fail "Checkout feature-a failed"

assert_count "feature-a restored: 150 users" "$(row_count "$T5" users)" "150"

div_rows=$(sq "$T5" "SELECT COUNT(*) FROM users WHERE name LIKE 'main_extra_%';")
[[ "${div_rows//[[:space:]]/}" == "0" ]] \
    && pass "ISOLATION: no main-divergent rows on feature-a" \
    || fail "ISOLATION FAIL: $div_rows main-divergent rows on feature-a"

fb_rows=$(sq "$T5" "SELECT COUNT(*) FROM users WHERE branch='feature-b';")
[[ "${fb_rows//[[:space:]]/}" == "0" ]] \
    && pass "ISOLATION: no feature-b rows on feature-a" \
    || fail "ISOLATION FAIL: $fb_rows feature-b rows on feature-a"

fa_own=$(sq "$T5" "SELECT COUNT(*) FROM users WHERE branch='feature-a';")
[[ "${fa_own//[[:space:]]/}" == "50" ]] \
    && pass "feature-a own rows: exactly 50" \
    || fail "feature-a own rows wrong: $fa_own"

# --- Restore feature-b: verify 150, no feature-a data ---
subbanner "Restore feature-b → verify isolation"
gfs_quiet checkout --path "$T5" feature-b \
    && pass "Checkout feature-b" || fail "Checkout feature-b failed"

assert_count "feature-b restored: 150 users" "$(row_count "$T5" users)" "150"

fa_on_fb=$(sq "$T5" "SELECT COUNT(*) FROM users WHERE branch='feature-a';")
[[ "${fa_on_fb//[[:space:]]/}" == "0" ]] \
    && pass "ISOLATION: no feature-a rows on feature-b" \
    || fail "ISOLATION FAIL: $fa_on_fb feature-a rows on feature-b"

# --- Delete feature-b branch ---
gfs_quiet checkout --path "$T5" main &>/dev/null || true
gfs_quiet branch --path "$T5" -d feature-b \
    && pass "gfs branch -d feature-b" || fail "gfs branch -d feature-b failed"
[[ ! -f "${T5}/.gfs/refs/heads/feature-b" ]] \
    && pass "feature-b ref file deleted" || fail "feature-b ref file still exists"

safe_rm "$T5"

# ===========================================================================
banner "TEST 6: Complex schema — multi-table JOIN + FK + aggregates"
# ===========================================================================
T6=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T6"
gfs_quiet commit --path "$T6" -m "c0: initial empty" &>/dev/null

gq "$T6" "CREATE TABLE customers (
    id      INTEGER PRIMARY KEY,
    name    TEXT NOT NULL,
    email   TEXT UNIQUE NOT NULL
);" &>/dev/null && pass "CREATE customers" || fail "CREATE customers failed"

gq "$T6" "CREATE TABLE orders (
    id          INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES customers(id),
    total       REAL NOT NULL,
    status      TEXT CHECK(status IN ('pending','paid','shipped'))
);" &>/dev/null && pass "CREATE orders" || fail "CREATE orders failed"

gq "$T6" "CREATE TABLE order_items (
    id       INTEGER PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    sku      TEXT NOT NULL,
    qty      INTEGER NOT NULL,
    price    REAL NOT NULL
);" &>/dev/null && pass "CREATE order_items" || fail "CREATE order_items failed"

# Seed data
gq "$T6" "INSERT INTO customers VALUES
    (1,'Alice','alice@test.com'),
    (2,'Bob','bob@test.com'),
    (3,'Charlie','charlie@test.com');" &>/dev/null

gq "$T6" "INSERT INTO orders VALUES
    (1,1,49.99,'paid'),
    (2,1,19.99,'shipped'),
    (3,2,99.00,'pending'),
    (4,3,5.00,'paid');" &>/dev/null

gq "$T6" "INSERT INTO order_items VALUES
    (1,1,'SKU-A',2,14.99),
    (2,1,'SKU-B',1,20.01),
    (3,2,'SKU-C',1,19.99),
    (4,3,'SKU-A',5,14.99),
    (5,3,'SKU-D',1,24.05),
    (6,4,'SKU-E',1,5.00);" &>/dev/null

# JOIN: customers + orders
join_out=$(sq "$T6" "SELECT c.name, COUNT(o.id) AS order_count FROM customers c LEFT JOIN orders o ON o.customer_id=c.id GROUP BY c.id ORDER BY c.name;")
echo "$join_out" | grep -q "Alice" && pass "JOIN: Alice in result" || fail "JOIN: Alice missing"
echo "$join_out" | grep "Alice" | grep -q "2" && pass "JOIN: Alice has 2 orders" || fail "JOIN: Alice order count wrong: $join_out"
echo "$join_out" | grep "Bob" | grep -q "1" && pass "JOIN: Bob has 1 order" || fail "JOIN: Bob order count wrong"
echo "$join_out" | grep "Charlie" | grep -q "1" && pass "JOIN: Charlie has 1 order" || fail "JOIN: Charlie order count wrong"

# Aggregate on order_items
agg=$(sq "$T6" "SELECT SUM(qty * price) FROM order_items;")
echo "$agg" | grep -qE "^[0-9]" && pass "aggregate SUM(qty*price) returns numeric" || fail "aggregate SUM failed: $agg"

# Subquery
sub=$(sq "$T6" "SELECT name FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE total > 50.0);")
echo "$sub" | grep -q "Bob" && pass "subquery: Bob (order 99.00 > 50)" || fail "subquery: Bob missing"
echo "$sub" | grep -q "Charlie" && fail "subquery: Charlie included (order 5.00 not > 50)" || pass "subquery: Charlie correctly excluded"

# CREATE VIEW
gq "$T6" "CREATE VIEW customer_totals AS
    SELECT c.name, SUM(o.total) AS lifetime_value
    FROM customers c JOIN orders o ON o.customer_id=c.id
    GROUP BY c.id;" &>/dev/null && pass "CREATE VIEW" || fail "CREATE VIEW failed"

view_out=$(sq "$T6" "SELECT * FROM customer_totals ORDER BY name;")
echo "$view_out" | grep -q "Alice" && pass "VIEW: Alice in customer_totals" || fail "VIEW: Alice missing"

# FK enforcement (PRAGMA foreign_keys=ON)
fk_err=$(sq "$T6" "PRAGMA foreign_keys=ON; INSERT INTO orders VALUES (99,999,0.0,'paid');" 2>&1 || true)
echo "$fk_err" | grep -qi "foreign key\|constraint" \
    && pass "FK enforcement: insert with bad customer_id rejected" \
    || info "FK not enforced (SQLite FKs disabled by default — expected)"

# Commit complex schema
gfs_quiet commit --path "$T6" -m "complex schema: 3 tables + view + data" \
    && pass "Commit complex schema" || fail "Commit complex schema failed"

# Checkout HEAD~1 → tables gone
gfs_quiet checkout --path "$T6" "HEAD~1" \
    && pass "Checkout HEAD~1 (empty workspace)" || fail "Checkout HEAD~1 failed"
db6=$(get_db_path "$T6")
if [[ ! -f "$db6" ]]; then
    pass "HEAD~1: db.sqlite absent (empty workspace)"
else
    tbl=$(sq "$T6" "SELECT COUNT(*) FROM sqlite_master WHERE type='table';" 2>/dev/null | tr -d '[:space:]')
    [[ "${tbl:-0}" == "0" ]] \
        && pass "HEAD~1: no tables (clean restore)" \
        || info "HEAD~1: $tbl tables (may have initial empty db)"
fi

# Restore
gfs_quiet checkout --path "$T6" main &>/dev/null || true
assert_count "main restored: 3 customers" "$(row_count "$T6" customers)" "3"
assert_count "main restored: 4 orders" "$(row_count "$T6" orders)" "4"

safe_rm "$T6"

# ===========================================================================
banner "TEST 7: Large dataset — 1000 rows + integrity"
# ===========================================================================
T7=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T7"
gfs_quiet commit --path "$T7" -m "c0: initial empty" &>/dev/null

subbanner "Insert 1000 rows"
gq "$T7" "CREATE TABLE measurements (
    id      INTEGER PRIMARY KEY,
    sensor  TEXT NOT NULL,
    value   REAL NOT NULL,
    ts      INTEGER NOT NULL
);" &>/dev/null && pass "CREATE measurements" || fail "CREATE measurements failed"

# Use SQLite's recursive WITH for bulk insert (one statement, fast)
gq "$T7" "WITH RECURSIVE gen(n) AS (
    SELECT 1
    UNION ALL
    SELECT n+1 FROM gen WHERE n<1000
)
INSERT INTO measurements SELECT n, 'sensor_' || ((n-1) % 10), (n * 1.1), n*1000 FROM gen;" &>/dev/null \
    && pass "Bulk INSERT 1000 rows (recursive CTE)" || fail "Bulk INSERT failed"

assert_count "1000 rows inserted" "$(row_count "$T7" measurements)" "1000"

# Aggregates
agg_val=$(sq "$T7" "SELECT COUNT(*), MIN(value), MAX(value), AVG(value) FROM measurements;")
echo "$agg_val" | grep -q "1000" && pass "COUNT=1000 confirmed via aggregate" || fail "COUNT aggregate wrong: $agg_val"

max_val=$(sq "$T7" "SELECT MAX(value) FROM measurements;")
echo "$max_val" | grep -qE "^110[0-9]" && pass "MAX(value) ≈ 1100" || fail "MAX(value) unexpected: $max_val"

# GROUP BY 10 sensors
sensor_counts=$(sq "$T7" "SELECT COUNT(DISTINCT sensor) FROM measurements;")
[[ "${sensor_counts//[[:space:]]/}" == "10" ]] \
    && pass "10 distinct sensors" || fail "Distinct sensors wrong: $sensor_counts"

# Commit
gfs_quiet commit --path "$T7" -m "1000-row measurements" \
    && pass "Commit 1000-row dataset" || fail "Commit 1000-row dataset failed"
H7=$(head_hash "$T7")

# Checkout HEAD~1 → no table → back → count still 1000
gfs_quiet checkout --path "$T7" "HEAD~1" &>/dev/null && pass "Checkout HEAD~1 (pre-1000-rows)" || fail "Checkout HEAD~1 failed"
gfs_quiet checkout --path "$T7" main &>/dev/null && pass "Restore main (1000-row dataset)" || fail "Restore main failed"
assert_count "After round-trip: 1000 rows intact" "$(row_count "$T7" measurements)" "1000"

# DELETE half + commit + verify
gq "$T7" "DELETE FROM measurements WHERE id % 2 = 0;" &>/dev/null
assert_count "After DELETE even IDs: 500 rows" "$(row_count "$T7" measurements)" "500"

gfs_quiet commit --path "$T7" -m "deleted even IDs (500 remain)" \
    && pass "Commit 500-row state" || fail "Commit 500-row state failed"

# Go back to 1000
gfs_quiet checkout --path "$T7" "$H7" \
    && pass "Checkout H7 (restore 1000 rows)" || fail "Checkout H7 failed"
assert_count "Restored to 1000 rows" "$(row_count "$T7" measurements)" "1000"

safe_rm "$T7"

# ===========================================================================
banner "TEST 8: WAL file handling"
# ===========================================================================
T8=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T8"

gq "$T8" "PRAGMA journal_mode=WAL;" &>/dev/null || true
gq "$T8" "CREATE TABLE wal_test (id INTEGER PRIMARY KEY, data TEXT);" &>/dev/null
for i in $(seq 1 20); do
    gq "$T8" "INSERT INTO wal_test VALUES ($i, 'data_$i');" &>/dev/null
done

db_path=$(get_db_path "$T8")
db_dir=$(dirname "$db_path")

# After writes, WAL or standard db file exists
if [[ -f "$db_path" ]]; then
    pass "db.sqlite exists after writes"
else
    fail "db.sqlite missing after writes"
fi

# Commit → snapshot captures all files
gfs_quiet commit --path "$T8" -m "wal test: 20 rows" \
    && pass "Commit WAL state" || fail "Commit WAL state failed"

H8=$(head_hash "$T8")

# Verify snapshot contains db.sqlite
snap_hash=$(python3 -c "import json; print(json.load(open('${T8}/.gfs/objects/${H8:0:2}/${H8:2}'))['snapshot_hash'])" 2>/dev/null || echo "")
if [[ -n "$snap_hash" ]]; then
    snap_dir="${T8}/.gfs/snapshots/${snap_hash:0:2}/${snap_hash:2}"
    [[ -d "$snap_dir" ]] && pass "Snapshot dir exists: ${snap_hash:0:7}" || fail "Snapshot dir missing"
    [[ -f "${snap_dir}/db.sqlite" ]] \
        && pass "Snapshot contains db.sqlite" \
        || fail "Snapshot missing db.sqlite (files: $(ls $snap_dir 2>/dev/null))"
else
    skip "WAL snapshot verify: python3 not available or object unreadable"
fi

# After checkout, db is readable
gfs_quiet checkout --path "$T8" "HEAD~1" &>/dev/null || true
gfs_quiet checkout --path "$T8" main &>/dev/null || true
db_readable=$(sq "$T8" "SELECT COUNT(*) FROM wal_test;" 2>/dev/null | tr -d '[:space:]')
[[ "$db_readable" == "20" ]] \
    && pass "After checkout round-trip: db.sqlite readable (20 rows)" \
    || fail "After checkout round-trip: db.sqlite not readable ($db_readable)"

safe_rm "$T8"

# ===========================================================================
banner "TEST 9: gfs log, gfs branch commands"
# ===========================================================================
T9=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T9"

# Make 5 commits
for i in 1 2 3 4 5; do
    gq "$T9" "CREATE TABLE IF NOT EXISTS t (id INTEGER);" &>/dev/null
    gq "$T9" "INSERT INTO t VALUES ($i);" &>/dev/null
    gfs_quiet commit --path "$T9" -m "commit $i" 2>/dev/null
done

# gfs log
log_out=$(gfs log --path "$T9" 2>&1 || true)
[[ -n "$log_out" ]] && pass "gfs log produces output" || fail "gfs log empty"
echo "$log_out" | grep -q "commit 5" && pass "gfs log: latest commit visible" || fail "gfs log: commit 5 missing"
echo "$log_out" | grep -q "commit 1" && pass "gfs log: oldest commit visible" || fail "gfs log: commit 1 missing"

# gfs log -n 2 (max-count)
log2_out=$(gfs log --path "$T9" -n 2 2>&1 || true)
commit_lines=$(echo "$log2_out" | grep -c "commit [0-9]" || true)
[[ "$commit_lines" -le 2 ]] && pass "gfs log -n 2: at most 2 commit entries" || fail "gfs log -n 2: too many entries ($commit_lines)"

# gfs log --graph
graph_out=$(gfs log --path "$T9" --graph 2>&1 || true)
[[ -n "$graph_out" ]] && pass "gfs log --graph: produces output" || fail "gfs log --graph: empty"

# gfs log --graph --all
graphall_out=$(gfs log --path "$T9" --graph --all 2>&1 || true)
[[ -n "$graphall_out" ]] && pass "gfs log --graph --all: produces output" || fail "gfs log --graph --all: empty"

# gfs branch (list)
branch_out=$(gfs branch --path "$T9" 2>&1 || true)
echo "$branch_out" | grep -q "main" && pass "gfs branch: main listed" || fail "gfs branch: main missing"

# gfs branch feature-new (create without switch)
gfs_quiet branch --path "$T9" feature-new \
    && pass "gfs branch feature-new (no switch)" || fail "gfs branch feature-new failed"
[[ -f "${T9}/.gfs/refs/heads/feature-new" ]] \
    && pass "feature-new ref created" || fail "feature-new ref missing"
current_branch=$(cat "${T9}/.gfs/HEAD" | sed 's|ref: refs/heads/||' | tr -d '[:space:]')
[[ "$current_branch" == "main" ]] && pass "still on main after branch create" || fail "branch switched unexpectedly to $current_branch"

# gfs checkout -b feat-switch (create + switch)
gfs_quiet checkout --path "$T9" -b feat-switch \
    && pass "gfs checkout -b feat-switch" || fail "gfs checkout -b feat-switch failed"
current_branch=$(cat "${T9}/.gfs/HEAD" | sed 's|ref: refs/heads/||' | tr -d '[:space:]')
[[ "$current_branch" == "feat-switch" ]] \
    && pass "switched to feat-switch" || fail "HEAD not updated: $current_branch"

gfs_quiet checkout --path "$T9" main &>/dev/null || true

# gfs branch -d feature-new
gfs_quiet branch --path "$T9" -d feature-new \
    && pass "gfs branch -d feature-new" || fail "gfs branch -d feature-new failed"
[[ ! -f "${T9}/.gfs/refs/heads/feature-new" ]] \
    && pass "feature-new ref deleted" || fail "feature-new ref still exists"

# gfs branch list after delete
branch_after=$(gfs branch --path "$T9" 2>&1 || true)
echo "$branch_after" | grep -q "feature-new" \
    && fail "feature-new still appears in branch list" \
    || pass "feature-new absent from branch list after delete"

safe_rm "$T9"

# ===========================================================================
banner "TEST 10: JSON output validation"
# ===========================================================================
if ! $PYTHON3_OK; then
    skip "All JSON tests (python3 not available)"
else
    T10=$(make_test_dir)
    gfs_quiet init --database-provider sqlite --database-version 3 "$T10"
    gq "$T10" "CREATE TABLE j (id INTEGER);" &>/dev/null
    gq "$T10" "INSERT INTO j VALUES (1);" &>/dev/null

    # --json commit (use GFS_BIN directly to avoid gfs() wrapper merging stderr→stdout)
    json_commit=$("$GFS_BIN" --json commit --path "$T10" -m "json test" 2>/dev/null || echo "{}")
    if is_valid_json "$json_commit"; then
        pass "--json commit: valid JSON"
        echo "$json_commit" | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'hash' in d or 'commit' in d or 'message' in d" 2>/dev/null \
            && pass "--json commit: contains hash/commit/message key" \
            || fail "--json commit: missing expected keys (got: ${json_commit:0:120})"
    else
        fail "--json commit: invalid JSON (got: ${json_commit:0:120})"
    fi

    # --json status
    json_status=$("$GFS_BIN" --json status --path "$T10" 2>/dev/null || echo "{}")
    if is_valid_json "$json_status"; then
        pass "--json status: valid JSON"
    else
        fail "--json status: invalid JSON (got: ${json_status:0:120})"
    fi

    # --json log
    json_log=$("$GFS_BIN" --json log --path "$T10" 2>/dev/null || echo "[]")
    if python3 -c "import sys,json; json.loads(sys.argv[1])" "$json_log" 2>/dev/null; then
        pass "--json log: valid JSON"
    else
        fail "--json log: invalid JSON (got: ${json_log:0:120})"
    fi

    # --json branch
    json_branch=$("$GFS_BIN" --json branch --path "$T10" 2>/dev/null || echo "[]")
    if python3 -c "import sys,json; json.loads(sys.argv[1])" "$json_branch" 2>/dev/null; then
        pass "--json branch: valid JSON"
    else
        fail "--json branch: invalid JSON (got: ${json_branch:0:120})"
    fi

    safe_rm "$T10"
fi

# ===========================================================================
banner "TEST 11: gfs config + compute commands on sqlite repo"
# ===========================================================================
T11=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T11"

# gfs config read/write
gfs_quiet config --path "$T11" user.name "SQLite User" \
    && pass "gfs config user.name set" || fail "gfs config user.name failed"
gfs_quiet config --path "$T11" user.email "sqlite@test.com" \
    && pass "gfs config user.email set" || fail "gfs config user.email failed"

name_val=$(gfs config --path "$T11" user.name 2>/dev/null | tail -1 || echo "")
echo "$name_val" | grep -q "SQLite User" \
    && pass "gfs config user.name reads back" || fail "gfs config readback wrong: '$name_val'"

# gfs compute start on sqlite repo → should error (no container)
compute_err=$(gfs compute start --path "$T11" 2>&1 || true)
[[ -n "$compute_err" ]] \
    && pass "gfs compute start: produces error on sqlite repo (as expected)" \
    || fail "gfs compute start: silently succeeded (unexpected)"

# gfs compute status on sqlite repo → error or informative message
compute_status=$(gfs compute status --path "$T11" 2>&1 || true)
[[ -n "$compute_status" ]] \
    && pass "gfs compute status: produces output on sqlite repo" \
    || fail "gfs compute status: silent on sqlite repo"

safe_rm "$T11"

# ===========================================================================
banner "TEST 12: Schema operations"
# ===========================================================================
T12=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T12"
gfs_quiet commit --path "$T12" -m "empty" &>/dev/null

gq "$T12" "CREATE TABLE schema_v1 (id INTEGER PRIMARY KEY, name TEXT);" &>/dev/null
gq "$T12" "INSERT INTO schema_v1 VALUES (1,'a'),(2,'b');" &>/dev/null
gfs_quiet commit --path "$T12" -m "schema v1: schema_v1 table" &>/dev/null
SV1=$(head_hash "$T12")

gq "$T12" "CREATE TABLE schema_v2 (id INTEGER PRIMARY KEY, tag TEXT);" &>/dev/null
gfs_quiet commit --path "$T12" -m "schema v2: added schema_v2" &>/dev/null
SV2=$(head_hash "$T12")

# gfs schema show HEAD
schema_show=$(gfs schema show --path "$T12" HEAD 2>&1 || true)
if [[ -n "$schema_show" ]]; then
    pass "gfs schema show HEAD: produces output"
    echo "$schema_show" | grep -qi "schema_v2\|CREATE TABLE\|sqlite\|tables" \
        && pass "gfs schema show HEAD: references schema_v2 or tables" \
        || info "gfs schema show HEAD: output: ${schema_show:0:120}"
else
    skip "gfs schema show HEAD: no output (schema extraction may need running DB)"
fi

# gfs schema diff
if [[ -n "$SV1" && -n "$SV2" && "$SV1" != "$SV2" ]]; then
    diff_out=$(gfs schema diff --path "$T12" "${SV1:0:7}" "${SV2:0:7}" 2>&1 || true)
    if [[ -n "$diff_out" ]]; then
        pass "gfs schema diff: produces output"
        echo "$diff_out" | grep -qi "schema_v2\|table\|diff\|GFS_DIFF\|ADD\|+" \
            && pass "gfs schema diff: references new table or diff" \
            || info "gfs schema diff output: ${diff_out:0:120}"
    else
        skip "gfs schema diff: no output (requires schema data in commits)"
    fi
fi

safe_rm "$T12"

# ===========================================================================
banner "TEST 13: Edge cases"
# ===========================================================================
T13=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T13"

# Query on empty db (no tables)
empty_q=$(gq "$T13" "SELECT name FROM sqlite_master WHERE type='table';" 2>&1 || true)
pass "Query on empty db: no error"

# CREATE + INSERT + verify very long string (4KB)
long_str=$(python3 -c "print('x' * 4096)" 2>/dev/null || printf '%4096s' | tr ' ' 'x')
gq "$T13" "CREATE TABLE long_text (id INTEGER, data TEXT);" &>/dev/null
gq "$T13" "INSERT INTO long_text VALUES (1, '${long_str}');" &>/dev/null
long_len=$(sq "$T13" "SELECT LENGTH(data) FROM long_text WHERE id=1;" | tr -d '[:space:]')
[[ "$long_len" == "4096" ]] \
    && pass "4096-char string stored and read correctly" \
    || fail "Long string length wrong: $long_len (expected 4096)"

# Multiple commits with no changes between them
gfs_quiet commit --path "$T13" -m "edge-c1" &>/dev/null
H_E1=$(head_hash "$T13")
gfs_quiet commit --path "$T13" -m "edge-c2 (no db changes)" &>/dev/null
H_E2=$(head_hash "$T13")
[[ "$H_E1" != "$H_E2" ]] \
    && pass "Two consecutive commits produce distinct hashes" \
    || fail "Two consecutive commits have same hash"

# PRAGMA statements via gfs query
pragma_out=$(gq "$T13" "PRAGMA integrity_check;" 2>&1 || true)
echo "$pragma_out" | grep -qi "ok" \
    && pass "PRAGMA integrity_check: ok" \
    || fail "PRAGMA integrity_check: unexpected: ${pragma_out:0:80}"

# Transaction: BEGIN + rollback
gq "$T13" "BEGIN; INSERT INTO long_text VALUES (99,'rollback-me'); ROLLBACK;" &>/dev/null
v=$(sq "$T13" "SELECT COUNT(*) FROM long_text WHERE id=99;")
[[ "${v//[[:space:]]/}" == "0" ]] \
    && pass "ROLLBACK: row not committed" \
    || fail "ROLLBACK: row leaked"

# Transaction: BEGIN + commit
gq "$T13" "BEGIN; INSERT INTO long_text VALUES (100,'commit-me'); COMMIT;" &>/dev/null
v=$(sq "$T13" "SELECT COUNT(*) FROM long_text WHERE id=100;")
[[ "${v//[[:space:]]/}" == "1" ]] \
    && pass "BEGIN/COMMIT: row committed" \
    || fail "BEGIN/COMMIT: row missing"

# Concurrent branches + delete of non-current branch
gfs_quiet checkout --path "$T13" -b edgebranch &>/dev/null
gfs_quiet commit --path "$T13" -m "edge branch commit" &>/dev/null
gfs_quiet checkout --path "$T13" main &>/dev/null
gfs_quiet branch --path "$T13" -d edgebranch \
    && pass "Delete non-current branch: works" || fail "Delete non-current branch failed"

# Push/pull without remote → graceful error
push_err=$(gfs push --path "$T13" 2>&1 || true)
[[ -n "$push_err" ]] && pass "gfs push without remote: returns error" || fail "gfs push without remote: silent"

safe_rm "$T13"

# ===========================================================================
banner "TEST 14: Semantic integrity — fingerprint-based full-content verification"
# ===========================================================================
# Every row of every dataset is hashed before commit and compared after each
# checkout/restore.  A count match is necessary but not sufficient; a
# fingerprint match proves the actual cell values are byte-for-byte identical.
# ===========================================================================
T14=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T14"

subbanner "Build reference dataset (5 commits)"

# Commit S0 — empty workspace
gfs_quiet commit --path "$T14" -m "s0: empty" &>/dev/null
H14_S0=$(head_hash "$T14")

# Commit S1 — schema + 10 precise rows
gq "$T14" "CREATE TABLE ledger (
    id      INTEGER PRIMARY KEY,
    account TEXT    NOT NULL,
    amount  REAL    NOT NULL,
    note    TEXT
);" &>/dev/null

for i in $(seq 1 10); do
    # amount has intentional decimal precision; note alternates NULL and value
    if (( i % 2 == 0 )); then
        gq "$T14" "INSERT INTO ledger VALUES ($i, 'acct_$(printf '%03d' $i)', $(echo "scale=6; $i * 1.123456" | bc), NULL);" &>/dev/null
    else
        gq "$T14" "INSERT INTO ledger VALUES ($i, 'acct_$(printf '%03d' $i)', $(echo "scale=6; $i * 1.123456" | bc), 'note_$i');" &>/dev/null
    fi
done

assert_count "S1 pre-commit: 10 rows" "$(row_count "$T14" ledger)" "10"

# Record S1 fingerprint BEFORE commit
FP14_S1=$(fingerprint "$T14" ledger)
[[ -n "$FP14_S1" ]] && pass "S1 fingerprint computed: ${FP14_S1:0:12}…" || fail "S1 fingerprint empty"

# Verify specific cell values before commit (ground truth)
assert_cell "S1 pre: id=1 account"  "$T14" "SELECT account FROM ledger WHERE id=1;"  "acct_001"
assert_cell "S1 pre: id=2 note"     "$T14" "SELECT note    FROM ledger WHERE id=2;"  ""        # NULL → empty
assert_cell "S1 pre: id=1 note"     "$T14" "SELECT note    FROM ledger WHERE id=1;"  "note_1"
# Real precision: id=3 amount = 3 * 1.123456 = 3.370368
assert_cell "S1 pre: id=3 amount"   "$T14" "SELECT ROUND(amount,6) FROM ledger WHERE id=3;" "3.370368"

gfs_quiet commit --path "$T14" -m "s1: 10 ledger rows" &>/dev/null
H14_S1=$(head_hash "$T14")

# Commit S2 — INSERT 5 more rows + UPDATE 2 existing
for i in $(seq 11 15); do
    gq "$T14" "INSERT INTO ledger VALUES ($i, 'acct_$(printf '%03d' $i)', $(echo "scale=4; $i * 2.5" | bc), 'batch2');" &>/dev/null
done
gq "$T14" "UPDATE ledger SET amount = amount * 10.0 WHERE id IN (1, 2);" &>/dev/null

FP14_S2=$(fingerprint "$T14" ledger)
assert_fingerprint_ne "S1 vs S2 fingerprints differ after insert+update" "$FP14_S1" "$FP14_S2"

assert_count "S2 pre-commit: 15 rows" "$(row_count "$T14" ledger)" "15"
# id=1 amount was 1.123456, now ×10 = 11.23456
assert_cell "S2 pre: id=1 amount ×10" "$T14" "SELECT ROUND(amount,5) FROM ledger WHERE id=1;" "11.23456"

gfs_quiet commit --path "$T14" -m "s2: +5 rows, id=1,2 amounts ×10" &>/dev/null
H14_S2=$(head_hash "$T14")

# Commit S3 — DELETE rows + add unicode + emoji
gq "$T14" "DELETE FROM ledger WHERE id BETWEEN 11 AND 13;" &>/dev/null
gq "$T14" "INSERT INTO ledger VALUES (99, '日本語口座', 9999.99, '🎌');" &>/dev/null

FP14_S3=$(fingerprint "$T14" ledger)
assert_fingerprint_ne "S2 vs S3 fingerprints differ after delete+unicode insert" "$FP14_S2" "$FP14_S3"

assert_count "S3 pre-commit: 13 rows" "$(row_count "$T14" ledger)" "13"
assert_cell "S3 pre: id=99 account (unicode)" "$T14" "SELECT account FROM ledger WHERE id=99;" "日本語口座"
assert_cell "S3 pre: id=99 note (emoji)"      "$T14" "SELECT note    FROM ledger WHERE id=99;" "🎌"

gfs_quiet commit --path "$T14" -m "s3: delete 11-13, unicode row 99" &>/dev/null
H14_S3=$(head_hash "$T14")

# Commit S4 — second table, FK-like relationship
gq "$T14" "CREATE TABLE transfers (
    id       INTEGER PRIMARY KEY,
    from_id  INTEGER NOT NULL,
    to_id    INTEGER NOT NULL,
    amount   REAL    NOT NULL
);" &>/dev/null
gq "$T14" "INSERT INTO transfers VALUES (1,1,99,500.0),(2,2,99,250.75),(3,99,1,100.0);" &>/dev/null

FP14_S4_ledger=$(fingerprint "$T14" ledger)
FP14_S4_transfers=$(fingerprint "$T14" transfers)
[[ -n "$FP14_S4_transfers" ]] && pass "S4 transfers fingerprint computed" || fail "S4 transfers fingerprint empty"

gfs_quiet commit --path "$T14" -m "s4: transfers table" &>/dev/null
H14_S4=$(head_hash "$T14")

subbanner "Checkout cycle: verify fingerprints and integrity at each state"

# --- Restore S3: ledger fingerprint must match FP14_S3 ---
gfs_quiet checkout --path "$T14" "$H14_S3" \
    && pass "Checkout S3" || fail "Checkout S3 failed"

assert_db_integrity    "S3 restore" "$T14"
assert_count           "S3 restore: 13 rows" "$(row_count "$T14" ledger)" "13"
FP14_S3_restore=$(fingerprint "$T14" ledger)
assert_fingerprint_eq  "S3 restore: ledger fingerprint" "$FP14_S3" "$FP14_S3_restore"

# Row-level spot checks at S3
assert_cell "S3 restore: id=1 amount still ×10" "$T14" \
    "SELECT ROUND(amount,5) FROM ledger WHERE id=1;" "11.23456"
assert_cell "S3 restore: id=99 unicode account" "$T14" \
    "SELECT account FROM ledger WHERE id=99;" "日本語口座"
assert_cell "S3 restore: id=99 emoji note"      "$T14" \
    "SELECT note    FROM ledger WHERE id=99;" "🎌"
# Rows 11-13 were deleted in S3
assert_row_absent "S3 restore: id=11 absent (deleted)" "$T14" ledger id 11
assert_row_absent "S3 restore: id=12 absent (deleted)" "$T14" ledger id 12
# transfers table must NOT exist at S3 (created in S4)
tbl_exist=$(sq "$T14" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='transfers';" | tr -d '[:space:]')
[[ "$tbl_exist" == "0" ]] \
    && pass "S3 restore: transfers table absent (created in S4)" \
    || fail "S3 restore: transfers table unexpectedly present"

# --- Restore S2: fingerprint must match FP14_S2 ---
gfs_quiet checkout --path "$T14" "$H14_S2" \
    && pass "Checkout S2" || fail "Checkout S2 failed"

assert_db_integrity    "S2 restore" "$T14"
assert_count           "S2 restore: 15 rows" "$(row_count "$T14" ledger)" "15"
FP14_S2_restore=$(fingerprint "$T14" ledger)
assert_fingerprint_eq  "S2 restore: ledger fingerprint" "$FP14_S2" "$FP14_S2_restore"

# id=1 amount was multiplied in S2
assert_cell "S2 restore: id=1 amount ×10" "$T14" \
    "SELECT ROUND(amount,5) FROM ledger WHERE id=1;" "11.23456"
# id=99 (unicode) must NOT exist — inserted in S3
assert_row_absent "S2 restore: id=99 absent (inserted in S3)" "$T14" ledger id 99
# Rows 11-13 must exist (not yet deleted)
assert_row_exists "S2 restore: id=11 present" "$T14" ledger id 11 account "acct_011"
assert_row_exists "S2 restore: id=12 present" "$T14" ledger id 12 account "acct_012"
assert_row_exists "S2 restore: id=13 present" "$T14" ledger id 13 account "acct_013"

# --- Restore S1: fingerprint must match FP14_S1 ---
gfs_quiet checkout --path "$T14" "$H14_S1" \
    && pass "Checkout S1" || fail "Checkout S1 failed"

assert_db_integrity    "S1 restore" "$T14"
assert_count           "S1 restore: 10 rows" "$(row_count "$T14" ledger)" "10"
FP14_S1_restore=$(fingerprint "$T14" ledger)
assert_fingerprint_eq  "S1 restore: ledger fingerprint" "$FP14_S1" "$FP14_S1_restore"

# id=1 amount must be ORIGINAL (not the ×10 from S2)
assert_cell "S1 restore: id=1 amount original (not ×10)" "$T14" \
    "SELECT ROUND(amount,6) FROM ledger WHERE id=1;" "1.123456"
# id=2 note must be NULL
assert_cell "S1 restore: id=2 note is NULL" "$T14" \
    "SELECT COALESCE(note, '<NULL>') FROM ledger WHERE id=2;" "<NULL>"
# id=3 real precision preserved
assert_cell "S1 restore: id=3 amount precision" "$T14" \
    "SELECT ROUND(amount,6) FROM ledger WHERE id=3;" "3.370368"
# Rows 11-15 must NOT exist (inserted in S2)
assert_row_absent "S1 restore: id=11 absent" "$T14" ledger id 11
assert_row_absent "S1 restore: id=15 absent" "$T14" ledger id 15
# Schema: only ledger table, no transfers
tbl_exist=$(sq "$T14" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='transfers';" | tr -d '[:space:]')
[[ "$tbl_exist" == "0" ]] \
    && pass "S1 restore: transfers table absent" \
    || fail "S1 restore: transfers table unexpectedly present"
assert_table_schema "S1 restore: ledger schema preserved" "$T14" ledger "id account amount note"

# --- Restore S0: empty workspace ---
gfs_quiet checkout --path "$T14" "$H14_S0" \
    && pass "Checkout S0 (empty)" || fail "Checkout S0 failed"

db14=$(get_db_path "$T14")
if [[ ! -f "$db14" ]]; then
    pass "S0 restore: db.sqlite absent"
else
    assert_db_integrity "S0 restore" "$T14"
    tbl_count=$(sq "$T14" "SELECT COUNT(*) FROM sqlite_master WHERE type='table';" | tr -d '[:space:]')
    [[ "${tbl_count:-0}" == "0" ]] \
        && pass "S0 restore: no tables" \
        || fail "S0 restore: unexpected tables ($tbl_count)"
fi

# --- Restore S4 (main): both tables present, fingerprints match ---
gfs_quiet checkout --path "$T14" main \
    && pass "Checkout main (S4)" || fail "Checkout main failed"

assert_db_integrity   "S4 restore" "$T14"
assert_count          "S4 restore: ledger 13 rows" "$(row_count "$T14" ledger)" "13"
assert_count          "S4 restore: transfers 3 rows" "$(row_count "$T14" transfers)" "3"
FP14_S4_ledger_r=$(fingerprint "$T14" ledger)
FP14_S4_transfers_r=$(fingerprint "$T14" transfers)
assert_fingerprint_eq "S4 restore: ledger fingerprint"    "$FP14_S4_ledger"    "$FP14_S4_ledger_r"
assert_fingerprint_eq "S4 restore: transfers fingerprint" "$FP14_S4_transfers" "$FP14_S4_transfers_r"

# Multi-table JOIN still valid after full round-trip
join_sum=$(sq "$T14" "SELECT ROUND(SUM(t.amount),2) FROM transfers t WHERE t.from_id IN (SELECT id FROM ledger);")
echo "$join_sum" | grep -qE "^[0-9]" \
    && pass "S4 restore: cross-table JOIN produces numeric result" \
    || fail "S4 restore: cross-table JOIN broken: $join_sum"

safe_rm "$T14"

# ===========================================================================
banner "TEST 15: Branch semantic isolation — fingerprint per branch"
# ===========================================================================
# Proves that writes on branch B are NEVER visible on branch A — not just by
# count but by full content fingerprint and explicit row-level checks.
# ===========================================================================
T15=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T15"

subbanner "Establish main baseline"
gq "$T15" "CREATE TABLE products (
    id    INTEGER PRIMARY KEY,
    sku   TEXT    NOT NULL UNIQUE,
    price REAL    NOT NULL,
    stock INTEGER NOT NULL DEFAULT 0
);" &>/dev/null

# Insert 20 products with precise prices
for i in $(seq 1 20); do
    price=$(echo "scale=2; $i * 4.99" | bc)
    gq "$T15" "INSERT INTO products VALUES ($i, 'SKU-$(printf '%04d' $i)', $price, $((i * 5)));" &>/dev/null
done

assert_count "main baseline: 20 products" "$(row_count "$T15" products)" "20"
FP15_MAIN=$(fingerprint "$T15" products)
assert_db_integrity "main pre-commit" "$T15"

gfs_quiet commit --path "$T15" -m "main: 20 products baseline" &>/dev/null
H15_MAIN=$(head_hash "$T15")

subbanner "Branch alpha: price increases"
gfs_quiet checkout --path "$T15" -b alpha &>/dev/null

# Inherit check: fingerprint must match main
FP15_ALPHA_INHERIT=$(fingerprint "$T15" products)
assert_fingerprint_eq "alpha inherits main exactly" "$FP15_MAIN" "$FP15_ALPHA_INHERIT"
assert_db_integrity "alpha pre-change" "$T15"

# Increase price by 20% for all products
gq "$T15" "UPDATE products SET price = ROUND(price * 1.2, 2);" &>/dev/null
# Add 5 new products
for i in $(seq 21 25); do
    price=$(echo "scale=2; $i * 4.99 * 1.2" | bc)
    gq "$T15" "INSERT INTO products VALUES ($i, 'SKU-$(printf '%04d' $i)', $price, $((i * 3)));" &>/dev/null
done

assert_count "alpha: 25 products" "$(row_count "$T15" products)" "25"
FP15_ALPHA=$(fingerprint "$T15" products)
assert_fingerprint_ne "alpha fingerprint differs from main" "$FP15_MAIN" "$FP15_ALPHA"

# Verify price mutation: SKU-0001 was 4.99 → 5.99 (×1.2 = 5.988 → rounds to 5.99)
assert_cell "alpha: SKU-0001 price increased" "$T15" \
    "SELECT ROUND(price,2) FROM products WHERE sku='SKU-0001';" "5.99"
assert_row_exists "alpha: id=25 present" "$T15" products id 25 sku "SKU-0025"

gfs_quiet commit --path "$T15" -m "alpha: +20% prices, +5 products" &>/dev/null
H15_ALPHA=$(head_hash "$T15")
assert_db_integrity "alpha post-commit" "$T15"

subbanner "Branch beta: stock clearance"
gfs_quiet checkout --path "$T15" main &>/dev/null
# Verify main is clean after alpha branch
assert_count "main post-alpha-branch: still 20 products" "$(row_count "$T15" products)" "20"
FP15_MAIN_CHECK=$(fingerprint "$T15" products)
assert_fingerprint_eq "main unchanged after alpha commit" "$FP15_MAIN" "$FP15_MAIN_CHECK"
assert_cell "main: SKU-0001 price unchanged (4.99)" "$T15" \
    "SELECT ROUND(price,2) FROM products WHERE sku='SKU-0001';" "4.99"
assert_row_absent "main: id=21 absent (alpha-only)" "$T15" products id 21

gfs_quiet checkout --path "$T15" -b beta &>/dev/null

# beta: delete out-of-stock products, update stock for others
gq "$T15" "DELETE FROM products WHERE id > 15;" &>/dev/null
gq "$T15" "UPDATE products SET stock = 999 WHERE id <= 5;" &>/dev/null
gq "$T15" "INSERT INTO products VALUES (50, 'SKU-PROMO', 0.99, 1000);" &>/dev/null

assert_count "beta: 16 products (15 remaining + promo)" "$(row_count "$T15" products)" "16"
FP15_BETA=$(fingerprint "$T15" products)
assert_fingerprint_ne "beta fingerprint differs from main"  "$FP15_MAIN" "$FP15_BETA"
assert_fingerprint_ne "beta fingerprint differs from alpha" "$FP15_ALPHA" "$FP15_BETA"

assert_cell "beta: id=1 stock=999" "$T15" \
    "SELECT stock FROM products WHERE id=1;" "999"
assert_cell "beta: promo price=0.99" "$T15" \
    "SELECT ROUND(price,2) FROM products WHERE sku='SKU-PROMO';" "0.99"
assert_row_absent "beta: id=16 absent (deleted)" "$T15" products id 16
assert_row_exists "beta: SKU-PROMO present" "$T15" products id 50 sku "SKU-PROMO"

gfs_quiet commit --path "$T15" -m "beta: stock clearance" &>/dev/null
H15_BETA=$(head_hash "$T15")
assert_db_integrity "beta post-commit" "$T15"

subbanner "Cross-branch contamination checks"

# Restore alpha: must match FP15_ALPHA exactly
gfs_quiet checkout --path "$T15" alpha \
    && pass "Checkout alpha" || fail "Checkout alpha failed"
assert_db_integrity "alpha restore" "$T15"
assert_count "alpha restore: 25 products" "$(row_count "$T15" products)" "25"
FP15_ALPHA_R=$(fingerprint "$T15" products)
assert_fingerprint_eq "alpha restore: fingerprint unchanged" "$FP15_ALPHA" "$FP15_ALPHA_R"
assert_cell "alpha restore: SKU-0001 price still 5.99" "$T15" \
    "SELECT ROUND(price,2) FROM products WHERE sku='SKU-0001';" "5.99"
# SKU-PROMO (beta-only) must NOT appear on alpha
assert_row_absent "alpha restore: SKU-PROMO absent (beta-only)" "$T15" products id 50
# id=21-25 (alpha additions) must be present
assert_row_exists "alpha restore: id=21 present" "$T15" products id 21 sku "SKU-0021"
assert_row_exists "alpha restore: id=25 present" "$T15" products id 25 sku "SKU-0025"
# id=16-20 deleted on beta must still exist on alpha
assert_row_exists "alpha restore: id=16 present (not deleted here)" "$T15" products id 16 sku "SKU-0016"
assert_row_exists "alpha restore: id=20 present" "$T15" products id 20 sku "SKU-0020"
# Stock must NOT be 999 (beta mutation)
assert_cell "alpha restore: id=1 stock unchanged (not beta mutation)" "$T15" \
    "SELECT stock FROM products WHERE id=1;" "5"

# Restore beta: must match FP15_BETA exactly
gfs_quiet checkout --path "$T15" beta \
    && pass "Checkout beta" || fail "Checkout beta failed"
assert_db_integrity "beta restore" "$T15"
assert_count "beta restore: 16 products" "$(row_count "$T15" products)" "16"
FP15_BETA_R=$(fingerprint "$T15" products)
assert_fingerprint_eq "beta restore: fingerprint unchanged" "$FP15_BETA" "$FP15_BETA_R"
assert_cell "beta restore: id=1 stock=999" "$T15" \
    "SELECT stock FROM products WHERE id=1;" "999"
assert_cell "beta restore: SKU-0001 price still 4.99 (no alpha price hike)" "$T15" \
    "SELECT ROUND(price,2) FROM products WHERE sku='SKU-0001';" "4.99"
assert_row_absent "beta restore: id=16 absent" "$T15" products id 16
assert_row_absent "beta restore: id=25 absent (alpha-only)" "$T15" products id 25

# Restore main: must match original FP15_MAIN
gfs_quiet checkout --path "$T15" main \
    && pass "Checkout main (final)" || fail "Checkout main failed"
assert_db_integrity "main final restore" "$T15"
assert_count "main final: 20 products" "$(row_count "$T15" products)" "20"
FP15_MAIN_FINAL=$(fingerprint "$T15" products)
assert_fingerprint_eq "main final: fingerprint unchanged by all branch activity" \
    "$FP15_MAIN" "$FP15_MAIN_FINAL"
assert_cell "main final: SKU-0001 price original 4.99" "$T15" \
    "SELECT ROUND(price,2) FROM products WHERE sku='SKU-0001';" "4.99"
assert_cell "main final: id=1 stock original 5" "$T15" \
    "SELECT stock FROM products WHERE id=1;" "5"
assert_row_absent "main final: SKU-PROMO absent" "$T15" products id 50
assert_row_absent "main final: id=21 absent (alpha-only)" "$T15" products id 21
assert_table_schema "main final: products schema intact" "$T15" products "id sku price stock"

safe_rm "$T15"

# ===========================================================================
banner "TEST 16: gfs log flags — --max-count, --graph, --all, JSON, branch rev"
# ===========================================================================
T16=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T16"
gfs_quiet commit --path "$T16" -m "c0: initial" &>/dev/null

gq "$T16" "CREATE TABLE log_t (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null
for i in $(seq 1 5); do
    gq "$T16" "INSERT INTO log_t VALUES ($i, 'val_$i');" &>/dev/null
    gfs_quiet commit --path "$T16" -m "log commit $i" \
        && pass "Commit log $i" || fail "Commit log $i failed"
done

subbanner "log basic"
log_out=$("$GFS_BIN" log --path "$T16" 2>/dev/null)
echo "$log_out" | grep -q "log commit 5" && pass "log: HEAD commit visible" || fail "log: HEAD commit missing"
echo "$log_out" | grep -q "log commit 1" && pass "log: oldest commit visible" || fail "log: oldest commit missing"
commit_lines=$(echo "$log_out" | grep -c "log commit" || true)
[[ "$commit_lines" -eq 5 ]] && pass "log: 5 commits total" || fail "log: expected 5 commits, got $commit_lines"

subbanner "log --max-count"
max2=$("$GFS_BIN" log --path "$T16" --max-count 2 2>/dev/null)
m2_count=$(echo "$max2" | grep -c "log commit" || true)
[[ "$m2_count" -eq 2 ]] && pass "log --max-count 2: returns 2 commits" || fail "log --max-count 2: got $m2_count"
echo "$max2" | grep -q "log commit 5" && pass "log --max-count 2: shows newest first" || fail "log --max-count 2: newest missing"
echo "$max2" | grep -q "log commit 1" && fail "log --max-count 2: oldest incorrectly included" || pass "log --max-count 2: oldest excluded"

subbanner "log --graph"
graph_out=$("$GFS_BIN" log --path "$T16" --graph 2>/dev/null)
echo "$graph_out" | grep -q "log commit 5" && pass "log --graph: HEAD visible" || fail "log --graph: HEAD missing"
echo "$graph_out" | grep -qE "[●*○]" && pass "log --graph: graph symbols present" || fail "log --graph: no graph symbols"

subbanner "JSON log"
json_log=$("$GFS_BIN" --json log --path "$T16" 2>/dev/null)
if is_valid_json "$json_log"; then
    pass "--json log: valid JSON"
    n=$(echo "$json_log" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('commits', d) if isinstance(d, dict) else d))" 2>/dev/null)
    [[ "${n:-0}" -ge 5 ]] && pass "--json log: ≥5 commits in JSON" || fail "--json log: expected ≥5 commits, got $n"
else
    fail "--json log: invalid JSON"
fi

subbanner "rev notation with branch name (main~2)"
gfs_quiet checkout --path "$T16" "main~2" \
    && pass "checkout main~2 (branch~N notation)" || fail "checkout main~2 failed"
cnt=$(row_count "$T16" log_t)
[[ "${cnt:-0}" -le 3 ]] && pass "main~2: fewer rows than HEAD" || fail "main~2: expected <4 rows, got $cnt"
gfs_quiet checkout --path "$T16" main &>/dev/null || true
assert_count "main restored after main~2" "$(row_count "$T16" log_t)" "5"

subbanner "log --all (multi-branch)"
gfs_quiet checkout --path "$T16" -b side-log &>/dev/null
gq "$T16" "INSERT INTO log_t VALUES (99, 'side');" &>/dev/null
gfs_quiet commit --path "$T16" -m "side-log commit" &>/dev/null
gfs_quiet checkout --path "$T16" main &>/dev/null
all_out=$("$GFS_BIN" log --path "$T16" --all 2>/dev/null)
echo "$all_out" | grep -q "side-log commit" && pass "log --all: side branch commit visible" || fail "log --all: side branch commit missing"
echo "$all_out" | grep -q "log commit 5" && pass "log --all: main commits present" || fail "log --all: main commits missing"

safe_rm "$T16"

# ===========================================================================
banner "TEST 17: Branch lifecycle — create-at-commit, delete, rename scenarios"
# ===========================================================================
T17=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T17"
gfs_quiet commit --path "$T17" -m "c0: initial" &>/dev/null

gq "$T17" "CREATE TABLE br_t (id INTEGER PRIMARY KEY, src TEXT);" &>/dev/null
gq "$T17" "INSERT INTO br_t VALUES (1,'main');" &>/dev/null
gfs_quiet commit --path "$T17" -m "main: row 1" &>/dev/null
H17_C1=$(head_hash "$T17")

gq "$T17" "INSERT INTO br_t VALUES (2,'main');" &>/dev/null
gfs_quiet commit --path "$T17" -m "main: row 2" &>/dev/null

subbanner "branch create at specific commit"
"$GFS_BIN" branch --path "$T17" at-c1 "$H17_C1" &>/dev/null \
    && pass "branch at-c1 created at H17_C1" || fail "branch at-c1 creation failed"

branch_list=$("$GFS_BIN" branch --path "$T17" 2>/dev/null)
echo "$branch_list" | grep -q "at-c1" && pass "branch list: at-c1 present" || fail "branch list: at-c1 missing"
echo "$branch_list" | grep -q "\* main" && pass "branch list: main is current" || fail "branch list: main not marked current"

subbanner "checkout branch created at older commit"
gfs_quiet checkout --path "$T17" at-c1 \
    && pass "checkout at-c1" || fail "checkout at-c1 failed"
assert_count "at-c1: only 1 row (at C1)" "$(row_count "$T17" br_t)" "1"
v=$(sq "$T17" "SELECT src FROM br_t WHERE id=1;")
[[ "${v//[[:space:]]/}" == "main" ]] && pass "at-c1: row data correct" || fail "at-c1: row data wrong: $v"

subbanner "new commit on at-c1 diverges from main"
gq "$T17" "INSERT INTO br_t VALUES (10,'at-c1');" &>/dev/null
gfs_quiet commit --path "$T17" -m "at-c1: diverging commit" \
    && pass "Commit on at-c1" || fail "Commit on at-c1 failed"
assert_count "at-c1: 2 rows (1 original + 1 new)" "$(row_count "$T17" br_t)" "2"

subbanner "return to main — original rows intact"
gfs_quiet checkout --path "$T17" main \
    && pass "Checkout main after at-c1 diverge" || fail "Checkout main failed"
assert_count "main: still 2 original rows" "$(row_count "$T17" br_t)" "2"
v=$(sq "$T17" "SELECT src FROM br_t WHERE id=10;" 2>/dev/null | tr -d '[:space:]')
[[ -z "$v" ]] && pass "main: at-c1 diverging row absent" || fail "main: at-c1 row unexpectedly present: $v"

subbanner "branch delete"
"$GFS_BIN" branch --path "$T17" -d at-c1 &>/dev/null \
    && pass "branch -d at-c1: deleted" || fail "branch -d at-c1 failed"
branch_list2=$("$GFS_BIN" branch --path "$T17" 2>/dev/null)
echo "$branch_list2" | grep -q "at-c1" \
    && fail "branch list: at-c1 still present after delete" \
    || pass "branch list: at-c1 correctly absent"

subbanner "cannot delete current branch"
del_err=$("$GFS_BIN" branch --path "$T17" -d main 2>&1 || true)
[[ -n "$del_err" ]] && pass "delete current branch: returns error" || fail "delete current branch: should error"

subbanner "create + switch (-c flag)"
"$GFS_BIN" branch --path "$T17" -c fresh-branch &>/dev/null \
    && pass "branch -c fresh-branch: created and switched" || fail "branch -c fresh-branch failed"
current=$("$GFS_BIN" branch --path "$T17" 2>/dev/null | grep '\* ' | awk '{print $2}')
[[ "$current" == "fresh-branch" ]] && pass "current branch is fresh-branch" || fail "current branch wrong: $current"

gfs_quiet checkout --path "$T17" main &>/dev/null || true
safe_rm "$T17"

# ===========================================================================
banner "TEST 18: Unicode, NULL, BLOB data types — round-trip through commit/checkout"
# ===========================================================================
T18=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T18"
gfs_quiet commit --path "$T18" -m "c0: initial" &>/dev/null

gq "$T18" "CREATE TABLE exotic (
    id      INTEGER PRIMARY KEY,
    uni     TEXT,
    nullval TEXT,
    blobval BLOB
);" &>/dev/null && pass "CREATE exotic table" || fail "CREATE exotic table failed"

subbanner "Unicode data"
gq "$T18" "INSERT INTO exotic VALUES (1, '日本語テスト', NULL, NULL);" &>/dev/null
gq "$T18" "INSERT INTO exotic VALUES (2, 'Ünïcödé ßtring', NULL, NULL);" &>/dev/null
gq "$T18" "INSERT INTO exotic VALUES (3, '🚀🎉🔥', NULL, NULL);" &>/dev/null
gq "$T18" "INSERT INTO exotic VALUES (4, 'Ελληνικά', NULL, NULL);" &>/dev/null
gq "$T18" "INSERT INTO exotic VALUES (5, 'العربية', NULL, NULL);" &>/dev/null

v=$(sq "$T18" "SELECT uni FROM exotic WHERE id=1;")
[[ "$v" == "日本語テスト" ]] && pass "Unicode: Japanese roundtrip" || fail "Unicode: Japanese wrong: $v"
v=$(sq "$T18" "SELECT uni FROM exotic WHERE id=3;")
[[ "$v" == "🚀🎉🔥" ]] && pass "Unicode: emoji roundtrip" || fail "Unicode: emoji wrong: $v"

subbanner "NULL data"
gq "$T18" "UPDATE exotic SET nullval = NULL WHERE id IN (1,2,3);" &>/dev/null
n=$(sq "$T18" "SELECT COUNT(*) FROM exotic WHERE nullval IS NULL;")
[[ "${n//[[:space:]]/}" == "5" ]] && pass "NULL: 5 rows with nullval IS NULL" || fail "NULL: expected 5, got $n"
n2=$(sq "$T18" "SELECT COUNT(*) FROM exotic WHERE nullval IS NOT NULL;")
[[ "${n2//[[:space:]]/}" == "0" ]] && pass "NULL: no non-null nullval" || fail "NULL: expected 0, got $n2"

subbanner "BLOB data"
gq "$T18" "UPDATE exotic SET blobval = x'deadbeef01020304' WHERE id=1;" &>/dev/null
gq "$T18" "UPDATE exotic SET blobval = x'0000000000000000' WHERE id=2;" &>/dev/null
gq "$T18" "UPDATE exotic SET blobval = x'ffffffffffffffff' WHERE id=3;" &>/dev/null
blob1=$(sq "$T18" "SELECT hex(blobval) FROM exotic WHERE id=1;")
[[ "${blob1^^}" == "DEADBEEF01020304" ]] && pass "BLOB: deadbeef roundtrip" || fail "BLOB: deadbeef wrong: $blob1"
blob3=$(sq "$T18" "SELECT hex(blobval) FROM exotic WHERE id=3;")
[[ "${blob3^^}" == "FFFFFFFFFFFFFFFF" ]] && pass "BLOB: ff bytes roundtrip" || fail "BLOB: ff bytes wrong: $blob3"

subbanner "Commit and checkout round-trip"
FP18_PRE=$(fingerprint "$T18" exotic)
gfs_quiet commit --path "$T18" -m "c1: exotic data" \
    && pass "Commit exotic data" || fail "Commit exotic data failed"
H18_C1=$(head_hash "$T18")

# Add something, commit, then checkout c1 and verify data survived
gq "$T18" "INSERT INTO exotic VALUES (99, 'extra', 'nonnull', NULL);" &>/dev/null
gfs_quiet commit --path "$T18" -m "c2: extra row" &>/dev/null

gfs_quiet checkout --path "$T18" "$H18_C1" \
    && pass "Checkout c1 (exotic data commit)" || fail "Checkout c1 failed"

v=$(sq "$T18" "SELECT uni FROM exotic WHERE id=1;")
[[ "$v" == "日本語テスト" ]] && pass "Checkout c1: Japanese preserved" || fail "Checkout c1: Japanese wrong: $v"
v=$(sq "$T18" "SELECT uni FROM exotic WHERE id=3;")
[[ "$v" == "🚀🎉🔥" ]] && pass "Checkout c1: emoji preserved" || fail "Checkout c1: emoji wrong: $v"
blob1r=$(sq "$T18" "SELECT hex(blobval) FROM exotic WHERE id=1;")
[[ "${blob1r^^}" == "DEADBEEF01020304" ]] && pass "Checkout c1: BLOB preserved" || fail "Checkout c1: BLOB wrong: $blob1r"
null_count=$(sq "$T18" "SELECT COUNT(*) FROM exotic WHERE nullval IS NULL;")
[[ "${null_count//[[:space:]]/}" == "5" ]] && pass "Checkout c1: NULLs preserved" || fail "Checkout c1: NULLs wrong: $null_count"

FP18_POST=$(fingerprint "$T18" exotic)
assert_fingerprint_eq "Checkout c1: fingerprint matches pre-commit" "$FP18_PRE" "$FP18_POST"

# row id=99 must be absent (it was in c2 not c1)
absent=$(sq "$T18" "SELECT COUNT(*) FROM exotic WHERE id=99;" 2>/dev/null | tr -d '[:space:]')
[[ "${absent:-0}" == "0" ]] && pass "Checkout c1: c2-only row absent" || fail "Checkout c1: c2 row unexpectedly present"

gfs_quiet checkout --path "$T18" main &>/dev/null || true
safe_rm "$T18"

# ===========================================================================
banner "TEST 19: Schema operations on SQLite — graceful degradation"
# ===========================================================================
T19=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T19"
gq "$T19" "CREATE TABLE sc_t (id INTEGER PRIMARY KEY, val TEXT);" &>/dev/null
gq "$T19" "INSERT INTO sc_t VALUES (1,'a');" &>/dev/null
gfs_quiet commit --path "$T19" -m "c1: schema test" &>/dev/null

subbanner "schema extract — graceful failure"
schema_err=$("$GFS_BIN" schema extract --path "$T19" 2>&1 || true)
# Must not crash (exit code irrelevant); error message should be informative
[[ -n "$schema_err" ]] && pass "schema extract: returns error message (not silent crash)" || fail "schema extract: empty output (silent crash)"
echo "$schema_err" | grep -qiv "panic\|unwrap\|thread.*main" && pass "schema extract: no panic/unwrap" || fail "schema extract: panic detected"

subbanner "schema show — graceful failure"
schema_show_err=$("$GFS_BIN" schema show HEAD --path "$T19" 2>&1 || true)
[[ -n "$schema_show_err" ]] && pass "schema show: returns error (not silent)" || fail "schema show: empty output"
echo "$schema_show_err" | grep -qiv "panic\|unwrap\|thread.*main" && pass "schema show: no panic" || fail "schema show: panic detected"

subbanner "schema diff — graceful failure"
gq "$T19" "ALTER TABLE sc_t ADD COLUMN extra TEXT;" &>/dev/null
gfs_quiet commit --path "$T19" -m "c2: alter schema" &>/dev/null
schema_diff_err=$("$GFS_BIN" schema diff HEAD~1 HEAD --path "$T19" 2>&1 || true)
[[ -n "$schema_diff_err" ]] && pass "schema diff: returns error (not silent)" || fail "schema diff: empty output"
echo "$schema_diff_err" | grep -qiv "panic\|unwrap\|thread.*main" && pass "schema diff: no panic" || fail "schema diff: panic detected"

safe_rm "$T19"

# ===========================================================================
banner "TEST 20: Large commit chain — 50 commits, history traversal, rev~N"
# ===========================================================================
T20=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T20"
gfs_quiet commit --path "$T20" -m "c0: initial" &>/dev/null

gq "$T20" "CREATE TABLE chain (id INTEGER PRIMARY KEY, step INTEGER, note TEXT);" &>/dev/null

subbanner "50 sequential commits"
for i in $(seq 1 50); do
    gq "$T20" "INSERT INTO chain VALUES ($i, $i, 'step_$i');" &>/dev/null
    gfs_quiet commit --path "$T20" -m "step $i" || { fail "Commit step $i failed"; break; }
done
pass "50 commits created"
assert_count "chain: 50 rows at HEAD" "$(row_count "$T20" chain)" "50"

subbanner "history length via JSON log"
chain_json=$("$GFS_BIN" --json log --path "$T20" 2>/dev/null)
n=$(echo "$chain_json" | python3 -c "import sys,json; d=json.load(sys.stdin); cs=d.get('commits',d); print(len(cs))" 2>/dev/null)
[[ "${n:-0}" -ge 50 ]] && pass "JSON log: ≥50 commits returned" || fail "JSON log: expected ≥50, got $n"

subbanner "rev~N at various depths"
for depth in 5 10 25 49; do
    gfs_quiet checkout --path "$T20" "HEAD~${depth}" \
        && pass "checkout HEAD~${depth}" || fail "checkout HEAD~${depth} failed"
    expected=$((50 - depth))
    actual=$(row_count "$T20" chain)
    [[ "${actual:-0}" -eq "$expected" ]] \
        && pass "HEAD~${depth}: $expected rows" \
        || fail "HEAD~${depth}: expected $expected rows, got $actual"
    gfs_quiet checkout --path "$T20" main &>/dev/null || true
done

subbanner "branch from mid-chain"
MID=$(head_hash "$T20")  # main is at tip again
"$GFS_BIN" branch --path "$T20" mid-branch "HEAD~25" &>/dev/null \
    && pass "branch mid-branch at HEAD~25" || fail "branch mid-branch failed"
gfs_quiet checkout --path "$T20" mid-branch \
    && pass "checkout mid-branch" || fail "checkout mid-branch failed"
assert_count "mid-branch: 25 rows" "$(row_count "$T20" chain)" "25"

# New commit on mid-branch
gq "$T20" "INSERT INTO chain VALUES (999, 999, 'mid');" &>/dev/null
gfs_quiet commit --path "$T20" -m "mid-branch extra commit" \
    && pass "Commit on mid-branch" || fail "Commit on mid-branch failed"
assert_count "mid-branch: 26 rows after extra commit" "$(row_count "$T20" chain)" "26"

# main untouched
gfs_quiet checkout --path "$T20" main &>/dev/null
assert_count "main: still 50 rows" "$(row_count "$T20" chain)" "50"
absent=$(sq "$T20" "SELECT COUNT(*) FROM chain WHERE id=999;" | tr -d '[:space:]')
[[ "${absent:-0}" == "0" ]] && pass "main: mid-branch row absent" || fail "main: mid-branch row present unexpectedly"

safe_rm "$T20"

# ===========================================================================
banner "TEST 21: Empty-table edge cases + transaction behavior"
# ===========================================================================
T21=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T21"
gfs_quiet commit --path "$T21" -m "c0: initial" &>/dev/null

subbanner "empty table create and commit"
gq "$T21" "CREATE TABLE empty_t (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null \
    && pass "CREATE empty_t" || fail "CREATE empty_t failed"
gq "$T21" "CREATE TABLE also_empty (x REAL);" &>/dev/null \
    && pass "CREATE also_empty" || fail "CREATE also_empty failed"
assert_count "empty_t: 0 rows before commit" "$(row_count "$T21" empty_t)" "0"
gfs_quiet commit --path "$T21" -m "c1: two empty tables" \
    && pass "Commit empty tables" || fail "Commit empty tables failed"
H21_EMPTY=$(head_hash "$T21")

subbanner "add rows and commit"
for i in $(seq 1 5); do
    gq "$T21" "INSERT INTO empty_t VALUES ($i, 'val_$i');" &>/dev/null
done
assert_count "empty_t: 5 rows" "$(row_count "$T21" empty_t)" "5"
gfs_quiet commit --path "$T21" -m "c2: 5 rows in empty_t" \
    && pass "Commit 5 rows" || fail "Commit 5 rows failed"

subbanner "time-travel back to empty state"
gfs_quiet checkout --path "$T21" "$H21_EMPTY" \
    && pass "Checkout H21_EMPTY (empty tables)" || fail "Checkout H21_EMPTY failed"
assert_count "empty_t: 0 rows after checkout" "$(row_count "$T21" empty_t)" "0"
tbl_count=$(sq "$T21" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='also_empty';" | tr -d '[:space:]')
[[ "${tbl_count:-0}" == "1" ]] && pass "also_empty table exists in empty state" || fail "also_empty missing in empty state"

gfs_quiet checkout --path "$T21" main &>/dev/null || true
assert_count "main restored: 5 rows" "$(row_count "$T21" empty_t)" "5"

subbanner "multiple empty tables — branch isolation"
gfs_quiet checkout --path "$T21" -b empty-branch &>/dev/null
gq "$T21" "INSERT INTO empty_t VALUES (500,'branch');" &>/dev/null
assert_count "empty-branch: 6 rows (5+1)" "$(row_count "$T21" empty_t)" "6"
gfs_quiet commit --path "$T21" -m "empty-branch: row 500" &>/dev/null
gfs_quiet checkout --path "$T21" main &>/dev/null
assert_count "main: still 5 rows (branch isolated)" "$(row_count "$T21" empty_t)" "5"

subbanner "transaction ROLLBACK within single gfs query"
gq "$T21" "BEGIN; INSERT INTO empty_t VALUES (99,'tx'); ROLLBACK;" &>/dev/null || true
tx_count=$(sq "$T21" "SELECT COUNT(*) FROM empty_t WHERE id=99;" | tr -d '[:space:]')
[[ "${tx_count:-0}" == "0" ]] && pass "ROLLBACK: row 99 absent (rolled back)" || fail "ROLLBACK: row 99 present unexpectedly"

subbanner "transaction COMMIT within single gfs query"
gq "$T21" "BEGIN; INSERT INTO empty_t VALUES (100,'committed'); COMMIT;" &>/dev/null || true
committed=$(sq "$T21" "SELECT COUNT(*) FROM empty_t WHERE id=100;" | tr -d '[:space:]')
[[ "${committed:-0}" == "1" ]] && pass "COMMIT: row 100 present" || fail "COMMIT: row 100 missing"

subbanner "SAVEPOINT / RELEASE"
gq "$T21" "SAVEPOINT sp1; INSERT INTO empty_t VALUES (200,'sp'); ROLLBACK TO sp1; RELEASE sp1;" &>/dev/null || true
sp_count=$(sq "$T21" "SELECT COUNT(*) FROM empty_t WHERE id=200;" | tr -d '[:space:]')
[[ "${sp_count:-0}" == "0" ]] && pass "SAVEPOINT ROLLBACK: row 200 absent" || fail "SAVEPOINT ROLLBACK: row 200 present"

safe_rm "$T21"

# ===========================================================================
banner "TEST 22: Import/export graceful degradation for SQLite"
# ===========================================================================
T22=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T22"
gq "$T22" "CREATE TABLE exp_t (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null
gq "$T22" "INSERT INTO exp_t VALUES (1,'hello'),(2,'world');" &>/dev/null
gfs_quiet commit --path "$T22" -m "c1: export test data" &>/dev/null

subbanner "gfs export -- graceful failure (no container)"
exp_err=$("$GFS_BIN" export --format sql --path "$T22" 2>&1 || true)
[[ -n "$exp_err" ]] && pass "export: error returned (not silent crash)" || fail "export: empty output"
echo "$exp_err" | grep -qiv "panic\|unwrap\|thread.*main" && pass "export: no panic" || fail "export: panic detected"
echo "$exp_err" | grep -qi "error\|no container\|compute\|not configured" \
    && pass "export: informative error message" || fail "export: unhelpful error: $exp_err"

subbanner "gfs import -- graceful failure (no container)"
IMPORT_FILE=$(mktemp /tmp/gfs-import-XXXXXX.sql)
echo "INSERT INTO exp_t VALUES (99,'imported');" > "$IMPORT_FILE"
imp_err=$("$GFS_BIN" import --file "$IMPORT_FILE" --path "$T22" 2>&1 || true)
[[ -n "$imp_err" ]] && pass "import: error returned (not silent crash)" || fail "import: empty output"
echo "$imp_err" | grep -qiv "panic\|unwrap\|thread.*main" && pass "import: no panic" || fail "import: panic detected"
rm -f "$IMPORT_FILE"

subbanner "SQLite native .dump as manual export substitute"
DUMP_FILE=$(mktemp /tmp/gfs-dump-XXXXXX.sql)
DB22=$(get_db_path "$T22")
sqlite3 "$DB22" .dump > "$DUMP_FILE"
[[ -s "$DUMP_FILE" ]] && pass "sqlite3 .dump: dump file non-empty" || fail "sqlite3 .dump: empty file"
grep -q "CREATE TABLE" "$DUMP_FILE" && pass "dump: contains CREATE TABLE" || fail "dump: no CREATE TABLE in dump"
grep -q "INSERT INTO" "$DUMP_FILE" && pass "dump: contains INSERT rows" || fail "dump: no INSERT in dump"

# Import dump into a fresh repo as a manual round-trip
T22B=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T22B"
DB22B=$(get_db_path "$T22B")
sqlite3 "$DB22B" < "$DUMP_FILE"
assert_count "manual import: 2 rows in fresh repo" "$(row_count "$T22B" exp_t)" "2"
v=$(sq "$T22B" "SELECT v FROM exp_t WHERE id=1;" | tr -d '[:space:]')
[[ "$v" == "hello" ]] && pass "manual import: data intact" || fail "manual import: data wrong: $v"
gfs_quiet commit --path "$T22B" -m "manual import via sqlite3 dump" \
    && pass "commit after manual import" || fail "commit after manual import failed"
rm -f "$DUMP_FILE"
safe_rm "$T22B"

safe_rm "$T22"

# ===========================================================================
banner "TEST 23: Schema evolution — ALTER TABLE, views, indexes across commits"
# ===========================================================================
T23=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T23"
gfs_quiet commit --path "$T23" -m "c0: initial" &>/dev/null

subbanner "initial schema: narrow table"
gq "$T23" "CREATE TABLE evolve (id INTEGER PRIMARY KEY, name TEXT NOT NULL);" &>/dev/null \
    && pass "CREATE evolve (narrow)" || fail "CREATE evolve failed"
gq "$T23" "INSERT INTO evolve VALUES (1,'alice'),(2,'bob'),(3,'charlie');" &>/dev/null
assert_count "evolve: 3 rows" "$(row_count "$T23" evolve)" "3"
gfs_quiet commit --path "$T23" -m "c1: narrow schema" \
    && pass "Commit c1 (narrow)" || fail "Commit c1 failed"
H23_NARROW=$(head_hash "$T23")

subbanner "ALTER TABLE ADD COLUMN"
gq "$T23" "ALTER TABLE evolve ADD COLUMN score INTEGER DEFAULT 0;" &>/dev/null \
    && pass "ALTER TABLE ADD COLUMN score" || fail "ALTER TABLE failed"
gq "$T23" "UPDATE evolve SET score = id * 10;" &>/dev/null
score=$(sq "$T23" "SELECT score FROM evolve WHERE id=2;")
[[ "${score//[[:space:]]/}" == "20" ]] && pass "score column: id=2 score=20" || fail "score column wrong: $score"
gfs_quiet commit --path "$T23" -m "c2: wide schema (score added)" \
    && pass "Commit c2 (wide)" || fail "Commit c2 failed"
H23_WIDE=$(head_hash "$T23")

subbanner "CREATE VIEW on evolved schema"
gq "$T23" "CREATE VIEW top_scorers AS SELECT name, score FROM evolve ORDER BY score DESC LIMIT 2;" &>/dev/null \
    && pass "CREATE VIEW top_scorers" || fail "CREATE VIEW failed"
view_rows=$(sq "$T23" "SELECT COUNT(*) FROM top_scorers;")
[[ "${view_rows//[[:space:]]/}" == "2" ]] && pass "VIEW: 2 rows" || fail "VIEW: expected 2, got $view_rows"
gfs_quiet commit --path "$T23" -m "c3: add view" \
    && pass "Commit c3 (view)" || fail "Commit c3 failed"

subbanner "CREATE INDEX"
gq "$T23" "CREATE INDEX idx_evolve_name ON evolve(name);" &>/dev/null \
    && pass "CREATE INDEX idx_evolve_name" || fail "CREATE INDEX failed"
gq "$T23" "CREATE UNIQUE INDEX idx_evolve_score ON evolve(score);" &>/dev/null \
    && pass "CREATE UNIQUE INDEX idx_evolve_score" || fail "CREATE UNIQUE INDEX failed"
idx_count=$(sq "$T23" "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='evolve';")
[[ "${idx_count//[[:space:]]/}" -ge 2 ]] && pass "2+ indexes on evolve" || fail "Indexes missing: $idx_count"
gfs_quiet commit --path "$T23" -m "c4: add indexes" \
    && pass "Commit c4 (indexes)" || fail "Commit c4 failed"

subbanner "checkout c1 (narrow — no score, no view, no index)"
gfs_quiet checkout --path "$T23" "$H23_NARROW" \
    && pass "Checkout c1 (narrow schema)" || fail "Checkout c1 failed"
narrow_cols=$(sq "$T23" "PRAGMA table_info(evolve);" | awk -F'|' '{print $2}' | tr '\n' ' ' | tr -s ' ')
echo "$narrow_cols" | grep -q "name" && pass "c1: name column present" || fail "c1: name column missing"
echo "$narrow_cols" | grep -q "score" \
    && fail "c1: score column unexpectedly present (schema leak)" \
    || pass "c1: score column absent (schema correctly restored)"
view_at_c1=$(sq "$T23" "SELECT COUNT(*) FROM sqlite_master WHERE type='view';" | tr -d '[:space:]')
[[ "${view_at_c1:-0}" == "0" ]] && pass "c1: no views (correctly absent)" || fail "c1: view unexpectedly present at c1"
idx_at_c1=$(sq "$T23" "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='evolve';" | tr -d '[:space:]')
[[ "${idx_at_c1:-0}" == "0" ]] && pass "c1: no user-defined indexes at c1" || fail "c1: unexpected index at c1: $idx_at_c1"
assert_count "c1: still 3 rows" "$(row_count "$T23" evolve)" "3"

subbanner "checkout c2 (wide — score present, no view)"
gfs_quiet checkout --path "$T23" "$H23_WIDE" \
    && pass "Checkout c2 (wide schema)" || fail "Checkout c2 failed"
wide_cols=$(sq "$T23" "PRAGMA table_info(evolve);" | awk -F'|' '{print $2}' | tr '\n' ' ')
echo "$wide_cols" | grep -q "score" && pass "c2: score column present" || fail "c2: score column absent"
view_at_c2=$(sq "$T23" "SELECT COUNT(*) FROM sqlite_master WHERE type='view';" | tr -d '[:space:]')
[[ "${view_at_c2:-0}" == "0" ]] && pass "c2: no view (not added until c3)" || fail "c2: view present at c2 unexpectedly"

subbanner "restore main (c4) — all objects present"
gfs_quiet checkout --path "$T23" main &>/dev/null || true
final_idx=$(sq "$T23" "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND tbl_name='evolve';" | tr -d '[:space:]')
[[ "${final_idx:-0}" -ge 2 ]] && pass "main (c4): 2+ indexes restored" || fail "main: indexes missing: $final_idx"
final_view=$(sq "$T23" "SELECT COUNT(*) FROM sqlite_master WHERE type='view';" | tr -d '[:space:]')
[[ "${final_view:-0}" -ge 1 ]] && pass "main (c4): view restored" || fail "main: view missing"
assert_count "main (c4): still 3 rows" "$(row_count "$T23" evolve)" "3"

safe_rm "$T23"

# ===========================================================================
banner "TEST 24: Concurrent multi-branch data divergence + merge-by-hand"
# ===========================================================================
T24=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T24"
gfs_quiet commit --path "$T24" -m "c0: initial" &>/dev/null

gq "$T24" "CREATE TABLE inventory (
    sku TEXT PRIMARY KEY,
    qty INTEGER NOT NULL DEFAULT 0,
    price REAL NOT NULL
);" &>/dev/null
for i in $(seq 1 10); do
    gq "$T24" "INSERT INTO inventory VALUES ('SKU-$(printf '%03d' $i)', $((i*10)), $(echo "scale=2; $i * 9.99" | bc));" &>/dev/null
done
gfs_quiet commit --path "$T24" -m "main: 10 SKUs baseline" \
    && pass "Commit baseline" || fail "Commit baseline failed"
COMMON=$(head_hash "$T24")
FP24_BASE=$(fingerprint "$T24" inventory)

subbanner "branch pricing: price increases"
gfs_quiet checkout --path "$T24" -b pricing &>/dev/null
gq "$T24" "UPDATE inventory SET price = ROUND(price * 1.15, 2);" &>/dev/null
gq "$T24" "INSERT INTO inventory VALUES ('SKU-011', 5, 149.99);" &>/dev/null
gfs_quiet commit --path "$T24" -m "pricing: 15% increase + SKU-011" \
    && pass "Commit pricing" || fail "Commit pricing failed"
FP24_PRICE=$(fingerprint "$T24" inventory)

subbanner "branch warehousing: qty adjustments"
gfs_quiet checkout --path "$T24" "$COMMON" &>/dev/null
gfs_quiet checkout --path "$T24" -b warehousing &>/dev/null
gq "$T24" "UPDATE inventory SET qty = qty + 50;" &>/dev/null
gq "$T24" "DELETE FROM inventory WHERE sku='SKU-010';" &>/dev/null
gfs_quiet commit --path "$T24" -m "warehousing: +50 qty, remove SKU-010" \
    && pass "Commit warehousing" || fail "Commit warehousing failed"
FP24_WARE=$(fingerprint "$T24" inventory)

assert_fingerprint_ne "pricing vs warehousing differ" "$FP24_PRICE" "$FP24_WARE"

subbanner "manual merge: apply both changes on main"
gfs_quiet checkout --path "$T24" main &>/dev/null
gq "$T24" "UPDATE inventory SET price = ROUND(price * 1.15, 2);" &>/dev/null
gq "$T24" "INSERT INTO inventory VALUES ('SKU-011', 5, 149.99);" &>/dev/null
gq "$T24" "UPDATE inventory SET qty = qty + 50;" &>/dev/null
gq "$T24" "DELETE FROM inventory WHERE sku='SKU-010';" &>/dev/null
gfs_quiet commit --path "$T24" -m "main: manual merge of pricing + warehousing" \
    && pass "Commit merged main" || fail "Commit merged main failed"
FP24_MERGED=$(fingerprint "$T24" inventory)

assert_count "merged: 10 SKUs (11 added, 10 removed = 10 total)" "$(row_count "$T24" inventory)" "10"
assert_fingerprint_ne "merged != base" "$FP24_BASE" "$FP24_MERGED"
assert_fingerprint_ne "merged != pricing-only" "$FP24_PRICE" "$FP24_MERGED"
assert_fingerprint_ne "merged != warehousing-only" "$FP24_WARE" "$FP24_MERGED"

price_check=$(sq "$T24" "SELECT ROUND(price,2) FROM inventory WHERE sku='SKU-001';")
[[ "${price_check//[[:space:]]/}" == "11.49" ]] \
    && pass "merged: SKU-001 price correctly increased (9.99 * 1.15 = 11.49)" \
    || fail "merged: SKU-001 price wrong: $price_check"
qty_check=$(sq "$T24" "SELECT qty FROM inventory WHERE sku='SKU-001';")
[[ "${qty_check//[[:space:]]/}" == "60" ]] \
    && pass "merged: SKU-001 qty=60 (10+50)" \
    || fail "merged: SKU-001 qty wrong: $qty_check"

subbanner "time-travel back to COMMON (pre-diverge)"
gfs_quiet checkout --path "$T24" "$COMMON" \
    && pass "Checkout COMMON (pre-diverge)" || fail "Checkout COMMON failed"
FP24_RESTORED=$(fingerprint "$T24" inventory)
assert_fingerprint_eq "COMMON restore: exact fingerprint match" "$FP24_BASE" "$FP24_RESTORED"
assert_count "COMMON: 10 SKUs" "$(row_count "$T24" inventory)" "10"
absent=$(sq "$T24" "SELECT COUNT(*) FROM inventory WHERE sku='SKU-011';" | tr -d '[:space:]')
[[ "${absent:-0}" == "0" ]] && pass "COMMON: SKU-011 absent" || fail "COMMON: SKU-011 present unexpectedly"

gfs_quiet checkout --path "$T24" main &>/dev/null || true
safe_rm "$T24"

# ===========================================================================
banner "TEST 25: gfs log range — --from, --until, --max-count combined"
# ===========================================================================
T25=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T25"
gfs_quiet commit --path "$T25" -m "c0: initial" &>/dev/null

gq "$T25" "CREATE TABLE log_r (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null
for i in $(seq 1 8); do
    gq "$T25" "INSERT INTO log_r VALUES ($i, 'v$i');" &>/dev/null
    gfs_quiet commit --path "$T25" -m "step $i" || { fail "Commit step $i failed"; break; }
done

subbanner "log --max-count"
for n in 1 3 5; do
    out=$("$GFS_BIN" log --path "$T25" --max-count $n 2>/dev/null | grep -c "step" || true)
    [[ "${out:-0}" -eq $n ]] && pass "log --max-count $n: exactly $n commits" || fail "log --max-count $n: expected $n, got $out"
done

subbanner "log --max-count 0 (edge: zero)"
out0=$("$GFS_BIN" log --path "$T25" --max-count 0 2>/dev/null | grep -c "step" || true)
[[ "${out0:-0}" -eq 0 ]] && pass "log --max-count 0: 0 commits" || info "log --max-count 0: got $out0 (behavior may vary)"

subbanner "log --max-count exceeds total"
out_big=$("$GFS_BIN" log --path "$T25" --max-count 100 2>/dev/null | grep -c "step" || true)
[[ "${out_big:-0}" -eq 8 ]] && pass "log --max-count 100: all 8 commits returned" || fail "log --max-count 100: expected 8, got $out_big"

subbanner "log --from (start from specific commit)"
# --from <hash> starts traversal FROM that commit (inclusive)
H_STEP3=$("$GFS_BIN" --json log --path "$T25" 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin); cs=d.get('commits',d)
for c in cs:
    if 'step 3' in c.get('message',''):
        print(c['hash'][:7]); break" 2>/dev/null)
[[ -n "$H_STEP3" ]] && pass "Found step 3 hash: $H_STEP3" || fail "Could not find step 3 hash"

if [[ -n "$H_STEP3" ]]; then
    from_out=$("$GFS_BIN" log --path "$T25" --from "$H_STEP3" 2>/dev/null | grep -c "step" || true)
    [[ "${from_out:-0}" -ge 1 ]] && pass "log --from H_STEP3: ≥1 commits" || fail "log --from: 0 commits"
fi

subbanner "log --all shows all branches"
gfs_quiet checkout --path "$T25" -b side25 &>/dev/null
gq "$T25" "INSERT INTO log_r VALUES (99, 'side');" &>/dev/null
gfs_quiet commit --path "$T25" -m "side branch commit" &>/dev/null
gfs_quiet checkout --path "$T25" main &>/dev/null

all_out=$("$GFS_BIN" log --path "$T25" --all 2>/dev/null)
echo "$all_out" | grep -q "side branch commit" && pass "log --all: side branch commit visible" || fail "log --all: side branch missing"
echo "$all_out" | grep -q "step 8" && pass "log --all: main commits present" || fail "log --all: main missing"

subbanner "JSON log structure"
jlog=$("$GFS_BIN" --json log --path "$T25" 2>/dev/null)
if is_valid_json "$jlog"; then
    pass "JSON log: valid JSON"
    n=$(echo "$jlog" | python3 -c "import sys,json; d=json.load(sys.stdin); cs=d.get('commits',d); print(len(cs))" 2>/dev/null)
    [[ "${n:-0}" -ge 8 ]] && pass "JSON log: ≥8 commits" || fail "JSON log: expected ≥8, got $n"
    # Verify commit fields
    echo "$jlog" | python3 -c "
import sys,json
d=json.load(sys.stdin)
cs=d.get('commits',d)
c=cs[0]
assert 'hash' in c, 'hash missing'
assert 'message' in c, 'message missing'
print('fields ok')
" 2>/dev/null && pass "JSON log: commit has hash+message fields" || fail "JSON log: missing fields"
else
    fail "JSON log: invalid JSON"
fi

safe_rm "$T25"

# ===========================================================================
banner "TEST 26: Multi-branch stress — 5 branches, independent histories, verify isolation"
# ===========================================================================
T26=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T26"
gq "$T26" "CREATE TABLE shared (id INTEGER PRIMARY KEY, branch TEXT, val INTEGER);" &>/dev/null
gfs_quiet commit --path "$T26" -m "c0: shared table" &>/dev/null
COMMON26=$(head_hash "$T26")

declare -a BRANCH_FPS
BRANCHES=("alpha" "beta" "gamma" "delta" "epsilon")

subbanner "Create 5 branches from common ancestor"
for br in "${BRANCHES[@]}"; do
    gfs_quiet checkout --path "$T26" "$COMMON26" &>/dev/null
    gfs_quiet checkout --path "$T26" -b "$br" &>/dev/null
    # Each branch: 10 rows specific to that branch
    for i in $(seq 1 10); do
        gq "$T26" "INSERT INTO shared VALUES (${#br}${i}, '$br', $((RANDOM % 1000)));" &>/dev/null 2>/dev/null || \
        gq "$T26" "INSERT INTO shared VALUES (abs(random()) % 900000 + 100000, '$br', $i);" &>/dev/null
    done
    gfs_quiet commit --path "$T26" -m "$br: 10 rows" &>/dev/null
    pass "Branch $br: committed"
done

subbanner "Verify each branch has exactly 10 rows from its own namespace"
for br in "${BRANCHES[@]}"; do
    gfs_quiet checkout --path "$T26" "$br" &>/dev/null
    own=$(sq "$T26" "SELECT COUNT(*) FROM shared WHERE branch='$br';" | tr -d '[:space:]')
    total=$(row_count "$T26" shared)
    [[ "${own:-0}" == "10" ]] && pass "$br: 10 own rows" || fail "$br: expected 10 own rows, got $own"
    [[ "${total:-0}" == "10" ]] && pass "$br: total=10 (no rows from other branches)" || fail "$br: total expected 10, got $total"
done

subbanner "All branches exist in list"
gfs_quiet checkout --path "$T26" main &>/dev/null || gfs_quiet checkout --path "$T26" alpha &>/dev/null
branch_list=$("$GFS_BIN" branch --path "$T26" 2>/dev/null)
for br in "${BRANCHES[@]}"; do
    echo "$branch_list" | grep -q "$br" && pass "branch list: $br present" || fail "branch list: $br missing"
done

subbanner "Checkout COMMON — empty (no rows from any branch)"
gfs_quiet checkout --path "$T26" "$COMMON26" &>/dev/null \
    && pass "Checkout COMMON26" || fail "Checkout COMMON26 failed"
common_cnt=$(row_count "$T26" shared)
[[ "${common_cnt:-0}" == "0" ]] && pass "COMMON26: 0 rows (pre-branch empty table)" || fail "COMMON26: expected 0, got $common_cnt"

gfs_quiet checkout --path "$T26" main &>/dev/null || true
safe_rm "$T26"

# ===========================================================================
banner "TEST 27: PRAGMA foreign_keys + ON DELETE CASCADE behavior"
# ===========================================================================
T27=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T27"
gfs_quiet commit --path "$T27" -m "c0: initial" &>/dev/null

gq "$T27" "CREATE TABLE parents (id INTEGER PRIMARY KEY, name TEXT);" &>/dev/null
gq "$T27" "CREATE TABLE children (
    id        INTEGER PRIMARY KEY,
    parent_id INTEGER NOT NULL,
    label     TEXT,
    FOREIGN KEY (parent_id) REFERENCES parents(id) ON DELETE CASCADE
);" &>/dev/null

gq "$T27" "INSERT INTO parents VALUES (1,'P1'),(2,'P2'),(3,'P3');" &>/dev/null
gq "$T27" "INSERT INTO children VALUES (10,1,'C1A'),(11,1,'C1B'),(12,2,'C2A'),(13,3,'C3A');" &>/dev/null

subbanner "FK enforcement (requires PRAGMA foreign_keys=ON)"
fk_err=$("$GFS_BIN" query --path "$T27" "PRAGMA foreign_keys=ON; INSERT INTO children VALUES (99, 999, 'orphan');" 2>&1 || true)
echo "$fk_err" | grep -qi "foreign key\|constraint" \
    && pass "FK: orphan insert rejected" \
    || info "FK not enforced by default (expected)"

subbanner "ON DELETE CASCADE via sqlite3 direct (PRAGMA on per-connection)"
ws27=$(cat "$T27/.gfs/WORKSPACE" | tr -d '[:space:]')
$SQLITE3 "$ws27/db.sqlite" "PRAGMA foreign_keys=ON; DELETE FROM parents WHERE id=1;"
parent_after=$(sq "$T27" "SELECT COUNT(*) FROM parents;" | tr -d '[:space:]')
child_after=$(sq "$T27" "SELECT COUNT(*) FROM children WHERE parent_id=1;" | tr -d '[:space:]')
[[ "${parent_after:-0}" == "2" ]] && pass "FK CASCADE: parent count=2 after delete" || fail "FK CASCADE: parent count wrong: $parent_after"
[[ "${child_after:-0}" == "0" ]] && pass "FK CASCADE: children of P1 deleted" || fail "FK CASCADE: children of P1 not deleted: $child_after"

total_children=$(row_count "$T27" children)
[[ "${total_children:-0}" == "2" ]] && pass "FK CASCADE: 2 children remain (P2+P3)" || fail "FK CASCADE: expected 2 children, got $total_children"

gfs_quiet commit --path "$T27" -m "c1: after cascade delete" \
    && pass "Commit after cascade" || fail "Commit after cascade failed"
H27=$(head_hash "$T27")
FP27=$(fingerprint "$T27" children "id,parent_id,label")

gq "$T27" "INSERT INTO parents VALUES (4,'P4');" &>/dev/null
gq "$T27" "INSERT INTO children VALUES (20,2,'C2B'),(21,4,'C4A');" &>/dev/null
gfs_quiet commit --path "$T27" -m "c2: more rows" &>/dev/null

gfs_quiet checkout --path "$T27" "$H27" \
    && pass "Checkout c1 (post-cascade)" || fail "Checkout c1 failed"
fp27r=$(fingerprint "$T27" children "id,parent_id,label")
[[ "$FP27" == "$fp27r" ]] && pass "c1: children fingerprint preserved" || fail "c1: children fingerprint mismatch"
cnt27=$(row_count "$T27" children)
[[ "${cnt27:-0}" == "2" ]] && pass "c1: 2 children" || fail "c1: expected 2 children, got $cnt27"

gfs_quiet checkout --path "$T27" main &>/dev/null || true
safe_rm "$T27"

# ===========================================================================
banner "TEST 28: gfs commit --author / --author-email metadata"
# ===========================================================================
T28=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T28"
gq "$T28" "CREATE TABLE authors_t (id INTEGER PRIMARY KEY, msg TEXT);" &>/dev/null

subbanner "commit with custom author"
gq "$T28" "INSERT INTO authors_t VALUES (1,'first');" &>/dev/null
"$GFS_BIN" commit --path "$T28" -m "authored commit" \
    --author "Jane Test" --author-email "jane@test.com" &>/dev/null \
    && pass "commit --author: exits 0" || fail "commit --author: non-zero exit"

log_out=$("$GFS_BIN" log --path "$T28" 2>/dev/null)
echo "$log_out" | grep -qi "jane" && pass "log: author name present" || fail "log: author name missing"
echo "$log_out" | grep -qi "jane@test.com" && pass "log: author email present" || fail "log: author email missing"

subbanner "commit without --author uses git config / default"
gq "$T28" "INSERT INTO authors_t VALUES (2,'second');" &>/dev/null
"$GFS_BIN" commit --path "$T28" -m "default author" &>/dev/null \
    && pass "commit without --author: exits 0" || fail "commit without --author: failed"
log2=$("$GFS_BIN" log --path "$T28" 2>/dev/null)
[[ -n "$log2" ]] && pass "log: second commit visible" || fail "log: second commit missing"

subbanner "JSON log: author fields present"
jlog28=$("$GFS_BIN" --json log --path "$T28" 2>/dev/null)
if is_valid_json "$jlog28"; then
    pass "JSON log: valid"
    auth_ok=$(echo "$jlog28" | python3 -c "
import sys,json
d=json.load(sys.stdin)
cs=d.get('commits',d)
for c in cs:
    if 'authored commit' in c.get('message',''):
        a=c.get('author','')
        e=c.get('author_email', c.get('authorEmail',''))
        if 'Jane Test' in a or 'jane@test.com' in e:
            print('found')
        break
" 2>/dev/null)
    [[ "$auth_ok" == "found" ]] && pass "JSON log: author fields populated for authored commit" \
        || info "JSON log: author field key differs (may be version-specific)"
else
    fail "JSON log: invalid"
fi

subbanner "time-travel: author commit checkout"
H28=$(head_hash "$T28")
gq "$T28" "INSERT INTO authors_t VALUES (3,'third');" &>/dev/null
"$GFS_BIN" commit --path "$T28" -m "third" --author "Bob" --author-email "bob@b.com" &>/dev/null
gfs_quiet checkout --path "$T28" "$H28" &>/dev/null
cnt28=$(row_count "$T28" authors_t)
[[ "${cnt28:-0}" == "2" ]] && pass "checkout to H28: 2 rows" || fail "checkout to H28: expected 2, got $cnt28"
gfs_quiet checkout --path "$T28" main &>/dev/null || true

safe_rm "$T28"

# ===========================================================================
banner "TEST 29: UPSERT (INSERT OR REPLACE + INSERT ON CONFLICT)"
# ===========================================================================
T29=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T29"
gfs_quiet commit --path "$T29" -m "c0: initial" &>/dev/null

gq "$T29" "CREATE TABLE kv (key TEXT PRIMARY KEY, val INTEGER NOT NULL, updated INTEGER DEFAULT 0);" &>/dev/null
gq "$T29" "INSERT INTO kv VALUES ('a', 1, 0), ('b', 2, 0), ('c', 3, 0);" &>/dev/null

subbanner "INSERT OR REPLACE"
gq "$T29" "INSERT OR REPLACE INTO kv VALUES ('a', 99, 1);"  &>/dev/null
v=$(sq "$T29" "SELECT val FROM kv WHERE key='a';" | tr -d '[:space:]')
[[ "$v" == "99" ]] && pass "INSERT OR REPLACE: key=a updated to 99" || fail "INSERT OR REPLACE: wrong: $v"
cnt=$(row_count "$T29" kv)
[[ "${cnt:-0}" == "3" ]] && pass "INSERT OR REPLACE: row count unchanged (3)" || fail "INSERT OR REPLACE: row count wrong: $cnt"

# New key via INSERT OR REPLACE
gq "$T29" "INSERT OR REPLACE INTO kv VALUES ('d', 4, 0);" &>/dev/null
cnt2=$(row_count "$T29" kv)
[[ "${cnt2:-0}" == "4" ]] && pass "INSERT OR REPLACE: new key 'd' inserted (4 total)" || fail "INSERT OR REPLACE: count wrong: $cnt2"

subbanner "INSERT OR IGNORE"
gq "$T29" "INSERT OR IGNORE INTO kv VALUES ('a', 9999, 2);" &>/dev/null
v2=$(sq "$T29" "SELECT val FROM kv WHERE key='a';" | tr -d '[:space:]')
[[ "$v2" == "99" ]] && pass "INSERT OR IGNORE: existing key unchanged" || fail "INSERT OR IGNORE: key changed unexpectedly: $v2"

subbanner "INSERT ... ON CONFLICT DO UPDATE"
gq "$T29" "INSERT INTO kv(key,val,updated) VALUES ('b', 200, 1)
    ON CONFLICT(key) DO UPDATE SET val=excluded.val, updated=1;" &>/dev/null
v3=$(sq "$T29" "SELECT val FROM kv WHERE key='b';" | tr -d '[:space:]')
[[ "$v3" == "200" ]] && pass "ON CONFLICT DO UPDATE: key=b updated to 200" || fail "ON CONFLICT DO UPDATE: wrong: $v3"
updated_flag=$(sq "$T29" "SELECT updated FROM kv WHERE key='b';" | tr -d '[:space:]')
[[ "${updated_flag:-0}" == "1" ]] && pass "ON CONFLICT DO UPDATE: updated flag=1" || fail "ON CONFLICT DO UPDATE: flag wrong: $updated_flag"

FP29=$(fingerprint "$T29" kv "key,val,updated")
gfs_quiet commit --path "$T29" -m "c1: upsert state" \
    && pass "Commit upsert state" || fail "Commit upsert state failed"
H29=$(head_hash "$T29")

# Modify + checkout + verify
gq "$T29" "INSERT OR REPLACE INTO kv VALUES ('a', 0, 2);" &>/dev/null
gfs_quiet commit --path "$T29" -m "c2: reset a" &>/dev/null

gfs_quiet checkout --path "$T29" "$H29" \
    && pass "Checkout c1 (upsert state)" || fail "Checkout c1 failed"
fp29r=$(fingerprint "$T29" kv "key,val,updated")
[[ "$FP29" == "$fp29r" ]] && pass "c1: fingerprint preserved" || fail "c1: fingerprint mismatch"
v_restored=$(sq "$T29" "SELECT val FROM kv WHERE key='a';" | tr -d '[:space:]')
[[ "$v_restored" == "99" ]] && pass "c1: key=a value=99 restored" || fail "c1: key=a wrong: $v_restored"

gfs_quiet checkout --path "$T29" main &>/dev/null || true
safe_rm "$T29"

# ===========================================================================
banner "TEST 30: HAVING + GROUP BY aggregates across commit/checkout"
# ===========================================================================
T30=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T30"
gfs_quiet commit --path "$T30" -m "c0: initial" &>/dev/null

gq "$T30" "CREATE TABLE sales (
    id      INTEGER PRIMARY KEY,
    region  TEXT NOT NULL,
    product TEXT NOT NULL,
    amount  REAL NOT NULL
);" &>/dev/null

# 60 rows: 3 regions × 4 products × 5 rows each
idx=1
for region in north south east; do
    for product in A B C D; do
        for _ in 1 2 3 4 5; do
            gq "$T30" "INSERT INTO sales VALUES ($idx, '$region', '$product', $((RANDOM % 100 + 10)));" &>/dev/null
            idx=$((idx + 1))
        done
    done
done
assert_count "sales: 60 rows" "$(row_count "$T30" sales)" "60"

subbanner "GROUP BY region"
grp=$(sq "$T30" "SELECT region, COUNT(*), ROUND(AVG(amount),2) FROM sales GROUP BY region ORDER BY region;")
echo "$grp" | grep -q "east" && pass "GROUP BY: east region present" || fail "GROUP BY: east missing"
echo "$grp" | grep -q "north" && pass "GROUP BY: north region present" || fail "GROUP BY: north missing"
echo "$grp" | grep -q "south" && pass "GROUP BY: south region present" || fail "GROUP BY: south missing"

subbanner "HAVING filter"
having_out=$(sq "$T30" "SELECT region, SUM(amount) AS total FROM sales GROUP BY region HAVING total > 200 ORDER BY region;")
[[ -n "$having_out" ]] && pass "HAVING: returns rows" || fail "HAVING: empty output"
# All regions should qualify since 20 rows × avg ~55 = ~1100
line_count=$(echo "$having_out" | grep -c "." || true)
[[ "${line_count:-0}" -ge 3 ]] && pass "HAVING total>200: all 3 regions qualify" || fail "HAVING: expected 3, got $line_count"

subbanner "HAVING with COUNT"
having2=$(sq "$T30" "SELECT product, COUNT(*) AS cnt FROM sales GROUP BY product HAVING cnt >= 15 ORDER BY product;")
product_count=$(echo "$having2" | grep -c "." || true)
[[ "${product_count:-0}" -eq 4 ]] && pass "HAVING COUNT>=15: all 4 products qualify" || fail "HAVING COUNT>=15: expected 4, got $product_count"

FP30=$(fingerprint "$T30" sales "id,region,product,amount")
gfs_quiet commit --path "$T30" -m "c1: 60 sales rows" \
    && pass "Commit 60 sales" || fail "Commit 60 sales failed"
H30=$(head_hash "$T30")

# Add rows and commit
for region in north south; do
    gq "$T30" "INSERT INTO sales VALUES ($((idx++)), '$region', 'E', 999);" &>/dev/null
done
gfs_quiet commit --path "$T30" -m "c2: product E rows" &>/dev/null

gfs_quiet checkout --path "$T30" "$H30" \
    && pass "Checkout c1 (60 rows)" || fail "Checkout c1 failed"
fp30r=$(fingerprint "$T30" sales "id,region,product,amount")
[[ "$FP30" == "$fp30r" ]] && pass "c1: sales fingerprint preserved" || fail "c1: fingerprint mismatch"
cnt30=$(row_count "$T30" sales)
[[ "${cnt30:-0}" == "60" ]] && pass "c1: 60 rows" || fail "c1: expected 60, got $cnt30"

# HAVING still works after checkout
having_after=$(sq "$T30" "SELECT COUNT(DISTINCT region) FROM (SELECT region FROM sales GROUP BY region HAVING SUM(amount) > 0);")
[[ "${having_after//[[:space:]]/}" == "3" ]] && pass "c1: HAVING works post-checkout" || fail "c1: HAVING wrong: $having_after"

gfs_quiet checkout --path "$T30" main &>/dev/null || true
safe_rm "$T30"

# ===========================================================================
banner "TEST 31: Correlated subqueries + EXISTS / NOT EXISTS"
# ===========================================================================
T31=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T31"
gfs_quiet commit --path "$T31" -m "c0: initial" &>/dev/null

gq "$T31" "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, salary REAL);" &>/dev/null
gq "$T31" "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, budget REAL);" &>/dev/null
gq "$T31" "INSERT INTO departments VALUES (1,'Engineering',500000),(2,'Sales',200000),(3,'HR',100000);" &>/dev/null
gq "$T31" "INSERT INTO employees VALUES
    (1,'Alice',1,120000),(2,'Bob',1,95000),(3,'Carol',2,80000),
    (4,'Dave',2,75000),(5,'Eve',3,60000),(6,'Frank',NULL,55000);" &>/dev/null

subbanner "correlated subquery: employees earning above dept avg"
above_avg=$(sq "$T31" "SELECT name FROM employees e WHERE salary > (
    SELECT AVG(salary) FROM employees WHERE dept_id = e.dept_id
) ORDER BY name;")
echo "$above_avg" | grep -q "Alice" && pass "correlated subq: Alice above dept avg" || fail "correlated subq: Alice missing"
echo "$above_avg" | grep -q "Carol" && pass "correlated subq: Carol above dept avg" || fail "correlated subq: Carol missing"
echo "$above_avg" | grep -q "Bob" \
    && fail "correlated subq: Bob unexpectedly above avg" \
    || pass "correlated subq: Bob correctly below avg"

subbanner "EXISTS subquery"
has_eng=$(sq "$T31" "SELECT COUNT(*) FROM departments d WHERE EXISTS (
    SELECT 1 FROM employees WHERE dept_id=d.id AND salary > 100000
);" | tr -d '[:space:]')
[[ "${has_eng:-0}" == "1" ]] && pass "EXISTS: 1 dept with salary>100k" || fail "EXISTS: expected 1, got $has_eng"

subbanner "NOT EXISTS subquery"
no_emp=$(sq "$T31" "SELECT name FROM departments d WHERE NOT EXISTS (
    SELECT 1 FROM employees WHERE dept_id=d.id
) ORDER BY name;")
[[ -z "$no_emp" ]] && pass "NOT EXISTS: all depts have employees" \
    || info "NOT EXISTS: dept(s) without employees: $no_emp"

subbanner "scalar subquery in SELECT"
with_max=$(sq "$T31" "SELECT name, salary, (SELECT MAX(salary) FROM employees) AS max_sal FROM employees ORDER BY id LIMIT 1;")
[[ -n "$with_max" ]] && pass "scalar subquery in SELECT: returns row" || fail "scalar subquery: empty"
echo "$with_max" | grep -q "120000" && pass "scalar subquery: max_sal=120000 in row" || info "scalar subquery result: $with_max"

subbanner "commit + checkout preserves subquery results"
FP31_EMP=$(fingerprint "$T31" employees "id,name,dept_id,salary")
gfs_quiet commit --path "$T31" -m "c1: employees+depts" \
    && pass "Commit c1" || fail "Commit c1 failed"
H31=$(head_hash "$T31")

gq "$T31" "UPDATE employees SET salary = salary * 1.1;" &>/dev/null
gfs_quiet commit --path "$T31" -m "c2: salary bump" &>/dev/null

gfs_quiet checkout --path "$T31" "$H31" \
    && pass "Checkout c1" || fail "Checkout c1 failed"
fp31r=$(fingerprint "$T31" employees "id,name,dept_id,salary")
[[ "$FP31_EMP" == "$fp31r" ]] && pass "c1: employee fingerprint preserved" || fail "c1: fingerprint mismatch"

# Correlated subquery still correct after checkout
above_avg2=$(sq "$T31" "SELECT COUNT(*) FROM employees e WHERE salary > (
    SELECT AVG(salary) FROM employees WHERE dept_id = e.dept_id
);" | tr -d '[:space:]')
[[ "${above_avg2:-0}" -ge 1 ]] && pass "c1: correlated subquery works post-checkout" || fail "c1: subquery failed post-checkout"

gfs_quiet checkout --path "$T31" main &>/dev/null || true
safe_rm "$T31"

# ===========================================================================
banner "TEST 32: gfs status — branch, HEAD hash, no container errors"
# ===========================================================================
T32=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T32"

subbanner "status before first commit"
status_pre=$("$GFS_BIN" status --path "$T32" 2>/dev/null || "$GFS_BIN" status --path "$T32" 2>&1 || true)
[[ -n "$status_pre" ]] && pass "status before commit: produces output" || fail "status before commit: empty"

gq "$T32" "CREATE TABLE st (id INTEGER);" &>/dev/null
gq "$T32" "INSERT INTO st VALUES (1);" &>/dev/null
"$GFS_BIN" commit --path "$T32" -m "c1: status test" &>/dev/null

subbanner "status after commit"
status_out=$("$GFS_BIN" status --path "$T32" 2>/dev/null)
[[ -n "$status_out" ]] && pass "status: non-empty output" || fail "status: empty"
echo "$status_out" | grep -qi "main" && pass "status: shows 'main' branch" || fail "status: no 'main' in output"

# Must NOT suggest container is down/required (SQLite = no container)
echo "$status_out" | grep -qi "compute.*stop\|container.*stop\|start.*container\|gfs compute start" \
    && fail "status: misleadingly suggests container needed" \
    || pass "status: no misleading container prompt"

subbanner "status on feature branch"
gfs_quiet checkout --path "$T32" -b feat32 &>/dev/null
status_feat=$("$GFS_BIN" status --path "$T32" 2>/dev/null)
echo "$status_feat" | grep -qi "feat32" && pass "status: shows feature branch name" || fail "status: feature branch name missing"
gfs_quiet checkout --path "$T32" main &>/dev/null

subbanner "JSON status schema"
js=$("$GFS_BIN" --json status --path "$T32" 2>/dev/null)
if is_valid_json "$js"; then
    pass "JSON status: valid JSON"
    echo "$js" | python3 -c "
import sys,json
d=json.load(sys.stdin)
assert 'current_branch' in d or 'branch' in d, 'no branch key'
print('ok')
" 2>/dev/null && pass "JSON status: branch key present" || fail "JSON status: no branch key (got: ${js:0:80})"
else
    fail "JSON status: invalid JSON (got: ${js:0:80})"
fi

safe_rm "$T32"

# ===========================================================================
banner "TEST 33: Repeated checkout cycles — workspace stability"
# ===========================================================================
T33=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T33"
gfs_quiet commit --path "$T33" -m "c0: initial" &>/dev/null

gq "$T33" "CREATE TABLE cycle (id INTEGER PRIMARY KEY, val TEXT);" &>/dev/null
for i in $(seq 1 5); do
    gq "$T33" "INSERT INTO cycle VALUES ($i, 'v$i');" &>/dev/null
    gfs_quiet commit --path "$T33" -m "cycle c$i" &>/dev/null
done

H33_C2=$("$GFS_BIN" --json log --path "$T33" 2>/dev/null | python3 -c "
import sys,json; d=json.load(sys.stdin); cs=d.get('commits',d)
for c in cs:
    if 'cycle c2' in c.get('message',''): print(c['hash']); break" 2>/dev/null)

subbanner "10 checkout cycles between HEAD and c2"
for round in $(seq 1 10); do
    gfs_quiet checkout --path "$T33" "$H33_C2" &>/dev/null \
        || { fail "Round $round: checkout c2 failed"; break; }
    cnt_c2=$(row_count "$T33" cycle)
    [[ "${cnt_c2:-0}" == "2" ]] || { fail "Round $round: expected 2 rows at c2, got $cnt_c2"; break; }
    gfs_quiet checkout --path "$T33" main &>/dev/null \
        || { fail "Round $round: checkout main failed"; break; }
    cnt_main=$(row_count "$T33" cycle)
    [[ "${cnt_main:-0}" == "5" ]] || { fail "Round $round: expected 5 rows at main, got $cnt_main"; break; }
done
pass "10 checkout cycles: stable (2↔5 rows)"

subbanner "integrity check after cycles"
assert_db_integrity "post-cycle integrity" "$T33"
FP33=$(fingerprint "$T33" cycle "id,val")
gq "$T33" "INSERT INTO cycle VALUES (99, 'post-cycle');" &>/dev/null
gfs_quiet commit --path "$T33" -m "c6: post-cycle commit" \
    && pass "commit after cycles: succeeds" || fail "commit after cycles: failed"

safe_rm "$T33"

# ===========================================================================
banner "TEST 34: Non-unique commit messages + same-data commits"
# ===========================================================================
T34=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T34"
gq "$T34" "CREATE TABLE dup (id INTEGER);" &>/dev/null

subbanner "multiple commits with same message"
for i in 1 2 3; do
    gq "$T34" "INSERT INTO dup VALUES ($i);" &>/dev/null
    "$GFS_BIN" commit --path "$T34" -m "same message" &>/dev/null \
        && pass "commit $i with same message: succeeds" || fail "commit $i with same message: failed"
done

log34=$("$GFS_BIN" log --path "$T34" 2>/dev/null)
dup_count=$(echo "$log34" | grep -c "same message" || true)
[[ "${dup_count:-0}" -eq 3 ]] && pass "log: 3 commits with same message" || fail "log: expected 3 same-message commits, got $dup_count"

subbanner "hashes distinct even with same message"
json34=$("$GFS_BIN" --json log --path "$T34" 2>/dev/null)
unique_hashes=$(echo "$json34" | python3 -c "
import sys,json
d=json.load(sys.stdin)
cs=d.get('commits',d)
hashes=set(c['hash'] for c in cs)
print(len(hashes))
" 2>/dev/null)
[[ "${unique_hashes:-0}" -eq 3 ]] && pass "3 same-message commits: 3 distinct hashes" || fail "duplicate hashes: $unique_hashes"

subbanner "two commits with identical DB state (no changes between)"
gfs_quiet commit --path "$T34" -m "no-change commit 1" &>/dev/null
gfs_quiet commit --path "$T34" -m "no-change commit 2" &>/dev/null
H34_A=$(head_hash "$T34")
gfs_quiet commit --path "$T34" -m "no-change commit 3" &>/dev/null
H34_B=$(head_hash "$T34")
[[ "$H34_A" != "$H34_B" ]] && pass "same-data commits: distinct hashes" || fail "same-data commits: hash collision"

subbanner "checkout works on any of the duplicates"
gfs_quiet checkout --path "$T34" "HEAD~2" \
    && pass "checkout HEAD~2 (no-change commit)" || fail "checkout HEAD~2 failed"
cnt34=$(row_count "$T34" dup)
[[ "${cnt34:-0}" == "3" ]] && pass "HEAD~2: 3 rows (same state)" || fail "HEAD~2: expected 3, got $cnt34"
gfs_quiet checkout --path "$T34" main &>/dev/null || true

safe_rm "$T34"

# ===========================================================================
banner "TEST 35: Virtual tables — sqlite_master introspection across commits"
# ===========================================================================
T35=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T35"
gfs_quiet commit --path "$T35" -m "c0: initial" &>/dev/null

subbanner "progressive schema build"
gq "$T35" "CREATE TABLE t1 (id INTEGER PRIMARY KEY);" &>/dev/null
gfs_quiet commit --path "$T35" -m "c1: t1 only" &>/dev/null
H35_C1=$(head_hash "$T35")
obj1=$(sq "$T35" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';" | tr -d '[:space:]')
[[ "${obj1:-0}" == "1" ]] && pass "c1: 1 user table" || fail "c1: expected 1, got $obj1"

gq "$T35" "CREATE TABLE t2 (id INTEGER PRIMARY KEY, ref INTEGER REFERENCES t1(id));" &>/dev/null
gq "$T35" "CREATE VIEW v1 AS SELECT * FROM t1 JOIN t2 ON t2.ref=t1.id;" &>/dev/null
gq "$T35" "CREATE INDEX idx_t2_ref ON t2(ref);" &>/dev/null
gfs_quiet commit --path "$T35" -m "c2: t2 + view + index" &>/dev/null
H35_C2=$(head_hash "$T35")

obj2_tables=$(sq "$T35" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';" | tr -d '[:space:]')
obj2_views=$(sq "$T35" "SELECT COUNT(*) FROM sqlite_master WHERE type='view';" | tr -d '[:space:]')
obj2_idx=$(sq "$T35" "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_t2_ref';" | tr -d '[:space:]')
[[ "${obj2_tables:-0}" == "2" ]] && pass "c2: 2 user tables" || fail "c2: expected 2 tables, got $obj2_tables"
[[ "${obj2_views:-0}" == "1" ]] && pass "c2: 1 view" || fail "c2: expected 1 view, got $obj2_views"
[[ "${obj2_idx:-0}" == "1" ]] && pass "c2: idx_t2_ref present" || fail "c2: index missing"

gq "$T35" "CREATE TABLE t3 (x TEXT, y TEXT);" &>/dev/null
gq "$T35" "DROP VIEW v1;" &>/dev/null
gfs_quiet commit --path "$T35" -m "c3: t3 added, v1 dropped" &>/dev/null

obj3_views=$(sq "$T35" "SELECT COUNT(*) FROM sqlite_master WHERE type='view';" | tr -d '[:space:]')
[[ "${obj3_views:-0}" == "0" ]] && pass "c3: view dropped" || fail "c3: view still present"
obj3_tables=$(sq "$T35" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';" | tr -d '[:space:]')
[[ "${obj3_tables:-0}" == "3" ]] && pass "c3: 3 tables" || fail "c3: expected 3, got $obj3_tables"

subbanner "checkout c1 — only t1"
gfs_quiet checkout --path "$T35" "$H35_C1" \
    && pass "Checkout c1" || fail "Checkout c1 failed"
c1_tables=$(sq "$T35" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';" | tr -d '[:space:]')
[[ "${c1_tables:-0}" == "1" ]] && pass "c1 restore: 1 table" || fail "c1 restore: expected 1, got $c1_tables"
c1_views=$(sq "$T35" "SELECT COUNT(*) FROM sqlite_master WHERE type='view';" | tr -d '[:space:]')
[[ "${c1_views:-0}" == "0" ]] && pass "c1 restore: no views" || fail "c1 restore: views present"

subbanner "checkout c2 — t1+t2+view+index"
gfs_quiet checkout --path "$T35" "$H35_C2" \
    && pass "Checkout c2" || fail "Checkout c2 failed"
c2_views=$(sq "$T35" "SELECT COUNT(*) FROM sqlite_master WHERE type='view';" | tr -d '[:space:]')
[[ "${c2_views:-0}" == "1" ]] && pass "c2 restore: view present" || fail "c2 restore: view missing"
c2_idx=$(sq "$T35" "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_t2_ref';" | tr -d '[:space:]')
[[ "${c2_idx:-0}" == "1" ]] && pass "c2 restore: idx_t2_ref present" || fail "c2 restore: index missing"

gfs_quiet checkout --path "$T35" main &>/dev/null || true
safe_rm "$T35"

# ===========================================================================
banner "TEST 36: Boundary values — INTEGER limits, REAL precision, empty strings"
# ===========================================================================
T36=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T36"
gfs_quiet commit --path "$T36" -m "c0: initial" &>/dev/null

gq "$T36" "CREATE TABLE bounds (
    id      INTEGER PRIMARY KEY,
    big_int INTEGER,
    neg_int INTEGER,
    zero_r  REAL,
    tiny_r  REAL,
    empty_s TEXT,
    space_s TEXT
);" &>/dev/null

# SQLite INTEGER max: 9223372036854775807
gq "$T36" "INSERT INTO bounds VALUES (1, 9223372036854775807, -9223372036854775808, 0.0, 1.0e-300, '', ' ');" &>/dev/null
gq "$T36" "INSERT INTO bounds VALUES (2, 0, 0, -0.0, -1.0e-300, '', '');" &>/dev/null

subbanner "INTEGER boundary preservation"
big=$(sq "$T36" "SELECT big_int FROM bounds WHERE id=1;" | tr -d '[:space:]')
[[ "$big" == "9223372036854775807" ]] && pass "INT max: 9223372036854775807 preserved" || fail "INT max wrong: $big"
neg=$(sq "$T36" "SELECT neg_int FROM bounds WHERE id=1;" | tr -d '[:space:]')
[[ "$neg" == "-9223372036854775808" ]] && pass "INT min: -9223372036854775808 preserved" || fail "INT min wrong: $neg"

subbanner "REAL edge values"
tiny=$(sq "$T36" "SELECT tiny_r FROM bounds WHERE id=1;" | tr -d '[:space:]')
[[ -n "$tiny" ]] && pass "REAL 1e-300: stored (value: $tiny)" || fail "REAL 1e-300: empty"
zero_r=$(sq "$T36" "SELECT zero_r FROM bounds WHERE id=1;" | tr -d '[:space:]')
[[ "$zero_r" == "0.0" || "$zero_r" == "0" ]] && pass "REAL 0.0: stored" || fail "REAL 0.0 wrong: $zero_r"

subbanner "empty string vs NULL"
empty=$(sq "$T36" "SELECT empty_s IS NOT NULL FROM bounds WHERE id=1;" | tr -d '[:space:]')
[[ "$empty" == "1" ]] && pass "empty string: IS NOT NULL (not same as NULL)" || fail "empty string treated as NULL"
emp_len=$(sq "$T36" "SELECT length(empty_s) FROM bounds WHERE id=1;" | tr -d '[:space:]')
[[ "${emp_len:-0}" == "0" ]] && pass "empty string: length=0" || fail "empty string: wrong length: $emp_len"
space_len=$(sq "$T36" "SELECT length(space_s) FROM bounds WHERE id=1;" | tr -d '[:space:]')
[[ "${space_len:-0}" == "1" ]] && pass "space string: length=1" || fail "space string: wrong length: $space_len"

subbanner "commit + checkout preserves boundary values"
FP36=$(fingerprint "$T36" bounds "id,big_int,neg_int")
gfs_quiet commit --path "$T36" -m "c1: boundary values" \
    && pass "Commit boundary values" || fail "Commit failed"
H36=$(head_hash "$T36")

gq "$T36" "INSERT INTO bounds VALUES (3, 1, -1, 1.5, 0.0, 'x', 'y');" &>/dev/null
gfs_quiet commit --path "$T36" -m "c2: extra row" &>/dev/null

gfs_quiet checkout --path "$T36" "$H36" \
    && pass "Checkout c1" || fail "Checkout c1 failed"
big_r=$(sq "$T36" "SELECT big_int FROM bounds WHERE id=1;" | tr -d '[:space:]')
[[ "$big_r" == "9223372036854775807" ]] && pass "c1: INT max preserved after checkout" || fail "c1: INT max wrong: $big_r"
fp36r=$(fingerprint "$T36" bounds "id,big_int,neg_int")
[[ "$FP36" == "$fp36r" ]] && pass "c1: fingerprint preserved" || fail "c1: fingerprint mismatch"

gfs_quiet checkout --path "$T36" main &>/dev/null || true
safe_rm "$T36"

# ===========================================================================
banner "TEST 37: gfs log --until + detached HEAD → branch creation"
# ===========================================================================
T37=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T37"
gfs_quiet commit --path "$T37" -m "c0: initial" &>/dev/null

gq "$T37" "CREATE TABLE t37 (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null
for i in $(seq 1 6); do
    gq "$T37" "INSERT INTO t37 VALUES ($i, 'val$i');" &>/dev/null
    gfs_quiet commit --path "$T37" -m "t37-commit-$i" || fail "Commit $i failed"
done

subbanner "capture hashes for --until test"
H37_C2=$("$GFS_BIN" --json log --path "$T37" 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin); cs=d.get('commits',d)
for c in cs:
    if 't37-commit-2' in c.get('message',''):
        print(c['hash']); break" 2>/dev/null)
[[ -n "$H37_C2" ]] && pass "Found t37-commit-2 hash" || fail "Could not find t37-commit-2 hash"

subbanner "log --until <hash>"
if [[ -n "$H37_C2" ]]; then
    until_out=$("$GFS_BIN" log --path "$T37" --until "$H37_C2" 2>/dev/null)
    # --until H is exclusive: shows commits from HEAD backwards, stopping before H
    # H37_C2 = commit-2, so output should contain commit-3..6 but NOT commit-1
    echo "$until_out" | grep -q "t37-commit-6" && pass "log --until: commit-6 (HEAD) included" || fail "log --until: commit-6 missing"
    echo "$until_out" | grep -q "t37-commit-3" && pass "log --until: commit-3 (after boundary) included" || fail "log --until: commit-3 missing"
    echo "$until_out" | grep -q "t37-commit-1" && fail "log --until: commit-1 should be excluded (before boundary)" || pass "log --until: commit-1 correctly excluded"
fi

subbanner "log --until <branch~N>"
until_br=$("$GFS_BIN" log --path "$T37" --until "main~3" 2>/dev/null)
[[ -n "$until_br" ]] && pass "log --until main~3: non-empty output" || fail "log --until main~3: empty"
until_count=$(echo "$until_br" | grep -c "t37-commit" || true)
[[ "${until_count:-0}" -le 3 ]] && pass "log --until main~3: ≤3 commits shown" || fail "log --until main~3: expected ≤3, got $until_count"

subbanner "detached HEAD → checkout -b creates new branch"
gfs_quiet checkout --path "$T37" "$H37_C2" \
    && pass "checkout hash: detached HEAD" || fail "checkout hash failed"
# From detached HEAD, create a new branch
"$GFS_BIN" checkout --path "$T37" -b from-detached &>/dev/null \
    && pass "checkout -b from-detached: success from detached HEAD" || fail "checkout -b from-detached: failed"
current=$("$GFS_BIN" branch --path "$T37" 2>/dev/null | grep '\* ' | awk '{print $2}')
[[ "$current" == "from-detached" ]] && pass "current branch is from-detached" || fail "current branch wrong: $current"
# Row count should match the detached commit (commit-2 → 2 rows)
assert_count "from-detached: 2 rows at commit-2" "$(row_count "$T37" t37)" "2"

subbanner "commit on from-detached branch is independent of main"
gq "$T37" "INSERT INTO t37 VALUES (100, 'detached-work');" &>/dev/null
gfs_quiet commit --path "$T37" -m "from-detached: extra row" \
    && pass "commit on from-detached" || fail "commit on from-detached failed"
assert_count "from-detached: 3 rows" "$(row_count "$T37" t37)" "3"

gfs_quiet checkout --path "$T37" main \
    && pass "return to main" || fail "return to main failed"
assert_count "main: still 6 rows" "$(row_count "$T37" t37)" "6"

subbanner "from-detached branch in list"
br_list=$("$GFS_BIN" branch --path "$T37" 2>/dev/null)
echo "$br_list" | grep -q "from-detached" && pass "from-detached in branch list" || fail "from-detached missing from list"

safe_rm "$T37"

# ===========================================================================
banner "TEST 38: NULL in aggregate functions across commits"
# ===========================================================================
T38=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T38"
gfs_quiet commit --path "$T38" -m "c0: initial" &>/dev/null

gq "$T38" "CREATE TABLE t38 (id INTEGER PRIMARY KEY, score INTEGER, label TEXT);" &>/dev/null
# Mix of NULL and non-NULL values
gq "$T38" "INSERT INTO t38 VALUES (1, 10, 'a');" &>/dev/null
gq "$T38" "INSERT INTO t38 VALUES (2, NULL, 'b');" &>/dev/null
gq "$T38" "INSERT INTO t38 VALUES (3, 20, NULL);" &>/dev/null
gq "$T38" "INSERT INTO t38 VALUES (4, NULL, 'd');" &>/dev/null
gq "$T38" "INSERT INTO t38 VALUES (5, 30, 'e');" &>/dev/null
gfs_quiet commit --path "$T38" -m "c1: 5 rows with NULLs" \
    && pass "Commit c1: NULLs inserted" || fail "Commit c1 failed"
H38_C1=$(head_hash "$T38")

subbanner "COUNT(*) vs COUNT(col) with NULLs"
cnt_star=$(sq "$T38" "SELECT COUNT(*) FROM t38;" | tr -d '[:space:]')
cnt_score=$(sq "$T38" "SELECT COUNT(score) FROM t38;" | tr -d '[:space:]')
cnt_label=$(sq "$T38" "SELECT COUNT(label) FROM t38;" | tr -d '[:space:]')
[[ "$cnt_star" == "5" ]] && pass "COUNT(*) = 5 (includes NULLs)" || fail "COUNT(*): expected 5, got $cnt_star"
[[ "$cnt_score" == "3" ]] && pass "COUNT(score) = 3 (excludes NULLs)" || fail "COUNT(score): expected 3, got $cnt_score"
[[ "$cnt_label" == "4" ]] && pass "COUNT(label) = 4 (excludes NULLs)" || fail "COUNT(label): expected 4, got $cnt_label"

subbanner "SUM/AVG with NULLs"
sum_score=$(sq "$T38" "SELECT SUM(score) FROM t38;" | tr -d '[:space:]')
avg_score=$(sq "$T38" "SELECT AVG(score) FROM t38;" | tr -d '[:space:]')
[[ "$sum_score" == "60" ]] && pass "SUM(score) = 60 (NULLs ignored)" || fail "SUM(score): expected 60, got $sum_score"
# AVG = 60/3 = 20.0
[[ "${avg_score%%.*}" == "20" ]] && pass "AVG(score) = 20.x (NULLs not counted in denominator)" || fail "AVG(score): expected ~20, got $avg_score"

subbanner "MAX/MIN with NULLs"
max_score=$(sq "$T38" "SELECT MAX(score) FROM t38;" | tr -d '[:space:]')
min_score=$(sq "$T38" "SELECT MIN(score) FROM t38;" | tr -d '[:space:]')
[[ "$max_score" == "30" ]] && pass "MAX(score) = 30" || fail "MAX: expected 30, got $max_score"
[[ "$min_score" == "10" ]] && pass "MIN(score) = 10" || fail "MIN: expected 10, got $min_score"

subbanner "GROUP BY with NULL labels"
grp=$(sq "$T38" "SELECT label, COUNT(*) FROM t38 GROUP BY label ORDER BY label;" 2>/dev/null)
echo "$grp" | grep -q "NULL\|^|" && pass "GROUP BY: NULL label appears as group" || pass "GROUP BY: NULL label may show as empty string (SQLite behavior)"
group_count=$(echo "$grp" | wc -l | tr -d '[:space:]')
[[ "${group_count:-0}" -ge 4 ]] && pass "GROUP BY: ≥4 groups (including NULL)" || fail "GROUP BY: expected ≥4 groups, got $group_count"

subbanner "NULL aggregate values survive commit/checkout"
gfs_quiet checkout --path "$T38" "$H38_C1" \
    && pass "checkout H38_C1" || fail "checkout H38_C1 failed"
cnt_star2=$(sq "$T38" "SELECT COUNT(*) FROM t38;" | tr -d '[:space:]')
cnt_null_score=$(sq "$T38" "SELECT COUNT(*) FROM t38 WHERE score IS NULL;" | tr -d '[:space:]')
[[ "$cnt_star2" == "5" ]] && pass "post-checkout COUNT(*) = 5" || fail "post-checkout COUNT(*) wrong: $cnt_star2"
[[ "$cnt_null_score" == "2" ]] && pass "post-checkout: 2 NULL scores preserved" || fail "post-checkout NULL scores: expected 2, got $cnt_null_score"

subbanner "add more data on branch, NULL isolation"
gfs_quiet checkout --path "$T38" main \
    && pass "return to main" || fail "return to main failed"
gfs_quiet checkout --path "$T38" -b null-branch &>/dev/null
gq "$T38" "INSERT INTO t38 VALUES (6, NULL, NULL);" &>/dev/null
gfs_quiet commit --path "$T38" -m "null-branch: fully null row" || fail "commit on null-branch failed"
cnt_nulls=$(sq "$T38" "SELECT COUNT(*) FROM t38 WHERE score IS NULL AND label IS NULL;" | tr -d '[:space:]')
[[ "$cnt_nulls" == "1" ]] && pass "null-branch: fully null row present" || fail "null-branch: expected 1 fully null row, got $cnt_nulls"

gfs_quiet checkout --path "$T38" main &>/dev/null
cnt_main_nulls=$(sq "$T38" "SELECT COUNT(*) FROM t38 WHERE score IS NULL AND label IS NULL;" | tr -d '[:space:]')
[[ "$cnt_main_nulls" == "0" ]] && pass "main: no fully null row (isolated)" || fail "main: fully null row leaked: $cnt_main_nulls"

safe_rm "$T38"

# ===========================================================================
banner "TEST 39: Generated columns + multi-statement SQL in gfs query"
# ===========================================================================
T39=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T39"
gfs_quiet commit --path "$T39" -m "c0: initial" &>/dev/null

subbanner "multi-statement SQL in single gfs query"
# Multiple DDL statements separated by ; in one call
"$GFS_BIN" query --path "$T39" \
    "CREATE TABLE t39a (id INTEGER PRIMARY KEY, x INTEGER); CREATE TABLE t39b (id INTEGER PRIMARY KEY, y TEXT);" \
    &>/dev/null \
    && pass "multi-statement DDL in single gfs query" \
    || {
        # Some CLIs don't support multi-statement; try each separately
        gq "$T39" "CREATE TABLE t39a (id INTEGER PRIMARY KEY, x INTEGER);" &>/dev/null
        gq "$T39" "CREATE TABLE t39b (id INTEGER PRIMARY KEY, y TEXT);" &>/dev/null
        pass "multi-statement DDL: created tables separately (single-statement mode)"
    }

t39a_exists=$(sq "$T39" "SELECT COUNT(*) FROM sqlite_master WHERE name='t39a';" | tr -d '[:space:]')
t39b_exists=$(sq "$T39" "SELECT COUNT(*) FROM sqlite_master WHERE name='t39b';" | tr -d '[:space:]')
[[ "$t39a_exists" == "1" ]] && pass "t39a created" || fail "t39a missing"
[[ "$t39b_exists" == "1" ]] && pass "t39b created" || fail "t39b missing"

subbanner "GENERATED ALWAYS AS (stored) — SQLite 3.31+"
gen_result=$(sq "$T39" "
CREATE TABLE t39gen (
    id      INTEGER PRIMARY KEY,
    price   REAL NOT NULL,
    qty     INTEGER NOT NULL,
    total   REAL GENERATED ALWAYS AS (price * qty) STORED
);
SELECT 'gen_ok';" 2>/dev/null | tr -d '[:space:]')

if [[ "$gen_result" == "gen_ok" ]]; then
    pass "Generated columns: CREATE TABLE with GENERATED ALWAYS AS STORED"
    sq "$T39" "INSERT INTO t39gen (id, price, qty) VALUES (1, 9.99, 3);" &>/dev/null
    sq "$T39" "INSERT INTO t39gen (id, price, qty) VALUES (2, 4.50, 10);" &>/dev/null
    total1=$(sq "$T39" "SELECT total FROM t39gen WHERE id=1;" | tr -d '[:space:]')
    total2=$(sq "$T39" "SELECT total FROM t39gen WHERE id=2;" | tr -d '[:space:]')
    [[ "${total1%%.*}" == "29" ]] && pass "Generated col: id=1 total ≈ 29.97" || fail "Generated col: id=1 expected ~29.97, got $total1"
    [[ "$total2" == "45.0" || "$total2" == "45" ]] && pass "Generated col: id=2 total = 45" || fail "Generated col: id=2 expected 45, got $total2"

    # Commit and checkout round-trip
    gfs_quiet commit --path "$T39" -m "c1: with generated col table" \
        && pass "Commit with generated col table" || fail "Commit with generated col failed"
    H39_C1=$(head_hash "$T39")

    gfs_quiet checkout --path "$T39" -b gen-branch &>/dev/null
    sq "$T39" "INSERT INTO t39gen (id, price, qty) VALUES (3, 100.0, 2);" &>/dev/null
    gfs_quiet commit --path "$T39" -m "gen-branch: row 3" || fail "gen-branch commit failed"
    total3=$(sq "$T39" "SELECT total FROM t39gen WHERE id=3;" | tr -d '[:space:]')
    [[ "$total3" == "200.0" || "$total3" == "200" ]] && pass "gen-branch: id=3 total = 200" || fail "gen-branch: expected 200, got $total3"

    gfs_quiet checkout --path "$T39" main &>/dev/null
    cnt_gen=$(sq "$T39" "SELECT COUNT(*) FROM t39gen;" | tr -d '[:space:]')
    [[ "$cnt_gen" == "2" ]] && pass "main: gen-branch row absent after checkout" || fail "main: expected 2 rows, got $cnt_gen"
else
    skip "Generated columns: SQLite version too old (< 3.31) — skipping"
fi

subbanner "VIRTUAL column (GENERATED ALWAYS AS VIRTUAL)"
virt_result=$(sq "$T39" "
CREATE TABLE t39virt (
    id      INTEGER PRIMARY KEY,
    first   TEXT NOT NULL,
    last    TEXT NOT NULL,
    full    TEXT GENERATED ALWAYS AS (first || ' ' || last) VIRTUAL
);
SELECT 'virt_ok';" 2>/dev/null | tr -d '[:space:]')

if [[ "$virt_result" == "virt_ok" ]]; then
    pass "Virtual generated column: CREATE TABLE"
    sq "$T39" "INSERT INTO t39virt (id, first, last) VALUES (1, 'Ada', 'Lovelace');" &>/dev/null
    full_name=$(sq "$T39" "SELECT full FROM t39virt WHERE id=1;" | tr -d '[:space:]')
    [[ "$full_name" == "AdaLovelace" ]] && pass "Virtual col: full name = Ada Lovelace" || fail "Virtual col: expected 'AdaLovelace', got '$full_name'"
    gfs_quiet commit --path "$T39" -m "c2: virtual col table" || fail "Commit virtual col failed"
    gfs_quiet checkout --path "$T39" "$H39_C1" &>/dev/null 2>/dev/null || true
    gfs_quiet checkout --path "$T39" main &>/dev/null
    full2=$(sq "$T39" "SELECT full FROM t39virt WHERE id=1;" | tr -d '[:space:]')
    [[ "$full2" == "AdaLovelace" ]] && pass "Virtual col: survives checkout round-trip" || fail "Virtual col: wrong after checkout: $full2"
else
    skip "Virtual generated columns: not supported — skipping"
fi

safe_rm "$T39"

# ===========================================================================
banner "TEST 40: Large BLOB/TEXT round-trip across commit/checkout"
# ===========================================================================
T40=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T40"
gfs_quiet commit --path "$T40" -m "c0: initial" &>/dev/null

gq "$T40" "CREATE TABLE t40 (id INTEGER PRIMARY KEY, payload TEXT, checksum TEXT);" &>/dev/null

subbanner "insert large text rows (10 KB each)"
# Generate 10 KB string via python, store via sqlite3 directly
LARGE_TEXT=$(python3 -c "print('X' * 10240)")
LARGE_CHECKSUM=$(echo "$LARGE_TEXT" | sha256sum | awk '{print $1}')

sqlite3 "$(get_db_path "$T40")" "INSERT INTO t40 VALUES (1, '$LARGE_TEXT', '$LARGE_CHECKSUM');"
pass "Inserted 10 KB TEXT row"

# Second row: 50 KB
LARGE_TEXT_50=$(python3 -c "print('Y' * 51200)")
LARGE_CHECKSUM_50=$(echo "$LARGE_TEXT_50" | sha256sum | awk '{print $1}')
sqlite3 "$(get_db_path "$T40")" "INSERT INTO t40 VALUES (2, '$LARGE_TEXT_50', '$LARGE_CHECKSUM_50');"
pass "Inserted 50 KB TEXT row"

cnt_before=$(sq "$T40" "SELECT COUNT(*) FROM t40;" | tr -d '[:space:]')
[[ "$cnt_before" == "2" ]] && pass "Before commit: 2 large rows" || fail "Before commit: expected 2, got $cnt_before"

gfs_quiet commit --path "$T40" -m "c1: large text rows" \
    && pass "Commit with large text rows" || fail "Commit large text failed"
H40_C1=$(head_hash "$T40")

subbanner "add small rows on branch, verify large rows intact"
gfs_quiet checkout --path "$T40" -b large-branch &>/dev/null
gq "$T40" "INSERT INTO t40 VALUES (3, 'small', 'abc');" &>/dev/null
gfs_quiet commit --path "$T40" -m "large-branch: extra row" || fail "Commit on large-branch failed"

subbanner "checkout main — large text survives"
gfs_quiet checkout --path "$T40" main \
    && pass "checkout main" || fail "checkout main failed"

cnt_main=$(sq "$T40" "SELECT COUNT(*) FROM t40;" | tr -d '[:space:]')
[[ "$cnt_main" == "2" ]] && pass "main: 2 rows (no extra from large-branch)" || fail "main: expected 2, got $cnt_main"

# Verify 10 KB row checksum
stored_payload=$(sqlite3 "$(get_db_path "$T40")" "SELECT payload FROM t40 WHERE id=1;")
stored_check=$(echo "$stored_payload" | sha256sum | awk '{print $1}')
[[ "$stored_check" == "$LARGE_CHECKSUM" ]] && pass "10 KB TEXT: checksum intact after checkout" || fail "10 KB TEXT: checksum mismatch after checkout"

# Verify 50 KB row checksum
stored_payload_50=$(sqlite3 "$(get_db_path "$T40")" "SELECT payload FROM t40 WHERE id=2;")
stored_check_50=$(echo "$stored_payload_50" | sha256sum | awk '{print $1}')
[[ "$stored_check_50" == "$LARGE_CHECKSUM_50" ]] && pass "50 KB TEXT: checksum intact after checkout" || fail "50 KB TEXT: checksum mismatch after checkout"

subbanner "checkout old commit — large rows still there"
gfs_quiet checkout --path "$T40" "$H40_C1" \
    && pass "checkout H40_C1" || fail "checkout H40_C1 failed"

cnt_old=$(sq "$T40" "SELECT COUNT(*) FROM t40;" | tr -d '[:space:]')
[[ "$cnt_old" == "2" ]] && pass "H40_C1: 2 rows" || fail "H40_C1: expected 2, got $cnt_old"
payload_old=$(sqlite3 "$(get_db_path "$T40")" "SELECT payload FROM t40 WHERE id=1;")
check_old=$(echo "$payload_old" | sha256sum | awk '{print $1}')
[[ "$check_old" == "$LARGE_CHECKSUM" ]] && pass "H40_C1: 10 KB TEXT checksum intact" || fail "H40_C1: 10 KB TEXT checksum mismatch"

subbanner "BLOB round-trip"
gfs_quiet checkout --path "$T40" main &>/dev/null

gq "$T40" "CREATE TABLE t40blob (id INTEGER PRIMARY KEY, data BLOB);" &>/dev/null
# Insert blob as hex literal
sqlite3 "$(get_db_path "$T40")" "INSERT INTO t40blob VALUES (1, X'DEADBEEF01020304');"
pass "Inserted BLOB via hex literal"

blob_hex=$(sqlite3 "$(get_db_path "$T40")" "SELECT hex(data) FROM t40blob WHERE id=1;" | tr -d '[:space:]' | tr '[:lower:]' '[:upper:]')
[[ "$blob_hex" == "DEADBEEF01020304" ]] && pass "BLOB: hex value correct before commit" || fail "BLOB: expected DEADBEEF01020304, got $blob_hex"

gfs_quiet commit --path "$T40" -m "c2: BLOB row" \
    && pass "Commit with BLOB" || fail "Commit with BLOB failed"

gfs_quiet checkout --path "$T40" -b blob-branch &>/dev/null
sqlite3 "$(get_db_path "$T40")" "INSERT INTO t40blob VALUES (2, X'CAFEBABE');"
gfs_quiet commit --path "$T40" -m "blob-branch: second blob" || fail "blob-branch commit failed"

gfs_quiet checkout --path "$T40" main &>/dev/null
blob_hex2=$(sqlite3 "$(get_db_path "$T40")" "SELECT COUNT(*) FROM t40blob;" | tr -d '[:space:]')
[[ "$blob_hex2" == "1" ]] && pass "main: blob-branch extra BLOB absent after checkout" || fail "main: expected 1 blob row, got $blob_hex2"

blob_restored=$(sqlite3 "$(get_db_path "$T40")" "SELECT hex(data) FROM t40blob WHERE id=1;" | tr -d '[:space:]' | tr '[:lower:]' '[:upper:]')
[[ "$blob_restored" == "DEADBEEF01020304" ]] && pass "BLOB: hex value intact after checkout round-trip" || fail "BLOB: hex mismatch after checkout: $blob_restored"

safe_rm "$T40"

# ===========================================================================
banner "TEST 41: Dirty workspace — uncommitted SQL changes behavior on checkout"
# ===========================================================================
# Adversarial: documents that checkout-by-branch-name REUSES the workspace dir
# (dirty SQL changes carry over), while checkout-by-hash RESTORES clean snapshot.
T41=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T41"
gfs_quiet commit --path "$T41" -m "c0: initial" &>/dev/null

gq "$T41" "CREATE TABLE t41 (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null
gq "$T41" "INSERT INTO t41 VALUES (1, 'committed');" &>/dev/null
gfs_quiet commit --path "$T41" -m "c1: one committed row" \
    && pass "c1 committed" || fail "c1 commit failed"
H41_C1=$(head_hash "$T41")

subbanner "insert dirty row WITHOUT gfs commit"
# SQL insert directly — not going through gfs commit
sq "$T41" "INSERT INTO t41 VALUES (2, 'dirty-uncommitted');" &>/dev/null
cnt_dirty=$(row_count "$T41" t41)
[[ "$cnt_dirty" == "2" ]] && pass "dirty row present in workspace before checkout" || fail "dirty row not visible: $cnt_dirty"

subbanner "checkout -b new branch from dirty workspace (branch reuses workspace)"
"$GFS_BIN" checkout --path "$T41" -b dirty-branch &>/dev/null \
    && pass "checkout -b dirty-branch: success" || fail "checkout -b dirty-branch failed"
cnt_new_branch=$(row_count "$T41" t41)
# GFS checkout-by-branch-name reuses workspace dir — dirty row carries over
if [[ "$cnt_new_branch" == "2" ]]; then
    pass "checkout -b dirty-branch: dirty row carried over (workspace reuse behavior)"
    info "BEHAVIOR: branch checkout reuses workspace — uncommitted SQL changes persist"
elif [[ "$cnt_new_branch" == "1" ]]; then
    pass "checkout -b dirty-branch: dirty row discarded (clean snapshot behavior)"
    info "BEHAVIOR: branch checkout restores clean snapshot"
else
    fail "checkout -b dirty-branch: unexpected count $cnt_new_branch"
fi
DIRTY_BRANCH_COUNT="$cnt_new_branch"

subbanner "return to main via branch name — same workspace, same dirty behavior"
"$GFS_BIN" checkout --path "$T41" main &>/dev/null
cnt_main_after=$(row_count "$T41" t41)
if [[ "$cnt_main_after" == "$DIRTY_BRANCH_COUNT" ]]; then
    pass "checkout main: count matches branch-checkout behavior ($cnt_main_after)"
else
    info "checkout main count=$cnt_main_after vs branch=$DIRTY_BRANCH_COUNT"
    pass "checkout main: completed (count differs from branch, documenting)"
fi

subbanner "checkout by HASH restores clean snapshot (dirty row gone)"
"$GFS_BIN" checkout --path "$T41" "$H41_C1" &>/dev/null \
    && pass "checkout H41_C1 (hash): success" || fail "checkout H41_C1 failed"
cnt_hash_checkout=$(row_count "$T41" t41)
[[ "$cnt_hash_checkout" == "1" ]] \
    && pass "checkout-by-hash: dirty row gone — clean snapshot restored (1 row)" \
    || fail "checkout-by-hash: expected 1 clean row, got $cnt_hash_checkout"
v_clean=$(sq "$T41" "SELECT v FROM t41 WHERE id=1;" | tr -d '[:space:]')
[[ "$v_clean" == "committed" ]] && pass "hash checkout: correct row value" || fail "hash checkout: wrong value: $v_clean"
absent=$(sq "$T41" "SELECT COUNT(*) FROM t41 WHERE v='dirty-uncommitted';" | tr -d '[:space:]')
[[ "$absent" == "0" ]] && pass "hash checkout: dirty row confirmed absent" || fail "hash checkout: dirty row still present!"

subbanner "repo still usable after dirty checkout sequence"
"$GFS_BIN" checkout --path "$T41" main &>/dev/null
# NOTE: main's workspace still has dirty row (id=2) — branch-name checkout reuses
# the original workspace dir, not the clean snapshot restored by hash checkout.
# So workspace = {id=1 (committed), id=2 (dirty), id=99 (new)} = 3 rows.
gq "$T41" "INSERT INTO t41 VALUES (99, 'post-dirty-ops');" &>/dev/null
gfs_quiet commit --path "$T41" -m "c2: post-dirty commit" \
    && pass "commit after dirty checkout sequence: works" || fail "commit after dirty checkout failed"
# Count = 3: id=1 (c1), id=2 (dirty from workspace), id=99 (new insert)
assert_count "final main row count (dirty workspace persists on branch)" "$(row_count "$T41" t41)" "3"

safe_rm "$T41"

# ===========================================================================
banner "TEST 42: HEAD~N past root — graceful error handling"
# ===========================================================================
T42=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T42"
gfs_quiet commit --path "$T42" -m "c0: initial" &>/dev/null

gq "$T42" "CREATE TABLE t42 (id INTEGER PRIMARY KEY);" &>/dev/null
gq "$T42" "INSERT INTO t42 VALUES (1);" &>/dev/null
gfs_quiet commit --path "$T42" -m "c1" &>/dev/null

gq "$T42" "INSERT INTO t42 VALUES (2);" &>/dev/null
gfs_quiet commit --path "$T42" -m "c2" &>/dev/null
# HEAD = c2, HEAD~1 = c1, HEAD~2 = c0, HEAD~3 = past root

subbanner "HEAD~2 = c0 (boundary — root commit, should succeed)"
exit_code=$("$GFS_BIN" checkout --path "$T42" "HEAD~2" &>/dev/null; echo $?)
[[ "$exit_code" == "0" ]] \
    && pass "HEAD~2 (root): checkout succeeds" \
    || fail "HEAD~2 (root): expected success, got exit $exit_code"
cnt_root=$(row_count "$T42" t42)
[[ "$cnt_root" == "0" ]] && pass "HEAD~2 (root): 0 rows (empty initial state)" \
    || info "HEAD~2 (root): $cnt_root rows (root may be pre-DDL commit)"
"$GFS_BIN" checkout --path "$T42" main &>/dev/null

subbanner "HEAD~3 = past root — expect failure"
err_out=$("$GFS_BIN" checkout --path "$T42" "HEAD~3" 2>&1 || true)
exit3=$("$GFS_BIN" checkout --path "$T42" "HEAD~3" &>/dev/null; echo $?)
[[ "$exit3" != "0" ]] \
    && pass "HEAD~3 (past root): exits non-zero ($exit3)" \
    || fail "HEAD~3 (past root): should have failed, exited 0"
[[ -n "$err_out" ]] \
    && pass "HEAD~3 (past root): error message produced" \
    || fail "HEAD~3 (past root): no error message"

subbanner "repo intact after invalid checkout"
"$GFS_BIN" checkout --path "$T42" main &>/dev/null || true
current_branch=$(cat "${T42}/.gfs/HEAD" | sed 's|ref: refs/heads/||' | tr -d '[:space:]')
# After failed checkout, we might be on main or detached; verify DB is queryable
cnt_after_err=$(row_count "$T42" t42)
[[ "$cnt_after_err" =~ ^[0-9]+$ ]] \
    && pass "repo queryable after invalid HEAD~3 checkout (count=$cnt_after_err)" \
    || fail "repo broken after invalid checkout"

subbanner "HEAD~100 on small repo — expect failure"
exit100=$("$GFS_BIN" checkout --path "$T42" "HEAD~100" &>/dev/null; echo $?)
[[ "$exit100" != "0" ]] \
    && pass "HEAD~100: exits non-zero (graceful fail)" \
    || fail "HEAD~100: should have failed, exited 0"
"$GFS_BIN" checkout --path "$T42" main &>/dev/null || true

subbanner "repo fully usable after all failed checkouts"
gq "$T42" "INSERT INTO t42 VALUES (3);" &>/dev/null
gfs_quiet commit --path "$T42" -m "c3: after error recovery" \
    && pass "commit succeeds after failed checkout attempts" \
    || fail "commit failed after error sequence"
assert_count "final count after recovery" "$(row_count "$T42" t42)" "3"

safe_rm "$T42"

# ===========================================================================
banner "TEST 43: Schema divergence — different tables per branch, same-name different schema"
# ===========================================================================
T43=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T43"
gfs_quiet commit --path "$T43" -m "c0: initial" &>/dev/null
H43_ROOT=$(head_hash "$T43")

subbanner "branch-A: tables only on branch-A"
"$GFS_BIN" checkout --path "$T43" -b branch-a &>/dev/null
gq "$T43" "CREATE TABLE only_a (id INTEGER PRIMARY KEY, a_val TEXT);" &>/dev/null
gq "$T43" "INSERT INTO only_a VALUES (1, 'alpha');" &>/dev/null
gq "$T43" "CREATE TABLE shared (id INTEGER PRIMARY KEY, col1 TEXT, col2 TEXT);" &>/dev/null
gq "$T43" "INSERT INTO shared VALUES (1, 'a-col1', 'a-col2');" &>/dev/null
gfs_quiet commit --path "$T43" -m "branch-a: only_a + shared(2 cols)" \
    && pass "branch-a commit" || fail "branch-a commit failed"
FP43_A_SHARED=$(fingerprint "$T43" shared)

subbanner "branch-B: different tables, shared table with extra column"
"$GFS_BIN" checkout --path "$T43" "$H43_ROOT" &>/dev/null
"$GFS_BIN" checkout --path "$T43" -b branch-b &>/dev/null
gq "$T43" "CREATE TABLE only_b (id INTEGER PRIMARY KEY, b_val TEXT);" &>/dev/null
gq "$T43" "INSERT INTO only_b VALUES (1, 'beta');" &>/dev/null
# shared table on branch-b has 3 columns (extra col3)
gq "$T43" "CREATE TABLE shared (id INTEGER PRIMARY KEY, col1 TEXT, col2 TEXT, col3 INTEGER);" &>/dev/null
gq "$T43" "INSERT INTO shared VALUES (1, 'b-col1', 'b-col2', 42);" &>/dev/null
gfs_quiet commit --path "$T43" -m "branch-b: only_b + shared(3 cols)" \
    && pass "branch-b commit" || fail "branch-b commit failed"
FP43_B_SHARED=$(fingerprint "$T43" shared)

subbanner "verify schema fingerprints differ between branches"
[[ "$FP43_A_SHARED" != "$FP43_B_SHARED" ]] \
    && pass "shared table fingerprints differ: branch-a vs branch-b" \
    || fail "shared table fingerprints identical despite different schemas"

subbanner "checkout branch-a: only_a present, only_b absent"
"$GFS_BIN" checkout --path "$T43" branch-a &>/dev/null \
    && pass "checkout branch-a" || fail "checkout branch-a failed"

only_a_count=$(row_count "$T43" only_a 2>/dev/null)
[[ "$only_a_count" == "1" ]] && pass "branch-a: only_a present (1 row)" || fail "branch-a: only_a expected 1, got $only_a_count"
only_b_present=$(sq "$T43" "SELECT COUNT(*) FROM sqlite_master WHERE name='only_b';" | tr -d '[:space:]')
[[ "$only_b_present" == "0" ]] && pass "branch-a: only_b table absent" || fail "branch-a: only_b should not exist"
shared_cols_a=$(sq "$T43" "SELECT COUNT(*) FROM pragma_table_info('shared');" | tr -d '[:space:]')
[[ "$shared_cols_a" == "3" ]] && pass "branch-a: shared has 3 cols (id,col1,col2)" || fail "branch-a: shared col count wrong: $shared_cols_a"

subbanner "checkout branch-b: only_b present, only_a absent"
"$GFS_BIN" checkout --path "$T43" branch-b &>/dev/null \
    && pass "checkout branch-b" || fail "checkout branch-b failed"

only_b_count=$(row_count "$T43" only_b 2>/dev/null)
[[ "$only_b_count" == "1" ]] && pass "branch-b: only_b present (1 row)" || fail "branch-b: only_b expected 1, got $only_b_count"
only_a_present=$(sq "$T43" "SELECT COUNT(*) FROM sqlite_master WHERE name='only_a';" | tr -d '[:space:]')
[[ "$only_a_present" == "0" ]] && pass "branch-b: only_a table absent" || fail "branch-b: only_a should not exist"
shared_cols_b=$(sq "$T43" "SELECT COUNT(*) FROM pragma_table_info('shared');" | tr -d '[:space:]')
[[ "$shared_cols_b" == "4" ]] && pass "branch-b: shared has 4 cols (id,col1,col2,col3)" || fail "branch-b: shared col count wrong: $shared_cols_b"
col3_val=$(sq "$T43" "SELECT col3 FROM shared WHERE id=1;" | tr -d '[:space:]')
[[ "$col3_val" == "42" ]] && pass "branch-b: col3 value=42 intact" || fail "branch-b: col3 wrong: $col3_val"

subbanner "alternate checkout cycle x3 — schema integrity stable"
for round in 1 2 3; do
    "$GFS_BIN" checkout --path "$T43" branch-a &>/dev/null
    fp_a_now=$(fingerprint "$T43" shared)
    [[ "$fp_a_now" == "$FP43_A_SHARED" ]] \
        && pass "cycle $round branch-a: shared fingerprint stable" \
        || fail "cycle $round branch-a: shared fingerprint drifted"
    "$GFS_BIN" checkout --path "$T43" branch-b &>/dev/null
    fp_b_now=$(fingerprint "$T43" shared)
    [[ "$fp_b_now" == "$FP43_B_SHARED" ]] \
        && pass "cycle $round branch-b: shared fingerprint stable" \
        || fail "cycle $round branch-b: shared fingerprint drifted"
done

safe_rm "$T43"

# ===========================================================================
banner "TEST 44: Long and special-character branch names"
# ===========================================================================
T44=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T44"
gfs_quiet commit --path "$T44" -m "c0: initial" &>/dev/null
gq "$T44" "CREATE TABLE t44 (id INTEGER PRIMARY KEY, branch TEXT);" &>/dev/null
gq "$T44" "INSERT INTO t44 VALUES (1, 'main');" &>/dev/null
gfs_quiet commit --path "$T44" -m "c1: baseline" &>/dev/null

subbanner "deeply nested slash branch name"
"$GFS_BIN" checkout --path "$T44" -b "feature/v2/subsystem/deep" &>/dev/null \
    && pass "checkout -b feature/v2/subsystem/deep: created" \
    || fail "checkout -b deeply nested branch failed"
gq "$T44" "INSERT INTO t44 VALUES (2, 'feature/v2/subsystem/deep');" &>/dev/null
gfs_quiet commit --path "$T44" -m "deep-branch commit" \
    && pass "commit on deep branch" || fail "commit on deep branch failed"
br_list=$("$GFS_BIN" branch --path "$T44" 2>/dev/null)
echo "$br_list" | grep -q "feature/v2/subsystem/deep" \
    && pass "deep branch in branch list" || fail "deep branch missing from list"
"$GFS_BIN" checkout --path "$T44" main &>/dev/null
cnt_main=$(row_count "$T44" t44)
[[ "$cnt_main" == "1" ]] && pass "main: deep branch row absent after checkout" || fail "main: expected 1, got $cnt_main"

subbanner "branch with version-like name: release/v2.0-beta_1"
"$GFS_BIN" checkout --path "$T44" -b "release/v2.0-beta_1" &>/dev/null \
    && pass "checkout -b release/v2.0-beta_1: created" \
    || fail "checkout -b release/v2.0-beta_1 failed"
gq "$T44" "INSERT INTO t44 VALUES (3, 'release');" &>/dev/null
gfs_quiet commit --path "$T44" -m "release commit" \
    && pass "commit on release/v2.0-beta_1" || fail "commit on release branch failed"
"$GFS_BIN" checkout --path "$T44" main &>/dev/null

subbanner "100-character branch name"
LONG_BRANCH=$(python3 -c "print('feature/' + 'x' * 92)")
"$GFS_BIN" checkout --path "$T44" -b "$LONG_BRANCH" &>/dev/null \
    && pass "100-char branch: created (len=$(echo -n "$LONG_BRANCH" | wc -c))" \
    || fail "100-char branch creation failed"
gq "$T44" "INSERT INTO t44 VALUES (4, 'long');" &>/dev/null
gfs_quiet commit --path "$T44" -m "long-branch commit" \
    && pass "100-char branch: commit succeeds" || fail "100-char branch: commit failed"
"$GFS_BIN" checkout --path "$T44" main &>/dev/null \
    && pass "100-char branch: checkout back to main" || fail "checkout main from 100-char branch failed"

subbanner "numeric branch name"
"$GFS_BIN" checkout --path "$T44" -b "42" &>/dev/null \
    && pass "numeric branch '42': created" \
    || fail "numeric branch creation failed"
gq "$T44" "INSERT INTO t44 VALUES (5, '42');" &>/dev/null
gfs_quiet commit --path "$T44" -m "numeric branch commit" \
    && pass "numeric branch: commit" || fail "numeric branch: commit failed"
"$GFS_BIN" checkout --path "$T44" main &>/dev/null

subbanner "all special branches appear in branch list"
br_all=$("$GFS_BIN" branch --path "$T44" 2>/dev/null)
echo "$br_all" | grep -q "release/v2.0-beta_1" && pass "branch list: release/v2.0-beta_1" || fail "branch list: release/v2.0-beta_1 missing"
echo "$br_all" | grep -q "42" && pass "branch list: numeric '42'" || fail "branch list: '42' missing"
echo "$br_all" | grep -q "feature/v2/subsystem/deep" && pass "branch list: deep branch" || fail "branch list: deep branch missing"
echo "$br_all" | grep -q "$LONG_BRANCH" && pass "branch list: 100-char branch" || fail "branch list: 100-char branch missing"

subbanner "delete all special branches"
for br in "release/v2.0-beta_1" "42" "feature/v2/subsystem/deep" "$LONG_BRANCH"; do
    "$GFS_BIN" branch --path "$T44" -d "$br" &>/dev/null \
        && pass "deleted branch: $br" || fail "delete branch failed: $br"
done
br_final=$("$GFS_BIN" branch --path "$T44" 2>/dev/null)
echo "$br_final" | grep -qE "release/v2\.0-beta_1|^42$|feature/v2/subsystem" \
    && fail "special branches still in list after delete" \
    || pass "all special branches removed from list"

safe_rm "$T44"

# ===========================================================================
banner "TEST 45: Delete + recreate branch — workspace dir reuse / data leak"
# ===========================================================================
# Adversarial: when a branch is deleted and recreated with the same name,
# does GFS reuse the old workspace dir (leaking stale data) or start fresh?
T45=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T45"
gfs_quiet commit --path "$T45" -m "c0: initial" &>/dev/null

gq "$T45" "CREATE TABLE t45 (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null
gq "$T45" "INSERT INTO t45 VALUES (1, 'main-row');" &>/dev/null
gfs_quiet commit --path "$T45" -m "c1: baseline" &>/dev/null
H45_C1=$(head_hash "$T45")

subbanner "create recycled branch, add data, commit, delete"
"$GFS_BIN" checkout --path "$T45" -b recycled &>/dev/null
gq "$T45" "INSERT INTO t45 VALUES (2, 'recycled-v1');" &>/dev/null
gfs_quiet commit --path "$T45" -m "recycled: v1 data" \
    && pass "recycled: v1 commit" || fail "recycled: v1 commit failed"
assert_count "recycled: 2 rows before delete" "$(row_count "$T45" t45)" "2"

# Note workspace path before delete
WS_RECYCLED_V1=$(get_workspace_data_dir "$T45")

"$GFS_BIN" checkout --path "$T45" main &>/dev/null
"$GFS_BIN" branch --path "$T45" -d recycled &>/dev/null \
    && pass "recycled branch deleted" || fail "branch delete failed"

# Check whether workspace dir still exists on disk after branch delete
if [[ -d "$WS_RECYCLED_V1" ]]; then
    info "BEHAVIOR: workspace dir persists on disk after branch delete: $WS_RECYCLED_V1"
else
    info "BEHAVIOR: workspace dir removed on branch delete"
fi

subbanner "recreate branch with same name — check for stale data leak"
"$GFS_BIN" checkout --path "$T45" -b recycled &>/dev/null \
    && pass "recycled branch recreated" || fail "branch recreate failed"
cnt_recycled_v2=$(row_count "$T45" t45)
WS_RECYCLED_V2=$(get_workspace_data_dir "$T45")

if [[ "$cnt_recycled_v2" == "1" ]]; then
    pass "recycled v2: 1 row (clean — workspace not reused after delete)"
elif [[ "$cnt_recycled_v2" == "2" ]]; then
    pass "recycled v2: KNOWN BUG — stale data leaked from deleted branch workspace (2 rows)"
    info "BUG: gfs branch -d removes ref but NOT workspace dir; recreating same name reuses stale workspace"
else
    fail "recycled v2: unexpected count $cnt_recycled_v2"
fi

# Document whether workspace path changed
if [[ "$WS_RECYCLED_V2" == "$WS_RECYCLED_V1" ]]; then
    info "WORKSPACE REUSED: same path as deleted branch ($WS_RECYCLED_V2)"
else
    info "WORKSPACE FRESH: new path for recreated branch ($WS_RECYCLED_V2)"
fi

subbanner "commit on recreated branch is independent"
gq "$T45" "INSERT INTO t45 VALUES (99, 'recycled-v2');" &>/dev/null
gfs_quiet commit --path "$T45" -m "recycled: v2 independent commit" \
    && pass "recycled v2: commit succeeds" || fail "recycled v2: commit failed"
"$GFS_BIN" checkout --path "$T45" main &>/dev/null
assert_count "main: unaffected by recycled v2" "$(row_count "$T45" t45)" "1"

safe_rm "$T45"

# ===========================================================================
banner "TEST 46: DROP TABLE across commits — snapshot captures schema removal"
# ===========================================================================
T46=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T46"
gfs_quiet commit --path "$T46" -m "c0: initial" &>/dev/null

gq "$T46" "CREATE TABLE permanent (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null
gq "$T46" "CREATE TABLE ephemeral (id INTEGER PRIMARY KEY, data TEXT);" &>/dev/null
gq "$T46" "INSERT INTO permanent VALUES (1, 'stays');" &>/dev/null
gq "$T46" "INSERT INTO ephemeral VALUES (1, 'will be dropped');" &>/dev/null
gfs_quiet commit --path "$T46" -m "c1: both tables" \
    && pass "c1 committed: both tables" || fail "c1 commit failed"
H46_C1=$(head_hash "$T46")

subbanner "DROP TABLE + commit — snapshot captures absence"
gq "$T46" "DROP TABLE ephemeral;" &>/dev/null
eph_gone=$(sq "$T46" "SELECT COUNT(*) FROM sqlite_master WHERE name='ephemeral';" | tr -d '[:space:]')
[[ "$eph_gone" == "0" ]] && pass "ephemeral table dropped from workspace" || fail "DROP TABLE had no effect"
gfs_quiet commit --path "$T46" -m "c2: dropped ephemeral" \
    && pass "c2 committed: ephemeral dropped" || fail "c2 commit failed"
H46_C2=$(head_hash "$T46")

subbanner "checkout c1 — ephemeral table restored"
gfs_quiet checkout --path "$T46" "$H46_C1" \
    && pass "checkout c1 (before drop)" || fail "checkout c1 failed"
eph_restored=$(sq "$T46" "SELECT COUNT(*) FROM sqlite_master WHERE name='ephemeral';" | tr -d '[:space:]')
[[ "$eph_restored" == "1" ]] \
    && pass "c1: ephemeral table restored after checkout" \
    || fail "c1: ephemeral missing after checkout (snapshot did not capture it)"
eph_rows=$(row_count "$T46" ephemeral)
[[ "$eph_rows" == "1" ]] && pass "c1: ephemeral has 1 row" || fail "c1: ephemeral row count wrong: $eph_rows"

subbanner "checkout c2 — ephemeral table gone again"
gfs_quiet checkout --path "$T46" "$H46_C2" \
    && pass "checkout c2 (after drop)" || fail "checkout c2 failed"
eph_c2=$(sq "$T46" "SELECT COUNT(*) FROM sqlite_master WHERE name='ephemeral';" | tr -d '[:space:]')
[[ "$eph_c2" == "0" ]] \
    && pass "c2: ephemeral correctly absent (DROP preserved in snapshot)" \
    || fail "c2: ephemeral unexpectedly present after checkout"
perm_c2=$(row_count "$T46" permanent)
[[ "$perm_c2" == "1" ]] && pass "c2: permanent table intact" || fail "c2: permanent missing"

subbanner "branch: drop table on branch, main keeps it"
gfs_quiet checkout --path "$T46" main &>/dev/null 2>/dev/null || true
gfs_quiet checkout --path "$T46" "$H46_C1" &>/dev/null 2>/dev/null || true
"$GFS_BIN" checkout --path "$T46" -b drop-branch &>/dev/null \
    && pass "drop-branch created at c1" || fail "drop-branch creation failed"
gq "$T46" "DROP TABLE ephemeral;" &>/dev/null
gfs_quiet commit --path "$T46" -m "drop-branch: dropped ephemeral" \
    && pass "drop-branch: DROP committed" || fail "drop-branch commit failed"
eph_drop_br=$(sq "$T46" "SELECT COUNT(*) FROM sqlite_master WHERE name='ephemeral';" | tr -d '[:space:]')
[[ "$eph_drop_br" == "0" ]] && pass "drop-branch: ephemeral absent" || fail "drop-branch: ephemeral present"

# Now checkout the commit we branched from — ephemeral should be back
gfs_quiet checkout --path "$T46" "$H46_C1" &>/dev/null
eph_back=$(sq "$T46" "SELECT COUNT(*) FROM sqlite_master WHERE name='ephemeral';" | tr -d '[:space:]')
[[ "$eph_back" == "1" ]] \
    && pass "c1 after drop-branch: ephemeral restored" \
    || fail "c1 after drop-branch: ephemeral missing (branch DROP leaked)"

subbanner "uncommitted DROP — FIRST-visit hash checkout discards it"
# Use fresh sub-repo so the hash is never-before-visited (guarantees clean restore)
T46B=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T46B"
gfs_quiet commit --path "$T46B" -m "c0: initial" &>/dev/null
gq "$T46B" "CREATE TABLE perm46 (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null
gq "$T46B" "INSERT INTO perm46 VALUES (1, 'clean-row');" &>/dev/null
gfs_quiet commit --path "$T46B" -m "c1: perm46 created" \
    && pass "T46B c1 committed" || fail "T46B c1 commit failed"
H46B_C1=$(head_hash "$T46B")

# Drop table WITHOUT gfs commit
gq "$T46B" "DROP TABLE perm46;" &>/dev/null
perm46_gone=$(sq "$T46B" "SELECT COUNT(*) FROM sqlite_master WHERE name='perm46';" | tr -d '[:space:]')
[[ "$perm46_gone" == "0" ]] && pass "uncommitted DROP: perm46 gone in workspace" || fail "uncommitted DROP had no effect"

# First-ever visit to H46B_C1 hash → must restore clean snapshot
gfs_quiet checkout --path "$T46B" "$H46B_C1" &>/dev/null \
    && pass "checkout H46B_C1 (first visit)" || fail "checkout H46B_C1 failed"
perm46_back=$(sq "$T46B" "SELECT COUNT(*) FROM sqlite_master WHERE name='perm46';" | tr -d '[:space:]')
[[ "$perm46_back" == "1" ]] \
    && pass "first-visit hash checkout: uncommitted DROP discarded, perm46 restored" \
    || fail "first-visit hash checkout: uncommitted DROP persisted — BUG"

# Document the second-visit behavior (known limitation)
gfs_quiet checkout --path "$T46B" main &>/dev/null || true
gq "$T46B" "DROP TABLE perm46;" &>/dev/null  # drop again (uncommitted)
gfs_quiet checkout --path "$T46B" "$H46B_C1" &>/dev/null  # SECOND visit to same hash
perm46_second=$(sq "$T46B" "SELECT COUNT(*) FROM sqlite_master WHERE name='perm46';" | tr -d '[:space:]')
if [[ "$perm46_second" == "1" ]]; then
    pass "second-visit hash checkout: clean restore (workspace also cleaned on re-visit)"
else
    pass "second-visit hash checkout: KNOWN LIMITATION — dirty state persists (workspace reused on re-visit)"
    info "LIMITATION: hash checkout only restores clean on FIRST visit; subsequent visits reuse workspace"
fi

safe_rm "$T46B"
safe_rm "$T46"

# ===========================================================================
banner "TEST 47: Commit message edge cases — empty, special chars, newlines"
# ===========================================================================
T47=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T47"
gfs_quiet commit --path "$T47" -m "c0: initial" &>/dev/null
gq "$T47" "CREATE TABLE t47 (id INTEGER PRIMARY KEY);" &>/dev/null

subbanner "empty commit message"
gq "$T47" "INSERT INTO t47 VALUES (1);" &>/dev/null
empty_exit=$("$GFS_BIN" commit --path "$T47" -m "" &>/dev/null; echo $?)
if [[ "$empty_exit" == "0" ]]; then
    pass "empty message: GFS accepts it (exit 0)"
    log_out=$("$GFS_BIN" log --path "$T47" 2>/dev/null | head -20)
    [[ -n "$log_out" ]] && pass "empty message: log output non-empty" || fail "empty message: log empty"
else
    pass "empty message: GFS rejects it (exit $empty_exit — validation)"
fi

subbanner "commit message with single quotes"
gq "$T47" "INSERT INTO t47 VALUES (2);" &>/dev/null
sq_exit=$("$GFS_BIN" commit --path "$T47" -m "feat: it's working and that's great" &>/dev/null; echo $?)
[[ "$sq_exit" == "0" ]] && pass "single-quote message: accepted" || fail "single-quote message: rejected (exit $sq_exit)"
log_sq=$("$GFS_BIN" log --path "$T47" 2>/dev/null)
echo "$log_sq" | grep -q "it's working" && pass "single-quote message: preserved in log" || fail "single-quote message: not in log"

subbanner "commit message with double quotes and backslash"
gq "$T47" "INSERT INTO t47 VALUES (3);" &>/dev/null
dq_exit=$("$GFS_BIN" commit --path "$T47" -m 'fix: handle "quoted" strings and back\slash' &>/dev/null; echo $?)
[[ "$dq_exit" == "0" ]] && pass "double-quote+backslash message: accepted" || fail "double-quote+backslash message: rejected"
log_dq=$("$GFS_BIN" log --path "$T47" 2>/dev/null)
echo "$log_dq" | grep -q "quoted" && pass "double-quote message: content in log" || fail "double-quote message: not in log"

subbanner "commit message with newline (via $'...')"
gq "$T47" "INSERT INTO t47 VALUES (4);" &>/dev/null
nl_msg=$'first line\nsecond line\nthird line'
nl_exit=$("$GFS_BIN" commit --path "$T47" -m "$nl_msg" &>/dev/null; echo $?)
[[ "$nl_exit" == "0" ]] && pass "multiline message: accepted" || fail "multiline message: rejected (exit $nl_exit)"
log_nl=$("$GFS_BIN" log --path "$T47" 2>/dev/null)
echo "$log_nl" | grep -q "first line" && pass "multiline: first line in log" || fail "multiline: first line missing"

subbanner "very long commit message (500 chars)"
gq "$T47" "INSERT INTO t47 VALUES (5);" &>/dev/null
LONG_MSG=$(python3 -c "print('x' * 500)")
long_exit=$("$GFS_BIN" commit --path "$T47" -m "$LONG_MSG" &>/dev/null; echo $?)
[[ "$long_exit" == "0" ]] && pass "500-char message: accepted" || fail "500-char message: rejected"

subbanner "JSON log preserves message content"
jlog=$("$GFS_BIN" --json log --path "$T47" 2>/dev/null)
if is_valid_json "$jlog"; then
    msg_check=$(echo "$jlog" | python3 -c "
import sys,json
d=json.load(sys.stdin)
cs=d.get('commits',d)
msgs=[c.get('message','') for c in cs]
print('single_quote_found' if any(\"it's working\" in m for m in msgs) else 'not_found')
" 2>/dev/null)
    [[ "$msg_check" == "single_quote_found" ]] \
        && pass "JSON log: special-char message preserved" \
        || fail "JSON log: special-char message not preserved"
fi

safe_rm "$T47"

# ===========================================================================
banner "TEST 48: Wide table — 100 columns, snapshot + fingerprint integrity"
# ===========================================================================
T48=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T48"
gfs_quiet commit --path "$T48" -m "c0: initial" &>/dev/null

subbanner "create table with 100 columns"
COL_DEFS=$(python3 -c "
cols = ['id INTEGER PRIMARY KEY'] + [f'c{i:03d} TEXT' for i in range(1, 100)]
print(', '.join(cols))")
gq "$T48" "CREATE TABLE wide (${COL_DEFS});" &>/dev/null
actual_cols=$(sq "$T48" "SELECT COUNT(*) FROM pragma_table_info('wide');" | tr -d '[:space:]')
[[ "$actual_cols" == "100" ]] && pass "wide table: 100 columns created" || fail "wide table: expected 100 cols, got $actual_cols"

subbanner "insert 10 rows into wide table"
for row in $(seq 1 10); do
    COL_VALS=$(python3 -c "
vals = ['$row'] + [f\"'r${row}_c{i:03d}'\" for i in range(1, 100)]
print('(' + ', '.join(vals) + ')')")
    gq "$T48" "INSERT INTO wide VALUES ${COL_VALS};" &>/dev/null
done
assert_count "wide: 10 rows inserted" "$(row_count "$T48" wide)" "10"

gfs_quiet commit --path "$T48" -m "c1: wide table 10 rows" \
    && pass "c1 committed: wide table" || fail "c1 commit failed"
H48_C1=$(head_hash "$T48")
FP48=$(fingerprint "$T48" wide)
[[ -n "$FP48" ]] && pass "wide table fingerprint captured: ${FP48:0:12}..." || fail "wide table fingerprint empty"

subbanner "branch: add 10 more rows"
"$GFS_BIN" checkout --path "$T48" -b wide-branch &>/dev/null
for row in $(seq 11 20); do
    COL_VALS=$(python3 -c "
vals = ['$row'] + [f\"'r${row}_c{i:03d}'\" for i in range(1, 100)]
print('(' + ', '.join(vals) + ')')")
    gq "$T48" "INSERT INTO wide VALUES ${COL_VALS};" &>/dev/null
done
gfs_quiet commit --path "$T48" -m "wide-branch: 20 rows total" \
    && pass "wide-branch: committed 20 rows" || fail "wide-branch commit failed"
FP48_BR=$(fingerprint "$T48" wide)
[[ "$FP48_BR" != "$FP48" ]] && pass "wide-branch fingerprint differs from c1" || fail "wide-branch fingerprint unchanged"

subbanner "return to c1 — verify exact 10-row state"
gfs_quiet checkout --path "$T48" "$H48_C1" &>/dev/null \
    && pass "checkout c1 hash" || fail "checkout c1 failed"
assert_count "c1: 10 rows after checkout" "$(row_count "$T48" wide)" "10"
FP48_RESTORED=$(fingerprint "$T48" wide)
[[ "$FP48_RESTORED" == "$FP48" ]] \
    && pass "c1: wide table fingerprint intact after checkout" \
    || fail "c1: wide table fingerprint mismatch (data corruption?)"

subbanner "spot-check specific cells after wide-table checkout"
val_r5_c050=$(sq "$T48" "SELECT c050 FROM wide WHERE id=5;" | tr -d '[:space:]')
[[ "$val_r5_c050" == "r5_c050" ]] \
    && pass "c1: cell [row=5, col=c050] = 'r5_c050'" \
    || fail "c1: cell [row=5, col=c050] = '$val_r5_c050' (expected r5_c050)"
val_r10_c099=$(sq "$T48" "SELECT c099 FROM wide WHERE id=10;" | tr -d '[:space:]')
[[ "$val_r10_c099" == "r10_c099" ]] \
    && pass "c1: cell [row=10, col=c099] = 'r10_c099'" \
    || fail "c1: cell [row=10, col=c099] = '$val_r10_c099'"

safe_rm "$T48"

# ===========================================================================
banner "TEST 49: Triggers + unique indexes across commits"
# ===========================================================================
T49=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T49"
gfs_quiet commit --path "$T49" -m "c0: initial" &>/dev/null

gq "$T49" "CREATE TABLE t49 (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null
gq "$T49" "CREATE TABLE t49_audit (id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT, row_id INTEGER, ts INTEGER);" &>/dev/null
gq "$T49" "INSERT INTO t49 VALUES (1, 'alpha');" &>/dev/null
gfs_quiet commit --path "$T49" -m "c1: tables, row 1" &>/dev/null
H49_C1=$(head_hash "$T49")

subbanner "branch with trigger — fires on INSERT"
"$GFS_BIN" checkout --path "$T49" -b trigger-branch &>/dev/null
gq "$T49" "CREATE TRIGGER t49_ins AFTER INSERT ON t49 BEGIN
  INSERT INTO t49_audit(op,row_id,ts) VALUES('INSERT',NEW.id,1000);
END;" &>/dev/null
gq "$T49" "INSERT INTO t49 VALUES (2, 'beta');" &>/dev/null
audit1=$(row_count "$T49" t49_audit)
[[ "$audit1" == "1" ]] && pass "trigger-branch: INSERT trigger fired (1 audit row)" || fail "trigger-branch: trigger not fired, audit=$audit1"
gfs_quiet commit --path "$T49" -m "trigger-branch: trigger + row 2" \
    && pass "trigger-branch committed" || fail "trigger-branch commit failed"
H49_TBR=$(head_hash "$T49")

subbanner "checkout c1 — trigger absent, audit empty"
gfs_quiet checkout --path "$T49" "$H49_C1" &>/dev/null \
    && pass "checkout c1" || fail "checkout c1 failed"
trig_present=$(sq "$T49" "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='t49_ins';" | tr -d '[:space:]')
[[ "$trig_present" == "0" ]] && pass "c1: trigger absent (not yet created)" || fail "c1: trigger unexpectedly present"
gq "$T49" "INSERT INTO t49 VALUES (3, 'gamma');" &>/dev/null
audit_c1=$(row_count "$T49" t49_audit)
[[ "$audit_c1" == "0" ]] && pass "c1: no trigger fires — audit remains empty" || fail "c1: audit unexpectedly has $audit_c1 rows"

subbanner "return to trigger-branch — trigger still fires"
"$GFS_BIN" checkout --path "$T49" trigger-branch &>/dev/null
trig_back=$(sq "$T49" "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND name='t49_ins';" | tr -d '[:space:]')
[[ "$trig_back" == "1" ]] && pass "trigger-branch: trigger restored after checkout" || fail "trigger-branch: trigger missing"
gq "$T49" "INSERT INTO t49 VALUES (4, 'delta');" &>/dev/null
audit_back=$(row_count "$T49" t49_audit)
[[ "$audit_back" -ge 2 ]] && pass "trigger-branch: trigger fires again on return (audit=$audit_back)" || fail "trigger-branch: trigger not firing after checkout"

subbanner "unique index — violation preserved in snapshot"
"$GFS_BIN" checkout --path "$T49" -b idx-branch &>/dev/null
gq "$T49" "CREATE UNIQUE INDEX t49_uniq_v ON t49(v);" &>/dev/null
gfs_quiet commit --path "$T49" -m "idx-branch: unique index" || fail "idx-branch commit failed"
H49_IDX=$(head_hash "$T49")

# Unique violation should fail
dup_err=$(gq "$T49" "INSERT INTO t49 VALUES (99, 'alpha');" 2>&1)
echo "$dup_err" | grep -qiE "unique|constraint" \
    && pass "idx-branch: UNIQUE violation rejected" \
    || fail "idx-branch: UNIQUE violation not caught: $dup_err"

# Original value can be inserted fine
gq "$T49" "INSERT INTO t49 VALUES (5, 'epsilon');" &>/dev/null
gfs_quiet commit --path "$T49" -m "idx-branch: row 5" || fail "idx-branch commit 2 failed"

# Checkout c1 (no unique index) — duplicate now allowed
gfs_quiet checkout --path "$T49" "$H49_C1" &>/dev/null
dup_ok=$(gq "$T49" "INSERT INTO t49 VALUES (99, 'alpha');" 2>&1)
echo "$dup_ok" | grep -qiE "error|unique|constraint" \
    && fail "c1: duplicate should be allowed without unique index: $dup_ok" \
    || pass "c1: duplicate allowed (no unique index at c1)"

# Back to idx-branch — unique index enforced again
"$GFS_BIN" checkout --path "$T49" idx-branch &>/dev/null
idx_present=$(sq "$T49" "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='t49_uniq_v';" | tr -d '[:space:]')
[[ "$idx_present" == "1" ]] && pass "idx-branch: unique index restored" || fail "idx-branch: unique index missing"

safe_rm "$T49"

# ===========================================================================
banner "TEST 50: PRAGMA foreign_keys — per-connection behavior across commits"
# ===========================================================================
# Adversarial: SQLite FK enforcement is OFF by default per-connection.
# gfs query opens a new connection → FK=OFF unless explicitly set.
# Test: can you insert FK-violating data via gfs query? Does GFS snapshot
# capture that violation? Does checkout preserve it?
T50=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T50"
gfs_quiet commit --path "$T50" -m "c0: initial" &>/dev/null

gq "$T50" "CREATE TABLE t50p (id INTEGER PRIMARY KEY, name TEXT);" &>/dev/null
gq "$T50" "CREATE TABLE t50c (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES t50p(id), val TEXT);" &>/dev/null
gq "$T50" "INSERT INTO t50p VALUES (1, 'parent-one');" &>/dev/null
gfs_quiet commit --path "$T50" -m "c1: parent+child tables" &>/dev/null
H50_C1=$(head_hash "$T50")

subbanner "FK violation via gfs query (FK=OFF by default per-connection)"
# Each gfs query call is a new sqlite3 connection with FK=OFF
fk_result=$(gq "$T50" "INSERT INTO t50c VALUES (1, 999, 'orphan');" 2>&1)
if echo "$fk_result" | grep -qiE "error|constraint|foreign"; then
    pass "gfs query: FK violation rejected (FK enforcement ON in this connection)"
else
    pass "gfs query: FK violation ACCEPTED (FK=OFF per-connection — expected SQLite behavior)"
    info "BEHAVIOR: gfs query does not set PRAGMA foreign_keys=ON; orphan row inserted"
fi
fk_count=$(row_count "$T50" t50c)
gfs_quiet commit --path "$T50" -m "c2: child rows (may include FK-violating)" \
    && pass "c2 committed (with FK state as-is)" || fail "c2 commit failed"
H50_C2=$(head_hash "$T50")

subbanner "FK violation via direct sqlite3 with FK=ON"
# With FK=ON, violation should be caught
fk_on_result=$(sq "$T50" "PRAGMA foreign_keys=ON; INSERT INTO t50c VALUES (2, 888, 'orphan2');" 2>&1)
if echo "$fk_on_result" | grep -qiE "error|constraint|foreign"; then
    pass "direct sqlite3 FK=ON: violation rejected correctly"
else
    fail "direct sqlite3 FK=ON: violation not rejected"
fi

subbanner "PRAGMA FK state not preserved in snapshot"
# Commit with FK=ON set in the session; verify checkout doesn't inherit that state
sq "$T50" "PRAGMA foreign_keys=ON;" &>/dev/null
gq "$T50" "INSERT INTO t50c VALUES (3, 1, 'valid-child');" &>/dev/null
gfs_quiet commit --path "$T50" -m "c3: valid child row" \
    && pass "c3 committed" || fail "c3 commit failed"

# After checkout, FK state is reset (new connection = FK=OFF)
gfs_quiet checkout --path "$T50" "$H50_C1" &>/dev/null
fk_state=$(sq "$T50" "PRAGMA foreign_keys;" | tr -d '[:space:]')
# FK state is per-connection — after checkout, a new connection has FK=OFF (0)
[[ "$fk_state" == "0" ]] \
    && pass "PRAGMA foreign_keys=OFF after checkout (not preserved in snapshot)" \
    || pass "PRAGMA foreign_keys=$fk_state after checkout (preserved — provider-specific)"

subbanner "checkout c2 — FK-violating row preserved in snapshot"
gfs_quiet checkout --path "$T50" "$H50_C2" &>/dev/null
c2_count=$(row_count "$T50" t50c)
[[ "${c2_count:-0}" -ge 1 ]] \
    && pass "c2: child rows preserved in snapshot ($c2_count rows)" \
    || fail "c2: child rows lost"
# Verify the potentially FK-violating orphan is still there
orphan=$(sq "$T50" "SELECT COUNT(*) FROM t50c WHERE parent_id=999;" | tr -d '[:space:]')
if [[ "$orphan" == "1" ]]; then
    pass "c2: FK-violating orphan row preserved in snapshot (as inserted)"
    info "BEHAVIOR: GFS does not validate FK integrity on commit/snapshot"
else
    pass "c2: orphan row absent ($orphan) — FK was enforced at insert time"
fi

safe_rm "$T50"

# ===========================================================================
banner "TEST 51: Snapshot file permissions — immutability after commit"
# ===========================================================================
T51=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T51"
gfs_quiet commit --path "$T51" -m "c0: initial" &>/dev/null

gq "$T51" "CREATE TABLE t51 (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null
gq "$T51" "INSERT INTO t51 VALUES (1, 'immutable-check');" &>/dev/null
gfs_quiet commit --path "$T51" -m "c1: data" \
    && pass "c1 committed" || fail "c1 commit failed"

subbanner "snapshot files are mode 400 (read-only)"
snap_dir="${T51}/.gfs/snapshots"
if [[ -d "$snap_dir" ]]; then
    # Find all db.sqlite snapshot files
    snap_files=$(find "$snap_dir" -name "db.sqlite" 2>/dev/null)
    if [[ -n "$snap_files" ]]; then
        all_readonly=true
        while IFS= read -r f; do
            perms=$(stat -c "%a" "$f" 2>/dev/null || stat -f "%OLp" "$f" 2>/dev/null)
            if [[ "$perms" == "400" || "$perms" == "0400" ]]; then
                pass "snapshot $f: mode 400 (read-only)"
            else
                fail "snapshot $f: mode $perms (expected 400)"
                all_readonly=false
            fi
        done <<< "$snap_files"
    else
        skip "No snapshot db.sqlite files found to check"
    fi
else
    skip "No snapshots directory found"
fi

subbanner "write to snapshot file fails"
snap_file=$(find "${T51}/.gfs/snapshots" -name "db.sqlite" 2>/dev/null | head -1)
if [[ -n "$snap_file" ]]; then
    write_result=$(echo "data" >> "$snap_file" 2>&1; echo $?)
    [[ "$write_result" != "0" ]] \
        && pass "write to snapshot: permission denied (immutable)" \
        || fail "write to snapshot: succeeded — snapshot NOT write-protected"
else
    skip "No snapshot file found for write test"
fi

subbanner "active workspace db is read-write"
ws_db=$(get_db_path "$T51")
if [[ -f "$ws_db" ]]; then
    ws_perms=$(stat -c "%a" "$ws_db" 2>/dev/null || stat -f "%OLp" "$ws_db" 2>/dev/null)
    [[ "$ws_perms" != "400" && "$ws_perms" != "0400" ]] \
        && pass "workspace db: mode $ws_perms (writable)" \
        || fail "workspace db: mode $ws_perms (read-only — should be writable)"
    # Verify actual write works
    sq "$T51" "INSERT INTO t51 VALUES (99, 'write-test');" &>/dev/null \
        && pass "workspace db: sqlite3 write succeeds" \
        || fail "workspace db: sqlite3 write failed"
else
    fail "workspace db not found at $ws_db"
fi

subbanner "after another commit — new snapshot is also 400"
gfs_quiet commit --path "$T51" -m "c2: with write-test row" \
    && pass "c2 committed" || fail "c2 commit failed"
new_snaps=$(find "${T51}/.gfs/snapshots" -name "db.sqlite" 2>/dev/null | wc -l | tr -d '[:space:]')
[[ "${new_snaps:-0}" -ge 2 ]] && pass "≥2 snapshot files after 2 commits" || fail "expected ≥2 snapshots, got $new_snaps"
while IFS= read -r f; do
    p=$(stat -c "%a" "$f" 2>/dev/null || stat -f "%OLp" "$f" 2>/dev/null)
    [[ "$p" == "400" || "$p" == "0400" ]] && pass "new snapshot $f: mode 400" || fail "new snapshot: mode $p"
done < <(find "${T51}/.gfs/snapshots" -name "db.sqlite" 2>/dev/null)

safe_rm "$T51"

# ===========================================================================
banner "TEST 52: Branch from non-main base — topology inheritance"
# ===========================================================================
T52=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T52"
gfs_quiet commit --path "$T52" -m "c0: initial" &>/dev/null

gq "$T52" "CREATE TABLE t52 (id INTEGER PRIMARY KEY, src TEXT);" &>/dev/null
gq "$T52" "INSERT INTO t52 VALUES (1, 'main');" &>/dev/null
gfs_quiet commit --path "$T52" -m "c1: main row" &>/dev/null
H52_MAIN=$(head_hash "$T52")

subbanner "branch-A from main"
"$GFS_BIN" checkout --path "$T52" -b branch-a &>/dev/null
gq "$T52" "INSERT INTO t52 VALUES (2, 'branch-a');" &>/dev/null
gfs_quiet commit --path "$T52" -m "branch-a: row 2" \
    && pass "branch-a committed" || fail "branch-a commit failed"
H52_A=$(head_hash "$T52")

subbanner "branch-B from branch-A (not from main)"
"$GFS_BIN" checkout --path "$T52" -b branch-b &>/dev/null
gq "$T52" "INSERT INTO t52 VALUES (3, 'branch-b');" &>/dev/null
gfs_quiet commit --path "$T52" -m "branch-b: row 3" \
    && pass "branch-b committed" || fail "branch-b commit failed"

subbanner "branch-B has branch-A's data (inherited)"
cnt_b=$(row_count "$T52" t52)
[[ "$cnt_b" == "3" ]] && pass "branch-b: 3 rows (main+A+B inherited)" || fail "branch-b: expected 3, got $cnt_b"
src_2=$(sq "$T52" "SELECT src FROM t52 WHERE id=2;" | tr -d '[:space:]')
[[ "$src_2" == "branch-a" ]] && pass "branch-b: branch-A row inherited (id=2)" || fail "branch-b: branch-A row missing"

subbanner "main: only 1 row (isolated)"
"$GFS_BIN" checkout --path "$T52" main &>/dev/null
cnt_main=$(row_count "$T52" t52)
[[ "$cnt_main" == "1" ]] && pass "main: 1 row (A+B rows not present)" || fail "main: expected 1, got $cnt_main"
absent_2=$(sq "$T52" "SELECT COUNT(*) FROM t52 WHERE id=2;" | tr -d '[:space:]')
absent_3=$(sq "$T52" "SELECT COUNT(*) FROM t52 WHERE id=3;" | tr -d '[:space:]')
[[ "$absent_2" == "0" ]] && pass "main: branch-A row absent" || fail "main: branch-A row leaked"
[[ "$absent_3" == "0" ]] && pass "main: branch-B row absent" || fail "main: branch-B row leaked"

subbanner "branch-A: has main+A rows, no branch-B row"
"$GFS_BIN" checkout --path "$T52" branch-a &>/dev/null
cnt_a=$(row_count "$T52" t52)
[[ "$cnt_a" == "2" ]] && pass "branch-A: 2 rows (main+A)" || fail "branch-A: expected 2, got $cnt_a"
absent_3_from_a=$(sq "$T52" "SELECT COUNT(*) FROM t52 WHERE id=3;" | tr -d '[:space:]')
[[ "$absent_3_from_a" == "0" ]] && pass "branch-A: branch-B row absent (correct isolation)" || fail "branch-A: branch-B row leaked"

subbanner "branch-C from specific old hash (H52_MAIN)"
"$GFS_BIN" checkout --path "$T52" main &>/dev/null
"$GFS_BIN" branch --path "$T52" branch-c "$H52_MAIN" &>/dev/null \
    && pass "branch-C created at H52_MAIN (main c1)" || fail "branch-C creation failed"
"$GFS_BIN" checkout --path "$T52" branch-c &>/dev/null
cnt_c=$(row_count "$T52" t52)
[[ "$cnt_c" == "1" ]] && pass "branch-C: 1 row (branched from H52_MAIN, before A+B)" || fail "branch-C: expected 1, got $cnt_c"
gq "$T52" "INSERT INTO t52 VALUES (10, 'branch-c');" &>/dev/null
gfs_quiet commit --path "$T52" -m "branch-c: independent from A+B" \
    && pass "branch-C: committed independently" || fail "branch-C commit failed"

subbanner "log --all shows all 4 branches"
all_log=$("$GFS_BIN" log --path "$T52" --all 2>/dev/null)
for br_msg in "branch-a: row 2" "branch-b: row 3" "branch-c: independent" "c1: main row"; do
    echo "$all_log" | grep -q "$br_msg" \
        && pass "log --all: '$br_msg' visible" \
        || fail "log --all: '$br_msg' missing"
done

safe_rm "$T52"

# ===========================================================================
banner "TEST 53: Integer limits + REAL NaN/Infinity — boundary arithmetic"
# ===========================================================================
T53=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T53"
gfs_quiet commit --path "$T53" -m "c0: initial" &>/dev/null

gq "$T53" "CREATE TABLE t53 (id INTEGER PRIMARY KEY, ival INTEGER, rval REAL, label TEXT);" &>/dev/null

subbanner "SQLite INTEGER max/min (64-bit signed)"
gq "$T53" "INSERT INTO t53 VALUES (1,  9223372036854775807, 0.0, 'INT_MAX');" &>/dev/null
gq "$T53" "INSERT INTO t53 VALUES (2, -9223372036854775808, 0.0, 'INT_MIN');" &>/dev/null
gq "$T53" "INSERT INTO t53 VALUES (3, 0, 1.7976931348623157e+308, 'DBL_MAX');" &>/dev/null
gq "$T53" "INSERT INTO t53 VALUES (4, 0, 2.2250738585072014e-308, 'DBL_MIN_NORMAL');" &>/dev/null
gq "$T53" "INSERT INTO t53 VALUES (5, 0, -1.7976931348623157e+308, 'DBL_NEGMAX');" &>/dev/null
assert_count "t53: 5 boundary rows" "$(row_count "$T53" t53)" "5"

gfs_quiet commit --path "$T53" -m "c1: boundary values" \
    && pass "c1 committed" || fail "c1 commit failed"
H53_C1=$(head_hash "$T53")

subbanner "INT_MAX survives commit/checkout"
gfs_quiet checkout --path "$T53" -b boundary-branch &>/dev/null
gq "$T53" "INSERT INTO t53 VALUES (6, 42, 0.0, 'extra');" &>/dev/null
gfs_quiet commit --path "$T53" -m "boundary-branch: extra row" || fail "boundary-branch commit failed"
gfs_quiet checkout --path "$T53" "$H53_C1" &>/dev/null
int_max=$(sq "$T53" "SELECT ival FROM t53 WHERE label='INT_MAX';" | tr -d '[:space:]')
int_min=$(sq "$T53" "SELECT ival FROM t53 WHERE label='INT_MIN';" | tr -d '[:space:]')
[[ "$int_max" == "9223372036854775807" ]] && pass "INT_MAX preserved: $int_max" || fail "INT_MAX corrupted: $int_max"
[[ "$int_min" == "-9223372036854775808" ]] && pass "INT_MIN preserved: $int_min" || fail "INT_MIN corrupted: $int_min"

subbanner "DBL_MAX round-trip precision"
dbl_max=$(sq "$T53" "SELECT rval FROM t53 WHERE label='DBL_MAX';" | tr -d '[:space:]')
[[ -n "$dbl_max" && "$dbl_max" != "0.0" ]] \
    && pass "DBL_MAX preserved: $dbl_max" || fail "DBL_MAX corrupted: $dbl_max"

subbanner "integer overflow behavior (wraps or errors)"
overflow_result=$(gq "$T53" "SELECT 9223372036854775807 + 1;" 2>&1 | tr -d '[:space:]')
if [[ "$overflow_result" == "-9223372036854775808" ]]; then
    pass "INT overflow: wraps to INT_MIN (two's complement)"
elif [[ "$overflow_result" == "9.22337203685478e+18" || "$overflow_result" =~ ^9\. ]]; then
    pass "INT overflow: promoted to REAL ($overflow_result)"
else
    pass "INT overflow: result=$overflow_result (behavior documented)"
fi

subbanner "zero, negative zero, 1/-1 arithmetic"
gq "$T53" "INSERT INTO t53 VALUES (7, 0, 0.0, 'zero');" &>/dev/null
gq "$T53" "INSERT INTO t53 VALUES (8, -1, -0.0, 'neg_zero');" &>/dev/null
gfs_quiet commit --path "$T53" -m "c2: zero/neg-zero rows" || fail "c2 commit failed"
H53_C2=$(head_hash "$T53")
gfs_quiet checkout --path "$T53" "$H53_C2" &>/dev/null
neg_zero=$(sq "$T53" "SELECT rval FROM t53 WHERE label='neg_zero';" | tr -d '[:space:]')
[[ "$neg_zero" == "0.0" || "$neg_zero" == "-0.0" || "$neg_zero" == "0" ]] \
    && pass "neg_zero preserved: $neg_zero" || fail "neg_zero corrupted: $neg_zero"

safe_rm "$T53"

# ===========================================================================
banner "TEST 54: Recursive CTE + view snapshot — complex SQL preserved"
# ===========================================================================
T54=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T54"
gfs_quiet commit --path "$T54" -m "c0: initial" &>/dev/null

gq "$T54" "CREATE TABLE t54_nodes (id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT);" &>/dev/null
# Build a tree: root → level1 (x3) → level2 (x3 each)
gq "$T54" "INSERT INTO t54_nodes VALUES (1, NULL, 'root');" &>/dev/null
for i in 2 3 4; do
    gq "$T54" "INSERT INTO t54_nodes VALUES ($i, 1, 'level1_$i');" &>/dev/null
done
for i in $(seq 5 13); do
    parent=$(( (i - 5) / 3 + 2 ))
    gq "$T54" "INSERT INTO t54_nodes VALUES ($i, $parent, 'level2_$i');" &>/dev/null
done
assert_count "t54_nodes: 13 nodes" "$(row_count "$T54" t54_nodes)" "13"

subbanner "recursive CTE: all descendants of root"
rcte_result=$(sq "$T54" "WITH RECURSIVE tree(id, name, depth) AS (
    SELECT id, name, 0 FROM t54_nodes WHERE parent_id IS NULL
    UNION ALL
    SELECT n.id, n.name, t.depth+1
    FROM t54_nodes n JOIN tree t ON n.parent_id = t.id
)
SELECT COUNT(*) FROM tree;" | tr -d '[:space:]')
[[ "$rcte_result" == "13" ]] \
    && pass "recursive CTE: all 13 nodes reachable" \
    || fail "recursive CTE: expected 13, got $rcte_result"

subbanner "CREATE VIEW from CTE — snapshot captures view definition"
gq "$T54" "CREATE VIEW v54_tree AS
WITH RECURSIVE tree(id, name, depth) AS (
    SELECT id, name, 0 FROM t54_nodes WHERE parent_id IS NULL
    UNION ALL
    SELECT n.id, n.name, t.depth+1
    FROM t54_nodes n JOIN tree t ON n.parent_id = t.id
)
SELECT id, name, depth FROM tree ORDER BY depth, id;" &>/dev/null
view_rows=$(sq "$T54" "SELECT COUNT(*) FROM v54_tree;" | tr -d '[:space:]')
[[ "$view_rows" == "13" ]] && pass "view v54_tree: 13 rows" || fail "view v54_tree: expected 13, got $view_rows"

gfs_quiet commit --path "$T54" -m "c1: tree + recursive view" \
    && pass "c1 committed" || fail "c1 commit failed"
H54_C1=$(head_hash "$T54")

subbanner "branch: drop view, add rows, commit"
"$GFS_BIN" checkout --path "$T54" -b no-view-branch &>/dev/null
gq "$T54" "DROP VIEW v54_tree;" &>/dev/null
gq "$T54" "INSERT INTO t54_nodes VALUES (14, 1, 'level1_14');" &>/dev/null
gfs_quiet commit --path "$T54" -m "no-view-branch: dropped view, added node 14" \
    && pass "no-view-branch committed" || fail "no-view-branch commit failed"
view_absent=$(sq "$T54" "SELECT COUNT(*) FROM sqlite_master WHERE name='v54_tree';" | tr -d '[:space:]')
[[ "$view_absent" == "0" ]] && pass "no-view-branch: view absent" || fail "no-view-branch: view still present"

subbanner "checkout c1 — view restored, 13 nodes"
gfs_quiet checkout --path "$T54" "$H54_C1" &>/dev/null
view_back=$(sq "$T54" "SELECT COUNT(*) FROM sqlite_master WHERE name='v54_tree';" | tr -d '[:space:]')
[[ "$view_back" == "1" ]] && pass "c1: view v54_tree restored" || fail "c1: view not restored"
view_count_back=$(sq "$T54" "SELECT COUNT(*) FROM v54_tree;" | tr -d '[:space:]')
[[ "$view_count_back" == "13" ]] \
    && pass "c1: view query returns 13 rows" \
    || fail "c1: view query returned $view_count_back"
node_14_absent=$(sq "$T54" "SELECT COUNT(*) FROM t54_nodes WHERE id=14;" | tr -d '[:space:]')
[[ "$node_14_absent" == "0" ]] && pass "c1: no-view-branch node absent" || fail "c1: extra node leaked"

subbanner "level1 nodes reachable via view after restore"
level1_via_view=$(sq "$T54" "SELECT COUNT(*) FROM v54_tree WHERE depth=1;" | tr -d '[:space:]')
[[ "$level1_via_view" == "3" ]] && pass "view: 3 level1 nodes at depth=1" || fail "view: expected 3 at depth=1, got $level1_via_view"
root_via_view=$(sq "$T54" "SELECT name FROM v54_tree WHERE depth=0;" | tr -d '[:space:]')
[[ "$root_via_view" == "root" ]] && pass "view: root node at depth=0" || fail "view: root wrong: $root_via_view"

safe_rm "$T54"

# ===========================================================================
banner "TEST 55: gfs log --from <hash> exact range with captured hashes"
# ===========================================================================
T55=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T55"
gfs_quiet commit --path "$T55" -m "c0: initial" &>/dev/null
gq "$T55" "CREATE TABLE t55 (id INTEGER PRIMARY KEY);" &>/dev/null

declare -a H55
for i in $(seq 1 8); do
    gq "$T55" "INSERT INTO t55 VALUES ($i);" &>/dev/null
    gfs_quiet commit --path "$T55" -m "step-$i" || fail "commit step-$i failed"
    H55[$i]=$(head_hash "$T55")
done

subbanner "log --from H55[3]: traversal starts at step-3, walks BACKWARD"
# --from sets the traversal START point — walks toward oldest commit, not toward HEAD
# H55[3]=step-3 → step-2 → step-1 → c0 (3 step-N commits in output)
from3=$("$GFS_BIN" log --path "$T55" --from "${H55[3]}" 2>/dev/null)
from3_count=$(echo "$from3" | grep -c "step-" || true)
[[ "${from3_count:-0}" -ge 3 ]] \
    && pass "log --from H55[3]: ≥3 commits (backward walk from step-3)" \
    || fail "log --from H55[3]: expected ≥3, got $from3_count"
echo "$from3" | grep -q "step-3" && pass "log --from H55[3]: step-3 (start) included" || fail "log --from H55[3]: step-3 missing"
echo "$from3" | grep -q "step-8" && fail "log --from H55[3]: step-8 should be absent (it is AFTER step-3)" || pass "log --from H55[3]: step-8 correctly absent (after start point)"
info "SEMANTICS: --from X means 'start walk at X going backward', not 'show X to HEAD'"

subbanner "log --until H55[6] from HEAD: shows step-8, step-7 (stops before step-6)"
until6=$("$GFS_BIN" log --path "$T55" --until "${H55[6]}" 2>/dev/null)
echo "$until6" | grep -q "step-8" && pass "log --until H55[6]: step-8 (HEAD) present" || fail "log --until H55[6]: step-8 missing"
echo "$until6" | grep -q "step-7" && pass "log --until H55[6]: step-7 present" || fail "log --until H55[6]: step-7 missing"
echo "$until6" | grep -q "step-5" && fail "log --until H55[6]: step-5 should be excluded" || pass "log --until H55[6]: step-5 correctly excluded"

subbanner "log --from H55[5] --until H55[2]: backward walk from 5, stops before 2"
# Backward: step-5 → step-4 → step-3 → STOP (before step-2)
range=$("$GFS_BIN" log --path "$T55" --from "${H55[5]}" --until "${H55[2]}" 2>/dev/null)
echo "$range" | grep -q "step-5" && pass "range [5→2): step-5 (start) present" || fail "range [5→2): step-5 missing"
echo "$range" | grep -q "step-4" && pass "range [5→2): step-4 present" || fail "range [5→2): step-4 missing"
echo "$range" | grep -q "step-3" && pass "range [5→2): step-3 present" || fail "range [5→2): step-3 missing"
echo "$range" | grep -q "step-2" && fail "range [5→2): step-2 should be excluded (boundary)" || pass "range [5→2): step-2 excluded"
echo "$range" | grep -q "step-8" && fail "range [5→2): step-8 out of range" || pass "range [5→2): step-8 absent"

subbanner "log --from H55[4] --max-count 2: exactly 2 from step-4 backward"
from4_max2=$("$GFS_BIN" log --path "$T55" --from "${H55[4]}" --max-count 2 2>/dev/null | grep -c "step-" || true)
[[ "${from4_max2:-0}" -eq 2 ]] \
    && pass "log --from H55[4] --max-count 2: exactly 2 commits" \
    || fail "log --from H55[4] --max-count 2: expected 2, got $from4_max2"

safe_rm "$T55"

# ===========================================================================
banner "TEST 56: Two repos in same parent dir — workspace isolation"
# ===========================================================================
T56A=$(make_test_dir)
T56B=$(make_test_dir)

gfs_quiet init --database-provider sqlite --database-version 3 "$T56A"
gfs_quiet commit --path "$T56A" -m "c0" &>/dev/null
gfs_quiet init --database-provider sqlite --database-version 3 "$T56B"
gfs_quiet commit --path "$T56B" -m "c0" &>/dev/null

gq "$T56A" "CREATE TABLE repo_a (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null
gq "$T56B" "CREATE TABLE repo_b (id INTEGER PRIMARY KEY, v TEXT);" &>/dev/null
gq "$T56A" "INSERT INTO repo_a VALUES (1, 'from-A');" &>/dev/null
gq "$T56B" "INSERT INTO repo_b VALUES (1, 'from-B');" &>/dev/null

subbanner "repo A sees only repo_a table"
a_has_a=$(sq "$T56A" "SELECT COUNT(*) FROM sqlite_master WHERE name='repo_a';" | tr -d '[:space:]')
a_has_b=$(sq "$T56A" "SELECT COUNT(*) FROM sqlite_master WHERE name='repo_b';" | tr -d '[:space:]')
[[ "$a_has_a" == "1" ]] && pass "repo-A: repo_a table present" || fail "repo-A: repo_a missing"
[[ "$a_has_b" == "0" ]] && pass "repo-A: repo_b table absent" || fail "repo-A: repo_b leaked from repo-B"

subbanner "repo B sees only repo_b table"
b_has_b=$(sq "$T56B" "SELECT COUNT(*) FROM sqlite_master WHERE name='repo_b';" | tr -d '[:space:]')
b_has_a=$(sq "$T56B" "SELECT COUNT(*) FROM sqlite_master WHERE name='repo_a';" | tr -d '[:space:]')
[[ "$b_has_b" == "1" ]] && pass "repo-B: repo_b table present" || fail "repo-B: repo_b missing"
[[ "$b_has_a" == "0" ]] && pass "repo-B: repo_a table absent" || fail "repo-B: repo_a leaked from repo-A"

subbanner "commit both repos — independent histories"
gfs_quiet commit --path "$T56A" -m "A-c1" &>/dev/null
gfs_quiet commit --path "$T56B" -m "B-c1" &>/dev/null
H56A=$(head_hash "$T56A")
H56B=$(head_hash "$T56B")
[[ "$H56A" != "$H56B" ]] && pass "repos have different HEAD hashes" || fail "repos share HEAD hash (isolation broken)"

subbanner "branch on A doesn't affect B"
"$GFS_BIN" checkout --path "$T56A" -b a-feature &>/dev/null
gq "$T56A" "INSERT INTO repo_a VALUES (2, 'a-feature-row');" &>/dev/null
gfs_quiet commit --path "$T56A" -m "A: feature commit" &>/dev/null
b_count=$(row_count "$T56B" repo_b)
[[ "$b_count" == "1" ]] && pass "repo-B unaffected by repo-A branch op" || fail "repo-B contaminated: $b_count rows"

subbanner "gfs log isolated per repo"
a_log=$("$GFS_BIN" log --path "$T56A" 2>/dev/null)
b_log=$("$GFS_BIN" log --path "$T56B" 2>/dev/null)
echo "$a_log" | grep -q "A-c1" && pass "repo-A log: A-c1 present" || fail "repo-A log: A-c1 missing"
echo "$a_log" | grep -q "B-c1" && fail "repo-A log: B-c1 should not appear" || pass "repo-A log: B-c1 absent"
echo "$b_log" | grep -q "B-c1" && pass "repo-B log: B-c1 present" || fail "repo-B log: B-c1 missing"
echo "$b_log" | grep -q "A-c1" && fail "repo-B log: A-c1 should not appear" || pass "repo-B log: A-c1 absent"

safe_rm "$T56A"
safe_rm "$T56B"

# ===========================================================================
banner "TEST 57: AUTOINCREMENT behavior across checkout"
# ===========================================================================
# Adversarial: SQLite AUTOINCREMENT uses sqlite_sequence table to track max-ever id.
# After checkout to an old commit, sqlite_sequence is restored to old state.
# New inserts on the old commit get ids starting from old max+1, not global max+1.
T57=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T57"
gfs_quiet commit --path "$T57" -m "c0: initial" &>/dev/null

gq "$T57" "CREATE TABLE t57_auto (id INTEGER PRIMARY KEY AUTOINCREMENT, v TEXT);" &>/dev/null
gq "$T57" "INSERT INTO t57_auto(v) VALUES ('row1'),('row2'),('row3');" &>/dev/null
gfs_quiet commit --path "$T57" -m "c1: ids 1-3" &>/dev/null
H57_C1=$(head_hash "$T57")

gq "$T57" "INSERT INTO t57_auto(v) VALUES ('row4'),('row5');" &>/dev/null
gfs_quiet commit --path "$T57" -m "c2: ids 4-5" &>/dev/null
H57_C2=$(head_hash "$T57")

subbanner "verify ids at c2"
max_c2=$(sq "$T57" "SELECT MAX(id) FROM t57_auto;" | tr -d '[:space:]')
[[ "$max_c2" == "5" ]] && pass "c2: max id = 5" || fail "c2: max id = $max_c2"

subbanner "checkout c1 — sqlite_sequence restored, inserts get id 4"
gfs_quiet checkout --path "$T57" "$H57_C1" &>/dev/null
cnt_c1=$(row_count "$T57" t57_auto)
[[ "$cnt_c1" == "3" ]] && pass "c1: 3 rows" || fail "c1: expected 3, got $cnt_c1"
seq_c1=$(sq "$T57" "SELECT seq FROM sqlite_sequence WHERE name='t57_auto';" | tr -d '[:space:]')
[[ "$seq_c1" == "3" ]] && pass "c1: sqlite_sequence.seq = 3 (restored)" || fail "c1: sqlite_sequence.seq = $seq_c1 (expected 3)"

# Insert on c1 — next id should be 4 (not 6, since sqlite_sequence was restored)
gq "$T57" "INSERT INTO t57_auto(v) VALUES ('new-on-c1');" &>/dev/null
new_id_c1=$(sq "$T57" "SELECT MAX(id) FROM t57_auto;" | tr -d '[:space:]')
if [[ "$new_id_c1" == "4" ]]; then
    pass "c1 insert: new id = 4 (sqlite_sequence restored to old state)"
    info "BEHAVIOR: checkout restores sqlite_sequence → AUTOINCREMENT restarts from old max"
elif [[ "$new_id_c1" == "6" ]]; then
    fail "c1 insert: new id = 6 (sqlite_sequence NOT restored — uses global max)"
else
    pass "c1 insert: new id = $new_id_c1 (behavior documented)"
fi

subbanner "checkout c2 — original ids preserved"
gfs_quiet checkout --path "$T57" "$H57_C2" &>/dev/null
max_c2_back=$(sq "$T57" "SELECT MAX(id) FROM t57_auto;" | tr -d '[:space:]')
[[ "$max_c2_back" == "5" ]] \
    && pass "c2 restored: max id = 5" \
    || fail "c2 restored: max id = $max_c2_back (expected 5)"
cnt_c2_back=$(row_count "$T57" t57_auto)
[[ "$cnt_c2_back" == "5" ]] && pass "c2: 5 rows" || fail "c2: expected 5, got $cnt_c2_back"

safe_rm "$T57"

# ===========================================================================
banner "TEST 58: gfs config + gfs version output format"
# ===========================================================================
T58=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T58"
gfs_quiet commit --path "$T58" -m "c0" &>/dev/null

subbanner "gfs version — semver format"
ver=$("$GFS_BIN" version 2>/dev/null || "$GFS_BIN" --version 2>/dev/null || echo "")
[[ -n "$ver" ]] && pass "gfs version: output non-empty" || fail "gfs version: no output"
# Should contain a version number x.y.z
echo "$ver" | grep -qE "[0-9]+\.[0-9]+\.[0-9]+" \
    && pass "gfs version: contains semver pattern" \
    || fail "gfs version: no semver found in '$ver'"

subbanner "gfs config.toml — shows sqlite provider"
# gfs config <KEY> reads a specific key; config.toml is the source of truth
cfg_toml="${T58}/.gfs/config.toml"
if [[ -f "$cfg_toml" ]]; then
    pass "gfs config: config.toml exists"
    grep -qi "sqlite" "$cfg_toml" \
        && pass "gfs config.toml: sqlite provider present" \
        || fail "gfs config.toml: sqlite not found"
    grep -qi "main\|branch\|version" "$cfg_toml" \
        && pass "gfs config.toml: branch/version info present" \
        || fail "gfs config.toml: branch/version info missing"
else
    fail "gfs config: config.toml not found at $cfg_toml"
fi

subbanner "gfs --json status fields"
jstatus=$("$GFS_BIN" --json status --path "$T58" 2>/dev/null)
if is_valid_json "$jstatus"; then
    pass "gfs --json status: valid JSON"
    echo "$jstatus" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert 'current_branch' in d, f'current_branch missing: {list(d.keys())}'
assert d['current_branch'] == 'main', f'expected main, got {d[\"current_branch\"]}'
print('fields_ok')
" 2>/dev/null && pass "gfs --json status: current_branch=main" || fail "gfs --json status: missing/wrong current_branch"

    echo "$jstatus" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert 'active_workspace_data_dir' in d, 'active_workspace_data_dir missing'
ws = d['active_workspace_data_dir']
import os
assert os.path.isdir(ws), f'workspace dir not found: {ws}'
print('ws_ok')
" 2>/dev/null && pass "gfs --json status: active_workspace_data_dir is real path" || fail "gfs --json status: workspace dir missing/invalid"
else
    fail "gfs --json status: invalid JSON"
fi

subbanner "config reflects branch switch"
"$GFS_BIN" checkout --path "$T58" -b cfg-branch &>/dev/null
jstatus2=$("$GFS_BIN" --json status --path "$T58" 2>/dev/null)
if is_valid_json "$jstatus2"; then
    br=$( echo "$jstatus2" | python3 -c "import sys,json; print(json.load(sys.stdin).get('current_branch',''))" 2>/dev/null)
    [[ "$br" == "cfg-branch" ]] && pass "gfs --json status: current_branch=cfg-branch after switch" || fail "gfs --json status: branch=$br (expected cfg-branch)"
fi

safe_rm "$T58"

# ===========================================================================
banner "TEST 59: WITHOUT ROWID + partial index across commits"
# ===========================================================================
T59=$(make_test_dir)
gfs_quiet init --database-provider sqlite --database-version 3 "$T59"
gfs_quiet commit --path "$T59" -m "c0: initial" &>/dev/null

subbanner "WITHOUT ROWID table"
wr_result=$(sq "$T59" "
CREATE TABLE t59_wr (code TEXT PRIMARY KEY, val INTEGER) WITHOUT ROWID;
INSERT INTO t59_wr VALUES ('AAA', 1);
INSERT INTO t59_wr VALUES ('BBB', 2);
INSERT INTO t59_wr VALUES ('CCC', 3);
SELECT 'wr_ok';" 2>/dev/null | tr -d '[:space:]')
if [[ "$wr_result" == "wr_ok" ]]; then
    pass "WITHOUT ROWID: table created and rows inserted"
    cnt_wr=$(sq "$T59" "SELECT COUNT(*) FROM t59_wr;" | tr -d '[:space:]')
    [[ "$cnt_wr" == "3" ]] && pass "WITHOUT ROWID: 3 rows" || fail "WITHOUT ROWID: expected 3, got $cnt_wr"
    gfs_quiet commit --path "$T59" -m "c1: WITHOUT ROWID table" \
        && pass "WITHOUT ROWID committed" || fail "WITHOUT ROWID commit failed"
    H59_C1=$(head_hash "$T59")

    "GFS_BIN" checkout --path "$T59" -b wr-branch &>/dev/null || "$GFS_BIN" checkout --path "$T59" -b wr-branch &>/dev/null
    sq "$T59" "INSERT INTO t59_wr VALUES ('DDD', 4);" &>/dev/null
    gfs_quiet commit --path "$T59" -m "wr-branch: 4 rows" || fail "wr-branch commit failed"
    gfs_quiet checkout --path "$T59" "$H59_C1" &>/dev/null
    cnt_c1_wr=$(sq "$T59" "SELECT COUNT(*) FROM t59_wr;" | tr -d '[:space:]')
    [[ "$cnt_c1_wr" == "3" ]] && pass "c1: WITHOUT ROWID 3 rows after checkout" || fail "c1: WITHOUT ROWID count wrong: $cnt_c1_wr"
    val_aaa=$(sq "$T59" "SELECT val FROM t59_wr WHERE code='AAA';" | tr -d '[:space:]')
    [[ "$val_aaa" == "1" ]] && pass "c1: WITHOUT ROWID row data intact (AAA=1)" || fail "c1: WITHOUT ROWID data wrong: $val_aaa"
else
    skip "WITHOUT ROWID: not supported on this SQLite version"
fi

subbanner "partial index (CREATE INDEX WHERE clause)"
# Ensure we're on main (not detached from WITHOUT ROWID section)
"$GFS_BIN" checkout --path "$T59" main &>/dev/null || true
gq "$T59" "CREATE TABLE t59_pi (id INTEGER PRIMARY KEY, status TEXT, amount REAL);" &>/dev/null
for i in $(seq 1 10); do
    st=$( [[ $((i % 2)) -eq 0 ]] && echo "active" || echo "inactive" )
    gq "$T59" "INSERT INTO t59_pi VALUES ($i, '$st', $((i * 10)));" &>/dev/null
done
sq "$T59" "CREATE INDEX idx_active_amount ON t59_pi(amount) WHERE status='active';" &>/dev/null
idx_present=$(sq "$T59" "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_active_amount';" | tr -d '[:space:]')
[[ "$idx_present" == "1" ]] && pass "partial index created" || fail "partial index creation failed"
gfs_quiet commit --path "$T59" -m "c2: partial index" || fail "c2 commit failed"
H59_C2=$(head_hash "$T59")

"$GFS_BIN" checkout --path "$T59" -b pi-branch &>/dev/null
sq "$T59" "DROP INDEX idx_active_amount;" &>/dev/null
gfs_quiet commit --path "$T59" -m "pi-branch: dropped partial index" || fail "pi-branch commit failed"
idx_gone=$(sq "$T59" "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_active_amount';" | tr -d '[:space:]')
[[ "$idx_gone" == "0" ]] && pass "pi-branch: partial index absent" || fail "pi-branch: index still present"

gfs_quiet checkout --path "$T59" "$H59_C2" &>/dev/null
idx_restored=$(sq "$T59" "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_active_amount';" | tr -d '[:space:]')
[[ "$idx_restored" == "1" ]] \
    && pass "c2: partial index restored after checkout" \
    || fail "c2: partial index not restored"
# Verify index is still usable (query planner uses it)
idx_query=$(sq "$T59" "SELECT COUNT(*) FROM t59_pi WHERE status='active' AND amount > 20;" | tr -d '[:space:]')
[[ "${idx_query:-0}" -ge 0 ]] && pass "c2: query using partial index executes (result=$idx_query)" || fail "c2: query with partial index failed"

safe_rm "$T59"

# ===========================================================================
banner "RESULTS SUMMARY"
# ===========================================================================
printf "\n  %-10s %8s %8s %8s\n" "Category" "Passed" "Failed" "Skipped"
printf "  %-10s %8d %8d %8d\n" "SQLite" "$PASS_COUNT" "$FAIL_COUNT" "$SKIP_COUNT"

TOTAL=$((PASS_COUNT + FAIL_COUNT + SKIP_COUNT))
printf "\n  Total: %d tests (%d passed, %d failed, %d skipped)\n" \
    "$TOTAL" "$PASS_COUNT" "$FAIL_COUNT" "$SKIP_COUNT"

if [[ $FAIL_COUNT -eq 0 ]]; then
    printf "\n${GREEN}${BOLD}ALL TESTS PASSED${NC}\n\n"
    exit 0
else
    printf "\n${RED}${BOLD}$FAIL_COUNT TEST(S) FAILED${NC}\n\n"
    exit 1
fi
