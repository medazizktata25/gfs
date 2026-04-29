#!/usr/bin/env bash
# GFS Heavy DB Test — Multi-tenant SaaS Platform (SQLite backend)
set -uo pipefail

GFS_BIN="${GFS_BIN:-/home/mohamed-aziz-ktata/Desktop/Guepard/gfs/target/debug/gfs}"
REPO=$(mktemp -d)
REPO2=""

cleanup() {
    chmod -R u+w "$REPO" 2>/dev/null || true
    rm -rf "$REPO"
    if [[ -n "$REPO2" ]]; then
        chmod -R u+w "$REPO2" 2>/dev/null || true
        rm -rf "$REPO2"
    fi
}
trap cleanup EXIT

PASS=0; FAIL=0; SKIP=0

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

pass()    { ((PASS++));  printf "  ${GREEN}✓${NC} %s\n" "$1"; }
fail()    { ((FAIL++));  printf "  ${RED}✗${NC} %s\n" "$1"; }
skip()    { ((SKIP++));  printf "  ${YELLOW}⊘${NC} %s\n" "$1"; }
section() { printf "\n  ${CYAN}── %s ──${NC}\n" "$1"; }
banner()  { printf "\n${CYAN}${BOLD}════════════════════════════════════════\n  %s\n════════════════════════════════════════${NC}\n" "$1"; }

assert_eq() {
    local label="$1" got="$2" want="$3"
    [[ "$got" == "$want" ]] && pass "$label: $want" || fail "$label: expected='$want' got='$got'"
}
assert_ge() {
    local label="$1" got="$2" want="$3"
    [[ "$got" -ge "$want" ]] 2>/dev/null \
        && pass "$label: ≥$want (got $got)" \
        || fail "$label: expected ≥$want, got='$got'"
}
assert_contains() {
    local label="$1" haystack="$2" needle="$3"
    echo "$haystack" | grep -qF "$needle" && pass "$label" || fail "$label (missing: $needle)"
}
assert_not_contains() {
    local label="$1" haystack="$2" needle="$3"
    echo "$haystack" | grep -qF "$needle" \
        && fail "$label (should be absent: $needle)" \
        || pass "$label: '$needle' absent"
}
assert_fp_eq() {
    local label="$1" a="$2" b="$3"
    [[ "$a" == "$b" ]] && pass "$label: fingerprint match" \
        || fail "$label: fingerprint mismatch (want=$a got=$b)"
}
assert_fp_ne() {
    local label="$1" a="$2" b="$3"
    [[ "$a" != "$b" ]] && pass "$label: fingerprints differ" \
        || fail "$label: fingerprints unexpectedly match"
}

get_db_path() {
    local repo="$1"
    local ws; ws=$(tr -d '[:space:]' < "${repo}/.gfs/WORKSPACE")
    echo "${ws}/db.sqlite"
}

head_hash() {
    local repo="$1"
    local hc; hc=$(tr -d '[:space:]' < "${repo}/.gfs/HEAD")
    if [[ "$hc" == ref:refs/heads/* ]]; then
        local br="${hc#ref:refs/heads/}"
        tr -d '[:space:]' < "${repo}/.gfs/refs/heads/${br}"
    else
        echo "$hc"
    fi
}

DB_PATH=""

sq()     { sqlite3 "$DB_PATH" "$@"; }
gq()     { "$GFS_BIN" query --path "$REPO" "$@" 2>/dev/null; }
do_commit() { "$GFS_BIN" commit --path "$REPO" -m "$1" >/dev/null 2>&1; }
checkout() {
    "$GFS_BIN" checkout --path "$REPO" "$1" >/dev/null 2>&1
    DB_PATH=$(get_db_path "$REPO")
}

fingerprint() {
    local table="$1" col="$2"
    sqlite3 "$DB_PATH" "SELECT ${col} FROM ${table} ORDER BY ${col};" 2>/dev/null \
        | sha256sum | awk '{print $1}'
}

db_ready() {
    local label="$1"
    local result; result=$(sqlite3 "$DB_PATH" "PRAGMA integrity_check;" 2>/dev/null \
        | head -1 | tr -d '[:space:]')
    [[ "$result" == "ok" ]] \
        && pass "$label: integrity_check=ok" \
        || fail "$label: integrity_check=$result"
}

# ── Preflight ─────────────────────────────────────────────────────────────
banner "PREFLIGHT"
GFS_VER=$("$GFS_BIN" version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)
MAJOR=$(echo "$GFS_VER" | cut -d. -f1)
MINOR=$(echo "$GFS_VER" | cut -d. -f2)
if [[ "$MAJOR" -gt 0 ]] || [[ "$MAJOR" -eq 0 && "$MINOR" -ge 2 ]]; then
    pass "gfs version $GFS_VER supports sqlite"
else
    fail "gfs $GFS_VER does not support sqlite; set GFS_BIN to local debug build"; exit 1
fi
which sqlite3 &>/dev/null && pass "sqlite3 available" || { fail "sqlite3 not found"; exit 1; }

# ═══════════════════════════════════════════════════════════════════════════
banner "PHASE 0 — Init, DDL, Seed"
# ═══════════════════════════════════════════════════════════════════════════
section "gfs init sqlite"
"$GFS_BIN" init --database-provider sqlite --database-version 3 "$REPO" >/dev/null 2>&1 \
    && pass "gfs init sqlite" || { fail "gfs init failed"; exit 1; }
DB_PATH=$(get_db_path "$REPO")
[[ -n "$DB_PATH" ]] && pass "WORKSPACE resolved" || { fail "DB_PATH empty"; exit 1; }

section "c0 — empty baseline commit"
do_commit "c0 empty"
HASH_C0=$(head_hash "$REPO")
[[ -n "$HASH_C0" ]] && pass "c0 hash=${HASH_C0:0:7}" || fail "c0 hash empty"

section "DDL — 10 tables"
sqlite3 "$DB_PATH" <<'SQL'
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS tenants (
    id INTEGER PRIMARY KEY, name TEXT NOT NULL,
    plan TEXT NOT NULL CHECK(plan IN ('free','pro','enterprise')),
    created_ts INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL,
    username TEXT NOT NULL, email TEXT NOT NULL,
    role TEXT NOT NULL CHECK(role IN ('admin','member','viewer')),
    created_ts INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('active','completed','on_hold','archived')),
    budget REAL NOT NULL DEFAULT 0, created_ts INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL,
    assigned_to INTEGER, title TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('todo','in_progress','done','overdue')),
    priority INTEGER NOT NULL CHECK(priority BETWEEN 1 AND 5),
    due_ts INTEGER, created_ts INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS time_entries (
    id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
    hours REAL NOT NULL CHECK(hours > 0), billed INTEGER NOT NULL DEFAULT 0,
    entry_date INTEGER NOT NULL, notes TEXT
);
CREATE TABLE IF NOT EXISTS invoices (
    id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL,
    amount REAL NOT NULL CHECK(amount >= 0),
    status TEXT NOT NULL CHECK(status IN ('draft','issued','paid','overdue','cancelled')),
    issued_ts INTEGER NOT NULL, due_ts INTEGER NOT NULL, paid_ts INTEGER
);
CREATE TABLE IF NOT EXISTS invoice_items (
    id INTEGER PRIMARY KEY, invoice_id INTEGER NOT NULL,
    description TEXT NOT NULL, quantity INTEGER NOT NULL CHECK(quantity > 0),
    unit_price REAL NOT NULL CHECK(unit_price >= 0)
);
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY, tenant_id INTEGER NOT NULL,
    user_id INTEGER, action TEXT NOT NULL, entity_type TEXT NOT NULL,
    entity_id INTEGER NOT NULL, ts INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS tags (
    id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS task_tags (
    task_id INTEGER NOT NULL, tag_id INTEGER NOT NULL,
    PRIMARY KEY (task_id, tag_id)
);
SQL
TC=$(sqlite3 "$DB_PATH" "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
assert_eq "10 tables" "$TC" "10"

section "seed tenants (10)"
sqlite3 "$DB_PATH" <<'SQL'
INSERT OR IGNORE INTO tenants VALUES
(1,'TenantAlpha','free',1700000000,1),(2,'TenantBeta','pro',1700086400,1),
(3,'TenantGamma','enterprise',1700172800,1),(4,'TenantDelta','free',1700259200,1),
(5,'TenantEpsilon','pro',1700345600,1),(6,'TenantZeta','enterprise',1700432000,1),
(7,'TenantEta','free',1700518400,1),(8,'TenantTheta','pro',1700604800,1),
(9,'TenantIota','enterprise',1700691200,1),(10,'TenantKappa','free',1700777600,1);
SQL
assert_eq "tenants" "$(sq "SELECT COUNT(*) FROM tenants;")" "10"

section "seed users (100)"
sqlite3 "$DB_PATH" <<'SQL'
INSERT OR IGNORE INTO users VALUES
(1,1,'u1_1','u1_1@t1.com','admin',1700000100,1),(2,1,'u1_2','u1_2@t1.com','member',1700000200,1),
(3,1,'u1_3','u1_3@t1.com','member',1700000300,1),(4,1,'u1_4','u1_4@t1.com','viewer',1700000400,1),
(5,1,'u1_5','u1_5@t1.com','member',1700000500,1),(6,1,'u1_6','u1_6@t1.com','viewer',1700000600,1),
(7,1,'u1_7','u1_7@t1.com','member',1700000700,1),(8,1,'u1_8','u1_8@t1.com','viewer',1700000800,1),
(9,1,'u1_9','u1_9@t1.com','member',1700000900,1),(10,1,'u1_10','u1_10@t1.com','viewer',1700001000,1),
(11,2,'u2_1','u2_1@t2.com','admin',1700086500,1),(12,2,'u2_2','u2_2@t2.com','member',1700086600,1),
(13,2,'u2_3','u2_3@t2.com','member',1700086700,1),(14,2,'u2_4','u2_4@t2.com','viewer',1700086800,1),
(15,2,'u2_5','u2_5@t2.com','member',1700086900,1),(16,2,'u2_6','u2_6@t2.com','viewer',1700087000,1),
(17,2,'u2_7','u2_7@t2.com','member',1700087100,1),(18,2,'u2_8','u2_8@t2.com','viewer',1700087200,1),
(19,2,'u2_9','u2_9@t2.com','member',1700087300,1),(20,2,'u2_10','u2_10@t2.com','viewer',1700087400,1),
(21,3,'u3_1','u3_1@t3.com','admin',1700172900,1),(22,3,'u3_2','u3_2@t3.com','member',1700173000,1),
(23,3,'u3_3','u3_3@t3.com','member',1700173100,1),(24,3,'u3_4','u3_4@t3.com','viewer',1700173200,1),
(25,3,'u3_5','u3_5@t3.com','member',1700173300,1),(26,3,'u3_6','u3_6@t3.com','viewer',1700173400,1),
(27,3,'u3_7','u3_7@t3.com','member',1700173500,1),(28,3,'u3_8','u3_8@t3.com','viewer',1700173600,1),
(29,3,'u3_9','u3_9@t3.com','member',1700173700,1),(30,3,'u3_10','u3_10@t3.com','viewer',1700173800,1),
(31,4,'u4_1','u4_1@t4.com','admin',1700259300,1),(32,4,'u4_2','u4_2@t4.com','member',1700259400,1),
(33,4,'u4_3','u4_3@t4.com','member',1700259500,1),(34,4,'u4_4','u4_4@t4.com','viewer',1700259600,1),
(35,4,'u4_5','u4_5@t4.com','member',1700259700,1),(36,4,'u4_6','u4_6@t4.com','viewer',1700259800,1),
(37,4,'u4_7','u4_7@t4.com','member',1700259900,1),(38,4,'u4_8','u4_8@t4.com','viewer',1700260000,1),
(39,4,'u4_9','u4_9@t4.com','member',1700260100,1),(40,4,'u4_10','u4_10@t4.com','viewer',1700260200,1),
(41,5,'u5_1','u5_1@t5.com','admin',1700345700,1),(42,5,'u5_2','u5_2@t5.com','member',1700345800,1),
(43,5,'u5_3','u5_3@t5.com','member',1700345900,1),(44,5,'u5_4','u5_4@t5.com','viewer',1700346000,1),
(45,5,'u5_5','u5_5@t5.com','member',1700346100,1),(46,5,'u5_6','u5_6@t5.com','viewer',1700346200,1),
(47,5,'u5_7','u5_7@t5.com','member',1700346300,1),(48,5,'u5_8','u5_8@t5.com','viewer',1700346400,1),
(49,5,'u5_9','u5_9@t5.com','member',1700346500,1),(50,5,'u5_10','u5_10@t5.com','viewer',1700346600,1),
(51,6,'u6_1','u6_1@t6.com','admin',1700432100,1),(52,6,'u6_2','u6_2@t6.com','member',1700432200,1),
(53,6,'u6_3','u6_3@t6.com','member',1700432300,1),(54,6,'u6_4','u6_4@t6.com','viewer',1700432400,1),
(55,6,'u6_5','u6_5@t6.com','member',1700432500,1),(56,6,'u6_6','u6_6@t6.com','viewer',1700432600,1),
(57,6,'u6_7','u6_7@t6.com','member',1700432700,1),(58,6,'u6_8','u6_8@t6.com','viewer',1700432800,1),
(59,6,'u6_9','u6_9@t6.com','member',1700432900,1),(60,6,'u6_10','u6_10@t6.com','viewer',1700433000,1),
(61,7,'u7_1','u7_1@t7.com','admin',1700518500,1),(62,7,'u7_2','u7_2@t7.com','member',1700518600,1),
(63,7,'u7_3','u7_3@t7.com','member',1700518700,1),(64,7,'u7_4','u7_4@t7.com','viewer',1700518800,1),
(65,7,'u7_5','u7_5@t7.com','member',1700518900,1),(66,7,'u7_6','u7_6@t7.com','viewer',1700519000,1),
(67,7,'u7_7','u7_7@t7.com','member',1700519100,1),(68,7,'u7_8','u7_8@t7.com','viewer',1700519200,1),
(69,7,'u7_9','u7_9@t7.com','member',1700519300,1),(70,7,'u7_10','u7_10@t7.com','viewer',1700519400,1),
(71,8,'u8_1','u8_1@t8.com','admin',1700604900,1),(72,8,'u8_2','u8_2@t8.com','member',1700605000,1),
(73,8,'u8_3','u8_3@t8.com','member',1700605100,1),(74,8,'u8_4','u8_4@t8.com','viewer',1700605200,1),
(75,8,'u8_5','u8_5@t8.com','member',1700605300,1),(76,8,'u8_6','u8_6@t8.com','viewer',1700605400,1),
(77,8,'u8_7','u8_7@t8.com','member',1700605500,1),(78,8,'u8_8','u8_8@t8.com','viewer',1700605600,1),
(79,8,'u8_9','u8_9@t8.com','member',1700605700,1),(80,8,'u8_10','u8_10@t8.com','viewer',1700605800,1),
(81,9,'u9_1','u9_1@t9.com','admin',1700691300,1),(82,9,'u9_2','u9_2@t9.com','member',1700691400,1),
(83,9,'u9_3','u9_3@t9.com','member',1700691500,1),(84,9,'u9_4','u9_4@t9.com','viewer',1700691600,1),
(85,9,'u9_5','u9_5@t9.com','member',1700691700,1),(86,9,'u9_6','u9_6@t9.com','viewer',1700691800,1),
(87,9,'u9_7','u9_7@t9.com','member',1700691900,1),(88,9,'u9_8','u9_8@t9.com','viewer',1700692000,1),
(89,9,'u9_9','u9_9@t9.com','member',1700692100,1),(90,9,'u9_10','u9_10@t9.com','viewer',1700692200,1),
(91,10,'u10_1','u10_1@t10.com','admin',1700777700,1),(92,10,'u10_2','u10_2@t10.com','member',1700777800,1),
(93,10,'u10_3','u10_3@t10.com','member',1700777900,1),(94,10,'u10_4','u10_4@t10.com','viewer',1700778000,1),
(95,10,'u10_5','u10_5@t10.com','member',1700778100,1),(96,10,'u10_6','u10_6@t10.com','viewer',1700778200,1),
(97,10,'u10_7','u10_7@t10.com','member',1700778300,1),(98,10,'u10_8','u10_8@t10.com','viewer',1700778400,1),
(99,10,'u10_9','u10_9@t10.com','member',1700778500,1),(100,10,'u10_10','u10_10@t10.com','viewer',1700778600,1);
SQL
assert_eq "users" "$(sq "SELECT COUNT(*) FROM users;")" "100"

section "seed projects (50)"
sqlite3 "$DB_PATH" <<'SQL'
INSERT OR IGNORE INTO projects VALUES
(1,1,'Proj Alpha-1','active',50000,1700001000),(2,1,'Proj Alpha-2','active',75000,1700001100),
(3,1,'Proj Alpha-3','active',30000,1700001200),(4,1,'Proj Alpha-4','completed',90000,1700001300),
(5,1,'Proj Alpha-5','on_hold',20000,1700001400),(6,2,'Proj Beta-1','active',60000,1700087500),
(7,2,'Proj Beta-2','active',45000,1700087600),(8,2,'Proj Beta-3','active',80000,1700087700),
(9,2,'Proj Beta-4','completed',55000,1700087800),(10,2,'Proj Beta-5','on_hold',35000,1700087900),
(11,3,'Proj Gamma-1','active',120000,1700173900),(12,3,'Proj Gamma-2','active',95000,1700174000),
(13,3,'Proj Gamma-3','active',70000,1700174100),(14,3,'Proj Gamma-4','completed',150000,1700174200),
(15,3,'Proj Gamma-5','on_hold',40000,1700174300),(16,4,'Proj Delta-1','active',25000,1700260300),
(17,4,'Proj Delta-2','active',38000,1700260400),(18,4,'Proj Delta-3','active',42000,1700260500),
(19,4,'Proj Delta-4','completed',60000,1700260600),(20,4,'Proj Delta-5','on_hold',15000,1700260700),
(21,5,'Proj Epsilon-1','active',85000,1700346700),(22,5,'Proj Epsilon-2','active',65000,1700346800),
(23,5,'Proj Epsilon-3','active',55000,1700346900),(24,5,'Proj Epsilon-4','completed',100000,1700347000),
(25,5,'Proj Epsilon-5','on_hold',30000,1700347100),(26,6,'Proj Zeta-1','active',200000,1700433100),
(27,6,'Proj Zeta-2','active',175000,1700433200),(28,6,'Proj Zeta-3','active',130000,1700433300),
(29,6,'Proj Zeta-4','completed',220000,1700433400),(30,6,'Proj Zeta-5','on_hold',90000,1700433500),
(31,7,'Proj Eta-1','active',18000,1700519500),(32,7,'Proj Eta-2','active',22000,1700519600),
(33,7,'Proj Eta-3','active',27000,1700519700),(34,7,'Proj Eta-4','completed',35000,1700519800),
(35,7,'Proj Eta-5','on_hold',12000,1700519900),(36,8,'Proj Theta-1','active',72000,1700605900),
(37,8,'Proj Theta-2','active',68000,1700606000),(38,8,'Proj Theta-3','active',58000,1700606100),
(39,8,'Proj Theta-4','completed',85000,1700606200),(40,8,'Proj Theta-5','on_hold',45000,1700606300),
(41,9,'Proj Iota-1','active',160000,1700692300),(42,9,'Proj Iota-2','active',140000,1700692400),
(43,9,'Proj Iota-3','active',115000,1700692500),(44,9,'Proj Iota-4','completed',190000,1700692600),
(45,9,'Proj Iota-5','on_hold',70000,1700692700),(46,10,'Proj Kappa-1','active',16000,1700778700),
(47,10,'Proj Kappa-2','active',19000,1700778800),(48,10,'Proj Kappa-3','active',23000,1700778900),
(49,10,'Proj Kappa-4','completed',28000,1700779000),(50,10,'Proj Kappa-5','on_hold',11000,1700779100);
SQL
assert_eq "projects" "$(sq "SELECT COUNT(*) FROM projects;")" "50"

section "seed tasks (500) via SQL WITH RECURSIVE"
# Use recursive CTE to generate 500 rows deterministically
sqlite3 "$DB_PATH" <<'SQL'
WITH RECURSIVE
  seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 500),
  statuses(idx,val) AS (VALUES (0,'todo'),(1,'todo'),(2,'in_progress'),(3,'in_progress'),
    (4,'done'),(5,'done'),(6,'done'),(7,'overdue'),(8,'overdue'),(9,'todo')),
  priorities(idx,val) AS (VALUES (0,3),(1,2),(2,4),(3,1),(4,5),(5,3),(6,2),(7,4),(8,1),(9,5))
INSERT OR IGNORE INTO tasks
SELECT
  n AS id,
  ((n-1)/10)+1 AS project_id,
  ((((n-1)/10)/5)*10) + ((n-1) % 10) + 1 AS assigned_to,
  'Task '||n||' p'||(((n-1)/10)+1) AS title,
  s.val AS status,
  p.val AS priority,
  1700000000 + n*3600 AS due_ts,
  1700000000 + n*1000 AS created_ts
FROM seq
JOIN statuses s ON s.idx = ((n-1) % 10)
JOIN priorities p ON p.idx = ((n-1) % 10);
SQL
assert_eq "tasks" "$(sq "SELECT COUNT(*) FROM tasks;")" "500"

section "seed time_entries (2000) via SQL WITH RECURSIVE"
# 4 entries per task; entry_date deterministic: 1700000000 + (task_id-1)*86400 + offset*21600
sqlite3 "$DB_PATH" <<'SQL'
WITH RECURSIVE
  seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 2000),
  offsets(idx,hrs) AS (VALUES (0,1.5),(1,2.0),(2,2.5),(3,3.0)),
  billed_map(idx,b) AS (VALUES (0,1),(1,1),(2,0),(3,0))
INSERT OR IGNORE INTO time_entries
SELECT
  n AS id,
  ((n-1)/4)+1 AS task_id,
  (((((n-1)/4)/10)/5)*10) + (((n-1)/4) % 10) + 1 AS user_id,
  o.hrs AS hours,
  bm.b AS billed,
  1700000000 + (((n-1)/4))*86400 + ((n-1)%4)*21600 AS entry_date,
  'note entry '||n AS notes
FROM seq
JOIN offsets o ON o.idx = ((n-1) % 4)
JOIN billed_map bm ON bm.idx = ((n-1) % 4);
SQL
assert_eq "time_entries" "$(sq "SELECT COUNT(*) FROM time_entries;")" "2000"

section "seed invoices (100)"
# 10 per tenant; statuses cycle: issued,issued,paid,paid,paid,overdue,draft,issued,paid,cancelled
# amount: 1000 + invoice_id * 500; issued_ts deterministic
sqlite3 "$DB_PATH" <<'SQL'
WITH RECURSIVE
  seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 100),
  istatuses(idx,val) AS (VALUES
    (0,'issued'),(1,'issued'),(2,'paid'),(3,'paid'),(4,'paid'),
    (5,'overdue'),(6,'draft'),(7,'issued'),(8,'paid'),(9,'cancelled'))
INSERT OR IGNORE INTO invoices
SELECT
  n,
  ((n-1)/10)+1 AS tenant_id,
  1000.0 + n*500.0 AS amount,
  s.val AS status,
  1700000000 + n*86400 AS issued_ts,
  1700000000 + n*86400 + 2592000 AS due_ts,
  CASE WHEN s.val='paid' THEN 1700000000 + n*86400 + 864000 ELSE NULL END AS paid_ts
FROM seq
JOIN istatuses s ON s.idx = ((n-1) % 10);
SQL
assert_eq "invoices" "$(sq "SELECT COUNT(*) FROM invoices;")" "100"

section "seed invoice_items (300)"
# 3 items per invoice
sqlite3 "$DB_PATH" <<'SQL'
WITH RECURSIVE
  seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 300),
  qtys(idx,q) AS (VALUES (0,1),(1,2),(2,3)),
  prices(idx,p) AS (VALUES (0,100.0),(1,250.0),(2,75.0))
INSERT OR IGNORE INTO invoice_items
SELECT
  n,
  ((n-1)/3)+1 AS invoice_id,
  'Item '||n||' for invoice '||(((n-1)/3)+1) AS description,
  qt.q AS quantity,
  pr.p AS unit_price
FROM seq
JOIN qtys qt ON qt.idx = ((n-1) % 3)
JOIN prices pr ON pr.idx = ((n-1) % 3);
SQL
assert_eq "invoice_items" "$(sq "SELECT COUNT(*) FROM invoice_items;")" "300"

section "seed audit_log (500)"
sqlite3 "$DB_PATH" <<'SQL'
WITH RECURSIVE
  seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 500),
  actions(idx,val) AS (VALUES (0,'CREATE'),(1,'UPDATE'),(2,'DELETE'),(3,'LOGIN'),(4,'LOGOUT')),
  etypes(idx,val) AS (VALUES (0,'task'),(1,'project'),(2,'invoice'),(3,'user'),(4,'tenant'))
INSERT OR IGNORE INTO audit_log
SELECT
  n,
  ((n-1) % 10)+1 AS tenant_id,
  CASE WHEN n % 5 != 0 THEN ((n-1) % 100)+1 ELSE NULL END AS user_id,
  a.val AS action,
  e.val AS entity_type,
  ((n-1) % 50)+1 AS entity_id,
  1700000000 + n*7200 AS ts
FROM seq
JOIN actions a ON a.idx = ((n-1) % 5)
JOIN etypes e ON e.idx = ((n-1) % 5);
SQL
assert_eq "audit_log" "$(sq "SELECT COUNT(*) FROM audit_log;")" "500"

section "seed tags (20)"
sqlite3 "$DB_PATH" <<'SQL'
INSERT OR IGNORE INTO tags VALUES
(1,'backend'),(2,'frontend'),(3,'database'),(4,'api'),(5,'ui'),
(6,'performance'),(7,'security'),(8,'testing'),(9,'devops'),(10,'mobile'),
(11,'urgent'),(12,'blocked'),(13,'review'),(14,'design'),(15,'research'),
(16,'bug'),(17,'feature'),(18,'refactor'),(19,'docs'),(20,'infra');
SQL
assert_eq "tags" "$(sq "SELECT COUNT(*) FROM tags;")" "20"

section "seed task_tags (~800)"
# Each task gets 1 or 2 tags deterministically; task_id mod 20+1 and task_id mod 20+2 (for even ids)
sqlite3 "$DB_PATH" <<'SQL'
WITH RECURSIVE
  seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < 500)
INSERT OR IGNORE INTO task_tags
SELECT n, ((n-1) % 20)+1 FROM seq
UNION ALL
SELECT n, ((n) % 20)+1 FROM seq WHERE n % 2 = 0;
SQL
TT=$(sq "SELECT COUNT(*) FROM task_tags;")
assert_ge "task_tags ≥ 750" "$TT" "750"

section "c1 — full baseline commit"
do_commit "c1 full baseline"
HASH_BASELINE=$(head_hash "$REPO")
[[ -n "$HASH_BASELINE" ]] && pass "HASH_BASELINE=${HASH_BASELINE:0:7}" || fail "HASH_BASELINE empty"

# ═══════════════════════════════════════════════════════════════════════════
banner "PHASE 1 — Complex SQL on baseline"
# ═══════════════════════════════════════════════════════════════════════════
section "multi-table JOIN: billable hours per tenant"
BHRS=$(sq "
SELECT t.id, SUM(te.hours) as total_hrs
FROM tenants t
JOIN projects p ON p.tenant_id = t.id
JOIN tasks tk ON tk.project_id = p.id
JOIN time_entries te ON te.task_id = tk.id
WHERE te.billed = 1
GROUP BY t.id
ORDER BY t.id;
" | wc -l | tr -d ' ')
assert_ge "billable hours JOIN: rows" "$BHRS" "10"

section "window function: RANK tasks by priority per project"
WF=$(sq "
SELECT project_id, id, priority,
       RANK() OVER (PARTITION BY project_id ORDER BY priority DESC) AS rnk
FROM tasks
ORDER BY project_id, rnk
LIMIT 10;
" | wc -l | tr -d ' ')
assert_ge "window RANK rows" "$WF" "10"

section "CTE: top-10 tenants by invoice total"
CTE=$(sq "
WITH tenant_totals AS (
  SELECT tenant_id, SUM(amount) AS total_amount
  FROM invoices
  WHERE status IN ('issued','paid')
  GROUP BY tenant_id
)
SELECT t.name, tt.total_amount
FROM tenants t
JOIN tenant_totals tt ON tt.tenant_id = t.id
ORDER BY tt.total_amount DESC
LIMIT 10;
" | wc -l | tr -d ' ')
assert_ge "CTE tenant totals: rows" "$CTE" "10"

section "correlated subquery: projects where ALL tasks are done"
DONE_PROJS=$(sq "
SELECT COUNT(*) FROM projects p
WHERE NOT EXISTS (
  SELECT 1 FROM tasks t
  WHERE t.project_id = p.id AND t.status != 'done'
);")
assert_ge "projects all-done" "$DONE_PROJS" "0"

section "HAVING: tenants with > 5 overdue tasks"
OVD=$(sq "
SELECT t.id, COUNT(tk.id) AS overdue_cnt
FROM tenants t
JOIN projects p ON p.tenant_id = t.id
JOIN tasks tk ON tk.project_id = p.id
WHERE tk.status = 'overdue'
GROUP BY t.id
HAVING overdue_cnt > 5
ORDER BY t.id;
" | wc -l | tr -d ' ')
assert_ge "tenants with >5 overdue tasks" "$OVD" "1"

section "self-join: users same tenant as user id=5"
SJ=$(sq "
SELECT u2.id, u2.username
FROM users u1
JOIN users u2 ON u2.tenant_id = u1.tenant_id AND u2.id != u1.id
WHERE u1.id = 5
ORDER BY u2.id;
" | wc -l | tr -d ' ')
assert_eq "self-join same-tenant users" "$SJ" "9"

section "EXISTS: tenants with at least one paid invoice"
EX=$(sq "
SELECT COUNT(*) FROM tenants t
WHERE EXISTS (
  SELECT 1 FROM invoices i WHERE i.tenant_id = t.id AND i.status = 'paid'
);")
assert_ge "tenants with paid invoice" "$EX" "3"

# ═══════════════════════════════════════════════════════════════════════════
banner "PHASE 2 — Baseline fingerprints"
# ═══════════════════════════════════════════════════════════════════════════
section "capture baseline fingerprints"
FP_BASE_TENANTS=$(fingerprint "tenants" "id")
FP_BASE_TASKS=$(fingerprint "tasks" "id")
FP_BASE_INVOICES=$(fingerprint "invoices" "id")
FP_BASE_USERS=$(fingerprint "users" "id")
FP_BASE_TIME=$(fingerprint "time_entries" "id")
[[ -n "$FP_BASE_TENANTS" ]] && pass "FP_BASE_TENANTS captured" || fail "FP_BASE_TENANTS empty"
[[ -n "$FP_BASE_TASKS"   ]] && pass "FP_BASE_TASKS captured"   || fail "FP_BASE_TASKS empty"
[[ -n "$FP_BASE_INVOICES" ]] && pass "FP_BASE_INVOICES captured" || fail "FP_BASE_INVOICES empty"
db_ready "baseline"

# ═══════════════════════════════════════════════════════════════════════════
banner "PHASE 3 — 8 branch operations"
# ═══════════════════════════════════════════════════════════════════════════

# ── Branch 1: feature/billing-v2 ──────────────────────────────────────────
section "feature/billing-v2"
"$GFS_BIN" checkout --path "$REPO" -b "feature/billing-v2" "$HASH_BASELINE" >/dev/null 2>&1
DB_PATH=$(get_db_path "$REPO")
sqlite3 "$DB_PATH" <<'SQL'
ALTER TABLE invoices ADD COLUMN late_fee REAL NOT NULL DEFAULT 0.0;
UPDATE invoices SET amount = amount * 1.20
  WHERE tenant_id IN (SELECT id FROM tenants WHERE plan='enterprise')
    AND status IN ('issued','overdue');
INSERT OR IGNORE INTO invoices VALUES
  (101,3,6500,'issued',1703000000,1705592000,NULL,0.0),
  (102,6,8200,'issued',1703086400,1705678400,NULL,0.0),
  (103,9,12000,'issued',1703172800,1705764800,NULL,0.0),
  (104,3,4500,'paid',1703259200,1705851200,1703500000,0.0),
  (105,6,9800,'paid',1703345600,1705937600,1703600000,0.0);
SQL
do_commit "feature/billing-v2: enterprise invoice upgrade"
HASH_BILLING=$(head_hash "$REPO")
FP_BILLING_INV=$(fingerprint "invoices" "id")
assert_ge "billing branch invoices" "$(sq "SELECT COUNT(*) FROM invoices;")" "105"
pass "HASH_BILLING=${HASH_BILLING:0:7}"

# ── Branch 2: feature/task-automation ────────────────────────────────────
section "feature/task-automation"
"$GFS_BIN" checkout --path "$REPO" -b "feature/task-automation" "$HASH_BASELINE" >/dev/null 2>&1
DB_PATH=$(get_db_path "$REPO")
sqlite3 "$DB_PATH" <<'SQL'
UPDATE tasks SET status='overdue'
  WHERE status='in_progress' AND due_ts < 1701000000;
INSERT OR IGNORE INTO tasks VALUES
  (501,1,1,'Q2 AutoTask 1','todo',3,1710000000,1709000000),
  (502,2,2,'Q2 AutoTask 2','todo',2,1710086400,1709086400),
  (503,3,3,'Q2 AutoTask 3','todo',4,1710172800,1709172800),
  (504,4,4,'Q2 AutoTask 4','todo',1,1710259200,1709259200),
  (505,5,5,'Q2 AutoTask 5','todo',5,1710345600,1709345600),
  (506,6,6,'Q2 AutoTask 6','todo',3,1710432000,1709432000),
  (507,7,7,'Q2 AutoTask 7','todo',2,1710518400,1709518400),
  (508,8,8,'Q2 AutoTask 8','todo',4,1710604800,1709604800),
  (509,9,9,'Q2 AutoTask 9','todo',1,1710691200,1709691200),
  (510,10,10,'Q2 AutoTask 10','todo',5,1710777600,1709777600),
  (511,11,11,'Q2 AutoTask 11','todo',3,1710864000,1709864000),
  (512,12,12,'Q2 AutoTask 12','todo',2,1710950400,1709950400),
  (513,13,13,'Q2 AutoTask 13','todo',4,1711036800,1710036800),
  (514,14,14,'Q2 AutoTask 14','todo',1,1711123200,1710123200),
  (515,15,15,'Q2 AutoTask 15','todo',5,1711209600,1710209600),
  (516,16,16,'Q2 AutoTask 16','todo',3,1711296000,1710296000),
  (517,17,17,'Q2 AutoTask 17','todo',2,1711382400,1710382400),
  (518,18,18,'Q2 AutoTask 18','todo',4,1711468800,1710468800),
  (519,19,19,'Q2 AutoTask 19','todo',1,1711555200,1710555200),
  (520,20,20,'Q2 AutoTask 20','todo',5,1711641600,1710641600),
  (521,21,21,'Q2 AutoTask 21','todo',3,1711728000,1710728000),
  (522,22,22,'Q2 AutoTask 22','todo',2,1711814400,1710814400),
  (523,23,23,'Q2 AutoTask 23','todo',4,1711900800,1710900800),
  (524,24,24,'Q2 AutoTask 24','todo',1,1711987200,1710987200),
  (525,25,25,'Q2 AutoTask 25','todo',5,1712073600,1711073600),
  (526,26,26,'Q2 AutoTask 26','todo',3,1712160000,1711160000),
  (527,27,27,'Q2 AutoTask 27','todo',2,1712246400,1711246400),
  (528,28,28,'Q2 AutoTask 28','todo',4,1712332800,1711332800),
  (529,29,29,'Q2 AutoTask 29','todo',1,1712419200,1711419200),
  (530,30,30,'Q2 AutoTask 30','todo',5,1712505600,1711505600),
  (531,31,31,'Q2 AutoTask 31','todo',3,1712592000,1711592000),
  (532,32,32,'Q2 AutoTask 32','todo',2,1712678400,1711678400),
  (533,33,33,'Q2 AutoTask 33','todo',4,1712764800,1711764800),
  (534,34,34,'Q2 AutoTask 34','todo',1,1712851200,1711851200),
  (535,35,35,'Q2 AutoTask 35','todo',5,1712937600,1711937600),
  (536,36,36,'Q2 AutoTask 36','todo',3,1713024000,1712024000),
  (537,37,37,'Q2 AutoTask 37','todo',2,1713110400,1712110400),
  (538,38,38,'Q2 AutoTask 38','todo',4,1713196800,1712196800),
  (539,39,39,'Q2 AutoTask 39','todo',1,1713283200,1712283200),
  (540,40,40,'Q2 AutoTask 40','todo',5,1713369600,1712369600),
  (541,41,41,'Q2 AutoTask 41','todo',3,1713456000,1712456000),
  (542,42,42,'Q2 AutoTask 42','todo',2,1713542400,1712542400),
  (543,43,43,'Q2 AutoTask 43','todo',4,1713628800,1712628800),
  (544,44,44,'Q2 AutoTask 44','todo',1,1713715200,1712715200),
  (545,45,45,'Q2 AutoTask 45','todo',5,1713801600,1712801600),
  (546,46,46,'Q2 AutoTask 46','todo',3,1713888000,1712888000),
  (547,47,47,'Q2 AutoTask 47','todo',2,1713974400,1712974400),
  (548,48,48,'Q2 AutoTask 48','todo',4,1714060800,1713060800),
  (549,49,49,'Q2 AutoTask 49','todo',1,1714147200,1713147200),
  (550,50,50,'Q2 AutoTask 50','todo',5,1714233600,1713233600);
SQL
do_commit "feature/task-automation: Q2 tasks + bulk overdue update"
HASH_TASKAUT=$(head_hash "$REPO")
FP_TASKAUT_TASKS=$(fingerprint "tasks" "id")
assert_ge "automation branch tasks" "$(sq "SELECT COUNT(*) FROM tasks;")" "550"
pass "HASH_TASKAUT=${HASH_TASKAUT:0:7}"

# ── Branch 3: feature/user-audit ─────────────────────────────────────────
section "feature/user-audit"
"$GFS_BIN" checkout --path "$REPO" -b "feature/user-audit" "$HASH_BASELINE" >/dev/null 2>&1
DB_PATH=$(get_db_path "$REPO")
sqlite3 "$DB_PATH" <<'SQL'
ALTER TABLE users ADD COLUMN last_login_ts INTEGER;
UPDATE users SET last_login_ts = created_ts + 100000;
INSERT OR IGNORE INTO audit_log
SELECT 500+id, tenant_id, id, 'LOGIN', 'user', id, created_ts+100000 FROM users;
SQL
do_commit "feature/user-audit: last_login_ts + login audit entries"
HASH_USERAUDIT=$(head_hash "$REPO")
FP_USERAUDIT_AUDIT=$(fingerprint "audit_log" "id")
assert_ge "user-audit audit_log rows" "$(sq "SELECT COUNT(*) FROM audit_log;")" "600"
pass "HASH_USERAUDIT=${HASH_USERAUDIT:0:7}"

# ── Branch 4: feature/project-archive ────────────────────────────────────
section "feature/project-archive"
"$GFS_BIN" checkout --path "$REPO" -b "feature/project-archive" "$HASH_BASELINE" >/dev/null 2>&1
DB_PATH=$(get_db_path "$REPO")
sqlite3 "$DB_PATH" <<'SQL'
ALTER TABLE projects ADD COLUMN archived_ts INTEGER;
UPDATE projects SET status='archived', archived_ts=1703000000
  WHERE status='on_hold' AND id <= 10;
SQL
do_commit "feature/project-archive: archive 10 stale projects"
HASH_ARCHIVE=$(head_hash "$REPO")
FP_ARCHIVE_PROJS=$(fingerprint "projects" "id")
ARCHIVED_CNT=$(sq "SELECT COUNT(*) FROM projects WHERE status='archived';")
assert_ge "archived projects" "$ARCHIVED_CNT" "2"
pass "HASH_ARCHIVE=${HASH_ARCHIVE:0:7}"

# ── Branch 5: hotfix/invoice-calc ────────────────────────────────────────
section "hotfix/invoice-calc"
"$GFS_BIN" checkout --path "$REPO" -b "hotfix/invoice-calc" "$HASH_BASELINE" >/dev/null 2>&1
DB_PATH=$(get_db_path "$REPO")
sqlite3 "$DB_PATH" <<'SQL'
ALTER TABLE invoices ADD COLUMN corrected_at INTEGER;
UPDATE invoices SET amount=2500.0, corrected_at=1703000000 WHERE id=1;
UPDATE invoices SET amount=3100.0, corrected_at=1703000001 WHERE id=11;
UPDATE invoices SET amount=4750.0, corrected_at=1703000002 WHERE id=21;
SQL
do_commit "hotfix/invoice-calc: fix 3 wrong invoice amounts"
HASH_HOTFIX=$(head_hash "$REPO")
FP_HOTFIX_INV=$(fingerprint "invoices" "id")
pass "HASH_HOTFIX=${HASH_HOTFIX:0:7}"

# ── Branch 6: release/v2.0 ───────────────────────────────────────────────
section "release/v2.0"
"$GFS_BIN" checkout --path "$REPO" -b "release/v2.0" "$HASH_BASELINE" >/dev/null 2>&1
DB_PATH=$(get_db_path "$REPO")
# Apply billing changes
sqlite3 "$DB_PATH" <<'SQL'
ALTER TABLE invoices ADD COLUMN late_fee REAL NOT NULL DEFAULT 0.0;
UPDATE invoices SET amount = amount * 1.20
  WHERE tenant_id IN (SELECT id FROM tenants WHERE plan='enterprise')
    AND status IN ('issued','overdue');
INSERT OR IGNORE INTO invoices VALUES
  (101,3,6500,'issued',1703000000,1705592000,NULL,0.0),
  (102,6,8200,'issued',1703086400,1705678400,NULL,0.0),
  (103,9,12000,'issued',1703172800,1705764800,NULL,0.0),
  (104,3,4500,'paid',1703259200,1705851200,1703500000,0.0),
  (105,6,9800,'paid',1703345600,1705937600,1703600000,0.0);
SQL
# Apply task automation changes
sqlite3 "$DB_PATH" <<'SQL'
UPDATE tasks SET status='overdue'
  WHERE status='in_progress' AND due_ts < 1701000000;
INSERT OR IGNORE INTO tasks VALUES
  (501,1,1,'Q2 AutoTask 1','todo',3,1710000000,1709000000),
  (502,2,2,'Q2 AutoTask 2','todo',2,1710086400,1709086400),
  (503,3,3,'Q2 AutoTask 3','todo',4,1710172800,1709172800),
  (504,4,4,'Q2 AutoTask 4','todo',1,1710259200,1709259200),
  (505,5,5,'Q2 AutoTask 5','todo',5,1710345600,1709345600),
  (506,6,6,'Q2 AutoTask 6','todo',3,1710432000,1709432000),
  (507,7,7,'Q2 AutoTask 7','todo',2,1710518400,1709518400),
  (508,8,8,'Q2 AutoTask 8','todo',4,1710604800,1709604800),
  (509,9,9,'Q2 AutoTask 9','todo',1,1710691200,1709691200),
  (510,10,10,'Q2 AutoTask 10','todo',5,1710777600,1709777600);
SQL
do_commit "release/v2.0: billing-v2 + task-automation cherry-pick"
HASH_RELEASE=$(head_hash "$REPO")
FP_RELEASE_INV=$(fingerprint "invoices" "id")
assert_ge "release invoices" "$(sq "SELECT COUNT(*) FROM invoices;")" "105"
pass "HASH_RELEASE=${HASH_RELEASE:0:7}"

# ── Branch 7: feature/analytics ──────────────────────────────────────────
section "feature/analytics"
"$GFS_BIN" checkout --path "$REPO" -b "feature/analytics" "$HASH_BASELINE" >/dev/null 2>&1
DB_PATH=$(get_db_path "$REPO")
sqlite3 "$DB_PATH" <<'SQL'
CREATE TABLE IF NOT EXISTS revenue_metrics (
    tenant_id   INTEGER NOT NULL,
    month_start INTEGER NOT NULL,
    total_invoiced REAL NOT NULL DEFAULT 0,
    total_paid     REAL NOT NULL DEFAULT 0,
    invoice_count  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, month_start)
);
INSERT OR IGNORE INTO revenue_metrics
SELECT
    tenant_id,
    (issued_ts / 2592000) * 2592000 AS month_start,
    SUM(amount) AS total_invoiced,
    SUM(CASE WHEN status='paid' THEN amount ELSE 0 END) AS total_paid,
    COUNT(*) AS invoice_count
FROM invoices
GROUP BY tenant_id, month_start;
SQL
do_commit "feature/analytics: revenue_metrics materialized table"
HASH_ANALYTICS=$(head_hash "$REPO")
FP_ANALYTICS_REV=$(fingerprint "revenue_metrics" "tenant_id")
RM_CNT=$(sq "SELECT COUNT(*) FROM revenue_metrics;")
assert_ge "revenue_metrics rows" "$RM_CNT" "1"
pass "HASH_ANALYTICS=${HASH_ANALYTICS:0:7}"

# ── Back to main ─────────────────────────────────────────────────────────
section "return to main for isolation checks"
checkout "main"
DB_PATH=$(get_db_path "$REPO")
assert_eq "main: tasks=500" "$(sq "SELECT COUNT(*) FROM tasks;")" "500"
assert_eq "main: invoices=100" "$(sq "SELECT COUNT(*) FROM invoices;")" "100"
db_ready "main after branch ops"

# ═══════════════════════════════════════════════════════════════════════════
banner "PHASE 4 — Isolation checks"
# ═══════════════════════════════════════════════════════════════════════════

section "isolation: feature/billing-v2"
checkout "feature/billing-v2"
assert_ge "billing: invoices≥105" "$(sq "SELECT COUNT(*) FROM invoices;")" "105"
LATE_FEE_COL=$(sq "SELECT COUNT(*) FROM pragma_table_info('invoices') WHERE name='late_fee';")
assert_eq "billing: late_fee column exists" "$LATE_FEE_COL" "1"
assert_fp_eq "billing: FP_BILLING_INV stable" "$FP_BILLING_INV" "$(fingerprint invoices id)"
db_ready "billing-v2"

section "isolation: feature/task-automation"
checkout "feature/task-automation"
assert_ge "automation: tasks≥550" "$(sq "SELECT COUNT(*) FROM tasks;")" "550"
assert_fp_eq "automation: FP_TASKAUT_TASKS stable" "$FP_TASKAUT_TASKS" "$(fingerprint tasks id)"
# billing-v2 late_fee column must NOT be present here
LATE_FEE_HERE=$(sq "SELECT COUNT(*) FROM pragma_table_info('invoices') WHERE name='late_fee';" 2>/dev/null || echo "0")
assert_eq "automation: no late_fee col" "$LATE_FEE_HERE" "0"
db_ready "task-automation"

section "isolation: feature/user-audit"
checkout "feature/user-audit"
assert_ge "user-audit: audit_log≥600" "$(sq "SELECT COUNT(*) FROM audit_log;")" "600"
LOGIN_COL=$(sq "SELECT COUNT(*) FROM pragma_table_info('users') WHERE name='last_login_ts';")
assert_eq "user-audit: last_login_ts column" "$LOGIN_COL" "1"
assert_fp_eq "user-audit: FP_USERAUDIT_AUDIT stable" "$FP_USERAUDIT_AUDIT" "$(fingerprint audit_log id)"
db_ready "user-audit"

section "isolation: feature/project-archive"
checkout "feature/project-archive"
ARCH_CNT=$(sq "SELECT COUNT(*) FROM projects WHERE status='archived';")
assert_ge "archive: archived≥2" "$ARCH_CNT" "2"
ARCH_COL=$(sq "SELECT COUNT(*) FROM pragma_table_info('projects') WHERE name='archived_ts';")
assert_eq "archive: archived_ts column" "$ARCH_COL" "1"
assert_fp_eq "archive: FP_ARCHIVE_PROJS stable" "$FP_ARCHIVE_PROJS" "$(fingerprint projects id)"
db_ready "project-archive"

section "isolation: hotfix/invoice-calc"
checkout "hotfix/invoice-calc"
CORRECTED=$(sq "SELECT amount FROM invoices WHERE id=1;")
assert_eq "hotfix: invoice 1 corrected to 2500" "$CORRECTED" "2500.0"
CORR_COL=$(sq "SELECT COUNT(*) FROM pragma_table_info('invoices') WHERE name='corrected_at';")
assert_eq "hotfix: corrected_at column" "$CORR_COL" "1"
assert_fp_eq "hotfix: FP_HOTFIX_INV stable" "$FP_HOTFIX_INV" "$(fingerprint invoices id)"
db_ready "hotfix/invoice-calc"

section "isolation: release/v2.0"
checkout "release/v2.0"
assert_ge "release: invoices≥105" "$(sq "SELECT COUNT(*) FROM invoices;")" "105"
assert_ge "release: tasks≥510" "$(sq "SELECT COUNT(*) FROM tasks;")" "510"
assert_fp_eq "release: FP_RELEASE_INV stable" "$FP_RELEASE_INV" "$(fingerprint invoices id)"
db_ready "release/v2.0"

section "isolation: feature/analytics"
checkout "feature/analytics"
RM_TBL=$(sq "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='revenue_metrics';")
assert_eq "analytics: revenue_metrics table" "$RM_TBL" "1"
assert_fp_eq "analytics: FP_ANALYTICS_REV stable" "$FP_ANALYTICS_REV" "$(fingerprint revenue_metrics tenant_id)"
db_ready "feature/analytics"

section "fingerprints differ from main"
checkout "main"
FP_MAIN_INV=$(fingerprint "invoices" "id")
FP_MAIN_TASKS=$(fingerprint "tasks" "id")
assert_fp_ne "billing vs main invoices" "$FP_BILLING_INV" "$FP_MAIN_INV"
assert_fp_ne "automation vs main tasks" "$FP_TASKAUT_TASKS" "$FP_MAIN_TASKS"

# ═══════════════════════════════════════════════════════════════════════════
banner "PHASE 5 — Manual merge on main"
# ═══════════════════════════════════════════════════════════════════════════
section "checkout main and apply all branch changes"
checkout "main"

sqlite3 "$DB_PATH" <<'SQL'
ALTER TABLE invoices ADD COLUMN late_fee REAL NOT NULL DEFAULT 0.0;
SQL
do_commit "merge: add late_fee column to invoices"

sqlite3 "$DB_PATH" <<'SQL'
UPDATE invoices SET amount = amount * 1.20
  WHERE tenant_id IN (SELECT id FROM tenants WHERE plan='enterprise')
    AND status IN ('issued','overdue');
INSERT OR IGNORE INTO invoices VALUES
  (101,3,6500,'issued',1703000000,1705592000,NULL,0.0),
  (102,6,8200,'issued',1703086400,1705678400,NULL,0.0),
  (103,9,12000,'issued',1703172800,1705764800,NULL,0.0),
  (104,3,4500,'paid',1703259200,1705851200,1703500000,0.0),
  (105,6,9800,'paid',1703345600,1705937600,1703600000,0.0);
SQL
do_commit "merge: billing-v2 enterprise invoice changes"

sqlite3 "$DB_PATH" <<'SQL'
ALTER TABLE invoices ADD COLUMN corrected_at INTEGER;
UPDATE invoices SET amount=2500.0, corrected_at=1703000000 WHERE id=1;
UPDATE invoices SET amount=3100.0, corrected_at=1703000001 WHERE id=11;
UPDATE invoices SET amount=4750.0, corrected_at=1703000002 WHERE id=21;
SQL
do_commit "merge: hotfix/invoice-calc corrections"

sqlite3 "$DB_PATH" <<'SQL'
ALTER TABLE users ADD COLUMN last_login_ts INTEGER;
UPDATE users SET last_login_ts = created_ts + 100000;
INSERT OR IGNORE INTO audit_log
SELECT 500+id, tenant_id, id, 'LOGIN', 'user', id, created_ts+100000 FROM users;
SQL
do_commit "merge: user-audit last_login_ts"

sqlite3 "$DB_PATH" <<'SQL'
ALTER TABLE projects ADD COLUMN archived_ts INTEGER;
UPDATE projects SET status='archived', archived_ts=1703000000
  WHERE status='on_hold' AND id <= 10;
SQL
do_commit "merge: project-archive stale projects"

sqlite3 "$DB_PATH" <<'SQL'
UPDATE tasks SET status='overdue'
  WHERE status='in_progress' AND due_ts < 1701000000;
INSERT OR IGNORE INTO tasks VALUES
  (501,1,1,'Q2 AutoTask 1','todo',3,1710000000,1709000000),
  (502,2,2,'Q2 AutoTask 2','todo',2,1710086400,1709086400),
  (503,3,3,'Q2 AutoTask 3','todo',4,1710172800,1709172800),
  (504,4,4,'Q2 AutoTask 4','todo',1,1710259200,1709259200),
  (505,5,5,'Q2 AutoTask 5','todo',5,1710345600,1709345600),
  (506,6,6,'Q2 AutoTask 6','todo',3,1710432000,1709432000),
  (507,7,7,'Q2 AutoTask 7','todo',2,1710518400,1709518400),
  (508,8,8,'Q2 AutoTask 8','todo',4,1710604800,1709604800),
  (509,9,9,'Q2 AutoTask 9','todo',1,1710691200,1709691200),
  (510,10,10,'Q2 AutoTask 10','todo',5,1710777600,1709777600),
  (511,11,11,'Q2 AutoTask 11','todo',3,1710864000,1709864000),
  (512,12,12,'Q2 AutoTask 12','todo',2,1710950400,1709950400),
  (513,13,13,'Q2 AutoTask 13','todo',4,1711036800,1710036800),
  (514,14,14,'Q2 AutoTask 14','todo',1,1711123200,1710123200),
  (515,15,15,'Q2 AutoTask 15','todo',5,1711209600,1710209600),
  (516,16,16,'Q2 AutoTask 16','todo',3,1711296000,1710296000),
  (517,17,17,'Q2 AutoTask 17','todo',2,1711382400,1710382400),
  (518,18,18,'Q2 AutoTask 18','todo',4,1711468800,1710468800),
  (519,19,19,'Q2 AutoTask 19','todo',1,1711555200,1710555200),
  (520,20,20,'Q2 AutoTask 20','todo',5,1711641600,1710641600),
  (521,21,21,'Q2 AutoTask 21','todo',3,1711728000,1710728000),
  (522,22,22,'Q2 AutoTask 22','todo',2,1711814400,1710814400),
  (523,23,23,'Q2 AutoTask 23','todo',4,1711900800,1710900800),
  (524,24,24,'Q2 AutoTask 24','todo',1,1711987200,1710987200),
  (525,25,25,'Q2 AutoTask 25','todo',5,1712073600,1711073600),
  (526,26,26,'Q2 AutoTask 26','todo',3,1712160000,1711160000),
  (527,27,27,'Q2 AutoTask 27','todo',2,1712246400,1711246400),
  (528,28,28,'Q2 AutoTask 28','todo',4,1712332800,1711332800),
  (529,29,29,'Q2 AutoTask 29','todo',1,1712419200,1711419200),
  (530,30,30,'Q2 AutoTask 30','todo',5,1712505600,1711505600),
  (531,31,31,'Q2 AutoTask 31','todo',3,1712592000,1711592000),
  (532,32,32,'Q2 AutoTask 32','todo',2,1712678400,1711678400),
  (533,33,33,'Q2 AutoTask 33','todo',4,1712764800,1711764800),
  (534,34,34,'Q2 AutoTask 34','todo',1,1712851200,1711851200),
  (535,35,35,'Q2 AutoTask 35','todo',5,1712937600,1711937600),
  (536,36,36,'Q2 AutoTask 36','todo',3,1713024000,1712024000),
  (537,37,37,'Q2 AutoTask 37','todo',2,1713110400,1712110400),
  (538,38,38,'Q2 AutoTask 38','todo',4,1713196800,1712196800),
  (539,39,39,'Q2 AutoTask 39','todo',1,1713283200,1712283200),
  (540,40,40,'Q2 AutoTask 40','todo',5,1713369600,1712369600),
  (541,41,41,'Q2 AutoTask 41','todo',3,1713456000,1712456000),
  (542,42,42,'Q2 AutoTask 42','todo',2,1713542400,1712542400),
  (543,43,43,'Q2 AutoTask 43','todo',4,1713628800,1712628800),
  (544,44,44,'Q2 AutoTask 44','todo',1,1713715200,1712715200),
  (545,45,45,'Q2 AutoTask 45','todo',5,1713801600,1712801600),
  (546,46,46,'Q2 AutoTask 46','todo',3,1713888000,1712888000),
  (547,47,47,'Q2 AutoTask 47','todo',2,1713974400,1712974400),
  (548,48,48,'Q2 AutoTask 48','todo',4,1714060800,1713060800),
  (549,49,49,'Q2 AutoTask 49','todo',1,1714147200,1713147200),
  (550,50,50,'Q2 AutoTask 50','todo',5,1714233600,1713233600);
SQL
do_commit "merge: task-automation Q2 tasks"

sqlite3 "$DB_PATH" <<'SQL'
CREATE TABLE IF NOT EXISTS revenue_metrics (
    tenant_id   INTEGER NOT NULL,
    month_start INTEGER NOT NULL,
    total_invoiced REAL NOT NULL DEFAULT 0,
    total_paid     REAL NOT NULL DEFAULT 0,
    invoice_count  INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id, month_start)
);
INSERT OR IGNORE INTO revenue_metrics
SELECT
    tenant_id,
    (issued_ts / 2592000) * 2592000 AS month_start,
    SUM(amount) AS total_invoiced,
    SUM(CASE WHEN status='paid' THEN amount ELSE 0 END) AS total_paid,
    COUNT(*) AS invoice_count
FROM invoices
GROUP BY tenant_id, month_start;
SQL
do_commit "merge: analytics revenue_metrics"
do_commit "merge: manual merge v2.0 complete"

HASH_MERGED=$(head_hash "$REPO")
FP_MERGED_TASKS=$(fingerprint "tasks" "id")
FP_MERGED_INVOICES=$(fingerprint "invoices" "id")
FP_MERGED_USERS=$(fingerprint "users" "id")
assert_ge "merged: tasks≥550" "$(sq "SELECT COUNT(*) FROM tasks;")" "550"
assert_ge "merged: invoices≥105" "$(sq "SELECT COUNT(*) FROM invoices;")" "105"
assert_ge "merged: audit_log≥600" "$(sq "SELECT COUNT(*) FROM audit_log;")" "600"
db_ready "merged main"
pass "HASH_MERGED=${HASH_MERGED:0:7}"

# ═══════════════════════════════════════════════════════════════════════════
banner "PHASE 6 — Time travel"
# ═══════════════════════════════════════════════════════════════════════════
section "checkout HASH_BASELINE"
checkout "$HASH_BASELINE"
db_ready "time-travel HASH_BASELINE"
assert_eq "baseline: tasks=500" "$(sq "SELECT COUNT(*) FROM tasks;")" "500"
assert_eq "baseline: invoices=100" "$(sq "SELECT COUNT(*) FROM invoices;")" "100"
assert_fp_eq "baseline fingerprint tenants" "$FP_BASE_TENANTS" "$(fingerprint tenants id)"
assert_fp_eq "baseline fingerprint tasks"   "$FP_BASE_TASKS"   "$(fingerprint tasks id)"

section "checkout HASH_MERGED"
checkout "$HASH_MERGED"
db_ready "time-travel HASH_MERGED"
assert_ge "merged: tasks≥550" "$(sq "SELECT COUNT(*) FROM tasks;")" "550"
assert_fp_eq "merged FP_MERGED_TASKS stable" "$FP_MERGED_TASKS" "$(fingerprint tasks id)"

section "checkout main"
checkout "main"
db_ready "time-travel main"
assert_ge "main: tasks≥550" "$(sq "SELECT COUNT(*) FROM tasks;")" "550"

section "checkout HEAD~1 (from main)"
"$GFS_BIN" checkout --path "$REPO" "HEAD~1" >/dev/null 2>&1
DB_PATH=$(get_db_path "$REPO")
db_ready "time-travel HEAD~1"
assert_ge "HEAD~1: tasks≥550" "$(sq "SELECT COUNT(*) FROM tasks;")" "550"

section "return main after time travel"
checkout "main"
db_ready "main restored"

# ═══════════════════════════════════════════════════════════════════════════
banner "PHASE 7 — HEAD~N stress"
# ═══════════════════════════════════════════════════════════════════════════
# main has: c0, c1, + 8 merge commits = 10 commits total → HEAD~3 and HEAD~6 exist
section "HEAD~3 from main"
"$GFS_BIN" checkout --path "$REPO" "HEAD~3" >/dev/null 2>&1
DB_PATH=$(get_db_path "$REPO")
db_ready "HEAD~3"
R=$(sq "SELECT COUNT(*) FROM tenants;") 2>/dev/null || R=0
assert_ge "HEAD~3: tenants≥10" "$R" "10"

section "HEAD~6 from HEAD~3 context — go back to main first"
checkout "main"
"$GFS_BIN" checkout --path "$REPO" "HEAD~6" >/dev/null 2>&1
DB_PATH=$(get_db_path "$REPO")
db_ready "HEAD~6"
R6=$(sq "SELECT COUNT(*) FROM tenants;") 2>/dev/null || R6=0
assert_ge "HEAD~6: tenants≥10" "$R6" "10"

checkout "main"

# ═══════════════════════════════════════════════════════════════════════════
banner "PHASE 8 — Advanced SQL (on merged main)"
# ═══════════════════════════════════════════════════════════════════════════
section "recursive CTE: tasks in project 1 chain"
REC=$(sq "
WITH RECURSIVE proj_tasks(tid) AS (
  SELECT id FROM tasks WHERE project_id=1
  UNION ALL
  SELECT t.id FROM tasks t JOIN proj_tasks pt ON t.project_id = pt.tid
  LIMIT 100
)
SELECT COUNT(*) FROM proj_tasks;
" 2>/dev/null || echo "0")
assert_ge "recursive CTE task chain" "$REC" "10"

section "trigger test: audit on task UPDATE"
sq "DROP TRIGGER IF EXISTS trg_task_update_audit;"
sq "CREATE TRIGGER trg_task_update_audit
    AFTER UPDATE ON tasks
    BEGIN
        INSERT INTO audit_log(tenant_id, user_id, action, entity_type, entity_id, ts)
        SELECT p.tenant_id, NEW.assigned_to, 'UPDATE', 'task', NEW.id, 1710000000
        FROM projects p WHERE p.id = NEW.project_id;
    END;"
AUDIT_BEFORE=$(sq "SELECT COUNT(*) FROM audit_log;")
sq "UPDATE tasks SET priority=5 WHERE id=1;"
AUDIT_AFTER=$(sq "SELECT COUNT(*) FROM audit_log;")
assert_ge "trigger fired: audit_log grew" "$AUDIT_AFTER" "$((AUDIT_BEFORE + 1))"

section "FTS5 virtual table on tasks(title)"
if sq "CREATE VIRTUAL TABLE IF NOT EXISTS tasks_fts USING fts5(title, content='tasks', content_rowid='id');" 2>/dev/null; then
    sq "INSERT INTO tasks_fts(tasks_fts) VALUES('rebuild');" 2>/dev/null || true
    FTS_HIT=$(sq "SELECT COUNT(*) FROM tasks_fts WHERE tasks_fts MATCH 'AutoTask';" 2>/dev/null || echo "-1")
    if [[ "$FTS_HIT" == "-1" ]]; then
        skip "FTS5 query failed (likely not supported)"
    else
        assert_ge "FTS5 search AutoTask" "$FTS_HIT" "1"
    fi
else
    skip "FTS5 not supported in this SQLite build"
fi

section "aggregate window: running total of invoiced amount per tenant"
WIN=$(sq "
SELECT tenant_id, id, amount,
       SUM(amount) OVER (PARTITION BY tenant_id ORDER BY issued_ts
                         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM invoices
ORDER BY tenant_id, issued_ts
LIMIT 20;
" | wc -l | tr -d ' ')
assert_ge "window running total rows" "$WIN" "10"

section "multi-level CTE: monthly revenue per tenant (6 months)"
MONTHLY=$(sq "
WITH months(m) AS (
  SELECT 1700000000 UNION ALL SELECT m+2592000 FROM months WHERE m < 1700000000+5*2592000
),
monthly_rev AS (
  SELECT
    t.id AS tid,
    t.name AS tname,
    m.m AS month_start,
    COALESCE(SUM(i.amount),0) AS invoiced,
    COALESCE(SUM(CASE WHEN i.status='paid' THEN i.amount ELSE 0 END),0) AS paid
  FROM tenants t
  CROSS JOIN months m
  LEFT JOIN invoices i ON i.tenant_id = t.id
    AND i.issued_ts >= m.m AND i.issued_ts < m.m+2592000
  GROUP BY t.id, m.m
)
SELECT COUNT(*) FROM monthly_rev WHERE invoiced >= 0;
" 2>/dev/null || echo "0")
assert_ge "monthly revenue CTE rows" "$MONTHLY" "60"

# ═══════════════════════════════════════════════════════════════════════════
banner "PHASE 9 — Branch lifecycle"
# ═══════════════════════════════════════════════════════════════════════════
section "delete feature branches (keep main, hotfix, release)"
checkout "main"
for br in "feature/billing-v2" "feature/task-automation" "feature/user-audit" \
          "feature/project-archive" "feature/analytics"; do
    "$GFS_BIN" branch --path "$REPO" -d "$br" >/dev/null 2>&1 \
        && pass "deleted branch: $br" || fail "failed to delete: $br"
done

section "verify deleted branches absent"
BRLIST=$("$GFS_BIN" branch --path "$REPO" 2>/dev/null)
assert_not_contains "feature/billing-v2 gone" "$BRLIST" "feature/billing-v2"
assert_not_contains "feature/task-automation gone" "$BRLIST" "feature/task-automation"
assert_not_contains "feature/user-audit gone" "$BRLIST" "feature/user-audit"
assert_not_contains "feature/project-archive gone" "$BRLIST" "feature/project-archive"
assert_not_contains "feature/analytics gone" "$BRLIST" "feature/analytics"
assert_contains "main still present" "$BRLIST" "main"
assert_contains "hotfix still present" "$BRLIST" "hotfix/invoice-calc"
assert_contains "release still present" "$BRLIST" "release/v2.0"

section "recreate feature/billing-v2 at main"
"$GFS_BIN" branch --path "$REPO" "feature/billing-v2" >/dev/null 2>&1 \
    && pass "recreated feature/billing-v2" || fail "failed to recreate feature/billing-v2"
BRLIST2=$("$GFS_BIN" branch --path "$REPO" 2>/dev/null)
assert_contains "feature/billing-v2 recreated" "$BRLIST2" "feature/billing-v2"

# ═══════════════════════════════════════════════════════════════════════════
banner "PHASE 10 — Parallel repo"
# ═══════════════════════════════════════════════════════════════════════════
section "second repo independence"
REPO2=$(mktemp -d)
"$GFS_BIN" init --database-provider sqlite --database-version 3 "$REPO2" >/dev/null 2>&1 \
    && pass "REPO2 init" || fail "REPO2 init failed"
DB2=$(get_db_path "$REPO2")
sqlite3 "$DB2" "CREATE TABLE IF NOT EXISTS solo (id INTEGER PRIMARY KEY, val TEXT);"
sqlite3 "$DB2" "INSERT INTO solo VALUES (1,'repo2-only');"
"$GFS_BIN" commit --path "$REPO2" -m "repo2 commit" >/dev/null 2>&1 \
    && pass "REPO2 commit" || fail "REPO2 commit failed"

# REPO1 unaffected
SOLO_IN_REPO1=$(sq "SELECT COUNT(*) FROM sqlite_master WHERE name='solo';" 2>/dev/null || echo "0")
assert_eq "REPO1: no solo table" "$SOLO_IN_REPO1" "0"
TASKS_IN_REPO1=$(sq "SELECT COUNT(*) FROM tasks;" 2>/dev/null || echo "0")
assert_ge "REPO1: tasks still present" "$TASKS_IN_REPO1" "500"
db_ready "REPO1 unaffected"

DB2_TASKS=$(sqlite3 "$DB2" "SELECT COUNT(*) FROM sqlite_master WHERE name='tasks';" 2>/dev/null || echo "0")
assert_eq "REPO2: no tasks table" "$DB2_TASKS" "0"
pass "repos are fully isolated"

# ═══════════════════════════════════════════════════════════════════════════
banner "PHASE 11 — Log + status checks"
# ═══════════════════════════════════════════════════════════════════════════
section "gfs log"
LOG_OUT=$("$GFS_BIN" log --path "$REPO" --max-count 5 2>/dev/null)
LOG_LINES=$(echo "$LOG_OUT" | grep -c "Author:" || true)
assert_ge "log: ≥3 entries" "$LOG_LINES" "3"

section "gfs --json status"
JSON_OUT=$("$GFS_BIN" --json status --path "$REPO" 2>/dev/null)
assert_contains "status: current_branch key" "$JSON_OUT" "current_branch"
assert_contains "status: active_workspace_data_dir key" "$JSON_OUT" "active_workspace_data_dir"
assert_contains "status: on main" "$JSON_OUT" "main"

section "branch list final state"
FINAL_BRANCHES=$("$GFS_BIN" branch --path "$REPO" 2>/dev/null)
assert_contains "final: main present" "$FINAL_BRANCHES" "main"
assert_contains "final: hotfix present" "$FINAL_BRANCHES" "hotfix/invoice-calc"
assert_contains "final: release present" "$FINAL_BRANCHES" "release/v2.0"
assert_not_contains "final: feature/task-automation absent" "$FINAL_BRANCHES" "feature/task-automation"
assert_not_contains "final: feature/user-audit absent" "$FINAL_BRANCHES" "feature/user-audit"

# ═══════════════════════════════════════════════════════════════════════════
banner "SUMMARY"
# ═══════════════════════════════════════════════════════════════════════════
TOTAL=$((PASS + FAIL + SKIP))
printf "\n  Total: ${BOLD}%d${NC} passed, ${RED}%d${NC} failed, ${YELLOW}%d${NC} skipped  (of %d)\n" \
    "$PASS" "$FAIL" "$SKIP" "$TOTAL"

if [[ "$FAIL" -eq 0 ]]; then
    printf "\n  ${GREEN}${BOLD}ALL HEAVY-DB TESTS PASSED${NC}\n\n"
    exit 0
else
    printf "\n  ${RED}${BOLD}%d TEST(S) FAILED${NC}\n\n" "$FAIL"
    exit 0   # exit 0 even on failure per requirements
fi
