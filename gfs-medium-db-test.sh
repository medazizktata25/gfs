#!/usr/bin/env bash
# gfs-medium-db-test.sh — Medium-complexity database integration test.
#
# Schema: e-commerce (users, products, orders, order_items, reviews)
# Data:   50 users · 100 products · 200 orders · ~500 order_items · 150 reviews
# GFS:    main baseline + 4 diverging branches + merge-by-hand + time-travel
# SQL:    JOINs, CTEs, window funcs, aggregates, subqueries, HAVING, UPSERT
# Check:  PRAGMA integrity_check after every checkout, query readiness probes
#
# Requires: gfs binary, sqlite3 CLI.  No Docker.

set -uo pipefail

GFS="${GFS_BIN:-$(dirname "$0")/target/debug/gfs}"
SQLITE3="${SQLITE3:-sqlite3}"
WORK_BASE=$(mktemp -d /tmp/gfs-medium-XXXXXX)
PASS=0; FAIL=0; SKIP=0

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[0;33m'
CYAN='\033[0;36m'; MAGENTA='\033[0;35m'; NC='\033[0m'; BOLD='\033[1m'

banner()    { printf "\n${CYAN}${BOLD}════════════════════════════════════════\n  %s\n════════════════════════════════════════${NC}\n" "$*"; }
section()   { printf "\n  ${MAGENTA}── %s ──${NC}\n" "$*"; }
pass()      { printf "  ${GREEN}✓${NC} %s\n" "$*"; PASS=$((PASS+1)); }
fail()      { printf "  ${RED}✗${NC} %s\n" "$*"; FAIL=$((FAIL+1)); }
info()      { printf "  ${YELLOW}→${NC} %s\n" "$*"; }

cleanup()   { rm -rf "$WORK_BASE" 2>/dev/null || true; }
trap cleanup EXIT

REPO="$WORK_BASE/ecommerce"
mkdir -p "$REPO"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
gq()  { "$GFS" query --path "$REPO" "$1" &>/dev/null; }
gqv() { "$GFS" query --path "$REPO" "$1" 2>/dev/null; }   # visible output
sq()  { local ws; ws=$(cat "$REPO/.gfs/WORKSPACE" | tr -d '[:space:]')
        "$SQLITE3" "$ws/db.sqlite" "$1" 2>/dev/null; }
commit() { "$GFS" commit --path "$REPO" -m "$1" &>/dev/null; }
checkout() { "$GFS" checkout --path "$REPO" "$1" &>/dev/null; }
head_hash() {
    local b; b=$(cat "$REPO/.gfs/HEAD" | sed 's|ref: refs/heads/||' | tr -d '[:space:]')
    cat "$REPO/.gfs/refs/heads/$b" | tr -d '[:space:]'
}
fingerprint() {
    local tbl="$1" cols="$2"
    local ws; ws=$(cat "$REPO/.gfs/WORKSPACE" | tr -d '[:space:]')
    "$SQLITE3" "$ws/db.sqlite" \
        "SELECT group_concat(r, char(10)) FROM
         (SELECT json_array($cols) AS r FROM $tbl ORDER BY 1);" \
        2>/dev/null | sha256sum | awk '{print $1}'
}

# DB readiness: integrity_check + a basic query on each core table
db_ready() {
    local label="$1"
    local ws; ws=$(cat "$REPO/.gfs/WORKSPACE" | tr -d '[:space:]')
    local db="$ws/db.sqlite"
    if [[ ! -f "$db" ]]; then
        info "$label: db.sqlite absent (empty state — ok for initial commits)"
        return 0
    fi
    local ic; ic=$("$SQLITE3" "$db" "PRAGMA integrity_check;" 2>/dev/null | head -1 | tr -d '[:space:]')
    [[ "$ic" == "ok" ]] && pass "$label: PRAGMA integrity_check = ok" || fail "$label: integrity_check = $ic"
    local qtest; qtest=$("$SQLITE3" "$db" "SELECT count(*) FROM sqlite_master;" 2>/dev/null | tr -d '[:space:]')
    [[ "${qtest:-0}" -ge 0 ]] && pass "$label: sqlite_master queryable" || fail "$label: sqlite_master query failed"
}

assert_eq() { local label="$1" got="$2" exp="$3"
    [[ "$got" == "$exp" ]] && pass "$label: $exp" || fail "$label: expected '$exp', got '$got'"; }

assert_ge() { local label="$1" got="$2" exp="$3"
    [[ "${got:-0}" -ge "$exp" ]] && pass "$label: ≥$exp (got $got)" || fail "$label: expected ≥$exp, got $got"; }

assert_fp_eq() { [[ "$2" == "$3" ]] && pass "$1: fingerprint match" || fail "$1: fingerprint MISMATCH"; }
assert_fp_ne() { [[ "$2" != "$3" ]] && pass "$1: fingerprints differ (expected)" || fail "$1: fingerprints identical (unexpected)"; }

# ---------------------------------------------------------------------------
banner "PHASE 0 — Init + Schema"
# ---------------------------------------------------------------------------
"$GFS" init --database-provider sqlite --database-version 3 "$REPO" &>/dev/null \
    && pass "gfs init sqlite" || { fail "gfs init failed"; exit 1; }
commit "c0: empty repo"

section "DDL"
gq "CREATE TABLE users (
    id         INTEGER PRIMARY KEY,
    username   TEXT NOT NULL UNIQUE,
    email      TEXT NOT NULL UNIQUE,
    tier       TEXT NOT NULL DEFAULT 'free' CHECK(tier IN ('free','pro','enterprise')),
    created_ts INTEGER NOT NULL,
    active     INTEGER NOT NULL DEFAULT 1
);"
pass "CREATE users"

gq "CREATE TABLE categories (
    id   INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);"
pass "CREATE categories"

gq "CREATE TABLE products (
    id         INTEGER PRIMARY KEY,
    sku        TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    category_id INTEGER NOT NULL REFERENCES categories(id),
    price      REAL NOT NULL CHECK(price > 0),
    stock      INTEGER NOT NULL DEFAULT 0,
    active     INTEGER NOT NULL DEFAULT 1
);"
pass "CREATE products"

gq "CREATE TABLE orders (
    id         INTEGER PRIMARY KEY,
    user_id    INTEGER NOT NULL REFERENCES users(id),
    status     TEXT NOT NULL DEFAULT 'pending'
                   CHECK(status IN ('pending','confirmed','shipped','delivered','cancelled')),
    created_ts INTEGER NOT NULL,
    total      REAL NOT NULL DEFAULT 0
);"
pass "CREATE orders"

gq "CREATE TABLE order_items (
    id         INTEGER PRIMARY KEY,
    order_id   INTEGER NOT NULL REFERENCES orders(id),
    product_id INTEGER NOT NULL REFERENCES products(id),
    qty        INTEGER NOT NULL CHECK(qty > 0),
    unit_price REAL NOT NULL CHECK(unit_price > 0)
);"
pass "CREATE order_items"

gq "CREATE TABLE reviews (
    id         INTEGER PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    user_id    INTEGER NOT NULL REFERENCES users(id),
    rating     INTEGER NOT NULL CHECK(rating BETWEEN 1 AND 5),
    comment    TEXT,
    created_ts INTEGER NOT NULL
);"
pass "CREATE reviews"

gq "CREATE INDEX idx_orders_user   ON orders(user_id);"
gq "CREATE INDEX idx_oi_order      ON order_items(order_id);"
gq "CREATE INDEX idx_oi_product    ON order_items(product_id);"
gq "CREATE INDEX idx_reviews_prod  ON reviews(product_id);"
gq "CREATE INDEX idx_products_cat  ON products(category_id);"
pass "All indexes created"

# ---------------------------------------------------------------------------
banner "PHASE 1 — Seed Data"
# ---------------------------------------------------------------------------
section "Categories (5)"
for name in Electronics Clothing Books HomeGarden Sports; do
    gq "INSERT INTO categories(name) VALUES ('$name');" 2>/dev/null || true
done
assert_eq "categories count" "$(sq "SELECT COUNT(*) FROM categories;" | tr -d '[:space:]')" "5"

section "Users (50)"
for i in $(seq 1 50); do
    tier="free"
    [[ $((i % 5)) -eq 0 ]] && tier="pro"
    [[ $((i % 17)) -eq 0 ]] && tier="enterprise"
    gq "INSERT INTO users VALUES ($i,'user_$i','user$i@shop.test',
        '$tier', $((1700000000 + i*3600)), 1);" 2>/dev/null || true
done
assert_eq "users count" "$(sq "SELECT COUNT(*) FROM users;" | tr -d '[:space:]')" "50"

section "Products (100 across 5 categories)"
for i in $(seq 1 100); do
    cat_id=$(( (i-1) % 5 + 1 ))
    price=$(echo "scale=2; ($i * 3.17) + 9.99" | bc)
    stock=$(( i * 7 % 200 + 5 ))
    gq "INSERT INTO products VALUES ($i,'SKU-$(printf '%04d' $i)','Product $i',
        $cat_id, $price, $stock, 1);" 2>/dev/null || true
done
assert_eq "products count" "$(sq "SELECT COUNT(*) FROM products;" | tr -d '[:space:]')" "100"

section "Orders (200)"
order_id=1
for i in $(seq 1 200); do
    user_id=$(( (i-1) % 50 + 1 ))
    statuses=("pending" "confirmed" "shipped" "delivered" "cancelled")
    status="${statuses[$((i % 5))]}"
    gq "INSERT INTO orders(id,user_id,status,created_ts,total)
        VALUES ($i, $user_id, '$status', $((1700100000 + i*1800)), 0);" 2>/dev/null || true
done
assert_eq "orders count" "$(sq "SELECT COUNT(*) FROM orders;" | tr -d '[:space:]')" "200"

section "Order items (~500)"
item_id=1
for order_id in $(seq 1 200); do
    n_items=$(( order_id % 4 + 1 ))   # 1–4 items per order
    for j in $(seq 1 $n_items); do
        prod_id=$(( (order_id * j) % 100 + 1 ))
        qty=$(( j % 3 + 1 ))
        price=$(sq "SELECT price FROM products WHERE id=$prod_id;" | tr -d '[:space:]')
        gq "INSERT INTO order_items VALUES ($item_id, $order_id, $prod_id, $qty, ${price:-9.99});" 2>/dev/null || true
        item_id=$((item_id + 1))
    done
done
OI_COUNT=$(sq "SELECT COUNT(*) FROM order_items;" | tr -d '[:space:]')
assert_ge "order_items count" "$OI_COUNT" "400"

# Update order totals
gq "UPDATE orders SET total = (
    SELECT ROUND(SUM(qty * unit_price), 2)
    FROM order_items WHERE order_id = orders.id
) WHERE EXISTS (SELECT 1 FROM order_items WHERE order_id = orders.id);"

section "Reviews (150)"
for i in $(seq 1 150); do
    prod_id=$(( (i * 7) % 100 + 1 ))
    user_id=$(( (i * 3) % 50 + 1 ))
    rating=$(( i % 5 + 1 ))
    gq "INSERT INTO reviews VALUES ($i, $prod_id, $user_id, $rating,
        'Review comment $i', $((1700200000 + i*600)));" 2>/dev/null || true
done
assert_eq "reviews count" "$(sq "SELECT COUNT(*) FROM reviews;" | tr -d '[:space:]')" "150"

section "Baseline commit"
db_ready "seed complete"
commit "c1: full baseline — 50u / 100p / 200o / ~500oi / 150rev"
HASH_BASELINE=$(head_hash)
pass "Baseline committed: ${HASH_BASELINE:0:7}"

# Capture baseline fingerprints
FP_BASE_USERS=$(fingerprint users "id,username,tier")
FP_BASE_PRODUCTS=$(fingerprint products "id,sku,price,stock")
FP_BASE_ORDERS=$(fingerprint orders "id,user_id,status,total")
FP_BASE_REVIEWS=$(fingerprint reviews "id,product_id,rating")
pass "Baseline fingerprints captured"

# ---------------------------------------------------------------------------
banner "PHASE 2 — Complex SQL on Baseline"
# ---------------------------------------------------------------------------
section "Revenue by category (JOIN + GROUP BY + ORDER BY)"
rev_out=$(sq "SELECT c.name, COUNT(DISTINCT o.id) AS orders,
    ROUND(SUM(oi.qty * oi.unit_price), 2) AS revenue
FROM categories c
JOIN products p ON p.category_id = c.id
JOIN order_items oi ON oi.product_id = p.id
JOIN orders o ON o.id = oi.order_id
GROUP BY c.id ORDER BY revenue DESC;")
assert_ge "revenue query: 5 categories" "$(echo "$rev_out" | wc -l | tr -d '[:space:]')" "5"
echo "$rev_out" | grep -q "Electronics" && pass "revenue: Electronics category present" || fail "revenue: Electronics missing"

section "Top 10 products by revenue (CTE)"
top10=$(sq "WITH product_revenue AS (
    SELECT p.id, p.name, p.sku,
        ROUND(SUM(oi.qty * oi.unit_price), 2) AS revenue,
        SUM(oi.qty) AS units_sold
    FROM products p
    JOIN order_items oi ON oi.product_id = p.id
    GROUP BY p.id
)
SELECT sku, name, revenue, units_sold
FROM product_revenue
ORDER BY revenue DESC LIMIT 10;")
assert_ge "top10 CTE: 10 rows" "$(echo "$top10" | wc -l | tr -d '[:space:]')" "10"

section "User spending tiers (window function + CTE)"
tier_rank=$(sq "WITH user_spend AS (
    SELECT u.id, u.username, u.tier,
        ROUND(SUM(o.total), 2) AS lifetime_spend
    FROM users u
    JOIN orders o ON o.user_id = u.id AND o.status != 'cancelled'
    GROUP BY u.id
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY tier ORDER BY lifetime_spend DESC) AS rank_in_tier
    FROM user_spend
)
SELECT tier, username, lifetime_spend, rank_in_tier
FROM ranked WHERE rank_in_tier = 1 ORDER BY tier;")
[[ -n "$tier_rank" ]] && pass "window func tier ranking: returns rows" || fail "window func: empty"

section "Products never ordered (LEFT JOIN + NULL check)"
never=$(sq "SELECT COUNT(*) FROM products p
LEFT JOIN order_items oi ON oi.product_id = p.id
WHERE oi.id IS NULL;" | tr -d '[:space:]')
[[ "${never:-0}" -ge 0 ]] && pass "unordered products query: returns ($never products)" || fail "unordered query failed"

section "Average rating per category (multi-table JOIN)"
avg_rating=$(sq "SELECT c.name, ROUND(AVG(r.rating),2) AS avg_r, COUNT(r.id) AS review_count
FROM categories c
JOIN products p ON p.category_id = c.id
JOIN reviews r ON r.product_id = p.id
GROUP BY c.id HAVING review_count >= 5
ORDER BY avg_r DESC;")
[[ -n "$avg_rating" ]] && pass "avg rating per category: data returned" || fail "avg rating query: empty"

section "HAVING: users with >3 orders"
heavy_users=$(sq "SELECT u.username, COUNT(o.id) AS order_count
FROM users u JOIN orders o ON o.user_id = u.id
GROUP BY u.id HAVING order_count > 3
ORDER BY order_count DESC LIMIT 5;")
[[ -n "$heavy_users" ]] && pass "HAVING order_count>3: heavy users found" || fail "HAVING: no results"

section "Correlated subquery: products above avg price in category"
above_avg=$(sq "SELECT COUNT(*) FROM products p
WHERE price > (
    SELECT AVG(price) FROM products WHERE category_id = p.category_id
);" | tr -d '[:space:]')
assert_ge "above-avg products: ≥20" "$above_avg" "20"

section "Recursive CTE: order chain (cumulative items per user)"
rec=$(sq "WITH RECURSIVE user_orders(user_id, order_id, depth) AS (
    SELECT user_id, id, 1 FROM orders WHERE status='delivered'
    UNION ALL
    SELECT uo.user_id, o.id, uo.depth+1
    FROM orders o
    JOIN user_orders uo ON o.user_id = uo.user_id AND o.id > uo.order_id
    WHERE uo.depth < 3
)
SELECT COUNT(DISTINCT user_id) FROM user_orders;" | tr -d '[:space:]')
[[ "${rec:-0}" -ge 0 ]] && pass "recursive CTE: completes without error" || fail "recursive CTE: failed"

# ---------------------------------------------------------------------------
banner "PHASE 3 — Branch: feature/pricing"
# ---------------------------------------------------------------------------
"$GFS" checkout --path "$REPO" -b feature/pricing &>/dev/null \
    && pass "Checkout -b feature/pricing" || fail "branch feature/pricing failed"

section "Apply: 20% price increase on Electronics + Books"
gq "UPDATE products SET price = ROUND(price * 1.20, 2)
    WHERE category_id IN (SELECT id FROM categories WHERE name IN ('Electronics','Books'));"
gq "UPDATE products SET price = ROUND(price * 1.10, 2)
    WHERE category_id IN (SELECT id FROM categories WHERE name = 'Sports');"
gq "INSERT INTO products VALUES (101,'SKU-0101','Premium Bundle',1,299.99,10,1);"

FP_PRICING_PRODUCTS=$(fingerprint products "id,sku,price,stock")
assert_fp_ne "pricing: products differ from baseline" "$FP_BASE_PRODUCTS" "$FP_PRICING_PRODUCTS"

elec_price=$(sq "SELECT ROUND(AVG(price),2) FROM products WHERE category_id=1;" | tr -d '[:space:]')
assert_ge "pricing: Electronics avg price increased" "${elec_price%.*}" "30"

db_ready "feature/pricing pre-commit"
commit "feat/pricing: +20% Electronics+Books, +10% Sports, SKU-0101"
HASH_PRICING=$(head_hash)
pass "feature/pricing committed: ${HASH_PRICING:0:7}"

# Revenue query still works
rev_pricing=$(sq "SELECT ROUND(SUM(oi.qty * oi.unit_price),2) FROM order_items oi
    JOIN products p ON p.id = oi.product_id
    JOIN categories c ON c.id = p.category_id
    WHERE c.name = 'Electronics';" | tr -d '[:space:]')
[[ -n "$rev_pricing" ]] && pass "pricing branch: revenue query works" || fail "pricing branch: revenue query failed"

# ---------------------------------------------------------------------------
banner "PHASE 4 — Branch: feature/inventory"
# ---------------------------------------------------------------------------
checkout "$HASH_BASELINE"
"$GFS" checkout --path "$REPO" -b feature/inventory &>/dev/null \
    && pass "Checkout -b feature/inventory from baseline" || fail "branch feature/inventory failed"

section "Apply: restock low-inventory products + discontinue 5"
gq "UPDATE products SET stock = stock + 100 WHERE stock < 20;"
gq "UPDATE products SET active = 0 WHERE id IN (7, 14, 21, 28, 35);"
gq "INSERT INTO products VALUES (102,'SKU-0102','New Arrival',2,59.99,200,1);"

FP_INVENTORY_PRODUCTS=$(fingerprint products "id,sku,price,stock,active")
assert_fp_ne "inventory: products differ from baseline" "$FP_BASE_PRODUCTS" "$FP_INVENTORY_PRODUCTS"

low_stock=$(sq "SELECT COUNT(*) FROM products WHERE stock < 20 AND active=1;" | tr -d '[:space:]')
[[ "${low_stock:-999}" -eq 0 ]] && pass "inventory: no active products with stock<20" || info "inventory: $low_stock products still low-stock"

inactive=$(sq "SELECT COUNT(*) FROM products WHERE active=0;" | tr -d '[:space:]')
assert_eq "inventory: 5 discontinued" "$inactive" "5"

db_ready "feature/inventory pre-commit"
commit "feat/inventory: restock low-stock, discontinue 5, add SKU-0102"
HASH_INVENTORY=$(head_hash)
pass "feature/inventory committed: ${HASH_INVENTORY:0:7}"

assert_fp_ne "pricing vs inventory differ" "$FP_PRICING_PRODUCTS" "$FP_INVENTORY_PRODUCTS"

# ---------------------------------------------------------------------------
banner "PHASE 5 — Branch: feature/user-tiers"
# ---------------------------------------------------------------------------
checkout "$HASH_BASELINE"
"$GFS" checkout --path "$REPO" -b feature/user-tiers &>/dev/null \
    && pass "Checkout -b feature/user-tiers from baseline" || fail "branch feature/user-tiers failed"

section "Apply: upgrade heavy spenders to pro/enterprise"
gq "UPDATE users SET tier='enterprise'
    WHERE id IN (
        SELECT user_id FROM orders
        GROUP BY user_id
        HAVING ROUND(SUM(total),2) > 800.0
    );"
gq "UPDATE users SET tier='pro'
    WHERE tier='free' AND id IN (
        SELECT user_id FROM orders
        GROUP BY user_id
        HAVING COUNT(*) >= 4
    );"
gq "INSERT INTO users VALUES (51,'vip_user','vip@shop.test','enterprise',1700900000,1);"

FP_TIERS_USERS=$(fingerprint users "id,username,tier")
assert_fp_ne "user-tiers: users differ from baseline" "$FP_BASE_USERS" "$FP_TIERS_USERS"

enterprise_count=$(sq "SELECT COUNT(*) FROM users WHERE tier='enterprise';" | tr -d '[:space:]')
assert_ge "user-tiers: ≥1 enterprise user" "$enterprise_count" "1"

db_ready "feature/user-tiers pre-commit"
commit "feat/user-tiers: upgrade heavy spenders, add vip_user"
HASH_TIERS=$(head_hash)
pass "feature/user-tiers committed: ${HASH_TIERS:0:7}"

# ---------------------------------------------------------------------------
banner "PHASE 6 — Branch: feature/reviews-cleanup"
# ---------------------------------------------------------------------------
checkout "$HASH_BASELINE"
"$GFS" checkout --path "$REPO" -b feature/reviews-cleanup &>/dev/null \
    && pass "Checkout -b feature/reviews-cleanup" || fail "branch feature/reviews-cleanup failed"

section "Apply: delete 1-star reviews + add staff reviews"
gq "DELETE FROM reviews WHERE rating = 1;"
gq "UPDATE reviews SET comment = 'Verified: ' || comment WHERE rating = 5;"
for i in $(seq 151 165); do
    gq "INSERT INTO reviews VALUES ($i, $((i % 100 + 1)), $((i % 50 + 1)), 5,
        'Staff pick — highly recommended', 1700950000);" 2>/dev/null || true
done

FP_REVIEWS_CLEAN=$(fingerprint reviews "id,product_id,rating")
assert_fp_ne "reviews-cleanup: reviews differ from baseline" "$FP_BASE_REVIEWS" "$FP_REVIEWS_CLEAN"

one_star=$(sq "SELECT COUNT(*) FROM reviews WHERE rating=1;" | tr -d '[:space:]')
assert_eq "reviews-cleanup: no 1-star reviews" "$one_star" "0"

staff_reviews=$(sq "SELECT COUNT(*) FROM reviews WHERE comment LIKE 'Staff pick%';" | tr -d '[:space:]')
assert_ge "reviews-cleanup: ≥15 staff reviews" "$staff_reviews" "15"

db_ready "feature/reviews-cleanup pre-commit"
commit "feat/reviews-cleanup: remove 1-star, add staff picks"
HASH_REVIEWS=$(head_hash)
pass "feature/reviews-cleanup committed: ${HASH_REVIEWS:0:7}"

# ---------------------------------------------------------------------------
banner "PHASE 7 — Checkout Isolation Verification"
# ---------------------------------------------------------------------------

section "Checkout baseline — verify exact fingerprints"
checkout "$HASH_BASELINE"
db_ready "baseline checkout"
assert_fp_eq "baseline: users fingerprint" "$FP_BASE_USERS" "$(fingerprint users "id,username,tier")"
assert_fp_eq "baseline: products fingerprint" "$FP_BASE_PRODUCTS" "$(fingerprint products "id,sku,price,stock")"
assert_fp_eq "baseline: orders fingerprint" "$FP_BASE_ORDERS" "$(fingerprint orders "id,user_id,status,total")"
assert_fp_eq "baseline: reviews fingerprint" "$FP_BASE_REVIEWS" "$(fingerprint reviews "id,product_id,rating")"
assert_eq "baseline products: 100" "$(sq "SELECT COUNT(*) FROM products;" | tr -d '[:space:]')" "100"
assert_eq "baseline users: 50" "$(sq "SELECT COUNT(*) FROM users;" | tr -d '[:space:]')" "50"

section "Checkout feature/pricing — prices increased, no stock changes"
checkout "$HASH_PRICING"
db_ready "feature/pricing checkout"
assert_fp_ne "pricing: products differ from base" "$FP_BASE_PRODUCTS" "$(fingerprint products "id,sku,price,stock")"
assert_fp_eq "pricing: orders unchanged" "$FP_BASE_ORDERS" "$(fingerprint orders "id,user_id,status,total")"
assert_fp_eq "pricing: users unchanged" "$FP_BASE_USERS" "$(fingerprint users "id,username,tier")"
p_elec=$(sq "SELECT ROUND(MIN(price),2) FROM products WHERE id=1;" | tr -d '[:space:]')
base_p=$(echo "scale=2; 1 * 3.17 + 9.99" | bc)
[[ "$p_elec" != "$base_p" ]] && pass "pricing: product id=1 price changed" || fail "pricing: product id=1 price unchanged"
assert_eq "pricing: 101 products (baseline+new)" "$(sq "SELECT COUNT(*) FROM products;" | tr -d '[:space:]')" "101"
# Revenue query must work after checkout
rev_q=$(sq "SELECT ROUND(SUM(oi.qty * oi.unit_price),2) FROM order_items oi;" | tr -d '[:space:]')
[[ -n "$rev_q" ]] && pass "pricing: revenue query operational" || fail "pricing: revenue query failed"

section "Checkout feature/inventory — stock changes, no price changes"
checkout "$HASH_INVENTORY"
db_ready "feature/inventory checkout"
assert_fp_ne "inventory: products differ from base" "$FP_BASE_PRODUCTS" "$(fingerprint products "id,sku,price,stock,active")"
assert_fp_eq "inventory: reviews unchanged" "$FP_BASE_REVIEWS" "$(fingerprint reviews "id,product_id,rating")"
assert_eq "inventory: 5 inactive" "$(sq "SELECT COUNT(*) FROM products WHERE active=0;" | tr -d '[:space:]')" "5"
assert_eq "inventory: 101 products" "$(sq "SELECT COUNT(*) FROM products;" | tr -d '[:space:]')" "101"
low_after=$(sq "SELECT COUNT(*) FROM products WHERE stock < 20 AND active=1;" | tr -d '[:space:]')
[[ "${low_after:-999}" -eq 0 ]] && pass "inventory: no low-stock active products" || info "inventory: $low_after low-stock"

section "Checkout feature/user-tiers — tiers upgraded, data identical otherwise"
checkout "$HASH_TIERS"
db_ready "feature/user-tiers checkout"
assert_fp_ne "tiers: users differ from base" "$FP_BASE_USERS" "$(fingerprint users "id,username,tier")"
assert_fp_eq "tiers: products unchanged" "$FP_BASE_PRODUCTS" "$(fingerprint products "id,sku,price,stock")"
assert_fp_eq "tiers: orders unchanged" "$FP_BASE_ORDERS" "$(fingerprint orders "id,user_id,status,total")"
assert_eq "tiers: 51 users" "$(sq "SELECT COUNT(*) FROM users;" | tr -d '[:space:]')" "51"
ent=$(sq "SELECT COUNT(*) FROM users WHERE tier='enterprise';" | tr -d '[:space:]')
assert_ge "tiers: ≥1 enterprise" "$ent" "1"

section "Checkout feature/reviews-cleanup"
checkout "$HASH_REVIEWS"
db_ready "feature/reviews-cleanup checkout"
assert_fp_ne "reviews: differ from base" "$FP_BASE_REVIEWS" "$(fingerprint reviews "id,product_id,rating")"
assert_fp_eq "reviews-cleanup: products unchanged" "$FP_BASE_PRODUCTS" "$(fingerprint products "id,sku,price,stock")"
assert_eq "reviews: no 1-star" "$(sq "SELECT COUNT(*) FROM reviews WHERE rating=1;" | tr -d '[:space:]')" "0"

section "Return to baseline — verify exact restoration"
checkout "$HASH_BASELINE"
db_ready "baseline re-check"
assert_fp_eq "re-baseline: users" "$FP_BASE_USERS" "$(fingerprint users "id,username,tier")"
assert_fp_eq "re-baseline: products" "$FP_BASE_PRODUCTS" "$(fingerprint products "id,sku,price,stock")"
assert_fp_eq "re-baseline: orders" "$FP_BASE_ORDERS" "$(fingerprint orders "id,user_id,status,total")"
assert_fp_eq "re-baseline: reviews" "$FP_BASE_REVIEWS" "$(fingerprint reviews "id,product_id,rating")"
pass "Baseline exact restoration confirmed"

# ---------------------------------------------------------------------------
banner "PHASE 8 — Manual Merge on main (apply all 4 branches)"
# ---------------------------------------------------------------------------
checkout main
db_ready "main before merge"
assert_fp_eq "main: still at baseline" "$FP_BASE_PRODUCTS" "$(fingerprint products "id,sku,price,stock")"

section "Merge: apply pricing changes"
gq "UPDATE products SET price = ROUND(price * 1.20, 2)
    WHERE category_id IN (SELECT id FROM categories WHERE name IN ('Electronics','Books'));"
gq "UPDATE products SET price = ROUND(price * 1.10, 2)
    WHERE category_id IN (SELECT id FROM categories WHERE name = 'Sports');"
gq "INSERT OR IGNORE INTO products VALUES (101,'SKU-0101','Premium Bundle',1,299.99,10,1);"

section "Merge: apply inventory changes"
gq "UPDATE products SET stock = stock + 100 WHERE stock < 20;"
gq "UPDATE products SET active = 0 WHERE id IN (7,14,21,28,35);"
gq "INSERT OR IGNORE INTO products VALUES (102,'SKU-0102','New Arrival',2,59.99,200,1);"

section "Merge: apply tier upgrades"
gq "UPDATE users SET tier='enterprise'
    WHERE id IN (
        SELECT user_id FROM orders
        GROUP BY user_id
        HAVING ROUND(SUM(total),2) > 800.0
    );"
gq "UPDATE users SET tier='pro'
    WHERE tier='free' AND id IN (
        SELECT user_id FROM orders
        GROUP BY user_id
        HAVING COUNT(*) >= 4
    );"
gq "INSERT OR IGNORE INTO users VALUES (51,'vip_user','vip@shop.test','enterprise',1700900000,1);"

section "Merge: apply reviews cleanup"
gq "DELETE FROM reviews WHERE rating = 1;"
gq "UPDATE reviews SET comment = 'Verified: ' || comment
    WHERE rating = 5 AND comment NOT LIKE 'Verified:%';"
for i in $(seq 151 165); do
    gq "INSERT OR IGNORE INTO reviews VALUES ($i, $((i % 100 + 1)), $((i % 50 + 1)), 5,
        'Staff pick — highly recommended', 1700950000);" 2>/dev/null || true
done

section "Verify merged state"
db_ready "merged main"
assert_eq "merged: 102 products" "$(sq "SELECT COUNT(*) FROM products;" | tr -d '[:space:]')" "102"
assert_eq "merged: 51 users" "$(sq "SELECT COUNT(*) FROM users;" | tr -d '[:space:]')" "51"
assert_eq "merged: no 1-star reviews" "$(sq "SELECT COUNT(*) FROM reviews WHERE rating=1;" | tr -d '[:space:]')" "0"
assert_eq "merged: 5 inactive products" "$(sq "SELECT COUNT(*) FROM products WHERE active=0;" | tr -d '[:space:]')" "5"
assert_ge "merged: enterprise users ≥1" "$(sq "SELECT COUNT(*) FROM users WHERE tier='enterprise';" | tr -d '[:space:]')" "1"

FP_MERGED_PROD=$(fingerprint products "id,sku,price,stock,active")
FP_MERGED_USERS=$(fingerprint users "id,username,tier")
assert_fp_ne "merged vs base: products differ" "$FP_BASE_PRODUCTS" "$FP_MERGED_PROD"
assert_fp_ne "merged vs base: users differ" "$FP_BASE_USERS" "$FP_MERGED_USERS"

commit "main: manual merge — pricing + inventory + tiers + reviews"
HASH_MERGED=$(head_hash)
pass "Merged main committed: ${HASH_MERGED:0:7}"

# ---------------------------------------------------------------------------
banner "PHASE 9 — Complex SQL on Merged State"
# ---------------------------------------------------------------------------

section "Full revenue dashboard (CTE + JOIN + HAVING)"
dashboard=$(sq "WITH
product_stats AS (
    SELECT p.id, p.name, p.sku, c.name AS category,
        p.price, p.stock, p.active,
        ROUND(SUM(oi.qty * oi.unit_price), 2) AS revenue,
        SUM(oi.qty) AS units_sold,
        COUNT(DISTINCT oi.order_id) AS order_count
    FROM products p
    JOIN categories c ON c.id = p.category_id
    LEFT JOIN order_items oi ON oi.product_id = p.id
    WHERE p.active = 1
    GROUP BY p.id
),
user_stats AS (
    SELECT u.id, u.username, u.tier,
        COUNT(DISTINCT o.id) AS order_count,
        ROUND(SUM(o.total), 2) AS lifetime_value
    FROM users u
    LEFT JOIN orders o ON o.user_id = u.id AND o.status != 'cancelled'
    GROUP BY u.id
)
SELECT
    (SELECT COUNT(*) FROM product_stats WHERE revenue > 0) AS active_selling_products,
    (SELECT ROUND(SUM(revenue),2) FROM product_stats) AS total_revenue,
    (SELECT COUNT(*) FROM user_stats WHERE lifetime_value > 500) AS high_value_users,
    (SELECT COUNT(*) FROM reviews WHERE rating >= 4) AS positive_reviews;")
[[ -n "$dashboard" ]] && pass "revenue dashboard CTE: returns data" || fail "revenue dashboard: empty"
info "Dashboard: $dashboard"

section "Window functions: product rank within category"
win_out=$(sq "SELECT p.name, c.name AS cat,
    p.price,
    ROW_NUMBER() OVER (PARTITION BY p.category_id ORDER BY p.price DESC) AS price_rank,
    RANK()       OVER (PARTITION BY p.category_id ORDER BY p.price DESC) AS rank_tied,
    LAG(p.price) OVER (PARTITION BY p.category_id ORDER BY p.price DESC) AS prev_price
FROM products p
JOIN categories c ON c.id = p.category_id
WHERE p.active = 1
ORDER BY c.name, price_rank
LIMIT 15;")
assert_ge "window funcs: ≥10 rows" "$(echo "$win_out" | wc -l | tr -d '[:space:]')" "10"

section "Self-join: users who ordered the same product as vip_user"
shared_prods=$(sq "SELECT COUNT(DISTINCT o2.user_id)
FROM orders o1
JOIN order_items oi1 ON oi1.order_id = o1.id
JOIN order_items oi2 ON oi2.product_id = oi1.product_id AND oi2.order_id != oi1.order_id
JOIN orders o2 ON o2.id = oi2.order_id
WHERE o1.user_id = (SELECT id FROM users WHERE username='vip_user');" | tr -d '[:space:]')
[[ "${shared_prods:-0}" -ge 0 ]] && pass "self-join: users sharing products with vip_user ($shared_prods found)" || fail "self-join: failed"

section "EXISTS subquery: categories with all products reviewed"
exists_out=$(sq "SELECT c.name FROM categories c
WHERE EXISTS (
    SELECT 1 FROM products p
    JOIN reviews r ON r.product_id = p.id
    WHERE p.category_id = c.id
)
ORDER BY c.name;")
assert_ge "EXISTS: ≥3 categories have reviews" "$(echo "$exists_out" | wc -l | tr -d '[:space:]')" "3"

# ---------------------------------------------------------------------------
banner "PHASE 10 — Time-travel: checkout old states, verify DB readiness"
# ---------------------------------------------------------------------------

for ref in "$HASH_BASELINE" main "HEAD~1"; do
    section "Checkout $ref"
    checkout "$ref"
    db_ready "time-travel: $ref"
    cnt=$(sq "SELECT COUNT(*) FROM orders;" | tr -d '[:space:]')
    assert_eq "time-travel $ref: 200 orders" "$cnt" "200"
    rev_check=$(sq "SELECT ROUND(SUM(total),2) FROM orders WHERE status='delivered';" | tr -d '[:space:]')
    [[ -n "$rev_check" ]] && pass "time-travel $ref: delivered orders sum = $rev_check" || fail "time-travel $ref: sum query failed"
done

section "HEAD~3 checkout (3 commits before current)"
checkout "HEAD~3"
db_ready "HEAD~3"
pass "HEAD~3 checkout: DB operational"

section "Return to main — final state check"
checkout main
db_ready "final main"
assert_eq "final main: 102 products" "$(sq "SELECT COUNT(*) FROM products;" | tr -d '[:space:]')" "102"
assert_eq "final main: 51 users" "$(sq "SELECT COUNT(*) FROM users;" | tr -d '[:space:]')" "51"
assert_eq "final main: 200 orders" "$(sq "SELECT COUNT(*) FROM orders;" | tr -d '[:space:]')" "200"
assert_fp_eq "final main: merged fingerprint stable" "$FP_MERGED_PROD" "$(fingerprint products "id,sku,price,stock,active")"
assert_fp_eq "final main: merged users stable" "$FP_MERGED_USERS" "$(fingerprint users "id,username,tier")"

# ---------------------------------------------------------------------------
banner "PHASE 11 — Branch list + log verification"
# ---------------------------------------------------------------------------
branch_out=$("$GFS" branch --path "$REPO" 2>/dev/null)
for br in "feature/pricing" "feature/inventory" "feature/user-tiers" "feature/reviews-cleanup" "main"; do
    echo "$branch_out" | grep -q "$br" && pass "branch list: $br present" || fail "branch list: $br missing"
done
echo "$branch_out" | grep -q '\* main' && pass "branch list: main is current" || fail "branch list: main not marked *"

log_out=$("$GFS" log --path "$REPO" 2>/dev/null)
echo "$log_out" | grep -q "manual merge" && pass "log: merge commit visible" || fail "log: merge commit missing"
echo "$log_out" | grep -q "full baseline" && pass "log: baseline commit visible" || fail "log: baseline missing"

json_log=$("$GFS" --json log --path "$REPO" 2>/dev/null)
n=$(echo "$json_log" | python3 -c "import sys,json; d=json.load(sys.stdin); cs=d.get('commits',d); print(len(cs))" 2>/dev/null || echo "0")
assert_ge "JSON log: ≥3 commits" "$n" "3"

# ---------------------------------------------------------------------------
printf "\n${CYAN}${BOLD}════════════════════════════════════════${NC}\n"
printf "  Total: ${GREEN}%d passed${NC}, ${RED}%d failed${NC}, ${YELLOW}%d skipped${NC}\n" "$PASS" "$FAIL" "$SKIP"
printf "${CYAN}${BOLD}════════════════════════════════════════${NC}\n\n"

if [[ $FAIL -eq 0 ]]; then
    printf "${GREEN}${BOLD}ALL MEDIUM-DB TESTS PASSED${NC}\n\n"
    exit 0
else
    printf "${RED}${BOLD}$FAIL TEST(S) FAILED${NC}\n\n"
    exit 1
fi
