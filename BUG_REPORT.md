# GFS Bug Report

**Generated:** 2026-04-29  
**Repo:** `/home/mohamed-aziz-ktata/Desktop/Guepard/gfs`  
**Providers tested:** SQLite, PostgreSQL, MySQL (ClickHouse TBD)  
**Discovery method:** Adversarial test suite (`gfs-sqlite-test.sh` tests 41–48) + cross-provider subagent investigation

---

## BUG-1: Dirty Workspace Persists on Branch-Name Checkout

### Severity: HIGH

### Description

SQL changes made to the database **without** `gfs commit` persist when switching branches by name (`gfs checkout <branch-name>`). Only `gfs checkout <hash>` (first visit) restores a clean snapshot.

### Root Cause

`gfs_repository.rs:596–635`:

```rust
let workspace_exists = workspace_path.exists();
if !workspace_exists {
    // populate from snapshot
}
```

Branch workspaces live at `workspaces/<branch-name>/0/data`. Once created they are **never** reset on return — the snapshot restore is skipped whenever the directory already exists. Only hash/detached checkouts (`workspaces/detached/<hash>/data`) get fresh directories.

### Affected Providers

| Provider | `checkout -b` (new branch) | `checkout main` (return) | `checkout <hash>` (first visit) |
|----------|---------------------------|--------------------------|----------------------------------|
| SQLite   | Dirty persists (reuses workspace) | Dirty persists | Clean restore ✓ |
| PostgreSQL | Clean restore ✓ (new dir) | **Dirty persists** | Clean restore ✓ |
| MySQL    | Clean restore ✓ (new dir) | **Dirty persists** | Clean restore ✓ |

SQLite is worse: even `checkout -b` on a new branch can carry dirty data if the workspace dir was already created (e.g., after a previous checkout-by-name to that branch).

### Reproduction (SQLite)

```bash
gfs init --database-provider sqlite --database-version 3 /tmp/test-repo
gfs commit --path /tmp/test-repo -m "c1: baseline"
gfs query --path /tmp/test-repo "CREATE TABLE t (id INT); INSERT INTO t VALUES (1);"
gfs commit --path /tmp/test-repo -m "c2: committed row"

# Dirty insert — NOT committed
gfs query --path /tmp/test-repo "INSERT INTO t VALUES (2);"

# Checkout branch by name
gfs checkout --path /tmp/test-repo main

# BUG: dirty row (id=2) still present
gfs query --path /tmp/test-repo "SELECT COUNT(*) FROM t;"  # → 2, expected 1
```

### Expected Behavior

`gfs checkout <branch-name>` should warn about uncommitted changes (like `git stash` / `git checkout` does) or restore clean state from the branch's latest committed snapshot.

### Workaround

Always `gfs commit` before switching branches. Or use `gfs checkout <commit-hash>` (first visit only — see BUG-2).

---

## BUG-2: Hash Checkout Only Restores Clean State on First Visit

### Severity: MEDIUM

### Description

`gfs checkout <commit-hash>` restores a clean snapshot **only on the first visit**. If the same hash has been checked out before, the workspace directory already exists and GFS reuses it — so any uncommitted SQL changes from the previous visit persist.

### Root Cause

Same as BUG-1: the `workspace_exists` guard. Detached/hash workspaces at `workspaces/detached/<hash>/data` are created per-hash, so the first visit is always fresh. But the second visit to the same hash finds the directory and skips snapshot restore.

### Reproduction

```bash
gfs checkout --path /tmp/repo "$HASH"      # first visit → clean
gfs query --path /tmp/repo "DROP TABLE t;" # dirty change, no commit
gfs checkout --path /tmp/repo "$HASH"      # second visit → workspace reused, DROP persists

# BUG: table still missing even though we checked out the hash where it existed
gfs query --path /tmp/repo "SELECT * FROM t;"  # → error: no such table
```

### Expected Behavior

`gfs checkout <hash>` should always restore the exact committed state at that hash, regardless of how many times the hash has been visited.

### Affected Providers

All providers (same `gfs_repository.rs` code path).

---

## BUG-3: Branch Delete Does Not Remove Workspace Directory

### Severity: MEDIUM

### Description

`gfs branch -d <name>` removes the branch reference (`.gfs/refs/heads/<name>`) but does **not** delete the workspace directory (`.gfs/workspaces/<name>/0/data`). When the same branch name is recreated, GFS finds the existing workspace and reuses it — leaking stale data from the deleted branch.

### Root Cause

`gfs branch -d` only removes the ref file. No cleanup of the associated workspace directory. On recreate, the `workspace_exists` guard (same as BUG-1) skips snapshot initialization because the directory is present.

### Reproduction (SQLite)

```bash
gfs checkout --path /tmp/repo -b recycled
gfs query --path /tmp/repo "INSERT INTO t VALUES (99, 'secret-data');"
gfs commit --path /tmp/repo -m "recycled: sensitive data"

gfs checkout --path /tmp/repo main
gfs branch --path /tmp/repo -d recycled

# Workspace dir .gfs/workspaces/recycled/0/data still exists on disk

gfs checkout --path /tmp/repo -b recycled  # recreate same name

# BUG: secret-data row still present
gfs query --path /tmp/repo "SELECT COUNT(*) FROM t;"  # → 2 (should be 1)
```

### Expected Behavior

`gfs branch -d` should also delete the workspace directory, or at minimum GFS should ignore existing workspace dirs for newly-recreated branches (always start from the branching-point snapshot).

### Security Note

If the workspace directory contains sensitive data from a deleted branch (e.g., credentials inserted for testing), that data persists on disk indefinitely and leaks into any future branch with the same name.

---

## BUG-4: `gfs query` Fails for MySQL via Hostname "localhost"

### Severity: LOW (MySQL only)

### Description

`gfs query` sends `-h localhost` to the `mysql` CLI. On Linux, `localhost` causes the MySQL client to use a Unix socket (`/var/run/mysqld/mysqld.sock`) instead of TCP, even when `-P <port>` is also specified. The socket doesn't exist inside the Docker container's mapped port, so the connection fails.

### Root Cause

`compute-docker/src/lib.rs:609` (approx) — `get_connection_info` returns `"localhost"` as the host string.

### Fix

Change `"localhost"` → `"127.0.0.1"` in the MySQL connection info. `127.0.0.1` forces TCP even on Linux.

### Affected Providers

MySQL only. PostgreSQL uses `psql` which handles `localhost` via TCP by default.

---

## BUG-5: `table_fingerprint` Silently Returns False-Equal Hashes on SQLite < 3.38

### Severity: LOW (test infrastructure)

### Description

The `table_fingerprint` test helper uses `json_array(table.*)` syntax (SQLite ≥3.38). On older SQLite versions this query fails silently (`2>/dev/null`). `sha256sum` of empty input returns `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`. Two calls on different tables both return this hash → fingerprint equality check passes falsely — even if the data differs.

### Impact

Any test using `table_fingerprint()` directly (not the portable `fingerprint()` wrapper) gives incorrect results on SQLite < 3.38. Tests could pass even with data corruption.

### Fix

Always use `fingerprint()` (CSV-based, portable) instead of `table_fingerprint()` (JSON-based, ≥3.38 only). The `fingerprint()` function uses `SELECT * FROM table ORDER BY 1` with `|` separator — works on all SQLite versions.

---

## ClickHouse Investigation Results

**Status**: Source-code confirmed (script could not execute — `clickhouse-client` not on host).

**Conclusion**: All three workspace bugs (BUG-1, BUG-2, BUG-3) are present for ClickHouse, identical to PostgreSQL/MySQL. The `workspace_exists` guard in `gfs_repository.rs` is provider-agnostic — ClickHouse goes through the same code path.

**Additional ClickHouse note**: `gfs query` will fail for ClickHouse (same pattern as MySQL localhost) because `clickhouse-client` is not installed on the host — only inside the Docker container. The supported version is `24.8.14.39` (the only version registered in `clickhouse.rs:179`).

**Complete provider matrix:**

| Bug | SQLite | PostgreSQL | MySQL | ClickHouse |
|-----|--------|-----------|-------|------------|
| BUG-1: dirty workspace on branch return | `checkout -b` AND `checkout main` | `checkout main` only | `checkout main` only | `checkout main` only (predicted) |
| BUG-2: hash checkout dirty on 2nd visit | ✓ confirmed | ✓ confirmed | ✓ confirmed | ✓ code-confirmed |
| BUG-3: branch -d leaves workspace | ✓ confirmed | ✓ confirmed | ✓ confirmed | ✓ code-confirmed |
| BUG-4: gfs query localhost/socket issue | N/A | N/A | ✓ confirmed | ✓ predicted (no host client) |

SQLite is uniquely worse for BUG-1: `checkout -b` also reuses workspace (because SQLite branch workspace dirs are created differently than container-based providers).

---

## Summary Table

| ID | Description | Severity | Providers | Root Cause File |
|----|-------------|----------|-----------|-----------------|
| BUG-1 | Dirty workspace persists on branch-name checkout | HIGH | All | `gfs_repository.rs:596` |
| BUG-2 | Hash checkout only clean on first visit | MEDIUM | All | `gfs_repository.rs:596` |
| BUG-3 | Branch delete leaves workspace dir (stale data leak) | MEDIUM | All | branch delete handler |
| BUG-4 | `gfs query` MySQL fails with `localhost` host | LOW | MySQL | `compute-docker/src/lib.rs:609` |
| BUG-5 | `table_fingerprint` gives false equality on SQLite < 3.38 | LOW | Test infra | `gfs-sqlite-test.sh` |

BUG-1, BUG-2, and BUG-3 share the same root cause: `workspace_exists` guard skipping snapshot restore. A single fix covers all three: always restore from snapshot on checkout, regardless of whether the workspace directory exists.

---

## Recommended Fix

In `gfs_repository.rs`, change the workspace initialization logic from:

```rust
// Current: skip if workspace dir already exists
let workspace_exists = workspace_path.exists();
if !workspace_exists {
    populate_from_snapshot(...);
}
```

To one of:
1. **Always restore** (safest): always populate from snapshot on checkout, discard workspace changes
2. **Warn + abort** (git-like): detect uncommitted changes, refuse checkout, prompt user to commit or stash
3. **Stash and restore** (most user-friendly): auto-stash uncommitted changes, restore on return

Option 2 matches Git's behavior and is the most predictable.
