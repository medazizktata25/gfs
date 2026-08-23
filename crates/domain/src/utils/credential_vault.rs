//! Per-database credential vault (RFC 008, Amendment A1): a database's revealable
//! password lives in a **data-plane-owned** store — node-local and OUTSIDE the
//! versioned repository tree — at mode `0600`, never in the cloud and never inside
//! a snapshot. The store is keyed **explicitly** by the database's
//! `(org, project, db)` identity triple under `$GUEPARD_SECRETS_DIR` (default: the
//! `secrets` sibling of the node's `repositories/` root). The node filesystem plus
//! `0600` are the trust boundary (RFC 008 §7).
//!
//! The triple is passed by the caller, never parsed back out of a repository path,
//! so the store location cannot drift with the shape of a path and two distinct
//! databases (distinct `db` UUID) can never resolve to the same directory.
//!
//! Migration: `get` falls back to the legacy in-repo `{repo}/.gfs/secrets/<name>`
//! when the out-of-tree secret is absent, so databases deployed before A1 stay
//! revealable; a `put` writes the new location and clears any legacy copy, so no
//! plaintext lingers in the repository tree after the first write.

use std::io;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

/// Legacy in-repo secrets directory (relative to a database's repo root) — read
/// for migration fallback only; new writes go to the out-of-tree store.
const LEGACY_SECRETS_DIR: &str = ".gfs/secrets";

/// Env override for the out-of-tree secrets base. Default: the `secrets` sibling
/// of the node's `repositories/` root.
const SECRETS_DIR_ENV: &str = "GUEPARD_SECRETS_DIR";

/// A node-local, data-plane-owned credential store, keyed by the database's
/// `(org, project, db)` identity triple.
pub struct RepoCredentialVault;

impl RepoCredentialVault {
    /// Write `value` to the out-of-tree secret at mode `0600` (creating the
    /// directory at `0700` if needed). Overwrites, re-pins the mode, and clears
    /// any legacy in-repo copy so the migration completes on first write.
    ///
    /// # Errors
    /// An invalid identity segment or secret `name`, a `repositories_dir` with no
    /// parent (can't site the store), or an underlying filesystem error.
    pub fn put(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
        name: &str,
        value: &[u8],
    ) -> io::Result<()> {
        let path = out_of_tree_dir(repositories_dir, org, project, db)?.join(validate_name(name)?);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
            set_mode(dir, 0o700)?;
        }
        write_secret_file(&path, value)?;
        // Migration: drop the pre-A1 in-repo copy so no plaintext lingers in the
        // versioned tree. The new out-of-tree copy is authoritative for reads, so
        // this is best-effort — but a real failure to remove it defeats A1's
        // intent, so surface it rather than swallow it.
        match std::fs::remove_file(legacy_dir(repositories_dir, org, project, db)?.join(name)) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(
                org,
                project,
                db,
                name,
                error = %e,
                "failed to clear legacy in-repo secret after migration; plaintext may linger in the repo tree"
            ),
        }
        Ok(())
    }

    /// Read the secret; `Ok(None)` when it exists in neither the out-of-tree store
    /// nor the legacy in-repo location (migration fallback).
    ///
    /// # Errors
    /// An invalid identity segment or `name`, or a filesystem error other than
    /// not-found.
    pub fn get(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
        name: &str,
    ) -> io::Result<Option<Vec<u8>>> {
        let file = validate_name(name)?;
        let new = out_of_tree_dir(repositories_dir, org, project, db)?.join(file);
        match std::fs::read(&new) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // Pre-A1 databases keep the secret in the repo tree.
                let legacy = legacy_dir(repositories_dir, org, project, db)?.join(file);
                match std::fs::read(&legacy) {
                    Ok(bytes) => Ok(Some(bytes)),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(e),
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Remove the named secret from both the out-of-tree store and any legacy
    /// in-repo copy; idempotent (a missing secret is `Ok`).
    ///
    /// # Errors
    /// An invalid identity segment or `name`, or a filesystem error other than
    /// not-found.
    pub fn delete(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
        name: &str,
    ) -> io::Result<()> {
        let file = validate_name(name)?;
        remove_if_present(&out_of_tree_dir(repositories_dir, org, project, db)?.join(file))?;
        remove_if_present(&legacy_dir(repositories_dir, org, project, db)?.join(file))
    }

    /// Remove the database's entire out-of-tree secrets directory. `destroy` must
    /// call this: the store lives outside `repositories/`, so a repository
    /// `remove_dir_all` no longer reaches it. Idempotent.
    ///
    /// # Errors
    /// An invalid identity segment or a filesystem error other than not-found.
    pub fn delete_all(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
    ) -> io::Result<()> {
        match std::fs::remove_dir_all(out_of_tree_dir(repositories_dir, org, project, db)?) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// The out-of-tree secrets directory for a database: `{base}/{org}/{project}/{db}`,
/// keyed by the explicit triple. `base` is `$GUEPARD_SECRETS_DIR`, else the
/// `secrets` sibling of `repositories_dir`.
fn out_of_tree_dir(
    repositories_dir: &Path,
    org: &str,
    project: &str,
    db: &str,
) -> io::Result<PathBuf> {
    let base = match std::env::var_os(SECRETS_DIR_ENV) {
        Some(b) if !b.is_empty() => PathBuf::from(b),
        _ => match repositories_dir.parent() {
            Some(node_root) => node_root.join("secrets"),
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "repositories_dir has no parent to site the secrets store beside: {}",
                        repositories_dir.display()
                    ),
                ));
            }
        },
    };
    Ok(base.join(triple(org, project, db)?))
}

/// The pre-A1 in-repo secrets directory (read/cleared for migration only).
fn legacy_dir(repositories_dir: &Path, org: &str, project: &str, db: &str) -> io::Result<PathBuf> {
    Ok(repositories_dir
        .join(triple(org, project, db)?)
        .join(LEGACY_SECRETS_DIR))
}

/// Validate the identity triple and join it as `{org}/{project}/{db}`.
fn triple(org: &str, project: &str, db: &str) -> io::Result<PathBuf> {
    Ok(PathBuf::from(validate_segment("org", org)?)
        .join(validate_segment("project", project)?)
        .join(validate_segment("db", db)?))
}

/// An identity segment must be a single, non-empty path component — no separators,
/// `.`/`..`, or NUL — so a triple can never escape the store root.
fn validate_segment<'a>(label: &str, value: &'a str) -> io::Result<&'a str> {
    let ok = !value.is_empty()
        && value != "."
        && value != ".."
        && !value.contains(['/', '\\', '\0']);
    if ok {
        Ok(value)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid {label} identity segment: {value:?}"),
        ))
    }
}

/// Secret names are `[a-z0-9_]+` (non-empty) — no separators, `..`, dots, or
/// uppercase — so a name can never escape the secrets directory.
fn validate_name(name: &str) -> io::Result<&str> {
    let ok = !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if ok {
        Ok(name)
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid secret name: {name:?} (expected [a-z0-9_]+)"),
        ))
    }
}

fn remove_if_present(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(unix)]
fn write_secret_file(path: &Path, value: &[u8]) -> io::Result<()> {
    use std::io::Write;
    // `mode(0o600)` applies only when this call creates the file; on an overwrite
    // it is ignored, so re-pin the mode explicitly afterwards.
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(value)?;
    set_mode(path, 0o600)
}

#[cfg(not(unix))]
fn write_secret_file(path: &Path, value: &[u8]) -> io::Result<()> {
    std::fs::write(path, value)
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> io::Result<()> {
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_mode(mode);
    std::fs::set_permissions(path, perms)
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORG: &str = "acme";
    const PROJECT: &str = "proj";
    const DB: &str = "db1";

    /// A tempdir with a `repositories/` root; the vault sites its store at the
    /// `secrets` sibling, i.e. `{tmp}/secrets/{org}/{project}/{db}`, inside the
    /// tempdir. Returns (guard, repositories_dir, expected out-of-tree dir).
    fn layout() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repositories_dir = tmp.path().join("repositories");
        std::fs::create_dir_all(&repositories_dir).unwrap();
        let vault = tmp.path().join("secrets").join(ORG).join(PROJECT).join(DB);
        (tmp, repositories_dir, vault)
    }

    #[test]
    fn put_then_get_roundtrips_out_of_tree() {
        let (_tmp, repos, vault) = layout();
        RepoCredentialVault::put(&repos, ORG, PROJECT, DB, "owner_password", b"s3cr3t").unwrap();
        assert!(
            vault.join("owner_password").exists(),
            "secret must live at the out-of-tree store {}",
            vault.display()
        );
        assert!(
            !repos.join(ORG).join(PROJECT).join(DB).join(".gfs/secrets/owner_password").exists(),
            "nothing must be written inside the repo tree"
        );
        assert_eq!(
            RepoCredentialVault::get(&repos, ORG, PROJECT, DB, "owner_password").unwrap(),
            Some(b"s3cr3t".to_vec())
        );
    }

    #[test]
    fn get_reads_legacy_in_repo_secret_as_migration_fallback() {
        let (_tmp, repos, _vault) = layout();
        let legacy = repos.join(ORG).join(PROJECT).join(DB).join(".gfs/secrets");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("owner_password"), b"legacy-pw").unwrap();
        assert_eq!(
            RepoCredentialVault::get(&repos, ORG, PROJECT, DB, "owner_password").unwrap(),
            Some(b"legacy-pw".to_vec()),
            "reveal must still resolve a pre-A1 in-repo secret"
        );
    }

    #[test]
    fn put_supersedes_and_clears_the_legacy_copy() {
        let (_tmp, repos, vault) = layout();
        let legacy = repos.join(ORG).join(PROJECT).join(DB).join(".gfs/secrets");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("owner_password"), b"old").unwrap();
        RepoCredentialVault::put(&repos, ORG, PROJECT, DB, "owner_password", b"new").unwrap();
        assert_eq!(
            RepoCredentialVault::get(&repos, ORG, PROJECT, DB, "owner_password").unwrap(),
            Some(b"new".to_vec())
        );
        assert!(
            !legacy.join("owner_password").exists(),
            "put must clear the legacy in-repo copy so no plaintext lingers"
        );
        assert!(vault.join("owner_password").exists());
    }

    #[test]
    fn distinct_dbs_never_collide() {
        let (_tmp, repos, _vault) = layout();
        RepoCredentialVault::put(&repos, ORG, PROJECT, "db-a", "owner_password", b"A").unwrap();
        RepoCredentialVault::put(&repos, ORG, PROJECT, "db-b", "owner_password", b"B").unwrap();
        assert_eq!(
            RepoCredentialVault::get(&repos, ORG, PROJECT, "db-a", "owner_password").unwrap(),
            Some(b"A".to_vec())
        );
        assert_eq!(
            RepoCredentialVault::get(&repos, ORG, PROJECT, "db-b", "owner_password").unwrap(),
            Some(b"B".to_vec()),
            "a distinct db must have its own, non-colliding secret"
        );
    }

    #[test]
    fn get_missing_is_none() {
        let (_tmp, repos, _v) = layout();
        assert_eq!(
            RepoCredentialVault::get(&repos, ORG, PROJECT, DB, "absent").unwrap(),
            None
        );
    }

    #[test]
    fn delete_is_idempotent_and_clears_both_locations() {
        let (_tmp, repos, _v) = layout();
        let legacy = repos.join(ORG).join(PROJECT).join(DB).join(".gfs/secrets");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("owner_password"), b"legacy").unwrap();
        RepoCredentialVault::put(&repos, ORG, PROJECT, DB, "owner_password", b"x").unwrap();
        RepoCredentialVault::delete(&repos, ORG, PROJECT, DB, "owner_password").unwrap();
        assert_eq!(
            RepoCredentialVault::get(&repos, ORG, PROJECT, DB, "owner_password").unwrap(),
            None
        );
        RepoCredentialVault::delete(&repos, ORG, PROJECT, DB, "owner_password").unwrap();
    }

    #[test]
    fn delete_all_removes_the_out_of_tree_dir() {
        let (_tmp, repos, vault) = layout();
        RepoCredentialVault::put(&repos, ORG, PROJECT, DB, "owner_password", b"x").unwrap();
        assert!(vault.exists());
        RepoCredentialVault::delete_all(&repos, ORG, PROJECT, DB).unwrap();
        assert!(!vault.exists(), "delete_all must remove the store dir");
        RepoCredentialVault::delete_all(&repos, ORG, PROJECT, DB).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn secret_file_is_0600_and_dir_0700() {
        let (_tmp, repos, vault) = layout();
        RepoCredentialVault::put(&repos, ORG, PROJECT, DB, "owner_password", b"x").unwrap();
        let file_mode = std::fs::metadata(vault.join("owner_password"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600, "secret must be 0600, got {file_mode:o}");
        let dir_mode = std::fs::metadata(&vault).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700, "secrets dir must be 0700, got {dir_mode:o}");
    }

    #[test]
    fn rejects_traversal_in_identity_or_name() {
        let (_tmp, repos, _v) = layout();
        // Traversal / separators in any identity segment are refused.
        for (o, p, d) in [("..", PROJECT, DB), (ORG, "a/b", DB), (ORG, PROJECT, ""), (ORG, ".", DB)] {
            assert!(
                RepoCredentialVault::get(&repos, o, p, d, "owner_password").is_err(),
                "must reject identity ({o:?},{p:?},{d:?})"
            );
        }
        // Bad secret names too.
        for bad in ["", "../escape", "a/b", "UPPER", "dot.name"] {
            assert!(
                RepoCredentialVault::put(&repos, ORG, PROJECT, DB, bad, b"x").is_err(),
                "must reject name {bad:?}"
            );
        }
    }
}
