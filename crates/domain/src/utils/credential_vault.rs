//! Per-repository credential vault (RFC 008, Amendment A1): a database's
//! revealable password lives in a **data-plane-owned** store — node-local and
//! OUTSIDE the versioned repository tree — at mode `0600`, never in the cloud and
//! never inside a snapshot. The store is keyed by the repository's
//! `org/project/db` tail under `$GUEPARD_SECRETS_DIR` (default: the `secrets`
//! sibling of the node's `repositories/` root). This is a dumb typed key/value
//! store over the filesystem; the node filesystem plus `0600` are the trust
//! boundary (RFC 008 §7).
//!
//! Migration: `get` falls back to the legacy in-repo `{repo}/.gfs/secrets/<name>`
//! when the out-of-tree secret is absent, so databases deployed before A1 stay
//! revealable; a `put` writes the new location and clears any legacy copy, so no
//! plaintext lingers in the repository tree after the first write.

use std::io;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

/// Legacy secrets directory relative to a repo root — read for migration
/// fallback only; new writes go to the out-of-tree store.
const LEGACY_SECRETS_DIR: &str = ".gfs/secrets";

/// Env override for the out-of-tree secrets base. Default: the `secrets` sibling
/// of the node's `repositories/` root, derived from the repo path.
const SECRETS_DIR_ENV: &str = "GUEPARD_SECRETS_DIR";

/// A node-local, data-plane-owned credential store for a database, keyed by the
/// repository's `org/project/db` tail.
pub struct RepoCredentialVault;

impl RepoCredentialVault {
    /// Write `value` to the out-of-tree secret at mode `0600` (creating the
    /// directory at `0700` if needed). Overwrites, re-pins the mode, and clears
    /// any legacy in-repo copy so the migration completes on first write.
    ///
    /// # Errors
    /// Invalid `name` (see [`validate_name`]), a repo path too shallow to derive
    /// the `org/project/db` key, or an underlying filesystem error.
    pub fn put(repo: &Path, name: &str, value: &[u8]) -> io::Result<()> {
        let path = secret_path(repo, name)?;
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
            set_mode(dir, 0o700)?;
        }
        write_secret_file(&path, value)?;
        // Best-effort: drop the pre-A1 copy so no plaintext lingers in the tree.
        let _ = remove_if_present(&legacy_secret_path(repo, name)?);
        Ok(())
    }

    /// Read the secret; `Ok(None)` when it exists in neither the out-of-tree
    /// store nor the legacy in-repo location (migration fallback).
    ///
    /// # Errors
    /// Invalid `name`, an un-derivable key, or a filesystem error other than
    /// not-found.
    pub fn get(repo: &Path, name: &str) -> io::Result<Option<Vec<u8>>> {
        match std::fs::read(secret_path(repo, name)?) {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // Pre-A1 databases keep the secret in the repo tree.
                match std::fs::read(legacy_secret_path(repo, name)?) {
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
    /// Invalid `name`, an un-derivable key, or a filesystem error other than
    /// not-found.
    pub fn delete(repo: &Path, name: &str) -> io::Result<()> {
        remove_if_present(&secret_path(repo, name)?)?;
        remove_if_present(&legacy_secret_path(repo, name)?)
    }

    /// Remove the database's entire out-of-tree secrets directory. `destroy` must
    /// call this: the store lives outside `repositories/`, so a repository
    /// `remove_dir_all` no longer reaches it. Idempotent.
    ///
    /// # Errors
    /// An un-derivable key or a filesystem error other than not-found.
    pub fn delete_all(repo: &Path) -> io::Result<()> {
        match std::fs::remove_dir_all(vault_dir(repo)?) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }
}

/// The out-of-tree secrets directory for `repo`: `{base}/{org}/{project}/{db}`,
/// keyed by the repo's own last three path components. `base` is
/// `$GUEPARD_SECRETS_DIR`, else the `secrets` sibling of the `repositories/` root
/// the repo lives under.
fn vault_dir(repo: &Path) -> io::Result<PathBuf> {
    let comps: Vec<_> = repo.components().collect();
    if comps.len() < 3 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "repo path too shallow to derive org/project/db key: {}",
                repo.display()
            ),
        ));
    }
    let split = comps.len() - 3;
    let tail: PathBuf = comps[split..].iter().collect(); // {org}/{project}/{db}
    let base = match std::env::var_os(SECRETS_DIR_ENV) {
        Some(b) if !b.is_empty() => PathBuf::from(b),
        _ => {
            // `repositories/` root = the repo path minus its org/project/db tail;
            // the store is its `secrets` sibling.
            let repositories_root: PathBuf = comps[..split].iter().collect();
            match repositories_root.parent() {
                Some(node_root) => node_root.join("secrets"),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("cannot derive secrets base from repo: {}", repo.display()),
                    ));
                }
            }
        }
    };
    Ok(base.join(tail))
}

/// Validate `name` and join it under the out-of-tree store.
fn secret_path(repo: &Path, name: &str) -> io::Result<PathBuf> {
    validate_name(name)?;
    Ok(vault_dir(repo)?.join(name))
}

/// The pre-A1 in-repo path for `name` (read/cleared for migration only).
fn legacy_secret_path(repo: &Path, name: &str) -> io::Result<PathBuf> {
    validate_name(name)?;
    Ok(repo.join(LEGACY_SECRETS_DIR).join(name))
}

/// Names are `[a-z0-9_]+` (non-empty) — no path separators, `..`, dots, or
/// uppercase — so a name can never escape the secrets directory.
fn validate_name(name: &str) -> io::Result<()> {
    let valid = !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
    if valid {
        Ok(())
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

    /// Build a repo shaped like the real layout — `{tmp}/repositories/{org}/{project}/{db}`
    /// — so the vault derives its out-of-tree store at `{tmp}/secrets/{org}/{project}/{db}`,
    /// inside the tempdir. Returns (tempdir guard, repo path, expected vault dir).
    fn repo() -> (tempfile::TempDir, PathBuf, PathBuf) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repo = tmp
            .path()
            .join("repositories")
            .join("acme")
            .join("proj")
            .join("db1");
        std::fs::create_dir_all(&repo).unwrap();
        let vault = tmp.path().join("secrets").join("acme").join("proj").join("db1");
        (tmp, repo, vault)
    }

    #[test]
    fn put_then_get_roundtrips_out_of_tree() {
        let (_tmp, repo, vault) = repo();
        RepoCredentialVault::put(&repo, "owner_password", b"s3cr3t").unwrap();
        // Stored OUTSIDE the repo tree, at the derived sibling location.
        assert!(
            vault.join("owner_password").exists(),
            "secret must live at the out-of-tree store {}",
            vault.display()
        );
        assert!(
            !repo.join(".gfs/secrets/owner_password").exists(),
            "nothing must be written inside the repo tree"
        );
        assert_eq!(
            RepoCredentialVault::get(&repo, "owner_password").unwrap(),
            Some(b"s3cr3t".to_vec())
        );
    }

    #[test]
    fn get_reads_legacy_in_repo_secret_as_migration_fallback() {
        let (_tmp, repo, _vault) = repo();
        // A pre-A1 database: secret only in the repo tree, nothing out-of-tree.
        let legacy = repo.join(".gfs/secrets");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("owner_password"), b"legacy-pw").unwrap();
        assert_eq!(
            RepoCredentialVault::get(&repo, "owner_password").unwrap(),
            Some(b"legacy-pw".to_vec()),
            "reveal must still resolve a pre-A1 in-repo secret"
        );
    }

    #[test]
    fn put_supersedes_and_clears_the_legacy_copy() {
        let (_tmp, repo, vault) = repo();
        let legacy = repo.join(".gfs/secrets");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("owner_password"), b"old").unwrap();
        RepoCredentialVault::put(&repo, "owner_password", b"new").unwrap();
        assert_eq!(
            RepoCredentialVault::get(&repo, "owner_password").unwrap(),
            Some(b"new".to_vec())
        );
        assert!(
            !legacy.join("owner_password").exists(),
            "put must clear the legacy in-repo copy so no plaintext lingers"
        );
        assert!(vault.join("owner_password").exists());
    }

    #[test]
    fn get_missing_is_none() {
        let (_tmp, repo, _v) = repo();
        assert_eq!(RepoCredentialVault::get(&repo, "absent").unwrap(), None);
    }

    #[test]
    fn delete_is_idempotent_and_clears_both_locations() {
        let (_tmp, repo, _v) = repo();
        let legacy = repo.join(".gfs/secrets");
        std::fs::create_dir_all(&legacy).unwrap();
        std::fs::write(legacy.join("owner_password"), b"legacy").unwrap();
        RepoCredentialVault::put(&repo, "owner_password", b"x").unwrap();
        RepoCredentialVault::delete(&repo, "owner_password").unwrap();
        assert_eq!(RepoCredentialVault::get(&repo, "owner_password").unwrap(), None);
        // Second delete on the now-absent secret is still Ok.
        RepoCredentialVault::delete(&repo, "owner_password").unwrap();
    }

    #[test]
    fn delete_all_removes_the_out_of_tree_dir() {
        let (_tmp, repo, vault) = repo();
        RepoCredentialVault::put(&repo, "owner_password", b"x").unwrap();
        assert!(vault.exists());
        RepoCredentialVault::delete_all(&repo).unwrap();
        assert!(!vault.exists(), "delete_all must remove the store dir");
        // Idempotent.
        RepoCredentialVault::delete_all(&repo).unwrap();
    }

    #[test]
    fn put_overwrites() {
        let (_tmp, repo, _v) = repo();
        RepoCredentialVault::put(&repo, "owner_password", b"first").unwrap();
        RepoCredentialVault::put(&repo, "owner_password", b"second").unwrap();
        assert_eq!(
            RepoCredentialVault::get(&repo, "owner_password").unwrap(),
            Some(b"second".to_vec())
        );
    }

    #[cfg(unix)]
    #[test]
    fn secret_file_is_0600_and_dir_0700() {
        let (_tmp, repo, vault) = repo();
        RepoCredentialVault::put(&repo, "owner_password", b"x").unwrap();
        let file_mode = std::fs::metadata(vault.join("owner_password"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600, "secret must be 0600, got {file_mode:o}");
        let dir_mode = std::fs::metadata(&vault).unwrap().permissions().mode() & 0o777;
        assert_eq!(dir_mode, 0o700, "secrets dir must be 0700, got {dir_mode:o}");
    }

    #[cfg(unix)]
    #[test]
    fn overwrite_keeps_0600_even_if_prior_mode_was_loose() {
        let (_tmp, repo, vault) = repo();
        RepoCredentialVault::put(&repo, "owner_password", b"first").unwrap();
        let path = vault.join("owner_password");
        set_mode(&path, 0o644).unwrap();
        RepoCredentialVault::put(&repo, "owner_password", b"second").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "overwrite must re-pin 0600, got {mode:o}");
    }

    #[test]
    fn rejects_names_that_could_escape_the_vault() {
        let (_tmp, repo, _v) = repo();
        for bad in ["", "../escape", "a/b", "UPPER", "with space", "dot.name", ".."] {
            assert!(
                RepoCredentialVault::put(&repo, bad, b"x").is_err(),
                "put must reject name {bad:?}"
            );
            assert!(
                RepoCredentialVault::get(&repo, bad).is_err(),
                "get must reject name {bad:?}"
            );
            assert!(
                RepoCredentialVault::delete(&repo, bad).is_err(),
                "delete must reject name {bad:?}"
            );
        }
    }
}
