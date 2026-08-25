//! Node-local, out-of-tree store location + safe file write, shared by the
//! non-secret node-local records (e.g. the intended managed-user record). Keyed
//! by the explicit `(org, project, db)` triple under `$GUEPARD_SECRETS_DIR`
//! (default: the `secrets` sibling of the node's `repositories/` root), so a
//! record cannot drift with the shape of a repository path.
//!
//! This holds only path + mode + atomic-write mechanics. Credential storage is a
//! data-plane concern behind a port — it does NOT live in the domain.

use std::io;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

/// Env override for the out-of-tree base. Default: the `secrets` sibling of the
/// node's `repositories/` root.
const SECRETS_DIR_ENV: &str = "GUEPARD_SECRETS_DIR";

/// The out-of-tree directory for a database: `{base}/{org}/{project}/{db}`.
pub(crate) fn out_of_tree_dir(
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
                        "repositories_dir has no parent to site the store beside: {}",
                        repositories_dir.display()
                    ),
                ));
            }
        },
    };
    Ok(base.join(triple(org, project, db)?))
}

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

#[cfg(unix)]
static TMP_WRITE_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Atomic replace at mode `0600`: write a sibling temp, fsync, rename over the
/// target. A crash mid-write leaves the OLD file intact (never truncated/empty),
/// and a concurrent reader never sees a partial file.
#[cfg(unix)]
pub(crate) fn write_secret_file(path: &Path, value: &[u8]) -> io::Result<()> {
    use std::io::Write;
    use std::sync::atomic::Ordering;
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;
    let fname = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no filename"))?;
    let tmp = dir.join(format!(
        ".{fname}.tmp.{}.{}",
        std::process::id(),
        TMP_WRITE_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let write = || -> io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)?;
        file.write_all(value)?;
        file.sync_all()
    };
    if let Err(e) = write() {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    set_mode(&tmp, 0o600)?;
    std::fs::rename(&tmp, path).inspect_err(|_| {
        let _ = std::fs::remove_file(&tmp);
    })
}

#[cfg(not(unix))]
pub(crate) fn write_secret_file(path: &Path, value: &[u8]) -> io::Result<()> {
    std::fs::write(path, value)
}

#[cfg(unix)]
pub(crate) fn set_mode(path: &Path, mode: u32) -> io::Result<()> {
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_mode(mode);
    std::fs::set_permissions(path, perms)
}

#[cfg(not(unix))]
pub(crate) fn set_mode(_path: &Path, _mode: u32) -> io::Result<()> {
    Ok(())
}
