//! Node-local **intended managed-user set** (RFC 012): the current set of managed
//! login-role NAMES a repository should have. Kept OUTSIDE the versioned tree, a
//! sibling of the credential vault (the same out-of-tree store, keyed by the
//! `(org, project, db)` triple). After a version swap (`checkout`/branch/clone),
//! reconcile drops any cluster login role not in this set, so a dropped user
//! cannot resurrect via time-travel. Access is live control-plane state; data is
//! what's versioned — this record is never snapshotted.
//!
//! Stored as a newline-delimited list at mode `0600` (dir `0700`). An absent
//! record yields the defensive default [`ALWAYS_INTENDED`] — never empty — so
//! reconcile can never misread "no record" as "drop everything".

use std::collections::BTreeSet;
use std::io;
use std::path::Path;

use crate::utils::credential_vault::{out_of_tree_dir, set_mode, write_secret_file};

/// Filename of the intended-set record within the out-of-tree store.
const INTENDED_USERS_FILE: &str = "intended_users";

/// The always-present managed roles (RFC 009): the customer's least-privileged
/// `owner` login and the `developers` group. These are provisioned at deploy,
/// are non-revocable via RFC 007, and reconcile never drops them.
pub const ALWAYS_INTENDED: [&str; 2] = ["owner", "developers"];

/// A node-local record of the managed login roles a repository intends to have.
pub struct IntendedUserSet;

impl IntendedUserSet {
    /// Load the intended set. An absent record (a pre-RFC-012 database, or one
    /// never mutated) yields the defensive default [`ALWAYS_INTENDED`] — never an
    /// empty set — so reconcile cannot misread a missing record as "drop all".
    ///
    /// # Errors
    /// An invalid identity segment or a filesystem error other than not-found.
    pub fn load(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
    ) -> io::Result<BTreeSet<String>> {
        let path = out_of_tree_dir(repositories_dir, org, project, db)?.join(INTENDED_USERS_FILE);
        let mut set = default_set();
        match std::fs::read_to_string(&path) {
            Ok(contents) => {
                for name in contents.lines().map(str::trim).filter(|l| !l.is_empty()) {
                    set.insert(name.to_string());
                }
                Ok(set)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(set),
            Err(e) => Err(e),
        }
    }

    /// Overwrite the record with `set` (the always-intended roles are folded in,
    /// so the record can never lose `owner`/`developers`). Creates the store dir at
    /// `0700` and writes the file at `0600`.
    ///
    /// # Errors
    /// An invalid identity segment, an un-siteable store, or a filesystem error.
    pub fn save(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
        set: &BTreeSet<String>,
    ) -> io::Result<()> {
        let dir = out_of_tree_dir(repositories_dir, org, project, db)?;
        std::fs::create_dir_all(&dir)?;
        set_mode(&dir, 0o700)?;
        let mut full = default_set();
        full.extend(set.iter().cloned());
        let body = full.into_iter().collect::<Vec<_>>().join("\n");
        write_secret_file(&dir.join(INTENDED_USERS_FILE), body.as_bytes())
    }

    /// Seed the record with the deploy defaults ([`ALWAYS_INTENDED`]) — call on a
    /// fresh deploy so the head cluster and the record agree from the start.
    ///
    /// # Errors
    /// As [`Self::save`].
    pub fn seed(repositories_dir: &Path, org: &str, project: &str, db: &str) -> io::Result<()> {
        Self::save(repositories_dir, org, project, db, &BTreeSet::new())
    }

    /// Add a managed user to the set (after its in-cluster create succeeds).
    ///
    /// # Errors
    /// As [`Self::load`] / [`Self::save`].
    pub fn add(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
        name: &str,
    ) -> io::Result<()> {
        let mut set = Self::load(repositories_dir, org, project, db)?;
        set.insert(name.to_string());
        Self::save(repositories_dir, org, project, db, &set)
    }

    /// Remove a managed user from the set (after its in-cluster drop). The
    /// always-intended roles cannot be removed.
    ///
    /// # Errors
    /// As [`Self::load`] / [`Self::save`].
    pub fn remove(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
        name: &str,
    ) -> io::Result<()> {
        if ALWAYS_INTENDED.contains(&name) {
            return Ok(());
        }
        let mut set = Self::load(repositories_dir, org, project, db)?;
        set.remove(name);
        Self::save(repositories_dir, org, project, db, &set)
    }
}

fn default_set() -> BTreeSet<String> {
    ALWAYS_INTENDED.iter().map(|s| s.to_string()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORG: &str = "acme";
    const PROJECT: &str = "proj";
    const DB: &str = "db1";

    fn layout() -> (tempfile::TempDir, std::path::PathBuf) {
        let tmp = tempfile::tempdir().expect("tempdir");
        let repos = tmp.path().join("repositories");
        std::fs::create_dir_all(&repos).unwrap();
        (tmp, repos)
    }

    #[test]
    fn absent_record_defaults_to_owner_and_developers() {
        let (_t, repos) = layout();
        let set = IntendedUserSet::load(&repos, ORG, PROJECT, DB).unwrap();
        assert_eq!(
            set,
            ["developers", "owner"].iter().map(|s| s.to_string()).collect()
        );
    }

    #[test]
    fn add_then_load_roundtrips_and_keeps_defaults() {
        let (_t, repos) = layout();
        IntendedUserSet::seed(&repos, ORG, PROJECT, DB).unwrap();
        IntendedUserSet::add(&repos, ORG, PROJECT, DB, "app_ro").unwrap();
        IntendedUserSet::add(&repos, ORG, PROJECT, DB, "app_rw").unwrap();
        let set = IntendedUserSet::load(&repos, ORG, PROJECT, DB).unwrap();
        assert!(set.contains("owner") && set.contains("developers"));
        assert!(set.contains("app_ro") && set.contains("app_rw"));
    }

    #[test]
    fn remove_drops_a_user_but_never_a_default() {
        let (_t, repos) = layout();
        IntendedUserSet::add(&repos, ORG, PROJECT, DB, "app_ro").unwrap();
        IntendedUserSet::remove(&repos, ORG, PROJECT, DB, "app_ro").unwrap();
        let set = IntendedUserSet::load(&repos, ORG, PROJECT, DB).unwrap();
        assert!(!set.contains("app_ro"), "removed user must be gone");
        // Removing a default is a no-op — the record must never lose owner/developers.
        IntendedUserSet::remove(&repos, ORG, PROJECT, DB, "owner").unwrap();
        let set = IntendedUserSet::load(&repos, ORG, PROJECT, DB).unwrap();
        assert!(set.contains("owner"), "owner must never be removable");
    }

    #[test]
    fn stored_out_of_tree_not_in_repo_and_0600() {
        let (tmp, repos) = layout();
        IntendedUserSet::add(&repos, ORG, PROJECT, DB, "app_ro").unwrap();
        let record = tmp
            .path()
            .join("secrets")
            .join(ORG)
            .join(PROJECT)
            .join(DB)
            .join("intended_users");
        assert!(record.exists(), "record must live out of tree at {}", record.display());
        assert!(
            !repos.join(ORG).join(PROJECT).join(DB).join(".gfs").exists(),
            "nothing written inside the repo tree"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&record).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "record must be 0600, got {mode:o}");
        }
    }

    #[test]
    fn rejects_traversal_in_identity() {
        let (_t, repos) = layout();
        assert!(IntendedUserSet::load(&repos, "..", PROJECT, DB).is_err());
        assert!(IntendedUserSet::add(&repos, ORG, "a/b", DB, "x").is_err());
    }
}
