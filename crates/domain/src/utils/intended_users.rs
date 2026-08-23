//! Node-local **intended managed-user record** (RFC 012): the current set of managed
//! login-role NAMES a repository should have, plus each one's current privilege
//! **preset** (RFC 012 phase 3). Kept OUTSIDE the versioned tree, a sibling of the
//! credential vault (the same out-of-tree store, keyed by the `(org, project, db)`
//! triple). After a version swap (`checkout`/branch/clone), reconcile drops any
//! cluster login role not in this record (phase 1), and re-applies each recorded
//! preset so a revoked privilege cannot resurrect (phase 3). Access is live
//! control-plane state; data is what's versioned — this record is never snapshotted.
//!
//! Stored newline-delimited at mode `0600` (dir `0700`), one entry per line as
//! `name` or `name<TAB>preset`. An absent record yields the defensive default
//! [`ALWAYS_INTENDED`] — never empty — so reconcile can never misread "no record" as
//! "drop everything". An old names-only record still loads (entries with no preset).

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::Path;

use crate::model::db_user::RolePreset;
use crate::utils::credential_vault::{out_of_tree_dir, set_mode, write_secret_file};

/// Filename of the intended-record within the out-of-tree store.
const INTENDED_USERS_FILE: &str = "intended_users";

/// The always-present managed roles (RFC 009): the customer's least-privileged
/// `owner` login and the `developers` group. These are provisioned at deploy,
/// are non-revocable via RFC 007, and reconcile never drops them. They carry no
/// preset (their privileges are provisioned directly, not via a managed preset).
pub const ALWAYS_INTENDED: [&str; 2] = ["owner", "developers"];

/// A node-local record of the managed login roles a repository intends to have,
/// each with its current preset (if any).
pub struct IntendedUserSet;

impl IntendedUserSet {
    /// Load the intended role **names**. An absent record yields the defensive
    /// default [`ALWAYS_INTENDED`] — never an empty set — so reconcile cannot
    /// misread a missing record as "drop all". (Phase-1 surface; unchanged.)
    ///
    /// # Errors
    /// An invalid identity segment or a filesystem error other than not-found.
    pub fn load(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
    ) -> io::Result<BTreeSet<String>> {
        Ok(Self::load_map(repositories_dir, org, project, db)?
            .into_keys()
            .collect())
    }

    /// Load each intended user's current **preset** (RFC 012 phase 3). Only entries
    /// that carry a preset are returned; `owner`/`developers` and no-preset users
    /// are absent. An absent/old names-only record yields an empty map.
    ///
    /// # Errors
    /// As [`Self::load`].
    pub fn load_presets(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
    ) -> io::Result<BTreeMap<String, RolePreset>> {
        Ok(Self::load_map(repositories_dir, org, project, db)?
            .into_iter()
            .filter_map(|(name, preset)| preset.map(|p| (name, p)))
            .collect())
    }

    /// Seed the record with the deploy defaults ([`ALWAYS_INTENDED`], no presets) —
    /// call on a fresh deploy so the head cluster and the record agree from the start.
    ///
    /// # Errors
    /// As [`Self::save_map`].
    pub fn seed(repositories_dir: &Path, org: &str, project: &str, db: &str) -> io::Result<()> {
        Self::save_map(repositories_dir, org, project, db, &default_map())
    }

    /// Record a managed user (after its in-cluster create succeeds), with the preset
    /// it was created with (if any). Overwrites any prior preset for that name.
    ///
    /// # Errors
    /// As [`Self::load`] / [`Self::save_map`].
    pub fn add(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
        name: &str,
        preset: Option<RolePreset>,
    ) -> io::Result<()> {
        let mut map = Self::load_map(repositories_dir, org, project, db)?;
        map.insert(name.to_string(), preset);
        Self::save_map(repositories_dir, org, project, db, &map)
    }

    /// Update a managed user's current preset (after an in-cluster `apply_preset`).
    /// Adds the entry if absent (defensive — head and the record must not diverge).
    ///
    /// # Errors
    /// As [`Self::load`] / [`Self::save_map`].
    pub fn record_preset(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
        name: &str,
        preset: RolePreset,
    ) -> io::Result<()> {
        // Reserved platform roles (owner/developers) are not managed via presets;
        // never record one for them (apply_preset rejects them anyway).
        if ALWAYS_INTENDED.contains(&name) {
            return Ok(());
        }
        let mut map = Self::load_map(repositories_dir, org, project, db)?;
        map.insert(name.to_string(), Some(preset));
        Self::save_map(repositories_dir, org, project, db, &map)
    }

    /// Remove a managed user from the record (after its in-cluster drop). The
    /// always-intended roles cannot be removed. Preserves other users' presets.
    ///
    /// # Errors
    /// As [`Self::load`] / [`Self::save_map`].
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
        let mut map = Self::load_map(repositories_dir, org, project, db)?;
        map.remove(name);
        Self::save_map(repositories_dir, org, project, db, &map)
    }

    /// Load the full name→optional-preset map, with the defaults folded in.
    fn load_map(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
    ) -> io::Result<BTreeMap<String, Option<RolePreset>>> {
        let path = out_of_tree_dir(repositories_dir, org, project, db)?.join(INTENDED_USERS_FILE);
        let mut map = default_map();
        match std::fs::read_to_string(&path) {
            Ok(contents) => {
                for line in contents.lines().map(str::trim).filter(|l| !l.is_empty()) {
                    let mut parts = line.splitn(2, '\t');
                    let name = parts.next().unwrap_or("").trim();
                    if name.is_empty() {
                        continue;
                    }
                    // An unknown/corrupt preset label degrades to "no preset" (the
                    // user is still tracked; it just won't be re-applied a preset).
                    let preset = parts.next().and_then(|p| RolePreset::parse(p.trim()));
                    map.insert(name.to_string(), preset);
                }
                Ok(map)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(map),
            Err(e) => Err(e),
        }
    }

    /// Overwrite the record with `map` (the always-intended roles are folded in, so
    /// the record can never lose `owner`/`developers`). Dir `0700`, file `0600`.
    fn save_map(
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
        map: &BTreeMap<String, Option<RolePreset>>,
    ) -> io::Result<()> {
        let dir = out_of_tree_dir(repositories_dir, org, project, db)?;
        std::fs::create_dir_all(&dir)?;
        set_mode(&dir, 0o700)?;
        let mut full = default_map();
        for (name, preset) in map {
            full.insert(name.clone(), *preset);
        }
        let body = full
            .into_iter()
            .map(|(name, preset)| match preset {
                Some(p) => format!("{name}\t{}", p.as_str()),
                None => name,
            })
            .collect::<Vec<_>>()
            .join("\n");
        write_secret_file(&dir.join(INTENDED_USERS_FILE), body.as_bytes())
    }
}

fn default_map() -> BTreeMap<String, Option<RolePreset>> {
    ALWAYS_INTENDED
        .iter()
        .map(|s| (s.to_string(), None))
        .collect()
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
        // No presets by default.
        assert!(
            IntendedUserSet::load_presets(&repos, ORG, PROJECT, DB)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn add_then_load_roundtrips_and_keeps_defaults() {
        let (_t, repos) = layout();
        IntendedUserSet::seed(&repos, ORG, PROJECT, DB).unwrap();
        IntendedUserSet::add(&repos, ORG, PROJECT, DB, "app_ro", Some(RolePreset::Readonly)).unwrap();
        IntendedUserSet::add(&repos, ORG, PROJECT, DB, "app_rw", Some(RolePreset::Readwrite)).unwrap();
        let set = IntendedUserSet::load(&repos, ORG, PROJECT, DB).unwrap();
        assert!(set.contains("owner") && set.contains("developers"));
        assert!(set.contains("app_ro") && set.contains("app_rw"));
        let presets = IntendedUserSet::load_presets(&repos, ORG, PROJECT, DB).unwrap();
        assert_eq!(presets.get("app_ro"), Some(&RolePreset::Readonly));
        assert_eq!(presets.get("app_rw"), Some(&RolePreset::Readwrite));
        // Defaults carry no preset.
        assert!(!presets.contains_key("owner") && !presets.contains_key("developers"));
    }

    #[test]
    fn record_preset_updates_an_existing_users_preset() {
        let (_t, repos) = layout();
        IntendedUserSet::add(&repos, ORG, PROJECT, DB, "app", Some(RolePreset::Readwrite)).unwrap();
        IntendedUserSet::record_preset(&repos, ORG, PROJECT, DB, "app", RolePreset::Readonly)
            .unwrap();
        let presets = IntendedUserSet::load_presets(&repos, ORG, PROJECT, DB).unwrap();
        assert_eq!(
            presets.get("app"),
            Some(&RolePreset::Readonly),
            "the downgrade must be the recorded current preset"
        );
    }

    #[test]
    fn remove_drops_a_user_but_never_a_default_and_keeps_other_presets() {
        let (_t, repos) = layout();
        IntendedUserSet::add(&repos, ORG, PROJECT, DB, "app_ro", Some(RolePreset::Readonly)).unwrap();
        IntendedUserSet::add(&repos, ORG, PROJECT, DB, "app_rw", Some(RolePreset::Readwrite)).unwrap();
        IntendedUserSet::remove(&repos, ORG, PROJECT, DB, "app_ro").unwrap();
        let set = IntendedUserSet::load(&repos, ORG, PROJECT, DB).unwrap();
        assert!(!set.contains("app_ro"), "removed user must be gone");
        // The other user's preset survives the rewrite.
        let presets = IntendedUserSet::load_presets(&repos, ORG, PROJECT, DB).unwrap();
        assert_eq!(presets.get("app_rw"), Some(&RolePreset::Readwrite));
        // Removing a default is a no-op — the record must never lose owner/developers.
        IntendedUserSet::remove(&repos, ORG, PROJECT, DB, "owner").unwrap();
        assert!(
            IntendedUserSet::load(&repos, ORG, PROJECT, DB)
                .unwrap()
                .contains("owner"),
            "owner must never be removable"
        );
    }

    #[test]
    fn old_names_only_record_still_loads() {
        let (tmp, repos) = layout();
        // Simulate a pre-phase-3 record: names only, no preset column.
        let dir = tmp.path().join("secrets").join(ORG).join(PROJECT).join(DB);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("intended_users"), "owner\ndevelopers\napp_legacy\n").unwrap();
        let set = IntendedUserSet::load(&repos, ORG, PROJECT, DB).unwrap();
        assert!(set.contains("app_legacy"));
        // No presets on a names-only record → phase-3 re-apply is a no-op for it.
        assert!(
            IntendedUserSet::load_presets(&repos, ORG, PROJECT, DB)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn stored_out_of_tree_not_in_repo_and_0600() {
        let (tmp, repos) = layout();
        IntendedUserSet::add(&repos, ORG, PROJECT, DB, "app_ro", Some(RolePreset::Readonly)).unwrap();
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
        assert!(IntendedUserSet::add(&repos, ORG, "a/b", DB, "x", None).is_err());
    }
}
