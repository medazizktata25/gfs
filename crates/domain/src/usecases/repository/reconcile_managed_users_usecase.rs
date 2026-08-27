//! Managed-user reconciliation (RFC 012, phase 1). After an operation that swaps
//! the workspace data directory (`checkout`, branch, clone/resume), the restored
//! `pg_authid` re-lists whatever managed roles existed at that data version — so a
//! user dropped since then reappears. This use case projects the repository's
//! **intended set** (node-local, current, non-versioned) back onto the cluster:
//! it drops every managed **login** role that is not in the intended set, making
//! role removal monotonic across time-travel. Access is live; data is versioned.
//!
//! Phase 1 reconciles role *existence* only. Privilege/grant revocation and
//! password rotation also live in the versioned catalog and are phase 2.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use crate::model::db_user::{RoleInfo, RolePreset, RoleSpec};
use crate::ports::compute::Compute;
use crate::ports::database_provider::DatabaseProviderRegistry;
use crate::usecases::repository::manage_users_usecase::{
    ManageUsersError, ManageUsersUseCase, is_reserved_role,
};
use crate::utils::intended_users::IntendedUserSet;

/// What a reconcile did, returned so the caller (the DP daemon) can audit it.
/// mak has no audit port — auditing is a control-plane/data-plane concern — so
/// the outcome is logged here and handed back for the caller to record.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReconcileOutcome {
    /// Managed login roles dropped because they were not in the intended set.
    pub dropped: Vec<String>,
    /// Surplus login roles that could not be dropped (they own objects in the
    /// restored older data) and were instead **quarantined** — disabled + password
    /// rotated — so access is removed without mutating customer data.
    pub quarantined: Vec<String>,
}

impl ReconcileOutcome {
    pub fn is_noop(&self) -> bool {
        self.dropped.is_empty() && self.quarantined.is_empty()
    }
}

/// How a version swap should treat the managed-user set — the checkout/clone
/// split made explicit, so a caller must declare which trust relationship the
/// restored data has to the current node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileMode {
    /// In-place time-travel (`checkout` / branch): faithful content. The platform
    /// re-asserts only its own config (drop tombstoned users, ensure-present,
    /// re-key, re-apply presets); a customer's own roles and grants are untouched.
    Faithful,
    /// A new trust domain (`clone`, typically prod→dev): the inherited managed
    /// state is derived, so platform credentials are re-keyed at the trust-boundary
    /// crossing — the caller fresh-resets the owner and tombstones the inherited
    /// login roles before reconciling.
    Derived,
}

impl ReconcileMode {
    /// Log/audit label.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Faithful => "faithful",
            Self::Derived => "derived",
        }
    }
}

/// What a preset re-apply did (RFC 012 phase 3). `failed` is returned so the caller
/// surfaces it — a failed re-apply leaves that role on its restored-snapshot
/// privileges (a revoked privilege may be live), which must be visible, not silent.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PresetReapplyOutcome {
    /// Roles whose current preset was re-applied.
    pub reapplied: Vec<String>,
    /// Roles whose preset re-apply failed (left on snapshot privileges).
    pub failed: Vec<String>,
}

/// What an adopt did (declarable intent). Returned so the caller can report
/// whether the role was newly promoted into the managed set or already tracked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptOutcome {
    /// The adopted role.
    pub username: String,
    /// Its current preset (read from the role's comment), carried into the record.
    pub preset: Option<RolePreset>,
    /// `false` if the role was already in the intended set (adopt is idempotent).
    pub newly_adopted: bool,
}

/// Reconcile the cluster's managed login roles to the repository's intended set.
pub struct ReconcileManagedUsersUseCase<R: DatabaseProviderRegistry> {
    manage: ManageUsersUseCase<R>,
}

impl<R: DatabaseProviderRegistry> ReconcileManagedUsersUseCase<R> {
    pub fn new(compute: Arc<dyn Compute>, registry: Arc<R>) -> Self {
        Self {
            manage: ManageUsersUseCase::new(compute, registry),
        }
    }

    /// Drop every managed **login** role on the cluster that is not in the
    /// repository's intended set. `repo_path` resolves the cluster (its `.gfs`
    /// config); the `(org, project, db)` triple + `repositories_dir` key the
    /// out-of-tree intended-set record. `mode` declares the trust relationship of
    /// the restored data (faithful in-place checkout vs derived clone) for the
    /// log/audit and the caller's surrounding re-key policy.
    ///
    /// Fail-closed: a listing error propagates (never leave surplus roles by
    /// silently skipping). Idempotent: an already-aligned cluster drops nothing.
    /// `owner`/`developers` and every intended user are always retained (the
    /// intended set defaults to `{owner, developers}` when no record exists, and
    /// `list_roles` never surfaces the system supers).
    ///
    /// # Errors
    /// Propagates a failure to read the intended set or to list/drop roles.
    pub async fn reconcile(
        &self,
        repo_path: &Path,
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
        mode: ReconcileMode,
    ) -> Result<ReconcileOutcome, ManageUsersError> {
        // Deprovision is intent-driven: reconcile removes only the login roles the
        // platform explicitly tombstoned (deprovisioned via the managed route) that
        // the restored snapshot has resurrected — never a role it merely does not
        // recognise (a customer's own SQL role survives). Fail-closed: a failure to
        // read the tombstones or list the cluster surfaces, never leaving a
        // resurrected deprovisioned role live.
        let tombstones = IntendedUserSet::load_tombstones(repositories_dir, org, project, db)
            .map_err(|e| ManageUsersError::Config(format!("read tombstones: {e}")))?;
        let roles = self.manage.list_roles(repo_path).await?;

        let mut outcome = ReconcileOutcome::default();
        for username in tombstoned_present_roles(&roles, &tombstones) {
            // `drop_role` is transactional + dependent-safe (REASSIGN OWNED +
            // DROP OWNED before DROP ROLE), so a surplus role that owns objects in
            // the restored older data is neither orphaned nor blocks the drop.
            match self.manage.drop_role(repo_path, &username).await {
                Ok(()) => outcome.dropped.push(username),
                Err(drop_err) => {
                    // DROP could not complete (e.g. REASSIGN/DROP OWNED could not
                    // fully clear this role's ownership in the restored data).
                    // Degrade to *disabled* instead of bricking the checkout:
                    // quarantine removes access (NOLOGIN + rotated password)
                    // without mutating customer data. The security guarantee
                    // ("this role has no access") rides on this cheap,
                    // dependency-free op, not on DROP. Only if quarantine ALSO
                    // fails do we fail-closed — a surplus role still able to log in
                    // with its snapshot password is unacceptable.
                    tracing::warn!(
                        user = %username,
                        error = %drop_err,
                        "reconcile could not drop a surplus managed login role; falling back to quarantine"
                    );
                    // A fresh, unknowable, thrown-away password (~244 bits). Never
                    // surfaced and never vaulted: its unknowability IS the control,
                    // and re-quarantine happens fresh on every future checkout.
                    let throwaway = format!(
                        "{}{}",
                        uuid::Uuid::new_v4().simple(),
                        uuid::Uuid::new_v4().simple()
                    );
                    if let Err(quarantine_err) =
                        self.manage.quarantine_role(repo_path, &username, &throwaway).await
                    {
                        return Err(ManageUsersError::Failed {
                            exit_code: 1,
                            message: format!(
                                "reconcile could neither drop nor quarantine surplus login role {username}: drop={drop_err}; quarantine={quarantine_err}"
                            ),
                        });
                    }
                    outcome.quarantined.push(username);
                }
            }
        }

        if !outcome.is_noop() {
            tracing::warn!(
                mode = mode.as_str(),
                dropped = ?outcome.dropped,
                quarantined = ?outcome.quarantined,
                "reconcile removed deprovisioned managed login roles resurrected by the version swap (revoked access made durable)"
            );
        }
        Ok(outcome)
    }

    /// The present managed **login** roles eligible for re-keying (non-reserved).
    /// The data plane resolves each one's current password from the node-local
    /// credential vault (behind a port) and hands them back to
    /// [`Self::reapply_passwords`] — the domain never reads a secret store itself.
    /// An enumeration failure propagates: a checkout cannot proceed if the roles
    /// cannot be listed.
    ///
    /// # Errors
    /// Propagates a failure to list the cluster's roles.
    pub async fn list_rekeyable_roles(
        &self,
        repo_path: &Path,
    ) -> Result<Vec<String>, ManageUsersError> {
        let roles = self.manage.list_roles(repo_path).await?;
        Ok(rekeyable_login_roles(&roles))
    }

    /// Apply each `(username -> current password)` to the cluster so a password
    /// rotated away since the checked-out commit cannot reappear (RFC 008 A2). The
    /// passwords are resolved out-of-band by the data plane (the domain never reads
    /// a secret store). Best-effort PER USER — a single failed apply is warned and
    /// skipped so one bad user cannot brick the version swap. Returns the roles
    /// re-keyed.
    ///
    /// # Errors
    /// Never fail-closes on a per-user apply; currently infallible at the boundary
    /// but returns `Result` for symmetry and future strictness.
    pub async fn reapply_passwords(
        &self,
        repo_path: &Path,
        passwords: &std::collections::BTreeMap<String, String>,
    ) -> Result<Vec<String>, ManageUsersError> {
        let mut rekeyed = Vec::new();
        for (username, password) in passwords {
            if let Err(e) = self.manage.set_password(repo_path, username, password).await {
                tracing::warn!(
                    user = %username,
                    error = %e,
                    "re-key: failed to apply the current password; leaving this role on its snapshot password"
                );
                continue;
            }
            rekeyed.push(username.clone());
        }
        if !rekeyed.is_empty() {
            tracing::warn!(
                rekeyed = ?rekeyed,
                "re-keyed managed login roles to their current passwords (a rotated-away password cannot reappear across the version swap)"
            );
        }
        Ok(rekeyed)
    }

    /// The live managed login roles that the restored snapshot is MISSING — a
    /// managed user created after the checked-out commit is absent from that older
    /// `pg_authid`. Returns each absent user with its recorded preset so the caller
    /// (the data plane) can re-create it with its current vaulted password, keeping
    /// current managed access complete across the version swap. Reserved roles
    /// (`owner`/`developers`) are always present (seeded) and never listed here.
    ///
    /// # Errors
    /// Propagates a failure to read the intended record or list the cluster's roles.
    pub async fn list_absent_intended_roles(
        &self,
        repo_path: &Path,
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
    ) -> Result<Vec<(String, Option<RolePreset>)>, ManageUsersError> {
        let intended = IntendedUserSet::load(repositories_dir, org, project, db)
            .map_err(|e| ManageUsersError::Config(format!("read intended set: {e}")))?;
        let presets = IntendedUserSet::load_presets(repositories_dir, org, project, db)
            .map_err(|e| ManageUsersError::Config(format!("read intended presets: {e}")))?;
        let present: BTreeSet<String> = self
            .manage
            .list_roles(repo_path)
            .await?
            .into_iter()
            .map(|r| r.username)
            .collect();
        Ok(intended
            .into_iter()
            .filter(|name| !present.contains(name) && !is_reserved_role(name))
            .map(|name| {
                let preset = presets.get(&name).copied();
                (name, preset)
            })
            .collect())
    }

    /// Re-create the given managed roles (each with its current password + preset,
    /// supplied by the caller from the vault + record), so a live managed user
    /// absent from the restored snapshot is present again after the version swap.
    /// Best-effort PER USER — a single failed create is warned and skipped so one
    /// bad user cannot brick the checkout. Returns the roles created.
    ///
    /// # Errors
    /// Never fail-closes on a per-user create; returns `Result` for symmetry.
    pub async fn ensure_present_roles(
        &self,
        repo_path: &Path,
        specs: &[RoleSpec],
    ) -> Result<Vec<String>, ManageUsersError> {
        let mut created = Vec::new();
        for spec in specs {
            if let Err(e) = self.manage.create_role(repo_path, spec).await {
                tracing::warn!(
                    user = %spec.username,
                    error = %e,
                    "ensure-present: failed to re-create a live managed role missing from the restored snapshot"
                );
                continue;
            }
            created.push(spec.username.clone());
        }
        if !created.is_empty() {
            tracing::warn!(
                created = ?created,
                "re-created live managed roles the restored snapshot was missing (current managed access made complete across the version swap)"
            );
        }
        Ok(created)
    }

    /// Adopt an existing customer login role into the managed set — promote it
    /// from customer content to platform-managed config, so it becomes durable
    /// (re-created by ensure-present if a restored snapshot predates it) instead of
    /// being treated as an untracked customer role. Reads the role's current preset
    /// from the engine and carries it into the record. Idempotent: adopting an
    /// already-tracked role refreshes its preset and reports `newly_adopted = false`.
    ///
    /// Rejects a reserved platform role (already managed) and a NOLOGIN role (only
    /// login users are adopted). The caller supplies + vaults the role's current
    /// password out of band, so ensure-present can re-create it unchanged.
    ///
    /// # Errors
    /// The role does not exist, is reserved, or is not a login role; or the
    /// intended-set read/write fails.
    pub async fn adopt_role(
        &self,
        repo_path: &Path,
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
        username: &str,
    ) -> Result<AdoptOutcome, ManageUsersError> {
        if is_reserved_role(username) {
            return Err(ManageUsersError::InvalidInput(format!(
                "'{username}' is a reserved platform role and is already platform-managed"
            )));
        }
        let role = self
            .manage
            .list_roles(repo_path)
            .await?
            .into_iter()
            .find(|r| r.username == username)
            .ok_or_else(|| {
                ManageUsersError::InvalidInput(format!(
                    "role '{username}' does not exist; nothing to adopt"
                ))
            })?;
        if !role.can_login {
            return Err(ManageUsersError::InvalidInput(format!(
                "'{username}' is a NOLOGIN group role, not a login user; only login users are adopted"
            )));
        }
        let preset = role.preset.as_deref().and_then(RolePreset::parse);
        let newly_adopted = !IntendedUserSet::load(repositories_dir, org, project, db)
            .map_err(|e| ManageUsersError::Config(format!("read intended set: {e}")))?
            .iter()
            .any(|n| n.as_str() == username);
        IntendedUserSet::add(repositories_dir, org, project, db, username, preset)
            .map_err(|e| ManageUsersError::Config(format!("record adopted role: {e}")))?;
        tracing::info!(
            user = %username,
            preset = ?preset,
            newly_adopted,
            "adopted a customer role into the managed set (durable across time-travel)"
        );
        Ok(AdoptOutcome {
            username: username.to_string(),
            preset,
            newly_adopted,
        })
    }

    /// Re-apply each present managed login role's current **preset** from the
    /// node-local record (RFC 012 phase 3). A version swap restores the snapshot's
    /// ACLs, so a privilege revoked since that commit would resurrect; `apply_preset`
    /// is declarative (revoke-all-then-grant-preset), so re-applying a role's current
    /// preset makes its privileges exactly that preset again — the revoked privilege
    /// stays revoked. `owner` is the deploy owner used for the preset's default
    /// privileges. A role with no recorded preset, or not present after the swap, is
    /// skipped (its privileges revert to the snapshot — no regression vs pre-phase-3).
    ///
    /// Best-effort per user (a failed apply is warned + skipped, never bricking the
    /// checkout); only a failure to enumerate roles propagates. Returns the outcome
    /// (roles re-applied + roles whose re-apply failed) so the caller surfaces the
    /// failures — a skipped role keeps its restored-snapshot privileges.
    ///
    /// # Errors
    /// Propagates a failure to read the record or to list the cluster's roles.
    pub async fn reapply_presets_from_record(
        &self,
        repo_path: &Path,
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
        owner: &str,
    ) -> Result<PresetReapplyOutcome, ManageUsersError> {
        let presets = IntendedUserSet::load_presets(repositories_dir, org, project, db)
            .map_err(|e| ManageUsersError::Config(format!("read intended presets: {e}")))?;
        if presets.is_empty() {
            return Ok(PresetReapplyOutcome::default());
        }
        // Only re-apply to roles that survive reconcile (present + can login). A
        // recorded user absent from this snapshot (created after the checked-out
        // commit) is skipped — apply_preset on a missing role would just error.
        let present: BTreeSet<String> = self
            .manage
            .list_roles(repo_path)
            .await?
            .into_iter()
            .filter(|r| r.can_login)
            .map(|r| r.username)
            .collect();

        let mut outcome = PresetReapplyOutcome::default();
        for (name, preset) in presets {
            if is_reserved_role(&name) || !present.contains(&name) {
                continue;
            }
            if let Err(e) = self
                .manage
                .apply_preset(repo_path, &name, preset, Some(owner))
                .await
            {
                tracing::warn!(
                    user = %name,
                    error = %e,
                    "re-apply preset: failed to enforce the recorded preset; leaving this role on its snapshot privileges"
                );
                outcome.failed.push(name);
                continue;
            }
            outcome.reapplied.push(name);
        }
        if !outcome.reapplied.is_empty() {
            tracing::warn!(
                reapplied = ?outcome.reapplied,
                "re-applied managed-user presets after the version swap (revoked privileges made durable)"
            );
        }
        if !outcome.failed.is_empty() {
            tracing::warn!(
                failed = ?outcome.failed,
                "re-apply preset FAILED for some users — they retain restored-snapshot privileges (a revoked privilege may be live); retry the checkout or re-apply the preset"
            );
        }
        Ok(outcome)
    }
}

/// The managed **login** roles eligible for re-key: non-reserved login roles.
/// Reserved roles (owner/developers/superusers) are excluded — see
/// [`ReconcileManagedUsersUseCase::rekey_from_vault`]. Pure, so the selection
/// policy is unit-tested directly while the vault-read + apply orchestration is
/// covered by the integration tests.
fn rekeyable_login_roles(roles: &[RoleInfo]) -> Vec<String> {
    roles
        .iter()
        .filter(|r| r.can_login && !is_reserved_role(&r.username))
        .map(|r| r.username.clone())
        .collect()
}

/// Compute the managed **login** roles on the cluster that are not in the intended
/// set — the surplus reconcile drops. Pure (no I/O): the drop *policy* is
/// unit-tested here directly, while the exec orchestration (list → drop) +
/// fail-closed behaviour are covered by the integration tests against a real
/// cluster. Phase 1 targets login roles only; a surplus NOLOGIN group is left.
///
/// Reserved platform roles are never surplus (defence-in-depth): `owner`/
/// `developers` are always in the intended set anyway, and `list_roles` filters
/// out the superusers — but guarding here too means a reserved login role can
/// never be selected for a `drop_role` that would be rejected and fail-close
/// (brick) the whole checkout, symmetric with the re-key path.
fn tombstoned_present_roles(roles: &[RoleInfo], tombstones: &BTreeSet<String>) -> Vec<String> {
    roles
        .iter()
        .filter(|r| {
            r.can_login && tombstones.contains(&r.username) && !is_reserved_role(&r.username)
        })
        .map(|r| r.username.clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn role(name: &str, can_login: bool) -> RoleInfo {
        RoleInfo {
            username: name.into(),
            can_login,
            is_superuser: false,
            preset: None,
        }
    }

    #[test]
    fn tombstoned_present_login_roles_are_dropped() {
        let roles = vec![
            role("owner", true),
            role("developers", false),
            role("app_keep", true),
            role("app_stale", true),
            role("grp_nologin", false),
        ];
        // Only app_stale was deprovisioned (tombstoned). app_keep is a live login
        // role, and an untracked customer role would be neither tombstoned nor
        // intended — both survive. Reconcile drops only what was deprovisioned.
        let tombstones: BTreeSet<String> = ["app_stale"].iter().map(|s| s.to_string()).collect();
        assert_eq!(
            tombstoned_present_roles(&roles, &tombstones),
            vec!["app_stale".to_string()],
            "only the tombstoned + present LOGIN role is dropped"
        );
    }

    #[test]
    fn untombstoned_login_roles_are_never_dropped() {
        // No tombstones → reconcile drops nothing, even a live login role the
        // platform does not recognise (a customer's own SQL role survives).
        let roles = vec![
            role("owner", true),
            role("developers", false),
            role("app_customer", true),
        ];
        assert!(tombstoned_present_roles(&roles, &BTreeSet::new()).is_empty());
    }

    #[test]
    fn only_login_roles_are_ever_tombstone_dropped() {
        // A NOLOGIN group is never dropped even when tombstoned (phase-1 scope).
        let roles = vec![role("x", true), role("grp", false)];
        let tombstones: BTreeSet<String> = ["x", "grp"].iter().map(|s| s.to_string()).collect();
        assert_eq!(
            tombstoned_present_roles(&roles, &tombstones),
            vec!["x".to_string()]
        );
    }

    #[test]
    fn rekeyable_is_nonreserved_login_roles_only() {
        let roles = vec![
            role("owner", true),       // reserved → skip (no rotation path; no-op)
            role("developers", false), // reserved + nologin → skip
            role("app_rw", true),      // re-key
            role("app_ro", true),      // re-key
            role("grp", false),        // nologin → skip
        ];
        assert_eq!(
            rekeyable_login_roles(&roles),
            vec!["app_rw".to_string(), "app_ro".to_string()],
            "only non-reserved LOGIN roles are re-keyed"
        );
    }

    #[test]
    fn reserved_login_roles_are_never_tombstone_dropped() {
        // Defence-in-depth: a reserved login role (owner) is never selected for drop
        // even if it somehow appears tombstoned — drop_role would reject it and
        // fail-close the whole checkout.
        let roles = vec![role("owner", true), role("app_x", true)];
        let tombstones: BTreeSet<String> =
            ["owner", "app_x"].iter().map(|s| s.to_string()).collect();
        assert_eq!(
            tombstoned_present_roles(&roles, &tombstones),
            vec!["app_x".to_string()],
            "a reserved login role is never dropped; only the non-reserved tombstoned one"
        );
    }

    #[test]
    fn reconcile_mode_labels_are_stable() {
        // The checkout/clone split is explicit: faithful in-place time-travel vs a
        // derived new-trust-domain clone. The labels flow into logs/audit.
        assert_eq!(ReconcileMode::Faithful.as_str(), "faithful");
        assert_eq!(ReconcileMode::Derived.as_str(), "derived");
    }
}
