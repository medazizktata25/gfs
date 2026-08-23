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

use crate::model::db_user::RoleInfo;
use crate::ports::compute::Compute;
use crate::ports::database_provider::DatabaseProviderRegistry;
use crate::usecases::repository::manage_users_usecase::{
    ManageUsersError, ManageUsersUseCase, is_reserved_role,
};
use crate::utils::credential_vault::RepoCredentialVault;
use crate::utils::intended_users::IntendedUserSet;

/// What a reconcile did, returned so the caller (the DP daemon) can audit it.
/// mak has no audit port — auditing is a control-plane/data-plane concern — so
/// the outcome is logged here and handed back for the caller to record.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReconcileOutcome {
    /// Managed login roles dropped because they were not in the intended set.
    pub dropped: Vec<String>,
}

impl ReconcileOutcome {
    pub fn is_noop(&self) -> bool {
        self.dropped.is_empty()
    }
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
    /// out-of-tree intended-set record. `trigger` names the operation for the
    /// log/audit.
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
        trigger: &str,
    ) -> Result<ReconcileOutcome, ManageUsersError> {
        let intended = IntendedUserSet::load(repositories_dir, org, project, db)
            .map_err(|e| ManageUsersError::Config(format!("read intended-user set: {e}")))?;

        // Fail-closed: a listing failure must surface, not leave surplus roles.
        let roles = self.manage.list_roles(repo_path).await?;

        let mut outcome = ReconcileOutcome::default();
        for username in surplus_login_roles(&roles, &intended) {
            // `drop_role` is transactional + dependent-safe (REASSIGN OWNED +
            // DROP OWNED before DROP ROLE), so a surplus role that owns objects in
            // the restored older data is neither orphaned nor blocks the drop.
            self.manage.drop_role(repo_path, &username).await?;
            outcome.dropped.push(username);
        }

        if !outcome.is_noop() {
            tracing::warn!(
                trigger,
                dropped = ?outcome.dropped,
                "reconcile dropped non-intended managed login roles (revoked access made durable across the version swap)"
            );
        }
        Ok(outcome)
    }

    /// Re-key each present managed **login** role to its current password from the
    /// node-local durability vault (`userpw_<username>`, RFC 008 A2). After a
    /// version swap the restored catalog holds each role's *snapshot* password, so
    /// a password rotated away since that commit would otherwise reappear. Reserved
    /// roles are skipped: `owner` has no rotation path (its vault value always
    /// equals the snapshot — a no-op) and the rest are not user-managed. A role
    /// with no vault entry (pre-A2 or clone-inherited) is left on its snapshot
    /// password — no regression over pre-A2 behaviour.
    ///
    /// Best-effort per user: a single unreadable entry or failed apply is warned
    /// and skipped (the role keeps its snapshot password — the documented degrade),
    /// never fail-closing the checkout — one bad entry must not brick every version
    /// swap. Only a failure to *enumerate* roles propagates. Returns roles re-keyed.
    ///
    /// # Errors
    /// Propagates a failure to list the cluster's roles.
    pub async fn rekey_from_vault(
        &self,
        repo_path: &Path,
        repositories_dir: &Path,
        org: &str,
        project: &str,
        db: &str,
    ) -> Result<Vec<String>, ManageUsersError> {
        let roles = self.manage.list_roles(repo_path).await?;
        let mut rekeyed = Vec::new();
        for username in rekeyable_login_roles(&roles) {
            let key = format!("userpw_{username}");
            // Best-effort PER USER: a single bad/unreadable entry (e.g. an
            // out-of-charset username whose key the vault rejects, or a transient
            // I/O error) must NOT fail-close the whole checkout — that would brick
            // every version swap of the database. Warn and leave that role on its
            // snapshot password (the documented degrade), and keep re-keying the
            // rest. A listing failure still propagates (we cannot enumerate).
            let entry = match RepoCredentialVault::get(repositories_dir, org, project, db, &key) {
                Ok(entry) => entry,
                Err(e) => {
                    tracing::warn!(
                        user = %username,
                        error = %e,
                        "re-key: unreadable durability-vault entry; leaving this role on its snapshot password"
                    );
                    continue;
                }
            };
            let Some(bytes) = entry else { continue }; // pre-A2 / inherited → keep snapshot pw
            let password = String::from_utf8_lossy(&bytes);
            if let Err(e) = self.manage.set_password(repo_path, &username, &password).await {
                tracing::warn!(
                    user = %username,
                    error = %e,
                    "re-key: failed to apply the vaulted password; leaving this role on its snapshot password"
                );
                continue;
            }
            rekeyed.push(username);
        }
        if !rekeyed.is_empty() {
            tracing::warn!(
                rekeyed = ?rekeyed,
                "re-keyed managed login roles to their current vaulted passwords (a rotated-away password cannot reappear across the version swap)"
            );
        }
        Ok(rekeyed)
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
fn surplus_login_roles(roles: &[RoleInfo], intended: &BTreeSet<String>) -> Vec<String> {
    roles
        .iter()
        .filter(|r| r.can_login && !intended.contains(&r.username))
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
    fn surplus_is_login_roles_not_in_the_intended_set() {
        let roles = vec![
            role("owner", true),
            role("developers", false),
            role("app_keep", true),
            role("app_stale", true),
            role("grp_nologin", false),
        ];
        let intended: BTreeSet<String> = ["owner", "developers", "app_keep"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(
            surplus_login_roles(&roles, &intended),
            vec!["app_stale".to_string()],
            "only the non-intended LOGIN role is surplus"
        );
    }

    #[test]
    fn intended_and_nonlogin_roles_are_never_surplus() {
        let roles = vec![
            role("owner", true),
            role("developers", false),
            role("app_keep", true),
        ];
        let intended: BTreeSet<String> = ["owner", "developers", "app_keep"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert!(surplus_login_roles(&roles, &intended).is_empty());
    }

    #[test]
    fn only_login_roles_are_ever_surplus() {
        // Even with an empty intended set, a NOLOGIN group is never dropped in phase 1.
        let roles = vec![role("x", true), role("grp", false)];
        assert_eq!(
            surplus_login_roles(&roles, &BTreeSet::new()),
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
}
