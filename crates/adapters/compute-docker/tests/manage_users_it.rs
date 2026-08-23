//! Integration: `ManageUsersUseCase` grant / revoke / list_privileges run inside
//! a real Postgres container — the live proof for the object-level grant work.
//! This drives the REAL use case through the REAL `DockerCompute` (not the mock
//! harness), so it exercises resolve(.gfs) → provider SQL → `Compute::exec` →
//! engine → map.
//!
//! Run: `GFS_DOCKER_IT=1 cargo test -p gfs-compute-docker --test manage_users_it -- --nocapture`

use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use gfs_compute_docker::DockerCompute;
use gfs_compute_docker::containers;
use gfs_domain::model::config::{EnvironmentConfig, GfsConfig, RuntimeConfig};
use gfs_domain::model::db_user::{
    GrantSpec, GrantableObject, Privilege, RevokeSpec, RolePreset, RoleSpec,
};
use gfs_domain::ports::database_provider::InMemoryDatabaseProviderRegistry;
use gfs_domain::usecases::repository::manage_users_usecase::ManageUsersUseCase;
use gfs_domain::usecases::repository::reconcile_managed_users_usecase::ReconcileManagedUsersUseCase;
use gfs_domain::utils::intended_users::IntendedUserSet;

/// A container name unique to each call (pid + counter) so tests never collide,
/// whether run in parallel or back-to-back within one test binary.
fn unique_container() -> String {
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    format!(
        "gfs-it-manage-users-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// Owns a throwaway `postgres:17` container and removes it on drop, so a failed
/// assertion (panic unwind) can never leak the container.
struct Postgres {
    container: String,
}

impl Postgres {
    fn start() -> Self {
        let container = unique_container();
        start_postgres(&container);
        Self { container }
    }

    fn psql(&self, sql: &str) -> String {
        psql(&self.container, sql)
    }

    fn name(&self) -> &str {
        &self.container
    }
}

impl Drop for Postgres {
    fn drop(&mut self) {
        stop_postgres(&self.container);
    }
}

fn docker_ok() -> bool {
    std::env::var("GFS_DOCKER_IT").ok().as_deref() == Some("1")
        && Command::new("docker")
            .args(["info"])
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
}

fn write_repo(path: &std::path::Path, container: &str) {
    std::fs::create_dir_all(path.join(".gfs")).expect("mkdir .gfs");
    let config = GfsConfig {
        mount_point: None,
        version: String::new(),
        description: String::new(),
        user: None,
        environment: Some(EnvironmentConfig {
            database_provider: "postgres".into(),
            database_version: "17".into(),
            database_port: None,
            display_name: None,
        }),
        runtime: Some(RuntimeConfig {
            runtime_provider: "docker".into(),
            runtime_version: "latest".into(),
            container_name: container.into(),
        }),
        storage: None,
        compute: None,
    };
    config.save(path).expect("save config");
}

/// Independent verification: run scalar SQL directly via `docker exec` psql.
fn psql(container: &str, sql: &str) -> String {
    let out = Command::new("docker")
        .args(["exec", container, "psql", "-U", "postgres", "-tAc", sql])
        .output()
        .expect("docker exec psql");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

fn start_postgres(container: &str) {
    let _ = Command::new("docker")
        .args(["rm", "-f", container])
        .output();
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--name",
            container,
            "-e",
            "POSTGRES_PASSWORD=postgres",
            "postgres:17",
        ])
        .status()
        .expect("docker run");
    assert!(status.success(), "docker run postgres:17 failed");

    // Gate on a real `SELECT 1` over the SAME channel the use case uses —
    // TCP `-h 127.0.0.1` with the password — not the unix socket. The postgres
    // image runs a temporary socket-only server during init, so a socket probe
    // (or `pg_isready`) reports ready while TCP is still refused; the use case
    // connects over TCP and would race "connection refused" / "starting up".
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let ready = Command::new("docker")
            .args([
                "exec",
                "-e",
                "PGPASSWORD=postgres",
                container,
                "psql",
                "-h",
                "127.0.0.1",
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-tAc",
                "SELECT 1",
            ])
            .output()
            .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).trim() == "1")
            .unwrap_or(false);
        if ready {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "postgres did not accept queries in time"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

fn stop_postgres(container: &str) {
    let _ = Command::new("docker")
        .args(["rm", "-f", container])
        .output();
}

#[tokio::test]
async fn grant_revoke_list_through_real_usecase() {
    if !docker_ok() {
        eprintln!("skip: set GFS_DOCKER_IT=1 and ensure docker is running");
        return;
    }

    let pg = Postgres::start();
    // Fixtures: a client role + a table to grant on.
    pg.psql("CREATE ROLE app_ro; CREATE TABLE public.t(id int);");

    let tmp = tempfile::tempdir().expect("tempdir");
    let repo = tmp.path();
    write_repo(repo, pg.name());

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");
    let uc = ManageUsersUseCase::new(compute, registry);

    let table = || GrantableObject::Table {
        schema: "public".into(),
        name: "t".into(),
    };

    // GRANT SELECT via the real use case, then verify the engine actually changed.
    uc.grant(
        repo,
        &GrantSpec {
            role: "app_ro".into(),
            object: table(),
            privileges: vec![Privilege::Select],
            with_grant_option: false,
            apply_to_future: None,
        },
    )
    .await
    .expect("uc.grant");
    let after_grant = pg.psql("SELECT has_table_privilege('app_ro','public.t','SELECT')");

    // LIST via the real use case — parses the live engine catalog JSON into
    // Vec<ObjectPrivilege> (the 4-way UNION projection end-to-end).
    let privs = uc
        .list_privileges(repo, "app_ro")
        .await
        .expect("uc.list_privileges");
    let has_select = privs.iter().any(|p| {
        p.object_type == "table" && p.object_name == "public.t" && p.privilege == "select"
    });

    // REVOKE via the real use case, then verify it's gone.
    uc.revoke(
        repo,
        &RevokeSpec {
            role: "app_ro".into(),
            object: table(),
            privileges: vec![Privilege::Select],
            cascade: false,
        },
    )
    .await
    .expect("uc.revoke");
    let after_revoke = pg.psql("SELECT has_table_privilege('app_ro','public.t','SELECT')");

    // The container is removed by `pg`'s Drop on scope exit — including on a
    // failed assertion below (panic unwind), so nothing leaks either way.
    assert_eq!(after_grant, "t", "SELECT must be granted after uc.grant");
    assert!(
        has_select,
        "uc.list_privileges must report the table SELECT grant; got: {privs:?}"
    );
    assert_eq!(after_revoke, "f", "SELECT must be gone after uc.revoke");
}

/// A preset's `ALTER DEFAULT PRIVILEGES` must be role-scoped to the deploy
/// `owner` so a preset user automatically sees tables the OWNER creates LATER —
/// not just the tables that existed at create time: `create_role` with
/// `default_privileges_owner: Some("owner")` emits `... FOR ROLE "owner" ...`.
/// The `reader_self` role (owner `None`) is the control: without `FOR ROLE`, a
/// preset only covers the connecting admin's future objects, so it must NOT see
/// the owner's future table.
#[tokio::test]
async fn preset_default_privileges_follow_owner_future_objects() {
    if !docker_ok() {
        eprintln!("skip: set GFS_DOCKER_IT=1 and ensure docker is running");
        return;
    }

    let pg = Postgres::start();
    // The customer's object-creating deploy owner, with CREATE on `public` so it
    // can make tables of its own.
    pg.psql("CREATE ROLE owner LOGIN; GRANT CREATE ON SCHEMA public TO owner;");

    let tmp = tempfile::tempdir().expect("tempdir");
    let repo = tmp.path();
    write_repo(repo, pg.name());

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");
    let uc = ManageUsersUseCase::new(compute, registry);

    // reader: readonly preset whose defaults are role-scoped to `owner` (the fix).
    uc.create_role(
        repo,
        &RoleSpec {
            username: "reader".into(),
            password: "pw_reader_1234".into(),
            preset: Some(RolePreset::Readonly),
            default_privileges_owner: Some("owner".into()),
        },
    )
    .await
    .expect("create reader");

    // reader_self: readonly preset with NO deploy owner — defaults role-scope to
    // the connecting admin (postgres), so they cannot cover owner's future objects.
    uc.create_role(
        repo,
        &RoleSpec {
            username: "reader_self".into(),
            password: "pw_reader_1234".into(),
            preset: Some(RolePreset::Readonly),
            default_privileges_owner: None,
        },
    )
    .await
    .expect("create reader_self");

    // The OWNER creates a FUTURE table (after both presets were applied).
    pg.psql("SET ROLE owner; CREATE TABLE public.future_t(id int); RESET ROLE;");

    let reader_sees = pg.psql("SELECT has_table_privilege('reader','public.future_t','SELECT')");
    let reader_self_sees =
        pg.psql("SELECT has_table_privilege('reader_self','public.future_t','SELECT')");

    assert_eq!(
        reader_sees, "t",
        "preset defaults FOR ROLE owner must auto-grant SELECT on owner's future table"
    );
    assert_eq!(
        reader_self_sees, "f",
        "control: without FOR ROLE owner, a preset must NOT cover the owner's future objects"
    );
}

/// `apply_preset` must be DECLARATIVE, not additive: downgrading a role from
/// `readwrite` to `readonly` must actually REVOKE the write privileges, not leave
/// them (an HTTP-200 downgrade that silently keeps INSERT/UPDATE/DELETE is a
/// security-correctness bug). Proven at the engine with `has_table_privilege`.
#[tokio::test]
async fn apply_preset_downgrade_revokes_write_privileges() {
    if !docker_ok() {
        eprintln!("skip: set GFS_DOCKER_IT=1 and ensure docker is running");
        return;
    }

    let pg = Postgres::start();
    // Owner + a pre-existing table the presets act on.
    pg.psql(
        "CREATE ROLE owner LOGIN; GRANT CREATE ON SCHEMA public TO owner; CREATE TABLE public.t(id int);",
    );

    let tmp = tempfile::tempdir().expect("tempdir");
    let repo = tmp.path();
    write_repo(repo, pg.name());

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");
    let uc = ManageUsersUseCase::new(compute, registry);

    // Create a readwrite user (write access), then DOWNGRADE to readonly.
    uc.create_role(
        repo,
        &RoleSpec {
            username: "u".into(),
            password: "pw_downgrade_1234".into(),
            preset: Some(RolePreset::Readwrite),
            default_privileges_owner: Some("owner".into()),
        },
    )
    .await
    .expect("create readwrite");
    let insert_before = pg.psql("SELECT has_table_privilege('u','public.t','INSERT')");

    uc.apply_preset(repo, "u", RolePreset::Readonly, Some("owner"))
        .await
        .expect("apply readonly");
    let insert_after = pg.psql("SELECT has_table_privilege('u','public.t','INSERT')");
    let select_after = pg.psql("SELECT has_table_privilege('u','public.t','SELECT')");

    assert_eq!(insert_before, "t", "readwrite preset must grant INSERT");
    assert_eq!(
        insert_after, "f",
        "downgrade to readonly must REVOKE INSERT (declarative, not additive)"
    );
    assert_eq!(select_after, "t", "readonly must still allow SELECT");
}

/// Reconcile after a version swap: the restored `pg_authid` re-lists a managed
/// login role that is no longer intended (it was dropped since this commit).
/// Reconcile drops that surplus role — making the prior removal durable across
/// time-travel — while every currently-intended role survives. A second pass is
/// a no-op (idempotent).
#[tokio::test]
async fn reconcile_drops_surplus_login_roles_keeps_intended_on_live_pg() {
    if !docker_ok() {
        eprintln!("skip: set GFS_DOCKER_IT=1 and ensure docker is running");
        return;
    }

    let pg = Postgres::start();
    // The cluster as an older checkout restores it: the deploy defaults, a
    // currently-intended app user, and a STALE login role that was dropped since
    // this commit (absent from the intended set) yet re-listed by pg_authid.
    pg.psql(
        "CREATE ROLE \"owner\" LOGIN; \
         CREATE ROLE developers NOLOGIN; \
         CREATE ROLE app_ro LOGIN PASSWORD 'pw_app_ro_1234'; \
         CREATE ROLE app_stale LOGIN PASSWORD 'pw_stale_1234';",
    );

    let tmp = tempfile::tempdir().expect("tempdir");
    let repositories_dir = tmp.path().join("repositories");
    let (org, project, db) = ("acme", "proj", "db-1234");
    let repo = repositories_dir.join(org).join(project).join(db);
    std::fs::create_dir_all(&repo).expect("mkdir repo");
    write_repo(&repo, pg.name());

    // The intended set: deploy defaults ({owner, developers}) plus the one app
    // user we still want. `app_stale` is deliberately NOT intended.
    IntendedUserSet::add(&repositories_dir, org, project, db, "app_ro").expect("seed intended");

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");
    let uc = ReconcileManagedUsersUseCase::new(compute, registry);

    let outcome = uc
        .reconcile(&repo, &repositories_dir, org, project, db, "checkout")
        .await
        .expect("reconcile");

    let exists = |role: &str| pg.psql(&format!("SELECT 1 FROM pg_roles WHERE rolname='{role}'"));

    // The container is removed by `pg`'s Drop even on a failed assertion below.
    assert_eq!(
        outcome.dropped,
        vec!["app_stale".to_string()],
        "only the surplus (non-intended) LOGIN role is dropped"
    );
    assert_eq!(exists("app_stale"), "", "app_stale must be gone after reconcile");
    assert_eq!(exists("app_ro"), "1", "the intended app user must survive");
    assert_eq!(exists("owner"), "1", "owner must survive");
    assert_eq!(exists("developers"), "1", "the developers group must survive");

    // Idempotent: a second reconcile on the now-aligned cluster drops nothing.
    let again = uc
        .reconcile(&repo, &repositories_dir, org, project, db, "checkout")
        .await
        .expect("reconcile idempotent");
    assert!(
        again.is_noop(),
        "an aligned cluster reconciles to a no-op, got: {:?}",
        again.dropped
    );
}

/// Attempt a password login as `user`, returning whether it authenticated.
/// Connects via the container's **non-loopback** address on purpose: the postgres
/// image's `pg_hba.conf` has `host all all 127.0.0.1/32 trust` (loopback ignores
/// the password), so a loopback probe cannot tell a right password from a wrong
/// one — the customer-facing `host all all all scram-sha-256` rule (which enforces
/// the password) matches the eth0 address instead.
fn login(container: &str, user: &str, password: &str) -> bool {
    let cmd = format!(
        "PGPASSWORD='{password}' psql -h \"$(hostname -i)\" -U '{user}' -d postgres -tAc 'SELECT 1'"
    );
    let out = Command::new("docker")
        .args(["exec", container, "sh", "-c", &cmd])
        .output()
        .expect("docker exec psql login");
    out.status.success() && String::from_utf8_lossy(&out.stdout).trim() == "1"
}

/// RFC 012 phase 2 / RFC 008 A2: a checkout restores a role's *snapshot* password.
/// `rekey_from_vault` re-applies the role's *current* password from the durability
/// vault, so a rotated-away password stops authenticating and the current one works.
#[tokio::test]
async fn rekey_from_vault_restores_current_password_over_a_stale_one_on_live_pg() {
    if !docker_ok() {
        eprintln!("skip: set GFS_DOCKER_IT=1 and ensure docker is running");
        return;
    }

    let pg = Postgres::start();
    // The role as a restored snapshot leaves it: LOGIN with the STALE password.
    pg.psql("CREATE ROLE app_rw LOGIN PASSWORD 'p_old_stale_1234';");

    let tmp = tempfile::tempdir().expect("tempdir");
    let repositories_dir = tmp.path().join("repositories");
    let (org, project, db) = ("acme", "proj", "db-rekey");
    let repo = repositories_dir.join(org).join(project).join(db);
    std::fs::create_dir_all(&repo).expect("mkdir repo");
    write_repo(&repo, pg.name());

    // The durability vault holds the CURRENT password (as create/set_password wrote it).
    gfs_domain::utils::credential_vault::RepoCredentialVault::put(
        &repositories_dir,
        org,
        project,
        db,
        "userpw_app_rw",
        b"p_new_current_1234",
    )
    .expect("seed durability vault");

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");
    let uc = ReconcileManagedUsersUseCase::new(compute, registry);

    // Baseline: the stale password authenticates, the current one does not (yet).
    assert!(login(pg.name(), "app_rw", "p_old_stale_1234"), "baseline: stale pw authenticates pre-rekey");

    let rekeyed = uc
        .rekey_from_vault(&repo, &repositories_dir, org, project, db)
        .await
        .expect("rekey");

    // The container is removed by `pg`'s Drop even on a failed assertion below.
    assert_eq!(rekeyed, vec!["app_rw".to_string()], "the non-reserved login role is re-keyed");
    assert!(
        login(pg.name(), "app_rw", "p_new_current_1234"),
        "current (vaulted) password must authenticate after re-key"
    );
    assert!(
        !login(pg.name(), "app_rw", "p_old_stale_1234"),
        "the stale snapshot password must no longer authenticate after re-key"
    );
}

/// Re-key must be best-effort per user: a role whose vault key the (lowercase-only)
/// vault rejects — e.g. an uppercase-named user, which the platform's username
/// charset allows — must be skipped, NOT fail-close the whole checkout. Regression
/// guard for the "one out-of-charset user bricks every version swap" bug.
#[tokio::test]
async fn rekey_skips_out_of_charset_username_without_bricking_on_live_pg() {
    if !docker_ok() {
        eprintln!("skip: set GFS_DOCKER_IT=1 and ensure docker is running");
        return;
    }

    let pg = Postgres::start();
    // An uppercase-named login role (its `userpw_App_RW` key is rejected by the
    // lowercase-only vault) beside a normal lowercase user that has a vault entry.
    pg.psql(
        "CREATE ROLE \"App_RW\" LOGIN PASSWORD 'upper_stays_1234'; \
         CREATE ROLE app_ro LOGIN PASSWORD 'ro_old_1234';",
    );

    let tmp = tempfile::tempdir().expect("tempdir");
    let repositories_dir = tmp.path().join("repositories");
    let (org, project, db) = ("acme", "proj", "db-mixedcase");
    let repo = repositories_dir.join(org).join(project).join(db);
    std::fs::create_dir_all(&repo).expect("mkdir repo");
    write_repo(&repo, pg.name());
    gfs_domain::utils::credential_vault::RepoCredentialVault::put(
        &repositories_dir,
        org,
        project,
        db,
        "userpw_app_ro",
        b"ro_new_1234",
    )
    .expect("seed vault for the lowercase user");

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");
    let uc = ReconcileManagedUsersUseCase::new(compute, registry);

    // Must NOT fail-close despite the uppercase role's unusable vault key.
    let rekeyed = uc
        .rekey_from_vault(&repo, &repositories_dir, org, project, db)
        .await
        .expect("re-key must not fail-close on an out-of-charset username");

    // The container is removed by `pg`'s Drop even on a failed assertion below.
    assert_eq!(
        rekeyed,
        vec!["app_ro".to_string()],
        "the valid lowercase user is re-keyed; the uppercase one is skipped, not fatal"
    );
    assert!(login(pg.name(), "app_ro", "ro_new_1234"), "app_ro re-keyed to its vaulted password");
    assert!(
        login(pg.name(), "App_RW", "upper_stays_1234"),
        "the skipped uppercase role is untouched — keeps its snapshot password"
    );
}
