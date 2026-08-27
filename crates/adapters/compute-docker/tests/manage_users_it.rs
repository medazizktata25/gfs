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
use gfs_domain::usecases::repository::reconcile_managed_users_usecase::{
    ReconcileManagedUsersUseCase, ReconcileMode,
};
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
/// not just the tables that existed at create time. Under the group model this
/// scoping is a property of the preset GROUP (`ALTER DEFAULT PRIVILEGES FOR ROLE
/// owner ... TO gfs_readonly`), set once and shared by every member — a database
/// has a single deploy owner, so all its readonly users correctly see the
/// owner's future objects via inherited group SELECT. The control is the *level*:
/// readonly confers SELECT, never INSERT, even on the owner's future table.
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

    // reader: readonly preset whose group defaults are role-scoped to `owner`.
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

    // The OWNER creates a FUTURE table (after the preset was applied).
    pg.psql("SET ROLE owner; CREATE TABLE public.future_t(id int); RESET ROLE;");

    let reader_selects =
        pg.psql("SELECT has_table_privilege('reader','public.future_t','SELECT')");
    let reader_inserts =
        pg.psql("SELECT has_table_privilege('reader','public.future_t','INSERT')");

    assert_eq!(
        reader_selects, "t",
        "readonly group defaults FOR ROLE owner must auto-grant SELECT on owner's future table"
    );
    assert_eq!(
        reader_inserts, "f",
        "readonly level must never confer INSERT, even on the owner's future table"
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
async fn reconcile_drops_tombstoned_roles_and_keeps_untracked_ones_on_live_pg() {
    if !docker_ok() {
        eprintln!("skip: set GFS_DOCKER_IT=1 and ensure docker is running");
        return;
    }

    let pg = Postgres::start();
    // As an older checkout restores it: the deploy defaults, a live managed user
    // (app_ro), a managed user DEPROVISIONED since this commit (app_stale, now
    // resurrected by the snapshot), and a role the customer created themselves via
    // raw SQL that the platform never tracked (app_untracked).
    pg.psql(
        "CREATE ROLE \"owner\" LOGIN; \
         CREATE ROLE developers NOLOGIN; \
         CREATE ROLE app_ro LOGIN PASSWORD 'pw_app_ro_1234'; \
         CREATE ROLE app_stale LOGIN PASSWORD 'pw_stale_1234'; \
         CREATE ROLE app_untracked LOGIN PASSWORD 'pw_untracked_1234';",
    );

    let tmp = tempfile::tempdir().expect("tempdir");
    let repositories_dir = tmp.path().join("repositories");
    let (org, project, db) = ("acme", "proj", "db-1234");
    let repo = repositories_dir.join(org).join(project).join(db);
    std::fs::create_dir_all(&repo).expect("mkdir repo");
    write_repo(&repo, pg.name());

    // app_ro is a live managed user; app_stale was deprovisioned (tombstoned).
    // app_untracked is deliberately NOT recorded — a customer's own SQL role, which
    // reconcile must never touch.
    IntendedUserSet::add(&repositories_dir, org, project, db, "app_ro", None).expect("record live user");
    IntendedUserSet::tombstone(&repositories_dir, org, project, db, "app_stale").expect("tombstone");

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");
    let uc = ReconcileManagedUsersUseCase::new(compute, registry);

    let outcome = uc
        .reconcile(&repo, &repositories_dir, org, project, db, ReconcileMode::Faithful)
        .await
        .expect("reconcile");

    let exists = |role: &str| pg.psql(&format!("SELECT 1 FROM pg_roles WHERE rolname='{role}'"));

    // The container is removed by `pg`'s Drop even on a failed assertion below.
    assert_eq!(
        outcome.dropped,
        vec!["app_stale".to_string()],
        "only the tombstoned + resurrected LOGIN role is dropped"
    );
    assert_eq!(exists("app_stale"), "", "the tombstoned role must be gone after reconcile");
    assert_eq!(exists("app_ro"), "1", "the live managed user must survive");
    assert_eq!(
        exists("app_untracked"),
        "1",
        "an UNTRACKED customer SQL role must NEVER be dropped (the allowlist over-reach is gone)"
    );
    assert_eq!(exists("owner"), "1", "owner must survive");
    assert_eq!(exists("developers"), "1", "the developers group must survive");

    // Idempotent: app_stale is gone, so a second reconcile drops nothing.
    let again = uc
        .reconcile(&repo, &repositories_dir, org, project, db, ReconcileMode::Faithful)
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

/// Run one statement AS a managed client user (over the customer-facing scram
/// endpoint, not the mgmt loopback), returning whether it SUCCEEDED. A
/// permission-denied error makes psql (with `ON_ERROR_STOP`) exit non-zero, so a
/// rejected escape reads as `false`.
fn as_user_ok(container: &str, user: &str, password: &str, sql: &str) -> bool {
    let cmd = format!(
        "PGPASSWORD='{password}' psql -h \"$(hostname -i)\" -U '{user}' -d postgres -v ON_ERROR_STOP=1 -c \"{sql}\""
    );
    Command::new("docker")
        .args(["exec", container, "sh", "-c", &cmd])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// A surplus role that cannot be DROPped (it owns objects in another database, so
/// REASSIGN/DROP OWNED in the reconcile db cannot clear the dependency) must be
/// QUARANTINED — disabled + password rotated — never left able to log in, and the
/// checkout must NOT be bricked. Degrade toward disabled, never toward exposed.
#[tokio::test]
async fn reconcile_quarantines_a_surplus_role_it_cannot_drop_on_live_pg() {
    if !docker_ok() {
        eprintln!("skip: set GFS_DOCKER_IT=1 and ensure docker is running");
        return;
    }

    let pg = Postgres::start();
    // Deploy defaults + a surplus (non-intended) LOGIN role that OWNS a table in a
    // *second* database. reconcile connects to the default `postgres` db, so its
    // REASSIGN/DROP OWNED there cannot reach the object in `otherdb` and DROP ROLE
    // fails — exactly the case the quarantine fallback exists for.
    pg.psql(
        "CREATE ROLE \"owner\" LOGIN; \
         CREATE ROLE developers NOLOGIN; \
         CREATE ROLE app_stale LOGIN PASSWORD 'pw_stale_1234';",
    );
    // Separate statement: CREATE DATABASE cannot run inside the implicit
    // transaction block that a multi-statement `psql -c` batch forms.
    pg.psql("CREATE DATABASE otherdb;");
    let exec_otherdb = |sql: &str| {
        let out = Command::new("docker")
            .args(["exec", pg.name(), "psql", "-U", "postgres", "-d", "otherdb", "-tAc", sql])
            .output()
            .expect("docker exec psql -d otherdb");
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    exec_otherdb(
        "CREATE TABLE owned_by_stale(id int); ALTER TABLE owned_by_stale OWNER TO app_stale;",
    );

    let tmp = tempfile::tempdir().expect("tempdir");
    let repositories_dir = tmp.path().join("repositories");
    let (org, project, db) = ("acme", "proj", "db-quar");
    let repo = repositories_dir.join(org).join(project).join(db);
    std::fs::create_dir_all(&repo).expect("mkdir repo");
    write_repo(&repo, pg.name());
    // app_stale was deprovisioned (tombstoned) but the restored snapshot resurrects
    // it; reconcile will try to drop it and fall back to quarantine.
    IntendedUserSet::seed(&repositories_dir, org, project, db).expect("seed intended");
    IntendedUserSet::tombstone(&repositories_dir, org, project, db, "app_stale").expect("tombstone");

    // The resurrected credential is LIVE before reconcile (the exposure this closes).
    assert!(
        login(pg.name(), "app_stale", "pw_stale_1234"),
        "precondition: the surplus role authenticates before reconcile"
    );
    let verifier_before = pg.psql("SELECT rolpassword FROM pg_authid WHERE rolname='app_stale'");

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");
    let uc = ReconcileManagedUsersUseCase::new(compute, registry);

    // Must NOT fail-close even though DROP ROLE cannot run: degrade to quarantine.
    let outcome = uc
        .reconcile(&repo, &repositories_dir, org, project, db, ReconcileMode::Faithful)
        .await
        .expect("reconcile must degrade to quarantine, never brick, when DROP can't run");

    assert!(
        outcome.dropped.is_empty(),
        "the undroppable role must not be reported dropped, got: {:?}",
        outcome.dropped
    );
    assert_eq!(
        outcome.quarantined,
        vec!["app_stale".to_string()],
        "the surplus role DROP could not remove must be quarantined"
    );

    // The resurrected credential is DEAD after reconcile — the security guarantee.
    assert!(
        !login(pg.name(), "app_stale", "pw_stale_1234"),
        "quarantine must kill the resurrected credential's login"
    );
    // Quarantine, not drop: the role still exists (its owned data is untouched)...
    assert_eq!(
        pg.psql("SELECT 1 FROM pg_roles WHERE rolname='app_stale'"),
        "1",
        "quarantine must not drop the role (no data destroyed)"
    );
    // ...login is disabled cluster-wide...
    assert_eq!(
        pg.psql("SELECT rolcanlogin FROM pg_roles WHERE rolname='app_stale'"),
        "f",
        "quarantine must set NOLOGIN"
    );
    // ...the resurrected password verifier was overwritten...
    let verifier_after = pg.psql("SELECT rolpassword FROM pg_authid WHERE rolname='app_stale'");
    assert_ne!(
        verifier_before, verifier_after,
        "quarantine must rotate the resurrected snapshot password"
    );
    // ...and the role's data in otherdb is intact (access removed, data preserved).
    assert_eq!(
        exec_otherdb("SELECT 1 FROM information_schema.tables WHERE table_name='owned_by_stale'"),
        "1",
        "quarantine must not destroy the role's owned objects"
    );

    // Idempotent: a second reconcile on the SAME data version is a no-op — the
    // role is already NOLOGIN, so it is no longer a *login* role to reconcile; it
    // stays neutralized (a real later checkout that restores LOGIN re-quarantines).
    let again = uc
        .reconcile(&repo, &repositories_dir, org, project, db, ReconcileMode::Faithful)
        .await
        .expect("second reconcile must not brick");
    assert!(
        again.is_noop(),
        "a quarantined (NOLOGIN) role needs no further reconcile, got dropped={:?} quarantined={:?}",
        again.dropped,
        again.quarantined
    );
    assert_eq!(
        pg.psql("SELECT rolcanlogin FROM pg_roles WHERE rolname='app_stale'"),
        "f",
        "the quarantined role stays disabled after a second reconcile"
    );
    assert!(
        !login(pg.name(), "app_stale", "pw_stale_1234"),
        "the quarantined role still cannot authenticate"
    );
}

/// Ensure-present (completeness): a LIVE managed user missing from the restored
/// snapshot is re-created with its current password + preset, so current managed
/// access is complete after a checkout to a commit that predates the user.
#[tokio::test]
async fn ensure_present_recreates_a_live_managed_user_missing_from_the_snapshot() {
    if !docker_ok() {
        eprintln!("skip: set GFS_DOCKER_IT=1 and ensure docker is running");
        return;
    }

    let pg = Postgres::start();
    // The snapshot as an OLD checkout restores it: deploy defaults present, but the
    // managed user app_x (created after this commit) is absent from pg_authid.
    pg.psql("CREATE ROLE owner LOGIN; CREATE ROLE developers NOLOGIN;");

    let tmp = tempfile::tempdir().expect("tempdir");
    let repositories_dir = tmp.path().join("repositories");
    let (org, project, db) = ("acme", "proj", "db-ensure");
    let repo = repositories_dir.join(org).join(project).join(db);
    std::fs::create_dir_all(&repo).expect("mkdir repo");
    write_repo(&repo, pg.name());
    // The record says app_x is a live managed user with a readonly preset.
    IntendedUserSet::add(&repositories_dir, org, project, db, "app_x", Some(RolePreset::Readonly))
        .expect("record live managed user");

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");
    let uc = ReconcileManagedUsersUseCase::new(compute, registry);

    // app_x is absent from the cluster → listed as absent, with its preset.
    let absent = uc
        .list_absent_intended_roles(&repo, &repositories_dir, org, project, db)
        .await
        .expect("list absent");
    assert_eq!(
        absent,
        vec![("app_x".to_string(), Some(RolePreset::Readonly))],
        "the missing live managed user is listed with its recorded preset"
    );

    // The data plane supplies the current vaulted password; re-create it.
    let specs = vec![RoleSpec {
        username: "app_x".to_string(),
        password: "pw_app_x_1234".to_string(),
        preset: Some(RolePreset::Readonly),
        default_privileges_owner: Some("owner".to_string()),
    }];
    let created = uc.ensure_present_roles(&repo, &specs).await.expect("ensure present");
    assert_eq!(created, vec!["app_x".to_string()]);

    // app_x now exists and authenticates with its current password.
    assert_eq!(
        pg.psql("SELECT 1 FROM pg_roles WHERE rolname='app_x'"),
        "1",
        "the missing live managed user was re-created"
    );
    assert!(
        login(pg.name(), "app_x", "pw_app_x_1234"),
        "the re-created user authenticates with its current password"
    );

    // Idempotent: nothing is absent now.
    let none = uc
        .list_absent_intended_roles(&repo, &repositories_dir, org, project, db)
        .await
        .expect("list absent again");
    assert!(none.is_empty(), "no live managed user is absent after ensure-present");
}

/// A checkout restores a role's *snapshot* password. `reapply_passwords` re-applies
/// the role's *current* password (resolved out-of-band by the data plane from the
/// vault port), so a rotated-away password stops authenticating and the current one
/// works. The vault I/O + key encoding are the DP adapter's concern (tested there).
#[tokio::test]
async fn reapply_passwords_applies_current_over_stale_on_live_pg() {
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

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");
    let uc = ReconcileManagedUsersUseCase::new(compute, registry);

    // Baseline: the stale password authenticates.
    assert!(login(pg.name(), "app_rw", "p_old_stale_1234"), "baseline: stale pw authenticates pre-rekey");

    // The DP enumerates rekeyable roles, resolves each current password from the
    // vault, and hands the map back to the domain to apply.
    let rekeyable = uc.list_rekeyable_roles(&repo).await.expect("list rekeyable");
    assert!(rekeyable.contains(&"app_rw".to_string()), "app_rw is a rekeyable login role");
    let mut passwords = std::collections::BTreeMap::new();
    passwords.insert("app_rw".to_string(), "p_new_current_1234".to_string());
    let rekeyed = uc.reapply_passwords(&repo, &passwords).await.expect("reapply");

    // The container is removed by `pg`'s Drop even on a failed assertion below.
    assert_eq!(rekeyed, vec!["app_rw".to_string()], "the login role is re-keyed");
    assert!(
        login(pg.name(), "app_rw", "p_new_current_1234"),
        "current password must authenticate after re-key"
    );
    assert!(
        !login(pg.name(), "app_rw", "p_old_stale_1234"),
        "the stale snapshot password must no longer authenticate after re-key"
    );
}

/// RFC 012 phase 3: a checkout restores a role's snapshot privileges. Re-applying
/// the role's *current* recorded preset (declaratively) enforces exactly that
/// preset — so a privilege downgraded/revoked since that commit stays revoked.
#[tokio::test]
async fn reapply_presets_enforces_the_recorded_preset_over_snapshot_privileges_on_live_pg() {
    if !docker_ok() {
        eprintln!("skip: set GFS_DOCKER_IT=1 and ensure docker is running");
        return;
    }

    let pg = Postgres::start();
    // Owner + a table; `app` created readwrite (has INSERT) — as a restored snapshot
    // of the pre-downgrade commit would leave it.
    pg.psql("CREATE ROLE owner LOGIN; GRANT CREATE ON SCHEMA public TO owner; CREATE TABLE public.t(id int);");

    let tmp = tempfile::tempdir().expect("tempdir");
    let repositories_dir = tmp.path().join("repositories");
    let (org, project, db) = ("acme", "proj", "db-preset");
    let repo = repositories_dir.join(org).join(project).join(db);
    std::fs::create_dir_all(&repo).expect("mkdir repo");
    write_repo(&repo, pg.name());

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");

    // The cluster has `app` as readwrite (snapshot state).
    ManageUsersUseCase::new(compute.clone(), registry.clone())
        .create_role(
            &repo,
            &RoleSpec {
                username: "app".into(),
                password: "pw_app_1234".into(),
                preset: Some(RolePreset::Readwrite),
                default_privileges_owner: Some("owner".into()),
            },
        )
        .await
        .expect("create readwrite app");
    let insert_before = pg.psql("SELECT has_table_privilege('app','public.t','INSERT')");

    // The node-local record says the CURRENT preset is readonly (a downgrade recorded
    // since the checked-out commit).
    gfs_domain::utils::intended_users::IntendedUserSet::add(
        &repositories_dir,
        org,
        project,
        db,
        "app",
        Some(RolePreset::Readonly),
    )
    .expect("record readonly preset");

    let outcome = ReconcileManagedUsersUseCase::new(compute, registry)
        .reapply_presets_from_record(&repo, &repositories_dir, org, project, db, "owner")
        .await
        .expect("reapply presets");

    let insert_after = pg.psql("SELECT has_table_privilege('app','public.t','INSERT')");
    let select_after = pg.psql("SELECT has_table_privilege('app','public.t','SELECT')");

    // The container is removed by `pg`'s Drop even on a failed assertion below.
    assert_eq!(outcome.reapplied, vec!["app".to_string()], "app's preset is re-applied");
    assert!(outcome.failed.is_empty(), "no re-apply failures");
    assert_eq!(insert_before, "t", "readwrite grants INSERT (snapshot state)");
    assert_eq!(
        insert_after, "f",
        "re-applying the recorded readonly preset revokes INSERT — the downgrade holds"
    );
    assert_eq!(select_after, "t", "readonly still allows SELECT");
}

/// Declarable intent: adopting a customer-created SQL role promotes it into the
/// managed set, so a later checkout to a snapshot that predates it re-creates the
/// role (ensure-present). A customer role that is NOT adopted stays untracked — it
/// is faithful content and is never re-created. Reserved and NOLOGIN roles cannot
/// be adopted.
#[tokio::test]
async fn adopt_promotes_a_customer_role_into_the_managed_set_making_it_durable() {
    if !docker_ok() {
        eprintln!("skip: set GFS_DOCKER_IT=1 and ensure docker is running");
        return;
    }

    let pg = Postgres::start();
    // Deploy defaults + two login roles a customer made via RAW SQL (untracked).
    pg.psql("CREATE ROLE owner LOGIN; CREATE ROLE developers NOLOGIN;");
    pg.psql("CREATE ROLE adopted_u LOGIN PASSWORD 'adopted_pw_1234';");
    pg.psql("CREATE ROLE untracked_u LOGIN PASSWORD 'untracked_pw_1234';");

    let tmp = tempfile::tempdir().expect("tempdir");
    let repositories_dir = tmp.path().join("repositories");
    let (org, project, db) = ("acme", "proj", "db-adopt");
    let repo = repositories_dir.join(org).join(project).join(db);
    std::fs::create_dir_all(&repo).expect("mkdir repo");
    write_repo(&repo, pg.name());

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");
    let uc = ReconcileManagedUsersUseCase::new(compute, registry);

    // Adopt one of the two customer roles; the other is left untracked.
    let outcome = uc
        .adopt_role(&repo, &repositories_dir, org, project, db, "adopted_u")
        .await
        .expect("adopt");
    assert!(outcome.newly_adopted, "first adopt is a fresh promotion");
    // Idempotent: re-adopting reports already-tracked.
    let again = uc
        .adopt_role(&repo, &repositories_dir, org, project, db, "adopted_u")
        .await
        .expect("re-adopt");
    assert!(!again.newly_adopted, "re-adopt is idempotent");

    // A reserved role, a NOLOGIN group role, and a non-existent role cannot be adopted.
    assert!(
        uc.adopt_role(&repo, &repositories_dir, org, project, db, "owner").await.is_err(),
        "a reserved platform role is not adoptable"
    );
    assert!(
        uc.adopt_role(&repo, &repositories_dir, org, project, db, "developers").await.is_err(),
        "a reserved NOLOGIN group role is not adoptable"
    );
    assert!(
        uc.adopt_role(&repo, &repositories_dir, org, project, db, "ghost").await.is_err(),
        "a non-existent role is not adoptable"
    );

    // Simulate a checkout to a snapshot that predates BOTH customer roles.
    pg.psql("DROP ROLE adopted_u; DROP ROLE untracked_u;");

    // Only the ADOPTED role is listed absent to re-create; the untracked one is faithful.
    let absent = uc
        .list_absent_intended_roles(&repo, &repositories_dir, org, project, db)
        .await
        .expect("list absent");
    assert_eq!(
        absent,
        vec![("adopted_u".to_string(), None)],
        "only the adopted role is re-created; the untracked customer role is left faithful"
    );

    // The data plane supplies the vaulted password (adopted with its current pw).
    let specs = vec![RoleSpec {
        username: "adopted_u".to_string(),
        password: "adopted_pw_1234".to_string(),
        preset: None,
        default_privileges_owner: Some("owner".to_string()),
    }];
    let created = uc.ensure_present_roles(&repo, &specs).await.expect("ensure present");
    assert_eq!(created, vec!["adopted_u".to_string()]);

    // adopted_u is back and authenticates; untracked_u stays gone.
    assert_eq!(
        pg.psql("SELECT 1 FROM pg_roles WHERE rolname='adopted_u'"),
        "1",
        "the adopted role was re-created"
    );
    assert!(
        login(pg.name(), "adopted_u", "adopted_pw_1234"),
        "the re-created adopted role authenticates with its vaulted password"
    );
    assert_eq!(
        pg.psql("SELECT count(*) FROM pg_roles WHERE rolname='untracked_u'"),
        "0",
        "the untracked customer role is NOT re-created (faithful content)"
    );
}

/// The reserved-group fence, verified (not enforced via an in-DB trigger — Postgres
/// event triggers do NOT fire on GRANT / role DDL). A managed customer user is
/// NOSUPERUSER NOCREATEROLE and holds its preset-group membership WITHOUT admin
/// option, so it cannot escape the platform-managed level via raw SQL: it can
/// neither drop nor alter a reserved group role, grant itself into a higher group,
/// create roles, nor self-escalate to superuser. "Membership == level" is only
/// trustworthy because these attempts are denied by the standard privilege system.
#[tokio::test]
async fn a_managed_user_cannot_escape_the_reserved_group_fence() {
    if !docker_ok() {
        eprintln!("skip: set GFS_DOCKER_IT=1 and ensure docker is running");
        return;
    }

    let pg = Postgres::start();
    let tmp = tempfile::tempdir().expect("tempdir");
    let repo = tmp.path();
    write_repo(repo, pg.name());

    let compute = Arc::new(DockerCompute::new().expect("docker compute"));
    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(&*registry).expect("register providers");
    let uc = ManageUsersUseCase::new(compute, registry);

    // A managed readonly user — the platform grants it INTO gfs_readonly.
    uc.create_role(
        repo,
        &RoleSpec {
            username: "app".into(),
            password: "app_pw_1234".into(),
            preset: Some(RolePreset::Readonly),
            default_privileges_owner: None,
        },
    )
    .await
    .expect("create app");

    // Fence preconditions: not privileged, and the group membership has no ADMIN OPTION.
    assert_eq!(
        pg.psql("SELECT (rolsuper OR rolcreaterole OR rolcreatedb)::text FROM pg_roles WHERE rolname='app'"),
        "false",
        "a managed user is never SUPERUSER / CREATEROLE / CREATEDB"
    );
    assert_eq!(
        pg.psql("SELECT COALESCE(bool_or(admin_option), false)::text FROM pg_auth_members m JOIN pg_roles g ON g.oid = m.roleid JOIN pg_roles u ON u.oid = m.member WHERE u.rolname='app' AND g.rolname IN ('gfs_readonly','gfs_readwrite','gfs_admin')"),
        "false",
        "the preset-group membership carries no ADMIN OPTION"
    );
    assert!(login(pg.name(), "app", "app_pw_1234"), "app can log in (baseline)");

    // Every privilege-escalation attempt, run AS app, must be REJECTED.
    let escapes = [
        "DROP ROLE gfs_readonly",
        "ALTER ROLE gfs_admin NOLOGIN",
        "GRANT gfs_admin TO app",
        "GRANT gfs_readwrite TO app",
        "CREATE ROLE evil LOGIN PASSWORD 'x'",
        "ALTER ROLE app SUPERUSER",
        "ALTER ROLE app CREATEROLE",
    ];
    for sql in escapes {
        assert!(
            !as_user_ok(pg.name(), "app", "app_pw_1234", sql),
            "the reserved-group fence must reject: {sql}"
        );
    }

    // The fence held: reserved groups intact, app not escalated, no new role created.
    assert_eq!(
        pg.psql("SELECT count(*)::text FROM pg_roles WHERE rolname IN ('gfs_readonly','gfs_readwrite','gfs_admin')"),
        "3",
        "the reserved group roles are intact"
    );
    assert_eq!(
        pg.psql("SELECT rolsuper::text FROM pg_roles WHERE rolname='app'"),
        "false",
        "app did not self-escalate to superuser"
    );
    assert_eq!(
        pg.psql("SELECT pg_has_role('app','gfs_admin','MEMBER')::text"),
        "false",
        "app did not grant itself into gfs_admin"
    );
    assert_eq!(
        pg.psql("SELECT count(*)::text FROM pg_roles WHERE rolname='evil'"),
        "0",
        "app created no roles (NOCREATEROLE)"
    );
}
