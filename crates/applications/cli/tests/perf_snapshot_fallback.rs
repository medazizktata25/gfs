//! Manual perf sweep for snapshot paths.
//!
//! This test is intentionally `#[ignore]` so it does not run in CI. It measures:
//! - baseline commit (host-side snapshot)
//! - forced permission-denied fallback commit (stream_snapshot)
//! - a heavier “real-ish” workload across multiple branches
//!
//! Run:
//! - `cargo test -p gfs-cli perf_snapshot_paths -- --ignored --nocapture`
//! - `cargo test -p gfs-cli perf_heavy_real_world -- --ignored --nocapture`

#![cfg(target_os = "linux")]

mod common;

use std::path::PathBuf;
use std::time::Instant;

use common::{cli_runner, container_runtime};
use gfs_domain::repo_utils::repo_layout;
use tempfile::Builder;

fn run_cmd_timed(mut cmd: std::process::Command) -> (std::time::Duration, std::process::Output) {
    let t0 = Instant::now();
    let out = cmd.output().expect("command output");
    (t0.elapsed(), out)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn temp_repo_dir() -> tempfile::TempDir {
    // Prefer a Docker-shareable dir on Linux (home), fall back to /var/tmp, then system temp.
    let base = std::env::var("HOME")
        .ok()
        .map(|h| {
            let p = PathBuf::from(h).join(".gfs-test-tmp");
            let _ = std::fs::create_dir_all(&p);
            p
        })
        .or_else(|| {
            let p = PathBuf::from("/var/tmp");
            p.exists().then_some(p)
        })
        .unwrap_or_else(std::env::temp_dir);

    Builder::new()
        .prefix("gfs-perf-")
        .tempdir_in(base)
        .expect("tempdir")
}

/// Guard that stops and removes a container on drop (best effort).
struct ContainerCleanupGuard(String);

impl Drop for ContainerCleanupGuard {
    fn drop(&mut self) {
        let _ = container_runtime::runtime_command()
            .args(["stop", &self.0])
            .output();
        let _ = container_runtime::runtime_command()
            .args(["rm", "-f", &self.0])
            .output();
    }
}

fn container_name(repo_path: &std::path::Path) -> String {
    repo_layout::get_runtime_config(repo_path)
        .ok()
        .flatten()
        .map(|r| r.container_name)
        .expect("runtime config container_name")
}

fn psql(repo_path: &std::path::Path, sql: &str) {
    let container = container_name(repo_path);
    let out = container_runtime::runtime_command()
        .args([
            "exec",
            &container,
            "psql",
            "-U",
            "postgres",
            "-d",
            "postgres",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            sql,
        ])
        .output()
        .expect("psql exec");
    assert!(
        out.status.success(),
        "psql failed\ncontainer: {container}\nsql: {sql}\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn head_ref_path(repo_path: &std::path::Path) -> PathBuf {
    let head = std::fs::read_to_string(repo_path.join(".gfs/HEAD")).expect("HEAD");
    let head = head.trim();
    if let Some(rest) = head.strip_prefix("ref: ") {
        return repo_path.join(".gfs").join(rest);
    }
    // Detached HEAD: HEAD file contains the hash itself.
    repo_path.join(".gfs/HEAD")
}

fn head_commit_hash(repo_path: &std::path::Path) -> String {
    let ref_path = head_ref_path(repo_path);
    std::fs::read_to_string(&ref_path)
        .unwrap_or_else(|_| panic!("head ref missing: {}", ref_path.display()))
        .trim()
        .to_string()
}

fn snapshot_dir_for_head(repo_path: &std::path::Path) -> PathBuf {
    let commit_hash = head_commit_hash(repo_path);
    let (d, f) = commit_hash.split_at(2);
    let obj_bytes = std::fs::read(repo_path.join(".gfs/objects").join(d).join(f)).unwrap();
    let commit: gfs_domain::model::commit::Commit = serde_json::from_slice(&obj_bytes).unwrap();
    let (pfx, rest) = commit.snapshot_hash.split_at(2);
    repo_path.join(".gfs/snapshots").join(pfx).join(rest)
}

#[test]
#[ignore]
fn perf_snapshot_paths() {
    let tmp = temp_repo_dir();
    let repo_path = tmp.path();

    let (init_ok, init_stdout, init_stderr) = cli_runner::run_gfs(vec![
        "gfs",
        "init",
        "--database-provider",
        "postgres",
        "--database-version",
        "17",
        repo_path.to_str().unwrap(),
    ]);
    assert!(
        init_ok,
        "gfs init failed\nstdout: {init_stdout}\nstderr: {init_stderr}"
    );

    let container_name = container_name(repo_path);
    let _cleanup = ContainerCleanupGuard(container_name.clone());

    // Baseline commit (host snapshot path).
    let t0 = Instant::now();
    let (ok1, _stdout1, stderr1) = cli_runner::gfs_commit(repo_path, "baseline", None, None);
    let baseline_dt = t0.elapsed();
    assert!(ok1, "baseline commit failed; stderr: {stderr1}");

    // Force the permission-denied precondition.
    let out = container_runtime::runtime_command()
        .args([
            "exec",
            "-u",
            "0:0",
            &container_name,
            "sh",
            "-lc",
            "touch /var/lib/postgresql/data/root_owned_test && chmod 600 /var/lib/postgresql/data/root_owned_test",
        ])
        .output()
        .expect("docker exec");
    assert!(
        out.status.success(),
        "failed to create root-owned file\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    // Fallback commit (should invoke stream_snapshot).
    let t1 = Instant::now();
    let (ok2, _stdout2, stderr2) = cli_runner::gfs_commit(repo_path, "after-root-file", None, None);
    let fallback_dt = t1.elapsed();
    assert!(ok2, "fallback commit failed; stderr: {stderr2}");
    // The in-process runner may or may not capture tracing logs reliably. We validate fallback
    // behavior by checking that the root-owned file made it into the snapshot and is readable.
    let snapdir = snapshot_dir_for_head(repo_path);
    let meta = std::fs::metadata(snapdir.join("root_owned_test")).expect("snapshot has file");
    assert!(
        meta.permissions().readonly(),
        "expected snapshot file to be read-only (finalized)"
    );

    eprintln!(
        "perf_snapshot_paths: baseline_commit_wall={:?}, fallback_commit_wall={:?}",
        baseline_dt, fallback_dt
    );
}

#[test]
#[ignore]
fn perf_heavy_real_world() {
    // Tune via env vars:
    // - GFS_PERF_ROWS (default 200000)
    // - GFS_PERF_PAYLOAD_BYTES (default 512)
    // - GFS_PERF_BRANCHES (default 3)
    // - GFS_PERF_COMMITS_PER_BRANCH (default 3)
    let rows = env_usize("GFS_PERF_ROWS", 200_000);
    let payload_bytes = env_usize("GFS_PERF_PAYLOAD_BYTES", 512);
    let branches = env_usize("GFS_PERF_BRANCHES", 3);
    let commits_per_branch = env_usize("GFS_PERF_COMMITS_PER_BRANCH", 3);
    let sleep_ms = env_u64("GFS_PERF_SLEEP_MS", 0);

    let tmp = temp_repo_dir();
    let repo_path = tmp.path();

    let (init_ok, init_stdout, init_stderr) = cli_runner::run_gfs(vec![
        "gfs",
        "init",
        "--database-provider",
        "postgres",
        "--database-version",
        "17",
        repo_path.to_str().unwrap(),
    ]);
    assert!(
        init_ok,
        "gfs init failed\nstdout: {init_stdout}\nstderr: {init_stderr}"
    );

    let container = container_name(repo_path);
    let _cleanup = ContainerCleanupGuard(container.clone());

    // Grow a realistic-ish DB.
    // Use unlogged table for speed, then CHECKPOINT happens during commit anyway.
    psql(
        repo_path,
        "CREATE UNLOGGED TABLE IF NOT EXISTS perf_big(id BIGINT PRIMARY KEY, payload TEXT);",
    );
    psql(repo_path, "TRUNCATE perf_big;");
    psql(
        repo_path,
        &format!(
            "INSERT INTO perf_big(id, payload)\n\
             SELECT gs, repeat(md5(gs::text), CEIL({payload_bytes}::numeric / 32)::int)\n\
             FROM generate_series(1, {rows}) AS gs;",
        ),
    );
    psql(repo_path, "ANALYZE perf_big;");

    // Baseline commit on main (host snapshot path expected).
    let t0 = Instant::now();
    let (ok, _out, err) = cli_runner::gfs_commit(repo_path, "heavy-main-0", None, None);
    let main0_dt = t0.elapsed();
    assert!(ok, "commit heavy-main-0 failed; stderr: {err}");
    let main0_snap = snapshot_dir_for_head(repo_path);
    let main0_size = std::fs::read_dir(&main0_snap).map(|_| ()).is_ok();
    let _ = main0_size; // keep cheap; size is measured via du below.

    let du0 = std::process::Command::new("du")
        .args(["-sb", main0_snap.to_str().unwrap()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .unwrap_or_default();

    // Snapshot-only timing (best-effort, does not affect correctness):
    // - host path: cp --reflink=auto -a <workspace>/. <dest>
    // - stream-ish path: docker cp <container>:<PGDATA>/. <dest>
    let workspace = repo_path.join(".gfs/workspaces/main/0/data");
    let snap_only_host = {
        let dest = repo_path.join(".gfs/tmp").join("perf-snap-only-host");
        let _ = std::fs::remove_dir_all(&dest);
        std::fs::create_dir_all(&dest).ok();
        let mut cmd = std::process::Command::new("cp");
        cmd.args(["--reflink=auto", "-a"])
            .arg(workspace.join("."))
            .arg(&dest);
        let (dt, out) = run_cmd_timed(cmd);
        let ok = out.status.success();
        (ok, dt, String::from_utf8_lossy(&out.stderr).to_string())
    };
    let snap_only_docker = {
        let dest = repo_path.join(".gfs/tmp").join("perf-snap-only-docker");
        let _ = std::fs::remove_dir_all(&dest);
        std::fs::create_dir_all(&dest).ok();
        let container = container_name(repo_path);
        let mut cmd = container_runtime::runtime_command();
        cmd.args([
            "cp",
            &format!("{container}:/var/lib/postgresql/data/."),
            dest.to_str().unwrap(),
        ]);
        let (dt, out) = run_cmd_timed(cmd);
        let ok = out.status.success();
        (ok, dt, String::from_utf8_lossy(&out.stderr).to_string())
    };

    eprintln!(
        "perf_heavy_real_world: main_commit_0_wall={main0_dt:?} du={du0:?} snap_only_host={:?} ok={} snap_only_docker={:?} ok={}",
        snap_only_host.1, snap_only_host.0, snap_only_docker.1, snap_only_docker.0
    );
    if !snap_only_host.0 {
        eprintln!(
            "perf_heavy_real_world: snap_only_host stderr={}",
            snap_only_host.2.trim()
        );
    }
    if !snap_only_docker.0 {
        eprintln!(
            "perf_heavy_real_world: snap_only_docker stderr={}",
            snap_only_docker.2.trim()
        );
    }

    // Create multiple branches and do commits.
    for b in 0..branches {
        let branch = format!("perf/b{b}");
        let (ok, _o, e) = cli_runner::run_gfs(vec![
            "gfs",
            "checkout",
            "--path",
            repo_path.to_str().unwrap(),
            "-b",
            &branch,
        ]);
        assert!(ok, "checkout -b {branch} failed: {e}");

        // Each branch: do a few commits; on the last branch + last commit, force fallback.
        for c in 0..commits_per_branch {
            // Mutate DB a bit so snapshot diffs are real.
            psql(
                repo_path,
                &format!(
                    "INSERT INTO perf_big(id, payload)\n\
                     SELECT {base} + gs, repeat(md5(({base}+gs)::text), 8)\n\
                     FROM generate_series(1, 5000) AS gs;",
                    base = (b * 10_000_000 + c * 100_000) as u64
                ),
            );

            if sleep_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(sleep_ms));
            }

            let want_fallback = b + 1 == branches && c + 1 == commits_per_branch;
            if want_fallback {
                // Deterministically trigger PermissionDenied in the bind mount.
                let container = container_name(repo_path);
                let out = container_runtime::runtime_command()
                    .args([
                        "exec",
                        "-u",
                        "0:0",
                        &container,
                        "sh",
                        "-lc",
                        "touch /var/lib/postgresql/data/root_owned_test && chmod 600 /var/lib/postgresql/data/root_owned_test",
                    ])
                    .output()
                    .expect("docker exec");
                assert!(out.status.success(), "failed to set up fallback trigger");
            }

            let msg = format!("{branch}-c{c}");
            let t = Instant::now();
            let (ok, _out, err) = cli_runner::gfs_commit(repo_path, &msg, None, None);
            let dt = t.elapsed();
            assert!(ok, "commit {msg} failed; stderr: {err}");

            let snap = snapshot_dir_for_head(repo_path);
            let du = std::process::Command::new("du")
                .args(["-sb", snap.to_str().unwrap()])
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
                .unwrap_or_default();

            if want_fallback {
                let meta =
                    std::fs::metadata(snap.join("root_owned_test")).expect("snapshot has file");
                assert!(
                    meta.permissions().readonly(),
                    "expected fallback snapshot file to be read-only (finalized)"
                );
            }

            eprintln!(
                "perf_heavy_real_world: branch={branch} commit={c} fallback={want_fallback} wall={dt:?} du={du:?}"
            );
        }

        // Return to main between branches to simulate real branching workflow.
        let (ok, _o, e) = cli_runner::run_gfs(vec![
            "gfs",
            "checkout",
            "--path",
            repo_path.to_str().unwrap(),
            "main",
        ]);
        assert!(ok, "checkout main failed: {e}");
    }
}
