//! End-to-end ClickHouse commit and checkout coverage.

mod common;

use std::thread;
use std::time::Duration;

use common::{cli_runner, clickhouse};
use gfs_domain::repo_utils::repo_layout;
use serial_test::serial;

#[test]
#[serial]
fn checkout_restores_clickhouse_data_and_reprovisions_container() {
    clickhouse::with_fresh_repo(|repo_path| {
        let container_id = clickhouse::get_container_id(repo_path);
        let setup = clickhouse::run_clickhouse_query(
            &container_id,
            "CREATE TABLE IF NOT EXISTS schema_test (id UInt32, name String) ENGINE = MergeTree ORDER BY id; INSERT INTO schema_test VALUES (1, 'alice')",
        );
        assert!(
            !setup.contains("Exception"),
            "creating initial ClickHouse data should succeed; got: {setup}"
        );

        let (ok, _, stderr) = cli_runner::gfs_commit(repo_path, "first", None, None);
        assert!(
            ok,
            "first ClickHouse commit should succeed; stderr: {stderr}"
        );
        let first_commit =
            repo_layout::get_current_commit_id(repo_path).expect("first commit hash");

        let insert = clickhouse::run_clickhouse_query(
            &container_id,
            "INSERT INTO schema_test VALUES (2, 'bob')",
        );
        assert!(
            !insert.contains("Exception"),
            "inserting second ClickHouse row should succeed; got: {insert}"
        );

        let (ok, _, stderr) = cli_runner::gfs_commit(repo_path, "second", None, None);
        assert!(
            ok,
            "second ClickHouse commit should succeed; stderr: {stderr}"
        );

        let (ok, _, stderr) = cli_runner::gfs_checkout(repo_path, &first_commit);
        assert!(
            ok,
            "checkout to the first ClickHouse commit should succeed; stderr: {stderr}"
        );

        let current_container_id = clickhouse::get_container_id(repo_path);
        assert!(
            clickhouse::wait_for_clickhouse(&current_container_id),
            "ClickHouse should become ready after checkout reprovisions the container"
        );

        let mut row_count = String::new();
        for _ in 0..10 {
            row_count = clickhouse::run_clickhouse_query(
                &current_container_id,
                "SELECT count() FROM schema_test",
            );
            if !row_count.contains("Exception") {
                break;
            }
            thread::sleep(Duration::from_secs(1));
        }

        assert!(
            !row_count.contains("Exception"),
            "count query should succeed after checkout; got: {row_count}"
        );
        assert_eq!(
            row_count.lines().next().unwrap_or("").trim(),
            "1",
            "checkout should restore the first committed ClickHouse state"
        );
    });
}
