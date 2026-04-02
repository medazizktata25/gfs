//! End-to-end ClickHouse SQL import coverage.

mod common;

use std::fs;

use common::{cli_runner, clickhouse};
use serial_test::serial;

#[test]
#[serial]
fn import_clickhouse_sql_loads_table_and_rows() {
    clickhouse::with_fresh_repo(|repo_path| {
        let import_dir = repo_path.join("import_data");
        fs::create_dir_all(&import_dir).expect("create import dir");
        let sql_path = import_dir.join("seed.sql");
        fs::write(
            &sql_path,
            r#"
CREATE TABLE IF NOT EXISTS sql_import (id UInt32, label String) ENGINE = MergeTree ORDER BY id;
INSERT INTO sql_import VALUES (10, 'ten'), (20, 'twenty');
"#,
        )
        .expect("write SQL import file");

        let (ok, stdout, stderr) = cli_runner::gfs_import(repo_path, &sql_path, Some("sql"));
        assert!(ok, "gfs import sql should succeed; stderr: {stderr}");
        if !stdout.is_empty() {
            assert!(
                stdout.contains("Imported from"),
                "stdout should mention import; got: {stdout}"
            );
        }

        let container_id = clickhouse::get_container_id(repo_path);
        let result = clickhouse::run_clickhouse_query(
            &container_id,
            "SELECT id, label FROM sql_import ORDER BY id FORMAT TSVRaw",
        );
        assert!(
            result.contains("10\tten"),
            "sql_import should contain (10, ten); got: {result}"
        );
        assert!(
            result.contains("20\ttwenty"),
            "sql_import should contain (20, twenty); got: {result}"
        );
    });
}
