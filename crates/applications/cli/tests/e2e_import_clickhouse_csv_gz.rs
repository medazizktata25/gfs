//! End-to-end ClickHouse gzipped CSV import coverage.

mod common;

use std::fs;
use std::process::Command;

use common::{cli_runner, clickhouse};
use serial_test::serial;

#[test]
#[serial]
fn import_clickhouse_csv_gz_infers_table_and_loads_rows() {
    clickhouse::with_fresh_repo(|repo_path| {
        let import_dir = repo_path.join("import_data");
        fs::create_dir_all(&import_dir).expect("create import dir");

        let csv_path = import_dir.join("sample_stories.csv");
        fs::write(&csv_path, "id,title,score\n1,hello,10\n2,world,20\n")
            .expect("write CSV import file");

        let gz_path = import_dir.join("sample_stories.csv.gz");
        let output = Command::new("gzip")
            .args(["-c", csv_path.to_str().expect("csv path as str")])
            .output()
            .expect("gzip sample CSV");
        assert!(output.status.success(), "gzip should succeed");
        fs::write(&gz_path, output.stdout).expect("write gzipped CSV");

        let (ok, stdout, stderr) = cli_runner::gfs_import(repo_path, &gz_path, None);
        assert!(ok, "gfs import csv.gz should succeed; stderr: {stderr}");
        if !stdout.is_empty() {
            assert!(
                stdout.contains("Imported from"),
                "stdout should mention import; got: {stdout}"
            );
        }

        let container_id = clickhouse::get_container_id(repo_path);
        let result = clickhouse::run_clickhouse_query(
            &container_id,
            "SELECT id, title, score FROM sample_stories ORDER BY id FORMAT TSVRaw",
        );
        assert!(
            result.contains("1\thello\t10"),
            "sample_stories should contain first row; got: {result}"
        );
        assert!(
            result.contains("2\tworld\t20"),
            "sample_stories should contain second row; got: {result}"
        );
    });
}
