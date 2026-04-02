//! Integration tests for `gfs schema extract` with ClickHouse.

use std::fs;

#[path = "../common/mod.rs"]
mod common;

use common::clickhouse::{get_container_id, run_clickhouse_query, with_fresh_repo};
use gfs_domain::model::datasource::DatasourceMetadata;
use serial_test::serial;

#[test]
#[serial]
fn schema_extract_clickhouse() {
    with_fresh_repo(|repo_path| {
        let container_id = get_container_id(repo_path);
        let ddl_output = run_clickhouse_query(
            &container_id,
            "CREATE TABLE IF NOT EXISTS schema_test (id UInt32, name String) ENGINE = MergeTree ORDER BY id",
        );
        assert!(
            !ddl_output.contains("Exception"),
            "creating schema_test should succeed; got: {ddl_output}"
        );

        let output_path = repo_path.join("schema.json");
        let (ok, _, stderr) =
            common::cli_runner::gfs_schema_extract(repo_path, Some(&output_path), false);

        assert!(ok, "gfs schema extract should succeed; stderr: {stderr}");

        let json = fs::read_to_string(&output_path).expect("read schema.json");
        let meta: DatasourceMetadata =
            serde_json::from_str(&json).expect("schema.json should be valid DatasourceMetadata");

        assert!(!meta.version.is_empty(), "version should not be empty");
        assert_eq!(meta.driver, "clickhouse");
        assert!(
            meta.schemas.iter().any(|s| s.name == "default"),
            "schemas should contain default; got: {:?}",
            meta.schemas
        );
        assert!(
            meta.tables.iter().any(|t| t.name == "schema_test"),
            "tables should contain schema_test; got: {:?}",
            meta.tables
        );
        assert!(
            meta.columns
                .iter()
                .any(|c| c.name == "id" && c.table == "schema_test"),
            "columns should contain id for schema_test; got: {:?}",
            meta.columns
        );
        assert!(
            meta.columns
                .iter()
                .any(|c| c.name == "name" && c.table == "schema_test"),
            "columns should contain name for schema_test; got: {:?}",
            meta.columns
        );
    });
}
