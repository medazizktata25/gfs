//! SQLite provider: file-based database, no container required.

use std::path::PathBuf;
use std::sync::Arc;

use gfs_domain::ports::compute::{ComputeDefinition, PortMapping};
use gfs_domain::ports::database_provider::{
    ConnectionParams, DatabaseProvider, DatabaseProviderArg, DatabaseProviderRegistry,
    ProviderError, Result, SIGTERM, SupportedFeature,
};

const NAME: &str = "sqlite";

/// Placeholder image — never pulled; SQLite runs on the host without a container.
const DEFAULT_IMAGE: &str = "sqlite:latest";

/// Filename used for the SQLite database file inside the workspace data directory.
pub const DB_FILENAME: &str = "db.sqlite";

/// Environment variable carrying the host-side absolute path to the SQLite file.
const ENV_DB_PATH: &str = "SQLITE_DB_PATH";

/// SQLite database provider.  Implements [`DatabaseProvider`] but does not
/// require a compute instance: `requires_compute()` returns `false`.
#[derive(Debug, Default)]
pub struct SqliteProvider;

impl SqliteProvider {
    pub fn new() -> Self {
        Self
    }

    fn definition_impl() -> ComputeDefinition {
        ComputeDefinition {
            image: DEFAULT_IMAGE.to_string(),
            env: vec![],
            // Port 0 signals "no port"; the port list must be non-empty for the
            // definition to satisfy callers that iterate over ports, but the
            // mapping is never used because `requires_compute` returns false.
            ports: vec![PortMapping {
                compute_port: 0,
                host_port: None,
            }],
            data_dir: PathBuf::from("/data"),
            host_data_dir: None,
            user: None,
            logs_dir: None,
            conf_dir: None,
            args: vec![],
        }
    }
}

impl DatabaseProvider for SqliteProvider {
    fn name(&self) -> &str {
        NAME
    }

    fn requires_compute(&self) -> bool {
        false
    }

    fn definition(&self) -> ComputeDefinition {
        Self::definition_impl()
    }

    fn default_port(&self) -> u16 {
        0
    }

    fn default_args(&self) -> Vec<DatabaseProviderArg> {
        vec![]
    }

    fn default_signal(&self) -> u32 {
        SIGTERM
    }

    fn connection_string(
        &self,
        params: &ConnectionParams,
    ) -> std::result::Result<String, ProviderError> {
        let path = params
            .get_env(ENV_DB_PATH)
            .ok_or_else(|| ProviderError::MissingEnvVar(ENV_DB_PATH.to_string()))?;
        Ok(format!("sqlite:///{}", path))
    }

    fn supported_versions(&self) -> Vec<String> {
        vec!["3".into()]
    }

    fn supported_features(&self) -> Vec<SupportedFeature> {
        vec![SupportedFeature {
            id: "schema".into(),
            description: "Schema and DDL management.".into(),
        }]
    }

    fn prepare_for_snapshot(&self, _params: &ConnectionParams) -> Result<Vec<String>> {
        // No container to run commands in.  SQLite WAL files (.db, .db-wal,
        // .db-shm) are copied as a unit; WAL recovery ensures consistency on
        // restore.  This is best-effort, not crash-consistent.
        Ok(vec![])
    }

    fn query_client_command(
        &self,
        params: &ConnectionParams,
        query: Option<&str>,
    ) -> std::result::Result<std::process::Command, ProviderError> {
        let path = params
            .get_env(ENV_DB_PATH)
            .ok_or_else(|| ProviderError::MissingEnvVar(ENV_DB_PATH.to_string()))?;

        let mut cmd = std::process::Command::new("sqlite3");
        cmd.arg(path);

        if let Some(q) = query {
            cmd.arg(q);
        }

        Ok(cmd)
    }

    fn schema_extraction_queries(&self) -> std::collections::HashMap<String, String> {
        let mut queries = std::collections::HashMap::new();

        queries.insert(
            "version".to_string(),
            "SELECT json_object('version', sqlite_version());".to_string(),
        );

        queries.insert(
            "tables".to_string(),
            "SELECT json_group_array(json_object('name', name, 'type', type)) \
             FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
                .to_string(),
        );

        queries.insert(
            "columns".to_string(),
            "SELECT json_group_array(\
               json_object('table', m.name, 'cid', p.cid, 'name', p.name, \
                           'type', p.type, 'notnull', p.\"notnull\", \
                           'dflt_value', p.dflt_value, 'pk', p.pk)) \
             FROM sqlite_master m \
             JOIN pragma_table_info(m.name) p \
             WHERE m.type='table' AND m.name NOT LIKE 'sqlite_%';"
                .to_string(),
        );

        queries
    }
}

/// Registers the SQLite provider in `registry` under the name `"sqlite"`.
pub fn register(registry: &impl DatabaseProviderRegistry) -> Result<()> {
    registry.register(Arc::new(SqliteProvider::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_and_default_port() {
        let p = SqliteProvider::new();
        assert_eq!(p.name(), "sqlite");
        assert_eq!(p.default_port(), 0);
    }

    #[test]
    fn requires_compute_is_false() {
        let p = SqliteProvider::new();
        assert!(!p.requires_compute());
    }

    #[test]
    fn connection_string_uses_env_db_path() {
        let p = SqliteProvider::new();
        let params = ConnectionParams {
            host: String::new(),
            port: 0,
            env: vec![("SQLITE_DB_PATH".into(), "/data/db.sqlite".into())],
        };
        assert_eq!(
            p.connection_string(&params).unwrap(),
            "sqlite:////data/db.sqlite"
        );
    }

    #[test]
    fn connection_string_missing_path_is_error() {
        let p = SqliteProvider::new();
        let params = ConnectionParams::default();
        assert!(matches!(
            p.connection_string(&params),
            Err(ProviderError::MissingEnvVar(_))
        ));
    }

    #[test]
    fn prepare_for_snapshot_returns_empty() {
        let p = SqliteProvider::new();
        let params = ConnectionParams::default();
        assert!(p.prepare_for_snapshot(&params).unwrap().is_empty());
    }

    #[test]
    fn query_client_command_interactive() {
        let p = SqliteProvider::new();
        let params = ConnectionParams {
            host: String::new(),
            port: 0,
            env: vec![("SQLITE_DB_PATH".into(), "/data/db.sqlite".into())],
        };
        let cmd = p.query_client_command(&params, None).unwrap();
        assert_eq!(cmd.get_program(), "sqlite3");
        let args: Vec<_> = cmd.get_args().collect();
        assert_eq!(args, ["/data/db.sqlite"]);
    }

    #[test]
    fn query_client_command_with_query() {
        let p = SqliteProvider::new();
        let params = ConnectionParams {
            host: String::new(),
            port: 0,
            env: vec![("SQLITE_DB_PATH".into(), "/data/db.sqlite".into())],
        };
        let cmd = p.query_client_command(&params, Some("SELECT 1;")).unwrap();
        let args: Vec<_> = cmd.get_args().collect();
        assert_eq!(args.len(), 2);
        assert_eq!(args[1], "SELECT 1;");
    }

    #[test]
    fn query_client_command_missing_path_is_error() {
        let p = SqliteProvider::new();
        let params = ConnectionParams::default();
        assert!(matches!(
            p.query_client_command(&params, None),
            Err(ProviderError::MissingEnvVar(_))
        ));
    }

    #[test]
    fn schema_extraction_queries_non_empty() {
        let p = SqliteProvider::new();
        let queries = p.schema_extraction_queries();
        assert!(queries.contains_key("version"));
        assert!(queries.contains_key("tables"));
        assert!(queries.contains_key("columns"));
    }

    #[test]
    fn default_signal_is_sigterm() {
        let p = SqliteProvider::new();
        assert_eq!(p.default_signal(), SIGTERM);
    }

    #[test]
    fn definition_has_placeholder_image() {
        let p = SqliteProvider::new();
        let def = p.definition();
        assert_eq!(def.image, "sqlite:latest");
    }
}
