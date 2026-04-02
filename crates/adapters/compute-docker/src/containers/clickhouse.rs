//! ClickHouse provider: compute definition, connection string, and related behaviour.

use std::path::PathBuf;
use std::sync::Arc;

use gfs_domain::ports::compute::{ComputeDefinition, EnvVar, PortMapping};
use gfs_domain::ports::database_provider::{
    ConnectionParams, DataFormat, DatabaseProvider, DatabaseProviderArg, DatabaseProviderRegistry,
    ExportSpec, ProviderError, Result, SIGTERM, SchemaExtractionSpec, SupportedFeature,
};

const NAME: &str = "clickhouse";

/// Default ClickHouse image blessed by DevOps.
const DEFAULT_IMAGE: &str = "clickhouse:24.8.14.39";

/// Path inside the container where ClickHouse stores data.
const CONTAINER_DATA_DIR: &str = "/var/lib/clickhouse";

const ENV_DB: &str = "CLICKHOUSE_DB";
const ENV_USER: &str = "CLICKHOUSE_USER";
const ENV_PASSWORD: &str = "CLICKHOUSE_PASSWORD";
const ENV_ACCESS_MANAGEMENT: &str = "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT";

const DEFAULT_DB: &str = "default";
const DEFAULT_USER: &str = "default";
const DEFAULT_PASSWORD: &str = "clickhouse";

#[derive(Debug)]
pub struct ClickhouseProvider;

impl ClickhouseProvider {
    pub fn new() -> Self {
        Self
    }

    fn definition_impl() -> ComputeDefinition {
        ComputeDefinition {
            image: DEFAULT_IMAGE.to_string(),
            env: vec![
                EnvVar {
                    name: ENV_DB.to_string(),
                    default: Some(DEFAULT_DB.to_string()),
                },
                EnvVar {
                    name: ENV_USER.to_string(),
                    default: Some(DEFAULT_USER.to_string()),
                },
                EnvVar {
                    name: ENV_PASSWORD.to_string(),
                    default: Some(DEFAULT_PASSWORD.to_string()),
                },
                EnvVar {
                    name: ENV_ACCESS_MANAGEMENT.to_string(),
                    default: Some("1".to_string()),
                },
            ],
            ports: vec![
                PortMapping {
                    compute_port: 9000,
                    host_port: None,
                },
                PortMapping {
                    compute_port: 8123,
                    host_port: None,
                },
            ],
            data_dir: PathBuf::from(CONTAINER_DATA_DIR),
            host_data_dir: None,
            user: None,
            logs_dir: None,
            conf_dir: None,
            args: vec![],
        }
    }

    fn user(params: &ConnectionParams) -> &str {
        params.get_env(ENV_USER).unwrap_or(DEFAULT_USER)
    }

    fn password(params: &ConnectionParams) -> &str {
        params.get_env(ENV_PASSWORD).unwrap_or(DEFAULT_PASSWORD)
    }

    fn database(params: &ConnectionParams) -> &str {
        params.get_env(ENV_DB).unwrap_or(DEFAULT_DB)
    }
}

impl Default for ClickhouseProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseProvider for ClickhouseProvider {
    fn name(&self) -> &str {
        NAME
    }

    fn definition(&self) -> ComputeDefinition {
        Self::definition_impl()
    }

    fn default_port(&self) -> u16 {
        9000
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
        Ok(format!(
            "clickhouse://{}:{}@{}:{}/{}",
            Self::user(params),
            Self::password(params),
            params.host,
            params.port,
            Self::database(params)
        ))
    }

    fn supported_versions(&self) -> Vec<String> {
        vec!["24.8.14.39".into()]
    }

    fn supported_features(&self) -> Vec<SupportedFeature> {
        vec![SupportedFeature {
            id: "schema".into(),
            description: "Schema extraction and schema-aware history.".into(),
        }]
    }

    fn prepare_for_snapshot(&self, _params: &ConnectionParams) -> Result<Vec<String>> {
        // `gfs commit` pauses the container before snapshotting. For ClickHouse we currently
        // rely on that crash-consistent snapshot and do not run extra pre-snapshot commands.
        Ok(vec![])
    }

    fn supported_export_formats(&self) -> Vec<DataFormat> {
        vec![DataFormat {
            id: "schema".into(),
            description: "Schema-only DDL export from system catalogs.".into(),
            file_extension: ".sql".into(),
        }]
    }

    fn export_spec(
        &self,
        params: &ConnectionParams,
        format: &str,
    ) -> std::result::Result<ExportSpec, ProviderError> {
        if format != "schema" {
            return Err(ProviderError::UnsupportedFormat(format.to_string()));
        }

        let user = Self::user(params);
        let password = Self::password(params);
        let db = Self::database(params);

        let command = format!(
            r#"set -eu

clickhouse_query() {{
    format="$1"
    query="$2"
    tries=20
    while [ "$tries" -gt 0 ]; do
        if output="$(CLICKHOUSE_USER="{user}" CLICKHOUSE_PASSWORD="{password}" clickhouse-client --host {host} --port {port} --database {db} --format "$format" --query "$query" 2>&1)"; then
            printf '%s\n' "$output"
            return 0
        fi
        tries=$((tries - 1))
        if [ "$tries" -eq 0 ]; then
            printf '%s\n' "$output" >&2
            return 1
        fi
        sleep 1
    done
}}

out=/data/schema.sql
: > "$out"

clickhouse_query TSVRaw "SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') ORDER BY name" |
while IFS= read -r database_name; do
    printf 'CREATE DATABASE IF NOT EXISTS `%s`;\n\n' "$database_name" >> "$out"
done

clickhouse_query TSVRaw "SELECT create_table_query FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND is_temporary = 0 ORDER BY database, name" |
while IFS= read -r ddl; do
    printf '%s;\n\n' "$ddl" >> "$out"
done"#,
            user = user,
            password = password,
            host = params.host,
            port = params.port,
            db = db,
        );

        Ok(ExportSpec {
            definition: ComputeDefinition {
                image: self.definition().image,
                env: vec![],
                ports: vec![],
                data_dir: PathBuf::from("/data"),
                host_data_dir: None,
                user: None,
                logs_dir: None,
                conf_dir: None,
                args: vec![],
            },
            command,
            output_filename: "schema.sql".into(),
        })
    }

    fn query_client_command(
        &self,
        params: &ConnectionParams,
        query: Option<&str>,
    ) -> std::result::Result<std::process::Command, ProviderError> {
        let mut cmd = std::process::Command::new("clickhouse-client");
        cmd.arg("--host").arg(&params.host);
        cmd.arg("--port").arg(params.port.to_string());
        cmd.arg("--user").arg(Self::user(params));
        cmd.arg("--password").arg(Self::password(params));
        cmd.arg("--database").arg(Self::database(params));

        if let Some(q) = query {
            cmd.arg("--query").arg(q);
        }

        Ok(cmd)
    }

    fn schema_extraction_queries(&self) -> std::collections::HashMap<String, String> {
        let mut queries = std::collections::HashMap::new();

        queries.insert("version".to_string(), "SELECT version();".to_string());

        queries.insert(
            "schemas".to_string(),
            "SELECT
                toInt64(cityHash64(name) % 9223372036854775807) AS id,
                name,
                engine AS owner
            FROM system.databases
            WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
            ORDER BY name"
                .to_string(),
        );

        queries.insert(
            "tables".to_string(),
            "SELECT
                toInt64(cityHash64(concat(database, '.', name)) % 9223372036854775807) AS id,
                database AS schema,
                name,
                CAST(false AS Bool) AS rls_enabled,
                CAST(false AS Bool) AS rls_forced,
                toInt64(ifNull(total_bytes, 0)) AS bytes,
                concat(toString(ifNull(total_bytes, 0)), ' bytes') AS size,
                toInt64(ifNull(total_rows, 0)) AS live_rows_estimate,
                0 AS dead_rows_estimate,
                nullIf(comment, '') AS comment,
                [] AS primary_keys,
                [] AS relationships
            FROM system.tables
            WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
                AND is_temporary = 0
            ORDER BY database, name"
                .to_string(),
        );

        queries.insert(
            "columns".to_string(),
            "SELECT
                concat(database, '.', table, '.', name) AS id,
                toInt64(cityHash64(concat(database, '.', table)) % 9223372036854775807) AS table_id,
                database AS schema,
                table AS `table`,
                name,
                position AS ordinal_position,
                type AS data_type,
                type AS format,
                CAST(false AS Bool) AS is_identity,
                CAST(NULL AS Nullable(String)) AS identity_generation,
                CAST(default_kind != '' AS Bool) AS is_generated,
                CAST(startsWith(type, 'Nullable(') AS Bool) AS is_nullable,
                CAST(default_kind != 'ALIAS' AS Bool) AS is_updatable,
                CAST(false AS Bool) AS is_unique,
                CAST(NULL AS Nullable(String)) AS `check`,
                nullIf(default_expression, '') AS default_value,
                [] AS enums,
                nullIf(comment, '') AS comment
            FROM system.columns
            WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
            ORDER BY database, table, position"
                .to_string(),
        );

        queries
    }

    fn schema_extraction_spec(
        &self,
        params: &ConnectionParams,
    ) -> std::result::Result<Option<SchemaExtractionSpec>, ProviderError> {
        let user = Self::user(params);
        let password = Self::password(params);
        let db = Self::database(params);
        let queries = self.schema_extraction_queries();
        let schemas_q = queries
            .get("schemas")
            .ok_or_else(|| ProviderError::InvalidParams("missing schemas query".into()))?;
        let tables_q = queries
            .get("tables")
            .ok_or_else(|| ProviderError::InvalidParams("missing tables query".into()))?;
        let columns_q = queries
            .get("columns")
            .ok_or_else(|| ProviderError::InvalidParams("missing columns query".into()))?;

        let command = format!(
            r#"set -eu

clickhouse_query() {{
    format="$1"
    query="$2"
    tries=20
    while [ "$tries" -gt 0 ]; do
        if output="$(CLICKHOUSE_USER="{user}" CLICKHOUSE_PASSWORD="{password}" clickhouse-client --host {host} --port {port} --database {db} --format "$format" --output_format_json_quote_64bit_integers=0 --query "$query" 2>&1)"; then
            printf '%s\n' "$output"
            return 0
        fi
        tries=$((tries - 1))
        if [ "$tries" -eq 0 ]; then
            printf '%s\n' "$output" >&2
            return 1
        fi
        sleep 1
    done
}}

json_rows_array() {{
    query="$1"
    rows="$(clickhouse_query JSONEachRow "$query")"
    if [ -z "$rows" ]; then
        printf '[]\n'
    else
        printf '['
        printf '%s\n' "$rows" | {{
            first=1
            while IFS= read -r line; do
                if [ "$first" -eq 0 ]; then
                    printf ','
                fi
                first=0
                printf '%s' "$line"
            done
        }}
        printf ']\n'
    fi
}}

echo "GFS_SCHEMA_VERSION"
clickhouse_query TSVRaw "SELECT version();"
echo "GFS_SCHEMA_SCHEMAS"
json_rows_array "$(cat <<'SCHEMAS_EOF'
{schemas_query}
SCHEMAS_EOF
)"
echo "GFS_SCHEMA_TABLES"
json_rows_array "$(cat <<'TABLES_EOF'
{tables_query}
TABLES_EOF
)"
echo "GFS_SCHEMA_COLUMNS"
json_rows_array "$(cat <<'COLUMNS_EOF'
{columns_query}
COLUMNS_EOF
)""#,
            user = user,
            password = password,
            host = params.host,
            port = params.port,
            db = db,
            schemas_query = schemas_q,
            tables_query = tables_q,
            columns_query = columns_q,
        );

        Ok(Some(SchemaExtractionSpec {
            definition: ComputeDefinition {
                image: self.definition().image,
                env: vec![],
                ports: vec![],
                data_dir: PathBuf::from("/tmp"),
                host_data_dir: None,
                user: None,
                logs_dir: None,
                conf_dir: None,
                args: vec![],
            },
            command,
        }))
    }
}

/// Registers the ClickHouse provider in `registry` under the name `"clickhouse"`.
pub fn register(registry: &impl DatabaseProviderRegistry) -> Result<()> {
    registry.register(Arc::new(ClickhouseProvider::new()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_string_uses_defaults() {
        let provider = ClickhouseProvider::new();
        let params = ConnectionParams {
            host: "localhost".to_string(),
            port: 9000,
            env: vec![],
        };
        let s = provider.connection_string(&params).unwrap();
        assert_eq!(s, "clickhouse://default:clickhouse@localhost:9000/default");
    }

    #[test]
    fn connection_string_uses_env_overrides() {
        let provider = ClickhouseProvider::new();
        let params = ConnectionParams {
            host: "db.example.com".to_string(),
            port: 19000,
            env: vec![
                (ENV_USER.to_string(), "analytics".to_string()),
                (ENV_PASSWORD.to_string(), "secret".to_string()),
                (ENV_DB.to_string(), "warehouse".to_string()),
            ],
        };
        let s = provider.connection_string(&params).unwrap();
        assert_eq!(
            s,
            "clickhouse://analytics:secret@db.example.com:19000/warehouse"
        );
    }

    #[test]
    fn name_and_default_port() {
        let provider = ClickhouseProvider::new();
        assert_eq!(provider.name(), "clickhouse");
        assert_eq!(provider.default_port(), 9000);
    }

    #[test]
    fn supported_export_formats_includes_schema() {
        let provider = ClickhouseProvider::new();
        let formats = provider.supported_export_formats();
        assert_eq!(formats.len(), 1);
        assert_eq!(formats[0].id, "schema");
    }

    #[test]
    fn export_spec_schema_produces_schema_sql() {
        let provider = ClickhouseProvider::new();
        let params = ConnectionParams {
            host: "db".to_string(),
            port: 9000,
            env: vec![],
        };

        let spec = provider.export_spec(&params, "schema").unwrap();
        assert_eq!(spec.output_filename, "schema.sql");
        assert!(spec.command.contains("create_table_query"));
        assert!(spec.command.contains("system.tables"));
    }

    #[test]
    fn export_spec_unsupported_format_returns_error() {
        let provider = ClickhouseProvider::new();
        let params = ConnectionParams {
            host: "db".to_string(),
            port: 9000,
            env: vec![],
        };

        let err = provider.export_spec(&params, "sql").unwrap_err();
        assert!(matches!(err, ProviderError::UnsupportedFormat(_)));
    }

    #[test]
    fn query_client_command_uses_clickhouse_client() {
        let provider = ClickhouseProvider::new();
        let params = ConnectionParams {
            host: "localhost".to_string(),
            port: 9000,
            env: vec![],
        };

        let cmd = provider
            .query_client_command(&params, Some("SELECT 1"))
            .unwrap();
        let args: Vec<_> = cmd
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();

        assert_eq!(cmd.get_program().to_string_lossy(), "clickhouse-client");
        assert!(args.contains(&"--query".to_string()));
        assert!(args.contains(&"SELECT 1".to_string()));
    }

    #[test]
    fn schema_extraction_spec_returns_some_with_delimiters() {
        let provider = ClickhouseProvider::new();
        let params = ConnectionParams {
            host: "db".to_string(),
            port: 9000,
            env: vec![],
        };

        let spec = provider.schema_extraction_spec(&params).unwrap().unwrap();
        assert!(spec.command.contains("GFS_SCHEMA_VERSION"));
        assert!(spec.command.contains("GFS_SCHEMA_SCHEMAS"));
        assert!(spec.command.contains("GFS_SCHEMA_TABLES"));
        assert!(spec.command.contains("GFS_SCHEMA_COLUMNS"));
        assert!(spec.command.contains("JSONEachRow"));
    }
}
