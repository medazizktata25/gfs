//! `gfs query` — query the database using native client (psql, mysql, sqlite3, etc.).

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use gfs_compute_docker::DockerCompute;
use gfs_domain::model::config::GfsConfig;
use gfs_domain::ports::compute::{Compute, InstanceId};
use gfs_domain::ports::database_provider::{
    ConnectionParams, DatabaseProviderRegistry, InMemoryDatabaseProviderRegistry,
};
use gfs_domain::repo_utils::repo_layout;

use crate::cli_utils::get_repo_dir;

/// Execute a SQL query against the running database instance.
///
/// If `query` is `None`, opens an interactive terminal session.
/// Otherwise, executes the query and prints the results.
///
/// The `database` parameter allows overriding the default database name
/// from the container configuration (ignored for file-based providers like SQLite).
pub async fn run(
    path: Option<PathBuf>,
    database: Option<String>,
    query: Option<String>,
) -> Result<()> {
    let repo_path = path.unwrap_or_else(get_repo_dir);

    // Load config to get provider name
    let config =
        GfsConfig::load(&repo_path).context("not a GFS repository (run gfs init first)")?;

    let environment = config
        .environment
        .as_ref()
        .context("no database configured (run gfs init with --database-provider)")?;

    let provider_name = &environment.database_provider;

    // Set up registry and resolve provider
    let registry_impl = InMemoryDatabaseProviderRegistry::new();
    gfs_compute_docker::containers::register_all(&registry_impl)
        .context("failed to register database providers")?;
    let registry: Arc<dyn DatabaseProviderRegistry> = Arc::new(registry_impl);

    let provider = registry
        .get(provider_name)
        .with_context(|| format!("unknown database provider: '{}'", provider_name))?;

    let params = if provider.requires_compute() {
        // Container-based path (PostgreSQL, MySQL, ClickHouse, …)
        let runtime = config
            .runtime
            .as_ref()
            .context("no runtime configured (run gfs init with --database-provider)")?;
        let container_name = &runtime.container_name;

        let compute = Arc::new(DockerCompute::new().map_err(|e| anyhow::anyhow!("{e}"))?);
        let instance_id = InstanceId(container_name.clone());
        let default_port = provider.default_port();

        let conn_info = compute
            .get_connection_info(&instance_id, default_port)
            .await
            .context(
                "failed to get connection info (is the database running? try 'gfs compute start')",
            )?;

        let mut env = conn_info.env;
        if let Some(db_name) = database {
            let db_env_var = match provider_name.as_str() {
                "postgres" => "POSTGRES_DB",
                "mysql" => "MYSQL_DATABASE",
                "clickhouse" => "CLICKHOUSE_DB",
                _ => "DATABASE",
            };
            env.retain(|(k, _)| k != db_env_var);
            env.push((db_env_var.to_string(), db_name));
        }

        ConnectionParams {
            host: conn_info.host,
            port: conn_info.port,
            env,
        }
    } else {
        // File-based path (SQLite): read database file from the active workspace data dir.
        let workspace_data_dir = repo_layout::get_active_workspace_data_dir(&repo_path)
            .context("failed to determine active workspace data directory")?;
        let db_path = workspace_data_dir.join("db.sqlite");

        ConnectionParams {
            host: String::new(),
            port: 0,
            env: vec![(
                "SQLITE_DB_PATH".to_string(),
                db_path.to_string_lossy().into_owned(),
            )],
        }
    };

    // Build the query command
    let mut cmd = provider
        .query_client_command(&params, query.as_deref())
        .context("failed to build query command")?;

    // Execute the command (let the OS handle "command not found" errors)
    let status = cmd.status().or_else(|e| {
        let client_name = cmd.get_program().to_string_lossy();
        if e.kind() == std::io::ErrorKind::NotFound {
            anyhow::bail!(
                "database client '{}' not found. Install it to use 'gfs query'.\n  \
                 - PostgreSQL: install postgresql client tools (psql)\n  \
                 - MySQL: install mysql client tools\n  \
                 - ClickHouse: install clickhouse client tools (clickhouse-client)\n  \
                 - SQLite: install sqlite3",
                client_name
            )
        } else {
            Err(e).with_context(|| format!("failed to execute '{}'", client_name))
        }
    })?;

    // Exit with the same code as the native client
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }

    Ok(())
}
