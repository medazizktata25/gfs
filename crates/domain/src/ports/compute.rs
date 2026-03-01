use std::path::PathBuf;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum ComputeError {
    #[error("instance not found: '{0}'")]
    NotFound(String),

    #[error("instance already exists: '{0}'")]
    AlreadyExists(String),

    #[error("instance is not running: '{0}'")]
    NotRunning(String),

    #[error("instance is already running: '{0}'")]
    AlreadyRunning(String),

    #[error("instance is already paused: '{0}'")]
    AlreadyPaused(String),

    #[error("instance is not paused: '{0}'")]
    NotPaused(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, ComputeError>;

// ---------------------------------------------------------------------------
// Supporting types
// ---------------------------------------------------------------------------

/// Unique identifier for a managed compute instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstanceId(pub String);

impl std::fmt::Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Runtime state of a compute instance.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstanceState {
    Starting,
    Running,
    Paused,
    Stopping,
    Stopped,
    Restarting,
    Failed,
    Unknown,
}

impl InstanceState {
    /// Returns the RFC 006 status string (snake_case) for this state.
    pub fn as_status_str(&self) -> &'static str {
        match self {
            InstanceState::Starting => "starting",
            InstanceState::Running => "running",
            InstanceState::Paused => "paused",
            InstanceState::Stopping => "stopping",
            InstanceState::Stopped => "stopped",
            InstanceState::Restarting => "restarting",
            InstanceState::Failed => "failed",
            InstanceState::Unknown => "unknown",
        }
    }
}

/// Live status of a compute instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstanceStatus {
    pub id: InstanceId,
    pub state: InstanceState,
    pub pid: Option<u32>,
    pub started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub exit_code: Option<i32>,
}

/// Host, port and env for building a database connection string.
/// Used by the status use case with [`crate::ports::database_provider::DatabaseProvider::connection_string`].
#[derive(Debug, Clone, Default)]
pub struct InstanceConnectionInfo {
    pub host: String,
    pub port: u16,
    pub env: Vec<(String, String)>,
}

/// Output captured from running a command in a compute instance.
#[derive(Debug, Clone)]
pub struct ExecOutput {
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

/// Options controlling how an instance is started.
#[derive(Debug, Default)]
pub struct StartOptions {
    /// Environment variables injected into the instance.
    pub env: Vec<(String, String)>,

    /// Whether to wait until the instance reaches `Running` before returning.
    pub wait: bool,
}

/// Options controlling how logs are streamed or fetched.
#[derive(Debug)]
pub struct LogsOptions {
    /// Maximum number of lines to return (most recent). `None` means all.
    pub tail: Option<usize>,

    /// Only return log entries after this timestamp.
    pub since: Option<chrono::DateTime<chrono::Utc>>,

    /// Include stdout (`true` by default).
    pub stdout: bool,

    /// Include stderr (`true` by default).
    pub stderr: bool,
}

impl Default for LogsOptions {
    fn default() -> Self {
        Self {
            tail: None,
            since: None,
            stdout: true,
            stderr: true,
        }
    }
}

/// A single log entry emitted by a compute instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub stream: LogStream,
    pub message: String,
}

/// Which output stream a log entry originated from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
}

/// Environment variable with an optional default. If `default` is `None`, a value
/// must be supplied at provision/start time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvVar {
    pub name: String,
    pub default: Option<String>,
}

/// Definition of a compute instance: image, directories, env (with optional defaults), and ports.
/// Used by [`Compute::provision`] to create and configure an instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeDefinition {
    /// Container/image identifier (e.g. Docker image name:tag).
    pub image: String,

    /// Environment variables to inject. Each has an optional default; missing defaults must be supplied at runtime.
    pub env: Vec<EnvVar>,

    /// Port mappings (mandatory). At least one port must be specified.
    pub ports: Vec<PortMapping>,

    /// Directory for instance data inside the container (mandatory).
    pub data_dir: PathBuf,

    /// Host path to bind-mount onto `data_dir`; if `Some`, enables persistent storage.
    pub host_data_dir: Option<PathBuf>,

    /// Optional "uid:gid" to run the container as (e.g. host user for bind mounts).
    /// When set, files created in host_data_dir are owned by this user on the host.
    pub user: Option<String>,

    /// Directory for instance logs (optional).
    pub logs_dir: Option<PathBuf>,

    /// Directory for instance configuration (optional).
    pub conf_dir: Option<PathBuf>,

    /// Optional command arguments passed to the container (e.g. PostgreSQL `-c key=value`).
    /// Populated from the database provider's `default_args()` when provisioning.
    pub args: Vec<String>,
}

/// A single port mapping (host port optional; container port required).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortMapping {
    /// Port inside the container.
    pub compute_port: u16,
    /// Port on the host; if `None`, the runtime may choose a free port.
    pub host_port: Option<u16>,
}

// ---------------------------------------------------------------------------
// Port
// ---------------------------------------------------------------------------

/// Port that abstracts lifecycle management of a compute instance (container,
/// VM, process, …).
#[async_trait]
pub trait Compute: Send + Sync {
    /// Create and configure an instance from a definition. Returns the new instance id.
    async fn provision(&self, definition: &ComputeDefinition) -> Result<InstanceId>;

    /// Start the instance identified by `id`.
    async fn start(&self, id: &InstanceId, options: StartOptions) -> Result<InstanceStatus>;

    /// Gracefully stop the instance identified by `id`.
    async fn stop(&self, id: &InstanceId) -> Result<InstanceStatus>;

    /// Stop then start the instance identified by `id`.
    async fn restart(&self, id: &InstanceId) -> Result<InstanceStatus>;

    /// Return the current runtime status of the instance.
    async fn status(&self, id: &InstanceId) -> Result<InstanceStatus>;

    /// Return host port and env for the given container port, for building a connection string.
    /// The adapter inspects the instance (e.g. Docker port bindings and container env).
    async fn get_connection_info(
        &self,
        id: &InstanceId,
        compute_port: u16,
    ) -> Result<InstanceConnectionInfo>;

    /// Run the given pre-snapshot commands inside the instance (e.g. database CHECKPOINT).
    /// Commands are executed in order; typically provided by the database provider.
    async fn prepare_for_snapshot(&self, id: &InstanceId, commands: &[String]) -> Result<()>;

    /// Fetch log entries produced by the instance.
    async fn logs(&self, id: &InstanceId, options: LogsOptions) -> Result<Vec<LogEntry>>;

    /// Suspend the instance using the cgroups freezer (or equivalent).
    async fn pause(&self, id: &InstanceId) -> Result<InstanceStatus>;

    /// Resume a previously paused instance.
    async fn unpause(&self, id: &InstanceId) -> Result<InstanceStatus>;

    /// Return the host path bound to the given container data path (e.g. `/var/lib/postgresql/data`).
    /// Used to detect when the container is bound to a different branch's workspace than the current one.
    async fn get_instance_data_mount_host_path(
        &self,
        id: &InstanceId,
        compute_data_path: &str,
    ) -> Result<Option<std::path::PathBuf>>;

    /// Stop the instance if running, then remove it. Used when recreating a container with a new data bind.
    async fn remove_instance(&self, id: &InstanceId) -> Result<()>;

    // -----------------------------------------------------------------------
    // Task execution (sidecar / ephemeral instances)
    // -----------------------------------------------------------------------

    /// Return connection info for reaching this instance **from within a linked
    /// task** (see [`run_task`](Self::run_task)).
    ///
    /// In containerised runtimes this typically returns the instance hostname
    /// (container name / IP) and the *container* port (not the mapped host
    /// port). In process-based runtimes it returns `localhost` and the
    /// listening port.
    async fn get_task_connection_info(
        &self,
        id: &InstanceId,
        compute_port: u16,
    ) -> Result<InstanceConnectionInfo>;

    /// Run an ephemeral compute instance to completion.
    ///
    /// The runtime creates a temporary instance from `definition`, executes
    /// `command` (as a shell command, e.g. via `sh -c`), captures stdout /
    /// stderr, and removes the instance once it exits.
    ///
    /// If `linked_to` is `Some`, the runtime ensures network connectivity
    /// between the task and the linked instance (e.g. same Docker network,
    /// same host, etc.).
    ///
    /// `definition.host_data_dir` / `definition.data_dir` are used for
    /// volume-mounting files into / out of the task (e.g. an export dump or
    /// import source).
    async fn run_task(
        &self,
        definition: &ComputeDefinition,
        command: &str,
        linked_to: Option<&InstanceId>,
    ) -> Result<ExecOutput>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_state_as_status_str() {
        assert_eq!(InstanceState::Starting.as_status_str(), "starting");
        assert_eq!(InstanceState::Running.as_status_str(), "running");
        assert_eq!(InstanceState::Paused.as_status_str(), "paused");
        assert_eq!(InstanceState::Stopping.as_status_str(), "stopping");
        assert_eq!(InstanceState::Stopped.as_status_str(), "stopped");
        assert_eq!(InstanceState::Restarting.as_status_str(), "restarting");
        assert_eq!(InstanceState::Failed.as_status_str(), "failed");
        assert_eq!(InstanceState::Unknown.as_status_str(), "unknown");
    }

    #[test]
    fn instance_id_display() {
        let id = InstanceId("abc-123".into());
        assert_eq!(id.to_string(), "abc-123");
    }

    #[test]
    fn logs_options_default() {
        let opts = LogsOptions::default();
        assert!(opts.tail.is_none());
        assert!(opts.since.is_none());
        assert!(opts.stdout);
        assert!(opts.stderr);
    }

    #[test]
    fn compute_error_display() {
        assert_eq!(
            ComputeError::NotFound("x".into()).to_string(),
            "instance not found: 'x'"
        );
        assert_eq!(
            ComputeError::AlreadyExists("y".into()).to_string(),
            "instance already exists: 'y'"
        );
        assert_eq!(
            ComputeError::NotRunning("z".into()).to_string(),
            "instance is not running: 'z'"
        );
        assert_eq!(
            ComputeError::AlreadyRunning("a".into()).to_string(),
            "instance is already running: 'a'"
        );
        assert_eq!(
            ComputeError::AlreadyPaused("b".into()).to_string(),
            "instance is already paused: 'b'"
        );
        assert_eq!(
            ComputeError::NotPaused("c".into()).to_string(),
            "instance is not paused: 'c'"
        );
        assert_eq!(
            ComputeError::Internal("msg".into()).to_string(),
            "internal error: msg"
        );
    }
}
