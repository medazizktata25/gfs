//! Status response DTO for the status use case.
//!
//! Read-only aggregate of repository, config, and compute runtime data.

use serde::{Deserialize, Serialize};

/// Top-level status response for a GFS repository and its compute instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusResponse {
    /// Name of the branch at HEAD (e.g. `main`, `develop`), or commit hash / `(detached)` when HEAD is detached.
    pub current_branch: String,

    /// Compute/database instance status. Omitted or partial when no compute is provisioned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compute: Option<ComputeStatus>,

    /// Path to the active workspace data directory (from WORKSPACE file). Used to detect bind mismatch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_workspace_data_dir: Option<String>,

    /// Set when the container is bound to a different path than the active workspace (e.g. after checkout).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bind_mismatch_warning: Option<String>,
}

/// Compute section of the status response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputeStatus {
    /// Compute/database provider (e.g. `postgresql`, `mysql`). From repo config `environment.database_provider`.
    pub provider: String,

    /// Database or image version (e.g. `16`, `latest`). From repo config `environment.database_version`.
    pub version: String,

    /// Current runtime state of the container: `starting`, `running`, `paused`, `stopping`, `stopped`, `restarting`, `failed`, `unknown`.
    pub container_status: String,

    /// Unique identifier of the compute instance (e.g. Docker container ID or name).
    pub container_id: String,

    /// Client connection string for the database (e.g. `postgresql://user:pass@localhost:5432/db`).
    /// Empty when not available (e.g. container not running or credentials not resolvable).
    #[serde(default)]
    pub connection_string: String,

    /// Host path the container's data volume is bound to (from Docker inspect). Used to detect bind mismatch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_bind_host_path: Option<String>,
}
