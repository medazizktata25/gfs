//! Btrfs-backed storage adapter for the [`StoragePort`] port.
//!
//! On Linux, directory snapshots and clones are implemented with `cp -a` and
//! optional reflinks (`--reflink=always`). Compression is applied through
//! `btrfs property set <path> compression <algo>` when configured.

use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;

use async_trait::async_trait;
use gfs_domain::model::config::{GfsConfig, StorageConfig};
use gfs_domain::ports::storage::{
    CloneOptions, MountStatus, Quota, Result, Snapshot, SnapshotId, SnapshotOptions, StorageError,
    StoragePort, VolumeId, VolumeStatus,
};
use tokio::process::Command;
use tracing::instrument;

const VALID_COMPRESSION_ALGOS: &[&str] = &["zstd", "zlib", "lzo"];
#[cfg(not(target_os = "linux"))]
const UNSUPPORTED_ERR: &str = "BtrfsStorage is only available on Linux";

#[derive(Debug, Clone)]
pub struct BtrfsStorage {
    compression: Option<String>,
    enable_reflink: bool,
}

impl Default for BtrfsStorage {
    fn default() -> Self {
        Self {
            compression: None,
            enable_reflink: true,
        }
    }
}

impl BtrfsStorage {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_repo(repo_path: &Path) -> Self {
        let storage = GfsConfig::load(repo_path)
            .ok()
            .and_then(|config| config.storage);
        Self::from_storage_config(storage.as_ref())
    }

    pub fn from_storage_config(storage: Option<&StorageConfig>) -> Self {
        Self {
            compression: storage.and_then(|cfg| cfg.compression.clone()),
            enable_reflink: storage.map(|cfg| cfg.enable_reflink).unwrap_or(true),
        }
    }

    #[cfg(target_os = "linux")]
    async fn copy_dir(&self, src: &str, dst: &str) -> Result<()> {
        let dst_path = Path::new(dst);
        if let Some(parent) = dst_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(StorageError::Io)?;
        }

        let mut command = Command::new("cp");
        if self.enable_reflink {
            command.arg("--reflink=always");
        }
        let output = command
            .args(["-a", src, dst])
            .output()
            .await
            .map_err(StorageError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(StorageError::Internal(format!(
                "copy '{}' -> '{}' failed: {}{}",
                src,
                dst,
                stderr.trim(),
                if stderr.as_ref().is_empty() {
                    stdout.trim().to_string()
                } else {
                    String::new()
                }
            )));
        }

        Ok(())
    }

    #[cfg(target_os = "linux")]
    async fn apply_runtime_settings(&self, path: &Path) {
        if let Some(compression) = &self.compression {
            apply_compression(path, compression);
        }
    }
}

pub fn is_btrfs(path: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        let output = StdCommand::new("stat")
            .args(["-f", "-c", "%T", &path.to_string_lossy()])
            .output();

        match output {
            Ok(output) if output.status.success() => {
                String::from_utf8_lossy(&output.stdout).trim() == "btrfs"
            }
            _ => false,
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        false
    }
}

pub fn apply_storage_config(path: &Path, storage: &StorageConfig) {
    #[cfg(target_os = "linux")]
    {
        if let Some(compression) = &storage.compression {
            apply_compression(path, compression);
        }

        if storage.enable_reflink {
            tracing::info!(
                "Reflink/CoW is enabled by default on btrfs for {}",
                path.display()
            );
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = (path, storage);
    }
}

#[cfg(target_os = "linux")]
fn apply_compression(path: &Path, algorithm: &str) {
    let normalized = algorithm.to_lowercase();
    if !VALID_COMPRESSION_ALGOS.contains(&normalized.as_str()) {
        tracing::warn!(
            "Invalid compression algorithm '{}'. Valid: {:?}",
            algorithm,
            VALID_COMPRESSION_ALGOS
        );
        return;
    }

    match StdCommand::new("btrfs")
        .args([
            "property",
            "set",
            &path.to_string_lossy(),
            "compression",
            normalized.as_str(),
        ])
        .output()
    {
        Ok(output) if output.status.success() => {
            tracing::info!("Enabled {} compression on {}", normalized, path.display());
        }
        Ok(output) => {
            tracing::warn!(
                "Failed to set compression on {}: {}",
                path.display(),
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        Err(err) => {
            tracing::warn!("btrfs command not available: {}", err);
        }
    }
}

#[cfg(target_os = "linux")]
async fn make_read_only(path: &Path) -> Result<()> {
    let output = Command::new("chmod")
        .args(["-R", "a-w"])
        .arg(path)
        .output()
        .await
        .map_err(StorageError::Io)?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(StorageError::Internal(format!(
            "chmod -R a-w '{}' failed: {}",
            path.display(),
            stderr.trim()
        )));
    }

    Ok(())
}

#[cfg(target_os = "linux")]
async fn status_linux(id: &VolumeId) -> Result<VolumeStatus> {
    if tokio::fs::metadata(&id.0).await.is_err() {
        return Err(StorageError::NotFound(id.0.clone()));
    }

    let output = Command::new("df")
        .args(["--block-size=1", &id.0])
        .output()
        .await
        .map_err(StorageError::Io)?;

    let (size_bytes, used_bytes) = if output.status.success() {
        parse_df_bytes_output(&String::from_utf8_lossy(&output.stdout))
    } else {
        (0, 0)
    };

    Ok(VolumeStatus {
        id: id.clone(),
        mount_point: Some(PathBuf::from(&id.0)),
        status: MountStatus::Mounted,
        size_bytes,
        used_bytes,
    })
}

#[cfg(target_os = "linux")]
async fn quota_linux(id: &VolumeId) -> Result<Quota> {
    let output = Command::new("df")
        .args(["--block-size=1", &id.0])
        .output()
        .await
        .map_err(StorageError::Io)?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(classify_stderr(&id.0, &stderr));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let data_line = stdout
        .lines()
        .nth(1)
        .ok_or_else(|| StorageError::Internal(format!("empty df output for '{}'", id)))?;

    let parts: Vec<&str> = data_line.split_whitespace().collect();
    if parts.len() < 4 {
        return Err(StorageError::Internal(format!(
            "unexpected df output format for '{}'",
            id
        )));
    }

    Ok(Quota {
        volume_id: id.clone(),
        limit_bytes: parts[1].parse().unwrap_or(0),
        used_bytes: parts[2].parse().unwrap_or(0),
        free_bytes: parts[3].parse().unwrap_or(0),
    })
}

#[cfg(target_os = "linux")]
fn parse_df_bytes_output(output: &str) -> (u64, u64) {
    let Some(data_line) = output.lines().nth(1) else {
        return (0, 0);
    };
    let parts: Vec<&str> = data_line.split_whitespace().collect();
    if parts.len() < 3 {
        return (0, 0);
    }
    (parts[1].parse().unwrap_or(0), parts[2].parse().unwrap_or(0))
}

fn classify_stderr(volume_id: &str, stderr: &str) -> StorageError {
    let lower = stderr.to_lowercase();
    if lower.contains("no such")
        || lower.contains("not found")
        || lower.contains("cannot find")
        || lower.contains("does not exist")
    {
        StorageError::NotFound(volume_id.to_owned())
    } else if lower.contains("busy") || lower.contains("in use") || lower.contains("being used") {
        StorageError::Busy(volume_id.to_owned())
    } else if lower.contains("already exists") || lower.contains("already exist") {
        StorageError::AlreadyExists(volume_id.to_owned())
    } else {
        StorageError::Internal(stderr.trim().to_owned())
    }
}

#[cfg(not(target_os = "linux"))]
fn unsupported() -> StorageError {
    StorageError::Internal(UNSUPPORTED_ERR.to_string())
}

#[async_trait]
impl StoragePort for BtrfsStorage {
    #[instrument(skip(self))]
    async fn mount(&self, _id: &VolumeId, mount_point: &Path) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            tokio::fs::create_dir_all(mount_point)
                .await
                .map_err(StorageError::Io)?;
            return Ok(());
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = mount_point;
            Err(unsupported())
        }
    }

    #[instrument(skip(self))]
    async fn unmount(&self, _id: &VolumeId) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            Ok(())
        }

        #[cfg(not(target_os = "linux"))]
        {
            Err(unsupported())
        }
    }

    #[instrument(skip(self))]
    async fn snapshot(&self, id: &VolumeId, options: SnapshotOptions) -> Result<Snapshot> {
        #[cfg(target_os = "linux")]
        {
            let source = Path::new(&id.0);
            let dest = match &options.label {
                Some(label) => PathBuf::from(label),
                None => {
                    let name = format!("snap-{}", chrono::Utc::now().format("%Y%m%dT%H%M%SZ"));
                    source
                        .parent()
                        .map(|parent| parent.join(&name))
                        .unwrap_or_else(|| PathBuf::from(&name))
                }
            };

            self.copy_dir(&id.0, &dest.to_string_lossy()).await?;
            self.apply_runtime_settings(&dest).await;
            make_read_only(&dest).await?;

            return Ok(Snapshot {
                id: SnapshotId(dest.to_string_lossy().into_owned()),
                volume_id: id.clone(),
                created_at: chrono::Utc::now(),
                size_bytes: 0,
                label: options.label,
            });
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = (id, options);
            Err(unsupported())
        }
    }

    #[instrument(skip(self))]
    async fn clone(
        &self,
        source: &VolumeId,
        target_id: VolumeId,
        options: CloneOptions,
    ) -> Result<VolumeStatus> {
        #[cfg(target_os = "linux")]
        {
            let src = options
                .from_snapshot
                .as_ref()
                .map(|snapshot| snapshot.0.as_str())
                .unwrap_or(&source.0);

            self.copy_dir(src, &target_id.0).await?;
            self.apply_runtime_settings(Path::new(&target_id.0)).await;

            return Ok(VolumeStatus {
                id: target_id.clone(),
                mount_point: Some(PathBuf::from(&target_id.0)),
                status: MountStatus::Mounted,
                size_bytes: 0,
                used_bytes: 0,
            });
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = (source, target_id, options);
            Err(unsupported())
        }
    }

    #[instrument(skip(self))]
    async fn status(&self, id: &VolumeId) -> Result<VolumeStatus> {
        #[cfg(target_os = "linux")]
        {
            return status_linux(id).await;
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = id;
            Err(unsupported())
        }
    }

    #[instrument(skip(self))]
    async fn quota(&self, id: &VolumeId) -> Result<Quota> {
        #[cfg(target_os = "linux")]
        {
            return quota_linux(id).await;
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = id;
            Err(unsupported())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_defaults_to_reflinks() {
        let storage = BtrfsStorage::new();
        assert!(storage.enable_reflink);
        assert!(storage.compression.is_none());
    }

    #[test]
    fn storage_reads_repo_config_shape() {
        let storage = BtrfsStorage::from_storage_config(Some(&StorageConfig {
            compression: Some("zstd".into()),
            enable_reflink: false,
        }));

        assert_eq!(storage.compression.as_deref(), Some("zstd"));
        assert!(!storage.enable_reflink);
    }
}
