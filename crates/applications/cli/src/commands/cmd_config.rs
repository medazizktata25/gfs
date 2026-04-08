//! `gfs config` — get or set repo-local config (user.name, user.email).
//!
//! Values are stored in `.gfs/config.toml` and used as default author for commits.

use std::path::PathBuf;
use std::process::Command;

use anyhow::Result;
use gfs_domain::model::config::GfsConfig;
use gfs_domain::model::errors::RepoError;
use gfs_domain::model::layout::GFS_DIR;
use gfs_domain::repo_utils::repo_layout;

use crate::cli_utils::get_repo_dir;

/// Supported config keys (git-style: user.name, user.email, storage.compression, storage.reflink).
const KEY_USER_NAME: &str = "user.name";
const KEY_USER_EMAIL: &str = "user.email";
const KEY_STORAGE_COMPRESSION: &str = "storage.compression";
const KEY_STORAGE_REFLINK: &str = "storage.reflink";

const SUPPORTED_KEYS: &[&str] = &[
    KEY_USER_NAME,
    KEY_USER_EMAIL,
    KEY_STORAGE_COMPRESSION,
    KEY_STORAGE_REFLINK,
];

/// Run `gfs config [--path <dir>] <key> [<value>]`.
/// - One argument (key): get; print value or nothing, exit 0.
/// - Two arguments (key, value): set; update .gfs/config.toml, no output on success.
pub fn run(path: Option<PathBuf>, key: String, value: Option<String>) -> Result<()> {
    let repo_path = path.unwrap_or_else(get_repo_dir);

    match value {
        None => get(&repo_path, &key),
        Some(v) => set(&repo_path, &key, &v),
    }
}

fn get(repo_path: &std::path::Path, key: &str) -> Result<()> {
    let config = match GfsConfig::load(repo_path) {
        Ok(c) => c,
        Err(e) => {
            return Err(repo_error_to_anyhow(e, repo_path));
        }
    };

    let out: String = match key {
        KEY_USER_NAME => config
            .user
            .as_ref()
            .and_then(|u| u.name.as_deref())
            .map(|s| s.to_string())
            .unwrap_or_default(),
        KEY_USER_EMAIL => config
            .user
            .as_ref()
            .and_then(|u| u.email.as_deref())
            .map(|s| s.to_string())
            .unwrap_or_default(),
        KEY_STORAGE_COMPRESSION => config
            .storage
            .as_ref()
            .and_then(|s| s.compression.as_deref())
            .map(|s| s.to_string())
            .unwrap_or_default(),
        KEY_STORAGE_REFLINK => config
            .storage
            .as_ref()
            .map(|s| s.enable_reflink.to_string())
            .unwrap_or_default(),
        _ => {
            anyhow::bail!(
                "unsupported config key '{}'; supported: {:?}",
                key,
                SUPPORTED_KEYS
            );
        }
    };

    if !out.is_empty() {
        print!("{out}");
    }
    Ok(())
}

fn set(repo_path: &std::path::Path, key: &str, value: &str) -> Result<()> {
    if !SUPPORTED_KEYS.contains(&key) {
        anyhow::bail!(
            "unsupported config key '{}'; supported: {:?}",
            key,
            SUPPORTED_KEYS
        );
    }

    let gfs_dir = repo_path.join(GFS_DIR);
    if !gfs_dir.exists() {
        repo_layout::init_repo_layout(repo_path, None)
            .map_err(|e| repo_error_to_anyhow(e, repo_path))?;
    }

    let mut config = GfsConfig::load(repo_path).map_err(|e| repo_error_to_anyhow(e, repo_path))?;

    match key {
        KEY_USER_NAME => {
            let mut user = config.user.clone().unwrap_or_default();
            user.name = Some(value.to_string());
            config.user = Some(user);
        }
        KEY_USER_EMAIL => {
            let mut user = config.user.clone().unwrap_or_default();
            user.email = Some(value.to_string());
            config.user = Some(user);
        }
        KEY_STORAGE_COMPRESSION => {
            let mut storage = config.storage.clone().unwrap_or_default();
            storage.compression = Some(value.to_string());
            config.storage = Some(storage);
        }
        KEY_STORAGE_REFLINK => {
            let mut storage = config.storage.clone().unwrap_or_default();
            storage.enable_reflink = value == "true" || value == "1";
            config.storage = Some(storage);
        }
        _ => unreachable!(),
    }
    config
        .save(repo_path)
        .map_err(|e| repo_error_to_anyhow(e, repo_path))?;

    // Apply storage config if storage-related key was changed
    if key == KEY_STORAGE_COMPRESSION || key == KEY_STORAGE_REFLINK {
        let is_btrfs = {
            let output = Command::new("stat")
                .args(["-f", "-c", "%T", &gfs_dir.to_string_lossy()])
                .output();
            match output {
                Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout).trim() == "btrfs",
                _ => false,
            }
        };

        if !is_btrfs {
            let msg = if key == KEY_STORAGE_COMPRESSION {
                "Warning: compression is only supported on btrfs. Your filesystem is not btrfs."
            } else {
                "Warning: reflink is only supported on btrfs. Your filesystem is not btrfs."
            };
            eprintln!("{}", msg);
        } else if let Err(e) = repo_layout::apply_storage_config(&gfs_dir) {
            tracing::warn!("Failed to apply storage config: {}", e);
        }
    }

    Ok(())
}

fn repo_error_to_anyhow(e: RepoError, repo_path: &std::path::Path) -> anyhow::Error {
    match &e {
        RepoError::NoRepoFound(_) | RepoError::MissingFile(_) | RepoError::IoError(_) => {
            anyhow::anyhow!("not a gfs repository: {}", repo_path.display())
        }
        _ => anyhow::anyhow!("{}", e),
    }
}
