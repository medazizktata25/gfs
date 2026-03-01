//! Use case for switching the active branch or commit (checkout).
//!
//! Orchestrates [`Repository`], [`Compute`], and [`DatabaseProviderRegistry`]:
//! stops the repo's compute instance (if any), runs checkout, then starts or
//! recreates the instance with a mount on the new branch/commit data dir.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

use crate::model::config::RuntimeConfig;
use crate::ports::compute::{Compute, ComputeError, InstanceId};
use crate::ports::database_provider::DatabaseProviderRegistry;
use crate::ports::repository::{Repository, RepositoryError};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum CheckoutRepoError {
    #[error("{0}")]
    Repository(#[from] RepositoryError),

    #[error("compute: {0}")]
    Compute(#[from] ComputeError),
}

// ---------------------------------------------------------------------------
// Use case
// ---------------------------------------------------------------------------

/// Use case for checking out a branch or commit.
///
/// When the repo has a compute container configured, stops it before checkout
/// and starts (or recreates with the new workspace mount) after checkout.
/// Resolves the revision, runs checkout, and returns the full commit hash.
pub struct CheckoutRepoUseCase<R: DatabaseProviderRegistry> {
    repository: Arc<dyn Repository>,
    compute: Arc<dyn Compute>,
    registry: Arc<R>,
}

impl<R: DatabaseProviderRegistry> CheckoutRepoUseCase<R> {
    pub fn new(
        repository: Arc<dyn Repository>,
        compute: Arc<dyn Compute>,
        registry: Arc<R>,
    ) -> Self {
        Self {
            repository,
            compute,
            registry,
        }
    }

    /// Check out `revision` (branch name or full 64-char commit hash) at `path`.
    /// When `create_branch` is `Some(name)`, creates a new branch at `revision`
    /// (or current HEAD if `revision` is empty) then checks out that branch.
    /// Returns the full commit hash on success for display (e.g. short hash).
    pub async fn run(
        &self,
        path: PathBuf,
        revision: String,
        create_branch: Option<String>,
    ) -> std::result::Result<String, CheckoutRepoError> {
        let revision = revision.trim().to_string();

        let container_id = self
            .repository
            .get_runtime_config(&path)
            .await
            .ok()
            .flatten()
            .and_then(|r| {
                let name = r.container_name.trim();
                if name.is_empty() {
                    None
                } else {
                    Some(InstanceId(name.to_string()))
                }
            });

        if let Some(ref id) = container_id {
            let _ = self.compute.stop(id).await?;
        }

        let commit_hash = self.do_checkout(&path, &revision, create_branch).await?;

        if let Some(ref id) = container_id {
            self.ensure_compute_started_after_checkout(&path, id)
                .await?;
        }

        Ok(commit_hash)
    }

    async fn do_checkout(
        &self,
        path: &Path,
        revision: &str,
        create_branch: Option<String>,
    ) -> std::result::Result<String, CheckoutRepoError> {
        if let Some(ref branch_name) = create_branch {
            let branch_name = branch_name.trim().to_string();
            if branch_name.is_empty() {
                return Err(CheckoutRepoError::Repository(
                    RepositoryError::RevisionNotFound("(empty branch name)".to_string()),
                ));
            }
            let start_rev = if revision.is_empty() {
                "HEAD".to_string()
            } else {
                revision.to_string()
            };
            let commit_hash = self.repository.rev_parse(path, &start_rev).await?;
            if commit_hash == "0" {
                return Err(CheckoutRepoError::Repository(RepositoryError::Internal(
                    "cannot create branch: start revision has no commits".to_string(),
                )));
            }
            self.repository
                .create_branch(path, &branch_name, &commit_hash)
                .await?;
            self.repository.checkout(path, &branch_name).await?;
            let out_hash = self.repository.get_current_commit_id(path).await?;
            return Ok(out_hash);
        }

        if revision.is_empty() {
            return Err(CheckoutRepoError::Repository(
                RepositoryError::RevisionNotFound("(empty)".to_string()),
            ));
        }

        self.repository.checkout(path, revision).await?;
        let commit_hash = self.repository.get_current_commit_id(path).await?;
        Ok(commit_hash)
    }

    /// Start the instance or recreate it with the current workspace data dir if the bind differs.
    async fn ensure_compute_started_after_checkout(
        &self,
        path: &Path,
        instance_id: &InstanceId,
    ) -> std::result::Result<(), CheckoutRepoError> {
        let active = self.repository.get_active_workspace_data_dir(path).await?;
        let active_str = active.to_string_lossy().into_owned();
        tracing::info!(
            "ensure_compute_started_after_checkout: active_workspace={:?}",
            active
        );

        let environment = match self.repository.get_environment_config(path).await? {
            Some(e) if !e.database_provider.is_empty() => e,
            _ => return Ok(()),
        };

        let provider = match self.registry.get(environment.database_provider.as_str()) {
            Some(p) => p,
            None => return Ok(()),
        };

        let mut definition = provider.definition();
        if !environment.database_version.is_empty() {
            let base = definition
                .image
                .split(':')
                .next()
                .unwrap_or(definition.image.as_str());
            definition.image = format!("{}:{}", base, environment.database_version);
        }
        definition.host_data_dir = Some(active.clone());
        let compute_data_path = definition.data_dir.to_string_lossy().into_owned();

        let current_bind = self
            .compute
            .get_instance_data_mount_host_path(instance_id, &compute_data_path)
            .await
            .ok()
            .flatten()
            .map(|p| p.to_string_lossy().into_owned());

        tracing::info!(
            "ensure_compute_started_after_checkout: current_bind={:?}, paths_differ={}",
            current_bind,
            paths_differ(&active_str, current_bind.as_deref().unwrap_or(""))
        );

        if !paths_differ(&active_str, current_bind.as_deref().unwrap_or("")) {
            tracing::info!("ensure_compute_started_after_checkout: starting existing container");
            let _ = self.compute.start(instance_id, Default::default()).await?;
            return Ok(());
        }

        tracing::info!(
            "ensure_compute_started_after_checkout: removing old container and creating new one"
        );
        self.compute.remove_instance(instance_id).await?;
        let new_id = self.compute.provision(&definition).await?;
        let _ = self.compute.start(&new_id, Default::default()).await?;
        self.repository
            .update_runtime_config(
                path,
                RuntimeConfig {
                    runtime_provider: "docker".to_string(),
                    runtime_version: "24".to_string(),
                    container_name: new_id.0.clone(),
                },
            )
            .await?;
        Ok(())
    }
}

fn paths_differ(active: &str, current_bind: &str) -> bool {
    let a = std::path::Path::new(active);
    let b = std::path::Path::new(current_bind);
    match (a.canonicalize(), b.canonicalize()) {
        (Ok(a), Ok(b)) => a != b,
        _ => active != current_bind,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::sync::Arc;

    use async_trait::async_trait;

    use crate::model::config::{EnvironmentConfig, RuntimeConfig};
    use crate::ports::compute::{
        Compute, ComputeDefinition, InstanceId, InstanceState, InstanceStatus, StartOptions,
    };
    use crate::ports::database_provider::{
        ConnectionParams, DatabaseProvider, DatabaseProviderArg, DatabaseProviderRegistry,
        ProviderError, Result as RegistryResult, SIGTERM, SupportedFeature,
    };
    use crate::ports::repository::Repository;

    struct MockRepository {
        current_commit: String,
        runtime_config: Option<RuntimeConfig>,
    }

    #[async_trait]
    impl Repository for MockRepository {
        async fn init(
            &self,
            _: &std::path::Path,
            _: Option<String>,
        ) -> crate::ports::repository::Result<()> {
            Ok(())
        }
        async fn get_workspace_data_dir_for_head(
            &self,
            _: &std::path::Path,
        ) -> crate::ports::repository::Result<PathBuf> {
            Ok(PathBuf::from("/workspace/data"))
        }
        async fn update_environment_config(
            &self,
            _: &std::path::Path,
            _: EnvironmentConfig,
        ) -> crate::ports::repository::Result<()> {
            Ok(())
        }
        async fn update_runtime_config(
            &self,
            _: &std::path::Path,
            _: RuntimeConfig,
        ) -> crate::ports::repository::Result<()> {
            Ok(())
        }
        async fn clone_repo(
            &self,
            _: &str,
            _: &std::path::Path,
        ) -> crate::ports::repository::Result<()> {
            Ok(())
        }
        async fn commit(
            &self,
            _: &std::path::Path,
            _: crate::model::commit::NewCommit,
        ) -> crate::ports::repository::Result<String> {
            Ok(String::new())
        }
        async fn checkout(
            &self,
            _: &std::path::Path,
            _: &str,
        ) -> crate::ports::repository::Result<()> {
            Ok(())
        }
        async fn create_branch(
            &self,
            _: &std::path::Path,
            _: &str,
            _: &str,
        ) -> crate::ports::repository::Result<()> {
            Ok(())
        }
        async fn log(
            &self,
            _: &std::path::Path,
            _: crate::ports::repository::LogOptions,
        ) -> crate::ports::repository::Result<Vec<crate::model::commit::CommitWithRefs>> {
            Ok(vec![])
        }
        async fn rev_parse(
            &self,
            _: &std::path::Path,
            rev: &str,
        ) -> crate::ports::repository::Result<String> {
            if rev == "0" {
                return Err(crate::ports::repository::RepositoryError::Internal(
                    "cannot create branch: start revision has no commits".into(),
                ));
            }
            Ok(self.current_commit.clone())
        }
        async fn push(
            &self,
            _: &std::path::Path,
            _: crate::ports::repository::RemoteOptions,
        ) -> crate::ports::repository::Result<()> {
            Ok(())
        }
        async fn pull(
            &self,
            _: &std::path::Path,
            _: crate::ports::repository::RemoteOptions,
        ) -> crate::ports::repository::Result<()> {
            Ok(())
        }
        async fn fetch(
            &self,
            _: &std::path::Path,
            _: crate::ports::repository::RemoteOptions,
        ) -> crate::ports::repository::Result<()> {
            Ok(())
        }
        async fn get_current_branch(
            &self,
            _: &std::path::Path,
        ) -> crate::ports::repository::Result<String> {
            Ok("main".into())
        }
        async fn get_current_commit_id(
            &self,
            _: &std::path::Path,
        ) -> crate::ports::repository::Result<String> {
            Ok(self.current_commit.clone())
        }
        async fn get_runtime_config(
            &self,
            _: &std::path::Path,
        ) -> crate::ports::repository::Result<Option<RuntimeConfig>> {
            Ok(self.runtime_config.clone())
        }
        async fn get_mount_point(
            &self,
            _: &std::path::Path,
        ) -> crate::ports::repository::Result<Option<String>> {
            Ok(None)
        }
        async fn get_environment_config(
            &self,
            _: &std::path::Path,
        ) -> crate::ports::repository::Result<Option<EnvironmentConfig>> {
            Ok(None)
        }
        async fn get_user_config(
            &self,
            _: &std::path::Path,
        ) -> crate::ports::repository::Result<Option<crate::model::config::UserConfig>> {
            Ok(None)
        }
        async fn ensure_snapshot_path(
            &self,
            _: &std::path::Path,
            _: &str,
        ) -> crate::ports::repository::Result<PathBuf> {
            Ok(PathBuf::from("/tmp/snap"))
        }
        async fn get_active_workspace_data_dir(
            &self,
            _: &std::path::Path,
        ) -> crate::ports::repository::Result<PathBuf> {
            Ok(PathBuf::from("/workspace/data"))
        }
    }

    struct MockCompute;

    #[async_trait]
    impl Compute for MockCompute {
        async fn provision(
            &self,
            _: &ComputeDefinition,
        ) -> crate::ports::compute::Result<InstanceId> {
            Ok(InstanceId("mock".into()))
        }
        async fn start(
            &self,
            id: &InstanceId,
            _: StartOptions,
        ) -> crate::ports::compute::Result<InstanceStatus> {
            Ok(InstanceStatus {
                id: id.clone(),
                state: InstanceState::Running,
                pid: None,
                started_at: None,
                exit_code: None,
            })
        }
        async fn stop(&self, id: &InstanceId) -> crate::ports::compute::Result<InstanceStatus> {
            Ok(InstanceStatus {
                id: id.clone(),
                state: InstanceState::Stopped,
                pid: None,
                started_at: None,
                exit_code: None,
            })
        }
        async fn restart(&self, id: &InstanceId) -> crate::ports::compute::Result<InstanceStatus> {
            Ok(InstanceStatus {
                id: id.clone(),
                state: InstanceState::Running,
                pid: None,
                started_at: None,
                exit_code: None,
            })
        }
        async fn status(&self, id: &InstanceId) -> crate::ports::compute::Result<InstanceStatus> {
            Ok(InstanceStatus {
                id: id.clone(),
                state: InstanceState::Running,
                pid: None,
                started_at: None,
                exit_code: None,
            })
        }
        async fn prepare_for_snapshot(
            &self,
            _: &InstanceId,
            _: &[String],
        ) -> crate::ports::compute::Result<()> {
            Ok(())
        }
        async fn logs(
            &self,
            _: &InstanceId,
            _: crate::ports::compute::LogsOptions,
        ) -> crate::ports::compute::Result<Vec<crate::ports::compute::LogEntry>> {
            Ok(vec![])
        }
        async fn pause(&self, id: &InstanceId) -> crate::ports::compute::Result<InstanceStatus> {
            Ok(InstanceStatus {
                id: id.clone(),
                state: InstanceState::Paused,
                pid: None,
                started_at: None,
                exit_code: None,
            })
        }
        async fn unpause(&self, id: &InstanceId) -> crate::ports::compute::Result<InstanceStatus> {
            Ok(InstanceStatus {
                id: id.clone(),
                state: InstanceState::Running,
                pid: None,
                started_at: None,
                exit_code: None,
            })
        }
        async fn get_connection_info(
            &self,
            _id: &InstanceId,
            port: u16,
        ) -> crate::ports::compute::Result<crate::ports::compute::InstanceConnectionInfo> {
            Ok(crate::ports::compute::InstanceConnectionInfo {
                host: "127.0.0.1".into(),
                port,
                env: vec![],
            })
        }
        async fn get_instance_data_mount_host_path(
            &self,
            _id: &InstanceId,
            _: &str,
        ) -> crate::ports::compute::Result<Option<PathBuf>> {
            Ok(None)
        }
        async fn remove_instance(&self, _id: &InstanceId) -> crate::ports::compute::Result<()> {
            Ok(())
        }
        async fn get_task_connection_info(
            &self,
            _id: &InstanceId,
            port: u16,
        ) -> crate::ports::compute::Result<crate::ports::compute::InstanceConnectionInfo> {
            Ok(crate::ports::compute::InstanceConnectionInfo {
                host: "172.17.0.2".into(),
                port,
                env: vec![],
            })
        }
        async fn run_task(
            &self,
            _: &ComputeDefinition,
            _: &str,
            _: Option<&InstanceId>,
        ) -> crate::ports::compute::Result<crate::ports::compute::ExecOutput> {
            Ok(crate::ports::compute::ExecOutput {
                exit_code: 0,
                stdout: String::new(),
                stderr: String::new(),
            })
        }
    }

    struct MockProvider;

    impl DatabaseProvider for MockProvider {
        fn name(&self) -> &str {
            "postgres"
        }
        fn definition(&self) -> ComputeDefinition {
            ComputeDefinition {
                image: "postgres:17".into(),
                env: vec![],
                ports: vec![],
                data_dir: PathBuf::from("/data"),
                host_data_dir: None,
                user: None,
                logs_dir: None,
                conf_dir: None,
                args: vec![],
            }
        }
        fn default_port(&self) -> u16 {
            5432
        }
        fn default_args(&self) -> Vec<DatabaseProviderArg> {
            vec![]
        }
        fn default_signal(&self) -> u32 {
            SIGTERM
        }
        fn connection_string(
            &self,
            _: &ConnectionParams,
        ) -> std::result::Result<String, ProviderError> {
            Ok("postgres://localhost:5432".into())
        }
        fn supported_versions(&self) -> Vec<String> {
            vec!["17".into()]
        }
        fn supported_features(&self) -> Vec<SupportedFeature> {
            vec![]
        }
        fn prepare_for_snapshot(&self, _: &ConnectionParams) -> RegistryResult<Vec<String>> {
            Ok(vec![])
        }
        fn query_client_command(
            &self,
            _: &ConnectionParams,
            _: Option<&str>,
        ) -> std::result::Result<std::process::Command, ProviderError> {
            Ok(std::process::Command::new("true"))
        }
    }

    struct MockRegistry;

    impl DatabaseProviderRegistry for MockRegistry {
        fn register(&self, _: Arc<dyn DatabaseProvider>) -> RegistryResult<()> {
            Ok(())
        }
        fn get(&self, name: &str) -> Option<Arc<dyn DatabaseProvider>> {
            if name.eq_ignore_ascii_case("postgres") {
                Some(Arc::new(MockProvider))
            } else {
                None
            }
        }
        fn list(&self) -> Vec<String> {
            vec!["postgres".into()]
        }
        fn unregister(&self, _: &str) -> Option<Arc<dyn DatabaseProvider>> {
            None
        }
    }

    #[tokio::test]
    async fn checkout_revision() {
        let repo = MockRepository {
            current_commit: "abc123".into(),
            runtime_config: None,
        };
        let usecase = CheckoutRepoUseCase::new(
            Arc::new(repo),
            Arc::new(MockCompute),
            Arc::new(MockRegistry),
        );
        let dir = tempfile::tempdir().unwrap();
        let result = usecase
            .run(dir.path().to_path_buf(), "main".into(), None)
            .await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "abc123");
    }

    #[tokio::test]
    async fn checkout_empty_revision() {
        let repo = MockRepository {
            current_commit: "abc123".into(),
            runtime_config: None,
        };
        let usecase = CheckoutRepoUseCase::new(
            Arc::new(repo),
            Arc::new(MockCompute),
            Arc::new(MockRegistry),
        );
        let dir = tempfile::tempdir().unwrap();
        let result = usecase.run(dir.path().to_path_buf(), "".into(), None).await;
        assert!(matches!(result, Err(CheckoutRepoError::Repository(_))));
    }

    #[tokio::test]
    async fn checkout_create_branch_empty_name() {
        let repo = MockRepository {
            current_commit: "abc123".into(),
            runtime_config: None,
        };
        let usecase = CheckoutRepoUseCase::new(
            Arc::new(repo),
            Arc::new(MockCompute),
            Arc::new(MockRegistry),
        );
        let dir = tempfile::tempdir().unwrap();
        let result = usecase
            .run(dir.path().to_path_buf(), "main".into(), Some("".into()))
            .await;
        assert!(matches!(result, Err(CheckoutRepoError::Repository(_))));
    }

    #[tokio::test]
    async fn checkout_with_container_stops_and_start() {
        let repo = MockRepository {
            current_commit: "abc123".into(),
            runtime_config: Some(RuntimeConfig {
                runtime_provider: "docker".into(),
                runtime_version: "24".into(),
                container_name: "container-1".into(),
            }),
        };
        let usecase = CheckoutRepoUseCase::new(
            Arc::new(repo),
            Arc::new(MockCompute),
            Arc::new(MockRegistry),
        );
        let dir = tempfile::tempdir().unwrap();
        let result = usecase
            .run(dir.path().to_path_buf(), "main".into(), None)
            .await;
        assert!(result.is_ok());
    }
}
