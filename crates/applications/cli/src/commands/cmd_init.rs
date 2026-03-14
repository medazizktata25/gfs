use std::path::PathBuf;
use std::sync::Arc;

use gfs_compute_docker::DockerCompute;
use gfs_compute_docker::containers;
use gfs_domain::adapters::gfs_repository::GfsRepository;
use gfs_domain::ports::compute::Compute;
use gfs_domain::ports::database_provider::InMemoryDatabaseProviderRegistry;
use gfs_domain::ports::repository::Repository;
use gfs_domain::usecases::repository::init_repo_usecase::InitRepositoryUseCase;

use crate::cli_utils::get_repo_dir;

pub async fn init(
    path: Option<PathBuf>,
    database_provider: Option<String>,
    database_version: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing::trace!("Initializing Guepard environment at: {:?}", path);

    let target_path = path.unwrap_or_else(get_repo_dir);

    let repository: Arc<dyn Repository> = Arc::new(GfsRepository::new());
    let compute: Arc<dyn Compute> =
        Arc::new(DockerCompute::new().map_err(|e| DockerCompute::format_connection_error(&e))?);

    let registry = Arc::new(InMemoryDatabaseProviderRegistry::new());
    containers::register_all(registry.as_ref())?;

    let use_case = InitRepositoryUseCase::new(repository, compute, registry);
    use_case
        .run(target_path, None, database_provider, database_version)
        .await?;

    Ok(())
}
