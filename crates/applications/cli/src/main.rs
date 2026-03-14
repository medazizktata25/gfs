//! `gfs` – Guepard data-plane CLI binary.
//!
//! Thin wrapper around the library. See `gfs_cli::run()` for programmatic use.

use gfs_cli::output::red;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match gfs_cli::run(std::env::args()).await {
        Ok(exit_code) => std::process::exit(exit_code),
        Err(err) => {
            eprintln!("{} {err:#}", red("error:"));
            std::process::exit(1);
        }
    }
}
