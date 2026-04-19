//! `gfs` – Guepard data-plane CLI binary.
//!
//! Thin wrapper around the library. See `gfs_cli::run()` for programmatic use.

use gfs_cli::output::red;
use serde_json::json;

fn wants_json(args: &[String]) -> bool {
    for a in args {
        if a == "--" {
            break;
        }
        if a == "--json" {
            return true;
        }
        if let Some(rest) = a.strip_prefix("--json=") {
            let v = rest.trim().to_ascii_lowercase();
            return matches!(v.as_str(), "1" | "true" | "yes" | "on");
        }
    }
    false
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let args: Vec<String> = std::env::args().collect();
    let wants_json = wants_json(&args);

    match gfs_cli::run(args).await {
        Ok(exit_code) => std::process::exit(exit_code),
        Err(err) => {
            if wants_json {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "error": {
                            "message": err.to_string(),
                            "details": format!("{err:#}"),
                        }
                    }))
                    .unwrap_or_else(|_| "{\"error\":{\"message\":\"serialization failed\"}}".into())
                );
            } else {
                eprintln!("{} {err}", red("error:"));
            }
            std::process::exit(1);
        }
    }
}
