use std::process::Command;
use std::sync::OnceLock;

fn command_exists(cmd: &str) -> bool {
    Command::new(cmd)
        .arg("--version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

pub fn runtime_binary() -> &'static str {
    static BIN: OnceLock<&'static str> = OnceLock::new();
    BIN.get_or_init(|| {
        if command_exists("docker") {
            "docker"
        } else if command_exists("podman") {
            "podman"
        } else {
            // Keep docker as default so existing failure messages remain actionable.
            "docker"
        }
    })
}

/// Create a runtime command pre-configured to target the same Docker daemon
/// that the `gfs` compute adapter uses.
///
/// On some Linux setups, the Docker CLI context (e.g. Docker Desktop) may point
/// to a different socket than the default `/var/run/docker.sock` used by the API client.
pub fn runtime_command() -> Command {
    let bin = runtime_binary();
    let mut cmd = Command::new(bin);

    #[cfg(unix)]
    {
        if bin == "docker" && std::path::Path::new("/var/run/docker.sock").exists() {
            cmd.env("DOCKER_HOST", "unix:///var/run/docker.sock");
        }
    }

    cmd
}
