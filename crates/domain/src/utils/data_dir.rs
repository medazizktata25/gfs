use std::io;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::process::Command;

#[cfg(unix)]
use crate::utils::current_user;

pub fn prepare_for_database_provider(provider_name: &str, host_data_dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    if provider_name.eq_ignore_ascii_case("clickhouse") {
        prepare_clickhouse_data_dir(host_data_dir)?;
    }

    let _ = (provider_name, host_data_dir);
    Ok(())
}

#[cfg(unix)]
fn prepare_clickhouse_data_dir(host_data_dir: &Path) -> io::Result<()> {
    let metadata = std::fs::metadata(host_data_dir)?;
    let mut permissions = metadata.permissions();
    permissions.set_mode(0o777);
    std::fs::set_permissions(host_data_dir, permissions)?;
    let _ = Command::new("chmod")
        .args(["-R", "a+rwX"])
        .arg(host_data_dir)
        .status();

    let Some(user_name) = current_user::current_user_name() else {
        return Ok(());
    };

    apply_setfacl(host_data_dir, &["-m", &format!("u:{user_name}:rwx")])?;
    apply_setfacl(host_data_dir, &["-d", "-m", &format!("u:{user_name}:rwx")])?;
    Ok(())
}

#[cfg(unix)]
fn apply_setfacl(path: &Path, args: &[&str]) -> io::Result<()> {
    match Command::new("setfacl").args(args).arg(path).output() {
        Ok(output) if output.status.success() => Ok(()),
        Ok(output) => Err(io::Error::other(format!(
            "setfacl failed for '{}': {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr).trim()
        ))),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}
