//! Helpers for current user identification.

use std::process::Command;

#[cfg(unix)]
fn id_value(flag: &str) -> Option<String> {
    let output = Command::new("id").args([flag]).output().ok()?.stdout;
    let value = String::from_utf8_lossy(&output).trim().to_string();
    if value.is_empty() {
        return None;
    }
    Some(value)
}

/// Return current Unix uid and gid as "uid:gid" for `docker run --user`.
/// Returns `None` if id cannot be determined (e.g. on Windows or when `id` is unavailable).
#[cfg(unix)]
pub fn current_user_uid_gid() -> Option<String> {
    let uid = id_value("-u")?;
    let gid = id_value("-g")?;
    Some(format!("{uid}:{gid}"))
}

#[cfg(unix)]
pub fn current_user_name() -> Option<String> {
    id_value("-un")
}

#[cfg(not(unix))]
pub fn current_user_uid_gid() -> Option<String> {
    None
}

#[cfg(not(unix))]
pub fn current_user_name() -> Option<String> {
    None
}
