//! Helpers for current user identification.

use std::process::Command;

/// Return current Unix uid and gid as "uid:gid" for `docker run --user`.
/// Returns `None` if id cannot be determined (e.g. on Windows or when `id` is unavailable).
#[cfg(unix)]
pub fn current_user_uid_gid() -> Option<String> {
    let uid = Command::new("id").args(["-u"]).output().ok()?.stdout;
    let gid = Command::new("id").args(["-g"]).output().ok()?.stdout;
    let uid = String::from_utf8_lossy(&uid).trim().to_string();
    let gid = String::from_utf8_lossy(&gid).trim().to_string();
    if uid.is_empty() || gid.is_empty() {
        return None;
    }
    Some(format!("{uid}:{gid}"))
}

#[cfg(not(unix))]
pub fn current_user_uid_gid() -> Option<String> {
    None
}
