use gfs_domain::ports::storage::StorageError;

/// Classify a command's stderr message into the appropriate [`StorageError`] variant.
///
/// Works across Linux and Windows; the same heuristic key-words appear in
/// errors from `cp`, `robocopy`, `df`, `icacls`, and PowerShell.
pub(crate) fn classify_stderr(volume_id: &str, stderr: &str) -> StorageError {
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
