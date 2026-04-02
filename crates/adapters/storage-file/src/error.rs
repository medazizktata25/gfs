use gfs_domain::ports::storage::StorageError;

/// Classify a command's stderr message into the appropriate [`StorageError`] variant.
///
/// Works across Linux and Windows; the same heuristic key-words appear in
/// errors from `cp`, `robocopy`, `df`, `attrib`, and PowerShell.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_stderr_not_found() {
        let err = classify_stderr("vol1", "No such file or directory");
        assert!(matches!(err, StorageError::NotFound(s) if s == "vol1"));

        let err = classify_stderr("vol2", "cannot find the path");
        assert!(matches!(err, StorageError::NotFound(s) if s == "vol2"));

        let err = classify_stderr("vol3", "does not exist");
        assert!(matches!(err, StorageError::NotFound(s) if s == "vol3"));
    }

    #[test]
    fn classify_stderr_busy() {
        let err = classify_stderr("vol1", "Device or resource busy");
        assert!(matches!(err, StorageError::Busy(s) if s == "vol1"));

        let err = classify_stderr("vol2", "volume in use");
        assert!(matches!(err, StorageError::Busy(s) if s == "vol2"));
    }

    #[test]
    fn classify_stderr_already_exists() {
        let err = classify_stderr("vol1", "File already exists");
        assert!(matches!(err, StorageError::AlreadyExists(s) if s == "vol1"));

        let err = classify_stderr("vol2", "target already exist");
        assert!(matches!(err, StorageError::AlreadyExists(s) if s == "vol2"));
    }

    #[test]
    fn classify_stderr_internal() {
        let err = classify_stderr("vol1", "Some unknown error");
        assert!(matches!(err, StorageError::Internal(s) if s == "Some unknown error"));
    }
}
