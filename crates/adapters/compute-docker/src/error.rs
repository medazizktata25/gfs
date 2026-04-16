use gfs_domain::ports::compute::ComputeError;

fn is_connection_error(err: &bollard::errors::Error) -> bool {
    if let bollard::errors::Error::IOError { err: io_err } = err {
        let kind = io_err.kind();
        if matches!(
            kind,
            std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::PermissionDenied
        ) {
            return true;
        }
    }
    err.to_string().to_ascii_lowercase().contains("connect")
}

/// Classify a bollard error into the appropriate [`ComputeError`] variant.
///
/// Bollard surfaces Docker daemon errors as [`bollard::errors::Error::DockerResponseServerError`]
/// with an HTTP status code and a message string. We inspect both the status and
/// the message body to produce the most specific `ComputeError`.
pub(crate) fn classify(container_id: &str, err: bollard::errors::Error) -> ComputeError {
    if is_connection_error(&err) {
        return ComputeError::NotAvailable("Docker".to_string());
    }
    match &err {
        bollard::errors::Error::DockerResponseServerError {
            status_code,
            message,
        } => {
            let msg = message.to_ascii_lowercase();
            match status_code {
                404 => ComputeError::NotFound(if container_id.is_empty() {
                    message.clone()
                } else {
                    container_id.to_owned()
                }),
                409 => {
                    if msg.contains("already started")
                        || msg.contains("is already running")
                        || msg.contains("container already running")
                    {
                        ComputeError::AlreadyRunning(container_id.to_owned())
                    } else if msg.contains("is not running") || msg.contains("not running") {
                        ComputeError::NotRunning(container_id.to_owned())
                    } else if msg.contains("already paused") {
                        ComputeError::AlreadyPaused(container_id.to_owned())
                    } else if msg.contains("is not paused") || msg.contains("not paused") {
                        ComputeError::NotPaused(container_id.to_owned())
                    } else {
                        ComputeError::Internal(message.clone())
                    }
                }
                _ => {
                    // Rootless Podman / cgroup v1 hosts cannot freeze container
                    // processes.  The daemon surfaces this as a 500 with a message
                    // that is semantically about *pausing* being unsupported.
                    //
                    // We require the message to be explicitly about pause/freeze
                    // to avoid false-positives from unrelated 500 errors that
                    // happen to mention "cgroup" or "not supported" in other contexts
                    // (e.g. "cgroup memory limit exceeded", "network feature not supported").
                    let is_about_pause =
                        msg.contains("pause") || msg.contains("freeze") || msg.contains("freezing");
                    let is_unsupported_reason = msg.contains("cgroup v1")
                        || msg.contains("rootless")
                        || msg.contains("pause is not")
                        || msg.contains("cannot pause")
                        || (msg.contains("cgroup")
                            && (msg.contains("freeze") || msg.contains("pause")))
                        || (msg.contains("not supported") && is_about_pause);
                    if is_about_pause && is_unsupported_reason {
                        ComputeError::PauseUnsupported(message.clone())
                    } else {
                        ComputeError::Internal(message.clone())
                    }
                }
            }
        }
        bollard::errors::Error::IOError { err } => ComputeError::Internal(err.to_string()),
        other => ComputeError::Internal(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    fn docker_err(status_code: u16, message: impl Into<String>) -> bollard::errors::Error {
        bollard::errors::Error::DockerResponseServerError {
            status_code,
            message: message.into(),
        }
    }

    #[test]
    fn classify_404_with_container_id() {
        let err = classify("cid-123", docker_err(404, "not found"));
        assert!(matches!(err, ComputeError::NotFound(s) if s == "cid-123"));
    }

    #[test]
    fn classify_404_empty_container_id_uses_message() {
        let err = classify("", docker_err(404, "No such container"));
        assert!(matches!(err, ComputeError::NotFound(s) if s == "No such container"));
    }

    #[test]
    fn classify_409_already_running() {
        let err = classify("c1", docker_err(409, "Container is already running"));
        assert!(matches!(err, ComputeError::AlreadyRunning(s) if s == "c1"));
    }

    #[test]
    fn classify_409_not_running() {
        let err = classify("c1", docker_err(409, "Container is not running"));
        assert!(matches!(err, ComputeError::NotRunning(s) if s == "c1"));
    }

    #[test]
    fn classify_409_already_paused() {
        let err = classify("c1", docker_err(409, "Container already paused"));
        assert!(matches!(err, ComputeError::AlreadyPaused(s) if s == "c1"));
    }

    #[test]
    fn classify_409_not_paused() {
        let err = classify("c1", docker_err(409, "Container is not paused"));
        assert!(matches!(err, ComputeError::NotPaused(s) if s == "c1"));
    }

    #[test]
    fn classify_409_fallback_to_internal() {
        let err = classify("c1", docker_err(409, "Some other conflict"));
        assert!(matches!(err, ComputeError::Internal(_)));
    }

    #[test]
    fn classify_500_internal() {
        let err = classify("c1", docker_err(500, "Server error"));
        assert!(matches!(err, ComputeError::Internal(s) if s == "Server error"));
    }

    #[test]
    fn classify_500_cgroup_v1_freeze_is_pause_unsupported() {
        let err = classify(
            "c1",
            docker_err(
                500,
                "OCI: cgroup v1 does not support freezing a single process",
            ),
        );
        assert!(matches!(err, ComputeError::PauseUnsupported(_)));
    }

    #[test]
    fn classify_500_pause_not_supported_rootless_is_pause_unsupported() {
        let err = classify("c1", docker_err(500, "pause is not supported on rootless"));
        assert!(matches!(err, ComputeError::PauseUnsupported(_)));
    }

    #[test]
    fn classify_500_unrelated_cgroup_error_is_internal() {
        let err = classify("c1", docker_err(500, "cgroup memory limit exceeded"));
        assert!(
            matches!(err, ComputeError::Internal(_)),
            "unrelated cgroup error must not be classified as PauseUnsupported"
        );
    }

    #[test]
    fn classify_500_unrelated_not_supported_is_internal() {
        let err = classify("c1", docker_err(500, "network feature not supported"));
        assert!(
            matches!(err, ComputeError::Internal(_)),
            "unrelated 'not supported' message must not be classified as PauseUnsupported"
        );
    }

    #[test]
    fn classify_io_error_connection_refused_is_not_available() {
        let err = classify(
            "c1",
            bollard::errors::Error::IOError {
                err: io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused"),
            },
        );
        assert!(matches!(err, ComputeError::NotAvailable(_)));
    }

    #[test]
    fn classify_io_error_permission_denied_is_not_available() {
        let err = classify(
            "c1",
            bollard::errors::Error::IOError {
                err: io::Error::new(io::ErrorKind::PermissionDenied, "permission denied"),
            },
        );
        assert!(matches!(err, ComputeError::NotAvailable(_)));
    }

    #[test]
    fn classify_io_error_other_is_internal() {
        let err = classify(
            "c1",
            bollard::errors::Error::IOError {
                err: io::Error::new(io::ErrorKind::TimedOut, "timed out"),
            },
        );
        assert!(matches!(err, ComputeError::Internal(_)));
    }
}
