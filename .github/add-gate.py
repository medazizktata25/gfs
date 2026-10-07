"""Add an env gate that skips only the WAL checkpoint, to main's sqlite.rs.

Reproduces the exact shape an earlier harness scored as "arm C", so the
contradiction between that run (0 failures of 60) and this branch's source removal
(12 of 40) can be resolved inside a single interleaved run.

Lives in a file rather than a heredoc inside the workflow: a heredoc terminator has
to start at column 1, which closes the YAML block scalar it sits in. That produced
invalid YAML twice in this repository's temporary harnesses.
"""
import sys

path = sys.argv[1]
source = open(path).read()
old = '''        if let Err(e) = conn.pragma_update(None, "wal_checkpoint", "TRUNCATE") {
            tracing::debug!(error = %e, "wal checkpoint did not complete; snapshotting WAL as-is");
        }'''
assert source.count(old) == 1, f"checkpoint block found {source.count(old)} times"
new = '''        if std::env::var("ARM_C").as_deref() != Ok("1") {
            if let Err(e) = conn.pragma_update(None, "wal_checkpoint", "TRUNCATE") {
                tracing::debug!(error = %e, "wal checkpoint did not complete; snapshotting WAL as-is");
            }
        }'''
open(path, "w").write(source.replace(old, new, 1))
print("gated binary source prepared")
