//! Permission model (spec 15.2): directories 0700; socket, signing key, provider credential,
//! databases and their WAL/SHM files 0600. Used by daemon startup and `doctor`.

use crate::config::Paths;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermIssue {
    pub path: PathBuf,
    pub expected: u32,
    /// `None` when the owner differs from the current user.
    pub actual: Option<u32>,
    pub detail: String,
}

fn check_one(path: &Path, expected: u32, out: &mut Vec<PermIssue>) {
    let Ok(md) = std::fs::symlink_metadata(path) else {
        return;
    };
    let uid = rustix::process::geteuid().as_raw();
    if md.uid() != uid {
        out.push(PermIssue {
            path: path.into(),
            expected,
            actual: None,
            detail: format!("owned by uid {}, expected {uid}", md.uid()),
        });
        return;
    }
    let mode = md.permissions().mode() & 0o777;
    // Stricter than expected is fine; any group/other bit beyond the expectation is not.
    if mode & !expected != 0 {
        out.push(PermIssue {
            path: path.into(),
            expected,
            actual: Some(mode),
            detail: format!("mode {mode:o}, expected {expected:o}"),
        });
    }
}

/// Every existing sensitive path under `paths` (+ extra files such as the provider credential).
pub fn check(paths: &Paths, extra_files: &[PathBuf]) -> Vec<PermIssue> {
    let mut out = Vec::new();
    for d in [&paths.root, &paths.state_dir, &paths.secrets_dir] {
        check_one(d, 0o700, &mut out);
    }
    let mut files: Vec<PathBuf> = vec![
        paths.socket.clone(),
        paths.signing_key.clone(),
        paths.lock.clone(),
    ];
    for db in [&paths.index_db, &paths.jobs_db] {
        for suffix in ["", "-wal", "-shm"] {
            let mut p = db.as_os_str().to_owned();
            p.push(suffix);
            files.push(PathBuf::from(p));
        }
    }
    files.extend(extra_files.iter().cloned());
    for f in files {
        check_one(&f, 0o600, &mut out);
    }
    out
}

/// Tighten modes the daemon itself created (databases, WAL/SHM, lock, socket). Foreign-owned
/// paths are reported, never chmodded.
pub fn tighten(issues: &[PermIssue]) -> std::io::Result<()> {
    for i in issues {
        if i.actual.is_some() {
            std::fs::set_permissions(&i.path, std::fs::Permissions::from_mode(i.expected))?;
        }
    }
    Ok(())
}
