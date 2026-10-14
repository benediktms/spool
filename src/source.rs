use rustix::{
    fd::OwnedFd,
    fs::{AtFlags, Dir, FileType, Mode, OFlags, fstat, openat, statat},
};
use std::{
    ffi::{OsStr, OsString},
    fs::{self, File},
    io,
    os::unix::ffi::OsStringExt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
};

#[derive(Debug, Clone)]
pub struct SourceFileInfo {
    pub path: PathBuf,
    pub len: u64,
    pub mtime_ms: i64,
    pub dev: u64,
    pub ino: u64,
}
#[derive(Debug)]
pub struct OpenedSource {
    pub file: File,
    pub info: SourceFileInfo,
}
#[derive(Debug, Clone)]
pub struct ScanError {
    pub path: PathBuf,
    pub kind: io::ErrorKind,
}
#[derive(Debug, Default)]
pub struct ScanOutcome {
    pub files: Vec<SourceFileInfo>,
    pub complete: bool,
    pub truncated: bool,
    pub next_after: Option<PathBuf>,
    pub errors: Vec<ScanError>,
}
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("source is outside enabled roots")]
    OutsideRoot,
    #[error("source is not a regular file owned by this user")]
    UnsafeFile,
    #[error("source IO: {0}")]
    Io(#[from] io::Error),
}
struct Root {
    path: PathBuf,
    fd: OwnedFd,
}
pub struct SourceRoots {
    roots: Vec<Root>,
}
impl SourceRoots {
    pub fn new(paths: &[PathBuf]) -> io::Result<Self> {
        let mut roots = Vec::with_capacity(paths.len());
        for path in paths {
            // Configured root symlinks are resolved once; every child is walked by held dirfd.
            let canonical = fs::canonicalize(path)?;
            let fd = openat(
                rustix::fs::CWD,
                canonical.as_path(),
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::empty(),
            )
            .map_err(io::Error::from)?;
            let st = fstat(&fd).map_err(io::Error::from)?;
            if FileType::from_raw_mode(st.st_mode) != FileType::Directory
                || st.st_uid != rustix::process::geteuid().as_raw()
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "source root must be a current-user directory",
                ));
            }
            roots.push(Root {
                path: canonical,
                fd,
            });
        }
        roots.sort_by(|a, b| a.path.cmp(&b.path));
        let mut filtered = Vec::with_capacity(roots.len());
        for root in roots {
            if !filtered
                .iter()
                .any(|parent: &Root| root.path.starts_with(&parent.path))
            {
                filtered.push(root);
            }
        }
        Ok(Self { roots: filtered })
    }
    pub fn scan(&self, cancel: &AtomicBool, max_entries: usize) -> ScanOutcome {
        self.scan_from(cancel, max_entries, None)
    }
    /// Yields only regular *.jsonl files: every supported transcript format is JSONL; anything else under a root is a session artifact, not a session.
    pub fn scan_from(
        &self,
        cancel: &AtomicBool,
        max_entries: usize,
        after: Option<&Path>,
    ) -> ScanOutcome {
        let mut out = ScanOutcome::default();
        let page_limit = max_entries.max(1);
        for root in &self.roots {
            if cancel.load(Ordering::Relaxed) {
                break;
            }
            if let Err(kind) =
                self.walk_dir(&root.fd, &root.path, after, page_limit, cancel, &mut out)
            {
                out.errors.push(ScanError {
                    path: root.path.clone(),
                    kind,
                });
            }
            if out.truncated {
                break;
            }
        }
        if out.truncated {
            out.next_after = out.files.last().map(|file| file.path.clone());
        } else {
            out.complete = !cancel.load(Ordering::Relaxed) && out.errors.is_empty();
        }
        out
    }
    fn walk_dir(
        &self,
        fd: &OwnedFd,
        dir: &Path,
        after: Option<&Path>,
        limit: usize,
        cancel: &AtomicBool,
        out: &mut ScanOutcome,
    ) -> Result<(), io::ErrorKind> {
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut entries = Vec::new();
        let iter = Dir::read_from(fd).map_err(|e| e.kind())?;
        for item in iter {
            match item {
                Ok(entry) => entries.push(entry.file_name().to_bytes().to_vec()),
                Err(e) => out.errors.push(ScanError {
                    path: dir.to_owned(),
                    kind: e.kind(),
                }),
            }
        }
        entries.sort_unstable();
        for name in entries {
            if cancel.load(Ordering::Relaxed) {
                return Ok(());
            }
            if name == b"." || name == b".." {
                continue;
            }
            let name = OsString::from_vec(name);
            let path = dir.join(&name);
            let stat = match statat(fd, &name, AtFlags::SYMLINK_NOFOLLOW) {
                Ok(stat) => stat,
                Err(e) => {
                    out.errors.push(ScanError {
                        path,
                        kind: e.kind(),
                    });
                    continue;
                }
            };
            match FileType::from_raw_mode(stat.st_mode) {
                FileType::Directory
                    if after.is_none_or(|a| path.as_path() > a || a.starts_with(&path)) =>
                {
                    let child = match openat(
                        fd,
                        &name,
                        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                        Mode::empty(),
                    ) {
                        Ok(child) => child,
                        Err(e) => {
                            out.errors.push(ScanError {
                                path,
                                kind: e.kind(),
                            });
                            continue;
                        }
                    };
                    let child_stat = match fstat(&child) {
                        Ok(stat) => stat,
                        Err(e) => {
                            out.errors.push(ScanError {
                                path,
                                kind: e.kind(),
                            });
                            continue;
                        }
                    };
                    if child_stat.st_uid != rustix::process::geteuid().as_raw() {
                        out.errors.push(ScanError {
                            path,
                            kind: io::ErrorKind::PermissionDenied,
                        });
                        continue;
                    }
                    self.walk_dir(&child, &path, after, limit, cancel, out)?;
                    if out.truncated || cancel.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                }
                FileType::RegularFile
                    if after.is_none_or(|a| path.as_path() > a)
                        && Path::new(&name).extension() == Some(OsStr::new("jsonl")) =>
                {
                    if out.files.len() == limit {
                        out.truncated = true;
                        return Ok(());
                    }
                    match self.open(&path) {
                        Ok(opened) => out.files.push(opened.info),
                        Err(e) => out.errors.push(ScanError {
                            path,
                            kind: match e {
                                SourceError::Io(e) => e.kind(),
                                _ => io::ErrorKind::PermissionDenied,
                            },
                        }),
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    pub fn open(&self, path: &Path) -> Result<OpenedSource, SourceError> {
        for root in &self.roots {
            let Ok(relative) = path.strip_prefix(&root.path) else {
                continue;
            };
            if relative
                .components()
                .any(|c| !matches!(c, std::path::Component::Normal(_)))
            {
                return Err(SourceError::OutsideRoot);
            }
            let mut current = File::from(dup_fd(&root.fd)?);
            let parts: Vec<_> = relative.components().collect();
            if parts.is_empty() {
                return Err(SourceError::UnsafeFile);
            }
            for (index, component) in parts.iter().enumerate() {
                let final_part = index + 1 == parts.len();
                let flags = OFlags::RDONLY
                    | OFlags::CLOEXEC
                    | OFlags::NOFOLLOW
                    | if final_part {
                        OFlags::empty()
                    } else {
                        OFlags::DIRECTORY
                    };
                let fd = openat(&current, component.as_os_str(), flags, Mode::empty())
                    .map_err(io::Error::from)?;
                let st = fstat(&fd).map_err(io::Error::from)?;
                if st.st_uid != rustix::process::geteuid().as_raw() {
                    return Err(SourceError::UnsafeFile);
                }
                let ty = FileType::from_raw_mode(st.st_mode);
                if (final_part && ty != FileType::RegularFile)
                    || (!final_part && ty != FileType::Directory)
                {
                    return Err(SourceError::UnsafeFile);
                }
                current = File::from(fd);
            }
            let st = fstat(&current).map_err(io::Error::from)?;
            let mtime_ms = st
                .st_mtime
                .saturating_mul(1000)
                .saturating_add((st.st_mtime_nsec / 1_000_000) as i64);
            return Ok(OpenedSource {
                file: current,
                info: SourceFileInfo {
                    path: path.to_owned(),
                    len: st.st_size.max(0) as u64,
                    mtime_ms,
                    dev: st.st_dev as u64,
                    ino: st.st_ino as u64,
                },
            });
        }
        Err(SourceError::OutsideRoot)
    }
}
fn dup_fd(fd: &OwnedFd) -> io::Result<OwnedFd> {
    rustix::io::fcntl_dupfd_cloexec(fd, 0).map_err(io::Error::from)
}
