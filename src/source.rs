#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

#[cfg(unix)]
pub use unix::SourceRoots;
#[cfg(windows)]
pub use windows::SourceRoots;

use std::{fs::File, io, path::PathBuf};

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
