//! Structured local file logging (spec 18). Logs never leave the machine and never carry content:
//! `logging.include_content` is reserved and ignored; no code path here or in the daemon logs
//! query text, conversation text, credentials or the signing key.

use crate::config::{LoggingConfig, Paths};
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing_subscriber::{EnvFilter, fmt::MakeWriter};

pub const LOG_FILE: &str = "spool.log";

/// `~/Library/Logs/Spool` on macOS, `<state>/logs` elsewhere. An explicit `SPOOL_HOME`
/// (isolated runs, tests) keeps logs inside that home.
pub fn log_dir(paths: &Paths) -> PathBuf {
    if std::env::var_os("SPOOL_HOME").is_none() && cfg!(target_os = "macos") {
        if let Some(h) = std::env::var_os("HOME") {
            return PathBuf::from(h).join("Library/Logs/Spool");
        }
    }
    if std::env::var_os("SPOOL_HOME").is_some() {
        return paths.root.join("logs");
    }
    paths.state_dir.join("logs")
}

/// Size-rotated append-only log file: `name`, `name.1` .. `name.<retained>` (newest first).
pub struct RotatingFile {
    dir: PathBuf,
    name: String,
    max_bytes: u64,
    retained: usize,
    file: File,
    size: u64,
}

fn open_log(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)
}

impl RotatingFile {
    pub fn open(dir: &Path, name: &str, max_bytes: u64, retained: usize) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        let path = dir.join(name);
        let file = open_log(&path)?;
        // A pre-existing file keeps its mode only if it is already private; tighten otherwise.
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        let size = file.metadata()?.len();
        Ok(Self {
            dir: dir.to_path_buf(),
            name: name.into(),
            max_bytes,
            retained,
            file,
            size,
        })
    }

    fn numbered(&self, i: usize) -> PathBuf {
        self.dir.join(format!("{}.{i}", self.name))
    }

    fn rotate(&mut self) -> io::Result<()> {
        let current = self.dir.join(&self.name);
        if self.retained == 0 {
            std::fs::remove_file(&current)?;
        } else {
            for i in (1..self.retained).rev() {
                if self.numbered(i).exists() {
                    std::fs::rename(self.numbered(i), self.numbered(i + 1))?;
                }
            }
            std::fs::rename(&current, self.numbered(1))?;
        }
        self.file = open_log(&current)?;
        self.size = 0;
        Ok(())
    }
}

impl Write for RotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.size > 0 && self.size + buf.len() as u64 > self.max_bytes {
            self.rotate()?;
        }
        let n = self.file.write(buf)?;
        self.size += n as u64;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// Cloneable handle used as the `tracing` writer; optionally mirrors to stderr (interactive runs).
#[derive(Clone)]
pub struct SharedLog {
    inner: Arc<Mutex<RotatingFile>>,
    tee_stderr: bool,
}

impl SharedLog {
    pub fn new(file: RotatingFile, tee_stderr: bool) -> Self {
        Self {
            inner: Arc::new(Mutex::new(file)),
            tee_stderr,
        }
    }
}

impl Write for SharedLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut g = self.inner.lock();
        let n = g.write(buf)?;
        if self.tee_stderr {
            let _ = io::stderr().write_all(&buf[..n]);
        }
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.lock().flush()
    }
}

impl<'a> MakeWriter<'a> for SharedLog {
    type Writer = SharedLog;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Install the global subscriber writing to the rotating log file (used by `spool daemon run`).
/// Returns the log file path. `RUST_LOG` overrides `logging.level`.
pub fn init(paths: &Paths, cfg: &LoggingConfig) -> anyhow::Result<PathBuf> {
    use std::io::IsTerminal;
    let dir = log_dir(paths);
    let file = RotatingFile::open(&dir, LOG_FILE, cfg.max_file_bytes, cfg.retained_files)?;
    let writer = SharedLog::new(file, io::stderr().is_terminal());
    let filter = match EnvFilter::try_from_env("RUST_LOG") {
        Ok(f) => f,
        Err(_) => {
            EnvFilter::try_new(&cfg.level).map_err(|e| anyhow::anyhow!("logging.level: {e}"))?
        }
    };
    tracing_subscriber::fmt()
        .with_writer(writer)
        .with_ansi(false)
        .with_env_filter(filter)
        .try_init()
        .map_err(|e| anyhow::anyhow!("installing log subscriber: {e}"))?;
    let path = dir.join(LOG_FILE);
    tracing::info!(log_file = %path.display(), "logging initialised");
    Ok(path)
}
