//! `spool logs [--follow]`: tail the local rotating log file.

use crate::config::Paths;
use crate::telemetry::{LOG_FILE, log_dir};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::Duration;

pub fn log_path(paths: &Paths) -> PathBuf {
    log_dir(paths).join(LOG_FILE)
}

/// Last `n` lines of `text` (including a final unterminated line).
pub fn last_lines(text: &str, n: usize) -> Vec<&str> {
    let mut v: Vec<&str> = text.lines().rev().take(n).collect();
    v.reverse();
    v
}

pub async fn run(paths: &Paths, follow: bool, lines: usize) -> anyhow::Result<()> {
    let path = log_path(paths);
    let mut pos = 0u64;
    match std::fs::read(&path) {
        Ok(bytes) => {
            pos = bytes.len() as u64;
            let text = String::from_utf8_lossy(&bytes);
            for l in last_lines(&text, lines) {
                println!("{l}");
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!(
                "no log file yet at {} (the daemon creates it on start)",
                path.display()
            );
            if !follow {
                return Ok(());
            }
        }
        Err(e) => return Err(e.into()),
    }
    if !follow {
        return Ok(());
    }
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let Ok(mut f) = std::fs::File::open(&path) else {
            continue;
        };
        let len = f.metadata()?.len();
        if len < pos {
            pos = 0; // rotated: the new file is smaller than what we already printed
        }
        if len > pos {
            f.seek(SeekFrom::Start(pos))?;
            let mut buf = Vec::new();
            f.take(len - pos).read_to_end(&mut buf)?;
            pos += buf.len() as u64;
            let mut out = std::io::stdout().lock();
            out.write_all(&buf)?;
            out.flush()?;
        }
    }
}
