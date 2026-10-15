//! Source access for the reader: safe open through `SourceRegistry`, rename re-resolution by file
//! identity, per-record `resolve_locator` with digest verification, and the local I/O / time
//! budgets (spec 14.2.5, 14.2.7.2). A positively detected mismatch is never reported as
//! "unavailable"; only the caller's policy decides whether unavailable may fall back.

use super::selection::{Snap, app_err};
use crate::adapters::{AdapterError, SessionAdapter};
use crate::domain::{Digest, ResolvedRecord, SourceLocator};
use crate::ipc::AppError;
use crate::source::SourceError;
use crate::sources::SourceRegistry;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

/// Upper bound on directory entries visited while re-locating a renamed source.
const MAX_RELOCATE_ENTRIES: usize = 20_000;
const RELOCATE_PAGE: usize = 1_000;
/// Largest unparsed tail the freshness check will scan for a newline.
const TAIL_SCAN_CAP: u64 = 1 << 20;

#[derive(Debug)]
pub enum ResolveFail {
    /// The source cannot be read right now (missing, unsafe, I/O error).
    Unavailable,
    /// The adapter cannot return byte-exact content for this record.
    Unsupported,
    Fatal(AppError),
}

pub fn source_changed() -> AppError {
    app_err(
        "SOURCE_CHANGED",
        "original content no longer matches the pinned observation",
        false,
        "new_read",
    )
}

pub fn source_unavailable() -> AppError {
    app_err(
        "SOURCE_UNAVAILABLE",
        "the source needed for this material cannot be read",
        true,
        "retry_later",
    )
}

pub fn resource_limit(limit: &str, message: &str, recovery: &str) -> AppError {
    let mut e = app_err("RESOURCE_LIMIT", message, false, recovery);
    e.details = Some(serde_json::json!({ "limit": limit }));
    e
}

/// Per-call read budgets applied to one [`SourceCtx`].
#[derive(Clone, Copy)]
pub struct SourceLimits {
    pub max_record_bytes: u64,
    pub max_read_bytes: u64,
    pub max_read_millis: u64,
}

pub struct SourceCtx {
    sources: Arc<SourceRegistry>,
    source_id: String,
    path: Option<PathBuf>,
    file_dev: Option<i64>,
    file_ino: Option<i64>,
    adapter: Arc<dyn SessionAdapter>,
    max_record_bytes: u64,
    max_read_bytes: u64,
    deadline: Instant,
    file: Option<Arc<File>>,
    info: Option<crate::source::SourceFileInfo>,
    bytes_read: u64,
    last: Option<(String, Arc<ResolvedRecord>)>,
}

impl SourceCtx {
    pub fn new(
        sources: Arc<SourceRegistry>,
        source_id: String,
        path: Option<PathBuf>,
        snap: &Snap,
        adapter: Arc<dyn SessionAdapter>,
        limits: SourceLimits,
    ) -> Self {
        Self {
            sources,
            source_id,
            path,
            file_dev: snap.file_dev,
            file_ino: snap.file_ino,
            adapter,
            max_record_bytes: limits.max_record_bytes,
            max_read_bytes: limits.max_read_bytes,
            deadline: Instant::now() + std::time::Duration::from_millis(limits.max_read_millis),
            file: None,
            info: None,
            bytes_read: 0,
            last: None,
        }
    }

    pub fn check_time(&self) -> Result<(), AppError> {
        if Instant::now() > self.deadline {
            return Err(resource_limit(
                "max_read_millis",
                "the local read time budget was exceeded",
                "narrow_selection",
            ));
        }
        Ok(())
    }

    async fn open_path(
        &self,
        path: PathBuf,
    ) -> Result<Option<(Arc<File>, crate::source::SourceFileInfo)>, ResolveFail> {
        let (sources, sid) = (self.sources.clone(), self.source_id.clone());
        match tokio::task::spawn_blocking(move || sources.open(&sid, &path)).await {
            Ok(Ok(o)) => Ok(Some((Arc::new(o.file), o.info))),
            Ok(Err(SourceError::Io(e))) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            _ => Err(ResolveFail::Unavailable),
        }
    }

    /// Open the source once per call. A missing path is re-resolved through file identity
    /// (dev/ino) with a bounded scan; digests are still verified per record afterwards.
    async fn ensure_open(&mut self) -> Result<Arc<File>, ResolveFail> {
        if let Some(f) = &self.file {
            return Ok(f.clone());
        }
        let path = self.path.clone().ok_or(ResolveFail::Unavailable)?;
        let mut found = self.open_path(path).await?;
        if found.is_none() {
            found = self.relocate().await?;
        }
        let (file, info) = found.ok_or(ResolveFail::Unavailable)?;
        self.file = Some(file.clone());
        self.info = Some(info);
        Ok(file)
    }

    async fn relocate(
        &mut self,
    ) -> Result<Option<(Arc<File>, crate::source::SourceFileInfo)>, ResolveFail> {
        let (Some(dev), Some(ino)) = (self.file_dev, self.file_ino) else {
            return Ok(None);
        };
        let mut after: Option<PathBuf> = None;
        let mut scanned = 0usize;
        loop {
            self.check_time().map_err(ResolveFail::Fatal)?;
            let (sources, sid, a) = (self.sources.clone(), self.source_id.clone(), after.clone());
            let out = tokio::task::spawn_blocking(move || {
                sources.scan_from(&sid, &AtomicBool::new(false), RELOCATE_PAGE, a.as_deref())
            })
            .await
            .map_err(|_| ResolveFail::Unavailable)?
            .ok_or(ResolveFail::Unavailable)?;
            scanned += out.files.len();
            if let Some(f) = out
                .files
                .iter()
                .find(|f| f.dev as i64 == dev && f.ino as i64 == ino)
            {
                return self.open_path(f.path.clone()).await;
            }
            if !out.truncated {
                return Ok(None);
            }
            if scanned >= MAX_RELOCATE_ENTRIES {
                return Err(ResolveFail::Fatal(resource_limit(
                    "scan",
                    "locating the renamed source exceeded the scan budget",
                    "new_read",
                )));
            }
            after = out.next_after;
        }
    }

    /// Re-read one record from the source and verify its digest.
    pub async fn resolve(
        &mut self,
        key: &str,
        loc: SourceLocator,
        digest: Digest,
    ) -> Result<Arc<ResolvedRecord>, ResolveFail> {
        if let Some((k, r)) = &self.last
            && k == key
        {
            return Ok(r.clone());
        }
        self.check_time().map_err(ResolveFail::Fatal)?;
        if self.bytes_read.saturating_add(loc.len) > self.max_read_bytes {
            return Err(ResolveFail::Fatal(resource_limit(
                "max_source_read_bytes",
                "the local source-read budget was exceeded",
                "narrow_selection",
            )));
        }
        let file = self.ensure_open().await?;
        let (adapter, max) = (self.adapter.clone(), self.max_record_bytes);
        enum Out {
            Ok(Box<ResolvedRecord>),
            Gone,
            Err(AdapterError),
        }
        let out = tokio::task::spawn_blocking(move || {
            match file.metadata() {
                Ok(m) if m.len() < loc.offset.saturating_add(loc.len) => return Out::Gone,
                Err(e) => return Out::Err(AdapterError::Io(e)),
                _ => {}
            }
            let mut f: &File = &file;
            match adapter.resolve_locator(&mut f, &loc, &digest, max) {
                Ok(r) => Out::Ok(Box::new(r)),
                Err(e) => Out::Err(e),
            }
        })
        .await
        .map_err(|_| ResolveFail::Unavailable)?;
        match out {
            Out::Ok(r) => {
                self.bytes_read += loc.len;
                let r = Arc::new(*r);
                self.last = Some((key.to_string(), r.clone()));
                Ok(r)
            }
            Out::Gone | Out::Err(AdapterError::LocatorMismatch(_)) => {
                Err(ResolveFail::Fatal(source_changed()))
            }
            Out::Err(AdapterError::Unsupported(_)) => Err(ResolveFail::Unsupported),
            Out::Err(AdapterError::LimitExceeded) => Err(ResolveFail::Fatal(resource_limit(
                "max_record_bytes",
                "a record exceeds the configured record size limit",
                "change_request",
            ))),
            Out::Err(_) => Err(ResolveFail::Unavailable),
        }
    }

    /// Is `snap` (a published snapshot) still a faithful observation of the current source?
    /// `Stale` => stage a fresh observation; `Unavailable` => the source cannot be checked.
    pub async fn observe(&mut self, snap: &Snap) -> Observation {
        let Ok(file) = self.ensure_open().await else {
            return Observation::Unavailable;
        };
        if let Some(info) = &self.info {
            // size:mtime:dev:ino identical to the observation => nothing changed, no record reads.
            if crate::ingest::stage::revision4(info) == snap.revision {
                return Observation::Current;
            }
            if let (Some(sd), Some(si)) = (snap.file_dev, snap.file_ino)
                && (info.dev as i64 != sd || info.ino as i64 != si)
            {
                return Observation::Stale;
            }
        }
        let prefix_end = snap.prefix_end.max(0) as u64;
        let Ok(len) = file.metadata().map(|m| m.len()) else {
            return Observation::Unavailable;
        };
        if len < prefix_end {
            return Observation::Stale;
        }
        for r in [snap.recs.first(), snap.recs.last()].into_iter().flatten() {
            match self
                .resolve(
                    &r.key,
                    SourceLocator {
                        offset: r.offset,
                        len: r.len,
                    },
                    r.digest,
                )
                .await
            {
                Ok(_) => {}
                Err(ResolveFail::Fatal(e)) if e.code == "SOURCE_CHANGED" => {
                    return Observation::Stale;
                }
                Err(ResolveFail::Fatal(_)) => return Observation::Unavailable,
                Err(_) => return Observation::Unavailable,
            }
        }
        if len > prefix_end {
            // ponytail: assumes line-oriented sources (all three adapters): a complete new record
            // exists iff the unparsed tail holds a newline. A format without that property would
            // need an adapter hook here.
            let tail = len - prefix_end;
            if tail > TAIL_SCAN_CAP {
                return Observation::Stale;
            }
            let res = tokio::task::spawn_blocking(move || {
                let mut buf = vec![0u8; tail as usize];
                file.read_exact_at(&mut buf, prefix_end)
                    .map(|_| buf.contains(&b'\n'))
            })
            .await;
            return match res {
                Ok(Ok(true)) => Observation::Stale,
                Ok(Ok(false)) => Observation::Current,
                _ => Observation::Unavailable,
            };
        }
        Observation::Current
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Observation {
    Current,
    Stale,
    Unavailable,
}
