//! Source access for the reader: safe open through `SourceRegistry`, rename re-resolution by file
//! identity, per-record `resolve_locator` with digest verification, and the local I/O / time
//! budgets (spec 14.2.5, 14.2.7.2). A positively detected mismatch is never reported as
//! "unavailable"; only the caller's policy decides whether unavailable may fall back.

use super::selection::{Rec, RecordLocator, Snap, app_err};
use crate::adapters::{AdapterError, HarnessAdapter};
use crate::domain::{Digest, ResolvedRecord, SourceLocator};
use crate::ipc::AppError;
use crate::source::SourceError;
use crate::source::opencode::{OpenCodeError, ProjectedMessage, ScanControl, lookup_message};
pub use crate::source::session::Observation;
use crate::source::session::{ObservationProbe, SourceHandle};
use crate::sources::SourceRegistry;
use crate::storage::index::Index;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

/// Upper bound on directory entries visited while re-locating a renamed source.
const MAX_RELOCATE_ENTRIES: usize = 20_000;
const RELOCATE_PAGE: usize = 1_000;

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
    source: ReadSource,
    file_dev: Option<i64>,
    file_ino: Option<i64>,
    max_record_bytes: u64,
    max_read_bytes: u64,
    deadline: Instant,
    handle: Option<SourceHandle>,
    bytes_read: u64,
    last: Option<(String, Arc<ResolvedRecord>)>,
}

#[derive(Clone)]
pub(crate) enum ReadSource {
    Jsonl {
        path: Option<PathBuf>,
        adapter: &'static dyn HarnessAdapter,
    },
    OpenCodeSqlite {
        index: Index,
        database: PathBuf,
        native_session: String,
    },
}

impl SourceCtx {
    pub(crate) fn new(
        sources: Arc<SourceRegistry>,
        source_id: String,
        source: ReadSource,
        snap: &Snap,
        limits: SourceLimits,
    ) -> Self {
        Self {
            sources,
            source_id,
            source,
            file_dev: snap.file_dev,
            file_ino: snap.file_ino,
            max_record_bytes: limits.max_record_bytes,
            max_read_bytes: limits.max_read_bytes,
            deadline: Instant::now() + std::time::Duration::from_millis(limits.max_read_millis),
            handle: None,
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

    async fn open_path(&self, path: PathBuf) -> Result<Option<SourceHandle>, ResolveFail> {
        let (sources, sid) = (self.sources.clone(), self.source_id.clone());
        match tokio::task::spawn_blocking(move || sources.open(&sid, &path)).await {
            Ok(Ok(o)) => Ok(Some(o.into())),
            Ok(Err(SourceError::Io(e))) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            _ => Err(ResolveFail::Unavailable),
        }
    }

    /// Open the source once per call. A missing path is re-resolved through file identity
    /// (dev/ino) with a bounded scan; digests are still verified per record afterwards.
    async fn ensure_open(&mut self) -> Result<SourceHandle, ResolveFail> {
        if let Some(f) = &self.handle {
            return Ok(f.clone());
        }
        let ReadSource::Jsonl { path, .. } = &self.source else {
            return Err(ResolveFail::Unavailable);
        };
        let path = path.clone().ok_or(ResolveFail::Unavailable)?;
        let mut found = self.open_path(path).await?;
        if found.is_none() {
            found = self.relocate().await?;
        }
        let handle = found.ok_or(ResolveFail::Unavailable)?;
        self.handle = Some(handle.clone());
        Ok(handle)
    }

    async fn relocate(&mut self) -> Result<Option<SourceHandle>, ResolveFail> {
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
    pub(crate) async fn resolve(&mut self, rec: &Rec) -> Result<Arc<ResolvedRecord>, ResolveFail> {
        if let Some((key, record)) = &self.last
            && key == &rec.key
        {
            return Ok(record.clone());
        }
        self.check_time().map_err(ResolveFail::Fatal)?;
        let (record, bytes) = match (&self.source, &rec.locator) {
            (ReadSource::Jsonl { adapter, .. }, RecordLocator::Jsonl(loc)) => (
                self.resolve_jsonl(*adapter, *loc, rec.digest).await?,
                loc.len,
            ),
            (
                ReadSource::OpenCodeSqlite {
                    index,
                    database,
                    native_session,
                },
                RecordLocator::OpenCodeSqlite {
                    session_id,
                    message_id,
                },
            ) if session_id == native_session => {
                self.resolve_native(index, database, session_id, message_id, rec.digest)
                    .await?
            }
            _ => return Err(ResolveFail::Fatal(source_changed())),
        };
        self.bytes_read += bytes;
        let record = Arc::new(record);
        self.last = Some((rec.key.clone(), record.clone()));
        Ok(record)
    }

    async fn resolve_jsonl(
        &mut self,
        adapter: &'static dyn HarnessAdapter,
        loc: SourceLocator,
        digest: Digest,
    ) -> Result<ResolvedRecord, ResolveFail> {
        if loc.len > self.max_read_bytes - self.bytes_read {
            return Err(read_bytes_limit());
        }
        let file = self.ensure_open().await?;
        let max = self.max_record_bytes;
        let source = self
            .sources
            .session_source(&self.source_id)
            .ok_or(ResolveFail::Unavailable)?;
        let out =
            tokio::task::spawn_blocking(move || source.resolve(adapter, &file, &loc, &digest, max))
                .await
                .map_err(|_| ResolveFail::Unavailable)?;
        match out {
            Ok(r) => Ok(r),
            Err(AdapterError::LocatorMismatch(_)) => Err(ResolveFail::Fatal(source_changed())),
            Err(AdapterError::Unsupported(_)) => Err(ResolveFail::Unsupported),
            Err(AdapterError::LimitExceeded) => Err(record_bytes_limit()),
            Err(_) => Err(ResolveFail::Unavailable),
        }
    }

    async fn resolve_native(
        &self,
        index: &Index,
        database: &std::path::Path,
        session_id: &str,
        message_id: &str,
        digest: Digest,
    ) -> Result<(ResolvedRecord, u64), ResolveFail> {
        let remaining = self.max_read_bytes - self.bytes_read;
        let max = remaining.min(self.max_record_bytes);
        let max = usize::try_from(max.min(i64::MAX as u64)).map_err(|_| record_bytes_limit())?;
        let cancel = AtomicBool::new(false);
        let control = ScanControl {
            cancel: &cancel,
            deadline: Some(self.deadline),
        };
        let lookup = lookup_message(
            index,
            &self.source_id,
            database,
            session_id,
            message_id,
            max,
            &control,
        );
        let message = tokio::time::timeout_at(self.deadline.into(), lookup)
            .await
            .map_err(|_| time_limit())
            .and_then(|result| {
                result.map_err(|e| native_error(e, remaining < self.max_record_bytes))
            })?;
        let bytes = message.as_ref().map_or(0, |message| message.source_bytes);
        let record =
            ProjectedMessage::verify(message, &digest).map_err(|e| native_error(e, false))?;
        Ok((record, bytes as u64))
    }

    /// Is `snap` (a published snapshot) still a faithful observation of the current source?
    /// `Stale` => stage a fresh observation; `Unavailable` => the source cannot be checked.
    pub async fn observe(&mut self, snap: &Snap) -> Observation {
        let Ok(file) = self.ensure_open().await else {
            return Observation::Unavailable;
        };
        let check = match file.begin_observation(
            &snap.revision,
            snap.file_dev,
            snap.file_ino,
            snap.prefix_end,
        ) {
            ObservationProbe::Final(outcome) => return outcome,
            ObservationProbe::Verify(check) => check,
        };
        for r in [snap.recs.first(), snap.recs.last()].into_iter().flatten() {
            if !matches!(r.locator, RecordLocator::Jsonl(_)) {
                return Observation::Unavailable;
            }
            match self.resolve(r).await {
                Ok(_) => {}
                Err(ResolveFail::Fatal(e)) if e.code == "SOURCE_CHANGED" => {
                    return Observation::Stale;
                }
                Err(ResolveFail::Fatal(_)) => return Observation::Unavailable,
                Err(_) => return Observation::Unavailable,
            }
        }
        tokio::task::spawn_blocking(move || file.finish_observation(check))
            .await
            .unwrap_or(Observation::Unavailable)
    }
}

fn native_error(error: OpenCodeError, page_limited: bool) -> ResolveFail {
    match error {
        OpenCodeError::Moved | OpenCodeError::Changed => ResolveFail::Fatal(source_changed()),
        OpenCodeError::UnsafeSource | OpenCodeError::Unavailable(_) => ResolveFail::Unavailable,
        OpenCodeError::Unsupported => ResolveFail::Fatal(app_err(
            "UNSUPPORTED_FORMAT",
            "the source uses unsupported OpenCode storage",
            false,
            "change_request",
        )),
        OpenCodeError::Limit if page_limited => read_bytes_limit(),
        OpenCodeError::Limit => record_bytes_limit(),
        OpenCodeError::Cancelled => time_limit(),
    }
}

fn read_bytes_limit() -> ResolveFail {
    ResolveFail::Fatal(resource_limit(
        "max_source_read_bytes",
        "the local source-read budget was exceeded",
        "narrow_selection",
    ))
}

fn record_bytes_limit() -> ResolveFail {
    ResolveFail::Fatal(resource_limit(
        "max_record_bytes",
        "a record exceeds the configured record size limit",
        "change_request",
    ))
}

fn time_limit() -> ResolveFail {
    ResolveFail::Fatal(resource_limit(
        "max_read_millis",
        "the local read time budget was exceeded",
        "narrow_selection",
    ))
}
