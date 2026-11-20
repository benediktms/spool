//! Session-source operations used by discovery and source access.

use super::{
    ObservationChange, OpenedSource, ScanError, ScanOutcome, SourceError, SourceFileInfo,
    SourceRoots, compare_observation,
};
use crate::adapters::{AdapterError, HarnessAdapter, ParseOptions, RecordSink};
use crate::domain::{
    Digest, ParsedSessionManifest, RecordMeta, ResolvedRecord, SourceLocator, digest,
};
use std::fs::File;
use std::io::{BufReader, Read};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

const TAIL_SCAN_CAP: u64 = 1 << 20;

#[derive(Debug, PartialEq, Eq)]
pub enum Observation {
    Current,
    Stale,
    Unavailable,
}

pub(crate) struct ObservationCheck {
    prefix_end: u64,
    len: u64,
}

pub(crate) enum ObservationProbe {
    Final(Observation),
    Verify(ObservationCheck),
}

#[derive(Clone)]
pub(crate) struct SourceHandle {
    file: Arc<File>,
    pub(crate) info: SourceFileInfo,
}

impl From<OpenedSource> for SourceHandle {
    fn from(source: OpenedSource) -> Self {
        Self {
            file: Arc::new(source.file),
            info: source.info,
        }
    }
}

impl SourceHandle {
    pub(crate) fn len(&self) -> std::io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    pub(crate) fn begin_observation(
        &self,
        revision: &str,
        dev: Option<i64>,
        ino: Option<i64>,
        prefix_end: i64,
    ) -> ObservationProbe {
        if super::revision4(&self.info) == revision {
            return ObservationProbe::Final(Observation::Current);
        }
        if let (Some(dev), Some(ino)) = (dev, ino)
            && (self.info.dev as i64 != dev || self.info.ino as i64 != ino)
        {
            return ObservationProbe::Final(Observation::Stale);
        }
        let prefix_end = prefix_end.max(0) as u64;
        let Ok(len) = self.len() else {
            return ObservationProbe::Final(Observation::Unavailable);
        };
        if len < prefix_end {
            return ObservationProbe::Final(Observation::Stale);
        }
        ObservationProbe::Verify(ObservationCheck { prefix_end, len })
    }

    pub(crate) fn finish_observation(&self, check: ObservationCheck) -> Observation {
        if check.len <= check.prefix_end {
            return Observation::Current;
        }
        let tail = check.len - check.prefix_end;
        if tail > TAIL_SCAN_CAP {
            return Observation::Stale;
        }
        let mut buf = vec![0; tail as usize];
        match self.file.read_exact_at(&mut buf, check.prefix_end) {
            Ok(()) if buf.contains(&b'\n') => Observation::Stale,
            Ok(()) => Observation::Current,
            Err(_) => Observation::Unavailable,
        }
    }
}

pub(crate) trait SessionSource: Send + Sync {
    fn scan_from(
        &self,
        cancel: &AtomicBool,
        max_entries: usize,
        after: Option<&Path>,
    ) -> ScanOutcome;

    fn open(&self, path: &Path) -> Result<OpenedSource, SourceError>;

    fn stream(
        &self,
        adapter: &dyn HarnessAdapter,
        source: OpenedSource,
        options: &ParseOptions,
        sink: &mut dyn RecordSink,
    ) -> Result<ParsedSessionManifest, AdapterError>;

    fn resolve(
        &self,
        adapter: &dyn HarnessAdapter,
        handle: &SourceHandle,
        locator: &SourceLocator,
        expected: &Digest,
        max_record_bytes: u64,
    ) -> Result<ResolvedRecord, AdapterError>;

    fn verify_observation(
        &self,
        observed: &SourceFileInfo,
        current: &OpenedSource,
        records: &[RecordMeta],
    ) -> ObservationChange;
}

pub(crate) struct JsonlSessionSource {
    roots: Vec<PathBuf>,
}

impl JsonlSessionSource {
    pub(crate) fn new(roots: Vec<PathBuf>) -> Self {
        Self { roots }
    }
}

impl SessionSource for JsonlSessionSource {
    fn scan_from(
        &self,
        cancel: &AtomicBool,
        max_entries: usize,
        after: Option<&Path>,
    ) -> ScanOutcome {
        match SourceRoots::new(&self.roots) {
            Ok(roots) => roots.scan_from(cancel, max_entries, after),
            Err(error) => ScanOutcome {
                complete: false,
                errors: self
                    .roots
                    .iter()
                    .cloned()
                    .map(|path| ScanError {
                        path,
                        kind: error.kind(),
                    })
                    .collect(),
                ..Default::default()
            },
        }
    }

    fn open(&self, path: &Path) -> Result<OpenedSource, SourceError> {
        SourceRoots::new(&self.roots)?.open(path)
    }

    fn stream(
        &self,
        adapter: &dyn HarnessAdapter,
        source: OpenedSource,
        options: &ParseOptions,
        sink: &mut dyn RecordSink,
    ) -> Result<ParsedSessionManifest, AdapterError> {
        let mut reader = BufReader::with_capacity(256 << 10, source.file.take(options.observe_len));
        adapter.parse_snapshot(&mut reader, options, sink)
    }

    fn resolve(
        &self,
        adapter: &dyn HarnessAdapter,
        handle: &SourceHandle,
        locator: &SourceLocator,
        expected: &Digest,
        max_record_bytes: u64,
    ) -> Result<ResolvedRecord, AdapterError> {
        if handle.len()? < locator.offset.saturating_add(locator.len) {
            return Err(AdapterError::LocatorMismatch("record truncated".into()));
        }
        let mut reader: &File = &handle.file;
        adapter.resolve_locator(&mut reader, locator, expected, max_record_bytes)
    }

    fn verify_observation(
        &self,
        observed: &SourceFileInfo,
        current: &OpenedSource,
        records: &[RecordMeta],
    ) -> ObservationChange {
        let change = compare_observation(observed, &current.info);
        if change == ObservationChange::Grew && !records.is_empty() {
            let picks = [0, records.len() / 2, records.len() - 1];
            let verified = picks.iter().all(|&i| {
                let record = &records[i];
                let mut buf = vec![0u8; record.locator.len as usize];
                current
                    .file
                    .read_exact_at(&mut buf, record.locator.offset)
                    .is_ok()
                    && digest(&buf) == record.digest
            });
            if !verified {
                return ObservationChange::Changed;
            }
        }
        change
    }
}
