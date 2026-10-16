//! Discovery and initial coverage (spec 7). Discovery only maintains the catalog (`sources`,
//! `sessions`, `source_files`); turning catalog state into Apalis tasks is reconciliation's job
//! (`jobs::scheduler`), so a crash between the two windows is repaired by the next pass.
//!
//! Rules enforced here:
//! * bootstrap cutoff = `bootstrap_started_at - bootstrap_days`, persisted once per enablement;
//! * after bootstrap the persisted watermark (minus overlap) is the cutoff, even after long downtime;
//! * unseen files older than the cutoff are never auto-indexed (and are not even opened);
//! * known sessions with an active snapshot are change-checked regardless of age;
//! * only a COMPLETE, error-free scan advances the watermark or marks files missing;
//! * a verified move (same inode, or same native id whose old path is gone) updates the path;
//! * exclusions are applied from the `exclusions` table on every upsert.

use crate::clock::SharedClock;
use crate::config::Config;
use crate::domain::*;
use crate::source::{ScanOutcome, SourceFileInfo};
use crate::sources::{SourceEntry, SourceRegistry};
use crate::storage::catalog::{self, DAY_MS, KnownFile, SourceRow};
use crate::storage::index::Index;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

mod page;

/// Header probe window: enough for the session header and subagent classification (adapters'
/// recommendation); the rest of the file is read by ingestion only.
pub const PROBE_BYTES: u64 = 64 * 1024;
pub const DEFAULT_PAGE: usize = 500;

#[derive(Debug, Clone, PartialEq)]
pub enum Selection {
    /// Periodic/startup discovery: the watermark rules decide.
    Auto,
    /// Explicit: sessions with activity at/after this instant (unix ms).
    Since(i64),
    /// Explicit: every session in the source. Never inferred.
    All,
}

#[derive(Debug, Clone)]
pub enum Mode {
    /// Auto: no operation, queue nothing here; sessions become `pending` and reconciliation submits.
    Auto,
    /// Explicit enqueue for operation `op`.
    Enqueue(String),
    /// Dry-run: catalog candidates inertly (`backfill`) so they can be estimated; nothing is queued.
    CatalogOnly,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct Report {
    pub source_id: String,
    pub complete: bool,
    pub coalesced: bool,
    pub finished: bool,
    pub scanned: usize,
    pub new_sessions: usize,
    pub changed: usize,
    pub queued: usize,
    pub already_current: usize,
    pub skipped_old: usize,
    pub skipped_excluded: usize,
    pub skipped_subagent: usize,
    pub debounced: usize,
    pub moved: usize,
    pub probe_failed: usize,
    pub newly_missing: u64,
    pub purged: u64,
    pub errors: usize,
    pub watermark: Option<i64>,
    pub next_after: Option<String>,
    /// Candidate session ids (dry-run estimation), bounded.
    #[serde(skip)]
    pub candidates: Vec<String>,
    pub error: Option<String>,
}

pub struct DiscoveryService {
    index: Index,
    sources: Arc<SourceRegistry>,
    cfg: Arc<Config>,
    clock: SharedClock,
    locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    cpu: Arc<Semaphore>,
    pub page_size: usize,
}

impl DiscoveryService {
    pub fn new(
        index: Index,
        sources: Arc<SourceRegistry>,
        cfg: Arc<Config>,
        clock: SharedClock,
    ) -> Self {
        Self {
            index,
            sources,
            cfg,
            clock,
            locks: Mutex::new(HashMap::new()),
            cpu: Arc::new(Semaphore::new(1)),
            page_size: DEFAULT_PAGE,
        }
    }

    fn lock_for(&self, source_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.locks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(source_id.to_string())
            .or_default()
            .clone()
    }

    /// Periodic/startup discovery of one source. At most one discovery per source runs at a time;
    /// an equivalent request that finds one running is coalesced into it.
    pub async fn run_auto(
        &self,
        source_id: &str,
        cancel: Arc<AtomicBool>,
    ) -> anyhow::Result<Report> {
        let lock = self.lock_for(source_id);
        let Ok(_guard) = lock.try_lock() else {
            return Ok(Report {
                source_id: source_id.into(),
                coalesced: true,
                ..Default::default()
            });
        };
        self.run(
            source_id,
            Selection::Auto,
            Mode::Auto,
            None,
            usize::MAX,
            cancel,
        )
        .await
    }

    /// Explicit enumeration (`index --since/--all`, dry-run). Waits for a running discovery of the
    /// same source. `after` resumes a durable cursor; at most `budget` sessions are queued per call.
    pub async fn run_explicit(
        &self,
        source_id: &str,
        sel: Selection,
        mode: Mode,
        after: Option<PathBuf>,
        budget: usize,
        cancel: Arc<AtomicBool>,
    ) -> anyhow::Result<Report> {
        anyhow::ensure!(
            sel != Selection::Auto,
            "explicit discovery needs a selection"
        );
        let lock = self.lock_for(source_id);
        let _guard = lock.lock().await;
        self.run(source_id, sel, mode, after, budget, cancel).await
    }

    async fn run(
        &self,
        source_id: &str,
        sel: Selection,
        mode: Mode,
        mut after: Option<PathBuf>,
        budget: usize,
        cancel: Arc<AtomicBool>,
    ) -> anyhow::Result<Report> {
        let mut rep = Report {
            source_id: source_id.into(),
            ..Default::default()
        };
        let Some(entry) = self.sources.entry(source_id).filter(|e| e.enabled) else {
            rep.error = Some("source is not enabled".into());
            return Ok(rep);
        };
        let auto = matches!(sel, Selection::Auto);
        let scan_started = self.clock.now_ms();
        let src = self.scan_source(source_id, auto, scan_started).await?;
        let mut ctx = self.page_ctx(&src, &entry, &sel, &mode, scan_started, &cancel);
        let mut state = PageState {
            debounce_floor: None,
            queued: 0,
            budget,
            stopped: false,
            cursor: None,
        };
        let mut any_errors = false;
        let mut completed = false;
        loop {
            let page = self.scan_page(source_id, &cancel, after.clone()).await?;
            let Some(page) = page else { break };
            if !page.errors.is_empty() {
                any_errors = true;
                rep.errors += page.errors.len();
            }
            rep.scanned += page.files.len();
            ctx.now = self.clock.now_ms();
            self.process_page(&ctx, &page.files, after.clone(), &mut state, &mut rep)
                .await?;
            if state.stopped {
                rep.next_after = state
                    .cursor
                    .take()
                    .map(|p| p.to_string_lossy().into_owned());
                break;
            }
            if page.complete {
                completed = true;
                break;
            }
            if page.truncated && page.next_after.is_some() && !cancel.load(Ordering::Relaxed) {
                after = page.next_after;
                continue;
            }
            break;
        }
        rep.queued = state.queued;
        rep.finished = completed && !any_errors;
        rep.complete = rep.finished;
        if auto {
            self.finish_auto(&mut rep, source_id, scan_started, &state, &cancel)
                .await?;
        }
        Ok(rep)
    }

    async fn scan_source(
        &self,
        source_id: &str,
        auto: bool,
        scan_started: i64,
    ) -> anyhow::Result<SourceRow> {
        Ok(if auto {
            catalog::begin_bootstrap(
                &self.index,
                source_id,
                scan_started,
                self.cfg.discovery.bootstrap_days,
            )
            .await?
        } else {
            catalog::source(&self.index, source_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("source row missing"))?
        })
    }

    fn page_ctx<'a>(
        &self,
        src: &'a SourceRow,
        entry: &SourceEntry,
        sel: &'a Selection,
        mode: &'a Mode,
        scan_started: i64,
        cancel: &'a Arc<AtomicBool>,
    ) -> PageCtx<'a> {
        let auto = matches!(sel, Selection::Auto);
        let overlap = self.cfg.discovery.watermark_overlap_seconds as i64 * 1000;
        let cutoff = src.discovery_watermark.map(|w| w - overlap).unwrap_or(
            src.bootstrap_cutoff
                .unwrap_or(scan_started - self.cfg.discovery.bootstrap_days as i64 * DAY_MS),
        );
        let eligibility = if src.discovery_watermark.is_none() {
            "bootstrap"
        } else {
            "active"
        };
        // An explicit request is consent to index what is on disk now; the engine handles a file that
        // keeps growing (follow-up). Only periodic discovery waits for files to settle.
        let debounce = if auto {
            self.cfg.discovery.debounce_seconds as i64 * 1000
        } else {
            0
        };
        PageCtx {
            source: src,
            kind: entry.kind,
            include_subagents: entry.include_subagents,
            sel,
            mode,
            cancel,
            cutoff,
            eligibility,
            debounce,
            scan_started,
            now: scan_started,
        }
    }

    async fn scan_page(
        &self,
        source_id: &str,
        cancel: &Arc<AtomicBool>,
        after: Option<PathBuf>,
    ) -> anyhow::Result<Option<ScanOutcome>> {
        let reg = self.sources.clone();
        let id = source_id.to_string();
        let c = cancel.clone();
        let n = self.page_size;
        let permit = self.cpu.clone().acquire_owned().await?;
        Ok(tokio::task::spawn_blocking(move || {
            let _p = permit;
            reg.scan_from(&id, &c, n, after.as_deref())
        })
        .await?)
    }

    async fn finish_auto(
        &self,
        rep: &mut Report,
        source_id: &str,
        scan_started: i64,
        state: &PageState,
        cancel: &AtomicBool,
    ) -> anyhow::Result<()> {
        let now = self.clock.now_ms();
        if rep.complete && !cancel.load(Ordering::Relaxed) {
            let (marked, purged) =
                catalog::reconcile_missing(&self.index, source_id, scan_started, now).await?;
            rep.newly_missing = marked;
            rep.purged = purged;
            let wm = state
                .debounce_floor
                .map_or(scan_started, |f| f.min(scan_started));
            rep.watermark = Some(wm);
            catalog::record_scan(&self.index, source_id, now, None, Some(wm)).await?;
        } else {
            // Incomplete: NEVER advance the watermark or tombstone anything (spec 7.3).
            rep.complete = false;
            let why = if cancel.load(Ordering::Relaxed) {
                "cancelled".to_string()
            } else {
                format!("incomplete scan, {} errors", rep.errors)
            };
            rep.error = Some(why.clone());
            catalog::record_scan(&self.index, source_id, now, Some(&why), None).await?;
        }
        let summary = serde_json::to_string(rep)?;
        catalog::app_meta_set(
            &self.index,
            &format!("discovery.last.{source_id}"),
            &summary,
        )
        .await?;
        Ok(())
    }
}

struct PageCtx<'a> {
    source: &'a SourceRow,
    kind: HarnessKind,
    include_subagents: bool,
    sel: &'a Selection,
    mode: &'a Mode,
    cancel: &'a Arc<AtomicBool>,
    cutoff: i64,
    eligibility: &'static str,
    debounce: i64,
    scan_started: i64,
    /// Clock reading taken when this page was read; stamps every catalog write for the page.
    now: i64,
}

impl PageCtx<'_> {
    /// Still inside the debounce window: the file may be mid-write.
    fn is_recent(&self, f: &SourceFileInfo) -> bool {
        self.now - f.mtime_ms < self.debounce
    }

    fn needs_probe(
        &self,
        f: &SourceFileInfo,
        known: &HashMap<String, KnownFile>,
        renamed: &HashSet<String>,
    ) -> bool {
        let key = f.path.to_string_lossy();
        if known.contains_key(key.as_ref()) {
            return false;
        }
        if self.is_recent(f) {
            return false;
        }
        if renamed.contains(key.as_ref()) {
            return true;
        }
        match self.sel {
            Selection::Auto => f.mtime_ms >= self.cutoff,
            Selection::Since(s) => f.mtime_ms >= *s,
            Selection::All => true,
        }
    }
}

struct PageState {
    /// Lowest mtime of a debounced file: the watermark must not pass it.
    debounce_floor: Option<i64>,
    queued: usize,
    budget: usize,
    stopped: bool,
    cursor: Option<PathBuf>,
}

impl PageState {
    fn floor(&mut self, mtime: i64) {
        self.debounce_floor = Some(self.debounce_floor.map_or(mtime, |f| f.min(mtime)));
    }
}
