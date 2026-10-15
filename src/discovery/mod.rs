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

use crate::adapters::{ParseOptions, RecordSink, SinkError, adapter_for};
use crate::clock::SharedClock;
use crate::config::Config;
use crate::domain::*;
use crate::ids;
use crate::source::SourceFileInfo;
use crate::sources::SourceRegistry;
use crate::storage::catalog::{self, DAY_MS, KnownFile, NewSession, SourceRow};
use crate::storage::index::Index;
use serde::Serialize;
use std::collections::HashMap;
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::Semaphore;

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

struct Probe {
    native_id: String,
    kind: SessionKind,
    cwd: Option<String>,
    activity: Option<i64>,
}

struct Discard<'a>(&'a AtomicBool);
impl RecordSink for Discard<'_> {
    fn record(&mut self, _: RecordMeta) -> Result<(), SinkError> {
        Ok(())
    }
    fn message(&mut self, _: MessageEntry) -> Result<(), SinkError> {
        Ok(())
    }
    fn tool_call(&mut self, _: ToolCallMeta) -> Result<(), SinkError> {
        Ok(())
    }
    fn tool_result(&mut self, _: ToolResultMeta) -> Result<(), SinkError> {
        Ok(())
    }
    fn warning(&mut self, _: ParseWarning) -> Result<(), SinkError> {
        Ok(())
    }
    fn cancelled(&self) -> &AtomicBool {
        self.0
    }
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

fn fallback_native(path: &std::path::Path) -> String {
    format!(
        "unprobed:{}",
        &ids::hex(&digest(path.to_string_lossy().as_bytes()))[..16]
    )
}

fn path_discriminator(path: &std::path::Path) -> String {
    ids::hex(&digest(path.to_string_lossy().as_bytes()))[..12].to_string()
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
        let src = if auto {
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
        };
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
            let reg = self.sources.clone();
            let id = source_id.to_string();
            let c = cancel.clone();
            let a = after.clone();
            let n = self.page_size;
            let permit = self.cpu.clone().acquire_owned().await?;
            let page = tokio::task::spawn_blocking(move || {
                let _p = permit;
                reg.scan_from(&id, &c, n, a.as_deref())
            })
            .await?;
            let Some(page) = page else { break };
            if !page.errors.is_empty() {
                any_errors = true;
                rep.errors += page.errors.len();
            }
            rep.scanned += page.files.len();
            let ctx = PageCtx {
                source: &src,
                kind: entry.kind,
                include_subagents: entry.include_subagents,
                sel: &sel,
                mode: &mode,
                cutoff,
                eligibility,
                debounce,
                scan_started,
                now: self.clock.now_ms(),
            };
            let page_after = after.clone();
            self.process_page(&ctx, page.files, page_after, &mut state, &mut rep, &cancel)
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

    async fn process_page(
        &self,
        ctx: &PageCtx<'_>,
        files: Vec<SourceFileInfo>,
        page_after: Option<PathBuf>,
        st: &mut PageState,
        rep: &mut Report,
        cancel: &Arc<AtomicBool>,
    ) -> anyhow::Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        let source_id = ctx.source.id.as_str();
        let paths: Vec<String> = files
            .iter()
            .map(|f| f.path.to_string_lossy().into_owned())
            .collect();
        let known: HashMap<String, KnownFile> = {
            let mut conn = self.index.reader.acquire().await?;
            catalog::known_by_paths(&mut conn, source_id, &paths)
                .await?
                .into_iter()
                .map(|k| (k.path.clone(), k))
                .collect()
        };
        // An unseen path may be a renamed known file even when it is older than the cutoff: identity
        // lookups are indexed and cheap, so check every unknown path (no probe needed to ask).
        let mut ident_hits: std::collections::HashSet<String> = std::collections::HashSet::new();
        {
            let mut conn = self.index.reader.acquire().await?;
            for f in &files {
                let key = f.path.to_string_lossy();
                if known.contains_key(key.as_ref()) {
                    continue;
                }
                if catalog::known_by_identity(&mut conn, source_id, f.dev, f.ino)
                    .await?
                    .is_some_and(|k| k.path != key)
                {
                    ident_hits.insert(key.into_owned());
                }
            }
        }
        // Which unseen files must be opened for identity? Auto skips files older than the cutoff
        // without touching them (an old unseen file is never auto-indexed, so no probe cost).
        let needs_probe = |f: &SourceFileInfo| -> bool {
            let key = f.path.to_string_lossy();
            if known.contains_key(key.as_ref()) {
                return false;
            }
            if ctx.is_recent(f) {
                return false;
            }
            if ident_hits.contains(key.as_ref()) {
                return true;
            }
            match ctx.sel {
                Selection::Auto => f.mtime_ms >= ctx.cutoff,
                Selection::Since(s) => f.mtime_ms >= *s,
                Selection::All => true,
            }
        };
        let to_probe: Vec<SourceFileInfo> =
            files.iter().filter(|f| needs_probe(f)).cloned().collect();
        let probes = self.probe_blocking(ctx, to_probe, cancel.clone()).await?;
        // Identity/old-path context for each probed file, read before the write transaction.
        let mut moves: HashMap<String, MoveInfo> = HashMap::new();
        for (info, probe) in &probes {
            let mut conn = self.index.reader.acquire().await?;
            // Inode reuse guard: an identity match only counts as a move when the native id agrees.
            let by_ident = catalog::known_by_identity(&mut conn, source_id, info.dev, info.ino)
                .await?
                .filter(|k| {
                    k.path != info.path.to_string_lossy()
                        && match probe {
                            Some(p) => p.native_id == k.native_id,
                            None => k.native_id.starts_with("unprobed:"),
                        }
                });
            let by_native = match probe {
                Some(p) => catalog::known_by_native(&mut conn, source_id, &p.native_id).await?,
                None => Vec::new(),
            };
            drop(conn);
            let candidates: Vec<(KnownFile, bool)> = {
                let list = by_native;
                let mut out = Vec::with_capacity(list.len());
                for k in list {
                    let p = PathBuf::from(&k.path);
                    let exists =
                        tokio::task::spawn_blocking(move || std::fs::symlink_metadata(p).is_ok())
                            .await?;
                    out.push((k, exists));
                }
                out
            };
            moves.insert(
                info.path.to_string_lossy().into_owned(),
                MoveInfo {
                    by_ident,
                    by_native: candidates,
                },
            );
        }
        let probe_map: HashMap<String, Option<Probe>> = probes
            .into_iter()
            .map(|(i, p)| (i.path.to_string_lossy().into_owned(), p))
            .collect();

        let mut tx = self.index.writer.begin().await?;
        let mut seen: Vec<String> = Vec::new();
        let mut last_done: Option<PathBuf> = None;
        for f in &files {
            if cancel.load(Ordering::Relaxed) {
                break;
            }
            if st.queued >= st.budget {
                // Resume strictly after the last processed file (or the page's own cursor).
                st.stopped = true;
                st.cursor = last_done.clone().or_else(|| page_after.clone());
                break;
            }
            let key = f.path.to_string_lossy().into_owned();
            let recent = ctx.is_recent(f);
            if let Some(k) = known.get(&key) {
                seen.push(key.clone());
                Self::apply_known(&mut tx, ctx, f, k, st, rep).await?;
            } else if recent {
                // Brand-new file still being written: skip, and keep the watermark below it.
                rep.debounced += 1;
                st.floor(f.mtime_ms);
            } else if let Some(p) = probe_map.get(&key) {
                Self::apply_unknown(&mut tx, ctx, f, p.as_ref(), moves.get(&key), st, rep).await?;
            } else {
                rep.skipped_old += 1;
            }
            last_done = Some(f.path.clone());
        }
        catalog::touch_seen(&mut tx, &seen, ctx.scan_started).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn probe_blocking(
        &self,
        ctx: &PageCtx<'_>,
        files: Vec<SourceFileInfo>,
        cancel: Arc<AtomicBool>,
    ) -> anyhow::Result<Vec<(SourceFileInfo, Option<Probe>)>> {
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let reg = self.sources.clone();
        let source_id = ctx.source.id.clone();
        let kind = ctx.kind;
        let max_record = self.cfg.ingestion.max_record_bytes;
        let permit = self.cpu.clone().acquire_owned().await?;
        Ok(tokio::task::spawn_blocking(move || {
            let _p = permit;
            let adapter = adapter_for(kind);
            let mut out = Vec::with_capacity(files.len());
            for info in files {
                if cancel.load(Ordering::Relaxed) {
                    break;
                }
                let probe = (|| {
                    let opened = reg.open(&source_id, &info.path).ok()?;
                    let opts = ParseOptions {
                        max_record_bytes: max_record,
                        max_source_bytes: PROBE_BYTES,
                        observe_len: PROBE_BYTES,
                        include_subagents: true,
                        blob_roots: Vec::new(),
                    };
                    let mut r = (&opened.file).take(PROBE_BYTES);
                    let mut sink = Discard(&cancel);
                    let m = adapter.parse_snapshot(&mut r, &opts, &mut sink).ok()?;
                    if m.native_session_id.is_empty() {
                        return None;
                    }
                    // Native last activity is "reliable" only when the probe saw the whole file.
                    let whole = m.prefix_end >= opened.info.len;
                    Some(Probe {
                        native_id: m.native_session_id,
                        kind: m.session_kind,
                        cwd: m.cwd,
                        activity: if whole { m.last_activity_at } else { None },
                    })
                })();
                out.push((info, probe));
            }
            out
        })
        .await?)
    }

    async fn apply_known(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        ctx: &PageCtx<'_>,
        f: &SourceFileInfo,
        k: &KnownFile,
        st: &mut PageState,
        rep: &mut Report,
    ) -> anyhow::Result<()> {
        let recent = ctx.is_recent(f);
        let unchanged = k.size == Some(f.len as i64)
            && k.mtime_ms == Some(f.mtime_ms)
            && k.dev == Some(f.dev as i64)
            && k.ino == Some(f.ino as i64);
        if matches!(ctx.mode, Mode::CatalogOnly) {
            // Dry run: never rewrite the stored stat, or a later real run would see a stale file as current.
            return Self::apply_explicit_known(
                tx,
                ctx,
                f,
                k,
                unchanged && k.exists_status == "present",
                st,
                rep,
            )
            .await;
        }
        let revived = k.exists_status != "present";
        if revived {
            sqlx::query("UPDATE sessions SET source_status = 'present', tombstoned_at = NULL, missing_confirmed_at = NULL WHERE id = ?").bind(&k.session_id).execute(&mut **tx).await?;
        }
        if unchanged && !revived {
            return Self::apply_explicit_known(tx, ctx, f, k, true, st, rep).await;
        }
        if !unchanged && recent {
            // Still being written: leave the stored stat, retry next scan, hold the watermark back.
            rep.debounced += 1;
            st.floor(f.mtime_ms);
            return Ok(());
        }
        catalog::upsert_source_file(tx, &k.session_id, f, ctx.now).await?;
        if !unchanged {
            rep.changed += 1;
        }
        if k.excluded {
            rep.skipped_excluded += 1;
            return Ok(());
        }
        if matches!(ctx.mode, Mode::Auto) {
            // Reopened old session: eligible again once recent activity exists (spec 7.1).
            let promote =
                k.eligibility == "backfill" && !k.has_snapshot && f.mtime_ms >= ctx.cutoff;
            if promote {
                catalog::set_eligibility(tx, &k.session_id, ctx.eligibility).await?;
            }
            if k.has_snapshot || promote || k.eligibility != "backfill" {
                catalog::mark_pending(tx, &k.session_id, None).await?;
            }
            return Ok(());
        }
        Self::apply_explicit_known(tx, ctx, f, k, false, st, rep).await
    }

    async fn apply_explicit_known(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        ctx: &PageCtx<'_>,
        f: &SourceFileInfo,
        k: &KnownFile,
        unchanged: bool,
        st: &mut PageState,
        rep: &mut Report,
    ) -> anyhow::Result<()> {
        if matches!(ctx.mode, Mode::Auto) {
            return Ok(());
        }
        let selected = match ctx.sel {
            Selection::Since(s) => f.mtime_ms >= *s,
            _ => true,
        };
        if !selected {
            rep.skipped_old += 1;
            return Ok(());
        }
        if k.excluded {
            rep.skipped_excluded += 1;
            return Ok(());
        }
        if k.session_kind == "subagent" && !ctx.include_subagents {
            rep.skipped_subagent += 1;
            return Ok(());
        }
        if unchanged
            && k.index_state == "indexed"
            && k.has_snapshot
            && !matches!(ctx.mode, Mode::Auto)
        {
            rep.already_current += 1;
            return Ok(());
        }
        match ctx.mode {
            Mode::CatalogOnly => {
                if rep.candidates.len() < 4096 {
                    rep.candidates.push(k.session_id.clone());
                }
                st.queued += 1;
            }
            Mode::Enqueue(op) => {
                catalog::set_eligibility(
                    tx,
                    &k.session_id,
                    if k.eligibility == "backfill" {
                        "manual"
                    } else {
                        k.eligibility.as_str()
                    },
                )
                .await?;
                catalog::mark_pending(tx, &k.session_id, Some(op)).await?;
                st.queued += 1;
            }
            Mode::Auto => {}
        }
        Ok(())
    }

    async fn apply_unknown(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        ctx: &PageCtx<'_>,
        f: &SourceFileInfo,
        probe: Option<&Probe>,
        mv: Option<&MoveInfo>,
        st: &mut PageState,
        rep: &mut Report,
    ) -> anyhow::Result<()> {
        let source_id = ctx.source.id.as_str();
        // 1. verified move by file identity
        if let Some(k) = mv.and_then(|m| m.by_ident.as_ref()) {
            Self::move_to(tx, ctx, k, f, rep).await?;
            return Ok(());
        }
        // 2. same native id whose old path is gone => move
        if let Some(k) = mv.and_then(|m| {
            m.by_native
                .iter()
                .find(|(_, exists)| !exists)
                .map(|(k, _)| k)
        }) {
            Self::move_to(tx, ctx, k, f, rep).await?;
            return Ok(());
        }
        let (native, kind, cwd, activity, failed) = match probe {
            Some(p) => (
                p.native_id.clone(),
                p.kind,
                p.cwd.clone(),
                p.activity.unwrap_or(f.mtime_ms),
                false,
            ),
            None => (
                fallback_native(&f.path),
                SessionKind::Unknown,
                None,
                f.mtime_ms,
                true,
            ),
        };
        if failed {
            rep.probe_failed += 1;
        }
        let eligible = match ctx.sel {
            Selection::Auto => activity >= ctx.cutoff,
            Selection::Since(s) => activity >= *s,
            Selection::All => true,
        };
        // Explicit selections do not catalog non-matching files; auto does (inert `backfill`) so a
        // copied old file is not re-probed on every scan.
        if !eligible && !matches!(ctx.sel, Selection::Auto) {
            rep.skipped_old += 1;
            return Ok(());
        }
        let has_native = mv.is_some_and(|m| !m.by_native.is_empty());
        let disc = if has_native {
            path_discriminator(&f.path)
        } else {
            String::new()
        };
        let label = match (&ctx.mode, eligible) {
            (Mode::Auto, true) => ctx.eligibility,
            (Mode::Auto, false) | (Mode::CatalogOnly, _) => "backfill",
            (Mode::Enqueue(_), _) => "manual",
        };
        let op = if let Mode::Enqueue(op) = &ctx.mode {
            Some(op.as_str())
        } else {
            None
        };
        let (sid, created) = catalog::upsert_session(
            tx,
            &NewSession {
                source_id,
                native_id: &native,
                discriminator: &disc,
                kind,
                cwd: cwd.as_deref(),
                created_at: None,
                last_activity_at: Some(activity),
                eligibility: label,
                operation_id: op,
            },
            ctx.now,
        )
        .await?;
        catalog::upsert_source_file(tx, &sid, f, ctx.now).await?;
        if created {
            rep.new_sessions += 1;
        }
        if failed {
            sqlx::query("UPDATE sessions SET index_state = 'unsupported', index_error = 'probe_failed' WHERE id = ?").bind(&sid).execute(&mut **tx).await?;
        }
        let excluded: i64 = sqlx::query_scalar("SELECT excluded FROM sessions WHERE id = ?")
            .bind(&sid)
            .fetch_one(&mut **tx)
            .await?;
        if excluded != 0 {
            rep.skipped_excluded += 1;
            return Ok(());
        }
        if kind == SessionKind::Subagent && !ctx.include_subagents {
            rep.skipped_subagent += 1;
            return Ok(());
        }
        match &ctx.mode {
            Mode::CatalogOnly => {
                if rep.candidates.len() < 4096 {
                    rep.candidates.push(sid);
                }
                st.queued += 1;
            }
            Mode::Enqueue(_) => {
                if !failed {
                    catalog::mark_pending(tx, &sid, op).await?;
                }
                st.queued += 1;
            }
            Mode::Auto => {}
        }
        Ok(())
    }

    /// Update path/stat of an existing session (rename or verified move) instead of recreating it.
    async fn move_to(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        ctx: &PageCtx<'_>,
        k: &KnownFile,
        f: &SourceFileInfo,
        rep: &mut Report,
    ) -> anyhow::Result<()> {
        let changed = k.size != Some(f.len as i64) || k.mtime_ms != Some(f.mtime_ms);
        catalog::upsert_source_file(tx, &k.session_id, f, ctx.now).await?;
        sqlx::query("UPDATE sessions SET source_status = 'present', tombstoned_at = NULL, missing_confirmed_at = NULL WHERE id = ?").bind(&k.session_id).execute(&mut **tx).await?;
        rep.moved += 1;
        if changed
            && !k.excluded
            && (k.has_snapshot || k.eligibility != "backfill")
            && matches!(ctx.mode, Mode::Auto)
        {
            catalog::mark_pending(tx, &k.session_id, None).await?;
        }
        Ok(())
    }
}

struct MoveInfo {
    by_ident: Option<KnownFile>,
    by_native: Vec<(KnownFile, bool)>,
}

struct PageCtx<'a> {
    source: &'a SourceRow,
    kind: HarnessKind,
    include_subagents: bool,
    sel: &'a Selection,
    mode: &'a Mode,
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
