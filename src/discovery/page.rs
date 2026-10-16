//! Page processing: plan reads (known files, renames, header probes, move context) outside the
//! write transaction, then apply the plan to the catalog inside one transaction.

use super::{DiscoveryService, Mode, PROBE_BYTES, PageCtx, PageState, Report, Selection};
use crate::adapters::{ParseOptions, RecordSink, SinkError, adapter_for};
use crate::domain::{
    MessageEntry, ParseWarning, RecordMeta, SessionKind, ToolCallMeta, ToolResultMeta, digest,
};
use crate::ids;
use crate::source::SourceFileInfo;
use crate::storage::catalog::{self, KnownFile, NewSession};
use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};

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
    pub(super) async fn process_page(
        &self,
        ctx: &PageCtx<'_>,
        files: &[SourceFileInfo],
        page_after: Option<PathBuf>,
        st: &mut PageState,
        rep: &mut Report,
    ) -> anyhow::Result<()> {
        if files.is_empty() {
            return Ok(());
        }
        let plan = self.plan_page(ctx, files).await?;

        let mut tx = self.index.writer.begin().await?;
        let seen = Self::apply_page(&mut tx, ctx, files, &plan, page_after, st, rep).await?;
        catalog::touch_seen(&mut tx, &seen, ctx.scan_started).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn plan_page(
        &self,
        ctx: &PageCtx<'_>,
        files: &[SourceFileInfo],
    ) -> anyhow::Result<PagePlan> {
        let source_id = ctx.source.id.as_str();
        let known = self.known_files(source_id, files).await?;
        let renamed = self.renamed_paths(source_id, files, &known).await?;
        // Which unseen files must be opened for identity? Auto skips files older than the cutoff
        // without touching them (an old unseen file is never auto-indexed, so no probe cost).
        let to_probe: Vec<SourceFileInfo> = files
            .iter()
            .filter(|f| ctx.needs_probe(f, &known, &renamed))
            .cloned()
            .collect();
        let probes = self.probe_blocking(ctx, to_probe).await?;
        // Identity/old-path context for each probed file, read before the write transaction.
        let moves = self.move_info(source_id, &probes).await?;
        let probes: HashMap<String, Option<Probe>> = probes
            .into_iter()
            .map(|(i, p)| (i.path.to_string_lossy().into_owned(), p))
            .collect();
        Ok(PagePlan {
            known,
            probes,
            moves,
        })
    }

    async fn known_files(
        &self,
        source_id: &str,
        files: &[SourceFileInfo],
    ) -> anyhow::Result<HashMap<String, KnownFile>> {
        let paths: Vec<String> = files
            .iter()
            .map(|f| f.path.to_string_lossy().into_owned())
            .collect();
        let mut conn = self.index.reader.acquire().await?;
        Ok(catalog::known_by_paths(&mut conn, source_id, &paths)
            .await?
            .into_iter()
            .map(|k| (k.path.clone(), k))
            .collect())
    }

    /// An unseen path may be a renamed known file even when it is older than the cutoff: identity
    /// lookups are indexed and cheap, so check every unknown path (no probe needed to ask).
    async fn renamed_paths(
        &self,
        source_id: &str,
        files: &[SourceFileInfo],
        known: &HashMap<String, KnownFile>,
    ) -> anyhow::Result<HashSet<String>> {
        let mut ident_hits: HashSet<String> = HashSet::new();
        let mut conn = self.index.reader.acquire().await?;
        for f in files {
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
        Ok(ident_hits)
    }

    async fn move_info(
        &self,
        source_id: &str,
        probes: &[(SourceFileInfo, Option<Probe>)],
    ) -> anyhow::Result<HashMap<String, MoveInfo>> {
        let mut moves: HashMap<String, MoveInfo> = HashMap::new();
        for (info, probe) in probes {
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
        Ok(moves)
    }

    /// Applies the planned page inside the caller's transaction; returns the paths to mark seen.
    async fn apply_page(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        ctx: &PageCtx<'_>,
        files: &[SourceFileInfo],
        plan: &PagePlan,
        mut page_after: Option<PathBuf>,
        st: &mut PageState,
        rep: &mut Report,
    ) -> anyhow::Result<Vec<String>> {
        let mut seen: Vec<String> = Vec::new();
        let mut last_done: Option<PathBuf> = None;
        for f in files {
            if ctx.cancel.load(Ordering::Relaxed) {
                break;
            }
            if st.queued >= st.budget {
                // Resume strictly after the last processed file (or the page's own cursor).
                st.stopped = true;
                st.cursor = last_done.take().or_else(|| page_after.take());
                break;
            }
            let key = f.path.to_string_lossy().into_owned();
            let recent = ctx.is_recent(f);
            if let Some(k) = plan.known.get(&key) {
                seen.push(key.clone());
                Self::apply_known(tx, ctx, f, k, st, rep).await?;
            } else if recent {
                // Brand-new file still being written: skip, and keep the watermark below it.
                rep.debounced += 1;
                st.floor(f.mtime_ms);
            } else if let Some(p) = plan.probes.get(&key) {
                Self::apply_unknown(tx, ctx, f, p.as_ref(), plan.moves.get(&key), st, rep).await?;
            } else {
                rep.skipped_old += 1;
            }
            last_done = Some(f.path.clone());
        }
        Ok(seen)
    }

    async fn probe_blocking(
        &self,
        ctx: &PageCtx<'_>,
        files: Vec<SourceFileInfo>,
    ) -> anyhow::Result<Vec<(SourceFileInfo, Option<Probe>)>> {
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let reg = self.sources.clone();
        let source_id = ctx.source.id.clone();
        let kind = ctx.kind;
        let max_record = self.cfg.ingestion.max_record_bytes;
        let cancel = ctx.cancel.clone();
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

struct PagePlan {
    known: HashMap<String, KnownFile>,
    probes: HashMap<String, Option<Probe>>,
    moves: HashMap<String, MoveInfo>,
}
