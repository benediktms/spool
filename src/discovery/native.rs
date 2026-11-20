use super::{DiscoveryService, Mode, PageCtx, Report, Selection};
use crate::domain::SessionKind;
use crate::source::opencode::{
    self, GuardedDb, NativeScanStage, OpenCodeError, SCAN_PAGE, ScanControl,
};
use crate::storage::{catalog, index};
use sqlx::{FromRow, Sqlite, SqliteConnection, Transaction};
use std::borrow::Cow;
use std::collections::HashSet;
use std::path::Path;
use std::time::{Duration, Instant};

const SCAN_TIMEOUT: Duration = Duration::from_secs(120);

struct NativeRun<'a> {
    ctx: &'a PageCtx<'a>,
    path: &'a Path,
    after: Option<&'a str>,
    budget: usize,
    control: ScanControl<'a>,
}

#[derive(FromRow)]
struct Entry {
    native_id: String,
    parent_id: Option<String>,
    directory: String,
    updated_at: i64,
    event_sequence: i64,
    session_id: Option<String>,
    eligibility: Option<String>,
    index_state: Option<String>,
    source_status: Option<String>,
    has_snapshot: bool,
    previous_sequence: Option<i64>,
    excluded: bool,
}

#[derive(FromRow)]
struct MissingCandidate {
    id: String,
    native_id: String,
    source_status: String,
    tombstoned_at: Option<i64>,
    active_snapshot_id: Option<String>,
}

impl DiscoveryService {
    pub(super) async fn run_native(
        &self,
        ctx: &PageCtx<'_>,
        path: Option<&Path>,
        after: Option<&Path>,
        budget: usize,
    ) -> anyhow::Result<Report> {
        let result = match (path, after.map(Path::to_str)) {
            (Some(path), None | Some(Some(_))) => {
                self.discover_native(&NativeRun {
                    ctx,
                    path,
                    after: after.and_then(Path::to_str),
                    budget,
                    control: ScanControl {
                        cancel: ctx.cancel,
                        deadline: Some(Instant::now() + SCAN_TIMEOUT),
                    },
                })
                .await
            }
            _ => Err(OpenCodeError::UnsafeSource),
        };
        match result {
            Ok(rep) => Ok(rep),
            Err(error) => {
                let rep = Report {
                    source_id: ctx.source.id.clone(),
                    errors: 1,
                    error: Some(error.to_string()),
                    ..Default::default()
                };
                if matches!(ctx.sel, Selection::Auto) {
                    catalog::record_scan(
                        &self.index,
                        &ctx.source.id,
                        self.clock.now_ms(),
                        rep.error.as_deref(),
                        None,
                    )
                    .await?;
                    catalog::app_meta_set(
                        &self.index,
                        &format!("discovery.last.{}", ctx.source.id),
                        &serde_json::to_string(&rep)?,
                    )
                    .await?;
                }
                Ok(rep)
            }
        }
    }

    async fn discover_native(&self, run: &NativeRun<'_>) -> Result<Report, OpenCodeError> {
        if run.budget == 0 {
            return Err(OpenCodeError::Limit);
        }
        run.control.check()?;
        let id = &run.ctx.source.id;
        let mut config_check = self.index.reader.acquire().await?;
        NativeScanStage::check_configuration(&mut config_check, run.ctx.source, run.path).await?;
        drop(config_check);
        let mut db = opencode::open_for_discovery(&self.index, id, run.path).await?;
        let stage =
            NativeScanStage::begin(&self.index, &mut db, run.ctx.source, run.path, run.after)
                .await?;
        let result = self.scan_and_apply(run, &stage, &mut db).await;
        if let Err(error) = &result {
            let movement = stage.record_movement(&self.index, error).await;
            let cleanup = stage.abort(&self.index).await;
            movement?;
            cleanup?;
        }
        result
    }

    async fn scan_and_apply(
        &self,
        run: &NativeRun<'_>,
        stage: &NativeScanStage,
        db: &mut GuardedDb,
    ) -> Result<Report, OpenCodeError> {
        let max_rows = if matches!(run.ctx.mode, Mode::Enqueue(_)) {
            run.budget.max(SCAN_PAGE)
        } else {
            usize::MAX
        };
        let mut rep = Report {
            source_id: run.ctx.source.id.clone(),
            ..Default::default()
        };
        let mut exhausted = false;
        while !exhausted && rep.scanned < max_rows {
            run.control.check()?;
            let limit = self
                .page_size
                .clamp(1, SCAN_PAGE)
                .min(max_rows - rep.scanned);
            let (count, done) = stage.next_page(&self.index, db, limit).await?;
            exhausted = done;
            rep.scanned += count;
        }

        run.control.check()?;
        stage.verify(db)?;
        let mut tx = self.index.writer.begin().await?;
        NativeScanStage::check_configuration(&mut tx, run.ctx.source, run.path).await?;
        let now = self.clock.now_ms();
        run.apply_entries(&mut tx, stage, now, exhausted, &mut rep)
            .await?;
        if matches!(run.ctx.sel, Selection::Auto) {
            if !rep.complete {
                return Err(OpenCodeError::Limit);
            }
            run.reconcile_missing(&mut tx, stage, db, now, &mut rep)
                .await?;
            record_native_success(&mut tx, now, &rep).await?;
        }
        stage.finish(&mut tx).await?;
        stage.verify(db)?;
        run.control.check()?;
        tx.commit().await?;
        Ok(rep)
    }
}

impl NativeRun<'_> {
    async fn apply_entries(
        &self,
        tx: &mut SqliteConnection,
        stage: &NativeScanStage,
        now: i64,
        exhausted: bool,
        rep: &mut Report,
    ) -> Result<(), OpenCodeError> {
        let mut after = self.after.unwrap_or_default().to_owned();
        let mut debounce_floor = self.ctx.scan_started;
        let mut processed = 0;
        loop {
            self.control.check()?;
            let entries: Vec<Entry> = sqlx::query_as(
                "SELECT e.native_id,e.parent_id,e.directory,e.updated_at,e.event_sequence, \
                 s.id AS session_id,s.eligibility,s.index_state,s.source_status, \
                 s.active_snapshot_id IS NOT NULL AS has_snapshot,o.event_sequence AS previous_sequence, \
                 (COALESCE(s.excluded,0)=1 OR EXISTS(SELECT 1 FROM exclusions x \
                  WHERE x.kind='source' AND x.target_id=?2)) AS excluded \
                 FROM native_scan_entries e LEFT JOIN sessions s \
                 ON s.source_id=?2 AND s.native_id=e.native_id AND s.discriminator='' \
                 LEFT JOIN native_session_observations o ON o.session_id=s.id \
                 WHERE e.scan_id=?1 AND e.native_id>?3 ORDER BY e.native_id LIMIT 500",
            )
            .bind(&stage.id)
            .bind(&self.ctx.source.id)
            .bind(&after)
            .fetch_all(&mut *tx)
            .await?;
            if entries.is_empty() {
                break;
            }
            for entry in entries {
                if rep.queued >= self.budget {
                    rep.scanned = processed;
                    rep.next_after = Some(after);
                    return Ok(());
                }
                self.apply_entry(tx, &entry, now, &mut debounce_floor, rep)
                    .await?;
                processed += 1;
                after = entry.native_id;
            }
        }
        rep.finished = exhausted;
        rep.complete = exhausted;
        if !exhausted {
            rep.next_after = Some(after);
        }
        if matches!(self.ctx.sel, Selection::Auto) {
            rep.watermark = Some(debounce_floor);
        }
        Ok(())
    }

    async fn apply_entry(
        &self,
        tx: &mut SqliteConnection,
        e: &Entry,
        now: i64,
        debounce_floor: &mut i64,
        rep: &mut Report,
    ) -> Result<(), OpenCodeError> {
        let auto = matches!(self.ctx.mode, Mode::Auto);
        let dry = matches!(self.ctx.mode, Mode::CatalogOnly);
        let selected = match self.ctx.sel {
            Selection::Auto => e.updated_at >= self.ctx.cutoff,
            Selection::Since(since) => e.updated_at >= *since,
            Selection::All => true,
        };
        let known = e.session_id.is_some();
        if !selected && (!auto || !known) {
            rep.skipped_old += 1;
            return Ok(());
        }
        if e.previous_sequence
            .is_some_and(|seq| seq > e.event_sequence)
        {
            return Err(OpenCodeError::Unsupported);
        }
        let changed = e.previous_sequence != Some(e.event_sequence);
        let revived = known && e.source_status.as_deref() != Some("present");
        if auto
            && changed
            && self.ctx.debounce > 0
            && now.saturating_sub(e.updated_at) < self.ctx.debounce
        {
            rep.debounced += 1;
            *debounce_floor = (*debounce_floor).min(e.updated_at);
            return Ok(());
        }
        let kind = if e.parent_id.is_some() {
            SessionKind::Subagent
        } else {
            SessionKind::Interactive
        };
        let blocked_kind = kind == SessionKind::Subagent && !self.ctx.include_subagents;
        let already_current =
            !changed && !revived && e.has_snapshot && e.index_state.as_deref() == Some("indexed");
        let (sid, created) = match e.session_id.as_deref() {
            Some(sid) if dry || (!changed && !revived) => (Cow::Borrowed(sid), false),
            _ => {
                let (sid, created) = catalog::upsert_session(
                    tx,
                    &catalog::NewSession {
                        source_id: &self.ctx.source.id,
                        native_id: &e.native_id,
                        discriminator: "",
                        kind,
                        cwd: Some(&e.directory),
                        created_at: None,
                        last_activity_at: Some(e.updated_at),
                        eligibility: match self.ctx.mode {
                            Mode::Auto => self.ctx.eligibility,
                            Mode::CatalogOnly => "backfill",
                            Mode::Enqueue(_) => "manual",
                        },
                        operation_id: None,
                    },
                    now,
                )
                .await?;
                (Cow::Owned(sid), created)
            }
        };
        let sid = sid.as_ref();
        rep.new_sessions += usize::from(created);
        if !dry && (changed || revived) {
            catalog::record_native_observation(tx, sid, Some(e.event_sequence), now).await?;
        }
        if known && changed && !dry {
            rep.changed += 1;
        }
        let excluded = e.excluded
            || created
                && sqlx::query_scalar::<_, bool>("SELECT excluded FROM sessions WHERE id=?")
                    .bind(sid)
                    .fetch_one(&mut *tx)
                    .await?;
        if excluded {
            rep.skipped_excluded += 1;
            return Ok(());
        }
        if blocked_kind {
            rep.skipped_subagent += 1;
            if !auto {
                return Ok(());
            }
        }

        let backfill = e.eligibility.as_deref() == Some("backfill");
        match self.ctx.mode {
            Mode::Auto if known => {
                let promote = backfill && !e.has_snapshot && selected;
                if promote {
                    catalog::set_eligibility(tx, sid, self.ctx.eligibility).await?;
                }
                if promote || (changed || revived) && (e.has_snapshot || !backfill) {
                    mark_native_pending(tx, sid, None, changed || revived).await?;
                }
            }
            Mode::Auto => {}
            _ if already_current => rep.already_current += 1,
            Mode::CatalogOnly => {
                if rep.candidates.len() < 4096 {
                    rep.candidates.push(sid.to_owned());
                }
                rep.queued += 1;
            }
            Mode::Enqueue(op) => {
                if backfill {
                    catalog::set_eligibility(tx, sid, "manual").await?;
                }
                mark_native_pending(tx, sid, Some(op), known && (changed || revived)).await?;
                rep.queued += 1;
            }
        }
        Ok(())
    }

    async fn reconcile_missing(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        stage: &NativeScanStage,
        db: &mut GuardedDb,
        now: i64,
        rep: &mut Report,
    ) -> Result<(), OpenCodeError> {
        let mut after = String::new();
        loop {
            self.control.check()?;
            let candidates: Vec<MissingCandidate> = sqlx::query_as(
                    "SELECT s.id,s.native_id,s.source_status,s.tombstoned_at,s.active_snapshot_id \
                     FROM sessions s WHERE s.source_id=? AND s.id>? AND s.missing_confirmed_at IS NULL \
                     AND NOT EXISTS (SELECT 1 FROM native_scan_entries e WHERE e.scan_id=? AND e.native_id=s.native_id) \
                     ORDER BY s.id LIMIT 500",
                )
                .bind(&self.ctx.source.id)
                .bind(&after)
                .bind(&stage.id)
                .fetch_all(&mut **tx)
                .await?;
            let Some(last) = candidates.last() else { break };
            after.clone_from(&last.id);
            let ids: Vec<String> = candidates.iter().map(|row| row.native_id.clone()).collect();
            let missing: HashSet<_> = opencode::missing_sessions(db, &ids)
                .await
                .map_err(opencode::classify_lookup_error)?
                .into_iter()
                .collect();
            for session in candidates {
                if !missing.contains(&session.native_id) {
                    continue;
                }
                if session.source_status == "present" {
                    sqlx::query(
                        "UPDATE sessions SET source_status='missing',tombstoned_at=? WHERE id=?",
                    )
                    .bind(now)
                    .bind(session.id)
                    .execute(&mut **tx)
                    .await?;
                    rep.newly_missing += 1;
                } else if session.source_status == "missing"
                    && session
                        .tombstoned_at
                        .is_some_and(|at| at < self.ctx.scan_started)
                {
                    if let Some(snapshot) = session.active_snapshot_id {
                        index::retire_snapshot(tx, &snapshot, now).await?;
                    }
                    sqlx::query("UPDATE sessions SET active_snapshot_id=NULL,generation=generation+1,missing_confirmed_at=?,task_id=NULL,index_error='source_removed' WHERE id=?")
                        .bind(now).bind(session.id).execute(&mut **tx).await?;
                    rep.purged += 1;
                }
            }
        }
        if rep.purged > 0 {
            index::bump_epoch(tx).await?;
        }
        Ok(())
    }
}

async fn mark_native_pending(
    tx: &mut SqliteConnection,
    sid: &str,
    op: Option<&str>,
    invalidate: bool,
) -> Result<(), OpenCodeError> {
    if invalidate {
        sqlx::query("UPDATE sessions SET generation=generation+1 WHERE id=?")
            .bind(sid)
            .execute(&mut *tx)
            .await?;
    }
    catalog::mark_pending(tx, sid, op).await?;
    Ok(())
}

async fn record_native_success(
    tx: &mut SqliteConnection,
    now: i64,
    rep: &Report,
) -> Result<(), OpenCodeError> {
    sqlx::query("UPDATE sources SET last_scan_at=?,last_scan_ok=1,last_scan_error=NULL,discovery_watermark=? WHERE id=?")
        .bind(now)
        .bind(rep.watermark)
        .bind(&rep.source_id)
        .execute(&mut *tx)
        .await?;
    let summary = serde_json::to_string(rep).map_err(|_| OpenCodeError::Limit)?;
    sqlx::query("INSERT INTO app_meta(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value")
        .bind(format!("discovery.last.{}", rep.source_id))
        .bind(summary)
        .execute(tx)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::ManualClock;
    use crate::config::{Config, SourceConfig};
    use crate::domain::HarnessKind;
    use crate::sources::{SourceEntry, SourceRegistry};
    use crate::storage::index::Index;
    use sqlx::{Connection, Executor, sqlite::SqliteConnectOptions};
    use std::path::PathBuf;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    const START: i64 = 10 * catalog::DAY_MS;
    const SCHEMA: &str =
        "CREATE TABLE session_v2(id TEXT PRIMARY KEY,parent_id TEXT,directory TEXT NOT NULL,title TEXT,time_updated INTEGER NOT NULL);
         CREATE TABLE session_message(id TEXT,session_id TEXT,type TEXT,seq INTEGER,data TEXT);
         CREATE TABLE session_part(message_id TEXT,session_id TEXT,ordinal INTEGER,type TEXT,data TEXT);
         CREATE TABLE event_sequence(aggregate_id TEXT PRIMARY KEY,seq INTEGER);";

    struct Fixture {
        _dir: tempfile::TempDir,
        writer: SqliteConnection,
        service: DiscoveryService,
        entry: SourceEntry,
        clock: Arc<ManualClock>,
        cancel: Arc<AtomicBool>,
    }

    impl Fixture {
        async fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("native.db");
            let mut writer = SqliteConnection::connect_with(
                &SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
            writer.execute(SCHEMA).await.unwrap();
            let idx = Index::open_for_test(dir.path()).await.unwrap();
            let mut cfg = Config::default();
            cfg.discovery.debounce_seconds = 0;
            cfg.sources = vec![SourceConfig {
                id: "oc".into(),
                enabled: true,
                kind: HarnessKind::OpenCode,
                roots: vec![path.to_string_lossy().into_owned()],
                include_subagents: false,
            }];
            let cfg = Arc::new(cfg);
            let registry = Arc::new(SourceRegistry::new(&cfg).unwrap());
            let clock = Arc::new(ManualClock::new(START));
            let entry = registry.entry("oc").unwrap();
            catalog::sync_sources(&idx, &registry.entries())
                .await
                .unwrap();
            let mut service = DiscoveryService::new(idx, registry, cfg, clock.clone());
            service.page_size = 2;
            Self {
                _dir: dir,
                writer,
                service,
                entry,
                clock,
                cancel: Arc::new(AtomicBool::new(false)),
            }
        }

        async fn run(
            &self,
            selection: Selection,
            mode: Mode,
            after: Option<String>,
            budget: usize,
        ) -> Report {
            self.service
                .run_explicit(
                    &self.entry.id,
                    selection,
                    mode,
                    after.map(PathBuf::from),
                    budget,
                    self.cancel.clone(),
                )
                .await
                .unwrap()
        }

        async fn auto(&self) -> Report {
            self.service
                .run_auto(&self.entry.id, self.cancel.clone())
                .await
                .unwrap()
        }

        async fn add_fixtures(&mut self) {
            sqlx::query(
                "INSERT INTO session_v2 VALUES ('a-old',NULL,'/synthetic',NULL,1),\
                ('b-parent',NULL,'/synthetic',NULL,?1),('c-child','b-parent','/synthetic',NULL,?1),\
                ('d-old',NULL,'/synthetic',NULL,2)",
            )
            .bind(START - 10_000)
            .execute(&mut self.writer)
            .await
            .unwrap();
            self.writer
                .execute("INSERT INTO event_sequence SELECT id,1 FROM session_v2")
                .await
                .unwrap();
        }

        async fn state(&self, native: &str) -> (String, String, Option<i64>) {
            sqlx::query_as(
                "SELECT s.eligibility,s.source_status,o.event_sequence FROM sessions s \
                 LEFT JOIN native_session_observations o ON o.session_id=s.id WHERE s.native_id=?",
            )
            .bind(native)
            .fetch_one(&self.service.index.reader)
            .await
            .unwrap()
        }
    }

    #[tokio::test]
    async fn opencode_discovery_preserves_bootstrap_manual_cursors_and_dry_run() {
        let mut f = Fixture::new().await;
        f.add_fixtures().await;
        let scanned = f.auto().await;
        assert!(scanned.complete, "{:?}", scanned.error);
        assert_eq!(
            (
                scanned.scanned,
                scanned.new_sessions,
                scanned.skipped_old,
                scanned.skipped_subagent
            ),
            (4, 2, 2, 1)
        );
        assert_eq!(scanned.watermark, Some(START));
        assert_eq!(f.state("b-parent").await.0, "bootstrap");

        f.writer
            .execute("UPDATE event_sequence SET seq=2 WHERE aggregate_id='b-parent'")
            .await
            .unwrap();
        let dry = f
            .run(Selection::Since(2), Mode::CatalogOnly, None, usize::MAX)
            .await;
        assert_eq!((dry.queued, dry.candidates.len()), (2, 2));
        assert_eq!(f.state("b-parent").await.2, Some(1));
        assert_eq!(
            f.state("d-old").await,
            ("backfill".into(), "present".into(), None)
        );

        let first = f
            .run(Selection::All, Mode::Enqueue("op".into()), None, 1)
            .await;
        assert_eq!(
            (first.queued, first.next_after.as_deref()),
            (1, Some("a-old"))
        );
        assert!(!first.finished);
        let second = f
            .run(
                Selection::All,
                Mode::Enqueue("op".into()),
                first.next_after,
                3,
            )
            .await;
        assert!(second.finished, "{:?}", second.error);
        assert_eq!(second.queued, 2);
        assert_eq!(f.state("a-old").await.0, "manual");
        assert_eq!(f.state("d-old").await.0, "manual");
        assert_eq!(
            f.state("b-parent").await,
            ("bootstrap".into(), "present".into(), Some(2))
        );
    }

    #[tokio::test]
    async fn opencode_discovery_failures_keep_watermark_revisions_and_presence() {
        let mut f = Fixture::new().await;
        f.add_fixtures().await;
        assert!(f.auto().await.complete);
        f.writer
            .execute(
                "DELETE FROM session_v2 WHERE id='b-parent'; \
            INSERT INTO session_v2 VALUES ('z-bad',NULL,'/synthetic',NULL,1)",
            )
            .await
            .unwrap();
        f.clock.advance_ms(1_000);
        let failed = f.auto().await;
        assert_eq!(
            failed.error.as_deref(),
            Some("unsupported OpenCode storage schema")
        );
        assert_eq!(f.state("b-parent").await.1, "present");
        let stored = catalog::source(&f.service.index, "oc")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.discovery_watermark, Some(START));
        assert_eq!(stored.last_scan_ok, Some(false));
        let runs: i64 = sqlx::query_scalar("SELECT count(*) FROM native_scan_runs")
            .fetch_one(&f.service.index.reader)
            .await
            .unwrap();
        assert_eq!(runs, 0);

        f.writer
            .execute("DELETE FROM session_v2 WHERE id='z-bad'")
            .await
            .unwrap();
        f.cancel.store(true, Ordering::Relaxed);
        assert!(!f.auto().await.complete);
        assert_eq!(f.state("b-parent").await.1, "present");
        f.cancel.store(false, Ordering::Relaxed);
        let missing = f.auto().await;
        assert_eq!((missing.newly_missing, missing.purged), (1, 0));
        f.clock.advance_ms(1);
        assert_eq!(f.auto().await.purged, 1);

        let path = &f.entry.roots[0];
        std::fs::rename(path, path.with_extension("moved")).unwrap();
        std::fs::write(path, b"not a SQLite database").unwrap();
        let replaced = f.auto().await;
        assert_eq!(
            replaced.error.as_deref(),
            Some("OpenCode database identity changed")
        );
        let diagnostic: String = sqlx::query_scalar(
            "SELECT diagnostic FROM native_source_identities WHERE source_id='oc'",
        )
        .fetch_one(&f.service.index.reader)
        .await
        .unwrap();
        assert_eq!(diagnostic, "SOURCE_MOVED");
    }
}
