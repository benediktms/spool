//! Foreground per-user daemon (spec 4, 17): state-dir lock, permission checks, databases, services,
//! owner-only local IPC, supervised Apalis workers.

use crate::clock::{self, SharedClock};
use crate::config::{Config, Paths};
use crate::control::{ControlDeps, ControlService};
use crate::embeddings::EmbeddingService;
use crate::ingest::engine::IngestEngine;
use crate::ipc::{self, AppError, Request};
use crate::jobs::backend::Liveness;
use crate::jobs::supervisor::JobSystem;
use crate::perms;
use crate::search::SearchService;
use crate::sources::SourceRegistry;
use crate::storage::{self, index::Index};
use crate::transcript::ReadService;
use crate::transcript::handle::KeyStore;
use crate::transcript::lease::LeaseManager;
use anyhow::{Context, Result, bail};
use sqlx::SqlitePool;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
/// Create or verify a private state directory.
pub fn ensure_private_dir(path: &std::path::Path) -> Result<()> {
    perms::ensure_private_dir(path).map_err(Into::into)
}

/// Identifier returned by `index_status` so a client can tell which dependency baseline runs.
pub const BASELINE_ID: &str = "apalis-sqlite-1.0.0+sqlx-0.9.0+rmcp-3.5.1";

#[cfg(windows)]
fn has_console_window() -> bool {
    // SAFETY: GetConsoleWindow takes no arguments and only observes this process's console.
    unsafe { !windows_sys::Win32::System::Console::GetConsoleWindow().is_null() }
}

pub struct Daemon {
    pub paths: Paths,
    pub cfg: Arc<Config>,
    pub index: Index,
    pub jobs_pool: SqlitePool,
    pub sqlite_version: String,
    pub clock: SharedClock,
    pub keys: KeyStore,
    pub sources: Arc<SourceRegistry>,
    pub embeddings: Arc<EmbeddingService>,
    pub engine: Arc<IngestEngine>,
    pub leases: LeaseManager,
    _lock: std::fs::File,
}

/// OS-held exclusive lock; released on process exit including crashes (spec 17.1).
fn lock_state_dir(p: &Paths) -> Result<std::fs::File> {
    let f = perms::open_private_file(&p.lock).context("opening daemon.lock")?;
    match f.try_lock() {
        Ok(()) => Ok(f),
        Err(std::fs::TryLockError::WouldBlock) => {
            bail!(
                "another spool daemon holds {} (socket {})",
                p.lock.display(),
                p.socket.display()
            )
        }
        Err(std::fs::TryLockError::Error(e)) => Err(e).context("locking daemon.lock"),
    }
}

fn tighten_database_files(paths: &Paths) -> Result<()> {
    for database in [&paths.index_db, &paths.jobs_db] {
        for suffix in ["", "-wal", "-shm"] {
            let mut file = database.as_os_str().to_owned();
            file.push(suffix);
            let file = PathBuf::from(file);
            match perms::set_private(&file, 0o600) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
    }
    Ok(())
}

impl Daemon {
    pub async fn open(mut paths: Paths, cfg: Config) -> Result<Self> {
        Self::open_with_clock(&mut paths, cfg, clock::system()).await
    }

    pub async fn open_with_clock(
        paths: &mut Paths,
        mut cfg: Config,
        clock: SharedClock,
    ) -> Result<Self> {
        cfg.resolve_paths(&paths.root)?; // idempotent: `~/` and relative source roots become absolute
        cfg.validate()?;
        paths.socket = cfg.socket_path(&paths.root)?;
        paths.validate()?;
        let paths = paths.clone();
        perms::ensure_private_dir(&paths.root)?;
        perms::ensure_private_dir(&paths.state_dir)?;
        perms::ensure_private_dir(&paths.secrets_dir)?;
        let lock = lock_state_dir(&paths)?;
        // Spec 17.1 step 3: verify ownership/permissions. Files we created are tightened; foreign
        // ownership or a loose secret is a startup error.
        let cred = cfg.credential_path(&paths.root)?;
        let issues = perms::check(&paths, std::slice::from_ref(&cred));
        let (ours, theirs): (Vec<_>, Vec<_>) = issues
            .into_iter()
            .partition(|i| i.actual.is_some() && i.path != paths.signing_key && i.path != cred);
        perms::tighten(&ours)?;
        if let Some(i) = theirs.first() {
            bail!(
                "unsafe permissions on {}: {}. Fix with chmod/chown, then restart",
                i.path.display(),
                i.detail
            );
        }
        let keys = KeyStore::load(&paths.signing_key).with_context(|| {
            format!(
                "handle-signing key {} is missing or unusable; run `spool init`",
                paths.signing_key.display()
            )
        })?;
        let busy = Duration::from_millis(cfg.storage.busy_timeout_ms);
        let index = Index::open(&paths.index_db, busy)
            .await
            .context("opening index.sqlite")?;
        let jobs_pool = storage::connect(&paths.jobs_db, 4, busy)
            .await
            .context("opening jobs.sqlite")?;
        let sqlite_version = storage::check_fts5(&index.writer)
            .await
            .context("FTS5 capability check failed")?;
        tighten_database_files(&paths)?;
        let cfg = Arc::new(cfg);
        let sources = Arc::new(SourceRegistry::new(&cfg)?);
        let embeddings = Arc::new(EmbeddingService::from_config(
            index.clone(),
            &cfg,
            &paths,
            clock.clone(),
        )?);
        embeddings
            .ensure_profile()
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        let engine = Arc::new(IngestEngine::new(
            index.clone(),
            cfg.clone(),
            sources.clone(),
            embeddings.clone(),
            clock.clone(),
        ));
        let leases = LeaseManager::new(index.clone(), cfg.reader.clone(), clock.clone());
        Ok(Self {
            paths,
            cfg,
            index,
            jobs_pool,
            sqlite_version,
            clock,
            keys,
            sources,
            embeddings,
            engine,
            leases,
            _lock: lock,
        })
    }

    /// Bind the private local IPC endpoint. Unix replaces only an owner-owned stale socket;
    /// Windows names a protected pipe from the configured socket path and never touches it.
    #[cfg(unix)]
    pub fn bind(&self) -> Result<ipc::transport::Listener> {
        use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};

        let sock = &self.paths.socket;
        let reserved = [
            &self.paths.config_file,
            &self.paths.index_db,
            &self.paths.jobs_db,
            &self.paths.lock,
            &self.paths.signing_key,
        ];
        if reserved
            .iter()
            .any(|r| sock == *r || sock.starts_with(&self.paths.secrets_dir))
        {
            bail!(
                "socket path {} collides with a reserved Spool path",
                sock.display()
            );
        }
        let parent = sock
            .parent()
            .context("socket path has no parent directory")?;
        perms::ensure_private_dir(parent)?;
        match std::fs::symlink_metadata(sock) {
            Ok(md) => {
                if !md.file_type().is_socket() || md.uid() != rustix::process::geteuid().as_raw() {
                    bail!(
                        "{} exists and is not a Unix socket owned by this user; refusing to remove it",
                        sock.display()
                    );
                }
                // A connectable socket belongs to a live daemon (e.g. another state dir sharing the path).
                if std::os::unix::net::UnixStream::connect(sock).is_ok() {
                    bail!(
                        "a live listener already owns {}; refusing to bind over it",
                        sock.display()
                    );
                }
                std::fs::remove_file(sock).context("removing stale socket")?;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        let listener = ipc::transport::bind(sock).context("binding socket")?;
        std::fs::set_permissions(sock, std::fs::Permissions::from_mode(0o600))?;
        Ok(listener)
    }

    #[cfg(windows)]
    pub fn bind(&self) -> Result<ipc::transport::Listener> {
        ipc::transport::bind(&self.paths.socket).context("binding local named pipe")
    }

    pub async fn run(self) -> Result<()> {
        // Readiness (spec 17.1): search/read are served as soon as storage is open; the socket is
        // bound BEFORE the job system starts. `JobSystem::start` only does bounded local work (DB
        // setup, durable enqueue of one catch-up discovery and the recurring ticks), never
        // provider calls or backfill, and `control` calls answer DAEMON_NOT_READY until it is up.
        #[cfg(windows)]
        let shutdown = ipc::transport::shutdown_signal(&self.paths)?;

        let search = Arc::new(SearchService::new(
            self.index.clone(),
            self.cfg.clone(),
            self.embeddings.clone(),
            self.leases.clone(),
            self.keys.clone(),
        ));
        let reader = Arc::new(ReadService::new(
            self.index.clone(),
            self.cfg.clone(),
            self.leases.clone(),
            self.keys.clone(),
            self.sources.clone(),
            self.engine.source_read_stager(),
        ));
        let control: Arc<OnceLock<Arc<ControlService>>> = Arc::new(OnceLock::new());
        let listener = self.bind()?;
        let handler: ipc::server::Handler = {
            let (control, search, reader) = (control.clone(), search.clone(), reader.clone());
            Arc::new(move |req: Request| {
                let (control, search, reader) = (control.clone(), search.clone(), reader.clone());
                Box::pin(async move {
                    let not_ready = || {
                        let mut e = AppError::new(
                            "DAEMON_NOT_READY",
                            "the daemon is still starting its job system",
                        );
                        e.retryable = true;
                        e.recovery = Some("retry_later".into());
                        e
                    };
                    match req {
                        Request::Tool { tool, args, .. } => match tool.as_str() {
                            "search_sessions" => search.call(args).await,
                            "read_session" => reader.call(args).await,
                            "index_status" => {
                                control
                                    .get()
                                    .ok_or_else(not_ready)?
                                    .index_status(args)
                                    .await
                            }
                            other => Err(AppError::new(
                                "INVALID_ARGUMENT",
                                format!("unknown tool {other}"),
                            )),
                        },
                        Request::Control { cmd, args, .. } => {
                            control.get().ok_or_else(not_ready)?.call(&cmd, args).await
                        }
                    }
                })
            })
        };
        let (tx, rx) = tokio::sync::watch::channel(false);
        let srv = tokio::spawn(ipc::server::serve(
            listener,
            handler,
            self.cfg.ipc.request_concurrency,
            self.cfg.ipc.max_request_bytes,
            rx,
        ));
        tracing::info!(socket = %self.paths.socket.display(), "spool daemon ready for search and read");
        let jobs = JobSystem::start(
            self.index.clone(),
            self.jobs_pool.clone(),
            self.cfg.clone(),
            self.sources.clone(),
            self.engine.clone(),
            self.clock.clone(),
            Liveness::default(),
        )
        .await?;
        tighten_database_files(&self.paths)?;
        let _ = control.set(Arc::new(ControlService::new(ControlDeps {
            index: self.index.clone(),
            jobs: jobs.clone(),
            cfg: self.cfg.clone(),
            sources: self.sources.clone(),
            engine: self.engine.clone(),
            leases: self.leases.clone(),
            keys: self.keys.clone(),
            paths: self.paths.clone(),
            clock: self.clock.clone(),
        })));
        tracing::info!("job system started");
        #[cfg(unix)]
        {
            let mut term =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = term.recv() => {}
            }
        }
        #[cfg(windows)]
        {
            if has_console_window() {
                tokio::select! {
                    result = tokio::signal::ctrl_c() => result?,
                    result = shutdown => result?,
                }
            } else {
                shutdown.await?;
            }
        }
        // Graceful shutdown (spec 17.2): stop accepting, stop workers within the target.
        let _ = tx.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(15), srv).await;
        jobs.shutdown(Duration::from_secs(15)).await;
        #[cfg(unix)]
        let _ = std::fs::remove_file(&self.paths.socket);
        Ok(())
    }
}

/// Entry point for `spool daemon run`: foreground, until SIGTERM/SIGINT.
pub async fn run_daemon(paths: Paths, cfg: Config) -> Result<()> {
    Daemon::open(paths, cfg).await?.run().await
}
