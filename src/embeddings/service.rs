//! `EmbeddingService`: consent gate, durable budget, concurrency permits, circuit breaker, query LRU and
//! cache-committing document embedding (spec 12, 4.1, 10.4-10.6).

use super::budget::{Ledger, Reservation, Reserve, budget_day, next_reset_ms};
use super::cache::QueryCache;
use super::health::{CircuitInfo, Health};
use super::openai::{OpenAiProvider, profile_spec, read_credential};
use super::provider::*;
use super::tokenizer::tokenizer;
use crate::clock::{SharedClock, Tz, UTC};
use crate::config::{Config, EmbeddingConfig, Paths, SearchConfig};
use crate::domain::Digest;
use crate::ingest::chunk::TokenCounter;
use crate::ipc::AppError;
use crate::storage::index::{Index, bump_epoch};
use futures::future::BoxFuture;
use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use sqlx::{QueryBuilder, Row, Sqlite};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{OnceCell, Semaphore};

/// Fixed, harmless text for explicit connectivity probes (never conversation text).
const PROBE_TEXT: &str = "spool provider probe";
/// Slack added to the provider's own timeout so the provider reports its classification first.
const OUTER_TIMEOUT_SLACK: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingState {
    Ready,
    NotApproved,
    NoCredential,
    Disabled,
    AuthBlocked,
    BudgetExhausted { resets_at_ms: i64 },
    Degraded(CircuitInfo),
}

#[derive(Debug, Clone, Serialize)]
pub struct BudgetLine {
    pub used: u64,
    pub limit: u64,
    pub exhausted: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct BudgetView {
    pub timezone: String,
    pub day: String,
    pub resets_at_ms: i64,
    pub ingest: BudgetLine,
    pub query: BudgetLine,
}

#[derive(Debug, Clone, Serialize)]
pub struct EmbeddingStatus {
    pub state: EmbeddingState,
    pub profile_id: String,
    /// The profile queries are answered in. It differs from `profile_id` (the configured target that ingestion
    /// embeds for) while a profile migration is still covering the corpus.
    pub active_profile_id: String,
    /// Content-free actionable detail (e.g. why the credential is unusable).
    pub detail: Option<String>,
    pub budget: BudgetView,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct EmbedProgress {
    /// Vectors newly committed by this call.
    pub embedded: usize,
    /// Inputs already present for the profile (or duplicates committed concurrently).
    pub cached: usize,
    /// Inputs not attempted: budget exhausted or cancelled. Retry later.
    pub deferred: usize,
    /// Inputs that can never be embedded (empty, or larger than the provider's per-input limit).
    pub rejected: usize,
    /// Input tokens: provider-reported where returned, else the reserved estimate.
    pub usage_tokens: u64,
    pub cancelled: bool,
    pub budget_exhausted: bool,
    pub resets_at_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VectorCoverage {
    pub profile_id: String,
    pub total_chunks: i64,
    pub embedded_chunks: i64,
    pub pending_chunks: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProbeOutcome {
    pub dimensions: usize,
    pub latency_ms: u64,
}

struct Gate {
    kind: EmbedErrorKind,
    detail: Option<String>,
}

#[derive(Default)]
struct UsageMirror {
    day: String,
    ingest: u64,
    query: u64,
}

/// Placed behind the service when the consent gate is closed: it can never be reached, but if it were it
/// refuses instead of sending anything.
struct Unavailable(ProfileSpec);

impl EmbeddingProvider for Unavailable {
    fn profile(&self) -> &ProfileSpec {
        &self.0
    }
    fn limits(&self) -> ProviderLimits {
        ProviderLimits::OPENAI
    }
    fn embed<'a>(
        &'a self,
        _p: Purpose,
        _i: &'a [String],
    ) -> BoxFuture<'a, Result<EmbedResponse, EmbedError>> {
        Box::pin(async {
            Err(EmbedError::new(
                EmbedErrorKind::Unavailable,
                "no provider is configured",
            ))
        })
    }
}

/// Marks an in-flight request ambiguous if its future is dropped (deadline, cancellation) before the outcome
/// is recorded. A request that was never dispatched is recorded as failed instead.
struct InFlight {
    ledger: Ledger,
    id: String,
    dispatched: bool,
    armed: bool,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(h) = tokio::runtime::Handle::try_current() {
            let (l, id, d) = (
                self.ledger.clone(),
                std::mem::take(&mut self.id),
                self.dispatched,
            );
            h.spawn(async move {
                let _ = if d {
                    l.ambiguous(&id, "cancelled").await
                } else {
                    l.failed(&id, "cancelled_before_dispatch").await
                };
            });
        }
    }
}

/// Query-side state: the profile whose stored vectors are searched, and the provider that can embed a query for it.
#[derive(Clone)]
struct Active {
    id: String,
    dimensions: usize,
    provider: Option<Arc<dyn EmbeddingProvider>>,
}

/// Builds the provider for a persisted profile other than the configured one.
pub type ProviderFactory =
    Arc<dyn Fn(&ProfileSpec) -> Option<Arc<dyn EmbeddingProvider>> + Send + Sync>;

pub struct EmbeddingService {
    index: Index,
    cfg: EmbeddingConfig,
    clock: SharedClock,
    provider: Arc<dyn EmbeddingProvider>,
    gate: Option<Gate>,
    spec: ProfileSpec,
    profile_id: String,
    ledger: Ledger,
    health: Health,
    ingest_sem: Arc<Semaphore>,
    query_sem: Arc<Semaphore>,
    /// Tokenization is CPU work: one permit acquired before `spawn_blocking`.
    cpu_sem: Arc<Semaphore>,
    cache: Mutex<QueryCache>,
    usage: Mutex<UsageMirror>,
    ready: OnceCell<()>,
    tok: Arc<dyn TokenCounter>,
    /// The profile queries use (see `refresh_active`); differs from `profile_id` only during a migration.
    active: RwLock<Active>,
    profile_providers: Option<ProviderFactory>,
    /// Zone of the daily budget day (`embedding.budget_timezone`).
    tz: Tz,
}

fn storage(_: sqlx::Error) -> EmbedError {
    EmbedError::new(EmbedErrorKind::Unavailable, "index storage error").retryable(true)
}

impl EmbedError {
    /// Map to the stable application error codes (spec 14.5) without leaking provider detail.
    pub fn to_app_error(&self) -> AppError {
        use EmbedErrorKind::*;
        let code = match self.kind {
            RateLimited => "RATE_LIMITED",
            BudgetExhausted => "BUDGET_EXHAUSTED",
            UnsupportedModel => "EMBEDDING_PROFILE_ERROR",
            _ => "QUERY_EMBEDDING_FAILED",
        };
        let mut e = AppError::new(code, self.to_string());
        e.retryable = self.retryable;
        e.recovery = Some(
            match self.kind {
                NotApproved => {
                    "set embedding.hosted_processing_approved = true to allow hosted embedding"
                }
                NoCredential => "create the provider credential file with mode 0600",
                Disabled => "set embedding.enabled = true",
                Auth => "fix the provider credential, then run the CLI provider probe",
                UnsupportedModel => "fix embedding.model / embedding.dimensions",
                BudgetExhausted => "wait for the daily budget reset or raise the configured budget",
                _ => "use lexical search or retry later",
            }
            .into(),
        );
        e
    }
}

impl EmbeddingService {
    pub fn new(
        index: Index,
        emb_cfg: EmbeddingConfig,
        search_cfg: SearchConfig,
        clock: SharedClock,
        provider: Arc<dyn EmbeddingProvider>,
    ) -> Self {
        let gate = if !emb_cfg.enabled {
            Some(Gate {
                kind: EmbedErrorKind::Disabled,
                detail: None,
            })
        } else if provider.is_hosted() && !emb_cfg.hosted_processing_approved {
            Some(Gate {
                kind: EmbedErrorKind::NotApproved,
                detail: None,
            })
        } else {
            None
        };
        Self::build(index, emb_cfg, search_cfg, clock, provider, gate)
    }

    /// Production wiring. Consent/credential problems never error: the service starts with a closed gate and
    /// reports it through `status()`; no provider is even constructed (so no socket can be opened).
    pub fn from_config(
        index: Index,
        cfg: &Config,
        paths: &Paths,
        clock: SharedClock,
    ) -> anyhow::Result<Self> {
        let e = &cfg.embedding;
        let spec = profile_spec(e)?;
        let closed = |kind, detail: Option<String>| {
            (
                Arc::new(Unavailable(spec.clone())) as Arc<dyn EmbeddingProvider>,
                Some(Gate { kind, detail }),
            )
        };
        let mut key_for_active = None;
        let (provider, gate) = if !e.enabled {
            closed(EmbedErrorKind::Disabled, None)
        } else if !e.hosted_processing_approved {
            closed(EmbedErrorKind::NotApproved, None)
        } else {
            match read_credential(&cfg.credential_path(&paths.root)?) {
                Ok(key) => {
                    key_for_active = Some(key.to_string());
                    (
                        Arc::new(OpenAiProvider::new(e, &key)?) as Arc<dyn EmbeddingProvider>,
                        None,
                    )
                }
                Err(p) => closed(EmbedErrorKind::NoCredential, Some(p.describe().into())),
            }
        };
        let service = Self::build(index, e.clone(), cfg.search.clone(), clock, provider, gate);
        Ok(match key_for_active {
            // During a profile migration queries still need the previous model. Same endpoint and credential only
            // (`provider_for` checks the endpoint); an old profile is rebuilt only when its id reproduces exactly.
            Some(key) => {
                let base = e.clone();
                service.with_profile_providers(Arc::new(move |spec: &ProfileSpec| {
                    let mut c = base.clone();
                    c.model = spec.model.clone();
                    c.dimensions = spec.dimensions;
                    c.profile_generation = spec.generation;
                    let p = OpenAiProvider::new(&c, &key).ok()?;
                    (p.profile().id() == spec.id())
                        .then(|| Arc::new(p) as Arc<dyn EmbeddingProvider>)
                }))
            }
            None => service,
        })
    }

    fn build(
        index: Index,
        cfg: EmbeddingConfig,
        search: SearchConfig,
        clock: SharedClock,
        provider: Arc<dyn EmbeddingProvider>,
        gate: Option<Gate>,
    ) -> Self {
        let spec = provider.profile().clone();
        let active = Active {
            id: spec.id(),
            dimensions: spec.dimensions,
            provider: Some(provider.clone()),
        };
        let tz = Tz::parse(&cfg.budget_timezone).unwrap_or(UTC); // `Config::validate` rejects anything else
        Self {
            ledger: Ledger::new(index.clone(), clock.clone(), tz),
            profile_id: spec.id(),
            spec,
            index,
            health: Health::default(),
            ingest_sem: Arc::new(Semaphore::new(cfg.ingestion_request_concurrency.max(1))),
            query_sem: Arc::new(Semaphore::new(cfg.query_request_concurrency.max(1))),
            cpu_sem: Arc::new(Semaphore::new(1)),
            cache: Mutex::new(QueryCache::new(
                search.query_cache_entries,
                search.query_cache_ttl_seconds as i64 * 1000,
            )),
            usage: Mutex::default(),
            ready: OnceCell::new(),
            tok: tokenizer(),
            cfg,
            clock,
            provider,
            gate,
            active: RwLock::new(active),
            profile_providers: None,
            tz,
        }
    }

    pub fn profile(&self) -> &ProfileSpec {
        &self.spec
    }

    /// Lets a profile that is not the configured one (the active profile of a migration) answer queries: `f`
    /// builds the provider for a persisted profile, or `None` when none can be built. `from_config` installs the
    /// OpenAI one; tests install their own.
    pub fn with_profile_providers(mut self, f: ProviderFactory) -> Self {
        self.profile_providers = Some(f);
        self
    }

    pub fn count_tokens(&self, text: &str) -> usize {
        self.tok.count(text)
    }

    /// Clears an authentication/profile block after the operator fixed the credential or configuration.
    pub fn reset_auth_block(&self) {
        self.health.reset();
    }

    fn gate_error(&self) -> Result<(), EmbedError> {
        use EmbedErrorKind::*;
        match &self.gate {
            None => Ok(()),
            Some(g) => Err(EmbedError::new(
                g.kind,
                match g.kind {
                    NotApproved => "hosted embedding is not approved",
                    NoCredential => "no usable provider credential",
                    _ => "embeddings are disabled",
                },
            )),
        }
    }

    fn limit(&self, p: Purpose) -> u64 {
        match p {
            Purpose::Ingest => self.cfg.daily_ingestion_token_budget,
            Purpose::Query => self.cfg.daily_query_token_budget,
        }
    }

    // ---- status ------------------------------------------------------------------------------------------

    pub fn status(&self) -> EmbeddingStatus {
        let now = self.clock.now_ms();
        let day = budget_day(now, self.tz);
        let resets_at_ms = next_reset_ms(now, self.tz);
        let (ui, uq) = {
            let u = self.usage.lock();
            if u.day == day {
                (u.ingest, u.query)
            } else {
                (0, 0)
            }
        };
        let line = |used, limit| BudgetLine {
            used,
            limit,
            exhausted: used >= limit,
        };
        let budget = BudgetView {
            timezone: self.tz.label(),
            day,
            resets_at_ms,
            ingest: line(ui, self.cfg.daily_ingestion_token_budget),
            query: line(uq, self.cfg.daily_query_token_budget),
        };
        let circuit = self.health.info(now);
        let state = if let Some(g) = &self.gate {
            match g.kind {
                EmbedErrorKind::NotApproved => EmbeddingState::NotApproved,
                EmbedErrorKind::NoCredential => EmbeddingState::NoCredential,
                _ => EmbeddingState::Disabled,
            }
        } else if self.health.blocked() == Some(EmbedErrorKind::Auth) {
            EmbeddingState::AuthBlocked
        } else if let Some(c) = circuit.clone().filter(|c| c.blocked) {
            EmbeddingState::Degraded(c)
        } else if budget.ingest.exhausted && budget.query.exhausted {
            EmbeddingState::BudgetExhausted { resets_at_ms }
        } else if let Some(c) = circuit {
            EmbeddingState::Degraded(c)
        } else {
            EmbeddingState::Ready
        };
        EmbeddingStatus {
            state,
            profile_id: self.profile_id.clone(),
            active_profile_id: self.active.read().id.clone(),
            detail: self.gate.as_ref().and_then(|g| g.detail.clone()),
            budget,
        }
    }

    async fn refresh_usage(&self) {
        let now = self.clock.now_ms();
        let day = budget_day(now, self.tz);
        if let (Ok(i), Ok(q)) = (
            self.ledger.used(now, Purpose::Ingest).await,
            self.ledger.used(now, Purpose::Query).await,
        ) {
            *self.usage.lock() = UsageMirror {
                day,
                ingest: i,
                query: q,
            };
        }
    }

    // ---- profile -----------------------------------------------------------------------------------------

    /// Registers the TARGET profile (the configured one, which ingestion embeds for) and settles which profile
    /// queries use (spec 10.6). A database with no active profile activates the configured one at once, since
    /// nothing is embedded yet. Otherwise the active profile stays until the target covers the currently published
    /// corpus; the switch is checked and made in one transaction. Returns the target profile id.
    pub async fn ensure_profile(&self) -> Result<String, AppError> {
        let id = self.upsert_profile().await?;
        self.refresh_active().await?;
        self.activate_if_covered().await?;
        self.refresh_usage().await;
        Ok(id)
    }

    /// Query vectors and stored vectors must come from the same model: `(id, dimensions)` of the ACTIVE profile.
    pub fn active_profile(&self) -> (String, usize) {
        let a = self.active.read();
        (a.id.clone(), a.dimensions)
    }

    async fn upsert_profile(&self) -> Result<String, AppError> {
        let storage_err = |_| {
            AppError::new(
                "STORAGE_ERROR",
                "required local state cannot be accessed safely",
            )
        };
        let s = &self.spec;
        let mut tx = self.index.writer.begin().await.map_err(storage_err)?;
        sqlx::query("INSERT INTO embedding_profiles(id, provider, endpoint_identity, model, dimensions, rendering_version, normalization, generation, created_at) VALUES (?,?,?,?,?,?,'l2',?,?) ON CONFLICT(id) DO NOTHING")
            .bind(&self.profile_id)
            .bind(&s.provider)
            .bind(&s.endpoint_identity)
            .bind(&s.model)
            .bind(s.dimensions as i64)
            .bind(s.rendering_version as i64)
            .bind(s.generation as i64)
            .bind(self.clock.now_ms())
            .execute(&mut *tx)
            .await
            .map_err(storage_err)?;
        let current: Option<String> =
            sqlx::query_scalar("SELECT value FROM app_meta WHERE key='active_embedding_profile'")
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage_err)?;
        if current.is_none() {
            sqlx::query("INSERT INTO app_meta(key, value) VALUES ('active_embedding_profile', ?)")
                .bind(&self.profile_id)
                .execute(&mut *tx)
                .await
                .map_err(storage_err)?;
            bump_epoch(&mut tx).await.map_err(storage_err)?;
        }
        tx.commit().await.map_err(storage_err)?;
        Ok(self.profile_id.clone())
    }

    /// Loads the active profile from `app_meta` into the query state. Stored vectors can only be searched with
    /// query vectors from the same model, so queries use a provider built for exactly that profile: the target
    /// provider when they are the same, otherwise one the factory builds for a profile on the SAME endpoint
    /// (credentials are never sent to another host). Without one, semantic queries fail with a typed error.
    async fn refresh_active(&self) -> Result<(), AppError> {
        let storage_err = |_| {
            AppError::new(
                "STORAGE_ERROR",
                "required local state cannot be accessed safely",
            )
        };
        let id: Option<String> =
            sqlx::query_scalar("SELECT value FROM app_meta WHERE key='active_embedding_profile'")
                .fetch_optional(&self.index.writer)
                .await
                .map_err(storage_err)?;
        let id = id.unwrap_or_else(|| self.profile_id.clone());
        let active = if id == self.profile_id {
            Active {
                id,
                dimensions: self.spec.dimensions,
                provider: Some(self.provider.clone()),
            }
        } else {
            let row = sqlx::query("SELECT provider, endpoint_identity, model, dimensions, rendering_version, generation FROM embedding_profiles WHERE id = ?")
                .bind(&id)
                .fetch_optional(&self.index.writer)
                .await
                .map_err(storage_err)?;
            match row {
                Some(r) => {
                    let spec = ProfileSpec {
                        provider: r.get("provider"),
                        endpoint_identity: r.get("endpoint_identity"),
                        model: r.get("model"),
                        dimensions: r.get::<i64, _>("dimensions") as usize,
                        rendering_version: r.get::<i64, _>("rendering_version") as u32,
                        generation: r.get::<i64, _>("generation") as u32,
                    };
                    Active {
                        provider: self.provider_for(&spec),
                        dimensions: spec.dimensions,
                        id,
                    }
                }
                None => Active {
                    id,
                    dimensions: 0,
                    provider: None,
                },
            }
        };
        *self.active.write() = active;
        Ok(())
    }

    fn provider_for(&self, spec: &ProfileSpec) -> Option<Arc<dyn EmbeddingProvider>> {
        if self.gate.is_some()
            || spec.provider != self.spec.provider
            || spec.endpoint_identity != self.spec.endpoint_identity
        {
            return None;
        }
        self.profile_providers.as_ref()?(spec)
    }

    /// Switches queries to the target profile when it covers EVERY chunk of the currently published, non-excluded
    /// corpus (the same eligibility search applies; an empty corpus is covered). The coverage check and the switch
    /// share one transaction on the single writer connection, so a publication or exclusion that lands during a
    /// migration is judged at the moment of the switch, not by an older snapshot of the corpus. The target is the
    /// profile this daemon was configured with at start-up (`self.profile_id`): configuration is read once per
    /// process and a changed `embedding` section only takes effect after a restart, so a late switch can never
    /// activate a profile the operator has since replaced. Returns whether the switch happened.
    pub async fn activate_if_covered(&self) -> Result<bool, AppError> {
        let storage_err = |_| {
            AppError::new(
                "STORAGE_ERROR",
                "required local state cannot be accessed safely",
            )
        };
        let mut tx = self.index.writer.begin().await.map_err(storage_err)?;
        let active: Option<String> =
            sqlx::query_scalar("SELECT value FROM app_meta WHERE key='active_embedding_profile'")
                .fetch_optional(&mut *tx)
                .await
                .map_err(storage_err)?;
        if active.as_deref() == Some(self.profile_id.as_str()) {
            return Ok(false);
        }
        let uncovered: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM chunks c JOIN sessions s ON s.active_snapshot_id = c.snapshot_id AND s.id = c.session_id \
               JOIN snapshots sn ON sn.id = c.snapshot_id AND sn.state = 'published' \
               WHERE s.excluded = 0 AND NOT EXISTS (SELECT 1 FROM embeddings e WHERE e.profile_id = ? AND e.input_hash = c.input_hash))",
        )
        .bind(&self.profile_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(storage_err)?;
        if uncovered {
            return Ok(false);
        }
        sqlx::query("INSERT INTO app_meta(key, value) VALUES ('active_embedding_profile', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
            .bind(&self.profile_id)
            .execute(&mut *tx)
            .await
            .map_err(storage_err)?;
        bump_epoch(&mut tx).await.map_err(storage_err)?;
        tx.commit().await.map_err(storage_err)?;
        self.refresh_active().await?;
        Ok(true)
    }

    /// One-time startup work shared by both paths: recover interrupted requests, register the profile.
    async fn ready(&self) -> Result<(), EmbedError> {
        self.ready
            .get_or_try_init(|| async {
                self.ledger.sweep_interrupted().await.map_err(storage)?;
                self.ensure_profile().await.map_err(|_| {
                    EmbedError::new(EmbedErrorKind::Unavailable, "index storage error")
                        .retryable(true)
                })?;
                Ok::<(), EmbedError>(())
            })
            .await
            .map(|_| ())
    }

    pub async fn vector_coverage(&self) -> Result<VectorCoverage, AppError> {
        let storage_err = |_| {
            AppError::new(
                "STORAGE_ERROR",
                "required local state cannot be accessed safely",
            )
        };
        let total: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM chunks c JOIN sessions s ON s.active_snapshot_id = c.snapshot_id",
        )
        .fetch_one(&self.index.reader)
        .await
        .map_err(storage_err)?;
        let embedded: i64 = sqlx::query_scalar("SELECT count(*) FROM chunks c JOIN sessions s ON s.active_snapshot_id = c.snapshot_id WHERE EXISTS (SELECT 1 FROM embeddings e WHERE e.profile_id = ? AND e.input_hash = c.input_hash)")
            .bind(&self.profile_id)
            .fetch_one(&self.index.reader)
            .await
            .map_err(storage_err)?;
        Ok(VectorCoverage {
            profile_id: self.profile_id.clone(),
            total_chunks: total,
            embedded_chunks: embedded,
            pending_chunks: total - embedded,
        })
    }

    // ---- tokenization ------------------------------------------------------------------------------------

    async fn tokenize<T: Send + 'static>(
        &self,
        items: Vec<T>,
        text: fn(&T) -> &str,
    ) -> Result<(Vec<T>, Vec<usize>), EmbedError> {
        let permit = self
            .cpu_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| EmbedError::new(EmbedErrorKind::Cancelled, "shutting down"))?;
        let tok = self.tok.clone();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let counts = items.iter().map(|i| tok.count(text(i))).collect();
            (items, counts)
        })
        .await
        .map_err(|_| EmbedError::new(EmbedErrorKind::Cancelled, "tokenization was interrupted"))
    }

    // ---- dispatch ----------------------------------------------------------------------------------------

    /// Reserve -> dispatched -> provider -> completed/failed/ambiguous. Every outcome updates health.
    async fn dispatch(
        &self,
        profile_id: &str,
        provider: &dyn EmbeddingProvider,
        purpose: Purpose,
        tokens: u64,
        inputs: &[String],
        dispatched_flag: Option<&AtomicBool>,
    ) -> Result<(EmbedResponse, u64), EmbedError> {
        let now = self.clock.now_ms();
        let fail = |e: EmbedError| {
            self.health.failure(&e, now);
            e
        };
        let (outcome, res) = self
            .ledger
            .reserve(profile_id, purpose, tokens, self.limit(purpose))
            .await
            .map_err(|e| fail(storage(e)))?;
        let Reservation { id, .. } = match (outcome, res) {
            (Reserve::Reserved, Some(r)) => r,
            _ => {
                let wait = Duration::from_millis((next_reset_ms(now, self.tz) - now).max(0) as u64);
                return Err(fail(
                    EmbedError::new(
                        EmbedErrorKind::BudgetExhausted,
                        "daily token budget exhausted",
                    )
                    .retryable(true)
                    .retry_after(Some(wait)),
                ));
            }
        };
        let mut guard = InFlight {
            ledger: self.ledger.clone(),
            id: id.clone(),
            dispatched: false,
            armed: true,
        };
        // The dispatched state is durable before the request can leave the process.
        if let Err(e) = self.ledger.dispatched(&id).await {
            let _ = self.ledger.failed(&id, "storage").await;
            guard.armed = false;
            return Err(fail(storage(e)));
        }
        guard.dispatched = true;
        if let Some(f) = dispatched_flag {
            f.store(true, Ordering::SeqCst);
        }
        let limit =
            Duration::from_secs(self.cfg.request_timeout_seconds.max(1)) + OUTER_TIMEOUT_SLACK;
        let result = match tokio::time::timeout(limit, provider.embed(purpose, inputs)).await {
            Ok(r) => r,
            Err(_) => Err(
                EmbedError::new(EmbedErrorKind::Timeout, "request timed out after send")
                    .retryable(true)
                    .ambiguous(true),
            ),
        };
        let out = match result {
            Ok(resp) => {
                let used = resp.usage_tokens.unwrap_or(tokens);
                let _ = self.ledger.completed(&id, Some(used)).await;
                self.health.success();
                Ok((resp, used))
            }
            Err(e) => {
                let _ = if e.ambiguous {
                    self.ledger.ambiguous(&id, e.kind.as_str()).await
                } else {
                    self.ledger.failed(&id, e.kind.as_str()).await
                };
                Err(fail(e))
            }
        };
        guard.armed = false;
        self.refresh_usage().await;
        out
    }

    // ---- queries -----------------------------------------------------------------------------------------

    pub async fn embed_query(
        &self,
        text: &str,
        deadline: Duration,
    ) -> Result<Vec<f32>, EmbedError> {
        self.embed_active_query(text, deadline)
            .await
            .map(|(_, v)| v)
    }

    /// Embeds `text` for the ACTIVE profile and returns that profile's id with the vector. A caller must search
    /// stored vectors of exactly that profile (the active profile may switch between this call and the search).
    pub async fn embed_active_query(
        &self,
        text: &str,
        deadline: Duration,
    ) -> Result<(String, Vec<f32>), EmbedError> {
        self.gate_error()?;
        if text.trim().is_empty() {
            return Err(EmbedError::new(EmbedErrorKind::BadResponse, "empty query"));
        }
        self.ready().await?;
        let active = self.active.read().clone();
        let now = self.clock.now_ms();
        if let Some(v) = self.cache.lock().get(&active.id, text, now) {
            return Ok((active.id.clone(), v));
        }
        self.health.admit(now)?;
        let dispatched = AtomicBool::new(false);
        match tokio::time::timeout(deadline, self.query_uncached(text, &dispatched, &active)).await
        {
            Ok(Ok(v)) => {
                self.cache
                    .lock()
                    .put(&active.id, text, v.clone(), self.clock.now_ms());
                Ok((active.id.clone(), v))
            }
            Ok(Err(e)) => Err(e),
            Err(_) => {
                self.refresh_usage().await;
                let sent = dispatched.load(Ordering::SeqCst);
                let e = EmbedError::new(
                    if sent {
                        EmbedErrorKind::Timeout
                    } else {
                        EmbedErrorKind::Unavailable
                    },
                    "query embedding deadline exceeded",
                )
                .retryable(true)
                .ambiguous(sent);
                // Only a request that actually went out says something about provider health.
                self.health.failure(
                    &if sent {
                        e.clone()
                    } else {
                        EmbedError::new(EmbedErrorKind::Cancelled, "deadline")
                    },
                    self.clock.now_ms(),
                );
                Err(e)
            }
        }
    }

    async fn query_uncached(
        &self,
        text: &str,
        dispatched: &AtomicBool,
        active: &Active,
    ) -> Result<Vec<f32>, EmbedError> {
        let release = |e: EmbedError| {
            self.health.failure(
                &EmbedError::new(EmbedErrorKind::Cancelled, "released"),
                self.clock.now_ms(),
            );
            e
        };
        let _permit = self
            .query_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| EmbedError::new(EmbedErrorKind::Cancelled, "shutting down"))?;
        let Some(provider) = active.provider.clone() else {
            return Err(release(EmbedError::new(
                EmbedErrorKind::Unavailable,
                "the active embedding profile has no usable provider",
            )));
        };
        let max = provider.limits().max_tokens_per_input;
        let (mut items, mut counts) = self
            .tokenize(vec![text.to_string()], |s| s.as_str())
            .await
            .map_err(release)?;
        if counts[0] > max {
            // Queries are never hashed or stored, so truncating an oversized query is safe.
            let cut = self.tok.prefix_within(&items[0], max);
            items[0].truncate(cut);
            counts[0] = self.tok.count(&items[0]);
        }
        let (resp, _) = self
            .dispatch(
                &active.id,
                &*provider,
                Purpose::Query,
                counts[0] as u64,
                &items,
                Some(dispatched),
            )
            .await?;
        resp.vectors.into_iter().next().ok_or_else(|| {
            EmbedError::new(EmbedErrorKind::BadResponse, "provider returned no vector")
        })
    }

    /// Explicit connectivity probe with fixed text. Bypasses (and on success clears) an auth block.
    pub async fn probe(&self) -> Result<ProbeOutcome, EmbedError> {
        self.gate_error()?;
        self.ready().await?;
        let started = Instant::now();
        let tokens = self.tok.count(PROBE_TEXT) as u64;
        let _permit = self
            .query_sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| EmbedError::new(EmbedErrorKind::Cancelled, "shutting down"))?;
        let (resp, _) = self
            .dispatch(
                &self.profile_id,
                &*self.provider,
                Purpose::Query,
                tokens,
                &[PROBE_TEXT.to_string()],
                None,
            )
            .await?;
        self.health.reset();
        Ok(ProbeOutcome {
            dimensions: resp.vectors.first().map_or(0, Vec::len),
            latency_ms: started.elapsed().as_millis() as u64,
        })
    }

    // ---- documents ---------------------------------------------------------------------------------------

    async fn cached_hashes(&self, hashes: &[Digest]) -> Result<HashSet<Digest>, EmbedError> {
        let mut out = HashSet::new();
        for chunk in hashes.chunks(500) {
            let mut qb = QueryBuilder::<Sqlite>::new(
                "SELECT input_hash FROM embeddings WHERE profile_id = ",
            );
            qb.push_bind(&self.profile_id).push(" AND input_hash IN (");
            let mut sep = qb.separated(", ");
            for h in chunk {
                sep.push_bind(h.to_vec());
            }
            sep.push_unseparated(")");
            let rows: Vec<Vec<u8>> = qb
                .build_query_scalar()
                .fetch_all(&self.index.reader)
                .await
                .map_err(storage)?;
            out.extend(
                rows.into_iter()
                    .filter_map(|r| Digest::try_from(r.as_slice()).ok()),
            );
        }
        Ok(out)
    }

    async fn commit_batch(
        &self,
        batch: &[(Digest, String, usize)],
        vectors: &[Vec<f32>],
    ) -> Result<usize, EmbedError> {
        let now = self.clock.now_ms();
        let mut tx = self.index.writer.begin().await.map_err(storage)?;
        let mut inserted = 0;
        for ((hash, _, _), v) in batch.iter().zip(vectors) {
            let r = sqlx::query("INSERT INTO embeddings(profile_id, input_hash, dimensions, vec, created_at) VALUES (?,?,?,?,?) ON CONFLICT(profile_id, input_hash) DO NOTHING")
                .bind(&self.profile_id)
                .bind(hash.to_vec())
                .bind(v.len() as i64)
                .bind(super::vectors::encode_le_f32(v))
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            inserted += r.rows_affected() as usize;
        }
        bump_epoch(&mut tx).await.map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(inserted)
    }

    /// Embeds `(input_hash, rendered text)` pairs that are not cached yet for the TARGET profile (the configured
    /// one). Each validated batch is committed in its own short transaction, so an error or cancellation keeps
    /// everything committed before it. Afterwards, while a profile migration is running, the switch to the target
    /// profile happens if it now covers the currently published corpus (spec 10.6).
    pub async fn embed_documents(
        &self,
        inputs: Vec<(Digest, String)>,
        cancel: &AtomicBool,
    ) -> Result<EmbedProgress, EmbedError> {
        let prog = self.embed_pending(inputs, cancel).await?;
        let _ = self.activate_if_covered().await;
        Ok(prog)
    }

    async fn embed_pending(
        &self,
        inputs: Vec<(Digest, String)>,
        cancel: &AtomicBool,
    ) -> Result<EmbedProgress, EmbedError> {
        self.gate_error()?;
        self.ready().await?;
        let mut prog = EmbedProgress::default();
        let mut seen = HashSet::new();
        let mut pending = Vec::new();
        for (h, t) in inputs {
            if !seen.insert(h) {
                continue;
            }
            if t.trim().is_empty() {
                prog.rejected += 1;
            } else {
                pending.push((h, t));
            }
        }
        let hashes: Vec<Digest> = pending.iter().map(|(h, _)| *h).collect();
        let cached = self.cached_hashes(&hashes).await?;
        prog.cached = cached.len();
        pending.retain(|(h, _)| !cached.contains(h));
        if pending.is_empty() {
            return Ok(prog);
        }

        let limits = self.provider.limits();
        let (items, counts) = self.tokenize(pending, |p| p.1.as_str()).await?;
        let max_inputs = self
            .cfg
            .batch_max_inputs
            .min(limits.max_inputs_per_request)
            .max(1);
        let max_tokens = self.cfg.batch_max_tokens.min(limits.max_tokens_per_request);
        let mut batches: Vec<Vec<(Digest, String, usize)>> = vec![Vec::new()];
        let mut cur_tokens = 0;
        for ((h, t), n) in items.into_iter().zip(counts) {
            if n > limits.max_tokens_per_input || n > max_tokens {
                prog.rejected += 1;
                continue;
            }
            let cur = batches.last_mut().expect("at least one batch");
            if !cur.is_empty() && (cur.len() >= max_inputs || cur_tokens + n > max_tokens) {
                batches.push(Vec::new());
                cur_tokens = 0;
            }
            cur_tokens += n;
            batches
                .last_mut()
                .expect("at least one batch")
                .push((h, t, n));
        }
        batches.retain(|b| !b.is_empty());

        for (i, batch) in batches.iter().enumerate() {
            let remaining: usize = batches[i..].iter().map(Vec::len).sum();
            if cancel.load(Ordering::SeqCst) {
                prog.cancelled = true;
                prog.deferred += remaining;
                return Ok(prog);
            }
            self.health.admit(self.clock.now_ms())?;
            let _permit = self
                .ingest_sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| EmbedError::new(EmbedErrorKind::Cancelled, "shutting down"))?;
            let texts: Vec<String> = batch.iter().map(|(_, t, _)| t.clone()).collect();
            let est: u64 = batch.iter().map(|(_, _, n)| *n as u64).sum();
            match self
                .dispatch(
                    &self.profile_id,
                    &*self.provider,
                    Purpose::Ingest,
                    est,
                    &texts,
                    None,
                )
                .await
            {
                Ok((resp, used)) => {
                    let inserted = self.commit_batch(batch, &resp.vectors).await?;
                    prog.embedded += inserted;
                    prog.cached += batch.len() - inserted;
                    prog.usage_tokens += used;
                }
                Err(e) if e.kind == EmbedErrorKind::BudgetExhausted => {
                    prog.budget_exhausted = true;
                    prog.resets_at_ms = Some(next_reset_ms(self.clock.now_ms(), self.tz));
                    prog.deferred += remaining;
                    return Ok(prog);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(prog)
    }
}

/// Standalone connectivity probe (`spool doctor --probe-provider`): no index, fixed harmless text, refuses
/// unless the consent gate is open and the credential file is usable.
pub async fn probe_provider(
    cfg: &EmbeddingConfig,
    credential_file: &Path,
) -> Result<ProbeOutcome, EmbedError> {
    if !cfg.enabled {
        return Err(EmbedError::new(
            EmbedErrorKind::Disabled,
            "embeddings are disabled",
        ));
    }
    if !cfg.hosted_processing_approved {
        return Err(EmbedError::new(
            EmbedErrorKind::NotApproved,
            "hosted embedding is not approved",
        ));
    }
    let key = read_credential(credential_file).map_err(|_| {
        EmbedError::new(
            EmbedErrorKind::NoCredential,
            "no usable provider credential",
        )
    })?;
    let provider = OpenAiProvider::new(cfg, &key).map_err(|_| {
        EmbedError::new(
            EmbedErrorKind::UnsupportedModel,
            "invalid embedding endpoint configuration",
        )
    })?;
    let started = Instant::now();
    let limit = Duration::from_secs(cfg.request_timeout_seconds.max(1)) + OUTER_TIMEOUT_SLACK;
    let inputs = [PROBE_TEXT.to_string()];
    let resp = tokio::time::timeout(limit, provider.embed(Purpose::Query, &inputs))
        .await
        .map_err(|_| {
            EmbedError::new(EmbedErrorKind::Timeout, "request timed out after send")
                .retryable(true)
                .ambiguous(true)
        })??;
    Ok(ProbeOutcome {
        dimensions: resp.vectors.first().map_or(0, Vec::len),
        latency_ms: started.elapsed().as_millis() as u64,
    })
}
