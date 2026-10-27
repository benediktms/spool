//! Locations and (later) the validated TOML configuration.

use std::path::{Path, PathBuf};

/// Max bytes of a `sockaddr_un` path on Unix targets (macOS is the smallest: 104 incl. NUL).
#[cfg(unix)]
pub const SOCKET_PATH_LIMIT: usize = 100;

fn home_dir() -> anyhow::Result<PathBuf> {
    #[cfg(windows)]
    {
        std::env::var_os("USERPROFILE")
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("USERPROFILE is not set"))
    }
    #[cfg(unix)]
    {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or_else(|| anyhow::anyhow!("HOME is not set"))
    }
}

#[derive(Debug, Clone)]
pub struct Paths {
    pub root: PathBuf,
    pub config_file: PathBuf,
    pub state_dir: PathBuf,
    pub socket: PathBuf,
    pub lock: PathBuf,
    pub index_db: PathBuf,
    pub jobs_db: PathBuf,
    pub secrets_dir: PathBuf,
    pub signing_key: PathBuf,
}

impl Paths {
    /// `SPOOL_HOME` overrides the platform default (tests, isolated runs).
    pub fn discover() -> anyhow::Result<Self> {
        if let Some(home) = std::env::var_os("SPOOL_HOME") {
            return Self::from_root(PathBuf::from(home));
        }
        #[cfg(windows)]
        let root = std::env::var_os("LOCALAPPDATA")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("USERPROFILE")
                    .map(|path| PathBuf::from(path).join("AppData/Local"))
            })
            .ok_or_else(|| anyhow::anyhow!("LOCALAPPDATA or USERPROFILE is not set"))?
            .join("Spool");
        #[cfg(target_os = "macos")]
        let root = home_dir()?.join("Library/Application Support/Spool");
        #[cfg(all(unix, not(target_os = "macos")))]
        let root = {
            let home = home_dir()?;
            std::env::var_os("XDG_STATE_HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join(".local/state"))
                .join("spool")
        };
        Self::from_root(root)
    }

    pub fn from_root(root: PathBuf) -> anyhow::Result<Self> {
        let state_dir = root.join("state");
        let secrets_dir = root.join("secrets");
        let p = Self {
            config_file: root.join("config.toml"),
            socket: state_dir.join("spool.sock"),
            lock: state_dir.join("daemon.lock"),
            index_db: state_dir.join("index.sqlite"),
            jobs_db: state_dir.join("jobs.sqlite"),
            signing_key: secrets_dir.join("handle-signing.key"),
            secrets_dir,
            state_dir,
            root,
        };
        p.validate()?;
        Ok(p)
    }
    pub fn validate(&self) -> anyhow::Result<()> {
        #[cfg(unix)]
        {
            let n = self.socket.as_os_str().len();
            anyhow::ensure!(
                n <= SOCKET_PATH_LIMIT,
                "socket path {} is {n} bytes; the platform limit is {SOCKET_PATH_LIMIT}. Set SPOOL_HOME to a shorter directory",
                self.socket.display()
            );
        }
        Ok(())
    }
}

#[cfg(unix)]
pub fn mode_of(p: &Path) -> std::io::Result<u32> {
    use std::os::unix::fs::PermissionsExt;
    Ok(std::fs::metadata(p)?.permissions().mode() & 0o777)
}

// ---------------------------------------------------------------------------------------------
// TOML configuration (spec 16.1)
// ---------------------------------------------------------------------------------------------

use serde::{Deserialize, Serialize};

macro_rules! cfg_struct {
    ($(#[$m:meta])* $name:ident { $($(#[$fm:meta])* $f:ident : $t:ty = $d:expr),* $(,)? }) => {
        $(#[$m])*
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        #[serde(deny_unknown_fields, default)]
        pub struct $name { $($(#[$fm])* pub $f: $t),* }
        impl Default for $name { fn default() -> Self { Self { $($f: $d),* } } }
    };
}

cfg_struct!(IpcConfig {
    socket_path: String = "state/spool.sock".into(),
    request_concurrency: usize = 16,
    max_request_bytes: usize = 1_048_576
});
cfg_struct!(McpConfig {
    allow_raw_records: bool = false
});
cfg_struct!(DiscoveryConfig {
    bootstrap_days: u32 = 7,
    interval_seconds: u64 = 60,
    debounce_seconds: u64 = 5,
    watermark_overlap_seconds: u64 = 300
});
cfg_struct!(IngestionConfig {
    session_concurrency: usize = 1,
    max_queued_sessions: usize = 10_000,
    max_record_bytes: u64 = 16 * 1024 * 1024,
    max_source_file_bytes: u64 = 2 * 1024 * 1024 * 1024,
    max_transient_attempts: u32 = 5,
});
cfg_struct!(ChunkingConfig {
    target_new_tokens: usize = 800,
    max_rendered_tokens: usize = 1200,
    overlap_tokens: usize = 200,
    version: u32 = 1
});
cfg_struct!(EmbeddingConfig {
    enabled: bool = true,
    hosted_processing_approved: bool = false,
    provider: String = "openai".into(),
    model: String = "text-embedding-3-small".into(),
    dimensions: usize = 768,
    profile_generation: u32 = 1,
    credential_file: String = "secrets/provider.key".into(),
    /// Base URL of the embeddings API. Overridable so tests can point at a local fake server.
    endpoint: String = "https://api.openai.com/v1/embeddings".into(),
    batch_max_inputs: usize = 32,
    batch_max_tokens: usize = 24_000,
    ingestion_request_concurrency: usize = 1,
    query_request_concurrency: usize = 2,
    request_timeout_seconds: u64 = 30,
    daily_ingestion_token_budget: u64 = 1_000_000,
    daily_query_token_budget: u64 = 50_000,
    budget_timezone: String = "UTC".into(),
});
cfg_struct!(SearchConfig {
    default_mode: String = "hybrid".into(),
    default_limit: usize = 10,
    max_limit: usize = 50,
    max_matches_per_session: usize = 3,
    candidate_limit: usize = 200,
    max_candidate_limit: usize = 2000,
    rrf_constant: u32 = 60,
    semantic_scan_concurrency: usize = 2,
    vector_batch_bytes: usize = 8 * 1024 * 1024,
    query_embedding_fallback_ms: u64 = 2000,
    semantic_query_deadline_ms: u64 = 5000,
    query_cache_entries: usize = 256,
    query_cache_ttl_seconds: u64 = 600,
    max_search_response_bytes: usize = 65_536,
});
cfg_struct!(ReaderConfig {
    lease_ttl_seconds: u64 = 1800,
    max_active_leases: usize = 256,
    max_retained_lease_bytes: u64 = 268_435_456,
    /// Local I/O bound for one read: bytes of source scanned when no direct locator works.
    max_source_read_bytes: u64 = 64 * 1024 * 1024,
    /// Local wall-clock bound for one read.
    max_read_millis: u64 = 10_000,
});
cfg_struct!(StorageConfig {
    warning_bytes: u64 = 1 << 30,
    pause_ingestion_bytes: u64 = 2 << 30,
    busy_timeout_ms: u64 = 5000
});
cfg_struct!(RuntimeConfig {
    async_worker_threads: usize = 2
});
cfg_struct!(LoggingConfig {
    level: String = "info".into(),
    include_content: bool = false,
    max_file_bytes: u64 = 10_485_760,
    retained_files: usize = 5
});

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub id: String,
    pub kind: crate::domain::HarnessKind,
    #[serde(default)]
    pub enabled: bool,
    pub roots: Vec<String>,
    #[serde(default)]
    pub include_subagents: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub config_version: u32,
    pub ipc: IpcConfig,
    pub mcp: McpConfig,
    pub runtime: RuntimeConfig,
    pub discovery: DiscoveryConfig,
    pub ingestion: IngestionConfig,
    pub chunking: ChunkingConfig,
    pub embedding: EmbeddingConfig,
    pub search: SearchConfig,
    pub reader: ReaderConfig,
    pub storage: StorageConfig,
    pub logging: LoggingConfig,
    pub sources: Vec<SourceConfig>,
}

impl Default for Config {
    fn default() -> Self {
        let src = |id: &str, kind, enabled, root: &str| SourceConfig {
            id: id.into(),
            kind,
            enabled,
            roots: vec![root.into()],
            include_subagents: false,
        };
        use crate::domain::HarnessKind::*;
        Self {
            config_version: 1,
            ipc: Default::default(),
            mcp: Default::default(),
            runtime: Default::default(),
            discovery: Default::default(),
            ingestion: Default::default(),
            chunking: Default::default(),
            embedding: Default::default(),
            search: Default::default(),
            reader: Default::default(),
            storage: Default::default(),
            logging: Default::default(),
            sources: vec![
                src("omp", Omp, true, "~/.omp/agent/sessions"),
                src("codex", Codex, false, "~/.codex/sessions"),
                src("claude", Claude, false, "~/.claude/projects"),
            ],
        }
    }
}

/// OpenAI documents 8,191 input tokens per embedding input for text-embedding-3 models.
pub const PROVIDER_MAX_INPUT_TOKENS: usize = 8191;

impl Config {
    /// Parse a TOML document (unknown fields rejected) and validate it. Relative paths are
    /// resolved against `base` (the config directory); `~/` is expanded without a shell.
    pub fn parse(text: &str, base: &Path) -> anyhow::Result<Self> {
        let mut c: Config =
            toml::from_str(text).map_err(|e| anyhow::anyhow!("config.toml: {}", e.message()))?;
        c.resolve_paths(base)?;
        c.validate()?;
        Ok(c)
    }

    pub fn load(paths: &Paths) -> anyhow::Result<Self> {
        match std::fs::read_to_string(&paths.config_file) {
            Ok(t) => Self::parse(&t, &paths.root),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut c = Config::default();
                c.resolve_paths(&paths.root)?;
                c.validate()?;
                Ok(c)
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn resolve_paths(&mut self, base: &Path) -> anyhow::Result<()> {
        for s in &mut self.sources {
            for r in &mut s.roots {
                *r = expand_path(r, base)?.to_string_lossy().into_owned();
            }
        }
        Ok(())
    }

    pub fn socket_path(&self, base: &Path) -> anyhow::Result<PathBuf> {
        expand_path(&self.ipc.socket_path, base)
    }

    pub fn credential_path(&self, base: &Path) -> anyhow::Result<PathBuf> {
        expand_path(&self.embedding.credential_file, base)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        use anyhow::ensure;
        ensure!(
            self.config_version == 1,
            "unsupported config_version {}",
            self.config_version
        );
        let ch = &self.chunking;
        ensure!(
            ch.target_new_tokens > 0 && ch.target_new_tokens <= ch.max_rendered_tokens,
            "chunking.target_new_tokens must be in 1..=max_rendered_tokens"
        );
        ensure!(
            ch.overlap_tokens < ch.max_rendered_tokens,
            "chunking.overlap_tokens must be below max_rendered_tokens"
        );
        ensure!(
            ch.max_rendered_tokens <= PROVIDER_MAX_INPUT_TOKENS,
            "chunking.max_rendered_tokens {} exceeds the provider input limit {PROVIDER_MAX_INPUT_TOKENS}",
            ch.max_rendered_tokens
        );
        let e = &self.embedding;
        ensure!(
            e.dimensions > 0 && e.dimensions <= 3072,
            "embedding.dimensions must be in 1..=3072"
        );
        ensure!(
            e.batch_max_inputs > 0 && e.batch_max_inputs <= 2048,
            "embedding.batch_max_inputs must be in 1..=2048"
        );
        ensure!(
            e.batch_max_tokens >= ch.max_rendered_tokens,
            "embedding.batch_max_tokens must be at least chunking.max_rendered_tokens"
        );
        ensure!(
            e.ingestion_request_concurrency >= 1 && e.query_request_concurrency >= 1,
            "embedding concurrency must be >= 1"
        );
        ensure!(
            e.provider == "openai",
            "unsupported embedding.provider {:?}",
            e.provider
        );
        crate::clock::Tz::parse(&e.budget_timezone)
            .map_err(|m| anyhow::anyhow!("embedding.budget_timezone: {m}"))?;
        let s = &self.search;
        ensure!(
            ["hybrid", "lexical", "semantic", "literal"].contains(&s.default_mode.as_str()),
            "invalid search.default_mode"
        );
        ensure!(
            s.default_limit >= 1 && s.default_limit <= s.max_limit && s.max_limit <= 50,
            "search limits must satisfy 1 <= default_limit <= max_limit <= 50"
        );
        ensure!(
            s.candidate_limit >= 1 && s.candidate_limit <= s.max_candidate_limit,
            "search.candidate_limit must be <= max_candidate_limit"
        );
        ensure!(
            s.max_matches_per_session >= 1 && s.max_matches_per_session <= 3,
            "search.max_matches_per_session must be in 1..=3"
        );
        ensure!(
            s.max_search_response_bytes >= 4096,
            "search.max_search_response_bytes too small"
        );
        let r = &self.reader;
        ensure!(
            r.lease_ttl_seconds >= 1
                && r.max_active_leases >= 1
                && r.max_retained_lease_bytes >= 1024,
            "invalid reader lease bounds"
        );
        ensure!(
            self.ipc.request_concurrency >= 1 && self.ipc.max_request_bytes >= 4096,
            "invalid ipc limits"
        );
        ensure!(
            self.ingestion.session_concurrency == 1,
            "ingestion.session_concurrency must be 1 in v1"
        );
        ensure!(
            self.ingestion.max_record_bytes >= 4096
                && self.ingestion.max_record_bytes <= self.ingestion.max_source_file_bytes,
            "invalid ingestion size limits"
        );
        ensure!(
            self.storage.warning_bytes <= self.storage.pause_ingestion_bytes,
            "storage.warning_bytes must not exceed pause_ingestion_bytes"
        );
        ensure!(
            self.runtime.async_worker_threads >= 1,
            "runtime.async_worker_threads must be >= 1"
        );
        let mut seen = std::collections::HashSet::new();
        for src in &self.sources {
            ensure!(
                !src.id.is_empty()
                    && src
                        .id
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
                "invalid source id {:?}",
                src.id
            );
            ensure!(seen.insert(&src.id), "duplicate source id {:?}", src.id);
            ensure!(!src.roots.is_empty(), "source {:?} has no roots", src.id);
            for root in &src.roots {
                ensure!(
                    Path::new(root).is_absolute(),
                    "source {:?} root {root:?} is not absolute",
                    src.id
                );
            }
        }
        Ok(())
    }

    /// Redacted TOML for `config show`: no credential material exists in the config (only the
    /// credential *path*), so redaction hides nothing else; kept explicit for future fields.
    pub fn show(&self) -> String {
        toml::to_string_pretty(self).unwrap_or_default()
    }

    /// Fingerprint inputs that invalidate extraction/chunking output when changed (spec 9.4).
    pub fn pipeline_fingerprint(
        &self,
        parser_versions: &[(&str, &str)],
        tokenizer: &str,
    ) -> String {
        let mut h = blake3::Hasher::new();
        h.update(b"spool-pipeline-v1");
        for (k, v) in parser_versions {
            h.update(k.as_bytes())
                .update(b"=")
                .update(v.as_bytes())
                .update(b";");
        }
        let c = &self.chunking;
        h.update(
            format!(
                "chunk:{}:{}:{}:{}:{tokenizer}",
                c.version, c.target_new_tokens, c.max_rendered_tokens, c.overlap_tokens
            )
            .as_bytes(),
        );
        h.update(b"extraction-policy:1");
        h.finalize().to_hex()[..32].to_string()
    }
}

/// Expand `~/` and resolve relative paths against `base`. Never invokes a shell or expands `$`/`` ` ``.
pub fn expand_path(raw: &str, base: &Path) -> anyhow::Result<PathBuf> {
    if let Some(rest) = raw
        .strip_prefix("~/")
        .or(if raw == "~" { Some("") } else { None })
    {
        let home = home_dir()?;
        return Ok(home.join(rest));
    }
    let p = Path::new(raw);
    Ok(if p.is_absolute() {
        p.to_path_buf()
    } else {
        base.join(p)
    })
}
