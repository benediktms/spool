//! clap definitions for the CLI surface of spec 16.2. Cross-flag rules live in `requests.rs`
//! (one place, unit-testable, enforced before any daemon call).

use clap::{Args, Parser, Subcommand, ValueEnum};

use crate::clock::Tz;

#[derive(Parser, Debug)]
#[command(
    name = "spool",
    version,
    about = "Local search engine for agent-session conversation history"
)]
pub struct Cli {
    /// Timezone for parsing and printing dates: UTC (default) or a fixed offset such as +02:00.
    #[arg(long, global = true, default_value = "UTC", value_parser = Tz::parse)]
    pub tz: Tz,
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Write config, create state/secrets directories and the handle-signing key.
    Init(InitArgs),
    /// Foreground daemon commands.
    Daemon {
        #[command(subcommand)]
        action: DaemonCmd,
    },
    /// Install and control the per-user service.
    Service {
        #[command(subcommand)]
        action: ServiceCmd,
    },
    /// Show daemon, queue and index status.
    Status {
        #[arg(long)]
        json: bool,
    },
    /// Print the local log file.
    Logs {
        /// Keep printing new lines (Ctrl-C to stop).
        #[arg(long)]
        follow: bool,
        /// Number of trailing lines to print first.
        #[arg(long, default_value_t = 100)]
        lines: usize,
    },
    /// Check configuration, permissions, sources, SQLite, embedding profile, daemon and service.
    Doctor {
        #[arg(long)]
        json: bool,
        /// Send one fixed harmless string to the hosted provider (needs approval + credential).
        #[arg(long)]
        probe_provider: bool,
    },
    /// Search indexed sessions.
    Search(SearchArgs),
    /// Read a session (read_session tool).
    Read(ReadArgs),
    /// Backfill and maintain the index.
    Index(IndexArgs),
    /// Inspect and control durable operations.
    Jobs {
        #[command(subcommand)]
        action: JobsCmd,
    },
    /// List, enable or disable transcript sources.
    Sources {
        #[command(subcommand)]
        action: SourcesCmd,
    },
    /// Signed-handle administration.
    Handles {
        #[command(subcommand)]
        action: HandlesCmd,
    },
    /// Run the stdio MCP bridge, or print client configuration (`mcp config`).
    Mcp {
        #[command(subcommand)]
        sub: Option<McpCmd>,
    },
    /// Inspect configuration.
    Config {
        #[command(subcommand)]
        action: ConfigCmd,
    },
}

#[derive(Args, Debug, Default)]
pub struct InitArgs {
    /// Enable this source (repeatable). Other sources keep their defaults.
    #[arg(long = "enable-source", value_name = "ID")]
    pub enable_source: Vec<String>,
    /// Disable this source (repeatable).
    #[arg(long = "disable-source", value_name = "ID")]
    pub disable_source: Vec<String>,
    /// Permit sending rendered conversation chunks and queries to the hosted embedding provider.
    #[arg(long = "hosted-processing", value_name = "yes|no", value_enum)]
    pub hosted_processing: Option<YesNo>,
    /// Replace an existing config.toml.
    #[arg(long)]
    pub force: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum YesNo {
    Yes,
    No,
}

#[derive(Subcommand, Debug)]
pub enum DaemonCmd {
    /// Run the daemon in the foreground until SIGTERM/SIGINT.
    Run,
}

#[derive(Subcommand, Debug)]
pub enum ServiceCmd {
    Install,
    Uninstall,
    Start,
    Stop,
    Restart,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchMode {
    Hybrid,
    Lexical,
    Semantic,
    Literal,
}

impl SearchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hybrid => "hybrid",
            Self::Lexical => "lexical",
            Self::Semantic => "semantic",
            Self::Literal => "literal",
        }
    }
}

#[derive(Args, Debug)]
pub struct SearchArgs {
    pub query: String,
    #[arg(long, value_enum)]
    pub mode: Option<SearchMode>,
    /// Restrict to a source id (repeatable).
    #[arg(long)]
    pub source: Vec<String>,
    /// Only sessions active after this time: 7d, 12h, 2w, or 2026-10-01[T..] (see --tz).
    #[arg(long)]
    pub since: Option<String>,
    /// Only sessions active before this time (same forms as --since).
    #[arg(long)]
    pub until: Option<String>,
    #[arg(long)]
    pub limit: Option<u32>,
    /// Print the raw tool result.
    #[arg(long)]
    pub json: bool,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
#[value(rename_all = "snake_case")]
pub enum SourcePolicy {
    PreferSource,
    RequireSource,
    IndexedOnly,
}

impl SourcePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreferSource => "prefer_source",
            Self::RequireSource => "require_source",
            Self::IndexedOnly => "indexed_only",
        }
    }
}

#[derive(Args, Debug, Default)]
pub struct ReadArgs {
    /// Session id (omit with --cursor).
    pub session_id: Option<String>,
    #[arg(long)]
    pub match_ref: Option<String>,
    #[arg(long)]
    pub message_ref: Option<String>,
    #[arg(long)]
    pub item_ref: Option<String>,
    #[arg(long)]
    pub from_ref: Option<String>,
    #[arg(long)]
    pub through_ref: Option<String>,
    #[arg(long)]
    pub branch_ref: Option<String>,
    /// Messages before a match/message (0-20, default 2).
    #[arg(long)]
    pub before: Option<u32>,
    /// Messages after a match/message (0-20, default 2).
    #[arg(long)]
    pub after: Option<u32>,
    #[arg(long)]
    pub with_tools: bool,
    /// Privileged raw records (needs mcp.allow_raw_records = true in the daemon).
    #[arg(long)]
    pub raw: bool,
    #[arg(long, value_enum)]
    pub source_policy: Option<SourcePolicy>,
    #[arg(long)]
    pub max_bytes: Option<u32>,
    #[arg(long)]
    pub max_items: Option<u32>,
    /// Continue a previous read; accepts only --max-bytes and --max-items.
    #[arg(long)]
    pub cursor: Option<String>,
}

#[derive(Args, Debug, Default)]
#[command(args_conflicts_with_subcommands = true)]
pub struct IndexArgs {
    #[command(subcommand)]
    pub sub: Option<IndexSub>,
    /// Backfill sessions active since this time (7d, 2026-10-01, ...).
    #[arg(long)]
    pub since: Option<String>,
    /// Backfill everything in the enabled sources.
    #[arg(long)]
    pub all: bool,
    /// Index one session.
    #[arg(long)]
    pub session: Option<String>,
    #[arg(long)]
    pub source: Option<String>,
    /// Report candidate counts and local token estimates; call no provider.
    #[arg(long)]
    pub dry_run: bool,
    /// Poll the operation and print progress. Disconnecting does not cancel it.
    #[arg(long)]
    pub wait: bool,
}

#[derive(Subcommand, Debug)]
pub enum IndexSub {
    /// Rebuild derived data.
    Rebuild {
        #[arg(long, conflicts_with = "embeddings_only")]
        lexical_only: bool,
        #[arg(long)]
        embeddings_only: bool,
    },
    /// Remove a session from the index (the source transcript is never touched).
    Remove {
        #[arg(long)]
        session: String,
        /// Keep it excluded across rediscovery and restart.
        #[arg(long)]
        exclude: bool,
    },
    /// Delete derived data nothing references any more.
    Purge {
        #[arg(long)]
        unreferenced: bool,
    },
}

#[derive(Subcommand, Debug)]
pub enum JobsCmd {
    Status { operation_id: String },
    Retry { operation_id: String },
    Cancel { operation_id: String },
}

#[derive(Subcommand, Debug)]
pub enum SourcesCmd {
    List,
    Enable { source_id: String },
    Disable { source_id: String },
}

#[derive(Subcommand, Debug)]
pub enum HandlesCmd {
    /// Replace the signing key; every issued handle stops working.
    RotateKey,
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum McpClient {
    Claude,
    Codex,
    Omp,
}

#[derive(Subcommand, Debug)]
pub enum McpCmd {
    /// Print (never write) the client configuration for the stdio bridge.
    Config {
        #[arg(long, value_enum)]
        client: McpClient,
        /// Absolute path of the spool binary (default: this executable).
        #[arg(long)]
        bin: Option<std::path::PathBuf>,
    },
}

#[derive(Subcommand, Debug)]
pub enum ConfigCmd {
    /// Print the effective configuration, secrets redacted, defaults vs overrides marked.
    Show,
}
