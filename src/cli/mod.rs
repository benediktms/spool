//! The `spool` command line (spec 16.2). Daemon-dependent commands go through the Unix socket
//! (`ipc::client::call`); `init`, `doctor`, `config show`, `service`, `logs` and `mcp config`
//! work from files alone.

pub mod args;
pub mod confshow;
pub mod doctor;
pub mod init;
pub mod logs;
pub mod mcpcfg;
pub mod remote;
pub mod requests;
pub mod timeparse;

use crate::clock::{self, SharedClock};
use crate::config::{Config, Paths};
use crate::service::{ServiceConfig, ServiceManager, SystemRunner};
use args::{
    Cmd, ConfigCmd, DaemonCmd, HandlesCmd, IndexArgs, JobsCmd, McpCmd, ServiceCmd, SourcesCmd,
};
use clap::Parser;
use remote::{CliError, Remote, print_json, render_kv};
use serde_json::{Value, json};
use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

pub use args::Cli;

/// Process entry point: parse, build a runtime sized by `runtime.async_worker_threads`, run.
pub fn main() -> ExitCode {
    let cli = Cli::parse();
    let paths = match Paths::discover() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("spool: {e:#}");
            return ExitCode::from(remote::EXIT_FAILURE);
        }
    };
    let threads = Config::load(&paths)
        .map(|c| c.runtime.async_worker_threads)
        .unwrap_or(2);
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("spool: cannot start the async runtime: {e}");
            return ExitCode::from(remote::EXIT_FAILURE);
        }
    };
    rt.block_on(async {
        match execute(cli, paths, clock::system()).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                if !e.message.is_empty() {
                    eprintln!("spool: {}", e.message);
                }
                e.exit_code()
            }
        }
    })
}

fn stderr_logging(default_level: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| default_level.into());
    // stdout is reserved for command output (and MCP frames for `spool mcp`).
    let _ = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_env_filter(filter)
        .try_init();
}

fn load_config(paths: &Paths) -> Result<Config, CliError> {
    Config::load(paths).map_err(|e| CliError::new(format!("{e:#}")))
}

fn remote_for(paths: &Paths) -> Result<Remote, CliError> {
    let cfg = load_config(paths)?;
    Ok(Remote::new(paths.socket.clone(), cfg.ipc.max_request_bytes))
}

fn service_manager(paths: &Paths) -> Result<ServiceManager, CliError> {
    let cfg = ServiceConfig::for_current_user(paths)?;
    Ok(ServiceManager::new(cfg, Arc::new(SystemRunner))?)
}

pub async fn execute(cli: Cli, paths: Paths, clock: SharedClock) -> Result<(), CliError> {
    let tz = cli.tz;
    match cli.cmd {
        Cmd::Init(a) => {
            let mut prompter = init::StdinPrompter;
            let interactive = std::io::stdin().is_terminal() && std::io::stdout().is_terminal();
            let p: Option<&mut dyn init::Prompter> = if interactive {
                Some(&mut prompter)
            } else {
                None
            };
            init::print_report(&init::run_init(&paths, &a, p)?);
        }
        Cmd::Daemon {
            action: DaemonCmd::Run,
        } => {
            let cfg = load_config(&paths)?;
            crate::telemetry::init(&paths, &cfg.logging)?;
            crate::daemon::run_daemon(paths, cfg).await?;
        }
        Cmd::Service { action } => {
            let m = service_manager(&paths)?;
            let msg = match action {
                ServiceCmd::Install => m.install()?,
                ServiceCmd::Uninstall => m.uninstall()?,
                ServiceCmd::Start => m.start()?,
                ServiceCmd::Stop => m.stop()?,
                ServiceCmd::Restart => m.restart()?,
            };
            println!("{msg}");
        }
        Cmd::Status { json } => {
            stderr_logging("warn");
            let v = remote_for(&paths)?.control("status", json!({})).await?;
            if json {
                print_json(&v);
            } else {
                let mut s = String::new();
                render_kv(&v, 0, &mut s);
                print!("{s}");
            }
        }
        Cmd::Logs { follow, lines } => logs::run(&paths, follow, lines).await?,
        Cmd::Doctor {
            json,
            probe_provider,
        } => {
            let svc = service_manager(&paths).map_err(|e| e.message);
            let report = doctor::run(&paths, svc, probe_provider).await;
            if json {
                print_json(&serde_json::to_value(&report).unwrap_or(Value::Null));
            } else {
                print!("{}", report.render());
            }
            if !report.ok {
                return Err(CliError {
                    exit: remote::EXIT_FAILURE,
                    message: String::new(),
                });
            }
        }
        Cmd::Search(a) => {
            let req = requests::search_request(&a, clock.now_ms(), tz).map_err(CliError::new)?;
            let v = remote_for(&paths)?.tool("search_sessions", req).await?;
            if a.json {
                print_json(&v);
            } else {
                print_search(&v);
            }
        }
        Cmd::Read(a) => {
            let req = requests::read_request(&a).map_err(CliError::new)?;
            print_json(&remote_for(&paths)?.tool("read_session", req).await?);
        }
        Cmd::Index(a) => index(&paths, a, clock.now_ms(), tz).await?,
        Cmd::Jobs { action } => {
            let (cmd, id) = match action {
                JobsCmd::Status { operation_id } => ("jobs_status", operation_id),
                JobsCmd::Retry { operation_id } => ("jobs_retry", operation_id),
                JobsCmd::Cancel { operation_id } => ("jobs_cancel", operation_id),
            };
            print_json(
                &remote_for(&paths)?
                    .control(cmd, json!({ "operation_id": id }))
                    .await?,
            );
        }
        Cmd::Sources { action } => {
            let (cmd, args) = match action {
                SourcesCmd::List => ("sources_list", json!({})),
                SourcesCmd::Enable { source_id } => ("sources_enable", json!({ "id": source_id })),
                SourcesCmd::Disable { source_id } => {
                    ("sources_disable", json!({ "id": source_id }))
                }
            };
            print_json(&remote_for(&paths)?.control(cmd, args).await?);
        }
        Cmd::Handles {
            action: HandlesCmd::RotateKey,
        } => {
            let v = remote_for(&paths)?
                .control("handles_rotate_key", json!({}))
                .await?;
            print_json(&v);
            println!(
                "note: every previously issued match/message/item/branch reference and cursor is now invalid"
            );
        }
        Cmd::Mcp { sub: None } => {
            stderr_logging("info");
            let max = Config::load(&paths)
                .map(|c| c.ipc.max_request_bytes)
                .unwrap_or(1 << 20);
            crate::mcp::bridge::run_stdio(paths.socket.clone(), max).await?;
        }
        Cmd::Mcp {
            sub: Some(McpCmd::Config { client, bin }),
        } => {
            let bin = match bin {
                Some(b) => b,
                None => std::fs::canonicalize(std::env::current_exe()?)?,
            };
            print!("{}", mcpcfg::render(client, &bin)?);
            eprintln!("note: {}", mcpcfg::placement_hint(client));
            if std::env::var_os("SPOOL_HOME").is_some() {
                eprintln!(
                    "note: SPOOL_HOME is set in this shell; the MCP client must be started with the same SPOOL_HOME"
                );
            }
        }
        Cmd::Config {
            action: ConfigCmd::Show,
        } => print!("{}", confshow::show(&paths)?),
    }
    Ok(())
}

async fn index(
    paths: &Paths,
    a: IndexArgs,
    now_ms: i64,
    tz: crate::clock::Tz,
) -> Result<(), CliError> {
    if a.wait && a.dry_run {
        return Err(CliError::new("--wait does not apply to --dry-run"));
    }
    let call = requests::index_request(&a, now_ms, tz, &crate::ids::new_id("idem"))
        .map_err(CliError::new)?;
    if let Some(ms) = call.since_ms {
        println!(
            "since: {} ({ms} ms since epoch, timezone {})",
            timeparse::format_rfc3339(ms, tz),
            tz.label()
        );
    }
    let remote = remote_for(paths)?;
    let v = remote.control(call.cmd, call.args).await?;
    print_json(&v);
    if a.wait {
        let id = v
            .get("operation_id")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new("the daemon returned no operation_id to wait for"))?;
        remote::wait_for_operation(&remote, id, Duration::from_secs(1)).await?;
    } else if let Some(id) = v.get("operation_id").and_then(Value::as_str) {
        println!(
            "operation {id} submitted; follow it with `spool jobs status {id}` (or pass --wait)"
        );
    }
    Ok(())
}

fn print_search(v: &Value) {
    let s = |x: &Value, k: &str| x.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    println!(
        "mode: {}{}",
        s(v, "mode_used"),
        if s(v, "mode_requested") != s(v, "mode_used") {
            format!(" (requested {})", s(v, "mode_requested"))
        } else {
            String::new()
        }
    );
    for w in v
        .get("warnings")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        println!("warning: {w}");
    }
    for r in v
        .get("results")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let title = r
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or("(untitled)");
        println!(
            "\n{}  [{}]  {}  {}",
            s(r, "session_id"),
            s(r, "source"),
            title,
            s(r, "last_activity_at")
        );
        for m in r
            .get("matches")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            println!("  match_ref: {}", s(m, "match_ref"));
            println!("  excerpt:   {}", s(m, "excerpt").replace('\n', " "));
        }
    }
    if v.get("truncated").and_then(Value::as_bool) == Some(true) {
        println!("\n(results truncated)");
    }
}
