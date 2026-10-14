//! Daemon access for CLI commands: everything goes through `ipc::client::call`.

use crate::ipc::client::{ClientError, call};
use crate::ipc::{AppError, Request};
use serde_json::Value;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

pub const EXIT_FAILURE: u8 = 1;
pub const EXIT_UNAVAILABLE: u8 = 3;

#[derive(Debug)]
pub struct CliError {
    pub exit: u8,
    pub message: String,
}

impl CliError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            exit: EXIT_FAILURE,
            message: message.into(),
        }
    }
    pub fn exit_code(&self) -> ExitCode {
        ExitCode::from(self.exit)
    }
}

impl From<anyhow::Error> for CliError {
    fn from(e: anyhow::Error) -> Self {
        Self::new(format!("{e:#}"))
    }
}

impl From<std::io::Error> for CliError {
    fn from(e: std::io::Error) -> Self {
        Self::new(e.to_string())
    }
}

impl From<ClientError> for CliError {
    fn from(e: ClientError) -> Self {
        match e {
            // The message already names the socket and the start commands.
            ClientError::Unavailable { .. } => Self {
                exit: EXIT_UNAVAILABLE,
                message: e.to_string(),
            },
            ClientError::App(a) => Self::new(format_app_error(&a)),
            other => Self::new(other.to_string()),
        }
    }
}

pub fn format_app_error(a: &AppError) -> String {
    let mut s = format!("{}: {}", a.code, a.message);
    if a.retryable {
        s.push_str(" (retryable)");
    }
    if let Some(r) = &a.recovery {
        s.push_str(&format!("\n  next: {r}"));
    }
    if let Some(d) = &a.details {
        s.push_str(&format!("\n  details: {d}"));
    }
    s
}

pub struct Remote {
    socket: PathBuf,
    max_request: usize,
    next: AtomicU64,
    not_ready_budget: Duration,
}

impl Remote {
    pub fn new(socket: PathBuf, max_request: usize) -> Self {
        Self {
            socket,
            max_request,
            next: AtomicU64::new(1),
            not_ready_budget: Duration::from_secs(10),
        }
    }

    fn id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }

    /// The daemon binds its socket before the job system is up; a retryable DAEMON_NOT_READY is
    /// retried with short backoff (about 10 s in total) before the error is reported.
    async fn call_retrying(&self, make: impl Fn(u64) -> Request) -> Result<Value, CliError> {
        let mut waited = Duration::ZERO;
        let mut delay = Duration::from_millis(100);
        loop {
            match call(&self.socket, make(self.id()), self.max_request).await {
                Err(ClientError::App(a))
                    if a.code == "DAEMON_NOT_READY"
                        && a.retryable
                        && waited < self.not_ready_budget =>
                {
                    tokio::time::sleep(delay).await;
                    waited += delay;
                    delay = (delay * 2).min(Duration::from_secs(1));
                }
                other => return Ok(other?),
            }
        }
    }

    pub async fn control(&self, cmd: &str, args: Value) -> Result<Value, CliError> {
        self.call_retrying(|id| Request::Control {
            id,
            cmd: cmd.into(),
            args: args.clone(),
        })
        .await
    }

    pub async fn tool(&self, tool: &str, args: Value) -> Result<Value, CliError> {
        self.call_retrying(|id| Request::Tool {
            id,
            tool: tool.into(),
            args: args.clone(),
        })
        .await
    }
}

pub fn print_json(v: &Value) {
    println!("{}", serde_json::to_string_pretty(v).unwrap_or_default());
}

/// Generic human rendering: `key: value` lines, nested objects indented, arrays of scalars inline.
pub fn render_kv(v: &Value, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    match v {
        Value::Object(m) => {
            for (k, val) in m {
                match val {
                    Value::Object(_) => {
                        out.push_str(&format!("{pad}{k}:\n"));
                        render_kv(val, indent + 2, out);
                    }
                    Value::Array(a) if a.iter().any(|x| x.is_object() || x.is_array()) => {
                        out.push_str(&format!("{pad}{k}:\n"));
                        for item in a {
                            out.push_str(&format!("{pad}  -\n"));
                            render_kv(item, indent + 4, out);
                        }
                    }
                    other => out.push_str(&format!("{pad}{k}: {}\n", scalar(other))),
                }
            }
        }
        other => out.push_str(&format!("{pad}{}\n", scalar(other))),
    }
}

fn scalar(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// States after which polling stops. `blocked` needs operator action, so waiting would hang.
pub fn is_terminal(state: &str) -> bool {
    matches!(state, "done" | "failed" | "cancelled" | "blocked")
}

/// Poll `jobs_status` until the operation reaches a terminal state, printing progress when it
/// changes. Interrupting the CLI never cancels the operation (it is durable in the daemon).
pub async fn wait_for_operation(
    remote: &Remote,
    operation_id: &str,
    interval: Duration,
) -> Result<(), CliError> {
    let mut last = String::new();
    loop {
        let v = match remote
            .control(
                "jobs_status",
                serde_json::json!({ "operation_id": operation_id }),
            )
            .await
        {
            Ok(v) => v,
            Err(e) if e.exit == EXIT_UNAVAILABLE => {
                return Err(CliError {
                    exit: EXIT_UNAVAILABLE,
                    message: format!(
                        "{}\nThe operation continues in the daemon; check it later with `spool jobs status {operation_id}`.",
                        e.message
                    ),
                });
            }
            Err(e) => return Err(e),
        };
        let state = v
            .get("state")
            .and_then(Value::as_str)
            .ok_or_else(|| CliError::new("jobs_status returned no `state`"))?;
        let line = match v.get("progress").filter(|p| !p.is_null()) {
            Some(p) => format!("{operation_id}: {state} {p}"),
            None => format!("{operation_id}: {state}"),
        };
        if line != last {
            println!("{line}");
            last = line;
        }
        if is_terminal(state) {
            return match state {
                "done" => Ok(()),
                other => {
                    let why = v.get("error").map(|e| format!(": {e}")).unwrap_or_default();
                    Err(CliError::new(format!(
                        "operation {operation_id} ended {other}{why}"
                    )))
                }
            };
        }
        tokio::time::sleep(interval).await;
    }
}
