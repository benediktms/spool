//! Local IPC over an owner-only platform-local endpoint (Unix socket or Windows named pipe).

pub mod client;
pub mod framing;
pub mod server;
pub mod transport;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Max serialized IPC frame in either direction; `ipc.max_request_bytes` bounds requests
/// further. A tool response additionally honours the reader's own `max_bytes` (spec 14.2.7.2).
pub const MAX_RESPONSE_FRAME: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    /// One of the three MCP tools, forwarded by `spool mcp`. `args` are the raw tool arguments.
    Tool { id: u64, tool: String, args: Value },
    /// CLI administration call (`cmd` names a control command).
    Control { id: u64, cmd: String, args: Value },
}

impl Request {
    pub fn id(&self) -> u64 {
        match self {
            Request::Tool { id, .. } | Request::Control { id, .. } => *id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    #[serde(flatten)]
    pub body: Outcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Ok(Value),
    Err(AppError),
}

/// Stable application error (spec 14.5). Never contains transcript text or raw SQLite errors.
#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct AppError {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<String>,
    /// Code-specific structured data (e.g. `branch_options`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl AppError {
    /// The exact structured payload a tool error carries on the MCP wire (spec 14.2.3 example):
    /// `{"error": {code, message, retryable, recovery?, ..details}}`. Anything that budgets an
    /// error response MUST measure this value with `mcp::budget::tool_result_bytes(.., true)`.
    pub fn to_tool_json(&self) -> Value {
        let mut e = serde_json::Map::new();
        e.insert("code".into(), Value::String(self.code.clone()));
        e.insert("retryable".into(), Value::Bool(self.retryable));
        if let Some(r) = &self.recovery {
            e.insert("recovery".into(), Value::String(r.clone()));
        }
        e.insert("message".into(), Value::String(self.message.clone()));
        if let Some(Value::Object(d)) = &self.details {
            for (k, v) in d {
                e.entry(k.clone()).or_insert_with(|| v.clone());
            }
        }
        serde_json::json!({ "error": Value::Object(e) })
    }

    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
            recovery: None,
            details: None,
        }
    }
}
