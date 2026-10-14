//! `spool mcp config --client ...`: print-only client configuration. Only shapes verified on this
//! machine (docs/dependency-baseline.md) are emitted: Claude Code (`--mcp-config`/`.mcp.json`),
//! Codex (`config.toml` `[mcp_servers.spool]`), OMP (project `.mcp.json`). No provider key, no
//! database path, no environment: the bridge finds its socket from the same SPOOL_HOME/defaults.

use super::args::McpClient;
use serde_json::json;
use std::path::Path;

pub fn render(client: McpClient, bin: &Path) -> anyhow::Result<String> {
    anyhow::ensure!(
        bin.is_absolute(),
        "the spool binary path must be absolute: {}",
        bin.display()
    );
    let bin = bin.to_string_lossy();
    Ok(match client {
        // Claude Code and OMP both read `{"mcpServers": {name: {type, command, args}}}`.
        McpClient::Claude | McpClient::Omp => {
            let v = json!({"mcpServers": {"spool": {"type": "stdio", "command": bin, "args": ["mcp"]}}});
            format!("{}\n", serde_json::to_string_pretty(&v)?)
        }
        McpClient::Codex => {
            let mut spool = toml::Table::new();
            spool.insert("command".into(), toml::Value::String(bin.into_owned()));
            spool.insert(
                "args".into(),
                toml::Value::Array(vec![toml::Value::String("mcp".into())]),
            );
            let mut servers = toml::Table::new();
            servers.insert("spool".into(), toml::Value::Table(spool));
            let mut root = toml::Table::new();
            root.insert("mcp_servers".into(), toml::Value::Table(servers));
            toml::to_string_pretty(&root)?
        }
    })
}

/// Where each client expects the output (shown on stderr so stdout stays pasteable).
pub fn placement_hint(client: McpClient) -> &'static str {
    match client {
        McpClient::Claude => {
            "save as .mcp.json in a project, or pass with `claude --mcp-config FILE`"
        }
        McpClient::Codex => "append to ~/.codex/config.toml",
        McpClient::Omp => "save as .mcp.json in a project directory",
    }
}
