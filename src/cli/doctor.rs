//! `spool doctor` (spec 18): verifies configuration, permissions, sources/adapters, SQLite+FTS5
//! capability (offline in-memory probe; the live databases are never opened here), embedding
//! profile and consent state, daemon/queue health via `index_status`, and service registration.
//! Provider connectivity is tested only with `--probe-provider`, using a fixed harmless string.

use super::remote::Remote;
use crate::config::{Config, Paths, mode_of};
use crate::service::{Definition, ServiceManager};
use crate::transcript::handle::HandleKey;
use serde::Serialize;
use serde_json::{Value, json};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Ok,
    Warn,
    Fail,
}

#[derive(Debug, Clone, Serialize)]
pub struct Check {
    pub name: String,
    pub status: Status,
    pub detail: String,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub ok: bool,
    pub checks: Vec<Check>,
}

impl Report {
    pub fn get(&self, name: &str) -> Option<&Check> {
        self.checks.iter().find(|c| c.name == name)
    }

    pub fn render(&self) -> String {
        let mut s = String::new();
        for c in &self.checks {
            let tag = match c.status {
                Status::Ok => "ok  ",
                Status::Warn => "WARN",
                Status::Fail => "FAIL",
            };
            s.push_str(&format!("[{tag}] {}: {}\n", c.name, c.detail));
        }
        s.push_str(if self.ok {
            "doctor: no failures\n"
        } else {
            "doctor: failures found\n"
        });
        s
    }
}

struct Acc(Vec<Check>);

impl Acc {
    fn add(&mut self, name: &str, status: Status, detail: impl Into<String>) {
        self.0.push(Check {
            name: name.into(),
            status,
            detail: detail.into(),
        });
    }
    fn ok(&mut self, name: &str, d: impl Into<String>) {
        self.add(name, Status::Ok, d)
    }
    fn warn(&mut self, name: &str, d: impl Into<String>) {
        self.add(name, Status::Warn, d)
    }
    fn fail(&mut self, name: &str, d: impl Into<String>) {
        self.add(name, Status::Fail, d)
    }
}

async fn sqlite_probe() -> Result<String, String> {
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    let opts = SqliteConnectOptions::new().in_memory(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .map_err(|e| e.to_string())?;
    crate::storage::check_fts5(&pool)
        .await
        .map_err(|e| e.to_string())
}

pub async fn run(
    paths: &Paths,
    service: Result<ServiceManager, String>,
    probe_provider: bool,
) -> Report {
    let mut a = Acc(Vec::new());

    let cfg = match Config::load(paths) {
        Ok(c) => {
            let src = if paths.config_file.exists() {
                paths.config_file.display().to_string()
            } else {
                "built-in defaults (no config.toml; run `spool init`)".into()
            };
            a.ok("config", format!("valid: {src}"));
            if c.logging.include_content {
                a.warn(
                    "logging",
                    "logging.include_content = true is ignored: spool never logs content",
                );
            }
            Some(c)
        }
        Err(e) => {
            a.fail("config", format!("{e:#}"));
            None
        }
    };

    // Permissions: directories 0700; socket, key, credential, databases and WAL/SHM 0600.
    let cred = cfg
        .as_ref()
        .and_then(|c| c.credential_path(&paths.root).ok());
    let extra: Vec<_> = cred.iter().cloned().collect();
    let issues = crate::perms::check(paths, &extra);
    if issues.is_empty() {
        a.ok(
            "permissions",
            "directories 0700; existing socket, key, credential, database and WAL files 0600",
        );
    }
    for i in &issues {
        a.fail("permissions", format!("{}: {}", i.path.display(), i.detail));
    }
    for (name, p) in [
        ("root", &paths.root),
        ("state", &paths.state_dir),
        ("secrets", &paths.secrets_dir),
    ] {
        if !p.exists() {
            a.warn(
                "directories",
                format!(
                    "{name} directory {} does not exist (run `spool init`)",
                    p.display()
                ),
            );
        }
    }

    match HandleKey::load(&paths.signing_key) {
        Ok(k) => a.ok(
            "signing_key",
            format!(
                "present, mode {:o}, epoch {}",
                mode_of(&paths.signing_key).unwrap_or(0),
                k.epoch()
            ),
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            a.fail("signing_key", "missing: run `spool init`")
        }
        Err(e) => a.fail("signing_key", e.to_string()),
    }

    for (name, p) in [
        ("index.sqlite", &paths.index_db),
        ("jobs.sqlite", &paths.jobs_db),
    ] {
        if p.exists() {
            let wal = {
                let mut w = p.as_os_str().to_owned();
                w.push("-wal");
                std::fs::metadata(std::path::PathBuf::from(w))
                    .map(|m| m.len())
                    .unwrap_or(0)
            };
            a.ok(
                "database",
                format!(
                    "{name}: {} bytes, WAL {wal} bytes",
                    std::fs::metadata(p).map(|m| m.len()).unwrap_or(0)
                ),
            );
        } else {
            a.warn(
                "database",
                format!("{name} not created yet (the daemon creates it on first start)"),
            );
        }
    }

    match sqlite_probe().await {
        Ok(v) => a.ok(
            "sqlite",
            format!("SQLite {v} with FTS5 (offline in-memory probe)"),
        ),
        Err(e) => a.fail("sqlite", format!("FTS5 capability probe failed: {e}")),
    }

    if let Some(cfg) = &cfg {
        for s in &cfg.sources {
            let adapter = crate::adapters::adapter_for(s.kind);
            let present = s
                .roots
                .iter()
                .filter(|r| std::path::Path::new(r).is_dir())
                .count();
            let detail = format!(
                "{} ({}) {}; adapter {} parser {}; {present}/{} roots present",
                s.id,
                s.kind.as_str(),
                if s.enabled { "enabled" } else { "disabled" },
                adapter.kind().as_str(),
                adapter.parser_version(),
                s.roots.len()
            );
            if s.enabled && present == 0 {
                a.warn("source", format!("{detail}: no root exists"));
            } else {
                a.ok("source", detail);
            }
        }
        embedding_checks(&mut a, cfg, cred.as_deref(), probe_provider).await;
    }

    daemon_check(&mut a, paths, cfg.as_ref()).await;

    match service {
        Ok(m) => match m.state() {
            Ok(s) => match (s.definition, s.registered) {
                (Definition::Missing, _) => a.warn(
                    "service",
                    format!(
                        "not installed ({}): `spool service install`",
                        s.definition_path.display()
                    ),
                ),
                (Definition::Foreign, _) => a.fail(
                    "service",
                    format!(
                        "{} exists but was not written by spool",
                        s.definition_path.display()
                    ),
                ),
                (Definition::Managed, Some(true)) => a.ok(
                    "service",
                    format!(
                        "{} registered, {}",
                        s.label,
                        if s.running == Some(true) {
                            "running"
                        } else {
                            "not running"
                        }
                    ),
                ),
                (Definition::Managed, _) => a.warn(
                    "service",
                    format!(
                        "{} installed but not registered: `spool service start`",
                        s.label
                    ),
                ),
            },
            Err(e) => a.warn("service", format!("cannot determine service state: {e:#}")),
        },
        Err(e) => a.warn("service", e),
    }

    let logs = crate::telemetry::log_dir(paths);
    if logs.is_dir() {
        a.ok("logs", logs.display().to_string());
    } else {
        a.warn(
            "logs",
            format!(
                "{} does not exist yet (created when the daemon starts)",
                logs.display()
            ),
        );
    }

    let ok = !a.0.iter().any(|c| c.status == Status::Fail);
    Report { ok, checks: a.0 }
}

async fn embedding_checks(a: &mut Acc, cfg: &Config, cred: Option<&std::path::Path>, probe: bool) {
    let e = &cfg.embedding;
    let has_cred = cred.map(|p| p.is_file()).unwrap_or(false);
    let usable = e.enabled && e.hosted_processing_approved && has_cred;
    if !e.enabled {
        a.ok("embedding", "disabled by config: lexical-only");
    } else if !e.hosted_processing_approved {
        a.ok(
            "embedding",
            "hosted processing NOT approved: lexical-only, no text leaves this machine",
        );
    } else if !has_cred {
        a.warn(
            "embedding",
            format!(
                "hosted processing approved but no credential file at {}: lexical-only",
                cred.map(|p| p.display().to_string()).unwrap_or_default()
            ),
        );
    } else {
        a.ok(
            "embedding",
            format!(
                "hosted semantic enabled: provider {} model {} dimensions {} generation {}",
                e.provider, e.model, e.dimensions, e.profile_generation
            ),
        );
    }
    if probe {
        match (usable, cred) {
            (true, Some(cred)) => match crate::embeddings::probe_provider(e, cred).await {
                Ok(o) => a.ok("provider", format!("probe succeeded: {} dimensions in {} ms", o.dimensions, o.latency_ms)),
                Err(err) => a.fail("provider", format!("probe failed: {err}")),
            },
            _ => a.fail("provider", "probe not attempted: needs embedding.enabled, hosted_processing_approved and a credential file"),
        }
    } else {
        a.ok(
            "provider",
            "not probed (use --probe-provider to send one fixed harmless string)",
        );
    }
}

async fn daemon_check(a: &mut Acc, paths: &Paths, cfg: Option<&Config>) {
    let remote = Remote::new(
        paths.socket.clone(),
        cfg.map(|c| c.ipc.max_request_bytes).unwrap_or(1 << 20),
    );
    let res =
        tokio::time::timeout(Duration::from_secs(5), remote.control("status", json!({}))).await;
    match res {
        Err(_) => a.fail("daemon", "status timed out after 5 s"),
        Ok(Err(e)) if e.exit == super::remote::EXIT_UNAVAILABLE => a.warn("daemon", e.message),
        Ok(Err(e)) => a.fail("daemon", e.message),
        Ok(Ok(v)) => {
            let state = v.get("status").and_then(Value::as_str).unwrap_or("unknown");
            let version = v
                .get("daemon_version")
                .and_then(Value::as_str)
                .unwrap_or("?");
            let detail = format!("running {version}, state {state}");
            match state {
                "storage_error" => a.fail("daemon", detail),
                "provider_blocked" | "ingestion_degraded" | "unsupported_format" => {
                    a.warn("daemon", detail)
                }
                _ => a.ok("daemon", detail),
            }
            for key in ["queue", "workers", "worker_health"] {
                if let Some(q) = v.get(key) {
                    a.ok("queue", format!("{key}: {q}"));
                }
            }
        }
    }
}
