//! Real stdio MCP client -> `spool mcp` -> owner-only Unix socket -> daemon.

use rmcp::{ServiceExt, model::CallToolRequestParams, transport::TokioChildProcess};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_spool");

struct Daemon {
    child: Child,
    home: PathBuf,
    _tmp: tempfile::TempDir,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_daemon() -> Daemon {
    // /tmp/ keeps the socket path under the platform sockaddr_un limit.
    let tmp = tempfile::Builder::new()
        .prefix("sp")
        .tempdir_in("/tmp")
        .unwrap();
    let home = tmp.path().join("h");
    let src = tmp.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    let init = Command::new(BIN)
        .args([
            "init",
            "--enable-source",
            "omp",
            "--hosted-processing",
            "no",
        ])
        .env("SPOOL_HOME", &home)
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "init failed: {}",
        String::from_utf8_lossy(&init.stderr)
    );
    let config = home.join("config.toml");
    let cfg = std::fs::read_to_string(&config).unwrap();
    std::fs::write(
        &config,
        cfg.replace("~/.omp/agent/sessions", src.to_str().unwrap()),
    )
    .unwrap();
    let log = tmp.path().join("daemon.log");
    let mut child = Command::new("sh")
        .args(["-c", "umask 022; exec \"$@\"", "sh", BIN, "daemon", "run"])
        .env("SPOOL_HOME", &home)
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let sock = home.join("state/spool.sock");
    let t = Instant::now();
    while !sock.exists() {
        if let Some(status) = child.try_wait().unwrap() {
            panic!(
                "daemon exited {status}: {}",
                std::fs::read_to_string(&log).unwrap()
            );
        }
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "daemon did not create socket"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    loop {
        let status = Command::new(BIN)
            .args(["status", "--json"])
            .env("SPOOL_HOME", &home)
            .output()
            .unwrap();
        if status.status.success() {
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(20),
            "daemon control plane not ready: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    Daemon {
        child,
        home,
        _tmp: tmp,
    }
}

fn mode(p: &Path) -> u32 {
    std::fs::metadata(p).unwrap().permissions().mode() & 0o777
}

#[tokio::test]
async fn stdio_client_reaches_daemon_tool_over_ipc() {
    let d = start_daemon();
    assert_eq!(mode(&d.home), 0o700);
    assert_eq!(mode(&d.home.join("state")), 0o700);
    assert_eq!(mode(&d.home.join("state/spool.sock")), 0o600);
    assert_eq!(mode(&d.home.join("state/index.sqlite")), 0o600);
    assert_eq!(mode(&d.home.join("state/jobs.sqlite")), 0o600);
    for db in ["index.sqlite", "jobs.sqlite"] {
        for suffix in ["-wal", "-shm"] {
            assert_eq!(mode(&d.home.join(format!("state/{db}{suffix}"))), 0o600);
        }
    }

    let mut cmd = tokio::process::Command::new(BIN);
    cmd.arg("mcp").env("SPOOL_HOME", &d.home);
    let client = ().serve(TokioChildProcess::new(cmd).unwrap()).await.unwrap();
    let info = client.peer_info().expect("peer info");
    eprintln!(
        "NEGOTIATED rmcp-client protocol_version={}",
        info.protocol_version
    );

    let tools = client.list_all_tools().await.unwrap();
    let mut names: Vec<_> = tools.iter().map(|t| t.name.to_string()).collect();
    names.sort();
    assert_eq!(names, ["index_status", "read_session", "search_sessions"]);
    let search = tools.iter().find(|t| t.name == "search_sessions").unwrap();
    let properties = &search.input_schema["properties"];
    assert!(properties.get("preview_target_bytes").is_some());
    assert!(properties.get("max_bytes").is_some());
    assert_eq!(properties["matches_per_session"]["maximum"], 32);
    let mut request = CallToolRequestParams::new("search_sessions");
    request.arguments = Some(
        serde_json::json!({"query":"synthetic", "mode":"lexical", "matches_per_session":32})
            .as_object()
            .unwrap()
            .clone(),
    );
    let expanded = client.call_tool(request).await.unwrap();
    assert_eq!(expanded.is_error, Some(false));

    let r = client
        .call_tool(CallToolRequestParams::new("index_status"))
        .await
        .unwrap();
    assert_eq!(r.is_error, Some(false));
    assert!(r.structured_content.is_none(), "one copy only");
    assert_eq!(r.content.len(), 1);
    let v: serde_json::Value =
        serde_json::from_str(&r.content[0].as_text().expect("text content").text).unwrap();
    eprintln!("TOOL RESULT {v}");
    assert_eq!(v["embedding"]["state"], "not_approved");
    assert_eq!(v["queue"]["queued"], 0);
    client.cancel().await.unwrap();
}

/// Raw JSON-RPC handshakes: record what the pinned SDK answers for every protocol version.
#[test]
fn raw_handshake_records_negotiated_versions() {
    let d = start_daemon();
    for requested in [
        "2024-11-05",
        "2025-03-26",
        "2025-06-18",
        "2025-11-25",
        "2026-07-28",
        "2099-01-01",
    ] {
        let mut c = Command::new(BIN)
            .arg("mcp")
            .env("SPOOL_HOME", &d.home)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let mut stdin = c.stdin.take().unwrap();
        let mut out = BufReader::new(c.stdout.take().unwrap());
        let init = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"initialize","params":{{"protocolVersion":"{requested}","capabilities":{{}},"clientInfo":{{"name":"raw","version":"0"}}}}}}"#
        );
        writeln!(stdin, "{init}").unwrap();
        let mut line = String::new();
        out.read_line(&mut line).unwrap();
        let v: serde_json::Value =
            serde_json::from_str(&line).expect("stdout carries only JSON-RPC");
        eprintln!(
            "HANDSHAKE requested={requested} -> {}",
            v["result"]["protocolVersion"]
        );
        drop(stdin);
        let _ = c.kill();
        let _ = c.wait();
    }
}
