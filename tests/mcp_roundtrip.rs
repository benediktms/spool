//! Real stdio MCP client -> `spool mcp` -> owner-only IPC -> initialized daemon.

use rmcp::{ServiceExt, model::CallToolRequestParams, transport::TokioChildProcess};
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;
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
        #[cfg(windows)]
        {
            let paths = spool::config::Paths::from_root(self.home.clone()).unwrap();
            let _ = spool::ipc::transport::request_shutdown(&paths);
        }
        #[cfg(unix)]
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_daemon() -> Daemon {
    let tmp = tempfile::Builder::new().prefix("sp").tempdir().unwrap();
    let home = tmp.path().join("h");
    let init = Command::new(BIN)
        .args(["init", "--hosted-processing", "no"])
        .env("SPOOL_HOME", &home)
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    let child = Command::new(BIN)
        .args(["daemon", "run"])
        .env("SPOOL_HOME", &home)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let t = Instant::now();
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
            "daemon did not become ready: {}",
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

#[cfg(unix)]
fn mode(p: &Path) -> u32 {
    std::fs::metadata(p).unwrap().permissions().mode() & 0o777
}

#[tokio::test]
async fn stdio_client_reaches_daemon_tool_over_ipc() {
    let d = start_daemon();
    #[cfg(unix)]
    {
        assert_eq!(mode(&d.home), 0o700);
        assert_eq!(mode(&d.home.join("state")), 0o700);
        assert_eq!(mode(&d.home.join("state/spool.sock")), 0o600);
        assert_eq!(mode(&d.home.join("state/index.sqlite")), 0o600);
        assert_eq!(mode(&d.home.join("state/jobs.sqlite")), 0o600);
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
    let names: Vec<_> = tools.iter().map(|t| t.name.to_string()).collect();
    assert_eq!(names, ["search_sessions", "read_session", "index_status"]);

    let r = client
        .call_tool(CallToolRequestParams::new("index_status"))
        .await
        .unwrap();
    assert_eq!(r.is_error, Some(false));
    assert!(
        r.structured_content
            .expect("structured content")
            .is_object()
    );
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
        let _ = c.wait();
    }
}
