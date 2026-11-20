//! MCP client edge behaviour over the real stdio bridge and real daemon, driven with raw JSON-RPC
//! frames (no SDK on the client side): both negotiation paths recorded from real clients, stdout
//! cleanliness, daemon down / restart between calls, cancellation mid-request, and a large paginated
//! read whose every serialized frame stays within `max_bytes` and reassembles byte-exactly.

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_spool");

struct Home {
    _tmp: tempfile::TempDir,
    home: PathBuf,
    daemon: Option<Child>,
}
impl Drop for Home {
    fn drop(&mut self) {
        self.kill_daemon();
    }
}
impl Home {
    fn kill_daemon(&mut self) {
        if let Some(mut d) = self.daemon.take() {
            let _ = d.kill();
            let _ = d.wait();
        }
    }
    fn start_daemon(&mut self) {
        self.start_daemon_with(Path::new(BIN));
    }
    fn start_daemon_with(&mut self, binary: &Path) {
        let _ = std::fs::remove_file(self.home.join("state/spool.sock"));
        let d = Command::new(binary)
            .args(["daemon", "run"])
            .env("SPOOL_HOME", &self.home)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        self.daemon = Some(d);
        let t = Instant::now();
        loop {
            if self.home.join("state/spool.sock").exists()
                && std::os::unix::net::UnixStream::connect(self.home.join("state/spool.sock"))
                    .is_ok()
            {
                // control calls answer DAEMON_NOT_READY until the job system is up: wait for it too
                if cli_with(binary, &self.home, &["status", "--json"]).0 {
                    return;
                }
            }
            assert!(
                t.elapsed() < Duration::from_secs(30),
                "daemon did not come up"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

fn cli(home: &Path, args: &[&str]) -> (bool, String) {
    cli_with(Path::new(BIN), home, args)
}

fn cli_with(binary: &Path, home: &Path, args: &[&str]) -> (bool, String) {
    let o = Command::new(binary)
        .args(args)
        .env("SPOOL_HOME", home)
        .output()
        .unwrap();
    (
        o.status.success(),
        String::from_utf8_lossy(&o.stdout).into_owned() + &String::from_utf8_lossy(&o.stderr),
    )
}

/// Session with a 2 MiB assistant message (multi-byte, quotes, backslashes, newlines) after a short user message.
fn big_session(text: &str) -> String {
    let user = json!({"type":"message","id":"u1","parentId":null,"timestamp":"2026-10-12T09:00:01Z","message":{"role":"user","content":[{"type":"text","text":"please produce the long answer"}]}});
    let asst = json!({"type":"message","id":"a1","parentId":"u1","timestamp":"2026-10-12T09:00:02Z","message":{"role":"assistant","content":[{"type":"text","text":text}]}});
    format!(
        "{}\n{}\n{user}\n{asst}\n",
        json!({"type":"title","v":1,"title":"Big answer","source":"user","updatedAt":"2026-10-12T09:00:00Z","pad":""}),
        json!({"type":"session","version":3,"id":"big-1","timestamp":"2026-10-12T09:00:00Z","cwd":"/tmp/p","title":"Big answer"})
    )
}

fn setup(with_big: bool) -> (Home, String) {
    let tmp = tempfile::Builder::new()
        .prefix("sp")
        .tempdir_in("/tmp")
        .unwrap();
    let home = tmp.path().join("h");
    let src = tmp.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    let line = "line ü€😀 \"quoted\" \\ back\tslash 0123456789\n";
    let text: String = line.repeat((2 << 20) / line.len() + 1);
    if with_big {
        std::fs::write(src.join("big.jsonl"), big_session(&text)).unwrap();
    }
    let (ok, out) = cli(
        &home,
        &[
            "init",
            "--enable-source",
            "omp",
            "--hosted-processing",
            "no",
        ],
    );
    assert!(ok, "{out}");
    let cfg = std::fs::read_to_string(home.join("config.toml")).unwrap();
    std::fs::write(
        home.join("config.toml"),
        cfg.replace("~/.omp/agent/sessions", src.to_str().unwrap()),
    )
    .unwrap();
    let mut h = Home {
        _tmp: tmp,
        home,
        daemon: None,
    };
    h.start_daemon();
    if with_big {
        let (ok, out) = cli(&h.home, &["index", "--since", "2026-10-12", "--wait"]);
        assert!(ok, "{out}");
    }
    (h, text)
}

struct Bridge {
    child: Child,
    stdin: ChildStdin,
    lines: Receiver<String>,
    seen: Vec<String>,
    next_id: i64,
}

impl Bridge {
    fn spawn(home: &Path) -> Self {
        Self::spawn_with(Path::new(BIN), home)
    }
    fn spawn_with(binary: &Path, home: &Path) -> Self {
        let mut child = Command::new(binary)
            .arg("mcp")
            .env("SPOOL_HOME", home)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let out = child.stdout.take().unwrap();
        let (tx, rx) = channel();
        std::thread::spawn(move || {
            for l in BufReader::new(out).lines().map_while(Result::ok) {
                if tx.send(l).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            stdin,
            lines: rx,
            seen: vec![],
            next_id: 1,
        }
    }
    fn send(&mut self, v: Value) {
        writeln!(self.stdin, "{v}").unwrap();
    }
    /// Next frame; every stdout line must be a JSON-RPC object (stdout carries only MCP frames).
    fn recv(&mut self, timeout: Duration) -> Option<(Value, usize)> {
        let l = self.lines.recv_timeout(timeout).ok()?;
        let v: Value =
            serde_json::from_str(&l).unwrap_or_else(|_| panic!("stdout line is not JSON: {l:?}"));
        assert_eq!(
            v["jsonrpc"], "2.0",
            "stdout line is not a JSON-RPC frame: {l:?}"
        );
        self.seen.push(l.clone());
        Some((v, l.len()))
    }
    fn call(&mut self, name: &str, args: Value) -> (Value, usize) {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":args}}));
        self.response(id, T)
    }
    fn response(&mut self, id: i64, timeout: Duration) -> (Value, usize) {
        let t = Instant::now();
        loop {
            let (v, n) = self
                .recv(timeout.saturating_sub(t.elapsed()))
                .unwrap_or_else(|| panic!("no response for id {id}"));
            if v["id"] == json!(id) {
                return (v, n);
            }
        }
    }
}
impl Drop for Bridge {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn tool_json(r: &Value) -> Value {
    assert!(
        r["result"].get("structuredContent").is_none(),
        "one copy only"
    );
    let c = r["result"]["content"].as_array().expect("content");
    assert_eq!(c.len(), 1);
    serde_json::from_str(c[0]["text"].as_str().expect("text")).unwrap()
}

const T: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires SPOOL_UPGRADE_BASELINE_BIN"]
async fn jsonl_upgrade_keeps_match_and_cursor() {
    use sqlx::Connection;

    let baseline = PathBuf::from(
        std::env::var_os("SPOOL_UPGRADE_BASELINE_BIN")
            .expect("SPOOL_UPGRADE_BASELINE_BIN is required"),
    );
    assert!(baseline.is_file(), "baseline binary is unavailable");
    let tmp = tempfile::Builder::new()
        .prefix("spu")
        .tempdir_in("/tmp")
        .unwrap();
    let home = tmp.path().join("h");
    let src = tmp.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(
        src.join("fixture.jsonl"),
        big_session("synthetic second message"),
    )
    .unwrap();
    let (ok, out) = cli_with(
        &baseline,
        &home,
        &[
            "init",
            "--enable-source",
            "omp",
            "--disable-source",
            "codex",
            "--disable-source",
            "claude",
            "--hosted-processing",
            "no",
        ],
    );
    assert!(ok, "{out}");
    let cfg_path = home.join("config.toml");
    let cfg = std::fs::read_to_string(&cfg_path)
        .unwrap()
        .replace("~/.omp/agent/sessions", src.to_str().unwrap());
    let parsed: toml::Table = cfg.parse().unwrap();
    for source in parsed["sources"].as_array().unwrap() {
        if source["enabled"].as_bool() == Some(true) {
            for root in source["roots"].as_array().unwrap() {
                assert!(Path::new(root.as_str().unwrap()).starts_with(tmp.path()));
            }
        }
    }
    std::fs::write(cfg_path, cfg).unwrap();
    let mut h = Home {
        _tmp: tmp,
        home,
        daemon: None,
    };
    h.start_daemon_with(&baseline);
    let (ok, out) = cli_with(
        &baseline,
        &h.home,
        &["index", "--since", "2026-10-12", "--wait"],
    );
    assert!(ok, "{out}");
    let mut old = Bridge::spawn_with(&baseline, &h.home);
    old.send(json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"upgrade-test","version":"1"}}}));
    old.response(0, T);
    old.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
    let (reply, _) = old.call("search_sessions", json!({"query":"please produce","mode":"lexical","sources":["omp"],"session_kinds":["interactive","unknown"]}));
    let found = tool_json(&reply);
    let hit = &found["results"][0];
    let session = hit["session_id"]
        .as_str()
        .expect("indexed session")
        .to_owned();
    let matched = hit["matches"][0]["match_ref"]
        .as_str()
        .expect("match")
        .to_owned();
    let (reply, _) = old.call(
        "read_session",
        json!({"session_id":session,"selection":{"kind":"session"},"max_items":1}),
    );
    let first = tool_json(&reply);
    let cursor = first["page"]["next_cursor"]
        .as_str()
        .expect("nonempty continuation")
        .to_owned();
    let (reply, _) = old.call("read_session", json!({"cursor":cursor}));
    let expected_continuation = tool_json(&reply);
    assert!(
        !expected_continuation["items"]
            .as_array()
            .unwrap()
            .is_empty(),
        "{expected_continuation}"
    );
    drop(old);
    h.kill_daemon();

    let options = sqlx::sqlite::SqliteConnectOptions::new()
        .filename(h.home.join("state/index.sqlite"))
        .read_only(true);
    let mut db = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    let before: String = sqlx::query_scalar("SELECT active_snapshot_id FROM sessions WHERE id=?")
        .bind(&session)
        .fetch_one(&mut db)
        .await
        .unwrap();
    db.close().await.unwrap();

    h.start_daemon();
    let mut current = Bridge::spawn(&h.home);
    current.send(json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"upgrade-test","version":"1"}}}));
    current.response(0, T);
    current.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
    let (reply, _) = current.call(
        "read_session",
        json!({"session_id":session,"selection":{"kind":"match","match_ref":matched}}),
    );
    let reopened = tool_json(&reply);
    assert!(
        !reopened["items"].as_array().unwrap().is_empty(),
        "{reopened}"
    );
    assert!(
        reopened["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["provenance"]["origin"] == "verified_source")
    );
    let (reply, _) = current.call("read_session", json!({"cursor":cursor}));
    let continued = tool_json(&reply);
    assert!(
        !continued["items"].as_array().unwrap().is_empty(),
        "{continued}"
    );
    assert_eq!(
        continued["items"], expected_continuation["items"],
        "the original cursor must continue at the same position"
    );
    assert!(
        continued["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["provenance"]["origin"] == "verified_source")
    );
    let mut db = sqlx::SqliteConnection::connect_with(&options)
        .await
        .unwrap();
    let after: String = sqlx::query_scalar("SELECT active_snapshot_id FROM sessions WHERE id=?")
        .bind(&session)
        .fetch_one(&mut db)
        .await
        .unwrap();
    assert_eq!(
        before, after,
        "upgrade must not rebuild the indexed snapshot"
    );
}

#[test]
fn both_recorded_negotiation_paths_work_with_clean_stdout() {
    let (h, _) = setup(false);
    // path A: initialize handshake as sent by Codex (2025-06-18) and OMP (2025-11-25)
    for version in ["2025-06-18", "2025-11-25"] {
        let mut b = Bridge::spawn(&h.home);
        b.send(json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":version,"capabilities":{},"clientInfo":{"name":"raw","version":"0"}}}));
        let (r, _) = b.response(0, T);
        assert_eq!(r["result"]["protocolVersion"], version);
        b.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
        b.send(json!({"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}));
        let (r, _) = b.response(1, T);
        let names: Vec<_> = r["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(names, ["search_sessions", "read_session", "index_status"]);
        b.send(json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"index_status","arguments":{}}}));
        let (r, _) = b.response(2, T);
        assert_eq!(r["result"]["isError"], false, "{r}");
    }
    // path B: stateless 2026-07-28 flow as sent by Claude Code (no initialize; per-request _meta)
    let meta = json!({"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"claude-code","version":"2.1.294"},"io.modelcontextprotocol/clientCapabilities":{}});
    let mut b = Bridge::spawn(&h.home);
    b.send(json!({"jsonrpc":"2.0","id":"server-discover-probe-1","method":"server/discover","params":{"_meta":meta}}));
    let (r, _) = b.recv(T).unwrap();
    assert!(
        r["result"]["supportedVersions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "2026-07-28"),
        "{r}"
    );
    b.send(json!({"jsonrpc":"2.0","id":0,"method":"tools/list","params":{"_meta":meta}}));
    let (r, _) = b.response(0, T);
    assert_eq!(r["result"]["tools"].as_array().unwrap().len(), 3);
    b.send(json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search_sessions","arguments":{"query":"anything","mode":"lexical"},"_meta":meta}}));
    let (r, _) = b.response(1, T);
    assert_eq!(r["result"]["isError"], false, "{r}");
    assert_eq!(r["result"]["resultType"], "complete");
}

#[test]
fn daemon_down_is_a_typed_error_and_the_same_bridge_recovers_after_restart() {
    let (mut h, _) = setup(false);
    let mut b = Bridge::spawn(&h.home);
    b.send(json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"raw","version":"0"}}}));
    b.response(0, T);
    b.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
    let call = |b: &mut Bridge, id: i64| {
        b.send(json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"index_status","arguments":{}}}));
        b.response(id, T).0
    };
    assert_eq!(call(&mut b, 1)["result"]["isError"], false);
    h.kill_daemon();
    let r = call(&mut b, 2);
    assert_eq!(r["result"]["isError"], true);
    let sc = tool_json(&r);
    let e = &sc["error"];
    assert_eq!(
        (e["code"].as_str(), e["retryable"].as_bool()),
        (Some("DAEMON_NOT_READY"), Some(true)),
        "{r}"
    );
    assert!(
        e["message"].as_str().unwrap().contains("spool daemon run"),
        "error names the start command"
    );
    h.start_daemon();
    assert_eq!(
        call(&mut b, 3)["result"]["isError"],
        false,
        "the same bridge process reconnects"
    );
}

#[test]
fn large_read_pages_within_budget_survive_restart_and_cancellation() {
    let (mut h, text) = setup(true);
    let mut b = Bridge::spawn(&h.home);
    b.send(json!({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"raw","version":"0"}}}));
    b.response(0, T);
    b.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}));
    let (r, _) = b.call(
        "search_sessions",
        json!({"query":"long answer","mode":"lexical"}),
    );
    let session = tool_json(&r)["results"][0]["session_id"]
        .as_str()
        .expect("session found")
        .to_string();

    const MAX: usize = 4096 + 28 * 1024; // 32768
    let mut assembled: Vec<u8> = Vec::new();
    let mut args = json!({"session_id": session, "max_bytes": MAX, "verbose": true});
    let mut pages = 0;
    let mut biggest = 0;
    let mut restarted = false;
    let mut cancelled = false;
    loop {
        let (r, frame_len) = b.call("read_session", args.clone());
        assert_eq!(r["result"]["isError"], false, "{r}");
        biggest = biggest.max(frame_len);
        assert!(
            frame_len <= MAX,
            "page frame is {frame_len} bytes, over max_bytes {MAX}"
        );
        let sc = &tool_json(&r);
        for item in sc["items"].as_array().unwrap() {
            if item["role"] == "assistant" {
                for blk in item["blocks"].as_array().unwrap() {
                    assert_eq!(blk["encoding"], "utf8");
                    let data = blk["data"].as_str().unwrap();
                    let (s, e) = (
                        blk["start_byte"].as_u64().unwrap() as usize,
                        blk["end_byte"].as_u64().unwrap() as usize,
                    );
                    assert_eq!(data.len(), e - s, "range matches the decoded fragment");
                    assert_eq!(
                        s,
                        assembled.len(),
                        "fragments advance without gaps or overlap"
                    );
                    assembled.extend_from_slice(data.as_bytes());
                }
            }
        }
        pages += 1;
        let Some(cursor) = sc["page"]["next_cursor"].as_str() else {
            break;
        };
        args = json!({"cursor": cursor, "max_bytes": MAX});
        if pages == 5 && !restarted {
            restarted = true;
            h.kill_daemon();
            h.start_daemon(); // cursor must survive a daemon restart (persisted manifests and signing key)
        }
        if pages == 8 && !cancelled {
            cancelled = true;
            // fire a request and cancel it immediately; the bridge must stay usable
            b.next_id += 1;
            let cancel_id = b.next_id;
            b.send(json!({"jsonrpc":"2.0","id":cancel_id,"method":"tools/call","params":{"name":"read_session","arguments":args.clone()}}));
            b.send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":cancel_id,"reason":"test"}}));
            // either a (complete) response or an error/none is acceptable for the cancelled id, but never garbage
            while let Some((v, _)) = b.recv(Duration::from_millis(700)) {
                assert!(
                    v["id"] == json!(cancel_id) || v.get("method").is_some(),
                    "unexpected frame {v}"
                );
            }
        }
    }
    assert!(
        pages > 60,
        "2 MiB under 32 KiB pages needs many pages, got {pages}"
    );
    assert_eq!(assembled.len(), text.len(), "total decoded bytes");
    assert!(
        assembled == text.as_bytes(),
        "reassembled content is byte-exact"
    );
    eprintln!(
        "LARGE-READ pages={pages} biggest_frame={biggest} bytes max_bytes={MAX} restarted={restarted} cancelled={cancelled}"
    );
}
