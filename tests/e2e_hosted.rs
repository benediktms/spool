//! Hosted-embedding path through the real daemon against a LOCAL fake provider (127.0.0.1): no real
//! provider, no paid data. Verifies consent gating (zero requests without approval), that only
//! rendered conversational chunk text and the query reach the provider (never tool payloads),
//! the credential is sent as a bearer header, and semantic search works with returned vectors.

use serde_json::{Value, json};
use std::collections::HashMap;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const BIN: &str = env!("CARGO_BIN_EXE_spool");
const DIMS: usize = 16;

const SESSION: &str = r#"{"type":"title","v":1,"title":"Cache notes","source":"user","updatedAt":"2026-10-12T09:00:00Z","pad":""}
{"type":"session","version":3,"id":"hosted-1","timestamp":"2026-10-12T09:00:00Z","cwd":"/tmp/project","title":"Cache notes"}
{"type":"message","id":"a0000001","parentId":null,"timestamp":"2026-10-12T09:00:01Z","message":{"role":"user","content":[{"type":"text","text":"How should we invalidate the memoized lookup table?"}]}}
{"type":"message","id":"a0000002","parentId":"a0000001","timestamp":"2026-10-12T09:00:02Z","message":{"role":"assistant","content":[{"type":"text","text":"Invalidate the lookup table whenever the schema version changes."},{"type":"toolCall","id":"call-1","name":"bash","arguments":{"command":"echo TOOLSENTINEL_ARGS"}}]}}
{"type":"message","id":"a0000003","parentId":"a0000002","timestamp":"2026-10-12T09:00:03Z","message":{"role":"toolResult","toolCallId":"call-1","toolName":"bash","content":[{"type":"text","text":"TOOLSENTINEL_RESULT"}],"isError":false}}
"#;

/// Deterministic bag-of-words embedding: each word hashes to one of DIMS buckets.
fn embed(text: &str) -> Vec<f32> {
    let mut v = vec![0f32; DIMS];
    for w in text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
    {
        let h = w
            .to_lowercase()
            .bytes()
            .fold(7u32, |a, b| a.wrapping_mul(31).wrapping_add(b as u32));
        v[(h as usize) % DIMS] += 1.0;
    }
    v
}

type Log = Arc<Mutex<Vec<(HashMap<String, String>, Value)>>>;

async fn fake_provider() -> (u16, Log) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let log: Log = Arc::default();
    let out = log.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = l.accept().await else {
                return;
            };
            let log = out.clone();
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let mut tmp = [0u8; 8192];
                let (head_end, clen) = loop {
                    let n = s.read(&mut tmp).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                    if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&buf[..p]).to_lowercase();
                        let clen = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .and_then(|v| v.trim().parse::<usize>().ok())
                            .unwrap_or(0);
                        break (p + 4, clen);
                    }
                };
                while buf.len() < head_end + clen {
                    let n = s.read(&mut tmp).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                let headers: HashMap<String, String> = head
                    .lines()
                    .skip(1)
                    .filter_map(|l| l.split_once(':'))
                    .map(|(k, v)| (k.trim().to_lowercase(), v.trim().to_string()))
                    .collect();
                let body: Value = serde_json::from_slice(&buf[head_end..head_end + clen]).unwrap();
                let inputs = body["input"].as_array().cloned().unwrap_or_default();
                let data: Vec<Value> = inputs.iter().enumerate().map(|(i, t)| json!({"object":"embedding","index":i,"embedding":embed(t.as_str().unwrap_or(""))})).collect();
                let resp = json!({"object":"list","data":data,"model":"text-embedding-3-small","usage":{"prompt_tokens":10,"total_tokens":10}}).to_string();
                log.lock().unwrap().push((headers, body));
                let _ = s.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{resp}", resp.len()).as_bytes()).await;
                let _ = s.shutdown().await;
            });
        }
    });
    (port, log)
}
struct Env {
    _tmp: tempfile::TempDir,
    home: PathBuf,
    source: PathBuf,
    daemon: Option<Child>,
}
impl Drop for Env {
    fn drop(&mut self) {
        if let Some(mut d) = self.daemon.take() {
            #[cfg(windows)]
            {
                let paths = spool::config::Paths::from_root(self.home.clone()).unwrap();
                let _ = spool::ipc::transport::request_shutdown(&paths);
            }
            #[cfg(unix)]
            let _ = d.kill();
            let _ = d.wait();
        }
    }
}

fn cli(home: &Path, args: &[&str]) -> (bool, String) {
    let o = Command::new(BIN)
        .args(args)
        .env("SPOOL_HOME", home)
        .output()
        .unwrap();
    (
        o.status.success(),
        String::from_utf8_lossy(&o.stdout).into_owned() + &String::from_utf8_lossy(&o.stderr),
    )
}

/// The socket is bound before the job system finishes starting (search/read are served immediately,
/// control calls answer DAEMON_NOT_READY meanwhile): wait for the control plane too.
fn wait_control_ready(home: &Path) {
    let t = Instant::now();
    loop {
        let (ok, out) = cli(home, &["status", "--json"]);
        if ok {
            return;
        }
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "control plane never became ready: {out}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn setup(port: u16, approved: bool) -> Env {
    let tmp = tempfile::Builder::new().prefix("sp").tempdir().unwrap();
    let home = tmp.path().join("h");
    let src = tmp.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(src.join("s1.jsonl"), SESSION).unwrap();
    let (ok, out) = cli(
        &home,
        &[
            "init",
            "--enable-source",
            "omp",
            "--hosted-processing",
            if approved { "yes" } else { "no" },
        ],
    );
    assert!(ok, "{out}");
    let mut cfg = std::fs::read_to_string(home.join("config.toml")).unwrap();
    cfg = cfg
        .replace(
            "~/.omp/agent/sessions",
            &src.to_string_lossy().replace('\\', "/"),
        )
        .replace("dimensions = 768", &format!("dimensions = {DIMS}"))
        .replace(
            "https://api.openai.com/v1/embeddings",
            &format!("http://127.0.0.1:{port}/v1/embeddings"),
        );
    std::fs::write(home.join("config.toml"), cfg).unwrap();
    let key = home.join("secrets/provider.key");
    std::fs::write(&key, "sk-test-fake-key\n").unwrap();
    #[cfg(unix)]
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    let daemon = Command::new(BIN)
        .args(["daemon", "run"])
        .env("SPOOL_HOME", &home)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    wait_control_ready(&home);
    Env {
        _tmp: tmp,
        home,
        source: src,
        daemon: Some(daemon),
    }
}

fn tool(home: &Path, args: &[&str]) -> Value {
    let (ok, out) = cli(home, args);
    assert!(
        ok,
        "{out}\n--- daemon log tail ---\n{}",
        std::fs::read_to_string(home.join("logs/spool.log"))
            .unwrap_or_default()
            .lines()
            .rev()
            .take(15)
            .collect::<Vec<_>>()
            .join("\n")
    );
    serde_json::from_str(&out).unwrap_or_else(|_| panic!("not json: {out}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn without_consent_the_provider_receives_nothing() {
    let (port, log) = fake_provider().await;
    let env = setup(port, false);
    let (ok, out) = cli(&env.home, &["index", "--since", "30d", "--wait"]);
    assert!(ok, "{out}");
    let r = tool(
        &env.home,
        &["search", "memoized lookup", "--mode", "hybrid", "--json"],
    );
    assert_eq!(
        r["mode_used"], "lexical",
        "hybrid degrades visibly to lexical without consent: {r}"
    );
    assert!(!r["results"].as_array().unwrap().is_empty());
    let (_, out) = cli(
        &env.home,
        &["search", "memoized lookup", "--mode", "semantic"],
    );
    assert!(
        out.to_lowercase().contains("embedding") || out.contains("EMBEDDING_PROFILE_ERROR"),
        "semantic-only is a typed error: {out}"
    );
    assert!(
        log.lock().unwrap().is_empty(),
        "no request may reach the provider without consent"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn with_consent_only_conversation_text_and_query_reach_the_provider() {
    let (port, log) = fake_provider().await;
    let env = setup(port, true);
    let (ok, out) = cli(&env.home, &["index", "--since", "30d", "--wait"]);
    assert!(ok, "{out}");
    // embeddings are produced after text publication; wait for vector coverage
    let t = Instant::now();
    loop {
        let (ok, out) = cli(
            &env.home,
            &[
                "search",
                "lookup table schema",
                "--mode",
                "semantic",
                "--json",
            ],
        );
        if !ok {
            let seen = log.lock().unwrap();
            eprintln!(
                "search failed: {out}\nprovider saw {} requests: {:?}",
                seen.len(),
                seen.iter()
                    .map(|(h, b)| (
                        h.clone(),
                        b.to_string().chars().take(300).collect::<String>()
                    ))
                    .collect::<Vec<_>>()
            );
            let (_, st) = cli(&env.home, &["status", "--json"]);
            eprintln!("STATUS {}", st.chars().take(6000).collect::<String>());
            panic!("semantic search failed");
        }
        let r: Value = serde_json::from_str(&out).unwrap();
        if r["coverage"]["embedded_chunks"].as_i64().unwrap_or(0) > 0
            && !r["results"].as_array().unwrap().is_empty()
        {
            assert_eq!(r["mode_used"], "semantic");
            break;
        }
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "vectors never became available: {r}"
        );
        std::thread::sleep(Duration::from_millis(300));
    }
    let seen = log.lock().unwrap();
    assert!(!seen.is_empty());
    for (headers, body) in seen.iter() {
        assert_eq!(
            headers.get("authorization").map(String::as_str),
            Some("Bearer sk-test-fake-key")
        );
        assert_eq!(body["model"], "text-embedding-3-small");
        assert_eq!(body["dimensions"], DIMS);
        let all = body.to_string();
        assert!(
            !all.contains("TOOLSENTINEL"),
            "tool payloads must never reach the provider: {all}"
        );
        assert!(
            !all.contains(&env.source.to_string_lossy().replace('\\', "/")),
            "source paths must never reach the provider"
        );
    }
    let sent: String = seen.iter().map(|(_, b)| b["input"].to_string()).collect();
    assert!(
        sent.contains("memoized lookup table") && sent.contains("USER:"),
        "rendered chunk text is what is embedded: {sent}"
    );
}
