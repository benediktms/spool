//! Embedding-profile migration through the real daemon against a LOCAL fake provider (127.0.0.1, synthetic data):
//! a model/dimension change keeps answering semantic queries with the OLD profile while the new one is built, a
//! source append during the migration is part of what must be covered, and the switch happens only when the
//! currently published corpus is covered (spec 10.6). Restarts the daemon between the two configurations.

use parking_lot::Mutex;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const BIN: &str = env!("CARGO_BIN_EXE_spool");
const MODEL_A: &str = "text-embedding-3-small";
const MODEL_B: &str = "text-embedding-3-large";
const DIMS_A: usize = 16;
const DIMS_B: usize = 24;

/// Bag-of-words embedding hashed into `dims` buckets.
fn embed(text: &str, dims: usize) -> Vec<f32> {
    let mut v = vec![0f32; dims];
    for w in text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| !w.is_empty())
    {
        let h = w
            .to_lowercase()
            .bytes()
            .fold(7u32, |a, b| a.wrapping_mul(31).wrapping_add(b as u32));
        v[(h as usize) % dims] += 1.0;
    }
    v
}

/// (model, dimensions, inputs) of every request the provider received.
type Log = Arc<Mutex<Vec<(String, usize, Vec<String>)>>>;

/// Local fake of the embeddings endpoint. Requests for MODEL_B fail with a server error until `allow_b` is set.
async fn fake_provider(allow_b: Arc<AtomicBool>) -> (u16, Log) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let log: Log = Arc::default();
    let out = log.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut s, _)) = l.accept().await else {
                return;
            };
            let (log, allow_b) = (out.clone(), allow_b.clone());
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
                let body: Value = serde_json::from_slice(&buf[head_end..head_end + clen]).unwrap();
                let model = body["model"].as_str().unwrap_or_default().to_string();
                let dims = body["dimensions"].as_u64().unwrap_or(0) as usize;
                let inputs: Vec<String> = body["input"]
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .map(|t| t.as_str().unwrap_or("").to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                let refuse = model == MODEL_B && !allow_b.load(Ordering::SeqCst);
                let (status, resp) = if refuse {
                    (
                        "500 Internal Server Error",
                        json!({"error": {"message": "model-b is not available yet"}}).to_string(),
                    )
                } else {
                    let data: Vec<Value> = inputs
                        .iter()
                        .enumerate()
                        .map(|(i, t)| json!({"object":"embedding","index":i,"embedding":embed(t, dims)}))
                        .collect();
                    ("200 OK", json!({"object":"list","data":data,"model":model,"usage":{"prompt_tokens":10,"total_tokens":10}}).to_string())
                };
                log.lock().push((model, dims, inputs));
                let _ = s
                    .write_all(
                        format!("HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{resp}", resp.len())
                            .as_bytes(),
                    )
                    .await;
                let _ = s.shutdown().await;
            });
        }
    });
    (port, log)
}

struct Env {
    _tmp: tempfile::TempDir,
    home: PathBuf,
    src: PathBuf,
    daemon: Option<Child>,
}
impl Drop for Env {
    fn drop(&mut self) {
        self.stop();
    }
}
impl Env {
    fn stop(&mut self) {
        if let Some(mut d) = self.daemon.take() {
            let _ = d.kill();
            let _ = d.wait();
        }
    }
    fn start(&mut self) {
        let _ = std::fs::remove_file(self.home.join("state/spool.sock"));
        self.daemon = Some(
            Command::new(BIN)
                .args(["daemon", "run"])
                .env("SPOOL_HOME", &self.home)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let t = Instant::now();
        while !self.home.join("state/spool.sock").exists() {
            assert!(t.elapsed() < Duration::from_secs(30));
            std::thread::sleep(Duration::from_millis(50));
        }
        loop {
            let (ok, out) = cli(&self.home, &["status", "--json"]);
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
    fn set_model(&self, model: &str, dims: usize, from: (&str, usize)) {
        let p = self.home.join("config.toml");
        let cfg = std::fs::read_to_string(&p).unwrap();
        let cfg = cfg
            .replace(
                &format!("model = \"{}\"", from.0),
                &format!("model = \"{model}\""),
            )
            .replace(
                &format!("dimensions = {}", from.1),
                &format!("dimensions = {dims}"),
            );
        std::fs::write(&p, cfg).unwrap();
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

fn session_jsonl(id: &str, texts: &[&str]) -> String {
    let mut lines = vec![
        r#"{"type":"title","v":1,"title":"t","source":"user","updatedAt":"2026-10-12T09:00:00Z","pad":""}"#.to_string(),
        format!(r#"{{"type":"session","version":3,"id":"{id}","timestamp":"2026-10-12T09:00:00Z","cwd":"/tmp/project","title":"{id}"}}"#),
    ];
    let mut parent = String::from("null");
    for (i, t) in texts.iter().enumerate() {
        let key = format!("{id}{i:04}");
        let role = if i % 2 == 0 { "user" } else { "assistant" };
        lines.push(format!(
            r#"{{"type":"message","id":"{key}","parentId":{parent},"timestamp":"2026-10-12T09:00:{:02}Z","message":{{"role":"{role}","content":[{{"type":"text","text":"{t}"}}]}}}}"#,
            i + 1
        ));
        parent = format!("\"{key}\"");
    }
    lines.join("\n") + "\n"
}

fn setup(port: u16) -> Env {
    let tmp = tempfile::Builder::new()
        .prefix("sp")
        .tempdir_in("/tmp")
        .unwrap();
    let home = tmp.path().join("h");
    let src = tmp.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    std::fs::write(
        src.join("s1.jsonl"),
        session_jsonl(
            "s1",
            &[
                "How do we invalidate the memoized lookup table?",
                "Invalidate the lookup table whenever the schema version changes.",
            ],
        ),
    )
    .unwrap();
    std::fs::write(
        src.join("s2.jsonl"),
        session_jsonl(
            "s2",
            &[
                "The kubernetes ingress controller times out under load.",
                "Raise the ingress controller proxy timeout and retry.",
            ],
        ),
    )
    .unwrap();
    let (ok, out) = cli(
        &home,
        &[
            "init",
            "--enable-source",
            "omp",
            "--hosted-processing",
            "yes",
        ],
    );
    assert!(ok, "{out}");
    let cfg = std::fs::read_to_string(home.join("config.toml")).unwrap();
    let cfg = cfg
        .replace("~/.omp/agent/sessions", src.to_str().unwrap())
        .replace("dimensions = 768", &format!("dimensions = {DIMS_A}"))
        .replace(
            "https://api.openai.com/v1/embeddings",
            &format!("http://127.0.0.1:{port}/v1/embeddings"),
        );
    // Every enabled root must be inside the temp dir: a default root would index real transcripts.
    let parsed: toml::Table = cfg.parse().unwrap();
    for s in parsed["sources"].as_array().unwrap() {
        if s["enabled"].as_bool() == Some(true) {
            for r in s["roots"].as_array().unwrap() {
                assert!(
                    r.as_str()
                        .unwrap()
                        .starts_with(tmp.path().to_str().unwrap()),
                    "root {r} escapes the sandbox"
                );
            }
        }
    }
    std::fs::write(home.join("config.toml"), cfg).unwrap();
    let key = home.join("secrets/provider.key");
    std::fs::write(&key, "sk-test-fake-key\n").unwrap();
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    Env {
        _tmp: tmp,
        home,
        src,
        daemon: None,
    }
}

fn json_cli(home: &Path, args: &[&str]) -> Value {
    let (ok, out) = cli(home, args);
    assert!(ok, "{out}");
    serde_json::from_str(&out).unwrap_or_else(|_| panic!("not json: {out}"))
}

fn embedding_ids(home: &Path) -> (String, String) {
    let st = json_cli(home, &["status", "--json"]);
    let e = &st["embedding"];
    (
        e["profile_id"].as_str().unwrap().to_string(),
        e["active_profile_id"].as_str().unwrap().to_string(),
    )
}

fn wait_for(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(
            t.elapsed() < Duration::from_secs(secs),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(300));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn profile_change_keeps_old_queries_until_the_published_corpus_is_covered() {
    let allow_b = Arc::new(AtomicBool::new(false));
    let (port, log) = fake_provider(allow_b.clone()).await;
    let mut env = setup(port);

    // ---- profile A: index and embed everything.
    env.start();
    let (ok, out) = cli(&env.home, &["index", "--since", "30d", "--wait"]);
    assert!(ok, "{out}");
    let search =
        |home: &Path, q: &str| json_cli(home, &["search", q, "--mode", "semantic", "--json"]);
    wait_for("profile A vectors", 40, || {
        let r = search(&env.home, "memoized lookup table");
        r["coverage"]["embedded_chunks"].as_i64().unwrap_or(0) >= 2
            && !r["results"].as_array().unwrap().is_empty()
    });
    let (id_a, active_a) = embedding_ids(&env.home);
    assert_eq!(id_a, active_a);
    env.stop();

    // ---- configuration moves to model B / other dimensions; B is not available yet. A source append lands.
    env.set_model(MODEL_B, DIMS_B, (MODEL_A, DIMS_A));
    let appended = session_jsonl(
        "s1",
        &[
            "How do we invalidate the memoized lookup table?",
            "Invalidate the lookup table whenever the schema version changes.",
            "Also purge the zebracrossing cache entries.",
        ],
    );
    std::fs::write(env.src.join("s1.jsonl"), appended).unwrap();
    let log_mark = log.lock().len();
    env.start();
    let (target, active) = embedding_ids(&env.home);
    assert_ne!(target, id_a, "the new model is a new profile");
    assert_eq!(active, id_a, "a changed profile never activates itself");
    // The append is published as text (lexical) even though B cannot embed it yet: part of what B must cover.
    wait_for("appended text published", 40, || {
        let r = json_cli(
            &env.home,
            &["search", "zebracrossing", "--mode", "lexical", "--json"],
        );
        !r["results"].as_array().unwrap().is_empty()
    });
    // Old semantic recall works while the target has no coverage; queries are embedded by model A only.
    let r = search(&env.home, "kubernetes ingress timeout");
    assert_eq!(r["mode_used"], "semantic", "{r}");
    assert_eq!(
        r["results"][0]["session_id"].as_str().is_some(),
        true,
        "{r}"
    );
    assert_eq!(
        embedding_ids(&env.home).1,
        id_a,
        "still on the old profile while B is unavailable"
    );
    {
        let seen = log.lock();
        for (model, dims, inputs) in &seen[log_mark..] {
            let is_query = inputs.len() == 1
                && !inputs[0].starts_with("USER:")
                && !inputs[0].starts_with("ASSISTANT:");
            if is_query {
                assert_eq!(
                    (model.as_str(), *dims),
                    (MODEL_A, DIMS_A),
                    "queries use the old profile: {inputs:?}"
                );
            }
        }
    }

    // ---- B becomes available: the whole current corpus (including the appended text) is embedded, then it switches.
    allow_b.store(true, Ordering::SeqCst);
    let (ok, out) = cli(&env.home, &["index", "--since", "30d", "--wait"]);
    assert!(ok, "{out}");
    wait_for("profile switch", 60, || {
        embedding_ids(&env.home).1 == target
    });
    let r = search(&env.home, "zebracrossing cache");
    assert_eq!(r["mode_used"], "semantic", "{r}");
    let hit = r["results"]
        .as_array()
        .unwrap()
        .iter()
        .any(|x| x["matches"].to_string().contains("zebracrossing"));
    assert!(
        hit,
        "the appended text is searchable semantically after the switch: {r}"
    );
    assert_eq!(
        r["coverage"]["embedded_chunks"], r["coverage"]["active_chunks"],
        "full coverage at the switch: {r}"
    );
    // Each model was only ever asked for its own dimensions, and the new model now answers queries.
    let seen = log.lock();
    let pairs: HashMap<&str, usize> = seen.iter().map(|(m, d, _)| (m.as_str(), *d)).collect();
    assert_eq!(pairs.get(MODEL_A), Some(&DIMS_A));
    assert_eq!(pairs.get(MODEL_B), Some(&DIMS_B));
    assert!(
        seen.iter()
            .any(|(m, _, i)| m == MODEL_B && i.len() == 1 && i[0].contains("zebracrossing")),
        "the query after the switch is embedded by model B"
    );
    assert!(
        seen.iter()
            .all(|(m, d, _)| (m == MODEL_A && *d == DIMS_A) || (m == MODEL_B && *d == DIMS_B))
    );
}
