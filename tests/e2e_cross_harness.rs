//! Parser -> index -> reader end to end for the Codex and Claude Code adapters through the real daemon:
//! sentinel exclusion in search, and `with_tools` turn closure / call-result linkage / orphans via
//! `read_session` over stdio MCP. Synthetic fixtures only (`fixtures/codex`, `fixtures/claude`).

use rmcp::{ServiceExt, model::CallToolRequestParams, transport::TokioChildProcess};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_spool");

struct Env {
    _tmp: tempfile::TempDir,
    home: PathBuf,
    daemon: Option<Child>,
}
impl Drop for Env {
    fn drop(&mut self) {
        if let Some(mut d) = self.daemon.take() {
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

fn setup() -> Env {
    let tmp = tempfile::Builder::new()
        .prefix("sp")
        .tempdir_in("/tmp")
        .unwrap();
    let home = tmp.path().join("h");
    let (codex, claude) = (tmp.path().join("codex"), tmp.path().join("claude/proj-a"));
    std::fs::create_dir_all(&codex).unwrap();
    std::fs::create_dir_all(&claude).unwrap();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
    std::fs::copy(
        root.join("codex/tool_turns.jsonl"),
        codex.join("rollout-fixture.jsonl"),
    )
    .unwrap();
    std::fs::copy(
        root.join("claude/saved-messages.jsonl"),
        claude.join("claude-fixture-1.jsonl"),
    )
    .unwrap();
    let (ok, out) = cli(
        &home,
        &[
            "init",
            "--enable-source",
            "codex",
            "--enable-source",
            "claude",
            "--disable-source",
            "omp",
            "--hosted-processing",
            "no",
        ],
    );
    assert!(ok, "{out}");
    let cfg = std::fs::read_to_string(home.join("config.toml")).unwrap();
    let cfg = cfg
        .replace("~/.codex/sessions", codex.to_str().unwrap())
        .replace(
            "~/.claude/projects",
            tmp.path().join("claude").to_str().unwrap(),
        );
    let parsed: toml::Table = cfg.parse().unwrap();
    for src in parsed["sources"].as_array().unwrap() {
        if src["enabled"].as_bool() == Some(true) {
            for r in src["roots"].as_array().unwrap() {
                assert!(
                    r.as_str()
                        .unwrap()
                        .starts_with(tmp.path().to_str().unwrap()),
                    "enabled source root {r} must be inside the temp dir: a default root would index real transcripts"
                );
            }
        }
    }
    std::fs::write(home.join("config.toml"), cfg).unwrap();
    let daemon = Command::new(BIN)
        .args(["daemon", "run"])
        .env("SPOOL_HOME", &home)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let t = Instant::now();
    while !home.join("state/spool.sock").exists() {
        assert!(t.elapsed() < Duration::from_secs(30));
        std::thread::sleep(Duration::from_millis(50));
    }
    wait_control_ready(&home);
    Env {
        _tmp: tmp,
        home,
        daemon: Some(daemon),
    }
}

type Client = rmcp::service::RunningService<rmcp::RoleClient, ()>;

async fn call(c: &Client, tool: &'static str, args: Value) -> (bool, Value) {
    let mut p = CallToolRequestParams::new(tool);
    p.arguments = args.as_object().cloned();
    let r = c.call_tool(p).await.unwrap();
    (
        r.is_error == Some(true),
        r.structured_content.expect("structured content"),
    )
}

fn kinds(v: &Value) -> Vec<String> {
    v["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["kind"].as_str().unwrap().to_string())
        .collect()
}

/// Follow `next_cursor` to selection exhaustion (default 32 KiB pages carry only a handful of items because
/// every item has several signed references) and return one value with all items and warnings.
async fn drain(c: &Client, mut r: Value) -> Value {
    let mut all = r.clone();
    for _ in 0..100 {
        let Some(cursor) = r["page"]["next_cursor"].as_str().map(str::to_string) else {
            return all;
        };
        let (err, next) = call(c, "read_session", json!({"cursor": cursor})).await;
        assert!(!err, "{next}");
        all["items"]
            .as_array_mut()
            .unwrap()
            .extend(next["items"].as_array().unwrap().iter().cloned());
        for w in next["warnings"].as_array().unwrap() {
            if !all["warnings"].as_array().unwrap().contains(w) {
                all["warnings"].as_array_mut().unwrap().push(w.clone());
            }
        }
        r = next;
    }
    panic!("selection did not exhaust within 100 pages");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codex_and_claude_fixtures_flow_through_search_and_with_tools_reads() {
    let env = setup();
    let (ok, out) = cli(&env.home, &["index", "--since", "30d", "--wait"]);
    assert!(ok, "{out}");
    let mut cmd = tokio::process::Command::new(BIN);
    cmd.arg("mcp").env("SPOOL_HOME", &env.home);
    let c: Client = ().serve(TokioChildProcess::new(cmd).unwrap()).await.unwrap();

    // no excluded sentinel is searchable, in any mode that touches local text
    for q in [
        "MUST_NOT_INDEX",
        "MUST_NOT_INDEX_RESULT_B",
        "SENTINEL",
        "MUST_NOT_INDEX_THINKING_SENTINEL",
    ] {
        for mode in ["lexical", "literal"] {
            let (err, r) = call(&c, "search_sessions", json!({"query": q, "mode": mode, "session_kinds": ["interactive","subagent","unknown"]})).await;
            assert!(!err, "{r}");
            assert!(
                r["results"].as_array().unwrap().is_empty(),
                "{mode} {q}: excluded text leaked into the index: {r}"
            );
        }
    }

    // ---- Codex
    let (_, r) = call(&c, "search_sessions", json!({"query": "Commentary is indexed", "mode": "lexical", "sources": ["codex"], "session_kinds": ["interactive", "unknown"]})).await;
    let codex_session = r["results"][0]["session_id"]
        .as_str()
        .expect("codex session indexed")
        .to_string();
    let (err, r) = call(
        &c,
        "read_session",
        json!({"session_id": codex_session, "view": "with_tools"}),
    )
    .await;
    assert!(!err, "{r}");
    let r = drain(&c, r).await;
    let k = kinds(&r);
    let calls = k.iter().filter(|x| *x == "tool_call").count();
    let results = k.iter().filter(|x| *x == "tool_result").count();
    eprintln!("CODEX with_tools kinds={k:?} warnings={}", r["warnings"]);
    assert!(
        calls >= 3,
        "three function calls in the fixture (two sharing a call id across turns): {k:?}"
    );
    assert!(results >= 3, "{k:?}");
    let dump = r.to_string();
    assert!(
        dump.contains("MUST_NOT_INDEX_ARGUMENT_B"),
        "with_tools returns verified tool arguments from the source"
    );
    assert!(dump.contains("MUST_NOT_INDEX_RESULT_B"));
    assert!(
        r["warnings"].as_array().unwrap().iter().any(|w| w["code"]
            .as_str()
            .unwrap_or("")
            .to_lowercase()
            .contains("orphan")),
        "orphan result is reported, not mispaired: {}",
        r["warnings"]
    );
    // the same call id in two different turns stays two distinct calls
    let repeat_calls = r["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["kind"] == "tool_call" && i["native_call_id"] == "repeat-call")
        .count();
    assert_eq!(
        repeat_calls, 2,
        "repeat-call appears in two turns and must not be merged"
    );
    // conversation view stays free of tool payloads
    let (_, conv) = call(&c, "read_session", json!({"session_id": codex_session})).await;
    assert!(
        !conv.to_string().contains("MUST_NOT_INDEX"),
        "conversation view must not contain tool payloads"
    );

    // ---- Claude
    let (_, r) = call(&c, "search_sessions", json!({"query": "second turn", "mode": "lexical", "sources": ["claude"], "session_kinds": ["interactive", "unknown"]})).await;
    let claude_session = r["results"][0]["session_id"]
        .as_str()
        .expect("claude session indexed")
        .to_string();
    let (err, r) = call(
        &c,
        "read_session",
        json!({"session_id": claude_session, "view": "with_tools"}),
    )
    .await;
    let r = if err {
        assert_eq!(r["error"]["code"], "BRANCH_REQUIRED", "{r}");
        let options = r["error"]["branch_options"].as_array().unwrap();
        assert!(options.len() >= 2, "the fixture forks at claude-u1");
        // pick the branch that contains the tool turns: try each until the dump has the second-turn result
        let mut found = None;
        for o in options {
            let (e2, r2) = call(&c, "read_session", json!({"session_id": claude_session, "view": "with_tools", "selection": {"kind": "session", "branch_ref": o["branch_ref"]}})).await;
            assert!(!e2, "{r2}");
            let r2 = drain(&c, r2).await;
            if r2
                .to_string()
                .contains("MUST_NOT_INDEX_SECOND_TURN_RESULT_SENTINEL")
            {
                found = Some(r2);
            } else {
                assert!(
                    !r2.to_string()
                        .contains("MUST_NOT_INDEX_SECOND_TURN_RESULT_SENTINEL")
                );
            }
        }
        found.expect("one branch carries the tool turns")
    } else {
        drain(&c, r).await
    };
    let k = kinds(&r);
    eprintln!("CLAUDE with_tools kinds={k:?} warnings={}", r["warnings"]);
    let dump = r.to_string();
    assert!(
        k.iter().filter(|x| *x == "tool_call").count() >= 2,
        "call-1 and call-2: {k:?}"
    );
    assert!(
        dump.contains("MUST_NOT_INDEX_RESULT_SENTINEL")
            && dump.contains("MUST_NOT_INDEX_SECOND_TURN_RESULT_SENTINEL")
    );
    assert!(
        !dump.contains("claude-branch") && !dump.contains("Branch child"),
        "sibling branch content is never imported"
    );
    assert!(
        !dump.contains("MUST_NOT_INDEX_THINKING_SENTINEL")
            && !dump.contains("MUST_NOT_INDEX_SUBAGENT_SENTINEL"),
        "reasoning and sidechain stay out of with_tools"
    );
    assert!(
        dump.contains("done authoritative") && !dump.contains("done initial"),
        "the authoritative saved update replaces the earlier streamed text"
    );
    c.cancel().await.unwrap();
}
