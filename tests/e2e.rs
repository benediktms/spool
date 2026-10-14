//! End-to-end through the real binaries: isolated `SPOOL_HOME`, synthetic OhMyPi session, real daemon,
//! manual indexing via the CLI, three concurrent stdio MCP clients (search/read/status), every tool
//! result validated against the tool's declared output schema, no hosted provider involved.

use rmcp::{ServiceExt, model::CallToolRequestParams, transport::TokioChildProcess};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_spool");

const SESSION: &str = r#"{"type":"title","v":1,"title":"Webhook review bot","source":"user","updatedAt":"2026-10-12T09:00:00Z","pad":""}
{"type":"session","version":3,"id":"e2e-omp-1","timestamp":"2026-10-12T09:00:00Z","cwd":"/tmp/project","title":"Webhook review bot"}
{"type":"message","id":"a0000001","parentId":null,"timestamp":"2026-10-12T09:00:01Z","message":{"role":"user","content":[{"type":"text","text":"Could the webhook receiver for our review bot run locally with foo::bar_baz?"}]}}
{"type":"message","id":"a0000002","parentId":"a0000001","timestamp":"2026-10-12T09:00:02Z","message":{"role":"assistant","content":[{"type":"text","text":"Yes, run the receiver on localhost and tunnel it."},{"type":"toolCall","id":"call-1","name":"bash","arguments":{"command":"echo TOOLSENTINEL_ARGS"}}]}}
{"type":"message","id":"a0000003","parentId":"a0000002","timestamp":"2026-10-12T09:00:03Z","message":{"role":"toolResult","toolCallId":"call-1","toolName":"bash","content":[{"type":"text","text":"TOOLSENTINEL_RESULT output"}],"isError":false}}
{"type":"message","id":"a0000004","parentId":"a0000003","timestamp":"2026-10-12T09:00:04Z","message":{"role":"assistant","content":[{"type":"text","text":"Done: the receiver listens on port 8787."}]}}
"#;

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
            "no",
        ],
    );
    assert!(ok, "init failed: {out}");
    let cfg = std::fs::read_to_string(home.join("config.toml")).unwrap();
    std::fs::write(
        home.join("config.toml"),
        cfg.replace("~/.omp/agent/sessions", src.to_str().unwrap()),
    )
    .unwrap();
    let daemon = Command::new(BIN)
        .args(["daemon", "run"])
        .env("SPOOL_HOME", &home)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let t = Instant::now();
    while !home.join("state/spool.sock").exists() {
        assert!(
            t.elapsed() < Duration::from_secs(30),
            "daemon did not start"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    wait_control_ready(&home);
    Env {
        _tmp: tmp,
        home,
        daemon: Some(daemon),
    }
}

async fn client(home: &Path) -> rmcp::service::RunningService<rmcp::RoleClient, ()> {
    let mut cmd = tokio::process::Command::new(BIN);
    cmd.arg("mcp").env("SPOOL_HOME", home);
    ().serve(TokioChildProcess::new(cmd).unwrap())
        .await
        .unwrap()
}

async fn call(
    c: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
    tool: &'static str,
    args: Value,
) -> (bool, Value) {
    let mut p = CallToolRequestParams::new(tool);
    p.arguments = args.as_object().cloned();
    let r = c.call_tool(p).await.unwrap();
    (
        r.is_error == Some(true),
        r.structured_content.expect("structured content"),
    )
}

fn validate(tools: &[rmcp::model::Tool], name: &str, v: &Value) {
    let t = tools.iter().find(|t| t.name == name).unwrap();
    let schema = Value::Object((**t.output_schema.as_ref().expect("output schema")).clone());
    let validator = jsonschema::validator_for(&schema).unwrap();
    let errs: Vec<String> = validator.iter_errors(v).map(|e| e.to_string()).collect();
    assert!(
        errs.is_empty(),
        "{name} output violates its own schema: {errs:?}\n{v}"
    );
}

#[tokio::test]
async fn real_daemon_index_search_read_with_three_concurrent_clients() {
    let env = setup();
    let (ok, out) = cli(&env.home, &["index", "--since", "30d", "--wait"]);
    assert!(ok, "index --wait failed: {out}");

    let c1 = client(&env.home).await;
    let tools = c1.list_all_tools().await.unwrap();
    let mut names: Vec<_> = tools.iter().map(|t| t.name.to_string()).collect();
    names.sort();
    assert_eq!(
        names,
        ["index_status", "read_session", "search_sessions"],
        "the MCP surface is exactly three tools"
    );
    for t in &tools {
        assert!(
            t.output_schema.is_some() || t.name == "x",
            "{} has an output schema",
            t.name
        );
        assert!(
            !serde_json::to_string(&t.input_schema)
                .unwrap()
                .contains("\"uint"),
            "non-standard formats are stripped"
        );
    }

    // three clients at once
    let c2 = client(&env.home).await;
    let c3 = client(&env.home).await;
    let (a, b, c) = tokio::join!(
        call(
            &c1,
            "search_sessions",
            json!({"query":"webhook receiver","mode":"lexical"})
        ),
        call(
            &c2,
            "search_sessions",
            json!({"query":"foo::bar_baz","mode":"literal"})
        ),
        call(&c3, "index_status", json!({})),
    );
    for (name, (err, v)) in [
        ("search_sessions", &a),
        ("search_sessions", &b),
        ("index_status", &c),
    ] {
        assert!(!err, "{name}: {v}");
        validate(&tools, name, v);
    }
    assert_eq!(a.1["results"][0]["title"], "Webhook review bot", "{}", a.1);
    assert!(
        a.1["results"][0]["matches"][0]["matched_at"].is_string(),
        "timestamps flow through from the adapter"
    );

    // read around the literal match, conversation then with_tools
    let session = b.1["results"][0]["session_id"]
        .as_str()
        .unwrap()
        .to_string();
    let match_ref = b.1["results"][0]["matches"][0]["match_ref"]
        .as_str()
        .unwrap()
        .to_string();
    let (err, r) = call(&c2, "read_session", json!({"session_id": session, "selection": {"kind":"match","match_ref": match_ref, "before":0, "after":3}})).await;
    assert!(!err, "{r}");
    validate(&tools, "read_session", &r);
    let text: String = r["items"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|i| i["blocks"].as_array().unwrap().iter())
        .map(|b| b["data"].as_str().unwrap_or(""))
        .collect();
    assert!(text.contains("foo::bar_baz") && text.contains("port 8787"));
    assert!(
        !text.contains("TOOLSENTINEL"),
        "conversation view excludes tool payloads"
    );

    let (err, r) = call(&c2, "read_session", json!({"session_id": session, "selection": {"kind":"match","match_ref": match_ref, "before":0, "after":3}, "view":"with_tools"})).await;
    assert!(!err, "{r}");
    validate(&tools, "read_session", &r);
    let all = r.to_string();
    assert!(
        all.contains("TOOLSENTINEL_ARGS") && all.contains("TOOLSENTINEL_RESULT"),
        "with_tools returns verified tool activity"
    );

    // raw view is refused unless locally enabled
    let (err, r) = call(
        &c1,
        "read_session",
        json!({"session_id": session, "view":"raw_records"}),
    )
    .await;
    assert!(err && r["error"]["code"] == "FORBIDDEN", "{r}");

    for c in [c1, c2, c3] {
        c.cancel().await.unwrap();
    }
    // no TCP listener: the daemon process holds no listening inet socket
    if let Some(d) = env.daemon.as_ref() {
        let lsof = Command::new("lsof")
            .args(["-nP", "-a", "-p", &d.id().to_string(), "-i"])
            .output();
        if let Ok(o) = lsof {
            assert!(
                String::from_utf8_lossy(&o.stdout).trim().is_empty(),
                "daemon must not hold any inet socket: {}",
                String::from_utf8_lossy(&o.stdout)
            );
        }
    }
}
