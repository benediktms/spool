//! Parser -> index -> reader across synthetic JSONL and OpenCode V2 sources, through real stdio MCP.

use rmcp::{ServiceExt, model::CallToolRequestParams, transport::TokioChildProcess};
use serde_json::{Value, json};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{Connection, Executor, SqliteConnection};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_spool");
const FACTORY_FIXTURE: &str = include_str!("../fixtures/factory/session-v2.jsonl");
const FACTORY_PATH: &str = "factory/project/5ffdaa21-fac0-4000-8000-000000000001.jsonl";

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

fn sandbox() -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix("sp")
        .tempdir_in("/tmp")
        .unwrap()
}

fn setup(tmp: tempfile::TempDir, native_db: Option<&Path>, factory: bool) -> Env {
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
    let mut args = vec![
        "init",
        "--enable-source",
        "codex",
        "--enable-source",
        "claude",
        "--disable-source",
        "omp",
        "--hosted-processing",
        "no",
    ];
    if factory {
        let path = tmp.path().join(FACTORY_PATH);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, FACTORY_FIXTURE).unwrap();
        std::fs::write(
            path.with_file_name("5ffdaa21-fac0-4000-8000-000000000002.jsonl"),
            include_bytes!("../fixtures/factory/subagent-v2.jsonl"),
        )
        .unwrap();
        std::fs::write(
            path.with_extension("settings.json"),
            r#"{"text":"sidecarlapis"}"#,
        )
        .unwrap();
        args.extend(["--enable-source", "factory"]);
    }
    let (ok, out) = cli(&home, &args);
    assert!(ok, "{out}");
    let cfg = std::fs::read_to_string(home.join("config.toml")).unwrap();
    let mut cfg = cfg
        .replace("~/.codex/sessions", codex.to_str().unwrap())
        .replace(
            "~/.claude/projects",
            tmp.path().join("claude").to_str().unwrap(),
        );
    if factory {
        cfg = cfg
            .replace(
                "~/.factory/sessions",
                tmp.path().join("factory").to_str().unwrap(),
            )
            .replace("allow_raw_records = false", "allow_raw_records = true")
            .replace("interval_seconds = 60", "interval_seconds = 1");
    }
    if let Some(db) = native_db {
        let path = toml::Value::String(db.to_str().unwrap().to_owned());
        cfg.push_str(&format!(
            "\n[[sources]]\nid = \"oc\"\nkind = \"opencode\"\nenabled = true\nroots = [{path}]\n"
        ));
    }
    let parsed: toml::Table = cfg.parse().unwrap();
    for src in parsed["sources"].as_array().unwrap() {
        if src["enabled"].as_bool() == Some(true) {
            for r in src["roots"].as_array().unwrap() {
                assert!(
                    Path::new(r.as_str().unwrap()).starts_with(tmp.path()),
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
    let env = Env {
        _tmp: tmp,
        home,
        daemon: Some(daemon),
    };
    let t = Instant::now();
    while !env.home.join("state/spool.sock").exists() {
        assert!(t.elapsed() < Duration::from_secs(30));
        std::thread::sleep(Duration::from_millis(50));
    }
    wait_control_ready(&env.home);
    env
}

type Client = rmcp::service::RunningService<rmcp::RoleClient, ()>;

async fn mcp(home: &Path) -> Client {
    let mut cmd = tokio::process::Command::new(BIN);
    cmd.arg("mcp").env("SPOOL_HOME", home);
    ().serve(TokioChildProcess::new(cmd).unwrap())
        .await
        .unwrap()
}

async fn call(c: &Client, tool: &'static str, args: Value) -> (bool, Value) {
    let mut p = CallToolRequestParams::new(tool);
    p.arguments = args.as_object().cloned();
    let r = c.call_tool(p).await.unwrap();
    (r.is_error == Some(true), {
        assert!(r.structured_content.is_none(), "one copy only");
        serde_json::from_str(&r.content[0].as_text().expect("text content").text).unwrap()
    })
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
    let env = setup(sandbox(), None, false);
    let (ok, out) = cli(&env.home, &["index", "--since", "2026-10-12", "--wait"]);
    assert!(ok, "{out}");
    let c = mcp(&env.home).await;

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
    let matched = &r["results"][0]["matches"][0];
    let mut selection =
        json!({"kind":"match","match_ref":matched["match_ref"],"excerpt_target_bytes":512});
    if let Some(branch) = matched.get("branch_ref") {
        selection["branch_ref"] = branch.clone();
    }
    let (excerpt_error, excerpt) = call(
        &c,
        "read_session",
        json!({"session_id":codex_session,"selection":selection}),
    )
    .await;
    assert!(!excerpt_error, "{excerpt}");
    assert_eq!(excerpt["source_policy"], "require_source");
    assert!(!excerpt.to_string().contains("MUST_NOT_INDEX"));
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
    let matched = &r["results"][0]["matches"][0];
    let mut selection =
        json!({"kind":"match","match_ref":matched["match_ref"],"excerpt_target_bytes":512});
    if let Some(branch) = matched.get("branch_ref") {
        selection["branch_ref"] = branch.clone();
    }
    let (excerpt_error, excerpt) = call(
        &c,
        "read_session",
        json!({"session_id":claude_session,"selection":selection}),
    )
    .await;
    assert!(!excerpt_error, "{excerpt}");
    assert_eq!(excerpt["source_policy"], "require_source");
    assert!(!excerpt.to_string().contains("MUST_NOT_INDEX"));
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

const NATIVE_QUERY: &str = "copperlattice";

async fn open_native_fixture(path: &Path) -> SqliteConnection {
    let mut writer = SqliteConnection::connect_with(
        &SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal),
    )
    .await
    .unwrap();
    writer
        .execute(
            r#"
            PRAGMA wal_autocheckpoint=0;
            CREATE TABLE session_v2(id TEXT PRIMARY KEY,parent_id TEXT,directory TEXT NOT NULL,title TEXT,time_updated INTEGER NOT NULL);
            CREATE TABLE session_message(id TEXT PRIMARY KEY,session_id TEXT NOT NULL,type TEXT NOT NULL,seq INTEGER NOT NULL,data TEXT NOT NULL,UNIQUE(session_id,seq));
            CREATE TABLE session_part(message_id TEXT NOT NULL,session_id TEXT NOT NULL,ordinal INTEGER NOT NULL,type TEXT NOT NULL,data TEXT NOT NULL,PRIMARY KEY(message_id,ordinal));
            CREATE TABLE event_sequence(aggregate_id TEXT PRIMARY KEY,seq INTEGER NOT NULL);
            BEGIN;
            INSERT INTO session_v2 VALUES ('ses_native',NULL,'/synthetic','Native fixture',1795104000000);
            INSERT INTO event_sequence VALUES ('ses_native',41);
            INSERT INTO session_message VALUES
              ('msg_z','ses_native','user',2,'{"text":"A native fixture question"}'),
              ('msg_b','ses_native','assistant',7,'{"content":[{"type":"text","text":"An inline basaltbeacon answer"}]}'),
              ('msg_a','ses_native','assistant',11,'{"content":[]}');
            INSERT INTO session_part VALUES
              ('msg_a','ses_native',4,'text','{"text":"Second piece"}'),
              ('msg_a','ses_native',0,'reasoning','{"text":"privatequartz"}'),
              ('msg_a','ses_native',2,'text','{"text":"First copperlattice piece"}');
            COMMIT;
            "#,
        )
        .await
        .unwrap();
    writer
}

fn index(home: &Path) {
    let (ok, out) = cli(home, &["index", "--since", "2026-10-12", "--wait"]);
    assert!(ok, "{out}");
}

async fn lexical(c: &Client, query: &str, source: &str) -> Value {
    let (err, r) = call(
        c,
        "search_sessions",
        json!({"query":query,"mode":"lexical","sources":[source],"session_kinds":["interactive","unknown"]}),
    )
    .await;
    assert!(!err, "{r}");
    r
}

fn match_request(search: &Value) -> Value {
    let result = &search["results"][0];
    let m = &result["matches"][0];
    assert!(
        result["session_id"].is_string(),
        "no search result: {search}"
    );
    assert!(m["match_ref"].is_string(), "no match reference: {search}");
    let mut request = json!({
        "session_id": result["session_id"],
        "source_policy": "require_source",
        "verbose": true,
        "selection": {"kind":"match","match_ref":m["match_ref"]}
    });
    if let Some(branch) = m.get("branch_ref") {
        request["selection"]["branch_ref"] = branch.clone();
    }
    request
}

async fn verified_match(c: &Client, request: &Value, text: &str, record: Option<&str>) {
    let (err, r) = call(c, "read_session", request.clone()).await;
    assert!(!err, "{r}");
    let r = drain(c, r).await;
    let items = r["items"].as_array().unwrap();
    assert!(!items.is_empty(), "{r}");
    assert!(
        items
            .iter()
            .all(|i| i["provenance"]["origin"] == "verified_source"),
        "{r}"
    );
    assert!(
        items.iter().any(|i| {
            record.is_none_or(|id| i["provenance"]["native_record_id"] == id)
                && i["blocks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|b| b["data"].as_str().is_some_and(|s| s.contains(text)))
        }),
        "{r}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opencode_v2_and_jsonl_search_and_verify_source_changes() {
    let tmp = sandbox();
    let db = tmp.path().join("native.db");
    let mut writer = open_native_fixture(&db).await;
    assert!(db.with_file_name("native.db-wal").metadata().unwrap().len() > 0);
    let env = setup(tmp, Some(&db), false);
    index(&env.home);
    let c = mcp(&env.home).await;

    let codex = match_request(&lexical(&c, "Commentary is indexed", "codex").await);
    verified_match(&c, &codex, "Commentary is indexed", None).await;
    let native = match_request(&lexical(&c, NATIVE_QUERY, "oc").await);
    verified_match(&c, &native, "First copperlattice piece", Some("msg_a")).await;
    let inline = match_request(&lexical(&c, "basaltbeacon", "oc").await);
    verified_match(&c, &inline, "An inline basaltbeacon answer", Some("msg_b")).await;
    assert_eq!(
        lexical(&c, "privatequartz", "oc").await["results"],
        json!([])
    );

    writer.execute(
        r#"BEGIN;
           INSERT INTO session_message VALUES ('msg_new','ses_native','user',15,'{"text":"A later feldsparbridge question"}');
           UPDATE event_sequence SET seq=64 WHERE aggregate_id='ses_native';
           COMMIT;"#,
    ).await.unwrap();
    verified_match(&c, &native, "First copperlattice piece", Some("msg_a")).await;
    index(&env.home);
    let append = match_request(&lexical(&c, "feldsparbridge", "oc").await);
    verified_match(
        &c,
        &append,
        "A later feldsparbridge question",
        Some("msg_new"),
    )
    .await;

    // Edit an old row, without changing either its message sequence or session time.
    writer.execute(
        r#"BEGIN;
           UPDATE session_part SET data='{"text":"Changed azuritespire piece"}' WHERE message_id='msg_a' AND ordinal=2;
           UPDATE event_sequence SET seq=65 WHERE aggregate_id='ses_native';
           COMMIT;"#,
    ).await.unwrap();
    let mut prefer_source = native;
    prefer_source["source_policy"] = json!("prefer_source");
    let (err, changed) = call(&c, "read_session", prefer_source).await;
    assert!(err, "{changed}");
    assert!(
        matches!(
            changed["error"]["code"].as_str(),
            Some("SOURCE_CHANGED" | "STALE_REFERENCE")
        ),
        "{changed}"
    );

    writer.close().await.unwrap();
    index(&env.home);
    let changed = match_request(&lexical(&c, "azuritespire", "oc").await);
    verified_match(&c, &changed, "Changed azuritespire piece", Some("msg_a")).await;
    assert_eq!(lexical(&c, NATIVE_QUERY, "oc").await["results"], json!([]));

    let moved = db.with_extension("moved");
    std::fs::rename(&db, &moved).unwrap();
    std::fs::copy(&moved, &db).unwrap();
    let (err, replacement) = call(&c, "read_session", changed).await;
    assert!(err, "{replacement}");
    assert_eq!(replacement["error"]["code"], "SOURCE_CHANGED");
    let (ok, doctor) = cli(&env.home, &["doctor", "--json"]);
    assert!(!ok, "{doctor}");
    let report: Value = serde_json::from_str(&doctor).unwrap();
    assert!(
        report["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| { check["name"] == "source_identity" && check["code"] == "SOURCE_MOVED" }),
        "{report}"
    );
    verified_match(&c, &codex, "Commentary is indexed", None).await;
    c.cancel().await.unwrap();
}

fn opencode_command(bin: &Path, spool_home: &Path, project: &Path) -> tokio::process::Command {
    let root = project.parent().unwrap();
    let mut cmd = tokio::process::Command::new(bin);
    cmd.env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", root.join("home"))
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("XDG_DATA_HOME", root.join("data"))
        .env("XDG_STATE_HOME", root.join("state"))
        .env("XDG_CACHE_HOME", root.join("cache"))
        .env("XDG_RUNTIME_DIR", root.join("runtime"))
        .env("TMPDIR", root)
        .env("OPENCODE_DB", root.join("client.db"))
        .env("OPENCODE_DISABLE_AUTOUPDATE", "true")
        .env("OPENCODE_DISABLE_FREE_MODELS", "true")
        .env("OPENCODE_DISABLE_MODELS_FETCH", "true")
        .env("SPOOL_HOME", spool_home)
        .env("SPOOL_FAKE_API_KEY", "local-fixture")
        .current_dir(project)
        .kill_on_drop(true);
    cmd
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn factory_empty_session_becomes_searchable_and_can_be_emptied_again() {
    use std::io::Write;

    let tmp = sandbox();
    let path = tmp.path().join(FACTORY_PATH);
    let env = setup(tmp, None, true);
    let (header, messages) = FACTORY_FIXTURE.split_once('\n').unwrap();
    std::fs::write(&path, format!("{header}\n")).unwrap();
    let c = mcp(&env.home).await;
    let status = |id: Option<Value>| async {
        let (err, result) = call(
            &c,
            "index_status",
            match id {
                Some(id) => json!({"session_id": id}),
                None => json!({"source": "factory"}),
            },
        )
        .await;
        assert!(!err, "{result}");
        result
    };
    let index_factory = || {
        let (ok, out) = cli(
            &env.home,
            &["index", "--all", "--source", "factory", "--wait"],
        );
        assert!(ok, "{out}");
    };

    index_factory();
    let initial = status(None).await;
    let session = initial["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["kind"] == "interactive")
        .unwrap();
    let id = session["session_id"].clone();
    assert_eq!(session["index_state"], "empty", "{initial}");
    assert_eq!(session["has_published_snapshot"], false);
    assert_eq!(
        initial["sources"][0]["sessions_by_state"],
        json!({"empty": 1})
    );
    assert_eq!(initial["queue"]["empty"], 1);
    assert_eq!(initial["queue"]["indexed"], 0);
    let (err, empty_read) = call(&c, "read_session", json!({"session_id": id})).await;
    assert!(!err, "{empty_read}");
    assert_eq!(empty_read["items"], json!([]));
    index_factory();
    assert_eq!(
        status(Some(id.clone())).await["session"]["index_state"],
        "empty"
    );

    let mut transcript = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    writeln!(transcript, r#"{{"type":"todo_state","todos":[]}}"#).unwrap();
    index_factory();
    assert_eq!(
        status(Some(id.clone())).await["session"]["index_state"],
        "empty"
    );
    transcript.write_all(messages.as_bytes()).unwrap();
    drop(transcript);
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if status(Some(id.clone())).await["session"]["index_state"] == "indexed" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "discovery never indexed the changed file"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let hit = match_request(&lexical(&c, "beryllattice", "factory").await);
    assert_eq!(hit["session_id"], id);
    verified_match(
        &c,
        &hit,
        "Please catalogue the beryllattice sample.",
        Some("f1"),
    )
    .await;
    assert_eq!(
        status(Some(id.clone())).await["session"]["index_state"],
        "indexed"
    );

    std::fs::write(&path, format!("{header}\n")).unwrap();
    index_factory();
    let settled = status(Some(id)).await;
    assert_eq!(settled["session"]["index_state"], "empty", "{settled}");
    assert_eq!(settled["session"]["has_published_snapshot"], false);
    assert_eq!(
        lexical(&c, "beryllattice", "factory").await["results"],
        json!([])
    );
    let (err, stale) = call(&c, "read_session", hit).await;
    assert!(err && stale["error"]["code"] == "SOURCE_CHANGED", "{stale}");
    c.cancel().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn factory_droid_discovers_searches_and_verifies_append_and_rewrite() {
    use std::io::Write;

    let tmp = sandbox();
    let path = tmp.path().join(FACTORY_PATH);
    let env = setup(tmp, None, true);
    index(&env.home);
    let c = mcp(&env.home).await;

    let original = match_request(&lexical(&c, "beryllattice", "factory").await);
    verified_match(
        &c,
        &original,
        "Please catalogue the beryllattice sample.",
        Some("f1"),
    )
    .await;
    for term in [
        "hookzircon",
        "argumenttopaz",
        "reasoningcitrine",
        "resultspinel",
        "todoquartz",
        "noticeperidot",
        "childamethyst",
        "sidecarlapis",
    ] {
        assert_eq!(
            lexical(&c, term, "factory").await["results"],
            json!([]),
            "{term}"
        );
    }
    for view in ["with_tools", "raw_records"] {
        let mut request = original.clone();
        request["view"] = json!(view);
        let (err, result) = call(&c, "read_session", request).await;
        assert!(err, "{result}");
        assert_eq!(result["error"]["code"], "VIEW_NOT_SUPPORTED");
    }
    assert_eq!(std::fs::read_to_string(&path).unwrap(), FACTORY_FIXTURE);

    let mut session = json!({
        "session_id": original["session_id"],
        "source_policy": "require_source",
        "max_items": 1,
    });
    let (err, first_page) = call(&c, "read_session", session.clone()).await;
    assert!(
        !err && first_page["page"]["next_cursor"].is_string(),
        "{first_page}"
    );
    let append = r#"{"type":"message","id":"f7","parentId":"f6","seq":7,"timestamp":"2026-11-25T09:00:07.000Z","message":{"role":"user","content":[{"type":"text","text":"Save the new celestitebridge sample too."}]}}"#;
    writeln!(
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap(),
        "{append}"
    )
    .unwrap();

    let frozen = drain(&c, first_page).await;
    assert_eq!(kinds(&frozen).len(), 4);
    assert!(!frozen["items"].to_string().contains("celestitebridge"));
    verified_match(
        &c,
        &original,
        "Please catalogue the beryllattice sample.",
        Some("f1"),
    )
    .await;
    index(&env.home);
    let appended = match_request(&lexical(&c, "celestitebridge", "factory").await);
    assert_eq!(appended["session_id"], original["session_id"]);
    verified_match(
        &c,
        &appended,
        "Save the new celestitebridge sample too.",
        Some("f7"),
    )
    .await;

    let rewritten = format!(
        "{}{append}\n",
        FACTORY_FIXTURE.replace("beryllattice", "tourmalinespire")
    );
    std::fs::write(&path, &rewritten).unwrap();
    let mut stale = original;
    stale["source_policy"] = json!("prefer_source");
    let (err, result) = call(&c, "read_session", stale).await;
    assert!(err, "{result}");
    assert_eq!(result["error"]["code"], "SOURCE_CHANGED", "{result}");
    index(&env.home);
    assert_eq!(
        lexical(&c, "beryllattice", "factory").await["results"],
        json!([])
    );
    let changed = match_request(&lexical(&c, "tourmalinespire", "factory").await);
    assert_eq!(changed["session_id"], session["session_id"]);
    verified_match(
        &c,
        &changed,
        "Please catalogue the tourmalinespire sample.",
        Some("f1"),
    )
    .await;
    session["max_items"] = json!(50);
    let (err, fresh) = call(&c, "read_session", session).await;
    assert!(!err, "{fresh}");
    assert_eq!(kinds(&drain(&c, fresh).await).len(), 5);
    assert_eq!(std::fs::read_to_string(&path).unwrap(), rewritten);
    c.cancel().await.unwrap();
}

async fn model_request(socket: &mut tokio::net::TcpStream) -> Option<Value> {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
    let mut reader = BufReader::new(socket);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    if line.is_empty() {
        return None;
    }
    assert!(line.starts_with("POST /v1/chat/completions "), "{line}");
    let mut length = None;
    loop {
        line.clear();
        assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
        if line == "\r\n" {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = Some(value.trim().parse::<usize>().unwrap());
        }
    }
    let length = length.expect("model request uses Content-Length");
    assert!(length <= 2_000_000);
    let mut body = vec![0; length];
    reader.read_exact(&mut body).await.unwrap();
    Some(serde_json::from_slice(&body).unwrap())
}

fn find_json(value: &Value, key: &str) -> Option<Value> {
    match value {
        Value::Object(map) if map.contains_key(key) => Some(value.clone()),
        Value::Object(map) => map.values().find_map(|v| find_json(v, key)),
        Value::Array(items) => items.iter().find_map(|v| find_json(v, key)),
        Value::String(text) => serde_json::from_str::<Value>(text)
            .ok()
            .and_then(|v| find_json(&v, key)),
        _ => None,
    }
}

fn model_has_tool(tools: &[Value], name: &str) -> bool {
    tools.iter().any(|tool| tool["function"]["name"] == name)
}

fn model_delta(request: &Value, stage: &mut u8) -> (Value, &'static str) {
    let Some(tools) = request["tools"]
        .as_array()
        .filter(|tools| !tools.is_empty())
    else {
        return (json!({"content":"Spool smoke fixture"}), "stop");
    };
    assert!(model_has_tool(tools, "spool_search_sessions"));
    assert!(model_has_tool(tools, "spool_read_session"));
    let messages = request["messages"].as_array().unwrap();
    let result = messages.iter().rev().find(|m| m["role"] == "tool");
    let (name, args, id) = match *stage {
        0 => {
            assert!(result.is_none(), "{request}");
            (
                "spool_search_sessions",
                json!({"query":NATIVE_QUERY,"mode":"lexical","sources":["oc"],"session_kinds":["interactive","unknown"]}),
                "call_smoke_search",
            )
        }
        1 => {
            let result = result.and_then(|m| find_json(&m["content"], "results"));
            let args = match_request(&result.expect("OpenCode model received MCP search results"));
            ("spool_read_session", args, "call_smoke_read")
        }
        2 => {
            let r = result
                .and_then(|m| find_json(&m["content"], "items"))
                .expect("OpenCode model received MCP read result");
            assert!(
                r["items"].as_array().unwrap().iter().any(|i| {
                    i["provenance"]["origin"] == "verified_source"
                        && i["provenance"]["native_record_id"] == "msg_a"
                        && i["blocks"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|b| b["data"] == "First copperlattice piece")
                }),
                "{r}"
            );
            *stage = 3;
            return (json!({"content":"Verified Spool match"}), "stop");
        }
        _ => return (json!({"content":"Spool smoke fixture"}), "stop"),
    };
    *stage += 1;
    (
        json!({"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":name,"arguments":args.to_string()}}]}),
        "tool_calls",
    )
}

async fn model_stub(listener: tokio::net::TcpListener) {
    use tokio::io::AsyncWriteExt;
    let mut stage = 0;
    while stage < 3 {
        let (mut socket, _) = listener.accept().await.unwrap();
        let Some(request) = model_request(&mut socket).await else {
            continue;
        };
        let (mut delta, finish) = model_delta(&request, &mut stage);
        delta["role"] = json!("assistant");
        let chunk = |delta: Value, reason: Value| {
            json!({
                "id":"fixture","object":"chat.completion.chunk","created":0,"model":"fixture",
                "choices":[{"index":0,"delta":delta,"finish_reason":reason}]
            })
        };
        let body = format!(
            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            chunk(delta, Value::Null),
            chunk(json!({}), json!(finish))
        );
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires SPOOL_OPENCODE_V2_BIN pointing to OpenCode V2.3.2"]
async fn opencode_v2_client_searches_and_reopens_match() {
    let bin = std::fs::canonicalize(
        std::env::var_os("SPOOL_OPENCODE_V2_BIN").expect("set SPOOL_OPENCODE_V2_BIN"),
    )
    .expect("resolve OpenCode executable");
    let tmp = sandbox();
    let project = tmp.path().join("oc/project");
    std::fs::create_dir_all(&project).unwrap();
    let version = opencode_command(&bin, &tmp.path().join("h"), &project)
        .arg("--version")
        .output()
        .await
        .unwrap();
    assert!(version.status.success(), "{version:?}");
    assert_eq!(
        String::from_utf8_lossy(&version.stdout).trim(),
        "opencode v2.3.2"
    );

    let db = tmp.path().join("native.db");
    open_native_fixture(&db).await.close().await.unwrap();
    let env = setup(tmp, Some(&db), false);
    index(&env.home);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}/v1", listener.local_addr().unwrap());
    let config = Command::new(BIN)
        .args(["mcp", "config", "--client", "opencode"])
        .env("SPOOL_HOME", &env.home)
        .output()
        .unwrap();
    assert!(config.status.success(), "{config:?}");
    let mut config: Value = serde_json::from_slice(&config.stdout).unwrap();
    config["mcp"]["servers"]["spool"]["codemode"] = json!(false);
    config["model"] = json!("fake/fixture");
    config["providers"] = json!({"fake":{
        "name":"Local smoke fake",
        "env":["SPOOL_FAKE_API_KEY"],
        "package":"@opencode/ai/providers/openai-compatible",
        "settings":{"baseURL":base_url,"encoding":"none","timeout":5000},
        "models":{"fixture":{"name":"Fixture","capabilities":{"tools":true},"limit":{"context":65536,"output":4096}}}
    }});
    std::fs::write(project.join("opencode.json"), config.to_string()).unwrap();

    let mut client = opencode_command(&bin, &env.home, &project);
    client.args([
        "--standalone",
        "run",
        "--model",
        "fake/fixture",
        "--title",
        "Spool smoke",
        "--format",
        "json",
        "--auto",
        "Search Spool for copperlattice and reopen the match.",
    ]);
    let (completed, output) = tokio::time::timeout(Duration::from_secs(45), async {
        let model = model_stub(listener);
        let run = client.output();
        tokio::pin!(model, run);
        tokio::select! {
            () = &mut model => (true, run.await),
            output = &mut run => (false, output),
        }
    })
    .await
    .expect("OpenCode did not complete the search and read loop");
    let output = output.unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(
        completed && stdout.contains("Verified Spool match"),
        "{stdout}\n{stderr}"
    );
}
