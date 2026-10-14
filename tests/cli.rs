//! CLI tests: argument validation, date parsing with a fixed clock, `mcp config`, `init`,
//! `config show`, daemon-unavailable behaviour, request mapping and `--wait` against an
//! in-process fake daemon, doctor, and local file logging. Synthetic data only; no network.

use clap::Parser;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use serde_json::{Value, json};
use spool::cli::args::{Cli, Cmd, IndexArgs, ReadArgs, SearchArgs};
use spool::cli::init::{Prompter, run_init};
use spool::cli::requests::{index_request, read_request, search_request};
use spool::cli::timeparse::{format_rfc3339, parse_when};
use spool::clock::{Tz, UTC};
use spool::config::Paths;
use spool::ipc::{AppError, Request, server::Handler};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const BIN: &str = env!("CARGO_BIN_EXE_spool");
/// 2026-10-12T12:00:00Z
const NOW: i64 = 1_791_806_400_000;

fn mode(p: &Path) -> u32 {
    std::fs::metadata(p).unwrap().permissions().mode() & 0o777
}

/// Short path under /tmp keeps the socket under the sockaddr_un limit.
fn temp_home() -> (tempfile::TempDir, PathBuf) {
    let t = tempfile::Builder::new()
        .prefix("sp")
        .tempdir_in("/tmp")
        .unwrap();
    let h = t.path().join("h");
    (t, h)
}

fn spool(home: &Path, args: &[&str]) -> Command {
    let mut c = Command::new(BIN);
    c.args(args)
        .env("SPOOL_HOME", home)
        .env("HOME", home.parent().unwrap())
        // Never look at the real service label.
        .env("SPOOL_SERVICE_LABEL", "com.spool.test.cli-nonexistent")
        .env_remove("RUST_LOG")
        .stdin(std::process::Stdio::null());
    c
}

fn run(home: &Path, args: &[&str]) -> Output {
    spool(home, args).output().unwrap()
}

fn out(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}
fn err(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).into_owned()
}

fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
    let mut v = vec!["spool"];
    v.extend_from_slice(args);
    Cli::try_parse_from(v)
}

fn read_args(args: &[&str]) -> ReadArgs {
    match parse(&[&["read"], args].concat()).unwrap().cmd {
        Cmd::Read(a) => a,
        _ => panic!("not read"),
    }
}

fn search_args(args: &[&str]) -> SearchArgs {
    match parse(&[&["search"], args].concat()).unwrap().cmd {
        Cmd::Search(a) => a,
        _ => panic!("not search"),
    }
}

fn index_args(args: &[&str]) -> IndexArgs {
    match parse(&[&["index"], args].concat()).unwrap().cmd {
        Cmd::Index(a) => a,
        _ => panic!("not index"),
    }
}

// ---------------------------------------------------------------- dates

#[test]
fn relative_dates_use_the_fixed_clock() {
    assert_eq!(format_rfc3339(NOW, UTC), "2026-10-12T12:00:00Z");
    assert_eq!(parse_when("7d", NOW, UTC).unwrap(), NOW - 7 * 86_400_000);
    assert_eq!(parse_when("12h", NOW, UTC).unwrap(), NOW - 12 * 3_600_000);
    assert_eq!(parse_when("2w", NOW, UTC).unwrap(), NOW - 14 * 86_400_000);
    assert_eq!(parse_when("30m", NOW, UTC).unwrap(), NOW - 30 * 60_000);
    assert_eq!(
        format_rfc3339(parse_when("7d", NOW, UTC).unwrap(), UTC),
        "2026-10-05T12:00:00Z"
    );
}

#[test]
fn civil_dates_honour_the_timezone() {
    let plus2 = Tz::parse("+02:00").unwrap();
    assert_eq!(
        format_rfc3339(parse_when("2026-10-01", NOW, UTC).unwrap(), UTC),
        "2026-10-01T00:00:00Z"
    );
    assert_eq!(
        format_rfc3339(parse_when("2026-10-01", NOW, plus2).unwrap(), UTC),
        "2026-09-30T22:00:00Z"
    );
    assert_eq!(
        format_rfc3339(parse_when("2026-10-01T08:30", NOW, plus2).unwrap(), UTC),
        "2026-10-01T06:30:00Z"
    );
    // An explicit offset in the input wins over --tz.
    assert_eq!(
        format_rfc3339(parse_when("2026-10-01T00:00:00Z", NOW, plus2).unwrap(), UTC),
        "2026-10-01T00:00:00Z"
    );
    assert_eq!(
        format_rfc3339(
            parse_when("2026-10-01T00:00:00-05:30", NOW, UTC).unwrap(),
            UTC
        ),
        "2026-10-01T05:30:00Z"
    );
    assert_eq!(format_rfc3339(NOW, plus2), "2026-10-12T14:00:00+02:00");
    // Leap day round trip.
    let leap = parse_when("2028-02-29T23:59:59Z", NOW, UTC).unwrap();
    assert_eq!(format_rfc3339(leap, UTC), "2028-02-29T23:59:59Z");
}

#[test]
fn bad_dates_and_zones_are_rejected() {
    for bad in [
        "",
        "soon",
        "7",
        "d",
        "-3d",
        "2026-13-01",
        "2026-02-30",
        "2026-10-01T25:00",
        "2026-1-1",
        "7days",
    ] {
        assert!(parse_when(bad, NOW, UTC).is_err(), "{bad:?} should fail");
    }
    assert!(Tz::parse("Europe/Berlin").is_err());
    assert!(Tz::parse("+15:00").is_err());
    assert_eq!(Tz::parse("-0530").unwrap(), Tz(-(5 * 3600 + 1800)));
    assert_eq!(Tz::parse("utc").unwrap(), UTC);
    assert_eq!(Tz::parse("+02:00").unwrap().label(), "+02:00");
}

// ---------------------------------------------------------------- argument parsing

#[test]
fn the_spec_command_surface_parses() {
    let ok: &[&[&str]] = &[
        &["init"],
        &[
            "init",
            "--enable-source",
            "codex",
            "--hosted-processing",
            "no",
        ],
        &["daemon", "run"],
        &["service", "install"],
        &["service", "uninstall"],
        &["service", "start"],
        &["service", "stop"],
        &["service", "restart"],
        &["status", "--json"],
        &["logs", "--follow"],
        &["doctor", "--json", "--probe-provider"],
        &[
            "search", "q", "--mode", "literal", "--source", "omp", "--since", "7d", "--limit", "10",
        ],
        &[
            "read",
            "ss_1",
            "--match-ref",
            "R",
            "--before",
            "1",
            "--after",
            "3",
            "--with-tools",
            "--source-policy",
            "indexed_only",
            "--max-bytes",
            "8192",
            "--max-items",
            "10",
        ],
        &[
            "read",
            "ss_1",
            "--from-ref",
            "A",
            "--through-ref",
            "B",
            "--branch-ref",
            "X",
            "--raw",
        ],
        &["read", "--cursor", "C", "--max-bytes", "4096"],
        &[
            "index",
            "--since",
            "7d",
            "--source",
            "omp",
            "--dry-run",
            "--wait",
        ],
        &["index", "--all", "--dry-run"],
        &["index", "--session", "ss_1", "--wait"],
        &["index", "rebuild", "--lexical-only"],
        &["index", "rebuild", "--embeddings-only"],
        &["index", "remove", "--session", "ss_1", "--exclude"],
        &["index", "purge", "--unreferenced"],
        &["jobs", "status", "op"],
        &["jobs", "retry", "op"],
        &["jobs", "cancel", "op"],
        &["sources", "list"],
        &["sources", "enable", "codex"],
        &["sources", "disable", "codex"],
        &["handles", "rotate-key"],
        &["mcp"],
        &["mcp", "config", "--client", "omp"],
        &["config", "show"],
        &["--tz", "+02:00", "index", "--since", "2026-10-01"],
    ];
    for a in ok {
        parse(a).unwrap_or_else(|e| panic!("{a:?} failed: {e}"));
    }
    assert!(matches!(
        parse(&["mcp"]).unwrap().cmd,
        Cmd::Mcp { sub: None }
    ));
}

#[test]
fn unknown_values_are_rejected_by_clap() {
    assert!(parse(&["search", "q", "--mode", "fuzzy"]).is_err());
    assert!(parse(&["read", "s", "--source-policy", "x"]).is_err());
    assert!(parse(&["mcp", "config", "--client", "cursor"]).is_err());
    assert!(parse(&["mcp", "config"]).is_err());
    assert!(parse(&["index", "rebuild", "--lexical-only", "--embeddings-only"]).is_err());
    assert!(parse(&["--tz", "Mars/Olympus", "status"]).is_err());
    assert!(parse(&["init", "--hosted-processing", "maybe"]).is_err());
    // Administration has no MCP-side counterpart and no implicit-all verb.
    assert!(parse(&["index", "--since", "7d", "rebuild"]).is_err());
}

// ---------------------------------------------------------------- read mapping + validation

#[test]
fn read_flags_map_one_to_one() {
    let r = read_request(&read_args(&[
        "ss_1",
        "--match-ref",
        "M",
        "--before",
        "1",
        "--after",
        "0",
        "--with-tools",
        "--max-bytes",
        "8192",
        "--max-items",
        "7",
        "--source-policy",
        "require_source",
        "--branch-ref",
        "B",
    ]))
    .unwrap();
    assert_eq!(
        r,
        json!({"session_id":"ss_1","selection":{"kind":"match","match_ref":"M","before":1,"after":0,"branch_ref":"B"},
               "view":"with_tools","source_policy":"require_source","max_bytes":8192,"max_items":7})
    );
    let r = read_request(&read_args(&["ss_1", "--message-ref", "X"])).unwrap();
    assert_eq!(
        r,
        json!({"session_id":"ss_1","selection":{"kind":"message","message_ref":"X"}})
    );
    let r = read_request(&read_args(&["ss_1", "--item-ref", "I", "--raw"])).unwrap();
    assert_eq!(
        r,
        json!({"session_id":"ss_1","selection":{"kind":"item","item_ref":"I"},"view":"raw_records"})
    );
    let r = read_request(&read_args(&[
        "ss_1",
        "--from-ref",
        "A",
        "--through-ref",
        "B",
    ]))
    .unwrap();
    assert_eq!(
        r["selection"],
        json!({"kind":"range","from_message_ref":"A","through_message_ref":"B"})
    );
    let r = read_request(&read_args(&["ss_1"])).unwrap();
    assert_eq!(
        r,
        json!({"session_id":"ss_1","selection":{"kind":"session"}})
    );
    let r = read_request(&read_args(&["ss_1", "--branch-ref", "B"])).unwrap();
    assert_eq!(r["selection"], json!({"kind":"session","branch_ref":"B"}));
    let r = read_request(&read_args(&[
        "--cursor",
        "C",
        "--max-bytes",
        "4096",
        "--max-items",
        "1",
    ]))
    .unwrap();
    assert_eq!(r, json!({"cursor":"C","max_bytes":4096,"max_items":1}));
}

#[test]
fn incompatible_read_flags_are_rejected_before_any_call() {
    let bad: &[&[&str]] = &[
        &["ss_1", "--match-ref", "M", "--message-ref", "X"],
        &["ss_1", "--match-ref", "M", "--item-ref", "I"],
        &[
            "ss_1",
            "--item-ref",
            "I",
            "--from-ref",
            "A",
            "--through-ref",
            "B",
        ],
        &["ss_1", "--from-ref", "A"],
        &["ss_1", "--through-ref", "B"],
        &["ss_1", "--with-tools", "--raw"],
        &["ss_1", "--before", "2"],
        &["ss_1", "--item-ref", "I", "--after", "1"],
        &[
            "ss_1",
            "--from-ref",
            "A",
            "--through-ref",
            "B",
            "--before",
            "1",
        ],
        &["ss_1", "--match-ref", "M", "--before", "21"],
        &["ss_1", "--item-ref", "I", "--branch-ref", "B"],
        &["ss_1", "--max-bytes", "100"],
        &["ss_1", "--max-bytes", "999999"],
        &["ss_1", "--max-items", "0"],
        &["ss_1", "--max-items", "201"],
        &[],
        &["--cursor", "C", "ss_1"],
        &["--cursor", "C", "--match-ref", "M"],
        &["--cursor", "C", "--before", "1"],
        &["--cursor", "C", "--with-tools"],
        &["--cursor", "C", "--raw"],
        &["--cursor", "C", "--source-policy", "indexed_only"],
        &["--cursor", "C", "--branch-ref", "B"],
    ];
    for a in bad {
        assert!(
            read_request(&read_args(a)).is_err(),
            "{a:?} should be rejected"
        );
    }
}

#[test]
fn invalid_read_flags_exit_1_without_touching_the_daemon() {
    let (_t, home) = temp_home(); // no daemon: a call would exit 3
    let o = run(&home, &["read", "ss_1", "--with-tools", "--raw"]);
    assert_eq!(o.status.code(), Some(1), "{}", err(&o));
    assert!(err(&o).contains("mutually exclusive"));
    let o = run(&home, &["read", "--cursor", "C", "--match-ref", "M"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(err(&o).contains("--cursor accepts only"));
}

#[test]
fn search_and_index_requests_resolve_relative_dates() {
    let r = search_request(
        &search_args(&[
            "hello",
            "--since",
            "7d",
            "--until",
            "2026-10-11",
            "--source",
            "omp",
            "--source",
            "codex",
            "--mode",
            "lexical",
            "--limit",
            "3",
        ]),
        NOW,
        UTC,
    )
    .unwrap();
    assert_eq!(
        r,
        json!({"query":"hello","mode":"lexical","sources":["omp","codex"],"after":"2026-10-05T12:00:00Z","before":"2026-10-11T00:00:00Z","limit":3})
    );
    assert_eq!(
        search_request(&search_args(&["hello"]), NOW, UTC).unwrap(),
        json!({"query":"hello"})
    );
    assert!(
        search_request(
            &search_args(&["hello", "--since", "2026-10-11", "--until", "2026-10-10"]),
            NOW,
            UTC
        )
        .is_err()
    );
    assert!(search_request(&search_args(&["  "]), NOW, UTC).is_err());
    assert!(search_request(&search_args(&["q", "--since", "yesterday"]), NOW, UTC).is_err());

    let c = index_request(
        &index_args(&["--since", "7d", "--source", "omp", "--dry-run"]),
        NOW,
        UTC,
        "k1",
    )
    .unwrap();
    assert_eq!(c.cmd, "index_since");
    assert_eq!(c.since_ms, Some(NOW - 7 * 86_400_000));
    assert_eq!(
        c.args,
        json!({"since_ms": NOW - 7 * 86_400_000, "source":"omp","dry_run":true,"idempotency_key":"k1"})
    );
    let c = index_request(&index_args(&["--all"]), NOW, UTC, "k2").unwrap();
    assert_eq!(
        (c.cmd, c.args),
        ("index_all", json!({"dry_run":false,"idempotency_key":"k2"}))
    );
    let c = index_request(&index_args(&["--session", "ss_9"]), NOW, UTC, "k3").unwrap();
    assert_eq!(
        (c.cmd, c.args),
        (
            "index_session",
            json!({"session_id":"ss_9","idempotency_key":"k3"})
        )
    );
    let c = index_request(
        &index_args(&["rebuild", "--embeddings-only"]),
        NOW,
        UTC,
        "k4",
    )
    .unwrap();
    assert_eq!(
        (c.cmd, c.args),
        (
            "index_rebuild",
            json!({"lexical_only":false,"embeddings_only":true,"idempotency_key":"k4"})
        )
    );
    let c = index_request(
        &index_args(&["remove", "--session", "ss_9", "--exclude"]),
        NOW,
        UTC,
        "k5",
    )
    .unwrap();
    assert_eq!(
        (c.cmd, c.args),
        (
            "index_remove",
            json!({"session_id":"ss_9","exclude":true,"idempotency_key":"k5"})
        )
    );
    let c = index_request(&index_args(&["purge", "--unreferenced"]), NOW, UTC, "k6").unwrap();
    assert_eq!(
        (c.cmd, c.args),
        (
            "index_purge",
            json!({"unreferenced":true,"idempotency_key":"k6"})
        )
    );
}

#[test]
fn index_modes_are_explicit_and_exclusive() {
    for bad in [
        &[][..],
        &["--all", "--since", "7d"],
        &["--all", "--session", "s"],
        &["--session", "s", "--dry-run"],
        &["--session", "s", "--source", "omp"],
        &["--since", "someday"],
        &["purge"],
    ] {
        assert!(
            index_request(&index_args(bad), NOW, UTC, "k").is_err(),
            "{bad:?}"
        );
    }
}

// ---------------------------------------------------------------- mcp config

#[test]
fn mcp_config_claude_and_omp_use_the_verified_json_shape() {
    let want = json!({"mcpServers":{"spool":{"type":"stdio","command":"/opt/spool/bin/spool","args":["mcp"]}}});
    for client in ["claude", "omp"] {
        let (_t, home) = temp_home();
        let o = run(
            &home,
            &[
                "mcp",
                "config",
                "--client",
                client,
                "--bin",
                "/opt/spool/bin/spool",
            ],
        );
        assert!(o.status.success(), "{}", err(&o));
        assert_eq!(serde_json::from_str::<Value>(&out(&o)).unwrap(), want);
    }
}

#[test]
fn mcp_config_codex_is_toml_and_carries_no_secrets_or_databases() {
    let (_t, home) = temp_home();
    let o = run(
        &home,
        &[
            "mcp",
            "config",
            "--client",
            "codex",
            "--bin",
            "/opt/spool/bin/spool",
        ],
    );
    assert!(o.status.success(), "{}", err(&o));
    let text = out(&o);
    let v: toml::Table = toml::from_str(&text).unwrap();
    let s = &v["mcp_servers"]["spool"];
    assert_eq!(s["command"].as_str(), Some("/opt/spool/bin/spool"));
    assert_eq!(
        s["args"].as_array().unwrap(),
        &vec![toml::Value::String("mcp".into())]
    );
    assert!(text.contains("[mcp_servers.spool]"), "{text}");
    for client in ["claude", "codex", "omp"] {
        let o = run(
            &home,
            &[
                "mcp",
                "config",
                "--client",
                client,
                "--bin",
                "/opt/spool/bin/spool",
            ],
        );
        let t = out(&o).to_lowercase();
        for forbidden in ["key", "token", "sqlite", "credential", "spool_home", "env"] {
            assert!(!t.contains(forbidden), "{client}: {forbidden} in output");
        }
    }
    // It only prints: nothing was written anywhere under the fake HOME.
    assert!(!home.parent().unwrap().join(".codex").exists());
    assert!(!home.parent().unwrap().join(".mcp.json").exists());
}

#[test]
fn mcp_config_defaults_to_this_binary_and_requires_an_absolute_path() {
    let (_t, home) = temp_home();
    let o = run(&home, &["mcp", "config", "--client", "claude"]);
    let v: Value = serde_json::from_str(&out(&o)).unwrap();
    let cmd = v["mcpServers"]["spool"]["command"].as_str().unwrap();
    assert!(Path::new(cmd).is_absolute());
    assert_eq!(
        std::fs::canonicalize(cmd).unwrap(),
        std::fs::canonicalize(BIN).unwrap()
    );
    let o = run(
        &home,
        &["mcp", "config", "--client", "claude", "--bin", "spool"],
    );
    assert!(!o.status.success());
    assert!(err(&o).contains("absolute"));
}

// ---------------------------------------------------------------- init

struct Script(Vec<bool>, Vec<String>);

impl Prompter for Script {
    fn ask(&mut self, q: &str, default: bool) -> std::io::Result<bool> {
        self.1.push(q.to_string());
        Ok(if self.0.is_empty() {
            default
        } else {
            self.0.remove(0)
        })
    }
}

fn load(home: &Path) -> spool::config::Config {
    spool::config::Config::load(&Paths::from_root(home.to_path_buf()).unwrap()).unwrap()
}

#[test]
fn init_creates_private_layout_and_keeps_consent_off() {
    let (_t, home) = temp_home();
    let paths = Paths::from_root(home.clone()).unwrap();
    let r = run_init(&paths, &Default::default(), None).unwrap();
    assert!(r.config_written && r.key_created && !r.hosted_processing_approved);
    assert_eq!(mode(&home), 0o700);
    assert_eq!(mode(&paths.state_dir), 0o700);
    assert_eq!(mode(&paths.secrets_dir), 0o700);
    assert_eq!(mode(&paths.signing_key), 0o600);
    assert_eq!(mode(&paths.config_file), 0o600);
    spool::transcript::handle::HandleKey::load(&paths.signing_key).unwrap();
    let cfg = load(&home);
    assert!(!cfg.embedding.hosted_processing_approved);
    assert!(
        r.notes.iter().any(|n| n.contains("safe defaults")),
        "{:?}",
        r.notes
    );
    // Recognized roots are reported; no source is enabled.
    assert!(
        r.sources.iter().all(|s| !s.enabled),
        "init without flags must enable nothing: {:?}",
        r.sources
    );
    assert!(load(&home).sources.iter().all(|s| !s.enabled));
    // Re-running is idempotent and keeps the same key.
    let key = std::fs::read(&paths.signing_key).unwrap();
    let r2 = run_init(&paths, &Default::default(), None).unwrap();
    assert!(!r2.config_written && !r2.key_created);
    assert_eq!(std::fs::read(&paths.signing_key).unwrap(), key);
}

#[test]
fn init_flags_decide_non_interactively_and_never_flip_consent_silently() {
    let (_t, home) = temp_home();
    let paths = Paths::from_root(home.clone()).unwrap();
    let a = match parse(&[
        "init",
        "--enable-source",
        "codex",
        "--disable-source",
        "omp",
        "--hosted-processing",
        "yes",
    ])
    .unwrap()
    .cmd
    {
        Cmd::Init(a) => a,
        _ => unreachable!(),
    };
    let r = run_init(&paths, &a, None).unwrap();
    assert!(r.hosted_processing_approved);
    let cfg = load(&home);
    assert!(cfg.embedding.hosted_processing_approved);
    let on: Vec<_> = cfg
        .sources
        .iter()
        .filter(|s| s.enabled)
        .map(|s| s.id.clone())
        .collect();
    assert_eq!(on, vec!["codex"]);
    assert!(r.notes.iter().any(|n| n.contains("provider key")));
    // Flags against an existing config are refused instead of silently applied.
    assert!(run_init(&paths, &a, None).is_err());
    // --force rewrites.
    let forced = match parse(&["init", "--force", "--hosted-processing", "no"])
        .unwrap()
        .cmd
    {
        Cmd::Init(a) => a,
        _ => unreachable!(),
    };
    run_init(&paths, &forced, None).unwrap();
    assert!(!load(&home).embedding.hosted_processing_approved);
}

#[test]
fn init_rejects_unknown_and_contradictory_sources() {
    let (_t, home) = temp_home();
    let paths = Paths::from_root(home.clone()).unwrap();
    let mk = |args: &[&str]| match parse(&[&["init"], args].concat()).unwrap().cmd {
        Cmd::Init(a) => a,
        _ => unreachable!(),
    };
    assert!(run_init(&paths, &mk(&["--enable-source", "cursor"]), None).is_err());
    assert!(
        run_init(
            &paths,
            &mk(&["--enable-source", "omp", "--disable-source", "omp"]),
            None
        )
        .is_err()
    );
    assert!(
        !paths.config_file.exists(),
        "a rejected init must not write config"
    );
}

#[test]
fn interactive_init_asks_and_defaults_consent_to_no() {
    let (_t, home) = temp_home();
    let paths = Paths::from_root(home.clone()).unwrap();
    // Questions: omp, codex, claude, hosted. Answer: omp yes, codex yes, claude no, hosted no.
    let mut s = Script(vec![true, true, false, false], vec![]);
    run_init(&paths, &Default::default(), Some(&mut s)).unwrap();
    assert_eq!(s.1.len(), 4);
    assert!(s.1[3].contains("Allow hosted embedding"));
    let cfg = load(&home);
    assert!(!cfg.embedding.hosted_processing_approved);
    assert_eq!(cfg.sources.iter().filter(|c| c.enabled).count(), 2);
    // Only an explicit yes turns consent on.
    let (_t2, home2) = temp_home();
    let paths2 = Paths::from_root(home2.clone()).unwrap();
    let mut s = Script(vec![true, false, false, true], vec![]);
    run_init(&paths2, &Default::default(), Some(&mut s)).unwrap();
    assert!(load(&home2).embedding.hosted_processing_approved);
    // A prompter that just accepts defaults keeps consent off.
    let (_t3, home3) = temp_home();
    let paths3 = Paths::from_root(home3.clone()).unwrap();
    run_init(
        &paths3,
        &Default::default(),
        Some(&mut Script(vec![], vec![])),
    )
    .unwrap();
    assert!(!load(&home3).embedding.hosted_processing_approved);
}

#[test]
fn init_binary_without_a_tty_keeps_safe_defaults_and_says_so() {
    let (_t, home) = temp_home();
    let o = run(&home, &["init"]);
    assert!(o.status.success(), "{}", err(&o));
    let t = out(&o);
    assert!(t.contains("safe defaults"), "{t}");
    assert!(t.contains("not approved"), "{t}");
    assert!(t.contains("nothing has been read or indexed"), "{t}");
    assert_eq!(mode(&home.join("secrets/handle-signing.key")), 0o600);
}

// ---------------------------------------------------------------- config show

#[test]
fn config_show_marks_defaults_and_overrides_and_redacts() {
    let (_t, home) = temp_home();
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(
        home.join("config.toml"),
        "config_version = 1\n[embedding]\nhosted_processing_approved = true\nendpoint = \"https://user:pw@example.test/v1/embeddings\"\n[search]\ndefault_limit = 7\n",
    )
    .unwrap();
    let o = run(&home, &["config", "show"]);
    assert!(o.status.success(), "{}", err(&o));
    let t = out(&o);
    assert!(
        t.contains("default_limit = 7  # override (default: 10)"),
        "{t}"
    );
    assert!(t.contains("max_limit = 50  # default"), "{t}");
    assert!(
        t.contains("hosted_processing_approved = true  # override (default: false)"),
        "{t}"
    );
    assert!(t.contains("<redacted>@example.test"), "{t}");
    assert!(!t.contains("pw@"), "{t}");
    assert!(
        t.contains("credential_file = \"secrets/provider.key\"  # default"),
        "{t}"
    );
    assert!(t.contains("[[sources]]"), "{t}");
}

#[test]
fn redaction_hides_secret_named_fields() {
    use spool::cli::confshow::redact;
    assert_eq!(
        redact("api_key", &toml::Value::String("sk-live".into())),
        toml::Value::String("<redacted>".into())
    );
    assert_eq!(
        redact("provider_key", &toml::Value::String("x".into())),
        toml::Value::String("<redacted>".into())
    );
    assert_eq!(
        redact("daily_query_token_budget", &toml::Value::Integer(5)),
        toml::Value::Integer(5)
    );
    assert_eq!(
        redact(
            "credential_file",
            &toml::Value::String("secrets/provider.key".into())
        ),
        toml::Value::String("secrets/provider.key".into())
    );
}

#[test]
fn config_show_rejects_an_invalid_config() {
    let (_t, home) = temp_home();
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(home.join("config.toml"), "mystery = 1\n").unwrap();
    let o = run(&home, &["config", "show"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(err(&o).contains("config.toml"));
}

// ---------------------------------------------------------------- daemon unavailable

#[test]
fn daemon_dependent_commands_explain_how_to_start_the_daemon() {
    let (_t, home) = temp_home();
    for args in [
        &["status"][..],
        &["status", "--json"],
        &["search", "hello"],
        &["read", "ss_1"],
        &["index", "--since", "7d"],
        &["index", "--session", "ss_1"],
        &["jobs", "status", "op_1"],
        &["sources", "list"],
        &["handles", "rotate-key"],
    ] {
        let o = run(&home, args);
        assert_eq!(o.status.code(), Some(3), "{args:?}: {}", err(&o));
        let e = err(&o);
        assert!(
            e.contains("spool service start") && e.contains("spool daemon run"),
            "{args:?}: {e}"
        );
        assert!(e.contains("spool.sock"), "{e}");
        assert!(
            out(&o).is_empty() || args[0] == "index",
            "{args:?} printed {}",
            out(&o)
        );
    }
}

// ---------------------------------------------------------------- fake daemon: mapping + --wait

type Seen = Arc<Mutex<Vec<(String, Value)>>>;

fn fake_daemon(home: &Path) -> (Seen, tokio::sync::watch::Sender<bool>) {
    std::fs::create_dir_all(home.join("state")).unwrap();
    let listener = tokio::net::UnixListener::bind(home.join("state/spool.sock")).unwrap();
    let seen: Seen = Default::default();
    let polls = Arc::new(AtomicUsize::new(0));
    let seen2 = seen.clone();
    let handler: Handler = Arc::new(
        move |req: Request| -> BoxFuture<'static, Result<Value, AppError>> {
            let (seen, polls) = (seen2.clone(), polls.clone());
            Box::pin(async move {
                let (name, args) = match req {
                    Request::Control { cmd, args, .. } => (cmd, args),
                    Request::Tool { tool, args, .. } => (format!("tool:{tool}"), args),
                };
                seen.lock().push((name.clone(), args.clone()));
                match name.as_str() {
                    "index_since" | "index_all" | "index_session"
                        if args["dry_run"] != json!(true) =>
                    {
                        Ok(json!({"operation_id":"op_test","state":"queued"}))
                    }
                    "index_since" | "index_all" => {
                        Ok(json!({"candidate_sessions":3,"estimated_tokens":1200,"excluded":1}))
                    }
                    "jobs_status" => Ok(match polls.fetch_add(1, Ordering::SeqCst) {
                        0 => json!({"operation_id":"op_test","state":"queued"}),
                        1 => {
                            json!({"operation_id":"op_test","state":"running","progress":{"done":1,"total":3}})
                        }
                        _ => {
                            json!({"operation_id":"op_test","state":"done","progress":{"done":3,"total":3}})
                        }
                    }),
                    "tool:search_sessions" => Ok(json!({
                    "mode_requested":"hybrid","mode_used":"lexical","warnings":[{"code":"query_embedding_unavailable"}],
                    "results":[{"session_id":"ss_a","source":"omp","title":"Synthetic","last_activity_at":"2026-10-10T00:00:00Z",
                                "matches":[{"match_ref":"MREF","excerpt":"USER: hello\nASSISTANT: world"}]}],
                    "truncated":false})),
                    "tool:read_session" => Ok(json!({"echo": args})),
                    "status" => Ok(
                        json!({"state":"lexical_only","daemon_version":"9.9.9","queue":{"queued":2},"workers":{"ingest":"healthy"}}),
                    ),
                    "handles_rotate_key" => Ok(json!({"epoch":2})),
                    _ => Err(AppError::new("OPERATION_NOT_FOUND", "no such operation")),
                }
            })
        },
    );
    let (tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(spool::ipc::server::serve(listener, handler, 4, 1 << 20, rx));
    (seen, tx)
}

async fn run_async(home: PathBuf, args: Vec<String>) -> Output {
    tokio::task::spawn_blocking(move || {
        let a: Vec<&str> = args.iter().map(String::as_str).collect();
        run(&home, &a)
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_since_wait_polls_progress_and_prints_the_resolved_cutoff() {
    let (_t, home) = temp_home();
    let (seen, _stop) = fake_daemon(&home);
    let o = run_async(
        home.clone(),
        vec![
            "--tz".into(),
            "+02:00".into(),
            "index".into(),
            "--since".into(),
            "7d".into(),
            "--source".into(),
            "omp".into(),
            "--wait".into(),
        ],
    )
    .await;
    assert!(o.status.success(), "{}", err(&o));
    let t = out(&o);
    assert!(
        t.contains("since: ") && t.contains("+02:00") && t.contains("timezone +02:00"),
        "{t}"
    );
    assert!(t.contains("op_test: queued"), "{t}");
    assert!(
        t.contains("op_test: running") && t.contains("\"done\":1"),
        "{t}"
    );
    assert!(t.contains("op_test: done"), "{t}");
    let seen = seen.lock();
    let (name, args) = &seen[0];
    assert_eq!(name, "index_since");
    let since = args["since_ms"].as_i64().unwrap();
    let age = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        - since;
    assert!(
        (7 * 86_400_000..7 * 86_400_000 + 120_000).contains(&age),
        "age {age}"
    );
    assert_eq!(args["source"], "omp");
    assert_eq!(args["dry_run"], false);
    assert!(
        args["idempotency_key"]
            .as_str()
            .unwrap()
            .starts_with("idem_")
    );
    assert!(seen.iter().filter(|(n, _)| n == "jobs_status").count() >= 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dry_run_prints_the_report_and_does_not_poll() {
    let (_t, home) = temp_home();
    let (seen, _stop) = fake_daemon(&home);
    let o = run_async(
        home.clone(),
        vec![
            "index".into(),
            "--since".into(),
            "2026-10-01".into(),
            "--dry-run".into(),
        ],
    )
    .await;
    assert!(o.status.success(), "{}", err(&o));
    let t = out(&o);
    assert!(t.contains("since: 2026-10-01T00:00:00Z"), "{t}");
    assert!(t.contains("\"candidate_sessions\": 3"), "{t}");
    assert!(!t.contains("operation"), "{t}");
    let s = seen.lock();
    assert_eq!(s.len(), 1);
    assert_eq!(s[0].1["dry_run"], true);
    assert_eq!(s[0].1["since_ms"], 1_790_812_800_000_i64);
    let o = run_async(
        home.clone(),
        vec![
            "index".into(),
            "--since".into(),
            "7d".into(),
            "--dry-run".into(),
            "--wait".into(),
        ],
    )
    .await;
    assert_eq!(o.status.code(), Some(1));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_read_status_and_errors_flow_through_the_socket() {
    let (_t, home) = temp_home();
    let (seen, _stop) = fake_daemon(&home);
    let o = run_async(
        home.clone(),
        vec![
            "search".into(),
            "hello".into(),
            "--since".into(),
            "7d".into(),
            "--limit".into(),
            "2".into(),
        ],
    )
    .await;
    assert!(o.status.success(), "{}", err(&o));
    let t = out(&o);
    assert!(t.contains("mode: lexical (requested hybrid)"), "{t}");
    assert!(
        t.contains("warning:") && t.contains("query_embedding_unavailable"),
        "{t}"
    );
    assert!(t.contains("ss_a  [omp]  Synthetic"), "{t}");
    assert!(
        t.contains("match_ref: MREF") && t.contains("USER: hello ASSISTANT: world"),
        "{t}"
    );

    let o = run_async(
        home.clone(),
        [
            "read",
            "ss_a",
            "--match-ref",
            "MREF",
            "--before",
            "1",
            "--raw",
        ]
        .map(String::from)
        .to_vec(),
    )
    .await;
    assert!(o.status.success(), "{}", err(&o));
    let v: Value = serde_json::from_str(&out(&o)).unwrap();
    assert_eq!(
        v["echo"]["selection"],
        json!({"kind":"match","match_ref":"MREF","before":1})
    );
    assert_eq!(v["echo"]["view"], "raw_records");

    let o = run_async(home.clone(), vec!["status".into()]).await;
    let t = out(&o);
    assert!(
        t.contains("state: lexical_only")
            && t.contains("queued: 2")
            && t.contains("ingest: healthy"),
        "{t}"
    );

    let o = run_async(
        home.clone(),
        vec!["jobs".into(), "cancel".into(), "nope".into()],
    )
    .await;
    assert_eq!(o.status.code(), Some(1));
    assert!(err(&o).contains("OPERATION_NOT_FOUND"), "{}", err(&o));

    let o = run_async(home.clone(), vec!["handles".into(), "rotate-key".into()]).await;
    assert!(out(&o).contains("now invalid"), "{}", out(&o));

    let s = seen.lock();
    let search = s.iter().find(|(n, _)| n == "tool:search_sessions").unwrap();
    assert!(search.1["after"].as_str().unwrap().ends_with('Z'));
    assert_eq!(search.1["limit"], 2);
}

// ---------------------------------------------------------------- doctor

#[test]
fn doctor_reports_missing_init_then_passes_and_catches_loose_modes() {
    let (_t, home) = temp_home();
    let o = run(&home, &["doctor", "--json"]);
    assert_eq!(o.status.code(), Some(1), "missing key must fail");
    let v: Value = serde_json::from_str(&out(&o)).unwrap();
    assert_eq!(v["ok"], false);
    assert!(
        v["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c["name"] == "signing_key" && c["status"] == "fail")
    );

    assert!(run(&home, &["init"]).status.success());
    let o = run(&home, &["doctor", "--json"]);
    let v: Value = serde_json::from_str(&out(&o)).unwrap();
    assert!(o.status.success(), "{}", out(&o));
    let names: Vec<&str> = v["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    for want in [
        "config",
        "permissions",
        "signing_key",
        "database",
        "sqlite",
        "source",
        "embedding",
        "provider",
        "daemon",
        "service",
        "logs",
    ] {
        assert!(names.contains(&want), "missing check {want}: {names:?}");
    }
    let by = |n: &str| {
        v["checks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == n)
            .unwrap()
            .clone()
    };
    assert!(by("sqlite")["detail"].as_str().unwrap().contains("FTS5"));
    assert_eq!(by("daemon")["status"], "warn");
    assert!(
        by("daemon")["detail"]
            .as_str()
            .unwrap()
            .contains("spool service start")
    );
    assert!(
        by("embedding")["detail"]
            .as_str()
            .unwrap()
            .contains("NOT approved")
    );
    assert!(
        by("provider")["detail"]
            .as_str()
            .unwrap()
            .contains("not probed")
    );

    // Loose permissions on the key and on the state directory are failures.
    std::fs::set_permissions(
        home.join("secrets/handle-signing.key"),
        std::fs::Permissions::from_mode(0o644),
    )
    .unwrap();
    std::fs::set_permissions(home.join("state"), std::fs::Permissions::from_mode(0o755)).unwrap();
    let o = run(&home, &["doctor"]);
    assert_eq!(o.status.code(), Some(1));
    let t = out(&o);
    assert!(
        t.contains("[FAIL] permissions") && t.contains("handle-signing.key"),
        "{t}"
    );
    assert!(t.contains("state"), "{t}");
}

#[test]
fn doctor_probe_provider_refuses_without_consent_and_never_calls_out() {
    let (_t, home) = temp_home();
    assert!(run(&home, &["init"]).status.success());
    let o = run(&home, &["doctor", "--probe-provider", "--json"]);
    let v: Value = serde_json::from_str(&out(&o)).unwrap();
    let p = v["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "provider")
        .unwrap()
        .clone();
    assert_eq!(p["status"], "fail");
    assert!(p["detail"].as_str().unwrap().contains("not attempted"));
}

#[test]
fn doctor_flags_a_broken_config() {
    let (_t, home) = temp_home();
    std::fs::create_dir_all(&home).unwrap();
    std::fs::write(home.join("config.toml"), "[search]\nmax_limit = 500\n").unwrap();
    let o = run(&home, &["doctor"]);
    assert_eq!(o.status.code(), Some(1));
    assert!(out(&o).contains("[FAIL] config"), "{}", out(&o));
}

// ---------------------------------------------------------------- telemetry

#[test]
fn log_files_rotate_by_size_and_stay_private() {
    use spool::telemetry::RotatingFile;
    use std::io::Write;
    let t = tempfile::tempdir().unwrap();
    let dir = t.path().join("logs");
    let mut f = RotatingFile::open(&dir, "t.log", 100, 2).unwrap();
    for i in 0..50 {
        f.write_all(format!("line {i:013}\n").as_bytes()).unwrap(); // 19 bytes
    }
    assert_eq!(mode(&dir), 0o700);
    for n in ["t.log", "t.log.1", "t.log.2"] {
        let p = dir.join(n);
        assert!(p.exists(), "{n}");
        assert!(std::fs::metadata(&p).unwrap().len() <= 100, "{n} too big");
        assert_eq!(mode(&p), 0o600);
    }
    assert!(!dir.join("t.log.3").exists());
    let newest = std::fs::read_to_string(dir.join("t.log")).unwrap();
    assert!(newest.contains("line 0000000000049"), "{newest}");
    let older = std::fs::read_to_string(dir.join("t.log.1")).unwrap();
    assert!(older.contains("line 00000000000"), "{older}");
}

#[test]
fn tracing_events_land_in_the_rotating_file() {
    use spool::telemetry::{RotatingFile, SharedLog};
    let t = tempfile::tempdir().unwrap();
    let w = SharedLog::new(
        RotatingFile::open(t.path(), "spool.log", 1 << 20, 1).unwrap(),
        false,
    );
    let sub = tracing_subscriber::fmt()
        .with_writer(w)
        .with_ansi(false)
        .finish();
    tracing::subscriber::with_default(sub, || {
        tracing::info!(operation = "op_1", sessions = 3, "discovery finished")
    });
    let text = std::fs::read_to_string(t.path().join("spool.log")).unwrap();
    assert!(
        text.contains("discovery finished")
            && text.contains("operation=\"op_1\"")
            && text.contains("sessions=3"),
        "{text}"
    );
}

#[test]
fn logs_command_prints_the_tail() {
    let (_t, home) = temp_home();
    let o = run(&home, &["logs"]);
    assert!(o.status.success());
    assert!(err(&o).contains("no log file yet"));
    std::fs::create_dir_all(home.join("logs")).unwrap();
    std::fs::write(
        home.join("logs/spool.log"),
        (1..=10).map(|i| format!("l{i}\n")).collect::<String>(),
    )
    .unwrap();
    let o = run(&home, &["logs", "--lines", "3"]);
    assert_eq!(out(&o), "l8\nl9\nl10\n");
    assert_eq!(spool::cli::logs::last_lines("a\nb\nc", 2), vec!["b", "c"]);
}

// ---------------------------------------------------------------- shipped example config

#[test]
fn config_example_is_valid_and_equals_the_built_in_defaults() {
    let root = Path::new("/tmp/spool-example-root");
    let example =
        spool::config::Config::parse(include_str!("../config.example.toml"), root).unwrap();
    let defaults = spool::config::Config::parse(
        &toml::to_string(&spool::config::Config::default()).unwrap(),
        root,
    )
    .unwrap();
    assert_eq!(example, defaults);
    assert!(
        !example.embedding.hosted_processing_approved,
        "the example must not enable hosted processing"
    );
}

#[test]
fn init_enable_source_enables_only_the_named_source() {
    let (_t, home) = temp_home();
    let o = run(&home, &["init", "--enable-source", "omp"]);
    assert!(o.status.success(), "{}", err(&o));
    let on: Vec<_> = load(&home)
        .sources
        .iter()
        .filter(|s| s.enabled)
        .map(|s| s.id.clone())
        .collect();
    assert_eq!(on, vec!["omp"]);
    let (_t, home) = temp_home();
    assert!(
        run(&home, &["init", "--enable-source", "codex"])
            .status
            .success()
    );
    let on: Vec<_> = load(&home)
        .sources
        .iter()
        .filter(|s| s.enabled)
        .map(|s| s.id.clone())
        .collect();
    assert_eq!(on, vec!["codex"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retryable_daemon_not_ready_is_retried_then_succeeds() {
    let (_t, home) = temp_home();
    std::fs::create_dir_all(home.join("state")).unwrap();
    let listener = tokio::net::UnixListener::bind(home.join("state/spool.sock")).unwrap();
    let n = Arc::new(AtomicUsize::new(0));
    let n2 = n.clone();
    let handler: Handler = Arc::new(
        move |_req: Request| -> BoxFuture<'static, Result<Value, AppError>> {
            let n = n2.clone();
            Box::pin(async move {
                if n.fetch_add(1, Ordering::SeqCst) < 3 {
                    let mut e = AppError::new("DAEMON_NOT_READY", "job system is starting");
                    e.retryable = true;
                    Err(e)
                } else {
                    Ok(json!({"state":"ready"}))
                }
            })
        },
    );
    let (_tx, rx) = tokio::sync::watch::channel(false);
    tokio::spawn(spool::ipc::server::serve(listener, handler, 4, 1 << 20, rx));
    let o = run_async(home.clone(), vec!["status".into(), "--json".into()]).await;
    assert!(o.status.success(), "{}", err(&o));
    assert!(out(&o).contains("ready"));
    assert_eq!(n.load(Ordering::SeqCst), 4);
}
