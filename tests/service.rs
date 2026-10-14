//! Service tests: plist/unit rendering and linting, foreign-definition protection, the exact
//! launchctl/systemctl command sequences (recording runner, isolated labels, temp directories),
//! and one opt-in real launchd lifecycle (`SPOOL_TEST_LAUNCHD=1`) under a random
//! `com.spool.test.*` label. Nothing here ever touches the `com.spool.daemon` label.

use parking_lot::Mutex;
use spool::service::{
    CmdOutput, CommandRunner, DEFAULT_LABEL, Definition, Platform, ServiceConfig, ServiceManager,
    SystemRunner,
};
use std::io;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_spool");

/// Records every command; emulates "loaded" state for `launchctl print`.
#[derive(Default)]
struct Recorder {
    calls: Mutex<Vec<String>>,
    loaded: AtomicBool,
}

impl Recorder {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().clone()
    }
}

impl CommandRunner for Recorder {
    fn run(&self, program: &str, args: &[String]) -> io::Result<CmdOutput> {
        let line = format!("{program} {}", args.join(" "));
        self.calls.lock().push(line);
        let ok = |stdout: &str| {
            Ok(CmdOutput {
                success: true,
                stdout: stdout.into(),
                stderr: String::new(),
            })
        };
        let no = || {
            Ok(CmdOutput {
                success: false,
                stdout: String::new(),
                stderr: "not found".into(),
            })
        };
        match (program, args.first().map(String::as_str)) {
            ("launchctl", Some("print")) if self.loaded.load(Ordering::SeqCst) => {
                ok("state = running\n")
            }
            ("launchctl", Some("print")) => no(),
            ("launchctl", Some("bootstrap")) => {
                self.loaded.store(true, Ordering::SeqCst);
                ok("")
            }
            ("launchctl", Some("bootout")) => {
                self.loaded.store(false, Ordering::SeqCst);
                ok("")
            }
            ("systemctl", Some("--user"))
                if args.get(1).map(String::as_str) == Some("is-enabled")
                    || args.get(1).map(String::as_str) == Some("is-active") =>
            {
                no()
            }
            _ => ok(""),
        }
    }
}

fn config(platform: Platform, dir: &Path, label: &str) -> ServiceConfig {
    ServiceConfig {
        platform,
        label: label.into(),
        binary: PathBuf::from("/opt/spool/bin/spool"),
        definition_dir: dir.join("defs"),
        log_dir: dir.join("logs"),
        spool_home: None,
        home: Some(PathBuf::from("/Users/test")),
        uid: 501,
    }
}

fn manager(platform: Platform, dir: &Path, label: &str) -> (ServiceManager, Arc<Recorder>) {
    let rec = Arc::new(Recorder::default());
    (
        ServiceManager::new(config(platform, dir, label), rec.clone()).unwrap(),
        rec,
    )
}

fn plutil(args: &[&str]) -> Option<std::process::Output> {
    Command::new("plutil").args(args).output().ok()
}

// ---------------------------------------------------------------- rendering

#[test]
fn plist_has_the_required_launchd_shape_and_lints() {
    let t = tempfile::tempdir().unwrap();
    let (m, _) = manager(Platform::Launchd, t.path(), DEFAULT_LABEL);
    let text = m.render();
    for want in [
        "<key>Label</key><string>com.spool.daemon</string>",
        "<key>RunAtLoad</key><true/>",
        "<key>SuccessfulExit</key><false/>",
        "<key>ThrottleInterval</key><integer>30</integer>",
        "<key>ProcessType</key><string>Background</string>",
    ] {
        assert!(text.contains(want), "missing {want}\n{text}");
    }
    assert!(text.contains(&format!(
        "<key>StandardOutPath</key><string>{}/logs/daemon.stdout.log</string>",
        t.path().display()
    )));
    assert!(text.contains(&format!(
        "<key>StandardErrorPath</key><string>{}/logs/daemon.stderr.log</string>",
        t.path().display()
    )));
    let args = text
        .split("<key>ProgramArguments</key>")
        .nth(1)
        .unwrap()
        .split("</array>")
        .next()
        .unwrap();
    let items: Vec<&str> = args
        .split("<string>")
        .skip(1)
        .map(|s| s.split("</string>").next().unwrap())
        .collect();
    assert_eq!(items, vec!["/opt/spool/bin/spool", "daemon", "run"]);
    // Environment: fixed PATH and HOME only; nothing inherited from a shell.
    let env = text
        .split("<key>EnvironmentVariables</key>")
        .nth(1)
        .unwrap();
    assert!(env.contains("<key>PATH</key>") && env.contains("<key>HOME</key>"));
    assert!(!env.contains("SPOOL_HOME") && !env.contains("KEY") && !env.contains("TOKEN"));

    // Lint the real file the manager writes.
    let rec_dir = t.path().join("defs");
    std::fs::create_dir_all(&rec_dir).unwrap();
    let file = rec_dir.join("x.plist");
    std::fs::write(&file, &text).unwrap();
    match plutil(&["-lint", file.to_str().unwrap()]) {
        Some(o) => assert!(
            o.status.success(),
            "plutil -lint: {}{}",
            String::from_utf8_lossy(&o.stdout),
            String::from_utf8_lossy(&o.stderr)
        ),
        None => eprintln!("plutil unavailable; plist lint skipped"),
    }
    if let Some(o) = plutil(&["-convert", "json", "-o", "-", file.to_str().unwrap()]) {
        let v: serde_json::Value = serde_json::from_slice(&o.stdout).unwrap();
        assert_eq!(v["Label"], "com.spool.daemon");
        assert_eq!(v["KeepAlive"]["SuccessfulExit"], false);
        assert_eq!(
            v["ProgramArguments"],
            serde_json::json!(["/opt/spool/bin/spool", "daemon", "run"])
        );
        assert_eq!(v["ThrottleInterval"], 30);
        assert_eq!(v["RunAtLoad"], true);
    }
}

#[test]
fn plist_escapes_xml_and_forwards_an_isolated_home() {
    let t = tempfile::tempdir().unwrap();
    let mut cfg = config(Platform::Launchd, t.path(), "com.spool.test.x");
    cfg.binary = PathBuf::from("/opt/a&b/<spool>");
    cfg.spool_home = Some(PathBuf::from("/tmp/sp1/h"));
    let m = ServiceManager::new(cfg, Arc::new(Recorder::default())).unwrap();
    let text = m.render();
    assert!(text.contains("/opt/a&amp;b/&lt;spool&gt;"));
    assert!(text.contains("<key>SPOOL_HOME</key><string>/tmp/sp1/h</string>"));
    let file = t.path().join("e.plist");
    std::fs::write(&file, &text).unwrap();
    if let Some(o) = plutil(&["-lint", file.to_str().unwrap()]) {
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stdout));
    }
}

#[test]
fn systemd_unit_text_is_exact() {
    let t = tempfile::tempdir().unwrap();
    let (m, _) = manager(Platform::Systemd, t.path(), DEFAULT_LABEL);
    assert_eq!(
        m.render(),
        "# Managed by spool; regenerate with `spool service install`.\n\
         [Unit]\nDescription=Spool local agent-session search daemon\n\n\
         [Service]\nType=simple\nExecStart=\"/opt/spool/bin/spool\" daemon run\nRestart=on-failure\nRestartSec=5\nTimeoutStopSec=20\n\n\
         [Install]\nWantedBy=default.target\n"
    );
    assert_eq!(m.unit_name(), "spool-daemon.service");
    assert!(m.definition_path().ends_with("defs/spool-daemon.service"));
    let mut cfg = config(Platform::Systemd, t.path(), "com.spool.test.u1");
    cfg.spool_home = Some(PathBuf::from("/tmp/a b/h"));
    let m = ServiceManager::new(cfg, Arc::new(Recorder::default())).unwrap();
    assert!(
        m.render()
            .contains("Environment=\"SPOOL_HOME=/tmp/a b/h\"\n")
    );
    assert_eq!(m.unit_name(), "com.spool.test.u1.service");
}

#[test]
fn invalid_labels_relative_binaries_and_root_are_refused() {
    let t = tempfile::tempdir().unwrap();
    for bad in ["", "../evil", "a b", "x/y", "<&>"] {
        assert!(
            ServiceManager::new(
                config(Platform::Launchd, t.path(), bad),
                Arc::new(Recorder::default())
            )
            .is_err(),
            "{bad:?}"
        );
    }
    let mut cfg = config(Platform::Launchd, t.path(), "com.spool.test.r");
    cfg.binary = PathBuf::from("spool");
    assert!(ServiceManager::new(cfg, Arc::new(Recorder::default())).is_err());
    let mut cfg = config(Platform::Launchd, t.path(), "com.spool.test.r");
    cfg.uid = 0;
    let rec = Arc::new(Recorder::default());
    let m = ServiceManager::new(cfg, rec.clone()).unwrap();
    for r in [m.install(), m.start(), m.stop(), m.restart(), m.uninstall()] {
        assert!(r.unwrap_err().to_string().contains("root"));
    }
    assert!(rec.calls().is_empty());
}

// ---------------------------------------------------------------- command sequences

#[test]
fn launchd_install_start_stop_restart_uninstall_sequence() {
    let t = tempfile::tempdir().unwrap();
    let label = "com.spool.test.seq";
    let (m, rec) = manager(Platform::Launchd, t.path(), label);
    let plist = t.path().join(format!("defs/{label}.plist"));

    m.install().unwrap();
    assert!(plist.exists());
    assert_eq!(
        std::fs::metadata(&plist).unwrap().permissions().mode_bits() & 0o777,
        0o644
    );
    assert_eq!(m.definition().unwrap(), Definition::Managed);
    assert_eq!(
        std::fs::metadata(t.path().join("logs"))
            .unwrap()
            .permissions()
            .mode_bits()
            & 0o777,
        0o700
    );
    assert_eq!(
        rec.calls(),
        vec![
            format!("launchctl print gui/501/{label}"),
            format!("launchctl bootstrap gui/501 {}", plist.display())
        ]
    );

    // Re-install replaces our own definition: boots the old one out first.
    rec.calls.lock().clear();
    m.install().unwrap();
    let c = rec.calls();
    assert_eq!(c[1], format!("launchctl bootout gui/501/{label}"));
    assert!(c[2].starts_with("launchctl bootstrap gui/501 "), "{c:?}");

    rec.calls.lock().clear();
    m.restart().unwrap();
    assert!(
        rec.calls()
            .contains(&format!("launchctl kickstart -k gui/501/{label}")),
        "{:?}",
        rec.calls()
    );

    rec.calls.lock().clear();
    m.stop().unwrap();
    assert!(
        rec.calls()
            .contains(&format!("launchctl bootout gui/501/{label}"))
    );
    assert!(m.stop().unwrap().contains("not loaded"));

    rec.calls.lock().clear();
    m.start().unwrap();
    let c = rec.calls();
    assert!(
        c.iter()
            .any(|l| l.starts_with("launchctl bootstrap gui/501 ")),
        "{c:?}"
    );
    assert!(
        c.contains(&format!("launchctl kickstart gui/501/{label}")),
        "{c:?}"
    );

    rec.calls.lock().clear();
    m.uninstall().unwrap();
    assert!(!plist.exists());
    assert!(
        rec.calls()
            .contains(&format!("launchctl bootout gui/501/{label}"))
    );
    assert_eq!(m.uninstall().unwrap(), "service is not installed");

    // Never the real label.
    assert!(rec.calls().iter().all(|c| !c.contains(DEFAULT_LABEL)));
}

#[test]
fn uninstall_preserves_config_index_keys_and_credentials() {
    let t = tempfile::tempdir().unwrap();
    let keep: Vec<PathBuf> = [
        "config.toml",
        "state/index.sqlite",
        "secrets/handle-signing.key",
        "secrets/provider.key",
    ]
    .iter()
    .map(|p| t.path().join(p))
    .collect();
    for p in &keep {
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, "x").unwrap();
    }
    let (m, _) = manager(Platform::Launchd, t.path(), "com.spool.test.keep");
    m.install().unwrap();
    m.uninstall().unwrap();
    assert!(keep.iter().all(|p| p.exists()));
}

#[test]
fn systemd_sequence_enables_without_lingering() {
    let t = tempfile::tempdir().unwrap();
    let (m, rec) = manager(Platform::Systemd, t.path(), DEFAULT_LABEL);
    let msg = m.install().unwrap();
    assert!(msg.contains("Lingering is not enabled"));
    assert_eq!(
        rec.calls(),
        vec![
            "systemctl --user daemon-reload",
            "systemctl --user enable spool-daemon.service"
        ]
    );
    rec.calls.lock().clear();
    m.start().unwrap();
    m.restart().unwrap();
    m.stop().unwrap();
    assert_eq!(
        rec.calls(),
        vec![
            "systemctl --user start spool-daemon.service",
            "systemctl --user restart spool-daemon.service",
            "systemctl --user stop spool-daemon.service"
        ]
    );
    rec.calls.lock().clear();
    m.uninstall().unwrap();
    assert_eq!(
        rec.calls(),
        vec![
            "systemctl --user stop spool-daemon.service",
            "systemctl --user disable spool-daemon.service",
            "systemctl --user daemon-reload"
        ]
    );
    assert!(!m.definition_path().exists());
    assert!(rec.calls().iter().all(|c| !c.contains("linger")));
}

// ---------------------------------------------------------------- foreign definitions

#[test]
fn foreign_definitions_are_never_overwritten_or_removed() {
    for platform in [Platform::Launchd, Platform::Systemd] {
        let t = tempfile::tempdir().unwrap();
        let (m, rec) = manager(platform, t.path(), "com.spool.test.foreign");
        std::fs::create_dir_all(t.path().join("defs")).unwrap();
        let foreign = "someone else's service\n";
        std::fs::write(m.definition_path(), foreign).unwrap();
        assert_eq!(m.definition().unwrap(), Definition::Foreign);
        let e = m.install().unwrap_err().to_string();
        assert!(e.contains("not written by spool"), "{e}");
        assert!(m.uninstall().is_err());
        assert!(m.start().is_err());
        assert_eq!(
            std::fs::read_to_string(m.definition_path()).unwrap(),
            foreign
        );
        assert!(
            rec.calls()
                .iter()
                .all(|c| !c.contains("bootstrap") && !c.contains("enable")),
            "{:?}",
            rec.calls()
        );
    }
}

#[test]
fn start_before_install_explains_what_to_do() {
    let t = tempfile::tempdir().unwrap();
    let (m, _) = manager(Platform::Launchd, t.path(), "com.spool.test.none");
    assert!(
        m.start()
            .unwrap_err()
            .to_string()
            .contains("spool service install")
    );
    let st = m.state().unwrap();
    assert_eq!(st.definition, Definition::Missing);
    assert_eq!(st.registered, Some(false));
}

// ---------------------------------------------------------------- opt-in real launchd lifecycle

trait ModeBits {
    fn mode_bits(&self) -> u32;
}
impl ModeBits for std::fs::Permissions {
    fn mode_bits(&self) -> u32 {
        std::os::unix::fs::PermissionsExt::mode(self)
    }
}

fn wait_for(what: &str, secs: u64, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while !f() {
        assert!(
            t.elapsed() < Duration::from_secs(secs),
            "timed out waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

struct Cleanup {
    target: String,
}

impl Drop for Cleanup {
    fn drop(&mut self) {
        // Only ever the isolated test label.
        let _ = Command::new("launchctl")
            .args(["bootout", &self.target])
            .output();
    }
}

#[test]
fn launchd_lifecycle_with_an_isolated_label() {
    if std::env::var("SPOOL_TEST_LAUNCHD").as_deref() != Ok("1") || !cfg!(target_os = "macos") {
        eprintln!("skipped: set SPOOL_TEST_LAUNCHD=1 on macOS to run the real launchd lifecycle");
        return;
    }
    let label = format!("com.spool.test.{}", &spool::ids::new_id("t")[2..14]);
    assert_ne!(label, DEFAULT_LABEL);
    let tmp = tempfile::Builder::new()
        .prefix("sp")
        .tempdir_in("/tmp")
        .unwrap();
    let home = tmp.path().join("h");
    let uid = rustix::process::geteuid().as_raw();
    let cfg = ServiceConfig {
        platform: Platform::Launchd,
        label: label.clone(),
        binary: PathBuf::from(BIN),
        definition_dir: tmp.path().join("agents"),
        log_dir: tmp.path().join("logs"),
        spool_home: Some(home.clone()),
        home: Some(tmp.path().to_path_buf()),
        uid,
    };
    let _cleanup = Cleanup {
        target: format!("gui/{uid}/{label}"),
    };
    let m = ServiceManager::new(cfg, Arc::new(SystemRunner)).unwrap();
    // The daemon never creates the signing key (spec 17.1); initialise the isolated home first.
    let init = Command::new(BIN)
        .arg("init")
        .env("SPOOL_HOME", &home)
        .env("HOME", tmp.path())
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );
    let sock = home.join("state/spool.sock");
    let connects = || std::os::unix::net::UnixStream::connect(&sock).is_ok();

    m.install().unwrap();
    wait_for("daemon socket after install", 30, connects);
    let st = m.state().unwrap();
    assert_eq!(
        (st.definition, st.registered, st.running),
        (Definition::Managed, Some(true), Some(true))
    );
    // Isolated home really was used; sensitive modes hold.
    assert_eq!(
        std::fs::metadata(&sock).unwrap().permissions().mode_bits() & 0o777,
        0o600
    );
    assert_eq!(
        std::fs::metadata(home.join("state"))
            .unwrap()
            .permissions()
            .mode_bits()
            & 0o777,
        0o700
    );

    m.stop().unwrap();
    wait_for("daemon to stop", 30, || !connects());
    wait_for("launchd to unload after stop", 15, || {
        m.state().unwrap().registered == Some(false)
    });

    m.start().unwrap();
    wait_for("daemon socket after start", 30, connects);
    m.restart().unwrap();
    wait_for("daemon socket after restart", 30, connects);

    m.uninstall().unwrap();
    wait_for("daemon to stop after uninstall", 30, || !connects());
    assert!(!m.definition_path().exists());
    wait_for("launchd to forget the label", 15, || {
        m.state().unwrap().registered == Some(false)
    });
    // Data stays after uninstall (the daemon created it under the isolated home).
    assert!(home.join("state/index.sqlite").exists());
}

#[test]
fn packaging_examples_lint_and_match_the_generated_shape() {
    let plist = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/packaging/com.spool.daemon.plist.example"
    );
    match plutil(&["-lint", plist]) {
        Some(o) => assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stdout)),
        None => eprintln!("plutil unavailable; skipped"),
    }
    let t = tempfile::tempdir().unwrap();
    let (m, _) = manager(Platform::Systemd, t.path(), DEFAULT_LABEL);
    let generated = m.render();
    let example = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/packaging/spool-daemon.service.example"
    ))
    .unwrap();
    // The example only differs by comments and the placeholder binary path.
    let strip = |s: &str| {
        s.lines()
            .filter(|l| !l.starts_with('#'))
            .map(|l| {
                l.replace("/opt/spool/bin/spool", "BIN")
                    .replace("/ABSOLUTE/PATH/TO/spool", "BIN")
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(strip(&example), strip(&generated));
}
