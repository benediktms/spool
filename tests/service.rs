//! Service tests: plist/unit rendering and linting, foreign-definition protection, the exact
//! launchctl/systemctl command sequences (recording runner, isolated labels, temp directories),
//! and one opt-in real launchd lifecycle (`SPOOL_TEST_LAUNCHD=1`) under a random
//! `com.spool.test.*` label. Nothing here ever touches the `com.spool.daemon` label.

use parking_lot::Mutex;
#[cfg(unix)]
use spool::service::DEFAULT_LABEL;
#[cfg(target_os = "macos")]
use spool::service::SystemRunner;
use spool::service::{
    CmdOutput, CommandRunner, Definition, Platform, ServiceConfig, ServiceManager,
};
use std::io;
use std::path::{Path, PathBuf};
#[cfg(any(unix, windows))]
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(any(target_os = "macos", windows))]
use std::time::{Duration, Instant};

/// Records every command; emulates "loaded" state for `launchctl print`.
#[derive(Default)]
struct Recorder {
    calls: Mutex<Vec<String>>,
    loaded: AtomicBool,
    task: AtomicBool,
    foreign_task: AtomicBool,
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
            (program, _) if program.ends_with("powershell.exe") => {
                if self.task.load(Ordering::SeqCst) && self.foreign_task.load(Ordering::SeqCst) {
                    ok(
                        r#"{"found":true,"xml":"<Description>another application</Description>","state":3}"#,
                    )
                } else if self.task.load(Ordering::SeqCst) {
                    ok(
                        r#"{"found":true,"xml":"<Description>SpoolManaged task</Description>","state":3}"#,
                    )
                } else {
                    ok(r#"{"found":false}"#)
                }
            }
            ("systemctl", Some("--user"))
                if args.get(1).map(String::as_str) == Some("is-enabled")
                    || args.get(1).map(String::as_str) == Some("is-active") =>
            {
                no()
            }
            ("schtasks.exe", Some("/Query"))
                if args.iter().any(|arg| arg == "/XML")
                    && self.task.load(Ordering::SeqCst)
                    && self.foreign_task.load(Ordering::SeqCst) =>
            {
                ok("<Description>another application</Description>")
            }
            ("schtasks.exe", Some("/Query"))
                if args.iter().any(|arg| arg == "/XML") && self.task.load(Ordering::SeqCst) =>
            {
                ok(&format!(
                    "<Description>SpoolManaged task: {}</Description>",
                    args.get(2).unwrap()
                ))
            }
            ("schtasks.exe", Some("/Query")) if args.iter().any(|arg| arg == "/XML") => {
                Ok(CmdOutput {
                    success: false,
                    stdout: String::new(),
                    stderr: "ERROR: The system cannot find the file specified.".into(),
                })
            }
            ("schtasks.exe", Some("/Query")) if self.task.load(Ordering::SeqCst) => {
                ok("Status: Ready\n")
            }
            ("schtasks.exe", Some("/Create")) => {
                let xml = args
                    .windows(2)
                    .find(|pair| pair[0] == "/XML")
                    .expect("task registration must receive its XML file");
                let mut options = std::fs::OpenOptions::new();
                options.read(true);
                #[cfg(windows)]
                {
                    use std::os::windows::fs::OpenOptionsExt;
                    // Model the scheduler opening its input without sharing the writer's handle.
                    options.share_mode(0);
                }
                use std::io::Read;
                let mut input = options.open(&xml[1])?;
                let mut bytes = Vec::new();
                input.read_to_end(&mut bytes)?;
                assert!(
                    bytes.starts_with(&[0xff, 0xfe]),
                    "task XML requires a UTF-16LE BOM"
                );
                let words: Vec<u16> = bytes[2..]
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
                    .collect();
                let text = String::from_utf16(&words).expect("task XML must be valid UTF-16");
                assert!(text.starts_with("<?xml version=\"1.0\" encoding=\"UTF-16\"?>"));
                self.task.store(true, Ordering::SeqCst);
                ok("")
            }
            ("schtasks.exe", Some("/Delete")) => {
                self.task.store(false, Ordering::SeqCst);
                ok("")
            }
            _ => ok(""),
        }
    }
}

fn test_binary() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from(r"C:\opt\spool\bin\spool.exe")
    } else {
        PathBuf::from("/opt/spool/bin/spool")
    }
}

#[cfg(unix)]
fn test_binary_text() -> &'static str {
    if cfg!(windows) {
        r"C:\opt\spool\bin\spool.exe"
    } else {
        "/opt/spool/bin/spool"
    }
}
fn config(platform: Platform, dir: &Path, label: &str) -> ServiceConfig {
    ServiceConfig {
        platform,
        label: label.into(),
        binary: test_binary(),
        definition_dir: dir.join("defs"),
        log_dir: dir.join("logs"),
        spool_home: None,
        home: Some(PathBuf::from("/Users/test")),
        uid: 501,
        user_sid: None,
    }
}

#[cfg(unix)]
fn manager(platform: Platform, dir: &Path, label: &str) -> (ServiceManager, Arc<Recorder>) {
    let rec = Arc::new(Recorder::default());
    (
        ServiceManager::new(config(platform, dir, label), rec.clone()).unwrap(),
        rec,
    )
}

fn windows_manager(dir: &Path, label: &str) -> (ServiceManager, Arc<Recorder>) {
    let rec = Arc::new(Recorder::default());
    let mut cfg = config(Platform::WindowsTask, dir, label);
    cfg.spool_home = Some(dir.join("spool home"));
    cfg.user_sid = Some("S-1-5-21-1000".into());
    (ServiceManager::new(cfg, rec.clone()).unwrap(), rec)
}

#[cfg(unix)]
fn plutil(args: &[&str]) -> Option<std::process::Output> {
    Command::new("plutil").args(args).output().ok()
}

// ---------------------------------------------------------------- rendering

#[cfg(unix)]
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
    assert_eq!(items, vec![test_binary_text(), "daemon", "run"]);
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
            serde_json::json!([test_binary_text(), "daemon", "run"])
        );
        assert_eq!(v["ThrottleInterval"], 30);
        assert_eq!(v["RunAtLoad"], true);
    }
}

#[cfg(unix)]
#[test]
fn plist_escapes_xml_and_forwards_an_isolated_home() {
    let t = tempfile::tempdir().unwrap();
    let mut cfg = config(Platform::Launchd, t.path(), "com.spool.test.x");
    cfg.binary = if cfg!(windows) {
        PathBuf::from(r"C:\opt\a&b\spool")
    } else {
        PathBuf::from("/opt/a&b/<spool>")
    };
    cfg.spool_home = Some(PathBuf::from("/tmp/sp1/h"));
    let m = ServiceManager::new(cfg, Arc::new(Recorder::default())).unwrap();
    let text = m.render();
    assert!(text.contains("a&amp;b"));
    assert!(text.contains("<key>SPOOL_HOME</key><string>/tmp/sp1/h</string>"));
    let file = t.path().join("e.plist");
    std::fs::write(&file, &text).unwrap();
    if let Some(o) = plutil(&["-lint", file.to_str().unwrap()]) {
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stdout));
    }
}

#[cfg(unix)]
#[test]
fn systemd_unit_text_is_exact() {
    let t = tempfile::tempdir().unwrap();
    let (m, _) = manager(Platform::Systemd, t.path(), DEFAULT_LABEL);
    #[cfg(not(windows))]
    assert_eq!(
        m.render(),
        "# Managed by spool; regenerate with `spool service install`.\n\
         [Unit]\nDescription=Spool local agent-session search daemon\n\n\
         [Service]\nType=simple\nExecStart=\"/opt/spool/bin/spool\" daemon run\nRestart=on-failure\nRestartSec=5\nTimeoutStopSec=20\n\n\
         [Install]\nWantedBy=default.target\n"
    );
    #[cfg(windows)]
    assert!(m.render().contains("ExecStart="));
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
fn windows_task_xml_and_lifecycle_are_safe_and_complete() {
    let t = tempfile::tempdir().unwrap();
    let (mut m, rec) = windows_manager(t.path(), "com.spool.test.windows");
    m.cfg.spool_home = Some(t.path().join("spool home &% 'quoted'"));
    m.cfg.binary = if cfg!(windows) {
        PathBuf::from(r"C:\opt\a & b%\spool's.exe")
    } else {
        PathBuf::from("/opt/a & b%/spool's.exe")
    };
    let xml = m.render();
    assert!(xml.contains("<LogonType>InteractiveToken</LogonType>"));
    assert!(xml.contains("<RunLevel>LeastPrivilege</RunLevel>"));
    assert!(xml.contains("SpoolManaged task: \\Spool-com.spool.test.windows-"));
    assert!(xml.contains("spool-service.exe"));
    assert!(xml.contains("<Arguments>&quot;"));
    assert!(!xml.contains("PowerShell"));

    m.install().unwrap();
    assert!(!m.cfg.definition_dir.join("daemon.ps1").exists());
    m.start().unwrap();
    m.restart().unwrap();
    m.stop().unwrap();
    m.uninstall().unwrap();

    let calls = rec.calls();
    assert!(
        calls
            .iter()
            .any(|call| call.starts_with("schtasks.exe /Create "))
    );
    assert!(
        calls
            .iter()
            .any(|call| call.starts_with("schtasks.exe /Run "))
    );
    assert!(
        calls
            .iter()
            .any(|call| call.starts_with("schtasks.exe /Delete "))
    );
    assert!(
        !calls
            .iter()
            .any(|call| call.starts_with("schtasks.exe /End "))
    );
}

#[cfg(windows)]
#[test]
fn windows_service_helper_runs_unicode_paths_without_a_task_registration() {
    struct Launcher {
        child: std::process::Child,
        paths: spool::config::Paths,
    }
    impl Drop for Launcher {
        fn drop(&mut self) {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            let _ = spool::ipc::transport::request_shutdown(&self.paths);
            let deadline = Instant::now() + Duration::from_secs(20);
            while Instant::now() < deadline {
                if self.child.try_wait().ok().flatten().is_some() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            let _ = Command::new("taskkill.exe")
                .args(["/PID", &self.child.id().to_string(), "/T", "/F"])
                .output();
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    let t = tempfile::tempdir().unwrap();
    let root = t.path().join("home 日本 & % '");
    let bin_dir = t.path().join("bin 日本 & % '");
    std::fs::create_dir_all(&bin_dir).unwrap();
    let binary = bin_dir.join("spool daemon's.exe");
    let service_binary = bin_dir.join("spool-service.exe");
    std::fs::copy(env!("CARGO_BIN_EXE_spool"), &binary).unwrap();
    std::fs::copy(env!("CARGO_BIN_EXE_spool-service"), &service_binary).unwrap();

    let init = Command::new(&binary)
        .args(["init", "--hosted-processing", "no"])
        .env("SPOOL_HOME", &root)
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap();
    assert!(
        init.status.success(),
        "{}",
        String::from_utf8_lossy(&init.stderr)
    );

    let mut paths = spool::config::Paths::from_root(root.clone()).unwrap();
    paths.socket = spool::config::Config::load(&paths)
        .unwrap()
        .socket_path(&paths.root)
        .unwrap();
    let user_sid = ServiceConfig::for_current_user(&paths).unwrap().user_sid;

    let rec = Arc::new(Recorder::default());
    let manager = ServiceManager::new(
        ServiceConfig {
            platform: Platform::WindowsTask,
            label: "com.spool.test.launcher".into(),
            binary,
            definition_dir: root.join("service"),
            log_dir: root.join("logs"),
            spool_home: Some(root.clone()),
            home: Some(root.clone()),
            uid: 1,
            user_sid,
        },
        rec,
    )
    .unwrap();
    manager.install().unwrap();

    let powershell = std::env::var_os("SystemRoot")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(r"C:\Windows"))
        .join("System32")
        .join("WindowsPowerShell")
        .join("v1.0")
        .join("powershell.exe");
    // TASK_VALIDATE_ONLY checks the native schema without registering any persistent task.
    let validation = Command::new(&powershell)
        .args([
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            r#"$ErrorActionPreference='Stop'
$s=New-Object -ComObject 'Schedule.Service'
$s.Connect()
$definition=$s.NewTask(0)
$definition.XmlText=$env:SPOOL_TEST_TASK_XML
$triggerUser=$definition.Triggers.Item(1).UserId
try { $triggerSid=[Security.Principal.SecurityIdentifier]::new($triggerUser) }
catch { $triggerSid=[Security.Principal.NTAccount]::new($triggerUser).Translate([Security.Principal.SecurityIdentifier]) }
if (-not $triggerSid.Equals([Security.Principal.WindowsIdentity]::GetCurrent().User)) {
    throw "logon trigger must belong to the current user: $triggerUser"
}
$null=$s.GetFolder('\').RegisterTask($env:SPOOL_TEST_TASK_NAME,$env:SPOOL_TEST_TASK_XML,1,$null,$null,3,$null)"#,
        ])
        .env("SPOOL_TEST_TASK_NAME", manager.definition_path())
        .env("SPOOL_TEST_TASK_XML", manager.render())
        .output()
        .unwrap();
    assert!(
        validation.status.success(),
        "native task XML validation failed: {}",
        String::from_utf8_lossy(&validation.stderr)
    );
    let child = Command::new(&service_binary)
        .arg(&manager.cfg.binary)
        .arg(&root)
        .env("SPOOL_HOME", t.path().join("wrong parent home"))
        .stdin(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let mut launcher = Launcher { child, paths };

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(
            launcher.child.try_wait().unwrap().is_none(),
            "service helper exited before the daemon became ready"
        );
        if Command::new(&manager.cfg.binary)
            .args(["status", "--json"])
            .env("SPOOL_HOME", &root)
            .stdin(std::process::Stdio::null())
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "daemon did not start from service helper"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    manager.stop().unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = launcher.child.try_wait().unwrap() {
            assert!(status.success(), "service helper exited with {status}");
            break;
        }
        assert!(Instant::now() < deadline, "service helper did not stop");
        std::thread::sleep(Duration::from_millis(100));
    }
    let after_stop = Command::new(&manager.cfg.binary)
        .args(["status", "--json"])
        .env("SPOOL_HOME", &root)
        .output()
        .unwrap();
    assert_eq!(
        after_stop.status.code(),
        Some(3),
        "daemon remained reachable"
    );
}

#[test]
fn foreign_windows_task_is_never_replaced_or_deleted() {
    let t = tempfile::tempdir().unwrap();
    let (m, rec) = windows_manager(t.path(), "com.spool.test.foreign-task");
    rec.task.store(true, Ordering::SeqCst);
    rec.foreign_task.store(true, Ordering::SeqCst);

    assert_eq!(m.definition().unwrap(), Definition::Foreign);
    assert!(m.install().is_err());
    assert!(m.start().is_err());
    assert!(m.uninstall().is_err());
    assert!(
        rec.calls()
            .iter()
            .all(|call| !call.starts_with("schtasks.exe /Create ")
                && !call.starts_with("schtasks.exe /Delete "))
    );
}
#[cfg(unix)]
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

#[cfg(unix)]
#[test]
fn launchd_install_start_stop_restart_uninstall_sequence() {
    let t = tempfile::tempdir().unwrap();
    let label = "com.spool.test.seq";
    let (m, rec) = manager(Platform::Launchd, t.path(), label);
    let plist = t.path().join(format!("defs/{label}.plist"));

    m.install().unwrap();
    assert!(plist.exists());
    #[cfg(unix)]
    {
        assert_eq!(
            std::fs::metadata(&plist).unwrap().permissions().mode_bits() & 0o777,
            0o644
        );
        assert_eq!(
            std::fs::metadata(t.path().join("logs"))
                .unwrap()
                .permissions()
                .mode_bits()
                & 0o777,
            0o700
        );
    }
    assert_eq!(m.definition().unwrap(), Definition::Managed);
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
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

#[cfg(unix)]
trait ModeBits {
    fn mode_bits(&self) -> u32;
}
#[cfg(unix)]
impl ModeBits for std::fs::Permissions {
    fn mode_bits(&self) -> u32 {
        std::os::unix::fs::PermissionsExt::mode(self)
    }
}

#[cfg(target_os = "macos")]
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

#[cfg(target_os = "macos")]
struct Cleanup {
    target: String,
}

#[cfg(target_os = "macos")]
impl Drop for Cleanup {
    fn drop(&mut self) {
        // Only ever the isolated test label.
        let _ = Command::new("launchctl")
            .args(["bootout", &self.target])
            .output();
    }
}

#[cfg(target_os = "macos")]
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
        user_sid: None,
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

#[cfg(unix)]
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
