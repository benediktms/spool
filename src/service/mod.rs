//! Per-user service registration: macOS LaunchAgent, Linux systemd user unit, and Windows Task Scheduler.
//!
//! Never root, never overwrites a definition spool did not write, uninstall preserves all data.
//! Everything that touches the OS goes through [`CommandRunner`] so tests can use an isolated
//! label, a temporary definition directory and a recording runner.

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use std::io;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const DEFAULT_LABEL: &str = "com.spool.daemon";
pub const SYSTEMD_UNIT: &str = "spool-daemon.service";
/// Isolation hook for tests and side-by-side installs; the default is the production label.
pub const LABEL_ENV: &str = "SPOOL_SERVICE_LABEL";
const MANAGED_KEY: &str = "SpoolManaged";
const MANAGED_COMMENT: &str = "# Managed by spool; regenerate with `spool service install`.";
const TASK_MARKER: &str = "SpoolManaged task";
/// launchd waits this long after SIGTERM before SIGKILL; above the daemon's 15 s graceful target.
const EXIT_TIMEOUT_SECS: u32 = 20;
const THROTTLE_SECS: u32 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Launchd,
    Systemd,
    WindowsTask,
}

impl Platform {
    pub fn current() -> Result<Self> {
        if cfg!(target_os = "macos") {
            Ok(Self::Launchd)
        } else if cfg!(target_os = "linux") {
            Ok(Self::Systemd)
        } else if cfg!(target_os = "windows") {
            Ok(Self::WindowsTask)
        } else {
            bail!("service management is supported on macOS, Linux, and Windows only")
        }
    }
}

#[derive(Debug, Clone)]
pub struct CmdOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

pub trait CommandRunner: Send + Sync {
    fn run(&self, program: &str, args: &[String]) -> io::Result<CmdOutput>;
}

pub struct SystemRunner;

impl CommandRunner for SystemRunner {
    fn run(&self, program: &str, args: &[String]) -> io::Result<CmdOutput> {
        let mut command = std::process::Command::new(program);
        command.args(args).stdin(std::process::Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
        }
        let o = command.output()?;
        Ok(CmdOutput {
            success: o.status.success(),
            stdout: String::from_utf8_lossy(&o.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&o.stderr).into_owned(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub platform: Platform,
    pub label: String,
    /// Absolute path of the `spool` binary the service runs.
    pub binary: PathBuf,
    /// `~/Library/LaunchAgents`, `~/.config/systemd/user`, or the private Windows registration directory.
    pub definition_dir: PathBuf,
    /// Directory for launchd's stdout/stderr files.
    pub log_dir: PathBuf,
    /// Forwarded as `SPOOL_HOME` (the Windows launcher always needs it).
    pub spool_home: Option<PathBuf>,
    pub home: Option<PathBuf>,
    /// Unix effective UID; Windows uses a nonzero sentinel.
    pub uid: u32,
    /// Current user SID for a Windows InteractiveToken task.
    pub user_sid: Option<String>,
}

impl ServiceConfig {
    pub fn for_current_user(paths: &crate::config::Paths) -> Result<Self> {
        let platform = Platform::current()?;
        let home = if cfg!(windows) {
            std::env::var_os("USERPROFILE").map(PathBuf::from)
        } else {
            std::env::var_os("HOME").map(PathBuf::from)
        };
        let need_home = || {
            home.clone()
                .context("the current user home directory is not set")
        };
        let definition_dir = match platform {
            Platform::Launchd => need_home()?.join("Library/LaunchAgents"),
            Platform::Systemd => match std::env::var_os("XDG_CONFIG_HOME") {
                Some(x) if !x.is_empty() => PathBuf::from(x).join("systemd/user"),
                _ => need_home()?.join(".config/systemd/user"),
            },
            Platform::WindowsTask => paths.root.join("service"),
        };
        let binary = std::fs::canonicalize(std::env::current_exe()?)
            .context("resolving the spool binary path")?;
        let spool_home = match platform {
            Platform::WindowsTask => Some(std::path::absolute(&paths.root)?),
            _ => std::env::var_os("SPOOL_HOME").map(PathBuf::from),
        };
        #[cfg(unix)]
        let uid = rustix::process::geteuid().as_raw();
        #[cfg(not(unix))]
        let uid = 1;
        #[cfg(windows)]
        let user_sid = Some(crate::perms::current_user_sid()?);
        #[cfg(not(windows))]
        let user_sid = None;
        Ok(Self {
            platform,
            label: std::env::var(LABEL_ENV).unwrap_or_else(|_| DEFAULT_LABEL.into()),
            binary,
            definition_dir,
            log_dir: crate::telemetry::log_dir(paths),
            spool_home,
            home,
            uid,
            user_sid,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Definition {
    Missing,
    Managed,
    Foreign,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ServiceState {
    pub label: String,
    pub definition_path: PathBuf,
    pub definition: Definition,
    /// launchd: bootstrapped into `gui/<uid>`; systemd: unit enabled. `None` if the query failed.
    pub registered: Option<bool>,
    pub running: Option<bool>,
}

pub struct ServiceManager {
    pub cfg: ServiceConfig,
    runner: Arc<dyn CommandRunner>,
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn valid_label(l: &str) -> bool {
    !l.is_empty()
        && l.len() <= 100
        && l.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
}

/// systemd `Environment=`/`ExecStart=` word quoting.
fn systemd_quote(s: &str) -> String {
    format!(
        "\"{}\"",
        s.replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace('%', "%%")
            .replace('$', "$$")
    )
}

fn task_name(label: &str, spool_home: &Path) -> String {
    let digest = blake3::hash(spool_home.as_os_str().as_encoded_bytes()).to_hex();
    format!(r"\Spool-{label}-{}", &digest.as_str()[..16])
}

fn windows_powershell() -> PathBuf {
    #[cfg(windows)]
    {
        std::env::var_os("SystemRoot")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(r"C:\Windows"))
            .join("System32")
            .join("WindowsPowerShell")
            .join("v1.0")
            .join("powershell.exe")
    }
    #[cfg(not(windows))]
    {
        PathBuf::from(r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe")
    }
}

fn windows_quote(s: &str) -> String {
    let mut quoted = String::with_capacity(s.len() + 2);
    quoted.push('"');
    let mut slashes = 0;
    for c in s.chars() {
        match c {
            '\\' => slashes += 1,
            '"' => {
                quoted.extend(std::iter::repeat_n('\\', slashes * 2 + 1));
                quoted.push('"');
                slashes = 0;
            }
            _ => {
                quoted.extend(std::iter::repeat_n('\\', slashes));
                quoted.push(c);
                slashes = 0;
            }
        }
    }
    quoted.extend(std::iter::repeat_n('\\', slashes * 2));
    quoted.push('"');
    quoted
}

fn powershell_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn no_control_chars(path: &Path) -> bool {
    !path
        .to_string_lossy()
        .chars()
        .any(|c| matches!(c, '\0' | '\r' | '\n'))
}

fn encoded_powershell(script: &str) -> String {
    let mut utf16 = Vec::with_capacity(script.len() * 2);
    for unit in script.encode_utf16() {
        utf16.extend_from_slice(&unit.to_le_bytes());
    }
    STANDARD.encode(utf16)
}

impl ServiceManager {
    pub fn new(cfg: ServiceConfig, runner: Arc<dyn CommandRunner>) -> Result<Self> {
        if !valid_label(&cfg.label) {
            bail!(
                "invalid service label {:?} (letters, digits, '.', '-', '_' only)",
                cfg.label
            );
        }
        if !cfg.binary.is_absolute() {
            bail!(
                "service binary path must be absolute: {}",
                cfg.binary.display()
            );
        }
        if cfg.platform == Platform::WindowsTask {
            let home = cfg
                .spool_home
                .as_deref()
                .context("Windows Task Scheduler needs an absolute SPOOL_HOME")?;
            if !home.is_absolute() || !no_control_chars(home) || !no_control_chars(&cfg.binary) {
                bail!("Windows service paths must be absolute and contain no control characters");
            }
            if cfg
                .user_sid
                .as_deref()
                .filter(|sid| !sid.is_empty())
                .is_none()
            {
                bail!("Windows Task Scheduler needs the current user SID");
            }
        }
        Ok(Self { cfg, runner })
    }

    pub fn unit_name(&self) -> String {
        if self.cfg.label == DEFAULT_LABEL {
            SYSTEMD_UNIT.into()
        } else {
            format!("{}.service", self.cfg.label)
        }
    }

    pub fn definition_path(&self) -> PathBuf {
        match self.cfg.platform {
            Platform::Launchd => self
                .cfg
                .definition_dir
                .join(format!("{}.plist", self.cfg.label)),
            Platform::Systemd => self.cfg.definition_dir.join(self.unit_name()),
            Platform::WindowsTask => PathBuf::from(task_name(
                &self.cfg.label,
                self.cfg.spool_home.as_deref().expect("validated in new"),
            )),
        }
    }

    fn service_binary(&self) -> PathBuf {
        self.cfg.binary.with_file_name("spool-service.exe")
    }
    fn domain(&self) -> String {
        format!("gui/{}", self.cfg.uid)
    }

    fn target(&self) -> String {
        format!("{}/{}", self.domain(), self.cfg.label)
    }

    /// Text of the launchd plist, systemd unit, or Task Scheduler XML.
    pub fn render(&self) -> String {
        match self.cfg.platform {
            Platform::Launchd => self.render_plist(),
            Platform::Systemd => self.render_unit(),
            Platform::WindowsTask => self.render_task(),
        }
    }

    pub fn render_plist(&self) -> String {
        let c = &self.cfg;
        let s = |v: &str| xml_escape(v);
        let mut env = String::new();
        // Minimal fixed environment: nothing is inherited from an interactive shell (no credentials).
        env.push_str("    <key>PATH</key><string>/usr/bin:/bin:/usr/sbin:/sbin</string>\n");
        if let Some(h) = &c.home {
            env.push_str(&format!(
                "    <key>HOME</key><string>{}</string>\n",
                s(&h.to_string_lossy())
            ));
        }
        if let Some(h) = &c.spool_home {
            env.push_str(&format!(
                "    <key>SPOOL_HOME</key><string>{}</string>\n",
                s(&h.to_string_lossy())
            ));
        }
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{label}</string>
  <key>{managed}</key><true/>
  <key>ProgramArguments</key>
  <array>
    <string>{bin}</string>
    <string>daemon</string>
    <string>run</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key>
  <dict>
    <key>SuccessfulExit</key><false/>
  </dict>
  <key>ThrottleInterval</key><integer>{throttle}</integer>
  <key>ExitTimeOut</key><integer>{exit_timeout}</integer>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>{out}</string>
  <key>StandardErrorPath</key><string>{err}</string>
  <key>EnvironmentVariables</key>
  <dict>
{env}  </dict>
</dict>
</plist>
"#,
            label = s(&c.label),
            managed = MANAGED_KEY,
            bin = s(&c.binary.to_string_lossy()),
            throttle = THROTTLE_SECS,
            exit_timeout = EXIT_TIMEOUT_SECS,
            out = s(&c.log_dir.join("daemon.stdout.log").to_string_lossy()),
            err = s(&c.log_dir.join("daemon.stderr.log").to_string_lossy()),
            env = env,
        )
    }

    pub fn render_unit(&self) -> String {
        let c = &self.cfg;
        let mut env = String::new();
        if let Some(h) = &c.spool_home {
            env = format!(
                "Environment={}\n",
                systemd_quote(&format!("SPOOL_HOME={}", h.display()))
            );
        }
        format!(
            "{MANAGED_COMMENT}\n[Unit]\nDescription=Spool local agent-session search daemon\n\n[Service]\nType=simple\nExecStart={bin} daemon run\nRestart=on-failure\nRestartSec=5\nTimeoutStopSec={timeout}\n{env}\n[Install]\nWantedBy=default.target\n",
            bin = systemd_quote(&c.binary.to_string_lossy()),
            timeout = EXIT_TIMEOUT_SECS,
        )
    }

    pub fn render_task(&self) -> String {
        let sid = self.cfg.user_sid.as_deref().expect("validated in new");
        let task = self.definition_path();
        let service_binary = self.service_binary();
        let home = self.cfg.spool_home.as_deref().expect("validated in new");
        let args = format!(
            "{} {}",
            windows_quote(&self.cfg.binary.to_string_lossy()),
            windows_quote(&home.to_string_lossy())
        );
        format!(
            r#"<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo><Description>{marker}: {task}</Description></RegistrationInfo>
  <Triggers><LogonTrigger><Enabled>true</Enabled><UserId>{sid}</UserId></LogonTrigger></Triggers>
  <Principals><Principal id="Author"><UserId>{sid}</UserId><LogonType>InteractiveToken</LogonType><RunLevel>LeastPrivilege</RunLevel></Principal></Principals>
  <Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries><StopIfGoingOnBatteries>false</StopIfGoingOnBatteries><AllowHardTerminate>false</AllowHardTerminate><StartWhenAvailable>true</StartWhenAvailable><RestartOnFailure><Interval>PT1M</Interval><Count>3</Count></RestartOnFailure><ExecutionTimeLimit>PT0S</ExecutionTimeLimit><Enabled>true</Enabled></Settings>
  <Actions Context="Author"><Exec><Command>{cmd}</Command><Arguments>{args}</Arguments><WorkingDirectory>{dir}</WorkingDirectory></Exec></Actions>
</Task>
"#,
            marker = xml_escape(TASK_MARKER),
            task = xml_escape(&task.to_string_lossy()),
            sid = xml_escape(sid),
            cmd = xml_escape(&service_binary.to_string_lossy()),
            args = xml_escape(&args),
            dir = xml_escape(&self.cfg.definition_dir.to_string_lossy()),
        )
    }

    fn ctl(&self, program: &str, args: &[&str]) -> Result<CmdOutput> {
        let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        self.runner
            .run(program, &args)
            .with_context(|| format!("running {program}"))
    }

    fn ctl_ok(&self, program: &str, args: &[&str]) -> Result<()> {
        let o = self.ctl(program, args)?;
        if !o.success {
            bail!("`{program} {}` failed: {}", args.join(" "), o.stderr.trim());
        }
        Ok(())
    }

    fn systemctl(&self, args: &[&str]) -> Result<CmdOutput> {
        let mut a = vec!["--user"];
        a.extend_from_slice(args);
        self.ctl("systemctl", &a)
    }

    fn task_info(&self) -> Result<Option<(String, u32)>> {
        let task = self.definition_path();
        let script = format!(
            r#"$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$service = New-Object -ComObject 'Schedule.Service'
$service.Connect()
$task = $null
try {{ $task = $service.GetFolder('\').GetTask({task}) }}
catch {{
  $exception = $_.Exception
  while ($null -ne $exception -and (([int64]$exception.HResult -band 0xffffffffL) -ne 0x80070002L)) {{ $exception = $exception.InnerException }}
  if ($null -eq $exception) {{ throw }}
}}
if ($null -eq $task) {{ '{{"found":false}}' }}
else {{ [pscustomobject]@{{found=$true;xml=$task.Xml;state=[int]$task.State}} | ConvertTo-Json -Compress }}"#,
            task = powershell_literal(&task.to_string_lossy())
        );
        let encoded = encoded_powershell(&script);
        let args = [
            "-NoProfile",
            "-NonInteractive",
            "-EncodedCommand",
            encoded.as_str(),
        ];
        let command = windows_powershell();
        let o = self.ctl(&command.to_string_lossy(), &args)?;
        if !o.success {
            bail!("Task Scheduler query failed: {}", o.stderr.trim());
        }
        let value: serde_json::Value =
            serde_json::from_str(o.stdout.trim()).context("parsing Task Scheduler query")?;
        if value.get("found").and_then(serde_json::Value::as_bool) != Some(true) {
            return Ok(None);
        }
        let xml = value
            .get("xml")
            .and_then(serde_json::Value::as_str)
            .context("Task Scheduler query returned no XML")?;
        let state = value
            .get("state")
            .and_then(serde_json::Value::as_u64)
            .context("Task Scheduler query returned no state")?;
        Ok(Some((
            xml.to_owned(),
            u32::try_from(state).context("Task Scheduler state is out of range")?,
        )))
    }

    pub fn definition(&self) -> Result<Definition> {
        if self.cfg.platform == Platform::WindowsTask {
            return Ok(match self.task_info()? {
                None => Definition::Missing,
                Some((xml, _)) if xml.contains(TASK_MARKER) => Definition::Managed,
                Some(_) => Definition::Foreign,
            });
        }
        match std::fs::read_to_string(self.definition_path()) {
            Ok(t) => {
                let ours = match self.cfg.platform {
                    Platform::Launchd => t.contains(&format!("<key>{MANAGED_KEY}</key>")),
                    Platform::Systemd => t.lines().next() == Some(MANAGED_COMMENT),
                    Platform::WindowsTask => unreachable!("handled above"),
                };
                Ok(if ours {
                    Definition::Managed
                } else {
                    Definition::Foreign
                })
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Definition::Missing),
            // An unreadable or non-UTF-8 file is by definition not ours.
            Err(e) if e.kind() == io::ErrorKind::InvalidData => Ok(Definition::Foreign),
            Err(e) => Err(e).context("reading the service definition"),
        }
    }

    pub fn state(&self) -> Result<ServiceState> {
        let definition = self.definition()?;
        let (registered, running) = match self.cfg.platform {
            Platform::Launchd => match self.ctl("launchctl", &["print", &self.target()]) {
                Ok(o) => (
                    Some(o.success),
                    Some(o.success && o.stdout.contains("state = running")),
                ),
                Err(_) => (None, None),
            },
            Platform::Systemd => {
                let u = self.unit_name();
                let enabled = self.systemctl(&["is-enabled", &u]).map(|o| o.success).ok();
                let active = self.systemctl(&["is-active", &u]).map(|o| o.success).ok();
                (enabled, active)
            }
            Platform::WindowsTask if definition == Definition::Missing => {
                (Some(false), Some(false))
            }
            Platform::WindowsTask => match self.task_info() {
                Ok(Some((_, state))) => (Some(true), Some(state == 4)),
                Ok(None) => (Some(false), Some(false)),
                Err(_) => (Some(true), None),
            },
        };
        Ok(ServiceState {
            label: self.cfg.label.clone(),
            definition_path: self.definition_path(),
            definition,
            registered,
            running,
        })
    }

    fn refuse_root(&self) -> Result<()> {
        if cfg!(unix) && self.cfg.uid == 0 {
            bail!("refusing to manage a service as root; spool installs per-user services only");
        }
        Ok(())
    }

    fn write_definition(&self) -> Result<()> {
        let path = self.definition_path();
        match self.definition()? {
            Definition::Foreign => bail!(
                "{} exists and was not written by spool; refusing to overwrite it",
                path.display()
            ),
            Definition::Missing | Definition::Managed => {}
        }
        std::fs::create_dir_all(&self.cfg.definition_dir)?;
        if self.cfg.platform == Platform::Launchd {
            std::fs::create_dir_all(&self.cfg.log_dir)?;
            #[cfg(unix)]
            std::fs::set_permissions(&self.cfg.log_dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let tmp = self.cfg.definition_dir.join(format!(
            ".{}.tmp",
            path.file_name().unwrap_or_default().to_string_lossy()
        ));
        std::fs::write(&tmp, self.render())?;
        #[cfg(unix)]
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o644))?;
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }

    fn install_task(&self) -> Result<()> {
        crate::perms::ensure_private_dir(&self.cfg.definition_dir)?;
        let xml = self
            .cfg
            .definition_dir
            .join(format!(".{}.xml", crate::ids::new_id("task")));
        let result = (|| {
            let mut f = crate::perms::create_private_file(&xml)?;
            let text = self.render();
            let capacity = text
                .len()
                .checked_mul(2)
                .and_then(|len| len.checked_add(2))
                .context("task XML is too large")?;
            let mut encoded = Vec::with_capacity(capacity);
            encoded.extend_from_slice(&[0xff, 0xfe]);
            encoded.extend(text.encode_utf16().flat_map(u16::to_le_bytes));
            f.write_all(&encoded)?;
            f.sync_all()?;
            drop(f);
            crate::perms::set_private(&xml, 0o600)?;
            self.ctl_ok(
                "schtasks.exe",
                &[
                    "/Create",
                    "/TN",
                    &self.definition_path().to_string_lossy(),
                    "/XML",
                    &xml.to_string_lossy(),
                    "/F",
                ],
            )
        })();
        let _ = std::fs::remove_file(&xml);
        result
    }

    fn graceful_task_stop(&self) -> Result<()> {
        #[cfg(windows)]
        {
            let root = self.cfg.spool_home.as_deref().expect("validated in new");
            let mut paths = crate::config::Paths::from_root(root.to_path_buf())?;
            let cfg = crate::config::Config::load(&paths)?;
            paths.socket = cfg.socket_path(&paths.root)?;
            match crate::ipc::transport::request_shutdown(&paths) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    let startup_deadline =
                        std::time::Instant::now() + std::time::Duration::from_secs(1);
                    loop {
                        match self.task_info()? {
                            Some((_, 2 | 4)) => {
                                self.ctl_ok(
                                    "schtasks.exe",
                                    &["/End", "/TN", &self.definition_path().to_string_lossy()],
                                )?;
                                return Ok(());
                            }
                            Some((_, 3)) | None => {
                                if std::time::Instant::now() >= startup_deadline {
                                    return Ok(());
                                }
                            }
                            Some(_) => return Ok(()),
                        }
                        std::thread::sleep(std::time::Duration::from_millis(50));
                    }
                }
                Err(e) => return Err(e).context("requesting graceful daemon shutdown"),
            }
            let deadline = std::time::Instant::now()
                + std::time::Duration::from_secs(EXIT_TIMEOUT_SECS.into());
            loop {
                let lock = crate::perms::open_private_file(&paths.lock)?;
                match lock.try_lock() {
                    Ok(()) => break,
                    Err(std::fs::TryLockError::WouldBlock) => {}
                    Err(std::fs::TryLockError::Error(e)) => {
                        return Err(e).context("checking daemon shutdown");
                    }
                }
                if std::time::Instant::now() >= deadline {
                    self.ctl_ok(
                        "schtasks.exe",
                        &["/End", "/TN", &self.definition_path().to_string_lossy()],
                    )?;
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
        Ok(())
    }

    /// Write the definition and register it. launchd: `bootstrap` starts the job at load
    /// (`RunAtLoad`); systemd: the unit is enabled for the user session but not started.
    pub fn install(&self) -> Result<String> {
        self.refuse_root()?;
        let path = self.definition_path();
        match self.cfg.platform {
            Platform::Launchd => {
                if self.definition()? == Definition::Foreign {
                    bail!(
                        "{} exists and was not written by spool; refusing to overwrite it",
                        path.display()
                    );
                }
                if self.state()?.registered == Some(true) {
                    self.ctl_ok("launchctl", &["bootout", &self.target()])?;
                }
                self.write_definition()?;
                self.ctl_ok(
                    "launchctl",
                    &["bootstrap", &self.domain(), &path.to_string_lossy()],
                )?;
                Ok(format!(
                    "installed {} and loaded it into {} (RunAtLoad starts the daemon)",
                    path.display(),
                    self.domain()
                ))
            }
            Platform::Systemd => {
                self.write_definition()?;
                self.ctl_ok("systemctl", &["--user", "daemon-reload"])?;
                self.ctl_ok("systemctl", &["--user", "enable", &self.unit_name()])?;
                Ok(format!(
                    "installed {} and enabled it; run `spool service start`. Lingering is not enabled: the service runs while you are logged in",
                    path.display()
                ))
            }
            Platform::WindowsTask => {
                if self.definition()? == Definition::Foreign {
                    bail!(
                        "{} exists and was not written by spool; refusing to overwrite it",
                        path.display()
                    );
                }
                self.install_task()?;
                Ok(format!(
                    "installed Task Scheduler logon task {}; run `spool service start`",
                    path.display()
                ))
            }
        }
    }

    pub fn start(&self) -> Result<String> {
        self.refuse_root()?;
        match self.cfg.platform {
            Platform::Launchd => {
                let st = self.state()?;
                match st.definition {
                    Definition::Missing => {
                        bail!("service is not installed; run `spool service install`")
                    }
                    Definition::Foreign => bail!(
                        "{} is not a spool-managed definition",
                        st.definition_path.display()
                    ),
                    Definition::Managed => {}
                }
                if st.registered != Some(true) {
                    self.ctl_ok(
                        "launchctl",
                        &[
                            "bootstrap",
                            &self.domain(),
                            &st.definition_path.to_string_lossy(),
                        ],
                    )?;
                }
                self.ctl_ok("launchctl", &["kickstart", &self.target()])?;
                Ok(format!("started {}", self.cfg.label))
            }
            Platform::Systemd => {
                self.require_managed()?;
                self.ctl_ok("systemctl", &["--user", "start", &self.unit_name()])?;
                Ok(format!("started {}", self.unit_name()))
            }
            Platform::WindowsTask => {
                self.require_managed()?;
                self.ctl_ok(
                    "schtasks.exe",
                    &["/Run", "/TN", &self.definition_path().to_string_lossy()],
                )?;
                Ok(format!("started {}", self.definition_path().display()))
            }
        }
    }

    /// launchd: `bootout` (unloads until the next `start`/login). systemd: `stop`.
    pub fn stop(&self) -> Result<String> {
        self.refuse_root()?;
        match self.cfg.platform {
            Platform::Launchd => {
                if self.state()?.registered != Some(true) {
                    return Ok(format!("{} is not loaded", self.cfg.label));
                }
                self.ctl_ok("launchctl", &["bootout", &self.target()])?;
                Ok(format!(
                    "stopped and unloaded {} (`spool service start` loads it again)",
                    self.cfg.label
                ))
            }
            Platform::Systemd => {
                self.ctl_ok("systemctl", &["--user", "stop", &self.unit_name()])?;
                Ok(format!("stopped {}", self.unit_name()))
            }
            Platform::WindowsTask => {
                self.require_managed()?;
                self.graceful_task_stop()?;
                Ok(format!("stopped {}", self.definition_path().display()))
            }
        }
    }

    pub fn restart(&self) -> Result<String> {
        self.refuse_root()?;
        match self.cfg.platform {
            Platform::Launchd => {
                if self.state()?.registered == Some(true) {
                    self.ctl_ok("launchctl", &["kickstart", "-k", &self.target()])?;
                    Ok(format!("restarted {}", self.cfg.label))
                } else {
                    self.start()
                }
            }
            Platform::Systemd => {
                self.require_managed()?;
                self.ctl_ok("systemctl", &["--user", "restart", &self.unit_name()])?;
                Ok(format!("restarted {}", self.unit_name()))
            }
            Platform::WindowsTask => {
                self.require_managed()?;
                self.graceful_task_stop()?;
                self.ctl_ok(
                    "schtasks.exe",
                    &["/Run", "/TN", &self.definition_path().to_string_lossy()],
                )?;
                Ok(format!("restarted {}", self.definition_path().display()))
            }
        }
    }

    fn require_managed(&self) -> Result<()> {
        match self.definition()? {
            Definition::Managed => Ok(()),
            Definition::Missing => bail!("service is not installed; run `spool service install`"),
            Definition::Foreign => bail!(
                "{} is not a spool-managed definition",
                self.definition_path().display()
            ),
        }
    }

    /// Remove registration and the definition. Config, index, keys and credentials stay.
    pub fn uninstall(&self) -> Result<String> {
        self.refuse_root()?;
        let path = self.definition_path();
        match self.definition()? {
            Definition::Foreign => bail!(
                "{} was not written by spool; refusing to remove it",
                path.display()
            ),
            Definition::Missing => return Ok("service is not installed".into()),
            Definition::Managed => {}
        }
        match self.cfg.platform {
            Platform::Launchd => {
                if self.state()?.registered == Some(true) {
                    self.ctl_ok("launchctl", &["bootout", &self.target()])?;
                }
                std::fs::remove_file(&path)?;
            }
            Platform::Systemd => {
                let u = self.unit_name();
                // Not-running / not-enabled are fine here; a missing systemctl is not.
                self.systemctl(&["stop", &u])?;
                self.systemctl(&["disable", &u])?;
                std::fs::remove_file(&path)?;
                self.ctl_ok("systemctl", &["--user", "daemon-reload"])?;
            }
            Platform::WindowsTask => {
                self.graceful_task_stop()?;
                self.ctl_ok(
                    "schtasks.exe",
                    &["/Delete", "/TN", &path.to_string_lossy(), "/F"],
                )?;
            }
        }
        Ok(format!(
            "removed {}; configuration, index, keys and credentials were left in place",
            path.display()
        ))
    }
}
