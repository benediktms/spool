//! Per-user service registration (spec 17.3 / 17.4): macOS LaunchAgent, Linux systemd user unit.
//!
//! Never root, never overwrites a definition spool did not write, uninstall preserves all data.
//! Everything that touches the OS goes through [`CommandRunner`] so tests can use an isolated
//! label, a temporary definition directory and a recording runner.

use anyhow::{Context, Result, bail};
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;

pub const DEFAULT_LABEL: &str = "com.spool.daemon";
pub const SYSTEMD_UNIT: &str = "spool-daemon.service";
/// Isolation hook for tests and side-by-side installs; the default is the production label.
pub const LABEL_ENV: &str = "SPOOL_SERVICE_LABEL";
const MANAGED_KEY: &str = "SpoolManaged";
const MANAGED_COMMENT: &str = "# Managed by spool; regenerate with `spool service install`.";
/// launchd waits this long after SIGTERM before SIGKILL; above the daemon's 15 s graceful target.
const EXIT_TIMEOUT_SECS: u32 = 20;
const THROTTLE_SECS: u32 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Platform {
    Launchd,
    Systemd,
}

impl Platform {
    pub fn current() -> Result<Self> {
        if cfg!(target_os = "macos") {
            Ok(Self::Launchd)
        } else if cfg!(target_os = "linux") {
            Ok(Self::Systemd)
        } else {
            bail!(
                "service management is supported on macOS (launchd) and Linux (systemd --user) only"
            )
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
        let o = std::process::Command::new(program)
            .args(args)
            .stdin(std::process::Stdio::null())
            .output()?;
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
    /// `~/Library/LaunchAgents` or `~/.config/systemd/user` (tests: a temp dir).
    pub definition_dir: PathBuf,
    /// Directory for launchd's stdout/stderr files.
    pub log_dir: PathBuf,
    /// Forwarded as `SPOOL_HOME` when the install runs with one (isolated homes).
    pub spool_home: Option<PathBuf>,
    pub home: Option<PathBuf>,
    pub uid: u32,
}

impl ServiceConfig {
    pub fn for_current_user(paths: &crate::config::Paths) -> Result<Self> {
        let platform = Platform::current()?;
        let home = std::env::var_os("HOME").map(PathBuf::from);
        let need_home = || home.clone().context("HOME is not set");
        let definition_dir = match platform {
            Platform::Launchd => need_home()?.join("Library/LaunchAgents"),
            Platform::Systemd => match std::env::var_os("XDG_CONFIG_HOME") {
                Some(x) if !x.is_empty() => PathBuf::from(x).join("systemd/user"),
                _ => need_home()?.join(".config/systemd/user"),
            },
        };
        let binary = std::fs::canonicalize(std::env::current_exe()?)
            .context("resolving the spool binary path")?;
        Ok(Self {
            platform,
            label: std::env::var(LABEL_ENV).unwrap_or_else(|_| DEFAULT_LABEL.into()),
            binary,
            definition_dir,
            log_dir: crate::telemetry::log_dir(paths),
            spool_home: std::env::var_os("SPOOL_HOME").map(PathBuf::from),
            home,
            uid: rustix::process::geteuid().as_raw(),
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
        }
    }

    fn domain(&self) -> String {
        format!("gui/{}", self.cfg.uid)
    }

    fn target(&self) -> String {
        format!("{}/{}", self.domain(), self.cfg.label)
    }

    /// Text of the launchd plist or systemd unit.
    pub fn render(&self) -> String {
        match self.cfg.platform {
            Platform::Launchd => self.render_plist(),
            Platform::Systemd => self.render_unit(),
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

    pub fn definition(&self) -> Result<Definition> {
        match std::fs::read_to_string(self.definition_path()) {
            Ok(t) => {
                let ours = match self.cfg.platform {
                    Platform::Launchd => t.contains(&format!("<key>{MANAGED_KEY}</key>")),
                    Platform::Systemd => t.lines().next() == Some(MANAGED_COMMENT),
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
        };
        Ok(ServiceState {
            label: self.cfg.label.clone(),
            definition_path: self.definition_path(),
            definition: self.definition()?,
            registered,
            running,
        })
    }

    fn refuse_root(&self) -> Result<()> {
        if self.cfg.uid == 0 {
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
            std::fs::set_permissions(&self.cfg.log_dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let tmp = self.cfg.definition_dir.join(format!(
            ".{}.tmp",
            path.file_name().unwrap_or_default().to_string_lossy()
        ));
        std::fs::write(&tmp, self.render())?;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o644))?;
        std::fs::rename(&tmp, &path)?;
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
            }
            Platform::Systemd => {
                let u = self.unit_name();
                // Not-running / not-enabled are fine here; a missing systemctl is not.
                self.systemctl(&["stop", &u])?;
                self.systemctl(&["disable", &u])?;
            }
        }
        std::fs::remove_file(&path)?;
        if self.cfg.platform == Platform::Systemd {
            self.ctl_ok("systemctl", &["--user", "daemon-reload"])?;
        }
        Ok(format!(
            "removed {}; configuration, index, keys and credentials were left in place",
            path.display()
        ))
    }
}
