//! `spool init` (spec 3.1): config, private directories, handle-signing key, recognized source
//! roots. Reads no transcript. Consent is explicit: a TTY is asked, otherwise explicit flags
//! decide, otherwise the safe defaults stay and the report says so. `false` is never turned
//! into `true` implicitly.

use super::args::{InitArgs, YesNo};
use crate::config::{Config, Paths};
use crate::transcript::handle::HandleKey;
use anyhow::{Context, Result, bail};
use std::io::{BufRead, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;

pub trait Prompter {
    /// Ask a yes/no question; end of input or an unrecognised answer means `default`.
    fn ask(&mut self, question: &str, default: bool) -> std::io::Result<bool>;
}

pub struct StdinPrompter;

impl Prompter for StdinPrompter {
    fn ask(&mut self, question: &str, default: bool) -> std::io::Result<bool> {
        let hint = if default { "[Y/n]" } else { "[y/N]" };
        print!("{question} {hint} ");
        std::io::stdout().flush()?;
        let mut line = String::new();
        if std::io::stdin().lock().read_line(&mut line)? == 0 {
            println!();
            return Ok(default);
        }
        Ok(match line.trim().to_ascii_lowercase().as_str() {
            "y" | "yes" => true,
            "n" | "no" => false,
            _ => default,
        })
    }
}

#[derive(Debug, Clone)]
pub struct SourceLine {
    pub id: String,
    pub kind: String,
    pub enabled: bool,
    pub roots: Vec<PathBuf>,
    pub present: bool,
}

#[derive(Debug)]
pub struct InitReport {
    pub root: PathBuf,
    pub config_written: bool,
    pub key_created: bool,
    pub hosted_processing_approved: bool,
    pub sources: Vec<SourceLine>,
    pub notes: Vec<String>,
}

const HEADER: &str = "# Spool configuration, written by `spool init`.\n\
# embedding.hosted_processing_approved = false keeps all conversation text on this machine\n\
# (lexical search only). Setting it to true lets rendered conversation chunks and semantic\n\
# search queries be sent to the configured hosted embedding provider, and needs a credential file.\n\n";

pub fn run_init(
    paths: &Paths,
    args: &InitArgs,
    mut prompter: Option<&mut dyn Prompter>,
) -> Result<InitReport> {
    let defaults = Config::default();
    let known: Vec<&str> = defaults.sources.iter().map(|s| s.id.as_str()).collect();
    for id in args.enable_source.iter().chain(&args.disable_source) {
        if !known.contains(&id.as_str()) {
            bail!(
                "unknown source {id:?}; recognized sources: {}",
                known.join(", ")
            );
        }
    }
    if let Some(id) = args
        .enable_source
        .iter()
        .find(|i| args.disable_source.contains(i))
    {
        bail!("source {id:?} is both enabled and disabled");
    }
    let has_flags = !args.enable_source.is_empty()
        || !args.disable_source.is_empty()
        || args.hosted_processing.is_some();
    let exists = paths.config_file.exists();
    if exists && has_flags && !args.force {
        bail!(
            "{} already exists; refusing to change it implicitly. Use `spool sources enable/disable`, edit the file, or pass --force to rewrite it",
            paths.config_file.display()
        );
    }

    for d in [&paths.root, &paths.state_dir, &paths.secrets_dir] {
        crate::daemon::ensure_private_dir(d)
            .with_context(|| format!("creating {}", d.display()))?;
    }

    let mut notes = Vec::new();
    let mut config_written = false;
    if !exists || args.force {
        let mut cfg = defaults.clone();
        for s in &mut cfg.sources {
            if args.enable_source.contains(&s.id) {
                s.enabled = true;
            } else if args.disable_source.contains(&s.id) {
                s.enabled = false;
            } else if let Some(p) = prompter.as_deref_mut() {
                let root = s.roots.first().cloned().unwrap_or_default();
                let found = crate::config::expand_path(&root, &paths.root)
                    .map(|p| p.exists())
                    .unwrap_or(false);
                let q = format!(
                    "Index source '{}' ({root}, {})?",
                    s.id,
                    if found { "found" } else { "not found" }
                );
                s.enabled = p.ask(&q, false)?;
            } else {
                // The user chooses sources; nothing is enabled implicitly.
                s.enabled = false;
            }
        }
        cfg.embedding.hosted_processing_approved = match (args.hosted_processing, prompter.as_deref_mut()) {
            (Some(v), _) => v == YesNo::Yes,
            (None, Some(p)) => p.ask(
                "Allow hosted embedding? Rendered conversation chunks and semantic search queries would be sent to the provider; text may contain pasted secrets and filtering is not a secret scanner. Needs a credential file.",
                false,
            )?,
            (None, None) => false,
        };
        if prompter.is_none() && args.enable_source.is_empty() {
            notes.push("non-interactive: kept the safe defaults, no source is enabled (nothing will be indexed). Use --enable-source ID to choose sources".into());
        }
        if prompter.is_none() && args.hosted_processing.is_none() {
            notes.push("hosted processing not specified: kept 'no' (lexical-only)".into());
        }
        let text = format!("{HEADER}{}", toml::to_string_pretty(&cfg)?);
        Config::parse(&text, &paths.root).context("generated config failed validation")?;
        let tmp = paths.root.join(".config.toml.tmp");
        {
            let _ = std::fs::remove_file(&tmp);
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&tmp)?;
            f.write_all(text.as_bytes())?;
        }
        std::fs::rename(&tmp, &paths.config_file)?;
        config_written = true;
    } else {
        notes.push(format!("kept existing {}", paths.config_file.display()));
    }

    let key_created = if paths.signing_key.exists() {
        false
    } else {
        HandleKey::create(&paths.signing_key)
            .with_context(|| format!("creating {}", paths.signing_key.display()))?;
        true
    };

    let cfg = Config::load(paths)?;
    if cfg.embedding.hosted_processing_approved {
        notes.push(format!(
            "hosted processing is ON: place the provider key in {} (mode 0600); without it the daemon stays lexical-only",
            cfg.credential_path(&paths.root)?.display()
        ));
    }
    let sources = cfg
        .sources
        .iter()
        .map(|s| {
            let roots: Vec<PathBuf> = s.roots.iter().map(PathBuf::from).collect();
            SourceLine {
                id: s.id.clone(),
                kind: s.kind.as_str().into(),
                enabled: s.enabled,
                present: roots.iter().any(|r| r.exists()),
                roots,
            }
        })
        .collect();
    Ok(InitReport {
        root: paths.root.clone(),
        config_written,
        key_created,
        hosted_processing_approved: cfg.embedding.hosted_processing_approved,
        sources,
        notes,
    })
}

pub fn print_report(r: &InitReport) {
    println!("spool home: {} (mode 0700)", r.root.display());
    println!(
        "config: {}",
        if r.config_written {
            "written"
        } else {
            "unchanged"
        }
    );
    println!(
        "handle-signing key: {}",
        if r.key_created {
            "created (mode 0600)"
        } else {
            "already present"
        }
    );
    println!("recognized sources (nothing has been read or indexed):");
    for s in &r.sources {
        println!(
            "  {:<8} {:<8} {:<8} {}{}",
            s.id,
            s.kind,
            if s.enabled { "enabled" } else { "disabled" },
            s.roots
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", "),
            if s.present { "" } else { "  (not found)" }
        );
    }
    println!(
        "hosted processing: {}",
        if r.hosted_processing_approved {
            "approved"
        } else {
            "not approved (lexical-only, no text leaves this machine)"
        }
    );
    for n in &r.notes {
        println!("note: {n}");
    }
    println!(
        "next: `spool service install && spool service start` (or `spool daemon run`), then `spool doctor`"
    );
}
