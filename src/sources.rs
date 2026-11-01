//! Configured transcript sources (spec 6.3, 16). Built from `Config.sources`; reloadable.
//!
//! `enabled` comes from the config file. `set_enabled_in_file` is the only writer: it edits
//! `config.toml` with `toml_edit` (comments and layout preserved), after which callers reload.
//! `SourceRoots` is built per call on purpose: it holds directory fds, and a long-lived fd to a
//! deleted-and-recreated root would make a scan of the *old* (empty) directory look complete.

use crate::config::{Config, SourceConfig};
use crate::domain::HarnessKind;
use crate::source::{OpenedSource, ScanError, ScanOutcome, SourceError, SourceRoots};
use anyhow::Context;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::atomic::AtomicBool;

#[derive(Debug, Clone, PartialEq)]
pub struct SourceEntry {
    pub id: String,
    pub kind: HarnessKind,
    pub enabled: bool,
    pub include_subagents: bool,
    pub roots: Vec<PathBuf>,
}

impl From<&SourceConfig> for SourceEntry {
    fn from(c: &SourceConfig) -> Self {
        Self {
            id: c.id.clone(),
            kind: c.kind,
            enabled: c.enabled,
            include_subagents: c.include_subagents,
            roots: c.roots.iter().map(PathBuf::from).collect(),
        }
    }
}

pub struct SourceRegistry {
    inner: RwLock<BTreeMap<String, SourceEntry>>,
}

impl SourceRegistry {
    pub fn new(cfg: &Config) -> anyhow::Result<Self> {
        let r = Self {
            inner: RwLock::new(BTreeMap::new()),
        };
        r.reload(cfg)?;
        Ok(r)
    }

    pub fn reload(&self, cfg: &Config) -> anyhow::Result<()> {
        let map: BTreeMap<String, SourceEntry> = cfg
            .sources
            .iter()
            .map(|s| (s.id.clone(), SourceEntry::from(s)))
            .collect();
        *self
            .inner
            .write()
            .map_err(|_| anyhow::anyhow!("source registry poisoned"))? = map;
        Ok(())
    }

    fn with<T>(&self, id: &str, f: impl FnOnce(&SourceEntry) -> T) -> Option<T> {
        self.inner.read().ok()?.get(id).map(f)
    }

    /// Every configured source (enabled or not), ordered by id.
    pub fn entries(&self) -> Vec<SourceEntry> {
        self.inner
            .read()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn entry(&self, id: &str) -> Option<SourceEntry> {
        self.with(id, Clone::clone)
    }

    pub fn enabled_ids(&self) -> Vec<String> {
        self.inner
            .read()
            .map(|m| {
                m.values()
                    .filter(|e| e.enabled)
                    .map(|e| e.id.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// False for unknown and disabled sources.
    pub fn is_enabled(&self, source_id: &str) -> bool {
        self.with(source_id, |e| e.enabled).unwrap_or(false)
    }

    pub fn kind(&self, source_id: &str) -> Option<HarnessKind> {
        self.with(source_id, |e| e.kind)
    }

    pub fn include_subagents(&self, source_id: &str) -> bool {
        self.with(source_id, |e| e.include_subagents)
            .unwrap_or(false)
    }

    /// Approved artifact roots for `ParseOptions::blob_roots`: the configured roots themselves.
    pub fn blob_roots(&self, source_id: &str) -> Vec<PathBuf> {
        self.with(source_id, |e| {
            if e.enabled {
                e.roots.clone()
            } else {
                Vec::new()
            }
        })
        .unwrap_or_default()
    }

    fn roots(&self, source_id: &str) -> Option<Result<SourceRoots, std::io::Error>> {
        let e = self.entry(source_id).filter(|e| e.enabled)?;
        Some(SourceRoots::new(&e.roots))
    }

    /// One page of a read-only scan. `None` for unknown/disabled sources. An unavailable root is an
    /// *incomplete* outcome carrying the error, never an empty complete one (spec 7.3).
    pub fn scan_from(
        &self,
        source_id: &str,
        cancel: &AtomicBool,
        max_entries: usize,
        after: Option<&Path>,
    ) -> Option<ScanOutcome> {
        Some(match self.roots(source_id)? {
            Ok(roots) => roots.scan_from(cancel, max_entries, after),
            Err(e) => {
                let entry = self.entry(source_id)?;
                ScanOutcome {
                    complete: false,
                    errors: entry
                        .roots
                        .into_iter()
                        .map(|path| ScanError {
                            path,
                            kind: e.kind(),
                        })
                        .collect(),
                    ..Default::default()
                }
            }
        })
    }

    /// Safe open (root containment + identity) of `path` under the source's roots.
    pub fn open(&self, source_id: &str, path: &Path) -> Result<OpenedSource, SourceError> {
        match self.roots(source_id) {
            None => Err(SourceError::OutsideRoot),
            Some(Ok(r)) => r.open(path),
            Some(Err(e)) => Err(SourceError::Io(e)),
        }
    }
}

/// Edit `enabled` of `[[sources]]` entry `id` in `config.toml`, preserving comments and layout.
/// When the file has no `sources` array (defaults are in effect) the default list is materialised
/// first, otherwise adding one entry would silently drop the other defaults.
pub fn set_enabled_in_file(config_file: &Path, id: &str, enabled: bool) -> anyhow::Result<()> {
    use toml_edit::{ArrayOfTables, DocumentMut, Item, Table, value};
    let text = match std::fs::read_to_string(config_file) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e).context("reading config.toml"),
    };
    let mut doc: DocumentMut = text.parse().context("parsing config.toml")?;
    if !doc.contains_key("sources") {
        let mut arr = ArrayOfTables::new();
        for s in Config::default().sources {
            let mut t = Table::new();
            t["id"] = value(s.id);
            t["kind"] = value(s.kind.as_str());
            t["enabled"] = value(s.enabled);
            let mut roots = toml_edit::Array::new();
            for r in s.roots {
                roots.push(r);
            }
            t["roots"] = value(roots);
            t["include_subagents"] = value(s.include_subagents);
            arr.push(t);
        }
        doc["sources"] = Item::ArrayOfTables(arr);
    }
    let arr = doc["sources"]
        .as_array_of_tables_mut()
        .context("`sources` must be an array of tables")?;
    let t = arr
        .iter_mut()
        .find(|t| t.get("id").and_then(|v| v.as_str()) == Some(id))
        .with_context(|| format!("unknown source {id:?}"))?;
    t["enabled"] = value(enabled);
    write_private(config_file, doc.to_string().as_bytes())
}

/// Atomic replace keeping the private ACL (the config may name credential paths).
fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("toml.tmp");
    let _ = std::fs::remove_file(&tmp);
    {
        let mut file = crate::perms::create_private_file(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    crate::perms::replace_private(&tmp, path)?;
    Ok(())
}
