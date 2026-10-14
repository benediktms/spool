use crate::domain::{MessageEntry, ParseWarning, RecordMeta, SourceLocator};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    pub key: String,
    pub messages: Vec<String>,
    pub parent_run: Option<String>,
}
#[derive(Debug, Default)]
pub struct SessionGraph {
    parents: HashMap<String, Option<String>>,
    aliases: HashMap<String, String>,
    message_keys: HashSet<String>,
    message_parent: HashMap<String, Option<String>>,
    runs: Vec<Run>,
    leaves: Vec<String>,
}
impl SessionGraph {
    pub fn build(records: &[RecordMeta], messages: &[MessageEntry]) -> (Self, Vec<ParseWarning>) {
        let mut g = Self::default();
        let mut warnings = Vec::new();
        for m in messages {
            for old in &m.supersedes {
                g.aliases.insert(old.clone(), m.record_key.clone());
            }
        }
        for r in records {
            g.parents.insert(r.key.clone(), r.parent_key.clone());
        }
        g.message_keys
            .extend(messages.iter().map(|m| m.record_key.clone()));
        let mut clear = HashSet::<String>::new();
        for r in records {
            if let Some(p) = &r.parent_key {
                if !g.parents.contains_key(p) {
                    warnings.push(warn("missing_parent", &r.key, r.locator));
                }
            }
            let mut p = r.parent_key.clone();
            let mut seen = HashSet::from([r.key.clone()]);
            let mut bad = false;
            while let Some(k) = p.clone() {
                if clear.contains(&k) {
                    break;
                }
                if !seen.insert(k.clone()) {
                    warnings.push(warn("cycle", &r.key, r.locator));
                    bad = true;
                    break;
                }
                match g.parents.get(&k) {
                    Some(next) => p = next.clone(),
                    None => {
                        p = None;
                        break;
                    }
                }
            }
            if !bad {
                clear.extend(seen); // every record on an acyclic chain: later walks stop here
            } else {
                g.parents.insert(r.key.clone(), None);
            }
        }
        for r in records {
            if !g.message_keys.contains(&r.key) {
                continue;
            }
            let mut p = r.parent_key.clone();
            let mut seen = HashSet::new();
            let mut ancestor = None;
            while let Some(k) = p {
                if !seen.insert(k.clone()) {
                    break;
                }
                let canonical = g.canonical_key(&k);
                // a later save of the same message follows records that descend from its earlier versions: skip them
                if g.message_keys.contains(canonical) && canonical != g.canonical_key(&r.key) {
                    ancestor = Some(canonical.to_owned());
                    break;
                }
                p = g.parents.get(&k).cloned().flatten();
            }
            g.message_parent.insert(r.key.clone(), ancestor);
        }
        let mut finished = HashSet::new();
        let mut cyclic = HashSet::new();
        for start in &g.message_keys {
            if finished.contains(start) {
                continue;
            }
            let mut path = Vec::new();
            let mut positions = HashMap::new();
            let mut current = Some(start.as_str());
            while let Some(key) = current {
                if finished.contains(key) {
                    break;
                }
                if let Some(&cycle_start) = positions.get(key) {
                    cyclic.extend(path[cycle_start..].iter().cloned());
                    break;
                }
                positions.insert(key.to_owned(), path.len());
                path.push(key.to_owned());
                current = g.message_parent.get(key).and_then(Option::as_deref);
            }
            finished.extend(path);
        }
        for key in &cyclic {
            g.message_parent.insert(key.clone(), None);
            if !warnings.iter().any(|warning| {
                warning.code == "cycle" && warning.record_key.as_deref() == Some(key)
            }) {
                if let Some(record) = records.iter().find(|record| record.key == *key) {
                    warnings.push(warn("cycle", key, record.locator));
                }
            }
        }
        let children: HashMap<String, usize> =
            g.message_parent
                .values()
                .flatten()
                .fold(HashMap::new(), |mut a, k| {
                    *a.entry(k.clone()).or_default() += 1;
                    a
                });
        let mut ordered: Vec<_> = messages.iter().map(|m| m.record_key.as_str()).collect();
        // first physical position of each record, built once (a per-comparison scan was quadratic)
        let mut ordinal: HashMap<&str, usize> = HashMap::with_capacity(records.len());
        for (i, r) in records.iter().enumerate() {
            ordinal.entry(r.key.as_str()).or_insert(i);
        }
        ordered.sort_by_key(|k| ordinal.get(k).copied().unwrap_or(usize::MAX));
        let mut run_of: HashMap<String, String> = HashMap::new();
        let mut current: Option<Run> = None;
        for k in ordered {
            let parent = g.message_parent.get(k).cloned().flatten();
            let continue_run = if let Some(run) = current.as_ref() {
                if let Some(last) = run.messages.last() {
                    !cyclic.contains(k)
                        && !cyclic.contains(last)
                        && parent.as_deref() == Some(last.as_str())
                        && children.get(last).copied().unwrap_or(0) == 1
                } else {
                    false
                }
            } else {
                false
            };
            if !continue_run {
                if let Some(run) = current.take() {
                    g.runs.push(run)
                }
                let parent_run = parent.as_ref().and_then(|p| run_of.get(p).cloned());
                current = Some(Run {
                    key: k.to_string(),
                    messages: Vec::new(),
                    parent_run,
                });
            }
            run_of.insert(k.to_string(), current.as_ref().unwrap().key.clone());
            current.as_mut().unwrap().messages.push(k.to_string());
        }
        if let Some(run) = current {
            g.runs.push(run)
        }
        let parents: HashSet<_> = g.message_parent.values().flatten().cloned().collect();
        g.leaves = messages
            .iter()
            .map(|m| m.record_key.clone())
            .filter(|k| !parents.contains(k))
            .collect();
        (g, warnings)
    }
    fn canonical_key<'a>(&'a self, key: &'a str) -> &'a str {
        let mut current = key;
        for _ in 0..self.aliases.len() {
            match self.aliases.get(current) {
                Some(next) => current = next,
                None => break,
            }
        }
        current
    }
    pub fn message_parent(&self, key: &str) -> Option<&str> {
        let key = self.canonical_key(key);
        self.message_parent.get(key).and_then(Option::as_deref)
    }
    pub fn runs(&self) -> &[Run] {
        &self.runs
    }
    pub fn leaves(&self) -> &[String] {
        &self.leaves
    }
    pub fn path_to<'a>(&'a self, leaf: &'a str) -> Vec<&'a str> {
        let mut out: Vec<&str> = Vec::new();
        let mut cur: Option<&str> = Some(self.canonical_key(leaf));
        let mut seen: HashSet<&str> = HashSet::new();
        while let Some(k) = cur {
            if !seen.insert(k) {
                break;
            }
            out.push(k);
            cur = self.message_parent(k);
        }
        out.reverse();
        out
    }
    pub fn is_ancestor(&self, a: &str, b: &str) -> bool {
        self.path_to(b).contains(&self.canonical_key(a))
    }
}
fn warn(code: &str, key: &str, locator: SourceLocator) -> ParseWarning {
    ParseWarning {
        code: code.into(),
        record_key: Some(key.into()),
        locator: Some(locator),
        detail: code.into(),
    }
}
