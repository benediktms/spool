//! Parent/turn/tool-call ancestry shared by harnesses whose records name their parent.

use crate::domain::{RecordMeta, ToolCallMeta};
use std::collections::{HashMap, HashSet};

#[derive(Default)]
pub(crate) struct RecordLineage {
    parent_by_key: HashMap<String, Option<String>>,
    turn_by_key: HashMap<String, String>,
    calls_by_key: HashMap<String, Vec<(String, u32)>>,
}

impl RecordLineage {
    pub(crate) fn ancestor_turn(&self, parent: Option<&str>) -> Option<String> {
        let mut current = parent;
        let mut seen = HashSet::new();
        while let Some(key) = current {
            if !seen.insert(key) {
                break;
            }
            if let Some(turn) = self.turn_by_key.get(key) {
                return Some(turn.clone());
            }
            current = self.parent_by_key.get(key).and_then(Option::as_deref);
        }
        None
    }

    /// Nearest ancestor record carrying the native call id; `None` when that record carries it
    /// more than once.
    pub(crate) fn ancestor_call(
        &self,
        parent: Option<&str>,
        native_call_id: &str,
    ) -> Option<(String, u32)> {
        let mut current = parent;
        let mut seen = HashSet::new();
        while let Some(key) = current {
            if !seen.insert(key) {
                break;
            }
            if let Some(matches) = self
                .calls_by_key
                .get(key)
                .filter(|calls| calls.iter().any(|(id, _)| id == native_call_id))
            {
                let mut matches = matches.iter().filter(|(id, _)| id == native_call_id);
                let call = matches.next()?;
                return matches.next().is_none().then(|| (key.to_owned(), call.1));
            }
            current = self.parent_by_key.get(key).and_then(Option::as_deref);
        }
        None
    }

    pub(crate) fn turn_of(&self, key: &str) -> Option<&str> {
        self.turn_by_key.get(key).map(String::as_str)
    }

    pub(crate) fn index(&mut self, meta: &RecordMeta, calls: &[ToolCallMeta]) {
        if let Some(turn_id) = &meta.turn_id {
            self.turn_by_key.insert(meta.key.clone(), turn_id.clone());
        }
        self.parent_by_key
            .insert(meta.key.clone(), meta.parent_key.clone());
        if !calls.is_empty() {
            let ids = calls
                .iter()
                .filter_map(|call| {
                    call.native_call_id
                        .as_ref()
                        .map(|id| (id.clone(), call.block_index))
                })
                .collect();
            self.calls_by_key.insert(meta.key.clone(), ids);
        }
    }
}
