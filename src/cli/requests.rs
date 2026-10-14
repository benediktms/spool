//! Flag validation and request construction. Pure functions: nothing here touches the daemon.
//! Incompatible combinations are rejected before any call (spec 16.2 / read-session contract 3).

use super::args::{IndexArgs, IndexSub, ReadArgs, SearchArgs};
use super::timeparse::parse_when;
use crate::clock::{Tz, UTC};
use serde_json::{Map, Value, json};

pub type Built<T> = Result<T, String>;

fn put<T: Into<Value>>(m: &mut Map<String, Value>, k: &str, v: Option<T>) {
    if let Some(v) = v {
        m.insert(k.into(), v.into());
    }
}

fn bounded(name: &str, v: Option<u32>, lo: u32, hi: u32) -> Built<()> {
    match v {
        Some(x) if x < lo || x > hi => Err(format!("--{name} must be between {lo} and {hi}")),
        _ => Ok(()),
    }
}

/// `search_sessions` arguments. Relative dates become absolute RFC 3339 UTC instants here.
pub fn search_request(a: &SearchArgs, now_ms: i64, tz: Tz) -> Built<Value> {
    if a.query.trim().is_empty() {
        return Err("the query must not be empty".into());
    }
    let mut m = Map::new();
    m.insert("query".into(), a.query.clone().into());
    put(&mut m, "mode", a.mode.map(|x| x.as_str()));
    if !a.source.is_empty() {
        m.insert("sources".into(), json!(a.source));
    }
    for (flag, key, v) in [("since", "after", &a.since), ("until", "before", &a.until)] {
        if let Some(raw) = v {
            let ms = parse_when(raw, now_ms, tz).map_err(|e| format!("--{flag}: {e}"))?;
            m.insert(key.into(), super::timeparse::format_rfc3339(ms, UTC).into());
        }
    }
    if let (Some(a_), Some(b_)) = (m.get("after"), m.get("before")) {
        if a_.as_str() >= b_.as_str() {
            return Err("--since must be earlier than --until".into());
        }
    }
    put(&mut m, "limit", a.limit);
    Ok(Value::Object(m))
}

/// `read_session` arguments: initial read or cursor-only continuation.
pub fn read_request(a: &ReadArgs) -> Built<Value> {
    bounded("max-bytes", a.max_bytes, 4096, 262_144)?;
    bounded("max-items", a.max_items, 1, 200)?;
    let mut m = Map::new();
    if let Some(cursor) = &a.cursor {
        let others = [
            ("session id", a.session_id.is_some()),
            ("--match-ref", a.match_ref.is_some()),
            ("--message-ref", a.message_ref.is_some()),
            ("--item-ref", a.item_ref.is_some()),
            ("--from-ref", a.from_ref.is_some()),
            ("--through-ref", a.through_ref.is_some()),
            ("--branch-ref", a.branch_ref.is_some()),
            ("--before", a.before.is_some()),
            ("--after", a.after.is_some()),
            ("--with-tools", a.with_tools),
            ("--raw", a.raw),
            ("--source-policy", a.source_policy.is_some()),
        ];
        if let Some((name, _)) = others.iter().find(|(_, set)| *set) {
            return Err(format!(
                "--cursor accepts only --max-bytes and --max-items; remove {name}"
            ));
        }
        m.insert("cursor".into(), cursor.clone().into());
        put(&mut m, "max_bytes", a.max_bytes);
        put(&mut m, "max_items", a.max_items);
        return Ok(Value::Object(m));
    }

    let session = a
        .session_id
        .clone()
        .ok_or("a session id (or --cursor) is required")?;
    if a.with_tools && a.raw {
        return Err("--with-tools and --raw are mutually exclusive".into());
    }
    let range = a.from_ref.is_some() || a.through_ref.is_some();
    if range && (a.from_ref.is_none() || a.through_ref.is_none()) {
        return Err("--from-ref and --through-ref must be given together".into());
    }
    let selectors = [
        ("--match-ref", a.match_ref.is_some()),
        ("--message-ref", a.message_ref.is_some()),
        ("--item-ref", a.item_ref.is_some()),
        ("--from-ref/--through-ref", range),
    ];
    let chosen: Vec<&str> = selectors
        .iter()
        .filter(|(_, s)| *s)
        .map(|(n, _)| *n)
        .collect();
    if chosen.len() > 1 {
        return Err(format!(
            "choose one selection; got {}",
            chosen.join(" and ")
        ));
    }
    let has_context = a.match_ref.is_some() || a.message_ref.is_some();
    if (a.before.is_some() || a.after.is_some()) && !has_context {
        return Err("--before/--after apply only to --match-ref or --message-ref".into());
    }
    bounded("before", a.before, 0, 20)?;
    bounded("after", a.after, 0, 20)?;
    if a.item_ref.is_some() && a.branch_ref.is_some() {
        return Err("--branch-ref cannot be combined with --item-ref".into());
    }

    let mut sel = Map::new();
    if let Some(r) = &a.match_ref {
        sel.insert("kind".into(), "match".into());
        sel.insert("match_ref".into(), r.clone().into());
    } else if let Some(r) = &a.message_ref {
        sel.insert("kind".into(), "message".into());
        sel.insert("message_ref".into(), r.clone().into());
    } else if let Some(r) = &a.item_ref {
        sel.insert("kind".into(), "item".into());
        sel.insert("item_ref".into(), r.clone().into());
    } else if range {
        sel.insert("kind".into(), "range".into());
        sel.insert("from_message_ref".into(), a.from_ref.clone().into());
        sel.insert("through_message_ref".into(), a.through_ref.clone().into());
    } else {
        sel.insert("kind".into(), "session".into());
    }
    put(&mut sel, "branch_ref", a.branch_ref.clone());
    if has_context {
        put(&mut sel, "before", a.before);
        put(&mut sel, "after", a.after);
    }

    m.insert("session_id".into(), session.into());
    m.insert("selection".into(), Value::Object(sel));
    if a.with_tools {
        m.insert("view".into(), "with_tools".into());
    } else if a.raw {
        m.insert("view".into(), "raw_records".into());
    }
    put(&mut m, "source_policy", a.source_policy.map(|p| p.as_str()));
    put(&mut m, "max_bytes", a.max_bytes);
    put(&mut m, "max_items", a.max_items);
    Ok(Value::Object(m))
}

/// A control command plus its JSON arguments, ready for `Request::Control`.
#[derive(Debug, PartialEq)]
pub struct ControlCall {
    pub cmd: &'static str,
    pub args: Value,
    /// Resolved absolute `--since` (ms), printed so dry-runs show the exact cutoff.
    pub since_ms: Option<i64>,
}

/// Maps `spool index ...` onto control commands. `idem` is the caller-generated idempotency key.
pub fn index_request(a: &IndexArgs, now_ms: i64, tz: Tz, idem: &str) -> Built<ControlCall> {
    if let Some(sub) = &a.sub {
        let (cmd, args) = match sub {
            IndexSub::Rebuild {
                lexical_only,
                embeddings_only,
            } => (
                "index_rebuild",
                json!({"lexical_only": lexical_only, "embeddings_only": embeddings_only, "idempotency_key": idem}),
            ),
            IndexSub::Remove { session, exclude } => (
                "index_remove",
                json!({"session_id": session, "exclude": exclude, "idempotency_key": idem}),
            ),
            IndexSub::Purge { unreferenced } => {
                if !unreferenced {
                    return Err("purge is never implicit: pass --unreferenced".into());
                }
                (
                    "index_purge",
                    json!({"unreferenced": true, "idempotency_key": idem}),
                )
            }
        };
        return Ok(ControlCall {
            cmd,
            args,
            since_ms: None,
        });
    }
    let modes = [a.since.is_some(), a.all, a.session.is_some()];
    match modes.iter().filter(|m| **m).count() {
        0 => {
            return Err(
                "choose one of --since, --all, --session, or a subcommand (rebuild, remove, purge)"
                    .into(),
            );
        }
        1 => {}
        _ => return Err("--since, --all and --session are mutually exclusive".into()),
    }
    if let Some(session) = &a.session {
        if a.dry_run {
            return Err("--dry-run does not apply to --session".into());
        }
        if a.source.is_some() {
            return Err("--source does not apply to --session".into());
        }
        return Ok(ControlCall {
            cmd: "index_session",
            args: json!({"session_id": session, "idempotency_key": idem}),
            since_ms: None,
        });
    }
    let mut args = Map::new();
    put(&mut args, "source", a.source.clone());
    args.insert("dry_run".into(), a.dry_run.into());
    args.insert("idempotency_key".into(), idem.into());
    if let Some(raw) = &a.since {
        let ms = parse_when(raw, now_ms, tz).map_err(|e| format!("--since: {e}"))?;
        args.insert("since_ms".into(), ms.into());
        return Ok(ControlCall {
            cmd: "index_since",
            args: Value::Object(args),
            since_ms: Some(ms),
        });
    }
    Ok(ControlCall {
        cmd: "index_all",
        args: Value::Object(args),
        since_ms: None,
    })
}
