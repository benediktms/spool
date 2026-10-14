//! `spool config show`: effective configuration, secrets redacted, every value marked as a
//! default or an override of the built-in default.

use crate::config::{Config, Paths};
use toml::{Table, Value};

fn secret_key(k: &str) -> bool {
    // `credential_file` is a path, not a secret; `*_token_budget` are numbers.
    k.ends_with("_key")
        || k.ends_with("_secret")
        || k.contains("password")
        || k == "authorization"
        || k == "api_key"
}

/// Hide URL userinfo (`https://user:pw@host/..`) and secret-named fields.
pub fn redact(key: &str, v: &Value) -> Value {
    match v {
        _ if secret_key(key) => Value::String("<redacted>".into()),
        Value::String(s) => match s.split_once("://") {
            Some((scheme, rest)) => {
                let authority_end = rest.find('/').unwrap_or(rest.len());
                match rest[..authority_end].rfind('@') {
                    Some(at) => Value::String(format!("{scheme}://<redacted>@{}", &rest[at + 1..])),
                    None => v.clone(),
                }
            }
            None => v.clone(),
        },
        _ => v.clone(),
    }
}

fn is_table_array(v: &Value) -> bool {
    matches!(v, Value::Array(a) if !a.is_empty() && a.iter().all(Value::is_table))
}

fn emit(t: &Table, def: Option<&Table>, path: &str, out: &mut String) {
    for (k, v) in t {
        if v.is_table() || is_table_array(v) {
            continue;
        }
        let shown = redact(k, v);
        let is_default = def.and_then(|d| d.get(k)) == Some(v);
        let tag = if is_default {
            "default".to_string()
        } else {
            match def.and_then(|d| d.get(k)) {
                Some(dv) => format!("override (default: {})", redact(k, dv)),
                None => "override".to_string(),
            }
        };
        out.push_str(&format!("{k} = {shown}  # {tag}\n"));
    }
    for (k, v) in t {
        let sub = if path.is_empty() {
            k.clone()
        } else {
            format!("{path}.{k}")
        };
        match v {
            Value::Table(st) => {
                out.push_str(&format!("\n[{sub}]\n"));
                emit(
                    st,
                    def.and_then(|d| d.get(k)).and_then(Value::as_table),
                    &sub,
                    out,
                );
            }
            arr if is_table_array(arr) => {
                let dflt = def.and_then(|d| d.get(k)).and_then(Value::as_array);
                for (i, item) in arr.as_array().into_iter().flatten().enumerate() {
                    out.push_str(&format!("\n[[{sub}]]\n"));
                    emit(
                        item.as_table().unwrap_or(&Table::new()),
                        dflt.and_then(|d| d.get(i)).and_then(Value::as_table),
                        &sub,
                        out,
                    );
                }
            }
            _ => {}
        }
    }
}

pub fn show(paths: &Paths) -> anyhow::Result<String> {
    let cfg = Config::load(paths)?;
    // Defaults go through the same path expansion so unchanged roots compare equal.
    let defaults = Config::parse(&toml::to_string(&Config::default())?, &paths.root)?;
    let (Value::Table(cur), Value::Table(def)) =
        (Value::try_from(&cfg)?, Value::try_from(&defaults)?)
    else {
        anyhow::bail!("config did not serialize to a table")
    };
    let mut out = format!(
        "# config file: {} ({})\n# secrets are redacted; the provider credential itself is never part of the config\n",
        paths.config_file.display(),
        if paths.config_file.exists() {
            "present"
        } else {
            "absent, built-in defaults"
        }
    );
    emit(&cur, Some(&def), "", &mut out);
    Ok(out)
}
