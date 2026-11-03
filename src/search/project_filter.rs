use super::query::normalize_abs_path;
use super::retrieve::storage_err;
use super::types::{ProjectRef, ResolvedProjects, Warning};
use crate::ipc::AppError;
use crate::projects::{normalize_remote, resolve};
use sqlx::{Row, SqlitePool};
use std::collections::BTreeMap;

pub(super) const MAX_PROJECT_IDS: usize = 256;
const MAX_ENTRY_BYTES: usize = 512;
const URL_LABEL: &str = "<git url>";

enum Form {
    Path,
    Url,
    OwnerRepo,
    Name,
}

fn form(entry: &str) -> Form {
    if entry.starts_with('/') {
        return Form::Path;
    }
    let scp = entry
        .split_once(':')
        .is_some_and(|(head, _)| head.contains('@') && !head.contains('/'));
    if entry.contains("://") || scp {
        Form::Url
    } else if entry.matches('/').count() == 1 {
        Form::OwnerRepo
    } else {
        Form::Name
    }
}

pub(super) struct Resolution {
    pub ids: Vec<String>,
    pub resolved: Vec<ResolvedProjects>,
    pub warnings: Vec<Warning>,
}

pub(super) async fn resolve_entries(
    pool: &SqlitePool,
    entries: &[String],
    invalid: impl Fn(String) -> AppError,
) -> Result<Resolution, AppError> {
    if entries.is_empty() || entries.len() > 16 {
        return Err(invalid(
            "projects must hold between 1 and 16 entries".into(),
        ));
    }
    if entries
        .iter()
        .any(|e| e.trim().is_empty() || e.len() > MAX_ENTRY_BYTES)
    {
        return Err(invalid(format!(
            "projects entries must be non-empty and at most {MAX_ENTRY_BYTES} bytes"
        )));
    }
    let mut ids: BTreeMap<String, ()> = BTreeMap::new();
    let mut resolved = Vec::new();
    let mut warnings = Vec::new();
    for raw in entries {
        let entry = raw.trim();
        let (label, rows) = match form(entry) {
            Form::Path => (entry.to_string(), by_path(pool, entry).await?),
            Form::Url => match normalize_remote(entry) {
                Ok(key) => {
                    let rows = query(pool, "key = ?1", &key).await?;
                    (key, rows)
                }
                Err(_) => (URL_LABEL.to_string(), Vec::new()),
            },
            Form::OwnerRepo => {
                let tail = format!("/{}", entry.trim_end_matches(".git"));
                let rows = query(
                    pool,
                    "kind = 'git_remote' AND length(key) > length(?1) \
                     AND lower(substr(key, -length(?1))) = lower(?1)",
                    &tail,
                )
                .await?;
                (entry.to_string(), rows)
            }
            Form::Name => (
                entry.to_string(),
                query(pool, "lower(name) = lower(?1)", entry).await?,
            ),
        };
        if rows.is_empty() {
            warnings.push(Warning {
                code: "project_not_found".into(),
                message: format!("No indexed project matches `{label}`."),
                cause: None,
            });
        }
        let mut projects = Vec::new();
        for (id, project) in rows {
            ids.insert(id, ());
            projects.push(project);
        }
        resolved.push(ResolvedProjects {
            input: label,
            projects,
        });
    }
    let mut ids: Vec<String> = ids.into_keys().collect();
    if ids.len() > MAX_PROJECT_IDS {
        ids.truncate(MAX_PROJECT_IDS);
        warnings.push(Warning {
            code: "project_filter_truncated".into(),
            message: format!("More than {MAX_PROJECT_IDS} projects matched; only the first {MAX_PROJECT_IDS} are searched."),
            cause: None,
        });
    }
    Ok(Resolution {
        ids,
        resolved,
        warnings,
    })
}

async fn query(
    pool: &SqlitePool,
    cond: &str,
    arg: &str,
) -> Result<Vec<(String, ProjectRef)>, AppError> {
    let sql = format!("SELECT id, name, remote FROM projects WHERE {cond} ORDER BY id");
    let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(arg)
        .fetch_all(pool)
        .await
        .map_err(storage_err)?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<String, _>(0),
                ProjectRef {
                    name: r.get(1),
                    remote: r.get(2),
                },
            )
        })
        .collect())
}

async fn by_path(pool: &SqlitePool, path: &str) -> Result<Vec<(String, ProjectRef)>, AppError> {
    let Some(norm) = normalize_abs_path(path) else {
        return Ok(Vec::new());
    };
    let mut candidates = ancestors(&norm);
    if let Ok(canon) = tokio::fs::canonicalize(&norm).await
        && let Some(c) = canon.to_str()
    {
        for a in ancestors(c) {
            if !candidates.contains(&a) {
                candidates.push(a);
            }
        }
    }
    candidates.truncate(128);
    let marks = vec!["?"; candidates.len()].join(",");
    let sql = format!(
        "SELECT pp.path, p.id, p.name, p.remote FROM project_paths pp \
         JOIN projects p ON p.id = pp.project_id WHERE pp.path IN ({marks})"
    );
    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
    for c in &candidates {
        q = q.bind(c);
    }
    let rows = q.fetch_all(pool).await.map_err(storage_err)?;
    let nearest = rows.iter().max_by_key(|r| r.get::<String, _>(0).len());
    if let Some(r) = nearest {
        return Ok(vec![(
            r.get(1),
            ProjectRef {
                name: r.get(2),
                remote: r.get(3),
            },
        )]);
    }
    let cwd = norm.clone();
    let key = tokio::task::spawn_blocking(move || resolve(&cwd, None).key)
        .await
        .map_err(|_| AppError::new("STORAGE_ERROR", "project resolution failed"))?;
    query(pool, "key = ?1", &key).await
}

fn ancestors(path: &str) -> Vec<String> {
    let mut out = vec![path.to_string()];
    let mut cur = path;
    while let Some(i) = cur.rfind('/') {
        cur = &cur[..i];
        out.push(if cur.is_empty() {
            "/".into()
        } else {
            cur.to_string()
        });
        if cur.is_empty() {
            break;
        }
    }
    out
}
