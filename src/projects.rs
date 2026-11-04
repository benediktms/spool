//! Project identity for sessions. Call filesystem work from `spawn_blocking`, never from an
//! async task or inside a write transaction.

use gix_config::file::{includes, init};
use gix_config::{File, Source};
use std::path::{Path, PathBuf};

/// Lexically normalize an absolute path (no filesystem access): collapse `//`, `.`, `..`, trailing `/`.
pub(crate) fn normalize_abs_path(p: &str) -> Option<String> {
    if !p.starts_with('/') {
        return None;
    }
    let mut out: Vec<&str> = Vec::new();
    for seg in p.split('/') {
        match seg {
            "" | "." => {}
            ".." => {
                out.pop();
            }
            s => out.push(s),
        }
    }
    Some(format!("/{}", out.join("/")))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProjectKind {
    GitRemote,
    GitDir,
    Path,
}

impl ProjectKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::GitRemote => "git_remote",
            Self::GitDir => "git_dir",
            Self::Path => "path",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectKey {
    pub kind: ProjectKind,
    pub key: String,
    pub name: String,
    pub remote: Option<String>,
}

impl ProjectKey {
    /// Stable id derived from the unique key.
    pub fn id(&self) -> String {
        format!("pj_{}", &blake3::hash(self.key.as_bytes()).to_hex()[..24])
    }
}

/// The message is fixed: the parser error would echo the URL, which can carry credentials.
#[derive(Debug, thiserror::Error)]
#[error("unparseable git remote")]
pub struct RemoteError;

/// Reduce a git remote URL to `host/owner/repo`: no userinfo, port, `.git` suffix or trailing
/// slash, lower-case host. Accepts `https://`, `ssh://` and scp-style `git@host:owner/repo`.
pub fn normalize_remote(url: &str) -> Result<String, RemoteError> {
    let parsed = gix_url::parse(url.trim()).map_err(|_| RemoteError)?;
    let host = parsed
        .host
        .as_deref()
        .filter(|h| !h.is_empty())
        .ok_or(RemoteError)?;
    let path = String::from_utf8_lossy(&parsed.path);
    let path = path.trim_matches('/');
    let path = path
        .strip_suffix(".git")
        .unwrap_or(path)
        .trim_end_matches('/');
    if path.is_empty() {
        return Err(RemoteError);
    }
    Ok(format!("{}/{path}", host.to_ascii_lowercase()))
}

/// Resolve the project of a session working directory. `native_remote` is a remote already
/// normalized by an adapter (the harness's own record of the repository); it wins over the
/// repository config. Discovery failures yield a `path` project, never an error.
pub fn resolve(cwd: &str, native_remote: Option<&str>) -> ProjectKey {
    resolve_in(cwd, native_remote, home_dir().as_deref())
}

fn resolve_in(cwd: &str, native_remote: Option<&str>, home: Option<&Path>) -> ProjectKey {
    if let Some(remote) = native_remote {
        return remote_project(remote.to_owned());
    }

    let git_dir = Path::new(cwd)
        .is_absolute()
        .then(|| common_git_dir(Path::new(cwd), home))
        .flatten();
    let Some(git_dir) = git_dir else {
        return path_project(cwd);
    };
    match origin_url(&git_dir).and_then(|u| normalize_remote(&u).ok()) {
        Some(remote) => remote_project(remote),
        None => ProjectKey {
            kind: ProjectKind::GitDir,
            name: git_dir_name(&git_dir),
            key: git_dir.to_string_lossy().into_owned(),
            remote: None,
        },
    }
}

fn remote_project(remote: String) -> ProjectKey {
    ProjectKey {
        kind: ProjectKind::GitRemote,
        name: remote.rsplit('/').next().unwrap_or(&remote).to_owned(),
        key: remote.clone(),
        remote: Some(remote),
    }
}

fn path_project(cwd: &str) -> ProjectKey {
    let key = normalize_abs_path(cwd).unwrap_or_else(|| cwd.to_owned());
    let name = key
        .rsplit('/')
        .find(|s| !s.is_empty())
        .unwrap_or(&key)
        .to_owned();
    ProjectKey {
        kind: ProjectKind::Path,
        key,
        name,
        remote: None,
    }
}

fn git_dir_name(git_dir: &Path) -> String {
    let dir = match git_dir.file_name().and_then(|n| n.to_str()) {
        Some(".git") => git_dir.parent(),
        _ => Some(git_dir),
    };
    let name = dir
        .and_then(Path::file_name)
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    name.strip_suffix(".git").map(str::to_owned).unwrap_or(name)
}

fn home_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").and_then(|h| std::fs::canonicalize(h).ok())
}

/// Canonical common git directory: shared by a repository and all its linked worktrees.
/// A repository at `home` (dotfiles) never captures a cwd below it; a cwd that is `home` itself still resolves.
fn common_git_dir(cwd: &Path, home: Option<&Path>) -> Option<PathBuf> {
    let cwd = std::fs::canonicalize(cwd).ok()?;
    let options = gix_discover::upwards::Options {
        ceiling_dirs: home.map(Path::to_path_buf).into_iter().collect(),
        match_ceiling_dir_or_error: false,
        ..Default::default()
    };
    let (repo, _) = gix_discover::upwards_opts(&cwd, options).ok()?;
    let (git_dir, work_dir) = repo.into_repository_and_work_tree_directories();
    let work_dir = work_dir.and_then(|w| std::fs::canonicalize(w).ok());
    if home.is_some() && work_dir.as_deref() == home && Some(cwd.as_path()) != home {
        return None;
    }
    let common = match gix_discover::path::from_plain_file(&git_dir.join("commondir")) {
        Some(Ok(rel)) => git_dir.join(rel),
        Some(Err(_)) => return None,
        None => git_dir,
    };
    std::fs::canonicalize(common).ok()
}

/// Reads only the repository's own config (plus its includes): the global and system config and
/// environment overrides are never loaded, so no user credentials or helpers are involved.
// ponytail: `url.<base>.insteadOf` rewrites are not applied; apply them from the parsed config if a real case needs it.
fn origin_url(common_dir: &Path) -> Option<String> {
    let mut cfg = File::from_path_no_includes(common_dir.join("config"), Source::Local).ok()?;
    let options = init::Options {
        includes: includes::Options::follow(
            Default::default(),
            includes::conditional::Context {
                git_dir: Some(common_dir),
                branch_name: None,
            },
        ),
        ..Default::default()
    };
    let mut with_includes = cfg.clone();
    if with_includes.resolve_includes(options).is_ok() {
        cfg = with_includes;
    }
    cfg.string("remote.origin.url").map(|v| v.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn paths_normalize() {
        assert_eq!(
            normalize_abs_path("/a//b/./c/../d/").as_deref(),
            Some("/a/b/d")
        );
        for (input, expected) in [
            ("/", "/"),
            ("/../../", "/"),
            ("/é/../不存在/", "/不存在"),
            ("/nonexistent/./path", "/nonexistent/path"),
        ] {
            assert_eq!(normalize_abs_path(input).as_deref(), Some(expected));
        }
        assert!(normalize_abs_path("rel/x").is_none());
    }

    fn init_repo(dir: &Path, config: &str) {
        let git = dir.join(".git");
        fs::create_dir_all(git.join("objects")).unwrap();
        fs::create_dir_all(git.join("refs")).unwrap();
        fs::write(git.join("HEAD"), "ref: refs/heads/main\n").unwrap();
        fs::write(git.join("config"), config).unwrap();
    }

    const ORIGIN: &str = "[remote \"origin\"]\n\turl = git@GitHub.com:Acme/widgets.git\n";

    #[test]
    fn remote_forms_normalize_to_host_owner_repo() {
        for url in [
            "https://github.com/Acme/widgets.git",
            "https://github.com/Acme/widgets/",
            "https://user:s3cret-token@github.com/Acme/widgets.git",
            "https://x-access-token:abc@GITHUB.com:8443/Acme/widgets",
            "ssh://git@github.com:22/Acme/widgets.git",
            "git@github.com:Acme/widgets.git",
        ] {
            assert_eq!(
                normalize_remote(url).unwrap(),
                "github.com/Acme/widgets",
                "{url}"
            );
        }
    }

    #[test]
    fn bad_remote_error_never_echoes_input() {
        for url in ["", "/local/path/repo", "https://user:topsecret@/"] {
            let err = normalize_remote(url).unwrap_err().to_string();
            assert_eq!(err, "unparseable git remote");
        }
    }

    #[test]
    fn repo_with_remote_groups_subdirectories() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("widgets");
        fs::create_dir_all(root.join("src/deep")).unwrap();
        init_repo(&root, ORIGIN);

        let p = resolve(root.join("src/deep").to_str().unwrap(), None);
        assert_eq!(p.kind, ProjectKind::GitRemote);
        assert_eq!(p.key, "github.com/Acme/widgets");
        assert_eq!(p.name, "widgets");
        assert_eq!(p.remote.as_deref(), Some("github.com/Acme/widgets"));
    }

    #[test]
    fn repo_without_remote_is_keyed_by_git_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("scratch");
        fs::create_dir_all(&root).unwrap();
        init_repo(&root, "[core]\n\tbare = false\n");

        let p = resolve(root.to_str().unwrap(), None);
        assert_eq!(p.kind, ProjectKind::GitDir);
        assert_eq!(p.name, "scratch");
        assert_eq!(p.remote, None);
        assert_eq!(
            p.key,
            fs::canonicalize(root.join(".git"))
                .unwrap()
                .to_string_lossy()
        );
    }

    #[test]
    fn linked_worktree_shares_the_main_repository_project() {
        let tmp = tempfile::tempdir().unwrap();
        let main = tmp.path().join("main");
        let wt = tmp.path().join("wt");
        fs::create_dir_all(&wt).unwrap();
        init_repo(&main, "");
        let private = main.join(".git/worktrees/wt");
        fs::create_dir_all(&private).unwrap();
        fs::write(private.join("HEAD"), "ref: refs/heads/topic\n").unwrap();
        fs::write(private.join("commondir"), "../..\n").unwrap();
        fs::write(
            private.join("gitdir"),
            format!("{}\n", wt.join(".git").display()),
        )
        .unwrap();
        fs::write(wt.join(".git"), format!("gitdir: {}\n", private.display())).unwrap();

        let a = resolve(main.to_str().unwrap(), None);
        let b = resolve(wt.to_str().unwrap(), None);
        assert_eq!(a.kind, ProjectKind::GitDir);
        assert_eq!(a, b);
    }

    #[test]
    fn config_include_supplies_the_remote() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("app");
        fs::create_dir_all(&root).unwrap();
        init_repo(&root, "[include]\n\tpath = extra.cfg\n");
        fs::write(root.join(".git/extra.cfg"), ORIGIN).unwrap();

        let p = resolve(root.to_str().unwrap(), None);
        assert_eq!(p.key, "github.com/Acme/widgets");
    }

    #[test]
    fn native_remote_wins_over_repository_config() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("widgets");
        fs::create_dir_all(&root).unwrap();
        init_repo(&root, ORIGIN);

        let p = resolve(root.to_str().unwrap(), Some("gitlab.com/o/native"));
        assert_eq!(p.key, "gitlab.com/o/native");
        assert_eq!(p.kind, ProjectKind::GitRemote);
    }

    #[test]
    fn non_git_and_missing_paths_are_path_projects() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("notes");
        fs::create_dir_all(&dir).unwrap();

        for cwd in [
            dir.to_str().unwrap(),
            "/definitely/not/here/",
            "relative/dir",
        ] {
            let p = resolve(cwd, None);
            assert_eq!(p.kind, ProjectKind::Path, "{cwd}");
        }
        let p = resolve("/definitely/not/here/", None);
        assert_eq!(
            (p.key.as_str(), p.name.as_str()),
            ("/definitely/not/here", "here")
        );
    }

    #[test]
    fn repo_at_home_does_not_capture_subdirectories() {
        let tmp = tempfile::tempdir().unwrap();
        let home = fs::canonicalize(tmp.path()).unwrap().join("home");
        let sub = home.join("notes");
        fs::create_dir_all(&sub).unwrap();
        init_repo(&home, ORIGIN);

        let below = resolve_in(sub.to_str().unwrap(), None, Some(&home));
        assert_eq!(below.kind, ProjectKind::Path);
        let at = resolve_in(home.to_str().unwrap(), None, Some(&home));
        assert_eq!(at.kind, ProjectKind::GitRemote);
    }
}
