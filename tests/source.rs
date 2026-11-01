use spool::source::SourceRoots;
#[cfg(unix)]
use std::os::unix::{
    fs::{PermissionsExt, symlink},
    net::UnixListener,
};
use std::{fs, path::PathBuf, sync::atomic::AtomicBool};

fn roots(path: &std::path::Path) -> SourceRoots {
    SourceRoots::new(&[path.to_owned()]).unwrap()
}
fn collect_pages(roots: &SourceRoots, limit: usize) -> Vec<PathBuf> {
    let cancel = AtomicBool::new(false);
    let mut after = None;
    let mut paths = Vec::new();
    loop {
        let page = roots.scan_from(&cancel, limit, after.as_deref());
        assert!(page.errors.is_empty());
        paths.extend(page.files.into_iter().map(|file| file.path));
        if page.complete {
            assert!(page.next_after.is_none());
            break;
        }
        let next = page
            .next_after
            .expect("incomplete page must have a resumable cursor");
        assert_eq!(paths.last(), Some(&next));
        after = Some(next);
    }
    paths
}

#[cfg(unix)]
#[test]
fn pagination_covers_large_flat_and_nested_directories_once() {
    let dir = tempfile::tempdir().unwrap();
    for i in 0..7 {
        fs::write(dir.path().join(format!("root-{i:02}.jsonl")), b"x").unwrap();
    }
    for group in ["a", "z/nested"] {
        let nested = dir.path().join(group);
        fs::create_dir_all(&nested).unwrap();
        for i in 0..4 {
            fs::write(nested.join(format!("child-{i:02}.jsonl")), b"x").unwrap();
        }
    }
    let roots = roots(dir.path());
    let paths = collect_pages(&roots, 3);
    let canonical_root = fs::canonicalize(dir.path()).unwrap();
    let mut expected: Vec<_> = fs::read_dir(&canonical_root)
        .unwrap()
        .flat_map(|entry| {
            let path = entry.unwrap().path();
            if path.is_dir() {
                let mut walk = Vec::new();
                collect_files(&path, &mut walk);
                walk
            } else {
                vec![path]
            }
        })
        .collect();
    expected.sort();
    assert_eq!(paths, expected);
    assert_eq!(paths.len(), 15);
}

#[cfg(unix)]
#[test]
fn cursor_survives_removals_and_additions_without_repeating_paths() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["a", "b", "c", "d", "e"] {
        fs::write(dir.path().join(format!("{name}.jsonl")), b"x").unwrap();
    }
    let roots = roots(dir.path());
    let cancel = AtomicBool::new(false);
    let first = roots.scan_from(&cancel, 2, None);
    assert!(!first.complete);
    assert_eq!(first.files.len(), 2);
    let after = first.next_after.unwrap();
    fs::remove_file(dir.path().join("a.jsonl")).unwrap();
    fs::remove_file(dir.path().join("d.jsonl")).unwrap();
    fs::write(dir.path().join("cc.jsonl"), b"x").unwrap();
    let paths = collect_pages_from(&roots, 2, Some(after));
    let names: Vec<_> = paths
        .iter()
        .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, ["c.jsonl", "cc.jsonl", "e.jsonl"]);
}

#[cfg(unix)]
#[test]
fn inaccessible_file_makes_scan_incomplete() {
    if rustix::process::geteuid().as_raw() == 0 {
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let blocked = dir.path().join("blocked.jsonl");
    fs::write(&blocked, b"x").unwrap();
    fs::set_permissions(&blocked, fs::Permissions::from_mode(0o000)).unwrap();
    let page = roots(dir.path()).scan(&AtomicBool::new(false), 10);
    assert!(!page.complete);
    assert!(
        page.errors
            .iter()
            .any(|error| error.kind == std::io::ErrorKind::PermissionDenied)
    );
}

#[cfg(unix)]
#[test]
fn symlink_and_replaced_parent_never_escape_root() {
    let temp = tempfile::tempdir().unwrap();
    let root_path = temp.path().join("root");
    let outside = temp.path().join("outside");
    fs::create_dir_all(root_path.join("nested")).unwrap();
    fs::create_dir_all(&outside).unwrap();
    fs::write(outside.join("secret.jsonl"), b"outside").unwrap();
    fs::write(root_path.join("nested/inside.jsonl"), b"inside").unwrap();
    let roots = roots(&root_path);
    let canonical_root = fs::canonicalize(&root_path).unwrap();

    symlink(
        outside.join("secret.jsonl"),
        canonical_root.join("linked-file.jsonl"),
    )
    .unwrap();
    let first = roots.scan(&AtomicBool::new(false), 20);
    assert!(!first.files.iter().any(|file| {
        file.path
            .file_name()
            .is_some_and(|name| name == "linked-file.jsonl")
    }));
    assert!(
        roots
            .open(&canonical_root.join("linked-file.jsonl"))
            .is_err()
    );

    fs::rename(canonical_root.join("nested"), canonical_root.join("held")).unwrap();
    symlink(&outside, canonical_root.join("nested")).unwrap();
    let replaced = roots.scan(&AtomicBool::new(false), 20);
    assert!(
        !replaced
            .files
            .iter()
            .any(|file| file.path.ends_with("nested/secret.jsonl"))
    );
    assert!(
        roots
            .open(&canonical_root.join("nested/secret.jsonl"))
            .is_err()
    );
    assert!(
        replaced
            .files
            .iter()
            .any(|file| file.path.ends_with("held/inside.jsonl"))
    );
}

#[cfg(unix)]
#[test]
fn non_regular_socket_entry_is_not_scanned_or_opened_as_a_source() {
    let dir = tempfile::tempdir().unwrap();
    let socket_path = dir.path().join("socket");
    let _listener = UnixListener::bind(&socket_path).unwrap();
    let roots = roots(dir.path());
    let canonical_root = fs::canonicalize(dir.path()).unwrap();
    let page = roots.scan(&AtomicBool::new(false), 20);
    assert!(page.files.is_empty());
    assert!(roots.open(&canonical_root.join("socket")).is_err());
}

#[cfg(unix)]
#[test]
fn scan_yields_only_jsonl_transcripts_and_ignores_unreadable_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let session = dir.path().join("proj/s1");
    fs::create_dir_all(session.join("tool-results")).unwrap();
    for path in [
        "proj/s1.jsonl",
        "proj/s1/Agent.jsonl",
        "proj/s1/1.bash.log",
        "proj/s1/notes.md",
        "proj/s1/tool-results/a.txt",
        "proj/s1/locked.log",
    ] {
        fs::write(dir.path().join(path), b"x").unwrap();
    }
    if rustix::process::geteuid().as_raw() != 0 {
        fs::set_permissions(
            session.join("locked.log"),
            fs::Permissions::from_mode(0o000),
        )
        .unwrap();
    }
    let roots = roots(dir.path());
    let canonical_root = fs::canonicalize(dir.path()).unwrap();
    assert_eq!(
        collect_pages(&roots, 1),
        [
            canonical_root.join("proj/s1/Agent.jsonl"),
            canonical_root.join("proj/s1.jsonl")
        ]
    );
    let page = roots.scan(&AtomicBool::new(false), 10);
    assert!(page.complete);
    assert!(page.errors.is_empty());
}

#[cfg(unix)]
#[test]
fn character_device_cannot_be_configured_as_a_source_root() {
    assert!(SourceRoots::new(&[PathBuf::from("/dev/null")]).is_err());
}

#[cfg(unix)]
fn collect_pages_from(
    roots: &SourceRoots,
    limit: usize,
    mut after: Option<PathBuf>,
) -> Vec<PathBuf> {
    let cancel = AtomicBool::new(false);
    let mut paths = Vec::new();
    loop {
        let page = roots.scan_from(&cancel, limit, after.as_deref());
        assert!(page.errors.is_empty());
        paths.extend(page.files.into_iter().map(|file| file.path));
        if page.complete {
            assert!(page.next_after.is_none());
            break;
        }
        after = page.next_after;
        assert!(after.is_some());
    }
    paths
}

#[cfg(unix)]
fn collect_files(dir: &std::path::Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_files(&path, files);
        } else {
            files.push(path);
        }
    }
}

#[cfg(windows)]
#[test]
fn windows_scan_open_rejects_escapes_and_keeps_file_identity() {
    use std::io::Read;
    use std::process::Command;

    let temp = tempfile::tempdir().unwrap();
    let root_path = temp.path().join("root");
    let outside = temp.path().join("outside");
    fs::create_dir_all(root_path.join("nested")).unwrap();
    fs::create_dir_all(&outside).unwrap();
    fs::write(root_path.join("a.jsonl"), b"one").unwrap();
    fs::write(root_path.join("nested/b.jsonl"), b"two").unwrap();
    fs::write(outside.join("secret.jsonl"), b"outside").unwrap();

    let roots = roots(&root_path);
    let canonical_root = fs::canonicalize(&root_path).unwrap();
    let first = roots.scan_from(&AtomicBool::new(false), 1, None);
    assert!(first.errors.is_empty(), "{:#?}", first.errors);
    assert!(first.truncated);
    assert_eq!(first.files.len(), 1);
    assert_eq!(
        first.next_after.as_ref(),
        first.files.first().map(|file| &file.path)
    );

    let cancelled = AtomicBool::new(true);
    let cancelled = roots.scan(&cancelled, 10);
    assert!(!cancelled.complete);
    assert!(cancelled.files.is_empty());

    let paths = collect_pages(&roots, 1);
    assert_eq!(paths.len(), 2);
    let a = canonical_root.join("a.jsonl");
    let info = roots
        .scan(&AtomicBool::new(false), 10)
        .files
        .into_iter()
        .find(|file| file.path == a)
        .unwrap();
    let mut body = String::new();
    let mut opened = roots.open(&a).unwrap();
    opened.file.read_to_string(&mut body).unwrap();
    assert_eq!(body, "one");
    fs::rename(&a, canonical_root.join("renamed.jsonl")).unwrap();
    let renamed = roots
        .scan(&AtomicBool::new(false), 10)
        .files
        .into_iter()
        .find(|file| file.path.ends_with("renamed.jsonl"))
        .unwrap();
    assert_eq!((renamed.dev, renamed.ino), (info.dev, info.ino));

    let junction = root_path.join("outside");
    assert!(
        Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&junction)
            .arg(&outside)
            .status()
            .unwrap()
            .success()
    );
    let page = roots.scan(&AtomicBool::new(false), 10);
    assert!(
        !page
            .files
            .iter()
            .any(|file| file.path.ends_with("outside/secret.jsonl"))
    );
    assert!(
        roots
            .open(&canonical_root.join("outside/secret.jsonl"))
            .is_err()
    );

    let windir = PathBuf::from(std::env::var_os("WINDIR").unwrap());
    assert!(SourceRoots::new(&[windir]).is_err());
}
