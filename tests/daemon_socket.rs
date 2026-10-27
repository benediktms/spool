//! Unix socket-path safety and Windows duplicate-daemon rejection.

use spool::config::{Config, Paths};
use spool::daemon::Daemon;
use spool::transcript::handle::HandleKey;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::net::UnixListener as StdListener;

async fn daemon_in(root: &std::path::Path, socket: &str) -> anyhow::Result<Daemon> {
    spool::daemon::ensure_private_dir(root)?;
    let paths = Paths::from_root(root.to_path_buf())?;
    spool::daemon::ensure_private_dir(&paths.secrets_dir)?;
    if !paths.signing_key.exists() {
        HandleKey::create(&paths.signing_key)?;
    }
    let mut cfg = Config::default();
    cfg.ipc.socket_path = socket.to_string();
    Daemon::open(paths, cfg).await
}

fn root() -> tempfile::TempDir {
    tempfile::Builder::new().prefix("sp").tempdir().unwrap()
}
fn private_root(t: &tempfile::TempDir) -> std::path::PathBuf {
    let root = t.path().join("h");
    spool::daemon::ensure_private_dir(&root).unwrap();
    root
}

#[cfg(unix)]
#[tokio::test]
async fn stale_owned_socket_is_replaced_and_new_socket_is_0600() {
    let t = root();
    let root = private_root(&t);
    std::fs::create_dir_all(root.join("state")).unwrap();
    let sock = root.join("state/spool.sock");
    drop(StdListener::bind(&sock).unwrap()); // listener gone, socket file stays: stale
    assert!(sock.exists());
    let d = daemon_in(&root, "state/spool.sock").await.unwrap();
    let _l = d.bind().unwrap();
    assert_eq!(
        std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[cfg(unix)]
#[tokio::test]
async fn regular_file_directory_and_symlink_at_socket_path_are_never_removed() {
    let t = root();
    let root = private_root(&t);
    std::fs::create_dir_all(root.join("state")).unwrap();
    let victim = root.join("state/victim.txt");
    std::fs::write(&victim, b"precious").unwrap();

    // regular file
    let sock = root.join("state/spool.sock");
    std::fs::write(&sock, b"not a socket").unwrap();
    let d = daemon_in(&root, "state/spool.sock").await.unwrap();
    assert!(d.bind().is_err());
    assert_eq!(std::fs::read(&sock).unwrap(), b"not a socket");
    drop(d);
    std::fs::remove_file(&sock).unwrap();

    // symlink to a file
    std::os::unix::fs::symlink(&victim, &sock).unwrap();
    let d = daemon_in(&root, "state/spool.sock").await.unwrap();
    assert!(d.bind().is_err());
    assert_eq!(std::fs::read(&victim).unwrap(), b"precious");
    assert!(
        std::fs::symlink_metadata(&sock)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    drop(d);
    std::fs::remove_file(&sock).unwrap();

    // a live listener is never bound over
    let _live = StdListener::bind(&sock).unwrap();
    let d = daemon_in(&root, "state/spool.sock").await.unwrap();
    assert!(d.bind().is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn reserved_paths_and_unsafe_parents_are_rejected() {
    let t = root();
    let root = private_root(&t);
    std::fs::create_dir_all(&root).unwrap();
    // socket path pointing at the signing key
    assert!(
        daemon_in(&root, "secrets/handle-signing.key")
            .await
            .unwrap()
            .bind()
            .is_err()
    );
    // symlinked parent directory is refused (not followed, not chmodded)
    let real = t.path().join("elsewhere");
    std::fs::create_dir_all(&real).unwrap();
    std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::os::unix::fs::symlink(&real, root.join("linkdir")).unwrap();
    assert!(
        daemon_in(&root, "linkdir/s.sock")
            .await
            .unwrap()
            .bind()
            .is_err()
    );
    assert_eq!(
        std::fs::metadata(&real).unwrap().permissions().mode() & 0o777,
        0o755,
        "target of a symlink must not be chmodded"
    );
}

#[cfg(windows)]
#[tokio::test]
async fn duplicate_daemon_is_rejected_after_binding_the_private_pipe() {
    let t = root();
    let root = private_root(&t);
    let first = daemon_in(&root, "state/spool.sock").await.unwrap();
    let _listener = first.bind().unwrap();
    match daemon_in(&root, "state/spool.sock").await {
        Ok(_) => panic!("second daemon acquired the state lock"),
        Err(error) => assert!(error.to_string().contains("another spool daemon")),
    }
}
