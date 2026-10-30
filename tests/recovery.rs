//! Milestone 0: a durable Apalis SQLite job survives a force-killed worker and is recovered.
//! The worker runs in a child process (this test binary re-executed) so a real process kill is possible.

mod jobs_common;

use apalis::prelude::*;
use jobs_common::wait_until;
use serde::{Deserialize, Serialize};
use spool::jobs::backend::{self, Liveness};
use spool::storage;
use sqlx::SqlitePool;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpikeJob {
    payload_version: u32,
    marker: String,
}

const QUEUE: &str = "spike";
const LIVENESS: Liveness = Liveness {
    heartbeat_interval: Duration::from_secs(1),
    missed_heartbeats: 2,
    poll_interval: Duration::from_millis(200),
};

async fn open(dir: &Path) -> SqlitePool {
    let pool = storage::connect(&dir.join("jobs.sqlite"), 4, Duration::from_secs(5))
        .await
        .unwrap();
    backend::setup(&pool).await.unwrap();
    pool
}

/// Child entry: run a worker whose handler records its attempt and hangs on attempt 1.
#[tokio::test]
async fn child_worker() {
    let Ok(dir) = std::env::var("SPOOL_RECOVERY_DIR") else {
        return;
    };
    let hang_first = std::env::var("SPOOL_RECOVERY_HANG").is_ok();
    let dir = PathBuf::from(dir);
    let pool = open(&dir).await;
    let d = dir.clone();
    let handler = move |job: SpikeJob, attempt: Attempt, wrk: WorkerContext| {
        let d = d.clone();
        async move {
            let line = format!(
                "{} attempt={} pid={}\n",
                job.marker,
                attempt.current(),
                std::process::id()
            );
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(d.join("runs.log"))
                .unwrap()
                .write_line(&line);
            if hang_first {
                // Signal the parent we hold the job, then block forever until killed.
                std::fs::write(d.join("claimed"), b"1").unwrap();
                futures::future::pending::<()>().await;
            }
            std::fs::write(d.join("done"), b"1").unwrap();
            wrk.stop().ok();
            Ok::<(), BoxDynError>(())
        }
    };
    // Supervisor behaviour: the worker keeps one name across incarnations. The pinned backend
    // refuses registration (`WORKER_ALREADY_EXISTS`) until the dead worker's last_seen is older
    // than the heartbeat interval; the supervisor retries. Registration then re-enqueues the
    // dead incarnation's orphans.
    loop {
        let storage = backend::storage::<SpikeJob>(&pool, QUEUE, LIVENESS)
            .poll_with_interval(LIVENESS.poll_interval);
        match WorkerBuilder::new(
            std::env::var("SPOOL_RECOVERY_NAME").unwrap_or_else(|_| "spool-ingest".into()),
        )
        .backend(storage)
        .build(handler.clone())
        .run()
        .await
        {
            Ok(_) => break,
            Err(e) if e.to_string().contains("WORKER_ALREADY_EXISTS") => {
                eprintln!("register refused, retrying: {e}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => panic!("worker failed: {e}"),
        }
    }
}

trait WriteLine {
    fn write_line(&mut self, s: &str);
}
impl WriteLine for std::fs::File {
    fn write_line(&mut self, s: &str) {
        use std::io::Write;
        self.write_all(s.as_bytes()).unwrap();
        self.sync_all().unwrap();
    }
}

fn spawn_child(dir: &Path, hang: bool, name: &str) -> std::process::Child {
    let log =
        std::fs::File::create(dir.join(if hang { "child1.log" } else { "child2.log" })).unwrap();
    let mut c = Command::new(std::env::current_exe().unwrap());
    c.args(["child_worker", "--exact", "--nocapture", "--test-threads=1"])
        .env("SPOOL_RECOVERY_DIR", dir)
        .env("RUST_LOG", "debug")
        .env("SPOOL_RECOVERY_NAME", name)
        .stdout(Stdio::null())
        .stderr(log);
    if hang {
        c.env("SPOOL_RECOVERY_HANG", "1");
    }
    c.spawn().unwrap()
}

fn wait_for(path: &Path, timeout: Duration) -> bool {
    let t = Instant::now();
    while t.elapsed() < timeout {
        if path.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

#[tokio::test]
async fn killed_worker_job_is_recovered() {
    kill_and_recover("spool-ingest").await;
}

/// A differently named replacement worker recovers the orphan through the backend heartbeat
/// (`heartbeat_interval * missed_heartbeats` after the dead worker's last_seen).
#[tokio::test]
async fn killed_worker_job_is_recovered_by_other_worker_heartbeat() {
    kill_and_recover("spool-ingest-2").await;
}

async fn kill_and_recover(second_name: &str) {
    if std::env::var("SPOOL_RECOVERY_DIR").is_ok() {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path();
    let pool = open(dir).await;

    // Durable enqueue before any worker exists.
    let mut sink = backend::storage::<SpikeJob>(&pool, QUEUE, LIVENESS);
    sink.push(SpikeJob {
        payload_version: 1,
        marker: "job-1".into(),
    })
    .await
    .unwrap();

    let mut c1 = spawn_child(dir, true, "spool-ingest");
    assert!(
        wait_for(&dir.join("claimed"), Duration::from_secs(60)),
        "worker never claimed job"
    );
    wait_until(60, "job to become Running", || async {
        let status: String = sqlx::query_scalar("SELECT status FROM Jobs")
            .fetch_one(&pool)
            .await
            .unwrap();
        status == "Running"
    })
    .await;
    c1.kill().unwrap(); // force-terminate the worker without graceful cleanup
    c1.wait().unwrap();
    let killed_at = Instant::now();
    let status: String = sqlx::query_scalar("SELECT status FROM Jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "Running", "job should be stuck Running after kill");

    // Incarnation 2 (same worker name) must recover and complete it.
    let mut c2 = spawn_child(dir, false, second_name);
    let ok = wait_for(&dir.join("done"), Duration::from_secs(30));
    if !ok {
        type JobRow = (String, String, i64, Option<String>, Option<String>);
        let rows: Vec<JobRow> =
            sqlx::query_as("SELECT id, status, attempts, lock_by, last_result FROM Jobs")
                .fetch_all(&pool)
                .await
                .unwrap();
        let workers: Vec<(String, i64)> = sqlx::query_as("SELECT id, last_seen FROM Workers")
            .fetch_all(&pool)
            .await
            .unwrap();
        let now: i64 = sqlx::query_scalar("SELECT CAST(strftime('%s','now') AS INTEGER)")
            .fetch_one(&pool)
            .await
            .unwrap();
        eprintln!(
            "jobs={rows:?}\nworkers={workers:?} now={now}\nchild2={}",
            std::fs::read_to_string(dir.join("child2.log")).unwrap_or_default()
        );
    }
    assert!(ok, "job not recovered after kill");
    let recovered_in = killed_at.elapsed();
    let _ = c2.wait();

    let (status, attempts): (String, i64) = sqlx::query_as("SELECT status, attempts FROM Jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    let log = std::fs::read_to_string(dir.join("runs.log")).unwrap();
    eprintln!(
        "RECOVERY second_worker={second_name} recovered_in={recovered_in:?} status={status} attempts={attempts}\n{log}"
    );
    assert_eq!(status, "Done");
    assert_eq!(
        log.lines().count(),
        2,
        "job should have executed exactly twice (kill + recovery): {log}"
    );
}
