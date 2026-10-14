//! Apalis SQLite backend wiring (spec 11). Only this module touches Apalis types.

use apalis_sqlite::{Config, SqliteStorage};
use sqlx::SqlitePool;
use std::time::Duration;

/// Worker liveness settings recorded in docs/dependency-baseline.md.
#[derive(Debug, Clone, Copy)]
pub struct Liveness {
    pub heartbeat_interval: Duration,
    pub missed_heartbeats: usize,
    pub poll_interval: Duration,
}

impl Default for Liveness {
    fn default() -> Self {
        // 30s * 2 = 60s orphan detection for another dead worker; a restarted worker with the
        // same name re-enqueues its own orphans at registration.
        Self {
            heartbeat_interval: Duration::from_secs(30),
            missed_heartbeats: 2,
            poll_interval: Duration::from_millis(500),
        }
    }
}

/// Apply the pinned backend's migrations only. `SqliteStorage::setup` is deliberately NOT used:
/// it also issues `PRAGMA synchronous = OFF`, contradicting the spec's FULL durability setting.
pub async fn setup(pool: &SqlitePool) -> Result<(), sqlx::migrate::MigrateError> {
    SqliteStorage::<()>::migrations().run(pool).await
}

pub fn storage<T>(pool: &SqlitePool, queue: &str, l: Liveness) -> SqliteStorage<T> {
    SqliteStorage::new(pool).with_config(
        Config::default()
            .queue(queue)
            .heartbeat_interval(l.heartbeat_interval)
            .missed_heartbeats(l.missed_heartbeats)
            .batch_size(1),
    )
}
