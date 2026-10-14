//! Durable token budget (spec 12.3). Usage is derived by summing `embedding_requests` rows created in the current
//! budget day, so reservations survive a process kill and nothing is kept only in memory. The day is the calendar
//! day in the operator-configured zone (`embedding.budget_timezone`: `UTC` or a fixed offset such as `+05:30`).

use super::provider::Purpose;
use crate::clock::{SharedClock, Tz};
use crate::storage::index::Index;

const DAY_MS: i64 = 86_400_000;

/// Calendar day `YYYY-MM-DD` of a unix-millisecond timestamp in `tz`.
pub fn budget_day(now_ms: i64, tz: Tz) -> String {
    let z = (now_ms + i64::from(tz.0) * 1000).div_euclid(DAY_MS) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

/// `[start, end)` in unix milliseconds of the budget day that contains `now_ms`.
pub fn day_bounds(now_ms: i64, tz: Tz) -> (i64, i64) {
    let off = i64::from(tz.0) * 1000;
    let start = (now_ms + off).div_euclid(DAY_MS) * DAY_MS - off;
    (start, start + DAY_MS)
}

/// Next local midnight strictly after `now_ms`.
pub fn next_reset_ms(now_ms: i64, tz: Tz) -> i64 {
    day_bounds(now_ms, tz).1
}

#[derive(Debug, Clone)]
pub struct Reservation {
    pub id: String,
    pub tokens: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum Reserve {
    Reserved,
    Exhausted { used: u64, limit: u64 },
}

#[derive(Clone)]
pub struct Ledger {
    index: Index,
    clock: SharedClock,
    tz: Tz,
}

// Completed rows count what the provider reported; failed rows are known-unsent; everything else
// (reserved, dispatched, ambiguous) counts its reservation conservatively. The day is a window over the request's
// creation time, so changing the budget timezone (or restarting) re-derives usage from the persisted rows.
const USED_SQL: &str = "SELECT COALESCE(SUM(CASE state WHEN 'failed' THEN 0 WHEN 'completed' THEN COALESCE(actual_tokens, reserved_tokens) ELSE reserved_tokens END), 0) \
                        FROM embedding_requests WHERE created_at >= ? AND created_at < ? AND purpose = ?";

impl Ledger {
    pub fn new(index: Index, clock: SharedClock, tz: Tz) -> Self {
        Self { index, clock, tz }
    }

    /// Tokens counted against the budget day that contains `now_ms`.
    pub async fn used(&self, now_ms: i64, purpose: Purpose) -> Result<u64, sqlx::Error> {
        let (start, end) = day_bounds(now_ms, self.tz);
        let n: i64 = sqlx::query_scalar(USED_SQL)
            .bind(start)
            .bind(end)
            .bind(purpose.as_str())
            .fetch_one(&self.index.reader)
            .await?;
        Ok(n.max(0) as u64)
    }

    /// Check-and-insert in one write transaction (the writer pool has one connection, so reservations
    /// serialize); the day comes from the injected clock.
    pub async fn reserve(
        &self,
        profile_id: &str,
        purpose: Purpose,
        tokens: u64,
        limit: u64,
    ) -> Result<(Reserve, Option<Reservation>), sqlx::Error> {
        let now = self.clock.now_ms();
        let day = budget_day(now, self.tz);
        let (start, end) = day_bounds(now, self.tz);
        let mut tx = self.index.writer.begin().await?;
        let used: i64 = sqlx::query_scalar(USED_SQL)
            .bind(start)
            .bind(end)
            .bind(purpose.as_str())
            .fetch_one(&mut *tx)
            .await?;
        let used = used.max(0) as u64;
        if used.saturating_add(tokens) > limit {
            tx.rollback().await?;
            return Ok((Reserve::Exhausted { used, limit }, None));
        }
        let id = crate::ids::new_id("er");
        sqlx::query("INSERT INTO embedding_requests(id, profile_id, purpose, budget_day, reserved_tokens, state, created_at) VALUES (?,?,?,?,?, 'reserved', ?)")
            .bind(&id)
            .bind(profile_id)
            .bind(purpose.as_str())
            .bind(&day)
            .bind(tokens as i64)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok((Reserve::Reserved, Some(Reservation { id, tokens })))
    }

    /// reserved -> dispatched. Must commit before the request leaves the process.
    pub async fn dispatched(&self, id: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE embedding_requests SET state='dispatched' WHERE id=? AND state='reserved'",
        )
        .bind(id)
        .execute(&self.index.writer)
        .await?;
        Ok(())
    }

    pub async fn completed(&self, id: &str, actual: Option<u64>) -> Result<(), sqlx::Error> {
        self.finish(id, "completed", actual, None).await
    }
    pub async fn failed(&self, id: &str, category: &str) -> Result<(), sqlx::Error> {
        self.finish(id, "failed", None, Some(category)).await
    }
    pub async fn ambiguous(&self, id: &str, category: &str) -> Result<(), sqlx::Error> {
        self.finish(id, "ambiguous", None, Some(category)).await
    }

    // Terminal states are final: a late guard cleanup never overwrites a recorded outcome.
    async fn finish(
        &self,
        id: &str,
        state: &str,
        actual: Option<u64>,
        category: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE embedding_requests SET state=?, actual_tokens=?, error_category=?, completed_at=? WHERE id=? AND state IN ('reserved','dispatched')")
            .bind(state)
            .bind(actual.map(|a| a as i64))
            .bind(category)
            .bind(self.clock.now_ms())
            .bind(id)
            .execute(&self.index.writer)
            .await?;
        Ok(())
    }

    /// Startup recovery (no request of this process is in flight yet): never-dispatched reservations were
    /// not sent; dispatched ones have an unknown outcome and stay counted.
    pub async fn sweep_interrupted(&self) -> Result<(), sqlx::Error> {
        let now = self.clock.now_ms();
        let mut tx = self.index.writer.begin().await?;
        sqlx::query("UPDATE embedding_requests SET state='failed', error_category='interrupted_before_dispatch', completed_at=? WHERE state='reserved'").bind(now).execute(&mut *tx).await?;
        sqlx::query("UPDATE embedding_requests SET state='ambiguous', error_category='interrupted', completed_at=? WHERE state='dispatched'").bind(now).execute(&mut *tx).await?;
        tx.commit().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::UTC;

    #[test]
    fn days() {
        assert_eq!(budget_day(0, UTC), "1970-01-01");
        assert_eq!(budget_day(DAY_MS - 1, UTC), "1970-01-01");
        assert_eq!(budget_day(DAY_MS, UTC), "1970-01-02");
        assert_eq!(budget_day(1_760_227_200_000, UTC), "2025-10-12");
        assert_eq!(budget_day(1_709_164_800_000, UTC), "2024-02-29");
        assert_eq!(next_reset_ms(DAY_MS - 1, UTC), DAY_MS);
        assert_eq!(next_reset_ms(DAY_MS, UTC), 2 * DAY_MS);
    }

    #[test]
    fn the_budget_day_follows_the_configured_fixed_offset() {
        let ist = Tz::parse("+05:30").unwrap();
        let t = 1_760_227_200_000; // 2025-10-12T00:00:00Z = 05:30 local
        assert_eq!(budget_day(t, UTC), "2025-10-12");
        assert_eq!(budget_day(t, ist), "2025-10-12");
        // 18:29:59.999Z is 23:59:59.999 local; the next millisecond starts the next local day.
        let rollover = t + 18 * 3_600_000 + 29 * 60_000 + 59_999;
        assert_eq!(budget_day(rollover, ist), "2025-10-12");
        assert_eq!(budget_day(rollover + 1, ist), "2025-10-13");
        assert_eq!(
            budget_day(rollover + 1, UTC),
            "2025-10-12",
            "UTC is still the 12th"
        );
        assert_eq!(next_reset_ms(rollover, ist), rollover + 1);
        assert_eq!(
            day_bounds(t, ist),
            (
                t - 5 * 3_600_000 - 30 * 60_000,
                t - 5 * 3_600_000 - 30 * 60_000 + DAY_MS
            )
        );
        let west = Tz::parse("-08:00").unwrap();
        assert_eq!(budget_day(t, west), "2025-10-11");
        assert_eq!(next_reset_ms(t, west), t + 8 * 3_600_000);
    }
}
