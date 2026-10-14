//! Injectable clock (budget reset, lease expiry and discovery tests need deterministic time).

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

pub trait Clock: Send + Sync {
    /// Unix milliseconds, UTC.
    fn now_ms(&self) -> i64;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
    }
}

/// Manually advanced clock for tests.
#[derive(Debug, Clone)]
pub struct ManualClock(Arc<AtomicI64>);

impl ManualClock {
    pub fn new(start_ms: i64) -> Self {
        Self(Arc::new(AtomicI64::new(start_ms)))
    }
    pub fn advance_ms(&self, d: i64) {
        self.0.fetch_add(d, Ordering::SeqCst);
    }
    pub fn set_ms(&self, v: i64) {
        self.0.store(v, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

pub type SharedClock = Arc<dyn Clock>;

pub fn system() -> SharedClock {
    Arc::new(SystemClock)
}

/// Fixed UTC offset in seconds: the only zones Spool supports are `UTC` and fixed offsets (`+02:00`, `-0530`).
/// Named zones with daylight-saving rules would need a tz database dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tz(pub i32);

pub const UTC: Tz = Tz(0);

impl Tz {
    pub fn parse(s: &str) -> Result<Tz, String> {
        let t = s.trim();
        if t.eq_ignore_ascii_case("utc") || t.eq_ignore_ascii_case("z") {
            return Ok(UTC);
        }
        let bad = || format!("invalid timezone {s:?}: use UTC or a fixed offset such as +02:00");
        let (sign, rest) = match t.as_bytes().first() {
            Some(b'+') => (1, &t[1..]),
            Some(b'-') => (-1, &t[1..]),
            _ => return Err(bad()),
        };
        let digits: String = rest.chars().filter(|c| *c != ':').collect();
        if !digits.chars().all(|c| c.is_ascii_digit()) {
            return Err(bad());
        }
        let (h, m) = match digits.len() {
            1 | 2 => (digits.parse::<i32>().map_err(|_| bad())?, 0),
            4 => (
                digits[..2].parse::<i32>().map_err(|_| bad())?,
                digits[2..].parse::<i32>().map_err(|_| bad())?,
            ),
            _ => return Err(bad()),
        };
        if h > 14 || m > 59 {
            return Err(bad());
        }
        Ok(Tz(sign * (h * 3600 + m * 60)))
    }

    pub fn label(self) -> String {
        if self.0 == 0 {
            return "UTC".into();
        }
        let a = self.0.abs();
        format!(
            "{}{:02}:{:02}",
            if self.0 < 0 { '-' } else { '+' },
            a / 3600,
            a % 3600 / 60
        )
    }
}
