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

pub(crate) fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

pub(crate) fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

pub(crate) fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        _ => 28,
    }
}

/// Absolute RFC 3339 timestamp with the `Z` (UTC) designator, optional fraction -> unix milliseconds.
pub fn parse_rfc3339_utc(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 20
        || !s.is_ascii()
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || *b.last()? != b'Z'
    {
        return None;
    }
    let num = |r: std::ops::Range<usize>| -> Option<i64> {
        let t = &s[r];
        t.bytes()
            .all(|c| c.is_ascii_digit())
            .then(|| t.parse().ok())
            .flatten()
    };
    let (y, mo, d, h, mi, sec) = (
        num(0..4)?,
        num(5..7)?,
        num(8..10)?,
        num(11..13)?,
        num(14..16)?,
        num(17..19)?,
    );
    if !(1..=12).contains(&mo)
        || d < 1
        || d > days_in_month(y, mo)
        || h > 23
        || mi > 59
        || sec > 59
        || y < 1
    {
        return None;
    }
    let mut ms = 0i64;
    let rest = &s[19..s.len() - 1];
    if !rest.is_empty() {
        let frac = rest.strip_prefix('.')?;
        if frac.is_empty() || !frac.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        let mut f = frac.to_string();
        f.truncate(3);
        while f.len() < 3 {
            f.push('0');
        }
        ms = f.parse().ok()?;
    }
    Some((days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + sec) * 1000 + ms)
}

/// Unix milliseconds -> `YYYY-MM-DDTHH:MM:SSZ` (`.mmm` only when non-zero).
pub fn format_rfc3339(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let frac = ms.rem_euclid(1000);
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let t = secs.rem_euclid(86_400);
    let base = format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}",
        t / 3600,
        t % 3600 / 60,
        t % 60
    );
    if frac == 0 {
        format!("{base}Z")
    } else {
        format!("{base}.{frac:03}Z")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_roundtrip_and_rejects() {
        let ms = parse_rfc3339_utc("2026-10-12T12:25:00Z").unwrap();
        assert_eq!(format_rfc3339(ms), "2026-10-12T12:25:00Z");
        assert_eq!(parse_rfc3339_utc("1970-01-01T00:00:00.5Z"), Some(500));
        assert_eq!(format_rfc3339(500), "1970-01-01T00:00:00.500Z");
        assert_eq!(
            parse_rfc3339_utc("2024-02-29T00:00:00Z")
                .map(format_rfc3339)
                .as_deref(),
            Some("2024-02-29T00:00:00Z")
        );
        for bad in [
            "2023-02-29T00:00:00Z",
            "yesterday",
            "2026-10-12",
            "2026-10-12T12:25:00",
            "2026-10-12T12:25:00+02:00",
            "2026-13-01T00:00:00Z",
        ] {
            assert!(parse_rfc3339_utc(bad).is_none(), "{bad}");
        }
    }
    #[test]
    fn timestamp_boundaries_and_parser_contract() {
        for (ms, expected) in [
            (i64::MIN, "-292275055-05-16T16:47:04.192Z"),
            (-86_400_001, "1969-12-30T23:59:59.999Z"),
            (-1001, "1969-12-31T23:59:58.999Z"),
            (-1000, "1969-12-31T23:59:59Z"),
            (-999, "1969-12-31T23:59:59.001Z"),
            (-1, "1969-12-31T23:59:59.999Z"),
            (0, "1970-01-01T00:00:00Z"),
            (86_400_000, "1970-01-02T00:00:00Z"),
            (i64::MAX, "292278994-08-17T07:12:55.807Z"),
        ] {
            assert_eq!(format_rfc3339(ms), expected);
        }
        for (s, ms) in [
            ("1970-01-01T00:00:00.1Z", 100),
            ("1970-01-01T00:00:00.12Z", 120),
            ("1970-01-01T00:00:00.123Z", 123),
            ("1970-01-01T00:00:00.1239Z", 123),
            ("1970-01-01T00:00:00.0009Z", 0),
        ] {
            assert_eq!(parse_rfc3339_utc(s), Some(ms));
        }
        for s in [
            "0001-01-01T00:00:00Z",
            "9999-12-31T23:59:59Z",
            "1900-02-28T23:59:59Z",
            "2000-02-29T00:00:00Z",
            "2024-02-29T00:00:00Z",
            "2100-03-01T00:00:00Z",
            "1999-12-31T23:59:59Z",
        ] {
            assert_eq!(parse_rfc3339_utc(s).map(format_rfc3339).as_deref(), Some(s));
        }
        for s in [
            "0000-01-01T00:00:00Z",
            "1900-02-29T00:00:00Z",
            "2100-02-29T00:00:00Z",
            "1970-01-01t00:00:00Z",
            "1970-01-01T00:00:00z",
            "1970-01-01T00:00:00+00:00",
            " 1970-01-01T00:00:00Z",
            "1970-01-01T00:00:00.Z",
            "1970-01-01T00:00:00.aZ",
            "é970-01-01T00:00:00Z",
        ] {
            assert_eq!(parse_rfc3339_utc(s), None, "{s}");
        }
    }
}
