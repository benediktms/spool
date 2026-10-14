//! CLI-side date handling (spec 16.2): relative and civil dates are resolved here, against an
//! explicit timezone and an explicit "now", into absolute instants. The MCP API only ever sees
//! absolute timestamps.
//!
//! Timezones are `crate::clock::Tz`: `UTC`/`Z` or fixed offsets.

use crate::clock::Tz;

// Howard Hinnant's civil-date algorithms (public domain).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 => 29,
        _ => 28,
    }
}

/// RFC 3339 with seconds precision, rendered in `tz` (`Z` for UTC).
pub fn format_rfc3339(ms: i64, tz: Tz) -> String {
    let secs = ms.div_euclid(1000) + tz.0 as i64;
    let (y, m, d) = civil_from_days(secs.div_euclid(86_400));
    let s = secs.rem_euclid(86_400);
    let off = if tz.0 == 0 {
        "Z".to_string()
    } else {
        tz.label()
    };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}{off}",
        s / 3600,
        s % 3600 / 60,
        s % 60
    )
}

fn num(s: &str) -> Option<i64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        None
    } else {
        s.parse().ok()
    }
}

/// Parse a civil date/time: `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM[:SS]` (in `tz`), or either with `Z`/`±HH:MM`.
fn parse_civil(input: &str, tz: Tz) -> Option<i64> {
    let (date, time) = match input.split_once(['T', 't', ' ']) {
        Some((d, t)) => (d, Some(t)),
        None => (input, None),
    };
    let mut dp = date.split('-');
    let (y, m, d) = (num(dp.next()?)?, num(dp.next()?)?, num(dp.next()?)?);
    if dp.next().is_some()
        || date.len() != 10
        || !(1..=12).contains(&m)
        || d < 1
        || d > days_in_month(y, m)
    {
        return None;
    }
    let mut offset = tz.0 as i64;
    let mut secs_of_day = 0;
    if let Some(t) = time {
        let (clock, zone) = match t.find(['Z', 'z', '+', '-']) {
            Some(i) => (&t[..i], Some(&t[i..])),
            None => (t, None),
        };
        if let Some(z) = zone {
            offset = Tz::parse(z).ok()?.0 as i64;
        }
        let mut cp = clock.split(':');
        let (hh, mm) = (num(cp.next()?)?, num(cp.next()?)?);
        let ss = match cp.next() {
            Some(s) => num(s)?,
            None => 0,
        };
        if cp.next().is_some() || hh > 23 || mm > 59 || ss > 59 {
            return None;
        }
        secs_of_day = hh * 3600 + mm * 60 + ss;
    }
    Some((days_from_civil(y, m, d) * 86_400 + secs_of_day - offset) * 1000)
}

/// `7d`, `12h`, `30m`, `45s`, `2w` (relative to `now_ms`, in the past) or a civil date/time.
pub fn parse_when(input: &str, now_ms: i64, tz: Tz) -> Result<i64, String> {
    let t = input.trim();
    if let Some(unit) = t
        .chars()
        .last()
        .filter(|c| matches!(c, 's' | 'm' | 'h' | 'd' | 'w'))
    {
        if let Some(n) = num(&t[..t.len() - 1]) {
            let per = match unit {
                's' => 1,
                'm' => 60,
                'h' => 3600,
                'd' => 86_400,
                _ => 604_800,
            };
            return n
                .checked_mul(per * 1000)
                .and_then(|d| now_ms.checked_sub(d))
                .ok_or_else(|| format!("relative date {input:?} is out of range"));
        }
    }
    parse_civil(t, tz).ok_or_else(|| {
        format!("invalid date {input:?}: use 7d/12h/30m/2w or YYYY-MM-DD[THH:MM[:SS]][Z|+HH:MM]")
    })
}
