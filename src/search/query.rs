//! Pure query-side helpers: RFC 3339 UTC parsing/formatting, safe FTS5 query construction, excerpt windows.

/// Characters the FTS5 `unicode61` tokenizer treats as token characters (letters, digits, combining marks).
fn is_tok(c: char) -> bool {
    c.is_alphanumeric() || ('\u{300}'..='\u{36F}').contains(&c)
}

fn tokens(s: &str) -> Vec<String> {
    s.split(|c| !is_tok(c))
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .collect()
}

/// Safe FTS5 MATCH expression built from user text: free words and `"quoted phrases"` become quoted FTS strings
/// (tokens are alphanumeric only, so no FTS syntax can leak), OR-combined, deduplicated in first-seen order.
/// A word that tokenizes into several tokens (`parse_snapshot`, `src/a.rs`) becomes one phrase so identifiers
/// are matched as a unit. Returns `None` when the text has no tokens at all. Second value: flat lowercase terms.
pub fn fts_query(text: &str) -> Option<(String, Vec<String>)> {
    let mut phrases: Vec<Vec<String>> = Vec::new();
    for (i, part) in text.split('"').enumerate() {
        if i % 2 == 1 {
            phrases.push(tokens(part)); // inside quotes (an unbalanced trailing quote counts as quoted)
        } else {
            phrases.extend(part.split_whitespace().map(tokens));
        }
    }
    let mut seen = std::collections::HashSet::new();
    let mut exprs = Vec::new();
    let mut terms = Vec::new();
    for p in phrases.into_iter().filter(|p| !p.is_empty()) {
        let e = format!("\"{}\"", p.join(" "));
        if seen.insert(e.clone()) {
            exprs.push(e);
            terms.extend(p);
        }
    }
    (!exprs.is_empty()).then(|| (exprs.join(" OR "), terms))
}

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
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

fn days_in_month(y: i64, m: i64) -> i64 {
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

/// Lexically normalize an absolute path (no filesystem access): collapse `//`, `.`, `..`, trailing `/`.
pub fn normalize_abs_path(p: &str) -> Option<String> {
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

fn floor_boundary(s: &str, mut i: usize) -> usize {
    i = i.min(s.len());
    while !s.is_char_boundary(i) {
        i -= 1;
    }
    i
}

const INJECTED_TAGS: [&str; 4] = [
    "oai-mem-citation",
    "environment_context",
    "system-reminder",
    "user_instructions",
];
const AGENTS_PREAMBLE: &str = "# AGENTS.md instructions";

fn remove_blocks(text: &str, open: &str, close: &str, is_tag: bool) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find(open) {
        let after = &rest[i + open.len()..];
        if is_tag && !after.starts_with(|c: char| c == '>' || c.is_whitespace()) {
            out.push_str(&rest[..i + open.len()]);
            rest = after;
            continue;
        }
        out.push_str(&rest[..i]);
        rest = match after.find(close) {
            Some(j) => &after[j + close.len()..],
            None if is_tag => "",
            None => after.find('\n').map_or("", |n| &after[n..]),
        };
    }
    out.push_str(rest);
    out
}

fn strip_raw(text: &str) -> String {
    let mut cur = text.to_string();
    for tag in INJECTED_TAGS {
        let close = format!("</{tag}>");
        let open = format!("<{tag}");
        if let Some(i) = cur.find(&close)
            && !cur[..i].contains(&open)
        {
            cur.drain(..i + close.len());
        }
        cur = remove_blocks(&cur, &open, &close, true);
    }
    remove_blocks(&cur, AGENTS_PREAMBLE, "</INSTRUCTIONS>", false)
}

/// Remove injected blocks (tag and content) and the AGENTS.md preamble; returns the input when nothing remains.
pub fn strip_injected(text: &str) -> String {
    let cur = strip_raw(text);
    if cur.trim().is_empty() {
        text.to_string()
    } else {
        cur
    }
}

/// Title candidate from a user prompt: injected blocks removed, trimmed, at most 120 bytes; `None` when empty.
pub fn title_from_prompt(text: &str) -> Option<String> {
    let cleaned = strip_raw(text);
    let t = cleaned.trim();
    (!t.is_empty()).then(|| t[..floor_boundary(t, 120)].trim_end().to_string())
}

/// A window of at most `max` bytes of the cleaned `body` (char boundaries), centred on `focus` (byte range of the
/// matched span in `body`) when given, else on the first occurrence of a lowercase ASCII term.
/// Returns `(excerpt, truncated)`.
pub fn excerpt(
    body: &str,
    terms: &[String],
    focus: Option<(usize, usize)>,
    max: usize,
) -> (String, bool) {
    let focus = focus.map(|(s, e)| {
        let s = floor_boundary(body, s);
        let e = floor_boundary(body, e).max(s);
        let cs = strip_raw(&body[..s]).len();
        (cs, cs + strip_raw(&body[s..e]).len())
    });
    let body = strip_injected(body);
    let body = body.as_str();
    if body.len() <= max {
        return (body.trim().to_string(), false);
    }
    let lower = body.to_ascii_lowercase(); // ASCII lowering keeps byte offsets
    let term_hit = |from: usize, to: usize| {
        terms
            .iter()
            .filter(|t| t.is_ascii())
            .filter_map(|t| lower.get(from..to).and_then(|w| w.find(t.as_str())))
            .min()
            .map(|h| h + from)
    };
    let start = match focus {
        Some((s, e)) if e > s => {
            let (s, e) = (s.min(body.len()), e.min(body.len()));
            if e - s >= max {
                term_hit(s, e).map_or(s, |h| h.saturating_sub(200).max(s))
            } else {
                (s + (e - s) / 2).saturating_sub(max / 2)
            }
        }
        _ => term_hit(0, body.len()).map_or(0, |h| h.saturating_sub(200)),
    };
    let start = floor_boundary(body, start.min(body.len().saturating_sub(max)));
    let end = floor_boundary(body, start + max);
    (body[start..end].trim().to_string(), true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fts_is_safe_and_keeps_identifiers() {
        let (q, terms) =
            fts_query(r#"parse_snapshot AND "exact phrase" NOT x* ) ( ' -- "unterminated tail"#)
                .unwrap();
        assert_eq!(
            q,
            r#""parse snapshot" OR "and" OR "exact phrase" OR "not" OR "x" OR "unterminated tail""#
        );
        assert!(terms.contains(&"snapshot".to_string()));
        assert!(fts_query(" ?! ## \"\" ").is_none());
    }

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
    fn excerpt_respects_char_boundaries() {
        let body = "é".repeat(1000);
        let (e, t) = excerpt(&body, &[], None, 1200);
        assert!(t && e.len() <= 1200 && e.is_char_boundary(e.len()));
        let long = format!("{}needle{}", "a".repeat(5000), "b".repeat(5000));
        let (e, _) = excerpt(&long, &["needle".into()], None, 1200);
        assert!(e.contains("needle"));
    }

    #[test]
    fn excerpt_centres_on_focus_and_strips_injected() {
        let body = format!(
            "USER: {}<system-reminder>junk</system-reminder>ASSISTANT: mid{}",
            "c".repeat(3000),
            "d".repeat(3000)
        );
        let at = body.find("mid").unwrap();
        let (e, t) = excerpt(&body, &[], Some((at, at + 3)), 1200);
        assert!(t && e.contains("mid") && !e.contains("junk"), "{e}");
        let kept =
            strip_injected("<oai-mem-citation>x</oai-mem-citation>real <user_instructions>y");
        assert_eq!(kept.trim(), "real");
        let doc = "# AGENTS.md instructions for /w\n\n<INSTRUCTIONS>\nrules\n</INSTRUCTIONS>\nask";
        assert_eq!(title_from_prompt(doc).as_deref(), Some("ask"));
        assert_eq!(
            title_from_prompt(&"é".repeat(100)).map(|t| t.len()),
            Some(120)
        );
    }

    #[test]
    fn paths_normalize() {
        assert_eq!(
            normalize_abs_path("/a//b/./c/../d/").as_deref(),
            Some("/a/b/d")
        );
        assert!(normalize_abs_path("rel/x").is_none());
    }
}
