//! Pure query-side helpers: safe FTS5 query construction and excerpt windows.

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

pub(super) fn support_units(text: &str) -> Vec<Vec<String>> {
    let mut units = Vec::new();
    for (i, part) in text.split('"').enumerate() {
        let parts: Vec<&str> = if i % 2 == 1 {
            vec![part]
        } else {
            part.split_whitespace().collect()
        };
        for part in parts {
            let unit = tokens(part);
            if !unit.is_empty()
                && !(unit.len() == 1
                    && matches!(
                        unit[0].as_str(),
                        "a" | "an"
                            | "and"
                            | "are"
                            | "as"
                            | "at"
                            | "be"
                            | "by"
                            | "did"
                            | "do"
                            | "for"
                            | "from"
                            | "how"
                            | "in"
                            | "is"
                            | "it"
                            | "of"
                            | "on"
                            | "or"
                            | "the"
                            | "to"
                            | "was"
                            | "what"
                            | "which"
                            | "why"
                            | "with"
                    ))
                && !units.contains(&unit)
            {
                units.push(unit);
            }
        }
    }
    units
}

pub(super) fn support(text: &str, units: &[Vec<String>]) -> Vec<(usize, usize)> {
    let mut words = Vec::new();
    let mut start = None;
    for (offset, c) in text
        .char_indices()
        .chain(std::iter::once((text.len(), ' ')))
    {
        if is_tok(c) {
            start.get_or_insert(offset);
        } else if let Some(begin) = start.take() {
            words.push((begin, text[begin..offset].to_lowercase()));
        }
    }
    units
        .iter()
        .enumerate()
        .filter_map(|(index, unit)| {
            words
                .windows(unit.len())
                .find(|window| window.iter().map(|(_, word)| word).eq(unit.iter()))
                .map(|window| (index, window[0].0))
        })
        .collect()
}

pub(super) use crate::clock::format_rfc3339;

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
const COMMAND_WRAPPERS: [&str; 4] = [
    "<command-name>",
    "<command-message>",
    "<local-command-stdout>",
    "<local-command-caveat>",
];
const AGENTS_PREAMBLE: &str = "# AGENTS.md instructions";

fn literal_citation_markers(text: &str) -> std::collections::HashSet<usize> {
    let mut markers = std::collections::HashSet::new();
    let mut offset = 0;
    let mut fence: Option<(u8, usize)> = None;
    for line in text.split_inclusive('\n') {
        let bytes = line.as_bytes();
        let indent = bytes.iter().take_while(|&&b| b == b' ').count();
        if indent <= 3 && indent < bytes.len() && matches!(bytes[indent], b'`' | b'~') {
            let kind = bytes[indent];
            let count = bytes[indent..].iter().take_while(|&&b| b == kind).count();
            if count >= 3 {
                match fence {
                    None => fence = Some((kind, count)),
                    Some((f, n)) if f == kind && count >= n => fence = None,
                    _ => {}
                }
                offset += line.len();
                continue;
            }
        }
        if fence.is_none() {
            let mut i = 0;
            let mut open = None;
            while i < bytes.len() {
                if bytes[i] != b'`' {
                    i += 1;
                    continue;
                }
                let count = bytes[i..].iter().take_while(|&&b| b == b'`').count();
                let escaped = bytes[..i].iter().rev().take_while(|&&b| b == b'\\').count() % 2 == 1;
                if !escaped {
                    match open {
                        None => open = Some((i, count)),
                        Some((start, n)) if n == count => {
                            if n <= 2
                                && matches!(
                                    &line[start + n..i],
                                    "<oai-mem-citation>" | "</oai-mem-citation>"
                                )
                            {
                                markers.insert(offset + start + n);
                            }
                            open = None;
                        }
                        _ => {}
                    }
                }
                i += count;
            }
        }
        offset += line.len();
    }
    markers
}

fn find_marker(
    text: &str,
    marker: &str,
    offsets: &[usize],
    literals: &std::collections::HashSet<usize>,
) -> Option<usize> {
    text.match_indices(marker).find_map(|(i, _)| {
        let literal = offsets.get(i).is_some_and(|start| literals.contains(start))
            && offsets.get(i + marker.len() - 1).copied()
                == offsets.get(i).map(|start| start + marker.len() - 1);
        (!literal).then_some(i)
    })
}

fn remove_blocks(
    text: &str,
    open: &str,
    close: &str,
    is_tag: bool,
    offsets: &[usize],
    literals: &std::collections::HashSet<usize>,
) -> Vec<std::ops::Range<usize>> {
    let mut ranges = Vec::new();
    let mut pos = 0;
    while let Some(i) = find_marker(&text[pos..], open, &offsets[pos..], literals) {
        let begin = pos + i;
        let after = begin + open.len();
        if is_tag && !text[after..].starts_with(|c: char| c == '>' || c.is_whitespace()) {
            ranges.push(pos..after);
            pos = after;
            continue;
        }
        ranges.push(pos..begin);
        pos = match find_marker(&text[after..], close, &offsets[after..], literals) {
            Some(j) => after + j + close.len(),
            None if is_tag => text.len(),
            None => text[after..].find('\n').map_or(text.len(), |n| after + n),
        };
    }
    ranges.push(pos..text.len());
    ranges
}

pub(super) fn visible_ranges(text: &str) -> Vec<std::ops::Range<usize>> {
    let literals = literal_citation_markers(text);
    let mut cur = text.to_string();
    let mut offsets: Vec<usize> = (0..text.len()).collect();
    let mut apply = |open: &str, close: &str, is_tag: bool| {
        let drop_prefix = if is_tag {
            find_marker(&cur, close, &offsets, &literals)
                .filter(|&i| find_marker(&cur[..i], open, &offsets[..i], &literals).is_none())
                .map_or(0, |i| i + close.len())
        } else {
            0
        };
        let ranges = remove_blocks(
            &cur[drop_prefix..],
            open,
            close,
            is_tag,
            &offsets[drop_prefix..],
            &literals,
        );
        let mut next = String::new();
        let mut mapped = Vec::new();
        for range in ranges {
            let range = range.start + drop_prefix..range.end + drop_prefix;
            next.push_str(&cur[range.clone()]);
            mapped.extend_from_slice(&offsets[range]);
        }
        cur = next;
        offsets = mapped;
    };
    for tag in INJECTED_TAGS {
        apply(&format!("<{tag}"), &format!("</{tag}>"), true);
    }
    apply(AGENTS_PREAMBLE, "</INSTRUCTIONS>", false);
    let mut out: Vec<std::ops::Range<usize>> = Vec::new();
    for offset in offsets {
        if let Some(last) = out.last_mut().filter(|r| r.end == offset) {
            last.end += 1;
        } else {
            out.push(offset..offset + 1);
        }
    }
    out
}

fn strip_raw(text: &str) -> String {
    visible_ranges(text).into_iter().map(|r| &text[r]).collect()
}

/// Remove injected blocks (tag and content) and the AGENTS.md preamble; returns the input when nothing remains.
#[cfg(test)]
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
    let command = COMMAND_WRAPPERS.iter().any(|tag| t.starts_with(tag));
    (!t.is_empty() && !command).then(|| t[..floor_boundary(t, 120)].trim_end().to_string())
}

/// A window of at most `max` bytes of the cleaned `body` (char boundaries), centred on `focus` (byte range of the
/// matched span in `body`) when given, else on the first occurrence of a lowercase ASCII term.
/// Returns `(excerpt, truncated)`.
#[cfg(test)]
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
    fn visible_ranges_preserve_original_utf8_offsets() {
        let text = format!(
            "🦀before<system-reminder>{}</system-reminder>after 1 < 2",
            "needle ".repeat(40)
        );
        let ranges = visible_ranges(&text);
        let kept: String = ranges.iter().map(|r| &text[r.clone()]).collect();
        assert_eq!(kept, "🦀beforeafter 1 < 2");
        assert!(
            ranges
                .iter()
                .all(|r| text.is_char_boundary(r.start) && text.is_char_boundary(r.end))
        );
        assert_eq!(&text[ranges[1].clone()], "after 1 < 2");
        assert_eq!(strip_raw("prefix</system-reminder>visible"), "visible");
        assert_eq!(
            strip_raw("a # AGENTS.md instructions secret</INSTRUCTIONS> b"),
            "a  b"
        );
    }

    #[test]
    fn citation_examples_do_not_hide_following_text() {
        let text = "before `<oai-mem-citation>` after `</oai-mem-citation>` final";
        assert_eq!(strip_raw(text), text);
        let double = "before ``<oai-mem-citation>`` after ``</oai-mem-citation>`` final";
        assert_eq!(strip_raw(double), double);
        assert_eq!(
            strip_raw("before `<oai-mem-citation> extra` tail"),
            "before `"
        );
        assert_eq!(
            strip_raw("```\n`<oai-mem-citation>`\n```\nsecret"),
            "```\n`"
        );
        assert_eq!(strip_raw("<oai-mem-citation>hidden"), "");
        assert_eq!(strip_raw("hidden</oai-mem-citation>visible"), "visible");
        assert_eq!(
            strip_raw("`<oai-mem-citation>` <oai-mem-citation>hidden</oai-mem-citation>visible"),
            "`<oai-mem-citation>` visible"
        );
        assert_eq!(strip_raw("before `<oai-mem-citation> tail"), "before `");
        assert_eq!(strip_raw("before `<oai-mem-citation x>` tail"), "before `");
        assert_eq!(
            strip_raw("before ```<oai-mem-citation>``` tail"),
            "before ```"
        );
        assert_eq!(
            strip_raw(
                "<oai-mem-citation>secret `</oai-mem-citation>` still secret</oai-mem-citation>visible"
            ),
            "visible"
        );
        assert_eq!(
            strip_raw("`<oai-mem-citation>` <system-reminder>secret</system-reminder> visible"),
            "`<oai-mem-citation>`  visible"
        );
        let unicode = "🦀 `<oai-mem-citation>` visible";
        let ranges = visible_ranges(unicode);
        assert_eq!(
            ranges
                .iter()
                .map(|r| &unicode[r.clone()])
                .collect::<String>(),
            unicode
        );
    }

    #[test]
    fn support_preserves_units_and_unicode() {
        let units = support_units("How did lease_refs and \"exact phrase\" explain café?");
        assert_eq!(
            support("workers lease_other exact aside phrase café", &units).len(),
            1
        );
        assert_eq!(support("lease_refs exact phrase café", &units).len(), 3);
        assert!(support_units("why and ?!").is_empty());
    }

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
        let login = "<command-name>/login</command-name>\n<command-message>login</command-message>";
        assert_eq!(title_from_prompt(login), None);
        assert_eq!(
            title_from_prompt("<local-command-stdout>Login successful</local-command-stdout>"),
            None
        );
        assert_eq!(
            title_from_prompt(&"é".repeat(100)).map(|t| t.len()),
            Some(120)
        );
    }
}
