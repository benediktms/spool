use super::{
    GMatch, outside_interval, query,
    retrieve::{Filters, SpanRow},
};
use std::collections::HashMap;

pub(super) struct Selection {
    pub(super) excerpt: String,
    pub(super) truncated: bool,
    pub(super) context_outside_time_filter: bool,
}

pub(super) fn select(
    m: &GMatch,
    terms: &[Vec<String>],
    filters: &Filters,
    all_spans: &HashMap<i64, Vec<SpanRow>>,
    bodies: &HashMap<i64, String>,
    target: usize,
) -> Option<Selection> {
    let semantic = m.matched_by.contains("semantic") && !m.matched_by.contains("lexical");
    let mut candidates = Vec::new();
    for id in &m.rowids {
        let body = &bodies[id];
        let visible = query::visible_ranges(body);
        let mut pos = 0usize;
        for span in &all_spans[id] {
            let rest = body.get(pos..).unwrap_or("");
            pos += ["USER: ", "ASSISTANT: "]
                .iter()
                .find(|label| rest.starts_with(**label))
                .map_or(0, |label| label.len());
            let end = pos
                .saturating_add((span.end - span.start).max(0) as usize)
                .min(body.len());
            if (!semantic || (*id == m.primary && !span.is_context))
                && body.is_char_boundary(pos)
                && body.is_char_boundary(end)
            {
                for range in &visible {
                    let start = pos.max(range.start);
                    let stop = end.min(range.end);
                    if start >= stop {
                        continue;
                    }
                    let width = (stop - start).div_ceil(32).max(target.max(1));
                    let mut sample = start;
                    while sample < stop {
                        let mut sample_end = sample.saturating_add(width).min(stop);
                        while !body.is_char_boundary(sample_end) {
                            sample_end += 1;
                        }
                        let focus = query::support(&body[sample..sample_end], terms)
                            .iter()
                            .map(|(_, at)| sample + at)
                            .min()
                            .unwrap_or(sample);
                        let (excerpt, clipped) = passage(body, start, stop, focus, target);
                        let score = query::support(&excerpt, terms).len();
                        if let Some(offset) = body[start..stop]
                            .match_indices(&excerpt)
                            .map(|(at, _)| start + at)
                            .find(|at| *at <= focus && focus < *at + excerpt.len())
                        {
                            candidates.push((score, *id, offset, excerpt, clipped, span));
                        }
                        sample = sample_end;
                    }
                }
            }
            pos = end.saturating_add(2);
        }
    }
    candidates.sort_by_key(|candidate| std::cmp::Reverse(candidate.0));
    let Some(first) = candidates.first() else {
        all_spans.get(&m.primary)?.first()?;
        return Some(Selection {
            excerpt: String::new(),
            truncated: false,
            context_outside_time_filter: false,
        });
    };
    let mut chosen = vec![first];
    let mut length = first.3.len();
    for candidate in candidates.iter().skip(1) {
        if chosen.len() == 3 {
            break;
        }
        if candidate.0 == 0 || candidate.5.is_context || length + 5 + candidate.3.len() > 8192 {
            continue;
        }
        if chosen.iter().any(|old| {
            old.1 == candidate.1
                && old.2 < candidate.2 + candidate.3.len()
                && candidate.2 < old.2 + old.3.len()
        }) {
            continue;
        }
        length += 5 + candidate.3.len();
        chosen.push(candidate);
    }
    chosen.sort_by_key(|candidate| {
        (
            m.rowids
                .iter()
                .position(|id| *id == candidate.1)
                .unwrap_or(usize::MAX),
            candidate.2,
        )
    });
    Some(Selection {
        excerpt: chosen
            .iter()
            .map(|candidate| candidate.3.as_str())
            .collect::<Vec<_>>()
            .join("\n…\n"),
        truncated: chosen.iter().any(|candidate| candidate.4),
        context_outside_time_filter: chosen
            .iter()
            .any(|candidate| outside_interval(filters, candidate.5.msg_ts)),
    })
}

pub(super) fn literal(body: &str, focus: usize, target: usize) -> (String, bool) {
    let Some(range) = query::visible_ranges(body)
        .into_iter()
        .find(|r| r.start <= focus && focus < r.end)
    else {
        return (String::new(), false);
    };
    passage(body, range.start, range.end, focus, target)
}

fn passage(body: &str, lower: usize, upper: usize, focus: usize, target: usize) -> (String, bool) {
    let before = &body[lower..focus];
    let after = &body[focus..upper];
    let line_start = before.rfind('\n').map_or(lower, |i| lower + i + 1);
    let line_end = after.find('\n').map_or(upper, |i| focus + i);
    let line = body[line_start..line_end].trim_start();
    let is_list = line.starts_with("- ") || line.starts_with("* ") || line.starts_with("+ ");
    let unit_start = if is_list {
        line_start
    } else {
        before.rfind("\n\n").map_or(lower, |i| lower + i + 2)
    };
    let unit_end = if is_list {
        list_end(body, line_end, upper)
    } else {
        after.find("\n\n").map_or(upper, |i| focus + i)
    };
    let hard = 8192;
    let (mut start, mut end, clipped) = if unit_end - unit_start <= hard {
        let mut start = unit_start;
        let mut end = unit_end;
        while end - start < target {
            let next = body[end..upper].trim_start_matches('\n');
            if next.is_empty() {
                break;
            }
            let next_start = upper - next.len();
            let list = next.starts_with("- ") || next.starts_with("* ") || next.starts_with("+ ");
            let next_end = if list {
                let line_end = next.find('\n').map_or(upper, |i| next_start + i);
                list_end(body, line_end, upper)
            } else {
                next.find("\n\n").map_or(upper, |i| next_start + i)
            };
            if next_end - start > hard || next_end - next_start > target {
                break;
            }
            end = next_end;
        }
        while end - start < target && start > lower {
            let previous = body[lower..start].trim_end_matches('\n');
            if previous.is_empty() {
                break;
            }
            let prev_end = lower + previous.len();
            let line_start = previous.rfind('\n').map_or(lower, |i| lower + i + 1);
            let line = &body[line_start..prev_end];
            let list = line.starts_with("- ") || line.starts_with("* ") || line.starts_with("+ ");
            let prev_start = if list {
                line_start
            } else {
                previous.rfind("\n\n").map_or(lower, |i| lower + i + 2)
            };
            if end - prev_start > hard || prev_end - prev_start > target {
                break;
            }
            start = prev_start;
        }
        (start, end, false)
    } else {
        let line_width = line_end - line_start;
        if line_width <= hard {
            (line_start, line_end, true)
        } else {
            let start = focus
                .saturating_sub(target / 3)
                .max(unit_start)
                .min(unit_end.saturating_sub(target));
            (start, start.saturating_add(target).min(unit_end), true)
        }
    };
    while !body.is_char_boundary(start) {
        start += 1;
    }
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    if clipped && line_end - line_start > hard {
        if let Some((offset, ch)) = body[start..focus]
            .char_indices()
            .find(|(_, c)| c.is_whitespace())
        {
            start += offset + ch.len_utf8();
        }
        if let Some((offset, _)) = body[focus..end]
            .char_indices()
            .rfind(|(_, c)| c.is_whitespace())
        {
            end = focus + offset;
        }
    }
    (body[start..end].to_string(), clipped)
}

fn list_end(body: &str, mut end: usize, upper: usize) -> usize {
    while end < upper {
        let next = &body[end + 1..upper];
        if !next.starts_with(' ') && !next.starts_with('\t') {
            break;
        }
        let line_end = next.find('\n').map_or(upper, |i| end + 1 + i);
        if body[end + 1..line_end].trim().is_empty() {
            break;
        }
        end = line_end;
    }
    end
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selection_scores_rendered_passages_and_finds_later_repeated_terms() {
        use std::collections::BTreeSet;

        let text = format!(
            "alpha {}\n\nbeta {}\n\nalpha beta decisive explanation.\n\n- qualification remains visible",
            "filler ".repeat(210),
            "filler ".repeat(210)
        );
        let span = SpanRow {
            record_key: "record".into(),
            start: 0,
            end: text.len() as i64,
            is_context: false,
            msg_ts: None,
        };
        let matched = GMatch {
            primary: 1,
            rowids: vec![1],
            ordinals: vec![0],
            spans: vec![(0, 0, span.clone())],
            matched_by: BTreeSet::from(["lexical"]),
            debug: None,
        };
        let result = select(
            &matched,
            &query::support_units("alpha beta"),
            &Filters::default(),
            &HashMap::from([(1, vec![span])]),
            &HashMap::from([(1, text.clone())]),
            1200,
        )
        .unwrap();
        assert!(result.excerpt.contains("decisive explanation"));
        assert!(result.excerpt.contains("qualification remains visible"));
        assert!(
            result
                .excerpt
                .split("\n…\n")
                .all(|piece| text.contains(piece))
        );
    }

    #[test]
    fn selection_keeps_separated_supported_passages() {
        use std::collections::BTreeSet;

        let text = format!(
            "alpha beta status is pending.\n\n{}\n\nalpha beta cause is a missing lease.\n- qualification applies\n  continuation applies",
            "unrelated ".repeat(300)
        );
        let span = SpanRow {
            record_key: "record".into(),
            start: 0,
            end: text.len() as i64,
            is_context: false,
            msg_ts: None,
        };
        let matched = GMatch {
            primary: 1,
            rowids: vec![1],
            ordinals: vec![0],
            spans: vec![(0, 0, span.clone())],
            matched_by: BTreeSet::from(["lexical"]),
            debug: None,
        };
        let selected = select(
            &matched,
            &query::support_units("alpha beta"),
            &Filters::default(),
            &HashMap::from([(1, vec![span])]),
            &HashMap::from([(1, text)]),
            256,
        )
        .unwrap();
        assert!(selected.excerpt.contains("status is pending"));
        assert!(selected.excerpt.contains("cause is a missing lease"));
        assert!(
            selected
                .excerpt
                .contains("qualification applies\n  continuation applies")
        );
        assert!(selected.excerpt.contains("\n…\n"));
        assert!(!selected.truncated);
    }

    #[test]
    fn passage_expands_complete_adjacent_units_up_to_soft_target() {
        let text = "Introduction needle.\n\nDecisive explanation.\n\n- first item\n- second item\n\nTrailing paragraph.";
        let (preview, clipped) = literal(text, text.find("needle").unwrap(), 55);
        assert_eq!(
            preview,
            "Introduction needle.\n\nDecisive explanation.\n\n- first item"
        );
        assert!(!clipped);
        let (preview, _) = literal(text, text.find("second").unwrap(), 100);
        assert!(preview.contains("- first item\n- second item"));
        let oversized = format!("needle\n\n{}", "x".repeat(8192));
        assert_eq!(literal(&oversized, 0, 1200).0, "needle");
    }

    #[test]
    fn literal_passage_is_verbatim_and_respects_hidden_and_utf8_boundaries() {
        let text = format!(
            "first\n\n{}needle{}\n\nlast",
            "🦀".repeat(2100),
            "x".repeat(100)
        );
        let focus = text.find("needle").unwrap();
        let (preview, clipped) = literal(&text, focus, 256);
        assert!(clipped);
        assert!(preview.contains("needle"));
        assert!(text.contains(&preview));
        let words = format!("{}needle {}", "word ".repeat(2000), "tail ".repeat(2000));
        let focus = words.find("needle").unwrap();
        let (preview, clipped) = literal(&words, focus, 256);
        assert!(clipped && preview.contains("needle"));
        assert!(preview.starts_with("word ") && preview.ends_with("tail"));
        let hidden = "visible <system-reminder>secret</system-reminder> tail";
        assert_eq!(literal(hidden, hidden.find("secret").unwrap(), 256).0, "");
    }
}
