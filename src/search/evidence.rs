use super::{
    GMatch, outside_interval, overlaps, query,
    retrieve::{Filters, SpanRow},
};
use std::collections::HashMap;

const MAX_MATCH_SPANS: usize = 8;
const MAX_MATCH_BYTES: usize = 1200;

pub(super) struct Selection {
    pub(super) spans: Vec<SpanRow>,
    pub(super) excerpt: String,
    pub(super) truncated: bool,
    pub(super) context_outside_time_filter: bool,
}

struct Window<'a> {
    rowid: i64,
    source: &'a SpanRow,
    body_start: usize,
    visible_start: usize,
    visible_end: usize,
    start: usize,
    end: usize,
    focus: usize,
    order: usize,
    hidden: bool,
}

fn windows_overlap(c: &Window<'_>, other: &Window<'_>) -> bool {
    c.source.record_key == other.source.record_key
        && c.source.block_index == other.source.block_index
        && c.source.start + ((c.start - c.body_start) as i64)
            < other.source.start + (other.end - other.body_start) as i64
        && other.source.start + ((other.start - other.body_start) as i64)
            < c.source.start + (c.end - c.body_start) as i64
}

pub(super) fn select(
    m: &GMatch,
    terms: &[Vec<String>],
    filters: &Filters,
    all_spans: &HashMap<i64, Vec<SpanRow>>,
    bodies: &HashMap<i64, String>,
) -> Option<Selection> {
    let mut spans = m.spans.clone();
    spans.sort_by_key(|(ord, seq, _)| (*ord, *seq));
    let mut merged: Vec<SpanRow> = Vec::new();
    for (_, _, s) in spans {
        match merged.last_mut() {
            Some(p)
                if p.record_key == s.record_key
                    && p.block_index == s.block_index
                    && s.start <= p.end
                    && p.start <= s.end =>
            {
                p.start = p.start.min(s.start);
                p.end = p.end.max(s.end);
            }
            _ => merged.push(s),
        }
    }
    let mut candidates = Vec::new();
    let mut fallback = None;
    for rowid in &m.rowids {
        let body = &bodies[rowid];
        let visible = query::visible_ranges(body);
        let mut pos = 0usize;
        for span in &all_spans[rowid] {
            let rest = body.get(pos..).unwrap_or("");
            pos += ["USER: ", "ASSISTANT: "]
                .iter()
                .find(|l| rest.starts_with(**l))
                .map_or(0, |l| l.len());
            let end = pos.saturating_add((span.end - span.start).max(0) as usize);
            if !span.is_context && end <= body.len() {
                let order = merged.iter().position(|s| overlaps(s, span)).unwrap_or(0);
                if fallback.is_none() || *rowid == m.primary {
                    let mut stop = pos.saturating_add(MAX_MATCH_BYTES).min(end);
                    while !body.is_char_boundary(stop) {
                        stop -= 1;
                    }
                    if stop > pos {
                        fallback = Some(Window {
                            rowid: *rowid,
                            source: span,
                            body_start: pos,
                            visible_start: pos,
                            visible_end: stop,
                            start: pos,
                            end: stop,
                            focus: pos,
                            order,
                            hidden: true,
                        });
                    }
                }
                for range in &visible {
                    let visible_start = pos.max(range.start);
                    let visible_end = end.min(range.end);
                    let mut part = visible_start;
                    while part < visible_end {
                        let mut stop = part.saturating_add(MAX_MATCH_BYTES / 2).min(visible_end);
                        while !body.is_char_boundary(stop) {
                            stop -= 1;
                        }
                        if stop == part {
                            break;
                        }
                        let mut focuses = vec![part];
                        let mut sample = part;
                        while sample < stop {
                            let mut sample_end = sample
                                .saturating_add(MAX_MATCH_BYTES / MAX_MATCH_SPANS)
                                .min(stop);
                            while !body.is_char_boundary(sample_end) {
                                sample_end += 1;
                            }
                            if sample_end == sample {
                                break;
                            }
                            if let Some(hit) = query::support(&body[sample..sample_end], terms)
                                .into_iter()
                                .map(|(_, at)| at)
                                .min()
                            {
                                focuses.push(sample + hit);
                            }
                            sample = sample_end;
                        }
                        for focus in focuses {
                            let mut start = focus.saturating_sub(200).max(visible_start).min(
                                visible_end
                                    .saturating_sub(MAX_MATCH_BYTES / 2)
                                    .max(visible_start),
                            );
                            while !body.is_char_boundary(start) {
                                start -= 1;
                            }
                            let mut window_end =
                                start.saturating_add(MAX_MATCH_BYTES / 2).min(visible_end);
                            while !body.is_char_boundary(window_end) {
                                window_end -= 1;
                            }
                            candidates.push(Window {
                                rowid: *rowid,
                                source: span,
                                body_start: pos,
                                visible_start,
                                visible_end,
                                start,
                                end: window_end,
                                focus,
                                order,
                                hidden: false,
                            });
                        }
                        part = stop;
                    }
                }
            }
            pos = end.saturating_add(2);
        }
    }
    if candidates.is_empty() {
        candidates.extend(fallback);
    }
    let separator = "\n…\n";
    let allocate = |chosen: &[usize]| {
        let mut remaining = MAX_MATCH_BYTES - separator.len() * (chosen.len() - 1);
        let widths: Vec<usize> = chosen
            .iter()
            .map(|index| {
                let c = &candidates[*index];
                let shared = chosen.iter().any(|other| {
                    *other != *index
                        && candidates[*other].source.record_key == c.source.record_key
                        && candidates[*other].source.block_index == c.source.block_index
                });
                if shared {
                    c.end - c.start
                } else {
                    (c.visible_end - c.visible_start).min(MAX_MATCH_BYTES)
                }
            })
            .collect();
        let mut allocation = vec![0; chosen.len()];
        let mut pending: Vec<usize> = (0..chosen.len()).collect();
        while !pending.is_empty() && remaining > 0 {
            let share = remaining / pending.len();
            let saturated: Vec<usize> = pending
                .iter()
                .copied()
                .filter(|i| widths[*i] <= share)
                .collect();
            if saturated.is_empty() {
                let extra = remaining % pending.len();
                for (n, index) in pending.iter().enumerate() {
                    allocation[*index] = share + usize::from(n < extra);
                }
                break;
            }
            for index in &saturated {
                allocation[*index] = widths[*index];
                remaining -= widths[*index];
            }
            pending.retain(|index| !saturated.contains(index));
        }
        let mut result = Vec::new();
        for (n, index) in chosen.iter().enumerate() {
            let c = &candidates[*index];
            let body = &bodies[&c.rowid];
            let width = allocation[n];
            let shared =
                widths[n] == c.end - c.start && (c.visible_end - c.visible_start) > widths[n];
            let lower = if shared { c.start } else { c.visible_start };
            let upper = if shared { c.end } else { c.visible_end };
            let mut start = c
                .focus
                .saturating_sub(width / 3)
                .max(lower)
                .min(upper.saturating_sub(width).max(lower));
            while !body.is_char_boundary(start) {
                start -= 1;
            }
            let mut end = start.saturating_add(width).min(upper);
            while !body.is_char_boundary(end) {
                end -= 1;
            }
            result.push((*index, start, end));
        }
        result
    };
    let score = |chosen: &[usize]| {
        let mut unique = std::collections::HashSet::new();
        let mut total = 0usize;
        for (index, start, end) in allocate(chosen) {
            let c = &candidates[index];
            if !c.hidden {
                let hits = query::support(&bodies[&c.rowid][start..end], terms);
                total += hits.len();
                unique.extend(hits.into_iter().map(|(unit, _)| unit));
            }
        }
        (3 * unique.len() + 2 * total) as i64 - 3 * chosen.len() as i64
    };
    let semantic = m.matched_by.contains("semantic");
    let first = candidates
        .iter()
        .enumerate()
        .filter(|(_, c)| !semantic || c.rowid == m.primary)
        .max_by_key(|(i, _)| (score(&[*i]), std::cmp::Reverse(*i)))
        .map(|(i, _)| i)
        .or_else(|| (!candidates.is_empty()).then_some(0));
    let first = first?;
    let mut chosen = vec![first];
    while chosen.len() < MAX_MATCH_SPANS {
        let current = score(&chosen);
        let next = candidates
            .iter()
            .enumerate()
            .filter(|(_, c)| {
                !chosen.iter().any(|i| {
                    let other = &candidates[*i];
                    windows_overlap(c, other)
                })
            })
            .map(|(i, _)| {
                let mut trial = chosen.clone();
                trial.push(i);
                (i, score(&trial))
            })
            .filter(|(_, value)| *value > current)
            .max_by_key(|(i, value)| (*value, std::cmp::Reverse(*i)));
        let Some((index, _)) = next else {
            break;
        };
        chosen.push(index);
    }
    if score(&chosen) > -(3 * chosen.len() as i64) {
        let mut order: Vec<_> = (0..candidates.len()).collect();
        order.sort_by_key(|i| (candidates[*i].order, candidates[*i].start));
        let mut used: usize = chosen
            .iter()
            .map(|i| candidates[*i].end - candidates[*i].start)
            .sum();
        for index in order {
            let c = &candidates[index];
            if chosen.len() == MAX_MATCH_SPANS || used >= MAX_MATCH_BYTES / 2 {
                break;
            }
            if chosen.iter().any(|i| {
                let other = &candidates[*i];
                windows_overlap(c, other)
            }) {
                continue;
            }
            let mut trial = chosen.clone();
            trial.push(index);
            if score(&trial) < score(&chosen) - 3 {
                continue;
            }
            used += c.end - c.start;
            chosen.push(index);
        }
    }
    let mut selected = Vec::new();
    let mut pieces = Vec::new();
    let mut excerpt_outside = false;
    for (index, start, end) in allocate(&chosen) {
        let c = &candidates[index];
        let body = &bodies[&c.rowid];
        let mut span = c.source.clone();
        span.start += (start - c.body_start) as i64;
        span.end = span.start + (end - start) as i64;
        let text = if c.hidden { "" } else { &body[start..end] };
        if c.hidden {
            selected.push((c.order, span));
        } else if !text.trim().is_empty() {
            excerpt_outside |= outside_interval(filters, c.source.msg_ts);
            pieces.push((c.order, span.start, text.trim().to_string()));
            selected.push((c.order, span));
        }
    }
    selected.sort_by_key(|(order, span)| (*order, span.start));
    pieces.sort_by_key(|(order, start, _)| (*order, *start));
    let excerpt = pieces
        .into_iter()
        .map(|(_, _, text)| text)
        .collect::<Vec<_>>()
        .join(separator);
    let selected: Vec<_> = selected.into_iter().map(|(_, span)| span).collect();
    let truncated = selected.len() > 1
        || selected
            .iter()
            .map(|s| (s.end - s.start) as usize)
            .sum::<usize>()
            < merged
                .iter()
                .map(|s| (s.end - s.start) as usize)
                .sum::<usize>();
    Some(Selection {
        spans: selected,
        excerpt,
        truncated,
        context_outside_time_filter: excerpt_outside,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn ties_follow_insertion_order_unless_semantic_prefers_primary() {
        let text = format!("needle {}", "x".repeat(593));
        let early = SpanRow {
            record_key: "early".into(),
            block_index: 0,
            start: 10,
            end: 610,
            is_context: false,
            msg_ts: Some(100),
        };
        let late = SpanRow {
            record_key: "late".into(),
            block_index: 0,
            start: 20,
            end: 620,
            is_context: false,
            msg_ts: Some(10),
        };
        let all_spans = HashMap::from([(1, vec![early.clone()]), (2, vec![late.clone()])]);
        let bodies = HashMap::from([(1, text.clone()), (2, text.clone())]);
        let filters = Filters {
            after: Some(50),
            ..Filters::default()
        };
        let terms = query::support_units("needle");
        for (semantic, key, start, outside) in
            [(false, "late", 20, true), (true, "early", 10, false)]
        {
            let m = GMatch {
                primary: 1,
                rowids: vec![2, 1],
                run_key: String::new(),
                ordinals: vec![0, 1],
                spans: vec![(0, 0, early.clone()), (1, 0, late.clone())],
                matched_by: if semantic {
                    BTreeSet::from(["semantic"])
                } else {
                    BTreeSet::from(["lexical"])
                },
                ctx_outside: false,
                debug: None,
            };
            let result = select(&m, &terms, &filters, &all_spans, &bodies).unwrap();
            assert_eq!(result.spans.len(), 1);
            assert_eq!(
                (
                    &*result.spans[0].record_key,
                    result.spans[0].start,
                    result.spans[0].end
                ),
                (key, start, start + 600)
            );
            assert_eq!(result.excerpt, text);
            assert!(result.truncated);
            assert_eq!(result.context_outside_time_filter, outside);
        }
    }
}
