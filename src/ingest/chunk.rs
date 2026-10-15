use crate::{
    domain::{Digest, Role, TextBlock, TimestampMs, digest},
    graph::{Run, SessionGraph},
};
use std::collections::HashMap;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkError {
    Cancelled,
    TokenLimit,
}
use std::sync::atomic::{AtomicBool, Ordering};
const PREFIX_WINDOW_BYTES: usize = 8 * 1024;
const MAX_CHUNK_NEW_BYTES: usize = 64 * 1024;
const MAX_CONTEXT_BYTES: usize = 64 * 1024;
const MAX_CHUNK_FRAGMENTS: usize = 128;
const TOKEN_BOUNDARY_MARGIN: usize = 4;

fn check_cancel(cancel: &AtomicBool) -> Result<(), ChunkError> {
    if cancel.load(Ordering::Relaxed) {
        Err(ChunkError::Cancelled)
    } else {
        Ok(())
    }
}

fn count_tokens(
    tok: &dyn TokenCounter,
    text: &str,
    cancel: &AtomicBool,
) -> Result<usize, ChunkError> {
    check_cancel(cancel)?;
    let count = tok.count(text);
    check_cancel(cancel)?;
    Ok(count)
}

fn prefix_tokens(
    tok: &dyn TokenCounter,
    text: &str,
    max_tokens: usize,
    cancel: &AtomicBool,
) -> Result<usize, ChunkError> {
    check_cancel(cancel)?;
    let bytes = tok.prefix_within(text, max_tokens);
    check_cancel(cancel)?;
    Ok(bytes)
}

pub trait TokenCounter: Send + Sync {
    fn identity(&self) -> &str;
    fn count(&self, text: &str) -> usize;
    fn prefix_within(&self, text: &str, max_tokens: usize) -> usize;
}

#[derive(Debug, Clone, Copy)]
pub struct ChunkConfig {
    pub target_new_tokens: usize,
    pub max_rendered_tokens: usize,
    pub overlap_tokens: usize,
    pub version: u32,
}
#[derive(Debug, Clone)]
pub struct ChunkMessage {
    pub key: String,
    pub role: Role,
    pub timestamp: Option<TimestampMs>,
    pub blocks: Vec<TextBlock>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkPlan {
    pub run_key: String,
    pub ordinal: u32,
    pub rendered: String,
    pub input_hash: Digest,
    pub rendered_tokens: usize,
    pub time_anchor: Option<TimestampMs>,
    pub spans: Vec<ChunkSpan>,
    pub is_tail: bool,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkSpan {
    pub message_key: String,
    pub block_index: u32,
    pub start: usize,
    pub end: usize,
    pub is_context: bool,
}

#[derive(Clone, Copy)]
struct Fragment<'a> {
    message: &'a ChunkMessage,
    block: &'a TextBlock,
    start: usize,
    end: usize,
    is_context: bool,
}
impl Fragment<'_> {
    fn text(&self) -> &str {
        &self.block.text[self.start..self.end]
    }
    fn label(&self) -> &'static str {
        speaker(self.message.role)
    }
    fn span(&self) -> ChunkSpan {
        ChunkSpan {
            message_key: self.message.key.clone(),
            block_index: self.block.index,
            start: self.start,
            end: self.end,
            is_context: self.is_context,
        }
    }
}
fn speaker(role: Role) -> &'static str {
    if role == Role::User {
        "USER: "
    } else {
        "ASSISTANT: "
    }
}
fn render(fragments: &[Fragment<'_>]) -> String {
    let mut out = String::new();
    for fragment in fragments {
        if !out.is_empty() {
            out.push_str("\n\n");
        }
        out.push_str(fragment.label());
        out.push_str(fragment.text());
    }
    out
}
fn render_with(context: &[Fragment<'_>], new: &[Fragment<'_>]) -> String {
    let mut out = String::new();
    for fragment in context.iter().chain(new) {
        if !out.is_empty() {
            out.push_str("\n\n");
        }
        out.push_str(fragment.label());
        out.push_str(fragment.text());
    }
    out
}
fn spans(context: &[Fragment<'_>], new: &[Fragment<'_>]) -> Vec<ChunkSpan> {
    context.iter().chain(new).map(Fragment::span).collect()
}
#[derive(Clone, Copy)]
struct Env<'a> {
    cfg: &'a ChunkConfig,
    tok: &'a dyn TokenCounter,
    cancel: &'a AtomicBool,
}
impl Env<'_> {
    fn context_limit(&self) -> usize {
        self.cfg
            .overlap_tokens
            .min(200)
            .min(self.cfg.max_rendered_tokens)
    }
}
struct RunBuf<'a> {
    run_key: &'a str,
    ordinal: u32,
    recent_context: Vec<Fragment<'a>>,
    current: Vec<Fragment<'a>>,
    tokens: usize,
    bytes: usize,
    time: Option<TimestampMs>,
}
#[derive(Clone, Copy)]
struct PrefixFit {
    bytes: usize,
    rendered_tokens: usize,
    new_tokens: usize,
}
fn largest_prefix<'a>(
    env: &Env<'_>,
    mut full_base: Vec<Fragment<'a>>,
    mut new_base: Vec<Fragment<'a>>,
    from: Fragment<'a>,
    current_tokens: usize,
) -> Result<Option<PrefixFit>, ChunkError> {
    let Env { cfg, tok, cancel } = *env;
    let Fragment {
        message,
        block,
        start,
        ..
    } = from;
    let text: &'a str = &block.text;
    let hard_limit = cfg.max_rendered_tokens;
    let target_limit = cfg.target_new_tokens.max(1);
    check_cancel(cancel)?;
    let rest = &text[start..];
    if rest.is_empty() {
        return Ok(None);
    }
    let window_end = floor_char_boundary(rest, rest.len().min(PREFIX_WINDOW_BYTES));
    let window = &rest[..window_end];
    let same_base = full_base.len() == new_base.len();
    let hard_base_tokens = if same_base {
        current_tokens
    } else {
        count_tokens(tok, &render(&full_base), cancel)?
    };
    let hard_remaining = hard_limit.saturating_sub(hard_base_tokens);
    let target_remaining = target_limit.saturating_sub(current_tokens);
    let rendering_overhead =
        count_tokens(tok, "\n\n", cancel)? + count_tokens(tok, speaker(message.role), cancel)?;
    let mut remaining_tokens = hard_remaining
        .min(target_remaining)
        .saturating_sub(rendering_overhead)
        .saturating_sub(TOKEN_BOUNDARY_MARGIN);
    for _ in 0..4 {
        check_cancel(cancel)?;
        let estimate = floor_char_boundary(
            window,
            prefix_tokens(tok, window, remaining_tokens, cancel)?.min(window.len()),
        );
        if estimate == 0 {
            break;
        }
        let piece = Fragment {
            message,
            block,
            start,
            end: start + estimate,
            is_context: false,
        };
        full_base.push(piece);
        new_base.push(piece);
        let rendered_tokens = count_tokens(tok, &render(&full_base), cancel)?;
        let new_tokens = if same_base {
            rendered_tokens
        } else {
            count_tokens(tok, &render(&new_base), cancel)?
        };
        full_base.pop();
        new_base.pop();
        if rendered_tokens <= hard_limit && new_tokens <= target_limit {
            return Ok(Some(PrefixFit {
                bytes: estimate,
                rendered_tokens,
                new_tokens,
            }));
        }
        let excess = rendered_tokens
            .saturating_sub(hard_limit)
            .max(new_tokens.saturating_sub(target_limit))
            .max(1);
        remaining_tokens = remaining_tokens.saturating_sub(excess);
    }
    Ok(None)
}
fn floor_char_boundary(text: &str, mut n: usize) -> usize {
    n = n.min(text.len());
    while n > 0 && !text.is_char_boundary(n) {
        n -= 1;
    }
    n
}
fn ceil_char_boundary(text: &str, mut n: usize) -> usize {
    n = n.min(text.len());
    while n < text.len() && !text.is_char_boundary(n) {
        n += 1;
    }
    n
}
fn select_context<'a>(
    candidates: &[Fragment<'a>],
    max_tokens: usize,
    tok: &dyn TokenCounter,
    cancel: &AtomicBool,
) -> Result<Vec<Fragment<'a>>, ChunkError> {
    if max_tokens == 0 {
        return Ok(Vec::new());
    }
    let mut selected = Vec::new();
    let mut selected_bytes = 0usize;
    'candidates: for candidate in candidates.iter().rev() {
        check_cancel(cancel)?;
        if selected.len() >= MAX_CHUNK_FRAGMENTS || selected_bytes >= MAX_CONTEXT_BYTES {
            break;
        }
        let rest = candidate.text();
        let window_len = rest
            .len()
            .min(PREFIX_WINDOW_BYTES)
            .min(MAX_CONTEXT_BYTES - selected_bytes);
        let window_offset = ceil_char_boundary(rest, rest.len().saturating_sub(window_len));
        let window = &rest[window_offset..];
        let selected_tokens = if selected.is_empty() {
            0
        } else {
            count_tokens(tok, &render(&selected), cancel)?
        };
        let wrapper = if selected.is_empty() {
            candidate.label()
        } else if candidate.message.role == Role::User {
            "\n\nUSER: "
        } else {
            "\n\nASSISTANT: "
        };
        let remaining = max_tokens
            .saturating_sub(selected_tokens)
            .saturating_sub(count_tokens(tok, wrapper, cancel)?);
        let estimate_len = prefix_tokens(tok, window, remaining, cancel)?.min(window.len());
        if estimate_len == 0 {
            break;
        }
        let mut begin = ceil_char_boundary(window, window.len().saturating_sub(estimate_len));
        for _ in 0..4 {
            check_cancel(cancel)?;
            if begin >= window.len() {
                break;
            }
            let estimate_start = window_offset + begin;
            let mut partial = selected.clone();
            partial.insert(
                0,
                Fragment {
                    start: candidate.start + estimate_start,
                    is_context: true,
                    ..*candidate
                },
            );
            let rendered_tokens = count_tokens(tok, &render(&partial), cancel)?;
            if rendered_tokens <= max_tokens {
                selected_bytes += candidate.end - (candidate.start + estimate_start);
                selected = partial;
                if begin == 0 && window_offset == 0 {
                    continue 'candidates;
                }
                return Ok(selected);
            }
            let suffix_bytes = window.len() - begin;
            let excess = rendered_tokens - max_tokens;
            let shrink = suffix_bytes
                .saturating_mul(excess)
                .checked_div(rendered_tokens)
                .unwrap_or(0)
                .max(1);
            begin = ceil_char_boundary(window, begin.saturating_add(shrink));
        }
        break;
    }
    Ok(selected)
}
fn plan(
    env: &Env<'_>,
    buf: &RunBuf<'_>,
    context: &[Fragment<'_>],
    is_tail: bool,
) -> Result<ChunkPlan, ChunkError> {
    let rendered = render_with(context, &buf.current);
    let rendered_tokens = count_tokens(env.tok, &rendered, env.cancel)?;
    Ok(ChunkPlan {
        run_key: buf.run_key.into(),
        ordinal: buf.ordinal,
        input_hash: digest(rendered.as_bytes()),
        rendered,
        rendered_tokens,
        time_anchor: buf.time,
        spans: spans(context, &buf.current),
        is_tail,
    })
}

/// Greedy, deterministic graph-run chunking. Only the most recent bounded source spans are
/// retained as overlap context; source text is sliced on UTF-8 boundaries and never copied into
/// an intermediate all-fragments buffer.
pub fn plan_chunks(
    graph: &SessionGraph,
    messages: &[ChunkMessage],
    cfg: &ChunkConfig,
    tok: &dyn TokenCounter,
    cancel: &AtomicBool,
) -> Result<Vec<ChunkPlan>, ChunkError> {
    check_cancel(cancel)?;
    let env = Env { cfg, tok, cancel };
    let mut by_key = HashMap::with_capacity(messages.len());
    for message in messages {
        check_cancel(cancel)?;
        by_key.insert(message.key.as_str(), message);
    }
    let mut runs = HashMap::with_capacity(graph.runs().len());
    for run in graph.runs() {
        check_cancel(cancel)?;
        runs.insert(run.key.as_str(), run);
    }
    let mut plans = Vec::new();
    for run in graph.runs() {
        check_cancel(cancel)?;
        let mut ancestor_runs: Vec<&Run> = Vec::new();
        let mut parent = run.parent_run.as_deref();
        while let Some(key) = parent {
            check_cancel(cancel)?;
            let Some(parent_run) = runs.get(key) else {
                break;
            };
            ancestor_runs.push(*parent_run);
            parent = parent_run.parent_run.as_deref();
        }
        let mut ancestors = Vec::new();
        for parent_run in ancestor_runs.iter().rev() {
            check_cancel(cancel)?;
            for msg_key in &parent_run.messages {
                check_cancel(cancel)?;
                if let Some(message) = by_key.get(msg_key.as_str()) {
                    for block in &message.blocks {
                        check_cancel(cancel)?;
                        ancestors.push(Fragment {
                            message,
                            block,
                            start: 0,
                            end: block.text.len(),
                            is_context: true,
                        });
                    }
                }
            }
        }
        let mut buf = RunBuf {
            run_key: &run.key,
            ordinal: 0,
            recent_context: select_context(&ancestors, env.context_limit(), tok, cancel)?,
            current: Vec::new(),
            tokens: 0,
            bytes: 0,
            time: None,
        };
        for key in &run.messages {
            check_cancel(cancel)?;
            let Some(message) = by_key.get(key.as_str()) else {
                continue;
            };
            for block in &message.blocks {
                check_cancel(cancel)?;
                let mut start = 0usize;
                while start < block.text.len() {
                    check_cancel(cancel)?;
                    if !buf.current.is_empty()
                        && (buf.tokens >= cfg.target_new_tokens.max(1)
                            || buf.bytes >= MAX_CHUNK_NEW_BYTES
                            || buf.current.len() >= MAX_CHUNK_FRAGMENTS)
                    {
                        flush_current(&env, &mut plans, &mut buf, false)?;
                    }
                    let mut context = if buf.current.is_empty() {
                        buf.recent_context.clone()
                    } else {
                        fit_context(&env, &buf.recent_context, &buf.current)?
                    };
                    let mut full_base = context.clone();
                    full_base.extend(buf.current.iter().copied());
                    let rest = Fragment {
                        message,
                        block,
                        start,
                        end: block.text.len(),
                        is_context: false,
                    };
                    let prefix_fit =
                        largest_prefix(&env, full_base, buf.current.clone(), rest, buf.tokens)?;
                    let mut take = prefix_fit.map_or(0, |fit| fit.bytes);
                    if take == 0 && !buf.current.is_empty() {
                        flush_current(&env, &mut plans, &mut buf, false)?;
                        continue;
                    }
                    if take == 0 {
                        context.clear();
                        take = block.text[start..]
                            .char_indices()
                            .nth(1)
                            .map(|(i, _)| i)
                            .unwrap_or(block.text.len() - start);
                    }
                    let fragment = Fragment {
                        end: start + take,
                        ..rest
                    };
                    let rendered_tokens = if let Some(fit) = prefix_fit {
                        fit.rendered_tokens
                    } else {
                        let mut candidate = context.clone();
                        candidate.extend(buf.current.iter().copied());
                        candidate.push(fragment);
                        count_tokens(tok, &render(&candidate), cancel)?
                    };
                    if rendered_tokens > cfg.max_rendered_tokens {
                        if !buf.current.is_empty() {
                            flush_current(&env, &mut plans, &mut buf, false)?;
                            continue;
                        }
                        return Err(ChunkError::TokenLimit);
                    }
                    buf.current.push(fragment);
                    buf.bytes += take;
                    buf.tokens = match prefix_fit {
                        Some(fit) => fit.new_tokens,
                        None => count_tokens(tok, &render(&buf.current), cancel)?,
                    };
                    if message.timestamp.is_some() {
                        buf.time = message.timestamp;
                    }
                    start += take;
                }
            }
        }
        flush_current(&env, &mut plans, &mut buf, true)?;
    }
    check_cancel(cancel)?;
    Ok(plans)
}

fn fit_context<'a>(
    env: &Env<'_>,
    recent: &[Fragment<'a>],
    new: &[Fragment<'a>],
) -> Result<Vec<Fragment<'a>>, ChunkError> {
    let Env { cfg, tok, cancel } = *env;
    let budget = cfg
        .max_rendered_tokens
        .saturating_sub(count_tokens(tok, &render(new), cancel)?);
    let mut context = select_context(recent, cfg.overlap_tokens.min(200).min(budget), tok, cancel)?;
    while !context.is_empty() {
        check_cancel(cancel)?;
        let mut all = context.clone();
        all.extend(new.iter().copied());
        if count_tokens(tok, &render(&all), cancel)? <= cfg.max_rendered_tokens {
            break;
        }
        context.remove(0);
    }
    Ok(context)
}

fn flush_current<'a>(
    env: &Env<'_>,
    plans: &mut Vec<ChunkPlan>,
    buf: &mut RunBuf<'a>,
    is_tail: bool,
) -> Result<(), ChunkError> {
    if buf.current.is_empty() {
        return Ok(());
    }
    let context = fit_context(env, &buf.recent_context, &buf.current)?;
    plans.push(plan(env, buf, &context, is_tail)?);
    buf.ordinal += 1;
    let candidates = buf
        .recent_context
        .iter()
        .chain(buf.current.iter())
        .copied()
        .collect::<Vec<_>>();
    buf.recent_context = select_context(&candidates, env.context_limit(), env.tok, env.cancel)?;
    buf.current.clear();
    buf.tokens = 0;
    buf.bytes = 0;
    buf.time = None;
    Ok(())
}
