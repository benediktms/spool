use spool::{
    domain::{BlockOrigin, MessageEntry, RecordKind, RecordMeta, Role, SourceLocator, TextBlock},
    graph::SessionGraph,
    ingest::chunk::{ChunkConfig, ChunkMessage, TokenCounter, plan_chunks},
};
struct Bytes;
impl TokenCounter for Bytes {
    fn identity(&self) -> &str {
        "test-byte-counter-v1"
    }
    fn count(&self, s: &str) -> usize {
        s.chars().count()
    }
    fn prefix_within(&self, s: &str, n: usize) -> usize {
        s.char_indices().nth(n).map(|(i, _)| i).unwrap_or(s.len())
    }
}
struct MeasuredTokenizer {
    inner: std::sync::Arc<dyn TokenCounter>,
    count_calls: std::sync::atomic::AtomicUsize,
    count_bytes: std::sync::atomic::AtomicUsize,
    prefix_calls: std::sync::atomic::AtomicUsize,
    prefix_bytes: std::sync::atomic::AtomicUsize,
}
impl TokenCounter for MeasuredTokenizer {
    fn identity(&self) -> &str {
        self.inner.identity()
    }
    fn count(&self, text: &str) -> usize {
        self.count_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.count_bytes
            .fetch_add(text.len(), std::sync::atomic::Ordering::Relaxed);
        self.inner.count(text)
    }
    fn prefix_within(&self, text: &str, max_tokens: usize) -> usize {
        self.prefix_calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.prefix_bytes
            .fetch_add(text.len(), std::sync::atomic::Ordering::Relaxed);
        self.inner.prefix_within(text, max_tokens)
    }
}
fn graph(text: &str) -> (SessionGraph, Vec<ChunkMessage>) {
    let d = spool::domain::digest(text.as_bytes());
    let records = vec![RecordMeta {
        key: "m1".into(),
        native_id: Some("m1".into()),
        parent_key: None,
        kind: RecordKind::Message,
        timestamp: Some(10),
        turn_id: None,
        locator: SourceLocator { offset: 0, len: 1 },
        digest: d,
    }];
    let entries = vec![MessageEntry {
        record_key: "m1".into(),
        role: Role::User,
        phase: None,
        blocks: vec![TextBlock {
            index: 0,
            origin: BlockOrigin::Authored,
            text: text.into(),
        }],
        attachments: vec![],
        supersedes: vec![],
    }];
    let (g, _) = SessionGraph::build(&records, &entries);
    (
        g,
        vec![ChunkMessage {
            key: "m1".into(),
            role: Role::User,
            timestamp: Some(10),
            blocks: entries[0].blocks.clone(),
        }],
    )
}
#[test]
fn fork_runs_get_bounded_overlap_from_their_shared_parent() {
    let records = vec![
        RecordMeta {
            key: "root".into(),
            native_id: Some("root".into()),
            parent_key: None,
            kind: RecordKind::Message,
            timestamp: None,
            turn_id: None,
            locator: SourceLocator { offset: 0, len: 1 },
            digest: [0; 32],
        },
        RecordMeta {
            key: "branch-a".into(),
            native_id: Some("branch-a".into()),
            parent_key: Some("root".into()),
            kind: RecordKind::Message,
            timestamp: None,
            turn_id: None,
            locator: SourceLocator { offset: 1, len: 1 },
            digest: [0; 32],
        },
        RecordMeta {
            key: "branch-b".into(),
            native_id: Some("branch-b".into()),
            parent_key: Some("root".into()),
            kind: RecordKind::Message,
            timestamp: None,
            turn_id: None,
            locator: SourceLocator { offset: 2, len: 1 },
            digest: [0; 32],
        },
    ];
    let entries = vec![
        MessageEntry {
            record_key: "root".into(),
            role: Role::User,
            phase: None,
            blocks: vec![TextBlock {
                index: 0,
                origin: BlockOrigin::Authored,
                text: "ROOT-HISTORY-0123456789".into(),
            }],
            attachments: vec![],
            supersedes: vec![],
        },
        MessageEntry {
            record_key: "branch-a".into(),
            role: Role::User,
            phase: None,
            blocks: vec![TextBlock {
                index: 0,
                origin: BlockOrigin::Authored,
                text: "branch-A-0123456789".into(),
            }],
            attachments: vec![],
            supersedes: vec![],
        },
        MessageEntry {
            record_key: "branch-b".into(),
            role: Role::User,
            phase: None,
            blocks: vec![TextBlock {
                index: 0,
                origin: BlockOrigin::Authored,
                text: "branch-B-0123456789".into(),
            }],
            attachments: vec![],
            supersedes: vec![],
        },
    ];
    let (graph, _) = SessionGraph::build(&records, &entries);
    let messages: Vec<_> = entries
        .iter()
        .map(|entry| ChunkMessage {
            key: entry.record_key.clone(),
            role: entry.role,
            timestamp: None,
            blocks: entry.blocks.clone(),
        })
        .collect();
    let cfg = ChunkConfig {
        target_new_tokens: 100,
        max_rendered_tokens: 45,
        overlap_tokens: 17,
        version: 1,
    };
    let plans = plan_chunks(
        &graph,
        &messages,
        &cfg,
        &Bytes,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .unwrap();
    for branch in ["branch-a", "branch-b"] {
        let plan = plans.iter().find(|plan| plan.run_key == branch).unwrap();
        assert!(plan.rendered_tokens <= cfg.max_rendered_tokens);
        assert!(plan.spans.iter().any(|span| span.message_key == "root"
            && span.is_context
            && span.start > 0
            && span.end == entries[0].blocks[0].text.len()));
    }
}
#[test]
fn oversized_unicode_block_is_fully_covered_at_utf8_boundaries() {
    let text = "abc🙂defghijkl";
    let (g, m) = graph(text);
    let cfg = ChunkConfig {
        target_new_tokens: 5,
        max_rendered_tokens: 12,
        overlap_tokens: 0,
        version: 1,
    };
    let plans = plan_chunks(
        &g,
        &m,
        &cfg,
        &Bytes,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .unwrap();
    assert!(plans.len() > 1);
    let mut ranges: Vec<_> = plans
        .iter()
        .flat_map(|p| p.spans.iter().map(|s| (s.start, s.end)))
        .collect();
    ranges.sort();
    assert_eq!(ranges.first().unwrap().0, 0);
    assert_eq!(ranges.last().unwrap().1, text.len());
    for w in ranges.windows(2) {
        assert_eq!(w[0].1, w[1].0)
    }
    for p in &plans {
        assert!(p.rendered_tokens <= cfg.max_rendered_tokens);
        assert!(!p.spans.is_empty());
    }
}
#[test]
fn same_input_has_stable_input_hashes() {
    let (g, m) = graph("repeatable text");
    let cfg = ChunkConfig {
        target_new_tokens: 8,
        max_rendered_tokens: 20,
        overlap_tokens: 2,
        version: 1,
    };
    let a = plan_chunks(
        &g,
        &m,
        &cfg,
        &Bytes,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .unwrap();
    let b = plan_chunks(
        &g,
        &m,
        &cfg,
        &Bytes,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(a, b);
}

#[test]
fn large_assistant_message_chunks_linearly_with_bounded_overlap() {
    use std::time::{Duration, Instant};

    const SOURCE_BYTES: usize = 2 * 1024 * 1024;
    let mut pattern = "word ".repeat(800);
    pattern.push_str("\n```rust\nfn greet() { println!(\"café 日本語 👋\"); }\n```\n");
    let mut text = String::with_capacity(SOURCE_BYTES);
    while text.len() + pattern.len() <= SOURCE_BYTES {
        text.push_str(&pattern);
    }
    for _ in 0..SOURCE_BYTES - text.len() {
        text.push('x');
    }
    assert_eq!(text.len(), SOURCE_BYTES);

    let config = ChunkConfig {
        target_new_tokens: 1_000,
        max_rendered_tokens: 1_200,
        overlap_tokens: 200,
        version: 1,
    };
    let started = Instant::now();
    let worker_text = text.clone();
    let tokenizer = std::sync::Arc::new(MeasuredTokenizer {
        inner: spool::embeddings::tokenizer::tokenizer(),
        count_calls: std::sync::atomic::AtomicUsize::new(0),
        count_bytes: std::sync::atomic::AtomicUsize::new(0),
        prefix_calls: std::sync::atomic::AtomicUsize::new(0),
        prefix_bytes: std::sync::atomic::AtomicUsize::new(0),
    });
    let worker_tokenizer = tokenizer.clone();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let (graph, mut messages) = graph(&worker_text);
        messages[0].role = Role::Assistant;
        let cancel = std::sync::atomic::AtomicBool::new(false);
        let first = plan_chunks(
            &graph,
            &messages,
            &config,
            worker_tokenizer.as_ref(),
            &cancel,
        )
        .unwrap();
        if sender.send(first).is_err() {
            return;
        }
        let repeated = plan_chunks(
            &graph,
            &messages,
            &config,
            worker_tokenizer.as_ref(),
            &cancel,
        )
        .unwrap();
        let _ = sender.send(repeated);
    });
    let plans = receiver.recv_timeout(Duration::from_secs(20)).unwrap_or_else(|_| {
        panic!(
            "2 MiB message did not chunk within 20 seconds: count calls={} bytes={}, prefix calls={} bytes={}",
            tokenizer.count_calls.load(std::sync::atomic::Ordering::Relaxed),
            tokenizer.count_bytes.load(std::sync::atomic::Ordering::Relaxed),
            tokenizer.prefix_calls.load(std::sync::atomic::Ordering::Relaxed),
            tokenizer.prefix_bytes.load(std::sync::atomic::Ordering::Relaxed),
        )
    });
    let remaining = Duration::from_secs(20)
        .checked_sub(started.elapsed())
        .expect("first chunking pass exceeded 20 seconds");
    let repeated = receiver
        .recv_timeout(remaining)
        .expect("determinism pass exceeded the 20-second total limit");
    assert!(started.elapsed() < Duration::from_secs(20));
    assert_eq!(plans, repeated);
    assert!(plans.len() > 1);

    let mut source_ranges: Vec<_> = plans
        .iter()
        .flat_map(|plan| {
            plan.spans
                .iter()
                .filter(|span| !span.is_context)
                .map(|span| (span.start, span.end))
        })
        .collect();
    source_ranges.sort_unstable();
    let mut covered_until = 0;
    for (start, end) in source_ranges {
        assert_eq!(
            start, covered_until,
            "source bytes must have no gaps or overlap"
        );
        assert!(end > start);
        covered_until = end;
    }
    assert_eq!(covered_until, text.len());

    for plan in &plans {
        assert!(plan.rendered_tokens <= config.max_rendered_tokens);
        let mut rendered_context = String::new();
        for span in plan.spans.iter().filter(|span| span.is_context) {
            if !rendered_context.is_empty() {
                rendered_context.push_str("\n\n");
            }
            rendered_context.push_str("ASSISTANT: ");
            rendered_context.push_str(&text[span.start..span.end]);
        }
        assert!(tokenizer.count(&rendered_context) <= config.overlap_tokens);
    }
    assert!(
        tokenizer
            .prefix_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
            <= tokenizer
                .prefix_calls
                .load(std::sync::atomic::Ordering::Relaxed)
                * 8
                * 1024,
        "each tokenizer prefix scan must stay within its bounded window"
    );
}

#[test]
fn cancellation_between_fragments_returns_no_partial_plan() {
    struct CancelOnSecondPrefix {
        cancelled: std::sync::Arc<std::sync::atomic::AtomicBool>,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl TokenCounter for CancelOnSecondPrefix {
        fn identity(&self) -> &str {
            "cancel-on-second-prefix"
        }
        fn count(&self, text: &str) -> usize {
            text.chars().count()
        }
        fn prefix_within(&self, text: &str, max_tokens: usize) -> usize {
            let bytes = text
                .char_indices()
                .nth(max_tokens)
                .map(|(index, _)| index)
                .unwrap_or(text.len());
            if self
                .calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                == 1
            {
                self.cancelled
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
            bytes
        }
    }

    let (graph, messages) = graph(&"fragment ".repeat(8));
    let config = ChunkConfig {
        target_new_tokens: 8,
        max_rendered_tokens: 40,
        overlap_tokens: 4,
        version: 1,
    };
    let cancelled = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let tokenizer = CancelOnSecondPrefix {
        cancelled: cancelled.clone(),
        calls: std::sync::atomic::AtomicUsize::new(0),
    };
    assert_eq!(
        plan_chunks(&graph, &messages, &config, &tokenizer, &cancelled),
        Err(spool::ingest::chunk::ChunkError::Cancelled)
    );
}
#[test]
fn long_unicode_code_chunks_reassemble_beyond_bounded_windows() {
    let pattern = "```rust\nfn café() { let greeting = \"日本語 👋\"; }\n```\n";
    let text = pattern.repeat(6_000);
    let (graph, messages) = graph(&text);
    let config = ChunkConfig {
        target_new_tokens: 1_000,
        max_rendered_tokens: 1_200,
        overlap_tokens: 200,
        version: 1,
    };
    let plans = plan_chunks(
        &graph,
        &messages,
        &config,
        &Bytes,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .unwrap();
    let mut ranges: Vec<_> = plans
        .iter()
        .flat_map(|plan| {
            plan.spans
                .iter()
                .filter(|span| !span.is_context)
                .map(|span| (span.start, span.end))
        })
        .collect();
    ranges.sort_unstable();
    let mut covered_until = 0;
    let mut reconstructed = String::with_capacity(text.len());
    for (start, end) in ranges {
        assert_eq!(start, covered_until);
        reconstructed.push_str(&text[start..end]);
        covered_until = end;
    }
    assert_eq!(covered_until, text.len());
    assert_eq!(reconstructed, text);
    assert!(plans.iter().all(|plan| plan.rendered_tokens <= 1_200));
    assert!(plans.len() > 32);
}
#[test]
fn fragment_cap_splits_without_losing_block_text() {
    let blocks: Vec<_> = (0..300)
        .map(|index| TextBlock {
            index,
            origin: BlockOrigin::Authored,
            text: "漢".into(),
        })
        .collect();
    let messages = [MessageEntry {
        record_key: "many-blocks".into(),
        role: Role::User,
        phase: None,
        blocks: blocks.clone(),
        attachments: Vec::new(),
        supersedes: Vec::new(),
    }];
    let records = [RecordMeta {
        key: "many-blocks".into(),
        native_id: Some("many-blocks".into()),
        parent_key: None,
        kind: RecordKind::Message,
        timestamp: None,
        turn_id: None,
        locator: SourceLocator { offset: 0, len: 1 },
        digest: [0; 32],
    }];
    let (graph, warnings) = SessionGraph::build(&records, &messages);
    assert!(warnings.is_empty());
    let chunk_messages = [ChunkMessage {
        key: "many-blocks".into(),
        role: Role::User,
        timestamp: None,
        blocks: blocks.clone(),
    }];
    let config = ChunkConfig {
        target_new_tokens: 2_000,
        max_rendered_tokens: 1_200,
        overlap_tokens: 200,
        version: 1,
    };
    let plans = plan_chunks(
        &graph,
        &chunk_messages,
        &config,
        &Bytes,
        &std::sync::atomic::AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(
        plans[0]
            .spans
            .iter()
            .filter(|span| !span.is_context)
            .count(),
        128
    );
    assert!(plans.iter().all(|plan| plan.rendered_tokens <= 1_200));
    for block in &blocks {
        let mut ranges: Vec<_> = plans
            .iter()
            .flat_map(|plan| plan.spans.iter())
            .filter(|span| !span.is_context && span.block_index == block.index)
            .map(|span| (span.start, span.end))
            .collect();
        ranges.sort_unstable();
        assert_eq!(ranges, [(0, block.text.len())]);
        assert_eq!(&block.text[ranges[0].0..ranges[0].1], "漢");
    }
}
