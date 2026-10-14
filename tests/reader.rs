//! `read_session` acceptance tests (spec 19.10 / read-session-contract section 12).
//! Real index, real ingest engine and stager, real OMP adapter, synthetic files, no network.

#[path = "reader_common/mod.rs"]
mod common;

use common::*;
use serde_json::{Value, json};
use spool::mcp::budget::tool_result_bytes;
use spool::transcript::refs::{ItemKind, ItemRef};
use std::io::Write;

fn text_of(item: &Value) -> String {
    String::from_utf8(frag_bytes(&item["blocks"][0])).unwrap()
}

fn msg_text(i: usize) -> String {
    format!("message number {i} qwz{i}")
}

fn items(v: &Value) -> &Vec<Value> {
    v["items"].as_array().unwrap()
}

fn append(path: &std::path::Path, lines: &[String]) {
    let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
    f.write_all(join(lines).as_bytes()).unwrap();
}

fn forked() -> Vec<String> {
    vec![
        omp_header(),
        omp_msg("m0", None, "user", "root question"),
        omp_msg("m1", Some("m0"), "assistant", "shared answer"),
        omp_msg("m2", Some("m1"), "user", "branch A follow up"),
        omp_msg("m3", Some("m2"), "assistant", "branch A answer"),
        omp_msg("x1", Some("m1"), "user", "branch B follow up"),
        omp_msg("x2", Some("x1"), "assistant", "branch B answer"),
    ]
}

// ---------------------------------------------------------------------------------------------
// Selection and navigation
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn known_session_opens_without_search_and_never_calls_the_provider() {
    let env = Env::new().await;
    let (sid, _) = env.indexed("a.jsonl", &linear(6)).await;
    let calls = env.provider.calls();
    let requests = env.n("SELECT count(*) FROM embedding_requests").await;
    let r = env.ok(json!({"session_id": sid})).await;
    assert_eq!(items(&r).len(), 6);
    for (i, it) in items(&r).iter().enumerate() {
        assert_eq!(it["kind"], "message");
        assert_eq!(it["role"], if i % 2 == 0 { "user" } else { "assistant" });
        assert_eq!(text_of(it), msg_text(i));
        assert_eq!(it["provenance"]["origin"], "verified_source");
        assert_eq!(it["item_end"], true);
    }
    assert_eq!(r["view"], "conversation");
    assert_eq!(r["source_policy"], "prefer_source");
    assert_eq!(r["snapshot"]["kind"], "indexed");
    assert_eq!(
        r["snapshot"]["lease_expires_at"],
        spool::transcript::types::iso(T0 + 1_800_000)
    );
    assert_eq!(r["page"]["selection_exhausted"], true);
    assert_eq!(r["page"]["stop_reason"], "selection_end");
    assert!(r["page"]["next_cursor"].is_null());
    assert_eq!(env.provider.calls(), calls, "reads never call a provider");
    assert_eq!(
        env.n("SELECT count(*) FROM embedding_requests").await,
        requests,
        "reads never create embedding requests"
    );
}

#[tokio::test]
async fn unindexed_known_session_reads_from_source_without_embedding_work() {
    let env = Env::new().await;
    let (sid, _) = env.add_omp("u.jsonl", &linear(4)).await;
    let r = env.ok(json!({"session_id": sid})).await;
    assert_eq!(items(&r).len(), 4);
    assert_eq!(r["snapshot"]["kind"], "source_read");
    assert_eq!(
        env.n("SELECT count(*) FROM snapshots WHERE state = 'read_only'")
            .await,
        1
    );
    assert_eq!(
        env.n("SELECT count(*) FROM chunks").await,
        0,
        "no search index is built"
    );
    assert_eq!(
        env.n("SELECT count(*) FROM sessions WHERE active_snapshot_id IS NOT NULL")
            .await,
        0,
        "nothing published"
    );
    assert_eq!(env.n("SELECT count(*) FROM embedding_requests").await, 0);
    assert_eq!(env.provider.calls(), 0);
    let e = env
        .err(json!({"session_id": sid, "source_policy": "indexed_only"}))
        .await;
    assert_eq!(e.code, "SESSION_NOT_INDEXED");
    // require_source also works against the staged read manifest
    let r = env
        .ok(json!({"session_id": sid, "source_policy": "require_source"}))
        .await;
    assert_eq!(items(&r).len(), 4);
}

#[tokio::test]
async fn match_spanning_messages_is_deduplicated_and_reopens_in_both_views() {
    let env = Env::new().await;
    let (sid, _) = env.indexed("a.jsonl", &linear(8)).await;
    let first = env.ok(json!({"session_id": sid})).await;
    let anchor = env.anchor_of(&first);
    // one match covering two messages (and two blocks of the same chunk)
    let (t2, t3) = (msg_text(2), msg_text(3));
    let m = env.match_ref(&anchor, &[("m002", 0, &t2), ("m003", 0, &t3)]);
    for view in ["conversation", "with_tools"] {
        let r = env.ok(json!({"session_id": sid, "view": view, "selection": {"kind": "match", "match_ref": m, "before": 1, "after": 1}})).await;
        let texts: Vec<String> = items(&r).iter().map(text_of).collect();
        assert_eq!(
            texts,
            (1..=4).map(msg_text).collect::<Vec<_>>(),
            "complete messages, no duplicated overlap ({view})"
        );
        assert_eq!(
            r["selection"]["before"],
            json!({"requested": 1, "resolved": 1})
        );
        assert_eq!(
            r["selection"]["after"],
            json!({"requested": 1, "resolved": 1})
        );
        assert_eq!(
            r["selection"]["anchor_message_refs"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(r["view"], view);
        assert!(
            r["navigation"]["before_message_ref"].is_string()
                && r["navigation"]["after_message_ref"].is_string()
        );
        // pinned observation: the same lease as the match handle
        assert_eq!(env.anchor_of(&r).lease, anchor.lease);
    }
    // altered content binding is rejected, not rebased
    let wrong = env.match_ref(&anchor, &[("m002", 0, "something else entirely")]);
    let e = env
        .err(json!({"session_id": sid, "selection": {"kind": "match", "match_ref": wrong}}))
        .await;
    assert_eq!(e.code, "STALE_REFERENCE");
}

#[tokio::test]
async fn mixed_block_envelope_counts_as_one_context_message() {
    let env = Env::new().await;
    let lines = vec![
        omp_header(),
        omp_msg("m0", None, "user", "first"),
        omp_msg_blocks("m1", Some("m0"), "assistant", &["part one", "part two"]),
        omp_control("c1", "m1"),
        omp_msg("m2", Some("c1"), "user", "third"),
    ];
    let (sid, _) = env.indexed("mixed.jsonl", &lines).await;
    let first = env.ok(json!({"session_id": sid})).await;
    assert_eq!(
        items(&first).len(),
        3,
        "control record and extra blocks are not messages"
    );
    let m1 = items(&first)[1]["message_ref"]
        .as_str()
        .unwrap()
        .to_string();
    let r = env.ok(json!({"session_id": sid, "selection": {"kind": "message", "message_ref": m1, "before": 1, "after": 1}})).await;
    assert_eq!(items(&r).len(), 3);
    let blocks = items(&r)[1]["blocks"].as_array().unwrap();
    assert_eq!(blocks.len(), 2);
    assert_ne!(blocks[0]["block_ref"], blocks[1]["block_ref"]);
    assert_eq!(
        String::from_utf8(frag_bytes(&blocks[1])).unwrap(),
        "part two"
    );
}

#[tokio::test]
async fn ranges_one_message_reversed_cross_session_cross_snapshot_and_sibling() {
    let env = Env::new().await;
    let (sid, path) = env.indexed("a.jsonl", &linear(6)).await;
    let (other, _) = env.indexed("b.jsonl", &linear(3)).await;
    let r = env.ok(json!({"session_id": sid})).await;
    let refs: Vec<String> = items(&r)
        .iter()
        .map(|i| i["message_ref"].as_str().unwrap().to_string())
        .collect();
    // inclusive, one message
    let one = env.ok(json!({"session_id": sid, "selection": {"kind": "range", "from_message_ref": refs[2], "through_message_ref": refs[2]}})).await;
    assert_eq!(items(&one).len(), 1);
    assert_eq!(text_of(&items(&one)[0]), msg_text(2));
    let span = env.ok(json!({"session_id": sid, "selection": {"kind": "range", "from_message_ref": refs[1], "through_message_ref": refs[3]}})).await;
    assert_eq!(
        items(&span).iter().map(text_of).collect::<Vec<_>>(),
        vec![msg_text(1), msg_text(2), msg_text(3)]
    );
    // reversed
    let e = env.err(json!({"session_id": sid, "selection": {"kind": "range", "from_message_ref": refs[3], "through_message_ref": refs[1]}})).await;
    assert_eq!(e.code, "INVALID_RANGE");
    // cross-session
    let o = env.ok(json!({"session_id": other})).await;
    let oref = items(&o)[0]["message_ref"].as_str().unwrap().to_string();
    let e = env.err(json!({"session_id": sid, "selection": {"kind": "range", "from_message_ref": refs[1], "through_message_ref": oref}})).await;
    assert_eq!(e.code, "INVALID_ARGUMENT");
    let e = env
        .err(json!({"session_id": sid, "selection": {"kind": "message", "message_ref": oref}}))
        .await;
    assert_eq!(e.code, "INVALID_ARGUMENT");
    // cross-snapshot: a newer snapshot of the same session
    append(&path, &[omp_msg("m006", Some("m005"), "user", "later")]);
    env.index(&sid).await.unwrap();
    let r2 = env.ok(json!({"session_id": sid})).await;
    assert_eq!(items(&r2).len(), 7);
    let new_ref = items(&r2)[6]["message_ref"].as_str().unwrap().to_string();
    let e = env.err(json!({"session_id": sid, "selection": {"kind": "range", "from_message_ref": refs[1], "through_message_ref": new_ref}})).await;
    assert_eq!(e.code, "INVALID_RANGE");
}

#[tokio::test]
async fn sibling_branch_range_and_branch_choice() {
    let env = Env::new().await;
    let (sid, _) = env.indexed("fork.jsonl", &forked()).await;
    // no implied current branch: a choice is returned, never an invented one
    let e = env.err(json!({"session_id": sid})).await;
    assert_eq!(e.code, "BRANCH_REQUIRED");
    assert_eq!(e.recovery.as_deref(), Some("select_branch"));
    let d = e.details.clone().unwrap();
    assert_eq!(d["branch_options_truncated"], false);
    let opts = d["branch_options"].as_array().unwrap();
    assert_eq!(opts.len(), 2);
    let mut seen = Vec::new();
    for o in opts {
        let r = env.ok(json!({"session_id": sid, "selection": {"kind": "session", "branch_ref": o["branch_ref"]}})).await;
        seen.push(items(&r).iter().map(text_of).collect::<Vec<_>>());
        assert_eq!(
            items(&r).len(),
            4,
            "root path to the leaf only, never sibling messages"
        );
        assert_eq!(r["selection"]["branch_ref"], o["branch_ref"]);
    }
    seen.sort();
    assert_eq!(seen[0][3], "branch A answer");
    assert_eq!(seen[1][3], "branch B answer");
    // refs from one read
    let a = env.ok(json!({"session_id": sid, "selection": {"kind": "session", "branch_ref": opts[0]["branch_ref"]}})).await;
    let b = env.ok(json!({"session_id": sid, "selection": {"kind": "session", "branch_ref": opts[1]["branch_ref"]}})).await;
    let (a_last, b_last) = (
        items(&a)[3]["message_ref"].as_str().unwrap().to_string(),
        items(&b)[3]["message_ref"].as_str().unwrap().to_string(),
    );
    let e = env.err(json!({"session_id": sid, "selection": {"kind": "range", "from_message_ref": a_last, "through_message_ref": b_last}})).await;
    assert_eq!(e.code, "INVALID_RANGE", "sibling-branch endpoints");
    // a message above the fork with enough `after` context needs a choice, restricted to leaves below it
    let shared = items(&a)[1]["message_ref"].as_str().unwrap().to_string();
    let e = env.err(json!({"session_id": sid, "selection": {"kind": "message", "message_ref": shared, "before": 0, "after": 1}})).await;
    assert_eq!(e.code, "BRANCH_REQUIRED");
    assert_eq!(
        e.details.unwrap()["branch_options"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    // ...but not when the window ends before the fork
    let r = env.ok(json!({"session_id": sid, "selection": {"kind": "message", "message_ref": shared, "before": 1, "after": 0}})).await;
    assert_eq!(items(&r).len(), 2);
    // a branch that does not contain the anchored history is rejected
    let a_branch = a["selection"]["branch_ref"].as_str().unwrap().to_string();
    let b_msg = items(&b)[2]["message_ref"].as_str().unwrap().to_string();
    let e = env.err(json!({"session_id": sid, "selection": {"kind": "message", "message_ref": b_msg, "branch_ref": a_branch}})).await;
    assert_eq!(e.code, "INVALID_RANGE");
}

#[tokio::test]
async fn branch_options_are_bounded_to_fifty_and_to_the_response_budget() {
    let env = Env::new().await;
    let mut lines = vec![omp_header(), omp_msg("m0", None, "user", "root")];
    for i in 0..60 {
        lines.push(omp_msg(
            &format!("a{i:02}"),
            Some("m0"),
            "assistant",
            &format!("leaf {i}"),
        ));
    }
    let (sid, _) = env.indexed("many.jsonl", &lines).await;
    let e = env
        .err(json!({"session_id": sid, "max_bytes": 262144}))
        .await;
    let d = e.details.clone().unwrap();
    assert_eq!(
        d["branch_options"].as_array().unwrap().len(),
        50,
        "count limit"
    );
    assert_eq!(d["branch_options_truncated"], true);
    for max in [4096usize, 16384] {
        let e = env.err(json!({"session_id": sid, "max_bytes": max})).await;
        let d = e.details.clone().unwrap();
        let n = d["branch_options"].as_array().unwrap().len();
        assert!(
            n < 50,
            "{max}: the byte budget, not the count, bounds the list ({n})"
        );
        assert_eq!(d["branch_options_truncated"], true);
        assert!(
            tool_result_bytes(&e.to_tool_json(), true) <= max,
            "{max}: serialized error within budget"
        );
    }
    // few leaves: nothing omitted
    let mut few = vec![omp_header(), omp_msg("m0", None, "user", "root")];
    for i in 0..3 {
        few.push(omp_msg(&format!("a{i}"), Some("m0"), "assistant", "x"));
    }
    let (sid, _) = env.indexed("few.jsonl", &few).await;
    let e = env.err(json!({"session_id": sid})).await;
    assert_eq!(e.details.unwrap()["branch_options_truncated"], false);
}

#[tokio::test]
async fn fixed_match_window_continues_to_exhaustion_then_widens_by_navigation() {
    let env = Env::new().await;
    let (sid, _) = env.indexed("a.jsonl", &linear(10)).await;
    let first = env.ok(json!({"session_id": sid})).await;
    let anchor = env.anchor_of(&first);
    let t4 = msg_text(4);
    let m = env.match_ref(&anchor, &[("m004", 0, &t4)]);
    let pages = env.pages(json!({"session_id": sid, "max_items": 1, "max_bytes": 32768, "selection": {"kind": "match", "match_ref": m, "before": 1, "after": 1}}), 32768).await;
    assert_eq!(pages.len(), 3);
    assert_eq!(pages[0]["page"]["stop_reason"], "item_limit");
    assert_eq!(pages[2]["page"]["stop_reason"], "selection_end");
    let all: Vec<String> = pages
        .iter()
        .flat_map(|p| items(p).iter().map(text_of))
        .collect();
    assert_eq!(
        all,
        vec![msg_text(3), msg_text(4), msg_text(5)],
        "no overlap or skip across cursors"
    );
    // widen: navigation refs open NEW reads and may deliberately repeat context
    let nav = &pages[2]["navigation"];
    let after = nav["after_message_ref"].as_str().expect("after ref");
    let before = nav["before_message_ref"].as_str().expect("before ref");
    let w = env.ok(json!({"session_id": sid, "selection": {"kind": "message", "message_ref": after, "before": 1, "after": 1}})).await;
    assert_eq!(
        items(&w).iter().map(text_of).collect::<Vec<_>>(),
        vec![msg_text(5), msg_text(6), msg_text(7)]
    );
    let w = env.ok(json!({"session_id": sid, "selection": {"kind": "message", "message_ref": before, "before": 0, "after": 0}})).await;
    assert_eq!(text_of(&items(&w)[0]), msg_text(2));
    // the lease of the original match carries through navigation
    assert_eq!(env.anchor_of(&w).lease, anchor.lease);
}

#[tokio::test]
async fn item_selection_returns_only_that_item_and_checks_view_class() {
    let env = Env::new().await;
    let (sid, _) = env.indexed("a.jsonl", &linear(4)).await;
    let r = env.ok(json!({"session_id": sid})).await;
    let item_ref = items(&r)[2]["item_ref"].as_str().unwrap().to_string();
    let one = env
        .ok(json!({"session_id": sid, "selection": {"kind": "item", "item_ref": item_ref}}))
        .await;
    assert_eq!(items(&one).len(), 1);
    assert_eq!(text_of(&items(&one)[0]), msg_text(2));
    assert_eq!(one["selection"]["item_ref"], json!(item_ref));
    // a tool item class is not permitted in the conversation view
    let anchor = env.anchor_of(&r);
    let rec = env.decode_item(&item_ref);
    let tool = ItemRef {
        anchor,
        kind: ItemKind::ToolCall,
        record: rec.record.clone(),
        block: Some(1),
        digest: rec.digest.clone(),
    }
    .encode(&env.keys.current());
    let e = env
        .err(json!({"session_id": sid, "selection": {"kind": "item", "item_ref": tool}}))
        .await;
    assert_eq!(e.code, "VIEW_NOT_SUPPORTED");
}

// ---------------------------------------------------------------------------------------------
// Views, policies and raw access
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn argument_validation_and_policy_rules() {
    let env = Env::new().await;
    let (sid, _) = env.indexed("a.jsonl", &linear(3)).await;
    let r = env
        .ok(json!({"session_id": sid, "view": "with_tools"}))
        .await;
    assert_eq!(
        r["source_policy"], "require_source",
        "non-conversation views default to require_source"
    );
    assert_eq!(items(&r).len(), 3, "no tool activity in this session");
    for (args, code) in [
        (json!({"session_id": sid, "bogus": 1}), "INVALID_ARGUMENT"),
        (json!({}), "INVALID_ARGUMENT"),
        (json!("text"), "INVALID_ARGUMENT"),
        (
            json!({"session_id": sid, "max_bytes": 100}),
            "INVALID_ARGUMENT",
        ),
        (
            json!({"session_id": sid, "max_bytes": 262145}),
            "INVALID_ARGUMENT",
        ),
        (
            json!({"session_id": sid, "max_items": 0}),
            "INVALID_ARGUMENT",
        ),
        (
            json!({"session_id": sid, "max_items": 201}),
            "INVALID_ARGUMENT",
        ),
        (
            json!({"session_id": sid, "selection": {"kind": "session", "extra": 1}}),
            "INVALID_ARGUMENT",
        ),
        (
            json!({"session_id": sid, "selection": {"kind": "session", "before": 1}}),
            "INVALID_ARGUMENT",
        ),
        (
            json!({"session_id": sid, "selection": {"kind": "weird"}}),
            "INVALID_ARGUMENT",
        ),
        (
            json!({"session_id": sid, "selection": {"kind": "message", "message_ref": "v1.g.1.x.y", "before": 21}}),
            "INVALID_ARGUMENT",
        ),
        (
            json!({"session_id": sid, "selection": {"kind": "message", "message_ref": "garbage"}}),
            "INVALID_ARGUMENT",
        ),
        (
            json!({"session_id": sid, "view": "with_tools", "source_policy": "indexed_only"}),
            "SOURCE_POLICY_CONFLICT",
        ),
        (
            json!({"session_id": sid, "view": "with_tools", "source_policy": "prefer_source"}),
            "SOURCE_POLICY_CONFLICT",
        ),
        (json!({"session_id": "ss_missing"}), "SESSION_NOT_FOUND"),
    ] {
        let e = env.err(args.clone()).await;
        assert_eq!(e.code, code, "{args}");
    }
    // wrong handle kind
    let m = items(&env.ok(json!({"session_id": sid})).await)[0]["message_ref"]
        .as_str()
        .unwrap()
        .to_string();
    let e = env
        .err(json!({"session_id": sid, "selection": {"kind": "match", "match_ref": m}}))
        .await;
    assert_eq!(e.code, "INVALID_ARGUMENT");
}

#[tokio::test]
async fn raw_view_requires_local_enablement() {
    let env = Env::new().await;
    let (sid, _) = env.indexed("a.jsonl", &linear(3)).await;
    let e = env
        .err(json!({"session_id": sid, "view": "raw_records"}))
        .await;
    assert_eq!(e.code, "FORBIDDEN");
    // a cursor issued under an enabled config stops working when enablement is removed
    let mut env = Env::with(|c| c.mcp.allow_raw_records = true).await;
    let (sid, _) = env.indexed("a.jsonl", &linear(6)).await;
    let r = env
        .ok(json!({"session_id": sid, "view": "raw_records", "max_items": 1}))
        .await;
    let cursor = r["page"]["next_cursor"].as_str().unwrap().to_string();
    env.reconfigure(|c| c.mcp.allow_raw_records = false);
    let e = env.err(json!({"cursor": cursor})).await;
    assert_eq!(
        e.code, "FORBIDDEN",
        "enablement is re-checked on continuation"
    );
}

#[tokio::test]
async fn raw_records_are_byte_exact_with_line_terminators() {
    let env = Env::with(|c| c.mcp.allow_raw_records = true).await;
    // CRLF terminators and non-ASCII text must survive byte for byte
    let lines = [
        omp_header(),
        omp_msg("m0", None, "user", "grüße 😀"),
        omp_msg("m1", Some("m0"), "assistant", "ok"),
    ];
    let content: String = lines.iter().map(|l| format!("{l}\r\n")).collect();
    let (sid, path) = env.add("omp", "crlf.jsonl", content.as_bytes()).await;
    env.index(&sid).await.unwrap();
    let r = env
        .ok(json!({"session_id": sid, "view": "raw_records"}))
        .await;
    assert_eq!(r["view"], "raw_records");
    assert_eq!(r["source_policy"], "require_source");
    let joined: Vec<u8> = items(&r)
        .iter()
        .flat_map(|i| frag_bytes(&i["blocks"][0]))
        .collect();
    assert_eq!(
        joined,
        std::fs::read(&path).unwrap(),
        "header + selected path, verbatim including CRLF"
    );
    for it in items(&r) {
        assert_eq!(it["kind"], "raw_record");
        assert_eq!(it["blocks"][0]["kind"], "raw_record");
        assert_eq!(it["blocks"][0]["fidelity"], "verbatim");
        assert_eq!(it["blocks"][0]["encoding"], "utf8");
        assert_eq!(it["provenance"]["origin"], "verified_source");
    }
    assert!(frag_bytes(&items(&r)[1]["blocks"][0]).ends_with(b"\r\n"));
    // raw item selection returns the originating record only
    let conv = env.ok(json!({"session_id": sid})).await;
    let item_ref = items(&conv)[0]["item_ref"].as_str().unwrap().to_string();
    let raw = env.ok(json!({"session_id": sid, "view": "raw_records", "selection": {"kind": "item", "item_ref": item_ref}})).await;
    assert_eq!(items(&raw).len(), 1);
    assert_eq!(
        frag_bytes(&items(&raw)[0]["blocks"][0]),
        format!("{}\r\n", lines[1]).into_bytes()
    );
    // range raw: records from the first selected message through the last
    let (r0, r1) = (
        items(&conv)[0]["message_ref"].as_str().unwrap(),
        items(&conv)[1]["message_ref"].as_str().unwrap(),
    );
    let rng = env.ok(json!({"session_id": sid, "view": "raw_records", "selection": {"kind": "range", "from_message_ref": r0, "through_message_ref": r1}})).await;
    assert_eq!(items(&rng).len(), 2);
    // explicit prefer_source / indexed_only are rejected for raw
    let e = env
        .err(json!({"session_id": sid, "view": "raw_records", "source_policy": "indexed_only"}))
        .await;
    assert_eq!(e.code, "SOURCE_POLICY_CONFLICT");
}

#[tokio::test]
async fn raw_view_of_a_malformed_record_is_verbatim_never_normalized() {
    let env = Env::with(|c| c.mcp.allow_raw_records = true).await;
    // a complete but invalid record is kept as an unknown record; raw access returns its exact bytes
    let content = format!(
        "{}\n{}\nnot json at all\n{}\n",
        omp_header(),
        omp_msg("m0", None, "user", "hi"),
        omp_msg("m1", Some("m0"), "assistant", "yo")
    );
    let (sid, _) = env.add("omp", "bad.jsonl", content.as_bytes()).await;
    env.index(&sid).await.unwrap();
    let (key, digest): (String, Vec<u8>) = sqlx::query_as("SELECT r.key, r.digest FROM records r JOIN sessions s ON s.active_snapshot_id = r.snapshot_id WHERE s.id = ? AND r.kind = 'unknown'")
        .bind(&sid)
        .fetch_one(&env.index.writer)
        .await
        .unwrap();
    let conv = env.ok(json!({"session_id": sid})).await;
    assert_eq!(items(&conv).len(), 2, "never conversation content");
    assert_eq!(conv["coverage"]["status"], "partial");
    let anchor = env.anchor_of(&conv);
    let item = ItemRef {
        anchor,
        kind: ItemKind::RawRecord,
        record: key,
        block: None,
        digest: spool::ids::hex(&digest)[..32].to_string(),
    }
    .encode(&env.keys.current());
    let raw = env.ok(json!({"session_id": sid, "view": "raw_records", "selection": {"kind": "item", "item_ref": item}})).await;
    assert_eq!(
        frag_bytes(&items(&raw)[0]["blocks"][0]),
        b"not json at all\n"
    );
    assert_eq!(items(&raw)[0]["blocks"][0]["fidelity"], "verbatim");
}

// ---------------------------------------------------------------------------------------------
// Pagination, fragmentation, budgets
// ---------------------------------------------------------------------------------------------

fn escape_heavy(bytes: usize) -> String {
    let unit = "q\"uote back\\slash\nline é€😀 tab\t";
    let mut out = String::with_capacity(bytes);
    for c in unit.chars().cycle() {
        if out.len() + c.len_utf8() > bytes {
            break;
        }
        out.push(c);
    }
    out
}

#[tokio::test]
async fn two_mib_assistant_text_fragments_under_32k_pages_and_reassembles_exactly() {
    let env = Env::new().await;
    let big = escape_heavy(2 * 1024 * 1024);
    let lines = vec![
        omp_header(),
        omp_msg("m0", None, "user", "question"),
        omp_msg("m1", Some("m0"), "assistant", &big),
        omp_msg("m2", Some("m1"), "user", "thanks"),
    ];
    // unindexed: chunking a 2 MiB message with the debug-build tokenizer is far slower than the read under test
    let (sid, _) = env.add_omp("big.jsonl", &lines).await;
    let pages = env.pages(json!({"session_id": sid}), 32768).await;
    assert!(pages.len() > 60, "{} pages", pages.len());
    for p in &pages {
        assert!(
            tool_result_bytes(p, false) <= 32768,
            "JSON escaping and the compatibility text copy are inside the budget"
        );
        assert_eq!(
            p["page"]["selection_exhausted"],
            p["page"]["next_cursor"].is_null()
        );
    }
    let blocks = reassemble(&pages);
    assert_eq!(blocks.len(), 3);
    assert_eq!(
        blocks[1].1,
        big.as_bytes(),
        "no missing or duplicated bytes, no split code point"
    );
    // the continued item keeps its item_ref; item_end only on its final page
    let mid: Vec<&Value> = pages
        .iter()
        .flat_map(|p| items(p).iter())
        .filter(|i| i["kind"] == "message" && i["role"] == "assistant")
        .collect();
    assert!(mid.len() > 60);
    assert!(mid.iter().all(|i| i["item_ref"] == mid[0]["item_ref"]));
    assert_eq!(mid.iter().filter(|i| i["item_end"] == true).count(), 1);
    assert_eq!(mid.last().unwrap()["item_end"], true);
    let first = &mid[0]["blocks"][0];
    assert_eq!(first["start_byte"], 0);
    assert_eq!(first["total_bytes"], big.len());
}

#[tokio::test]
async fn indexed_two_mib_assistant_message_reads_through_the_publish_path() {
    let env = Env::new().await;
    let big = escape_heavy(2 * 1024 * 1024);
    let lines = vec![
        omp_header(),
        omp_msg("m0", None, "user", "question"),
        omp_msg("m1", Some("m0"), "assistant", &big),
    ];
    let (sid, _) = env.indexed("bigidx.jsonl", &lines).await;
    let pages = env.pages(json!({"session_id": sid}), 32768).await;
    assert_eq!(
        pages[0]["snapshot"]["kind"], "indexed",
        "served from the published snapshot"
    );
    assert_eq!(reassemble(&pages)[1].1, big.as_bytes());
    // the indexed copy is byte-identical too
    let idx = env
        .pages(
            json!({"session_id": sid, "source_policy": "indexed_only"}),
            32768,
        )
        .await;
    assert_eq!(reassemble(&idx)[1].1, big.as_bytes());
    assert_eq!(
        idx[0]["items"][1]["provenance"]["origin"],
        "indexed_snapshot"
    );
}

#[tokio::test]
async fn two_mib_raw_record_fragments_and_reassembles_exactly() {
    let env = Env::with(|c| c.mcp.allow_raw_records = true).await;
    let big = escape_heavy(2 * 1024 * 1024);
    let lines = vec![omp_header(), omp_msg("m0", None, "user", &big)];
    let (sid, path) = env.add_omp("bigraw.jsonl", &lines).await;
    let pages = env
        .pages(json!({"session_id": sid, "view": "raw_records"}), 32768)
        .await;
    assert!(pages.len() > 60);
    let blocks = reassemble(&pages);
    assert_eq!(blocks.len(), 2);
    let file = std::fs::read(&path).unwrap();
    let header_len = join(&lines[..1]).len();
    assert_eq!(blocks[0].1, &file[..header_len]);
    assert_eq!(
        blocks[1].1,
        &file[header_len..],
        "raw record offsets are relative to the exact record bytes"
    );
}

#[tokio::test]
async fn max_items_counts_logical_items_including_a_continued_one() {
    let env = Env::new().await;
    let big = escape_heavy(100_000);
    let lines = vec![
        omp_header(),
        omp_msg("m0", None, "user", "a"),
        omp_msg("m1", Some("m0"), "assistant", &big),
        omp_msg("m2", Some("m1"), "user", "c"),
        omp_msg("m3", Some("m2"), "assistant", "d"),
    ];
    let (sid, _) = env.indexed("items.jsonl", &lines).await;
    let pages = env
        .pages(json!({"session_id": sid, "max_items": 2}), 32768)
        .await;
    for p in &pages {
        assert!(items(p).len() <= 2);
    }
    let mut logical: Vec<String> = Vec::new();
    for p in &pages {
        for i in items(p) {
            let r = i["item_ref"].as_str().unwrap().to_string();
            if !logical.contains(&r) {
                logical.push(r);
            }
        }
    }
    assert_eq!(logical.len(), 4);
    assert_eq!(reassemble(&pages)[1].1, big.as_bytes());
    assert!(
        pages
            .iter()
            .any(|p| p["page"]["stop_reason"] == "item_limit")
    );
    assert!(
        pages
            .iter()
            .any(|p| p["page"]["stop_reason"] == "byte_limit")
    );
}

#[tokio::test]
async fn escaping_is_budgeted_and_a_nonprogressing_budget_is_rejected() {
    let env = Env::new().await;
    // 3000 quotes: 6000 bytes once escaped, twice with the text copy
    let quotes = "\"".repeat(3000);
    let (sid, _) = env
        .indexed(
            "q.jsonl",
            &[omp_header(), omp_msg("m0", None, "user", &quotes)],
        )
        .await;
    let pages = env
        .pages(json!({"session_id": sid, "max_bytes": 12288}), 12288)
        .await;
    assert!(pages.len() >= 3, "{} pages", pages.len());
    assert!(
        items(&pages[0])[0]["blocks"][0]["end_byte"]
            .as_u64()
            .unwrap()
            < 12288 / 3,
        "decoded bytes per page are far below max_bytes because of escaping"
    );
    assert_eq!(reassemble(&pages)[0].1, quotes.as_bytes());
    // an envelope that cannot fit even one fragment is RESOURCE_LIMIT, never an unchanged cursor
    let long_id = "x".repeat(2600);
    let (sid, _) = env
        .indexed(
            "long.jsonl",
            &[omp_header(), omp_msg(&long_id, None, "user", "hello")],
        )
        .await;
    let e = env.err(json!({"session_id": sid, "max_bytes": 8192})).await;
    assert_eq!(e.code, "RESOURCE_LIMIT");
    assert_eq!(e.recovery.as_deref(), Some("increase_max_bytes"));
    let r = env
        .ok(json!({"session_id": sid, "max_bytes": 262144}))
        .await;
    assert_eq!(items(&r).len(), 1);
}

#[tokio::test]
async fn local_read_budgets_return_resource_limit() {
    let env = Env::with(|c| c.reader.max_source_read_bytes = 400).await;
    let (sid, _) = env.indexed("a.jsonl", &linear(30)).await;
    let e = env.err(json!({"session_id": sid})).await;
    assert_eq!(e.code, "RESOURCE_LIMIT");
    assert_eq!(e.details.unwrap()["limit"], "max_source_read_bytes");
    // indexed_only performs no source reads, so the I/O budget cannot apply
    let r = env
        .pages(
            json!({"session_id": sid, "source_policy": "indexed_only"}),
            32768,
        )
        .await;
    assert_eq!(r.iter().map(|p| items(p).len()).sum::<usize>(), 30);
    let env = Env::with(|c| c.reader.max_read_millis = 0).await;
    let (sid, _) = env.indexed("a.jsonl", &linear(30)).await;
    std::thread::sleep(std::time::Duration::from_millis(5));
    let e = env.err(json!({"session_id": sid})).await;
    assert_eq!(e.code, "RESOURCE_LIMIT");
    assert_eq!(e.details.unwrap()["limit"], "max_read_millis");
}

#[tokio::test]
async fn cursors_replay_and_reject_new_selection_fields() {
    let env = Env::new().await;
    let (sid, _) = env.indexed("a.jsonl", &linear(6)).await;
    let p1 = env.ok(json!({"session_id": sid, "max_items": 2})).await;
    let c = p1["page"]["next_cursor"].as_str().unwrap().to_string();
    let a = env.ok(json!({"cursor": c})).await;
    let b = env.ok(json!({"cursor": c})).await;
    assert_eq!(a["items"], b["items"], "a cursor is reusable, not consumed");
    assert_eq!(text_of(&items(&a)[0]), msg_text(2));
    // different limits may change the endpoint but never the start
    let wide = env.ok(json!({"cursor": c, "max_items": 200})).await;
    assert_eq!(items(&wide)[0]["item_ref"], items(&a)[0]["item_ref"]);
    assert_eq!(items(&wide).len(), 4);
    for extra in [
        json!({"view": "with_tools"}),
        json!({"session_id": sid}),
        json!({"selection": {"kind": "session"}}),
        json!({"source_policy": "indexed_only"}),
        json!({"unknown": 1}),
    ] {
        let mut args = json!({"cursor": c});
        args.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert_eq!(env.err(args).await.code, "INVALID_ARGUMENT");
    }
    let mut bad = c.clone();
    let last = bad.pop().unwrap();
    bad.push(if last == 'A' { 'B' } else { 'A' });
    assert_eq!(
        env.err(json!({"cursor": bad})).await.code,
        "INVALID_ARGUMENT",
        "altered signature"
    );
    assert_eq!(
        env.err(json!({"cursor": "nonsense"})).await.code,
        "INVALID_ARGUMENT"
    );
}

// ---------------------------------------------------------------------------------------------
// Source observation, consistency
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn append_only_growth_keeps_old_cursors_frozen_and_new_reads_see_new_records() {
    let env = Env::new().await;
    let (sid, path) = env.indexed("a.jsonl", &linear(6)).await;
    let p1 = env.ok(json!({"session_id": sid, "max_items": 2})).await;
    let c = p1["page"]["next_cursor"].as_str().unwrap().to_string();
    append(
        &path,
        &[
            omp_msg("m006", Some("m005"), "user", "appended one"),
            omp_msg("m007", Some("m006"), "assistant", "appended two"),
        ],
    );
    let mut texts: Vec<String> = items(&p1).iter().map(text_of).collect();
    let mut cursor = Some(c);
    while let Some(c) = cursor {
        let p = env.ok(json!({"cursor": c, "max_items": 2})).await;
        texts.extend(items(&p).iter().map(text_of));
        cursor = p["page"]["next_cursor"].as_str().map(str::to_string);
    }
    assert_eq!(
        texts,
        (0..6).map(msg_text).collect::<Vec<_>>(),
        "records beyond the frozen endpoint never appear midway"
    );
    // a new initial read observes the grown file through a staged source-read snapshot
    let fresh = env.ok(json!({"session_id": sid})).await;
    assert_eq!(items(&fresh).len(), 8);
    assert_eq!(fresh["snapshot"]["kind"], "source_read");
    assert_eq!(text_of(&items(&fresh)[7]), "appended two");
    // reading is read-only: nothing was published by the reads
    assert_eq!(
        env.n("SELECT count(*) FROM snapshots WHERE state = 'published'")
            .await,
        1
    );
    // once indexing catches up, the published snapshot is reused after validation
    env.index(&sid).await.unwrap();
    let again = env.ok(json!({"session_id": sid})).await;
    assert_eq!(again["snapshot"]["kind"], "indexed");
    assert_eq!(items(&again).len(), 8);
    assert_eq!(
        env.n("SELECT count(*) FROM snapshots WHERE kind = 'source_read'")
            .await,
        1,
        "no further observation was staged"
    );
}

#[tokio::test]
async fn rewritten_record_fails_and_is_never_masked_by_fallback() {
    let env = Env::new().await;
    let (sid, path) = env.indexed("a.jsonl", &linear(6)).await;
    let p1 = env.ok(json!({"session_id": sid, "max_items": 2})).await;
    let c = p1["page"]["next_cursor"].as_str().unwrap().to_string();
    // same length, different content: a rewrite of a selected record
    let text = std::fs::read_to_string(&path)
        .unwrap()
        .replace("qwz3", "qwz9");
    std::fs::write(&path, text).unwrap();
    let e = env.err(json!({"cursor": c, "max_items": 50})).await;
    assert_eq!(e.code, "SOURCE_CHANGED");
    assert!(!e.retryable);
    // an explicit require_source cursor behaves the same
    let strict = env
        .ok(json!({"session_id": sid, "source_policy": "indexed_only", "max_items": 2}))
        .await;
    assert!(strict["page"]["next_cursor"].is_string());
    let e = env
        .err(json!({"session_id": sid, "source_policy": "prefer_source"}))
        .await;
    assert_eq!(
        e.code, "SOURCE_CHANGED",
        "an initial read that fails verification is an error, not a partial transcript"
    );
    // the caller can still explicitly ask for the older retained conversation snapshot
    let old = env
        .ok(json!({"session_id": sid, "source_policy": "indexed_only"}))
        .await;
    assert_eq!(text_of(&items(&old)[3]), msg_text(3));
    assert_eq!(old["items"][3]["provenance"]["origin"], "indexed_snapshot");
}

#[tokio::test]
async fn truncation_is_a_positive_change_for_old_cursors() {
    let env = Env::new().await;
    let (sid, path) = env.indexed("a.jsonl", &linear(6)).await;
    let p1 = env.ok(json!({"session_id": sid, "max_items": 2})).await;
    let c = p1["page"]["next_cursor"].as_str().unwrap().to_string();
    let bytes = std::fs::read(&path).unwrap();
    std::fs::write(&path, &bytes[..bytes.len() / 4]).unwrap();
    assert_eq!(
        env.err(json!({"cursor": c, "max_items": 50})).await.code,
        "SOURCE_CHANGED"
    );
}

#[tokio::test]
async fn missing_source_falls_back_only_under_prefer_source_with_visible_provenance() {
    let env = Env::new().await;
    let (sid, path) = env.indexed("a.jsonl", &linear(4)).await;
    std::fs::remove_file(&path).unwrap();
    let r = env.ok(json!({"session_id": sid})).await;
    assert_eq!(items(&r).len(), 4);
    assert!(
        items(&r)
            .iter()
            .all(|i| i["provenance"]["origin"] == "indexed_snapshot")
    );
    assert!(
        r["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["code"] == "SOURCE_FALLBACK"),
        "fallback is visible"
    );
    assert_eq!(text_of(&items(&r)[1]), msg_text(1));
    let e = env
        .err(json!({"session_id": sid, "source_policy": "require_source"}))
        .await;
    assert_eq!(e.code, "SOURCE_UNAVAILABLE");
    assert!(e.retryable);
    let r = env
        .ok(json!({"session_id": sid, "source_policy": "indexed_only"}))
        .await;
    assert_eq!(items(&r).len(), 4, "indexed_only needs no source access");
    assert!(r["warnings"].as_array().unwrap().is_empty());
    // with_tools/raw never fall back to cached text
    let e = env
        .err(json!({"session_id": sid, "view": "with_tools"}))
        .await;
    assert_eq!(e.code, "SOURCE_UNAVAILABLE");
    // unindexed + missing source: nothing to fall back to
    let (sid2, path2) = env.add_omp("gone.jsonl", &linear(2)).await;
    std::fs::remove_file(&path2).unwrap();
    assert!(matches!(
        env.err(json!({"session_id": sid2})).await.code.as_str(),
        "SOURCE_UNAVAILABLE" | "SESSION_NOT_FOUND"
    ));
}

#[tokio::test]
async fn renamed_source_is_re_resolved_by_identity_and_digest() {
    let env = Env::new().await;
    let (sid, path) = env.indexed("a.jsonl", &linear(4)).await;
    let moved = path.with_file_name("renamed.jsonl");
    std::fs::rename(&path, &moved).unwrap();
    let r = env
        .ok(json!({"session_id": sid, "source_policy": "require_source"}))
        .await;
    assert_eq!(items(&r).len(), 4);
    assert!(
        items(&r)
            .iter()
            .all(|i| i["provenance"]["origin"] == "verified_source")
    );
    assert!(r["warnings"].as_array().unwrap().is_empty());
}

// ---------------------------------------------------------------------------------------------
// Leases, restart, handles, exclusion
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn handles_survive_a_restart_that_keeps_key_and_manifests() {
    let mut env = Env::new().await;
    let (sid, _) = env.indexed("a.jsonl", &linear(6)).await;
    let p1 = env.ok(json!({"session_id": sid, "max_items": 2})).await;
    let c = p1["page"]["next_cursor"].as_str().unwrap().to_string();
    let m = items(&p1)[1]["message_ref"].as_str().unwrap().to_string();
    env.restart().await;
    let p2 = env.ok(json!({"cursor": c, "max_items": 2})).await;
    assert_eq!(text_of(&items(&p2)[0]), msg_text(2));
    let r = env.ok(json!({"session_id": sid, "selection": {"kind": "message", "message_ref": m, "before": 0, "after": 0}})).await;
    assert_eq!(text_of(&items(&r)[0]), msg_text(1));
}

#[tokio::test]
async fn leases_expire_expose_expiry_and_sweep_releases_retired_snapshots() {
    let env = Env::new().await;
    let (sid, path) = env.indexed("a.jsonl", &linear(6)).await;
    let p1 = env.ok(json!({"session_id": sid, "max_items": 2})).await;
    assert_eq!(
        p1["snapshot"]["lease_expires_at"],
        spool::transcript::types::iso(T0 + 1_800_000)
    );
    let c = p1["page"]["next_cursor"].as_str().unwrap().to_string();
    let snap1 = p1["snapshot"]["snapshot_id"].as_str().unwrap().to_string();
    // a newer snapshot retires the first; the lease keeps it (and its manifest) readable
    append(&path, &[omp_msg("m006", Some("m005"), "user", "later")]);
    env.index(&sid).await.unwrap();
    let state: String = sqlx::query_scalar("SELECT state FROM snapshots WHERE id = ?")
        .bind(&snap1)
        .fetch_one(&env.index.writer)
        .await
        .unwrap();
    assert_eq!(state, "retired_leased");
    assert_eq!(env.n("SELECT count(*) FROM chunks c JOIN snapshots s ON s.id = c.snapshot_id WHERE s.state = 'retired_leased'").await, 0, "never searchable");
    let p2 = env.ok(json!({"cursor": c, "max_items": 50})).await;
    assert_eq!(items(&p2).len(), 4);
    assert_eq!(p2["snapshot"]["snapshot_id"], json!(snap1));
    // expiry: cursor and refs fail explicitly, then the sweep releases lease, manifest and snapshot
    env.clock.advance_ms(1_800_001);
    assert_eq!(env.err(json!({"cursor": c})).await.code, "CURSOR_EXPIRED");
    let m = items(&p1)[0]["message_ref"].as_str().unwrap().to_string();
    assert_eq!(
        env.err(json!({"session_id": sid, "selection": {"kind": "message", "message_ref": m}}))
            .await
            .code,
        "CURSOR_EXPIRED"
    );
    let (leases, snaps) = env.svc.sweep().await.unwrap();
    assert!(leases >= 1 && snaps >= 1);
    let left: i64 = sqlx::query_scalar("SELECT count(*) FROM snapshots WHERE id = ?")
        .bind(&snap1)
        .fetch_one(&env.index.writer)
        .await
        .unwrap();
    assert_eq!(left, 0, "released with the lease");
    assert_eq!(env.n("SELECT count(*) FROM read_manifests").await, 0);
    assert_eq!(env.err(json!({"cursor": c})).await.code, "CURSOR_EXPIRED");
}

#[tokio::test]
async fn lease_count_and_retained_byte_bounds_reject_new_reads() {
    let env = Env::with(|c| c.reader.max_active_leases = 2).await;
    let (sid, _) = env.indexed("a.jsonl", &linear(3)).await;
    env.ok(json!({"session_id": sid})).await;
    env.ok(json!({"session_id": sid})).await;
    let e = env.err(json!({"session_id": sid})).await;
    assert_eq!(e.code, "RESOURCE_LIMIT");
    assert!(e.retryable);
    env.clock.advance_ms(1_800_001);
    env.svc.sweep().await.unwrap();
    env.ok(json!({"session_id": sid})).await;
    let env = Env::with(|c| c.reader.max_retained_lease_bytes = 1).await;
    let (sid, _) = env.indexed("a.jsonl", &linear(3)).await;
    assert_eq!(
        env.err(json!({"session_id": sid})).await.code,
        "RESOURCE_LIMIT"
    );
}

#[tokio::test]
async fn altered_and_rotated_handles_fail_explicitly() {
    let env = Env::new().await;
    let (sid, _) = env.indexed("a.jsonl", &linear(6)).await;
    let p1 = env.ok(json!({"session_id": sid, "max_items": 2})).await;
    let cursor = p1["page"]["next_cursor"].as_str().unwrap().to_string();
    let m = items(&p1)[0]["message_ref"].as_str().unwrap().to_string();
    let mut bad = m.clone();
    let last = bad.pop().unwrap();
    bad.push(if last == 'A' { 'B' } else { 'A' });
    assert_eq!(
        env.err(json!({"session_id": sid, "selection": {"kind": "message", "message_ref": bad}}))
            .await
            .code,
        "INVALID_ARGUMENT"
    );
    // key rotation revokes everything the old key signed
    env.keys.rotate().unwrap();
    let e = env
        .err(json!({"session_id": sid, "selection": {"kind": "message", "message_ref": m}}))
        .await;
    assert_eq!(e.code, "STALE_REFERENCE");
    assert!(e.message.contains("rotated"));
    let e = env.err(json!({"cursor": cursor})).await;
    assert_eq!(e.code, "CURSOR_EXPIRED");
    assert!(e.message.contains("rotated"));
    env.ok(json!({"session_id": sid})).await;
}

async fn mk(env: &Env, n: &str) -> (String, String, String) {
    let (sid, _) = env.indexed(n, &linear(6)).await;
    let p = env.ok(json!({"session_id": sid, "max_items": 2})).await;
    let c = p["page"]["next_cursor"].as_str().unwrap().to_string();
    let m = items(&p)[0]["message_ref"].as_str().unwrap().to_string();
    (sid, c, m)
}

#[tokio::test]
async fn exclusion_removal_and_disablement_beat_a_valid_cursor_on_every_call() {
    let env = Env::new().await;
    // exclusion flag, even though the lease is still live
    let (sid, c, m) = mk(&env, "ex.jsonl").await;
    sqlx::query("UPDATE sessions SET excluded = 1 WHERE id = ?")
        .bind(&sid)
        .execute(&env.index.writer)
        .await
        .unwrap();
    assert_eq!(env.err(json!({"cursor": c})).await.code, "FORBIDDEN");
    assert_eq!(
        env.err(json!({"session_id": sid, "selection": {"kind": "message", "message_ref": m}}))
            .await
            .code,
        "FORBIDDEN"
    );
    assert_eq!(env.err(json!({"session_id": sid})).await.code, "FORBIDDEN");
    // exclusion rows (session and source scope)
    let (sid, c, _) = mk(&env, "ex2.jsonl").await;
    sqlx::query("INSERT INTO exclusions(kind, target_id, created_at) VALUES ('session', ?, 0)")
        .bind(&sid)
        .execute(&env.index.writer)
        .await
        .unwrap();
    assert_eq!(env.err(json!({"cursor": c})).await.code, "FORBIDDEN");
    // removed session
    let (sid, c, _) = mk(&env, "rm.jsonl").await;
    sqlx::query("UPDATE sessions SET tombstoned_at = 1 WHERE id = ?")
        .bind(&sid)
        .execute(&env.index.writer)
        .await
        .unwrap();
    assert_eq!(
        env.err(json!({"cursor": c})).await.code,
        "SESSION_NOT_FOUND"
    );
    // source disablement
    let (sid, c, _) = mk(&env, "dis.jsonl").await;
    let mut cfg = (*env.cfg).clone();
    cfg.sources.iter_mut().for_each(|s| s.enabled = false);
    env.sources.reload(&cfg).unwrap();
    assert_eq!(env.err(json!({"cursor": c})).await.code, "FORBIDDEN");
    assert_eq!(env.err(json!({"session_id": sid})).await.code, "FORBIDDEN");
}

#[test]
fn tool_definition_has_object_schemas_and_is_read_only() {
    let t = spool::transcript::ReadService::tool_definition();
    assert_eq!(t.name, "read_session");
    assert_eq!(
        t.input_schema.get("type").and_then(|v| v.as_str()),
        Some("object")
    );
    assert!(t.output_schema.is_some());
    assert_eq!(
        t.annotations.as_ref().and_then(|a| a.read_only_hint),
        Some(true)
    );
}

// ---------------------------------------------------------------------------------------------
// Tool activity (turn closure), against the real OMP adapter's verified turn/call ownership
// ---------------------------------------------------------------------------------------------

fn omp_assistant(id: &str, parent: &str, blocks: Vec<Value>) -> String {
    json!({"type":"message","id":id,"parentId":parent,"timestamp":"2026-10-12T00:00:01Z","message":{"role":"assistant","content":blocks}}).to_string()
}

fn text_block(t: &str) -> Value {
    json!({"type":"text","text":t})
}

fn call_block(id: &str, name: &str, arguments: Value) -> Value {
    json!({"type":"toolCall","id":id,"name":name,"arguments":arguments})
}

fn omp_result(id: &str, parent: &str, call_id: &str, content: Value) -> String {
    json!({"type":"message","id":id,"parentId":parent,"timestamp":"2026-10-12T00:00:02Z","message":{"role":"toolResult","toolCallId":call_id,"toolName":"t","isError":false,"content":content}}).to_string()
}

/// Two turns with parallel calls, an out-of-order (async) result, a call without result, an orphan
/// result, a REPEATED native call id in the second turn, and a sibling branch holding another result.
fn tool_session() -> Vec<String> {
    vec![
        omp_header(),
        omp_msg("u1", None, "user", "ask one"),
        omp_assistant(
            "a1",
            "u1",
            vec![
                text_block("I will look"),
                call_block("call-a", "read", json!("{\"path\":\"a\"}")),
                call_block("call-b", "search", json!({"q": "x"})),
            ],
        ),
        omp_result(
            "rb",
            "a1",
            "call-b",
            json!([{"type":"text","text":"result b"}]),
        ),
        omp_result("ra", "rb", "call-a", json!("result a")),
        omp_assistant(
            "a2",
            "ra",
            vec![
                text_block("final answer one"),
                call_block("call-nores", "bash", json!("{}")),
            ],
        ),
        omp_result("orphan", "a2", "ghost", json!("orphan payload")),
        omp_msg("u2", Some("orphan"), "user", "ask two"),
        omp_assistant(
            "a3",
            "u2",
            vec![
                text_block("answer two"),
                call_block("call-a", "read", json!("{}")),
            ],
        ),
        omp_result("r3", "a3", "call-a", json!("result turn two")),
        omp_result("rbranch", "a1", "call-a", json!("sibling branch result")),
        omp_assistant("abranch", "rbranch", vec![text_block("branch answer")]),
    ]
}

async fn branch_with(env: &Env, sid: &str, needle: &str) -> String {
    let e = env.err(json!({"session_id": sid})).await;
    assert_eq!(e.code, "BRANCH_REQUIRED");
    for o in e.details.unwrap()["branch_options"].as_array().unwrap() {
        let r = env.ok(json!({"session_id": sid, "selection": {"kind": "session", "branch_ref": o["branch_ref"]}})).await;
        if items(&r).iter().any(|i| text_of(i) == needle) {
            return o["branch_ref"].as_str().unwrap().to_string();
        }
    }
    panic!("no branch contains {needle}");
}

fn tag(i: &Value) -> String {
    match i["kind"].as_str().unwrap() {
        "message" => format!("msg:{}", text_of(i)),
        "tool_call" => format!("call:{}", i["native_call_id"].as_str().unwrap()),
        "tool_result" => format!(
            "result:{}",
            String::from_utf8(frag_bytes(&i["blocks"][0])).unwrap()
        ),
        k => k.to_string(),
    }
}

#[tokio::test]
async fn with_tools_turn_closure_follows_verified_ownership_not_proximity() {
    let env = Env::new().await;
    let (sid, _) = env.indexed("tools.jsonl", &tool_session()).await;
    let branch = branch_with(&env, &sid, "answer two").await;
    let r = env.ok(json!({"session_id": sid, "view": "with_tools", "max_bytes": 262144, "selection": {"kind": "session", "branch_ref": branch}})).await;
    assert_eq!(r["page"]["selection_exhausted"], true);
    let seq: Vec<String> = items(&r).iter().map(tag).collect();
    assert_eq!(
        seq,
        vec![
            "msg:ask one",
            "msg:I will look",
            "call:call-a",
            "call:call-b",
            "result:result b", // asynchronous result stays at its persisted position (before call-a's)
            "result:result a",
            "msg:final answer one",
            "call:call-nores",
            "result:orphan payload",
            "msg:ask two",
            "msg:answer two",
            "call:call-a",
            "result:result turn two",
        ]
    );
    assert!(
        !seq.iter().any(|s| s.contains("sibling")),
        "results on sibling branches are not imported"
    );
    let it = items(&r);
    // parallel calls keep their native ids; the repeated id in turn two links only inside its own turn
    let call_a1 = &it[2];
    let call_a3 = &it[11];
    assert_eq!(call_a1["result_state"], "result_available");
    assert_eq!(call_a1["result_refs"].as_array().unwrap().len(), 1);
    assert_eq!(
        call_a1["result_refs"][0], it[5]["item_ref"],
        "call-a (turn 1) pairs with its own result"
    );
    assert_eq!(
        call_a3["result_refs"][0], it[12]["item_ref"],
        "call-a (turn 2) pairs with the turn-2 result"
    );
    assert_eq!(
        it[4]["tool_ref"], it[3]["item_ref"],
        "result b links to call b"
    );
    assert_eq!(it[5]["tool_ref"], call_a1["item_ref"]);
    // absent result: stated plainly, nothing about running or failed
    assert_eq!(it[7]["result_state"], "no_result_in_snapshot");
    assert!(it[7]["result_refs"].as_array().unwrap().is_empty());
    // orphan keeps null tool_ref and raises a warning + partial coverage
    assert!(it[8]["tool_ref"].is_null());
    assert_eq!(it[8]["native_call_id"], "ghost");
    assert!(
        r["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["code"] == "ORPHAN_TOOL_RESULT")
    );
    assert_eq!(r["coverage"]["status"], "partial");
    // only the trailing result lies past the last selected message record
    for (n, i) in it.iter().enumerate() {
        assert_eq!(
            i.get("outside_message_window")
                .map(|o| o == true)
                .unwrap_or(false),
            n == 12,
            "item {n}"
        );
    }
    // payload fidelity: original argument string verbatim, structured object normalized
    assert_eq!(it[2]["blocks"][0]["kind"], "tool_arguments");
    assert_eq!(it[2]["blocks"][0]["fidelity"], "verbatim");
    assert_eq!(
        String::from_utf8(frag_bytes(&it[2]["blocks"][0])).unwrap(),
        "{\"path\":\"a\"}"
    );
    assert_eq!(it[3]["blocks"][0]["fidelity"], "normalized_json");
    assert_eq!(
        String::from_utf8(frag_bytes(&it[3]["blocks"][0])).unwrap(),
        "{\"q\":\"x\"}"
    );
    assert_eq!(it[4]["blocks"][0]["kind"], "tool_result");
    assert_eq!(it[4]["is_error"], false);
    assert!(
        it.iter()
            .all(|i| i["provenance"]["origin"] == "verified_source")
    );
    // the conversation view of the same path carries none of it
    let conv = env.ok(json!({"session_id": sid, "max_bytes": 262144, "selection": {"kind": "session", "branch_ref": branch}})).await;
    assert_eq!(
        items(&conv).iter().map(tag).collect::<Vec<_>>(),
        vec![
            "msg:ask one",
            "msg:I will look",
            "msg:final answer one",
            "msg:ask two",
            "msg:answer two"
        ]
    );
}

#[tokio::test]
async fn tools_outside_the_message_window_are_marked_and_other_turns_stay_out() {
    let env = Env::new().await;
    let (sid, _) = env.indexed("tools.jsonl", &tool_session()).await;
    let branch = branch_with(&env, &sid, "answer two").await;
    let conv = env
        .ok(json!({"session_id": sid, "selection": {"kind": "session", "branch_ref": branch}}))
        .await;
    let a2 = items(&conv)[2]["message_ref"].as_str().unwrap().to_string();
    let r = env.ok(json!({"session_id": sid, "view": "with_tools", "selection": {"kind": "message", "message_ref": a2, "before": 0, "after": 0, "branch_ref": branch}})).await;
    let seq: Vec<(String, bool)> = items(&r)
        .iter()
        .map(|i| {
            (
                tag(i),
                i.get("outside_message_window")
                    .map(|v| v == true)
                    .unwrap_or(false),
            )
        })
        .collect();
    assert_eq!(
        seq,
        vec![
            ("call:call-a".to_string(), true),
            ("call:call-b".to_string(), true),
            ("result:result b".to_string(), true),
            ("result:result a".to_string(), true),
            ("msg:final answer one".to_string(), false),
            ("call:call-nores".to_string(), false),
            ("result:orphan payload".to_string(), true),
        ],
        "turn 1 closure around one message: text outside the window excluded, turn 2 never included"
    );
    // item selection: just that result, no turn closure
    let result_ref = items(&r)[3]["item_ref"].as_str().unwrap().to_string();
    let one = env.ok(json!({"session_id": sid, "view": "with_tools", "selection": {"kind": "item", "item_ref": result_ref}})).await;
    assert_eq!(items(&one).len(), 1);
    assert_eq!(tag(&items(&one)[0]), "result:result a");
    assert!(items(&one)[0]["tool_ref"].is_string());
    // a call item: results are listed along ONE safe path, never from sibling branches
    let call_ref = items(&r)[0]["item_ref"].as_str().unwrap().to_string();
    let e = env.err(json!({"session_id": sid, "view": "with_tools", "selection": {"kind": "item", "item_ref": call_ref}})).await;
    assert_eq!(
        e.code, "BRANCH_REQUIRED",
        "call-a has linked results on two branches below it"
    );
    assert_eq!(
        e.details.as_ref().unwrap()["branch_options"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        e.details.as_ref().unwrap()["branch_options_truncated"],
        false
    );
    let call = env.ok(json!({"session_id": sid, "view": "with_tools", "selection": {"kind": "item", "item_ref": call_ref, "branch_ref": branch}})).await;
    assert_eq!(items(&call).len(), 1);
    assert_eq!(items(&call)[0]["result_refs"].as_array().unwrap().len(), 1);
    assert_eq!(
        items(&call)[0]["result_refs"][0],
        items(&r)[3]["item_ref"],
        "the main-branch result, not the sibling's"
    );
    // the sibling branch picks the sibling result
    let e = env.err(json!({"session_id": sid, "view": "with_tools", "selection": {"kind": "item", "item_ref": call_ref}})).await;
    let other = e.details.unwrap()["branch_options"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["branch_ref"].as_str().unwrap().to_string())
        .find(|b| *b != branch)
        .unwrap();
    let call2 = env.ok(json!({"session_id": sid, "view": "with_tools", "selection": {"kind": "item", "item_ref": call_ref, "branch_ref": other}})).await;
    assert_eq!(items(&call2)[0]["result_refs"].as_array().unwrap().len(), 1);
    assert_ne!(items(&call2)[0]["result_refs"][0], items(&r)[3]["item_ref"]);
    let sib = env.ok(json!({"session_id": sid, "view": "with_tools", "selection": {"kind": "item", "item_ref": items(&call2)[0]["result_refs"][0], "branch_ref": other}})).await;
    assert_eq!(tag(&items(&sib)[0]), "result:sibling branch result");
    // call-b has results on one branch only: unambiguous without a branch_ref
    let b_ref = items(&r)[1]["item_ref"].as_str().unwrap().to_string();
    let callb = env.ok(json!({"session_id": sid, "view": "with_tools", "selection": {"kind": "item", "item_ref": b_ref}})).await;
    assert_eq!(items(&callb)[0]["result_refs"].as_array().unwrap().len(), 1);
    // a branch that does not contain the item is rejected
    let other_only = env.err(json!({"session_id": sid, "view": "with_tools", "selection": {"kind": "item", "item_ref": items(&r)[3]["item_ref"], "branch_ref": other}})).await;
    assert_eq!(other_only.code, "INVALID_RANGE");
    // tool item classes need the with_tools view
    let e = env
        .err(json!({"session_id": sid, "selection": {"kind": "item", "item_ref": call_ref}}))
        .await;
    assert_eq!(e.code, "VIEW_NOT_SUPPORTED");
}

#[tokio::test]
async fn two_mib_tool_result_and_arguments_fragment_and_reassemble() {
    let env = Env::new().await;
    let big = escape_heavy(2 * 1024 * 1024);
    let lines = vec![
        omp_header(),
        omp_msg("u1", None, "user", "run it"),
        omp_assistant(
            "a1",
            "u1",
            vec![text_block("running"), call_block("c1", "bash", json!(big))],
        ),
        omp_result("r1", "a1", "c1", json!(big)),
        omp_assistant("a2", "r1", vec![text_block("done")]),
    ];
    let (sid, _) = env.add_omp("bigtool.jsonl", &lines).await;
    let pages = env
        .pages(json!({"session_id": sid, "view": "with_tools"}), 32768)
        .await;
    assert!(pages.len() > 100, "{} pages", pages.len());
    let blocks = reassemble(&pages);
    let payloads: Vec<&Vec<u8>> = blocks
        .iter()
        .map(|(_, b)| b)
        .filter(|b| b.len() == big.len())
        .collect();
    assert_eq!(
        payloads.len(),
        2,
        "tool arguments and tool result both fragmented"
    );
    assert!(
        payloads.iter().all(|p| **p == big.as_bytes()),
        "no missing or duplicated bytes"
    );
    let kinds: Vec<&str> = pages
        .iter()
        .flat_map(|p| items(p).iter())
        .map(|i| i["kind"].as_str().unwrap())
        .collect();
    assert!(kinds.contains(&"tool_call") && kinds.contains(&"tool_result"));
    for p in &pages {
        assert!(tool_result_bytes(p, false) <= 32768);
    }
}

// ---------------------------------------------------------------------------------------------
// Codex / Claude tool provenance through the real adapters -> stage_snapshot -> reader
// ---------------------------------------------------------------------------------------------

fn fixture_bytes(rel: &str) -> Vec<u8> {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("fixtures")
            .join(rel),
    )
    .unwrap_or_else(|e| panic!("{rel}: {e}"))
}

#[tokio::test]
async fn claude_with_tools_links_by_native_id_within_each_turn() {
    let env = Env::new().await;
    let (sid, _) = env
        .add(
            "claude",
            "cl.jsonl",
            &fixture_bytes("claude/saved-messages.jsonl"),
        )
        .await;
    let branch = branch_with(&env, &sid, "second turn").await;
    let r = env.ok(json!({"session_id": sid, "view": "with_tools", "max_bytes": 262144, "selection": {"kind": "session", "branch_ref": branch}})).await;
    let it = items(&r);
    let calls: Vec<usize> = (0..it.len())
        .filter(|i| it[*i]["kind"] == "tool_call")
        .collect();
    let results: Vec<usize> = (0..it.len())
        .filter(|i| it[*i]["kind"] == "tool_result")
        .collect();
    assert!(
        calls.len() >= 3 && results.len() >= 3,
        "{:?}",
        it.iter().map(tag).collect::<Vec<_>>()
    );
    // `call-2` is used by two different turns: the final result pairs only with the second turn's call
    let second_turn_msg = it
        .iter()
        .position(|i| i["kind"] == "message" && text_of(i) == "second turn")
        .unwrap();
    let call2_turn2 = calls
        .iter()
        .copied()
        .find(|c| *c > second_turn_msg)
        .expect("turn-2 call");
    let last_result = *results.last().unwrap();
    assert_eq!(it[call2_turn2]["native_call_id"], "call-2");
    assert_eq!(it[last_result]["native_call_id"], "call-2");
    assert_eq!(it[last_result]["tool_ref"], it[call2_turn2]["item_ref"]);
    assert_eq!(it[call2_turn2]["result_refs"].as_array().unwrap().len(), 1);
    assert_eq!(
        it[call2_turn2]["result_refs"][0],
        it[last_result]["item_ref"]
    );
    // every linked result points at a call that is part of the same page (nothing imported from elsewhere)
    for r_i in &results {
        if let Some(t) = it[*r_i]["tool_ref"].as_str() {
            assert!(
                it.iter().any(|i| i["item_ref"] == t),
                "linked call is in the selected path"
            );
        }
    }
    // closure around one message marks the other tool records outside the window
    let msg = items(&env.ok(json!({"session_id": sid, "max_bytes": 262144, "selection": {"kind": "session", "branch_ref": branch}})).await)
        .iter()
        .find(|i| text_of(i) == "second turn")
        .unwrap()["message_ref"]
        .as_str()
        .unwrap()
        .to_string();
    let w = env.ok(json!({"session_id": sid, "view": "with_tools", "max_bytes": 262144, "selection": {"kind": "message", "message_ref": msg, "before": 0, "after": 0, "branch_ref": branch}})).await;
    assert!(
        items(&w)
            .iter()
            .any(|i| i["kind"] == "message" && i["outside_message_window"].is_null())
    );
    assert!(
        items(&w)
            .iter()
            .any(|i| i["kind"] == "tool_result" && i["outside_message_window"] == true),
        "the trailing result lies after the last selected message record"
    );
    // the adapter's turn rule puts every assistant record under the one user prompt in one turn, so the
    // earlier call-1 is part of this turn's closure but outside the one-message window
    assert!(items(&w).iter().any(|i| i["kind"] == "tool_call"
        && i["native_call_id"] == "call-1"
        && i["outside_message_window"] == true));
    assert!(
        items(&w)
            .iter()
            .all(|i| i["kind"] != "message" || text_of(i) == "second turn"),
        "conversational text outside the window stays excluded"
    );
}

#[tokio::test]
async fn codex_turn_window_excludes_the_next_turn_and_reused_call_id_links_within_its_own_turn() {
    let env = Env::new().await;
    let (sid, _) = env
        .add(
            "codex",
            "cx.jsonl",
            &fixture_bytes("codex/tool_turns.jsonl"),
        )
        .await;
    let all = env
        .ok(json!({"session_id": sid, "max_bytes": 262144}))
        .await;
    let msg_ref = |text: &str| {
        items(&all)
            .iter()
            .find(|i| text_of(i).starts_with(text))
            .unwrap_or_else(|| panic!("no message {text}"))["message_ref"]
            .clone()
    };
    let window = |m: Value| {
        let env = &env;
        let sid = sid.clone();
        async move {
            env.ok(json!({"session_id": sid, "view": "with_tools", "max_bytes": 262144, "selection": {"kind": "message", "message_ref": m, "before": 0, "after": 0}})).await
        }
    };
    let payload = |r: &Value| r.to_string();
    // turn 1 window: both turn-1 calls (one with the reused id) and their results; nothing of turn 2
    let t1 = window(msg_ref("Commentary is indexed")).await;
    let d1 = payload(&t1);
    assert!(
        d1.contains("MUST_NOT_INDEX_ARGUMENT_A")
            && d1.contains("MUST_NOT_INDEX_RESULT_A")
            && d1.contains("MUST_NOT_INDEX_RESULT_B")
    );
    assert!(
        !d1.contains("TURN_2") && !d1.contains("MUST_NOT_INDEX_ORPHAN"),
        "the next turn is not imported"
    );
    let call_a = items(&t1)
        .iter()
        .find(|i| i["kind"] == "tool_call" && i["native_call_id"] == "repeat-call")
        .unwrap();
    assert_eq!(
        call_a["result_refs"].as_array().unwrap().len(),
        1,
        "turn-1 repeat-call pairs only with the turn-1 result"
    );
    // turn 2 window: the reused id pairs with the turn-2 result only; the unknown-call result is an orphan
    let t2 = window(msg_ref("Second prompt")).await;
    let d2 = payload(&t2);
    assert!(
        d2.contains("MUST_NOT_INDEX_ARGUMENT_TURN_2")
            && d2.contains("MUST_NOT_INDEX_RESULT_TURN_2")
    );
    assert!(
        !d2.contains("MUST_NOT_INDEX_ARGUMENT_A") && !d2.contains("MUST_NOT_INDEX_RESULT_A"),
        "the previous turn is not imported"
    );
    let call_c = items(&t2)
        .iter()
        .find(|i| i["kind"] == "tool_call" && i["native_call_id"] == "repeat-call")
        .unwrap();
    assert_eq!(call_c["result_refs"].as_array().unwrap().len(), 1);
    assert!(
        t2["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["code"] == "ORPHAN_TOOL_RESULT")
    );
}
