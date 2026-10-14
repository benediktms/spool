//! `search_sessions` correctness (spec 19.3). Offline: fake embedding provider, rows seeded straight into the schema.

mod search_common;

use search_common::*;
use serde_json::{Value, json};
use spool::embeddings::fake::Fault;
use spool::embeddings::provider::Purpose;
use spool::mcp::budget::tool_result_bytes;
use spool::search::{SearchService, format_rfc3339, parse_rfc3339_utc};
use spool::transcript::lease::LeaseState;
use spool::transcript::refs::{BranchRef, MatchRef, short_digest};
use std::time::Duration;

fn matches_of<'a>(v: &'a Value, sid: &str) -> &'a Vec<Value> {
    v["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["session_id"] == sid)
        .unwrap_or_else(|| panic!("{sid} missing from {:?}", ids(v)))["matches"]
        .as_array()
        .unwrap()
}

fn frames(parts: &[&[u8]]) -> String {
    let mut b = Vec::new();
    for p in parts {
        b.extend_from_slice(&(p.len() as u64).to_le_bytes());
        b.extend_from_slice(p);
    }
    short_digest(&b)
}

#[tokio::test]
async fn exact_symbol_recall() {
    let env = Env::new().await;
    env.simple(
        "s_sym",
        "omp",
        "/w/a",
        &[("renamed parse_snapshot_v2 to read_manifest", Some(T0))],
    )
    .await;
    for i in 0..5 {
        env.simple(
            &format!("s_fill{i}"),
            "omp",
            "/w/a",
            &[("general talk about parsing and snapshots", Some(T0))],
        )
        .await;
    }
    let r = env
        .ok(json!({"query": "parse_snapshot_v2", "mode": "lexical"}))
        .await;
    assert_eq!(ids(&r), ["s_sym"]);
    assert_eq!(matches_of(&r, "s_sym")[0]["matched_by"], json!(["lexical"]));
    // Raw FTS syntax is plain text, never an error.
    for q in [
        "parse_snapshot_v2 AND (",
        "NEAR(a b)",
        "\"unterminated",
        "x* OR -y",
        "col:val",
    ] {
        env.ok(json!({"query": q, "mode": "lexical"})).await;
    }
    assert_eq!(env.provider.calls(), 0, "lexical never calls the provider");
}

#[tokio::test]
async fn punctuation_distinct_literal_matches() {
    let env = Env::new().await;
    let texts = [
        ("l1", "call foo.bar() now"),
        ("l2", "call foo_bar() now"),
        ("l3", "call foo-bar now"),
        ("l4", "say \"hi\" there"),
        ("l5", r"path C:\dir\x here"),
        ("l6", "100% sure"),
        ("l7", "100x sure"),
        ("l8", "what?! really"),
    ];
    for (id, t) in texts {
        env.simple(id, "omp", "/w", &[(t, Some(T0))]).await;
    }
    let lit = |q: &str| json!({"query": q, "mode": "literal"});
    assert_eq!(ids(&env.ok(lit("foo.bar()")).await), ["l1"]);
    assert_eq!(
        ids(&env.ok(lit("foo_")).await),
        ["l2"],
        "underscore is not a wildcard"
    );
    assert_eq!(
        ids(&env.ok(lit("100%")).await),
        ["l6"],
        "percent is not a wildcard"
    );
    assert_eq!(ids(&env.ok(lit("\"hi\"")).await), ["l4"]);
    assert_eq!(ids(&env.ok(lit(r"C:\dir")).await), ["l5"]);
    assert_eq!(ids(&env.ok(lit("?!")).await), ["l8"]);
    assert!(
        ids(&env.ok(lit("Foo.Bar")).await).is_empty(),
        "case-sensitive by default"
    );
    let r = env
        .ok(json!({"query": "Foo.Bar", "mode": "literal", "case_sensitive": false}))
        .await;
    assert_eq!(ids(&r), ["l1"]);
    // The hit is mapped to an anchored span: bytes 5..14 of the block "call foo.bar() now".
    let r = env.ok(lit("foo.bar()")).await;
    let m = &matches_of(&r, "l1")[0];
    assert_eq!(m["matched_by"], json!(["literal"]));
    let mr = MatchRef::decode(&env.keys.current(), m["match_ref"].as_str().unwrap()).unwrap();
    assert_eq!(
        (
            mr.spans[0].r.as_str(),
            mr.spans[0].b,
            mr.spans[0].s,
            mr.spans[0].e
        ),
        ("m0", 0, 5, 14)
    );
    assert_eq!(mr.content, frames(&[b"foo.bar()"]));
    assert!(m["excerpt"].as_str().unwrap().contains("foo.bar()"));
    assert_eq!(env.provider.calls(), 0, "literal never calls the provider");
}

#[tokio::test]
async fn literal_is_not_prefiltered_by_fts_and_not_biased_by_long_sessions() {
    let env = Env::with(|c| c.search.max_candidate_limit = 50).await;
    let many: Vec<(String, Option<i64>)> = (0..200)
        .map(|i| (format!("needle@@ {i}"), Some(T0)))
        .collect();
    let refs: Vec<(&str, Option<i64>)> = many.iter().map(|(s, t)| (s.as_str(), *t)).collect();
    env.simple_with("a_long", "omp", "/w", "interactive", T0, &refs, false)
        .await;
    env.simple("b_other", "omp", "/w", &[("one needle@@ only", Some(T0))])
        .await;
    let r = env.ok(json!({"query": "@@", "mode": "literal"})).await;
    assert_eq!(ids(&r), ["a_long", "b_other"]);
    assert_eq!(matches_of(&r, "a_long").len(), 3);
}

#[tokio::test]
async fn tokenless_and_empty_queries() {
    let env = Env::new().await;
    env.simple("s1", "omp", "/w", &[("what?! really", Some(T0))])
        .await;
    let r = env.ok(json!({"query": "?!", "mode": "lexical"})).await;
    assert!(r["results"].as_array().unwrap().is_empty());
    assert_eq!(r["suggest_literal"], true);
    assert!(
        r["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["code"] == "no_lexical_tokens")
    );
    assert_eq!(
        ids(&env.ok(json!({"query": "?!", "mode": "literal"})).await),
        ["s1"]
    );
    for q in ["", "   ", "\n\t"] {
        assert_eq!(
            code(env.search(json!({"query": q, "mode": "lexical"})).await),
            "INVALID_ARGUMENT"
        );
    }
}

#[tokio::test]
async fn invalid_arguments_are_rejected() {
    let env = Env::new().await;
    env.simple("s1", "omp", "/w", &[("hello world", Some(T0))])
        .await;
    let bad = [
        json!({"query": "x", "bogus": 1}),
        json!({"query": "x", "mode": "fuzzy"}),
        json!({"query": "x", "session_kinds": ["weird"]}),
        json!({"query": "x", "limit": 0}),
        json!({"query": "x", "limit": 51}),
        json!({"query": "x", "matches_per_session": 4}),
        json!({"query": "x", "after": "yesterday"}),
        json!({"query": "x", "after": "2026-10-01T00:00:00+02:00"}),
        json!({"query": "x", "after": "2026-10-02T00:00:00Z", "before": "2026-10-01T00:00:00Z"}),
        json!({"query": "x", "after": "2026-10-01T00:00:00Z", "before": "2026-10-01T00:00:00Z"}),
        json!({"query": "x", "project_path": "relative/path"}),
        json!({"query": "x", "sources": "omp"}),
        json!({"mode": "lexical"}),
        json!({"query": "y".repeat(4097), "mode": "lexical"}),
    ];
    for a in bad {
        assert_eq!(code(env.search(a.clone()).await), "INVALID_ARGUMENT", "{a}");
    }
    assert_eq!(
        code(
            env.search(json!({"query": "x", "bogus": "SECRET-QUERY-TEXT"}))
                .await
        ),
        "INVALID_ARGUMENT"
    );
    let e = env
        .search(json!({"query": "x", "limit": "SECRET-QUERY-TEXT"}))
        .await
        .unwrap_err();
    assert!(!e.message.contains("SECRET"), "errors never echo values");
    // 1,024 provider-token limit applies to hosted modes only.
    let many = "a ".repeat(1100);
    assert!(many.len() < 4096);
    assert_eq!(
        code(env.search(json!({"query": many, "mode": "hybrid"})).await),
        "INVALID_ARGUMENT"
    );
    assert_eq!(
        code(env.search(json!({"query": many, "mode": "semantic"})).await),
        "INVALID_ARGUMENT"
    );
    env.ok(json!({"query": many, "mode": "lexical"})).await;
    assert_eq!(env.provider.calls(), 0);
}

#[tokio::test]
async fn pure_paraphrase_retrieval_via_semantic() {
    // Tiny batches exercise the bounded batch loop.
    let env = Env::with(|c| c.search.vector_batch_bytes = 16 * 4 * 2).await;
    env.simple(
        "s_car",
        "omp",
        "/w",
        &[("the automobile broke down again", Some(T0))],
    )
    .await;
    env.simple(
        "s_cook",
        "omp",
        "/w",
        &[("baking dinner in the kitchen", Some(T0))],
    )
    .await;
    env.simple("s_db", "omp", "/w", &[("sqlite storage layout", Some(T0))])
        .await;
    for i in 0..6 {
        env.simple(
            &format!("s_x{i}"),
            "omp",
            "/w",
            &[(&format!("melody and guitar {i}"), Some(T0))],
        )
        .await;
    }
    let q = json!({"query": "driving a sedan", "mode": "semantic"});
    let r = env.ok(q.clone()).await;
    assert_eq!(ids(&r)[0], "s_car");
    assert_eq!(
        matches_of(&r, "s_car")[0]["matched_by"],
        json!(["semantic"])
    );
    assert_eq!(r["mode_used"], "semantic");
    let lex = env
        .ok(json!({"query": "driving a sedan", "mode": "lexical"}))
        .await;
    assert!(
        lex["results"].as_array().unwrap().is_empty(),
        "no keyword overlap"
    );
    // The query is embedded exactly once per search.
    let before = env.provider.calls_for(Purpose::Query);
    env.ok(json!({"query": "wire connection", "mode": "semantic"}))
        .await;
    assert_eq!(env.provider.calls_for(Purpose::Query), before + 1);
}

#[tokio::test]
async fn hybrid_returns_session_absent_from_lexical_candidates() {
    let env = Env::new().await;
    env.simple(
        "s_sem",
        "omp",
        "/w",
        &[("the automobile stalls again", Some(T0))],
    )
    .await;
    env.simple(
        "s_lex",
        "omp",
        "/w",
        &[("sedan trouble at the shop", Some(T0))],
    )
    .await;
    env.simple("s_none", "omp", "/w", &[("baking dinner", Some(T0))])
        .await;
    let lex = env
        .ok(json!({"query": "sedan trouble", "mode": "lexical"}))
        .await;
    assert_eq!(ids(&lex), ["s_lex"]);
    let r = env
        .ok(json!({"query": "sedan trouble", "mode": "hybrid"}))
        .await;
    let got = ids(&r);
    assert!(
        got.contains(&"s_sem".to_string()) && got.contains(&"s_lex".to_string()),
        "{got:?}"
    );
    assert_eq!(
        matches_of(&r, "s_sem")[0]["matched_by"],
        json!(["semantic"])
    );
    assert_eq!(
        matches_of(&r, "s_lex")[0]["matched_by"],
        json!(["lexical", "semantic"])
    );
    assert_eq!(got[0], "s_lex", "found by both retrievers ranks first");
    assert_eq!(r["mode_used"], "hybrid");
}

#[tokio::test]
async fn filters_apply_before_candidate_limits() {
    let env = Env::with(|c| {
        c.search.candidate_limit = 5;
        c.search.max_candidate_limit = 5;
    })
    .await;
    // 30 strong lexical hits that the filters exclude, 1 weak eligible hit.
    for i in 0..30 {
        env.simple(
            &format!("omp_{i:02}"),
            "omp",
            "/w/noise",
            &[("widget widget widget", Some(T0))],
        )
        .await;
    }
    env.simple(
        "codex_target",
        "codex",
        "/w/target",
        &[("a long story ending with a widget", Some(T0))],
    )
    .await;
    env.simple_with(
        "sub",
        "codex",
        "/w/target",
        "subagent",
        T0,
        &[("widget widget", Some(T0))],
        true,
    )
    .await;
    for (extra, expect) in [
        (json!({"sources": ["codex"]}), vec!["codex_target"]),
        (
            json!({"session_ids": ["codex_target"]}),
            vec!["codex_target"],
        ),
        (json!({"project_path": "/w/target"}), vec!["codex_target"]),
        (
            json!({"project_path": "/w/target/./"}),
            vec!["codex_target"],
        ),
        (
            json!({"sources": ["codex"], "session_kinds": ["subagent"]}),
            vec!["sub"],
        ),
        (
            json!({"sources": ["codex"], "session_kinds": ["subagent", "interactive"]}),
            vec!["sub", "codex_target"],
        ),
    ] {
        for mode in ["lexical", "semantic"] {
            let mut a = json!({"query": "widget", "mode": mode});
            a.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let mut got = ids(&env.ok(a.clone()).await);
            got.sort();
            let mut want: Vec<String> = expect.iter().map(|s| s.to_string()).collect();
            want.sort();
            assert_eq!(got, want, "{a}");
        }
    }
    // Exact project match, not a substring match.
    env.simple(
        "alpha_extra",
        "omp",
        "/w/alpha-extra",
        &[("gadget", Some(T0))],
    )
    .await;
    env.simple("alpha", "omp", "/w/alpha", &[("gadget", Some(T0))])
        .await;
    assert_eq!(
        ids(&env
            .ok(json!({"query": "gadget", "mode": "lexical", "project_path": "/w/alpha"}))
            .await),
        ["alpha"]
    );
    // Default session kind is interactive only.
    assert!(
        !ids(&env
            .ok(json!({"query": "widget", "mode": "lexical", "limit": 50}))
            .await)
        .contains(&"sub".to_string())
    );
}

#[tokio::test]
async fn fusion_is_deterministic_and_uses_reciprocal_rank() {
    let env = Env::new().await;
    // Identical text -> identical bm25 and cosine: ties break on the stable chunk id, not insertion order.
    for (sid, lid) in [("s_z", "chunk-b"), ("s_y", "chunk-a"), ("s_x", "chunk-c")] {
        env.session(sid, "omp", "/w", "interactive", T0).await;
        let snap = format!("sn_{sid}");
        env.snapshot(sid, &snap, "published").await;
        env.message(
            &snap,
            "m0",
            0,
            "user",
            Some(T0),
            None,
            "gamma delta epsilon",
        )
        .await;
        env.chunk(
            sid,
            &snap,
            lid,
            search_common::Chunk {
                body: "USER: gamma delta epsilon",
                spans: vec![("m0", 0, 0, 19, false)],
                anchor: Some(T0),
                run: "r",
                ordinal: 0,
                embed: true,
            },
        )
        .await;
        env.activate(sid, &snap).await;
    }
    env.simple("s_other", "omp", "/w", &[("automobile search", Some(T0))])
        .await;
    let q =
        json!({"query": "gamma delta epsilon automobile", "mode": "hybrid", "debug_scores": true});
    let a = env.ok(q.clone()).await;
    let b = env.ok(q.clone()).await;
    assert_eq!(ids(&a), ids(&b));
    let tie: Vec<String> = ids(&a).into_iter().filter(|s| s != "s_other").collect();
    assert_eq!(
        tie,
        ["s_y", "s_z", "s_x"],
        "ties ordered by chunk logical id"
    );
    for r in a["results"].as_array().unwrap() {
        for m in r["matches"].as_array().unwrap() {
            let d = &m["debug"];
            let part = |k: &str| d[k].as_u64().map_or(0.0, |rank| 1.0 / (60.0 + rank as f64));
            let want = part("lexical_rank") + part("semantic_rank");
            assert!(
                (d["fused_score"].as_f64().unwrap() - want).abs() < 1e-12,
                "{d}"
            );
        }
    }
    // Session score is its best chunk, never a sum: a session with many weak chunks does not beat a strong single one.
    let env = Env::new().await;
    let many: Vec<(String, Option<i64>)> = (0..40)
        .map(|i| (format!("deploy note {i}"), Some(T0)))
        .collect();
    let refs: Vec<(&str, Option<i64>)> = many.iter().map(|(s, t)| (s.as_str(), *t)).collect();
    env.simple_with("s_many", "omp", "/w", "interactive", T0, &refs, false)
        .await;
    env.simple_with(
        "s_one",
        "omp",
        "/w",
        "interactive",
        T0,
        &[("deploy deploy deploy rollout", Some(T0))],
        false,
    )
    .await;
    let r = env.ok(json!({"query": "deploy", "mode": "lexical"})).await;
    assert_eq!(ids(&r)[0], "s_one");
}

#[tokio::test]
async fn prefer_recent_only_reorders_near_ties() {
    let env = Env::with(|c| c.search.max_search_response_bytes = 1 << 20).await;
    // 48 identical sessions: RRF scores of adjacent ranks differ by < 1% only deep in the list.
    for i in 0..48 {
        env.simple_with(
            &format!("q{i:02}"),
            "omp",
            "/w",
            "interactive",
            T0 + i * 1000,
            &[("quokka habitat", Some(T0))],
            false,
        )
        .await;
    }
    let q = |extra: bool| json!({"query": "quokka", "mode": "lexical", "limit": 50, "prefer_recent": extra, "debug_scores": true});
    let plain = env.ok(q(false)).await;
    let recent = env.ok(q(true)).await;
    let (p, r) = (ids(&plain), ids(&recent));
    assert_eq!(
        p[..5],
        ["q00", "q01", "q02", "q03", "q04"],
        "relevance then stable id"
    );
    assert_eq!(
        r[..30],
        p[..30],
        "recency never reorders clearly separated scores"
    );
    assert_ne!(r, p, "near-tied tail is reordered newest first");
    let mut sorted = r.clone();
    sorted.sort();
    assert_eq!(sorted, p);
    let score = |s: &str| {
        let res = recent["results"]
            .as_array()
            .unwrap()
            .iter()
            .find(|x| x["session_id"] == s)
            .unwrap();
        res["matches"][0]["debug"]["fused_score"].as_f64().unwrap()
    };
    for i in 0..r.len() {
        for j in i + 1..r.len() {
            assert!(
                score(&r[i]) >= score(&r[j]) * 0.99,
                "{} before {} is not a near-tie",
                r[i],
                r[j]
            );
        }
    }
}

#[tokio::test]
async fn duplicate_spans_are_merged_and_context_not_duplicated() {
    let env = Env::new().await;
    let sid = "s_dup";
    env.session(sid, "omp", "/w", "interactive", T0).await;
    let snap = "sn_dup";
    env.snapshot(sid, snap, "published").await;
    let mut parent: Option<String> = None;
    for i in 0..11 {
        let key = format!("m{i}");
        env.message(
            snap,
            &key,
            i,
            "user",
            Some(T0),
            parent.as_deref(),
            &format!("frobnicate item {i}"),
        )
        .await;
        parent = Some(key);
    }
    let len = |i: usize| format!("frobnicate item {i}").len() as i64;
    // (ordinal, core message, optional context message)
    for (ord, core, ctx) in [
        (1, 0, None),
        (2, 1, Some(0)),
        (5, 4, None),
        (7, 6, None),
        (9, 8, None),
        (10, 9, Some(8)),
    ] {
        let ck = format!("m{core}");
        let xk = ctx.map(|c| format!("m{c}"));
        let mut spans = vec![];
        if let Some(x) = &xk {
            spans.push((x.as_str(), 0, 0, len(ctx.unwrap()), true));
        }
        spans.push((ck.as_str(), 0, 0, len(core), false));
        let body = format!("USER: frobnicate item {core}");
        env.chunk(
            sid,
            snap,
            &format!("c{ord:02}"),
            search_common::Chunk {
                body: &body,
                spans,
                anchor: Some(T0),
                run: "run0",
                ordinal: ord,
                embed: false,
            },
        )
        .await;
    }
    env.activate(sid, snap).await;
    let r = env
        .ok(json!({"query": "frobnicate", "mode": "lexical"}))
        .await;
    let ms = matches_of(&r, sid);
    assert_eq!(
        ms.len(),
        3,
        "adjacent chunks merge, the cap is 3 distinct spans"
    );
    let mut shapes: Vec<Vec<String>> = ms
        .iter()
        .map(|m| {
            MatchRef::decode(&env.keys.current(), m["match_ref"].as_str().unwrap())
                .unwrap()
                .spans
                .into_iter()
                .map(|s| s.r)
                .collect()
        })
        .collect();
    shapes.sort();
    assert!(
        shapes.iter().any(|s| s == &["m0", "m1"]),
        "ordinals 1+2 merged: {shapes:?}"
    );
    assert!(shapes.iter().all(|s| !s.is_empty()));
    // The context span (m0 inside chunk 2, m8 inside chunk 10) is never part of a reference of its own.
    for s in &shapes {
        let uniq: std::collections::BTreeSet<_> = s.iter().collect();
        assert_eq!(uniq.len(), s.len(), "no duplicated spans: {s:?}");
    }
    let r = env
        .ok(json!({"query": "frobnicate", "mode": "lexical", "matches_per_session": 1}))
        .await;
    assert_eq!(matches_of(&r, sid).len(), 1);
}

#[tokio::test]
async fn long_session_does_not_hide_other_sessions() {
    let env = Env::with(|c| c.search.candidate_limit = 20).await;
    let many: Vec<(String, Option<i64>)> = (0..300)
        .map(|i| (format!("zephyr zephyr zephyr {i}"), Some(T0)))
        .collect();
    let refs: Vec<(&str, Option<i64>)> = many.iter().map(|(s, t)| (s.as_str(), *t)).collect();
    env.simple_with("s_long", "omp", "/w", "interactive", T0, &refs, false)
        .await;
    for i in 0..3 {
        let filler = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua";
        env.simple_with(
            &format!("s_other{i}"),
            "omp",
            "/w",
            "interactive",
            T0,
            &[(&format!("{filler} zephyr {filler} {i}"), Some(T0))],
            false,
        )
        .await;
    }
    let r = env
        .ok(json!({"query": "zephyr", "mode": "lexical", "limit": 4}))
        .await;
    let mut got = ids(&r);
    got.sort();
    assert_eq!(
        got,
        ["s_long", "s_other0", "s_other1", "s_other2"],
        "adaptive expansion past the first 20 candidates"
    );
    // Never unbounded: with max_candidate_limit capped the other sessions stay out of reach.
    let env = Env::with(|c| {
        c.search.candidate_limit = 20;
        c.search.max_candidate_limit = 40;
    })
    .await;
    env.simple_with("s_long", "omp", "/w", "interactive", T0, &refs, false)
        .await;
    env.simple_with("s_other", "omp", "/w", "interactive", T0, &[("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua zephyr", Some(T0))], false).await;
    assert_eq!(
        ids(&env
            .ok(json!({"query": "zephyr", "mode": "lexical", "limit": 4}))
            .await),
        ["s_long"]
    );
}

#[tokio::test]
async fn inactive_staging_retired_and_excluded_never_leak() {
    let env = Env::new().await;
    env.simple(
        "s_ok",
        "omp",
        "/w",
        &[("ordinary visible leaktoken text", Some(T0))],
    )
    .await;
    // excluded
    env.simple(
        "s_excl",
        "omp",
        "/w",
        &[("leaktoken in excluded", Some(T0))],
    )
    .await;
    env.exclude("s_excl").await;
    // staging snapshot next to an active one
    env.simple("s_stage", "omp", "/w", &[("plain", Some(T0))])
        .await;
    // no active snapshot, only staging
    env.session("s_noactive", "omp", "/w", "interactive", T0)
        .await;
    // retired_leased old generation / published but not active
    env.simple("s_retired", "omp", "/w", &[("plain", Some(T0))])
        .await;
    env.simple("s_oldpub", "omp", "/w", &[("plain", Some(T0))])
        .await;
    for (sid, snap, state) in [
        ("s_stage", "sn_stage_x", "staging"),
        ("s_noactive", "sn_na_x", "staging"),
        ("s_retired", "sn_ret_x", "retired_leased"),
        ("s_oldpub", "sn_old_x", "published"),
    ] {
        env.snapshot(sid, snap, state).await;
        env.message(
            snap,
            "m0",
            0,
            "user",
            Some(T0),
            None,
            "leaktoken in hidden snapshot",
        )
        .await;
        env.chunk(
            sid,
            snap,
            &format!("{snap}-c"),
            search_common::Chunk {
                body: "USER: leaktoken in hidden snapshot",
                spans: vec![("m0", 0, 0, 27, false)],
                anchor: Some(T0),
                run: "r",
                ordinal: 0,
                embed: true,
            },
        )
        .await;
    }
    for mode in ["lexical", "semantic", "hybrid", "literal"] {
        let r = env
            .ok(json!({"query": "leaktoken in hidden snapshot", "mode": mode, "limit": 50}))
            .await;
        let got = ids(&r);
        // s_stage/s_retired/s_oldpub still own a legitimate active chunk ("plain"); only their hidden chunks must not appear.
        for bad in ["s_excl", "s_noactive"] {
            assert!(
                !got.contains(&bad.to_string()),
                "{mode} leaked {bad}: {got:?}"
            );
        }
        for r in r["results"].as_array().unwrap() {
            for m in r["matches"].as_array().unwrap() {
                let ex = m["excerpt"].as_str().unwrap();
                assert!(
                    !ex.contains("hidden snapshot") && !ex.contains("in excluded"),
                    "{mode} excerpt came from a hidden snapshot"
                );
            }
        }
    }
    for mode in ["lexical", "literal"] {
        let got = ids(&env
            .ok(json!({"query": "leaktoken", "mode": mode, "limit": 50}))
            .await);
        assert_eq!(got, ["s_ok"], "{mode}");
    }
    let r = env
        .ok(json!({"query": "leaktoken", "mode": "lexical"}))
        .await;
    assert_eq!(
        r["coverage"]["indexed_sessions"], 4,
        "excluded and snapshot-less sessions are not counted"
    );
}

#[tokio::test]
async fn embedding_profiles_are_separated() {
    let env = Env::new().await;
    env.simple_with(
        "s_a",
        "omp",
        "/w",
        "interactive",
        T0,
        &[("the automobile broke down", Some(T0))],
        false,
    )
    .await;
    sqlx::query("INSERT INTO embedding_profiles(id, provider, endpoint_identity, model, dimensions, rendering_version, normalization, generation, created_at) VALUES ('other-profile','fake','x','other',16,1,'unit',1,0)")
        .execute(&env.index.writer)
        .await
        .unwrap();
    // The other profile holds the *perfect* vector for the query; it must never be scored.
    let body = "USER: the automobile broke down";
    env.embed_vec(
        "other-profile",
        &env.provider.vector_for("driving a sedan"),
        &spool::domain::digest(body.as_bytes()),
    )
    .await;
    let r = env
        .ok(json!({"query": "driving a sedan", "mode": "semantic"}))
        .await;
    assert!(r["results"].as_array().unwrap().is_empty());
    assert_eq!(r["coverage"]["embedded_chunks"], 0);
    assert_eq!(r["coverage"]["active_chunks"], 1);
    assert!(
        r["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["code"] == "semantic_index_empty")
    );
    // Once the active profile has a vector the session is found.
    env.embed(
        &env.profile_id.clone(),
        body,
        &spool::domain::digest(body.as_bytes()),
    )
    .await;
    let r = env
        .ok(json!({"query": "driving a sedan", "mode": "semantic"}))
        .await;
    assert_eq!(ids(&r), ["s_a"]);
    assert_eq!(r["coverage"]["embedded_chunks"], 1);
}

#[tokio::test]
async fn provider_fallback_and_typed_semantic_errors() {
    // Fresh environment per fault: auth blocking and the circuit breaker are sticky by design.
    async fn env_with(fault: Option<Fault>) -> Env {
        let env = Env::with(|c| {
            c.search.query_embedding_fallback_ms = 150;
            c.search.semantic_query_deadline_ms = 300;
        })
        .await;
        env.simple("s1", "omp", "/w", &[("sedan trouble", Some(T0))])
            .await;
        env.provider.set_fault(fault);
        env
    }
    for fault in [
        Fault::Hang,
        Fault::Server,
        Fault::Network,
        Fault::Auth,
        Fault::RateLimited(Duration::from_secs(30)),
    ] {
        let env = env_with(Some(fault.clone())).await;
        let started = std::time::Instant::now();
        let r = env.ok(json!({"query": "sedan trouble"})).await;
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "fallback is bounded"
        );
        assert_eq!(r["mode_requested"], "hybrid", "{fault:?}");
        assert_eq!(r["mode_used"], "lexical", "{fault:?}");
        assert_eq!(ids(&r), ["s1"]);
        let w = r["warnings"].as_array().unwrap();
        assert!(
            w.iter().any(|w| w["code"] == "query_embedding_unavailable"),
            "{fault:?}: {w:?}"
        );
    }
    for (fault, want) in [
        (Fault::Hang, "QUERY_EMBEDDING_FAILED"),
        (Fault::Auth, "EMBEDDING_PROFILE_ERROR"),
        (Fault::RateLimited(Duration::from_secs(30)), "RATE_LIMITED"),
    ] {
        let env = env_with(Some(fault.clone())).await;
        assert_eq!(
            code(
                env.search(json!({"query": "sedan trouble", "mode": "semantic"}))
                    .await
            ),
            want,
            "{fault:?}"
        );
    }
    let env = env_with(None).await;
    let r = env
        .ok(json!({"query": "sedan trouble", "mode": "hybrid"}))
        .await;
    assert_eq!(r["mode_used"], "hybrid");
    assert!(r["warnings"].as_array().unwrap().is_empty());

    // Lexical and literal never touch the provider, even when it is broken.
    let before = env.provider.calls();
    env.provider.set_fault(Some(Fault::Server));
    env.ok(json!({"query": "sedan", "mode": "lexical"})).await;
    env.ok(json!({"query": "sedan", "mode": "literal"})).await;
    assert_eq!(env.provider.calls(), before);
}

#[tokio::test]
async fn output_is_bounded_including_text_copy() {
    let env = Env::with(|c| c.search.max_search_response_bytes = 9000).await;
    let big = format!("budgettok {}", "word ".repeat(700)); // > 1,200 bytes
    for i in 0..12 {
        env.simple_with(
            &format!("s{i:02}"),
            "omp",
            "/w",
            "interactive",
            T0,
            &[(&big, Some(T0))],
            false,
        )
        .await;
    }
    let r = env
        .ok(json!({"query": "budgettok", "mode": "lexical", "limit": 12}))
        .await;
    assert!(
        tool_result_bytes(&r, false) <= 9000,
        "{}",
        tool_result_bytes(&r, false)
    );
    assert_eq!(r["truncated"], true);
    let n = r["results"].as_array().unwrap().len();
    assert!((1..12).contains(&n), "{n}");
    assert!(
        r["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["code"] == "response_truncated")
    );
    for s in r["results"].as_array().unwrap() {
        for m in s["matches"].as_array().unwrap() {
            assert!(m["excerpt"].as_str().unwrap().len() <= 1200);
            assert_eq!(m["truncated"], true);
            // refs of the kept matches stay valid
            MatchRef::decode(&env.keys.current(), m["match_ref"].as_str().unwrap()).unwrap();
        }
    }
    // Default budget: 10 sessions x 3 matches of long excerpts still fit 64 KiB.
    let env = Env::new().await;
    for i in 0..10 {
        let texts: Vec<(String, Option<i64>)> = (0..6)
            .map(|k| (format!("budgettok{k} {}", "word ".repeat(700)), Some(T0)))
            .collect();
        let t: Vec<(&str, Option<i64>)> = texts.iter().map(|(s, t)| (s.as_str(), *t)).collect();
        env.simple_with(&format!("d{i}"), "omp", "/w", "interactive", T0, &t, false)
            .await;
    }
    let r = env
        .ok(json!({"query": "budgettok0 budgettok2 budgettok4", "mode": "lexical"}))
        .await;
    assert!(tool_result_bytes(&r, false) <= 65_536);
}

#[tokio::test]
async fn date_anchor_semantics() {
    let env = Env::new().await;
    let (a, b, c) = (T0 - 3000, T0 - 1000, T0);
    env.simple("d_a", "omp", "/w", &[("marker one", Some(a))])
        .await;
    env.simple("d_b", "omp", "/w", &[("marker two", Some(b))])
        .await;
    env.simple("d_c", "omp", "/w", &[("marker three", Some(c))])
        .await;
    env.simple("d_u", "omp", "/w", &[("marker unknown", None)])
        .await;
    let f = |ms: i64| format_rfc3339(ms);
    let run = |extra: Value| {
        let mut args = json!({"query": "marker", "mode": "lexical"});
        args.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let env = &env;
        async move {
            let mut v = ids(&env.ok(args).await);
            v.sort();
            v
        }
    };
    assert_eq!(run(json!({})).await, ["d_a", "d_b", "d_c", "d_u"]);
    assert_eq!(
        run(json!({"after": f(b)})).await,
        ["d_b", "d_c"],
        "after is inclusive; unknown time excluded"
    );
    assert_eq!(
        run(json!({"after": f(b), "before": f(c)})).await,
        ["d_b"],
        "before is exclusive"
    );
    assert_eq!(run(json!({"before": f(b)})).await, ["d_a"]);
    assert_eq!(
        run(json!({"after": f(b), "before": f(c), "include_unknown_time": true})).await,
        ["d_b", "d_u"]
    );
    assert_eq!(
        run(json!({"include_unknown_time": true})).await,
        ["d_a", "d_b", "d_c", "d_u"]
    );
    let r = env
        .ok(json!({"query": "marker", "mode": "lexical", "after": f(b)}))
        .await;
    assert_eq!(matches_of(&r, "d_b")[0]["matched_at"], f(b));
    assert_eq!(parse_rfc3339_utc(&f(b)), Some(b));

    // Context before `after` is returned only as labelled context of an in-interval match.
    env.session("d_ctx", "omp", "/w", "interactive", T0).await;
    env.snapshot("d_ctx", "sn_ctx", "published").await;
    env.message(
        "sn_ctx",
        "m0",
        0,
        "user",
        Some(T0 - 100_000),
        None,
        "earlier marker context",
    )
    .await;
    env.message(
        "sn_ctx",
        "m1",
        1,
        "assistant",
        Some(T0),
        Some("m0"),
        "marker answer",
    )
    .await;
    env.chunk(
        "d_ctx",
        "sn_ctx",
        "ctx-1",
        search_common::Chunk {
            body: "USER: earlier marker context\n\nASSISTANT: marker answer",
            spans: vec![("m0", 0, 0, 22, true), ("m1", 0, 0, 13, false)],
            anchor: Some(T0),
            run: "r",
            ordinal: 0,
            embed: false,
        },
    )
    .await;
    env.activate("d_ctx", "sn_ctx").await;
    let r = env.ok(json!({"query": "marker answer", "mode": "lexical", "after": f(T0 - 1000), "session_ids": ["d_ctx"]})).await;
    assert_eq!(
        matches_of(&r, "d_ctx")[0]["context_outside_time_filter"],
        true
    );
    let r = env.ok(json!({"query": "marker answer", "mode": "lexical", "after": f(T0 - 200_000), "session_ids": ["d_ctx"]})).await;
    assert_eq!(
        matches_of(&r, "d_ctx")[0]["context_outside_time_filter"],
        false
    );
    let r = env
        .ok(json!({"query": "marker answer", "mode": "lexical", "session_ids": ["d_ctx"]}))
        .await;
    assert_eq!(
        matches_of(&r, "d_ctx")[0]["context_outside_time_filter"],
        false,
        "no time filter, nothing to label"
    );
}

#[tokio::test]
async fn lease_budget_exhaustion_is_resource_limit() {
    let env = Env::with(|c| c.reader.max_active_leases = 0).await;
    env.simple("s1", "omp", "/w", &[("zebra", Some(T0))]).await;
    let e = env
        .search(json!({"query": "zebra", "mode": "lexical"}))
        .await
        .unwrap_err();
    assert_eq!(e.code, "RESOURCE_LIMIT");
    assert!(e.retryable);
    // No results means nothing to pin, so an empty search still succeeds.
    assert!(
        env.ok(json!({"query": "nomatch", "mode": "lexical"})).await["results"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn match_refs_round_trip_with_one_lease() {
    let env = Env::new().await;
    env.simple(
        "s1",
        "omp",
        "/w/one",
        &[("alpha omega", Some(T0)), ("irrelevant filler", Some(T0))],
    )
    .await;
    env.simple("s2", "codex", "/w/two", &[("omega again", Some(T0))])
        .await;
    let r = env.ok(json!({"query": "omega", "mode": "lexical"})).await;
    assert_eq!(r["schema_version"], 1);
    assert_eq!(r["mode_requested"], "lexical");
    assert_eq!(r["mode_used"], "lexical");
    assert!(r["index_epoch"].as_i64().unwrap() >= 2);
    assert_eq!(r["truncated"], false);
    let s1 = r["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["session_id"] == "s1")
        .unwrap();
    assert_eq!(s1["source"], "omp");
    assert_eq!(s1["title"], "title s1");
    assert_eq!(s1["title_origin"], "native");
    assert_eq!(s1["project_path"], "/w/one");
    assert_eq!(s1["session_kind"], "interactive");
    assert_eq!(s1["source_status"], "present");
    assert_eq!(r["coverage"]["indexed_sessions"], 2);
    assert_eq!(r["coverage"]["active_chunks"], 3);
    assert_eq!(r["coverage"]["pending_sessions"], 0);
    assert_eq!(
        r["coverage"]["last_successful_discovery_at"],
        format_rfc3339(T0 - 1000)
    );
    assert!(!r["coverage"]["scope"].as_str().unwrap().is_empty());

    let key = env.keys.current();
    let mut lease_ids = std::collections::BTreeSet::new();
    let mut manifests = std::collections::BTreeSet::new();
    for s in r["results"].as_array().unwrap() {
        for m in s["matches"].as_array().unwrap() {
            let mr = MatchRef::decode(&key, m["match_ref"].as_str().unwrap()).unwrap();
            assert_eq!(mr.anchor.session, s["session_id"]);
            assert_eq!(
                mr.anchor.snapshot,
                format!("sn_{}", s["session_id"].as_str().unwrap())
            );
            lease_ids.insert(mr.anchor.lease.clone());
            manifests.insert(mr.anchor.manifest.clone());
            let expect_len = if s["session_id"] == "s1" {
                "alpha omega".len()
            } else {
                "omega again".len()
            };
            assert_eq!(
                (
                    mr.spans[0].r.as_str(),
                    mr.spans[0].b,
                    mr.spans[0].s as usize,
                    mr.spans[0].e as usize
                ),
                ("m0", 0, 0, expect_len)
            );
            let text: &[u8] = if s["session_id"] == "s1" {
                b"alpha omega"
            } else {
                b"omega again"
            };
            assert_eq!(mr.content, frames(&[text]));
            assert!(
                env.leases
                    .covers(&mr.anchor.lease, &mr.anchor.manifest)
                    .await
                    .unwrap()
            );
            let want_exp = T0 + env.leases.ttl_ms();
            assert_eq!(m["match_ref_expires_at"], format_rfc3339(want_exp));
            assert_eq!(
                env.leases.state(&mr.anchor.lease).await.unwrap(),
                LeaseState::Live {
                    expires_at_ms: want_exp
                }
            );
            assert!(
                m.get("branch_ref").is_none(),
                "linear sessions never need a branch_ref"
            );
        }
    }
    assert_eq!(lease_ids.len(), 1, "one lease per response");
    assert_eq!(manifests.len(), 2);
    // A rotated key invalidates previously issued refs.
    let tok = r["results"][0]["matches"][0]["match_ref"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(MatchRef::decode(&key, &tok).is_ok());
}

#[tokio::test]
async fn branch_ref_is_issued_only_when_needed() {
    let env = Env::new().await;
    env.session("s_fork", "omp", "/w", "interactive", T0).await;
    env.snapshot("s_fork", "sn_fork", "published").await;
    env.message(
        "sn_fork",
        "m0",
        0,
        "user",
        Some(T0),
        None,
        "root question pivotword",
    )
    .await;
    env.message(
        "sn_fork",
        "m1",
        1,
        "assistant",
        Some(T0),
        Some("m0"),
        "shared answer",
    )
    .await;
    env.message(
        "sn_fork",
        "m2a",
        2,
        "user",
        Some(T0),
        Some("m1"),
        "left branch tailword",
    )
    .await;
    env.message(
        "sn_fork",
        "m2b",
        3,
        "user",
        Some(T0),
        Some("m1"),
        "right branch",
    )
    .await;
    let ch =
        |run: &'static str, ord, key: &'static str, len, body: &'static str| search_common::Chunk {
            body,
            spans: vec![(key, 0, 0, len, false)],
            anchor: Some(T0),
            run,
            ordinal: ord,
            embed: false,
        };
    env.chunk(
        "s_fork",
        "sn_fork",
        "k0",
        ch("runA", 0, "m0", 23, "USER: root question pivotword"),
    )
    .await;
    env.chunk(
        "s_fork",
        "sn_fork",
        "k1",
        ch("runB", 0, "m2a", 20, "USER: left branch tailword"),
    )
    .await;
    env.activate("s_fork", "sn_fork").await;
    env.simple(
        "s_lin",
        "omp",
        "/w",
        &[("linear tailword", Some(T0)), ("more", Some(T0))],
    )
    .await;

    let r = env
        .ok(json!({"query": "tailword", "mode": "lexical"}))
        .await;
    let m = &matches_of(&r, "s_fork")[0];
    let br = BranchRef::decode(
        &env.keys.current(),
        m["branch_ref"]
            .as_str()
            .expect("unique leaf under the match"),
    )
    .unwrap();
    assert_eq!(br.leaf, "m2a");
    assert_eq!(br.anchor.snapshot, "sn_fork");
    assert!(matches_of(&r, "s_lin")[0].get("branch_ref").is_none());
    // Before the fork the match belongs to two branches: omitted, the reader answers BRANCH_REQUIRED.
    let r = env
        .ok(json!({"query": "pivotword", "mode": "lexical"}))
        .await;
    assert!(matches_of(&r, "s_fork")[0].get("branch_ref").is_none());
}

#[tokio::test]
async fn concurrent_semantic_searches_share_the_permit_pool() {
    let env = std::sync::Arc::new(Env::with(|c| c.search.semantic_scan_concurrency = 1).await);
    for i in 0..4 {
        env.simple(
            &format!("s{i}"),
            "omp",
            "/w",
            &[(&format!("automobile number {i}"), Some(T0))],
        )
        .await;
    }
    let mut hs = Vec::new();
    for _ in 0..6 {
        let e = env.clone();
        hs.push(tokio::spawn(async move {
            e.ok(json!({"query": "driving a sedan", "mode": "semantic"}))
                .await
        }));
    }
    for h in hs {
        assert_eq!(ids(&h.await.unwrap()).len(), 4);
    }
}

#[tokio::test]
async fn tool_definition_is_honest_about_hosted_modes() {
    let t = SearchService::tool_definition();
    assert_eq!(t.name, "search_sessions");
    let d = t.description.as_deref().unwrap();
    assert!(d.contains("SEND THE QUERY TEXT") && d.contains("hosted"));
    assert!(
        d.contains("`lexical`")
            && d.contains("`literal`")
            && d.contains("hosted embedding provider")
    );
    let a = t.annotations.as_ref().unwrap();
    assert_eq!(a.open_world_hint, Some(true));
    assert_eq!(a.read_only_hint, Some(true));
    let input = serde_json::to_value(&*t.input_schema).unwrap();
    assert_eq!(input["additionalProperties"], false);
    assert!(
        input["required"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r == "query")
    );
    let out = serde_json::to_value(t.output_schema.as_ref().unwrap().as_ref()).unwrap();
    for f in [
        "schema_version",
        "mode_requested",
        "mode_used",
        "index_epoch",
        "coverage",
        "results",
        "warnings",
        "truncated",
    ] {
        assert!(
            out["properties"].get(f).is_some(),
            "output schema lacks {f}"
        );
    }
}

#[tokio::test]
async fn real_outputs_validate_against_the_output_schema() {
    let t = SearchService::tool_definition();
    let schema = serde_json::to_value(t.output_schema.as_ref().unwrap().as_ref()).unwrap();
    let v = jsonschema::validator_for(&schema).unwrap();
    let env = Env::new().await;
    env.simple(
        "s1",
        "omp",
        "/w",
        &[("what?! automobile sedan trouble", Some(T0))],
    )
    .await;
    env.simple("s2", "omp", "/w", &[("the automobile stalls", Some(T0))])
        .await;
    let mut outs = vec![];
    for a in [
        json!({"query": "sedan", "mode": "lexical"}),
        json!({"query": "sedan", "mode": "lexical", "debug_scores": true}),
        json!({"query": "?!", "mode": "lexical"}),
        json!({"query": "?!", "mode": "literal"}),
        json!({"query": "driving a sedan", "mode": "semantic", "debug_scores": true}),
        json!({"query": "driving a sedan", "mode": "hybrid", "debug_scores": true}),
        json!({"query": "nothing matches zzz", "mode": "lexical"}),
    ] {
        outs.push(env.ok(a).await);
    }
    env.provider.set_fault(Some(Fault::Server));
    outs.push(env.ok(json!({"query": "sedan"})).await);
    for o in outs {
        let errs: Vec<String> = v.iter_errors(&o).map(|e| e.to_string()).collect();
        assert!(errs.is_empty(), "{errs:?} for {o}");
    }
}

/// Spec 13.2: time filters and `matched_at` use the anchor of the chunk that holds the match, in every mode. A
/// literal hit in an earlier message of a chunk is therefore dated by that chunk, not by its own message.
#[tokio::test]
async fn literal_uses_the_chunk_anchor_like_lexical() {
    let env = Env::new().await;
    let f = |ms: i64| format_rfc3339(ms);
    let (t1, t2) = (T0 + 1000, T0 + 2000);
    // One chunk holds the user message (t1) and the assistant message (t2); its anchor is the last message.
    env.session("lit_a", "omp", "/w", "interactive", T0).await;
    env.snapshot("lit_a", "sn_a", "published").await;
    env.message("sn_a", "m1", 0, "user", Some(t1), None, "ask [L]? now")
        .await;
    env.message(
        "sn_a",
        "m2",
        1,
        "assistant",
        Some(t2),
        Some("m1"),
        "northstar answer",
    )
    .await;
    env.chunk(
        "lit_a",
        "sn_a",
        "lit_a-c0",
        search_common::Chunk {
            body: "USER: ask [L]? now\n\nASSISTANT: northstar answer",
            spans: vec![("m1", 0, 0, 12, false), ("m2", 0, 0, 16, false)],
            anchor: Some(t2),
            run: "r",
            ordinal: 0,
            embed: false,
        },
    )
    .await;
    env.activate("lit_a", "sn_a").await;
    let inside = json!({"after": f(t1 + 500), "before": f(t2 + 500)});
    let outside = json!({"after": f(T0 + 500), "before": f(t1 + 500)});
    let q = |query: &str, mode: &str, window: &Value| {
        let mut a = json!({"query": query, "mode": mode});
        a.as_object_mut()
            .unwrap()
            .extend(window.as_object().unwrap().clone());
        env.ok(a)
    };
    for (query, mode) in [("northstar", "lexical"), ("[L]?", "literal")] {
        let r = q(query, mode, &inside).await;
        assert_eq!(
            ids(&r),
            ["lit_a"],
            "{mode}: the chunk anchor is inside the window"
        );
        assert_eq!(
            matches_of(&r, "lit_a")[0]["matched_at"],
            f(t2),
            "{mode}: matched_at is the chunk anchor"
        );
        assert_eq!(
            matches_of(&r, "lit_a")[0]["context_outside_time_filter"],
            true,
            "{mode}: the returned text of m1 (t1) lies outside the interval"
        );
        assert!(
            ids(&q(query, mode, &outside).await).is_empty(),
            "{mode}: the earlier message's own time must not admit the match"
        );
    }
    // The matched text itself is in-window (m2, t2): no outside-interval label; the same chunk's earlier text has one.
    let r = q("northstar", "literal", &inside).await;
    assert_eq!(matches_of(&r, "lit_a")[0]["matched_at"], f(t2));
    assert_eq!(
        matches_of(&r, "lit_a")[0]["context_outside_time_filter"],
        false
    );

    // A match in a message that is only CONTEXT of a later chunk is dated by the chunk that owns it.
    env.session("lit_b", "omp", "/w", "interactive", T0).await;
    env.snapshot("lit_b", "sn_b", "published").await;
    env.message("sn_b", "m1", 0, "user", Some(t1), None, "ask [L]? now")
        .await;
    env.message(
        "sn_b",
        "m2",
        1,
        "assistant",
        Some(t2),
        Some("m1"),
        "northstar answer",
    )
    .await;
    for (ordinal, anchor, spans, body) in [
        (0, t1, vec![("m1", 0, 0, 12, false)], "USER: ask [L]? now"),
        (
            1,
            t2,
            vec![("m1", 0, 0, 12, true), ("m2", 0, 0, 16, false)],
            "USER: ask [L]? now\n\nASSISTANT: northstar answer",
        ),
    ] {
        env.chunk(
            "lit_b",
            "sn_b",
            &format!("lit_b-c{ordinal}"),
            search_common::Chunk {
                body,
                spans,
                anchor: Some(anchor),
                run: "r",
                ordinal,
                embed: false,
            },
        )
        .await;
    }
    env.activate("lit_b", "sn_b").await;
    let mut a = json!({"query": "[L]?", "mode": "literal", "session_ids": ["lit_b"]});
    a.as_object_mut()
        .unwrap()
        .extend(outside.as_object().unwrap().clone());
    let r = env.ok(a.clone()).await;
    assert_eq!(matches_of(&r, "lit_b")[0]["matched_at"], f(t1));
    a.as_object_mut().unwrap().extend(
        json!({"after": f(t1 + 500), "before": f(t2 + 500)})
            .as_object()
            .unwrap()
            .clone(),
    );
    assert!(
        ids(&env.ok(a).await).is_empty(),
        "the context copy in the later chunk does not date the match"
    );
}

/// Spec 10.6: a model or dimension change builds the new profile in the background while queries keep using the
/// old active profile; the switch happens in one transaction once the CURRENTLY published corpus is covered.
#[tokio::test]
async fn profile_migration_queries_the_old_profile_until_the_current_corpus_is_covered() {
    use spool::domain::digest;
    use spool::embeddings::FakeProvider;
    use spool::embeddings::provider::EmbeddingProvider;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    let env = Env::new().await;
    let old_id = env.profile_id.clone();
    let body = |t: &str| format!("USER: {t}");
    for (id, t) in [
        ("m1", "driving a sedan today"),
        ("m2", "kernel scheduler latency"),
    ] {
        env.simple(id, "omp", "/w", &[(t, Some(T0))]).await;
    }

    // The configured profile changes model and dimensions; the old one is rebuilt only on the same endpoint.
    let fake_b = Arc::new(FakeProvider::new(24).with_model("fake-b", 1));
    let fake_old = Arc::new(FakeProvider::new(16));
    let mut ecfg = env.cfg.embedding.clone();
    ecfg.dimensions = 24;
    let old_for_factory = fake_old.clone();
    let shared: spool::clock::SharedClock = Arc::new(env.clock.clone());
    let svc_b = Arc::new(
        spool::embeddings::EmbeddingService::new(
            env.index.clone(),
            ecfg,
            env.cfg.search.clone(),
            shared.clone(),
            fake_b.clone(),
        )
        .with_profile_providers(Arc::new(move |spec| {
            (spec.dimensions == 16).then(|| old_for_factory.clone() as Arc<dyn EmbeddingProvider>)
        })),
    );
    let new_id = svc_b.ensure_profile().await.unwrap();
    assert_ne!(new_id, old_id);
    let active_in_db = || async {
        sqlx::query_scalar::<_, String>(
            "SELECT value FROM app_meta WHERE key='active_embedding_profile'",
        )
        .fetch_one(&env.index.reader)
        .await
        .unwrap()
    };
    assert_eq!(
        active_in_db().await,
        old_id,
        "a changed profile never activates itself"
    );
    assert_eq!(svc_b.active_profile(), (old_id.clone(), 16));
    let search = SearchService::new(
        env.index.clone(),
        env.cfg.clone(),
        svc_b.clone(),
        env.leases.clone(),
        env.keys.clone(),
        shared,
    );
    let sem = |q: &'static str| json!({"query": q, "mode": "semantic"});

    // Old semantic recall keeps working; the query is embedded by the OLD profile, never by the new model.
    let r = search.call(sem("driving a sedan")).await.unwrap();
    assert_eq!(ids(&r)[0], "m1");
    assert_eq!(fake_old.calls_for(Purpose::Query), 1);
    assert_eq!(
        fake_b.calls_for(Purpose::Query),
        0,
        "no new-model query before the switch"
    );

    // Partial coverage of the new profile does not switch.
    let cancel = AtomicBool::new(false);
    svc_b
        .embed_documents(
            vec![(
                digest(body("driving a sedan today").as_bytes()),
                body("driving a sedan today"),
            )],
            &cancel,
        )
        .await
        .unwrap();
    assert_eq!(active_in_db().await, old_id);
    assert_eq!(
        ids(&search.call(sem("driving a sedan")).await.unwrap())[0],
        "m1"
    );

    // A session published meanwhile (this one needs vectors the new profile lacks) blocks the switch even after
    // everything that was published when the migration started is covered.
    env.simple_with(
        "m3",
        "omp",
        "/w",
        "interactive",
        T0,
        &[("garden tomato soil", Some(T0))],
        false,
    )
    .await;
    svc_b
        .embed_documents(
            vec![(
                digest(body("kernel scheduler latency").as_bytes()),
                body("kernel scheduler latency"),
            )],
            &cancel,
        )
        .await
        .unwrap();
    assert_eq!(
        active_in_db().await,
        old_id,
        "the older migration snapshot finishing is not enough"
    );
    // A later session whose input hash is already cached for the new profile is covered without a request.
    env.simple_with(
        "m4",
        "omp",
        "/w",
        "interactive",
        T0,
        &[("driving a sedan today", Some(T0))],
        false,
    )
    .await;
    let before = fake_b.calls_for(Purpose::Ingest);
    svc_b
        .embed_documents(
            vec![(
                digest(body("driving a sedan today").as_bytes()),
                body("driving a sedan today"),
            )],
            &cancel,
        )
        .await
        .unwrap();
    assert_eq!(
        fake_b.calls_for(Purpose::Ingest),
        before,
        "cached hash needs no request"
    );
    assert_eq!(active_in_db().await, old_id, "m3 is still uncovered");

    // Full coverage of the current corpus switches atomically; queries now use the new model and dimensions.
    let epoch = env.index.epoch().await.unwrap();
    svc_b
        .embed_documents(
            vec![(
                digest(body("garden tomato soil").as_bytes()),
                body("garden tomato soil"),
            )],
            &cancel,
        )
        .await
        .unwrap();
    assert_eq!(active_in_db().await, new_id);
    assert!(env.index.epoch().await.unwrap() > epoch);
    assert_eq!(svc_b.active_profile(), (new_id.clone(), 24));
    let r = search.call(sem("garden tomato")).await.unwrap();
    assert_eq!(ids(&r)[0], "m3");
    assert_eq!(fake_b.calls_for(Purpose::Query), 1);
    assert_eq!(
        fake_old.calls_for(Purpose::Query),
        1,
        "the old provider is no longer asked"
    );
}
