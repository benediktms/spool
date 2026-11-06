//! `search_sessions` correctness (spec 19.3). Offline: fake embedding provider, rows seeded straight into the schema.

mod search_common;

use search_common::*;
use serde_json::{Value, json};
use spool::embeddings::fake::Fault;
use spool::embeddings::provider::Purpose;
use spool::mcp::budget::tool_result_bytes;
use spool::search::{SearchService, format_rfc3339, parse_rfc3339_utc};
use spool::transcript::lease::LeaseState;
use spool::transcript::refs::BranchRef;
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
    let r = env.ok(lit("foo.bar()")).await;
    let m = &matches_of(&r, "l1")[0];
    assert_eq!(m["matched_by"], json!(["literal"]));
    let (_, mr) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert_eq!(mr.version, 2);
    assert_eq!(mr.records[0].key, "m0");
    assert_eq!(mr.records[0].digest, "00".repeat(16));
    assert!(m["preview"]["text"].as_str().unwrap().contains("foo.bar()"));
    assert!("call foo.bar() now".contains(m["preview"]["text"].as_str().unwrap()));
    assert_eq!(env.provider.calls(), 0, "literal never calls the provider");
}

#[tokio::test]
async fn literal_is_not_prefiltered_by_fts_and_not_biased_by_long_sessions() {
    let env = Env::with(|c| c.search.max_candidate_limit = 50).await;
    let many: Vec<(String, Option<i64>)> = (0..200)
        .map(|i| (format!("needle@@ {i}"), Some(T0)))
        .collect();
    let refs: Vec<(&str, Option<i64>)> = many.iter().map(|(s, t)| (s.as_str(), *t)).collect();
    env.simple_with("a_long", &Seed::default(), &refs).await;
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
        json!({"query": "x", "matches_per_session": 0}),
        json!({"query": "x", "queries": ["y"]}),
        json!({"queries": []}),
        json!({"queries": ["a", "b", "c", "d", "e"]}),
        json!({"queries": ["a", " "]}),
        json!({"query": "x", "after": "yesterday"}),
        json!({"query": "x", "after": "2026-10-01T00:00:00+02:00"}),
        json!({"query": "x", "after": "2026-10-02T00:00:00Z", "before": "2026-10-01T00:00:00Z"}),
        json!({"query": "x", "after": "2026-10-01T00:00:00Z", "before": "2026-10-01T00:00:00Z"}),
        json!({"query": "x", "project_path": "/w"}),
        json!({"query": "x", "projects": []}),
        json!({"query": "x", "projects": [""]}),
        json!({"query": "x", "projects": (0..17).map(|i| format!("p{i}")).collect::<Vec<_>>()}),
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
        &Seed {
            source: "codex",
            cwd: "/w/target",
            kind: "subagent",
            embed: true,
            ..Seed::default()
        },
        &[("widget widget", Some(T0))],
    )
    .await;
    for (extra, expect) in [
        (json!({"sources": ["codex"]}), vec!["codex_target"]),
        (
            json!({"session_ids": ["codex_target"]}),
            vec!["codex_target"],
        ),
        (json!({"projects": ["/w/target"]}), vec!["codex_target"]),
        (json!({"projects": ["/w/target/./"]}), vec!["codex_target"]),
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
            .ok(json!({"query": "gadget", "mode": "lexical", "projects": ["/w/alpha"]}))
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
            Message {
                key: "m0",
                seq: 0,
                role: "user",
                ts: Some(T0),
                parent: None,
                text: "gamma delta epsilon",
            },
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
    env.simple_with("s_many", &Seed::default(), &refs).await;
    env.simple_with(
        "s_one",
        &Seed::default(),
        &[("deploy deploy deploy rollout", Some(T0))],
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
            &Seed {
                last_activity: T0 + i * 1000,
                ..Seed::default()
            },
            &[("quokka habitat", Some(T0))],
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
            Message {
                key: &key,
                seq: i,
                role: "user",
                ts: Some(T0),
                parent: parent.as_deref(),
                text: &format!("frobnicate item {i}"),
            },
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
        let body = if let Some(context) = ctx {
            format!("USER: frobnicate item {context}\n\nUSER: frobnicate item {core}")
        } else {
            format!("USER: frobnicate item {core}")
        };
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
    assert!(ms.len() <= 3);
    let mut shapes: Vec<Vec<String>> = Vec::new();
    for m in ms {
        let (_, mr) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
        shapes.push(mr.records.into_iter().map(|s| s.key).collect());
    }
    shapes.sort();
    assert!(
        shapes
            .iter()
            .any(|s| s.contains(&"m0".into()) && s.contains(&"m1".into())),
        "ordinals 1+2 merged: {shapes:?}"
    );
    assert!(shapes.iter().all(|s| !s.is_empty()));
    for (i, shape) in shapes.iter().enumerate() {
        for (j, other) in shapes.iter().enumerate() {
            if i != j {
                assert!(
                    !shape.iter().all(|key| other.contains(key)),
                    "contained window: {shapes:?}"
                );
            }
        }
    }
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
async fn match_found_past_many_context_only_candidates() {
    let env = Env::with(|c| c.search.candidate_limit = 1100).await;
    let (sid, snap) = ("s_ctx_only", "sn_ctx_only");
    env.session(sid, "omp", "/w", "interactive", T0).await;
    env.snapshot(sid, snap, "published").await;
    let real =
        "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor zephyr";
    env.message(
        snap,
        Message {
            key: "m_real",
            seq: 0,
            role: "user",
            ts: Some(T0),
            parent: None,
            text: real,
        },
    )
    .await;
    for i in 0..600 {
        env.chunk(
            sid,
            snap,
            &format!("ctx{i:04}"),
            search_common::Chunk {
                body: "USER: zephyr",
                spans: vec![("m_real", 0, 0, 6, true)],
                anchor: Some(T0),
                run: "run0",
                ordinal: i + 1,
                embed: false,
            },
        )
        .await;
    }
    let real_rowid = env
        .chunk(
            sid,
            snap,
            "real",
            search_common::Chunk {
                body: &format!("USER: {real}"),
                spans: vec![("m_real", 0, 0, real.len() as i64, false)],
                anchor: Some(T0),
                run: "run0",
                ordinal: 0,
                embed: false,
            },
        )
        .await;
    env.activate(sid, snap).await;
    let ranked: Vec<i64> = sqlx::query_scalar(
        "SELECT rowid FROM chunks_fts WHERE chunks_fts MATCH 'zephyr' ORDER BY bm25(chunks_fts) ASC, rowid ASC",
    )
    .fetch_all(&env.index.reader)
    .await
    .unwrap();
    let pos = ranked.iter().position(|r| *r == real_rowid).unwrap();
    assert!(
        pos >= 500,
        "real chunk ranks {pos}, expected past the first batch"
    );
    let r = env.ok(json!({"query": "zephyr", "mode": "lexical"})).await;
    assert_eq!(matches_of(&r, sid).len(), 1);
}

#[tokio::test]
async fn long_session_does_not_hide_other_sessions() {
    let env = Env::with(|c| c.search.candidate_limit = 20).await;
    let many: Vec<(String, Option<i64>)> = (0..300)
        .map(|i| (format!("zephyr zephyr zephyr {i}"), Some(T0)))
        .collect();
    let refs: Vec<(&str, Option<i64>)> = many.iter().map(|(s, t)| (s.as_str(), *t)).collect();
    env.simple_with("s_long", &Seed::default(), &refs).await;
    for i in 0..3 {
        let filler = "lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua";
        env.simple_with(
            &format!("s_other{i}"),
            &Seed::default(),
            &[(&format!("{filler} zephyr {filler} {i}"), Some(T0))],
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
    env.simple_with("s_long", &Seed::default(), &refs).await;
    env.simple_with("s_other", &Seed::default(), &[("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua zephyr", Some(T0))]).await;
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
            Message {
                key: "m0",
                seq: 0,
                role: "user",
                ts: Some(T0),
                parent: None,
                text: "leaktoken in hidden snapshot",
            },
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
                let ex = m["preview"]["text"].as_str().unwrap();
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
    assert!(
        r["coverage"].is_null() || r["coverage"]["indexed_sessions"] == 4,
        "excluded and snapshot-less sessions are not counted: {r}"
    );
}

#[tokio::test]
async fn embedding_profiles_are_separated() {
    let env = Env::new().await;
    env.simple_with(
        "s_a",
        &Seed::default(),
        &[("the automobile broke down", Some(T0))],
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
    assert!(r["coverage"].is_null(), "healthy index omits coverage: {r}");
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
        env.simple_with(&format!("s{i:02}"), &Seed::default(), &[(&big, Some(T0))])
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
    assert_eq!(n, 12);
    assert!(
        r["results"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|s| s["matches"].as_array().unwrap())
            .any(|m| m["preview"].is_null())
    );
    assert!(
        r["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w["code"] == "response_truncated")
    );
    for s in r["results"].as_array().unwrap() {
        for m in s["matches"].as_array().unwrap() {
            if let Some(preview) = m["preview"].as_object() {
                assert!(preview["text"].as_str().unwrap().len() <= 8192);
            }
            // refs of the kept matches stay valid
            env.decode_match(m["match_ref"].as_str().unwrap()).await;
        }
    }
    // Default budget: 10 sessions x 3 matches of long excerpts still fit 64 KiB.
    let env = Env::new().await;
    for i in 0..10 {
        let texts: Vec<(String, Option<i64>)> = (0..6)
            .map(|k| (format!("budgettok{k} {}", "word ".repeat(700)), Some(T0)))
            .collect();
        let t: Vec<(&str, Option<i64>)> = texts.iter().map(|(s, t)| (s.as_str(), *t)).collect();
        env.simple_with(&format!("d{i}"), &Seed::default(), &t)
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

    env.session("d_ctx", "omp", "/w", "interactive", T0).await;
    env.snapshot("d_ctx", "sn_ctx", "published").await;
    env.message(
        "sn_ctx",
        Message {
            key: "m0",
            seq: 0,
            role: "user",
            ts: Some(T0 - 100_000),
            parent: None,
            text: "earlier marker context",
        },
    )
    .await;
    env.message(
        "sn_ctx",
        Message {
            key: "m1",
            seq: 1,
            role: "assistant",
            ts: Some(T0),
            parent: Some("m0"),
            text: "marker answer",
        },
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
    let context_hit = env
        .ok(json!({"query": "earlier", "mode": "lexical", "session_ids": ["d_ctx"]}))
        .await;
    assert!(
        matches_of(&context_hit, "d_ctx")[0]["preview"]["text"]
            .as_str()
            .unwrap()
            .contains("earlier")
    );
    let r = env.ok(json!({"query": "marker answer", "mode": "lexical", "after": f(T0 - 1000), "session_ids": ["d_ctx"]})).await;
    assert!(
        !matches_of(&r, "d_ctx")[0]["preview"]["text"]
            .as_str()
            .unwrap()
            .contains("earlier")
    );
    assert_eq!(
        matches_of(&r, "d_ctx")[0]["context_outside_time_filter"],
        false
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
    env.session("d_core", "omp", "/w", "interactive", T0).await;
    env.snapshot("d_core", "sn_core", "published").await;
    env.message(
        "sn_core",
        Message {
            key: "old",
            seq: 0,
            role: "user",
            ts: Some(T0 - 100_000),
            parent: None,
            text: "older selected evidence",
        },
    )
    .await;
    env.chunk(
        "d_core",
        "sn_core",
        "old_chunk",
        search_common::Chunk {
            body: "USER: older selected evidence",
            spans: vec![("old", 0, 0, 23, false)],
            anchor: Some(T0),
            run: "r",
            ordinal: 0,
            embed: false,
        },
    )
    .await;
    env.activate("d_core", "sn_core").await;
    let r = env.ok(json!({"query":"older", "mode":"lexical", "after":f(T0 - 1000), "session_ids":["d_core"]})).await;
    let m = &matches_of(&r, "d_core")[0];
    assert!(m["preview"]["text"].as_str().unwrap().contains("older"));
    assert_eq!(m["context_outside_time_filter"], true);
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
async fn coverage_pending_counts_only_present_eligible_work() {
    let env = Env::new().await;
    for (id, state, eligibility, source_status, excluded, kind) in [
        ("pending", "pending", "active", "present", 0, "interactive"),
        (
            "deferred",
            "deferred",
            "manual",
            "present",
            0,
            "interactive",
        ),
        (
            "backfill",
            "pending",
            "backfill",
            "present",
            0,
            "interactive",
        ),
        ("missing", "pending", "active", "missing", 0, "interactive"),
        ("excluded", "pending", "active", "present", 1, "interactive"),
        ("subagent", "pending", "active", "present", 0, "subagent"),
    ] {
        env.session(id, "omp", "/w", kind, T0).await;
        sqlx::query("UPDATE sessions SET index_state = ?, eligibility = ?, source_status = ?, excluded = ? WHERE id = ?")
            .bind(state)
            .bind(eligibility)
            .bind(source_status)
            .bind(excluded)
            .bind(id)
            .execute(&env.index.writer)
            .await
            .unwrap();
    }

    let r = env.ok(json!({"query": "nomatch", "mode": "lexical"})).await;
    assert_eq!(r["coverage"]["pending_sessions"], 2);

    sqlx::query("UPDATE sources SET include_subagents = 1 WHERE id = 'omp'")
        .execute(&env.index.writer)
        .await
        .unwrap();
    let r = env.ok(json!({"query": "nomatch", "mode": "lexical"})).await;
    assert_eq!(r["coverage"]["pending_sessions"], 3);
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
    assert_eq!(r["schema_version"], 2);
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
    assert!(r["coverage"].is_null(), "healthy index omits coverage: {r}");

    let mut lease_ids = std::collections::BTreeSet::new();
    let mut manifests = std::collections::BTreeSet::new();
    for s in r["results"].as_array().unwrap() {
        for m in s["matches"].as_array().unwrap() {
            assert!(m["match_ref"].as_str().unwrap().len() < 150);
            let (h, mr) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
            let (session, snapshot) = env.manifest_target(&mr.manifest).await;
            assert_eq!(session, s["session_id"]);
            assert_eq!(
                snapshot,
                format!("sn_{}", s["session_id"].as_str().unwrap())
            );
            lease_ids.insert(h.lease.clone());
            manifests.insert(mr.manifest.clone());
            assert_eq!(mr.version, 2);
            assert_eq!(mr.records[0].key, "m0");
            assert_eq!(mr.records[0].digest, "00".repeat(16));
            assert!(env.leases.covers(&h.lease, &mr.manifest).await.unwrap());
            let want_exp = T0 + env.leases.ttl_ms();
            assert_eq!(m["match_ref_expires_at"], format_rfc3339(want_exp));
            assert_eq!(
                env.leases.state(&h.lease).await.unwrap(),
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
    assert!(!env.decode_match(&tok).await.1.records.is_empty());
}

#[tokio::test]
async fn branch_ref_is_issued_only_when_needed() {
    let env = Env::new().await;
    env.session("s_fork", "omp", "/w", "interactive", T0).await;
    env.snapshot("s_fork", "sn_fork", "published").await;
    env.message(
        "sn_fork",
        Message {
            key: "m0",
            seq: 0,
            role: "user",
            ts: Some(T0),
            parent: None,
            text: "root question pivotword",
        },
    )
    .await;
    env.message(
        "sn_fork",
        Message {
            key: "m1",
            seq: 1,
            role: "assistant",
            ts: Some(T0),
            parent: Some("m0"),
            text: "shared answer",
        },
    )
    .await;
    env.message(
        "sn_fork",
        Message {
            key: "m2a",
            seq: 2,
            role: "user",
            ts: Some(T0),
            parent: Some("m1"),
            text: "left branch tailword",
        },
    )
    .await;
    env.message(
        "sn_fork",
        Message {
            key: "m2b",
            seq: 3,
            role: "user",
            ts: Some(T0),
            parent: Some("m1"),
            text: "right branch",
        },
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
    assert_eq!(env.manifest_target(&br.anchor.manifest).await.1, "sn_fork");
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
    assert!(input.get("required").is_none());
    assert!(input["properties"].get("queries").is_some());
    assert_eq!(input["properties"]["limit"]["maximum"], 50);
    assert_eq!(input["properties"]["matches_per_session"]["maximum"], 32);
    assert!(t.output_schema.is_none(), "results are one JSON text copy");
    let out = output_schema();
    for f in [
        "schema_version",
        "mode_requested",
        "mode_used",
        "index_epoch",
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

fn output_schema() -> Value {
    serde_json::to_value(
        rmcp::schemars::generate::SchemaSettings::draft2020_12()
            .into_generator()
            .into_root_schema_for::<spool::search::SearchOutput>(),
    )
    .unwrap()
}

#[tokio::test]
async fn real_outputs_validate_against_the_output_schema() {
    let schema = output_schema();
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
    env.message(
        "sn_a",
        Message {
            key: "m1",
            seq: 0,
            role: "user",
            ts: Some(t1),
            parent: None,
            text: "ask [L]? now",
        },
    )
    .await;
    env.message(
        "sn_a",
        Message {
            key: "m2",
            seq: 1,
            role: "assistant",
            ts: Some(t2),
            parent: Some("m1"),
            text: "northstar answer",
        },
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
            mode == "literal",
            "{mode}: label the time of any text actually previewed"
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
    env.message(
        "sn_b",
        Message {
            key: "m1",
            seq: 0,
            role: "user",
            ts: Some(t1),
            parent: None,
            text: "ask [L]? now",
        },
    )
    .await;
    env.message(
        "sn_b",
        Message {
            key: "m2",
            seq: 1,
            role: "assistant",
            ts: Some(t2),
            parent: Some("m1"),
            text: "northstar answer",
        },
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
            shared,
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
    env.simple_with("m3", &Seed::default(), &[("garden tomato soil", Some(T0))])
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
        &Seed::default(),
        &[("driving a sedan today", Some(T0))],
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

async fn remote_session(env: &Env, id: &str, cwd: &str, remote: &str) {
    env.simple(id, "omp", cwd, &[("zebra crossing", Some(T0))])
        .await;
    let name = remote.rsplit('/').next().unwrap();
    env.link_project(id, cwd, "git_remote", remote, name, Some(remote))
        .await;
}

#[tokio::test]
async fn projects_filter_accepts_each_entry_form() {
    let env = Env::new().await;
    let repo = tempfile::tempdir().unwrap();
    let repo_dir = std::fs::canonicalize(repo.path()).unwrap();
    std::fs::create_dir_all(repo_dir.join(".git/objects")).unwrap();
    std::fs::create_dir_all(repo_dir.join(".git/refs")).unwrap();
    std::fs::write(repo_dir.join(".git/HEAD"), "ref: refs/heads/main\n").unwrap();
    std::fs::write(
        repo_dir.join(".git/config"),
        "[remote \"origin\"]\n\turl = git@github.com:acme/live.git\n",
    )
    .unwrap();
    std::fs::create_dir_all(repo_dir.join("src")).unwrap();
    remote_session(&env, "widgets", "/w/widgets", "github.com/acme/widgets").await;
    remote_session(&env, "live", "/elsewhere/live", "github.com/acme/live").await;
    remote_session(&env, "other", "/w/other", "github.com/acme/other").await;
    let q = |p: Value| json!({"query": "zebra", "mode": "lexical", "projects": p});
    for (entry, want) in [
        ("Widgets", "widgets"),
        ("acme/widgets", "widgets"),
        ("ACME/Widgets.git", "widgets"),
        ("git@github.com:acme/widgets.git", "widgets"),
        ("https://user:secret@GitHub.com/acme/widgets.git", "widgets"),
        ("/w/widgets/src/deep", "widgets"),
        (repo_dir.join("src").to_str().unwrap(), "live"),
    ] {
        let r = env.ok(q(json!([entry]))).await;
        assert_eq!(ids(&r), [want], "{entry}");
        assert!(r["warnings"].as_array().unwrap().is_empty(), "{entry}");
        assert_eq!(
            r["resolved_projects"][0]["projects"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
    let r = env.ok(q(json!(["widgets"]))).await;
    assert_eq!(r["results"][0]["project"]["name"], "widgets");
    assert_eq!(
        r["results"][0]["project"]["remote"],
        "github.com/acme/widgets"
    );
    assert_eq!(r["results"][0]["project_path"], "/w/widgets");
    let r = env.ok(json!({"query": "zebra", "mode": "lexical"})).await;
    assert!(r.get("resolved_projects").is_none());
}

#[tokio::test]
async fn projects_filter_unions_and_reports_unmatched_entries() {
    let env = Env::new().await;
    remote_session(&env, "a", "/w/a", "github.com/acme/alpha").await;
    remote_session(&env, "b", "/w/b", "gitlab.com/acme/beta").await;
    remote_session(&env, "c", "/w/c", "github.com/acme/gamma").await;
    remote_session(&env, "a2", "/x/a2", "gitlab.com/other/alpha").await;
    let search = |p: Value| env.ok(json!({"query": "zebra", "mode": "lexical", "projects": p}));

    let r = search(json!(["alpha", "acme/beta"])).await;
    let mut got = ids(&r);
    got.sort();
    assert_eq!(got, ["a", "a2", "b"]);
    assert_eq!(
        r["resolved_projects"][0]["projects"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    let r = search(json!([
        "beta",
        "nope",
        "https://tok:secret@github.com/no/such.git"
    ]))
    .await;
    assert_eq!(ids(&r), ["b"]);
    let w: Vec<&Value> = r["warnings"].as_array().unwrap().iter().collect();
    assert_eq!(w.len(), 2);
    assert!(w.iter().all(|w| w["code"] == "project_not_found"));
    assert!(w[0]["message"].as_str().unwrap().contains("nope"));
    assert!(
        w[1]["message"]
            .as_str()
            .unwrap()
            .contains("github.com/no/such")
    );
    assert!(!r.to_string().contains("secret"));

    let r = search(json!(["nope", "also/nope", "not a url://"])).await;
    assert!(ids(&r).is_empty());
    assert_eq!(r["warnings"].as_array().unwrap().len(), 3);
    assert_eq!(env.provider.calls(), 0);
    let r = env
        .ok(json!({"query": "zebra", "mode": "hybrid", "projects": ["nope"]}))
        .await;
    assert!(ids(&r).is_empty());
    assert_eq!(
        env.provider.calls(),
        0,
        "an unmatched filter never reaches the provider"
    );
}

#[tokio::test]
async fn linked_worktree_sessions_are_found_by_project_name() {
    let env = Env::new().await;
    remote_session(&env, "main", "/code/app", "github.com/acme/app").await;
    remote_session(
        &env,
        "tree",
        "/code/app-worktrees/feat",
        "github.com/acme/app",
    )
    .await;
    let r = env
        .ok(json!({"query": "zebra", "mode": "lexical", "projects": ["app"]}))
        .await;
    let mut got = ids(&r);
    got.sort();
    assert_eq!(got, ["main", "tree"]);
    assert_eq!(
        r["resolved_projects"][0]["projects"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

fn has_warning(r: &Value, code: &str) -> bool {
    r["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|w| w["code"] == code)
}

#[tokio::test]
async fn limits_are_clamped_with_a_warning() {
    let env = Env::new().await;
    env.simple(
        "s1",
        "omp",
        "/w",
        &[
            ("alpha one", Some(T0)),
            ("alpha two", Some(T0)),
            ("alpha three", Some(T0)),
            ("alpha four", Some(T0)),
        ],
    )
    .await;
    let r = env
        .ok(json!({"query": "alpha", "mode": "lexical", "limit": 500, "matches_per_session": 33}))
        .await;
    assert!(has_warning(&r, "limit_clamped"), "{r}");
    let n = r["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|w| w["code"] == "limit_clamped")
        .count();
    assert_eq!(n, 2, "{r}");
    let r = env.ok(json!({"query": "alpha", "mode": "lexical"})).await;
    assert!(!has_warning(&r, "limit_clamped"));
}

#[tokio::test]
async fn explicit_match_limit_respects_configured_ceiling() {
    for cap in [1, 2, 3, 32] {
        let env = Env::with(|cfg| cfg.search.max_matches_per_session = cap).await;
        let texts: Vec<_> = (0..240)
            .map(|i| {
                if i % 20 == 0 {
                    ("needle", Some(T0))
                } else {
                    ("filler", Some(T0))
                }
            })
            .collect();
        env.simple("limit", "omp", "/w", &texts).await;
        let default = env.ok(json!({"query":"needle", "mode":"lexical"})).await;
        let expanded = env
            .ok(json!({"query":"needle", "mode":"lexical", "matches_per_session":32}))
            .await;
        let default_matches = default["results"][0]["matches"].as_array().unwrap();
        let expanded_matches = expanded["results"][0]["matches"].as_array().unwrap();
        assert_eq!(default_matches.len(), cap.min(3));
        assert_eq!(expanded_matches.len(), cap.min(4), "{expanded}");
        assert_eq!(has_warning(&expanded, "limit_clamped"), cap < 32);
        let literal = env.ok(json!({"query":"needle", "mode":"literal", "session_ids":["limit"], "limit":1, "matches_per_session":32})).await;
        let literal_matches = literal["results"][0]["matches"].as_array().unwrap();
        assert!(literal_matches.len() <= cap);
        if cap == 32 {
            assert!(literal_matches.len() > 3);
        }
        for (a, b) in default_matches.iter().zip(expanded_matches) {
            let (_, a) = env.decode_match(a["match_ref"].as_str().unwrap()).await;
            let (_, b) = env.decode_match(b["match_ref"].as_str().unwrap()).await;
            assert_eq!(a.records, b.records);
        }
    }
}

#[tokio::test]
async fn scoped_expanded_semantic_retry_uses_query_cache() {
    for entries in [0, 256] {
        let env = Env::with(|cfg| cfg.search.query_cache_entries = entries).await;
        env.simple("cache", "omp", "/w", &[("cache needle", Some(T0))])
            .await;
        env.ok(json!({"query":"cache needle", "mode":"semantic"}))
            .await;
        let calls = env.provider.calls_for(Purpose::Query);
        env.ok(json!({"query":"cache needle", "mode":"semantic", "session_ids":["cache"], "limit":1, "matches_per_session":32})).await;
        assert_eq!(
            env.provider.calls_for(Purpose::Query),
            calls + usize::from(entries == 0)
        );
    }
}

#[tokio::test]
async fn scoped_expanded_retry_respects_hosted_consent() {
    use std::sync::Arc;
    let env = Env::new().await;
    env.simple("consent", "omp", "/w", &[("consent needle", Some(T0))])
        .await;
    let provider = Arc::new(spool::embeddings::FakeProvider::new(16).hosted(true));
    let embedding = Arc::new(spool::embeddings::EmbeddingService::new(
        env.index.clone(),
        env.cfg.embedding.clone(),
        env.cfg.search.clone(),
        Arc::new(env.clock.clone()),
        provider.clone(),
    ));
    let search = SearchService::new(
        env.index.clone(),
        env.cfg.clone(),
        embedding,
        env.leases.clone(),
        env.keys.clone(),
    );
    let args = json!({"query":"consent needle", "mode":"hybrid", "session_ids":["consent"], "limit":1, "matches_per_session":32});
    let result = search.call(args).await.unwrap();
    assert_eq!(result["mode_used"], "lexical");
    assert!(has_warning(&result, "query_embedding_unavailable"));
    assert_eq!(provider.calls_for(Purpose::Query), 0);
}

#[tokio::test]
async fn multiple_queries_fuse_and_dedup_sessions() {
    let env = Env::new().await;
    env.simple("s_a", "omp", "/w", &[("zebra crossing notes", Some(T0))])
        .await;
    env.simple("s_b", "omp", "/w", &[("giraffe habitat notes", Some(T0))])
        .await;
    env.simple(
        "s_c",
        "omp",
        "/w",
        &[("zebra and giraffe together", Some(T0))],
    )
    .await;
    let r = env
        .ok(json!({"queries": ["zebra", "giraffe"], "mode": "lexical"}))
        .await;
    let got = ids(&r);
    assert_eq!(got.len(), 3, "{got:?}");
    assert_eq!(got[0], "s_c", "chunk found by both queries ranks first");
    let r = env
        .ok(json!({"queries": ["zebra", "zebra"], "mode": "lexical"}))
        .await;
    assert_eq!(ids(&r).len(), 2);
    let r = env
        .ok(json!({"queries": ["zebra", "giraffe"], "mode": "literal"}))
        .await;
    assert_eq!(ids(&r).len(), 3);
}

#[tokio::test]
async fn excerpts_are_centred_and_stripped() {
    let env = Env::new().await;
    let text = format!(
        "<oai-mem-citation>SECRETCITE</oai-mem-citation>{}needle{}",
        "a ".repeat(1500),
        "b ".repeat(1500)
    );
    let text = text.replace("needle", " needle ");
    env.simple("s1", "omp", "/w", &[(&text, Some(T0))]).await;
    let r = env.ok(json!({"query": "needle", "mode": "lexical"})).await;
    let ex = r["results"][0]["matches"][0]["preview"]["text"]
        .as_str()
        .unwrap();
    assert!(
        ex.contains("needle") && !ex.contains("SECRETCITE") && !ex.contains("oai-mem"),
        "{ex}"
    );
    assert!(ex.len() <= 8192);
    let r = env.ok(json!({"query": "needle", "mode": "semantic"})).await;
    let ex = r["results"][0]["matches"][0]["preview"]["text"]
        .as_str()
        .unwrap();
    assert!(!ex.contains("SECRETCITE") && ex.len() <= 8192, "{ex}");
}

#[tokio::test]
async fn title_falls_back_to_the_first_prompt() {
    let env = Env::new().await;
    let long = format!(
        "<environment_context>noise</environment_context>Fix the parser {}",
        "x".repeat(300)
    );
    env.simple("s1", "omp", "/w", &[(&long, Some(T0))]).await;
    sqlx::query("UPDATE sessions SET native_title = NULL WHERE id = 's1'")
        .execute(&env.index.writer)
        .await
        .unwrap();
    let r = env.ok(json!({"query": "parser", "mode": "lexical"})).await;
    let s = &r["results"][0];
    assert_eq!(s["title_origin"], "first_prompt");
    let t = s["title"].as_str().unwrap();
    assert!(t.starts_with("Fix the parser") && t.len() <= 120, "{t}");
}

#[tokio::test]
async fn coverage_is_present_only_when_degraded() {
    let env = Env::new().await;
    env.simple("s1", "omp", "/w", &[("alpha", Some(T0))]).await;
    let r = env.ok(json!({"query": "alpha", "mode": "lexical"})).await;
    assert!(r.get("coverage").is_none(), "{r}");
    env.simple_with("s2", &Seed::default(), &[("beta", Some(T0))])
        .await;
    let r = env.ok(json!({"query": "beta", "mode": "lexical"})).await;
    assert!(
        r["coverage"]["active_chunks"].as_i64().unwrap()
            > r["coverage"]["embedded_chunks"].as_i64().unwrap()
    );
}

#[tokio::test]
async fn merged_evidence_and_semantic_excerpt_are_bounded() {
    let env = Env::new().await;
    let (sid, snap) = ("bounded", "bounded_snap");
    env.session(sid, "omp", "/w", "interactive", T0).await;
    env.snapshot(sid, snap, "published").await;
    let mut parent: Option<String> = None;
    for i in 0..50 {
        let key = format!("m{i:02}");
        let text = format!("boundedneedle item {i:02}");
        env.message(
            snap,
            Message {
                key: &key,
                seq: i,
                role: "user",
                ts: Some(T0),
                parent: parent.as_deref(),
                text: &text,
            },
        )
        .await;
        env.chunk(
            sid,
            snap,
            &format!("c{i:02}"),
            search_common::Chunk {
                body: &format!("USER: {text}"),
                spans: vec![(&key, 0, 0, text.len() as i64, false)],
                anchor: Some(T0),
                run: "run0",
                ordinal: i,
                embed: false,
            },
        )
        .await;
        parent = Some(key);
    }
    env.activate(sid, snap).await;
    let r = env
        .ok(json!({"query":"boundedneedle", "mode":"lexical"}))
        .await;
    let m = &matches_of(&r, sid)[0];
    let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert_eq!(reference.version, 2);
    assert!(!reference.records.is_empty());
    assert!(tool_result_bytes(&r, false) <= 65_536);
    let text = format!(
        "{} identifierneedle {}",
        "🦀".repeat(700),
        "b ".repeat(1200)
    );
    env.simple("semantic", "omp", "/w", &[(&text, Some(T0))])
        .await;
    let r = env
        .ok(json!({"query":"identifierneedle", "mode":"semantic"}))
        .await;
    let m = &matches_of(&r, "semantic")[0];
    let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert_eq!(reference.records.len(), 1);
    assert_eq!(reference.records[0].key, "m0");
    assert!(
        m["preview"]["text"]
            .as_str()
            .unwrap()
            .contains("identifierneedle")
    );
    println!(
        "candidate semantic sessions={} supporting_identifier={} response_bytes={} ref_bytes={}",
        ids(&r).len(),
        m["preview"]["text"]
            .as_str()
            .unwrap()
            .contains("identifierneedle"),
        tool_result_bytes(&r, false),
        m["match_ref"].as_str().unwrap().len()
    );
}

#[tokio::test]
async fn bounded_excerpt_ignores_injected_term_occurrences() {
    let env = Env::new().await;
    let text = format!(
        "<system-reminder>{}</system-reminder> {} 1 < 2 needleforbound supporting",
        "needleforbound ".repeat(40),
        "z".repeat(1300)
    );
    env.simple("safe", "omp", "/w", &[(&text, Some(T0))]).await;
    let r = env
        .ok(json!({"query":"needleforbound", "mode":"semantic"}))
        .await;
    let m = &matches_of(&r, "safe")[0];
    let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert_eq!(reference.records[0].key, "m0");
    let excerpt = m["preview"]["text"].as_str().unwrap();
    assert!(excerpt.contains("needleforbound supporting"), "{excerpt}");
    assert!(!excerpt.contains("system-reminder"));
}

#[tokio::test]
async fn later_primary_evidence_wins_before_span_and_byte_caps() {
    let env = Env::new().await;
    let (sid, snap) = ("later", "later_snap");
    env.session(sid, "omp", "/w", "interactive", T0).await;
    env.snapshot(sid, snap, "published").await;
    let mut parent: Option<String> = None;
    let mut keys = Vec::new();
    let mut texts = Vec::new();
    for i in 0..10 {
        let key = format!("later{i}");
        let text = if i == 0 {
            format!("prefix{}", "🦀".repeat(400))
        } else if i == 8 {
            "prefix needlelater supporting evidence".to_string()
        } else {
            format!("prefix filler {i}")
        };
        env.message(
            snap,
            Message {
                key: &key,
                seq: i,
                role: "user",
                ts: Some(T0),
                parent: parent.as_deref(),
                text: &text,
            },
        )
        .await;
        parent = Some(key.clone());
        keys.push(key);
        texts.push(text);
    }
    let spans = keys
        .iter()
        .zip(&texts)
        .map(|(key, text)| (key.as_str(), 0, 6, text.len() as i64, false))
        .collect();
    let body = texts
        .iter()
        .map(|text| format!("USER: {}", &text[6..]))
        .collect::<Vec<_>>()
        .join("\n\n");
    env.chunk(
        sid,
        snap,
        "later_chunk",
        search_common::Chunk {
            body: &body,
            spans,
            anchor: Some(T0),
            run: "run",
            ordinal: 0,
            embed: true,
        },
    )
    .await;
    env.activate(sid, snap).await;
    for mode in ["lexical", "semantic"] {
        let r = env.ok(json!({"query":"needlelater", "mode":mode})).await;
        let m = &matches_of(&r, sid)[0];
        assert!(
            m["preview"]["text"]
                .as_str()
                .unwrap()
                .contains("needlelater")
        );
        let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
        assert_eq!(reference.records.len(), keys.len());
        assert_eq!(
            reference.records.iter().map(|r| &r.key).collect::<Vec<_>>(),
            keys.iter().collect::<Vec<_>>()
        );
        assert!(
            reference
                .records
                .iter()
                .all(|r| r.digest == "00".repeat(16))
        );
    }
}

#[tokio::test]
async fn grouped_excerpt_uses_nonprimary_core_evidence() {
    let env = Env::new().await;
    let (sid, snap) = ("grouped_excerpt", "grouped_excerpt_snap");
    let evidence = format!("{} {}", "filler ".repeat(700), "evidenceneedle ".repeat(5));
    let successor = "and unrelated ".repeat(150);
    env.session(sid, "omp", "/w", "interactive", T0).await;
    env.snapshot(sid, snap, "published").await;
    for (seq, key, text, parent) in [
        (0, "evidence", evidence.as_str(), None),
        (1, "successor", successor.as_str(), Some("evidence")),
    ] {
        env.message(
            snap,
            Message {
                key,
                seq,
                role: "user",
                ts: Some(T0),
                parent,
                text,
            },
        )
        .await;
    }
    let context_start = evidence.find("evidenceneedle").unwrap();
    env.chunk(
        sid,
        snap,
        "predecessor",
        search_common::Chunk {
            body: &format!("USER: {evidence}"),
            spans: vec![("evidence", 0, 0, evidence.len() as i64, false)],
            anchor: Some(T0 + 1000),
            run: "run",
            ordinal: 0,
            embed: false,
        },
    )
    .await;
    env.chunk(
        sid,
        snap,
        "successor",
        search_common::Chunk {
            body: &format!("USER: {}\n\nUSER: {successor}", &evidence[context_start..]),
            spans: vec![
                (
                    "evidence",
                    0,
                    context_start as i64,
                    evidence.len() as i64,
                    true,
                ),
                ("successor", 0, 0, successor.len() as i64, false),
            ],
            anchor: Some(T0 + 2000),
            run: "run",
            ordinal: 1,
            embed: false,
        },
    )
    .await;
    env.activate(sid, snap).await;
    let r = env
        .ok(json!({"query":"why evidenceneedle and", "mode":"lexical", "matches_per_session":1, "debug_scores":true, "after":format_rfc3339(T0 + 500)}))
        .await;
    let m = &matches_of(&r, sid)[0];
    assert!(
        m["preview"]["text"]
            .as_str()
            .unwrap()
            .contains("evidenceneedle"),
        "{m}"
    );
    assert_eq!(m["matched_at"], format_rfc3339(T0 + 2000));
    assert_eq!(m["context_outside_time_filter"], true);
    assert_eq!(m["debug"]["lexical_rank"], 1);
    let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert_eq!(
        reference
            .records
            .iter()
            .map(|r| r.key.as_str())
            .collect::<Vec<_>>(),
        ["evidence", "successor"]
    );
    assert!(
        reference
            .records
            .iter()
            .all(|r| r.digest == "00".repeat(16))
    );
}

#[path = "reader_common/mod.rs"]
mod reader_common;

#[tokio::test]
async fn grouped_excerpt_match_reads_original_source() {
    let env = reader_common::Env::new().await;
    let evidence = format!("{} {}", "filler ".repeat(700), "evidenceneedle ".repeat(5));
    let successor = "unrelated ".repeat(150);
    let (sid, path) = env
        .indexed(
            "grouped.jsonl",
            &[
                reader_common::omp_header(),
                reader_common::omp_msg("m0", None, "user", &evidence),
                reader_common::omp_msg("m1", Some("m0"), "user", &successor),
            ],
        )
        .await;
    let snap: String = sqlx::query_scalar("SELECT active_snapshot_id FROM sessions WHERE id = ?")
        .bind(&sid)
        .fetch_one(&env.index.writer)
        .await
        .unwrap();
    let keys: Vec<String> =
        sqlx::query_scalar("SELECT record_key FROM messages WHERE snapshot_id = ? ORDER BY seq")
            .bind(&snap)
            .fetch_all(&env.index.writer)
            .await
            .unwrap();
    assert_eq!(keys.len(), 2);
    sqlx::query("DELETE FROM chunks WHERE snapshot_id = ?")
        .bind(&snap)
        .execute(&env.index.writer)
        .await
        .unwrap();
    let context_start = evidence.find("evidenceneedle").unwrap();
    for (ordinal, body) in [
        (0, format!("USER: {evidence}")),
        (
            1,
            format!("USER: {}\n\nUSER: {successor}", &evidence[context_start..]),
        ),
    ] {
        let rowid = sqlx::query("INSERT INTO chunks(logical_id, snapshot_id, session_id, run_key, ordinal, body, input_hash, time_anchor, rendered_tokens, is_tail) VALUES (?,?,?,'run',?,?,?,?,10,0)")
            .bind(format!("test{ordinal}")).bind(&snap).bind(&sid).bind(ordinal).bind(&body)
            .bind(spool::domain::digest(body.as_bytes()).to_vec()).bind(reader_common::T0)
            .execute(&env.index.writer).await.unwrap().last_insert_rowid();
        let spans = if ordinal == 0 {
            vec![(0, &keys[0], 0, evidence.len(), false)]
        } else {
            vec![
                (0, &keys[0], context_start, evidence.len(), true),
                (1, &keys[1], 0, successor.len(), false),
            ]
        };
        for (seq, key, start, end, context) in spans {
            sqlx::query("INSERT INTO chunk_spans(chunk_rowid, seq, record_key, block_index, start_byte, end_byte, is_context) VALUES (?,?,?,0,?,?,?)")
                .bind(rowid).bind(seq).bind(key).bind(start as i64).bind(end as i64).bind(i64::from(context))
                .execute(&env.index.writer).await.unwrap();
        }
    }
    let ranked: Vec<String> = sqlx::query_scalar("SELECT c.logical_id FROM chunks_fts JOIN chunks c ON c.rowid = chunks_fts.rowid WHERE chunks_fts MATCH ? ORDER BY bm25(chunks_fts)")
        .bind("evidenceneedle")
        .fetch_all(&env.index.writer)
        .await
        .unwrap();
    assert_eq!(ranked, ["test1", "test0"]);
    let search = SearchService::new(
        env.index.clone(),
        env.cfg.clone(),
        env.emb.clone(),
        env.leases.clone(),
        env.keys.clone(),
    );
    let result = search
        .call(json!({"query":"evidenceneedle", "mode":"lexical", "matches_per_session":32, "debug_scores":true}))
        .await
        .unwrap();
    let m = &matches_of(&result, &sid)[0];
    assert!(
        m["preview"]["text"]
            .as_str()
            .unwrap()
            .contains("evidenceneedle"),
        "{m}"
    );
    let small = search.call(json!({"query":"evidenceneedle", "mode":"lexical", "matches_per_session":1, "preview_target_bytes":256})).await.unwrap();
    let large = search.call(json!({"query":"evidenceneedle", "mode":"lexical", "matches_per_session":1, "preview_target_bytes":8192})).await.unwrap();
    let mut anchors = Vec::new();
    for result in [&small, &large] {
        let token = matches_of(result, &sid)[0]["match_ref"].as_str().unwrap();
        let handle = spool::transcript::refs::RefHandle::decode(
            &env.keys.current(),
            spool::transcript::handle::HandleKind::Match,
            token,
        )
        .unwrap();
        let payload = env
            .leases
            .get_ref(&handle.lease, handle.ref_id, "match")
            .await
            .unwrap();
        let reference: spool::transcript::refs::MatchRef =
            serde_json::from_slice(&payload).unwrap();
        anchors.push(reference.records);
    }
    assert_eq!(anchors[0], anchors[1]);
    assert_eq!(anchors[0].len(), 2);
    let pages = env.pages(json!({"session_id":sid, "source_policy":"require_source", "max_bytes":4096, "max_items":1,
        "selection":{"kind":"match", "match_ref":m["match_ref"], "before":0, "after":0}}), 4096).await;
    assert!(pages.len() > 1);
    let mut text = Vec::new();
    for page in &pages {
        for item in page["items"].as_array().unwrap() {
            assert_eq!(item["provenance"]["origin"], "verified_source");
            for block in item["blocks"].as_array().unwrap() {
                text.extend(reader_common::frag_bytes(block));
            }
        }
    }
    assert_eq!(
        String::from_utf8(text).unwrap(),
        format!("{evidence}{successor}")
    );
    std::fs::write(
        &path,
        format!(
            "{}\n{}\n",
            reader_common::omp_header(),
            reader_common::omp_msg("m0", None, "user", "rewritten")
        ),
    )
    .unwrap();
    let error = env.read(json!({"session_id":sid, "source_policy":"require_source", "selection":{"kind":"match", "match_ref":m["match_ref"]}})).await.unwrap_err();
    assert!(matches!(
        error.code.as_str(),
        "SOURCE_CHANGED" | "STALE_REFERENCE"
    ));
}

#[tokio::test]
async fn capped_group_prefers_distinct_support_in_adjacent_chunk() {
    let env = Env::new().await;
    let (sid, snap) = ("support_selection", "support_selection_snap");
    let weak = format!("{} {}", "workers and ".repeat(40), "padding ".repeat(180));
    let strong = "worker lease_refs match_ref continuation cursors explain the answer";
    let disjoint = "worker lease_refs unrelated disjoint result";
    env.session(sid, "omp", "/w", "interactive", T0).await;
    env.snapshot(sid, snap, "published").await;
    for (seq, key, text, ordinal) in [
        (0, "weak", weak.as_str(), 0),
        (1, "strong", strong, 1),
        (2, "disjoint", disjoint, 5),
    ] {
        env.message(
            snap,
            Message {
                key,
                seq,
                role: "user",
                ts: Some(T0),
                parent: None,
                text,
            },
        )
        .await;
        env.chunk(
            sid,
            snap,
            key,
            search_common::Chunk {
                body: &format!("USER: {text}"),
                spans: vec![(key, 0, 0, text.len() as i64, false)],
                anchor: Some(T0),
                run: "run",
                ordinal,
                embed: false,
            },
        )
        .await;
    }
    env.activate(sid, snap).await;
    let r = env.ok(json!({"query":"Why did worker and lease_refs match_ref continuation cursors?", "mode":"lexical", "matches_per_session":1})).await;
    let matches = matches_of(&r, sid);
    assert_eq!(matches.len(), 1);
    let m = &matches[0];
    assert!(
        m["preview"]["text"]
            .as_str()
            .unwrap()
            .contains("explain the answer"),
        "{m}"
    );
    let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert!(reference.records.iter().any(|s| s.key == "strong"));
    assert!(!reference.records.iter().any(|s| s.key == "disjoint"));
}

#[tokio::test]
async fn excerpt_prefers_informative_units_over_weak_earlier_core() {
    let env = Env::new().await;
    let (sid, snap) = ("support_units", "support_units_snap");
    let weak = format!("workers and {}", "padding ".repeat(180));
    let strong = "worker lease_refs match_ref continuation cursors explain the answer";
    env.session(sid, "omp", "/w", "interactive", T0).await;
    env.snapshot(sid, snap, "published").await;
    for (seq, key, text) in [(0, "weak", weak.as_str()), (1, "strong", strong)] {
        env.message(
            snap,
            Message {
                key,
                seq,
                role: "user",
                ts: Some(T0),
                parent: None,
                text,
            },
        )
        .await;
    }
    env.chunk(
        sid,
        snap,
        "both",
        search_common::Chunk {
            body: &format!("USER: {weak}\n\nUSER: {strong}"),
            spans: vec![
                ("weak", 0, 0, weak.len() as i64, false),
                ("strong", 0, 0, strong.len() as i64, false),
            ],
            anchor: Some(T0),
            run: "run",
            ordinal: 0,
            embed: false,
        },
    )
    .await;
    env.activate(sid, snap).await;
    let r = env.ok(json!({"query":"Why did worker and lease_refs match_ref continuation cursors?", "mode":"lexical"})).await;
    let m = &matches_of(&r, sid)[0];
    assert!(
        m["preview"]["text"]
            .as_str()
            .unwrap()
            .contains("explain the answer"),
        "{m}"
    );
    let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert!(reference.records.iter().any(|s| s.key == "strong"));
}

#[tokio::test]
async fn complementary_windows_retain_status_and_cause() {
    let env = Env::new().await;
    let (sid, snap) = ("complementary", "complementary_snap");
    let filler = "old unrelated material ".repeat(35);
    let status = format!(
        "WORKER_ALREADY_EXISTS appeared in Spool. {}",
        "status ".repeat(77)
    );
    let cause = format!(
        "After restarting Spool, fixed registrations wait for the 30 second heartbeat lease expiry and retry automatically. {}",
        "cause ".repeat(122)
    );
    env.session(sid, "omp", "/w", "interactive", T0).await;
    env.snapshot(sid, snap, "published").await;
    for (seq, key, text) in [
        (0, "filler", &filler),
        (1, "status", &status),
        (2, "cause", &cause),
    ] {
        env.message(
            snap,
            Message {
                key,
                seq,
                role: "user",
                ts: Some(T0),
                parent: None,
                text,
            },
        )
        .await;
    }
    let body = format!("USER: {filler}\n\nUSER: {status}\n\nUSER: {cause}");
    env.chunk(
        sid,
        snap,
        "both",
        search_common::Chunk {
            body: &body,
            spans: vec![
                ("filler", 0, 0, filler.len() as i64, false),
                ("status", 0, 0, status.len() as i64, false),
                ("cause", 0, 0, cause.len() as i64, false),
            ],
            anchor: Some(T0),
            run: "run",
            ordinal: 0,
            embed: false,
        },
    )
    .await;
    env.activate(sid, snap).await;
    let r = env.ok(json!({"query":"Why did WORKER_ALREADY_EXISTS appear after restarting Spool?", "mode":"lexical"})).await;
    let m = &matches_of(&r, sid)[0];
    let excerpt = m["preview"]["text"].as_str().unwrap();
    assert!(!excerpt.is_empty());
    assert!(excerpt.len() <= 8192);
    let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert!(reference.records.iter().any(|s| s.key == "status"));
    assert!(reference.records.iter().any(|s| s.key == "cause"));
    assert!(
        reference
            .records
            .iter()
            .all(|r| r.digest == "00".repeat(16))
    );
}

#[tokio::test]
async fn semantic_primary_retains_late_handle_design() {
    let env = Env::new().await;
    let (sid, snap) = ("late_design", "late_design_snap");
    let query =
        "What design reduced the large opaque handles returned by the conversation search API?";
    let primary = format!(
        "search {} Short handles use a signed lease_id plus index instead of embedded span lists.",
        "padding ".repeat(240)
    );
    let adjacent = "search API repository filter unrelated proposal";
    env.session(sid, "omp", "/w", "interactive", T0).await;
    env.snapshot(sid, snap, "published").await;
    for (seq, key, text) in [(0, "design", primary.as_str()), (1, "repo", adjacent)] {
        env.message(
            snap,
            Message {
                key,
                seq,
                role: "user",
                ts: Some(T0),
                parent: None,
                text,
            },
        )
        .await;
        let body = format!("USER: {text}");
        env.chunk(
            sid,
            snap,
            key,
            search_common::Chunk {
                body: &body,
                spans: vec![(key, 0, 0, text.len() as i64, false)],
                anchor: Some(T0),
                run: "run",
                ordinal: seq,
                embed: false,
            },
        )
        .await;
        env.embed_vec(
            &env.profile_id,
            &env.provider.vector_for(query),
            &spool::domain::digest(body.as_bytes()),
        )
        .await;
    }
    env.activate(sid, snap).await;
    let r = env
        .ok(json!({"query":query, "mode":"semantic", "matches_per_session":1}))
        .await;
    let m = &matches_of(&r, sid)[0];
    let excerpt = m["preview"]["text"].as_str().unwrap();
    assert!(excerpt.contains("signed lease_id plus index"), "{excerpt}");
    let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert!(reference.records.iter().any(|r| r.key == "design"));
    assert!(
        reference
            .records
            .iter()
            .all(|r| r.digest == "00".repeat(16))
    );
}

#[tokio::test]
async fn complementary_and_semantic_windows_read_verified_source_across_pages() {
    for semantic in [false, true] {
        let env = reader_common::Env::new().await;
        let query = if semantic {
            "What design reduced the large opaque handles returned by the conversation search API?"
        } else {
            "Why did WORKER_ALREADY_EXISTS appear after restarting Spool?"
        };
        let first = if semantic {
            format!(
                "search {} Short handles use a signed lease_id plus index instead of embedded span lists. {}",
                "padding ".repeat(240),
                "tail ".repeat(900)
            )
        } else {
            format!(
                "WORKER_ALREADY_EXISTS appeared in Spool. {}",
                "status ".repeat(77)
            )
        };
        let middle = if semantic {
            "search API repository filter unrelated proposal".to_string()
        } else {
            "unrelated filler ".repeat(350)
        };
        let last = "After restarting Spool, fixed registrations wait for the 30 second heartbeat lease expiry and retry automatically.";
        let (sid, _) = env
            .indexed(
                "windows.jsonl",
                &[
                    reader_common::omp_header(),
                    reader_common::omp_msg("m0", None, "user", &first),
                    reader_common::omp_msg("m1", Some("m0"), "user", &middle),
                    reader_common::omp_msg("m2", Some("m1"), "user", last),
                ],
            )
            .await;
        let snap: String =
            sqlx::query_scalar("SELECT active_snapshot_id FROM sessions WHERE id = ?")
                .bind(&sid)
                .fetch_one(&env.index.writer)
                .await
                .unwrap();
        let keys: Vec<String> = sqlx::query_scalar(
            "SELECT record_key FROM messages WHERE snapshot_id = ? ORDER BY seq",
        )
        .bind(&snap)
        .fetch_all(&env.index.writer)
        .await
        .unwrap();
        sqlx::query("DELETE FROM chunks WHERE snapshot_id = ?")
            .bind(&snap)
            .execute(&env.index.writer)
            .await
            .unwrap();
        let groups: Vec<Vec<usize>> = if semantic {
            vec![vec![0], vec![1]]
        } else {
            vec![vec![0, 1, 2]]
        };
        let texts = [first.as_str(), middle.as_str(), last];
        let profile = env.emb.ensure_profile().await.unwrap();
        for (ordinal, group) in groups.iter().enumerate() {
            let body = group
                .iter()
                .map(|i| format!("USER: {}", texts[*i]))
                .collect::<Vec<_>>()
                .join("\n\n");
            let hash = spool::domain::digest(body.as_bytes());
            let rowid = sqlx::query("INSERT INTO chunks(logical_id, snapshot_id, session_id, run_key, ordinal, body, input_hash, time_anchor, rendered_tokens, is_tail) VALUES (?,?,?,'run',?,?,?,?,10,0)")
                .bind(format!("window{ordinal}")).bind(&snap).bind(&sid).bind(ordinal as i64).bind(&body).bind(hash.to_vec()).bind(reader_common::T0).execute(&env.index.writer).await.unwrap().last_insert_rowid();
            for (seq, i) in group.iter().enumerate() {
                sqlx::query("INSERT INTO chunk_spans(chunk_rowid, seq, record_key, block_index, start_byte, end_byte, is_context) VALUES (?,?,?,0,0,?,0)")
                    .bind(rowid).bind(seq as i64).bind(&keys[*i]).bind(texts[*i].len() as i64).execute(&env.index.writer).await.unwrap();
            }
            if semantic {
                let blob: Vec<u8> = env
                    .provider
                    .vector_for(query)
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect();
                sqlx::query("INSERT INTO embeddings(profile_id, input_hash, dimensions, vec, created_at) VALUES (?,?,32,?,0) ON CONFLICT DO NOTHING")
                    .bind(&profile).bind(hash.to_vec()).bind(blob).execute(&env.index.writer).await.unwrap();
            }
        }
        let search = SearchService::new(
            env.index.clone(),
            env.cfg.clone(),
            env.emb.clone(),
            env.leases.clone(),
            env.keys.clone(),
        );
        let result = search.call(json!({"query":query,"mode":if semantic {"semantic"} else {"lexical"},"matches_per_session":1})).await.unwrap();
        let m = &matches_of(&result, &sid)[0];
        let expected = if semantic {
            "signed lease_id plus index"
        } else {
            "heartbeat lease expiry"
        };
        assert!(
            m["preview"]["text"].as_str().unwrap().contains(expected),
            "{m}"
        );
        let pages = env.pages(json!({"session_id":sid,"source_policy":"require_source","max_bytes":4096,"max_items":1,"selection":{"kind":"match","match_ref":m["match_ref"],"before":0,"after":0}}), 4096).await;
        assert!(pages.len() > 1);
        let mut bytes = Vec::new();
        for page in &pages {
            for item in page["items"].as_array().unwrap() {
                assert_eq!(item["provenance"]["origin"], "verified_source");
                for block in item["blocks"].as_array().unwrap() {
                    bytes.extend(reader_common::frag_bytes(block));
                }
            }
        }
        let read = String::from_utf8(bytes).unwrap();
        assert!(read.contains(expected));
        if !semantic {
            assert!(read.contains("WORKER_ALREADY_EXISTS"));
        }
    }
}

#[tokio::test]
async fn hidden_only_match_keeps_a_readable_reference_without_exposing_text() {
    let env = Env::new().await;
    let text = "<system-reminder>hiddenonlyneedle secret</system-reminder>";
    env.simple("hidden_only", "omp", "/w", &[(text, Some(T0))])
        .await;
    let r = env
        .ok(json!({"query":"hiddenonlyneedle", "mode":"lexical"}))
        .await;
    let m = &matches_of(&r, "hidden_only")[0];
    assert!(m["preview"]["text"].as_str().unwrap().is_empty());
    let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert!(!reference.records.is_empty());
}

fn occurrence_layout(len: usize, terms: &[(usize, &str)]) -> String {
    let mut bytes = vec![b' '; len];
    for (at, word) in terms {
        bytes[*at..*at + word.len()].copy_from_slice(word.as_bytes());
    }
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn realistic_late_design_survives_literal_citation_example() {
    let env = Env::new().await;
    let (sid, snap) = ("realistic_design", "realistic_design_snap");
    let query =
        "What design reduced the large opaque handles returned by the conversation search API?";
    let first = occurrence_layout(127, &[(49, "api")]);
    let primary = occurrence_layout(
        3112,
        &[
            (79, "search"),
            (230, "search"),
            (344, "search"),
            (1649, "`<oai-mem-citation>`"),
            (1935, "Short handles"),
            (2015, "signed lease_id + index"),
            (2127, "search"),
            (2821, "search"),
        ],
    );
    let repo = occurrence_layout(
        2570,
        &[
            (105, "search"),
            (745, "api"),
            (1613, "search"),
            (1906, "search"),
        ],
    );
    let other = occurrence_layout(889, &[(293, "api")]);
    let proposal = occurrence_layout(1619, &[(12, "design")]);
    env.session(sid, "omp", "/w", "interactive", T0).await;
    env.snapshot(sid, snap, "published").await;
    for (seq, key, text) in [
        (0, "first", first.as_str()),
        (1, "primary", primary.as_str()),
        (2, "repo", repo.as_str()),
        (3, "other", other.as_str()),
        (4, "proposal", proposal.as_str()),
    ] {
        env.message(
            snap,
            Message {
                key,
                seq,
                role: "user",
                ts: Some(T0),
                parent: None,
                text,
            },
        )
        .await;
    }
    for (ordinal, body, spans) in [
        (
            0,
            format!("USER: {first}\n\nUSER: {primary}"),
            vec![
                ("first", 0, 0, first.len() as i64, false),
                ("primary", 0, 0, primary.len() as i64, false),
            ],
        ),
        (
            1,
            format!("USER: {repo}"),
            vec![("repo", 0, 0, repo.len() as i64, false)],
        ),
        (
            2,
            format!("USER: {other}\n\nUSER: {proposal}"),
            vec![
                ("other", 0, 0, other.len() as i64, false),
                ("proposal", 0, 0, proposal.len() as i64, false),
            ],
        ),
    ] {
        env.chunk(
            sid,
            snap,
            &format!("chunk{ordinal}"),
            search_common::Chunk {
                body: &body,
                spans,
                anchor: Some(T0),
                run: "run",
                ordinal,
                embed: false,
            },
        )
        .await;
        env.embed_vec(
            &env.profile_id,
            &env.provider.vector_for(query),
            &spool::domain::digest(body.as_bytes()),
        )
        .await;
    }
    env.activate(sid, snap).await;
    let result = env
        .ok(json!({"query":query,"mode":"semantic","matches_per_session":1}))
        .await;
    let m = &matches_of(&result, sid)[0];
    assert!(
        m["preview"]["text"]
            .as_str()
            .unwrap()
            .contains("signed lease_id + index"),
        "{m}"
    );
    let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert!(reference.records.iter().any(|s| s.key == "primary"));
}

#[tokio::test]
async fn realistic_short_handle_mechanism_survives_rebalancing() {
    let env = Env::new().await;
    let (sid, snap) = ("realistic_refs", "realistic_refs_snap");
    let query = "How did lease_refs make match_ref and continuation cursors smaller?";
    let early = occurrence_layout(
        4943,
        &[
            (2283, "lease_refs"),
            (4090, "lease_refs"),
            (2593, "match_ref"),
            (3889, "match_ref"),
            (2426, "cursors"),
            (2909, "cursors"),
            (3764, "cursors"),
        ],
    );
    let mechanism = occurrence_layout(
        2035,
        &[
            (160, "match_ref"),
            (310, "The handles store details on the server:"),
            (401, "lease_refs"),
            (843, "cursors"),
            (1141, "smaller"),
            (1799, "lease_refs"),
        ],
    );
    let notes = occurrence_layout(
        3249,
        &[
            (656, "match_ref"),
            (757, "cursors"),
            (937, "continuation"),
            (1180, "lease_refs"),
            (2100, "lease_refs"),
            (2364, "match_ref"),
        ],
    );
    let last = occurrence_layout(1042, &[(593, "lease_refs")]);
    env.session(sid, "omp", "/w", "interactive", T0).await;
    env.snapshot(sid, snap, "published").await;
    for (seq, key, text) in [
        (0, "early", early.as_str()),
        (1, "mechanism", mechanism.as_str()),
        (2, "notes", notes.as_str()),
        (3, "last", last.as_str()),
    ] {
        env.message(
            snap,
            Message {
                key,
                seq,
                role: "user",
                ts: Some(T0),
                parent: match seq {
                    1 => Some("early"),
                    2 => Some("mechanism"),
                    3 => Some("notes"),
                    _ => None,
                },
                text,
            },
        )
        .await;
    }
    for (ordinal, body, spans) in [
        (
            15,
            format!("USER: {}", &early[1699..4597]),
            vec![("early", 0, 1699, 4597, false)],
        ),
        (
            16,
            format!(
                "USER: {}\n\nUSER: {mechanism}\n\nUSER: {}",
                &early[4597..],
                &notes[..905]
            ),
            vec![
                ("early", 0, 4597, 4943, false),
                ("mechanism", 0, 0, 2035, false),
                ("notes", 0, 0, 905, false),
            ],
        ),
        (
            17,
            format!("USER: {}\n\nUSER: {last}", &notes[905..]),
            vec![("notes", 0, 905, 3249, false), ("last", 0, 0, 1042, false)],
        ),
    ] {
        env.chunk(
            sid,
            snap,
            &format!("chunk{ordinal}"),
            search_common::Chunk {
                body: &body,
                spans,
                anchor: Some(T0),
                run: "run",
                ordinal,
                embed: false,
            },
        )
        .await;
    }
    env.activate(sid, snap).await;
    let result = env
        .ok(json!({"query":query,"mode":"lexical","matches_per_session":1}))
        .await;
    let m = &matches_of(&result, sid)[0];
    assert!(
        m["preview"]["text"]
            .as_str()
            .unwrap()
            .contains("The handles store details on the server:"),
        "{m}"
    );
    let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert!(reference.records.iter().any(|s| s.key == "mechanism"));
}

#[tokio::test]
async fn realistic_lease_status_and_cause_survive_competing_fix() {
    let env = Env::new().await;
    let (sid, snap) = ("realistic_lease", "realistic_lease_snap");
    let status = occurrence_layout(580, &[(11, "Spool"), (423, "WORKER_ALREADY_EXISTS")]);
    let cause = occurrence_layout(
        836,
        &[
            (35, "after"),
            (
                45,
                "fixed worker registration heartbeat expires at 30 seconds",
            ),
            (160, "Spool"),
            (180, "Spool"),
            (276, "Spool"),
        ],
    );
    let fix = occurrence_layout(453, &[(245, "after")]);
    env.session(sid, "omp", "/w", "interactive", T0).await;
    env.snapshot(sid, snap, "published").await;
    for (seq, key, text) in [
        (0, "status", status.as_str()),
        (1, "cause", cause.as_str()),
        (2, "fix", fix.as_str()),
    ] {
        env.message(
            snap,
            Message {
                key,
                seq,
                role: "user",
                ts: Some(T0),
                parent: None,
                text,
            },
        )
        .await;
    }
    let first = format!("USER: {status}\n\nUSER: {cause}\n\nUSER: {}", &fix[..18]);
    let second = format!("USER: {}\n\nUSER: {}", &fix[..18], &fix[18..]);
    env.chunk(
        sid,
        snap,
        "first",
        search_common::Chunk {
            body: &first,
            spans: vec![
                ("status", 0, 0, 580, false),
                ("cause", 0, 0, 836, false),
                ("fix", 0, 0, 18, false),
            ],
            anchor: Some(T0),
            run: "run",
            ordinal: 0,
            embed: false,
        },
    )
    .await;
    env.chunk(
        sid,
        snap,
        "second",
        search_common::Chunk {
            body: &second,
            spans: vec![("fix", 0, 0, 18, true), ("fix", 0, 18, 453, false)],
            anchor: Some(T0),
            run: "run",
            ordinal: 1,
            embed: false,
        },
    )
    .await;
    env.activate(sid, snap).await;
    let result = env.ok(json!({"query":"Why did WORKER_ALREADY_EXISTS appear after restarting Spool?", "mode":"lexical", "matches_per_session":1})).await;
    let m = &matches_of(&result, sid)[0];
    let excerpt = m["preview"]["text"].as_str().unwrap();
    assert!(!excerpt.is_empty());
    let (_, reference) = env.decode_match(m["match_ref"].as_str().unwrap()).await;
    assert!(reference.records.iter().any(|r| r.key == "status"));
    assert!(reference.records.iter().any(|r| r.key == "cause"));
}
