use serde_json::Value;
use spool::embeddings::tokenizer;

#[path = "search_common/mod.rs"]
mod search_common;

#[path = "reader_common/mod.rs"]
mod reader_common;

async fn capture_read(reader: &spool::transcript::ReadService, first: Value) -> Value {
    use serde_json::json;
    let mut pages = Vec::new();
    let mut bytes = 0usize;
    let mut args = first;
    loop {
        match reader.call(args).await {
            Ok(page) => {
                bytes += serde_json::to_vec(&page).unwrap().len();
                let cursor = page["page"]["next_cursor"].as_str().map(str::to_owned);
                let exhausted = page["page"]["selection_exhausted"] == true;
                pages.push(page);
                match cursor {
                    None => {
                        return json!({"origin":"indexed_snapshot", "complete":exhausted, "page_count":pages.len(), "serialized_bytes":bytes, "pages":pages});
                    }
                    Some(_) if pages.len() >= 32 || bytes >= 8 * 1024 * 1024 => {
                        return json!({"origin":"indexed_snapshot", "complete":false, "reason":"capture_limit", "page_count":pages.len(), "serialized_bytes":bytes, "pages":pages});
                    }
                    Some(cursor) => args = json!({"cursor":cursor}),
                }
            }
            Err(error) => {
                return json!({"origin":"indexed_snapshot", "complete":false, "error":error.to_tool_json(), "page_count":pages.len(), "serialized_bytes":bytes, "pages":pages});
            }
        }
    }
}

#[tokio::test]
async fn capture_read_follows_pages() {
    use serde_json::json;
    let env = reader_common::Env::new().await;
    let (sid, _) = env
        .indexed("eval-pagination.jsonl", &reader_common::linear(6))
        .await;
    let captured = capture_read(
        &env.svc,
        json!({"session_id":sid,"source_policy":"indexed_only","max_items":1}),
    )
    .await;
    assert_eq!(captured["complete"], true);
    assert!(captured["page_count"].as_u64().unwrap() > 1);
    assert_eq!(
        captured["pages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|page| page["items"].as_array().unwrap().len())
            .sum::<usize>(),
        6
    );
}

async fn collect_ranks(
    service: &spool::search::SearchService,
    manifest: &Value,
    modes: &[&str],
    snippets: &mut Vec<Value>,
    reader: Option<&spool::transcript::ReadService>,
) -> Vec<Value> {
    use serde_json::json;
    let mut results = Vec::new();
    for mode in modes {
        for query in manifest["queries"].as_array().unwrap() {
            let mut args = json!({"query":query["query"],"mode":mode,"limit":10});
            if let Some(projects) = query.get("projects") {
                args["projects"] = projects.clone();
            }
            let response = service.call(args).await.expect("search failed");
            assert_eq!(response["mode_used"], *mode);
            assert_eq!(response["truncated"], false);
            if query.get("projects").is_some() {
                assert!(response["warnings"].as_array().unwrap().is_empty());
            }
            let session_ids: Vec<_> = response["results"]
                .as_array()
                .expect("missing results")
                .iter()
                .map(|result| result["session_id"].clone())
                .collect();
            for result in response["results"].as_array().unwrap() {
                let mut reads = Vec::new();
                if let Some(reader) = reader {
                    for m in result["matches"].as_array().unwrap() {
                        reads.push(capture_read(reader, json!({"session_id": result["session_id"], "source_policy": "indexed_only", "selection": {"kind": "match", "match_ref": m["match_ref"]}})).await);
                    }
                }
                snippets.push(json!({"mode":mode,"query_id":query["id"],"session_id":result["session_id"],"matches":result["matches"],"reads":reads}));
            }
            results.push(json!({"mode":mode,"query_id":query["id"],"session_ids":session_ids,"warnings":response["warnings"],"coverage":response["coverage"]}));
        }
    }
    results
}

fn score(results: &[Value], matrix: &Value) -> Value {
    use serde_json::json;
    let mut reports = Vec::new();
    for mode in ["lexical", "semantic", "hybrid"] {
        let mut count = 0usize;
        let mut recall5 = 0.0;
        let mut recall10 = 0.0;
        let mut reciprocal = 0.0;
        let mut negative = 0usize;
        let mut negative_empty = 0usize;
        for result in results.iter().filter(|r| r["mode"] == mode) {
            let relevant: std::collections::HashSet<_> = matrix["judgments"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|row| {
                    row["query_id"] == result["query_id"] && row["judgment"] == "proposed_relevant"
                })
                .filter_map(|row| row["session_id"].as_str())
                .collect();
            let ids = result["session_ids"].as_array().unwrap();
            if relevant.is_empty() {
                negative += 1;
                negative_empty += usize::from(ids.is_empty());
                continue;
            }
            count += 1;
            let ranks: Vec<_> = ids
                .iter()
                .enumerate()
                .filter(|(_, id)| id.as_str().is_some_and(|id| relevant.contains(id)))
                .map(|(i, _)| i + 1)
                .collect();
            recall5 +=
                ranks.iter().filter(|rank| **rank <= 5).count() as f64 / relevant.len() as f64;
            recall10 += ranks.len() as f64 / relevant.len() as f64;
            reciprocal += ranks.first().map_or(0.0, |rank| 1.0 / *rank as f64);
        }
        if count > 0 {
            reports.push(json!({"mode":mode,"positive_queries":count,"recall_at_5":recall5/count as f64,"recall_at_10":recall10/count as f64,"mrr_at_10":reciprocal/count as f64,"negative_queries":negative,"negative_empty":negative_empty}));
        }
    }
    json!({"judgments":"proposed; acceptance required", "snippet_usefulness":"unjudged", "modes":reports})
}

fn query_token_preflight(manifest: &Value) -> Result<(usize, usize), &'static str> {
    let queries = manifest["queries"].as_array().ok_or("missing queries")?;
    if queries.len() != 50 {
        return Err("expected exactly 50 queries");
    }
    let tokenizer = tokenizer();
    let mut total = 0usize;
    for query in queries {
        let text = query["query"].as_str().ok_or("missing query text")?;
        let tokens = tokenizer.count(text);
        if text.trim().is_empty() || text.len() > 4096 || tokens > 1024 {
            return Err("query exceeds search input limits");
        }
        total = total.checked_add(tokens).ok_or("token total overflow")?;
    }
    Ok((queries.len() * 2, total * 2))
}

#[test]
fn preflight_rejects_incomplete_or_invalid_query_sets() {
    use serde_json::json;
    assert!(query_token_preflight(&json!({"queries": []})).is_err());
    let queries: Vec<_> = (0..50).map(|_| json!({"query": "test query"})).collect();
    let result = query_token_preflight(&json!({"queries": queries})).unwrap();
    assert_eq!(result.0, 100);
    assert!(result.1 > 0);
    let invalid: Vec<_> = (0..50).map(|_| json!({"query": ""})).collect();
    assert!(query_token_preflight(&json!({"queries": invalid})).is_err());
    let results = vec![json!({"mode":"lexical","query_id":"q","session_ids":["a"]})];
    let matrix = json!({"judgments":[{"query_id":"q","session_id":"a","judgment":"proposed_relevant"},{"query_id":"q","session_id":"b","judgment":"proposed_relevant"}]});
    let report = score(&results, &matrix);
    assert_eq!(report["modes"][0]["recall_at_5"], 0.5);
    assert_eq!(report["modes"][0]["mrr_at_10"], 1.0);
}

#[tokio::test]
async fn synthetic_search_uses_query_embeddings_only() {
    use serde_json::json;
    use spool::embeddings::provider::Purpose;
    let env = search_common::Env::new().await;
    env.simple_with(
        "synthetic",
        &search_common::Seed {
            embed: true,
            ..Default::default()
        },
        &[("synthetic database conversation", None)],
    )
    .await;
    let before: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT rowid, input_hash FROM chunks ORDER BY rowid")
            .fetch_all(&env.index.reader)
            .await
            .unwrap();
    let queries: Vec<_> = (0..50)
        .map(|i| json!({"id":i,"query":format!("database synthetic query {i}")}))
        .collect();
    let manifest = json!({"queries":queries});
    assert_eq!(query_token_preflight(&manifest).unwrap().0, 100);
    let results = collect_ranks(
        &env.svc,
        &manifest,
        &["semantic", "hybrid"],
        &mut Vec::new(),
        None,
    )
    .await;
    assert_eq!(results.len(), 100);
    assert_eq!(env.provider.calls_for(Purpose::Ingest), 0);
    assert!(env.provider.calls_for(Purpose::Query) <= 100);
    let after: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT rowid, input_hash FROM chunks ORDER BY rowid")
            .fetch_all(&env.index.reader)
            .await
            .unwrap();
    assert_eq!(before, after);
}

#[test]
#[ignore = "explicit private manifest; offline query-token preflight"]
fn controlled_query_token_preflight() {
    let path = std::env::var_os("SPOOL_CONTROLLED_EVAL_MANIFEST")
        .expect("explicit private manifest path required");
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(path).expect("manifest unavailable"))
            .expect("invalid manifest");
    let (max_calls, query_tokens) = query_token_preflight(&manifest).expect("preflight failed");
    println!("maximum_query_embedding_calls={max_calls} estimated_query_tokens={query_tokens}");
}

#[tokio::test]
#[ignore = "explicit private corpus and manifest; offline lexical retrieval"]
async fn controlled_lexical_preview() {
    run_controlled(false, false).await;
}

#[tokio::test]
#[ignore = "explicitly approved private queries; sends query text to configured provider"]
async fn controlled_hosted_evaluation() {
    run_controlled(true, false).await;
}

#[tokio::test]
#[ignore = "explicit private corpus and manifest; offline expanded lexical retrieval"]
async fn controlled_explicit_matches() {
    run_controlled(false, true).await;
}

async fn run_controlled(hosted: bool, expanded: bool) {
    use serde_json::json;
    use spool::clock;
    use spool::config::{Config, Paths};
    use spool::embeddings::{EmbeddingService, EmbeddingState, FakeProvider};
    use spool::search::SearchService;
    use spool::storage::index::Index;
    use spool::transcript::handle::{HandleKey, KeyStore};
    use spool::transcript::lease::LeaseManager;
    use std::sync::Arc;
    use std::time::Duration;

    let corpus = std::env::var_os("SPOOL_CONTROLLED_EVAL_CORPUS")
        .expect("explicit private corpus path required");
    let manifest = std::env::var_os("SPOOL_CONTROLLED_EVAL_MANIFEST")
        .expect("explicit private manifest path required");
    let manifest_bytes = std::fs::read(manifest).unwrap();
    let manifest: Value = serde_json::from_slice(&manifest_bytes).unwrap();
    let (_, query_tokens) = query_token_preflight(&manifest).expect("invalid query set");
    if hosted {
        for (name, bytes) in [
            ("SPOOL_CONTROLLED_EVAL_MANIFEST_BLAKE3", manifest_bytes),
            (
                "SPOOL_CONTROLLED_EVAL_CORPUS_BLAKE3",
                std::fs::read(&corpus).unwrap(),
            ),
            (
                "SPOOL_CONTROLLED_EVAL_MATRIX_BLAKE3",
                std::fs::read(
                    std::env::var_os("SPOOL_CONTROLLED_EVAL_MATRIX")
                        .expect("judgment matrix required"),
                )
                .unwrap(),
            ),
        ] {
            let expected = std::env::var(name).expect("approved artifact hash required");
            assert_eq!(
                blake3::hash(&bytes).to_hex().as_str(),
                expected,
                "approved artifact hash mismatch"
            );
            if name == "SPOOL_CONTROLLED_EVAL_MATRIX_BLAKE3" {
                let matrix: Value =
                    serde_json::from_slice(&bytes).expect("invalid judgment matrix");
                let rows = matrix["judgments"].as_array().expect("missing judgments");
                assert!(!rows.is_empty());
                assert!(
                    rows.iter().all(|row| matches!(
                        row["judgment"].as_str(),
                        Some(
                            "proposed_relevant" | "proposed_not_relevant" | "no_searchable_content"
                        )
                    )),
                    "incomplete judgment matrix"
                );
            }
        }
    }
    let mut tmp = Some(tempfile::tempdir().unwrap());
    let workdir = if hosted {
        let path = tmp.take().unwrap().keep();
        eprintln!(
            "private evaluation working index and ledger: {}",
            path.display()
        );
        path
    } else {
        tmp.as_ref().unwrap().path().to_path_buf()
    };
    let copy = workdir.join("index.sqlite");
    let mut wal = corpus.clone();
    wal.push("-wal");
    assert!(
        std::fs::metadata(wal).map_or(true, |m| m.len() == 0),
        "frozen corpus has uncheckpointed WAL content"
    );
    std::fs::copy(corpus, &copy).expect("copy frozen index");
    let index = Index::open(&copy, Duration::from_secs(5)).await.unwrap();
    if hosted {
        let requests: i64 = sqlx::query_scalar("SELECT count(*) FROM embedding_requests")
            .fetch_one(&index.reader)
            .await
            .unwrap();
        assert_eq!(requests, 0, "frozen evaluation ledger must be empty");
        use std::collections::HashSet;
        let matrix: Value = serde_json::from_slice(
            &std::fs::read(std::env::var_os("SPOOL_CONTROLLED_EVAL_MATRIX").unwrap()).unwrap(),
        )
        .unwrap();
        let queries: HashSet<_> = manifest["queries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|q| q["id"].as_str().expect("missing query id").to_owned())
            .collect();
        assert_eq!(queries.len(), 50, "query ids must be unique");
        let sessions: Vec<String> = sqlx::query_scalar("SELECT id FROM sessions")
            .fetch_all(&index.reader)
            .await
            .unwrap();
        let expected: HashSet<_> = queries
            .iter()
            .flat_map(|q| sessions.iter().map(move |s| (q.clone(), s.clone())))
            .collect();
        let rows = matrix["judgments"].as_array().unwrap();
        let actual: HashSet<_> = rows
            .iter()
            .map(|row| {
                (
                    row["query_id"].as_str().unwrap().to_owned(),
                    row["session_id"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        assert_eq!(rows.len(), expected.len(), "duplicate or missing judgments");
        assert_eq!(actual, expected, "judgments must cover the frozen corpus");
    }
    let mut cfg = if hosted {
        assert_eq!(
            std::env::var("SPOOL_CONTROLLED_EVAL_APPROVED").as_deref(),
            Ok("yes")
        );
        let path = std::path::PathBuf::from(
            std::env::var_os("SPOOL_CONTROLLED_EVAL_CONFIG").expect("configuration path required"),
        );
        assert!(path.is_absolute(), "configuration path must be absolute");
        let base = path.parent().unwrap();
        let mut cfg = Config::parse(&std::fs::read_to_string(&path).unwrap(), base).unwrap();
        assert!(cfg.embedding.enabled && cfg.embedding.hosted_processing_approved);
        assert!(u64::try_from(query_tokens).unwrap() <= cfg.embedding.daily_query_token_budget);
        cfg.embedding.credential_file = cfg
            .credential_path(base)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        cfg.embedding.daily_ingestion_token_budget = 0;
        cfg.embedding.daily_query_token_budget = u64::try_from(query_tokens).unwrap();
        cfg.search = Config::default().search;
        cfg
    } else {
        Config::default()
    };
    if !hosted {
        cfg.embedding.enabled = false;
        for source in &mut cfg.sources {
            source.enabled = true;
            source.roots = vec![workdir.to_string_lossy().into_owned()];
        }
    }
    let cfg = Arc::new(cfg);
    let clock = clock::system();
    let embedding = Arc::new(if hosted {
        let paths = Paths::from_root(workdir.clone()).unwrap();
        EmbeddingService::from_config(index.clone(), &cfg, &paths, clock.clone()).unwrap()
    } else {
        EmbeddingService::new(
            index.clone(),
            cfg.embedding.clone(),
            cfg.search.clone(),
            clock.clone(),
            Arc::new(FakeProvider::new(64)),
        )
    });
    if hosted {
        assert_eq!(embedding.status().state, EmbeddingState::Ready);
        let active: String =
            sqlx::query_scalar("SELECT value FROM app_meta WHERE key='active_embedding_profile'")
                .fetch_one(&index.reader)
                .await
                .unwrap();
        assert_eq!(
            embedding.profile().id(),
            active,
            "configured profile differs from frozen vectors"
        );
        let coverage = embedding.vector_coverage().await.unwrap();
        assert!(
            coverage.total_chunks > 0
                && coverage.pending_chunks == 0
                && coverage.embedded_chunks == coverage.total_chunks
                && coverage.profile_id == active
        );
    }
    let path = workdir.join("handle.key");
    HandleKey::create(&path).unwrap();
    let keys = KeyStore::load(&path).unwrap();
    let leases = LeaseManager::new(index.clone(), cfg.reader.clone(), clock.clone());
    let sources = Arc::new(spool::sources::SourceRegistry::new(&cfg).unwrap());
    let engine = Arc::new(spool::ingest::engine::IngestEngine::new(
        index.clone(),
        cfg.clone(),
        sources.clone(),
        embedding.clone(),
        clock,
    ));
    let reader = spool::transcript::ReadService::new(
        index.clone(),
        cfg.clone(),
        leases.clone(),
        keys.clone(),
        sources,
        engine.source_read_stager(),
    );
    let service = SearchService::new(index.clone(), cfg, embedding, leases.clone(), keys.clone());
    if expanded {
        let comparison = std::process::Command::new("python3")
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/controlled_compare.py"
            ))
            .status()
            .expect("default capture comparison failed to start");
        assert!(
            comparison.success(),
            "default capture differs from baseline"
        );
        use spool::transcript::handle::HandleKind;
        use spool::transcript::refs::{MatchRef, RefHandle};
        let started = std::time::Instant::now();
        let trace: Value = serde_json::from_slice(
            &std::fs::read(
                std::env::var_os("SPOOL_CONTROLLED_EVAL_BASELINE_TRACE")
                    .expect("baseline trace required"),
            )
            .unwrap(),
        )
        .unwrap();
        let mut captures = Vec::new();
        for (id, primary, rowids, key, digest) in [
            (
                "codex_remote_secret",
                27097,
                vec![27096, 27097, 27098],
                "451a3a90-e47d-461f-abf8-7ab2a3ead367",
                "e4bea6f371f735f11533fae9e23a1b6507fbdb416ad0cf786da3a373e821777d",
            ),
            (
                "project_migration_trigger",
                27119,
                vec![27118, 27119, 27120],
                "b2537e6b-89d4-4c29-afbd-f49c3fe5ceb4",
                "dee55858ab15b39be04bc731c210720b45d5607be7491d53d02da4d1a73c7e27",
            ),
        ] {
            let query = manifest["queries"]
                .as_array()
                .unwrap()
                .iter()
                .find(|q| q["id"] == id)
                .unwrap();
            let entry = trace
                .as_array()
                .unwrap()
                .iter()
                .find(|t| t["query_id"] == id && t["session_id"] == "ss_ea6d9471703be31b101c28ea")
                .unwrap();
            assert_eq!(entry["snapshot_id"], "snap_ad3ba43876b5925889052103");
            let window = entry["candidates"]
                .as_array()
                .unwrap()
                .iter()
                .find(|c| c["primary"] == primary && c["rowids"] == json!(rowids))
                .unwrap();
            let mut expected: Vec<_> = window["anchors"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect();
            expected.sort();
            let mut args = json!({"query":query["query"], "mode":"lexical", "session_ids":["ss_ea6d9471703be31b101c28ea"], "limit":1, "matches_per_session":32, "max_bytes":65536});
            if let Some(projects) = query.get("projects") {
                args["projects"] = projects.clone();
            }
            let response = service.call(args).await.unwrap();
            let response_bytes = spool::mcp::budget::tool_result_bytes(&response, false);
            let matches = response["results"][0]["matches"].as_array().unwrap();
            let mut found = None;
            for m in matches {
                let handle = RefHandle::decode(
                    &keys.current(),
                    HandleKind::Match,
                    m["match_ref"].as_str().unwrap(),
                )
                .unwrap();
                let payload = leases
                    .get_ref(&handle.lease, handle.ref_id, "match")
                    .await
                    .unwrap();
                let reference: MatchRef = serde_json::from_slice(&payload).unwrap();
                let mut actual: Vec<_> = reference.records.iter().map(|r| r.key.clone()).collect();
                actual.sort();
                if actual == expected {
                    let record = reference.records.iter().find(|r| r.key == key).unwrap();
                    let indexed_digest: Vec<u8> = sqlx::query_scalar(
                        "SELECT digest FROM records WHERE snapshot_id = ? AND key = ?",
                    )
                    .bind("snap_ad3ba43876b5925889052103")
                    .bind(key)
                    .fetch_one(&index.reader)
                    .await
                    .unwrap();
                    assert_eq!(
                        indexed_digest
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect::<String>(),
                        digest
                    );
                    assert!(digest.starts_with(&record.digest));
                    let read = capture_read(&reader, json!({"session_id":"ss_ea6d9471703be31b101c28ea", "source_policy":"indexed_only", "selection":{"kind":"match", "match_ref":m["match_ref"]}})).await;
                    assert_eq!(read["complete"], true);
                    let mut read_anchors: Vec<_> =
                        read["pages"][0]["selection"]["anchor_message_refs"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|v| {
                                spool::transcript::refs::MessageRef::decode(
                                    &keys.current(),
                                    v.as_str().unwrap(),
                                )
                                .unwrap()
                                .record
                            })
                            .collect();
                    read_anchors.sort();
                    assert_eq!(read_anchors, expected);
                    found = Some(read);
                    break;
                }
            }
            assert!(found.is_some(), "correction window missing: {id}");
            assert!(response_bytes < 65536);
            captures.push(
                json!({"id":id,"response":response,"response_bytes":response_bytes,"read":found}),
            );
        }
        let path =
            std::env::var_os("SPOOL_CONTROLLED_EVAL_SNIPPETS").expect("private capture required");
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .unwrap();
        serde_json::to_writer(file, &captures).unwrap();
        println!("expanded elapsed_ms={}", started.elapsed().as_millis());
        return;
    }
    let mut snippets = Vec::new();
    let mut snippet_file = std::env::var_os("SPOOL_CONTROLLED_EVAL_SNIPPETS").map(|path| {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .expect("cannot create private snippet file")
    });
    assert!(
        !hosted || snippet_file.is_some(),
        "private snippet path required"
    );
    let results = collect_ranks(
        &service,
        &manifest,
        if hosted {
            &["semantic", "hybrid"][..]
        } else {
            &["lexical"][..]
        },
        &mut snippets,
        Some(&reader),
    )
    .await;
    if !hosted && let Some(path) = std::env::var_os("SPOOL_CONTROLLED_EVAL_LITERAL_PROBES") {
        let probes: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        for (probe_index, probe) in probes["probes"]
            .as_array()
            .expect("missing literal probes")
            .iter()
            .enumerate()
        {
            let response = service
                .call(json!({"query": probe["literal"], "mode": "literal", "limit": 10}))
                .await
                .expect("literal search failed");
            assert_eq!(response["mode_used"], "literal");
            assert_eq!(response["truncated"], false);
            let ids: Vec<_> = response["results"]
                .as_array()
                .unwrap()
                .iter()
                .map(|result| &result["session_id"])
                .collect();
            for result in response["results"].as_array().unwrap() {
                let mut reads = Vec::new();
                for m in result["matches"].as_array().unwrap() {
                    reads.push(capture_read(&reader, json!({"session_id": result["session_id"], "source_policy": "indexed_only", "selection": {"kind": "match", "match_ref": m["match_ref"]}})).await);
                }
                snippets.push(json!({"mode":"literal","query_id":probe_index,"session_id":result["session_id"],"matches":result["matches"],"reads":reads}));
            }
            for expected in probe["expected_sessions"].as_array().unwrap() {
                assert!(
                    ids.contains(&expected),
                    "literal probe missed a retained session"
                );
            }
        }
    }
    if let Some(file) = snippet_file.as_mut() {
        serde_json::to_writer(file, &snippets).expect("write private snippets");
    }
    let metrics = std::env::var_os("SPOOL_CONTROLLED_EVAL_MATRIX").map(|path| {
        let matrix: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert!(
            matrix["judgments"]
                .as_array()
                .unwrap()
                .iter()
                .all(|row| row["judgment"] != "pending")
        );
        score(&results, &matrix)
    });
    println!("{}", json!({"queries": results,"metrics":metrics}));
}
