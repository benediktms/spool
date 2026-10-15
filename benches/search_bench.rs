//! Search benchmark (spec 20): seeds a real `index.sqlite` with N synthetic chunks (text + FTS rows +
//! 768-dim float32 vectors) and measures the real `SearchService` in-process. Not an IPC benchmark.
//!
//!   cargo bench --bench search_bench -- 15000 100000
//!
//! Prints a markdown table for docs/performance-results.md. Vectors come from the deterministic
//! `FakeProvider` (hash-based), so timings measure scan/scoring/SQLite, not model quality.

use serde_json::json;
use spool::clock::{self, SharedClock};
use spool::config::Config;
use spool::embeddings::{EmbeddingService, FakeProvider};
use spool::search::SearchService;
use spool::storage::index::Index;
use spool::transcript::handle::{HandleKey, KeyStore};
use spool::transcript::lease::LeaseManager;
use std::sync::Arc;
use std::time::{Duration, Instant};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

fn vocab(n: usize) -> Vec<String> {
    let syll = [
        "ka", "to", "mi", "re", "su", "lo", "ve", "ni", "pa", "do", "ri", "xe", "bu", "ta", "go",
        "fi",
    ];
    (0..n)
        .map(|i| {
            let mut x = i + 1;
            let mut w = String::new();
            while x > 0 {
                w.push_str(syll[x % 16]);
                x /= 16;
            }
            w
        })
        .collect()
}

fn pct(v: &mut [Duration], p: f64) -> f64 {
    v.sort();
    v[((v.len() as f64 * p).ceil() as usize)
        .saturating_sub(1)
        .min(v.len() - 1)]
    .as_secs_f64()
        * 1000.0
}

fn rss_mib() -> f64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok();
    out.and_then(|o| {
        String::from_utf8_lossy(&o.stdout)
            .trim()
            .parse::<f64>()
            .ok()
    })
    .unwrap_or(0.0)
        / 1024.0
}

async fn run(n_chunks: usize) {
    const DIMS: usize = 768;
    let tmp = tempfile::tempdir().unwrap();
    let index = Index::open_for_test(tmp.path()).await.unwrap();
    let mut cfg = Config::default();
    cfg.embedding.enabled = true;
    cfg.embedding.dimensions = DIMS;
    cfg.reader.max_active_leases = 100_000; // every search pins a lease; the benchmark issues hundreds of queries
    let cfg = Arc::new(cfg);
    let clock: SharedClock = clock::system();
    let provider = Arc::new(FakeProvider::new(DIMS));
    let emb = Arc::new(EmbeddingService::new(
        index.clone(),
        cfg.embedding.clone(),
        cfg.search.clone(),
        clock.clone(),
        provider.clone(),
    ));
    let profile = emb.ensure_profile().await.unwrap();
    HandleKey::create(&tmp.path().join("k")).unwrap();
    let keys = KeyStore::load(&tmp.path().join("k")).unwrap();
    let leases = LeaseManager::new(index.clone(), cfg.reader.clone(), clock.clone());
    let svc = SearchService::new(index.clone(), cfg.clone(), emb.clone(), leases, keys);

    // ---- seed: 20 chunks per session, one message+block+span per chunk
    let words = vocab(4000);
    let mut rng = Rng(0x9E3779B97F4A7C15);
    let now = clock.now_ms();
    let t0 = Instant::now();
    let mut tx = index.writer.begin().await.unwrap();
    sqlx::query("INSERT INTO sources(id, kind, roots_json, enabled, last_scan_at, last_scan_ok) VALUES ('omp','omp','[]',1,?,1)").bind(now).execute(&mut *tx).await.unwrap();
    let sessions = n_chunks.div_ceil(20);
    let mut known_literal = String::new();
    for s in 0..sessions {
        let (sid, snap) = (format!("ss_{s:06}"), format!("sn_{s:06}"));
        sqlx::query("INSERT INTO sessions(id, source_id, native_id, session_kind, cwd, native_title, last_activity_at, created_at, index_state, discovered_at, active_snapshot_id) VALUES (?, 'omp', ?, 'interactive', ?, ?, ?, ?, 'indexed', 0, ?)")
            .bind(&sid).bind(&sid).bind(format!("/proj/p{}", s % 40)).bind(format!("session {s}")).bind(now - (s as i64) * 60_000).bind(now - (s as i64) * 60_000 - 1000).bind(&snap)
            .execute(&mut *tx).await.unwrap();
        sqlx::query("INSERT INTO snapshots(id, session_id, kind, state, source_revision, fingerprint, generation, prefix_end, created_at, published_at, retained_bytes) VALUES (?,?, 'indexed','published','r','f',1,0,0,0,1000)")
            .bind(&snap).bind(&sid).execute(&mut *tx).await.unwrap();
        for c in 0..20.min(n_chunks - s * 20) {
            let key = format!("{sid}_m{c}"); // native record ids are unique per session
            let mut text = String::new();
            for _ in 0..90 {
                // Zipf-ish: bias toward low indices
                let r = (rng.next() % 1_000_000) as f64 / 1_000_000.0;
                text.push_str(&words[((r * r * r) * words.len() as f64) as usize]);
                text.push(' ');
            }
            if s == sessions / 2 && c == 7 {
                text.push_str("needle_symbol::unique_42");
                known_literal = "needle_symbol::unique_42".into();
            }
            let body = format!("USER: {text}");
            let hash = spool::domain::digest(body.as_bytes());
            sqlx::query("INSERT INTO records(snapshot_id, key, ord, kind, ts, offset, len, digest) VALUES (?,?,?, 'message', ?, 0, 10, zeroblob(32))").bind(&snap).bind(&key).bind(c as i64).bind(now - c as i64).execute(&mut *tx).await.unwrap();
            sqlx::query("INSERT INTO messages(snapshot_id, record_key, seq, role, ts) VALUES (?,?,?, 'user', ?)").bind(&snap).bind(&key).bind(c as i64).bind(now - c as i64).execute(&mut *tx).await.unwrap();
            sqlx::query("INSERT INTO blocks(snapshot_id, record_key, idx, origin, text, text_digest) VALUES (?,?,0,'authored',?, zeroblob(32))").bind(&snap).bind(&key).bind(&text).execute(&mut *tx).await.unwrap();
            let rowid = sqlx::query("INSERT INTO chunks(logical_id, snapshot_id, session_id, run_key, ordinal, body, input_hash, time_anchor, rendered_tokens) VALUES (?,?,?, 'r', ?, ?, ?, ?, 120)")
                .bind(format!("{sid}-{c}")).bind(&snap).bind(&sid).bind(c as i64).bind(&body).bind(hash.to_vec()).bind(now - c as i64).execute(&mut *tx).await.unwrap().last_insert_rowid();
            sqlx::query("INSERT INTO chunk_spans(chunk_rowid, seq, record_key, block_index, start_byte, end_byte, is_context) VALUES (?,0,?,0,0,?,0)").bind(rowid).bind(&key).bind(text.len() as i64).execute(&mut *tx).await.unwrap();
            let v = provider.vector_for(&body);
            let blob: Vec<u8> = v.iter().flat_map(|x| x.to_le_bytes()).collect();
            sqlx::query("INSERT INTO embeddings(profile_id, input_hash, dimensions, vec, created_at) VALUES (?,?,?,?,0) ON CONFLICT DO NOTHING").bind(&profile).bind(hash.to_vec()).bind(DIMS as i64).bind(blob).execute(&mut *tx).await.unwrap();
        }
        if s % 200 == 199 {
            tx.commit().await.unwrap();
            tx = index.writer.begin().await.unwrap();
        }
    }
    tx.commit().await.unwrap();
    let seed = t0.elapsed();
    let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&index.writer)
        .await;
    let db_mib = std::fs::metadata(tmp.path().join("index.sqlite"))
        .unwrap()
        .len() as f64
        / 1048576.0;
    let vec_mib = (n_chunks * DIMS * 4) as f64 / 1048576.0;

    // ---- measure
    let queries: Vec<String> = (0..60)
        .map(|i| format!("{} {}", words[(i * 37) % 400], words[(i * 91) % 900 + 50]))
        .collect();
    let mut rows = Vec::new();
    let common = vec![words[0].clone(); 30];
    // chunk anchors lie within 20 ms before `now`: the window admits every candidate hit (worst case for the chunk join)
    let window = json!({"after": spool::search::format_rfc3339(now - 10_000), "before": spool::search::format_rfc3339(now + 1_000)});
    for (label, mode, windowed, qs) in [
        ("lexical", "lexical", false, queries.clone()),
        ("literal", "literal", false, vec![known_literal.clone(); 30]),
        ("literal, common word", "literal", false, common.clone()),
        ("literal, common word, time window", "literal", true, common),
        ("semantic", "semantic", false, queries.clone()),
        ("hybrid", "hybrid", false, queries.clone()),
    ] {
        // warm up
        let _ = svc.call(json!({"query": qs[0], "mode": mode})).await;
        let mut lat = Vec::new();
        for (i, q) in qs.iter().enumerate() {
            let q = if mode == "semantic" || mode == "hybrid" {
                format!("{q} variant{i}")
            } else {
                q.clone()
            };
            let mut args = json!({"query": q, "mode": mode, "limit": 10});
            if windowed {
                args.as_object_mut()
                    .unwrap()
                    .extend(window.as_object().unwrap().clone());
            }
            let t = Instant::now();
            let r = svc.call(args).await;
            lat.push(t.elapsed());
            if label == "literal" {
                assert_eq!(
                    r.unwrap()["results"].as_array().unwrap().len(),
                    1,
                    "literal needle found exactly"
                );
            } else {
                let r = r.unwrap();
                if label.starts_with("literal, common") {
                    assert_eq!(
                        r["results"].as_array().unwrap().len(),
                        10,
                        "common word fills the page"
                    );
                }
            }
        }
        rows.push((
            label,
            pct(&mut lat.clone(), 0.5),
            pct(&mut lat.clone(), 0.95),
            pct(&mut lat, 0.99),
        ));
    }
    println!("\n### {n_chunks} chunks ({sessions} sessions)\n");
    println!(
        "seed {:.1}s, index.sqlite {:.1} MiB, raw vector payload {:.1} MiB, process RSS after run {:.0} MiB\n",
        seed.as_secs_f64(),
        db_mib,
        vec_mib,
        rss_mib()
    );
    println!("| mode | p50 ms | p95 ms | p99 ms |\n|---|---|---|---|");
    for (m, a, b, c) in rows {
        println!("| {m} | {a:.1} | {b:.1} | {c:.1} |");
    }
}

fn main() {
    let sizes: Vec<usize> = std::env::args()
        .skip(1)
        .filter_map(|a| a.parse().ok())
        .collect();
    let sizes = if sizes.is_empty() {
        vec![15_000]
    } else {
        sizes
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    for n in sizes {
        rt.block_on(run(n));
    }
}
