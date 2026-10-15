//! Curated retrieval evaluation (spec 20.1) over a SYNTHETIC supported corpus with manually authored
//! queries and relevance annotations (session-level + supporting phrase). The semantic retriever here is
//! the deterministic concept-table `FakeProvider`, so the semantic/hybrid numbers evaluate the pipeline
//! (filters, scan, fusion, grouping, branch handling), NOT the quality of a real embedding model. A live
//! evaluation needs a provider key and the user's hosted-processing consent and is not run here.
//!
//! `cargo test --test retrieval_eval -- --nocapture` prints the report that is recorded in
//! docs/retrieval-eval.md.

#[path = "search_common/mod.rs"]
mod search_common;

use search_common::*;
use serde_json::{Value, json};
use spool::clock::{ManualClock, SharedClock};
use spool::config::Config;
use spool::embeddings::{EmbeddingService, FakeProvider};
use spool::search::SearchService;
use spool::storage::index::Index;
use spool::transcript::handle::{HandleKey, KeyStore};
use spool::transcript::lease::LeaseManager;
use std::sync::Arc;

const DAY: i64 = 86_400_000;

const GROUPS: &[&[&str]] = &[
    &[
        "webhook",
        "callback",
        "notification",
        "endpoint",
        "receiver",
    ],
    &["hmac", "signature", "checksum", "authenticity"],
    &["rollback", "revert", "undo", "restore"],
    &["flaky", "intermittent", "nondeterministic", "unreliable"],
    &[
        "reducer", "state", "store", "context", "redraw", "redux", "flux", "rerender",
    ],
    &[
        "borrow",
        "ownership",
        "lifetime",
        "reference",
        "aliasing",
        "mutability",
        "checker",
    ],
    &["kubernetes", "rollout", "deployment", "cluster", "helm"],
    &[
        "embedding",
        "vector",
        "similarity",
        "cosine",
        "nearest",
        "neighbour",
        "neighbor",
        "lookup",
    ],
    &[
        "flexbox",
        "grid",
        "layout",
        "responsive",
        "arrangement",
        "mobile",
        "adaptive",
    ],
    &[
        "oauth",
        "token",
        "refresh",
        "login",
        "credential",
        "signin",
        "authentication",
        "renewal",
    ],
    &[
        "profiling",
        "flamegraph",
        "hotspot",
        "performance",
        "slow",
        "latency",
        "bottleneck",
        "speed",
    ],
    &["logging", "logs", "tracing", "telemetry", "observability"],
    &[
        "rebase",
        "merge",
        "bisect",
        "conflict",
        "integrate",
        "combine",
        "regression",
        "culprit",
    ],
    &[
        "jquery",
        "legacy",
        "deprecated",
        "outdated",
        "obsolete",
        "archaic",
    ],
    &["postgres", "migration", "schema", "column", "database"],
    &[
        "sqlite",
        "embedded",
        "file",
        "serverless",
        "lightweight",
        "engine",
    ],
];

struct Session {
    id: &'static str,
    cwd: &'static str,
    age_days: i64,
    msgs: &'static [&'static str],
}

const CORPUS: &[Session] = &[
    Session {
        id: "s_webhook",
        cwd: "/proj/review-bot",
        age_days: 2,
        msgs: &[
            "We want a local review bot. Could the webhook receiver run on my laptop?",
            "Yes. Run the receiver on localhost and expose it through a tunnel such as ngrok.",
            "Verify every delivery with verify_signature() using the shared HMAC secret before parsing the payload.",
            "Reject requests whose X-Hub-Signature-256 header does not match, and answer 401.",
            "Queue the pull_request events and reply to the GitHub App asynchronously.",
        ],
    },
    Session {
        id: "s_migrations",
        cwd: "/proj/billing",
        age_days: 5,
        msgs: &[
            "Plan the postgres migration that adds a column to the invoices table.",
            "Use ALTER TABLE invoices ADD COLUMN tax_code text; and set lock_timeout to 2s so deploys do not block.",
            "If the deploy fails we can roll the migration back with the down script, then restore from the nightly dump.",
            "Backfill the new column in batches of 10000 rows outside the migration transaction.",
        ],
    },
    Session {
        id: "s_flaky",
        cwd: "/proj/billing",
        age_days: 9,
        msgs: &[
            "test_checkout_flow is flaky on CI, it fails roughly one run in twenty.",
            "The race is between the payment stub and the order writer; both touch order_state without a lock.",
            "Seed the random generator and add a retry with jitter only around the network call, never around assertions.",
            "After fixing the ordering the flaky failures disappeared for two weeks.",
        ],
    },
    Session {
        id: "s_react",
        cwd: "/proj/web",
        age_days: 12,
        msgs: &[
            "Our React tree re-renders too often when the shared context value changes.",
            "Split the context in two and move the heavy state into a reducer with useReducer.",
            "Wrap the list rows in React.memo and pass stable callbacks with useCallback.",
            "Selectors from a store library avoid re-rendering components that read unrelated slices.",
        ],
    },
    Session {
        id: "s_rust",
        cwd: "/proj/engine",
        age_days: 3,
        msgs: &[
            "The compiler reports error E0502: cannot borrow `buf` as mutable because it is also borrowed as immutable.",
            "Shorten the immutable borrow by cloning the slice length first, then take the mutable reference.",
            "A struct holding a &'a str needs an explicit lifetime parameter; for a global cache use &'static str or Arc<str>.",
            "Share the cache across tasks with Arc<Mutex<HashMap<..>>> and keep the guard scope tiny.",
        ],
    },
    Session {
        id: "s_k8s",
        cwd: "/proj/infra",
        age_days: 7,
        msgs: &[
            "The kubernetes deployment rollout stalls because the readiness probe never succeeds.",
            "Point the probe at /healthz on port 8080 and give it initialDelaySeconds 10.",
            "Template the image tag in the helm values file and run helm upgrade --install with --atomic.",
            "To go back to the previous version use kubectl rollout undo deployment/api.",
        ],
    },
    Session {
        id: "s_vectors",
        cwd: "/proj/search",
        age_days: 1,
        msgs: &[
            "Which embedding dimension should we store, 768 or 3072?",
            "Cosine similarity on unit vectors is just a dot product, so normalize at write time.",
            "Exact scan over fifteen thousand vectors is fast enough; an ANN index is premature.",
            "Quantizing float32 to int8 would cut memory four times but changes ranking slightly.",
        ],
    },
    Session {
        id: "s_css",
        cwd: "/proj/web",
        age_days: 20,
        msgs: &[
            "Make the dashboard responsive: use display: grid with grid-template-columns repeat(auto-fit, minmax(240px, 1fr)).",
            "Flexbox is better for the toolbar; set gap and flex-wrap instead of margins.",
            "Add breakpoints at 640px and 1024px and test with the device emulator.",
        ],
    },
    Session {
        id: "s_oauth",
        cwd: "/proj/billing",
        age_days: 15,
        msgs: &[
            "Implement OAuth login with PKCE for the single page app.",
            "Refresh the access token a minute before expiry using the refresh token stored in an httpOnly cookie.",
            "Protect the session cookie with SameSite=Lax and add a CSRF token to state-changing requests.",
        ],
    },
    Session {
        id: "s_perf",
        cwd: "/proj/search",
        age_days: 4,
        msgs: &[
            "The search endpoint is slow under load, let's capture a CPU flamegraph with perf.",
            "The hotspot is a Vec<String> allocation per candidate; reuse a scratch buffer instead.",
            "Sorting all scores is O(n log n); a binary heap of size K brings it down and improves cache locality.",
        ],
    },
    Session {
        id: "s_legacy",
        cwd: "/proj/legacy",
        age_days: 60,
        msgs: &[
            "The recipe site still uses jQuery 1.12 with inline scripts.",
            "Migrate the form handlers to ES modules one page at a time and delete the jQuery plugin shims.",
            "Keep the old $.ajax wrapper behind a feature flag until the checkout page is converted.",
        ],
    },
    Session {
        id: "s_logging",
        cwd: "/proj/infra",
        age_days: 6,
        msgs: &[
            "We need structured logging with request ids and log levels per module.",
            "Redact secrets such as authorization headers before the log line is written.",
            "Export OpenTelemetry traces to the collector and sample 10 percent of requests.",
        ],
    },
    Session {
        id: "s_git",
        cwd: "/proj/web",
        age_days: 25,
        msgs: &[
            "Should we rebase or merge the long-running feature branch?",
            "Use git rebase -i to squash fixups, and resolve each conflict once instead of repeatedly.",
            "To find the commit that broke the build run git bisect start, then git bisect run ./ci.sh.",
        ],
    },
];

/// Branching session: one shared prefix, two persisted branches with different storage decisions.
const FORK_ID: &str = "s_fork";

struct Q {
    cat: &'static str,
    q: &'static str,
    mode_filters: Value,
    relevant: &'static [&'static str],
}

fn queries() -> Vec<Q> {
    let f = |v: Value| v;
    let n = json!({});
    vec![
        // paraphrases: no keyword overlap with the supporting text (lexical is expected to miss)
        Q {
            cat: "paraphrase",
            q: "incoming notification endpoint hosted on my own machine",
            mode_filters: n.clone(),
            relevant: &["s_webhook"],
        },
        Q {
            cat: "paraphrase",
            q: "authenticity checksum for delivered messages",
            mode_filters: n.clone(),
            relevant: &["s_webhook"],
        },
        Q {
            cat: "paraphrase",
            q: "revert the database schema change",
            mode_filters: n.clone(),
            relevant: &["s_migrations"],
        },
        Q {
            cat: "paraphrase",
            q: "intermittent unreliable checks in the pipeline",
            mode_filters: n.clone(),
            relevant: &["s_flaky"],
        },
        Q {
            cat: "paraphrase",
            q: "stop wasteful redraws with redux",
            mode_filters: n.clone(),
            relevant: &["s_react"],
        },
        Q {
            cat: "paraphrase",
            q: "mutability aliasing complaint from the checker",
            mode_filters: n.clone(),
            relevant: &["s_rust"],
        },
        Q {
            cat: "paraphrase",
            q: "cluster stuck, revert to the earlier release",
            mode_filters: n.clone(),
            relevant: &["s_k8s"],
        },
        Q {
            cat: "paraphrase",
            q: "nearest neighbour lookup",
            mode_filters: n.clone(),
            relevant: &["s_vectors"],
        },
        Q {
            cat: "paraphrase",
            q: "adaptive arrangement for small screens",
            mode_filters: n.clone(),
            relevant: &["s_css"],
        },
        Q {
            cat: "paraphrase",
            q: "signin authentication renewal strategy",
            mode_filters: n.clone(),
            relevant: &["s_oauth"],
        },
        Q {
            cat: "paraphrase",
            q: "latency bottleneck diagnosis",
            mode_filters: n.clone(),
            relevant: &["s_perf"],
        },
        Q {
            cat: "paraphrase",
            q: "observability and diagnostics setup",
            mode_filters: n.clone(),
            relevant: &["s_logging"],
        },
        Q {
            cat: "paraphrase",
            q: "outdated obsolete library upgrade plan",
            mode_filters: n.clone(),
            relevant: &["s_legacy"],
        },
        Q {
            cat: "paraphrase",
            q: "integrate work and track down the culprit regression",
            mode_filters: n.clone(),
            relevant: &["s_git"],
        },
        Q {
            cat: "paraphrase",
            q: "lightweight serverless storage engine",
            mode_filters: n.clone(),
            relevant: &[FORK_ID],
        },
        // exact symbols / error text
        Q {
            cat: "symbol",
            q: "verify_signature()",
            mode_filters: n.clone(),
            relevant: &["s_webhook"],
        },
        Q {
            cat: "symbol",
            q: "E0502",
            mode_filters: n.clone(),
            relevant: &["s_rust"],
        },
        Q {
            cat: "symbol",
            q: "test_checkout_flow",
            mode_filters: n.clone(),
            relevant: &["s_flaky"],
        },
        Q {
            cat: "symbol",
            q: "kubectl rollout undo",
            mode_filters: n.clone(),
            relevant: &["s_k8s"],
        },
        Q {
            cat: "symbol",
            q: "ALTER TABLE invoices ADD COLUMN",
            mode_filters: n.clone(),
            relevant: &["s_migrations"],
        },
        Q {
            cat: "symbol",
            q: "Arc<Mutex<HashMap<..>>>",
            mode_filters: n.clone(),
            relevant: &["s_rust"],
        },
        Q {
            cat: "symbol",
            q: "X-Hub-Signature-256",
            mode_filters: n.clone(),
            relevant: &["s_webhook"],
        },
        Q {
            cat: "symbol",
            q: "git bisect run ./ci.sh",
            mode_filters: n.clone(),
            relevant: &["s_git"],
        },
        Q {
            cat: "symbol",
            q: "grid-template-columns repeat(auto-fit, minmax(240px, 1fr))",
            mode_filters: n.clone(),
            relevant: &["s_css"],
        },
        Q {
            cat: "symbol",
            q: "lock_timeout",
            mode_filters: n.clone(),
            relevant: &["s_migrations"],
        },
        // project-filtered
        Q {
            cat: "project",
            q: "state management and rendering",
            mode_filters: f(json!({"project_path":"/proj/web"})),
            relevant: &["s_react"],
        },
        Q {
            cat: "project",
            q: "token refresh",
            mode_filters: f(json!({"project_path":"/proj/billing"})),
            relevant: &["s_oauth"],
        },
        Q {
            cat: "project",
            q: "slow search under load",
            mode_filters: f(json!({"project_path":"/proj/search"})),
            relevant: &["s_perf"],
        },
        Q {
            cat: "project",
            q: "rollback",
            mode_filters: f(json!({"project_path":"/proj/billing"})),
            relevant: &["s_migrations"],
        },
        Q {
            cat: "project",
            q: "probe rollout",
            mode_filters: f(json!({"project_path":"/proj/infra"})),
            relevant: &["s_k8s"],
        },
        // old reopened session (older than the default bootstrap window but indexed)
        Q {
            cat: "old",
            q: "jQuery plugin shims",
            mode_filters: n.clone(),
            relevant: &["s_legacy"],
        },
        Q {
            cat: "old",
            q: "ES modules one page at a time",
            mode_filters: n.clone(),
            relevant: &["s_legacy"],
        },
        Q {
            cat: "old",
            q: "recipe site inline scripts",
            mode_filters: n.clone(),
            relevant: &["s_legacy"],
        },
        Q {
            cat: "old",
            q: "$.ajax wrapper feature flag",
            mode_filters: n.clone(),
            relevant: &["s_legacy"],
        },
        Q {
            cat: "old",
            q: "rebase interactive squash fixups",
            mode_filters: n.clone(),
            relevant: &["s_git"],
        },
        // branch-specific
        Q {
            cat: "branch",
            q: "sqlite storage decision",
            mode_filters: n.clone(),
            relevant: &[FORK_ID],
        },
        Q {
            cat: "branch",
            q: "postgres storage decision",
            mode_filters: n.clone(),
            relevant: &[FORK_ID],
        },
        Q {
            cat: "branch",
            q: "single file database simple deployment",
            mode_filters: n.clone(),
            relevant: &[FORK_ID],
        },
        Q {
            cat: "branch",
            q: "managed server replicas",
            mode_filters: n.clone(),
            relevant: &[FORK_ID],
        },
        Q {
            cat: "branch",
            q: "PRAGMA journal_mode",
            mode_filters: n.clone(),
            relevant: &[FORK_ID],
        },
        // related topics across sessions (any of the listed sessions is relevant)
        Q {
            cat: "related",
            q: "performance of vector search",
            mode_filters: n.clone(),
            relevant: &["s_vectors", "s_perf"],
        },
        Q {
            cat: "related",
            q: "database change safety",
            mode_filters: n.clone(),
            relevant: &["s_migrations", FORK_ID],
        },
        Q {
            cat: "related",
            q: "deployment problems",
            mode_filters: n.clone(),
            relevant: &["s_k8s", "s_migrations"],
        },
        Q {
            cat: "related",
            q: "secrets and credentials handling",
            mode_filters: n.clone(),
            relevant: &["s_logging", "s_oauth", "s_webhook"],
        },
        Q {
            cat: "related",
            q: "intermittent failures and races",
            mode_filters: n.clone(),
            relevant: &["s_flaky", "s_rust"],
        },
        // negative: nothing in the corpus supports these (lexical/literal must return nothing)
        Q {
            cat: "negative",
            q: "zxqvbn plokmi",
            mode_filters: n.clone(),
            relevant: &[],
        },
        Q {
            cat: "negative",
            q: "quokkaforge",
            mode_filters: n.clone(),
            relevant: &[],
        },
        Q {
            cat: "negative",
            q: "frobnicator_9000",
            mode_filters: n.clone(),
            relevant: &[],
        },
        Q {
            cat: "negative",
            q: "wibblewobble splorch",
            mode_filters: n.clone(),
            relevant: &[],
        },
        Q {
            cat: "negative",
            q: "xyzzy_plugh",
            mode_filters: n,
            relevant: &[],
        },
    ]
}

const STOP: &[&str] = &[
    "a", "an", "the", "and", "or", "of", "on", "in", "to", "for", "with", "is", "are", "my", "our",
    "from", "why", "which", "how", "what", "that", "this", "it", "as", "at", "by",
];

fn words(s: &str) -> std::collections::BTreeSet<String> {
    s.split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() > 1)
        .map(|w| w.to_lowercase())
        .filter(|w| !STOP.contains(&w.as_str()))
        .collect()
}

/// A paraphrase query must share NO content word with the text of its relevant session(s): the category
/// is defined as "lexical retrieval has nothing to match".
fn paraphrase_overlap(q: &Q) -> Vec<String> {
    let qw = words(q.q);
    let mut texts: Vec<String> = CORPUS
        .iter()
        .filter(|s| q.relevant.contains(&s.id))
        .flat_map(|s| s.msgs.iter().map(|m| m.to_string()))
        .collect();
    if q.relevant.contains(&FORK_ID) {
        texts.push("Which embedded file database should the review bot use for its queue? Branch A: choose sqlite, a single file with PRAGMA journal_mode=WAL keeps deployment simple. With sqlite we only need one writer and backups are just a file copy after a checkpoint. Branch B: choose postgres on a managed server so replicas and failover come for free. With postgres we can add read replicas and use the managed backup service.".into());
    }
    let dw = words(&texts.join(" "));
    qw.intersection(&dw).cloned().collect()
}

async fn build() -> Env {
    let tmp = tempfile::tempdir().unwrap();
    let index = Index::open_for_test(tmp.path()).await.unwrap();
    let mut cfg = Config::default();
    cfg.embedding.enabled = true;
    cfg.embedding.dimensions = 64;
    let cfg = Arc::new(cfg);
    let clock = ManualClock::new(T0 + 100 * DAY);
    let shared: SharedClock = Arc::new(clock.clone());
    let provider = Arc::new(FakeProvider::with_concepts(64, GROUPS));
    let emb = Arc::new(EmbeddingService::new(
        index.clone(),
        cfg.embedding.clone(),
        cfg.search.clone(),
        shared.clone(),
        provider.clone(),
    ));
    let profile_id = emb.ensure_profile().await.unwrap();
    let key_path = tmp.path().join("handle.key");
    HandleKey::create(&key_path).unwrap();
    let keys = KeyStore::load(&key_path).unwrap();
    let leases = LeaseManager::new(index.clone(), cfg.reader.clone(), shared);
    let svc = SearchService::new(
        index.clone(),
        cfg.clone(),
        emb.clone(),
        leases.clone(),
        keys.clone(),
    );
    let env = Env {
        tmp,
        index,
        provider,
        emb,
        svc,
        leases,
        keys,
        clock,
        cfg,
        profile_id,
    };
    sqlx::query("INSERT INTO sources(id, kind, roots_json, enabled, last_scan_at, last_scan_ok) VALUES ('omp','omp','[]',1,?,1)").bind(T0).execute(&env.index.writer).await.unwrap();
    let now = T0 + 100 * DAY;
    for s in CORPUS {
        let last = now - s.age_days * DAY;
        let texts: Vec<(&str, Option<i64>)> = s
            .msgs
            .iter()
            .enumerate()
            .map(|(i, m)| (*m, Some(last - (s.msgs.len() - i) as i64 * 60_000)))
            .collect();
        let seed = search_common::Seed {
            cwd: s.cwd,
            last_activity: last,
            embed: true,
            ..search_common::Seed::default()
        };
        env.simple_with(s.id, &seed, &texts).await;
    }
    // fork: m0 (root) -> a1,a2 (sqlite branch) and b1,b2 (postgres branch)
    let last = now - 8 * DAY;
    env.session(FORK_ID, "omp", "/proj/review-bot", "interactive", last)
        .await;
    let snap = format!("sn_{FORK_ID}");
    env.snapshot(FORK_ID, &snap, "published").await;
    let nodes: &[(&str, Option<&str>, &str)] = &[
        (
            "m0",
            None,
            "Which embedded file database should the review bot use for its queue?",
        ),
        (
            "a1",
            Some("m0"),
            "Branch A: choose sqlite, a single file with PRAGMA journal_mode=WAL keeps deployment simple.",
        ),
        (
            "a2",
            Some("a1"),
            "With sqlite we only need one writer and backups are just a file copy after a checkpoint.",
        ),
        (
            "b1",
            Some("m0"),
            "Branch B: choose postgres on a managed server so replicas and failover come for free.",
        ),
        (
            "b2",
            Some("b1"),
            "With postgres we can add read replicas and use the managed backup service.",
        ),
    ];
    for (i, (key, parent, text)) in nodes.iter().enumerate() {
        env.message(
            &snap,
            Message {
                key,
                seq: i as i64,
                role: "user",
                ts: Some(last + i as i64 * 1000),
                parent: *parent,
                text,
            },
        )
        .await;
    }
    // runs: shared prefix chunk, then one chunk per branch (context from the shared ancestor only)
    let runs: &[(&str, &[&str])] = &[
        ("run_root", &["m0"]),
        ("run_a", &["a1", "a2"]),
        ("run_b", &["b1", "b2"]),
    ];
    for (ord, (run, keys)) in runs.iter().enumerate() {
        let mut body = String::new();
        let mut spans = Vec::new();
        for k in *keys {
            let t = nodes.iter().find(|n| n.0 == *k).unwrap().2;
            body.push_str(&format!("USER: {t}\n\n"));
            spans.push((*k, 0i64, 0i64, t.len() as i64, false));
        }
        env.chunk(
            FORK_ID,
            &snap,
            &format!("{FORK_ID}-{run}"),
            Chunk {
                body: body.trim_end(),
                spans,
                anchor: Some(last + 4000),
                run,
                ordinal: ord as i64,
                embed: true,
            },
        )
        .await;
    }
    env.activate(FORK_ID, &snap).await;
    env
}

struct Score {
    hits5: usize,
    hits10: usize,
    rr: f64,
    n: usize,
    empty_ok: usize,
    n_neg: usize,
}

fn rank_of(v: &Value, relevant: &[&str]) -> Option<usize> {
    ids(v)
        .iter()
        .position(|id| relevant.contains(&id.as_str()))
        .map(|p| p + 1)
}

#[tokio::test]
async fn curated_retrieval_evaluation() {
    let env = build().await;
    let qs = queries();
    assert_eq!(qs.len(), 50, "the evaluation set has exactly 50 queries");
    let bad: Vec<_> = qs
        .iter()
        .filter(|q| q.cat == "paraphrase")
        .map(|q| (q.q, paraphrase_overlap(q)))
        .filter(|(_, o)| !o.is_empty())
        .collect();
    assert!(
        bad.is_empty(),
        "paraphrase queries must not share content words with the relevant text: {bad:?}"
    );
    let mut report = String::new();
    let mut by_mode: Vec<(&str, Score)> = Vec::new();
    for mode in ["lexical", "semantic", "hybrid"] {
        let mut sc = Score {
            hits5: 0,
            hits10: 0,
            rr: 0.0,
            n: 0,
            empty_ok: 0,
            n_neg: 0,
        };
        let mut cat: std::collections::BTreeMap<&str, (usize, usize)> = Default::default();
        for q in &qs {
            let mut args = q.mode_filters.clone();
            args["query"] = json!(q.q);
            args["mode"] = json!(mode);
            args["limit"] = json!(10);
            let r = match env.search(args).await {
                Ok(v) => v,
                Err(e) => panic!("{mode} {:?}: {e}", q.q),
            };
            if q.relevant.is_empty() {
                sc.n_neg += 1;
                if ids(&r).is_empty() {
                    sc.empty_ok += 1;
                }
                continue;
            }
            sc.n += 1;
            let e = cat.entry(q.cat).or_default();
            e.1 += 1;
            if let Some(rank) = rank_of(&r, q.relevant) {
                sc.rr += 1.0 / rank as f64;
                if rank <= 5 {
                    sc.hits5 += 1;
                    e.0 += 1;
                }
                if rank <= 10 {
                    sc.hits10 += 1;
                }
            }
        }
        report.push_str(&format!("\n{mode}: per-category Recall@5 {:?}", cat));
        by_mode.push((mode, sc));
    }

    // literal correctness: every symbol query's literal text must be found (as an exact substring) in a relevant session
    let mut lit_ok = 0;
    let mut lit_n = 0;
    for q in qs.iter().filter(|q| q.cat == "symbol") {
        lit_n += 1;
        let r = env
            .ok(json!({"query": q.q, "mode": "literal", "limit": 10}))
            .await;
        if rank_of(&r, q.relevant).is_some() {
            lit_ok += 1;
        }
    }
    // branch hygiene: a branch-specific result excerpt must not mix the sibling branch
    let a = env.ok(json!({"query": "sqlite storage decision", "mode": "lexical", "session_ids": [FORK_ID]})).await;
    let b = env.ok(json!({"query": "postgres storage decision", "mode": "lexical", "session_ids": [FORK_ID]})).await;
    let ex = |v: &Value| {
        v["results"][0]["matches"][0]["excerpt"]
            .as_str()
            .unwrap_or("")
            .to_string()
    };
    let (ea, eb) = (ex(&a), ex(&b));
    let branch_clean = !(ea.contains("postgres") && ea.contains("sqlite"))
        && !(eb.contains("postgres") && eb.contains("sqlite"));

    println!(
        "\n=== retrieval evaluation: 50 queries ({} judged, {} negative) ===",
        by_mode[0].1.n, by_mode[0].1.n_neg
    );
    println!("| mode | Recall@5 | Recall@10 | MRR | negative queries returning nothing |");
    println!("|---|---|---|---|---|");
    for (m, s) in &by_mode {
        println!(
            "| {m} | {:.3} ({}/{}) | {:.3} ({}/{}) | {:.3} | {}/{} |",
            s.hits5 as f64 / s.n as f64,
            s.hits5,
            s.n,
            s.hits10 as f64 / s.n as f64,
            s.hits10,
            s.n,
            s.rr / s.n as f64,
            s.empty_ok,
            s.n_neg
        );
    }
    println!("literal exact retrieval on {lit_n} symbol queries: {lit_ok}/{lit_n}");
    println!("branch excerpts free of sibling-branch text: {branch_clean}{report}");

    assert_eq!(
        lit_ok, lit_n,
        "literal retrieval must be exact whenever the in-scope span contains the literal"
    );
    assert!(branch_clean);
    let lex = &by_mode[0].1;
    let hyb = &by_mode[2].1;
    assert_eq!(
        lex.empty_ok, lex.n_neg,
        "lexical returns nothing for tokens absent from the corpus"
    );
    assert!(
        hyb.hits5 >= lex.hits5,
        "hybrid must not be worse than lexical on this set"
    );
}
