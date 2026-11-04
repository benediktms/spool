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
use spool::config::{Config, Paths};
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
            mode_filters: f(json!({"projects":["/proj/web"]})),
            relevant: &["s_react"],
        },
        Q {
            cat: "project",
            q: "token refresh",
            mode_filters: f(json!({"projects":["/proj/billing"]})),
            relevant: &["s_oauth"],
        },
        Q {
            cat: "project",
            q: "slow search under load",
            mode_filters: f(json!({"projects":["/proj/search"]})),
            relevant: &["s_perf"],
        },
        Q {
            cat: "project",
            q: "rollback",
            mode_filters: f(json!({"projects":["/proj/billing"]})),
            relevant: &["s_migrations"],
        },
        Q {
            cat: "project",
            q: "probe rollout",
            mode_filters: f(json!({"projects":["/proj/infra"]})),
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
            relevant: &["s_migrations"],
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
            relevant: &["s_flaky"],
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

async fn build(real: Option<Config>) -> Env {
    let tmp = tempfile::tempdir().unwrap();
    let index = Index::open_for_test(tmp.path()).await.unwrap();
    let hosted = real.is_some();
    let mut cfg = real.unwrap_or_default();
    if !hosted {
        cfg.embedding.enabled = true;
        cfg.embedding.dimensions = 64;
    }
    let cfg = Arc::new(cfg);
    let clock = ManualClock::new(T0 + 100 * DAY);
    let shared: SharedClock = Arc::new(clock.clone());
    let provider = Arc::new(FakeProvider::with_concepts(64, GROUPS));
    let emb = Arc::new(if hosted {
        let paths = Paths::from_root(tmp.path().to_path_buf()).unwrap();
        EmbeddingService::from_config(index.clone(), &cfg, &paths, shared.clone()).unwrap()
    } else {
        EmbeddingService::new(
            index.clone(),
            cfg.embedding.clone(),
            cfg.search.clone(),
            shared.clone(),
            provider.clone(),
        )
    });
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
            embed: !hosted,
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
                embed: !hosted,
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
    let env = build(None).await;
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
        report.push_str(&format!("\n{mode}: per-category Hit@5 {:?}", cat));
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
    println!("| mode | Hit@5 | Hit@10 | MRR | negative queries returning nothing |");
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

const ANCHORS: &[&[(&str, &str, &str)]] = &[
    &[("s_webhook", "m0", "webhook receiver")],
    &[("s_webhook", "m2", "HMAC secret")],
    &[("s_migrations", "m2", "roll the migration back")],
    &[("s_flaky", "m0", "flaky on CI")],
    &[("s_react", "m1", "reducer")],
    &[("s_rust", "m0", "cannot borrow")],
    &[("s_k8s", "m3", "rollout undo")],
    &[("s_vectors", "m2", "Exact scan")],
    &[("s_css", "m0", "responsive")],
    &[("s_oauth", "m1", "Refresh the access token")],
    &[("s_perf", "m0", "slow under load")],
    &[("s_logging", "m0", "structured logging")],
    &[("s_legacy", "m1", "Migrate the form handlers")],
    &[("s_git", "m2", "git bisect")],
    &[(FORK_ID, "a1", "sqlite, a single file")],
    &[("s_webhook", "m2", "verify_signature()")],
    &[("s_rust", "m0", "E0502")],
    &[("s_flaky", "m0", "test_checkout_flow")],
    &[("s_k8s", "m3", "kubectl rollout undo")],
    &[("s_migrations", "m1", "ALTER TABLE invoices ADD COLUMN")],
    &[("s_rust", "m3", "Arc<Mutex<HashMap<..>>>")],
    &[("s_webhook", "m3", "X-Hub-Signature-256")],
    &[("s_git", "m2", "git bisect run ./ci.sh")],
    &[(
        "s_css",
        "m0",
        "grid-template-columns repeat(auto-fit, minmax(240px, 1fr))",
    )],
    &[("s_migrations", "m1", "lock_timeout")],
    &[("s_react", "m1", "state into a reducer")],
    &[("s_oauth", "m1", "Refresh the access token")],
    &[("s_perf", "m0", "search endpoint is slow")],
    &[("s_migrations", "m2", "roll the migration back")],
    &[("s_k8s", "m0", "readiness probe")],
    &[("s_legacy", "m1", "jQuery plugin shims")],
    &[("s_legacy", "m1", "ES modules one page at a time")],
    &[("s_legacy", "m0", "recipe site")],
    &[("s_legacy", "m2", "$.ajax wrapper")],
    &[("s_git", "m1", "rebase -i to squash fixups")],
    &[(FORK_ID, "a1", "choose sqlite")],
    &[(FORK_ID, "b1", "choose postgres")],
    &[(FORK_ID, "a1", "single file")],
    &[(FORK_ID, "b1", "managed server")],
    &[(FORK_ID, "a1", "PRAGMA journal_mode")],
    &[
        ("s_vectors", "m2", "Exact scan"),
        ("s_perf", "m0", "search endpoint is slow"),
    ],
    &[("s_migrations", "m1", "lock_timeout")],
    &[
        ("s_k8s", "m0", "deployment rollout stalls"),
        ("s_migrations", "m2", "deploy fails"),
    ],
    &[
        ("s_logging", "m1", "Redact secrets"),
        ("s_oauth", "m1", "refresh token"),
        ("s_webhook", "m2", "HMAC secret"),
    ],
    &[("s_flaky", "m1", "The race")],
    &[],
    &[],
    &[],
    &[],
    &[],
];

#[tokio::test]
async fn supporting_anchors_resolve() {
    let env = build(None).await;
    let qs = queries();
    assert_eq!(ANCHORS.len(), qs.len());
    for (q, anchors) in qs.iter().zip(ANCHORS) {
        assert_eq!(anchors.len(), q.relevant.len());
        for (session, key, text) in *anchors {
            assert!(q.relevant.contains(session));
            let body: String = sqlx::query_scalar("SELECT b.text FROM blocks b JOIN snapshots s ON s.id=b.snapshot_id WHERE s.session_id=? AND b.record_key=?")
                .bind(session).bind(key).fetch_one(&env.index.reader).await.unwrap();
            assert!(body.contains(text), "supporting anchor missing");
        }
    }
}

fn eval_config(path: &std::path::Path) -> Result<Config, &'static str> {
    if !path.is_absolute() {
        return Err("configuration path must be absolute");
    }
    let text = std::fs::read_to_string(path).map_err(|_| "configuration unavailable")?;
    let base = path.parent().ok_or("configuration unavailable")?;
    let mut cfg = Config::parse(&text, base).map_err(|_| "invalid configuration")?;
    if !cfg.embedding.enabled || !cfg.embedding.hosted_processing_approved {
        return Err("hosted processing is not enabled and approved");
    }
    cfg.embedding.credential_file = cfg
        .credential_path(base)
        .map_err(|_| "invalid credential path")?
        .to_string_lossy()
        .into_owned();
    cfg.embedding.daily_ingestion_token_budget =
        cfg.embedding.daily_ingestion_token_budget.min(10_000);
    cfg.embedding.daily_query_token_budget = cfg.embedding.daily_query_token_budget.min(5_000);
    cfg.search = Config::default().search;
    Ok(cfg)
}

fn complete_result(result: &Value, mode: &str) -> bool {
    result["mode_used"] == mode
        && result["truncated"] == false
        && result["coverage"].is_null()
        && result["warnings"].as_array().is_some_and(Vec::is_empty)
}

fn complete_coverage(coverage: &spool::embeddings::VectorCoverage, profile: &str) -> bool {
    coverage.total_chunks > 0
        && coverage.pending_chunks == 0
        && coverage.embedded_chunks == coverage.total_chunks
        && coverage.profile_id == profile
}

fn sufficient_budget(ingest: usize, query: usize, cfg: &Config) -> bool {
    u64::try_from(ingest).is_ok_and(|n| n <= cfg.embedding.daily_ingestion_token_budget)
        && u64::try_from(query).is_ok_and(|n| n <= cfg.embedding.daily_query_token_budget)
}

#[derive(Default)]
struct Metrics {
    n: usize,
    hit5: usize,
    hit10: usize,
    reciprocal: f64,
}
impl Metrics {
    fn add(&mut self, rank: Option<usize>) {
        self.n += 1;
        if let Some(rank) = rank.filter(|rank| *rank <= 10) {
            self.hit10 += 1;
            self.hit5 += usize::from(rank <= 5);
            self.reciprocal += 1.0 / rank as f64;
        }
    }
    fn json(&self) -> Value {
        json!({"denominator": self.n, "hit_at_5": self.hit5 as f64 / self.n as f64,
            "hit_at_10": self.hit10 as f64 / self.n as f64, "mrr_at_10": self.reciprocal / self.n as f64})
    }
}

async fn hosted_evaluation(cfg: Config) -> Result<Value, &'static str> {
    use spool::embeddings::EmbeddingState;
    use std::sync::atomic::AtomicBool;
    let started = std::time::Instant::now();
    let env = build(Some(cfg)).await;
    if env.emb.status().state != EmbeddingState::Ready {
        return Err("provider gate closed");
    }
    let rows: Vec<String> = sqlx::query_scalar("SELECT body FROM chunks ORDER BY rowid")
        .fetch_all(&env.index.reader)
        .await
        .map_err(|_| "fixture read failed")?;
    let qs = queries();
    let ingest = rows.iter().map(|s| env.emb.count_tokens(s)).sum();
    let query = qs.iter().map(|q| env.emb.count_tokens(q.q) * 2).sum();
    if !sufficient_budget(ingest, query, &env.cfg) {
        return Err("preflight budget insufficient");
    }
    let docs = rows
        .into_iter()
        .map(|s| (spool::domain::digest(s.as_bytes()), s))
        .collect();
    let progress = env
        .emb
        .embed_documents(docs, &AtomicBool::new(false))
        .await
        .map_err(|e| {
            if e.kind == spool::embeddings::EmbedErrorKind::Auth {
                "provider authentication failed"
            } else {
                "document embedding failed"
            }
        })?;
    if progress.rejected > 0
        || progress.deferred > 0
        || progress.cancelled
        || progress.budget_exhausted
    {
        return Err("document embedding incomplete");
    }
    let coverage = env
        .emb
        .vector_coverage()
        .await
        .map_err(|_| "coverage check failed")?;
    if !complete_coverage(&coverage, &env.profile_id) {
        return Err("document coverage incomplete");
    }
    let mut modes = serde_json::Map::new();
    for mode in ["lexical", "semantic", "hybrid"] {
        let mut total = Metrics::default();
        let mut categories: std::collections::BTreeMap<&str, Metrics> = Default::default();
        let mut ranks = Vec::new();
        let mut negative_empty = 0;
        for (i, q) in qs.iter().enumerate() {
            let mut args = q.mode_filters.clone();
            args["query"] = json!(q.q);
            args["mode"] = json!(mode);
            args["limit"] = json!(10);
            let result = env.search(args).await.map_err(|_| "search failed")?;
            if !complete_result(&result, mode) {
                return Err("search result incomplete or fallback");
            }
            let rank = rank_of(&result, q.relevant);
            if q.relevant.is_empty() {
                negative_empty += usize::from(ids(&result).is_empty());
            } else {
                total.add(rank);
                categories.entry(q.cat).or_default().add(rank);
            }
            let results = ids(&result);
            let relevant_ranks: Vec<_> = q.relevant.iter().map(|id| json!({"session_id": id, "rank": results.iter().position(|result| result == id).map(|i| i + 1)})).collect();
            ranks.push(
                json!({"query_id": format!("q{:02}", i + 1), "relevant_ranks": relevant_ranks}),
            );
        }
        let categories: serde_json::Map<String, Value> = categories
            .into_iter()
            .map(|(k, v)| (k.to_owned(), v.json()))
            .collect();
        modes.insert(mode.into(), json!({"aggregate": total.json(), "categories": categories, "negative_empty": negative_empty, "negative_denominator": 5, "queries": ranks}));
    }
    for q in qs.iter().filter(|q| q.cat == "symbol") {
        let result = env
            .search(json!({"query": q.q, "mode": "literal", "limit": 10}))
            .await
            .map_err(|_| "literal search failed")?;
        if !complete_result(&result, "literal") || rank_of(&result, q.relevant).is_none() {
            return Err("literal correctness failed");
        }
    }
    for (query, forbidden) in [
        ("sqlite storage decision", "postgres"),
        ("postgres storage decision", "sqlite"),
    ] {
        let result = env
            .search(json!({"query": query, "mode": "lexical", "session_ids": [FORK_ID]}))
            .await
            .map_err(|_| "branch check failed")?;
        if !complete_result(&result, "lexical") {
            return Err("branch check incomplete");
        }
        let excerpt = result["results"][0]["matches"][0]["excerpt"]
            .as_str()
            .ok_or("branch result missing")?;
        if excerpt.contains(forbidden) {
            return Err("branch hygiene failed");
        }
    }
    let usage: Vec<(String, i64, i64, i64)> = sqlx::query_as("SELECT purpose, COALESCE(SUM(reserved_tokens),0), COALESCE(SUM(actual_tokens),0), COALESCE(SUM(CASE WHEN state != 'completed' THEN reserved_tokens ELSE 0 END),0) FROM embedding_requests GROUP BY purpose ORDER BY purpose")
        .fetch_all(&env.index.reader).await.map_err(|_| "usage read failed")?;
    let profile = env.emb.profile();
    Ok(
        json!({"schema_version": 1, "dataset": "spool-synthetic-retrieval-v2", "judgment_revision": "v2 removes s_fork from q42 and s_rust from q45; corpus and queries unchanged", "methodology": "real-model retrieval on synthetic sessions; incomplete relevance judgments; MRR truncated at 10", "profile": {"id": env.profile_id, "provider": profile.provider, "endpoint_identity": profile.endpoint_identity, "model": profile.model, "dimensions": profile.dimensions, "generation": profile.generation, "rendering_version": profile.rendering_version}, "settings": {"limit": 10, "candidate_limit": env.cfg.search.candidate_limit, "rrf_constant": env.cfg.search.rrf_constant, "ingest_budget": env.cfg.embedding.daily_ingestion_token_budget, "query_budget": env.cfg.embedding.daily_query_token_budget}, "preflight_estimates": {"ingest": ingest, "query": query}, "usage": usage.into_iter().map(|(purpose,reserved,reported,ambiguous)| json!({"purpose": purpose, "reserved": reserved, "completed_reported_or_estimated": reported, "ambiguous": ambiguous})).collect::<Vec<_>>(), "elapsed_ms": started.elapsed().as_millis(), "modes": modes}),
    )
}

#[tokio::test]
#[ignore = "explicitly consented real provider; spends tokens"]
async fn real_provider_retrieval_evaluation() {
    let path = std::env::var_os("SPOOL_RETRIEVAL_EVAL_CONFIG")
        .expect("explicit configuration path required");
    let cfg = eval_config(std::path::Path::new(&path)).expect("evaluation configuration rejected");
    let result = tokio::time::timeout(std::time::Duration::from_secs(600), hosted_evaluation(cfg))
        .await
        .expect("evaluation deadline exceeded")
        .expect("evaluation failed");
    println!("{}", serde_json::to_string(&result).unwrap());
}

#[test]
fn preflight_and_metrics_reject_incomplete_inputs() {
    assert!(eval_config(std::path::Path::new("relative.toml")).is_err());
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("config.toml");
    std::fs::write(&path, "[embedding]\nenabled = false\n").unwrap();
    assert!(eval_config(&path).is_err());
    std::fs::write(
        &path,
        "[embedding]\nenabled = true\nhosted_processing_approved = false\n",
    )
    .unwrap();
    assert!(eval_config(&path).is_err());
    let mut cfg = Config::default();
    cfg.embedding.daily_ingestion_token_budget = 4;
    cfg.embedding.daily_query_token_budget = 8;
    assert!(sufficient_budget(4, 8, &cfg));
    assert!(!sufficient_budget(5, 8, &cfg));
    assert!(!sufficient_budget(4, 9, &cfg));
    assert!(!complete_result(
        &json!({"mode_used":"lexical", "truncated":false, "warnings":[]}),
        "hybrid"
    ));
    assert!(!complete_result(
        &json!({"mode_used":"hybrid", "truncated":true, "warnings":[]}),
        "hybrid"
    ));
    let mut coverage = spool::embeddings::VectorCoverage {
        profile_id: "p".into(),
        total_chunks: 2,
        embedded_chunks: 1,
        pending_chunks: 1,
    };
    assert!(!complete_coverage(&coverage, "p"));
    coverage.embedded_chunks = 2;
    coverage.pending_chunks = 0;
    assert!(complete_coverage(&coverage, "p"));
    assert!(!complete_coverage(&coverage, "other"));
    let mut metrics = Metrics::default();
    for rank in [Some(1), Some(6), None, Some(11)] {
        metrics.add(rank);
    }
    assert_eq!(metrics.n, 4);
    assert_eq!(metrics.hit5, 1);
    assert_eq!(metrics.hit10, 2);
    assert!((metrics.reciprocal - 7.0 / 6.0).abs() < 1e-10);
    assert_eq!(
        rank_of(
            &json!({"results":[{"session_id":"x"},{"session_id":"b"}]}),
            &["a", "b"]
        ),
        Some(2)
    );
}

#[tokio::test]
async fn hosted_wiring_uses_only_synthetic_data_and_redacts_report() {
    use std::os::unix::fs::PermissionsExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut data = Vec::new();
                let mut buf = [0; 4096];
                let (header_end, len) = loop {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    data.extend_from_slice(&buf[..n]);
                    if let Some(i) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                        let end = i + 4;
                        let header = String::from_utf8_lossy(&data[..end]);
                        let len = header
                            .lines()
                            .find_map(|line| {
                                line.split_once(':')
                                    .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                                    .and_then(|(_, v)| v.trim().parse::<usize>().ok())
                            })
                            .unwrap();
                        break (end, len);
                    }
                };
                while data.len() < header_end + len {
                    let n = stream.read(&mut buf).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    data.extend_from_slice(&buf[..n]);
                }
                let request: Value =
                    serde_json::from_slice(&data[header_end..header_end + len]).unwrap();
                let inputs = request["input"].as_array().unwrap();
                let vectors: Vec<Value> = inputs
                    .iter()
                    .enumerate()
                    .map(|(index, _)| json!({"index": index, "embedding": [1.0, 0.0, 0.0, 0.0]}))
                    .collect();
                let body = json!({"data": vectors, "usage": {"total_tokens": 1}}).to_string();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            });
        }
    });
    let tmp = tempfile::tempdir().unwrap();
    let credential = tmp.path().join("credential");
    std::fs::write(&credential, "secret-sentinel").unwrap();
    std::fs::set_permissions(&credential, std::fs::Permissions::from_mode(0o600)).unwrap();
    let mut cfg = Config::default();
    cfg.embedding.hosted_processing_approved = true;
    cfg.embedding.dimensions = 4;
    cfg.embedding.endpoint = format!("http://{address}/v1/embeddings");
    cfg.embedding.credential_file = credential.to_string_lossy().into_owned();
    cfg.embedding.daily_ingestion_token_budget = 10_000;
    cfg.embedding.daily_query_token_budget = 5_000;
    let report = hosted_evaluation(cfg).await.expect("loopback evaluation");
    let json = report.to_string();
    for forbidden in [
        "secret-sentinel",
        "webhook receiver",
        "incoming notification endpoint",
        "/v1/embeddings",
        credential.to_str().unwrap(),
    ] {
        assert!(!json.contains(forbidden));
    }
    assert_eq!(report["modes"]["hybrid"]["aggregate"]["denominator"], 45);
    assert_eq!(report["modes"]["hybrid"]["negative_denominator"], 5);
    server.abort();
}

#[tokio::test]
async fn preflight_failures_send_no_requests() {
    use std::os::unix::fs::PermissionsExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let credential = tmp.path().join("key");
    std::fs::write(&credential, "synthetic-key").unwrap();
    std::fs::set_permissions(&credential, std::fs::Permissions::from_mode(0o600)).unwrap();
    let mut cfg = Config::default();
    cfg.embedding.hosted_processing_approved = true;
    cfg.embedding.endpoint = format!("http://{}/v1/embeddings", listener.local_addr().unwrap());
    cfg.embedding.credential_file = credential.to_string_lossy().into_owned();
    cfg.embedding.daily_ingestion_token_budget = 1;
    assert_eq!(
        hosted_evaluation(cfg.clone()).await.unwrap_err(),
        "preflight budget insufficient"
    );
    cfg.embedding.daily_ingestion_token_budget = 10_000;
    cfg.embedding.credential_file = tmp.path().join("missing").to_string_lossy().into_owned();
    assert_eq!(
        hosted_evaluation(cfg).await.unwrap_err(),
        "provider gate closed"
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
}
