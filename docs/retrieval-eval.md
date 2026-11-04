# Retrieval evaluation (spec 20.1)

**Scope of this document.** This is an evaluation of the *retrieval pipeline* on a small, fully synthetic,
manually authored corpus. The historical results below do **not** measure recall on a real user corpus or evaluate a real embedding model. Specifically:

* Corpus: 13 linear sessions plus one forked session (two persisted branches) across 7 project paths, 3 to 5
  user messages each, authored by hand in `tests/retrieval_eval.rs`. Ages range from 1 to 60 days.
* Queries: exactly 50, hand-written, with hand-annotated relevant sessions: 15 paraphrase, 10 exact symbol/error
  text, 5 project-filtered, 5 old reopened-session, 5 branch-specific, 5 related-topic (several relevant sessions),
  5 negative (nothing in the corpus supports them).
* Paraphrase queries are *enforced* to share no content word (stop words excluded) with their relevant text; the
  test fails otherwise, so lexical retrieval has nothing but incidental stop-word overlap to match.
* Semantic retrieval uses the deterministic concept-table `FakeProvider` (`src/embeddings/fake.rs`), whose
  concept groups were written by the same author as the queries (`GROUPS` in the test). Semantic and hybrid
  numbers therefore show that the *machinery* (exact scan, fusion, grouping, filters, branch handling) works when
  the vectors are good. They say nothing about how `text-embedding-3-small` ranks real conversations.
* The historical results exclude a judged live-provider evaluation and any
  evaluation on a real transcript corpus. The 90% Recall@5 goal of spec 20.1 is a goal for *curated real
  supported-corpus queries*; it is **not** claimed as met here.

Run the current v2 judgments with `cargo test --test retrieval_eval -- --nocapture` (about 1 s; asserts the evaluation set has 50
queries, paraphrase disjointness, literal exactness, branch hygiene, lexical silence on negatives, and hybrid >= lexical).

## Historical fake-provider v1 results (observed 2026-10-12, 45 judged queries + 5 negatives)

| mode | Hit@5 | Hit@10 | MRR | negative queries returning nothing |
|---|---|---|---|---|
| lexical | 0.822 (37/45) | 0.844 (38/45) | 0.734 | 5/5 |
| semantic (fake concepts) | 0.933 (42/45) | 1.000 (45/45) | 0.876 | 0/5 (nearest neighbour is returned by design: no similarity cutoff, spec 13.6) |
| hybrid (RRF k=60) | 0.956 (43/45) | 1.000 (45/45) | 0.913 | 0/5 (as semantic) |

Literal mode: exact retrieval on all 10 symbol/error-text queries (10/10), including `Arc<Mutex<HashMap<..>>>`,
`verify_signature()`, `X-Hub-Signature-256` and a CSS function with commas and parentheses.

Hit@k is the fraction of judged queries with at least one relevant session in the top k results.
Recall@k measures the fraction of all relevant sessions retrieved, so these hit rates do not establish
the spec’s Recall@5 goal when a query has multiple relevant sessions.

Per-category Hit@5 (hits/queries):

| category | lexical | semantic | hybrid |
|---|---|---|---|
| paraphrase (15) | 8 | 15 | 13 |
| symbol (10) | 10 | 8 | 10 |
| project-filtered (5) | 4 | 5 | 5 |
| old session (5) | 5 | 5 | 5 |
| branch-specific (5) | 5 | 4 | 5 |
| related topics (5) | 5 | 5 | 5 |

Branch hygiene: for the forked session, lexical excerpts of the "sqlite" and the "postgres" branch queries never
contain text from the sibling branch.

## Observations worth acting on

* Lexical retrieval ORs every query token, including English function words ("the", "for", "on"). On
  paraphrase queries that share no content word with the answer, the lexical list is therefore dominated by
  incidental stop-word matches; fusing it dilutes a correct semantic ranking (hybrid 13/15 vs semantic 15/15 on
  that category). The spec forbids dropping *technical identifiers* as stop words; a conservative English
  function-word filter applied only when enough content tokens remain is a candidate improvement. It was not
  made here because the spec asks for tuning decisions to come from real-corpus failures.
* Semantic-only scores identifiers worse than lexical/literal (8/10 symbol queries), which is why literal mode and
  hybrid fusion exist.
* Provider/model changes must re-run this evaluation (spec 20.1) before the default profile changes; a real-model evaluation must report its own results and retain the fake-provider caveat for these numbers.

## Real model on synthetic data

The ignored `real_provider_retrieval_evaluation` test uses the same synthetic sessions and
50 queries with the configured embedding provider. It creates a temporary index and ledger;
it does not read the installed index or any transcript. It requires an absolute path to an
existing configuration with embeddings enabled, hosted processing approved, and a usable
private credential file. Run it only when sending this synthetic data to that provider is
intended:

```sh
SPOOL_RETRIEVAL_EVAL_CONFIG=/absolute/path/to/config.toml cargo test --locked --test retrieval_eval real_provider_retrieval_evaluation -- --ignored --exact --nocapture --test-threads=1
```

The run caps the isolated ingestion and query budgets at 10,000 and 5,000 estimated tokens,
respectively, or the configured lower budgets. It preflights all documents and two query
passes without assuming cache hits. These are per-run reservations, not a currency cap or
limit across runs. A ten-minute deadline and incomplete coverage, provider errors, fallback,
or truncated search responses fail the run without a completed report.

The versioned JSON report identifies the dataset, provider profile, settings, per-category
and aggregate Hit@5, Hit@10 and MRR@10, query IDs and relevant session ranks, negative empty
counts, and usage. It contains no query or document text, credentials, full URLs or vectors.
The ledger's completed usage combines provider-reported tokens with an estimate when the
provider does not report usage; it cannot distinguish the two. Reserved and ambiguous tokens
are shown separately. Redirect output locally if a record is needed. Each invocation uses a
fresh ledger and may incur new charges.

The manually authored message anchors verify that each existing label points to retained
fixture text; they do not establish complete relevance judgments or snippet usefulness.
Consequently the report does not calculate Recall@k. Negative empty counts are separate:
semantic and hybrid nearest-neighbour retrieval have no relevance cutoff. There is no hosted
quality threshold. Directly seeded chunks do not test transcript adapters or chunk planning.
A later real-corpus evaluation needs separately selected and approved conversation text and
manual relevance judgments before any provider request; these synthetic results do not
establish the specification's real-corpus recall goal.

Dataset v2 removes `s_fork` from q42 (database change safety) and `s_rust` from q45
(intermittent failures and races). The former discusses backups without a database change;
the latter discusses concurrency without an observed intermittent race. Queries and corpus
are unchanged. The historical fake-provider table above uses v1 judgments. A fresh offline
fake-provider v2 run produced Hit@5 / Hit@10 / MRR@10 of 0.800 / 0.822 / 0.712 lexical,
0.933 / 1.000 / 0.876 semantic, and 0.956 / 1.000 / 0.902 hybrid.

The following v2 scores were rescored offline from the saved ranks of the single provider
invocation on 2026-11-04, using OpenRouter's `openai/text-embedding-3-small` profile at
768 dimensions. No new provider request was made. They cover 45 positive synthetic queries:

| mode | Hit@5 | Hit@10 | MRR@10 | negative empty |
|---|---:|---:|---:|---:|
| lexical | 0.800 | 0.822 | 0.712 | 5/5 |
| semantic | 1.000 | 1.000 | 0.933 | 0/5 |
| hybrid | 0.978 | 1.000 | 0.931 | 0/5 |

For q42 the remaining relevant session ranked second in semantic and hybrid mode and was
absent from the lexical top ten. For q45 the remaining session ranked first in all modes.
For q02, the relevant session ranked sixth in hybrid and first in semantic mode. The run
reserved 1,022 ingestion and 256 query tokens, with no ambiguous usage. The ledger recorded
the same completed usage values, which can include estimates.
