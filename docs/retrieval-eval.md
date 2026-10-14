# Retrieval evaluation (spec 20.1)

**Scope of this document.** This is an evaluation of the *retrieval pipeline* on a small, fully synthetic,
manually authored corpus. It is **not** a measurement of recall on a real user corpus and it does **not**
evaluate a real embedding model. Specifically:

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
* Not run: a live-provider evaluation (needs a provider key and the user's hosted-processing consent) and any
  evaluation on a real transcript corpus. The 90% Recall@5 goal of spec 20.1 is a goal for *curated real
  supported-corpus queries*; it is **not** claimed as met here.

Reproduce: `cargo test --test retrieval_eval -- --nocapture` (about 1 s; asserts the evaluation set has 50
queries, paraphrase disjointness, literal exactness, branch hygiene, lexical silence on negatives, and hybrid >= lexical).

## Results (observed 2026-10-12, 45 judged queries + 5 negatives)

| mode | Recall@5 | Recall@10 | MRR | negative queries returning nothing |
|---|---|---|---|---|
| lexical | 0.822 (37/45) | 0.844 (38/45) | 0.734 | 5/5 |
| semantic (fake concepts) | 0.933 (42/45) | 1.000 (45/45) | 0.876 | 0/5 (nearest neighbour is returned by design: no similarity cutoff, spec 13.6) |
| hybrid (RRF k=60) | 0.956 (43/45) | 1.000 (45/45) | 0.913 | 0/5 (as semantic) |

Literal mode: exact retrieval on all 10 symbol/error-text queries (10/10), including `Arc<Mutex<HashMap<..>>>`,
`verify_signature()`, `X-Hub-Signature-256` and a CSS function with commas and parentheses.

Per-category Recall@5 (hits/queries):

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
* Provider/model changes must re-run this evaluation (spec 20.1) before the default profile changes; with a real
  model this file's numbers must be regenerated and the fake-provider caveat removed.
