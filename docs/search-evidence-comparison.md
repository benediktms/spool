# Search evidence comparison

This compares current main `932735e80ffb95d5d8c05a159802942b5b93ffb8` with the uncommitted search-evidence-v2 change based on `07c0bc9c49158df6316f6ba76b55c46948d028e5`. The tracked binary diff at capture had SHA-256 `102a6b536c6c53c60a032579cddec4f454ba2a16f720c5debb14a92b86a3b46a`; the untracked evaluation harness had SHA-256 `483773e4a6be764d6d879c2b7a8b82c4aaba0b4968949ae987c94fa6395233fe`. Both were unchanged after capture.

The same frozen index, 50 queries, accepted session judgments and five literal probes were used for both versions. The index contains 20 sessions and 99 active chunks. This measures lexical and literal search and indexed reads, not fresh ingestion, semantic search, or source-verified reads. No embedding provider was called. Source IDs were enabled with isolated empty roots, and reads used `indexed_only`. Every returned match was read independently to completion using cursor-only continuations, capped at 32 pages or 8 MiB per selection. Neither capture reached a cap or returned an error. Every returned page reported an indexed snapshot.

| Measure | Main | V2 |
| --- | ---: | ---: |
| Search result sessions across 55 queries | 491 | 491 |
| Match previews and read calls | 735 | 779 |
| Combined preview text, UTF-8 bytes | 675,141 | 564,800 |
| Read pages | 822 | 1,645 |
| Serialized read page bytes | 12,168,316 | 41,130,323 |
| Incomplete selections or read errors | 0 | 0 |

For an agent that reads fewer results, the table below includes all matches in the first returned session for each lexical query, and all matches in the highest-ranked relevant session for each positive lexical query. These are subsets of the totals above, not additional calls. Calls are not deduplicated: overlapping selections count each time the caller reads them.

| Lexical selection | Main calls / pages / bytes | V2 calls / pages / bytes |
| --- | ---: | ---: |
| Top session, 50 queries | 120 / 148 / 2,949,161 | 126 / 304 / 7,753,613 |
| First relevant session, 45 positive queries | 105 / 133 / 2,585,900 | 109 / 273 / 6,972,370 |

All 55 ordered session lists and first relevant ranks were identical. For the 45 positive lexical queries, recall at 5 was 0.9778, recall at 10 was 1.0, and reciprocal rank at 10 was 0.9543 in both runs. All five lexical negative queries returned sessions in both runs. These session-level labels do not establish whether a preview or a read answers its query. Preview usefulness and complete read usefulness require separate paired qualitative assessment; do not compare a preview from one version with a full read from the other. The byte and page totals are aggregate output volume, not latency or a performance benchmark.

## Reproduction

The private inputs and capture files contain transcript content. Keep them in a private directory and do not publish them. The input BLAKE3 digests are:

- `corpus.sqlite`: `fac974ff2792ae63eed3ea8583bac9ce02157d1403bed36b35a081ea55b4d782`
- `manifest-review-50-v3.json`: `236b6bd91275d228d247615574f3359f4204af15bc575d6a6c36d33445b54e24`
- `judgment-matrix-v1.json`: `3182f4fe3a152d7cb4f992fb8459f24cb6d8fcdb3ddb27ceecaa06f6d2e524e8`
- `literal-probes-v1.json`: `baada47d57285ce5ec80ffd92dd3545b4a709348ff25b26112fda8aee7ab1c24`

Archive the exact main commit into a new private directory. Copy `tests/controlled_retrieval_eval.rs` from the candidate into that archive, removing only the `reader_common` module declaration and `capture_read_follows_pages` unit test, which depend on the candidate's reader fixture. The capture and scoring functions are otherwise identical. Set `SPOOL_CONTROLLED_EVAL_CORPUS`, `SPOOL_CONTROLLED_EVAL_MANIFEST`, `SPOOL_CONTROLLED_EVAL_MATRIX`, and `SPOOL_CONTROLLED_EVAL_LITERAL_PROBES` to the respective absolute input paths. Set `SPOOL_CONTROLLED_EVAL_SNIPPETS` to a new private output file for each run. With separate target directories, run this command in each tree:

```sh
cargo test --locked --release --test controlled_retrieval_eval controlled_lexical_preview -- --ignored --exact --nocapture
```

The private evidence is a JSON array of query/session rows with previews and full read pages; the test log contains ranked session lists and metrics. Main uses `excerpt` and v2 uses `preview.text` for preview text. Compare combined previews and complete read pages at the same rank and evidence stage. Match references and cursor values are version-specific and should not be treated as content. Output sizes include serialized page metadata and cursor values.

## Source observations

The search grouping and evidence implementation and the read implementation in main are identical to the `07c0bc9` base; main also changes query title handling in `src/search/query.rs`. This fixed-index comparison does not exercise source adapter changes in main.

In v2, `src/search/evidence.rs:45` scores a visible span by distinct supported query units and focuses on the earliest hit; `src/search/query.rs:90` supplies the first hit for each unit. The passage builder at `src/search/evidence.rs:82` then emits a contiguous passage. Main's `src/search/evidence.rs:169` scores the text allocated to its multiple windows under its 1,200-byte budget. A difference between support across a whole span and coverage in the emitted passage is a plausible explanation for weaker previews, but this comparison alone does not establish causation; it needs paired assessment and an ablation.

V2 also groups neighboring candidates and builds anchors from the selected window's records (`src/search/mod.rs:163`, `src/search/mod.rs:1079`, `src/search/mod.rs:1589`). Both readers resolve selections to canonical whole messages between their anchors (`src/transcript/selection.rs:498`). The larger v2 read output can therefore reflect broader anchor unions and overlapping selections; whole-message reads were already present on main. The aggregate counts do not isolate the contribution of each mechanism.

## Paired qualitative assessment

Two assessors independently reviewed the highest-ranked relevant session for each of the 45 positive lexical queries. They graded combined previews and complete paginated reads separately: S = sufficient, P = partial, A = absent, M = misleading. These are judgments against the frozen query intent, not automatic search scores. The original v2 capture above was assessed, before the later renderer experiment.

| Stage | Main S / P / A / M | V2 S / P / A / M |
| --- | ---: | ---: |
| Combined previews | 35 / 7 / 1 / 2 | 26 / 7 / 10 / 2 |
| Complete reads | 40 / 2 / 1 / 2 | 39 / 4 / 1 / 1 |

A fresh assessment grades `worktree_identity` main preview partial; an earlier assessment graded it sufficient. The assessor applied a stricter interpretation of the requested causal explanation, so the counts should not be interpreted as a label change. `old_cursors` reads fell from sufficient to partial and `git_discovery_home` from sufficient to absent because their decisive records were missing from v2's selected reads. `worktree_identity` improved from partial to sufficient. `project_migration_trigger` and `claude_cwd_filter` improved to partial from misleading and absent respectively, without establishing a sufficient answer. These results do not establish an overall quality improvement.

| Query ID | Preview main → v2 | Read main → v2 |
| --- | --- | --- |
| lease_identifier | S → A | S → S |
| refs_identifier | S → S | S → S |
| install_identifier | S → S | S → S |
| adapter_codex | S → S | S → S |
| adapter_claude | S → S | S → S |
| adapter_tail | S → A | S → S |
| claude_mcp_rebuild | P → A | S → S |
| discovery_module_boundary | S → S | S → S |
| local_retrieval_deadline | S → S | S → S |
| backfill_subagents | S → S | S → S |
| coverage_index_measurement | S → S | S → S |
| performance_target | S → S | S → S |
| contributing_newlines | S → A | S → S |
| guidance_spool_poc | P → S | S → S |
| batching_hotpaths | S → S | S → S |
| adapter_invariants | S → A | S → S |
| wal_retention | S → S | S → S |
| subagent_policy | S → S | S → S |
| read_budget | S → S | S → S |
| worktree_identity | P → P | P → S |
| git_config_scope | S → S | S → S |
| read_verbose_cursor | S → S | S → S |
| old_cursors | S → P | S → P |
| git_discovery_home | S → A | S → A |
| codex_remote_secret | M → M | M → M |
| fingerprint_backfill | P → A | P → P |
| project_migration_trigger | M → P | M → P |
| query_score_fusion | P → P | S → S |
| multi_query_fallback | S → S | S → S |
| lease_storage_limit | S → S | S → S |
| unmatched_project_provider | S → S | S → S |
| backup_vs_rehearsal | P → S | S → S |
| project_maintenance_timing | S → M | S → S |
| omp_artifact_correction | S → S | S → S |
| claude_cwd_filter | A → A | A → P |
| mcp_socket | S → A | S → S |
| chunking_test | S → P | S → S |
| profile_budget | P → A | S → S |
| retry_jitter | S → S | S → S |
| sqlite_permissions | S → P | S → S |
| metric_labels | S → S | S → S |
| wrapper_metadata | S → S | S → S |
| status_wait | S → P | S → S |
| restart_deadline | S → S | S → S |
| ci_shards | S → S | S → S |

The observed preview regressions justify testing passage selection against these paired queries. The two confirmed read regressions require separate investigation of selected record anchors; changing passage rendering alone cannot recover records absent from a read selection. Preserve the fixed corpus and assess preview and read stages independently when evaluating a repair.

## Renderer experiment

A later isolated renderer experiment used the same frozen inputs and capture protocol. Its `src/search/evidence.rs` SHA-256 was `4924660cadd5991569d2121fe73f68668c3cb2b8afcd7177e7048444d134365e`; the tracked diff SHA-256 was `be9965f84ae59f664c226663c2a5e19907cbef88fb407ff4b99371d1a46704eb`. Both were stable during capture. Compared with the original v2 capture, all 55 ranked session lists, all 491 per-session match counts, and all 779 selected anchor record-and-digest sets and read item sequences were unchanged. Identity comparison used the record, digest, item kind and block in the handle payloads, since opaque lease and signature values change between runs. All reads were performed again and completed; the experiment returned 1,645 indexed pages and 41,130,323 serialized page bytes. Its combined preview text was 566,695 UTF-8 bytes. These invariants establish the scope of this particular capture, not a general equivalence proof. The assessors regraded all 45 positive queries with the same rubric. Experiment previews were 26 sufficient, 8 partial, 9 absent and 2 misleading, compared with original v2 at 26 / 7 / 10 / 2 and main at 35 / 7 / 1 / 2. The only grade change from original v2 was `fingerprint_backfill`, absent to partial: the preview now states that a parser change does not reparse old sessions, but omits the scheduling cause. Combined preview text was unchanged for 21 of the first 25 queries. The number of sufficient previews did not improve. The remaining per-query grades match the original v2 table above.

## Selector experiment

The subsequent selector experiment retained the renderer experiment and changed `src/search/mod.rs` (SHA-256 `1c99da9a865e85f59de06b48b0edeac987ee5f79558536a0bc18993f8aead3c0`). The tracked diff SHA-256 was `9eb3558ec2647bc2fc8cf6339ac6f70be83ad10a0ff7d392149574a70ac916f0`. Both source hashes and the tracked diff were stable during capture. The same frozen inputs and release capture command were used.

All 55 ranked session lists remained unchanged. Compared with the renderer experiment, 50 of 491 session results changed match count, 148 changed their union of anchor record sets, and 127 changed their union of returned read records. The previously missing decisive records for `old_cursors` and `git_discovery_home` were present in their highest-ranked relevant sessions. Paired assessment confirmed both answers were sufficient in the complete reads. Across all 45 positive queries, selector previews scored 28 sufficient / 6 partial / 8 absent / 3 misleading; reads scored 41 / 1 / 1 / 2. All main-sufficient reads remained sufficient and `worktree_identity` improved from partial to sufficient. Relative to original v2, `project_migration_trigger` read fell from partial to misleading and `claude_cwd_filter` fell from partial to absent, matching their main grades. Nine main-sufficient previews failed to remain sufficient; these results do not establish quality readiness.

The selector capture returned 729 independently read matches, 1,484 indexed pages and 36,725,015 serialized page bytes, with no read errors or incomplete selections. Reading every match in the top session of each lexical query required 120 calls, 275 pages and 7,082,113 bytes. The corresponding first-relevant-session totals were 105 calls, 246 pages and 6,405,217 bytes. These totals count overlapping outputs for each call.

The earlier selector built up to three neighboring candidate windows and ranked each by its strongest member. In the two recovered cases, overlapping windows repeated higher-ranked witnesses and filled the three-match cap before the answer-bearing window, even though that window was retrieved. The selector experiment skips a primary already covered by a retained window. Other preview losses require separate evidence: a complete read returns whole messages and does not by itself prove the selected chunk span contains the answer.

Selector grades for the same highest-ranked relevant sessions are below; the earlier table gives the paired main and original v2 grades.

| Query ID | Selector preview | Selector read | Fragment preview | Complete passages preview |
| --- | --- | --- | --- | --- |
| lease_identifier | A | S | S | S |
| refs_identifier | S | S | S | S |
| install_identifier | S | S | S | S |
| adapter_codex | S | S | S | S |
| adapter_claude | S | S | S | S |
| adapter_tail | A | S | S | S |
| claude_mcp_rebuild | A | S | P | S |
| discovery_module_boundary | S | S | S | S |
| local_retrieval_deadline | S | S | S | S |
| backfill_subagents | S | S | S | S |
| coverage_index_measurement | S | S | S | S |
| performance_target | S | S | S | S |
| contributing_newlines | A | S | S | S |
| guidance_spool_poc | S | S | P | S |
| batching_hotpaths | S | S | S | S |
| adapter_invariants | A | S | S | S |
| wal_retention | S | S | S | S |
| subagent_policy | S | S | S | S |
| read_budget | S | S | S | S |
| worktree_identity | P | S | P | S |
| git_config_scope | S | S | S | S |
| read_verbose_cursor | S | S | S | S |
| old_cursors | P | S | P | S |
| git_discovery_home | S | S | S | S |
| codex_remote_secret | M | M | M | M |
| fingerprint_backfill | P | P | P | P |
| project_migration_trigger | M | M | M | M |
| query_score_fusion | P | S | S | S |
| multi_query_fallback | S | S | S | S |
| lease_storage_limit | S | S | S | S |
| unmatched_project_provider | S | S | S | S |
| backup_vs_rehearsal | S | S | P | S |
| project_maintenance_timing | M | S | S | S |
| omp_artifact_correction | S | S | S | S |
| claude_cwd_filter | A | A | A | A |
| mcp_socket | A | S | S | S |
| chunking_test | P | S | S | S |
| profile_budget | A | S | P | P |
| retry_jitter | S | S | S | S |
| sqlite_permissions | S | S | S | S |
| metric_labels | S | S | S | S |
| wrapper_metadata | S | S | S | S |
| status_wait | P | S | S | S |
| restart_deadline | S | S | S | S |
| ci_shards | S | S | S | S |

A separate reconstruction checked six remaining preview misses (`lease_identifier`, `adapter_tail`, `contributing_newlines`, `adapter_invariants`, `claude_mcp_rebuild`, and `old_cursors`). Their labeled answer ranges were inside eligible spans of selected lexical windows, and reconstructed anchor sets matched the selector capture. For these cases, the original first-focus passage would include each labeled answer range; passage boundary or target clipping alone therefore does not explain the misses. Five answer passages tied an earlier selected passage on literal-unit support; the `adapter_invariants` answer passage scored lower. This is evidence for those six labeled cases, not proof that a particular scoring change will improve all queries.

## Fragment renderer ablation

A subsequent experimental renderer reused main's multiple-fragment policy while retaining the selector experiment. The renderer SHA-256 was `8fc9446bd3a3b80a1749b52d2e94e7053814a173cb54ffe5f77876574799bee3` and tracked diff SHA-256 was `81c2ed9d8470adb611984506d5b1f9ecd8e8794c8f4285210970cb3215715d60`; both source files and diff were stable during capture. It preserved all 55 ranked session lists, per-session match counts, all 729 canonical anchor sets and read-item sequences. The capture returned 1,484 indexed pages and 36,725,015 serialized page bytes with no errors or incomplete reads; combined preview text was 683,168 bytes. Paired preview assessment scored 35 sufficient / 7 partial / 1 absent / 2 misleading, the same aggregate as main but with different query outcomes. The fragment preview for `old_cursors` was partial where main was sufficient; `query_score_fusion` improved from partial to sufficient. Thus 34 of 35 main-sufficient previews remained sufficient. Compared with the selector experiment, `guidance_spool_poc` and `backup_vs_rehearsal` fell from sufficient to partial because relevant details were clipped. Complete reads retained the selector assessment of 41 / 1 / 1 / 2.

This ablation is an experiment, not a completed fix: it conflicts with the current contiguous-passage assumption and two of 49 focused search integration tests failed on time-context behavior. The renderer worker reported its unit tests and library Clippy passed. Any adoption requires resolving the contract and failing tests.

The fragment ablation is diagnostic and is no longer active. Its source and patch are preserved privately. The experiment owner restored the passing renderer (`4924660cadd5991569d2121fe73f68668c3cb2b8afcd7177e7048444d134365e`) while keeping the selector change. After restoration, 13 search unit tests and 49 search integration tests passed, as did library Clippy, scoped formatting and diff checks.

The comparison supports retaining decoupled read anchors and testing bounded witness diversity. It also supports investigating a few complete passages under a soft budget to retain main’s multi-passage coverage without its observed fragment clipping. Any such direction needs a contract decision and incremental paired validation: preserve every one of the 35 main-sufficient previews and 40 main-sufficient reads, evaluate page and byte costs, and satisfy date and source contracts. These are recommendations, not an approved implementation plan.

## Complete-passage candidate

After approval of separated complete passages, the candidate renderer SHA-256 was `73c3e58db5e11761bc923a9a26d376f88c434d222b192009e1717f67699808a9`, with selector SHA-256 `1c99da9a865e85f59de06b48b0edeac987ee5f79558536a0bc18993f8aead3c0` and tracked diff SHA-256 `4ef60e9b1f518c22eb091d8d8841a5fb94717dab2a875da57a3189b600c72ed8`. These identities were stable throughout capture. Using the same frozen inputs and release command, all 55 session rankings, 491 session match counts, 729 canonical anchor sets and read-item sequences matched the selector capture. All 729 reads completed with no errors, returning 1,484 indexed pages and 36,725,015 serialized bytes. Combined preview text was 1,142,119 UTF-8 bytes, compared with 544,465 for selector and 675,141 for main. These are output volumes, not performance measurements.

The implementation worker reported focused search unit, search and reader-schema integration tests, and library Clippy passed. The fixed-rubric assessment passed the paired gate: previews scored 40 sufficient / 2 partial / 1 absent / 2 misleading, and every one of the 35 main-sufficient previews remained sufficient. The unchanged reads scored 41 / 1 / 1 / 2, retaining all 40 main-sufficient reads. Five main-partial previews improved to sufficient: `claude_mcp_rebuild`, `guidance_spool_poc`, `worktree_identity`, `query_score_fusion` and `backup_vs_rehearsal`. Remaining preview gaps were partial `fingerprint_backfill` and `profile_budget`, absent `claude_cwd_filter`, and misleading `codex_remote_secret` and `project_migration_trigger`. This is limited to the frozen labeled queries.

## Paired synthetic performance

The same benchmark source (SHA-256 `07d31ef1ee41a2a453761658bd49ef56cd75b57ede1f6635cf9e8788294819c9`) was built in release mode against exact main `932735e` and the complete-passage candidate. Three sequential, alternating-order pairs used 15,000 synthetic chunks; one pair used 100,000. Each run warmed each mode once, then timed 60 lexical, semantic and hybrid queries and 30 queries for each literal mode. Fake embeddings were local. Seeding was outside query timings. The table gives the median of three run-level p95 values at 15,000 and the single p95 at 100,000; the latter cannot establish a stable tail distribution.

| Mode | 15k main → candidate p95 ms | 100k main → candidate p95 ms |
| --- | ---: | ---: |
| Lexical | 8.1 → 10.4 | 46.2 → 53.7 |
| Literal exact | 11.4 → 12.2 | 93.5 → 80.1 |
| Literal common | 38.1 → 41.2 | 183.7 → 189.1 |
| Literal common, time filtered | 95.5 → 98.2 | 625.8 → 639.6 |
| Semantic | 100.7 → 107.5 | 906.5 → 901.3 |
| Hybrid | 112.3 → 112.9 | 860.5 → 849.0 |

At 15k the candidate's median p50/p99 in milliseconds were lexical 9.2/15.2, literal exact 11.1/13.8, common literal 37.7/41.7, time-filtered common 94.7/98.5, semantic 99.3/112.9, and hybrid 106.2/117.1. Main's were respectively 6.7/12.1, 10.9/13.1, 35.2/38.8, 92.3/97.3, 94.0/104.5, and 102.4/116.0. In the single 100k pair, semantic p99 was 1,109.5 ms on main and 3,195.0 ms on the candidate; this tail observation requires caution because there is only one pair.

The 15k candidate emitted 994,102 lexical response bytes across 600 sessions and 837 matches, compared with main's 979,595 bytes, 600 sessions and 809 matches. The common-literal workload emitted 968,640 versus 966,840 bytes, both across 300 sessions and 900 matches. Thus timing compares the same inputs but not identical output shapes or match counts. For the frozen real-corpus capture, preview text was 1,142,119 versus 675,141 bytes and exhaustive indexed reads were 36,725,015 versus 12,168,316 serialized bytes; these costs must be considered alongside the quality gains.

Candidate-only preview events at 15k had median total elapsed time across each full mode workload of 12.5 ms lexical, 13.5 ms semantic and 16.0 ms hybrid; the 100k single run measured 10.8, 11.9 and 12.0 ms respectively. Main emits no equivalent phase events, so these numbers do not support a baseline preview-phase ratio. At 15k, whole-process user CPU ranged 13.09–13.65 s main and 13.68–14.27 s candidate, including seeding; peak RSS ranged 79.4–79.9 MiB and 78.4–78.8 MiB respectively. At 100k, user CPU was 89.49 versus 90.72 s and peak RSS 77.2 versus 74.0 MiB, again including seeding. Process CPU, wall time and RSS are not per-query measurements. One machine, a small sample and any host contention limit causal or general performance claims.
