# Adapter verification

## Isolated owned-module suite

Because the full application was still being integrated, the owned modules were copied into a temporary minimal crate at `/tmp/spool-owned-check` (dependencies: `blake3`, `rustix` with `fs/std/process`, `serde`, `serde_json`, `thiserror`, and dev-dependency `tempfile`). This checks adapter, graph, chunk, and source code plus fixtures/tests without changing the shared crate's library wiring. It does not verify full application/core integration. Scratch Cargo resolution used Rust 1.96-compatible crate versions, including rustix 1.1.5, rather than the repository lockfile.
After saving this evidence, the scratch crate, its target directory, and the temporary findings file were removed.

Command:

```sh
CARGO_TARGET_DIR=/tmp/spool-owned-check-target cargo test --offline --manifest-path /tmp/spool-owned-check/Cargo.toml
```

Formatting command:

```sh
rustfmt --edition 2024 spool/src/adapters/claude.rs spool/src/adapters/codex.rs spool/src/adapters/omp.rs spool/src/graph.rs spool/src/source.rs spool/src/ingest/chunk.rs spool/tests/adapter_common/mod.rs spool/tests/adapters.rs spool/tests/adapters_claude.rs spool/tests/adapters_codex.rs spool/tests/adapters_omp.rs spool/tests/adapter_conformance.rs spool/tests/chunking.rs spool/tests/graph.rs spool/tests/source.rs
```


Result: **16 passed, 0 failed, 7 test suites**; no compiler warnings. Suite counts: adapters 5, chunking 3, graph 1, redacted real-parser smoke 1, source 6. Full output: [`artifact://318`](artifact://318).

The real-parser smoke read at most 64 KiB from one local Claude JSONL file and sent `MessageEntry` values to a discard-only sink; it asserted only a known session ID and nonzero record/message counts. No transcript body was printed, logged, copied into fixtures, or saved in this document.

## Real repository targeted run

Command:

```sh
CARGO_TARGET_DIR=/tmp/spool-core-target cargo test --test adapters_omp --test adapters_codex --test adapters_claude --test chunking --test graph --test source
```

Result: **16 passed, 0 failed, 6 suites**; adapters_omp 3, adapters_codex 2, adapters_claude 1, chunking 3, graph 1, source 6. Three compiler warnings remain (OMP `Message.content`, OMP legacy text signature, and `SessionRow.fingerprint`). Full output: [`artifact://533`](artifact://533).

The adapter edge tests use synthetic fixtures only; no real transcript body was printed or logged.

## Final targeted integration pass

Library check:

```sh
CARGO_TARGET_DIR=/tmp/spool-core-target cargo check --lib
```

Result: successful; the same three non-fatal warnings remain. Full output: [`artifact://579`](artifact://579).

Targeted tests:

```sh
CARGO_TARGET_DIR=/tmp/spool-core-target cargo test --test adapter_conformance --test adapters_omp --test adapters_codex --test adapters_claude --test chunking --test graph --test source
```

Result: **73 passed, 0 failed, 7 suites**; `adapter_conformance` 57, `adapters_omp` 3, `adapters_codex` 2, `adapters_claude` 1, `chunking` 3, `graph` 1, `source` 6. Full output: [`artifact://588`](artifact://588).

The 57 conformance cases passed as the same 19 named cases for each adapter: `normal_exchange_has_both_roles`, `commentary_and_final_phase_policy_is_explicit`, `mixed_text_tools_and_nontext_blocks_are_separated`, `code_fences_tabs_and_short_replies_are_verbatim`, `prompt_injection_is_retained_as_ordinary_data`, `duplicate_stream_and_event_policy_is_explicit`, `tool_results_in_user_envelopes_never_become_text`, `control_hook_summary_system_developer_and_unknown_sentinels_are_excluded`, `unknown_record_and_block_counts_are_visible`, `unicode_text_is_byte_exact_through_locators`, `same_content_moved_file_keeps_all_locators`, `fork_graph_has_both_leaves_or_documents_missing_lineage`, `missing_parent_warns_and_leaves_an_isolated_message`, `cycle_warns_and_isolates_every_cycle_message`, `malformed_complete_line_warns_unknown_then_parsing_continues_to_incomplete_tail`, `skipped_tool_with_invalid_utf8_does_not_stop_later_messages`, `oversized_record_is_rejected`, `subagent_source_and_raw_parent_are_classified_without_verification`, and `version_policy_is_explicit_for_the_pinned_schema`.

All fixtures in this pass are synthetic. No real transcript body was printed, logged, copied into fixtures, or saved in this document.

## Coverage exercised

- Claude synthetic update fixture groups by `message.id`, emits physical UUID record keys and `supersedes`, and retains tool results and non-repeated calls. Physical parent links are stored as written, so superseded versions and the tool-result envelopes between them stay on the record path; an update with no parent inherits the previous version's parent. The message graph skips earlier versions of the same message when it projects the message parent, so a later save never becomes its own ancestor. It does not globally deduplicate native IDs. The same call ID in a separate assistant message/turn remains distinct, and tool results remain distinct across turns. End to end: `tests/reader.rs` (`claude_with_tools_links_by_native_id_within_each_turn`, `codex_turn_window_excludes_the_next_turn_and_reused_call_id_links_within_its_own_turn`) and `tests/e2e_cross_harness.rs`.
- Codex synthetic rollout retains persisted-record ancestry across intervening control records; the pinned schema does not expose a separate record-parent field. Canonical `agent_message` is recognized, as are the pinned response-item tags. OMP title-slot/tree and trailing incomplete JSONL fixtures also pass.
- Adapter locators round-trip every record after a same-content file move, reject a digest mutation, and preserve later OMP locators after rewriting its title slot. Timestamp assertions verify record times plus manifest header start and max activity, including `2026-10-12T09:00:01Z` → `1791795601000`.
- Graph fixture resolves a child whose parent points at a superseded physical message key to the authoritative message in ancestry/path traversal.
- Staging builds the graph from raw physical parents and hands the superseded keys to `SessionGraph` as `supersedes`; it does not rewrite parents through aliases first (that made a later save's link to its earlier save a self-parent and a false `cycle` warning). Unit tests in `src/ingest/stage.rs` (`graph_tests`): three saves of one message separated by a tool record produce no warning, the message parent is the prior distinct message, and a genuine two-record cycle still warns. The Claude parser version was bumped so unchanged sessions re-stage.
- Chunk tests cover bounded parent overlap, hard rendered-token limits, UTF-8-safe long Unicode/code splitting with exact source reassembly, cancellation, fragment caps, stable hashes, and a 2 MiB message completing the two-pass performance check in 18.68 seconds.
- Source tests cover 15 files across flat/nested directories paginated in pages of 3 with no omissions/duplicates and completion only at the final page; continuation after entries are added/removed; permission errors; symlink exclusion; replacing a parent directory with an outside symlink; non-regular Unix socket exclusion/rejection; and rejecting `/dev/null` as a configured source root without opening the device.

- The source tests do not create a character-device node inside an approved user-owned root (not available without privileged `mknod`); `/dev/null` root rejection and non-regular socket handling are covered. Adapter unit tests use an in-memory sink and do not exercise call/result persistence or linking. A synthetic integrated cancellation test verifies staging cleanup and no snapshot publication.
- The shared `tests/adapter_conformance.rs` matrix was formatted and exercised in a targeted run above; its 57 passing cases are included in that count.

## Follow-up verification

Observed target results:

- `adapter_conformance`: 57 passed; `adapters_claude`: 2 passed; `adapters_codex`: 3 passed. These suites completed successfully before the combined run timed out after 360 seconds in the chunking stress case; no aggregate pass is claimed.
- `cargo test --test chunking -- --test-threads=1`: 7 passed, including the 2 MiB two-pass performance case (18.68 seconds).
- `cargo test --test graph`: 1 passed.
- `cargo test --test ingest cancellation_during_chunking_discards_staging_without_publication`: 1 passed.


