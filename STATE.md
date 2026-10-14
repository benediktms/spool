# Spool maintainer handoff (2026-10-13)

Normative requirements: `docs/spec.md`, `docs/read-session-contract.md`. This file is a handoff, not a release sign-off.
Read `docs/known-gaps.md` for deviations and `docs/performance-results.md` for measured numbers.

## Resolved in the final integration pass (each by reading the current source)

- Transient ingestion retries use Apalis' native `retry` layer (`src/jobs/scheduler.rs` `IngestRetry`, wired in
  `src/jobs/supervisor.rs`); there is no application retry scheduler. `tests/jobs.rs` observes backoff, jitter and `Retry-After`.
- A tool-call item selection lists results along one path only (`BRANCH_REQUIRED` otherwise); `tests/reader.rs`.
- Claude: physical parent links are stored as written; `src/graph.rs` skips earlier versions of the same message when it
  projects the message parent. This restored the superseded record's calls and the tool-result envelope on the
  `with_tools` path. Graph construction no longer rescans parent chains or record positions per record.
- Codex/Claude `with_tools` end to end: `tests/e2e_cross_harness.rs` follows cursors (a default 32 KiB page holds about
  eight items because every item carries signed references); `tests/reader.rs` has a Codex two-turn window test.
- Removed four compiler warnings (dead parameter/fields) and one test warning.
- `scripts/perf_daemon.py` refuses to start unless every enabled source root is inside its sandbox, and fails unless all
  sessions are indexed and a query returns hits.

## Latest targeted verification for profile migration, budget zones and context metadata

- Frozen-source batch: `CARGO_TARGET_DIR=/tmp/spool-core-target cargo test --locked --no-fail-fast --test embeddings --test search --test e2e_profile_migration --test e2e_hosted --test jobs --test ingest --test control --test cli --test e2e` — all 9 targets passed, 183 tests total; `/tmp/spool-release-profile-zone.log`.
- Actual isolated CLI/MCP smoke against `target/release/spool` (SHA-256 `70415e678d9f53a99daba22b5efd47ddab5041c49c441fc87f72f29d58fa6c89`) passed 57 checks covering indexing/publication, lexical/literal search, UTC chunk-anchor filtering, `context_outside_time_filter=true` for returned text whose message time lies outside the interval, source-verified `with_tools`, cursor/append, stale rewrite, and daemon restart/reconnect; `/tmp/spool-release-cli-smoke-frozen.log`.
- Actual release binary profile-migration smoke against a localhost-only fake HTTP embeddings provider passed: old-profile queries remained active while the new provider profile was unavailable, appended text was published and covered, then the full-coverage profile switch and semantic query succeeded; `/tmp/spool-release-migration-smoke.log`. All fixtures and enabled roots were created in a temporary sandbox; no real transcript roots or paid provider were used.
## Open, honestly

- Search during ingestion is 2.22x to 2.63x idle p95 (spec: 2x). Cause not isolated; 4 async threads was worse. Documented exception.
- Not verifiable here: real Linux systemd lifecycle; live OpenAI provider and real-corpus retrieval evaluation (no key or consent).
- The 50-query retrieval evaluation uses a fake concept-table model; it makes no claim about the 90% Recall@5 goal.
- Full test, lint and typecheck runs belong to CI (`.github/workflows/ci.yml`); this pass ran targeted tests only.

## Rules that stay in force

No real transcript roots in tests or benchmarks (an earlier worker once indexed `~/.omp`; that daemon was killed and its
paths are gone). Use an isolated `SPOOL_HOME` and sources inside a temp sandbox. No persistent production service install.
