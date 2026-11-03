# End-to-end smoke results (observed 2026-10-12, macOS arm64)

The 2026-10-12 runs below use the real `spool` binary built from this repository, an isolated `SPOOL_HOME`
under `/tmp`, and **synthetic** session files only (`fixtures/omp/*` and a small hand-written OhMyPi
session containing a fork, a tool call and a tool result). No hosted provider was involved except the
local fake on `127.0.0.1`. Nothing was installed persistently and no harness configuration was modified:
clients were pointed at the daemon with per-run flags (`claude --mcp-config ... --strict-mcp-config`,
`codex -c mcp_servers.spool.*` overrides, `omp` with a project-local `.mcp.json` in a temp directory),
through a tee wrapper that records the MCP frames.

## Manual CLI smoke (live daemon, `spool daemon run`)

```
spool init --enable-source omp --hosted-processing no
  -> spool home /tmp/spsmoke/h (mode 0700), config written, signing key created (mode 0600),
     hosted processing: not approved (lexical-only, no text leaves this machine)
spool daemon run                        # startup discovery bootstrapped 3 sessions
spool search "webhook receiver" --mode lexical
  -> 3 matches in session "Webhook review bot" (signed match_ref per match, excerpts "USER: Could the webhook receiver ...")
spool search "foo::bar_baz" --mode literal
  -> 1 exact match (punctuation-distinct literal)
spool read SESSION --match-ref REF --before 0 --after 3
  -> BRANCH_REQUIRED (the match is shared by two persisted branches), two branch_options; no branch was invented
spool read SESSION --match-ref REF --branch-ref BR --with-tools --after 3
  -> items with provenance.origin = verified_source, record locators and digests, tool call/result by verified ownership
spool read SESSION --branch-ref BR --max-items 1        # page 1: item_limit, next_cursor set
  (append one record to the source file)
spool read --cursor CURSOR --max-items 10               # replay: appended record NOT included, selection_end
kill -9 <daemon>; spool daemon run                      # restart
  -> workers re-register after backend WORKER_ALREADY_EXISTS retries (about 30 s with the default 30 s heartbeat),
     3 sessions indexed, queue empty, the old cursor still valid, search works
```

## Bugs this smoke found (all fixed and covered by tests afterwards)

| Finding | Fix / test |
|---|---|
| Every adapter emitted `timestamp: None` (null `matched_at`, no time anchors) | `domain::parse_timestamp_ms`, exact-value adapter assertions |
| Incomplete trailing record caused an endless ingest follow-up loop (220 jobs in 2 minutes; `index --wait` never finished) | IngestSlice fix + `omp_fixtures_settle_without_follow_up_loops` |
| `index --since --wait` returned `done` before the session was indexed | JobsSlice fix; `tests/e2e.rs` asserts search works right after `--wait` |
| Claude Code rejected `search_sessions` results: output violated the declared `outputSchema` (`suggest_literal` required but omitted) | optional fields marked `default` in the schema; schema validation of real outputs in `tests/search.rs` and `tests/e2e.rs` |
| Clients logged `unknown format "uint32"` for the reader schema | bridge strips non-standard `format` values from advertised schemas |

## Real MCP clients against the live daemon (frames recorded)

| Client | Negotiated protocol | `tools/list` | `search_sessions` (lexical "webhook receiver") |
|---|---|---|---|
| Claude Code 2.1.294 | 2026-07-28 (stateless `server/discover`) | `search_sessions, read_session, index_status`, no non-standard formats | ok, `isError: false`, 1 result session; answer "Webhook review bot — 3 matches" |
| Codex CLI 0.162.1 | 2025-06-18 (`initialize`) | same | ok, "Webhook review bot — 3 matches" |
| OMP 18.10.1 | 2025-11-25 (`initialize`) | same | ok, "Webhook review bot — 3 matches" |

(The first Claude Code attempt failed on the schema bug above; the retest after the fix passed.)

## Automated end-to-end tests that run this flow

* `tests/e2e.rs` — real daemon + CLI indexing + three concurrent rmcp stdio clients; every tool result is
  validated against the tool's declared output schema; conversation view hides tool payloads, `with_tools`
  returns verified tool activity, raw view is `FORBIDDEN` unless enabled, daemon holds no inet socket.
* `tests/e2e_hosted.rs` — local fake HTTP provider on `127.0.0.1`: with `hosted_processing_approved = false`
  the provider receives **zero** requests (hybrid degrades visibly to lexical, semantic-only is a typed error);
  with consent only rendered conversation chunk text and the query are sent (asserted: no tool sentinel, no
  source paths, bearer credential header present, model and dimensions as configured) and semantic search works.
* `tests/daemon_socket.rs` — only a stale owned Unix socket is replaced; regular file, symlink, live
  listener, reserved paths and symlinked parents are refused and left untouched.

Client edge checks: `tests/e2e_mcp_edge.rs` (3 tests) drives the stdio bridge with recorded-shape raw MCP/SDK
scenarios: the initialize 2025-06-18 and 2025-11-25 and stateless 2026-07-28 negotiation paths, a typed error while the
daemon is down with recovery after it returns, a daemon restart between calls, paginated large reads within the byte
budget, cancellation, and clean stdout frames. Not tested: cancellation in the middle of a call, and daemon
reconnection, with the real Claude Code / Codex / OMP binaries, which expose no scripted way to do either (see
`docs/known-gaps.md`). The bridge opens a new IPC connection per call and a cancelled call drops its in-flight IPC call.

## Latest targeted profile, time-zone and context smoke (2026-10-13)

- Frozen profile/zone/context source: `CARGO_TARGET_DIR=/tmp/spool-core-target cargo test --locked --no-fail-fast --test embeddings --test search --test e2e_profile_migration --test e2e_hosted --test jobs --test ingest --test control --test cli --test e2e` passed all 9 targets (183 tests); log: `/tmp/spool-release-profile-zone.log`.
- Native `target/release/spool`, SHA-256 `70415e678d9f53a99daba22b5efd47ddab5041c49c441fc87f72f29d58fa6c89`: isolated CLI/MCP smoke passed 57 assertions covering date-anchored lexical/literal results, `context_outside_time_filter=true` when returned context text comes from an out-of-window message, source-verified tool reads, append/cursor behavior, stale rewrite handling, and daemon restart/reconnect; log: `/tmp/spool-release-cli-smoke-frozen.log`.
- The same release binary passed the profile-change/append/coverage/switch/query flow against a local fake HTTP embeddings endpoint on loopback. Both profiles used only their configured dimensions; no paid provider or real transcript roots; log: `/tmp/spool-release-migration-smoke.log`.

## Current verification (2026-11-03)

- Release `cd1cf49` / `78bab5f` was installed through `~/bin/spool`, a symlink to the repository release binary.
  The production `com.spool.daemon` launchd service restarted. Doctor passed all six permission checks, including
  database, WAL, and SHM mode 0600. Workers were healthy and status was ready.
- A fresh isolated hosted-disabled CLI smoke passed ingestion, search, read, and restart checks. Its results are
  in `/tmp/sp-manual-cr8vyxkz/results.json`.
- A synthetic hosted smoke used the configured OpenRouter endpoint and `openai/text-embedding-3-small` with
  768 dimensions. Three conversations produced three vector-covered chunks. Semantic paraphrase search returned
  the cached result first. Hybrid and lexical search, source-verified read, append and new embedding, and restart
  persistence passed. MCP stdio protocol `2025-11-25` passed tools/list, status, semantic search, and read.
  Artifacts are in `/tmp/splive-g8efq0fr/`.
- The initial smoke found a permissions bug. Merged PR #10 fixed it and added regression coverage.

These are functional smoke checks, not a judged retrieval accuracy evaluation, the ignored direct OpenAI test,
or a Linux service lifecycle test.

The isolated opt-in launchd lifecycle test passed on 2026-11-03 in 33.73 seconds and again in 35.08 seconds. Each run covered two installs,
stop, start, restart, and uninstall. Two earlier first-install runs timed out after 30 seconds waiting for the
socket, although launchd reported the service as registered and running. The cause is unknown, so the passing
runs do not establish consistent first-install reliability. The isolated labels were cleaned up. The production
service and providers were not touched by this test.

The current development binary (`cargo build --locked --bin spool`) also passed a fresh isolated smoke with
hosted processing disabled. It covered readiness, all six SQLite file modes under umask 022, doctor,
indexing with `--wait`, lexical BM25 search, source-verified reads, append, reindexing, restart persistence,
and graceful shutdown. Results are in `/tmp/sp-dev-4yfmjifk/results.json`.
Against an isolated socket that accepted but did not answer, the development CLI returned a status timeout
in 5.02 seconds with exit code 1. Results are in `/tmp/sp-hung-0jh9_ipt/results.json`.
