# Known gaps and verification limits

This file lists, against `docs/spec.md` and `docs/read-session-contract.md`, what is **not** implemented, **not**
verified, or implemented with a documented deviation. It is not an exhaustive coverage claim: for what each test
suite and run actually covered, read `docs/smoke-results.md`, `docs/dependency-baseline.md`,
`docs/adapter-verification.md`, `docs/performance-results.md` and `docs/retrieval-eval.md`. It is deliberately blunt.

## Verification that could not be done on this machine

| Item | Why | What exists instead |
|---|---|---|
| Real Linux systemd user-service lifecycle (spec 17.4) | The host is macOS; no systemd | Unit text is generated and compared in tests; the command sequences are checked with a recording runner. Not run against a real `systemctl --user`. |
| Direct OpenAI provider contract and judged retrieval evaluation with a real model | The ignored direct OpenAI test and a judged evaluation remain unrun. | A synthetic OpenRouter smoke used the configured real model on 2026-11-03. See `docs/smoke-results.md`. Local fake HTTP provider tests also cover hosted behavior. |
| Retrieval evaluation on a real corpus; the spec 20.1 goal of 90% Recall@5 | Needs real transcripts and a real embedding model | `docs/retrieval-eval.md`: 50 synthetic queries, fake concept-table embeddings; no claim about the goal. |
| Search latency during ingestion within 2x idle p95 (spec 20) | Not met: measured 2.22x to 2.63x on a synthetic 300-session, 24,600-chunk corpus (absolute p95 about 60 ms); cause not isolated, no code changed for it | `docs/performance-results.md`: startup on a populated index, ten-minute idle CPU, ingestion time, RSS, WAL and parse cost of excluded tool payloads are measured. |
| Reference hardware (Apple Silicon, 16 GiB) | Benchmarks ran on an M3 Max, 36 GiB | Hardware is stated with the results. |
| Per-client cancellation and reconnection with the real Claude Code / Codex / OMP binaries | Those clients do not expose a scripted cancel/reconnect | Both negotiation paths those clients use (initialize 2025-06-18 and 2025-11-25, stateless 2026-07-28) are driven with raw recorded-shape JSON-RPC frames in `tests/e2e_mcp_edge.rs`, including daemon down, daemon restart between calls, cancellation, and stdout cleanliness. Real clients were verified for handshake, `tools/list` and one `tools/call` each. |
| Full production launchd lifecycle (spec 19) | The complete install, kill, recovery, stop and uninstall sequence was not verified for the production label. | The installed `com.spool.daemon` was restarted and healthy on 2026-11-03. The isolated label `com.spool.test.*` lifecycle test passed twice on 2026-11-03, after two first-install socket timeouts of unknown cause. See `docs/smoke-results.md`. |

## Functional limits and deviations

* **Read byte budget.** The contract permits `max_bytes` from 4,096. The result includes JSON escaping and
  signed references. Match references and cursors are tested below 150 bytes (`tests/reader.rs`). A budget
  that cannot fit the next fragment returns `RESOURCE_LIMIT` with recovery `increase_max_bytes`. The required
  budget depends on the result; the minimum accepted input does not guarantee every read can progress.
* **Tool item selection and sibling branches.** Fixed. A tool-call `item_ref` lists linked results along one path
  only; results on several branches return `BRANCH_REQUIRED` unless `branch_ref` picks one (tests/reader.rs).
* **Codex and Claude `with_tools`** is covered end to end through the daemon and MCP in `tests/e2e_cross_harness.rs`.
* **`VIEW_NOT_SUPPORTED` for raw view** is implemented but not exercised end to end (all three advertised formats
  are JSONL and support byte-exact resolution). Invalid-UTF-8 raw blocks (base64) are covered by unit tests only.
* **Compressed/archived Codex rollouts** are explicitly `Unsupported` (spec allows support or explicit status).
* **Time zones.** The CLI accepts `UTC` and fixed offsets only for relative dates; named DST zones need a tz
  database dependency. The MCP API takes absolute UTC (`Z`) timestamps only.
* **Lexical retrieval ORs English function words.** Observed to dilute hybrid ranking on paraphrase queries with no
  content-word overlap (docs/retrieval-eval.md). Not changed, per the spec's rule that tuning follows real-corpus
  evidence.
* **Exact vector scan at 100,000 chunks** is about 0.7 s for semantic/hybrid (docs/performance-results.md); the
  15,000-chunk targets are met. No ANN index was added (spec: measure first).
* **Apalis retry.** Transient ingestion failures use Apalis' native `retry` layer (`IngestRetry`: 5 attempts, 5 s
  base, 15 min cap, jitter, `Retry-After` floor) on the same task; there is no application retry scheduler. The
  backoff is an in-process sleep (`tests/jobs.rs` measures it); backend re-enqueue after a crash increments attempts.
* **Worker re-registration.** After any daemon exit the backend refuses to re-register the same worker name
  for up to one heartbeat interval (30 s default, `WORKER_ALREADY_EXISTS`); the supervisor waits out the remaining
  lease before registering (one info log, no error spam), so ingestion resumes about 30 s after a restart, while
  search and read are available immediately.
* **Provider consent is all-or-nothing per install** (`embedding.hosted_processing_approved`); there is no
  per-source consent.
* **Encryption.** The index is plain SQLite protected only by owner-only filesystem permissions; there is no
  application-layer encryption and no isolation from a hostile process of the same user (spec 15.2).
