# Known gaps and verification limits

This file lists, against `docs/spec.md` and `docs/read-session-contract.md`, what is **not** implemented, **not**
verified, or implemented with a documented deviation. It is not an exhaustive coverage claim: for what each test
suite and run actually covered, read `docs/smoke-results.md`, `docs/dependency-baseline.md`,
`docs/adapter-verification.md`, `docs/performance-results.md` and `docs/retrieval-eval.md`. It is deliberately blunt.

## Verification limits

| Item | Why | What exists instead |
|---|---|---|
| Fresh macOS LaunchAgent and Linux systemd user-service runtime lifecycle | This Windows session did not run either OS service manager | Existing generated-definition and recording-runner tests remain; historical records are preserved in the linked evidence files. |
| Windows automatic logon startup and uninstall lifecycle | User-requested installation verified registration, start, stop, restart, and OMP connection, but did not log the user out or remove the installed task | Native `TASK_VALIDATE_ONLY`, the native windowless helper, foreign-task protection, and CLI/service regressions run without registering persistent test tasks. |
| Windows 10 and ARM variants | Native smoke was on Windows 11 x64 only | Windows 10/11 x64 is the support target; Windows 10 and ARM variants have not been verified. |
| Live OpenAI provider contract and retrieval evaluation with a real model | Needs a provider key and the user's hosted-processing consent; none was given | Local fake HTTP provider on 127.0.0.1 (tests/e2e_hosted.rs, tests/embeddings), opt-in `tests/live_provider.rs` (ignored unless `SPOOL_LIVE_PROVIDER_KEY` is set) that has never been run. |
| Retrieval evaluation on a real corpus; the spec 20.1 goal of 90% Recall@5 | Needs real transcripts and a real embedding model | `docs/retrieval-eval.md`: 50 synthetic queries, fake concept-table embeddings; no claim about the goal. |
| Search latency during ingestion within 2x idle p95 (spec 20) | Not met: measured 2.22x to 2.63x on a synthetic 300-session, 24,600-chunk corpus (absolute p95 about 60 ms); cause not isolated, no code changed for it | `docs/performance-results.md`: startup on a populated index, ten-minute idle CPU, ingestion time, RSS, WAL and parse cost of excluded tool payloads are measured. |
| Reference hardware (Apple Silicon, 16 GiB) | Benchmarks ran on an M3 Max, 36 GiB | Hardware is stated with the results. |
| Per-client cancellation and reconnection with the real Claude Code / Codex / OMP binaries | Those clients do not expose a scripted cancel/reconnect | Both negotiation paths those clients use (initialize 2025-06-18 and 2025-11-25, stateless 2026-07-28) are driven with raw recorded-shape JSON-RPC frames in `tests/e2e_mcp_edge.rs`, including daemon down, daemon restart between calls, cancellation, and stdout cleanliness. Real clients were verified for handshake, `tools/list` and one `tools/call` each. |
| Real-launchd lifecycle for the production label `com.spool.daemon` | Never installed (the user did not ask for a persistent service) | Isolated label `com.spool.test.*` lifecycle test (opt-in `SPOOL_TEST_LAUNCHD=1`), passed. |
| Real Claude Code / Codex / OMP binaries on Windows | The native Windows smoke drove recorded-shape stdio JSON-RPC, not installed client binaries | Historical client checks remain documented in the dependency proof; Windows-native compatibility is not yet verified with those binaries. |


## Functional limits and deviations

* **Minimum practical `max_bytes`.** The contract permits `max_bytes` from 4,096. `read_session` counts the
  structured result plus rmcp's JSON-escaped compatibility text copy plus signed references (each 350 to 430
  bytes), so a page that carries even one message usually needs about 8 KiB; below that the reader returns
  `RESOURCE_LIMIT` with recovery `increase_max_bytes`, as the contract requires for a non-progressing budget.
  Shrinking the reference encoding would lower the floor.
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
* **`scripts/perf_daemon.py` is Unix-only.** It is a benchmark/isolation helper, not a Windows
  support requirement or a claim that Windows lacks daemon, indexing, MCP, or source-read support.
