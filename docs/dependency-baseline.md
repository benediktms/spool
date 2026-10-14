# Dependency baseline (Milestone 0)

> Historical evidence note: the Milestone 0 proofs below ran when only `index_status` was exposed. The shipped surface is now exactly `search_sessions`, `read_session` and `index_status`; see `docs/smoke-results.md` for the current end-to-end client runs against the full daemon.

Observed 2026-10-12 on macOS (Darwin arm64), rustc/cargo 1.96.0. Everything below was run, not
copied from documentation. `Cargo.lock` is written and pins the set (nothing is committed yet; no git commit has been made).

**Verification scope:** all proofs below ran in a scratch copy that excluded the adapter worker's in-progress modules (`adapters/{omp,codex,claude}.rs`, `graph.rs`, `source.rs`, `ingest/chunk.rs`). They have not yet been repeated in the real repository tree; that repeat is required once those modules are integrated.

## Pinned versions (from `Cargo.lock` / `cargo metadata`)

| Crate | Version | Notes |
|---|---|---|
| `apalis` | 1.0.0 (stable) | features: `tracing retry limit timeout catch-panic` (default-features off) |
| `apalis-core` | 1.0.0 | transitive |
| `apalis-sqlite` | 1.0.0 (stable) | default features (`tokio-comp migrate json`) |
| `sqlx` | 0.9.0 | features `sqlite runtime-tokio migrate`; same major as `apalis-sqlite`'s requirement (`sqlx 0.9.0`) |
| `sqlx-sqlite` / `libsqlite3-sys` | 0.9.0 / 0.37.0 (bundled SQLite 3.51.3) | **one** `libsqlite3-sys` in the graph |
| `tokio` | 1.53.2 | |
| `rmcp` | 3.5.1 | features `server client macros schemars transport-io transport-child-process` (`client` + child-process only exercise it in tests; the shipped bridge uses server + `transport-io`) |
| `rustix` | 1.x | `fs std process` (flock, openat, geteuid) |

All are stable releases; no prerelease is used. (Spec section 2 noted that retrieved docs showed
differing release candidates; `apalis-sqlite 1.0.0` / `apalis 1.0.0` are published stable.)

## SQLite linkage and FTS5

* `cargo tree -i libsqlite3-sys` lists exactly one package, `libsqlite3-sys v0.37.0`, reached only
  via `sqlx-sqlite v0.9.0` (consumers: `sqlx`, `apalis-sqlite`, `sqlx-macros-core`). `cargo tree -d`
  shows no duplicate `libsqlite3-sys`. No `rusqlite` is in the graph.
* Feature chain: `sqlx/sqlite` enables `sqlx-sqlite/bundled` -> `libsqlite3-sys/bundled`, so the SQLite
  library is built from source inside the binary, not the system library.
* `tests/storage_fts.rs` (passes): through SQLx it opens a WAL database, runs the startup capability
  probe (`storage::check_fts5`, creates a temp FTS5 table with `unicode61 remove_diacritics 2`,
  inserts, MATCHes), creates the spec's external-content `chunks_fts`, runs a bm25-ordered query and
  `integrity-check`, and asserts `PRAGMA synchronous = FULL`, `journal_mode = wal`,
  `foreign_keys = 1` on pool connections. Observed `sqlite_version() = 3.51.3`.

## Apalis behaviour that the design depends on (read from `apalis-sqlite 1.0.0` source and observed)

* `SqliteStorage::setup` runs `PRAGMA synchronous = OFF` (and other pragmas) on one pooled connection,
  contradicting the spec's `synchronous = FULL`. Spool therefore does **not** call it; it runs
  `SqliteStorage::<()>::migrations().run(pool)` only (`src/jobs/backend.rs`), on a pool whose every
  connection is configured by `storage::connect`.
* Native uniqueness: `TaskBuilder::idempotency_key(..)` exists (`examples/unique_jobs.rs`). Used for
  coalescing later; correctness still does not depend on it (spec 11.3).
* Worker liveness settings (`apalis_sqlite::Config`): `heartbeat_interval` (default 30 s),
  `missed_heartbeats` (default 2), `batch_size` (default 10; Spool uses 1), `poll_interval` via
  `poll_with_interval`. Orphan detection: every heartbeat a live worker re-enqueues Running/Queued
  tasks whose lock owner's `last_seen` is older than `heartbeat_interval * missed_heartbeats`
  (seconds, integer). Re-enqueue sets the task `Pending`, `attempts = attempts + 1`.
* Registration (`register`): first re-enqueues orphans locked by a worker with the **same name**
  whose `last_seen` is at least `heartbeat_interval` old, then inserts the worker. Registering a
  worker name whose `last_seen` is not older than `heartbeat_interval` fails with
  `WORKER_ALREADY_EXISTS` (the transaction rolls back). The supervisor must therefore retry
  registration; the test does exactly that.
* Retry semantics: Spool wraps the index handler with Apalis' `retry` layer (`scheduler::IngestRetry`,
  5 attempts, 5 s base, 15 min cap, jitter, `Retry-After` floor); the backend itself applies no delay on a
  failed handler. `tests/jobs.rs` observes the layer's timing.

## Durable job kill/recovery proof (`tests/recovery.rs`, passes)

The worker runs in a child process (the test binary re-executed). A task is pushed to `jobs.sqlite`
before any worker exists; child 1 claims it, writes a marker and blocks; the test `SIGKILL`s child 1
(`Child::kill`) and asserts the job is stuck `Running`; child 2 starts and the job must complete.
Liveness used in the test: `heartbeat_interval = 1 s`, `missed_heartbeats = 2`, poll 200 ms.

Observed output:

```
second_worker=spool-ingest   recovered_in=1.60s status=Done attempts=2   (runs: attempt=1 pid A, attempt=2 pid B)
second_worker=spool-ingest-2 recovered_in=2.26s status=Done attempts=2   (runs: attempt=1 pid A, attempt=2 pid B)
```

First variant: same worker name, recovered by registration (after the supervisor retry loop gets past
`WORKER_ALREADY_EXISTS`). Second variant: differently named worker recovers it through the
heartbeat of the new worker (`heartbeat * missed` = 2 s). The job executed exactly twice (kill +
recovery), which is the at-least-once behaviour the spec requires handlers to tolerate.
With production defaults (30 s x 2) the second path would take about 60 s, within the spec's
two-minute target; the default same-name path takes at most about 30 s plus restart time. Production
values and the supervisor are finalized in M4.

## stdio MCP -> IPC -> daemon round trip

Path: MCP client -> stdio -> `spool mcp` (rmcp server, `src/mcp/bridge.rs`) -> length-prefixed JSON
over the owner-only Unix socket (`src/ipc/`) -> `spool daemon run` (`src/daemon.rs`) -> `index_status`.
No HTTP listener or client exists. At this milestone the bridge exposes only `index_status`
(the other two tools were added later with their real implementations).

`tests/mcp_roundtrip.rs` (passes): starts the real daemon binary in an isolated `SPOOL_HOME`,
asserts modes (root/state dirs 0700; socket, `index.sqlite`, `jobs.sqlite` 0600), then an rmcp stdio
client spawns `spool mcp`, lists tools (`["index_status"]`) and calls `index_status`; result observed:

```
{"baseline":"apalis-sqlite-1.0.0+sqlx-0.9.0+rmcp-3.5.1","daemon_version":"0.1.0","fts5":true,"queue":{"queued":0},"sqlite_version":"3.51.3"}
```

### Protocol negotiation with the SDK (raw initialize, `raw_handshake_records_negotiated_versions`)

| Client requests | `spool mcp` answers |
|---|---|
| 2024-11-05 | 2024-11-05 |
| 2025-03-26 | 2025-03-26 |
| 2025-06-18 | 2025-06-18 |
| 2025-11-25 | 2025-11-25 |
| 2026-07-28 | 2025-11-25 (via `initialize`; see Claude Code row for the stateless flow) |
| 2099-01-01 | 2025-11-25 |

Server discovery answer (`server/discover`) lists `supportedVersions`:
`2024-11-05, 2025-03-26, 2025-06-18, 2025-11-25, 2026-07-28`.

### Real clients (each ran a real model turn that called the tool through a tee wrapper that recorded the frames)

Isolated, nothing persisted in the harnesses' configuration: Claude via `--mcp-config ... --strict-mcp-config`,
Codex via transient `-c mcp_servers.spool.*` overrides, OMP via a project-local `.mcp.json` in a temp
directory. The daemon used an isolated `SPOOL_HOME` under `/tmp`. Tool called: `index_status` only; no
transcript data was involved.

| Client | Version | Negotiated protocol (observed frames) | Result |
|---|---|---|---|
| Claude Code | 2.1.294 | **2026-07-28**, stateless: `server/discover` probe, then `tools/list`, `tools/call` with per-request `_meta` (no `initialize`) | tool returned structured result; final answer echoed the baseline string |
| Codex CLI | 0.162.1 | `initialize` with `2025-06-18`; server answered `2025-06-18` | same |
| OMP | 18.10.1 | `initialize` with `2025-11-25`; server answered `2025-11-25` | same |
| rmcp client (test) | 3.5.1 | `2025-11-25` | same |

Also observed: with the daemon stopped, Claude Code received a structured tool error
`DAEMON_NOT_READY` (`retryable = true`, `recovery = "start_daemon"`) with the exact start command,
and `spool mcp` stdout carried only JSON-RPC frames (diagnostics on stderr).

## Caveats recorded honestly

* The rmcp `tracing` INFO lines from the bridge go to stderr; they are not content.
* Recovery test timings use a 1 s heartbeat; production heartbeat/missed values are not tuned beyond the defaults.
* Cancellation, large-result pagination and daemon reconnection with raw recorded-shape frames of the pinned client negotiation paths are now covered by `tests/e2e_mcp_edge.rs`; real client binaries do not expose a scripted cancel (see `docs/known-gaps.md`).
