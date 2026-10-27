# End-to-end smoke results

The 2026-10-12 macOS arm64 runs use the real `spool` binary built from this repository, an isolated
`SPOOL_HOME` under `/tmp`, and **synthetic** session files only (`fixtures/omp/*` and a small hand-written OhMyPi
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

## Native Windows smoke (2026-10-25)

Observed on Windows x64 with the MSVC Rust 1.98.1 toolchain. The real debug executable used an
isolated `SPOOL_HOME`, synthetic OhMyPi transcripts, hosted processing disabled, and paths containing
spaces and non-ASCII characters. No persistent task/service or MCP client configuration was installed.

Passed through the actual CLI and stdio MCP bridge:

* `init`, foreground daemon readiness, and rejection of a second daemon using the same state.
* `index --all --wait`, lexical search, and source-verified reads containing the expected conversation.
* MCP initialization with protocol `2025-11-25`, all three advertised tools, search, and read.
* `doctor --json`, cooperative shutdown through the protected Windows event, restart against the
  existing index, and the same CLI/MCP results after restart.
* A configured endpoint identity longer than 100 characters: CLI, MCP, and shutdown all used it;
  the Unix socket-path limit does not apply to Windows named pipes.
* Read-only PowerShell ACL inspection of 14 state paths: ownership matched the current user, only
  that SID and SYSTEM appeared in access rules, and directory DACLs were protected from inheritance.
* `init` and `config show` with `HOME` and `SPOOL_HOME` removed: state was placed under an isolated
  `LOCALAPPDATA\Spool`, and suggested source roots expanded under the isolated `USERPROFILE`.
* The real read-only Task Scheduler COM query through `doctor`, with no task installed.

The Windows source regression exercised ordered pagination, cancellation, rename-stable file identity,
and junction escape rejection. Existing cross-harness, hosted-consent (loopback fake provider),
MCP restart/cancellation, and embedding-profile-migration end-to-end targets also passed on Windows.

The real generated PowerShell launcher also passed with a copied executable and captured home containing
Unicode, spaces, apostrophes, `&`, and `%`. Its parent supplied a different `SPOOL_HOME`; the daemon
still used the captured home, became ready, stopped through the service manager's cooperative event
path, and left its endpoint unreachable. Task registration was intercepted by the recording runner.

This smoke found and fixed three native-runtime defects: an async future captured a raw event handle
without retaining its RAII owner, synchronous native relative file opens lacked an explicit
`SYNCHRONIZE` access right, and suggested source roots still required `HOME` on Windows. Regressions
for event lifetime, source traversal, and Windows home expansion remain in the suite.

Final native checks:

* `cargo clippy --locked --all-targets --all-features -- -D warnings` passed.
* `cargo test --locked --no-fail-fast --lib --test cli --test service --test source --test daemon_socket`
  passed all 66 tests, including the real PowerShell launcher and `HOME`-unset initialization.
* `cargo test --locked --no-fail-fast --test daemon_socket --test discovery --test ingest --test reader --test control`
  passed all 116 tests.
* The `e2e`, `e2e_cross_harness`, `e2e_hosted`, `e2e_mcp_edge`, `e2e_profile_migration`, and
  `mcp_roundtrip` targets passed all 10 tests. The full suite remains a CI check.

Verification limits: actual Task Scheduler registration, logon startup, and persistent task lifecycle
were not exercised. Task rendering, quoting, command sequencing, and foreign-task protection are
covered with a recording runner. This run did not repeat macOS/Linux runtime verification or test
the real Claude Code, Codex, or OMP client binaries on Windows.

## User-requested Windows installation and OMP connection (2026-10-25)

After the user explicitly requested installation, `cargo install --path . --locked` installed
`%USERPROFILE%\.cargo\bin\spool.exe`. The OMP user-level `~/.omp/agent/mcp.json` gained only the
`spool` entry; existing MCP servers were preserved. The per-user Task Scheduler task was registered
and left running. Only OMP was enabled, using the normal seven-day bootstrap; hosted processing
remained disabled.

Observed against the installed release executable:

* Native task registration, service start, cooperative stop, start after upgrade, and restart passed.
* `doctor --json` passed, including private ACLs, the registered/running service, and the log directory.
  `spool logs` found the scheduled daemon's output with no explicit `SPOOL_HOME` in the CLI.
* OMP 18.12.6, through its real RPC local `/mcp test spool` command, reported a connected server with
  `search_sessions`, `read_session`, and `index_status`. `agentInvoked` was false: no model turn ran.
* A real stdio MCP `index_status` call reached the running daemon and confirmed hosted processing was
  not approved. Verification inspected metadata, not private transcript contents.
* All-target/all-feature Clippy and the CLI/service targets passed (39 tests).

Actual registration exposed two handoff defects missed by earlier mock tests: keeping the temporary
XML writer open prevented the scheduler from consuming it, and its XML encoding needed a matching
UTF-16LE BOM/declaration. Native validation also enforces the one-minute minimum restart interval.
The registration-input regression now consumes the closed file, and the Windows launcher regression
uses the real scheduler's `TASK_VALIDATE_ONLY` path without creating a persistent test task.
Windows log discovery also now uses the same root whether `SPOOL_HOME` is captured by the task or
omitted by an ordinary CLI.

Not exercised: an actual logout/login trigger or uninstall of the user's installed task.

## Console-free Windows service startup (2026-10-27)

The PowerShell startup action was replaced by the native GUI-subsystem `spool-service.exe`.
The helper starts `spool.exe daemon run` with `CREATE_NO_WINDOW`, captures the configured absolute
home, waits for the daemon, and returns its exit status. Captured service-manager commands also use
`CREATE_NO_WINDOW`; the ordinary CLI remains a console executable.

Observed on Windows 11 x64:

* `cargo fmt --all`, all-target/all-feature Clippy with warnings denied, and the release binary build
  passed. All three Windows service tests passed, including native scheduler XML validation,
  foreign-task protection, and a real helper run through Unicode and shell-special-character paths.
* An isolated, hosted-disabled native-helper smoke became ready despite an incorrect parent
  `SPOOL_HOME`, shut down through the protected event, returned exit status zero, and left the
  endpoint unreachable.
* Win32 probes found `GetConsoleWindow() == NULL` for both helper and daemon. A headless daemon can
  still have an attachable console object; the acceptance check is absence of a console window,
  not failure of `AttachConsole`.
* Both binaries were installed in `%USERPROFILE%\.cargo\bin`. The user's existing managed task was
  updated to invoke that helper directly, became running, and passed `doctor --json`. Probes of
  the actual scheduled helper and daemon again found no console window. The obsolete generated
  `service\daemon.ps1` was removed; configuration, provider key, and MCP connections were preserved.
* An `index_status` call through the existing OMP MCP connection reached the upgraded daemon and
  reported `ready`, with no pending embeddings or degraded workers.

Not exercised: logout/login triggering or service uninstall. A separate review check exposed an
existing Windows credential-fixture failure in
`from_config_with_open_gate_embeds_through_the_listener_and_status_is_ready` (`NoCredential` instead
of `Ready`); the full suite is not claimed passing by these startup checks.

## Upstream rebase verification (2026-10-27)

Updated `main` from `40ece89` to `50d67e6` (disabled-subagent backfill counting) and restored all
uncommitted Windows changes without conflicts.

* Formatting and all-target/all-feature Clippy with warnings denied passed.
* The `cli`, `control`, `daemon_socket`, `mcp_roundtrip`, `service`, and `source` targets passed
  all 64 tests, including upstream's per-source subagent-policy regression.
* The rebuilt release binaries passed an isolated, hosted-disabled smoke with one recent
  interactive transcript, one older interactive transcript, and one disabled subagent. Normal
  bootstrap admitted only the recent transcript; explicit `index --since 30d --wait` admitted the
  older interactive transcript. Lexical search and match-reference reads returned the expected
  conversation text, excluded tool payloads, and did not return the disabled subagent.
* The native helper and daemon again had no console window. Protected-event shutdown returned
  zero and left the isolated endpoint unreachable.
* Reinstalled both binaries; installed SHA-256 hashes matched the rebuilt release files. The
  managed task was refreshed, started, and left running. `doctor --json` passed, both scheduled
  processes had no console window, and the existing OMP MCP connection reported `ready`, provider
  `ready`, no pending vectors, and healthy workers.

The earlier review findings remain unchanged; this was not a full-suite or macOS/Linux runtime run.
