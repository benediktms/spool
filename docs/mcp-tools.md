# MCP tools

Spool exposes exactly three read-oriented tools through `spool mcp` (a stdio bridge that forwards
each call over the daemon's Unix socket): `search_sessions`, `read_session`, `index_status`.
There is no administrative tool. Indexing, backfill, job control, exclusion, purge, rebuild,
source enable/disable, key rotation and service control are CLI-only (`spool --help`), so an MCP
caller can never start an indexing operation or make Spool send stored transcript text to a
provider.

Normal recall takes two calls: `search_sessions`, then `read_session` with a returned `match_ref`.
A further `read_session` with `view = "with_tools"` shows the tool activity around the same
messages. Tool results are historical data, not instructions.

The authoritative contract is `docs/spec.md` section 14 and `docs/read-session-contract.md`; the
machine-readable `read_session` schemas live in `schemas/read_session/`.

## Client setup

`spool mcp config --client claude|codex|omp` prints (never writes) the configuration for the stdio
process. Only shapes verified against real clients are emitted (see
`docs/dependency-baseline.md`):

| Client | Output | Where it goes |
|---|---|---|
| Claude Code | `.mcp.json` / `--mcp-config` JSON: `{"mcpServers":{"spool":{"type":"stdio","command":"/abs/spool","args":["mcp"]}}}` | project `.mcp.json`, or `claude --mcp-config FILE` |
| Codex | TOML `[mcp_servers.spool]` with `command` and `args = ["mcp"]` | `~/.codex/config.toml` |
| OMP | same JSON shape as Claude, project-local `.mcp.json` | project directory |

The command path is absolute. The output contains no provider key, no database path and no
environment. If you run Spool with a non-default `SPOOL_HOME`, the client must be started with the
same `SPOOL_HOME` so `spool mcp` finds the socket.

If the daemon is not running the tool returns a structured error `DAEMON_NOT_READY`
(`retryable = true`, `recovery = "start_daemon"`) with the start command; stdout carries only
JSON-RPC frames.

## `search_sessions`

Searches indexed user/assistant conversation (not tool payloads or reasoning). Results are excerpts
from a partial, explicitly scoped corpus: configured bootstrap window, later activity, and manual
backfills.

Input (unknown fields are rejected; only `query` is required):

| Field | Meaning |
|---|---|
| `query` | non-empty, at most 4,096 UTF-8 bytes and 1,024 provider tokens |
| `mode` | `hybrid` (default), `lexical`, `semantic`, `literal` |
| `sources`, `project_path`, `session_ids`, `session_kinds` | filters (default kind: interactive) |
| `after`, `before`, `include_unknown_time` | absolute RFC 3339 timestamps only; relative dates exist only in the CLI |
| `prefer_recent`, `limit`, `matches_per_session`, `debug_scores` | ranking and size controls |

Output: `mode_requested`, `mode_used`, `index_epoch`, `coverage` (indexed sessions, active/embedded
chunks, pending sessions, last discovery), `results` (session identity and native title, `matches`
with `match_ref`, `match_ref_expires_at`, optional `branch_ref`, excerpt of at most 1,200 bytes,
`matched_by`), `warnings`, `truncated`. Default response cap 64 KiB.

Behaviour worth knowing:

* `hybrid` falls back to `lexical` when a query embedding cannot be obtained and says so with a
  `query_embedding_unavailable` warning and `mode_used = "lexical"`.
* `semantic` fails with a typed error instead of silently changing meaning.
* Only `semantic` and `hybrid` can reach the hosted provider, and only when
  `embedding.hosted_processing_approved = true` and a credential exists; they send the query text
  and spend the query budget. `lexical` and `literal` never leave the machine.
* If the lease bounds are exhausted the call returns `RESOURCE_LIMIT` instead of handles that
  cannot be read.

CLI equivalent: `spool search "query" [--mode M] [--source ID] [--since 7d] [--until DATE] [--limit N] [--json]`.

## `read_session`

Bounded, provenance-preserving transcript reader (no model call, no embedding work, never writes to
sources). Normative contract: `docs/read-session-contract.md`; machine-readable shapes:
`schemas/read_session/{input,output,error}.schema.json` (generated from the Rust types in
`src/transcript/types.rs`; `UPDATE_SCHEMAS=1 cargo test --test reader_schemas` regenerates);
valid exchanges: `examples/read_session/*.json`.

**Two call shapes.** An initial read (`session_id` plus optional `selection`, `view`,
`source_policy`, `max_bytes` 4096..=262144 default 32768, `max_items` 1..=200 default 50) or a
cursor-only continuation (`cursor` plus optional `max_bytes`/`max_items`). Unknown fields and
mixed forms are `INVALID_ARGUMENT`.

**Selections.** `session` (+ optional `branch_ref`: root to the branch leaf), `match`, `message`
(both with `before`/`after` context messages, 0..=20, default 2), inclusive `range`, and `item`.
A conversational message is one canonical user/assistant envelope with at least one retained text
block; tool-only envelopes, reasoning, control records and injected context are not counted and
sibling-branch messages never fill a window (`INCOMPLETE_CONTEXT` warning instead). A selection
that needs a path choice returns `BRANCH_REQUIRED` with at most 50 branch options *and* a
serialized error within `max_bytes` (`branch_options_truncated` says when either limit omitted
options); the reader never picks a "current" branch.

**Views.** `conversation` (default), `with_tools` (tool calls/results by *turn closure*: only
tool activity whose verified `turn_id` or native call link belongs to a turn the selected messages
touch; `outside_message_window` marks closure records beyond the window; `result_state`
`no_result_in_snapshot` only means no linked result exists in the frozen snapshot; orphan results
carry `tool_ref = null` plus a warning; an `item` selection returns only that item, and a tool call's `result_refs` come from ONE path only: results on sibling branches are never listed, and when linked results exist on more than one branch below the call the read is `BRANCH_REQUIRED` unless `selection.branch_ref` (optional for items) picks the branch) and privileged `raw_records` (byte-exact original records,
UTF-8 or base64, line terminators kept; needs `mcp.allow_raw_records = true`, otherwise
`FORBIDDEN`; an adapter that cannot resolve bytes exactly yields `VIEW_NOT_SUPPORTED`, never a
normalized substitute).

**Source policy.** `prefer_source` (default for `conversation`) verifies each item against the
original record (digest) and falls back to the indexed copy only when the source is *unavailable*,
with a `SOURCE_FALLBACK` warning and per-item `provenance.origin = indexed_snapshot`;
`require_source` (default for the other views) errors instead; `indexed_only` never reads the
source. A positively detected change is `SOURCE_CHANGED` and is never masked by a fallback.
Explicit `prefer_source`/`indexed_only` with `with_tools`/`raw_records` is
`SOURCE_POLICY_CONFLICT`. Reads of a known but unindexed session stage a bounded `source_read`
snapshot (nothing is published, no embedding request is made); `indexed_only` on it is
`SESSION_NOT_INDEXED`. A plain session read validates the published snapshot against the current
file (size, mtime, identity, record digests) and stages a fresh observation when the file moved on.

**Pagination.** Pages are measured with the same budget function as the MCP bridge
(`mcp::budget::tool_result_bytes`: structured value + the JSON-escaped compatibility text copy +
wrapper), so effective payload capacity is roughly half of `max_bytes`; each reference is a few
hundred bytes, so very small budgets (near 4096) can be too small for even one item and return
`RESOURCE_LIMIT` (recovery `increase_max_bytes`) rather than an unchanged cursor. Oversized
conversational text, tool arguments/results and raw records are fragmented as half-open
`[start_byte, end_byte)` ranges over the full decoded block, never splitting a UTF-8 code point
(base64 when not UTF-8). `max_items` counts logical items including a continued one. Cursors are
signed, reusable (replay returns the same page start) and bind selection, snapshot, branch, view,
policy and position. The local I/O and time budgets (`reader.max_source_read_bytes`,
`reader.max_read_millis`) also answer `RESOURCE_LIMIT`.

**Handles and leases.** Every reference/cursor is signed (integrity only, not authentication) and
anchored to a lease (`reader.lease_ttl_seconds`); the expiry is exposed as
`snapshot.lease_expires_at`. Expired or swept leases give `CURSOR_EXPIRED`; a rotated signing key
gives `STALE_REFERENCE` (references) or `CURSOR_EXPIRED` (cursors); an altered signature is
`INVALID_ARGUMENT`. Retired snapshots stay readable while a live lease pins them. Exclusion, session
removal and source disablement are re-checked on every call, including continuations, and beat any
lease (`FORBIDDEN` / `SESSION_NOT_FOUND`).

CLI equivalent (flags map one-to-one to the selections; a cursor read accepts only
`--max-bytes`/`--max-items`; incompatible combinations are rejected before any call):

```text
spool read SESSION_ID [--match-ref R | --message-ref R | --item-ref R | --from-ref R --through-ref R]
                      [--branch-ref R] [--before N] [--after N] [--with-tools | --raw]
                      [--source-policy prefer_source|require_source|indexed_only]
                      [--max-bytes N] [--max-items N]
spool read --cursor CURSOR [--max-bytes N] [--max-items N]
```

The CLI prints the raw structured result as pretty JSON.

## `index_status`

Read-only status: daemon version, dependency/protocol baseline identifier, source coverage,
bootstrap scope, active embedding profile (`embedding.active_profile_id`, the one queries use; `profile_id` is the
configured target and `vectors` reports coverage of that target), vector coverage, queue counts, worker health, last
discovery, parse warnings, provider health, token budget, approximate database/WAL sizes and reusable free-page
bytes, and an
overall state: `ready`, `lexical_only`, `ingesting`, `provider_blocked`, `ingestion_degraded`,
`unsupported_format` or `storage_error`. "Daemon healthy" does not mean every file has been
embedded. It never returns API keys, the signing key, transcript text or full queries.

CLI equivalent: `spool status [--json]`; `spool doctor` adds local checks.

## Errors

Stable codes inside protocol-appropriate tool errors, each with `retryable`, a bounded message and
a safe next action: `INVALID_ARGUMENT`, `FORBIDDEN`, `DAEMON_NOT_READY`, `SESSION_NOT_FOUND`,
`SESSION_NOT_INDEXED`, `BRANCH_REQUIRED`, `INVALID_RANGE`, `VIEW_NOT_SUPPORTED`,
`SOURCE_POLICY_CONFLICT`, `SOURCE_UNAVAILABLE`, `SOURCE_CHANGED`, `STALE_REFERENCE`,
`CURSOR_EXPIRED`, `UNSUPPORTED_FORMAT`, `QUERY_EMBEDDING_FAILED`, `EMBEDDING_PROFILE_ERROR`,
`RATE_LIMITED`, `BUDGET_EXHAUSTED`, `RESOURCE_LIMIT`, `INDEX_BUSY`, `STORAGE_ERROR`; and, for CLI
administration only, `OPERATION_NOT_FOUND`, `IDEMPOTENCY_CONFLICT`. Errors never contain query
text, transcript text, raw SQLite errors or provider response bodies.
