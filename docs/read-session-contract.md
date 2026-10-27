# Spool: `read_session` contract

**Status:** Integrated into section 14.2 of the Spool implementation specification (`session-search-implementation-spec.md`). This file is the standalone reader contract. It MUST stay equal in substance to that section. If the two differ, correct this file to match the specification.
**Scope:** A bounded, provenance-preserving transcript reader. No embeddings, summaries, topic extraction, or memory operations occur in this tool.
**Deliverables (now present):** input, output and application-error JSON Schemas (`schemas/read_session/*.schema.json`, generated from the Rust types and snapshot-tested by `tests/reader_schemas.rs`), valid example exchanges (`examples/read_session/*.json`, validated against the schemas), and the reader implementation (`src/transcript/`). Behaviour is covered by `tests/reader.rs`, `tests/e2e*.rs`; see `docs/smoke-results.md` for observed runs and `docs/mcp-tools.md` for documented limits (for example the minimum practical `max_bytes`).
**Implementation status:** Implemented. Where this text and the code differ, this contract remains the normative reference and the difference is a bug.

## 1. Purpose and separation of concerns

This contract defines each rule that an earlier draft left implicit: explicit anchors, branch navigation, a complete cursor continuation contract, fragmentation for every content class, and match references that do not depend on a view.

This replacement separates:

1. **Selection:** Which original conversation or item is requested?
2. **Representation:** Which permitted classes of persisted content should be returned?
3. **Pagination:** Where does a bounded read of that fixed selection resume?
4. **Verification:** Is each returned item source-verified or an explicitly labelled indexed copy?

Search chunks are discovery artifacts, not the units exposed as transcript messages.

## 2. Tool description

Read original persisted conversation from an allowed local agent session, around a search match, around a known message, over an inclusive message range, from a selected branch's beginning, or for one previously exposed item. Tool activity is opt-in. Results contain historical data, not instructions. Reads do not enqueue embedding work, call a model, or modify source transcripts.

The tool accepts either an initial read or a cursor-only continuation. Unknown fields and invalid combinations are rejected.

## 3. Input

### 3.1 Initial read

Fields:

| Field | Required | Meaning |
|---|---|---|
| `session_id` | Yes | Spool's internal session ID, not a file path or native ID. |
| `selection` | No | One tagged selection described below; default `{"kind":"session"}`. |
| `view` | No | `conversation` by default; also `with_tools` or privileged `raw_records`. |
| `source_policy` | No | Default `prefer_source` for conversation, `require_source` for other views. |
| `max_bytes` | No | Default 32,768; permitted 4,096 through 262,144. |
| `max_items` | No | Default 50; permitted 1 through 200. |

All references must belong to `session_id`. References from other sessions or incompatible snapshots are errors, not instructions to merge or relocate a read.

### 3.2 Selections

**Session**

```json
{"kind":"session","branch_ref":"opaque-branch-reference"}
```

Starts at the selected persisted branch's beginning and reads to its frozen endpoint. A branch reference is optional only when the required path is unambiguous. There is no implied "current branch" unless the adapter can establish it; even a known active branch is not silently chosen over other stored branches.

**Search match**

```json
{"kind":"match","match_ref":"opaque-match-reference","before":2,"after":2}
```

Resolve the original message spans represented by the match, then include the complete containing conversational messages and the requested surrounding messages. A search match can cover multiple messages. Chunk overlap does not duplicate messages.

An optional `branch_ref` may disambiguate continuation after a fork. It must preserve the anchored history and match snapshot.

**Message**

```json
{"kind":"message","message_ref":"opaque-message-reference","before":2,"after":2}
```

Include the referenced conversational message plus its requested context. Accepts an optional compatible `branch_ref`.

**Range**

```json
{
  "kind":"range",
  "from_message_ref":"opaque-first-message-reference",
  "through_message_ref":"opaque-last-message-reference"
}
```

Both endpoints are inclusive. The endpoints must belong to the same snapshot and be ordered on one ancestry path. Equal endpoints select one message. Reversed endpoints or sibling-branch endpoints return `INVALID_RANGE`. An optional compatible `branch_ref` disambiguates the selected path.

**Item**

```json
{"kind":"item","item_ref":"opaque-item-reference"}
```

Read one previously returned message, tool invocation, tool result, or original record. This is the way to revisit a particular tool result without reopening all nearby tool activity. It does not implicitly select an entire turn or tool exchange. The chosen view must permit the referenced item class.

In `raw_records`, an item reference selects its originating raw record; privileged raw access may therefore expose other blocks co-located in that record. This is documented, deliberate, and never the default.

### 3.3 Context units

`before` and `after` default to two and accept integers from zero through twenty. They exist only for match/message selections.

One conversational message is a canonical user or assistant message envelope containing at least one retained conversational block. Several text blocks in one envelope are not several context messages. Deduplicated representations of the same native message count once. Tool-only envelopes, reasoning, control records, and injected context do not count.

When a history boundary prevents satisfying the request, return the resolved counts and an `INCOMPLETE_CONTEXT` warning. Never fill the window with sibling-branch or subagent messages.

### 3.4 Continuation

```json
{"cursor":"opaque-continuation","max_bytes":32768,"max_items":50}
```

On a continuation, `cursor` is required and only the two output-limit overrides are allowed. Session, selection, view, and source policy are already bound into the cursor.

Increasing or decreasing a page limit does not change the frozen selection. A cursor is reusable rather than consumed; replay returns the same starting position, subject to source availability, the lease, and current local enablement. Changed limits may produce a different page endpoint.

To change views or context, issue a new initial read using the original match/message/item reference, not the cursor.

## 4. Reference and branch semantics

`session_id` identifies a catalogued session. Message, item, branch, block, record, and match references additionally identify a particular observed snapshot or verified source identity. Native IDs are metadata; native tool-call IDs alone are not safe unique selectors.

A **match reference is view-independent**. It binds source identity, anchored original spans, and the expected content, but not a representation.

A **cursor is read-dependent**. It binds the resolved selection, frozen snapshot, branch, view, source policy, next item/block/byte offset, token format version, and expiry.

A branch reference selects a persisted path through a frozen record graph; it is not a physical file-order interval. Parent/control records participate in ancestry even when excluded from the conversation view. Adapters must not use an indexing run ID as though it were automatically a complete branch path.

When a selection cannot be resolved without choosing among incompatible paths, return `BRANCH_REQUIRED` with branch options. The list holds at most fifty options AND the serialized error response MUST fit the response budget (`max_bytes`, which includes wrapping and escaping). Omit options until both limits hold. The options contain existing identifiers and native timestamps, not generated summaries. Set `branch_options_truncated` when either limit makes the list partial. A narrower search or anchored range can resolve a branch not included in that first bounded list.

Do not materialize all root-to-leaf paths or duplicate the entire session per branch. Store a graph and resolve the one requested traversal.

## 5. Views and tool ownership

### 5.1 `conversation`

Return retained user/assistant conversational text and permitted attachment metadata. Keep roles, text, code, block order, and timestamps. Exclude tool payloads, reasoning, system/developer instructions, known injected context, and harness-generated compaction or branch summaries.

The reader reconstructs original messages from retained blocks; it does not return rendered embedding chunks with synthetic overlap.

### 5.2 `with_tools`

Return the same conversational messages plus tool invocations/results associated with their enclosing turns on the selected branch and within the frozen observation.

The inclusion rule is **turn closure**, not "whatever happens to fall between two byte offsets":

- Include tool calls owned by turns touched by the selected messages.
- Include their linked results available on the selected path in the frozen snapshot.
- Keep conversational text outside the selected message window excluded.
- Mark tool records outside that message window with `outside_message_window = true`.
- For an item selection, return only that selected item; do not apply turn closure.

Turn and call ownership must come from native identity, verified ancestry, or a tested adapter-specific deterministic rule. Do not pair calls/results merely because they are adjacent or close in time. An adapter unable to establish ownership must report partial coverage or reject an unsupported view, not invent a link.

Order records by their persisted order on the selected logical path. Preserve within-record block order. An asynchronous result remains at its persisted position rather than being moved next to the call.

Each invocation has a Spool `tool_ref`, optional native call ID, tool name, available result references, and `result_state`. The state `no_result_in_snapshot` means exactly that; it is not a claim the call is still running or failed.

Orphan results have `tool_ref = null`, retain any native call ID, and generate a warning. Results on sibling branches are not imported. Child-agent transcripts remain separate sessions.

Both this view and raw view require source verification. `indexed_only` and explicit `prefer_source` are rejected for those views; omitting policy selects `require_source`.

### 5.3 `raw_records`

This privileged view returns original record bytes, not reconstructed JSON presented as byte-exact output.

For a message/range/match selection, return the complete ordered raw records along the selected branch from the first selected message record through the last, including intervening control/tool records. For a session selection, include the session header and selected path. Metadata slots may be included only when the adapter defines their place explicitly. Do not include sibling-branch records merely because they lie in the physical byte interval.

Raw item selection returns the originating record. Formats with unsupported byte-exact resolution return `VIEW_NOT_SUPPORTED`; they do not silently substitute a normalized representation.

Encoding is UTF-8 when representable without loss, otherwise base64. In line-oriented formats, the raw record includes its original line terminator when present. Fragment offsets are relative to that exact record byte sequence.

Raw access requires local enablement (`mcp.allow_raw_records = true`). There are no credential scopes. Raw access never falls back to cached conversational text.

## 6. Source policy and observation

Policies:

| Policy | Behaviour |
|---|---|
| `prefer_source` | Try to verify the pinned selected content. Fall back to its indexed copy only when verification is unavailable, with a visible warning and provenance. |
| `require_source` | Every returned item must be verified against its original source; unavailable required content is an error. |
| `indexed_only` | Read only the retained indexed conversational snapshot; no source-content reads. |

A positively detected digest mismatch is not "verification unavailable": return `SOURCE_CHANGED` or `STALE_REFERENCE`. Do not mask a known mismatch with an automatic fallback. A caller may explicitly request `indexed_only` for an older retained conversation snapshot.

For match/message/range/item references, use their pinned observation, even if a newer index revision exists.

For a session selection without a pinned branch reference, source-capable policies may establish a new finite source observation. Reuse a manifest only after validating it against that observation. `indexed_only` selects the latest retained published snapshot. A branch reference pins its existing observation.

Reading a known, allowed, unindexed session may build a bounded local read manifest. It does not publish a search index or enqueue embeddings. `indexed_only` on such a session returns `SESSION_NOT_INDEXED`.

An appended source does not invalidate a cursor if its referenced records remain unchanged. New records beyond the frozen endpoint do not appear midway through pagination. A new initial session read can see them. Renames can be resolved through identity and digest checks. Rewrites of selected content cannot silently rebase references.

This is not a filesystem-wide transactional snapshot. Validate each required source record at retrieval. If raw/tool bytes disappear or change between pages, fail explicitly. Spool does not retain every raw transcript merely to keep cursors alive.

## 7. Successful output

The structural definition is `schemas/read_session/output.schema.json` (generated from the Rust types and snapshot-tested). This section remains the normative description; if the schema and this section differ, the difference is a bug.

The response contains:

- `session`: internal/native identity and native title.
- `snapshot`: observed snapshot ID, kind (`indexed` or `source_read`), observation timestamp, and the expiry of the read's reference lease.
- `view` and resolved `source_policy`.
- `selection`: resolved branch, inclusive message endpoints, anchors, and requested/resolved context counts.
- `items`: ordered typed items with content fragments and provenance.
- `page`: next cursor, selection exhaustion, and reason the page stopped.
- `navigation`: message references immediately before/after the entire selected conversational window.
- `coverage`: completeness of known supported content in the frozen selection, independent of pagination.
- `warnings`: bounded typed notices.

### 7.1 Item variants

| `kind` | Required variant information |
|---|---|
| `message` | Message reference, user/assistant role, optional native phase. |
| `tool_call` | Tool reference, native call ID if present, tool name, result refs/state, boundary flag. |
| `tool_result` | Linked tool reference or null, native call ID if present, native error flag or null, boundary flag. |
| `raw_record` | Its original-record provenance and verbatim record fragments. |

All variants contain `item_ref`, timestamp or null, turn reference or null, `blocks`, `item_end`, and provenance. Metadata null means unknown/unavailable, not a guessed value.

Every content-bearing item contains `provenance.origin`, either `verified_source` or `indexed_snapshot`, plus opaque source/record references, expected record digest, optional native record ID, and an optional raw source-byte locator. Offsets are validated hints, not sufficient identity on their own.

Conversation source fallback can produce differently sourced items in the same page; their individual provenance must make that explicit.

### 7.2 Content fidelity

Every block reports `fidelity`:

- `verbatim`: the selected original text/bytes are preserved.
- `normalized_json`: a structured payload has been reserialized without a claim of original byte formatting.
- `metadata`: generated structural metadata, such as an attachment descriptor, not invented conversational content.

Conversational text must be verbatim UTF-8. Tool arguments stored as an original string preserve that string; structured object payloads may be normalized JSON and are labelled accordingly. Raw records must always be verbatim.

## 8. Pagination and fragmentation

Pagination traverses one fixed selection in chronological order. It does not automatically widen a match window or wander into a sibling branch.

`next_cursor` continues unreturned material **inside that selection**. `navigation.before_message_ref` and `navigation.after_message_ref` open new reads outside the selected window. They are not cursor substitutes and may deliberately repeat context in a new read.

A result sets `selection_exhausted = true` only when every representable selected item has been enumerated; then `next_cursor = null` and `stop_reason = selection_end`. Otherwise the cursor is non-null and the reason is `byte_limit` or `item_limit`.

Selection exhaustion does not mean:
- the entire session was read;
- the latest on-disk append was included;
- missing original data was recovered;
- every tool call has a result.

The independent `coverage.status` is `partial` when supported selected material is missing or cannot be linked, with warnings explaining what is known. Expected exclusions in a conversation view do not themselves imply partial coverage.

### 8.1 Oversized data

Fragmentation applies to conversational text, tool arguments, tool results, and raw records, not only raw records.

Each block fragment includes:

```text
block_ref, kind, encoding, fidelity, data
start_byte, end_byte, total_bytes, is_last
```

Ranges are half-open `[start_byte, end_byte)`. They refer to the full decoded block byte sequence, not JSON-escaped response positions or base64 character positions. Raw-record block offsets refer to the original record bytes. Normalized-JSON offsets refer to the declared normalized representation; its serializer version must be pinned in the read manifest.

For UTF-8, never split inside a code point. For base64, decode `data` before comparing its length to the byte range. The response remains valid JSON even when its string contains only part of an original JSON tool payload.

`is_last` means the fragment ends at `total_bytes`; it does not mean the full block is present on this page. `item_end` means this page reaches the final block's end for that item.

One logical item may therefore appear on consecutive pages with the same `item_ref`, while its emitted content fragments advance without overlap. Metadata repetition is allowed. Decoded content bytes must not be skipped or repeated when following returned cursors.

`max_items` counts distinct logical items represented in a page, including a continued item. A page stops at an item boundary for the item limit; the byte limit can stop inside a block. No next-page context overlap is added.

### 8.2 Budget and progress

`max_bytes` caps the final serialized tool result, including wrapping, escaping, metadata, cursors, branch options, and any compatibility text copy. The MCP bridge and the IPC layer MUST share the budget calculation with the reader. Do not budget only plaintext.

If the next item's mandatory envelope plus even one valid fragment cannot fit, return `RESOURCE_LIMIT`. Never issue a success page with an unchanged cursor that can loop forever. Empty success is valid only at genuine selection exhaustion.

The server must separately enforce local I/O/scan/time budgets. Exceeding one returns `RESOURCE_LIMIT` with a recovery action; it does not turn the budget boundary into a fabricated end-of-session.

## 9. Handle lifetime and local safety

Handles are versioned, signed opaque tokens. The signature protects integrity: it detects forged or
altered handles. It does not authenticate a client and it does not authorize access. Spool has no
client credentials. The access boundary is platform-private state: Unix state directories 0700 and
sensitive files/socket 0600; Windows protected current-user + SYSTEM ACLs and local named-pipe
ownership checks. These controls give no isolation from a hostile process that runs as the same
operating-system user. Validate current local enablement, enabled source roots, and exclusions on
every call. Raw view needs local enablement (`mcp.allow_raw_records`).

The daemon signs handles with a persistent signing key. It stores the key in a platform-private file
(Unix mode 0600). Explicit key rotation revokes the handles that the old key signed.

Retain the manifest and indexed material that issued live handles need, for a bounded lease. Expose the actual expiry. A suggested initial TTL is thirty minutes. The TTL is configurable. A search match handle uses the same lease mechanism as a read handle, and the search output exposes its expiry. Bound the number of active leases and the bytes of retained state. Both bounds are configurable. Reject new reads under resource pressure with `RESOURCE_LIMIT`. Do not promise unbounded retention.

Retired conversational snapshots and manifests that a live lease pins are an explicit exception to the specification's limit of one active snapshot plus one staging snapshot per session. Release them when the lease expires. Never use them for search. Expired leases MUST NOT keep retired data.

Persist manifests and the signing key. A normal daemon restart MUST NOT invalidate unexpired handles. Do not keep a SQLite read transaction open between calls. Key rotation, explicit purge or exclusion, or an unsupported handle-version change can revoke handles, with an explicit error.

Exclusion or deletion takes precedence over a lease. Cached content must not bypass an exclusion or removal.

Source reads and external blobs must obey configured root containment and safe handle-relative,
no-follow opening with identity validation. Windows validates ownership and volume/file identity;
all platforms reject root escapes and reparse points/symlinks. No selector accepts arbitrary local
paths, URLs, SQL, or shell commands.

## 10. Errors

`schemas/read_session/error.schema.json` describes the application payload. Protocol validation stays in the MCP SDK and the local IPC layer; this schema does not redefine the MCP protocol.

| Code | Meaning / recovery |
|---|---|
| `INVALID_ARGUMENT` | Unknown field, mixed initial/cursor arguments, malformed bound, or conflicting reference/session. |
| `SESSION_NOT_FOUND` | No accessible known session. Do not disclose hidden sessions. |
| `SESSION_NOT_INDEXED` | Indexed-only read has no available indexed snapshot. |
| `FORBIDDEN` | A disallowed view or source: for example raw view that local configuration did not enable, or an excluded or disabled source. |
| `BRANCH_REQUIRED` | A path choice is necessary; return bounded branch options. |
| `INVALID_RANGE` | Reversed, cross-snapshot, or incompatible branch endpoints. |
| `VIEW_NOT_SUPPORTED` | Adapter or selected item cannot support this representation. |
| `SOURCE_POLICY_CONFLICT` | Explicit policy is incompatible with the requested view. |
| `SOURCE_UNAVAILABLE` | Source required for the requested material cannot be read. |
| `SOURCE_CHANGED` | Expected original content failed verification. |
| `STALE_REFERENCE` | Anchor cannot safely resolve in the expected observation. |
| `CURSOR_EXPIRED` | Cursor/required read lease has expired; start a new read. |
| `UNSUPPORTED_FORMAT` | Source cannot be parsed under a supported adapter contract. |
| `RESOURCE_LIMIT` | Response, scan, time, or retained-state budget prevents progress. |
| `STORAGE_ERROR` | Required local state cannot be accessed safely. |

`retryable` means retrying the same request without an intentional change may succeed. Stale references and invalid ranges normally are not retryable. Supply a distinct recovery action such as `re_search`, `new_read`, `select_branch`, or `change_request`.

An initial request that fails source verification returns a tool error, not a successful empty transcript. Earlier successful pages remain historical results; an error on a later page does not claim their bytes were never valid.

## 11. Required changes elsewhere in the specification

Section 14.2 of the specification holds this contract. These sections hold the coordinated requirements:

1. **Search output (14.1):** The match reference is view-independent and anchored to original spans, not to a mutable chunk ID. The output exposes the handle expiry. Branch information is enough to resolve or disambiguate the read path.
2. **Identity model (6.1):** Distinguish conversational message IDs, content blocks, original records, tool invocations, result items, and read snapshots. Native call IDs are not globally unique selectors.
3. **Storage (10.2, 10.4):** Keep bounded, persistent manifests and lease-pinned retired snapshots. Store tool relationship and locator metadata when needed. Do not put tool payloads in the embedding corpus.
4. **Reader (14.2):** Support fragment continuations for every large content class. Enforce output and input-I/O budgets separately. Never require a provider call.
5. **Error catalog (14.5):** Add the read-specific errors and recovery information above.
6. **CLI (16.2):** Map match, message, range, item, and cursor semantics directly. Do not invent different CLI range rules.
7. **Documentation:** Explain cached provenance, snapshot-pinned reads, branch selection, tool turn closure, lease expiry, and the difference between pagination and widening context.

These are transcript-retrieval mechanics, not episode generation, summaries, or memory consolidation.

## 12. Required implementation acceptance tests

### Selection and navigation

- Reopen one search match in conversation and with-tools views.
- Open a known session without first performing a search.
- Open a known unindexed session locally without creating embedding requests.
- Resolve a match crossing multiple chunks/messages and deduplicate overlap.
- Count mixed-block envelopes as one conversational message.
- Read a one-message inclusive range.
- Reject reversed, cross-session, cross-snapshot, and sibling-branch ranges.
- Return a branch choice instead of selecting an invented current branch.
- Continue a fixed match window to exhaustion, then widen it through navigation refs.

### Tools and fidelity

- Include calls/results by verified turn ownership, not physical proximity.
- Preserve parallel call IDs, asynchronous result order, and multiple results.
- Mark tools included by turn closure outside the message window.
- Report an absent result without declaring the call running or failed.
- Preserve orphan results with a warning instead of mispairing them.
- Read one tool result by item reference without returning unrelated activity.
- Reject unavailable raw byte fidelity rather than normalize silently.
- Preserve UTF-8, binary/base64 records, and line endings as specified.

### Pagination and consistency

- Fragment a 2 MiB assistant response, tool result, and raw record under 32 KiB.
- Reassemble decoded fragments with no missing or duplicated content.
- Account for JSON escaping and any compatibility text copy in the limit.
- Reject a nonprogressing too-small budget.
- Replay a cursor from the same position; reject new selection/view fields.
- Permit safe append-only growth while excluding new records from old cursors.
- Fail on a rewritten selected record, and safely re-resolve a rename.
- Preserve required indexed manifests across restart until their lease expires.
- Revoke access immediately after exclusion even with an otherwise valid cursor.
- Keep a lease-pinned retired snapshot and its manifest readable until the lease expires, then release them.
- Enforce the configured lease count and retained-byte bounds, and reject new reads under pressure with `RESOURCE_LIMIT`.
- Honor and expose the expiry of a search match handle.
- Reject a handle with an altered signature. Accept unexpired handles after a daemon restart that keeps the signing key and manifests. Return an explicit error after key rotation.
- Limit `BRANCH_REQUIRED` to fifty options AND the serialized response budget, and set `branch_options_truncated` when either limit omits options.
- Reject raw view with `FORBIDDEN` unless local configuration enables it.

### Resource and failure behaviour

- Keep lexical/transcript reads independent of embedding-provider availability.
- Distinguish missing source fallback from positively changed source content.
- Bound unresolved-locator scans and external-blob reads.
- Keep all pages valid JSON and within configured response limits.
- Release expired read leases without retaining unbounded retired snapshots.
- Ensure current local enablement and exclusions are checked on every continuation.

These tests are implemented in `tests/reader.rs`, `tests/reader_schemas.rs` (schema snapshots and example validation) and the end-to-end suites `tests/e2e.rs` and `tests/e2e_mcp_edge.rs`. Known gaps against this list are recorded in `docs/known-gaps.md`.
