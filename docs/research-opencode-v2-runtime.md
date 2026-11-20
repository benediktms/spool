# OpenCode V2.3.2 storage: runtime verification

The installed `opencode v2.3.2` created a fresh SQLite database through its normal server APIs. The server ran on loopback with isolated `HOME`, XDG paths, and `OPENCODE_DB`; it used a local fake provider for text, a `read` call on a synthetic file, and compaction. SQLite observations used `mode=ro`. No personal transcript database or hosted model was used.

See the [pinned upstream source verification](research-opencode-v2-storage.md) for the corresponding implementation paths.

## Observations

| Probe | Observed behavior |
|---|---|
| Completed turn | `session_v2.id` identifies the session. `session_message` contains user, assistant, synthetic, and idle records. Message IDs are primary keys; the unique `(session_id, seq)` index supports bounded walks in persisted order. User and assistant content can be read directly from `session_message.data`. |
| Change detection | `event_sequence.seq`, keyed by `aggregate_id = session_v2.id`, advanced on enqueue, completed turns, revert and compaction. A write to an unrelated session did not advance it. Enqueue alone advanced the sequence before the inbox item became a message. The `event` table was empty for these sessions despite the sequence advancing. |
| Revert | Staging set `session_v2.revert` without removing projected rows; the V2 list-messages endpoint still returned them. Clearing removed the marker without removing rows. Committing deleted the selected message and later rows. A verification lookup must not assume that an old message ID still exists. |
| Compaction | Successful manual compaction added a `compaction` message with `status = "completed"` and preserved earlier projected messages. A failed compaction also emitted a typed `compaction` record. Neither is ordinary conversation. |
| Tool records and split parts | By default a completed `read` and a separate argument-error case both persisted as an inline assistant `content[]` tool block with call ID, input, and output or error. With `experimental.part_rows = true`, assistant `content` was empty on the message row and the text/tool records lived in `session_part`, keyed by `(message_id, ordinal)`. Forks copied split parts too. |
| Fork and subagent | A child session set `parent_id`; a fork instead set `fork_session_id` and `fork_boundary` and copied history under new message IDs. Deleting the parent removed it and its child, but the fork and its copied history remained. |
| Import | A synthetic transcript imported via the V2 session transfer API appeared in the same `session_v2` and `session_message` tables, with no import marker in `metadata` for that case. No provider-name or text-based rule can establish external provenance from this observation. |
| Offline read | A read-only SQLite connection could query the WAL-mode database after the isolated server stopped. |

## Implications for Spool

- Use the native session ID for cataloging. Treat the per-session sequence as a **candidate change signal**, not as a proof of unchanged content or an append-only offset. Confirm its coverage across supported write paths before relying on it as a skip condition. Use record IDs and content digests for verification.
- Query the session and its ordered message projection under a bounded read transaction, and account for both inline and split assistant content in lookup and verification. Keep synthetic, idle and compaction records out of conversation search, and keep tool input and output out of ordinary conversation. Read only explicitly selected transcript columns; the same database contains credential tables.
- Do not require the original parent to read a fork's copied history. Do not label imports as native-authored or try to deduplicate them by matching conversation text. Including staged-but-uncommitted messages matches the observed list-messages API; committed deletions must be reflected.

The probes did not establish the complete set of sequence update paths, structured question/answer persistence, or byte-exact `raw_records` support. APIs exercised are documented in the [OpenCode V2 API reference](https://opencode.ai/docs/api).
