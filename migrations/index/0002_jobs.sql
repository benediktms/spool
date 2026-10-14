-- Jobs/discovery bookkeeping (JobsSlice). Additive only; 0001 is untouched.
-- index_attempts : consecutive transient failures of the current source revision (retry policy, spec 11.7)
-- defer_until    : unix ms before which reconciliation must not resubmit (budget reset, backoff)
-- task_id        : Apalis task id of the most recently submitted IndexSession task (liveness check)
-- submit_seq     : monotonic per-session counter used to build idempotency keys
-- operation_id   : explicit operation (index --since/--all/--session) that requested this session
-- missing_confirmed_at : second completed scan confirmed the file is gone; derived data was removed
ALTER TABLE sessions ADD COLUMN index_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN defer_until INTEGER;
ALTER TABLE sessions ADD COLUMN task_id TEXT;
ALTER TABLE sessions ADD COLUMN submit_seq INTEGER NOT NULL DEFAULT 0;
ALTER TABLE sessions ADD COLUMN operation_id TEXT;
ALTER TABLE sessions ADD COLUMN missing_confirmed_at INTEGER;

CREATE INDEX sessions_work ON sessions(index_state, eligibility, source_status);
CREATE INDEX sessions_operation ON sessions(operation_id);
CREATE INDEX source_files_session_checked ON source_files(last_checked_at);
CREATE INDEX operations_state ON operations(state);
CREATE INDEX source_files_identity ON source_files(dev, ino);
