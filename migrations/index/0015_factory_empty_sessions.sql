-- no-transaction
PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;

CREATE TABLE sessions_new (
    id                    TEXT PRIMARY KEY,
    source_id             TEXT NOT NULL REFERENCES sources(id),
    native_id             TEXT NOT NULL,
    discriminator         TEXT NOT NULL DEFAULT '',
    lineage_native_id     TEXT,
    lineage_verified      INTEGER NOT NULL DEFAULT 0,
    native_title          TEXT,
    cwd                   TEXT,
    session_kind          TEXT NOT NULL DEFAULT 'unknown' CHECK (session_kind IN ('interactive','subagent','unknown')),
    created_at            INTEGER,
    last_activity_at      INTEGER,
    active_snapshot_id    TEXT,
    generation            INTEGER NOT NULL DEFAULT 0,
    excluded              INTEGER NOT NULL DEFAULT 0 CHECK (excluded IN (0,1)),
    exclusion_reason      TEXT,
    observed_fingerprint  TEXT,
    coverage_status       TEXT NOT NULL DEFAULT 'unknown',
    source_status         TEXT NOT NULL DEFAULT 'present' CHECK (source_status IN ('present','missing','unreadable')),
    eligibility           TEXT NOT NULL DEFAULT 'bootstrap' CHECK (eligibility IN ('bootstrap','active','backfill','manual')),
    index_state           TEXT NOT NULL DEFAULT 'pending' CHECK (index_state IN ('pending','indexed','empty','deferred','failed','unsupported')),
    index_error           TEXT,
    discovered_at         INTEGER NOT NULL,
    tombstoned_at         INTEGER,
    index_attempts        INTEGER NOT NULL DEFAULT 0,
    defer_until           INTEGER,
    task_id               TEXT,
    submit_seq            INTEGER NOT NULL DEFAULT 0,
    operation_id          TEXT,
    missing_confirmed_at  INTEGER,
    project_id            TEXT REFERENCES projects(id),
    remote                TEXT,
    empty_source_revision TEXT,
    UNIQUE (source_id, native_id, discriminator)
) WITHOUT ROWID;

INSERT INTO sessions_new SELECT sessions.*, NULL FROM sessions;
DROP TABLE sessions;
ALTER TABLE sessions_new RENAME TO sessions;

CREATE INDEX sessions_source ON sessions(source_id);
CREATE INDEX sessions_activity ON sessions(last_activity_at);
CREATE INDEX sessions_cwd ON sessions(cwd);
CREATE INDEX sessions_kind ON sessions(session_kind);
CREATE INDEX sessions_state ON sessions(index_state);
CREATE INDEX sessions_work ON sessions(index_state, eligibility, source_status);
CREATE INDEX sessions_operation ON sessions(operation_id);
CREATE INDEX sessions_project ON sessions(project_id);

UPDATE sessions AS s SET index_state = 'pending', index_attempts = 0, defer_until = NULL, index_error = NULL
WHERE s.index_state = 'indexed' AND s.excluded = 0 AND s.source_status = 'present'
  AND s.source_id IN (SELECT id FROM sources WHERE kind = 'factory')
  AND EXISTS (SELECT 1 FROM snapshots sn WHERE sn.id = s.active_snapshot_id AND sn.coverage = 'complete'
              AND NOT EXISTS (SELECT 1 FROM chunks c WHERE c.snapshot_id = sn.id));

COMMIT;
PRAGMA foreign_keys = ON;
