CREATE TABLE native_session_observations (
    session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
    event_sequence INTEGER CHECK (event_sequence IS NULL OR event_sequence >= 0),
    last_checked_at INTEGER NOT NULL
) WITHOUT ROWID;
