CREATE TABLE native_scan_runs (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL UNIQUE REFERENCES sources(id),
    parent_dev INTEGER NOT NULL,
    parent_ino INTEGER NOT NULL,
    main_dev INTEGER NOT NULL,
    main_ino INTEGER NOT NULL,
    cursor TEXT,
    exhausted INTEGER NOT NULL DEFAULT 0 CHECK (exhausted IN (0,1))
) WITHOUT ROWID;

CREATE TABLE native_scan_entries (
    scan_id TEXT NOT NULL REFERENCES native_scan_runs(id) ON DELETE CASCADE,
    native_id TEXT NOT NULL,
    parent_id TEXT,
    directory TEXT NOT NULL,
    title TEXT,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (scan_id, native_id)
) WITHOUT ROWID;
