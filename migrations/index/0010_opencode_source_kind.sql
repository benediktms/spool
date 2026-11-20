-- no-transaction
PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;
CREATE TABLE sources_new (
    id                   TEXT PRIMARY KEY,
    kind                 TEXT NOT NULL CHECK (kind IN ('omp','codex','claude','opencode')),
    roots_json           TEXT NOT NULL,
    enabled              INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0,1)),
    include_subagents    INTEGER NOT NULL DEFAULT 0 CHECK (include_subagents IN (0,1)),
    config_revision      INTEGER NOT NULL DEFAULT 1,
    bootstrap_started_at INTEGER,
    bootstrap_cutoff     INTEGER,
    discovery_watermark  INTEGER,
    last_scan_at         INTEGER,
    last_scan_ok         INTEGER,
    last_scan_error      TEXT
) WITHOUT ROWID;
INSERT INTO sources_new SELECT * FROM sources;
DROP TABLE sources;
ALTER TABLE sources_new RENAME TO sources;
COMMIT;
PRAGMA foreign_keys = ON;
