-- Spool index.sqlite schema v1 (spec 10.2). Timestamps are unix milliseconds UTC unless noted.
-- Digests and vectors are BLOBs. No raw tool payloads are stored anywhere.

CREATE TABLE app_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE sources (
    id                   TEXT PRIMARY KEY,
    kind                 TEXT NOT NULL CHECK (kind IN ('omp','codex','claude')),
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

CREATE TABLE sessions (
    id                  TEXT PRIMARY KEY,
    source_id           TEXT NOT NULL REFERENCES sources(id),
    native_id           TEXT NOT NULL,
    discriminator       TEXT NOT NULL DEFAULT '',
    lineage_native_id   TEXT,
    lineage_verified    INTEGER NOT NULL DEFAULT 0,
    native_title        TEXT,
    cwd                 TEXT,
    session_kind        TEXT NOT NULL DEFAULT 'unknown' CHECK (session_kind IN ('interactive','subagent','unknown')),
    created_at          INTEGER,
    last_activity_at    INTEGER,
    active_snapshot_id  TEXT,
    generation          INTEGER NOT NULL DEFAULT 0,
    excluded            INTEGER NOT NULL DEFAULT 0 CHECK (excluded IN (0,1)),
    exclusion_reason    TEXT,
    observed_fingerprint TEXT,
    coverage_status     TEXT NOT NULL DEFAULT 'unknown',
    source_status       TEXT NOT NULL DEFAULT 'present' CHECK (source_status IN ('present','missing','unreadable')),
    eligibility         TEXT NOT NULL DEFAULT 'bootstrap' CHECK (eligibility IN ('bootstrap','active','backfill','manual')),
    index_state         TEXT NOT NULL DEFAULT 'pending' CHECK (index_state IN ('pending','indexed','deferred','failed','unsupported')),
    index_error         TEXT,
    discovered_at       INTEGER NOT NULL,
    tombstoned_at       INTEGER,
    UNIQUE (source_id, native_id, discriminator)
) WITHOUT ROWID;
CREATE INDEX sessions_source ON sessions(source_id);
CREATE INDEX sessions_activity ON sessions(last_activity_at);
CREATE INDEX sessions_cwd ON sessions(cwd);
CREATE INDEX sessions_kind ON sessions(session_kind);
CREATE INDEX sessions_state ON sessions(index_state);

CREATE TABLE source_files (
    session_id     TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
    path           TEXT NOT NULL,
    dev            INTEGER,
    ino            INTEGER,
    size           INTEGER,
    mtime_ms       INTEGER,
    format         TEXT,
    exists_status  TEXT NOT NULL DEFAULT 'present',
    last_checked_at INTEGER
) WITHOUT ROWID;
CREATE INDEX source_files_path ON source_files(path);

CREATE TABLE snapshots (
    id              TEXT PRIMARY KEY,
    session_id      TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    kind            TEXT NOT NULL CHECK (kind IN ('indexed','source_read')),
    state           TEXT NOT NULL CHECK (state IN ('staging','published','retired','retired_leased','read_only')),
    source_revision TEXT NOT NULL,
    fingerprint     TEXT NOT NULL,
    generation      INTEGER NOT NULL,
    prefix_end      INTEGER NOT NULL,
    file_dev        INTEGER,
    file_ino        INTEGER,
    coverage        TEXT NOT NULL DEFAULT 'complete',
    active_leaf_key TEXT,
    counts_json     TEXT NOT NULL DEFAULT '{}',
    serializer_version INTEGER NOT NULL DEFAULT 1,
    created_at      INTEGER NOT NULL,
    published_at    INTEGER,
    retained_bytes  INTEGER NOT NULL DEFAULT 0,
    UNIQUE (id, session_id)
) WITHOUT ROWID;
CREATE INDEX snapshots_session_state ON snapshots(session_id, state);

CREATE TABLE records (
    snapshot_id TEXT NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    key         TEXT NOT NULL,
    ord         INTEGER NOT NULL,
    native_id   TEXT,
    parent_key  TEXT,
    kind        TEXT NOT NULL,
    ts          INTEGER,
    offset      INTEGER NOT NULL,
    len         INTEGER NOT NULL,
    digest      BLOB NOT NULL CHECK (length(digest) = 32),
    turn_id     TEXT,
    PRIMARY KEY (snapshot_id, key)
) WITHOUT ROWID;
CREATE INDEX records_ord ON records(snapshot_id, ord);
CREATE INDEX records_parent ON records(snapshot_id, parent_key);

CREATE TABLE messages (
    snapshot_id        TEXT NOT NULL,
    record_key         TEXT NOT NULL,
    seq                INTEGER NOT NULL,
    role               TEXT NOT NULL CHECK (role IN ('user','assistant')),
    phase              TEXT,
    ts                 INTEGER,
    parent_message_key TEXT,
    PRIMARY KEY (snapshot_id, record_key),
    FOREIGN KEY (snapshot_id, record_key) REFERENCES records(snapshot_id, key) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE INDEX messages_seq ON messages(snapshot_id, seq);

CREATE TABLE blocks (
    snapshot_id TEXT NOT NULL,
    record_key  TEXT NOT NULL,
    idx         INTEGER NOT NULL,
    origin      TEXT NOT NULL CHECK (origin IN ('authored','unknown')),
    text        TEXT NOT NULL,
    text_digest BLOB NOT NULL,
    PRIMARY KEY (snapshot_id, record_key, idx),
    FOREIGN KEY (snapshot_id, record_key) REFERENCES messages(snapshot_id, record_key) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE TABLE attachments (
    snapshot_id TEXT NOT NULL,
    record_key  TEXT NOT NULL,
    idx         INTEGER NOT NULL,
    kind        TEXT NOT NULL,
    mime        TEXT,
    bytes       INTEGER,
    PRIMARY KEY (snapshot_id, record_key, idx),
    FOREIGN KEY (snapshot_id, record_key) REFERENCES messages(snapshot_id, record_key) ON DELETE CASCADE
) WITHOUT ROWID;

CREATE TABLE tool_calls (
    snapshot_id    TEXT NOT NULL,
    record_key     TEXT NOT NULL,
    block_index    INTEGER NOT NULL,
    native_call_id TEXT,
    tool_name      TEXT NOT NULL,
    turn_id        TEXT,
    PRIMARY KEY (snapshot_id, record_key, block_index),
    FOREIGN KEY (snapshot_id, record_key) REFERENCES records(snapshot_id, key) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE INDEX tool_calls_turn ON tool_calls(snapshot_id, turn_id);

CREATE TABLE tool_results (
    snapshot_id      TEXT NOT NULL,
    record_key       TEXT NOT NULL,
    block_index      INTEGER NOT NULL,
    native_call_id   TEXT,
    call_record_key  TEXT,
    call_block_index INTEGER,
    is_error         INTEGER,
    turn_id          TEXT,
    PRIMARY KEY (snapshot_id, record_key, block_index),
    FOREIGN KEY (snapshot_id, record_key) REFERENCES records(snapshot_id, key) ON DELETE CASCADE
) WITHOUT ROWID;
CREATE INDEX tool_results_call ON tool_results(snapshot_id, call_record_key, call_block_index);

CREATE TABLE parse_warnings (
    snapshot_id TEXT NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    seq         INTEGER NOT NULL,
    code        TEXT NOT NULL,
    record_key  TEXT,
    offset      INTEGER,
    len         INTEGER,
    detail      TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, seq)
) WITHOUT ROWID;

CREATE TABLE chunks (
    rowid         INTEGER PRIMARY KEY,
    logical_id    TEXT NOT NULL,
    snapshot_id   TEXT NOT NULL,
    session_id    TEXT NOT NULL,
    run_key       TEXT NOT NULL,
    ordinal       INTEGER NOT NULL,
    body          TEXT NOT NULL,
    input_hash    BLOB NOT NULL CHECK (length(input_hash) = 32),
    time_anchor   INTEGER,
    rendered_tokens INTEGER NOT NULL,
    is_tail       INTEGER NOT NULL DEFAULT 0,
    UNIQUE (snapshot_id, logical_id),
    FOREIGN KEY (snapshot_id, session_id) REFERENCES snapshots(id, session_id) ON DELETE CASCADE
);
CREATE INDEX chunks_snapshot ON chunks(snapshot_id, run_key, ordinal);
CREATE INDEX chunks_hash ON chunks(input_hash);
CREATE INDEX chunks_anchor ON chunks(time_anchor);

CREATE TABLE chunk_spans (
    chunk_rowid INTEGER NOT NULL REFERENCES chunks(rowid) ON DELETE CASCADE,
    seq         INTEGER NOT NULL,
    record_key  TEXT NOT NULL,
    block_index INTEGER NOT NULL,
    start_byte  INTEGER NOT NULL,
    end_byte    INTEGER NOT NULL,
    is_context  INTEGER NOT NULL CHECK (is_context IN (0,1)),
    PRIMARY KEY (chunk_rowid, seq)
) WITHOUT ROWID;

CREATE VIRTUAL TABLE chunks_fts USING fts5(
    body,
    content='chunks',
    content_rowid='rowid',
    tokenize='unicode61 remove_diacritics 2'
);
CREATE TRIGGER chunks_fts_ai AFTER INSERT ON chunks BEGIN
    INSERT INTO chunks_fts(rowid, body) VALUES (new.rowid, new.body);
END;
CREATE TRIGGER chunks_fts_ad AFTER DELETE ON chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, body) VALUES ('delete', old.rowid, old.body);
END;
CREATE TRIGGER chunks_fts_au AFTER UPDATE OF body ON chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, body) VALUES ('delete', old.rowid, old.body);
    INSERT INTO chunks_fts(rowid, body) VALUES (new.rowid, new.body);
END;

CREATE TABLE embedding_profiles (
    id                 TEXT PRIMARY KEY,
    provider           TEXT NOT NULL,
    endpoint_identity  TEXT NOT NULL,
    model              TEXT NOT NULL,
    dimensions         INTEGER NOT NULL CHECK (dimensions > 0),
    rendering_version  INTEGER NOT NULL,
    normalization      TEXT NOT NULL,
    generation         INTEGER NOT NULL,
    created_at         INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE embeddings (
    profile_id  TEXT NOT NULL REFERENCES embedding_profiles(id),
    input_hash  BLOB NOT NULL CHECK (length(input_hash) = 32),
    dimensions  INTEGER NOT NULL,
    vec         BLOB NOT NULL,
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (profile_id, input_hash),
    CHECK (length(vec) = dimensions * 4)
) WITHOUT ROWID;

CREATE TABLE embedding_requests (
    id               TEXT PRIMARY KEY,
    profile_id       TEXT NOT NULL,
    purpose          TEXT NOT NULL CHECK (purpose IN ('ingest','query')),
    budget_day       TEXT NOT NULL,
    reserved_tokens  INTEGER NOT NULL,
    actual_tokens    INTEGER,
    state            TEXT NOT NULL CHECK (state IN ('reserved','dispatched','completed','failed','ambiguous')),
    error_category   TEXT,
    created_at       INTEGER NOT NULL,
    completed_at     INTEGER
) WITHOUT ROWID;
CREATE INDEX embedding_requests_day ON embedding_requests(budget_day, purpose);

CREATE TABLE exclusions (
    kind       TEXT NOT NULL CHECK (kind IN ('session','source')),
    target_id  TEXT NOT NULL,
    reason     TEXT,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (kind, target_id)
) WITHOUT ROWID;

CREATE TABLE read_manifests (
    id                 TEXT PRIMARY KEY,
    session_id         TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    snapshot_id        TEXT NOT NULL,
    kind               TEXT NOT NULL CHECK (kind IN ('indexed','source_read')),
    source_id          TEXT NOT NULL,
    file_dev           INTEGER,
    file_ino           INTEGER,
    prefix_end         INTEGER NOT NULL,
    serializer_version INTEGER NOT NULL,
    created_at         INTEGER NOT NULL,
    retained_bytes     INTEGER NOT NULL,
    UNIQUE (snapshot_id),
    FOREIGN KEY (snapshot_id, session_id) REFERENCES snapshots(id, session_id) ON DELETE CASCADE
) WITHOUT ROWID;

-- A lease pins one or more manifests (one search response = one lease over many sessions).
CREATE TABLE read_leases (
    id             TEXT PRIMARY KEY,
    expires_at     INTEGER NOT NULL,
    state          TEXT NOT NULL DEFAULT 'live' CHECK (state IN ('live','revoked')),
    bytes_charged  INTEGER NOT NULL DEFAULT 0,
    created_at     INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX read_leases_expiry ON read_leases(expires_at);

CREATE TABLE lease_manifests (
    lease_id    TEXT NOT NULL REFERENCES read_leases(id) ON DELETE CASCADE,
    manifest_id TEXT NOT NULL REFERENCES read_manifests(id) ON DELETE CASCADE,
    PRIMARY KEY (lease_id, manifest_id)
) WITHOUT ROWID;
CREATE INDEX lease_manifests_manifest ON lease_manifests(manifest_id);

-- Durable CLI operations (index/backfill/rebuild...). `task_id` is the Apalis task id.
CREATE TABLE operations (
    id              TEXT PRIMARY KEY,
    kind            TEXT NOT NULL,
    params_json     TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    task_id         TEXT,
    state           TEXT NOT NULL CHECK (state IN ('accepted','running','done','failed','cancelled')),
    progress_json   TEXT NOT NULL DEFAULT '{}',
    error           TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
) WITHOUT ROWID;

INSERT INTO app_meta(key, value) VALUES ('schema_version','1'), ('index_epoch','0'), ('signing_key_epoch','1');
