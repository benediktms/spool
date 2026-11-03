CREATE TABLE lease_refs (
    lease_id TEXT NOT NULL REFERENCES read_leases(id) ON DELETE CASCADE,
    ref_id   INTEGER NOT NULL,
    kind     TEXT NOT NULL,
    payload  BLOB NOT NULL,
    PRIMARY KEY (lease_id, ref_id)
) WITHOUT ROWID;
