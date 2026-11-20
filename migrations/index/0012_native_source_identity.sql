CREATE TABLE native_source_identities (
    source_id TEXT PRIMARY KEY REFERENCES sources(id),
    parent_dev INTEGER NOT NULL,
    parent_ino INTEGER NOT NULL,
    main_dev INTEGER NOT NULL,
    main_ino INTEGER NOT NULL,
    diagnostic TEXT CHECK (diagnostic IS NULL OR diagnostic = 'SOURCE_MOVED')
) WITHOUT ROWID;
