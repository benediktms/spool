ALTER TABLE native_scan_entries ADD COLUMN event_sequence INTEGER
    CHECK (event_sequence IS NULL OR event_sequence >= 0);
