CREATE INDEX chunks_coverage ON chunks(snapshot_id, session_id, input_hash);
CREATE INDEX embeddings_key ON embeddings(profile_id, input_hash);
