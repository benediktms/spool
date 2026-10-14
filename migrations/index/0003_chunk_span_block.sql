-- Literal search dates each hit by the chunk whose new-content span holds it: look the span up by its block.
CREATE INDEX chunk_spans_block ON chunk_spans(record_key, block_index, start_byte);
