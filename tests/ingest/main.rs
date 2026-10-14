//! Ingestion pipeline tests (spec 5.7, 8, 9.3-9.4, 10.4-10.6, 11.5, 19.4) over synthetic fixtures,
//! the real adapters, the real schema and the deterministic fake embedding provider.

mod common;
mod failpoints;
mod pipeline;
mod providers;
mod recovery;
mod source_changes;
