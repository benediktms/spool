//! Embedding provider seam, OpenAI client, durable budget, caches (spec 12; 4.1, 10.5, 10.6).

pub mod budget;
pub mod cache;
pub mod fake;
pub mod health;
pub mod openai;
pub mod provider;
mod service;
pub mod tokenizer;
pub mod vectors;

pub use fake::FakeProvider;
pub use health::CircuitInfo;
pub use openai::OpenAiProvider;
pub use provider::{
    EmbedError, EmbedErrorKind, EmbedResponse, EmbeddingProvider, ProfileSpec, ProviderLimits,
    Purpose, RENDERING_VERSION,
};
pub use service::{
    BudgetLine, BudgetView, EmbedProgress, EmbeddingService, EmbeddingState, EmbeddingStatus,
    ProbeOutcome, VectorCoverage, probe_provider,
};
pub use tokenizer::tokenizer;
