# Contributing to Spool

## Contracts before style

`docs/spec.md` and `docs/read-session-contract.md` define behavior. Read the affected flow and its callers before changing it. Preserve wire formats, schema, ordering, cancellation, resource limits, and privacy boundaries unless the change explicitly updates those contracts.

Prefer the smallest complete change. Reuse existing types, helpers, SQL batching conventions, and installed dependencies before adding another abstraction. Do not introduce traits, configuration, concurrency, or dependencies for hypothetical requirements.

## Ownership and interfaces

- Borrow data while its owner remains available. For example, publication reads SQL row fields as `&str`; allocate an owned value only when the result must outlive the row.
- Move owned data when ownership is required instead of borrowing and immediately cloning it. Cheap shared-handle clones are different from cloning strings, vectors, or parsed transcripts.
- Accept slices or borrowed views when callers do not need to transfer ownership. Keep public interfaces narrow and internal helpers private.
- Group parameters that represent one concept, not arbitrary arguments to satisfy a lint. Existing examples are `StageContext`, `SourceLimits`, `ScanLimits`, and `LexicalQuery`.
- Separate stable inputs from changing state. The chunk planner uses borrowed `Env` inputs and a `RunBuf` for the current chunk, counters, and overlap context.
- Remove unused state and parameters rather than keeping them for symmetry or a future seam. Search and read services get lease timing through `LeaseManager`; they do not carry their own unused clock.
- Use enums and newtypes when they prevent a concrete mistake. Prefer `From` for infallible conversions and `TryFrom` for conversions that can fail.
- Use checked arithmetic for sizes and counts derived from input, configuration, or growing data. Use `try_from` for narrowing or sign-changing integer conversions; a cast needs a locally established bound.
- Choose loops or iterator chains for readability. Avoid collecting only to iterate once unless the collection establishes a required ownership, batching, or ordering boundary.

## Modules and boundaries

- Organize modules by responsibility, not line count or one type per file. Keep related small types beside their consumers; avoid generic `utils` modules and speculative crate splits.
- When a function grows beyond 50 lines, consider splitting it into smaller functions with cohesive responsibilities. This is a review signal, not a hard limit; do not split a cohesive function merely to satisfy the line count.
- Use `pub(crate)` for crate-internal access and `pub` for deliberate APIs. Do not widen production visibility merely to reach an item from a test.
- Validate external encodings, sizes, and required fields at the adapter boundary. Enforce shared invariants in the common implementation so sibling callers cannot bypass them.
- Keep protocol conversion at the boundary. Where callers classify typed errors, use exhaustive matching so new variants require a decision.
- Document public contracts that signatures cannot express: ordering, cancellation, error conditions, and panics. Keep documentation aligned with observable behavior.

## SQLite and bounded work

- Batch repeated reads and writes instead of awaiting one statement per candidate or span. Bound both the candidate window and the SQL bind count; do not replace sequential work with unbounded buffering or task spawning.
- Search span loading currently uses windows of at most 500 candidate rowids. Publication uses a 500-bind budget, with rows per statement derived from the number of columns. Follow the local convention rather than assuming one row always uses one bind.
- Preserve explicit ordering. Span retrieval orders by chunk rowid and span sequence; sequence numbering restarts for each chunk.
- Do not assume SQLite `RETURNING` rows match input order. Publication maps returned rowids by logical ID, whose uniqueness is scoped to the snapshot.
- Keep publication inside one guarded transaction: validate generation, fingerprint, exclusion and staging state; insert chunks and spans; switch the active snapshot; retire the previous snapshot; bump the epoch; commit. Batching must not expose partial publication or change rollback behavior.
- A cache entry must represent a complete result, including an empty result. The search span cache records chunks with no spans so grouping can progress without repeated queries.
- Keep allocated storage accounting separate from reusable free pages. The WAL retention setting is not a hard growth cap; pinned readers can prevent checkpoint/reset. VACUUM remains explicit maintenance.

## Async, errors, and concurrency

- Do not hold synchronous lock guards across `.await`. Use a lexical scope to release the guard before asynchronous work.
- Preserve deadlines, cooperative cancellation, bounded queues, and existing concurrency limits through refactors. Additional tasks or threads are not an automatic scalability improvement.
- Keep CPU-heavy work off async executor threads when measurement justifies it; reuse existing blocking-work boundaries.
- Propagate recoverable errors with context and retain distinctions that callers use. Use `expect` only for established invariants, not external input or routine I/O failures.
- Keep commit and shutdown explicit where failure matters. Preserve transaction rollback on every error path.
- Supervise spawned tasks: retain handles, handle unexpected exits, and join or cancel tasks during shutdown. Detach only when the result and lifetime genuinely require no supervision.
- Check cancellation safety for futures used in `tokio::select!`. Dropping a losing future must not silently lose work; cancellation during a database commit may leave an uncertain outcome. A transaction cannot roll back external side effects.
- Use typed errors where callers must choose different actions, such as retrying, rejecting, or reporting. Use `anyhow` at orchestration boundaries that report errors without classifying them. Add context for the operation or resource, not a duplicate of the source message.

## Secrets and unsafe code

- Never put credentials or secret-bearing URLs in errors or logs. If a parser error can echo a secret, return a fixed diagnostic instead of attaching that source error.
- Do not derive `Debug` or `Serialize` for secret-bearing types without ensuring the secret is omitted. Fail explicitly when required configuration is missing; defaults must be safe and valid.
- Avoid `unsafe`. When it is necessary, minimize the block and put a `SAFETY` comment immediately above it explaining the conditions that make the operation sound.

## Clippy and formatting

- Separate distinct steps inside functions with a blank line: validation, preparation, execution, and response or error handling. Keep closely related statements together. Do not put a blank line between every statement.
- `rustfmt` owns all other formatting. Run `cargo fmt --all`; do not hand-format.

All enabled Clippy warnings must be corrected, including tests and benchmarks. Do not add `allow(clippy::...)`, `expect(clippy::...)`, command-line lint suppression, or relaxed lint thresholds to make a check pass.

`Cargo.toml` sets `clippy::all` to `deny`. Use `deny`, not a blanket `forbid`: dependency derive macros generate internal lint attributes that conflict with `forbid`. This is not permission to suppress warnings in Spool code.

The existing Rust `dead_code` allowances in the four shared integration fixture modules are separate from Clippy suppressions: each fixture is compiled into multiple test binaries with different helper usage. Do not export helpers merely to hide unused-code warnings. Production dead fields and no-op type-check scaffolds should be deleted.

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
```

CI runs Clippy across every target and feature. Full test-suite runs belong to CI; use affected test targets locally.

## Verification and performance

- Run the smallest affected test targets, for example `cargo test --locked --test ingest --test search` for publication/search changes. Cover consumer-visible boundaries, errors, ordering, and state transitions rather than copies of implementation details.
- Exercise batching boundaries. Existing regressions cover publication across multiple chunk/span batches, rollback after a later batch fails, and finding a readable match beyond the first span-fetch window.
- Verify nontrivial changes through the real executable as well as tests. Use an isolated `SPOOL_HOME`, synthetic sources inside a temporary sandbox, and hosted processing disabled. Never point tests or benchmarks at real transcript roots or install a persistent production service as part of a check.
- `scripts/perf_daemon.py` exercises ingestion, search, and restart with those isolation guards. It refuses an enabled source outside its sandbox and requires indexed sessions and search hits.
- Measure before claiming scalability gains. Keep baseline and changed runs comparable, distinguish smoke checks from benchmarks, and report host contention and sample counts. A single faster run is not a causal result or a benchmark median.
- Record measurements and remaining limits in `docs/performance-results.md`. The search-during-ingestion 2x p95 ratio is not consistently met; the refactor does not establish otherwise. Do not add ANN indexing or concurrency machinery without evidence for that specific bottleneck.
- Commit `Cargo.lock` and use `--locked` for reproducible builds. Enable only dependency features that the implementation needs.
- Do not hand-edit generated artifacts. Use the repository's existing generation or schema-update path when the corresponding contract changes; do not introduce the POC's SQLx metadata workflow into Spool by analogy.

## Further reading

These patterns were checked against Jon Gjengset's *Rust for Rustaceans*, especially ownership and borrowed interfaces, error propagation, async execution, and starting with simple concurrency. The supplied PDF is an early-access edition; consult the [author's updates and errata](https://rust-for-rustaceans.com/) rather than treating edition-specific examples as current language rules.

The contribution guide in `app-rust-poc/CONTRIBUTING.md` was also compared for general Rust standards. Ownership, checked conversions, responsibility-based modules, secret-safe errors, supervised tasks, and cancellation safety apply here. Its workspace layout, PostgreSQL metadata, protobuf/GraphQL generation, deployment tooling, and permission for scoped Clippy exceptions do not define Spool's conventions.
