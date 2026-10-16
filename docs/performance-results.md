# Performance results (spec 20)

Everything here was measured on 2026-10-12 on the machine below. Numbers describe this software on this
hardware with *synthetic* data; they are not claims about a user's corpus or about other machines.

**Hardware / build.** Apple M3 Max, 14 cores, 36 GiB RAM, local SSD, macOS (Darwin arm64), rustc 1.96.0.
Search benchmark: `--release`. Daemon startup/idle numbers: `debug` build (not representative of release; see below).
**Not the spec's reference laptop** (Apple Silicon 16 GiB); report hardware separately as the spec requires.

## Search benchmark (`benches/search_bench.rs`)

Reproduce: `cargo bench --bench search_bench -- 15000 100000`.

What it does: seeds a real `index.sqlite` (WAL, FULL synchronous, FTS5 external-content table and triggers) with N
chunks of 90 Zipf-distributed pseudo-words, one message/block/span per chunk, 20 chunks per session, plus a
768-dimension float32 vector per chunk (deterministic hash-based `FakeProvider` vectors), then calls the real
`SearchService` in-process 60 times per mode (30 for literal) after one warm-up call. Queries are two random words
from a 4,000-word vocabulary, i.e. most terms match thousands of chunks (a worst case for OR-combined FTS).
Semantic and hybrid queries are all distinct, so the query-embedding cache never helps. Page cache is warm.
This measures service latency (SQLite, scoring, fusion, grouping, lease pinning), **not** IPC/MCP framing and
**not** provider latency (the fake provider answers instantly).

### 15,000 chunks (750 sessions): the spec's reference corpus size

index.sqlite 97.5 MiB (raw vector payload 43.9 MiB); seeding 2.7 s; benchmark-process RSS after the run 76 MiB. Re-measured after literal search began dating hits by their chunk anchor (migration 0003 adds `chunk_spans_block`); the earlier run on the old literal query (no chunk lookup) was lexical 6.7/7.8, literal 10.2/10.9, semantic 91.3/95.5, hybrid 145.9/151.9 ms (p50/p95).

| mode | p50 ms | p95 ms | p99 ms | spec target |
|---|---|---|---|---|
| lexical | 6.2 | 7.6 | 11.1 | p95 < 150 ms: met |
| literal (one match) | 9.4 | 10.6 | 11.0 | p95 < 150 ms: met |
| literal, word present in every block, no time filter | 32.1 | 34.2 | 38.9 | p95 < 150 ms: met |
| literal, same word, time window admitting every hit | 85.1 | 91.6 | 92.9 | p95 < 150 ms: met. Worst case for the chunk lookup; without migration 0003's index and with record keys repeated across sessions it was 260 ms p50. Native record ids are unique per session, so the benchmark now uses unique keys. |
| semantic (local scan, vector already available) | 85.1 | 89.2 | 94.1 | n/a |
| hybrid (local retrieval after query vector) | 139.2 | 145.6 | 148.9 | p95 < 350 ms: met |

### 100,000 chunks (5,000 sessions): stress benchmark

index.sqlite 638.3 MiB (raw vector payload 293.0 MiB); seeding 24.5 s; RSS after the run 70 MiB.

| mode | p50 ms | p95 ms | p99 ms |
|---|---|---|---|
| lexical | 27.6 | 32.4 | 51.9 |
| literal | 70.8 | 73.9 | 74.0 |
| semantic | 689.5 | 842.9 | 973.5 |
| hybrid | 704.7 | 724.0 | 736.5 |

**Finding (spec 20: "run a separate 100,000-chunk stress benchmark to establish the exact-scan limit").** The
exact scan scales linearly with the vector payload: at 100,000 chunks semantic and hybrid retrieval take about
0.7 s (p95 0.72 to 0.84 s), over the 350 ms hybrid target that applies at 15,000 chunks. Lexical and literal stay
well under 150 ms at 100,000 chunks. Per spec, nothing was silently swapped for an external vector service; options
if a user's corpus approaches this size are a bounded in-memory vector cache or a replaceable local ANN index,
chosen after measuring a real corpus.

History worth knowing: the first run of this benchmark exposed a real defect (lexical search at 15,000 chunks
returned `INDEX_BUSY` after exceeding its deadline because one query joined 2,000 candidates across
chunks/sessions/snapshots, and the semantic scan probed the vector table once per chunk). It was fixed in
`src/search/retrieve.rs` (bare FTS5 top-K ranking then per-page eligibility checks; sequential vector scan keyed by
`(input_hash, rowid)`; cached coverage counts) and the numbers above are after the fix.

## Other spec 20 targets

| Target | Result |
|---|---|
| Ready for existing search after startup (< 2 s) | Release build restarted on the populated index (300 sessions, 24,600 chunks, 205.3 MiB `index.sqlite`): first search that returned hits 0.093 s after process spawn. Met. (Empty-index startup: 0.053 to 0.077 s over four runs.) |
| Idle daemon CPU (< 1% of one core over ten minutes) | Release build on the same populated index with source discovery running (default 60 s interval over 300 source files): 4.55 s CPU over 602 s = 0.76% of one core, RSS 43 to 48 MiB. Met, with a thin margin; other test processes were running on the machine and a 5 s parse-cost script overlapped, which affects scheduling but not the daemon's own CPU time. Empty index: 0.27% over 601 s (earlier run), 0.35 to 0.42% over 20 to 120 s. |
| Healthy local search during ingestion (within 2x idle p95 and a documented absolute deadline) | **Not met on the ratio.** Release build, synthetic corpus of 300 sessions x 200 messages (65.3 MiB source, 36.6 MiB text), lexical queries on the populating index: p95 58.5 to 60.1 ms during ingestion against 22.9 to 24.3 ms after, ratios 2.22x, 2.22x, 2.40x, 2.63x over four runs. The absolute p95 stays far under the 150 ms lexical target; p99 63 to 86 ms, max 66 to 115 ms. Raising `runtime.async_worker_threads` from 2 to 4 made it worse (2.50x), so this is not worker starvation; the cause was not isolated and no code was changed to chase the ratio. Documented exception. |
| Ingestion throughput and resources (same runs) | 300 of 300 sessions (24,600 chunks) indexed in 76.8 to 76.9 s. (An earlier run of an older build of the same harness hit its 1,500 s limit with 0 sessions indexed; the cause was not re-isolated, it is presumed to be the chunker cost fixed since.) Daemon CPU 95.7 to 96.8 s, RSS 26 to 62 MiB while ingesting, 63 to 71 MiB afterwards, WAL 10.7 to 12.0 MiB. The script exits non-zero unless all sessions are indexed, a query returns hits, and the restarted daemon answers a query with hits. |
| Steady RSS on the 15,000-chunk reference corpus (< 200 MiB) | Daemon: 43 to 48 MiB idle and at most 71 MiB after indexing the 24,600-chunk corpus above (hosted processing off). Benchmark process: 69 MiB after the 15,000-chunk run and 70 MiB after 100,000 chunks. Met. |
| Ingestion embedding concurrency never exceeds one request | Covered by tests (`tests/embeddings`, `FakeProvider::max_in_flight`). |
| Unchanged-session ingestion: zero embedding calls | Covered by tests (`tests/ingest`, unchanged and duplicate-delivery cases). |
| Completed-batch recovery: no provider calls for durably cached batches | Covered by tests (`tests/ingest` kill-point cases). |
| Search response at most 64 KiB; read response at most caller budget and 256 KiB | Enforced and tested (`tests/search.rs`, `tests/reader.rs`, `tests/e2e_mcp_edge.rs`). |
| Large excluded tool payload must not change embedding tokens or chunk count, and parse cost | `tests/ingest` covers token and chunk-count invariance. Measured with a throwaway script on a daemon with hosted processing off (synthetic OhMyPi data, 10 sessions x 100 messages, release build): the same conversation without tool results (0.6 MiB source) and with four excluded tool results per session spliced into the parent chain (40.6 MiB source) both indexed 10 sessions into 310 chunks with identical chunk input-hash sets; elapsed 2.13 s vs 2.04 s, daemon CPU 1.27 s vs 1.28 s, peak RSS 47 vs 52 MiB. So 40 MiB of excluded payload added no measurable parse time or chunks at this size; larger sizes were not measured. |
| Provider latency | Not measured (no hosted calls were made). |

## Retrieval quality

See `docs/retrieval-eval.md` (50-query synthetic evaluation; fake embedding model; no real-corpus claim).

## Bounded SQL batching refactor (2026-10-15)

Search now loads candidate spans in batches of at most 500 rowids rather than issuing a query per
candidate. Publication inserts chunks and their ordered spans in bounded multi-row statements inside
the same guarded transaction; chunk-to-span mapping uses logical IDs, not `RETURNING` row order.

Measured with `scripts/perf_daemon.py --idle-seconds 1 --populated-idle-seconds 1 --sessions 300
--messages 200`, first against the installed pre-refactor binary, then against the refactored release
binary. Both runs indexed all 300 sessions into 24,600 chunks, returned search hits, and answered a
search after restart. Hosted processing was disabled and all sources were synthetic sandbox files.

| Metric | Before | After batching |
|---|---:|---:|
| Ingestion elapsed | 88.4 s | 74.8 s |
| Search during ingestion p95 | 198.2 ms | 70.6 ms |
| Search after ingestion p95 | 30.3 ms | 35.1 ms |
| During/after p95 ratio | 6.55x | 2.01x |
| Restart to search with hits | 0.112 s | 0.101 s |

These are single sequential samples, not benchmark medians. Other development work ran on the host;
the baseline tail latency was unusually high compared with the earlier measurements above. The
results demonstrate functioning ingestion, search and restart, but do not isolate a causal speedup or
establish compliance with the 2x latency-ratio target. The shortened idle samples do not replace the
ten-minute idle CPU measurement.

After the Clippy-driven context and ownership cleanup, 313 tests across 13 affected integration
targets passed. Formatting and `cargo clippy --locked --all-targets --all-features -- -D warnings`
also passed without warnings. A final release-binary smoke indexed 20 synthetic sessions into 340
chunks and returned search hits before and after restart. Its during/after p95 ratio was 3.14x
(24.0/7.6 ms), further evidence that the ratio target is not consistently met.

## Coverage-count I/O (2026-10-16)

Measured once on a private backup of the local index, using a fresh SQLite process for each count.
Migration 0004 and the coverage-only join hints preserve exact counts while avoiding chunk bodies and vectors.

| Count | Result before / after | Page misses before | Page misses after |
|---|---|---|---|
| Active chunks | 3,056 / 3,056 | 1,851 | 442 |
| Embedded chunks | 2,732 / 2,732 | 5,451 | 482 |

Both queries use `COVERING INDEX chunks_coverage`; the embedded count also uses
`COVERING INDEX embeddings_key`. The semantic scan still uses the embeddings primary key and
`chunks_hash`, with no temporary sort.

`chunks_coverage` occupies 76 pages (311,296 bytes) for 3,056 chunks: 102 bytes/row and 2.3% of
the chunks table. `embeddings_key` occupies 40 pages (163,840 bytes) for 2,424 vectors:
68 bytes/row and 1.4% of the embeddings table. The private backup was deleted afterward.

Release executable smoke: 20 sessions × 20 messages, all 20 indexed, search hits before and
after restart. This is functional smoke evidence, not a latency benchmark. Counts remain cached
per epoch; this change does not interrupt SQLite statements after an outer retrieval timeout.
