# Spool

A local search engine for agent-session conversation history. Spool indexes transcripts written by
OhMyPi, Codex and Claude Code, and serves three read-only MCP tools — `search_sessions`,
`read_session`, `index_status` — over a stdio bridge to a per-user daemon. Administration is
CLI-only. Transcript sources are read-only.

Local by default: lexical search needs no network. Hosted embeddings (semantic/hybrid search) are
off until you set `embedding.hosted_processing_approved = true` and provide a credential.

## Install

From a checkout: `cargo install --path . --locked` (Rust 1.94 or newer: the highest `rust-version` among the locked
dependencies is 1.94 for sqlx 0.9; the build is tested with 1.96.0 and `cargo check` also passes with 1.95.0) puts
`spool` in `~/.cargo/bin`; or `cargo build --release --locked` and use `target/release/spool`. To publish a checksum for
a release binary:
`shasum -a 256 target/release/spool`. A local, git-ignored `dist/` holds the checked build
(`spool-0.1.0-aarch64-apple-darwin` and `SHA256SUMS`; verify with `cd dist && shasum -a 256 -c SHA256SUMS`). There is
no prebuilt-binary pipeline; CI (`.github/workflows/ci.yml`) builds and tests only.

## Quickstart

```text
cargo build --release
target/release/spool init                    # asks which sources to enable (default: none); hosted processing defaults to NO
target/release/spool service install         # per-user LaunchAgent (macOS) / systemd user unit (Linux)
target/release/spool service start
target/release/spool doctor                  # permissions, sources, SQLite/FTS5, embedding state, daemon, service
target/release/spool index --since 7d --dry-run
target/release/spool index --since 7d --wait
target/release/spool search "webhook receiver" --since 30d
target/release/spool mcp config --client claude   # prints MCP client config; never writes it
```

Non-interactive init: `spool init --enable-source codex --hosted-processing no`. Without flags and
without a TTY the safe defaults are kept and reported: no source is enabled (nothing is indexed until
you choose, e.g. `--enable-source omp` or `spool sources enable omp`) and hosted processing is off.

No service manager? Run `spool daemon run` in a terminal instead.

## Commands

```text
spool init | daemon run | service install|uninstall|start|stop|restart
spool status [--json]   spool logs [--follow]   spool doctor [--json] [--probe-provider]
spool search QUERY [--mode hybrid|lexical|semantic|literal] [--source ID] [--since 7d] [--limit N]
spool read SESSION_ID [--match-ref R | --message-ref R | --item-ref R | --from-ref R --through-ref R] ...
spool read --cursor CURSOR
spool index --since 7d | --all | --session ID [--source ID] [--dry-run] [--wait]
spool index rebuild | remove --session ID --exclude | purge --unreferenced
spool jobs status|retry|cancel OPERATION_ID
spool sources list|enable|disable ID     spool handles rotate-key
spool mcp                                   (stdio bridge)     spool mcp config --client claude|codex|omp
spool config show                           (redacted; defaults vs overrides)
```

`--tz UTC|+HH:MM` sets the timezone used to parse and print dates (relative dates are resolved in the
CLI; the MCP API takes absolute timestamps only).

## Security in one paragraph

Filesystem permissions are the only access control (directories 0700; socket, key, credential,
databases 0600). There is no isolation from a hostile process running as the same user and the
databases are not encrypted by Spool. The tool-block filter is not a secret scanner. See
[docs/privacy-and-security.md](docs/privacy-and-security.md).

## Documentation

* [docs/operations.md](docs/operations.md) — install, services, backfill, doctor, backup/restore
* [docs/architecture.md](docs/architecture.md) — processes, IPC, storage, layout
* [docs/privacy-and-security.md](docs/privacy-and-security.md) — access model, consent, logging
* [docs/mcp-tools.md](docs/mcp-tools.md) — tool overview and client setup
* [docs/adapter-support.md](docs/adapter-support.md) — supported transcript formats
* [config.example.toml](config.example.toml) — every configuration key with its default
* Specs: [docs/spec.md](docs/spec.md), [docs/read-session-contract.md](docs/read-session-contract.md)
