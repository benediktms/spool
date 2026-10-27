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
`spool` in `~/.cargo/bin` on Unix and both `spool.exe` and `spool-service.exe` in
`%USERPROFILE%\.cargo\bin` on Windows; or `cargo build --release --locked` and use
`target/release/spool` (`spool.exe` on Windows). Keep `spool-service.exe` beside `spool.exe` when
copying a Windows build: Task Scheduler uses that native helper to start without a console window. Windows
native source builds need the MSVC Rust toolchain and Visual Studio C++ Build Tools plus a Windows
SDK (for native SQLite, zstd, and aws-lc dependencies). A Developer PowerShell is not required when
the build can auto-detect that toolchain. To publish a checksum for a release binary:
`shasum -a 256 target/release/spool`. A local, git-ignored `dist/` holds the checked build
(`spool-0.1.0-aarch64-apple-darwin` and `SHA256SUMS`; verify with `cd dist && shasum -a 256 -c SHA256SUMS`). There
is no prebuilt-binary pipeline or installer; CI (`.github/workflows/ci.yml`) builds and tests only.
Native Windows x64 support targets Windows 10/11; the executable smoke ran on Windows 11 x64.
Windows 10 and ARM Windows variants have not been verified.

## Quickstart

After `cargo install --path . --locked`, these commands work on all platforms:

```text
spool init                    # asks which sources to enable; hosted processing defaults to NO
spool service install         # LaunchAgent / systemd user unit / Windows logon task
spool service start
spool doctor
spool index --since 7d --dry-run
spool index --since 7d --wait
spool search "webhook receiver" --since 30d
spool mcp config --client claude
```

For a checkout build on Windows, use `.\target\release\spool.exe` instead of `spool`;
macOS/Linux use `target/release/spool`.

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

Filesystem permissions are the only access control: Unix uses directories 0700 and sensitive files/socket 0600;
Windows uses equivalent protected current-user + SYSTEM ACLs and a local named pipe. There is no isolation from a
hostile process running as the same user, and databases are not encrypted by Spool. The tool-block filter is not a
secret scanner. See [docs/privacy-and-security.md](docs/privacy-and-security.md).

## Documentation

* [docs/operations.md](docs/operations.md) — install, services, backfill, doctor, backup/restore
* [docs/architecture.md](docs/architecture.md) — processes, IPC, storage, layout
* [docs/privacy-and-security.md](docs/privacy-and-security.md) — access model, consent, logging
* [docs/mcp-tools.md](docs/mcp-tools.md) — tool overview and client setup
* [docs/adapter-support.md](docs/adapter-support.md) — supported transcript formats
* [config.example.toml](config.example.toml) — every configuration key with its default
* Specs: [docs/spec.md](docs/spec.md), [docs/read-session-contract.md](docs/read-session-contract.md)
