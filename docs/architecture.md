# Architecture

Spool is one Rust package (library `spool` + binary `spool`) that indexes agent-session transcripts
from local harnesses (OhMyPi, Codex, Claude Code) and serves search and reads to MCP clients. The
authoritative design is `docs/spec.md`; the contracts between the parsers, graph, chunker and ingest code are in
`docs/interfaces.md`.

```text
MCP client --stdio--> `spool mcp` ----\
                                       >-- owner-only Unix socket --> one per-user daemon
`spool` CLI (status, index, ...) -----/                                 |
                                                   +--------------------+-------------------+
                                              request layer        discovery           Apalis workers
                                             (search/read/status)  (read-only scans)   (ingest=1, control=1)
                                                   |                    |                    |
                                              index.sqlite <---- parse/filter/chunk ---- source adapters
                                                   |                                         |
                                              hosted embeddings (opt-in)             transcript files (read-only)
                                      jobs.sqlite: Apalis execution state only
```

* **One daemon per user**, a foreground process supervised by launchd/systemd (no self-daemonizing).
  It alone opens the databases writable, under an OS-held exclusive lock on `state/daemon.lock`.
* **IPC** is length-prefixed JSON over `state/spool.sock` (`src/ipc/`). Requests are either `Tool`
  (`search_sessions`, `read_session`, `index_status`, forwarded by the bridge) or `Control`
  (administration commands used only by the CLI). Frames and responses are size-bounded.
* **`spool mcp`** (`src/mcp/bridge.rs`) is a disposable stdio MCP server. It opens no database,
  starts no worker and forks no daemon; it forwards tool calls over the socket and returns
  `DAEMON_NOT_READY` with the start command when the socket is unreachable.
* **CLI** (`src/cli/`): `init`, `doctor`, `config show`, `service *`, `logs`, `mcp config` work from
  files and the OS; everything else calls the daemon through `ipc::client::call`. Relative dates
  (`--since 7d`) are resolved in the CLI against an explicit timezone and sent as absolute
  timestamps.
* **Service management** (`src/service/`): `ServiceManager` parameterised by label, definition
  directory and a `CommandRunner`, rendering a LaunchAgent plist (`com.spool.daemon`) or a systemd
  user unit (`spool-daemon.service`). Tests use an isolated label and a recording runner.
* **Logging** (`src/telemetry/`): size-rotated structured file logs; initialised by `daemon run`.
* **Storage**: `index.sqlite` (sessions, snapshots, chunks, FTS5, vectors as little-endian f32
  BLOBs, leases, budgets) and `jobs.sqlite` (Apalis). SQLite is bundled via SQLx; FTS5 is verified
  at startup and by `doctor`. Schema: `migrations/index/0001_init.sql`.
* **Ingestion**: adapters stream records through one shared JSONL pipeline (`src/adapters/jsonl.rs`;
  each harness supplies only format-specific hooks), the pipeline stages a snapshot, chunks
  deterministically, and publishes in one short transaction; embeddings are optional and fill in
  afterwards, so text is searchable before (or without) vectors.
* **Reads** pin a manifest under a lease and re-read the original source file with digest
  verification; Spool does not keep full transcript copies for reads.

Process ownership boundaries (who may touch what): CLI/bridge never open writable databases; the
daemon never writes to source transcript roots; `spool init` is the only place the handle-signing key
is created.

## Directory layout

```text
macOS: ~/Library/Application Support/Spool/          Linux: ~/.local/state/spool/ (XDG)
  config.toml                  (0600)
  state/ (0700)  index.sqlite  jobs.sqlite  daemon.lock  spool.sock      (files 0600)
  secrets/ (0700) handle-signing.key  provider.key                       (0600)
macOS logs: ~/Library/Logs/Spool/     Linux logs: <state>/logs/
macOS service: ~/Library/LaunchAgents/com.spool.daemon.plist
Linux service: ~/.config/systemd/user/spool-daemon.service
```

`SPOOL_HOME` replaces the root (including logs) for isolated runs and tests. Two further
environment hooks exist for isolation: `SPOOL_SERVICE_LABEL` (service label, default
`com.spool.daemon`) and `HOME`/`XDG_CONFIG_HOME` for the service definition directory.
