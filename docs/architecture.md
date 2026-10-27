# Architecture

Spool is one Rust package (library `spool` + binary `spool`) that indexes agent-session transcripts
from local harnesses (OhMyPi, Codex, Claude Code) and serves search and reads to MCP clients. The
authoritative design is `docs/spec.md`; the contracts between the parsers, graph, chunker and ingest code are in
`docs/interfaces.md`.

```text
MCP client --stdio--> `spool mcp` ----\
                                       >-- owner-only local IPC --> one per-user daemon
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

* **One daemon per user**, a foreground process supervised by launchd/systemd/Task Scheduler (no
  self-daemonizing). It alone opens the databases writable, under an OS-held exclusive lock on
  `state/daemon.lock`.
* **IPC** is length-prefixed JSON over the configured local endpoint (`state/spool.sock` by default):
  a Unix-domain socket on macOS/Linux, or a local named pipe on Windows whose name hashes the
  absolute configured socket path. The configured value remains endpoint identity; Windows creates
  no `spool.sock` file. Requests are either `Tool` (`search_sessions`, `read_session`, `index_status`,
  forwarded by the bridge) or `Control` (administration commands used only by the CLI). Frames and
  responses are size-bounded.
* **`spool mcp`** (`src/mcp/bridge.rs`) is a disposable stdio MCP server. It opens no database,
  starts no worker and forks no daemon; it forwards tool calls over the local endpoint and returns
  `DAEMON_NOT_READY` with the start command when the endpoint is unreachable.
* **CLI** (`src/cli/`): `init`, `doctor`, `config show`, `service *`, `logs`, `mcp config` work from
  files and the OS; everything else calls the daemon through `ipc::client::call`. Relative dates
  (`--since 7d`) are resolved in the CLI against an explicit timezone and sent as absolute
  timestamps.
* **Service management** (`src/service/`): renders a per-user LaunchAgent plist
  (`com.spool.daemon`), systemd user unit (`spool-daemon.service`), or Windows Task Scheduler task.
  Tests use an isolated label and a recording runner.
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
Windows: %LOCALAPPDATA%\Spool\  (or %USERPROFILE%\AppData\Local\Spool fallback)
  config.toml
  state/  index.sqlite  jobs.sqlite  daemon.lock
  secrets/  handle-signing.key  provider.key
Unix permissions: directories 0700; sensitive files and Unix socket 0600.
Windows equivalent: protected DACL granting full access only to the current user and LocalSystem;
  the named pipe is local-only and has no filesystem entry.
macOS logs: ~/Library/Logs/Spool/     Linux logs: <state>/logs/     Windows logs: <root>/logs/
macOS service: ~/Library/LaunchAgents/com.spool.daemon.plist
Linux service: ~/.config/systemd/user/spool-daemon.service
Windows service: per-user Task Scheduler task invoking spool-service.exe beside spool.exe
```

`SPOOL_HOME` replaces the root (including logs) for isolated runs and tests. Two further environment
hooks exist for isolation: `SPOOL_SERVICE_LABEL` (service label, default `com.spool.daemon`) and
`HOME`/`XDG_CONFIG_HOME` for Unix service definition directories.
