# Privacy and security

## Access model (the exact statement)

**Filesystem permissions are the only access control.** On Unix, directories are mode 0700 and the
socket plus sensitive files (handle-signing key, provider credential file, `index.sqlite`,
`jobs.sqlite`, their WAL/SHM files, `daemon.lock`, `config.toml`, log files) are mode 0600. On
Windows, Spool uses protected DACLs that grant full access only to the current user and LocalSystem,
rejects reparse points, and checks current-user ownership; files may inherit only approved ACEs.
The IPC endpoint is a local named pipe, whose server is first-instance protected and whose clients
validate the server owner before sending a request. There are no client credentials, scopes or
authentication step: anything allowed to open the local endpoint can use every enabled MCP tool.

**This gives no isolation from a hostile process running as the same operating-system user.** Such a
process can read the databases, signing key and credential file directly. Spool does not claim
otherwise.

**There is no application-layer encryption.** The SQLite databases and logs are plain files.
Protection at rest relies on your OS account permissions and, optionally, full-disk encryption
(FileVault / LUKS / BitLocker) that you enable yourself.

Handle signing (match, message, item, branch references and cursors) protects the integrity of
handles. It is not client authentication. `spool handles rotate-key` replaces the key and revokes
every issued handle.

There is no HTTP listener, no TCP listener and no HTTP MCP client anywhere in Spool.

## What is stored

The index contains sensitive conversational text (user and assistant messages, chunked) and
vectors derived from it if embeddings are enabled, plus metadata (source paths, session ids,
titles, timestamps). Tool payloads and reasoning are not indexed. Source transcripts are opened
read-only and never modified or deleted; removing a session from Spool never deletes the original.

**The filter is not a secret scanner.** Excluding tool blocks and reasoning reduces what is
indexed, but conversational text can itself contain secrets you pasted. Spool does not detect or
redact secrets automatically. Choose which sources are enabled before ingestion, and exclude
sessions you do not want indexed (`spool index remove --session ID --exclude`; the exclusion
survives rediscovery and restart).

Logical removal and `spool index purge --unreferenced` are not forensic secure erasure: old SQLite
pages, filesystem snapshots and earlier backups may retain data.

## Hosted processing consent

By default nothing leaves the machine. Semantic and hybrid search need a hosted embedding
provider, and sending anything to it requires **both**:

1. `embedding.hosted_processing_approved = true` in `config.toml` (default `false`), and
2. a credential in the provider credential file (default `secrets/provider.key`, Unix mode 0600 or
   its protected Windows ACL equivalent).

`spool init` asks for this explicitly (interactive prompt on a TTY, otherwise the
`--hosted-processing yes|no` flag; with neither it keeps `false` and says so). Nothing converts
`false` to `true` implicitly, and an existing config is never modified by `init` without `--force`.

When approved, what is transmitted: rendered conversational chunk text (when indexing) and search
queries (semantic/hybrid `search_sessions`, spending the query budget). Not transmitted: source
paths, raw tool payloads, credentials, full transcript records. MCP callers cannot trigger
ingestion; only `semantic`/`hybrid` searches can reach the provider, and only when approved.
Daily token budgets bound both paths (`embedding.daily_*_token_budget`).

Without approval or without a credential the daemon keeps lexical indexing and search working
(`lexical_only` state; the two causes are reported distinctly by `spool doctor` and status).
Tests use a local fake server on 127.0.0.1 and never the network.

## Logs, diagnostics, telemetry

* Logs are local, structured, size-rotated files (`~/Library/Logs/Spool/spool.log` on macOS,
  `<state>/logs/spool.log` on Linux, `<root>/logs/spool.log` on Windows; `SPOOL_HOME` keeps them under
  that home). Windows uses the same path whether the CLI discovers its default home or the task captures it explicitly.
  Configured by `logging.max_file_bytes` and `logging.retained_files`.
* Logs carry identifiers, counts and timings. They never contain query text, conversation text,
  tool payloads, provider credentials or headers, the signing key, or provider error bodies.
  `logging.include_content` is reserved and ignored.
* There is no telemetry upload, no update check and no external metrics collector.
* `spool config show` redacts secret-named fields and URL credentials. The config file itself holds
  only the *path* of the credential file.
* `spool doctor` verifies the private modes/ACLs listed above (including the signing key, credential
  file, databases and WAL files) and never prints key material. It contacts the provider only with
  `--probe-provider`, using a fixed harmless string.

## Service environment

The LaunchAgent / systemd user unit does not inherit your shell environment: launchd gets a fixed
`PATH` and `HOME` (plus `SPOOL_HOME` only when you installed with one). The Windows Task Scheduler
launcher captures only an absolute `SPOOL_HOME` and executable path. Credentials come from the
private file, not from environment variables. Services are per-user, never root or elevated, and
`spool service install` refuses to overwrite a definition it did not write.
