# msgvault

[![Go 1.25+](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Docs](https://img.shields.io/badge/Docs-msgvault.io-blue)](https://msgvault.io)

[Documentation](https://msgvault.io) · [Setup Guide](https://msgvault.io/guides/oauth-setup/) · [Interactive TUI](https://msgvault.io/usage/tui/)

> **Pre-alpha software.** APIs, storage format, and CLI flags may change without notice. Back up your data.

An offline Gmail archive tool that exports and stores your email data locally with full-text search. Sync commands are read-only. Deletion is a separate, explicit workflow that requires user confirmation — see the [documentation](https://msgvault.io) for details. The app requests `gmail.modify` scope to support deletion staging.

## Why msgvault?

Gmail holds decades of correspondence, but there's no easy way to keep a searchable local copy. msgvault downloads your messages via the Gmail API, stores them in a compact SQLite + Parquet stack, and gives you a fast terminal UI for exploring the archive. Multi-account support means personal and work inboxes live in one place.

## Features

- **Full Gmail backup** — raw MIME, attachments, labels, and metadata
- **Interactive TUI** — drill-down analytics powered by DuckDB over Parquet
- **Full-text search** — FTS5 with Gmail-like query syntax (`from:`, `has:attachment`, date ranges)
- **Incremental sync** — History API picks up only new and changed messages
- **Multi-account** — archive several Gmail accounts in a single database
- **Resumable** — interrupted syncs resume from the last checkpoint
- **Content-addressed attachments** — deduplicated by SHA-256
- **MCP server** — expose your archive to AI assistants

## Installation

Requires **Go 1.25+** and a C compiler (GCC/Clang) for CGO.

```bash
git clone https://github.com/wesm/msgvault.git
cd msgvault
make install        # builds and installs to ~/.local/bin
```

## Quick Start

```bash
msgvault init-db
msgvault add-account you@gmail.com          # opens browser for OAuth
msgvault sync-full you@gmail.com --limit 100
msgvault tui
```

OAuth requires a Google Cloud project with the Gmail API enabled.
See the **[Setup Guide](https://msgvault.io/guides/oauth-setup/)** for step-by-step instructions.

## Commands

| Command | Description |
|---------|-------------|
| `init-db` | Create the database |
| `add-account EMAIL` | Authorize a Gmail account (use `--headless` for servers) |
| `sync-full EMAIL` | Full sync (`--limit N`, `--after`/`--before` for date ranges) |
| `sync-incremental EMAIL` | Sync only new/changed messages |
| `tui` | Launch the interactive TUI (`--account` to filter) |
| `search QUERY` | Search messages (`--json` for machine output) |
| `stats` | Show archive statistics |
| `verify EMAIL` | Verify archive integrity against Gmail |
| `export-eml` | Export a message as `.eml` |
| `build-cache` | Rebuild the Parquet analytics cache |
| `repair-encoding` | Fix UTF-8 encoding issues |
| `list-senders` / `list-domains` / `list-labels` | Explore metadata |

See the [CLI Reference](https://msgvault.io/usage/cli/) for full details.

## Configuration

All data lives in `~/.msgvault/` by default (override with `MSGVAULT_HOME`).

```toml
# ~/.msgvault/config.toml
[oauth]
client_secrets = "/path/to/client_secret.json"

[sync]
rate_limit_qps = 5
```

See the [Configuration Guide](https://msgvault.io/configuration/) for all options.

## MCP Server

msgvault includes an MCP server that exposes your archive to AI assistants. See the [MCP documentation](https://msgvault.io/usage/mcp/) for setup instructions.

## Documentation

- [Setup Guide](https://msgvault.io/guides/oauth-setup/) — OAuth, first sync, headless servers
- [Searching](https://msgvault.io/usage/searching/) — query syntax and operators
- [Interactive TUI](https://msgvault.io/usage/tui/) — keybindings, views, deletion staging
- [CLI Reference](https://msgvault.io/usage/cli/) — all commands and flags
- [Multi-Account](https://msgvault.io/usage/multi-account/) — managing multiple Gmail accounts
- [Configuration](https://msgvault.io/configuration/) — config file and environment variables
- [Architecture](https://msgvault.io/architecture/storage/) — SQLite, Parquet, and attachment storage
- [MCP Server](https://msgvault.io/usage/mcp/) — AI assistant integration
- [Troubleshooting](https://msgvault.io/troubleshooting/) — common issues and fixes
- [Development](https://msgvault.io/development/) — contributing, testing, building

## Development

```bash
git clone https://github.com/wesm/msgvault.git
cd msgvault
make test           # run tests
make lint           # run linter
make install        # build and install
```

## License

MIT — see [LICENSE](LICENSE) for details.
