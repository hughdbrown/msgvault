# Query Package Design

## Overview

The `internal/query` package provides a backend-agnostic query layer for msgvault.
It supports two use cases:

1. **TUI aggregate views** - Fast grouping by sender, domain, label, time
2. **CLI/API message retrieval** - List and detail queries

## Backend Strategy

The package defines an `Engine` interface that can be implemented by:

### SQLiteEngine (Current)

- Uses direct SQLite queries with JOINs
- Flexible: supports all filters and sorting options
- Performance: adequate for small-medium databases (<100k messages)
- Always available as fallback

### ParquetEngine (Future)

- Uses Arrow/Parquet files for denormalized analytics data
- Much faster for aggregates (~3000x vs SQLite JOINs)
- Read-only, requires periodic rebuild from SQLite
- Best for large databases (100k+ messages)

## Parquet Schema (Planned)

```
messages.parquet (partitioned by year):
- id: int64
- source_message_id: string
- subject: string
- sent_at: timestamp
- size_estimate: int64
- from_email: string
- from_domain: string
- to_emails: list<string>  # denormalized
- labels: list<string>     # denormalized
- attachment_count: int32
- attachment_size: int64
```

This denormalized schema avoids JOINs, enabling fast scans.

## Hybrid Approach

For production use with large databases:

```go
// Create hybrid engine that uses Parquet for aggregates, SQLite for details
engine := query.NewHybridEngine(
    query.NewParquetEngine(parquetDir),
    query.NewSQLiteEngine(db),
)
```

The hybrid engine routes queries to the appropriate backend:
- Aggregate queries → Parquet (fast scans)
- Message detail queries → SQLite (has body, raw MIME)

## Build Process

```bash
# Build/rebuild Parquet files from SQLite
msgvault build-analytics [--full-rebuild]

# Files stored in ~/.msgvault/analytics/
# - messages/year=2024/*.parquet
# - _last_sync.json (incremental state)
```

## Go Libraries

Potential libraries for Parquet support:
- `github.com/apache/arrow/go` - Official Arrow implementation
- `github.com/xitongsys/parquet-go` - Pure Go Parquet
- `github.com/marcboeker/go-duckdb` - DuckDB via CGO (SQL interface)

DuckDB is attractive because it provides a SQL interface over Parquet,
similar to the Python implementation which uses DuckDB for ETL.
