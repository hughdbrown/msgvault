package query

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb"
)

// parquetTable defines a table to be written as a Parquet file.
type parquetTable struct {
	name    string // e.g. "messages", "sources"
	subdir  string // subdirectory path relative to tmpDir, e.g. "messages/year=2024"
	file    string // filename, e.g. "data.parquet"
	columns string // column definition for the VALUES AS clause
	values  string // SQL VALUES rows (without the outer VALUES keyword)
	empty   bool   // if true, write schema-only empty file using WHERE false
}

// parquetBuilder creates a temp directory with Parquet test data files.
type parquetBuilder struct {
	t      *testing.T
	tables []parquetTable
}

// newParquetBuilder creates a new builder for Parquet test fixtures.
func newParquetBuilder(t *testing.T) *parquetBuilder {
	t.Helper()
	return &parquetBuilder{t: t}
}

// addTable adds a table definition to be written as Parquet.
func (b *parquetBuilder) addTable(name, subdir, file, columns, values string) *parquetBuilder {
	b.tables = append(b.tables, parquetTable{
		name:    name,
		subdir:  subdir,
		file:    file,
		columns: columns,
		values:  values,
	})
	return b
}

// addEmptyTable adds an empty table (schema only, no rows) to be written as Parquet.
func (b *parquetBuilder) addEmptyTable(name, subdir, file, columns, dummyValues string) *parquetBuilder {
	b.tables = append(b.tables, parquetTable{
		name:    name,
		subdir:  subdir,
		file:    file,
		columns: columns,
		values:  dummyValues,
		empty:   true,
	})
	return b
}

// build creates the temp directory, writes all Parquet files, and returns the
// analytics directory path and a cleanup function.
func (b *parquetBuilder) build() (string, func()) {
	b.t.Helper()

	tmpDir, err := os.MkdirTemp("", "msgvault-test-parquet-*")
	if err != nil {
		b.t.Fatalf("create temp dir: %v", err)
	}

	// Create all required directories
	for _, tbl := range b.tables {
		dir := filepath.Join(tmpDir, tbl.subdir)
		if err := os.MkdirAll(dir, 0755); err != nil {
			os.RemoveAll(tmpDir)
			b.t.Fatalf("create dir %s: %v", dir, err)
		}
	}

	// Open DuckDB for data generation
	db, err := sql.Open("duckdb", "")
	if err != nil {
		os.RemoveAll(tmpDir)
		b.t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	// Write each table
	for _, tbl := range b.tables {
		path := escapePath(filepath.Join(tmpDir, tbl.subdir, tbl.file))
		writeTableParquet(b.t, db, path, tbl.columns, tbl.values, tbl.empty)
	}

	cleanup := func() {
		os.RemoveAll(tmpDir)
	}
	return tmpDir, cleanup
}

// escapePath normalizes a file path for use in DuckDB SQL strings.
func escapePath(p string) string {
	return strings.ReplaceAll(filepath.ToSlash(p), "'", "''")
}

// writeTableParquet writes a single table's data to a Parquet file using DuckDB.
func writeTableParquet(t *testing.T, db *sql.DB, path, columns, values string, empty bool) {
	t.Helper()

	var query string
	if empty {
		query = fmt.Sprintf(`
			COPY (
				SELECT * FROM (VALUES %s) AS t(%s)
				WHERE false
			) TO '%s' (FORMAT PARQUET)
		`, values, columns, path)
	} else {
		query = fmt.Sprintf(`
			COPY (
				SELECT * FROM (VALUES %s) AS t(%s)
			) TO '%s' (FORMAT PARQUET)
		`, values, columns, path)
	}

	if _, err := db.Exec(query); err != nil {
		t.Fatalf("create parquet %s: %v", path, err)
	}
}

// Common column definitions for Parquet tables.
const (
	messagesCols          = "id, source_id, source_message_id, conversation_id, subject, snippet, sent_at, size_estimate, has_attachments, deleted_from_source_at, year, month"
	sourcesCols           = "id, account_email"
	participantsCols      = "id, email_address, domain, display_name"
	messageRecipientsCols = "message_id, participant_id, recipient_type, display_name"
	labelsCols            = "id, name"
	messageLabelsCols     = "message_id, label_id"
	attachmentsCols       = "message_id, size, filename"
)

// assertAggregationCount finds a row by key in aggregate results and checks its count.
func assertAggregationCount(t *testing.T, results []AggregateRow, key string, expectedCount int64) {
	t.Helper()
	for _, r := range results {
		if r.Key == key {
			if r.Count != expectedCount {
				t.Errorf("%s: expected count %d, got %d", key, expectedCount, r.Count)
			}
			return
		}
	}
	t.Errorf("key %q not found in results", key)
}
