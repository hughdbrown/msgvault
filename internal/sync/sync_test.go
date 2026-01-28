package sync

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/store"
)

// Sample MIME message for testing
var testMIME = []byte(`From: sender@example.com
To: recipient@example.com
Subject: Test Message
Date: Mon, 01 Jan 2024 12:00:00 +0000
Content-Type: text/plain; charset="utf-8"

This is a test message body.
`)

func TestFullSync(t *testing.T) {
	// Create temp directory for database
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Open database
	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	// Initialize schema
	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Create mock API with test messages
	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 3,
		HistoryID:     12345,
	}
	mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	mock.AddMessage("msg2", testMIME, []string{"INBOX", "SENT"})
	mock.AddMessage("msg3", testMIME, []string{"SENT"})

	// Create syncer
	syncer := New(mock, st, nil)

	// Run full sync
	ctx := context.Background()
	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	// Verify summary
	if summary.MessagesAdded != 3 {
		t.Errorf("expected 3 messages added, got %d", summary.MessagesAdded)
	}
	if summary.Errors != 0 {
		t.Errorf("expected 0 errors, got %d", summary.Errors)
	}
	if summary.FinalHistoryID != 12345 {
		t.Errorf("expected history ID 12345, got %d", summary.FinalHistoryID)
	}

	// Verify API calls
	if mock.ProfileCalls != 1 {
		t.Errorf("expected 1 profile call, got %d", mock.ProfileCalls)
	}
	if mock.LabelsCalls != 1 {
		t.Errorf("expected 1 labels call, got %d", mock.LabelsCalls)
	}
	if len(mock.GetMessageCalls) != 3 {
		t.Errorf("expected 3 message fetches, got %d", len(mock.GetMessageCalls))
	}

	// Verify database state
	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}
	if stats.MessageCount != 3 {
		t.Errorf("expected 3 messages in db, got %d", stats.MessageCount)
	}
}

func TestFullSyncResume(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Open database
	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Create mock with pagination
	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 4,
		HistoryID:     12345,
	}
	mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	mock.AddMessage("msg2", testMIME, []string{"INBOX"})
	mock.AddMessage("msg3", testMIME, []string{"INBOX"})
	mock.AddMessage("msg4", testMIME, []string{"INBOX"})
	mock.MessagePages = [][]string{
		{"msg1", "msg2"},
		{"msg3", "msg4"},
	}

	// First sync - only first page
	syncer := New(mock, st, nil)
	ctx := context.Background()

	// Simulate partial sync by only processing first page
	summary1, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("first sync: %v", err)
	}
	if summary1.MessagesAdded != 4 {
		t.Errorf("expected 4 messages added, got %d", summary1.MessagesAdded)
	}

	// Second sync should skip already-synced messages
	mock.Reset()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 4,
		HistoryID:     12346,
	}
	mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	mock.AddMessage("msg2", testMIME, []string{"INBOX"})
	mock.AddMessage("msg3", testMIME, []string{"INBOX"})
	mock.AddMessage("msg4", testMIME, []string{"INBOX"})

	summary2, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}

	// Should skip all 4 since they already exist
	if summary2.MessagesAdded != 0 {
		t.Errorf("expected 0 new messages on re-sync, got %d", summary2.MessagesAdded)
	}
}

func TestFullSyncWithErrors(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Open database
	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Create mock with one failing message
	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 3,
		HistoryID:     12345,
	}
	mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	mock.AddMessage("msg2", testMIME, []string{"INBOX"})
	mock.AddMessage("msg3", testMIME, []string{"INBOX"})

	// Make msg2 fail to fetch
	mock.GetMessageError["msg2"] = &gmail.NotFoundError{Path: "/messages/msg2"}

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("sync with errors: %v", err)
	}

	// Should have 2 added, 1 error
	if summary.MessagesAdded != 2 {
		t.Errorf("expected 2 messages added, got %d", summary.MessagesAdded)
	}
	if summary.Errors != 1 {
		t.Errorf("expected 1 error, got %d", summary.Errors)
	}
}

func TestMIMEParsing(t *testing.T) {
	// Test that MIME parsing extracts correct fields
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	complexMIME := []byte(`From: "John Doe" <john@example.com>
To: "Jane Smith" <jane@example.com>, bob@example.com
Cc: cc@example.com
Subject: Re: Meeting Notes
Date: Tue, 15 Jan 2024 14:30:00 -0500
Message-ID: <msg123@example.com>
In-Reply-To: <msg122@example.com>
Content-Type: multipart/mixed; boundary="boundary123"

--boundary123
Content-Type: text/plain; charset="utf-8"

Hello,

This is the message body.

Best regards,
John

--boundary123
Content-Type: application/pdf; name="document.pdf"
Content-Disposition: attachment; filename="document.pdf"
Content-Transfer-Encoding: base64

JVBERi0xLjQKJeLjz9MKMSAwIG9iago8PC9UeXBlL0NhdGFsb2cvUGFnZXMgMiAwIFI+PgplbmRv
Ymo=
--boundary123--
`)

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12345,
	}
	mock.AddMessage("complex1", complexMIME, []string{"INBOX"})

	opts := DefaultOptions()
	opts.AttachmentsDir = filepath.Join(tmpDir, "attachments")
	syncer := New(mock, st, opts)

	ctx := context.Background()
	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("sync: %v", err)
	}

	if summary.MessagesAdded != 1 {
		t.Errorf("expected 1 message added, got %d", summary.MessagesAdded)
	}

	// Verify stats include attachment
	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}
	if stats.AttachmentCount != 1 {
		t.Errorf("expected 1 attachment, got %d", stats.AttachmentCount)
	}
}

func TestFullSyncEmptyInbox(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Mock with no messages
	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 0,
		HistoryID:     12345,
	}

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("sync empty inbox: %v", err)
	}

	if summary.MessagesAdded != 0 {
		t.Errorf("expected 0 messages added, got %d", summary.MessagesAdded)
	}
	if summary.MessagesFound != 0 {
		t.Errorf("expected 0 messages found, got %d", summary.MessagesFound)
	}
}

func TestFullSyncProfileError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.ProfileError = fmt.Errorf("auth failed")

	syncer := New(mock, st, nil)
	ctx := context.Background()

	_, err = syncer.Full(ctx, "test@example.com")
	if err == nil {
		t.Error("expected error when profile fails")
	}
}

func TestFullSyncAllDuplicates(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 3,
		HistoryID:     12345,
	}
	mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	mock.AddMessage("msg2", testMIME, []string{"INBOX"})
	mock.AddMessage("msg3", testMIME, []string{"INBOX"})

	syncer := New(mock, st, nil)
	ctx := context.Background()

	// First sync
	_, err = syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("first sync: %v", err)
	}

	// Second sync with same messages - all should be skipped
	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("second sync: %v", err)
	}

	if summary.MessagesAdded != 0 {
		t.Errorf("expected 0 messages added on re-sync, got %d", summary.MessagesAdded)
	}
	if summary.MessagesSkipped != 3 {
		t.Errorf("expected 3 messages skipped, got %d", summary.MessagesSkipped)
	}
}

func TestFullSyncNoResume(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 2,
		HistoryID:     12345,
	}
	mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	mock.AddMessage("msg2", testMIME, []string{"INBOX"})

	// Force no resume
	opts := DefaultOptions()
	opts.NoResume = true

	syncer := New(mock, st, opts)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("sync with NoResume: %v", err)
	}

	if summary.WasResumed {
		t.Error("expected WasResumed to be false with NoResume option")
	}
	if summary.MessagesAdded != 2 {
		t.Errorf("expected 2 messages added, got %d", summary.MessagesAdded)
	}
}

func TestFullSyncAllErrors(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 3,
		HistoryID:     12345,
	}
	mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	mock.AddMessage("msg2", testMIME, []string{"INBOX"})
	mock.AddMessage("msg3", testMIME, []string{"INBOX"})

	// Make all messages fail
	mock.GetMessageError["msg1"] = &gmail.NotFoundError{Path: "/messages/msg1"}
	mock.GetMessageError["msg2"] = &gmail.NotFoundError{Path: "/messages/msg2"}
	mock.GetMessageError["msg3"] = &gmail.NotFoundError{Path: "/messages/msg3"}

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("sync with all errors: %v", err)
	}

	if summary.MessagesAdded != 0 {
		t.Errorf("expected 0 messages added, got %d", summary.MessagesAdded)
	}
	if summary.Errors != 3 {
		t.Errorf("expected 3 errors, got %d", summary.Errors)
	}
}

func TestFullSyncWithQuery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 2,
		HistoryID:     12345,
	}
	mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	mock.AddMessage("msg2", testMIME, []string{"INBOX"})

	opts := DefaultOptions()
	opts.Query = "before:2024/06/01"

	syncer := New(mock, st, opts)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("sync with query: %v", err)
	}

	// Verify query was passed to the Gmail API
	if mock.LastQuery != "before:2024/06/01" {
		t.Errorf("expected query %q, got %q", "before:2024/06/01", mock.LastQuery)
	}

	// The mock doesn't filter by query, but we can verify sync works with query option
	if summary.MessagesAdded != 2 {
		t.Errorf("expected 2 messages added, got %d", summary.MessagesAdded)
	}
}

func TestFullSyncPagination(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 6,
		HistoryID:     12345,
	}

	// Add 6 messages across 3 pages
	for i := 1; i <= 6; i++ {
		mock.AddMessage(fmt.Sprintf("msg%d", i), testMIME, []string{"INBOX"})
	}
	mock.MessagePages = [][]string{
		{"msg1", "msg2"},
		{"msg3", "msg4"},
		{"msg5", "msg6"},
	}

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("sync with pagination: %v", err)
	}

	if summary.MessagesAdded != 6 {
		t.Errorf("expected 6 messages added, got %d", summary.MessagesAdded)
	}

	// Verify ListMessages was called 3 times (one per page)
	if mock.ListMessagesCalls != 3 {
		t.Errorf("expected 3 list calls (one per page), got %d", mock.ListMessagesCalls)
	}
}

func TestSyncerWithLogger(t *testing.T) {
	mock := gmail.NewMockAPI()
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	st, err := store.Open(filepath.Join(tmpDir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	syncer := New(mock, st, nil)

	// Test WithLogger chainability
	syncer = syncer.WithLogger(nil)
	if syncer == nil {
		t.Error("WithLogger should return syncer for chaining")
	}
}

func TestSyncerWithProgress(t *testing.T) {
	mock := gmail.NewMockAPI()
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	st, err := store.Open(filepath.Join(tmpDir, "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()
	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	syncer := New(mock, st, nil)

	// Test WithProgress chainability
	syncer = syncer.WithProgress(gmail.NullProgress{})
	if syncer == nil {
		t.Error("WithProgress should return syncer for chaining")
	}
}

// Tests for incremental sync

func TestIncrementalSyncNoSource(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	syncer := New(mock, st, nil)
	ctx := context.Background()

	// Should fail - no source exists
	_, err = syncer.Incremental(ctx, "test@example.com")
	if err == nil {
		t.Error("expected error for incremental sync without source")
	}
}

func TestIncrementalSyncNoHistoryID(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Create source without history ID
	_, err = st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}

	mock := gmail.NewMockAPI()
	syncer := New(mock, st, nil)
	ctx := context.Background()

	// Should fail - no history ID
	_, err = syncer.Incremental(ctx, "test@example.com")
	if err == nil {
		t.Error("expected error for incremental sync without history ID")
	}
}

func TestIncrementalSyncAlreadyUpToDate(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Create source with history ID
	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}
	if err := st.UpdateSourceSyncCursor(source.ID, "12345"); err != nil {
		t.Fatalf("UpdateSourceSyncCursor: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 10,
		HistoryID:     12345, // Same as cursor
	}

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Incremental(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("incremental sync: %v", err)
	}

	// Should complete with no changes
	if summary.MessagesAdded != 0 {
		t.Errorf("expected 0 messages added, got %d", summary.MessagesAdded)
	}
}

func TestIncrementalSyncWithChanges(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Create source with history ID
	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}
	if err := st.UpdateSourceSyncCursor(source.ID, "12340"); err != nil {
		t.Fatalf("UpdateSourceSyncCursor: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 10,
		HistoryID:     12350, // Newer than cursor
	}
	mock.AddMessage("new-msg-1", testMIME, []string{"INBOX"})
	mock.AddMessage("new-msg-2", testMIME, []string{"INBOX"})

	// Set up history records
	mock.HistoryRecords = []gmail.HistoryRecord{
		{
			MessagesAdded: []gmail.HistoryMessage{
				{Message: gmail.MessageID{ID: "new-msg-1", ThreadID: "thread_new-msg-1"}},
			},
		},
		{
			MessagesAdded: []gmail.HistoryMessage{
				{Message: gmail.MessageID{ID: "new-msg-2", ThreadID: "thread_new-msg-2"}},
			},
		},
	}
	mock.HistoryID = 12350

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Incremental(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("incremental sync: %v", err)
	}

	if summary.MessagesAdded != 2 {
		t.Errorf("expected 2 messages added, got %d", summary.MessagesAdded)
	}
}

func TestIncrementalSyncWithDeletions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// First do a full sync to have some messages
	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 2,
		HistoryID:     12340,
	}
	mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	mock.AddMessage("msg2", testMIME, []string{"INBOX"})

	syncer := New(mock, st, nil)
	ctx := context.Background()

	// Full sync first
	_, err = syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	// Now simulate deletion via incremental
	mock.Profile.HistoryID = 12350
	mock.HistoryRecords = []gmail.HistoryRecord{
		{
			MessagesDeleted: []gmail.HistoryMessage{
				{Message: gmail.MessageID{ID: "msg1", ThreadID: "thread_msg1"}},
			},
		},
	}
	mock.HistoryID = 12350

	summary, err := syncer.Incremental(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("incremental sync with deletions: %v", err)
	}

	// Should process the deletion
	if summary.MessagesFound != 1 {
		t.Errorf("expected 1 history record processed, got %d", summary.MessagesFound)
	}

	// Verify deletion was persisted - msg1 should have deleted_from_source_at set
	var deletedAt sql.NullTime
	err = st.DB().QueryRow(st.Rebind("SELECT deleted_from_source_at FROM messages WHERE source_message_id = ?"), "msg1").Scan(&deletedAt)
	if err != nil {
		t.Fatalf("query deleted_from_source_at: %v", err)
	}
	if !deletedAt.Valid {
		t.Error("msg1 should have deleted_from_source_at set after incremental sync with deletion")
	}

	// Verify msg2 is NOT marked as deleted
	err = st.DB().QueryRow(st.Rebind("SELECT deleted_from_source_at FROM messages WHERE source_message_id = ?"), "msg2").Scan(&deletedAt)
	if err != nil {
		t.Fatalf("query deleted_from_source_at for msg2: %v", err)
	}
	if deletedAt.Valid {
		t.Error("msg2 should NOT have deleted_from_source_at set")
	}
}

func TestIncrementalSyncHistoryExpired(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Create source with old history ID
	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}
	if err := st.UpdateSourceSyncCursor(source.ID, "1000"); err != nil {
		t.Fatalf("UpdateSourceSyncCursor: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 10,
		HistoryID:     12350,
	}
	// Simulate history 404
	mock.HistoryError = &gmail.NotFoundError{Path: "/history"}

	syncer := New(mock, st, nil)
	ctx := context.Background()

	_, err = syncer.Incremental(ctx, "test@example.com")
	if err == nil {
		t.Error("expected error for expired history")
	}
}

func TestIncrementalSyncProfileError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Create source with history ID
	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}
	if err := st.UpdateSourceSyncCursor(source.ID, "12345"); err != nil {
		t.Fatalf("UpdateSourceSyncCursor: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.ProfileError = fmt.Errorf("auth failed")

	syncer := New(mock, st, nil)
	ctx := context.Background()

	_, err = syncer.Incremental(ctx, "test@example.com")
	if err == nil {
		t.Error("expected error when profile fails")
	}
}

func TestIncrementalSyncWithLabelAdded(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// First do a full sync to have a message
	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12340,
	}
	mock.AddMessage("msg1", testMIME, []string{"INBOX"})

	syncer := New(mock, st, nil)
	ctx := context.Background()

	// Full sync first
	_, err = syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	// Now simulate label addition via incremental
	mock.Profile.HistoryID = 12350
	mock.HistoryRecords = []gmail.HistoryRecord{
		{
			LabelsAdded: []gmail.HistoryLabelChange{
				{
					Message:  gmail.MessageID{ID: "msg1", ThreadID: "thread_msg1"},
					LabelIDs: []string{"STARRED"},
				},
			},
		},
	}
	mock.HistoryID = 12350
	// Update the mock message with new labels
	mock.Messages["msg1"].LabelIDs = []string{"INBOX", "STARRED"}

	summary, err := syncer.Incremental(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("incremental sync with label added: %v", err)
	}

	// Should process the label change
	if summary.MessagesFound != 1 {
		t.Errorf("expected 1 history record processed, got %d", summary.MessagesFound)
	}
}

func TestIncrementalSyncWithLabelRemoved(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// First do a full sync to have a message with multiple labels
	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12340,
	}
	mock.AddMessage("msg1", testMIME, []string{"INBOX", "STARRED"})

	syncer := New(mock, st, nil)
	ctx := context.Background()

	// Full sync first
	_, err = syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	// Now simulate label removal via incremental
	mock.Profile.HistoryID = 12350
	mock.HistoryRecords = []gmail.HistoryRecord{
		{
			LabelsRemoved: []gmail.HistoryLabelChange{
				{
					Message:  gmail.MessageID{ID: "msg1", ThreadID: "thread_msg1"},
					LabelIDs: []string{"STARRED"},
				},
			},
		},
	}
	mock.HistoryID = 12350
	// Update the mock message to reflect removed label
	mock.Messages["msg1"].LabelIDs = []string{"INBOX"}

	summary, err := syncer.Incremental(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("incremental sync with label removed: %v", err)
	}

	// Should process the label change
	if summary.MessagesFound != 1 {
		t.Errorf("expected 1 history record processed, got %d", summary.MessagesFound)
	}
}

func TestIncrementalSyncLabelAddedToNewMessage(t *testing.T) {
	// Test case: Label is added to a message we don't have locally yet
	// This should trigger a fetch of the new message
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Create source with history ID and labels already synced
	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}
	if err := st.UpdateSourceSyncCursor(source.ID, "12340"); err != nil {
		t.Fatalf("UpdateSourceSyncCursor: %v", err)
	}
	// Pre-populate labels so handleLabelChange can work
	if _, err := st.EnsureLabel(source.ID, "INBOX", "Inbox", "system"); err != nil {
		t.Fatalf("EnsureLabel INBOX: %v", err)
	}
	if _, err := st.EnsureLabel(source.ID, "STARRED", "Starred", "system"); err != nil {
		t.Fatalf("EnsureLabel STARRED: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12350,
	}
	// Add message that we don't have locally
	mock.AddMessage("new-msg", testMIME, []string{"INBOX", "STARRED"})

	mock.HistoryRecords = []gmail.HistoryRecord{
		{
			LabelsAdded: []gmail.HistoryLabelChange{
				{
					Message:  gmail.MessageID{ID: "new-msg", ThreadID: "thread_new-msg"},
					LabelIDs: []string{"STARRED"},
				},
			},
		},
	}
	mock.HistoryID = 12350

	syncer := New(mock, st, nil)
	ctx := context.Background()

	_, err = syncer.Incremental(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("incremental sync: %v", err)
	}

	// The message should have been added to the database via handleLabelChange
	// Note: MessagesAdded counter doesn't capture messages added via label changes
	// (this is a known limitation), but we can verify the DB state
	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("GetStats() error = %v", err)
	}
	if stats.MessageCount != 1 {
		t.Errorf("expected 1 message in DB, got %d", stats.MessageCount)
	}
}

func TestIncrementalSyncLabelRemovedFromMissingMessage(t *testing.T) {
	// Test case: Label is removed from a message we don't have locally
	// This should be a no-op (don't fetch just to remove a label)
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Create source with history ID but no messages
	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}
	if err := st.UpdateSourceSyncCursor(source.ID, "12340"); err != nil {
		t.Fatalf("UpdateSourceSyncCursor: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12350,
	}
	// Don't add the message to mock - simulating we don't have it

	mock.HistoryRecords = []gmail.HistoryRecord{
		{
			LabelsRemoved: []gmail.HistoryLabelChange{
				{
					Message:  gmail.MessageID{ID: "unknown-msg", ThreadID: "thread_unknown"},
					LabelIDs: []string{"STARRED"},
				},
			},
		},
	}
	mock.HistoryID = 12350

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Incremental(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("incremental sync: %v", err)
	}

	// Should not have added any messages (label removal from unknown message is no-op)
	if summary.MessagesAdded != 0 {
		t.Errorf("expected 0 messages added, got %d", summary.MessagesAdded)
	}
}

// MIME message with attachment for testing
var testMIMEWithAttachment = []byte(`From: sender@example.com
To: recipient@example.com
Subject: Test with Attachment
Date: Mon, 01 Jan 2024 12:00:00 +0000
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="boundary123"

--boundary123
Content-Type: text/plain; charset="utf-8"

This is the message body.
--boundary123
Content-Type: application/octet-stream; name="test.bin"
Content-Disposition: attachment; filename="test.bin"
Content-Transfer-Encoding: base64

SGVsbG8gV29ybGQh
--boundary123--
`)

func TestFullSyncWithAttachment(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12345,
	}
	mock.AddMessage("msg-with-attachment", testMIMEWithAttachment, []string{"INBOX"})

	// Set up attachments directory
	attachDir := filepath.Join(tmpDir, "attachments")
	opts := &Options{
		AttachmentsDir: attachDir,
	}
	syncer := New(mock, st, opts)

	ctx := context.Background()
	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	if summary.MessagesAdded != 1 {
		t.Errorf("expected 1 message added, got %d", summary.MessagesAdded)
	}

	// Check if attachment directory was created
	if _, err := os.Stat(attachDir); os.IsNotExist(err) {
		t.Error("attachments directory should have been created")
	}

	// Check database for attachment record
	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("GetStats() error = %v", err)
	}
	if stats.AttachmentCount != 1 {
		t.Errorf("expected 1 attachment in db, got %d", stats.AttachmentCount)
	}
}

func TestFullSyncWithEmptyAttachment(t *testing.T) {
	// Test that empty attachments are skipped
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// MIME with empty attachment content (just headers)
	emptyAttachMIME := []byte(`From: sender@example.com
To: recipient@example.com
Subject: Empty Attachment
Date: Mon, 01 Jan 2024 12:00:00 +0000
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="boundary123"

--boundary123
Content-Type: text/plain; charset="utf-8"

Body text.
--boundary123
Content-Type: application/octet-stream; name="empty.bin"
Content-Disposition: attachment; filename="empty.bin"
Content-Transfer-Encoding: base64


--boundary123--
`)

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12345,
	}
	mock.AddMessage("msg-empty-attach", emptyAttachMIME, []string{"INBOX"})

	attachDir := filepath.Join(tmpDir, "attachments")
	opts := &Options{
		AttachmentsDir: attachDir,
	}
	syncer := New(mock, st, opts)

	ctx := context.Background()
	_, err = syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	// Empty attachments should be skipped
	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("GetStats() error = %v", err)
	}
	if stats.AttachmentCount != 0 {
		t.Errorf("expected 0 attachments (empty should be skipped), got %d", stats.AttachmentCount)
	}
}

func TestFullSyncAttachmentDeduplication(t *testing.T) {
	// Test that same attachment content is deduplicated
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 2,
		HistoryID:     12345,
	}
	// Two messages with identical attachment content
	mock.AddMessage("msg1-attach", testMIMEWithAttachment, []string{"INBOX"})
	mock.AddMessage("msg2-attach", testMIMEWithAttachment, []string{"INBOX"})

	attachDir := filepath.Join(tmpDir, "attachments")
	opts := &Options{
		AttachmentsDir: attachDir,
	}
	syncer := New(mock, st, opts)

	ctx := context.Background()
	_, err = syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	// Should have 2 attachment records (one per message) but only one file
	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("GetStats() error = %v", err)
	}
	if stats.AttachmentCount != 2 {
		t.Errorf("expected 2 attachment records, got %d", stats.AttachmentCount)
	}

	// Verify filesystem deduplication - should only have one file (content-addressed)
	// Use WalkDir to count files recursively (content-addressed storage uses subdirectories)
	var fileCount int
	err = filepath.WalkDir(attachDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			fileCount++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir(attachments) error = %v", err)
	}
	if fileCount != 1 {
		t.Errorf("expected 1 file in attachments dir (deduped), got %d", fileCount)
	}
}

// MIME message with no subject
var testMIMENoSubject = []byte(`From: sender@example.com
To: recipient@example.com
Date: Mon, 01 Jan 2024 12:00:00 +0000
Content-Type: text/plain; charset="utf-8"

Message with no subject line.
`)

func TestFullSyncNoSubject(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12345,
	}
	mock.AddMessage("msg-no-subject", testMIMENoSubject, []string{"INBOX"})

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	if summary.MessagesAdded != 1 {
		t.Errorf("expected 1 message added, got %d", summary.MessagesAdded)
	}
}

// MIME message with multiple recipients (CC and BCC)
var testMIMEMultipleRecipients = []byte(`From: sender@example.com
To: to1@example.com, to2@example.com
Cc: cc1@example.com
Bcc: bcc1@example.com
Subject: Multiple Recipients
Date: Mon, 01 Jan 2024 12:00:00 +0000
Content-Type: text/plain; charset="utf-8"

Message with multiple recipients.
`)

func TestFullSyncMultipleRecipients(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12345,
	}
	mock.AddMessage("msg-multi-recip", testMIMEMultipleRecipients, []string{"INBOX"})

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	if summary.MessagesAdded != 1 {
		t.Errorf("expected 1 message added, got %d", summary.MessagesAdded)
	}
}

func TestFullSyncWithMIMEParseError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 2,
		HistoryID:     12345,
	}
	// One valid message, one with broken MIME
	mock.AddMessage("msg-good", testMIME, []string{"INBOX"})
	// Add an invalid MIME message (completely malformed)
	mock.Messages["msg-bad"] = &gmail.RawMessage{
		ID:           "msg-bad",
		ThreadID:     "thread_msg-bad",
		LabelIDs:     []string{"INBOX"},
		Raw:          []byte("not valid mime at all - just garbage"),
		Snippet:      "This is the snippet preview",
		SizeEstimate: 100,
	}

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	// Both messages should be stored - bad MIME gets placeholder body
	if summary.MessagesAdded != 2 {
		t.Errorf("expected 2 messages added (including MIME error with placeholder), got %d", summary.MessagesAdded)
	}
	// No errors - MIME parse failures are now warnings, not errors
	if summary.Errors != 0 {
		t.Errorf("expected 0 errors (MIME failures stored with placeholder), got %d", summary.Errors)
	}

	// Verify the bad message was stored with placeholder content
	var bodyText string
	err = st.DB().QueryRow(`
		SELECT body_text FROM messages WHERE source_message_id = 'msg-bad'
	`).Scan(&bodyText)
	if err != nil {
		t.Fatalf("query bad message: %v", err)
	}
	if !strings.Contains(bodyText, "MIME parsing failed") {
		t.Errorf("expected placeholder body with error message, got: %s", bodyText)
	}

	// Verify raw MIME was preserved
	var rawData []byte
	err = st.DB().QueryRow(`
		SELECT raw_data FROM message_raw mr
		JOIN messages m ON m.id = mr.message_id
		WHERE m.source_message_id = 'msg-bad'
	`).Scan(&rawData)
	if err != nil {
		t.Fatalf("query raw data: %v", err)
	}
	// Raw data is zlib compressed, but should exist
	if len(rawData) == 0 {
		t.Error("expected raw MIME data to be preserved")
	}
}

func TestFullSyncMessageFetchError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 2,
		HistoryID:     12345,
	}
	mock.AddMessage("msg-good", testMIME, []string{"INBOX"})

	// Configure message list to return both IDs, but only one exists
	mock.MessagePages = [][]string{{"msg-good", "msg-missing"}}
	// msg-missing won't be in Messages map, so GetMessageRaw will fail with 404

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	// Should have added the good message, skipped the missing one
	if summary.MessagesAdded != 1 {
		t.Errorf("expected 1 message added, got %d", summary.MessagesAdded)
	}
}

func TestIncrementalSyncLabelsError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Create source with history ID
	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}
	if err := st.UpdateSourceSyncCursor(source.ID, "12340"); err != nil {
		t.Fatalf("UpdateSourceSyncCursor: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12350,
	}
	// Make labels call fail
	mock.LabelsError = fmt.Errorf("labels API error")

	syncer := New(mock, st, nil)
	ctx := context.Background()

	_, err = syncer.Incremental(ctx, "test@example.com")
	if err == nil {
		t.Error("expected error when labels sync fails")
	}
}

func TestFullSyncResumeWithCursor(t *testing.T) {
	// Test resuming a sync from a saved checkpoint with pre-existing data
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 4,
		HistoryID:     12345,
	}
	// Two pages of messages
	mock.MessagePages = [][]string{
		{"msg1", "msg2"},
		{"msg3", "msg4"},
	}
	mock.AddMessage("msg1", testMIME, []string{"INBOX"})
	mock.AddMessage("msg2", testMIME, []string{"INBOX"})
	mock.AddMessage("msg3", testMIME, []string{"INBOX"})
	mock.AddMessage("msg4", testMIME, []string{"INBOX"})

	// Setup: Create source
	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}

	// Pre-populate page 1 messages (msg1, msg2) to simulate a real interrupted sync
	// This models the scenario where page 1 was processed but page 2 wasn't
	syncer := New(mock, st, nil)
	ctx := context.Background()

	// First: Process just page 1 by running a sync with only page 1 in mock
	mock.MessagePages = [][]string{{"msg1", "msg2"}}
	_, err = syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("initial sync (page 1): %v", err)
	}

	// Verify page 1 messages are in DB
	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("GetStats after page 1: %v", err)
	}
	if stats.MessageCount != 2 {
		t.Fatalf("expected 2 messages after page 1, got %d", stats.MessageCount)
	}

	// Now set up for resume: restore both pages and create an "interrupted" sync
	mock.MessagePages = [][]string{
		{"msg1", "msg2"},
		{"msg3", "msg4"},
	}
	mock.ListMessagesCalls = 0 // Reset call counter

	// Start a new sync run that will be "interrupted"
	syncID, err := st.StartSync(source.ID, "full")
	if err != nil {
		t.Fatalf("StartSync: %v", err)
	}

	// Save checkpoint as if we processed page 1 and are ready for page 2
	checkpoint := &store.Checkpoint{
		PageToken:         "page_1", // MockAPI expects "page_N" format
		MessagesProcessed: 2,
		MessagesAdded:     2,
	}
	if err := st.UpdateSyncCheckpoint(syncID, checkpoint); err != nil {
		t.Fatalf("UpdateSyncCheckpoint: %v", err)
	}

	// Resume the sync
	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync resume: %v", err)
	}

	// Should have resumed from checkpoint
	if !summary.WasResumed {
		t.Error("expected WasResumed = true")
	}
	if summary.ResumedFromToken != "page_1" {
		t.Errorf("expected ResumedFromToken = 'page_1', got %q", summary.ResumedFromToken)
	}

	// Summary includes cumulative counts (checkpointed + new)
	if summary.MessagesAdded != 4 {
		t.Errorf("expected 4 total messages in summary, got %d", summary.MessagesAdded)
	}

	// Verify only page 2 was fetched (resumed from page_1 token)
	if mock.ListMessagesCalls != 1 {
		t.Errorf("expected 1 ListMessages call (resumed from page_1), got %d", mock.ListMessagesCalls)
	}

	// Verify all 4 messages are now in the database (2 from page 1 + 2 from page 2)
	stats, err = st.GetStats()
	if err != nil {
		t.Fatalf("GetStats() error = %v", err)
	}
	if stats.MessageCount != 4 {
		t.Errorf("expected 4 messages in DB (page 1 + page 2), got %d", stats.MessageCount)
	}
}

func TestFullSyncHTMLOnlyMessage(t *testing.T) {
	// Test message with HTML body but no plain text
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	htmlOnlyMIME := []byte(`From: sender@example.com
To: recipient@example.com
Subject: HTML Only
Date: Mon, 01 Jan 2024 12:00:00 +0000
MIME-Version: 1.0
Content-Type: text/html; charset="utf-8"

<html><body><p>This is HTML only content.</p></body></html>
`)

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12345,
	}
	mock.AddMessage("msg-html-only", htmlOnlyMIME, []string{"INBOX"})

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	if summary.MessagesAdded != 1 {
		t.Errorf("expected 1 message added, got %d", summary.MessagesAdded)
	}
}

// MIME message with duplicate recipients across To/Cc/Bcc
// Tests: deduplication, and preferring non-empty display names
var testMIMEDuplicateRecipients = []byte(`From: sender@example.com
To: duplicate@example.com, other@example.com, "Duplicate Person" <duplicate@example.com>
Cc: cc-dup@example.com, "CC Duplicate" <cc-dup@example.com>
Bcc: bcc-dup@example.com, bcc-dup@example.com
Subject: Duplicate Recipients
Date: Mon, 01 Jan 2024 12:00:00 +0000
Content-Type: text/plain; charset="utf-8"

Message with duplicate recipients in To, Cc, and Bcc fields.
`)

func TestFullSyncDuplicateRecipients(t *testing.T) {
	// Test that duplicate emails in To/Cc/Bcc:
	// 1. Don't cause UNIQUE constraint failures
	// 2. Prefer non-empty display names when duplicates have different names
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12345,
	}
	mock.AddMessage("msg-dup-recip", testMIMEDuplicateRecipients, []string{"INBOX"})

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync with duplicate recipients: %v", err)
	}

	if summary.MessagesAdded != 1 {
		t.Errorf("expected 1 message added, got %d", summary.MessagesAdded)
	}
	if summary.Errors != 0 {
		t.Errorf("expected 0 errors, got %d", summary.Errors)
	}

	// Verify the message was stored correctly
	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("GetStats() error = %v", err)
	}
	if stats.MessageCount != 1 {
		t.Errorf("expected 1 message in db, got %d", stats.MessageCount)
	}

	// Verify To recipients are deduplicated: duplicate@example.com appears twice, other once = 2 unique
	var toCount int
	err = st.DB().QueryRow(st.Rebind(`
		SELECT COUNT(*) FROM message_recipients mr
		JOIN messages m ON mr.message_id = m.id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'to'
	`), "msg-dup-recip").Scan(&toCount)
	if err != nil {
		t.Fatalf("query To recipient count: %v", err)
	}
	if toCount != 2 {
		t.Errorf("expected 2 unique To recipients, got %d", toCount)
	}

	// Verify Cc recipients are deduplicated: cc-dup@example.com appears twice = 1 unique
	var ccCount int
	err = st.DB().QueryRow(st.Rebind(`
		SELECT COUNT(*) FROM message_recipients mr
		JOIN messages m ON mr.message_id = m.id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'cc'
	`), "msg-dup-recip").Scan(&ccCount)
	if err != nil {
		t.Fatalf("query Cc recipient count: %v", err)
	}
	if ccCount != 1 {
		t.Errorf("expected 1 unique Cc recipient, got %d", ccCount)
	}

	// Verify Bcc recipients are deduplicated: bcc-dup@example.com appears twice = 1 unique
	var bccCount int
	err = st.DB().QueryRow(st.Rebind(`
		SELECT COUNT(*) FROM message_recipients mr
		JOIN messages m ON mr.message_id = m.id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'bcc'
	`), "msg-dup-recip").Scan(&bccCount)
	if err != nil {
		t.Fatalf("query Bcc recipient count: %v", err)
	}
	if bccCount != 1 {
		t.Errorf("expected 1 unique Bcc recipient, got %d", bccCount)
	}

	// Verify display name preference: duplicate@example.com first appears without name,
	// then with "Duplicate Person" - should prefer the non-empty name
	var displayName string
	err = st.DB().QueryRow(st.Rebind(`
		SELECT mr.display_name FROM message_recipients mr
		JOIN messages m ON mr.message_id = m.id
		JOIN participants p ON mr.participant_id = p.id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'to' AND p.email_address = ?
	`), "msg-dup-recip", "duplicate@example.com").Scan(&displayName)
	if err != nil {
		t.Fatalf("query display name: %v", err)
	}
	if displayName != "Duplicate Person" {
		t.Errorf("expected display name 'Duplicate Person' (non-empty preferred), got %q", displayName)
	}

	// Verify Cc display name preference: first empty, then "CC Duplicate"
	err = st.DB().QueryRow(st.Rebind(`
		SELECT mr.display_name FROM message_recipients mr
		JOIN messages m ON mr.message_id = m.id
		JOIN participants p ON mr.participant_id = p.id
		WHERE m.source_message_id = ? AND mr.recipient_type = 'cc' AND p.email_address = ?
	`), "msg-dup-recip", "cc-dup@example.com").Scan(&displayName)
	if err != nil {
		t.Fatalf("query Cc display name: %v", err)
	}
	if displayName != "CC Duplicate" {
		t.Errorf("expected Cc display name 'CC Duplicate' (non-empty preferred), got %q", displayName)
	}
}

func TestFullSyncDateFallbackToInternalDate(t *testing.T) {
	// Test that when the Date header is unparseable, SentAt falls back to InternalDate
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	// Message with unparseable Date header
	badDateMIME := []byte(`From: sender@example.com
To: recipient@example.com
Subject: Bad Date
Date: This is not a valid date
Content-Type: text/plain; charset="utf-8"

Message with invalid date header.
`)

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12345,
	}
	// InternalDate is Jan 15, 2024 12:00:00 UTC in milliseconds
	mock.Messages["msg-bad-date"] = &gmail.RawMessage{
		ID:           "msg-bad-date",
		ThreadID:     "thread-bad-date",
		LabelIDs:     []string{"INBOX"},
		Raw:          badDateMIME,
		InternalDate: 1705320000000, // 2024-01-15T12:00:00Z
	}
	mock.MessagePages = [][]string{{"msg-bad-date"}}

	syncer := New(mock, st, nil)
	ctx := context.Background()

	_, err = syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	// Verify SentAt was set to InternalDate
	var sentAt, internalDate string
	err = st.DB().QueryRow(st.Rebind(`
		SELECT sent_at, internal_date FROM messages WHERE source_message_id = ?
	`), "msg-bad-date").Scan(&sentAt, &internalDate)
	if err != nil {
		t.Fatalf("query message: %v", err)
	}

	// Both should be set and equal (fallback behavior)
	if sentAt == "" {
		t.Errorf("SentAt should not be empty (should fallback to InternalDate)")
	}
	if internalDate == "" {
		t.Errorf("InternalDate should not be empty")
	}
	if sentAt != internalDate {
		t.Errorf("SentAt (%q) should equal InternalDate (%q) when Date header is unparseable", sentAt, internalDate)
	}

	// Verify the date is correct (2024-01-15T12:00:00Z)
	if !strings.Contains(sentAt, "2024-01-15") || !strings.Contains(sentAt, "12:00:00") {
		t.Errorf("SentAt = %q, expected to contain 2024-01-15 12:00:00", sentAt)
	}
}

func TestFullSyncEmptyRawMIME(t *testing.T) {
	// Test that messages with empty raw MIME data are handled gracefully (counted as errors, not crashes)
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 2,
		HistoryID:     12345,
	}

	// One good message, one with empty raw MIME
	mock.AddMessage("msg-good", testMIME, []string{"INBOX"})
	mock.Messages["msg-empty-raw"] = &gmail.RawMessage{
		ID:           "msg-empty-raw",
		ThreadID:     "thread-empty-raw",
		LabelIDs:     []string{"INBOX"},
		Raw:          []byte{}, // Empty raw MIME
		SizeEstimate: 0,
	}

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	// Should have added the good message and counted the empty one as an error
	if summary.MessagesAdded != 1 {
		t.Errorf("expected 1 message added, got %d", summary.MessagesAdded)
	}
	if summary.Errors != 1 {
		t.Errorf("expected 1 error (empty raw MIME), got %d", summary.Errors)
	}
}

func TestFullSyncEmptyThreadID(t *testing.T) {
	// Test that messages with empty thread ID use message ID as fallback
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12345,
	}
	// Enable using the RawMessage.ThreadID for listing (allows empty thread IDs)
	mock.UseRawThreadID = true

	// Message with empty thread ID
	mock.Messages["msg-no-thread"] = &gmail.RawMessage{
		ID:           "msg-no-thread",
		ThreadID:     "", // Empty thread ID - should fallback to message ID
		LabelIDs:     []string{"INBOX"},
		Raw:          testMIME,
		SizeEstimate: int64(len(testMIME)),
	}
	mock.MessagePages = [][]string{{"msg-no-thread"}}

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	if summary.MessagesAdded != 1 {
		t.Errorf("expected 1 message added, got %d", summary.MessagesAdded)
	}
	if summary.Errors != 0 {
		t.Errorf("expected 0 errors, got %d", summary.Errors)
	}

	// Verify the message was stored with a valid thread (using message ID as thread ID)
	var threadSourceID string
	err = st.DB().QueryRow(st.Rebind(`
		SELECT c.source_conversation_id FROM conversations c
		JOIN messages m ON m.conversation_id = c.id
		WHERE m.source_message_id = ?
	`), "msg-no-thread").Scan(&threadSourceID)
	if err != nil {
		t.Fatalf("query thread: %v", err)
	}
	if threadSourceID != "msg-no-thread" {
		t.Errorf("expected thread source_conversation_id = 'msg-no-thread' (fallback), got %q", threadSourceID)
	}
}

func TestFullSyncListEmptyThreadIDRawPresent(t *testing.T) {
	// Test that when list response has empty threadID but raw message has threadID,
	// we use raw.ThreadID (not the message ID fallback)
	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	if err := st.InitSchema(); err != nil {
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 1,
		HistoryID:     12345,
	}

	// Configure mock: list returns empty threadID, but raw message has a threadID
	mock.ListThreadIDOverride = map[string]string{
		"msg-list-empty": "", // List response has empty threadID
	}
	mock.Messages["msg-list-empty"] = &gmail.RawMessage{
		ID:           "msg-list-empty",
		ThreadID:     "actual-thread-from-raw", // Raw message has the real threadID
		LabelIDs:     []string{"INBOX"},
		Raw:          testMIME,
		SizeEstimate: int64(len(testMIME)),
	}
	mock.MessagePages = [][]string{{"msg-list-empty"}}

	syncer := New(mock, st, nil)
	ctx := context.Background()

	summary, err := syncer.Full(ctx, "test@example.com")
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}

	if summary.MessagesAdded != 1 {
		t.Errorf("expected 1 message added, got %d", summary.MessagesAdded)
	}
	if summary.Errors != 0 {
		t.Errorf("expected 0 errors, got %d", summary.Errors)
	}

	// Verify the thread ID came from raw.ThreadID, not the message ID fallback
	var threadSourceID string
	err = st.DB().QueryRow(st.Rebind(`
		SELECT c.source_conversation_id FROM conversations c
		JOIN messages m ON m.conversation_id = c.id
		WHERE m.source_message_id = ?
	`), "msg-list-empty").Scan(&threadSourceID)
	if err != nil {
		t.Fatalf("query thread: %v", err)
	}
	// Should use raw.ThreadID, not message ID
	if threadSourceID != "actual-thread-from-raw" {
		t.Errorf("expected thread source_conversation_id = 'actual-thread-from-raw' (from raw), got %q", threadSourceID)
	}
}
