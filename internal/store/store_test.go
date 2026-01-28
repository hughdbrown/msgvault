package store_test

import (
	"database/sql"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/mime"
	"github.com/wesm/msgvault/internal/store"
	"github.com/wesm/msgvault/internal/testutil"
)

// mustNoErr fails the test immediately if err is non-nil.
// Use this for setup operations where failure means the test cannot proceed.
func mustNoErr(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

func TestStore_Open(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Store should be usable
	if st.DB() == nil {
		t.Error("DB() returned nil")
	}
}

func TestStore_GetStats_Empty(t *testing.T) {
	st := testutil.NewTestStore(t)

	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("GetStats() error = %v", err)
	}

	if stats.MessageCount != 0 {
		t.Errorf("MessageCount = %d, want 0", stats.MessageCount)
	}
}

func TestStore_Source_CreateAndGet(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Create source
	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource() error = %v", err)
	}

	if source.ID == 0 {
		t.Error("source ID should be non-zero")
	}
	if source.SourceType != "gmail" {
		t.Errorf("SourceType = %q, want %q", source.SourceType, "gmail")
	}
	if source.Identifier != "test@example.com" {
		t.Errorf("Identifier = %q, want %q", source.Identifier, "test@example.com")
	}

	// Get same source again (should return existing)
	source2, err := st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource() second call error = %v", err)
	}

	if source2.ID != source.ID {
		t.Errorf("second call ID = %d, want %d", source2.ID, source.ID)
	}
}

func TestStore_Source_UpdateSyncCursor(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	if err != nil {
		t.Fatalf("GetOrCreateSource() error = %v", err)
	}

	err = st.UpdateSourceSyncCursor(source.ID, "12345")
	if err != nil {
		t.Fatalf("UpdateSourceSyncCursor() error = %v", err)
	}

	// Verify cursor was updated
	updated, err := st.GetSourceByIdentifier("test@example.com")
	if err != nil {
		t.Fatalf("GetSourceByIdentifier() error = %v", err)
	}

	if !updated.SyncCursor.Valid || updated.SyncCursor.String != "12345" {
		t.Errorf("SyncCursor = %v, want 12345", updated.SyncCursor)
	}
}

func TestStore_Conversation(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")

	// Create conversation
	convID, err := st.EnsureConversation(source.ID, "thread-123", "Test Thread")
	if err != nil {
		t.Fatalf("EnsureConversation() error = %v", err)
	}

	if convID == 0 {
		t.Error("conversation ID should be non-zero")
	}

	// Get same conversation (should return existing)
	convID2, err := st.EnsureConversation(source.ID, "thread-123", "Test Thread")
	if err != nil {
		t.Fatalf("EnsureConversation() second call error = %v", err)
	}

	if convID2 != convID {
		t.Errorf("second call ID = %d, want %d", convID2, convID)
	}
}

func TestStore_Message_Upsert(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-123", "Test Thread")
	mustNoErr(t, err, "EnsureConversation")

	msg := &store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-456",
		MessageType:     "email",
		Subject:         sql.NullString{String: "Test Subject", Valid: true},
		BodyText:        sql.NullString{String: "Test body", Valid: true},
		SizeEstimate:    1000,
		SentAt:          sql.NullTime{Time: time.Now(), Valid: true},
	}

	// Insert
	msgID, err := st.UpsertMessage(msg)
	if err != nil {
		t.Fatalf("UpsertMessage() error = %v", err)
	}

	if msgID == 0 {
		t.Error("message ID should be non-zero")
	}

	// Update (same source_message_id)
	msg.Subject = sql.NullString{String: "Updated Subject", Valid: true}
	msgID2, err := st.UpsertMessage(msg)
	if err != nil {
		t.Fatalf("UpsertMessage() update error = %v", err)
	}

	if msgID2 != msgID {
		t.Errorf("update ID = %d, want %d", msgID2, msgID)
	}

	// Verify stats
	stats, err := st.GetStats()
	mustNoErr(t, err, "GetStats")
	if stats.MessageCount != 1 {
		t.Errorf("MessageCount = %d, want 1", stats.MessageCount)
	}
}

func TestStore_MessageExistsBatch(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-123", "Test")
	mustNoErr(t, err, "EnsureConversation")

	// Insert some messages
	ids := []string{"msg-1", "msg-2", "msg-3"}
	for _, id := range ids {
		_, err := st.UpsertMessage(&store.Message{
			ConversationID:  convID,
			SourceID:        source.ID,
			SourceMessageID: id,
			MessageType:     "email",
		})
		mustNoErr(t, err, "UpsertMessage")
	}

	// Check which exist
	checkIDs := []string{"msg-1", "msg-2", "msg-4", "msg-5"}
	existing, err := st.MessageExistsBatch(source.ID, checkIDs)
	if err != nil {
		t.Fatalf("MessageExistsBatch() error = %v", err)
	}

	if len(existing) != 2 {
		t.Errorf("len(existing) = %d, want 2", len(existing))
	}
	if _, ok := existing["msg-1"]; !ok {
		t.Error("msg-1 should exist")
	}
	if _, ok := existing["msg-2"]; !ok {
		t.Error("msg-2 should exist")
	}
	if _, ok := existing["msg-4"]; ok {
		t.Error("msg-4 should not exist")
	}
}

func TestStore_MessageRaw(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-123", "Test")
	mustNoErr(t, err, "EnsureConversation")

	msgID, err := st.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-1",
		MessageType:     "email",
	})
	mustNoErr(t, err, "UpsertMessage")

	rawData := []byte("From: test@example.com\r\nSubject: Test\r\n\r\nBody")

	// Store raw data
	err = st.UpsertMessageRaw(msgID, rawData)
	if err != nil {
		t.Fatalf("UpsertMessageRaw() error = %v", err)
	}

	// Retrieve raw data
	retrieved, err := st.GetMessageRaw(msgID)
	if err != nil {
		t.Fatalf("GetMessageRaw() error = %v", err)
	}

	if string(retrieved) != string(rawData) {
		t.Errorf("retrieved data = %q, want %q", retrieved, rawData)
	}
}

func TestStore_Participant(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Create participant
	pid, err := st.EnsureParticipant("alice@example.com", "Alice Smith", "example.com")
	if err != nil {
		t.Fatalf("EnsureParticipant() error = %v", err)
	}

	if pid == 0 {
		t.Error("participant ID should be non-zero")
	}

	// Get same participant
	pid2, err := st.EnsureParticipant("alice@example.com", "Alice", "example.com")
	if err != nil {
		t.Fatalf("EnsureParticipant() second call error = %v", err)
	}

	if pid2 != pid {
		t.Errorf("second call ID = %d, want %d", pid2, pid)
	}
}

func TestStore_EnsureParticipantsBatch(t *testing.T) {
	st := testutil.NewTestStore(t)

	addresses := []mime.Address{
		{Email: "alice@example.com", Name: "Alice", Domain: "example.com"},
		{Email: "bob@example.org", Name: "Bob", Domain: "example.org"},
		{Email: "", Name: "No Email", Domain: ""}, // Should be skipped
	}

	result, err := st.EnsureParticipantsBatch(addresses)
	if err != nil {
		t.Fatalf("EnsureParticipantsBatch() error = %v", err)
	}

	if len(result) != 2 {
		t.Errorf("len(result) = %d, want 2", len(result))
	}
	if _, ok := result["alice@example.com"]; !ok {
		t.Error("alice@example.com should be in result")
	}
	if _, ok := result["bob@example.org"]; !ok {
		t.Error("bob@example.org should be in result")
	}
}

func TestStore_Label(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")

	// Create label
	lid, err := st.EnsureLabel(source.ID, "INBOX", "Inbox", "system")
	if err != nil {
		t.Fatalf("EnsureLabel() error = %v", err)
	}

	if lid == 0 {
		t.Error("label ID should be non-zero")
	}

	// Get same label
	lid2, err := st.EnsureLabel(source.ID, "INBOX", "Inbox", "system")
	if err != nil {
		t.Fatalf("EnsureLabel() second call error = %v", err)
	}

	if lid2 != lid {
		t.Errorf("second call ID = %d, want %d", lid2, lid)
	}
}

func TestStore_EnsureLabelsBatch(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")

	labels := map[string]string{
		"INBOX":       "Inbox",
		"SENT":        "Sent",
		"Label_12345": "My Label",
	}

	result, err := st.EnsureLabelsBatch(source.ID, labels)
	if err != nil {
		t.Fatalf("EnsureLabelsBatch() error = %v", err)
	}

	if len(result) != 3 {
		t.Errorf("len(result) = %d, want 3", len(result))
	}
	for sourceLabelID := range labels {
		if _, ok := result[sourceLabelID]; !ok {
			t.Errorf("%s should be in result", sourceLabelID)
		}
	}
}

func TestStore_MessageLabels(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-123", "Test")
	mustNoErr(t, err, "EnsureConversation")

	msgID, err := st.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-1",
		MessageType:     "email",
	})
	mustNoErr(t, err, "UpsertMessage")

	lid1, err := st.EnsureLabel(source.ID, "INBOX", "Inbox", "system")
	mustNoErr(t, err, "EnsureLabel INBOX")
	lid2, err := st.EnsureLabel(source.ID, "STARRED", "Starred", "system")
	mustNoErr(t, err, "EnsureLabel STARRED")

	// Set labels
	err = st.ReplaceMessageLabels(msgID, []int64{lid1, lid2})
	if err != nil {
		t.Fatalf("ReplaceMessageLabels() error = %v", err)
	}

	// Verify labels were set (query message_labels directly)
	var count int
	err = st.DB().QueryRow(st.Rebind("SELECT COUNT(*) FROM message_labels WHERE message_id = ?"), msgID).Scan(&count)
	mustNoErr(t, err, "count labels")
	if count != 2 {
		t.Errorf("label count = %d, want 2", count)
	}

	// Replace with different labels
	lid3, err := st.EnsureLabel(source.ID, "SENT", "Sent", "system")
	mustNoErr(t, err, "EnsureLabel SENT")
	err = st.ReplaceMessageLabels(msgID, []int64{lid3})
	if err != nil {
		t.Fatalf("ReplaceMessageLabels() replace error = %v", err)
	}

	// Verify only new label remains
	err = st.DB().QueryRow(st.Rebind("SELECT COUNT(*) FROM message_labels WHERE message_id = ?"), msgID).Scan(&count)
	mustNoErr(t, err, "count labels after replace")
	if count != 1 {
		t.Errorf("label count after replace = %d, want 1", count)
	}

	// Verify it's the right label
	var labelID int64
	err = st.DB().QueryRow(st.Rebind("SELECT label_id FROM message_labels WHERE message_id = ?"), msgID).Scan(&labelID)
	mustNoErr(t, err, "get label_id")
	if labelID != lid3 {
		t.Errorf("label_id = %d, want %d (SENT)", labelID, lid3)
	}
}

func TestStore_MessageRecipients(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-123", "Test")
	mustNoErr(t, err, "EnsureConversation")

	msgID, err := st.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-1",
		MessageType:     "email",
	})
	mustNoErr(t, err, "UpsertMessage")

	pid1, err := st.EnsureParticipant("alice@example.com", "Alice", "example.com")
	mustNoErr(t, err, "EnsureParticipant alice")
	pid2, err := st.EnsureParticipant("bob@example.org", "Bob", "example.org")
	mustNoErr(t, err, "EnsureParticipant bob")

	// Set recipients
	err = st.ReplaceMessageRecipients(msgID, "to", []int64{pid1, pid2}, []string{"Alice", "Bob"})
	if err != nil {
		t.Fatalf("ReplaceMessageRecipients() error = %v", err)
	}

	// Verify recipients were set
	var count int
	err = st.DB().QueryRow(st.Rebind("SELECT COUNT(*) FROM message_recipients WHERE message_id = ? AND recipient_type = 'to'"), msgID).Scan(&count)
	mustNoErr(t, err, "count recipients")
	if count != 2 {
		t.Errorf("recipient count = %d, want 2", count)
	}

	// Replace recipients
	err = st.ReplaceMessageRecipients(msgID, "to", []int64{pid1}, []string{"Alice"})
	if err != nil {
		t.Fatalf("ReplaceMessageRecipients() replace error = %v", err)
	}

	// Verify only one recipient remains
	err = st.DB().QueryRow(st.Rebind("SELECT COUNT(*) FROM message_recipients WHERE message_id = ? AND recipient_type = 'to'"), msgID).Scan(&count)
	mustNoErr(t, err, "count recipients after replace")
	if count != 1 {
		t.Errorf("recipient count after replace = %d, want 1", count)
	}

	// Verify it's the right recipient
	var participantID int64
	err = st.DB().QueryRow(st.Rebind("SELECT participant_id FROM message_recipients WHERE message_id = ? AND recipient_type = 'to'"), msgID).Scan(&participantID)
	mustNoErr(t, err, "get participant_id")
	if participantID != pid1 {
		t.Errorf("participant_id = %d, want %d (alice)", participantID, pid1)
	}
}

func TestStore_MarkMessageDeleted(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-123", "Test")
	mustNoErr(t, err, "EnsureConversation")

	msgID, err := st.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-1",
		MessageType:     "email",
	})
	mustNoErr(t, err, "UpsertMessage")

	// Verify not deleted initially
	var deletedAt sql.NullTime
	err = st.DB().QueryRow(st.Rebind("SELECT deleted_from_source_at FROM messages WHERE id = ?"), msgID).Scan(&deletedAt)
	mustNoErr(t, err, "check deleted_from_source_at before")
	if deletedAt.Valid {
		t.Error("deleted_from_source_at should be NULL before MarkMessageDeleted")
	}

	err = st.MarkMessageDeleted(source.ID, "msg-1")
	if err != nil {
		t.Fatalf("MarkMessageDeleted() error = %v", err)
	}

	// Verify deleted flag is now set
	err = st.DB().QueryRow(st.Rebind("SELECT deleted_from_source_at FROM messages WHERE id = ?"), msgID).Scan(&deletedAt)
	mustNoErr(t, err, "check deleted_from_source_at after")
	if !deletedAt.Valid {
		t.Error("deleted_from_source_at should be set after MarkMessageDeleted")
	}
}

func TestStore_Attachment(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-123", "Test")
	mustNoErr(t, err, "EnsureConversation")

	msgID, err := st.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-1",
		MessageType:     "email",
		HasAttachments:  true,
	})
	mustNoErr(t, err, "UpsertMessage")

	err = st.UpsertAttachment(msgID, "document.pdf", "application/pdf", "/path/to/file", "abc123hash", 1024)
	if err != nil {
		t.Fatalf("UpsertAttachment() error = %v", err)
	}

	// Upsert same attachment (should not error, dedupe by content_hash)
	err = st.UpsertAttachment(msgID, "document.pdf", "application/pdf", "/path/to/file", "abc123hash", 1024)
	if err != nil {
		t.Fatalf("UpsertAttachment() duplicate error = %v", err)
	}

	stats, err := st.GetStats()
	mustNoErr(t, err, "GetStats")
	if stats.AttachmentCount != 1 {
		t.Errorf("AttachmentCount = %d, want 1", stats.AttachmentCount)
	}
}

func TestStore_SyncRun(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")

	// Start a sync
	syncID, err := st.StartSync(source.ID, "full")
	if err != nil {
		t.Fatalf("StartSync() error = %v", err)
	}

	if syncID == 0 {
		t.Error("sync ID should be non-zero")
	}

	// Get active sync
	active, err := st.GetActiveSync(source.ID)
	if err != nil {
		t.Fatalf("GetActiveSync() error = %v", err)
	}

	if active == nil {
		t.Fatal("expected active sync, got nil")
	}
	if active.ID != syncID {
		t.Errorf("active sync ID = %d, want %d", active.ID, syncID)
	}
	if active.Status != "running" {
		t.Errorf("status = %q, want %q", active.Status, "running")
	}
}

func TestStore_SyncCheckpoint(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	syncID, err := st.StartSync(source.ID, "full")
	mustNoErr(t, err, "StartSync")

	cp := &store.Checkpoint{
		PageToken:         "next-page-token",
		MessagesProcessed: 100,
		MessagesAdded:     50,
		MessagesUpdated:   10,
		ErrorsCount:       2,
	}

	err = st.UpdateSyncCheckpoint(syncID, cp)
	if err != nil {
		t.Fatalf("UpdateSyncCheckpoint() error = %v", err)
	}

	// Verify checkpoint was saved
	active, err := st.GetActiveSync(source.ID)
	mustNoErr(t, err, "GetActiveSync")
	if active == nil {
		t.Fatal("expected active sync, got nil")
	}
	if active.MessagesProcessed != 100 {
		t.Errorf("MessagesProcessed = %d, want 100", active.MessagesProcessed)
	}
}

func TestStore_SyncComplete(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	syncID, err := st.StartSync(source.ID, "full")
	mustNoErr(t, err, "StartSync")

	err = st.CompleteSync(syncID, "history-12345")
	if err != nil {
		t.Fatalf("CompleteSync() error = %v", err)
	}

	// Active sync should be nil now
	active, err := st.GetActiveSync(source.ID)
	mustNoErr(t, err, "GetActiveSync")
	if active != nil {
		t.Errorf("expected no active sync after completion, got %+v", active)
	}

	// Should have a successful sync
	lastSync, err := st.GetLastSuccessfulSync(source.ID)
	if err != nil {
		t.Fatalf("GetLastSuccessfulSync() error = %v", err)
	}
	if lastSync == nil {
		t.Fatal("expected last successful sync, got nil")
	}
	if lastSync.Status != "completed" {
		t.Errorf("status = %q, want %q", lastSync.Status, "completed")
	}
}

func TestStore_SyncFail(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	syncID, err := st.StartSync(source.ID, "full")
	mustNoErr(t, err, "StartSync")

	err = st.FailSync(syncID, "network error")
	if err != nil {
		t.Fatalf("FailSync() error = %v", err)
	}

	// Active sync should be nil now
	active, err := st.GetActiveSync(source.ID)
	mustNoErr(t, err, "GetActiveSync")
	if active != nil {
		t.Errorf("expected no active sync after failure, got %+v", active)
	}

	// Verify sync status is "failed" and error message is stored
	var status, errorMsg string
	err = st.DB().QueryRow(st.Rebind("SELECT status, error_message FROM sync_runs WHERE id = ?"), syncID).Scan(&status, &errorMsg)
	mustNoErr(t, err, "query sync status")
	if status != "failed" {
		t.Errorf("sync status = %q, want %q", status, "failed")
	}
	if errorMsg != "network error" {
		t.Errorf("error_message = %q, want %q", errorMsg, "network error")
	}
}

func TestStore_MarkMessageDeletedByGmailID(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-123", "Test")
	mustNoErr(t, err, "EnsureConversation")

	_, err = st.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "gmail-msg-123",
		MessageType:     "email",
	})
	mustNoErr(t, err, "UpsertMessage")

	// Mark as deleted (trash)
	err = st.MarkMessageDeletedByGmailID(false, "gmail-msg-123")
	if err != nil {
		t.Fatalf("MarkMessageDeletedByGmailID(trash) error = %v", err)
	}

	// Mark as permanently deleted
	err = st.MarkMessageDeletedByGmailID(true, "gmail-msg-123")
	if err != nil {
		t.Fatalf("MarkMessageDeletedByGmailID(permanent) error = %v", err)
	}

	// Non-existent message should not error (no rows affected is OK)
	err = st.MarkMessageDeletedByGmailID(true, "nonexistent-id")
	if err != nil {
		t.Fatalf("MarkMessageDeletedByGmailID(nonexistent) error = %v", err)
	}
}

func TestStore_GetMessageRaw_NotFound(t *testing.T) {
	st := testutil.NewTestStore(t)

	// Try to get raw for non-existent message
	_, err := st.GetMessageRaw(99999)
	if err == nil {
		t.Error("GetMessageRaw() should error for non-existent message")
	}
}

func TestStore_UpsertMessage_AllFields(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-all", "Full Thread")
	mustNoErr(t, err, "EnsureConversation")

	now := time.Now()
	msg := &store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-all-fields",
		MessageType:     "email",
		Subject:         sql.NullString{String: "Full Subject", Valid: true},
		BodyText:        sql.NullString{String: "Body text content", Valid: true},
		BodyHTML:        sql.NullString{String: "<p>HTML content</p>", Valid: true},
		Snippet:         sql.NullString{String: "Preview snippet", Valid: true},
		SizeEstimate:    2048,
		SentAt:          sql.NullTime{Time: now, Valid: true},
		ReceivedAt:      sql.NullTime{Time: now.Add(time.Second), Valid: true},
		InternalDate:    sql.NullTime{Time: now, Valid: true},
		HasAttachments:  true,
		AttachmentCount: 2,
		IsFromMe:        true,
	}

	msgID, err := st.UpsertMessage(msg)
	if err != nil {
		t.Fatalf("UpsertMessage() error = %v", err)
	}

	if msgID == 0 {
		t.Error("message ID should be non-zero")
	}
}

func TestStore_UpsertMessage_MinimalFields(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-min", "Min Thread")
	mustNoErr(t, err, "EnsureConversation")

	// Only required fields, no optional fields
	msg := &store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-minimal",
		MessageType:     "email",
	}

	msgID, err := st.UpsertMessage(msg)
	if err != nil {
		t.Fatalf("UpsertMessage() error = %v", err)
	}

	if msgID == 0 {
		t.Error("message ID should be non-zero")
	}
}

func TestStore_UpsertMessageRaw_Update(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-raw", "Raw Thread")
	mustNoErr(t, err, "EnsureConversation")

	msgID, err := st.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-raw-update",
		MessageType:     "email",
	})
	mustNoErr(t, err, "UpsertMessage")

	// Insert raw data
	rawData1 := []byte("Original raw content")
	err = st.UpsertMessageRaw(msgID, rawData1)
	if err != nil {
		t.Fatalf("UpsertMessageRaw() error = %v", err)
	}

	// Update with new raw data
	rawData2 := []byte("Updated raw content that is different")
	err = st.UpsertMessageRaw(msgID, rawData2)
	if err != nil {
		t.Fatalf("UpsertMessageRaw() update error = %v", err)
	}

	// Verify updated data
	retrieved, err := st.GetMessageRaw(msgID)
	mustNoErr(t, err, "GetMessageRaw")
	if string(retrieved) != string(rawData2) {
		t.Errorf("retrieved = %q, want %q", retrieved, rawData2)
	}
}

func TestStore_MessageExistsBatch_Empty(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")

	// Check with empty list
	result, err := st.MessageExistsBatch(source.ID, []string{})
	if err != nil {
		t.Fatalf("MessageExistsBatch(empty) error = %v", err)
	}
	if len(result) != 0 {
		t.Errorf("len(result) = %d, want 0", len(result))
	}
}

func TestStore_ReplaceMessageLabels_Empty(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-labels", "Labels Test")
	mustNoErr(t, err, "EnsureConversation")

	msgID, err := st.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-labels",
		MessageType:     "email",
	})
	mustNoErr(t, err, "UpsertMessage")

	lid1, err := st.EnsureLabel(source.ID, "INBOX", "Inbox", "system")
	mustNoErr(t, err, "EnsureLabel INBOX")
	lid2, err := st.EnsureLabel(source.ID, "STARRED", "Starred", "system")
	mustNoErr(t, err, "EnsureLabel STARRED")

	// Add labels
	err = st.ReplaceMessageLabels(msgID, []int64{lid1, lid2})
	mustNoErr(t, err, "ReplaceMessageLabels")

	// Verify labels were added
	var count int
	err = st.DB().QueryRow(st.Rebind("SELECT COUNT(*) FROM message_labels WHERE message_id = ?"), msgID).Scan(&count)
	mustNoErr(t, err, "count labels")
	if count != 2 {
		t.Errorf("label count = %d, want 2", count)
	}

	// Replace with empty list (remove all labels)
	err = st.ReplaceMessageLabels(msgID, []int64{})
	if err != nil {
		t.Fatalf("ReplaceMessageLabels(empty) error = %v", err)
	}

	// Verify all labels were removed
	err = st.DB().QueryRow(st.Rebind("SELECT COUNT(*) FROM message_labels WHERE message_id = ?"), msgID).Scan(&count)
	mustNoErr(t, err, "count labels after empty")
	if count != 0 {
		t.Errorf("label count after empty = %d, want 0", count)
	}
}

func TestStore_ReplaceMessageRecipients_Empty(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-recip", "Recipients Test")
	mustNoErr(t, err, "EnsureConversation")

	msgID, err := st.UpsertMessage(&store.Message{
		ConversationID:  convID,
		SourceID:        source.ID,
		SourceMessageID: "msg-recip",
		MessageType:     "email",
	})
	mustNoErr(t, err, "UpsertMessage")

	pid1, err := st.EnsureParticipant("alice@example.com", "Alice", "example.com")
	mustNoErr(t, err, "EnsureParticipant")

	// Add recipient
	err = st.ReplaceMessageRecipients(msgID, "to", []int64{pid1}, []string{"Alice"})
	mustNoErr(t, err, "ReplaceMessageRecipients")

	// Verify recipient was added
	var count int
	err = st.DB().QueryRow(st.Rebind("SELECT COUNT(*) FROM message_recipients WHERE message_id = ? AND recipient_type = 'to'"), msgID).Scan(&count)
	mustNoErr(t, err, "count recipients")
	if count != 1 {
		t.Errorf("recipient count = %d, want 1", count)
	}

	// Replace with empty list
	err = st.ReplaceMessageRecipients(msgID, "to", []int64{}, []string{})
	if err != nil {
		t.Fatalf("ReplaceMessageRecipients(empty) error = %v", err)
	}

	// Verify all recipients were removed
	err = st.DB().QueryRow(st.Rebind("SELECT COUNT(*) FROM message_recipients WHERE message_id = ? AND recipient_type = 'to'"), msgID).Scan(&count)
	mustNoErr(t, err, "count recipients after empty")
	if count != 0 {
		t.Errorf("recipient count after empty = %d, want 0", count)
	}
}

func TestStore_GetActiveSync_NoSync(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")

	// No sync started yet
	active, err := st.GetActiveSync(source.ID)
	if err != nil {
		t.Fatalf("GetActiveSync() error = %v", err)
	}
	if active != nil {
		t.Errorf("expected nil active sync, got %+v", active)
	}
}

func TestStore_GetLastSuccessfulSync_None(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")

	// No successful sync yet
	lastSync, err := st.GetLastSuccessfulSync(source.ID)
	if err != nil {
		t.Fatalf("GetLastSuccessfulSync() error = %v", err)
	}
	if lastSync != nil {
		t.Errorf("expected nil last sync, got %+v", lastSync)
	}
}

func TestStore_GetSourceByIdentifier_NotFound(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetSourceByIdentifier("nonexistent@example.com")
	if err != nil {
		t.Fatalf("GetSourceByIdentifier() error = %v", err)
	}
	if source != nil {
		t.Errorf("expected nil source, got %+v", source)
	}
}

func TestStore_GetStats_WithData(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-stats", "Stats Test")
	mustNoErr(t, err, "EnsureConversation")

	// Add multiple messages
	for i := 0; i < 5; i++ {
		_, err := st.UpsertMessage(&store.Message{
			ConversationID:  convID,
			SourceID:        source.ID,
			SourceMessageID: "msg-" + string(rune('a'+i)),
			MessageType:     "email",
			SizeEstimate:    int64(1000 * (i + 1)),
		})
		mustNoErr(t, err, "UpsertMessage")
	}

	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("GetStats() error = %v", err)
	}

	if stats.MessageCount != 5 {
		t.Errorf("MessageCount = %d, want 5", stats.MessageCount)
	}
	if stats.ThreadCount == 0 {
		t.Error("ThreadCount should be non-zero")
	}
}

func TestStore_CountMessagesForSource(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-count", "Count Test")
	mustNoErr(t, err, "EnsureConversation")

	// Initially zero
	count, err := st.CountMessagesForSource(source.ID)
	if err != nil {
		t.Fatalf("CountMessagesForSource() error = %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}

	// Add messages
	for i := 0; i < 3; i++ {
		_, err := st.UpsertMessage(&store.Message{
			ConversationID:  convID,
			SourceID:        source.ID,
			SourceMessageID: "count-msg-" + string(rune('a'+i)),
			MessageType:     "email",
		})
		mustNoErr(t, err, "UpsertMessage")
	}

	count, err = st.CountMessagesForSource(source.ID)
	if err != nil {
		t.Fatalf("CountMessagesForSource() error = %v", err)
	}
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}

	// Mark one as deleted - should not be counted
	err = st.MarkMessageDeleted(source.ID, "count-msg-a")
	mustNoErr(t, err, "MarkMessageDeleted")

	count, err = st.CountMessagesForSource(source.ID)
	if err != nil {
		t.Fatalf("CountMessagesForSource() after delete error = %v", err)
	}
	if count != 2 {
		t.Errorf("count after delete = %d, want 2", count)
	}
}

func TestStore_CountMessagesWithRaw(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-raw-count", "Raw Count Test")
	mustNoErr(t, err, "EnsureConversation")

	// Initially zero
	count, err := st.CountMessagesWithRaw(source.ID)
	if err != nil {
		t.Fatalf("CountMessagesWithRaw() error = %v", err)
	}
	if count != 0 {
		t.Errorf("count = %d, want 0", count)
	}

	// Add messages, some with raw data
	rawData := []byte("From: test@example.com\r\nSubject: Test\r\n\r\nBody")

	for i := 0; i < 4; i++ {
		msgID, err := st.UpsertMessage(&store.Message{
			ConversationID:  convID,
			SourceID:        source.ID,
			SourceMessageID: "raw-count-msg-" + string(rune('a'+i)),
			MessageType:     "email",
		})
		mustNoErr(t, err, "UpsertMessage")

		// Only store raw for first 2 messages
		if i < 2 {
			err = st.UpsertMessageRaw(msgID, rawData)
			mustNoErr(t, err, "UpsertMessageRaw")
		}
	}

	count, err = st.CountMessagesWithRaw(source.ID)
	if err != nil {
		t.Fatalf("CountMessagesWithRaw() error = %v", err)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
}

func TestStore_GetRandomMessageIDs(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-random", "Random Test")
	mustNoErr(t, err, "EnsureConversation")

	// Empty source
	ids, err := st.GetRandomMessageIDs(source.ID, 5)
	if err != nil {
		t.Fatalf("GetRandomMessageIDs(empty) error = %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("len(ids) = %d, want 0 for empty source", len(ids))
	}

	// Add 10 messages
	allIDs := make(map[int64]bool)
	for i := 0; i < 10; i++ {
		msgID, err := st.UpsertMessage(&store.Message{
			ConversationID:  convID,
			SourceID:        source.ID,
			SourceMessageID: "random-msg-" + string(rune('a'+i)),
			MessageType:     "email",
		})
		mustNoErr(t, err, "UpsertMessage")
		allIDs[msgID] = true
	}

	// Sample fewer than available
	ids, err = st.GetRandomMessageIDs(source.ID, 5)
	if err != nil {
		t.Fatalf("GetRandomMessageIDs() error = %v", err)
	}
	if len(ids) != 5 {
		t.Errorf("len(ids) = %d, want 5", len(ids))
	}

	// All returned IDs should be valid
	for _, id := range ids {
		if !allIDs[id] {
			t.Errorf("returned ID %d is not in allIDs", id)
		}
	}

	// All returned IDs should be unique
	seen := make(map[int64]bool)
	for _, id := range ids {
		if seen[id] {
			t.Errorf("duplicate ID %d returned", id)
		}
		seen[id] = true
	}

	// Sample more than available - should return all
	ids, err = st.GetRandomMessageIDs(source.ID, 20)
	if err != nil {
		t.Fatalf("GetRandomMessageIDs(more than available) error = %v", err)
	}
	if len(ids) != 10 {
		t.Errorf("len(ids) = %d, want 10", len(ids))
	}
}

func TestStore_GetRandomMessageIDs_ExcludesDeleted(t *testing.T) {
	st := testutil.NewTestStore(t)

	source, err := st.GetOrCreateSource("gmail", "test@example.com")
	mustNoErr(t, err, "GetOrCreateSource")
	convID, err := st.EnsureConversation(source.ID, "thread-random-del", "Random Deleted Test")
	mustNoErr(t, err, "EnsureConversation")

	// Add 5 messages
	for i := 0; i < 5; i++ {
		_, err := st.UpsertMessage(&store.Message{
			ConversationID:  convID,
			SourceID:        source.ID,
			SourceMessageID: "random-del-msg-" + string(rune('a'+i)),
			MessageType:     "email",
		})
		mustNoErr(t, err, "UpsertMessage")
	}

	// Delete 2 messages
	err = st.MarkMessageDeleted(source.ID, "random-del-msg-a")
	mustNoErr(t, err, "MarkMessageDeleted a")
	err = st.MarkMessageDeleted(source.ID, "random-del-msg-c")
	mustNoErr(t, err, "MarkMessageDeleted c")

	// Should only return 3 (non-deleted) messages
	ids, err := st.GetRandomMessageIDs(source.ID, 10)
	if err != nil {
		t.Fatalf("GetRandomMessageIDs() error = %v", err)
	}
	if len(ids) != 3 {
		t.Errorf("len(ids) = %d, want 3 (5 total - 2 deleted)", len(ids))
	}
}
