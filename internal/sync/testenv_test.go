package sync

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/store"
)

const testEmail = "test@example.com"

type TestEnv struct {
	Store   *store.Store
	Mock    *gmail.MockAPI
	Syncer  *Syncer
	TmpDir  string
	Context context.Context
}

func NewTestEnv(t *testing.T) (*TestEnv, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "msgvault-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "test.db")
	st, err := store.Open(dbPath)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("open store: %v", err)
	}

	if err := st.InitSchema(); err != nil {
		st.Close()
		os.RemoveAll(tmpDir)
		t.Fatalf("init schema: %v", err)
	}

	mock := gmail.NewMockAPI()
	// Default profile setup
	mock.Profile = &gmail.Profile{
		EmailAddress:  "test@example.com",
		MessagesTotal: 0,
		HistoryID:     1000,
	}

	env := &TestEnv{
		Store:   st,
		Mock:    mock,
		TmpDir:  tmpDir,
		Context: context.Background(),
	}

	// Helper to create syncer easily
	env.Syncer = New(mock, st, nil)

	cleanup := func() {
		st.Close()
		os.RemoveAll(tmpDir)
	}
	return env, cleanup
}

// SetupSource creates a source and sets its sync cursor, returning the source.
func (e *TestEnv) SetupSource(t *testing.T, historyID string) *store.Source {
	t.Helper()
	source, err := e.Store.GetOrCreateSource("gmail", e.Mock.Profile.EmailAddress)
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}
	if err := e.Store.UpdateSourceSyncCursor(source.ID, historyID); err != nil {
		t.Fatalf("UpdateSourceSyncCursor: %v", err)
	}
	return source
}

// MustCreateSource creates a source without setting a sync cursor.
func (e *TestEnv) MustCreateSource(t *testing.T) *store.Source {
	t.Helper()
	source, err := e.Store.GetOrCreateSource("gmail", e.Mock.Profile.EmailAddress)
	if err != nil {
		t.Fatalf("GetOrCreateSource: %v", err)
	}
	return source
}

// newTestEnv creates a TestEnv and registers cleanup via t.Cleanup.
func newTestEnv(t *testing.T) *TestEnv {
	t.Helper()
	env, cleanup := NewTestEnv(t)
	t.Cleanup(cleanup)
	return env
}

// seedMessages sets the profile totals/historyID and adds messages to the mock.
func seedMessages(env *TestEnv, total int64, historyID uint64, msgs ...string) {
	env.Mock.Profile.MessagesTotal = total
	env.Mock.Profile.HistoryID = historyID
	for _, id := range msgs {
		env.Mock.AddMessage(id, testMIME, []string{"INBOX"})
	}
}

// runFullSync runs a full sync and fails the test on error.
func runFullSync(t *testing.T, env *TestEnv) *gmail.SyncSummary {
	t.Helper()
	summary, err := env.Syncer.Full(env.Context, testEmail)
	if err != nil {
		t.Fatalf("full sync: %v", err)
	}
	return summary
}

// runIncrementalSync runs an incremental sync and fails the test on error.
func runIncrementalSync(t *testing.T, env *TestEnv) *gmail.SyncSummary {
	t.Helper()
	summary, err := env.Syncer.Incremental(env.Context, testEmail)
	if err != nil {
		t.Fatalf("incremental sync: %v", err)
	}
	return summary
}

// assertSummary checks common SyncSummary fields. Use -1 to skip a check.
func assertSummary(t *testing.T, s *gmail.SyncSummary, added, errors, skipped, found int64) {
	t.Helper()
	if added >= 0 && s.MessagesAdded != added {
		t.Errorf("expected %d messages added, got %d", added, s.MessagesAdded)
	}
	if errors >= 0 && s.Errors != errors {
		t.Errorf("expected %d errors, got %d", errors, s.Errors)
	}
	if skipped >= 0 && s.MessagesSkipped != skipped {
		t.Errorf("expected %d messages skipped, got %d", skipped, s.MessagesSkipped)
	}
	if found >= 0 && s.MessagesFound != found {
		t.Errorf("expected %d messages found, got %d", found, s.MessagesFound)
	}
}

// mustStats calls GetStats and fails on error.
func mustStats(t *testing.T, st *store.Store) *store.Stats {
	t.Helper()
	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	return stats
}

// assertMessageCount checks the message count in the store.
func assertMessageCount(t *testing.T, st *store.Store, want int64) {
	t.Helper()
	stats := mustStats(t, st)
	if stats.MessageCount != want {
		t.Errorf("expected %d messages in db, got %d", want, stats.MessageCount)
	}
}

// assertAttachmentCount checks the attachment count in the store.
func assertAttachmentCount(t *testing.T, st *store.Store, want int64) {
	t.Helper()
	stats := mustStats(t, st)
	if stats.AttachmentCount != want {
		t.Errorf("expected %d attachments in db, got %d", want, stats.AttachmentCount)
	}
}

// withAttachmentsDir creates a syncer with an attachments directory and returns the dir path.
func withAttachmentsDir(t *testing.T, env *TestEnv) string {
	t.Helper()
	attachDir := filepath.Join(env.TmpDir, "attachments")
	opts := &Options{AttachmentsDir: attachDir}
	env.Syncer = New(env.Mock, env.Store, opts)
	return attachDir
}

// countFiles counts regular files recursively under dir.
func countFiles(t *testing.T, dir string) int {
	t.Helper()
	var count int
	err := filepath.WalkDir(dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDir(%s): %v", dir, err)
	}
	return count
}
