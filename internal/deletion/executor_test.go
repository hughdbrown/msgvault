package deletion

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/testutil"
)

// mockAPIWithErrors extends gmail.MockAPI with error injection for trash/delete
type mockAPIWithErrors struct {
	*gmail.MockAPI
	mu           sync.Mutex
	trashErrors  map[string]error
	deleteErrors map[string]error
	batchError   error
}

func newMockAPI() *mockAPIWithErrors {
	return &mockAPIWithErrors{
		MockAPI:      gmail.NewMockAPI(),
		trashErrors:  make(map[string]error),
		deleteErrors: make(map[string]error),
	}
}

func (m *mockAPIWithErrors) TrashMessage(ctx context.Context, messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Record the call
	m.TrashCalls = append(m.TrashCalls, messageID)

	if err, ok := m.trashErrors[messageID]; ok {
		return err
	}
	return nil
}

func (m *mockAPIWithErrors) DeleteMessage(ctx context.Context, messageID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Record the call
	m.DeleteCalls = append(m.DeleteCalls, messageID)

	if err, ok := m.deleteErrors[messageID]; ok {
		return err
	}
	return nil
}

func (m *mockAPIWithErrors) BatchDeleteMessages(ctx context.Context, messageIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.BatchDeleteCalls = append(m.BatchDeleteCalls, messageIDs)

	if m.batchError != nil {
		return m.batchError
	}
	return nil
}

// trackingProgress records progress events for testing
type trackingProgress struct {
	mu          sync.Mutex
	startTotal  int
	progressLog []struct{ processed, succeeded, failed int }
	completed   bool
	finalSucc   int
	finalFail   int
}

func (p *trackingProgress) OnStart(total int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.startTotal = total
}

func (p *trackingProgress) OnProgress(processed, succeeded, failed int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.progressLog = append(p.progressLog, struct{ processed, succeeded, failed int }{processed, succeeded, failed})
}

func (p *trackingProgress) OnComplete(succeeded, failed int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.completed = true
	p.finalSucc = succeeded
	p.finalFail = failed
}

func TestNullProgress(t *testing.T) {
	// NullProgress should not panic
	p := NullProgress{}
	p.OnStart(10)
	p.OnProgress(5, 4, 1)
	p.OnComplete(9, 1)
}

func TestDefaultExecuteOptions(t *testing.T) {
	opts := DefaultExecuteOptions()

	if opts.Method != MethodTrash {
		t.Errorf("Method = %q, want %q", opts.Method, MethodTrash)
	}
	if opts.BatchSize != 100 {
		t.Errorf("BatchSize = %d, want 100", opts.BatchSize)
	}
	if !opts.Resume {
		t.Error("Resume = false, want true")
	}
}

func TestNewExecutor(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()

	exec := NewExecutor(mgr, store, mockAPI)
	if exec == nil {
		t.Fatal("NewExecutor returned nil")
	}
}

func TestExecutor_WithLogger(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	exec := NewExecutor(mgr, store, newMockAPI()).WithLogger(logger)

	if exec.logger != logger {
		t.Error("WithLogger did not set logger")
	}
}

func TestExecutor_WithProgress(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)

	progress := &trackingProgress{}
	exec := NewExecutor(mgr, store, newMockAPI()).WithProgress(progress)

	if exec.progress != progress {
		t.Error("WithProgress did not set progress")
	}
}

func TestExecutor_Execute_Success(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()
	progress := &trackingProgress{}

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	// Create a manifest with some messages
	gmailIDs := []string{"msg1", "msg2", "msg3"}
	manifest, err := mgr.CreateManifest("test deletion", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	// Execute
	ctx := context.Background()
	if err := exec.Execute(ctx, manifest.ID, nil); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// Verify all messages were trashed
	if len(mockAPI.TrashCalls) != 3 {
		t.Errorf("TrashCalls = %d, want 3", len(mockAPI.TrashCalls))
	}

	// Verify progress tracking
	if !progress.completed {
		t.Error("OnComplete was not called")
	}
	if progress.finalSucc != 3 {
		t.Errorf("finalSucc = %d, want 3", progress.finalSucc)
	}
	if progress.finalFail != 0 {
		t.Errorf("finalFail = %d, want 0", progress.finalFail)
	}

	// Verify manifest moved to completed
	completed, err := mgr.ListCompleted()
	if err != nil {
		t.Fatalf("ListCompleted() error = %v", err)
	}
	if len(completed) != 1 {
		t.Errorf("ListCompleted() = %d, want 1", len(completed))
	}
}

func TestExecutor_Execute_WithDeleteMethod(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()

	exec := NewExecutor(mgr, store, mockAPI)

	gmailIDs := []string{"msg1", "msg2"}
	manifest, err := mgr.CreateManifest("permanent delete", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	opts := &ExecuteOptions{
		Method:    MethodDelete,
		BatchSize: 100,
		Resume:    true,
	}

	ctx := context.Background()
	if err := exec.Execute(ctx, manifest.ID, opts); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// Verify DeleteMessage was called instead of TrashMessage
	if len(mockAPI.DeleteCalls) != 2 {
		t.Errorf("DeleteCalls = %d, want 2", len(mockAPI.DeleteCalls))
	}
	if len(mockAPI.TrashCalls) != 0 {
		t.Errorf("TrashCalls = %d, want 0", len(mockAPI.TrashCalls))
	}
}

func TestExecutor_Execute_WithFailures(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()
	progress := &trackingProgress{}

	// Inject error for msg2
	mockAPI.trashErrors["msg2"] = errors.New("trash failed")

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	gmailIDs := []string{"msg1", "msg2", "msg3"}
	manifest, err := mgr.CreateManifest("partial failure", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.Execute(ctx, manifest.ID, nil); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// 2 succeeded, 1 failed
	if progress.finalSucc != 2 {
		t.Errorf("finalSucc = %d, want 2", progress.finalSucc)
	}
	if progress.finalFail != 1 {
		t.Errorf("finalFail = %d, want 1", progress.finalFail)
	}

	// Partial success still goes to completed
	completed, err := mgr.ListCompleted()
	if err != nil {
		t.Fatalf("ListCompleted() error = %v", err)
	}
	if len(completed) != 1 {
		t.Errorf("ListCompleted() = %d, want 1", len(completed))
	}

	// Check failed IDs are recorded
	loaded, _, err := mgr.GetManifest(manifest.ID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if len(loaded.Execution.FailedIDs) != 1 {
		t.Errorf("FailedIDs = %v, want 1 entry", loaded.Execution.FailedIDs)
	}
	if loaded.Execution.FailedIDs[0] != "msg2" {
		t.Errorf("FailedIDs[0] = %q, want %q", loaded.Execution.FailedIDs[0], "msg2")
	}
}

func TestExecutor_Execute_AllFail(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()

	// All messages fail
	mockAPI.trashErrors["msg1"] = errors.New("fail 1")
	mockAPI.trashErrors["msg2"] = errors.New("fail 2")

	exec := NewExecutor(mgr, store, mockAPI)

	gmailIDs := []string{"msg1", "msg2"}
	manifest, err := mgr.CreateManifest("total failure", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.Execute(ctx, manifest.ID, nil); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// All failed -> moved to failed status
	failed, err := mgr.ListFailed()
	if err != nil {
		t.Fatalf("ListFailed() error = %v", err)
	}
	if len(failed) != 1 {
		t.Errorf("ListFailed() = %d, want 1", len(failed))
	}
}

func TestExecutor_Execute_ContextCancelled(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()

	exec := NewExecutor(mgr, store, mockAPI)

	// Create manifest with many messages
	gmailIDs := make([]string, 100)
	for i := range gmailIDs {
		gmailIDs[i] = fmt.Sprintf("msg%d", i)
	}
	manifest, err := mgr.CreateManifest("interrupt test", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	// Cancel context immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = exec.Execute(ctx, manifest.ID, nil)
	if err != context.Canceled {
		t.Errorf("Execute() error = %v, want context.Canceled", err)
	}

	// Manifest should remain in in_progress (for resume)
	inProgress, err := mgr.ListInProgress()
	if err != nil {
		t.Fatalf("ListInProgress() error = %v", err)
	}
	if len(inProgress) != 1 {
		t.Errorf("ListInProgress() = %d, want 1", len(inProgress))
	}
}

func TestExecutor_Execute_SmallBatchSize(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()

	exec := NewExecutor(mgr, store, mockAPI)

	gmailIDs := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
	manifest, err := mgr.CreateManifest("small batch test", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	// Use very small batch size
	opts := &ExecuteOptions{
		Method:    MethodTrash,
		BatchSize: 2,
		Resume:    true,
	}

	ctx := context.Background()
	if err := exec.Execute(ctx, manifest.ID, opts); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// Verify all were processed
	if len(mockAPI.TrashCalls) != 5 {
		t.Errorf("TrashCalls = %d, want 5", len(mockAPI.TrashCalls))
	}
}

func TestExecutor_Execute_ManifestNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)

	exec := NewExecutor(mgr, store, newMockAPI())

	ctx := context.Background()
	err = exec.Execute(ctx, "nonexistent-id", nil)
	if err == nil {
		t.Error("Execute() should error for nonexistent manifest")
	}
}

func TestExecutor_Execute_InvalidStatus(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)

	exec := NewExecutor(mgr, store, newMockAPI())

	// Create, execute to completion
	manifest, err := mgr.CreateManifest("completed test", []string{"msg1"}, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.Execute(ctx, manifest.ID, nil); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// Try to execute again
	err = exec.Execute(ctx, manifest.ID, nil)
	if err == nil {
		t.Error("Execute() should error for completed manifest")
	}
}

func TestExecutor_Execute_ResumeFromInProgress(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()

	exec := NewExecutor(mgr, store, mockAPI)

	// Create a manifest that's already in_progress with some progress
	gmailIDs := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
	manifest := NewManifest("in-progress resume", gmailIDs)
	manifest.Status = StatusInProgress
	manifest.Execution = &Execution{
		StartedAt:          time.Now().Add(-time.Hour),
		Method:             MethodTrash,
		Succeeded:          2,
		Failed:             0,
		LastProcessedIndex: 2, // Already processed msg1 and msg2
	}
	if err := mgr.SaveManifest(manifest); err != nil {
		t.Fatalf("SaveManifest() error = %v", err)
	}

	ctx := context.Background()
	opts := &ExecuteOptions{
		Method:    MethodTrash,
		BatchSize: 100,
		Resume:    true,
	}

	if err := exec.Execute(ctx, manifest.ID, opts); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// Should only process msg3, msg4, msg5 (skipping msg1, msg2)
	if len(mockAPI.TrashCalls) != 3 {
		t.Errorf("TrashCalls = %d, want 3 (resume from index 2)", len(mockAPI.TrashCalls))
	}

	// Verify final counts include all 5
	loaded, _, err := mgr.GetManifest(manifest.ID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if loaded.Execution.Succeeded != 5 {
		t.Errorf("Succeeded = %d, want 5", loaded.Execution.Succeeded)
	}
}

func TestExecutor_ExecuteBatch_Success(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()
	progress := &trackingProgress{}

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	// Create manifest with messages
	gmailIDs := []string{"msg1", "msg2", "msg3"}
	manifest, err := mgr.CreateManifest("batch delete", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.ExecuteBatch(ctx, manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// Verify batch delete was called
	if len(mockAPI.BatchDeleteCalls) != 1 {
		t.Errorf("BatchDeleteCalls = %d, want 1", len(mockAPI.BatchDeleteCalls))
	}
	if len(mockAPI.BatchDeleteCalls[0]) != 3 {
		t.Errorf("BatchDeleteCalls[0] length = %d, want 3", len(mockAPI.BatchDeleteCalls[0]))
	}

	// Verify progress
	if !progress.completed {
		t.Error("OnComplete was not called")
	}
	if progress.finalSucc != 3 {
		t.Errorf("finalSucc = %d, want 3", progress.finalSucc)
	}

	// Verify completed
	completed, err := mgr.ListCompleted()
	if err != nil {
		t.Fatalf("ListCompleted() error = %v", err)
	}
	if len(completed) != 1 {
		t.Errorf("ListCompleted() = %d, want 1", len(completed))
	}
}

func TestExecutor_ExecuteBatch_LargeBatch(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()

	exec := NewExecutor(mgr, store, mockAPI)

	// Create manifest with >1000 messages (Gmail batch limit)
	gmailIDs := make([]string, 1500)
	for i := range gmailIDs {
		gmailIDs[i] = fmt.Sprintf("msg%d", i)
	}
	manifest, err := mgr.CreateManifest("large batch", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.ExecuteBatch(ctx, manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// Should be split into 2 batches (1000 + 500)
	if len(mockAPI.BatchDeleteCalls) != 2 {
		t.Errorf("BatchDeleteCalls = %d, want 2", len(mockAPI.BatchDeleteCalls))
	}
	if len(mockAPI.BatchDeleteCalls[0]) != 1000 {
		t.Errorf("BatchDeleteCalls[0] length = %d, want 1000", len(mockAPI.BatchDeleteCalls[0]))
	}
	if len(mockAPI.BatchDeleteCalls[1]) != 500 {
		t.Errorf("BatchDeleteCalls[1] length = %d, want 500", len(mockAPI.BatchDeleteCalls[1]))
	}
}

func TestExecutor_ExecuteBatch_WithBatchError(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()

	// Batch delete fails, should fall back to individual deletes
	mockAPI.batchError = errors.New("batch failed")

	exec := NewExecutor(mgr, store, mockAPI)

	gmailIDs := []string{"msg1", "msg2", "msg3"}
	manifest, err := mgr.CreateManifest("batch fallback", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.ExecuteBatch(ctx, manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// Should have attempted batch, then fallen back to individual
	if len(mockAPI.BatchDeleteCalls) != 1 {
		t.Errorf("BatchDeleteCalls = %d, want 1", len(mockAPI.BatchDeleteCalls))
	}
	if len(mockAPI.DeleteCalls) != 3 {
		t.Errorf("DeleteCalls = %d, want 3 (fallback)", len(mockAPI.DeleteCalls))
	}
}

func TestExecutor_ExecuteBatch_InvalidStatus(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)

	exec := NewExecutor(mgr, store, newMockAPI())

	// Create and move to in_progress
	manifest, err := mgr.CreateManifest("wrong status", []string{"msg1"}, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}
	if err := mgr.MoveManifest(manifest.ID, StatusPending, StatusInProgress); err != nil {
		t.Fatalf("MoveManifest() error = %v", err)
	}

	ctx := context.Background()
	err = exec.ExecuteBatch(ctx, manifest.ID)
	if err == nil {
		t.Error("ExecuteBatch() should error for non-pending manifest")
	}
}

func TestExecutor_ExecuteBatch_ContextCancelled(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()

	exec := NewExecutor(mgr, store, mockAPI)

	// Create manifest with many messages
	gmailIDs := make([]string, 2500)
	for i := range gmailIDs {
		gmailIDs[i] = fmt.Sprintf("msg%d", i)
	}
	manifest, err := mgr.CreateManifest("cancel batch", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	// Cancel context immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = exec.ExecuteBatch(ctx, manifest.ID)
	if err != context.Canceled {
		t.Errorf("ExecuteBatch() error = %v, want context.Canceled", err)
	}
}

func TestExecutor_ExecuteBatch_ManifestNotFound(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)

	exec := NewExecutor(mgr, store, newMockAPI())

	ctx := context.Background()
	err = exec.ExecuteBatch(ctx, "nonexistent-id")
	if err == nil {
		t.Error("ExecuteBatch() should error for nonexistent manifest")
	}
}

// TestExecutor_Execute_NotFoundTreatedAsSuccess verifies that 404 (already deleted)
// is treated as success, making deletion idempotent.
func TestExecutor_Execute_NotFoundTreatedAsSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()
	progress := &trackingProgress{}

	// msg2 returns 404 (already deleted on server)
	mockAPI.trashErrors["msg2"] = &gmail.NotFoundError{Path: "/users/me/messages/msg2/trash"}

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	gmailIDs := []string{"msg1", "msg2", "msg3"}
	manifest, err := mgr.CreateManifest("idempotent test", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.Execute(ctx, manifest.ID, nil); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// All 3 should count as success (msg2's 404 is treated as success)
	if progress.finalSucc != 3 {
		t.Errorf("finalSucc = %d, want 3 (404 should count as success)", progress.finalSucc)
	}
	if progress.finalFail != 0 {
		t.Errorf("finalFail = %d, want 0", progress.finalFail)
	}

	// Verify manifest moved to completed (not failed)
	completed, err := mgr.ListCompleted()
	if err != nil {
		t.Fatalf("ListCompleted() error = %v", err)
	}
	if len(completed) != 1 {
		t.Errorf("ListCompleted() = %d, want 1", len(completed))
	}

	// Verify no failed IDs recorded
	loaded, _, err := mgr.GetManifest(manifest.ID)
	if err != nil {
		t.Fatalf("GetManifest() error = %v", err)
	}
	if len(loaded.Execution.FailedIDs) != 0 {
		t.Errorf("FailedIDs = %v, want empty", loaded.Execution.FailedIDs)
	}
}

// TestExecutor_ExecuteBatch_FallbackNotFoundTreatedAsSuccess verifies that
// when batch delete fails and falls back to individual deletes,
// 404 errors are still treated as success.
func TestExecutor_ExecuteBatch_FallbackNotFoundTreatedAsSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()
	progress := &trackingProgress{}

	// Batch delete fails, triggering fallback to individual deletes
	mockAPI.batchError = errors.New("batch failed")
	// msg2 returns 404 in fallback (already deleted on server)
	mockAPI.deleteErrors["msg2"] = &gmail.NotFoundError{Path: "/users/me/messages/msg2"}

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	gmailIDs := []string{"msg1", "msg2", "msg3"}
	manifest, err := mgr.CreateManifest("batch fallback 404", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.ExecuteBatch(ctx, manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// All 3 should succeed (msg2's 404 in fallback is treated as success)
	if progress.finalSucc != 3 {
		t.Errorf("finalSucc = %d, want 3 (404 should count as success)", progress.finalSucc)
	}
	if progress.finalFail != 0 {
		t.Errorf("finalFail = %d, want 0", progress.finalFail)
	}
}

// TestExecutor_ExecuteBatch_FallbackWithNon404Failures verifies that
// non-404 failures during fallback are properly counted as failures.
func TestExecutor_ExecuteBatch_FallbackWithNon404Failures(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()
	progress := &trackingProgress{}

	// Batch delete fails, triggering fallback
	mockAPI.batchError = errors.New("batch failed")
	// msg2 returns a non-404 error (permission denied, etc.)
	mockAPI.deleteErrors["msg2"] = errors.New("permission denied")

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	gmailIDs := []string{"msg1", "msg2", "msg3"}
	manifest, err := mgr.CreateManifest("batch fallback failures", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.ExecuteBatch(ctx, manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// 2 succeeded, 1 failed
	if progress.finalSucc != 2 {
		t.Errorf("finalSucc = %d, want 2", progress.finalSucc)
	}
	if progress.finalFail != 1 {
		t.Errorf("finalFail = %d, want 1", progress.finalFail)
	}
}

// TestExecutor_Execute_WithDeleteMethod_404 tests 404 handling with permanent delete method.
func TestExecutor_Execute_WithDeleteMethod_404(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()
	progress := &trackingProgress{}

	// msg2 returns 404 (already permanently deleted)
	mockAPI.deleteErrors["msg2"] = &gmail.NotFoundError{Path: "/users/me/messages/msg2"}

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	gmailIDs := []string{"msg1", "msg2", "msg3"}
	manifest, err := mgr.CreateManifest("delete method 404", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	opts := &ExecuteOptions{
		Method:    MethodDelete,
		BatchSize: 100,
		Resume:    true,
	}

	ctx := context.Background()
	if err := exec.Execute(ctx, manifest.ID, opts); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// All 3 should succeed (msg2's 404 is treated as success)
	if progress.finalSucc != 3 {
		t.Errorf("finalSucc = %d, want 3 (404 should count as success)", progress.finalSucc)
	}
	if progress.finalFail != 0 {
		t.Errorf("finalFail = %d, want 0", progress.finalFail)
	}
}

// Tests using DeletionMockAPI for more sophisticated scenarios

// TestExecutor_Execute_UsingDeletionMockAPI tests with the comprehensive mock.
func TestExecutor_Execute_UsingDeletionMockAPI(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := gmail.NewDeletionMockAPI()
	progress := &trackingProgress{}

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	gmailIDs := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
	manifest, err := mgr.CreateManifest("deletion mock test", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.Execute(ctx, manifest.ID, nil); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// All should succeed
	if progress.finalSucc != 5 {
		t.Errorf("finalSucc = %d, want 5", progress.finalSucc)
	}

	// Verify call sequence
	if len(mockAPI.TrashCalls) != 5 {
		t.Errorf("TrashCalls = %d, want 5", len(mockAPI.TrashCalls))
	}
}

// TestExecutor_Execute_DeletionMock_MixedErrors tests mixed success/404/error.
func TestExecutor_Execute_DeletionMock_MixedErrors(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := gmail.NewDeletionMockAPI()
	progress := &trackingProgress{}

	// msg2: 404 (should count as success)
	mockAPI.SetNotFoundError("msg2")
	// msg4: permanent error
	mockAPI.TrashErrors["msg4"] = errors.New("server error")

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	gmailIDs := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
	manifest, err := mgr.CreateManifest("mixed errors test", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.Execute(ctx, manifest.ID, nil); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// 4 succeeded (including 404), 1 failed
	if progress.finalSucc != 4 {
		t.Errorf("finalSucc = %d, want 4", progress.finalSucc)
	}
	if progress.finalFail != 1 {
		t.Errorf("finalFail = %d, want 1", progress.finalFail)
	}
}

// TestExecutor_ExecuteBatch_DeletionMock tests batch with DeletionMockAPI.
func TestExecutor_ExecuteBatch_DeletionMock(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := gmail.NewDeletionMockAPI()
	progress := &trackingProgress{}

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	gmailIDs := []string{"msg1", "msg2", "msg3"}
	manifest, err := mgr.CreateManifest("batch mock test", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.ExecuteBatch(ctx, manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// All succeeded
	if progress.finalSucc != 3 {
		t.Errorf("finalSucc = %d, want 3", progress.finalSucc)
	}

	// Verify batch was called once with all 3 IDs
	if len(mockAPI.BatchDeleteCalls) != 1 {
		t.Errorf("BatchDeleteCalls = %d, want 1", len(mockAPI.BatchDeleteCalls))
	}
}

// TestExecutor_ExecuteBatch_DeletionMock_FallbackMixed tests batch fallback with mixed results.
func TestExecutor_ExecuteBatch_DeletionMock_FallbackMixed(t *testing.T) {
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}
	store := testutil.NewTestStore(t)
	mockAPI := gmail.NewDeletionMockAPI()
	progress := &trackingProgress{}

	// Batch fails, fallback to individual
	mockAPI.BatchDeleteError = errors.New("batch not supported")
	// msg2: 404 (should count as success)
	mockAPI.SetNotFoundError("msg2")
	// msg3: permanent error
	mockAPI.DeleteErrors["msg3"] = errors.New("permission denied")

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	gmailIDs := []string{"msg1", "msg2", "msg3", "msg4"}
	manifest, err := mgr.CreateManifest("batch fallback mixed", gmailIDs, Filters{})
	if err != nil {
		t.Fatalf("CreateManifest() error = %v", err)
	}

	ctx := context.Background()
	if err := exec.ExecuteBatch(ctx, manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// 3 succeeded (msg1, msg2 as 404, msg4), 1 failed (msg3)
	if progress.finalSucc != 3 {
		t.Errorf("finalSucc = %d, want 3", progress.finalSucc)
	}
	if progress.finalFail != 1 {
		t.Errorf("finalFail = %d, want 1", progress.finalFail)
	}

	// Verify batch was attempted, then individual deletes
	if len(mockAPI.BatchDeleteCalls) != 1 {
		t.Errorf("BatchDeleteCalls = %d, want 1", len(mockAPI.BatchDeleteCalls))
	}
	if len(mockAPI.DeleteCalls) != 4 {
		t.Errorf("DeleteCalls = %d, want 4 (fallback)", len(mockAPI.DeleteCalls))
	}
}

// TestDeletionMockAPI_CallSequence verifies call sequence tracking.
func TestDeletionMockAPI_CallSequence(t *testing.T) {
	mockAPI := gmail.NewDeletionMockAPI()

	ctx := context.Background()
	_ = mockAPI.TrashMessage(ctx, "msg1")
	_ = mockAPI.DeleteMessage(ctx, "msg2")
	_ = mockAPI.BatchDeleteMessages(ctx, []string{"msg3", "msg4"})

	if len(mockAPI.CallSequence) != 3 {
		t.Fatalf("CallSequence length = %d, want 3", len(mockAPI.CallSequence))
	}

	if mockAPI.CallSequence[0].Operation != "trash" {
		t.Errorf("CallSequence[0].Operation = %q, want %q", mockAPI.CallSequence[0].Operation, "trash")
	}
	if mockAPI.CallSequence[1].Operation != "delete" {
		t.Errorf("CallSequence[1].Operation = %q, want %q", mockAPI.CallSequence[1].Operation, "delete")
	}
	if mockAPI.CallSequence[2].Operation != "batch_delete" {
		t.Errorf("CallSequence[2].Operation = %q, want %q", mockAPI.CallSequence[2].Operation, "batch_delete")
	}
}

// TestDeletionMockAPI_Reset verifies state reset.
func TestDeletionMockAPI_Reset(t *testing.T) {
	mockAPI := gmail.NewDeletionMockAPI()
	mockAPI.TrashErrors["msg1"] = errors.New("error")
	mockAPI.TrashCalls = []string{"msg1"}

	mockAPI.Reset()

	if len(mockAPI.TrashErrors) != 0 {
		t.Errorf("TrashErrors not cleared")
	}
	if len(mockAPI.TrashCalls) != 0 {
		t.Errorf("TrashCalls not cleared")
	}
}

// TestDeletionMockAPI_GetCallCount verifies call count helpers.
func TestDeletionMockAPI_GetCallCount(t *testing.T) {
	mockAPI := gmail.NewDeletionMockAPI()
	ctx := context.Background()

	_ = mockAPI.TrashMessage(ctx, "msg1")
	_ = mockAPI.TrashMessage(ctx, "msg1")
	_ = mockAPI.TrashMessage(ctx, "msg2")

	if mockAPI.GetTrashCallCount("msg1") != 2 {
		t.Errorf("GetTrashCallCount(msg1) = %d, want 2", mockAPI.GetTrashCallCount("msg1"))
	}
	if mockAPI.GetTrashCallCount("msg2") != 1 {
		t.Errorf("GetTrashCallCount(msg2) = %d, want 1", mockAPI.GetTrashCallCount("msg2"))
	}
	if mockAPI.GetTrashCallCount("msg3") != 0 {
		t.Errorf("GetTrashCallCount(msg3) = %d, want 0", mockAPI.GetTrashCallCount("msg3"))
	}
}

// TestDeletionMockAPI_Close verifies Close is a no-op.
func TestDeletionMockAPI_Close(t *testing.T) {
	mockAPI := gmail.NewDeletionMockAPI()
	if err := mockAPI.Close(); err != nil {
		t.Errorf("Close() error = %v, want nil", err)
	}
}

// TestDeletionMockAPI_SetTransientFailure tests transient failure simulation.
func TestDeletionMockAPI_SetTransientFailure(t *testing.T) {
	mockAPI := gmail.NewDeletionMockAPI()
	ctx := context.Background()

	// Set msg1 to fail 2 times then succeed
	mockAPI.SetTransientFailure("msg1", 2, true)

	// First call: fails
	err := mockAPI.TrashMessage(ctx, "msg1")
	if err == nil {
		t.Error("First call should fail")
	}

	// Second call: fails
	err = mockAPI.TrashMessage(ctx, "msg1")
	if err == nil {
		t.Error("Second call should fail")
	}

	// Third call: succeeds
	err = mockAPI.TrashMessage(ctx, "msg1")
	if err != nil {
		t.Errorf("Third call should succeed, got error: %v", err)
	}
}

// TestDeletionMockAPI_Hooks tests before hooks.
func TestDeletionMockAPI_Hooks(t *testing.T) {
	mockAPI := gmail.NewDeletionMockAPI()
	ctx := context.Background()

	hookCalled := false
	mockAPI.BeforeTrash = func(messageID string) error {
		hookCalled = true
		if messageID == "blocked" {
			return errors.New("blocked by hook")
		}
		return nil
	}

	// Normal call - hook allows it
	err := mockAPI.TrashMessage(ctx, "msg1")
	if err != nil {
		t.Errorf("TrashMessage(msg1) error = %v", err)
	}
	if !hookCalled {
		t.Error("BeforeTrash hook was not called")
	}

	// Blocked call
	err = mockAPI.TrashMessage(ctx, "blocked")
	if err == nil {
		t.Error("TrashMessage(blocked) should error")
	}
}

// TestDeletionMockAPI_BeforeDeleteHook tests delete hook.
func TestDeletionMockAPI_BeforeDeleteHook(t *testing.T) {
	mockAPI := gmail.NewDeletionMockAPI()
	ctx := context.Background()

	mockAPI.BeforeDelete = func(messageID string) error {
		return errors.New("delete hook error")
	}

	err := mockAPI.DeleteMessage(ctx, "msg1")
	if err == nil {
		t.Error("DeleteMessage should fail with hook error")
	}
}

// TestDeletionMockAPI_BeforeBatchDeleteHook tests batch delete hook.
func TestDeletionMockAPI_BeforeBatchDeleteHook(t *testing.T) {
	mockAPI := gmail.NewDeletionMockAPI()
	ctx := context.Background()

	mockAPI.BeforeBatchDelete = func(ids []string) error {
		return errors.New("batch hook error")
	}

	err := mockAPI.BatchDeleteMessages(ctx, []string{"msg1", "msg2"})
	if err == nil {
		t.Error("BatchDeleteMessages should fail with hook error")
	}
}

// TestDeletionMockAPI_GetDeleteCallCount tests delete call counting.
func TestDeletionMockAPI_GetDeleteCallCount(t *testing.T) {
	mockAPI := gmail.NewDeletionMockAPI()
	ctx := context.Background()

	_ = mockAPI.DeleteMessage(ctx, "msg1")
	_ = mockAPI.DeleteMessage(ctx, "msg1")

	if mockAPI.GetDeleteCallCount("msg1") != 2 {
		t.Errorf("GetDeleteCallCount(msg1) = %d, want 2", mockAPI.GetDeleteCallCount("msg1"))
	}
}

// TestDeletionMockAPI_SetTransientDeleteFailure tests transient delete failures.
func TestDeletionMockAPI_SetTransientDeleteFailure(t *testing.T) {
	mockAPI := gmail.NewDeletionMockAPI()
	ctx := context.Background()

	// Set msg1 to fail 1 time then succeed (for delete, not trash)
	mockAPI.SetTransientFailure("msg1", 1, false)

	// First call: fails
	err := mockAPI.DeleteMessage(ctx, "msg1")
	if err == nil {
		t.Error("First delete call should fail")
	}

	// Second call: succeeds
	err = mockAPI.DeleteMessage(ctx, "msg1")
	if err != nil {
		t.Errorf("Second delete call should succeed, got error: %v", err)
	}
}

// TestNullProgress_AllMethods exercises all NullProgress methods for coverage.
func TestNullProgress_AllMethods(t *testing.T) {
	p := NullProgress{}
	// These are no-ops but we need to call them for coverage
	p.OnStart(100)
	p.OnProgress(50, 40, 10)
	p.OnComplete(90, 10)
	// If we get here without panic, the test passes
}
