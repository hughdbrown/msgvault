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

// ExecutorTestContext encapsulates common test dependencies for executor tests.
type ExecutorTestContext struct {
	Mgr      *Manager
	MockAPI  *mockAPIWithErrors
	Exec     *Executor
	Progress *trackingProgress
	Dir      string
	t        *testing.T
}

// NewExecutorTestContext creates a new test context with all dependencies initialized.
func NewExecutorTestContext(t *testing.T) *ExecutorTestContext {
	t.Helper()
	tmpDir := t.TempDir()
	mgr, err := NewManager(tmpDir)
	if err != nil {
		t.Fatalf("NewManager() error = %v", err)
	}

	store := testutil.NewTestStore(t)
	mockAPI := newMockAPI()
	progress := &trackingProgress{}

	exec := NewExecutor(mgr, store, mockAPI).WithProgress(progress)

	return &ExecutorTestContext{
		Mgr:      mgr,
		MockAPI:  mockAPI,
		Exec:     exec,
		Progress: progress,
		Dir:      tmpDir,
		t:        t,
	}
}

// CreateManifest creates a manifest with the given name and Gmail IDs.
func (c *ExecutorTestContext) CreateManifest(name string, ids []string) *Manifest {
	c.t.Helper()
	manifest, err := c.Mgr.CreateManifest(name, ids, Filters{})
	if err != nil {
		c.t.Fatalf("CreateManifest(%q) error = %v", name, err)
	}
	return manifest
}

// AssertResult verifies the final success and failure counts.
func (c *ExecutorTestContext) AssertResult(wantSucc, wantFail int) {
	c.t.Helper()
	if c.Progress.finalSucc != wantSucc {
		c.t.Errorf("finalSucc = %d, want %d", c.Progress.finalSucc, wantSucc)
	}
	if c.Progress.finalFail != wantFail {
		c.t.Errorf("finalFail = %d, want %d", c.Progress.finalFail, wantFail)
	}
}

// AssertCompleted verifies that OnComplete was called.
func (c *ExecutorTestContext) AssertCompleted() {
	c.t.Helper()
	if !c.Progress.completed {
		c.t.Error("OnComplete was not called")
	}
}

// AssertTrashCalls verifies the number of TrashMessage calls.
func (c *ExecutorTestContext) AssertTrashCalls(want int) {
	c.t.Helper()
	if len(c.MockAPI.TrashCalls) != want {
		c.t.Errorf("TrashCalls = %d, want %d", len(c.MockAPI.TrashCalls), want)
	}
}

// AssertDeleteCalls verifies the number of DeleteMessage calls.
func (c *ExecutorTestContext) AssertDeleteCalls(want int) {
	c.t.Helper()
	if len(c.MockAPI.DeleteCalls) != want {
		c.t.Errorf("DeleteCalls = %d, want %d", len(c.MockAPI.DeleteCalls), want)
	}
}

// AssertCompletedCount verifies the number of completed manifests.
func (c *ExecutorTestContext) AssertCompletedCount(want int) {
	c.t.Helper()
	completed, err := c.Mgr.ListCompleted()
	if err != nil {
		c.t.Fatalf("ListCompleted() error = %v", err)
	}
	if len(completed) != want {
		c.t.Errorf("ListCompleted() = %d, want %d", len(completed), want)
	}
}

// AssertFailedCount verifies the number of failed manifests.
func (c *ExecutorTestContext) AssertFailedCount(want int) {
	c.t.Helper()
	failed, err := c.Mgr.ListFailed()
	if err != nil {
		c.t.Fatalf("ListFailed() error = %v", err)
	}
	if len(failed) != want {
		c.t.Errorf("ListFailed() = %d, want %d", len(failed), want)
	}
}

// Execute runs the executor with default options.
func (c *ExecutorTestContext) Execute(manifestID string) error {
	return c.Exec.Execute(context.Background(), manifestID, nil)
}

// ExecuteWithOpts runs the executor with custom options.
func (c *ExecutorTestContext) ExecuteWithOpts(manifestID string, opts *ExecuteOptions) error {
	return c.Exec.Execute(context.Background(), manifestID, opts)
}

// ExecuteBatch runs the batch executor.
func (c *ExecutorTestContext) ExecuteBatch(manifestID string) error {
	return c.Exec.ExecuteBatch(context.Background(), manifestID)
}

// AssertBatchDeleteCalls verifies the number of BatchDeleteMessages calls.
func (c *ExecutorTestContext) AssertBatchDeleteCalls(want int) {
	c.t.Helper()
	if len(c.MockAPI.BatchDeleteCalls) != want {
		c.t.Errorf("BatchDeleteCalls = %d, want %d", len(c.MockAPI.BatchDeleteCalls), want)
	}
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
	ctx := NewExecutorTestContext(t)
	manifest := ctx.CreateManifest("test deletion", []string{"msg1", "msg2", "msg3"})

	if err := ctx.Execute(manifest.ID); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertTrashCalls(3)
	ctx.AssertCompleted()
	ctx.AssertResult(3, 0)
	ctx.AssertCompletedCount(1)
}

func TestExecutor_Execute_WithDeleteMethod(t *testing.T) {
	ctx := NewExecutorTestContext(t)
	manifest := ctx.CreateManifest("permanent delete", []string{"msg1", "msg2"})

	opts := &ExecuteOptions{
		Method:    MethodDelete,
		BatchSize: 100,
		Resume:    true,
	}

	if err := ctx.ExecuteWithOpts(manifest.ID, opts); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertDeleteCalls(2)
	ctx.AssertTrashCalls(0)
}

func TestExecutor_Execute_WithFailures(t *testing.T) {
	ctx := NewExecutorTestContext(t)
	ctx.MockAPI.trashErrors["msg2"] = errors.New("trash failed")

	manifest := ctx.CreateManifest("partial failure", []string{"msg1", "msg2", "msg3"})

	if err := ctx.Execute(manifest.ID); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertResult(2, 1)
	ctx.AssertCompletedCount(1)

	// Check failed IDs are recorded
	loaded, _, err := ctx.Mgr.GetManifest(manifest.ID)
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
	ctx := NewExecutorTestContext(t)
	ctx.MockAPI.trashErrors["msg1"] = errors.New("fail 1")
	ctx.MockAPI.trashErrors["msg2"] = errors.New("fail 2")

	manifest := ctx.CreateManifest("total failure", []string{"msg1", "msg2"})

	if err := ctx.Execute(manifest.ID); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertFailedCount(1)
}

func TestExecutor_Execute_ContextCancelled(t *testing.T) {
	tc := NewExecutorTestContext(t)

	// Create manifest with many messages
	gmailIDs := make([]string, 100)
	for i := range gmailIDs {
		gmailIDs[i] = fmt.Sprintf("msg%d", i)
	}
	manifest := tc.CreateManifest("interrupt test", gmailIDs)

	// Cancel context immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := tc.Exec.Execute(ctx, manifest.ID, nil)
	if err != context.Canceled {
		t.Errorf("Execute() error = %v, want context.Canceled", err)
	}

	// Manifest should remain in in_progress (for resume)
	inProgress, err := tc.Mgr.ListInProgress()
	if err != nil {
		t.Fatalf("ListInProgress() error = %v", err)
	}
	if len(inProgress) != 1 {
		t.Errorf("ListInProgress() = %d, want 1", len(inProgress))
	}
}

func TestExecutor_Execute_SmallBatchSize(t *testing.T) {
	ctx := NewExecutorTestContext(t)
	manifest := ctx.CreateManifest("small batch test", []string{"msg1", "msg2", "msg3", "msg4", "msg5"})

	opts := &ExecuteOptions{
		Method:    MethodTrash,
		BatchSize: 2,
		Resume:    true,
	}

	if err := ctx.ExecuteWithOpts(manifest.ID, opts); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertTrashCalls(5)
}

func TestExecutor_Execute_ManifestNotFound(t *testing.T) {
	ctx := NewExecutorTestContext(t)

	err := ctx.Execute("nonexistent-id")
	if err == nil {
		t.Error("Execute() should error for nonexistent manifest")
	}
}

func TestExecutor_Execute_InvalidStatus(t *testing.T) {
	ctx := NewExecutorTestContext(t)
	manifest := ctx.CreateManifest("completed test", []string{"msg1"})

	// Execute to completion
	if err := ctx.Execute(manifest.ID); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	// Try to execute again
	err := ctx.Execute(manifest.ID)
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
	ctx := NewExecutorTestContext(t)
	manifest := ctx.CreateManifest("batch delete", []string{"msg1", "msg2", "msg3"})

	if err := ctx.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	ctx.AssertBatchDeleteCalls(1)
	if len(ctx.MockAPI.BatchDeleteCalls[0]) != 3 {
		t.Errorf("BatchDeleteCalls[0] length = %d, want 3", len(ctx.MockAPI.BatchDeleteCalls[0]))
	}
	ctx.AssertCompleted()
	ctx.AssertResult(3, 0)
	ctx.AssertCompletedCount(1)
}

func TestExecutor_ExecuteBatch_LargeBatch(t *testing.T) {
	ctx := NewExecutorTestContext(t)

	// Create manifest with >1000 messages (Gmail batch limit)
	gmailIDs := make([]string, 1500)
	for i := range gmailIDs {
		gmailIDs[i] = fmt.Sprintf("msg%d", i)
	}
	manifest := ctx.CreateManifest("large batch", gmailIDs)

	if err := ctx.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// Should be split into 2 batches (1000 + 500)
	ctx.AssertBatchDeleteCalls(2)
	if len(ctx.MockAPI.BatchDeleteCalls[0]) != 1000 {
		t.Errorf("BatchDeleteCalls[0] length = %d, want 1000", len(ctx.MockAPI.BatchDeleteCalls[0]))
	}
	if len(ctx.MockAPI.BatchDeleteCalls[1]) != 500 {
		t.Errorf("BatchDeleteCalls[1] length = %d, want 500", len(ctx.MockAPI.BatchDeleteCalls[1]))
	}
}

func TestExecutor_ExecuteBatch_WithBatchError(t *testing.T) {
	ctx := NewExecutorTestContext(t)
	ctx.MockAPI.batchError = errors.New("batch failed")

	manifest := ctx.CreateManifest("batch fallback", []string{"msg1", "msg2", "msg3"})

	if err := ctx.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	// Should have attempted batch, then fallen back to individual
	ctx.AssertBatchDeleteCalls(1)
	ctx.AssertDeleteCalls(3)
}

func TestExecutor_ExecuteBatch_InvalidStatus(t *testing.T) {
	ctx := NewExecutorTestContext(t)
	manifest := ctx.CreateManifest("wrong status", []string{"msg1"})

	// Move to in_progress
	if err := ctx.Mgr.MoveManifest(manifest.ID, StatusPending, StatusInProgress); err != nil {
		t.Fatalf("MoveManifest() error = %v", err)
	}

	err := ctx.ExecuteBatch(manifest.ID)
	if err == nil {
		t.Error("ExecuteBatch() should error for non-pending manifest")
	}
}

func TestExecutor_ExecuteBatch_ContextCancelled(t *testing.T) {
	tc := NewExecutorTestContext(t)

	// Create manifest with many messages
	gmailIDs := make([]string, 2500)
	for i := range gmailIDs {
		gmailIDs[i] = fmt.Sprintf("msg%d", i)
	}
	manifest := tc.CreateManifest("cancel batch", gmailIDs)

	// Cancel context immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := tc.Exec.ExecuteBatch(ctx, manifest.ID)
	if err != context.Canceled {
		t.Errorf("ExecuteBatch() error = %v, want context.Canceled", err)
	}
}

func TestExecutor_ExecuteBatch_ManifestNotFound(t *testing.T) {
	ctx := NewExecutorTestContext(t)

	err := ctx.ExecuteBatch("nonexistent-id")
	if err == nil {
		t.Error("ExecuteBatch() should error for nonexistent manifest")
	}
}

// TestExecutor_Execute_NotFoundTreatedAsSuccess verifies that 404 (already deleted)
// is treated as success, making deletion idempotent.
func TestExecutor_Execute_NotFoundTreatedAsSuccess(t *testing.T) {
	ctx := NewExecutorTestContext(t)
	ctx.MockAPI.trashErrors["msg2"] = &gmail.NotFoundError{Path: "/users/me/messages/msg2/trash"}

	manifest := ctx.CreateManifest("idempotent test", []string{"msg1", "msg2", "msg3"})

	if err := ctx.Execute(manifest.ID); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertResult(3, 0)
	ctx.AssertCompletedCount(1)

	// Verify no failed IDs recorded
	loaded, _, err := ctx.Mgr.GetManifest(manifest.ID)
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
	ctx := NewExecutorTestContext(t)
	ctx.MockAPI.batchError = errors.New("batch failed")
	ctx.MockAPI.deleteErrors["msg2"] = &gmail.NotFoundError{Path: "/users/me/messages/msg2"}

	manifest := ctx.CreateManifest("batch fallback 404", []string{"msg1", "msg2", "msg3"})

	if err := ctx.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	ctx.AssertResult(3, 0)
}

// TestExecutor_ExecuteBatch_FallbackWithNon404Failures verifies that
// non-404 failures during fallback are properly counted as failures.
func TestExecutor_ExecuteBatch_FallbackWithNon404Failures(t *testing.T) {
	ctx := NewExecutorTestContext(t)
	ctx.MockAPI.batchError = errors.New("batch failed")
	ctx.MockAPI.deleteErrors["msg2"] = errors.New("permission denied")

	manifest := ctx.CreateManifest("batch fallback failures", []string{"msg1", "msg2", "msg3"})

	if err := ctx.ExecuteBatch(manifest.ID); err != nil {
		t.Fatalf("ExecuteBatch() error = %v", err)
	}

	ctx.AssertResult(2, 1)
}

// TestExecutor_Execute_WithDeleteMethod_404 tests 404 handling with permanent delete method.
func TestExecutor_Execute_WithDeleteMethod_404(t *testing.T) {
	ctx := NewExecutorTestContext(t)
	ctx.MockAPI.deleteErrors["msg2"] = &gmail.NotFoundError{Path: "/users/me/messages/msg2"}

	manifest := ctx.CreateManifest("delete method 404", []string{"msg1", "msg2", "msg3"})

	opts := &ExecuteOptions{
		Method:    MethodDelete,
		BatchSize: 100,
		Resume:    true,
	}

	if err := ctx.ExecuteWithOpts(manifest.ID, opts); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	ctx.AssertResult(3, 0)
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
