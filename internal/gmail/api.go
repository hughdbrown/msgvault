// Package gmail provides a Gmail API client with rate limiting and retry logic.
package gmail

import (
	"context"
	"time"
)

// API defines the interface for Gmail operations.
// This interface enables mocking for tests without hitting the real API.
type API interface {
	// GetProfile returns the authenticated user's profile.
	GetProfile(ctx context.Context) (*Profile, error)

	// ListLabels returns all labels for the account.
	ListLabels(ctx context.Context) ([]*Label, error)

	// ListMessages returns message IDs matching the query.
	// Use pageToken for pagination. Returns next page token if more results exist.
	ListMessages(ctx context.Context, query string, pageToken string) (*MessageListResponse, error)

	// GetMessageRaw fetches a single message with raw MIME data.
	GetMessageRaw(ctx context.Context, messageID string) (*RawMessage, error)

	// GetMessagesRawBatch fetches multiple messages in parallel with rate limiting.
	// Returns results in the same order as input IDs. Failed fetches return nil.
	GetMessagesRawBatch(ctx context.Context, messageIDs []string) ([]*RawMessage, error)

	// ListHistory returns changes since the given history ID.
	ListHistory(ctx context.Context, startHistoryID uint64, pageToken string) (*HistoryResponse, error)

	// TrashMessage moves a message to trash (recoverable for 30 days).
	TrashMessage(ctx context.Context, messageID string) error

	// DeleteMessage permanently deletes a message.
	DeleteMessage(ctx context.Context, messageID string) error

	// BatchDeleteMessages permanently deletes multiple messages (max 1000).
	BatchDeleteMessages(ctx context.Context, messageIDs []string) error

	// Close releases any resources held by the client.
	Close() error
}

// Profile represents a Gmail user profile.
type Profile struct {
	EmailAddress  string
	MessagesTotal int64
	ThreadsTotal  int64
	HistoryID     uint64
}

// Label represents a Gmail label.
type Label struct {
	ID                    string
	Name                  string
	Type                  string // "system" or "user"
	MessagesTotal         int64
	MessagesUnread        int64
	MessageListVisibility string
	LabelListVisibility   string
}

// MessageListResponse contains a page of message IDs.
type MessageListResponse struct {
	Messages           []MessageID
	NextPageToken      string
	ResultSizeEstimate int64
}

// MessageID represents a message reference from list operations.
type MessageID struct {
	ID       string
	ThreadID string
}

// RawMessage contains the raw MIME data for a message.
type RawMessage struct {
	ID           string
	ThreadID     string
	LabelIDs     []string
	Snippet      string
	HistoryID    uint64
	InternalDate int64 // Unix milliseconds
	SizeEstimate int64
	Raw          []byte // Decoded from base64url
}

// HistoryResponse contains changes since a history ID.
type HistoryResponse struct {
	History       []HistoryRecord
	NextPageToken string
	HistoryID     uint64
}

// HistoryRecord represents a single history change.
type HistoryRecord struct {
	ID              uint64
	MessagesAdded   []HistoryMessage
	MessagesDeleted []HistoryMessage
	LabelsAdded     []HistoryLabelChange
	LabelsRemoved   []HistoryLabelChange
}

// HistoryMessage represents a message in history.
type HistoryMessage struct {
	Message MessageID
}

// HistoryLabelChange represents a label change in history.
type HistoryLabelChange struct {
	Message  MessageID
	LabelIDs []string
}

// SyncProgress reports sync progress to the caller.
type SyncProgress interface {
	// OnStart is called when sync begins.
	OnStart(total int64)

	// OnProgress is called periodically during sync.
	OnProgress(processed, added, skipped int64)

	// OnComplete is called when sync finishes.
	OnComplete(summary *SyncSummary)

	// OnError is called when an error occurs.
	OnError(err error)
}

// SyncSummary contains statistics about a completed sync.
type SyncSummary struct {
	StartTime        time.Time
	EndTime          time.Time
	Duration         time.Duration
	MessagesFound    int64
	MessagesAdded    int64
	MessagesUpdated  int64
	MessagesSkipped  int64
	BytesDownloaded  int64
	Errors           int64
	FinalHistoryID   uint64
	WasResumed       bool
	ResumedFromToken string
}

// SyncProgressWithDate is an optional extension of SyncProgress
// that provides message date info for better progress context.
type SyncProgressWithDate interface {
	SyncProgress
	// OnLatestDate reports the date of the most recently processed message.
	// This helps show where in the mailbox the sync is currently processing.
	OnLatestDate(date time.Time)
}

// NullProgress is a no-op progress reporter.
type NullProgress struct{}

func (NullProgress) OnStart(total int64)                        {}
func (NullProgress) OnProgress(processed, added, skipped int64) {}
func (NullProgress) OnComplete(summary *SyncSummary)            {}
func (NullProgress) OnError(err error)                          {}
func (NullProgress) OnLatestDate(date time.Time)                {}
