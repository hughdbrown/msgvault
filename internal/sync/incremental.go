package sync

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/wesm/msgvault/internal/gmail"
	"github.com/wesm/msgvault/internal/store"
)

// Incremental performs an incremental sync using the Gmail History API.
// Falls back to full sync if history is too old (404 error).
func (s *Syncer) Incremental(ctx context.Context, email string) (*gmail.SyncSummary, error) {
	startTime := time.Now()
	summary := &gmail.SyncSummary{StartTime: startTime}

	// Get source - must already exist for incremental sync
	source, err := s.store.GetSourceByIdentifier(email)
	if err != nil {
		return nil, fmt.Errorf("get source: %w", err)
	}
	if source == nil {
		return nil, fmt.Errorf("no source found for %s - run full sync first", email)
	}

	// Get last history ID
	if !source.SyncCursor.Valid || source.SyncCursor.String == "" {
		return nil, fmt.Errorf("no history ID for %s - run full sync first", email)
	}

	startHistoryID, err := strconv.ParseUint(source.SyncCursor.String, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid history ID %q: %w", source.SyncCursor.String, err)
	}

	// Start sync
	syncID, err := s.store.StartSync(source.ID, "incremental")
	if err != nil {
		return nil, fmt.Errorf("start sync: %w", err)
	}

	// Defer failure handling
	defer func() {
		if r := recover(); r != nil {
			_ = s.store.FailSync(syncID, fmt.Sprintf("panic: %v", r))
			panic(r)
		}
	}()

	// Get profile for current history ID
	profile, err := s.client.GetProfile(ctx)
	if err != nil {
		_ = s.store.FailSync(syncID, err.Error())
		return nil, fmt.Errorf("get profile: %w", err)
	}

	s.logger.Info("incremental sync", "email", email, "start_history", startHistoryID, "current_history", profile.HistoryID)

	// If history IDs match, nothing to do
	if startHistoryID >= profile.HistoryID {
		s.logger.Info("already up to date")
		_ = s.store.CompleteSync(syncID, strconv.FormatUint(profile.HistoryID, 10))
		summary.EndTime = time.Now()
		summary.Duration = summary.EndTime.Sub(summary.StartTime)
		summary.FinalHistoryID = profile.HistoryID
		return summary, nil
	}

	// Sync labels first (new labels may have been created)
	labelMap, err := s.syncLabels(ctx, source.ID)
	if err != nil {
		_ = s.store.FailSync(syncID, err.Error())
		return nil, fmt.Errorf("sync labels: %w", err)
	}

	// Process history
	checkpoint := &store.Checkpoint{}
	pageToken := ""

	for {
		historyResp, err := s.client.ListHistory(ctx, startHistoryID, pageToken)
		if err != nil {
			// Check for 404 - history too old
			var notFound *gmail.NotFoundError
			if errors.As(err, &notFound) {
				s.logger.Warn("history too old, falling back to full sync")
				_ = s.store.FailSync(syncID, "history too old")
				// Caller should trigger full sync
				return nil, ErrHistoryExpired
			}
			_ = s.store.FailSync(syncID, err.Error())
			return nil, fmt.Errorf("list history: %w", err)
		}

		// Process each history record
		for _, record := range historyResp.History {
			// Handle added messages
			for _, added := range record.MessagesAdded {
				// Fetch and ingest the new message
				raw, err := s.client.GetMessageRaw(ctx, added.Message.ID)
				if err != nil {
					var notFound *gmail.NotFoundError
					if errors.As(err, &notFound) {
						// Message was deleted before we could fetch it
						continue
					}
					s.logger.Warn("failed to fetch added message", "id", added.Message.ID, "error", err)
					checkpoint.ErrorsCount++
					continue
				}

				err = s.ingestMessage(ctx, source.ID, raw, added.Message.ThreadID, labelMap)
				if err != nil {
					s.logger.Warn("failed to ingest added message", "id", added.Message.ID, "error", err)
					checkpoint.ErrorsCount++
					continue
				}

				checkpoint.MessagesAdded++
				summary.BytesDownloaded += int64(len(raw.Raw))
			}

			// Handle deleted messages
			for _, deleted := range record.MessagesDeleted {
				if err := s.store.MarkMessageDeleted(source.ID, deleted.Message.ID); err != nil {
					s.logger.Warn("failed to mark message deleted", "id", deleted.Message.ID, "error", err)
					checkpoint.ErrorsCount++
				}
			}

			// Handle label changes
			for _, labelAdded := range record.LabelsAdded {
				if err := s.handleLabelChange(ctx, source.ID, labelAdded.Message.ID, labelAdded.Message.ThreadID, labelAdded.LabelIDs, labelMap, true); err != nil {
					s.logger.Warn("failed to handle label add", "id", labelAdded.Message.ID, "error", err)
				}
			}

			for _, labelRemoved := range record.LabelsRemoved {
				if err := s.handleLabelChange(ctx, source.ID, labelRemoved.Message.ID, labelRemoved.Message.ThreadID, labelRemoved.LabelIDs, labelMap, false); err != nil {
					s.logger.Warn("failed to handle label remove", "id", labelRemoved.Message.ID, "error", err)
				}
			}

			checkpoint.MessagesProcessed++
		}

		// Report progress
		s.progress.OnProgress(checkpoint.MessagesProcessed, checkpoint.MessagesAdded, 0)

		// Save checkpoint
		pageToken = historyResp.NextPageToken
		checkpoint.PageToken = pageToken
		if err := s.store.UpdateSyncCheckpoint(syncID, checkpoint); err != nil {
			s.logger.Warn("failed to save checkpoint", "error", err)
		}

		// No more pages
		if pageToken == "" {
			break
		}
	}

	// Update source with final history ID
	historyIDStr := strconv.FormatUint(profile.HistoryID, 10)
	if err := s.store.UpdateSourceSyncCursor(source.ID, historyIDStr); err != nil {
		s.logger.Warn("failed to update sync cursor", "error", err)
	}

	// Mark sync complete
	if err := s.store.CompleteSync(syncID, historyIDStr); err != nil {
		s.logger.Warn("failed to complete sync", "error", err)
	}

	// Build summary
	summary.EndTime = time.Now()
	summary.Duration = summary.EndTime.Sub(summary.StartTime)
	summary.MessagesFound = checkpoint.MessagesProcessed
	summary.MessagesAdded = checkpoint.MessagesAdded
	summary.MessagesUpdated = checkpoint.MessagesUpdated
	summary.Errors = checkpoint.ErrorsCount
	summary.FinalHistoryID = profile.HistoryID

	s.progress.OnComplete(summary)
	return summary, nil
}

// handleLabelChange processes a label addition or removal.
// If the message doesn't exist locally, it may need to be fetched.
func (s *Syncer) handleLabelChange(ctx context.Context, sourceID int64, messageID, threadID string, gmailLabelIDs []string, labelMap map[string]int64, isAdd bool) error {
	// Check if message exists
	existing, err := s.store.MessageExistsBatch(sourceID, []string{messageID})
	if err != nil {
		return err
	}

	internalID, exists := existing[messageID]

	if !exists {
		// Message doesn't exist locally - if adding labels, we should fetch it
		if isAdd {
			raw, err := s.client.GetMessageRaw(ctx, messageID)
			if err != nil {
				return err
			}
			return s.ingestMessage(ctx, sourceID, raw, threadID, labelMap)
		}
		// Removing labels from non-existent message is a no-op
		return nil
	}

	// Get current labels
	// For simplicity, we'll just re-fetch and update all labels
	// A more efficient approach would track individual adds/removes
	raw, err := s.client.GetMessageRaw(ctx, messageID)
	if err != nil {
		return err
	}

	// Convert Gmail label IDs to internal IDs
	var labelIDs []int64
	for _, gmailID := range raw.LabelIDs {
		if id, ok := labelMap[gmailID]; ok {
			labelIDs = append(labelIDs, id)
		}
	}

	return s.store.ReplaceMessageLabels(internalID, labelIDs)
}
