package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/wesm/msgvault/internal/query"
	"github.com/wesm/msgvault/internal/search"
)

type handlers struct {
	engine query.Engine
}

func (h *handlers) searchMessages(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	queryStr, _ := args["query"].(string)
	if queryStr == "" {
		return mcp.NewToolResultError("query parameter is required"), nil
	}

	limit := intArg(args, "limit", 20)
	offset := intArg(args, "offset", 0)

	q := search.Parse(queryStr)

	// Try fast search first (metadata only), fall back to full FTS.
	results, err := h.engine.SearchFast(ctx, q, query.MessageFilter{}, limit, offset)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("search failed: %v", err)), nil
	}

	// If fast search returns nothing and query has free text, try full FTS.
	if len(results) == 0 && len(q.TextTerms) > 0 {
		results, err = h.engine.Search(ctx, q, limit, offset)
		if err != nil {
			return mcp.NewToolResultError(fmt.Sprintf("search failed: %v", err)), nil
		}
	}

	return jsonResult(results)
}

func (h *handlers) getMessage(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	id, ok := args["id"].(float64)
	if !ok {
		return mcp.NewToolResultError("id parameter is required"), nil
	}

	msg, err := h.engine.GetMessage(ctx, int64(id))
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("message not found: %v", err)), nil
	}

	return jsonResult(msg)
}

func (h *handlers) listMessages(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	filter := query.MessageFilter{
		Limit:  intArg(args, "limit", 20),
		Offset: intArg(args, "offset", 0),
	}

	if v, ok := args["from"].(string); ok && v != "" {
		filter.Sender = v
	}
	if v, ok := args["to"].(string); ok && v != "" {
		filter.Recipient = v
	}
	if v, ok := args["label"].(string); ok && v != "" {
		filter.Label = v
	}
	if v, ok := args["has_attachment"].(bool); ok && v {
		filter.WithAttachmentsOnly = true
	}
	if v, ok := args["after"].(string); ok && v != "" {
		if t, err := time.Parse("2006-01-02", v); err == nil {
			filter.After = &t
		}
	}
	if v, ok := args["before"].(string); ok && v != "" {
		if t, err := time.Parse("2006-01-02", v); err == nil {
			filter.Before = &t
		}
	}

	results, err := h.engine.ListMessages(ctx, filter)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("list failed: %v", err)), nil
	}

	return jsonResult(results)
}

func (h *handlers) getStats(ctx context.Context, _ mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	stats, err := h.engine.GetTotalStats(ctx, query.StatsOptions{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("stats failed: %v", err)), nil
	}

	accounts, err := h.engine.ListAccounts(ctx)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("accounts failed: %v", err)), nil
	}

	resp := struct {
		Stats    *query.TotalStats  `json:"stats"`
		Accounts []query.AccountInfo `json:"accounts"`
	}{
		Stats:    stats,
		Accounts: accounts,
	}

	return jsonResult(resp)
}

func (h *handlers) aggregate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()

	groupBy, _ := args["group_by"].(string)
	if groupBy == "" {
		return mcp.NewToolResultError("group_by parameter is required"), nil
	}

	opts := query.AggregateOptions{
		Limit: intArg(args, "limit", 50),
	}

	if v, ok := args["after"].(string); ok && v != "" {
		if t, err := time.Parse("2006-01-02", v); err == nil {
			opts.After = &t
		}
	}
	if v, ok := args["before"].(string); ok && v != "" {
		if t, err := time.Parse("2006-01-02", v); err == nil {
			opts.Before = &t
		}
	}

	var (
		rows []query.AggregateRow
		err  error
	)

	switch groupBy {
	case "sender":
		rows, err = h.engine.AggregateBySender(ctx, opts)
	case "recipient":
		rows, err = h.engine.AggregateByRecipient(ctx, opts)
	case "domain":
		rows, err = h.engine.AggregateByDomain(ctx, opts)
	case "label":
		rows, err = h.engine.AggregateByLabel(ctx, opts)
	case "time":
		rows, err = h.engine.AggregateByTime(ctx, opts)
	default:
		return mcp.NewToolResultError(fmt.Sprintf("invalid group_by: %s", groupBy)), nil
	}

	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("aggregate failed: %v", err)), nil
	}

	return jsonResult(rows)
}

// intArg extracts an integer from a map, with a default value.
// JSON numbers arrive as float64.
func intArg(args map[string]any, key string, def int) int {
	if v, ok := args[key].(float64); ok {
		return int(v)
	}
	return def
}

func jsonResult(v any) (*mcp.CallToolResult, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal error: %v", err)), nil
	}
	return mcp.NewToolResultText(string(data)), nil
}
