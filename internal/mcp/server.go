package mcp

import (
	"context"
	"os"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/wesm/msgvault/internal/query"
)

// Serve creates an MCP server with email archive tools and serves over stdio.
// It blocks until stdin is closed or the context is cancelled.
func Serve(ctx context.Context, engine query.Engine) error {
	s := server.NewMCPServer(
		"msgvault",
		"1.0.0",
		server.WithToolCapabilities(false),
	)

	h := &handlers{engine: engine}

	s.AddTool(searchMessagesTool(), h.searchMessages)
	s.AddTool(getMessageTool(), h.getMessage)
	s.AddTool(listMessagesTool(), h.listMessages)
	s.AddTool(getStatsTool(), h.getStats)
	s.AddTool(aggregateTool(), h.aggregate)

	stdio := server.NewStdioServer(s)
	return stdio.Listen(ctx, os.Stdin, os.Stdout)
}

func searchMessagesTool() mcp.Tool {
	return mcp.NewTool("search_messages",
		mcp.WithDescription("Search emails using Gmail-like query syntax. Supports from:, to:, subject:, label:, has:attachment, before:, after:, and free text."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithString("query",
			mcp.Required(),
			mcp.Description("Gmail-style search query (e.g. 'from:alice subject:meeting after:2024-01-01')"),
		),
		mcp.WithNumber("limit",
			mcp.Description("Maximum results to return (default 20)"),
		),
		mcp.WithNumber("offset",
			mcp.Description("Number of results to skip for pagination (default 0)"),
		),
	)
}

func getMessageTool() mcp.Tool {
	return mcp.NewTool("get_message",
		mcp.WithDescription("Get full message details including body text, recipients, labels, and attachments by message ID."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithNumber("id",
			mcp.Required(),
			mcp.Description("Message ID"),
		),
	)
}

func listMessagesTool() mcp.Tool {
	return mcp.NewTool("list_messages",
		mcp.WithDescription("List messages with optional filters. Returns message summaries sorted by date."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithString("from",
			mcp.Description("Filter by sender email address"),
		),
		mcp.WithString("to",
			mcp.Description("Filter by recipient email address"),
		),
		mcp.WithString("label",
			mcp.Description("Filter by Gmail label"),
		),
		mcp.WithString("after",
			mcp.Description("Only messages after this date (YYYY-MM-DD)"),
		),
		mcp.WithString("before",
			mcp.Description("Only messages before this date (YYYY-MM-DD)"),
		),
		mcp.WithBoolean("has_attachment",
			mcp.Description("Only messages with attachments"),
		),
		mcp.WithNumber("limit",
			mcp.Description("Maximum results to return (default 20)"),
		),
		mcp.WithNumber("offset",
			mcp.Description("Number of results to skip for pagination (default 0)"),
		),
	)
}

func getStatsTool() mcp.Tool {
	return mcp.NewTool("get_stats",
		mcp.WithDescription("Get archive overview: total messages, size, attachment count, and accounts."),
		mcp.WithReadOnlyHintAnnotation(true),
	)
}

func aggregateTool() mcp.Tool {
	return mcp.NewTool("aggregate",
		mcp.WithDescription("Get grouped statistics (e.g. top senders, domains, labels, or message volume over time)."),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithString("group_by",
			mcp.Required(),
			mcp.Description("Dimension to group by"),
			mcp.Enum("sender", "recipient", "domain", "label", "time"),
		),
		mcp.WithNumber("limit",
			mcp.Description("Maximum groups to return (default 50)"),
		),
		mcp.WithString("after",
			mcp.Description("Only messages after this date (YYYY-MM-DD)"),
		),
		mcp.WithString("before",
			mcp.Description("Only messages before this date (YYYY-MM-DD)"),
		),
	)
}
