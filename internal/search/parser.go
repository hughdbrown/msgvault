// Package search provides Gmail-like search query parsing.
package search

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Query represents a parsed search query with all supported filters.
type Query struct {
	TextTerms     []string   // Full-text search terms
	FromAddrs     []string   // from: filters
	ToAddrs       []string   // to: filters
	CcAddrs       []string   // cc: filters
	BccAddrs      []string   // bcc: filters
	SubjectTerms  []string   // subject: filters
	Labels        []string   // label: filters
	HasAttachment *bool      // has:attachment
	BeforeDate    *time.Time // before: filter
	AfterDate     *time.Time // after: filter
	LargerThan    *int64     // larger: filter (bytes)
	SmallerThan   *int64     // smaller: filter (bytes)
	AccountID     *int64     // in: account filter
}

// IsEmpty returns true if the query has no search criteria.
func (q *Query) IsEmpty() bool {
	return len(q.TextTerms) == 0 &&
		len(q.FromAddrs) == 0 &&
		len(q.ToAddrs) == 0 &&
		len(q.CcAddrs) == 0 &&
		len(q.BccAddrs) == 0 &&
		len(q.SubjectTerms) == 0 &&
		len(q.Labels) == 0 &&
		q.HasAttachment == nil &&
		q.BeforeDate == nil &&
		q.AfterDate == nil &&
		q.LargerThan == nil &&
		q.SmallerThan == nil
}

// Parse parses a Gmail-like search query string into a Query object.
//
// Supported operators:
//   - from:, to:, cc:, bcc: - address filters
//   - subject: - subject text search
//   - label: or l: - label filter
//   - has:attachment - attachment filter
//   - before:, after: - date filters (YYYY-MM-DD)
//   - older_than:, newer_than: - relative date filters (e.g., 7d, 2w, 1m, 1y)
//   - larger:, smaller: - size filters (e.g., 5M, 100K)
//   - Bare words and "quoted phrases" - full-text search
func Parse(queryStr string) *Query {
	q := &Query{}
	tokens := tokenize(queryStr)

	for _, token := range tokens {
		// Check if it's a quoted phrase
		if strings.HasPrefix(token, "\"") && strings.HasSuffix(token, "\"") && len(token) > 2 {
			q.TextTerms = append(q.TextTerms, token[1:len(token)-1])
			continue
		}

		// Check for operator:value pattern
		if idx := strings.Index(token, ":"); idx != -1 {
			op := strings.ToLower(token[:idx])
			value := token[idx+1:]

			// Strip quotes from value
			if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
				value = value[1 : len(value)-1]
			}

			switch op {
			case "from":
				q.FromAddrs = append(q.FromAddrs, strings.ToLower(value))
			case "to":
				q.ToAddrs = append(q.ToAddrs, strings.ToLower(value))
			case "cc":
				q.CcAddrs = append(q.CcAddrs, strings.ToLower(value))
			case "bcc":
				q.BccAddrs = append(q.BccAddrs, strings.ToLower(value))
			case "subject":
				q.SubjectTerms = append(q.SubjectTerms, value)
			case "label", "l":
				q.Labels = append(q.Labels, value)
			case "has":
				if strings.ToLower(value) == "attachment" || strings.ToLower(value) == "attachments" {
					b := true
					q.HasAttachment = &b
				}
			case "before":
				if t := parseDate(value); t != nil {
					q.BeforeDate = t
				}
			case "after":
				if t := parseDate(value); t != nil {
					q.AfterDate = t
				}
			case "older_than":
				if t := parseRelativeDate(value); t != nil {
					q.BeforeDate = t
				}
			case "newer_than":
				if t := parseRelativeDate(value); t != nil {
					q.AfterDate = t
				}
			case "larger":
				if size := parseSize(value); size != nil {
					q.LargerThan = size
				}
			case "smaller":
				if size := parseSize(value); size != nil {
					q.SmallerThan = size
				}
			default:
				// Unknown operator - treat as text
				q.TextTerms = append(q.TextTerms, token)
			}
			continue
		}

		// Not an operator - treat as text search term
		q.TextTerms = append(q.TextTerms, token)
	}

	return q
}

// tokenize splits a query string, preserving quoted phrases and operator:value pairs.
// Handles cases like subject:"foo bar" where the operator and quoted value should stay together.
func tokenize(queryStr string) []string {
	var tokens []string
	var current strings.Builder
	inQuotes := false
	quoteChar := rune(0)
	// Track if we just saw a colon (for op:"value" handling)
	afterColon := false
	// Track if this quoted section started as op:"value" (quote immediately after colon)
	opQuoted := false

	for _, char := range queryStr {
		if (char == '"' || char == '\'') && !inQuotes {
			// Start of quoted section
			inQuotes = true
			quoteChar = char
			// If we just saw a colon, this is an op:"value" case
			opQuoted = afterColon
			// If we just saw a colon, keep building the same token (op:"value" case)
			if !afterColon && current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			// Include the quote in the token for op:"value" case
			if afterColon {
				current.WriteRune(char)
			}
			afterColon = false
		} else if char == quoteChar && inQuotes {
			// End of quoted section
			inQuotes = false
			// Check if this was an op:"value" case (quote started after colon)
			if opQuoted {
				// Include the closing quote and save the whole token
				current.WriteRune(char)
				tokens = append(tokens, current.String())
				current.Reset()
			} else if current.Len() > 0 {
				// Standalone quoted phrase (may contain colons, but not op:"value")
				tokens = append(tokens, "\""+current.String()+"\"")
				current.Reset()
			}
			quoteChar = 0
			opQuoted = false
		} else if char == ' ' && !inQuotes {
			if current.Len() > 0 {
				tokens = append(tokens, current.String())
				current.Reset()
			}
			afterColon = false
		} else {
			current.WriteRune(char)
			afterColon = (char == ':')
		}
	}

	if current.Len() > 0 {
		tokens = append(tokens, current.String())
	}

	return tokens
}

// parseDate parses date strings like YYYY-MM-DD or YYYY/MM/DD.
func parseDate(value string) *time.Time {
	formats := []string{
		"2006-01-02",
		"2006/01/02",
		"01/02/2006",
		"02/01/2006",
	}

	value = strings.TrimSpace(value)
	for _, format := range formats {
		if t, err := time.Parse(format, value); err == nil {
			t = t.UTC()
			return &t
		}
	}
	return nil
}

// parseRelativeDate parses relative dates like 7d, 2w, 1m, 1y.
func parseRelativeDate(value string) *time.Time {
	value = strings.TrimSpace(strings.ToLower(value))
	re := regexp.MustCompile(`^(\d+)([dwmy])$`)
	match := re.FindStringSubmatch(value)
	if match == nil {
		return nil
	}

	amount, _ := strconv.Atoi(match[1])
	unit := match[2]
	now := time.Now().UTC()

	var result time.Time
	switch unit {
	case "d":
		result = now.AddDate(0, 0, -amount)
	case "w":
		result = now.AddDate(0, 0, -amount*7)
	case "m":
		result = now.AddDate(0, -amount, 0)
	case "y":
		result = now.AddDate(-amount, 0, 0)
	default:
		return nil
	}

	return &result
}

// parseSize parses size strings like 5M, 100K, 1G into bytes.
func parseSize(value string) *int64 {
	value = strings.TrimSpace(strings.ToUpper(value))
	multipliers := map[string]int64{
		"K":  1024,
		"KB": 1024,
		"M":  1024 * 1024,
		"MB": 1024 * 1024,
		"G":  1024 * 1024 * 1024,
		"GB": 1024 * 1024 * 1024,
	}

	for suffix, mult := range multipliers {
		if strings.HasSuffix(value, suffix) {
			numStr := value[:len(value)-len(suffix)]
			if num, err := strconv.ParseFloat(numStr, 64); err == nil {
				result := int64(num * float64(mult))
				return &result
			}
			return nil
		}
	}

	// Plain number (bytes)
	if num, err := strconv.ParseInt(value, 10, 64); err == nil {
		return &num
	}
	return nil
}
