package search

import (
	"testing"
	"time"
)

func TestParse_BasicOperators(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		wantFrom []string
		wantTo   []string
		wantText []string
	}{
		{
			name:     "from operator",
			query:    "from:alice@example.com",
			wantFrom: []string{"alice@example.com"},
		},
		{
			name:   "to operator",
			query:  "to:bob@example.com",
			wantTo: []string{"bob@example.com"},
		},
		{
			name:     "multiple from",
			query:    "from:alice@example.com from:bob@example.com",
			wantFrom: []string{"alice@example.com", "bob@example.com"},
		},
		{
			name:     "bare text",
			query:    "hello world",
			wantText: []string{"hello", "world"},
		},
		{
			name:     "quoted phrase",
			query:    `"hello world"`,
			wantText: []string{"hello world"},
		},
		{
			name:     "mixed operators and text",
			query:    "from:alice@example.com meeting notes",
			wantFrom: []string{"alice@example.com"},
			wantText: []string{"meeting", "notes"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := Parse(tt.query)

			if len(tt.wantFrom) > 0 {
				if len(q.FromAddrs) != len(tt.wantFrom) {
					t.Errorf("FromAddrs: got %v, want %v", q.FromAddrs, tt.wantFrom)
				}
				for i, addr := range tt.wantFrom {
					if i < len(q.FromAddrs) && q.FromAddrs[i] != addr {
						t.Errorf("FromAddrs[%d]: got %s, want %s", i, q.FromAddrs[i], addr)
					}
				}
			}

			if len(tt.wantTo) > 0 {
				if len(q.ToAddrs) != len(tt.wantTo) {
					t.Errorf("ToAddrs: got %v, want %v", q.ToAddrs, tt.wantTo)
				}
			}

			if len(tt.wantText) > 0 {
				if len(q.TextTerms) != len(tt.wantText) {
					t.Errorf("TextTerms: got %v, want %v", q.TextTerms, tt.wantText)
				}
				for i, term := range tt.wantText {
					if i < len(q.TextTerms) && q.TextTerms[i] != term {
						t.Errorf("TextTerms[%d]: got %s, want %s", i, q.TextTerms[i], term)
					}
				}
			}
		})
	}
}

func TestParse_HasAttachment(t *testing.T) {
	q := Parse("has:attachment")
	if q.HasAttachment == nil || !*q.HasAttachment {
		t.Errorf("HasAttachment: expected true, got %v", q.HasAttachment)
	}
}

func TestParse_Dates(t *testing.T) {
	q := Parse("after:2024-01-15 before:2024-06-30")

	if q.AfterDate == nil {
		t.Fatal("AfterDate is nil")
	}
	expectedAfter := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	if !q.AfterDate.Equal(expectedAfter) {
		t.Errorf("AfterDate: got %v, want %v", q.AfterDate, expectedAfter)
	}

	if q.BeforeDate == nil {
		t.Fatal("BeforeDate is nil")
	}
	expectedBefore := time.Date(2024, 6, 30, 0, 0, 0, 0, time.UTC)
	if !q.BeforeDate.Equal(expectedBefore) {
		t.Errorf("BeforeDate: got %v, want %v", q.BeforeDate, expectedBefore)
	}
}

func TestParse_RelativeDates(t *testing.T) {
	q := Parse("newer_than:7d")
	if q.AfterDate == nil {
		t.Fatal("AfterDate is nil for newer_than")
	}

	// Should be approximately 7 days ago
	expected := time.Now().UTC().AddDate(0, 0, -7)
	diff := q.AfterDate.Sub(expected)
	if diff < -time.Second || diff > time.Second {
		t.Errorf("AfterDate: got %v, expected around %v", q.AfterDate, expected)
	}
}

func TestParse_Sizes(t *testing.T) {
	tests := []struct {
		query string
		check func(q *Query) bool
	}{
		{
			query: "larger:5M",
			check: func(q *Query) bool {
				return q.LargerThan != nil && *q.LargerThan == 5*1024*1024
			},
		},
		{
			query: "smaller:100K",
			check: func(q *Query) bool {
				return q.SmallerThan != nil && *q.SmallerThan == 100*1024
			},
		},
		{
			query: "larger:1G",
			check: func(q *Query) bool {
				return q.LargerThan != nil && *q.LargerThan == 1024*1024*1024
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			q := Parse(tt.query)
			if !tt.check(q) {
				t.Errorf("Size filter not parsed correctly for %q", tt.query)
			}
		})
	}
}

func TestParse_Labels(t *testing.T) {
	q := Parse("label:INBOX l:work")
	if len(q.Labels) != 2 {
		t.Fatalf("Labels: expected 2, got %d", len(q.Labels))
	}
	if q.Labels[0] != "INBOX" {
		t.Errorf("Labels[0]: got %s, want INBOX", q.Labels[0])
	}
	if q.Labels[1] != "work" {
		t.Errorf("Labels[1]: got %s, want work", q.Labels[1])
	}
}

func TestParse_Subject(t *testing.T) {
	q := Parse("subject:urgent")
	if len(q.SubjectTerms) != 1 || q.SubjectTerms[0] != "urgent" {
		t.Errorf("SubjectTerms: got %v, want [urgent]", q.SubjectTerms)
	}
}

func TestParse_QuotedOperatorValue(t *testing.T) {
	// Test that subject:"foo bar" keeps the quoted phrase with the operator
	tests := []struct {
		name        string
		query       string
		wantSubject []string
		wantText    []string
		wantFrom    []string
		wantLabels  []string
	}{
		{
			name:        "subject with quoted phrase",
			query:       `subject:"meeting notes"`,
			wantSubject: []string{"meeting notes"},
		},
		{
			name:        "subject with quoted phrase and other terms",
			query:       `subject:"project update" from:alice@example.com`,
			wantSubject: []string{"project update"},
			wantFrom:    []string{"alice@example.com"},
		},
		{
			name:       "label with quoted value containing spaces",
			query:      `label:"My Important Label"`,
			wantLabels: []string{"My Important Label"},
		},
		{
			name:        "mixed quoted and unquoted",
			query:       `subject:urgent subject:"very important" search term`,
			wantSubject: []string{"urgent", "very important"},
			wantText:    []string{"search", "term"},
		},
		{
			name:     "from with quoted display name style (edge case)",
			query:    `from:"alice@example.com"`,
			wantFrom: []string{"alice@example.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := Parse(tt.query)

			if len(tt.wantSubject) > 0 {
				if len(q.SubjectTerms) != len(tt.wantSubject) {
					t.Errorf("SubjectTerms: got %v, want %v", q.SubjectTerms, tt.wantSubject)
				} else {
					for i, want := range tt.wantSubject {
						if q.SubjectTerms[i] != want {
							t.Errorf("SubjectTerms[%d]: got %q, want %q", i, q.SubjectTerms[i], want)
						}
					}
				}
			}

			if len(tt.wantText) > 0 {
				if len(q.TextTerms) != len(tt.wantText) {
					t.Errorf("TextTerms: got %v, want %v", q.TextTerms, tt.wantText)
				}
			}

			if len(tt.wantFrom) > 0 {
				if len(q.FromAddrs) != len(tt.wantFrom) {
					t.Errorf("FromAddrs: got %v, want %v", q.FromAddrs, tt.wantFrom)
				}
			}

			if len(tt.wantLabels) > 0 {
				if len(q.Labels) != len(tt.wantLabels) {
					t.Errorf("Labels: got %v, want %v", q.Labels, tt.wantLabels)
				} else {
					for i, want := range tt.wantLabels {
						if q.Labels[i] != want {
							t.Errorf("Labels[%d]: got %q, want %q", i, q.Labels[i], want)
						}
					}
				}
			}
		})
	}
}

func TestParse_ComplexQuery(t *testing.T) {
	q := Parse(`from:alice@example.com to:bob@example.com subject:meeting has:attachment after:2024-01-01 "project report"`)

	if len(q.FromAddrs) != 1 || q.FromAddrs[0] != "alice@example.com" {
		t.Errorf("FromAddrs: got %v", q.FromAddrs)
	}
	if len(q.ToAddrs) != 1 || q.ToAddrs[0] != "bob@example.com" {
		t.Errorf("ToAddrs: got %v", q.ToAddrs)
	}
	if len(q.SubjectTerms) != 1 || q.SubjectTerms[0] != "meeting" {
		t.Errorf("SubjectTerms: got %v", q.SubjectTerms)
	}
	if q.HasAttachment == nil || !*q.HasAttachment {
		t.Errorf("HasAttachment: expected true")
	}
	if q.AfterDate == nil {
		t.Errorf("AfterDate: expected not nil")
	}
	if len(q.TextTerms) != 1 || q.TextTerms[0] != "project report" {
		t.Errorf("TextTerms: got %v", q.TextTerms)
	}
}

func TestQuery_IsEmpty(t *testing.T) {
	tests := []struct {
		query   string
		isEmpty bool
	}{
		{"", true},
		{"from:alice@example.com", false},
		{"hello", false},
		{"has:attachment", false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			q := Parse(tt.query)
			if q.IsEmpty() != tt.isEmpty {
				t.Errorf("IsEmpty(%q): got %v, want %v", tt.query, q.IsEmpty(), tt.isEmpty)
			}
		})
	}
}

func TestParse_QuotedPhraseWithColon(t *testing.T) {
	// Test that quoted phrases containing colons (but not as operators) are
	// treated as text terms, not misclassified as operator:value pairs.
	tests := []struct {
		name     string
		query    string
		wantText []string
		wantFrom []string
	}{
		{
			name:     "quoted phrase with colon",
			query:    `"foo:bar"`,
			wantText: []string{"foo:bar"},
		},
		{
			name:     "quoted phrase with time",
			query:    `"meeting at 10:30"`,
			wantText: []string{"meeting at 10:30"},
		},
		{
			name:     "quoted phrase with URL-like content",
			query:    `"check http://example.com"`,
			wantText: []string{"check http://example.com"},
		},
		{
			name:     "quoted phrase with multiple colons",
			query:    `"a:b:c:d"`,
			wantText: []string{"a:b:c:d"},
		},
		{
			name:     "quoted colon phrase mixed with real operator",
			query:    `from:alice@example.com "subject:not an operator"`,
			wantFrom: []string{"alice@example.com"},
			wantText: []string{"subject:not an operator"},
		},
		{
			name:     "operator followed by quoted colon phrase",
			query:    `"re: meeting notes" from:bob@example.com`,
			wantText: []string{"re: meeting notes"},
			wantFrom: []string{"bob@example.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := Parse(tt.query)

			if len(tt.wantText) > 0 {
				if len(q.TextTerms) != len(tt.wantText) {
					t.Errorf("TextTerms: got %v, want %v", q.TextTerms, tt.wantText)
				} else {
					for i, want := range tt.wantText {
						if q.TextTerms[i] != want {
							t.Errorf("TextTerms[%d]: got %q, want %q", i, q.TextTerms[i], want)
						}
					}
				}
			}

			if len(tt.wantFrom) > 0 {
				if len(q.FromAddrs) != len(tt.wantFrom) {
					t.Errorf("FromAddrs: got %v, want %v", q.FromAddrs, tt.wantFrom)
				} else {
					for i, want := range tt.wantFrom {
						if q.FromAddrs[i] != want {
							t.Errorf("FromAddrs[%d]: got %q, want %q", i, q.FromAddrs[i], want)
						}
					}
				}
			}
		})
	}
}
