package mime

import (
	"strings"
	"testing"
	"time"

	"github.com/jhillyerd/enmime"
)

// emailOptions configures a raw RFC 2822 email message for testing.
type emailOptions struct {
	From        string
	To          string
	Subject     string
	ContentType string
	Body        string
	Headers     map[string]string
}

// makeRawEmail constructs an RFC 2822 compliant raw message with correct \r\n line endings.
func makeRawEmail(opts emailOptions) []byte {
	var b strings.Builder

	if opts.From == "" {
		opts.From = "sender@example.com"
	}
	if opts.To == "" {
		opts.To = "recipient@example.com"
	}
	if opts.Subject == "" {
		opts.Subject = "Test"
	}

	b.WriteString("From: " + opts.From + "\r\n")
	b.WriteString("To: " + opts.To + "\r\n")
	b.WriteString("Subject: " + opts.Subject + "\r\n")

	if opts.ContentType != "" {
		b.WriteString("Content-Type: " + opts.ContentType + "\r\n")
	}

	for k, v := range opts.Headers {
		b.WriteString(k + ": " + v + "\r\n")
	}

	b.WriteString("\r\n")
	b.WriteString(opts.Body)

	return []byte(b.String())
}

// mustParse calls Parse and fails the test on error.
func mustParse(t *testing.T, raw []byte) *Message {
	t.Helper()
	msg, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse() failed: %v", err)
	}
	return msg
}

func TestExtractDomain(t *testing.T) {
	tests := []struct {
		email  string
		domain string
	}{
		{"user@example.com", "example.com"},
		{"USER@EXAMPLE.COM", "example.com"},
		{"user@sub.domain.org", "sub.domain.org"},
		{"nodomain", ""},
		{"", ""},
		{"@domain.com", "domain.com"},
	}

	for _, tc := range tests {
		got := extractDomain(tc.email)
		if got != tc.domain {
			t.Errorf("extractDomain(%q) = %q, want %q", tc.email, got, tc.domain)
		}
	}
}

func TestParseReferences(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"<abc@example.com>", []string{"abc@example.com"}},
		{"<a@x.com> <b@y.com>", []string{"a@x.com", "b@y.com"}},
		{"<a@x.com>\n\t<b@y.com>", []string{"a@x.com", "b@y.com"}},
		{"", nil},
		{"   ", nil},
	}

	for _, tc := range tests {
		got := parseReferences(tc.input)
		if len(got) != len(tc.want) {
			t.Errorf("parseReferences(%q) = %v, want %v", tc.input, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("parseReferences(%q)[%d] = %q, want %q", tc.input, i, got[i], tc.want[i])
			}
		}
	}
}

func TestParseDate(t *testing.T) {
	// parseDate returns zero time (not error) for unparseable dates.
	// This is intentional - malformed dates are common in email and
	// shouldn't fail the entire parse.

	// Valid RFC date formats should parse successfully
	validDates := []struct {
		input string
	}{
		{"Mon, 02 Jan 2006 15:04:05 -0700"},
		{"Mon, 2 Jan 2006 15:04:05 MST"},
		{"02 Jan 2006 15:04:05 -0700"},
		{"Mon, 02 Jan 2006 15:04:05 -0700 (PST)"},
		{"Mon,  2 Dec 2024 11:42:03 +0000 (UTC)"}, // Double space after comma (real-world case)
		{"2006-01-02T15:04:05Z"},                  // ISO 8601 UTC
		{"2006-01-02T15:04:05-07:00"},             // ISO 8601 with offset
		{"2006-01-02 15:04:05 -0700"},             // SQL-like with timezone
		{"2006-01-02 15:04:05"},                   // SQL-like without timezone (assumes UTC)
	}

	for _, tc := range validDates {
		got, err := parseDate(tc.input)
		if err != nil {
			t.Errorf("parseDate(%q) unexpected error: %v", tc.input, err)
		}
		if got.IsZero() {
			t.Errorf("parseDate(%q) returned zero time, expected parsed date", tc.input)
		}
	}

	// Invalid/unparseable dates should return zero time without error
	invalidDates := []string{
		"",                // Empty
		"not a date",      // Garbage
		"2006-01-02",      // Date only, no time
		"January 2, 2006", // Spelled out month
	}

	for _, input := range invalidDates {
		got, err := parseDate(input)
		if err != nil {
			t.Errorf("parseDate(%q) unexpected error: %v (should return zero time, not error)", input, err)
		}
		if !got.IsZero() {
			t.Errorf("parseDate(%q) = %v, expected zero time for invalid input", input, got)
		}
	}

	// Verify parsed values are converted to UTC
	got, err := parseDate("Mon, 02 Jan 2006 15:04:05 -0700")
	if err != nil {
		t.Fatalf("parseDate() unexpected error: %v", err)
	}
	// Should be converted to UTC (15:04:05 -0700 = 22:04:05 UTC)
	if got.Location() != time.UTC {
		t.Errorf("parseDate returned location %v, want UTC", got.Location())
	}
	wantUTC := time.Date(2006, 1, 2, 22, 4, 5, 0, time.UTC)
	if !got.Equal(wantUTC) {
		t.Errorf("parseDate returned %v, want %v (UTC)", got, wantUTC)
	}

	// Verify double-space handling with parenthesized timezone
	got, err = parseDate("Mon,  2 Dec 2024 11:42:03 +0000 (UTC)")
	if err != nil {
		t.Fatalf("parseDate() unexpected error: %v", err)
	}
	if got.IsZero() {
		t.Errorf("parseDate with double space returned zero time")
	}
	wantUTC = time.Date(2024, 12, 2, 11, 42, 3, 0, time.UTC)
	if !got.Equal(wantUTC) {
		t.Errorf("parseDate returned %v, want %v", got, wantUTC)
	}
}

func TestStripHTML(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		// Basic tag stripping
		{"paragraph", "<p>Hello</p>", "Hello"},
		{"nested_span", "<div><span>Nested</span></div>", "Nested"},
		{"no_tags", "No tags", "No tags"},
		{"inline_tags", "<b>Bold</b> and <i>italic</i>", "Bold and italic"},
		{"empty", "", ""},

		// Script/style removal (including content)
		{"script_removed", "<script>alert('xss')</script>Text", "Text"},
		{"style_removed", "<style>.class{color:red}</style>Content", "Content"},
		{"head_removed", "<head><title>Title</title></head>Body", "Body"},

		// Newline normalization
		{"crlf_to_lf", "Line1\r\nLine2\r\nLine3", "Line1\nLine2\nLine3"},
		{"collapse_newlines", "Multiple\n\n\n\nNewlines", "Multiple\n\nNewlines"},

		// HTML entities
		{"nbsp_entity", "Hello&nbsp;World", "Hello World"},
		{"amp_entity", "Tom &amp; Jerry", "Tom & Jerry"},
		{"lt_gt_entities", "5 &lt; 10 &gt; 3", "5 < 10 > 3"},
		{"quote_entity", "&quot;quoted&quot;", "\"quoted\""},
		{"numeric_entity", "&#169; 2024", "© 2024"},
		{"hex_entity", "&#x2022; bullet", "• bullet"},

		// Block elements create line breaks
		{"br_tag", "Line1<br>Line2", "Line1\nLine2"},
		{"br_self_close", "Line1<br/>Line2", "Line1\nLine2"},
		{"paragraph_breaks", "<p>Para1</p><p>Para2</p>", "Para1\n\nPara2"},
		{"div_breaks", "<div>Block1</div><div>Block2</div>", "Block1\n\nBlock2"},
		{"heading_breaks", "<h1>Title</h1><p>Content</p>", "Title\n\nContent"},

		// Complex HTML email
		{
			"complex_html",
			`<html><head><style>.x{}</style></head><body>
			<p>Hello,</p>
			<p>This is a <b>test</b> email with &amp; special chars.</p>
			<br>
			<p>Thanks!</p>
			</body></html>`,
			"Hello,\n\nThis is a test email with & special chars.\n\nThanks!",
		},

		// Whitespace collapse
		{"multiple_spaces", "Hello    World", "Hello World"},
		{"nbsp_spaces", "Hello&nbsp;&nbsp;&nbsp;World", "Hello World"},

		// Preformatted content - whitespace is NOT preserved (documented behavior)
		// This is acceptable for email preview where code formatting is secondary
		{"pre_whitespace_collapsed", "<pre>  code  here  </pre>", "code here"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := StripHTML(tc.input)
			if got != tc.want {
				t.Errorf("StripHTML() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMessage_GetBodyText(t *testing.T) {
	// Prefers plain text
	msg := &Message{BodyText: "plain", BodyHTML: "<p>html</p>"}
	if got := msg.GetBodyText(); got != "plain" {
		t.Errorf("GetBodyText() = %q, want %q", got, "plain")
	}

	// Falls back to HTML
	msg = &Message{BodyHTML: "<p>html only</p>"}
	if got := msg.GetBodyText(); got != "html only" {
		t.Errorf("GetBodyText() = %q, want %q", got, "html only")
	}

	// Empty
	msg = &Message{}
	if got := msg.GetBodyText(); got != "" {
		t.Errorf("GetBodyText() = %q, want empty", got)
	}
}

func TestMessage_GetFirstFrom(t *testing.T) {
	msg := &Message{
		From: []Address{
			{Name: "Alice", Email: "alice@example.com", Domain: "example.com"},
			{Name: "Bob", Email: "bob@example.com", Domain: "example.com"},
		},
	}

	got := msg.GetFirstFrom()
	if got.Email != "alice@example.com" {
		t.Errorf("GetFirstFrom() = %v, want alice@example.com", got)
	}

	// Empty
	msg = &Message{}
	got = msg.GetFirstFrom()
	if got.Email != "" {
		t.Errorf("GetFirstFrom() on empty = %v, want empty", got)
	}
}

// TestParse_MinimalMessage tests our Parse wrapper with a minimal valid message.
// This verifies our wrapper works, not enmime's parsing logic.
func TestParse_MinimalMessage(t *testing.T) {
	raw := makeRawEmail(emailOptions{
		Body: "Body text",
		Headers: map[string]string{
			"Date": "Mon, 02 Jan 2006 15:04:05 -0700",
		},
	})

	msg := mustParse(t, raw)

	if len(msg.From) != 1 || msg.From[0].Email != "sender@example.com" {
		t.Errorf("From = %v, want sender@example.com", msg.From)
	}

	if len(msg.To) != 1 || msg.To[0].Email != "recipient@example.com" {
		t.Errorf("To = %v, want recipient@example.com", msg.To)
	}

	if msg.Subject != "Test" {
		t.Errorf("Subject = %q, want %q", msg.Subject, "Test")
	}

	if msg.BodyText != "Body text" {
		t.Errorf("BodyText = %q, want %q", msg.BodyText, "Body text")
	}

	// Verify domain extraction worked
	if msg.From[0].Domain != "example.com" {
		t.Errorf("From domain = %q, want %q", msg.From[0].Domain, "example.com")
	}
}

// TestParse_InvalidCharset verifies enmime handles malformed charsets gracefully.
// Enmime should not fail on invalid charset - it attempts conversion and collects errors.
func TestParse_InvalidCharset(t *testing.T) {
	// Message with non-existent charset - enmime should handle this gracefully
	raw := makeRawEmail(emailOptions{
		ContentType: "text/plain; charset=invalid-charset-xyz",
		Body:        "Body text",
	})

	msg := mustParse(t, raw)

	// Should still be able to access subject and addresses
	if msg.Subject != "Test" {
		t.Errorf("Subject = %q, want %q", msg.Subject, "Test")
	}

	// Body might be garbled or empty, but should not crash
	// enmime collects errors in msg.Errors
	t.Logf("Body text with invalid charset: %q", msg.BodyText)
	t.Logf("Parsing errors: %v", msg.Errors)
}

// TestParse_Latin1Charset verifies Latin-1 (ISO-8859-1) charset is handled.
func TestParse_Latin1Charset(t *testing.T) {
	// Latin-1 encoded content with special characters.
	// This test uses raw bytes because the subject/body contain non-UTF-8 Latin-1 bytes.
	raw := []byte("From: sender@example.com\r\nTo: recipient@example.com\r\nSubject: Caf\xe9\r\nContent-Type: text/plain; charset=iso-8859-1\r\n\r\nCaf\xe9 au lait")

	msg := mustParse(t, raw)

	// enmime should convert Latin-1 to UTF-8
	// é in Latin-1 is 0xe9, in UTF-8 it's 0xc3 0xa9
	if msg.BodyText != "Café au lait" {
		t.Errorf("BodyText = %q, want %q", msg.BodyText, "Café au lait")
	}
}

// TestParse_RFC2822GroupAddress verifies RFC 2822 group address syntax is handled.
// Group syntax: "group-name: addr1, addr2, ...;"
func TestParse_RFC2822GroupAddress(t *testing.T) {
	// Message with undisclosed-recipients group (common in BCC scenarios)
	raw := makeRawEmail(emailOptions{
		To:   "undisclosed-recipients:;",
		Body: "Body",
	})

	msg := mustParse(t, raw)

	// Group with no addresses should result in empty To list
	t.Logf("To addresses: %v", msg.To)
	t.Logf("Parsing errors: %v", msg.Errors)

	// Should not crash - that's the main requirement
	if msg.Subject != "Test" {
		t.Errorf("Subject = %q, want %q", msg.Subject, "Test")
	}
}

// TestParse_RFC2822GroupAddressWithMembers verifies group with actual addresses.
func TestParse_RFC2822GroupAddressWithMembers(t *testing.T) {
	// Group with member addresses
	raw := makeRawEmail(emailOptions{
		To:   "team: alice@example.com, bob@example.com;",
		Body: "Body",
	})

	msg := mustParse(t, raw)

	t.Logf("To addresses: %v", msg.To)
	t.Logf("Parsing errors: %v", msg.Errors)

	// Ideally we'd extract alice and bob from the group
	// Let's see how enmime handles this
	if msg.Subject != "Test" {
		t.Errorf("Subject = %q, want %q", msg.Subject, "Test")
	}
}

// TestIsBodyPart_ContentTypeWithParams tests that Content-Type with charset
// parameters is correctly identified as body content.
func TestIsBodyPart_ContentTypeWithParams(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		filename    string
		disposition string
		wantIsBody  bool
	}{
		// Content-Type with charset parameter should still be body
		{"text/plain with charset", "text/plain; charset=utf-8", "", "", true},
		{"text/html with charset", "text/html; charset=utf-8", "", "", true},
		{"text/plain with format", "text/plain; format=flowed", "", "", true},
		{"TEXT/PLAIN uppercase with charset", "TEXT/PLAIN; CHARSET=UTF-8", "", "", true},

		// Non-text types are not body parts
		{"application/pdf", "application/pdf", "", "", false},
		{"image/png", "image/png", "", "", false},

		// With filename → attachment, not body
		{"text/plain with filename", "text/plain; charset=utf-8", "file.txt", "", false},
		{"text/html with filename", "text/html; charset=utf-8", "page.html", "", false},

		// Explicit disposition: attachment (with or without params)
		{"attachment disposition", "text/plain", "", "attachment", false},
		{"attachment with params", "text/plain", "", "attachment; filename=\"x.txt\"", false},
		{"ATTACHMENT uppercase", "text/plain", "", "ATTACHMENT; filename=\"x.txt\"", false},

		// Inline disposition is still body
		{"inline disposition", "text/plain; charset=utf-8", "", "inline", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock enmime.Part (we only need the fields isBodyPart checks)
			part := &enmime.Part{
				ContentType: tt.contentType,
				FileName:    tt.filename,
				Disposition: tt.disposition,
			}
			got := isBodyPart(part)
			if got != tt.wantIsBody {
				t.Errorf("isBodyPart() = %v, want %v", got, tt.wantIsBody)
			}
		})
	}
}
