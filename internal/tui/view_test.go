package tui

import (
	"strings"
	"sync"
	"testing"

	"github.com/charmbracelet/lipgloss"
	"github.com/muesli/termenv"
)

// ansiStart is the escape sequence prefix found in styled terminal output.
const ansiStart = "\x1b["

// colorProfileMu serializes tests that mutate the global lipgloss color profile.
var colorProfileMu sync.Mutex

// forceColorProfile sets lipgloss to ANSI color output for tests that assert
// on styled output. It acquires colorProfileMu to prevent data races with
// parallel tests and restores the original profile via t.Cleanup.
func forceColorProfile(t *testing.T) {
	t.Helper()
	colorProfileMu.Lock()
	orig := lipgloss.ColorProfile()
	lipgloss.SetColorProfile(termenv.ANSI)
	t.Cleanup(func() {
		lipgloss.SetColorProfile(orig)
		colorProfileMu.Unlock()
	})
}

func stripANSI(s string) string {
	// Simple ANSI stripper for test assertions
	var out strings.Builder
	inEsc := false
	for _, r := range s {
		if r == '\x1b' {
			inEsc = true
			continue
		}
		if inEsc {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inEsc = false
			}
			continue
		}
		out.WriteRune(r)
	}
	return out.String()
}

// assertHighlight checks that applyHighlight produces the expected plain text
// (after stripping ANSI) and, when wantANSI is true, that the raw output
// contains ANSI escape sequences.
func assertHighlight(t *testing.T, text string, terms []string, wantText string, wantANSI bool) {
	t.Helper()
	result := applyHighlight(text, terms)
	stripped := stripANSI(result)
	if stripped != wantText {
		t.Errorf("text content mismatch:\n  got:  %q\n  want: %q", stripped, wantText)
	}
	if wantANSI {
		if !strings.Contains(result, ansiStart) {
			t.Errorf("expected raw output to contain ANSI escapes, got %q", result)
		}
	}
}

// assertHighlightUnchanged checks that applyHighlight returns the input
// unchanged when no terms match.
func assertHighlightUnchanged(t *testing.T, text string, terms []string) {
	t.Helper()
	result := applyHighlight(text, terms)
	if result != text {
		t.Errorf("expected unchanged output for no match, got: %q", result)
	}
}

func TestApplyHighlight(t *testing.T) {
	tests := []struct {
		name     string
		text     string
		terms    []string
		wantText string
		wantANSI bool
	}{
		{"no terms", "hello world", nil, "hello world", false},
		{"single match", "hello world", []string{"world"}, "hello world", true},
		{"case insensitive", "Hello World", []string{"hello"}, "Hello World", true},
		{"multiple terms", "hello world foo", []string{"hello", "foo"}, "hello world foo", true},
		{"overlapping matches", "abcdef", []string{"abcd", "cdef"}, "abcdef", true},
		{"adjacent matches", "aabb", []string{"aa", "bb"}, "aabb", true},
		{"nested matches", "abcdef", []string{"abcdef", "cd"}, "abcdef", true},
		{"no match", "hello world", []string{"xyz"}, "hello world", false},
		{"unicode text", "café résumé", []string{"café"}, "café résumé", true},
		{"unicode case folding", "Ünïcödé", []string{"ünïcödé"}, "Ünïcödé", true},
		{"empty text", "", []string{"hello"}, "", false},
		{"empty term filtered", "hello", []string{""}, "hello", false},
		{"CJK characters", "hello 世界 world", []string{"世界"}, "hello 世界 world", true},
		{"repeated matches", "ababab", []string{"ab"}, "ababab", true},
	}

	forceColorProfile(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertHighlight(t, tt.text, tt.terms, tt.wantText, tt.wantANSI)
		})
	}
}

func TestApplyHighlightProducesOutput(t *testing.T) {
	forceColorProfile(t)

	// Verify that highlighting actually modifies the output when matches exist.
	result := applyHighlight("hello world", []string{"world"})
	if result == "hello world" {
		t.Errorf("expected styled output to differ from input, got unchanged: %q", result)
	}
	if !strings.Contains(result, "world") {
		t.Errorf("highlighted output missing matched text: %q", result)
	}

	// No match should return input unchanged
	assertHighlightUnchanged(t, "hello world", []string{"xyz"})
}
