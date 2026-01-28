package cmd

import (
	"testing"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
)

func TestDetectAndDecode_Windows1252(t *testing.T) {
	// Windows-1252 specific characters: smart quotes (0x91-0x94), en/em dash (0x96, 0x97)
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "smart single quote (apostrophe)",
			input:    []byte("Rand\x92s Opponent"), // 0x92 = right single quote U+2019
			expected: "Rand\u2019s Opponent",
		},
		{
			name:     "en dash",
			input:    []byte("Limited Time Only \x96 50 Percent"), // 0x96 = en dash U+2013
			expected: "Limited Time Only \u2013 50 Percent",
		},
		{
			name:     "em dash",
			input:    []byte("Costco Travel\x97Exclusive"), // 0x97 = em dash U+2014
			expected: "Costco Travel\u2014Exclusive",
		},
		{
			name:     "trademark symbol",
			input:    []byte("Craftsman\xae Tools"), // 0xAE = ®
			expected: "Craftsman® Tools",
		},
		{
			name:     "registered trademark in Windows-1252",
			input:    []byte("Windows\xae 7"), // 0xAE = ®
			expected: "Windows® 7",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := detectAndDecode(tt.input)
			if err != nil {
				t.Fatalf("detectAndDecode() error = %v", err)
			}
			if result != tt.expected {
				t.Errorf("detectAndDecode() = %q, want %q", result, tt.expected)
			}
			if !utf8.ValidString(result) {
				t.Errorf("detectAndDecode() result is not valid UTF-8")
			}
		})
	}
}

func TestDetectAndDecode_Latin1(t *testing.T) {
	// ISO-8859-1 (Latin-1) characters
	tests := []struct {
		name     string
		input    []byte
		expected string
	}{
		{
			name:     "o with acute accent",
			input:    []byte("Mir\xf3 - Picasso"), // 0xF3 = ó
			expected: "Miró - Picasso",
		},
		{
			name:     "c with cedilla",
			input:    []byte("Gar\xe7on"), // 0xE7 = ç
			expected: "Garçon",
		},
		{
			name:     "u with umlaut",
			input:    []byte("M\xfcnchen"), // 0xFC = ü
			expected: "München",
		},
		{
			name:     "n with tilde",
			input:    []byte("Espa\xf1a"), // 0xF1 = ñ
			expected: "España",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := detectAndDecode(tt.input)
			if err != nil {
				t.Fatalf("detectAndDecode() error = %v", err)
			}
			if result != tt.expected {
				t.Errorf("detectAndDecode() = %q, want %q", result, tt.expected)
			}
			if !utf8.ValidString(result) {
				t.Errorf("detectAndDecode() result is not valid UTF-8")
			}
		})
	}
}

func TestDetectAndDecode_AsianEncodings(t *testing.T) {
	// For short Asian text samples, automatic charset detection is ambiguous
	// since the same bytes can be valid in multiple encodings.
	// The key requirement is that the output is valid UTF-8.
	tests := []struct {
		name  string
		input []byte
	}{
		{
			name:  "Shift-JIS Japanese",
			input: []byte{0x82, 0xb1, 0x82, 0xf1, 0x82, 0xc9, 0x82, 0xbf, 0x82, 0xcd}, // "こんにちは"
		},
		{
			name:  "GBK Simplified Chinese",
			input: []byte{0xc4, 0xe3, 0xba, 0xc3}, // "你好"
		},
		{
			name:  "Big5 Traditional Chinese",
			input: []byte{0xa9, 0x6f, 0xa6, 0x6e}, // "你好"
		},
		{
			name:  "EUC-KR Korean",
			input: []byte{0xbe, 0xc8, 0xb3, 0xe7}, // "안녕"
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := detectAndDecode(tt.input)
			if err != nil {
				t.Fatalf("detectAndDecode() error = %v", err)
			}
			if !utf8.ValidString(result) {
				t.Errorf("detectAndDecode() result is not valid UTF-8: %q", result)
			}
			// Result should not be empty
			if len(result) == 0 {
				t.Errorf("detectAndDecode() returned empty string")
			}
		})
	}
}

func TestDetectAndDecode_AlreadyUTF8(t *testing.T) {
	// Already valid UTF-8 should pass through
	input := []byte("Hello, 世界! Привет!")
	expected := "Hello, 世界! Привет!"

	result, err := detectAndDecode(input)
	if err != nil {
		t.Fatalf("detectAndDecode() error = %v", err)
	}
	if result != expected {
		t.Errorf("detectAndDecode() = %q, want %q", result, expected)
	}
}

func TestGetEncodingByName(t *testing.T) {
	tests := []struct {
		name     string
		charset  string
		expected interface{}
	}{
		{"Windows-1252 standard", "windows-1252", charmap.Windows1252},
		{"Windows-1252 CP1252", "CP1252", charmap.Windows1252},
		{"ISO-8859-1 standard", "ISO-8859-1", charmap.ISO8859_1},
		{"ISO-8859-1 lowercase", "iso-8859-1", charmap.ISO8859_1},
		{"ISO-8859-1 latin1", "latin1", charmap.ISO8859_1},
		{"Shift_JIS standard", "Shift_JIS", japanese.ShiftJIS},
		{"Shift_JIS lowercase", "shift_jis", japanese.ShiftJIS},
		{"EUC-JP standard", "EUC-JP", japanese.EUCJP},
		{"EUC-KR standard", "EUC-KR", korean.EUCKR},
		{"GBK standard", "GBK", simplifiedchinese.GBK},
		{"GB2312 maps to GBK", "GB2312", simplifiedchinese.GBK},
		{"Big5 standard", "Big5", traditionalchinese.Big5},
		{"KOI8-R standard", "KOI8-R", charmap.KOI8R},
		{"Unknown returns nil", "unknown-charset", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getEncodingByName(tt.charset)
			if result != tt.expected {
				t.Errorf("getEncodingByName(%q) = %v, want %v", tt.charset, result, tt.expected)
			}
		})
	}
}

func TestSanitizeUTF8(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "valid UTF-8 unchanged",
			input:    "Hello, 世界!",
			expected: "Hello, 世界!",
		},
		{
			name:     "invalid byte replaced",
			input:    "Hello\x80World",
			expected: "Hello�World",
		},
		{
			name:     "multiple invalid bytes",
			input:    "Test\x80\x81\x82String",
			expected: "Test���String",
		},
		{
			name:     "truncated UTF-8 sequence",
			input:    "Hello\xc3", // Incomplete UTF-8 sequence
			expected: "Hello�",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeUTF8(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeUTF8(%q) = %q, want %q", tt.input, result, tt.expected)
			}
			if !utf8.ValidString(result) {
				t.Errorf("sanitizeUTF8() result is not valid UTF-8")
			}
		})
	}
}

