package testutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNewTestStore(t *testing.T) {
	st := NewTestStore(t)

	// Verify store is usable
	stats, err := st.GetStats()
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}

	// Fresh database should have no messages
	if stats.MessageCount != 0 {
		t.Errorf("expected 0 messages, got %d", stats.MessageCount)
	}
}

func TestTempDir(t *testing.T) {
	dir := TempDir(t)

	// Verify directory exists
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat temp dir: %v", err)
	}
	if !info.IsDir() {
		t.Errorf("expected directory, got file")
	}
}

func TestWriteAndReadFile(t *testing.T) {
	dir := TempDir(t)
	content := []byte("hello world")

	path := WriteFile(t, dir, "test.txt", content)

	// Verify file was written
	MustExist(t, path)

	// Verify content
	AssertFileContent(t, path, string(content))
}

func TestWriteFileSubdir(t *testing.T) {
	dir := TempDir(t)
	content := []byte("nested content")

	path := WriteFile(t, dir, "subdir/nested/test.txt", content)

	MustExist(t, path)
	MustExist(t, filepath.Join(dir, "subdir", "nested"))
}

func TestMustExist(t *testing.T) {
	dir := TempDir(t)
	path := WriteFile(t, dir, "exists.txt", []byte("data"))

	// Should not panic
	MustExist(t, path)
	MustExist(t, dir)
}

func TestMustNotExist(t *testing.T) {
	dir := TempDir(t)

	// Should not panic for non-existent path
	MustNotExist(t, filepath.Join(dir, "does-not-exist.txt"))
}

func TestValidateRelativePath(t *testing.T) {
	dir := TempDir(t)

	// Use filepath.Abs to get a platform-appropriate absolute path
	absPath, err := filepath.Abs("/some/path.txt")
	if err != nil {
		t.Fatalf("failed to get absolute path: %v", err)
	}

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name:    "absolute path",
			path:    absPath,
			wantErr: true,
		},
		{
			name:    "rooted path",
			path:    string(filepath.Separator) + "rooted" + string(filepath.Separator) + "path.txt",
			wantErr: true,
		},
		{
			name:    "escape dot dot",
			path:    "../escape.txt",
			wantErr: true,
		},
		{
			name:    "escape dot dot nested",
			path:    "subdir/../../escape.txt",
			wantErr: true,
		},
		{
			name:    "escape just dot dot",
			path:    "..",
			wantErr: true,
		},
		{
			name:    "valid simple",
			path:    "simple.txt",
			wantErr: false,
		},
		{
			name:    "valid subdir",
			path:    "subdir/file.txt",
			wantErr: false,
		},
		{
			name:    "valid deep",
			path:    "a/b/c/deep.txt",
			wantErr: false,
		},
		{
			name:    "valid with dots",
			path:    "file-with-dots.test.txt",
			wantErr: false,
		},
		{
			name:    "valid current dir",
			path:    "./current.txt",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateRelativePath(dir, tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateRelativePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestWriteFileWithValidPaths(t *testing.T) {
	dir := TempDir(t)

	// These should all succeed
	validPaths := []string{
		"simple.txt",
		"subdir/file.txt",
		"a/b/c/deep.txt",
	}

	for _, name := range validPaths {
		t.Run(name, func(t *testing.T) {
			path := WriteFile(t, dir, name, []byte("data"))
			MustExist(t, path)
		})
	}
}
