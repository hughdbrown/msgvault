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
	read := ReadFile(t, path)
	if string(read) != string(content) {
		t.Errorf("expected %q, got %q", content, read)
	}
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

func TestValidateRelativePathRejectsAbsolutePath(t *testing.T) {
	dir := TempDir(t)

	// Use filepath.Abs to get a platform-appropriate absolute path
	absPath, err := filepath.Abs("/some/path.txt")
	if err != nil {
		t.Fatalf("failed to get absolute path: %v", err)
	}

	err = validateRelativePath(dir, absPath)
	if err == nil {
		t.Errorf("expected error for absolute path: %s", absPath)
	}
}

func TestValidateRelativePathRejectsRootedPaths(t *testing.T) {
	dir := TempDir(t)

	// Test rooted path (starts with separator)
	rootedPath := string(filepath.Separator) + "rooted" + string(filepath.Separator) + "path.txt"
	err := validateRelativePath(dir, rootedPath)
	if err == nil {
		t.Errorf("expected error for rooted path: %s", rootedPath)
	}
}

func TestValidateRelativePathRejectsDotDotEscape(t *testing.T) {
	dir := TempDir(t)

	testCases := []string{
		"../escape.txt",
		"subdir/../../escape.txt",
		"..",
	}

	for _, name := range testCases {
		t.Run(name, func(t *testing.T) {
			err := validateRelativePath(dir, name)
			if err == nil {
				t.Errorf("expected error for path: %s", name)
			}
		})
	}
}

func TestValidateRelativePathAllowsValidPaths(t *testing.T) {
	dir := TempDir(t)

	validPaths := []string{
		"simple.txt",
		"subdir/file.txt",
		"a/b/c/deep.txt",
		"file-with-dots.test.txt",
		"./current.txt",
	}

	for _, name := range validPaths {
		t.Run(name, func(t *testing.T) {
			err := validateRelativePath(dir, name)
			if err != nil {
				t.Errorf("unexpected error for path %s: %v", name, err)
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
