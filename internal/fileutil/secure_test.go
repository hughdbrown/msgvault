package fileutil

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestSecureWriteFile(t *testing.T) {
	tests := []struct {
		name string
		perm os.FileMode
	}{
		{"owner_only_0600", 0600},
		{"permissive_0644", 0644},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "testfile")
			data := []byte("hello secure world")

			if err := SecureWriteFile(path, data, tt.perm); err != nil {
				t.Fatalf("SecureWriteFile: %v", err)
			}

			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if string(got) != string(data) {
				t.Errorf("content = %q, want %q", got, data)
			}

			if runtime.GOOS != "windows" {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatalf("Stat: %v", err)
				}
				if got := info.Mode().Perm(); got != tt.perm {
					t.Errorf("perm = %04o, want %04o", got, tt.perm)
				}
			}
		})
	}
}

func TestSecureMkdirAll(t *testing.T) {
	tests := []struct {
		name string
		perm os.FileMode
	}{
		{"owner_only_0700", 0700},
		{"permissive_0755", 0755},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "a", "b", "c")

			if err := SecureMkdirAll(path, tt.perm); err != nil {
				t.Fatalf("SecureMkdirAll: %v", err)
			}

			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("Stat: %v", err)
			}
			if !info.IsDir() {
				t.Error("expected directory")
			}

			if runtime.GOOS != "windows" {
				if got := info.Mode().Perm(); got != tt.perm {
					t.Errorf("perm = %04o, want %04o", got, tt.perm)
				}
			}
		})
	}
}

func TestSecureChmod(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testfile")

	if err := os.WriteFile(path, []byte("data"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := SecureChmod(path, 0600); err != nil {
		t.Fatalf("SecureChmod: %v", err)
	}

	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat: %v", err)
		}
		if got := info.Mode().Perm(); got != 0600 {
			t.Errorf("perm = %04o, want 0600", got)
		}
	}
}

func TestSecureOpenFile(t *testing.T) {
	tests := []struct {
		name string
		perm os.FileMode
	}{
		{"owner_only_0600", 0600},
		{"permissive_0644", 0644},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "testfile")

			f, err := SecureOpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, tt.perm)
			if err != nil {
				t.Fatalf("SecureOpenFile: %v", err)
			}
			if _, err := f.Write([]byte("data")); err != nil {
				f.Close()
				t.Fatalf("Write: %v", err)
			}
			if err := f.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if string(got) != "data" {
				t.Errorf("content = %q, want %q", got, "data")
			}

			if runtime.GOOS != "windows" {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatalf("Stat: %v", err)
				}
				if got := info.Mode().Perm(); got != tt.perm {
					t.Errorf("perm = %04o, want %04o", got, tt.perm)
				}
			}
		})
	}
}

func TestSecureWriteFile_NonexistentParent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "no", "such", "dir", "file")

	err := SecureWriteFile(path, []byte("data"), 0600)
	if err == nil {
		t.Fatal("expected error for nonexistent parent dir")
	}
}

func TestSecureOpenFile_ReadOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "testfile")
	if err := os.WriteFile(path, []byte("existing"), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	f, err := SecureOpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("SecureOpenFile read-only: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 100)
	n, err := f.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != "existing" {
		t.Errorf("content = %q, want %q", buf[:n], "existing")
	}
}
