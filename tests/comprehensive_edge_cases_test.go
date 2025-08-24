package tests

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestReaddirErrorConditions tests various error conditions in Readdir
func TestReaddirErrorConditions(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: Readdir on file instead of directory
	t.Run("ReaddirOnFile", func(t *testing.T) {
		w := fs.NewWriter("file.txt")
		w.Write([]byte("content"))
		w.Close()

		f, err := fs.Open("file.txt")
		if err != nil {
			t.Fatal(err)
		}

		if dirFile, ok := f.(interface {
			Readdir(int) ([]os.FileInfo, error)
		}); ok {
			_, err = dirFile.Readdir(0)
			if err == nil || err.Error() != "not a directory" {
				t.Fatalf("expected 'not a directory', got %v", err)
			}
		}
	})

	// Test 2: Readdir with path normalization
	t.Run("PathNormalization", func(t *testing.T) {
		// Create files in a directory
		w1 := fs.NewWriter("testdir/file1.txt")
		w1.Write([]byte("content1"))
		w1.Close()

		w2 := fs.NewWriter("testdir/subdir/file2.txt")
		w2.Write([]byte("content2"))
		w2.Close()

		// Open directory without trailing slash
		f, err := fs.Open("testdir")
		if err != nil {
			t.Fatal(err)
		}

		if dirFile, ok := f.(interface {
			Readdir(int) ([]os.FileInfo, error)
		}); ok {
			infos, err := dirFile.Readdir(0)
			if err != nil {
				t.Fatal(err)
			}
			// Should find file1.txt and subdir
			if len(infos) < 1 {
				t.Fatal("expected at least 1 entry")
			}
		}
	})

	// Test 3: Readdir with limit
	t.Run("ReaddirWithLimit", func(t *testing.T) {
		// Create multiple files
		for i := 0; i < 5; i++ {
			w := fs.NewWriter(fmt.Sprintf("limitdir/file%d.txt", i))
			w.Write([]byte("content"))
			w.Close()
		}

		f, err := fs.Open("limitdir")
		if err != nil {
			t.Fatal(err)
		}

		if dirFile, ok := f.(interface {
			Readdir(int) ([]os.FileInfo, error)
		}); ok {
			// Read only 2 entries
			infos, err := dirFile.Readdir(2)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			if len(infos) != 2 {
				t.Fatalf("expected 2 entries, got %d", len(infos))
			}

			// Read next 2
			infos, err = dirFile.Readdir(2)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			if len(infos) != 2 {
				t.Fatalf("expected 2 entries, got %d", len(infos))
			}

			// Read remaining
			infos, err = dirFile.Readdir(2)
			if err != io.EOF {
				t.Fatalf("expected io.EOF, got %v", err)
			}
			if len(infos) != 1 {
				t.Fatalf("expected 1 entry, got %d", len(infos))
			}
		}
	})

	// Test 4: Readdir on nested paths
	t.Run("NestedPaths", func(t *testing.T) {
		// Create deeply nested structure
		w := fs.NewWriter("a/b/c/d/file.txt")
		w.Write([]byte("deep"))
		w.Close()

		// Open intermediate directory
		f, err := fs.Open("a/b")
		if err != nil {
			t.Fatal(err)
		}

		if dirFile, ok := f.(interface {
			Readdir(int) ([]os.FileInfo, error)
		}); ok {
			infos, err := dirFile.Readdir(0)
			if err != nil {
				t.Fatal(err)
			}
			// Should find directory c
			found := false
			for _, info := range infos {
				if info.Name() == "c" && info.IsDir() {
					found = true
					break
				}
			}
			if !found {
				t.Fatal("expected to find directory 'c'")
			}
		}
	})
}

// TestReadDirCleanName tests ReadDir with names that need cleaning
func TestReadDirCleanName(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create files with various path formats
	w1 := fs.NewWriter("cleandir/file.txt")
	w1.Write([]byte("content"))
	w1.Close()

	// Manually insert a directory with trailing slash
	_, err = db.Exec("INSERT OR REPLACE INTO file_metadata (path, type) VALUES (?, ?)",
		"cleandir/subdir/", "dir")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("cleandir")
	if err != nil {
		t.Fatal(err)
	}

	if dirFile, ok := f.(interface {
		ReadDir(int) ([]os.DirEntry, error)
	}); ok {
		entries, err := dirFile.ReadDir(0)
		if err != nil {
			t.Fatal(err)
		}

		// Check that names are properly cleaned
		for _, entry := range entries {
			name := entry.Name()
			if name == "" || name == "/" || strings.HasSuffix(name, "/") {
				t.Fatalf("invalid entry name: %q", name)
			}
		}
	}
}

// TestCreateFileInfoEdgeCases tests edge cases in createFileInfo
func TestCreateFileInfoEdgeCases(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: Empty file
	t.Run("EmptyFile", func(t *testing.T) {
		w := fs.NewWriter("empty.txt")
		w.Close() // No content

		f, err := fs.Open("empty.txt")
		if err != nil {
			t.Fatal(err)
		}

		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}

		if info.Size() != 0 {
			t.Fatalf("expected size 0, got %d", info.Size())
		}
	})

	// Test 2: Root directory
	t.Run("RootDirectory", func(t *testing.T) {
		f, err := fs.Open("")
		if err != nil {
			t.Fatal(err)
		}

		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}

		if !info.IsDir() {
			t.Fatal("root should be a directory")
		}
		if info.Name() != "/" {
			t.Fatalf("expected name '/', got %s", info.Name())
		}
	})

	// Test 3: Non-existent path
	t.Run("NonExistentPath", func(t *testing.T) {
		_, err := fs.Open("does/not/exist")
		if err == nil {
			t.Fatal("expected error for non-existent path")
		}
	})
}

// TestGetTotalSizeCornerCases tests corner cases in getTotalSize
func TestGetTotalSizeCornerCases(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Test file with metadata but no fragments
	t.Run("MetadataNoFragments", func(t *testing.T) {
		// Insert file metadata directly
		_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)",
			"metadata_only.txt", "file")
		if err != nil {
			t.Fatal(err)
		}

		f, err := fs.Open("metadata_only.txt")
		if err != nil {
			t.Fatal(err)
		}

		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}

		if info.Size() != 0 {
			t.Fatalf("expected size 0, got %d", info.Size())
		}
	})
}

// TestReadMoreEdgeCases tests edge cases in Read
func TestReadMoreEdgeCases(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Test reading at exact EOF
	t.Run("ReadAtEOF", func(t *testing.T) {
		w := fs.NewWriter("eof_test.txt")
		w.Write([]byte("1234"))
		w.Close()

		f, err := fs.Open("eof_test.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Read all content
		buf := make([]byte, 4)
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n != 4 {
			t.Fatalf("expected 4 bytes, got %d", n)
		}

		// Now at EOF, next read should return EOF immediately
		n, err = f.Read(buf)
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
		if n != 0 {
			t.Fatalf("expected 0 bytes, got %d", n)
		}
	})

	// Test reading with various buffer sizes
	t.Run("VariousBufferSizes", func(t *testing.T) {
		content := make([]byte, 10000) // Larger than fragment size
		for i := range content {
			content[i] = byte(i % 256)
		}

		w := fs.NewWriter("large.txt")
		w.Write(content)
		w.Close()

		f, err := fs.Open("large.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Read with small buffer
		buf := make([]byte, 100)
		totalRead := 0
		for {
			n, err := f.Read(buf)
			totalRead += n
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}

		if totalRead != len(content) {
			t.Fatalf("expected %d bytes, got %d", len(content), totalRead)
		}
	})
}
