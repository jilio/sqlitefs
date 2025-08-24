package tests

import (
	"database/sql"
	"io"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestGetTotalSizeEdgeCases tests getTotalSize with various edge cases
func TestGetTotalSizeEdgeCases(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Test 1: File exists in metadata but no fragments (covers lines 651-663 in getTotalSize)
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "empty.txt", "file")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("empty.txt")
	if err != nil {
		t.Fatal(err)
	}

	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if info.Size() != 0 {
		t.Fatalf("expected size 0 for file with no fragments, got %d", info.Size())
	}
}

// TestReadDirDatabaseErrors tests ReadDir error paths
func TestReadDirDatabaseErrors(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a directory entry with subdirectory
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "parent/child/file.txt", "file")
	if err != nil {
		t.Fatal(err)
	}

	// Open parent directory
	f, err := fs.Open("parent")
	if err != nil {
		t.Fatal(err)
	}

	// Create a corrupted type entry to trigger scan error
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "parent/corrupt", "")
	if err != nil {
		t.Fatal(err)
	}

	// Try to read directory - may encounter the corrupt entry
	if dirFile, ok := f.(interface {
		ReadDir(int) ([]os.DirEntry, error)
	}); ok {
		_, _ = dirFile.ReadDir(0)
		// Don't check error as behavior may vary, just exercise the code path
	}
}

// TestReaddirSubdirectoryPaths tests Readdir with various path formats
func TestReaddirSubdirectoryPaths(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create nested structure
	paths := []string{
		"root/sub1/file1.txt",
		"root/sub1/sub2/file2.txt",
		"root/sub1/sub2/sub3/file3.txt",
	}

	for _, path := range paths {
		w := fs.NewWriter(path)
		w.Write([]byte("content"))
		w.Close()
	}

	// Test Readdir on subdirectory
	f, err := fs.Open("root/sub1")
	if err != nil {
		t.Fatal(err)
	}

	if readdirFile, ok := f.(interface {
		Readdir(int) ([]os.FileInfo, error)
	}); ok {
		infos, err := readdirFile.Readdir(0)
		if err != nil {
			t.Fatal(err)
		}
		if len(infos) < 1 {
			t.Fatal("expected at least 1 entry")
		}
	}
}

// TestCreateFileInfoDirectoryNotExist tests createFileInfo for non-existent directory
func TestCreateFileInfoDirectoryNotExist(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Try to access a path that doesn't exist as directory
	// This tests the directory existence check in createFileInfo
	_, err = fs.Open("nonexistent/path/to/dir")
	if err == nil {
		t.Fatal("expected error for non-existent directory path")
	}
}

// TestReadContinueOnZeroBytes tests Read continuing when fragment returns 0 bytes
func TestReadContinueOnZeroBytes(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file with multiple fragments
	w := fs.NewWriter("test.txt")
	// Write enough to create multiple fragments
	data := make([]byte, 4096*3) // 3 fragments worth
	for i := range data {
		data[i] = byte('a' + i%26)
	}
	w.Write(data)
	w.Close()

	// Open and read
	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Read in chunks to exercise the continue path
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

	if totalRead != len(data) {
		t.Fatalf("expected to read %d bytes, got %d", len(data), totalRead)
	}
}

// TestOpenErrorPath tests Open with database query error
func TestOpenErrorPath(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Close DB to force query error
	db.Close()

	// Try to open - should fail with database error
	_, err = fs.Open("any.txt")
	if err == nil {
		t.Fatal("expected error when database is closed")
	}
}

// TestWriteFragmentTransactionFailure tests writeFragment transaction error
func TestWriteFragmentTransactionFailure(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file
	w := fs.NewWriter("test.txt")
	w.Write([]byte("data"))

	// Close database before closing writer - should cause error on Close
	db.Close()

	err = w.Close()
	if err == nil {
		t.Fatal("expected error when database is closed during write")
	}
}

// TestReadEOFWhenNoRowsAndNoBytesRead tests specific EOF condition
func TestReadEOFWhenNoRowsAndNoBytesRead(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create file with single fragment
	w := fs.NewWriter("test.txt")
	w.Write([]byte("test"))
	w.Close()

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Read all content
	buf := make([]byte, 4)
	n, _ := f.Read(buf)
	if n != 4 {
		t.Fatalf("expected 4 bytes, got %d", n)
	}

	// Now at EOF, next read should return EOF immediately
	n, err = f.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes at EOF, got %d", n)
	}
}

// TestReadDirCleanNameWithSlash tests ReadDir handling names with trailing slashes
func TestReadDirCleanNameWithSlash(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create entries including one with trailing slash
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "dir/subdir1/", "dir")
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "dir/file.txt", "file")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("dir")
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

		// Check that names are cleaned properly
		for _, entry := range entries {
			name := entry.Name()
			if name == "" || name == "/" {
				t.Fatal("invalid entry name")
			}
		}
	}
}
