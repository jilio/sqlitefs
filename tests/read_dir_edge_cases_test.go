package tests

import (
	"database/sql"
	"io"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestGetTotalSizeNoFragments tests getTotalSize when file has metadata but no fragments
func TestGetTotalSizeNoFragments(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create a file with content
	w := fs.NewWriter("test.txt")
	w.Write([]byte("content"))
	w.Close()

	// Delete fragments but keep metadata
	_, err = db.Exec("DELETE FROM file_fragments WHERE file_id = (SELECT id FROM file_metadata WHERE path = ?)", "test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Open and stat should return size 0
	f, err := fs.Open("test.txt")
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
}

// TestReadEOFConditions tests various EOF conditions in Read
func TestReadEOFConditions(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create a file
	content := []byte("test")
	w := fs.NewWriter("test.txt")
	w.Write(content)
	w.Close()

	// Test 1: Read exact size, then read again
	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(content))
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != len(content) {
		t.Fatalf("expected %d bytes, got %d", len(content), n)
	}

	// Second read should return EOF immediately
	n, err = f.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// TestReadDirOnFile tests calling ReadDir on a file (not directory)
func TestReadDirOnFile(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create a file
	w := fs.NewWriter("test.txt")
	w.Write([]byte("content"))
	w.Close()

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Try to ReadDir on a file
	if dirFile, ok := f.(interface{ ReadDir(int) ([]os.DirEntry, error) }); ok {
		_, err = dirFile.ReadDir(0)
		if err == nil {
			t.Fatal("expected error when calling ReadDir on file")
		}
		if err.Error() != "not a directory" {
			t.Fatalf("expected 'not a directory', got: %v", err)
		}
	}
}

// TestReaddirOnFile tests calling Readdir on a file (not directory)
func TestReaddirOnFile(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create a file
	w := fs.NewWriter("test.txt")
	w.Write([]byte("content"))
	w.Close()

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Try to Readdir on a file
	if readdirFile, ok := f.(interface{ Readdir(int) ([]os.FileInfo, error) }); ok {
		_, err = readdirFile.Readdir(0)
		if err == nil {
			t.Fatal("expected error when calling Readdir on file")
		}
		if err.Error() != "not a directory" {
			t.Fatalf("expected 'not a directory', got: %v", err)
		}
	}
}

// TestReadDirPathWithoutSlash tests ReadDir with path not ending in slash
func TestReadDirPathWithoutSlash(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create files in directory (path without trailing slash)
	w1 := fs.NewWriter("mydir/file1.txt")
	w1.Write([]byte("content1"))
	w1.Close()

	w2 := fs.NewWriter("mydir/file2.txt")
	w2.Write([]byte("content2"))
	w2.Close()

	// Open directory without trailing slash
	f, err := fs.Open("mydir")
	if err != nil {
		t.Fatal(err)
	}

	// ReadDir should normalize path
	if dirFile, ok := f.(interface{ ReadDir(int) ([]os.DirEntry, error) }); ok {
		entries, err := dirFile.ReadDir(0)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 2 {
			t.Fatalf("expected 2 entries, got %d", len(entries))
		}
	}
}

// TestCreateFileInfoNonExistentDir tests createFileInfo on non-existent directory
func TestCreateFileInfoNonExistentDir(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Try to open non-existent directory - should fail at Open
	_, err = fs.Open("nonexistentdir")
	if err == nil {
		t.Fatal("expected error opening non-existent directory")
	}
}

// TestReadZeroBytesFragment tests Read when fragment has zero bytes
func TestReadZeroBytesFragment(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create file with content
	w := fs.NewWriter("test.txt")
	w.Write([]byte("hello"))
	w.Close()

	// Get file ID
	var fileID int64
	err = db.QueryRow("SELECT id FROM file_metadata WHERE path = ?", "test.txt").Scan(&fileID)
	if err != nil {
		t.Fatal(err)
	}
	
	// Insert empty fragment at index 1
	_, err = db.Exec("INSERT INTO file_fragments (file_id, fragment_index, fragment) VALUES (?, ?, ?)", 
		fileID, 1, []byte{})
	if err != nil {
		t.Fatal(err)
	}

	// Open and read
	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 100)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should still read the non-empty content from fragment 0
	if n == 0 {
		t.Fatal("expected to read some bytes")
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("expected 'hello', got %s", string(buf[:n]))
	}
}

// TestReadNoFragments tests Read when file has no fragments
func TestReadNoFragments(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create file
	w := fs.NewWriter("test.txt")
	w.Write([]byte("content"))
	w.Close()

	// Delete all fragments
	_, err = db.Exec("DELETE FROM file_fragments WHERE file_id = (SELECT id FROM file_metadata WHERE path = ?)", "test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Open and try to read
	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 10)
	n, err := f.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// TestSeekEndError tests Seek with io.SeekEnd when getTotalSize would fail
func TestSeekEndError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create file
	w := fs.NewWriter("test.txt")
	w.Write([]byte("content"))
	w.Close()

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Close DB to cause getTotalSize to fail
	db.Close()

	// Try to seek from end
	if seeker, ok := f.(io.Seeker); ok {
		_, err = seeker.Seek(-1, io.SeekEnd)
		if err == nil {
			t.Fatal("expected error when database closed")
		}
	}
}

// TestOpenDatabaseClosed tests Open when database is closed
func TestOpenDatabaseClosed(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create file first
	w := fs.NewWriter("test.txt")
	w.Write([]byte("content"))
	w.Close()

	// Close database
	db.Close()

	// Try to open file - should fail
	_, err = fs.Open("test.txt")
	if err == nil {
		t.Fatal("expected error when database closed")
	}
}

// TestWriteFragmentError tests writeFragment error handling
func TestWriteFragmentError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Write to create the file
	w := fs.NewWriter("test.txt")
	w.Write([]byte("initial"))
	w.Close()

	// Close database to cause write errors
	db.Close()

	// Try to write again - should fail
	w2 := fs.NewWriter("test2.txt")
	_, err = w2.Write([]byte("will fail"))
	// Error may be deferred to Close
	if err == nil {
		err = w2.Close()
	}
	if err == nil {
		t.Fatal("expected error when database closed")
	}
}

// TestReadDirWithSubdirAndTrailingSlash tests various subdirectory scenarios
func TestReadDirWithSubdirAndTrailingSlash(t *testing.T) {
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
	w1 := fs.NewWriter("parent/child/file.txt")
	w1.Write([]byte("content"))
	w1.Close()

	// Manually insert dir with trailing slash (edge case)
	_, err = db.Exec("INSERT OR REPLACE INTO file_metadata (path, type) VALUES (?, ?)", 
		"parent/subdir/", "dir")
	if err != nil {
		t.Fatal(err)
	}

	// Open parent dir
	f, err := fs.Open("parent")
	if err != nil {
		t.Fatal(err)
	}

	// ReadDir should handle entries correctly
	if dirFile, ok := f.(interface{ ReadDir(int) ([]os.DirEntry, error) }); ok {
		entries, err := dirFile.ReadDir(0)
		if err != nil {
			t.Fatal(err)
		}
		// Should have child and subdir
		if len(entries) < 1 {
			t.Fatal("expected at least 1 entry")
		}
		for _, entry := range entries {
			if entry.Name() == "" {
				t.Fatal("entry name should not be empty")
			}
		}
	}
}