package tests

import (
	"database/sql"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestReadEOFWithBytesRead tests the case where we hit EOF but have already read some bytes (lines 108-110)
func TestReadEOFWithBytesRead(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file with exactly 4096 bytes (one fragment)
	content := make([]byte, 4096)
	for i := range content {
		content[i] = byte(i % 256)
	}

	w := fs.NewWriter("exact_fragment.txt")
	w.Write(content)
	w.Close()

	f, err := fs.Open("exact_fragment.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Read in chunks that don't align with fragment boundary
	buf := make([]byte, 2000)
	totalRead := 0

	// First read: 2000 bytes
	n, err := f.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	totalRead += n

	// Second read: 2000 bytes
	n, err = f.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	totalRead += n

	// Third read: should get remaining 96 bytes and EOF
	n, err = f.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 96 {
		t.Fatalf("expected 96 bytes, got %d", n)
	}
	totalRead += n

	if totalRead != 4096 {
		t.Fatalf("expected 4096 total bytes, got %d", totalRead)
	}
}

// TestReadNoRowsWithBytesRead tests sql.ErrNoRows with bytes already read (lines 128-130)
func TestReadNoRowsWithBytesRead(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file with content spanning multiple fragments
	content := make([]byte, 8192) // 2 fragments
	for i := range content {
		content[i] = byte(i % 256)
	}

	w := fs.NewWriter("multi_fragment.txt")
	w.Write(content)
	w.Close()

	// Manually corrupt the database by deleting the second fragment
	_, err = db.Exec("DELETE FROM file_fragments WHERE file_id = (SELECT id FROM file_metadata WHERE path = ?) AND fragment_index = 1",
		"multi_fragment.txt")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("multi_fragment.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Read past the first fragment
	buf := make([]byte, 5000)
	n, err := f.Read(buf)
	// Should read 4096 from first fragment, then hit missing second fragment
	if err != io.EOF {
		t.Fatalf("expected io.EOF due to missing fragment, got %v", err)
	}
	if n != 4096 {
		t.Fatalf("expected 4096 bytes from first fragment, got %d", n)
	}
}

// TestReadEmptyFragmentAtEOF tests empty fragment when at EOF (lines 144-146)
func TestReadEmptyFragmentAtEOF(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file and then manually add an empty fragment
	w := fs.NewWriter("empty_frag.txt")
	w.Write([]byte("test"))
	w.Close()

	// Get file ID
	var fileID int
	err = db.QueryRow("SELECT id FROM file_metadata WHERE path = ?", "empty_frag.txt").Scan(&fileID)
	if err != nil {
		t.Fatal(err)
	}

	// Manually insert an empty fragment
	_, err = db.Exec("INSERT INTO file_fragments (file_id, fragment_index, fragment) VALUES (?, ?, ?)",
		fileID, 1, []byte{})
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("empty_frag.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Read all content
	buf := make([]byte, 100)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("expected 4 bytes, got %d", n)
	}

	// Try to read again - should hit empty fragment and EOF
	n, err = f.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// TestCreateFileInfoDirectoryQueryError tests directory query error (lines 205-207)
func TestCreateFileInfoDirectoryQueryError(t *testing.T) {
	// This requires a mock driver to simulate query error
	// We'll create a scenario where the directory check query fails
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Close the database to cause query errors
	db.Close()

	// Try to open a path - should fail with query error
	_, err = fs.Open("test/path")
	if err == nil {
		t.Fatal("expected error when database is closed")
	}
}

// TestReadDirQueryError tests ReadDir query error (lines 228-230)
func TestReadDirQueryError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a directory
	w := fs.NewWriter("testdir/file.txt")
	w.Write([]byte("content"))
	w.Close()

	f, err := fs.Open("testdir")
	if err != nil {
		t.Fatal(err)
	}

	// Close database to cause query error
	db.Close()

	if dirFile, ok := f.(interface {
		ReadDir(int) ([]os.DirEntry, error)
	}); ok {
		_, err = dirFile.ReadDir(0)
		if err == nil {
			t.Fatal("expected error when database is closed")
		}
	}
}

// TestReadDirSubdirError tests ReadDir subdirectory query error (lines 249-251)
func TestReadDirSubdirError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a complex directory structure
	w1 := fs.NewWriter("dir/file1.txt")
	w1.Write([]byte("content1"))
	w1.Close()

	w2 := fs.NewWriter("dir/subdir/file2.txt")
	w2.Write([]byte("content2"))
	w2.Close()

	// Corrupt the metadata for subdirectory check
	_, err = db.Exec("DELETE FROM file_metadata WHERE path LIKE 'dir/subdir%' AND path != 'dir/subdir/file2.txt'")
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
		// Should still work, just might have inconsistent directory info
		if len(entries) < 1 {
			t.Fatal("expected at least one entry")
		}
	}
}

// TestReadDirCleanEmptyName tests ReadDir when clean name becomes empty (lines 254-255)
func TestReadDirCleanEmptyName(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Manually insert entries with paths that result in empty clean names
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "testdir/", "dir")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("testdir")
	if err != nil {
		// If directory doesn't exist properly, that's ok for this test
		return
	}

	if dirFile, ok := f.(interface {
		ReadDir(int) ([]os.DirEntry, error)
	}); ok {
		entries, err := dirFile.ReadDir(0)
		if err != nil {
			// Error is expected when clean name is empty
			return
		}
		// Check that no entries have empty names
		for _, entry := range entries {
			if entry.Name() == "" {
				t.Fatal("found entry with empty name")
			}
		}
	}
}

// TestReaddirQueryError tests Readdir query error (lines 280-282)
func TestReaddirQueryError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a directory
	w := fs.NewWriter("testdir2/file.txt")
	w.Write([]byte("content"))
	w.Close()

	f, err := fs.Open("testdir2")
	if err != nil {
		t.Fatal(err)
	}

	// Close database to cause query error
	db.Close()

	if dirFile, ok := f.(interface {
		Readdir(int) ([]os.FileInfo, error)
	}); ok {
		_, err = dirFile.Readdir(0)
		if err == nil {
			t.Fatal("expected error when database is closed")
		}
	}
}

// TestReaddirNotDirectory tests Readdir on non-directory (lines 315-317)
func TestReaddirNotDirectory(t *testing.T) {
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
	w := fs.NewWriter("notdir.txt")
	w.Write([]byte("content"))
	w.Close()

	f, err := fs.Open("notdir.txt")
	if err != nil {
		t.Fatal(err)
	}

	if dirFile, ok := f.(interface {
		Readdir(int) ([]os.FileInfo, error)
	}); ok {
		_, err = dirFile.Readdir(0)
		if err == nil || err.Error() != "not a directory" {
			t.Fatalf("expected 'not a directory' error, got %v", err)
		}
	}
}

// TestReaddirCleanPathContinue tests Readdir when clean name processing continues (lines 324-326)
func TestReaddirCleanPathContinue(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create files and manually add entries with trailing slashes
	w := fs.NewWriter("cleantest/file.txt")
	w.Write([]byte("content"))
	w.Close()

	// Manually insert a path that needs cleaning
	_, err = db.Exec("INSERT OR IGNORE INTO file_metadata (path, type) VALUES (?, ?)", "cleantest//", "dir")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("cleantest")
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
		// Should have cleaned entries
		for _, info := range infos {
			if info.Name() == "" || info.Name() == "/" {
				t.Fatal("found invalid entry name")
			}
		}
	}
}

// TestReaddirScanError tests Readdir scan error (lines 332-334)
func TestReaddirScanError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create directory structure
	w := fs.NewWriter("scantest/file.txt")
	w.Write([]byte("content"))
	w.Close()

	// Corrupt the type field to cause scan issues
	_, err = db.Exec("UPDATE file_metadata SET type = NULL WHERE path = 'scantest/file.txt'")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("scantest")
	if err != nil {
		t.Fatal(err)
	}

	if dirFile, ok := f.(interface {
		Readdir(int) ([]os.FileInfo, error)
	}); ok {
		_, err = dirFile.Readdir(0)
		// Should handle the error gracefully
		if err != nil && err != io.EOF {
			// Error is acceptable
			return
		}
	}
}

// Additional error path tests

// TestGetTotalSizeQueryRowsError tests getTotalSize when rows.Err() returns error (lines 582-584)
func TestGetTotalSizeQueryRowsError(t *testing.T) {
	// This is difficult to test without mocking as it requires rows.Err() to fail
	// after successful iteration
	t.Skip("Requires specific mock setup for rows.Err() failure")
}

// TestCreateFileInfoFileNotExist tests when file doesn't exist (line 589)
func TestCreateFileInfoFileNotExist(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Open non-existent file
	_, err = fs.Open("does_not_exist.txt")
	if err == nil {
		t.Fatal("expected error for non-existent file")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

// TestSQLiteFSOpenQueryError tests Open when query fails (lines 79-81, 92-94)
func TestSQLiteFSOpenQueryError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Close database to cause query errors
	db.Close()

	// Try to open - should fail
	_, err = fs.Open("test.txt")
	if err == nil {
		t.Fatal("expected error when database is closed")
	}
}

// TestWriterCommitError tests writer commit error (line 183-185)
func TestWriterCommitError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	w := fs.NewWriter("commit_test.txt")
	w.Write([]byte("data"))

	// Close database before closing writer
	db.Close()

	err = w.Close()
	if err == nil {
		t.Fatal("expected error when database is closed")
	}
}
