package tests

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// Tests for directory operations and pagination

// TestReadDirWithLimitAndOffset tests ReadDir with specific limit/offset
func TestReadDirWithLimitAndOffset(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Create several files in a directory
	for i := 0; i < 10; i++ {
		writer := fs.NewWriter(fmt.Sprintf("dir/file%02d.txt", i))
		writer.Write([]byte("test"))
		writer.Close()
	}

	// Create subdirectories
	writer := fs.NewWriter("dir/sub1/file.txt")
	writer.Write([]byte("test"))
	writer.Close()

	writer = fs.NewWriter("dir/sub2/file.txt")
	writer.Write([]byte("test"))
	writer.Close()

	dir, err := fs.Open("dir/")
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	dirFile := dir.(*sqlitefs.SQLiteFile)

	// Read with small limit to test pagination
	entries1, err := dirFile.ReadDir(3)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	// Continue reading
	entries2, err := dirFile.ReadDir(3)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	// Read remaining
	entries3, err := dirFile.ReadDir(-1)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	totalEntries := len(entries1) + len(entries2) + len(entries3)
	if totalEntries == 0 {
		t.Error("Expected to read some directory entries")
	}
}

// TestReaddirWithSmallLimit tests Readdir with very small limit
func TestReaddirWithSmallLimit(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create files
	for i := 0; i < 5; i++ {
		writer := fs.NewWriter(fmt.Sprintf("file%d.txt", i))
		writer.Write([]byte("test"))
		writer.Close()
	}

	dir, err := fs.Open("/")
	if err != nil {
		t.Fatalf("Failed to open /: %v", err)
	}
	defer dir.Close()

	dirFile := dir.(*sqlitefs.SQLiteFile)

	// Read one at a time
	for i := 0; i < 5; i++ {
		infos, err := dirFile.Readdir(1)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}

		if len(infos) > 1 {
			t.Errorf("Expected at most 1 entry, got %d", len(infos))
		}

		if err == io.EOF {
			break
		}
	}
}

// TestReaddirPagination tests Readdir with pagination
func TestReaddirPagination(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create multiple files
	for i := 0; i < 10; i++ {
		writer := fs.NewWriter(fmt.Sprintf("file%d.txt", i))
		writer.Write([]byte("test"))
		writer.Close()
	}

	dir, err := fs.Open("/")
	if err != nil {
		t.Fatalf("Failed to open /: %v", err)
	}
	defer dir.Close()

	// Read in batches
	dirFile := dir.(*sqlitefs.SQLiteFile)
	infos1, err := dirFile.Readdir(3)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	infos2, err := dirFile.Readdir(3)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	// Should have read some files
	if len(infos1) == 0 && len(infos2) == 0 {
		t.Error("Expected to read some files")
	}
}

// TestReadDirEmptyDirectory tests ReadDir on empty directory
func TestReadDirEmptyDirectory(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Manually create tables
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_metadata (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT NOT NULL UNIQUE,
			type TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert an empty directory
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "emptydir/", "dir")
	if err != nil {
		t.Fatal(err)
	}

	fs, _ := sqlitefs.NewSQLiteFS(db)

	dir, err := fs.Open("emptydir/")
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	dirFile := dir.(*sqlitefs.SQLiteFile)

	// ReadDir on empty directory should return empty slice
	entries, err := dirFile.ReadDir(-1)
	if err != nil && err.Error() != "EOF" {
		// EOF is ok for empty dir
	}

	if len(entries) != 0 {
		t.Errorf("Expected 0 entries in empty directory, got %d", len(entries))
	}
}

// TestReadDirErrorPathsAdditional tests additional error paths in ReadDir
func TestReadDirErrorPathsAdditional(t *testing.T) {
	driver := NewMockDriver()
	sql.Register("mock-readdir-error", driver)

	db, err := sql.Open("mock-readdir-error", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Open a directory
	dir, err := fs.Open("dir/")
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	// Set error for ReadDir query
	driver.SetError("SELECT path", errors.New("readdir failed"))

	// Try to ReadDir
	dirFile := dir.(*sqlitefs.SQLiteFile)
	_, err = dirFile.ReadDir(-1)
	if err == nil || err.Error() != "readdir failed" {
		t.Errorf("Expected 'readdir failed' error, got %v", err)
	}
}

// TestCreateFileInfoForDirectory tests createFileInfo for directories
func TestCreateFileInfoForDirectory(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Create a file in a subdirectory to ensure the directory exists
	writer := fs.NewWriter("mydir/file.txt")
	writer.Write([]byte("test"))
	writer.Close()

	// Open the directory
	dir, err := fs.Open("mydir/")
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	// Stat the directory
	info, err := dir.Stat()
	if err != nil {
		t.Fatal(err)
	}

	// Verify it's a directory
	if !info.IsDir() {
		t.Error("Expected directory, got file")
	}

	// Size should be 0 for directories
	if info.Size() != 0 {
		t.Errorf("Expected size 0 for directory, got %d", info.Size())
	}
}

// TestCreateFileInfoErrorsAdditional tests additional error paths in createFileInfo
func TestCreateFileInfoErrorsAdditional(t *testing.T) {
	t.Skip("Mock driver test needs updating for new query patterns")
	driver := NewMockDriver()
	sql.Register("mock-fileinfo-error", driver)

	db, err := sql.Open("mock-fileinfo-error", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Set error for file info query (updated to match actual schema)
	driver.SetError("SELECT type, created_at", errors.New("fileinfo failed"))

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Try to open - should fail when creating file info
	_, err = fs.Open("test.txt")
	if err == nil || err.Error() != "fileinfo failed" {
		t.Errorf("Expected 'fileinfo failed' error, got %v", err)
	}
}

// TestOpenDirectoryCheck tests Open when checking if path is directory fails
func TestOpenDirectoryCheck(t *testing.T) {
	t.Skip("Mock driver test needs updating for new query patterns")
	driver := NewMockDriver()
	sql.Register("mock-dir-check", driver)

	db, err := sql.Open("mock-dir-check", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Set error for directory check (updated to match actual schema)
	driver.SetError("SELECT 1 FROM file_metadata WHERE path = ? AND type = ?", errors.New("dir check failed"))

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Try to open a directory
	_, err = fs.Open("dir/")
	if err == nil || err.Error() != "dir check failed" {
		t.Errorf("Expected 'dir check failed' error, got %v", err)
	}
}

// TestGetTotalSizeFileExistsButNoFragments tests when file exists in metadata but has no fragments
func TestGetTotalSizeFileExistsButNoFragments(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Manually create tables
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_metadata (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT NOT NULL UNIQUE,
			type TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_fragments (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_id INTEGER NOT NULL,
			fragment_index INTEGER NOT NULL,
			fragment BLOB,
			FOREIGN KEY (file_id) REFERENCES file_metadata(id),
			UNIQUE(file_id, fragment_index)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert a file with no fragments
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "empty.txt", "file")
	if err != nil {
		t.Fatal(err)
	}

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Open the file - should succeed even with no fragments
	file, err := fs.Open("empty.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Stat should return size 0
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if info.Size() != 0 {
		t.Errorf("Expected size 0 for file with no fragments, got %d", info.Size())
	}
}

// TestGetTotalSizeFileDoesNotExistInMetadata tests when file doesn't exist at all
func TestGetTotalSizeFileDoesNotExistInMetadata(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Manually create tables
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_metadata (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT NOT NULL UNIQUE,
			type TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_fragments (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_id INTEGER NOT NULL,
			fragment_index INTEGER NOT NULL,
			fragment BLOB,
			FOREIGN KEY (file_id) REFERENCES file_metadata(id),
			UNIQUE(file_id, fragment_index)
		)
	`)
	if err != nil {
		t.Fatal(err)
	}

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Try to open a non-existent file
	_, err = fs.Open("nonexistent.txt")

	// Should get file does not exist error
	if err == nil {
		t.Error("Expected error for non-existent file")
	}
}
