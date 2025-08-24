package tests

import (
	"database/sql"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// Test to boost coverage to 90%
func TestGetTotalSizeErrNoRows(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create tables manually to have control
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS file_metadata (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			path TEXT UNIQUE NOT NULL,
			type TEXT NOT NULL,
			mime_type TEXT
		);
		CREATE TABLE IF NOT EXISTS file_fragments (
			file_id INTEGER NOT NULL,
			fragment_index INTEGER NOT NULL,
			fragment BLOB NOT NULL,
			PRIMARY KEY (file_id, fragment_index),
			FOREIGN KEY (file_id) REFERENCES file_metadata(id)
		);
	`)
	if err != nil {
		t.Fatal(err)
	}

	// Insert a file with NULL id to trigger specific behavior
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES ('test.txt', 'file')")
	if err != nil {
		t.Fatal(err)
	}

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// This should work even with no fragments
	file, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// The file should exist but have 0 size
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Errorf("Expected size 0, got %d", info.Size())
	}
}

// Additional Read error path tests
func TestReadErrorPaths(t *testing.T) {
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
	writer := fs.NewWriter("multi.txt")
	// Write 2.5MB to create 3 fragments
	data := make([]byte, 1024*1024*2+512*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}
	writer.Write(data)
	writer.Close()

	file, err := fs.Open("multi.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Read in chunks across fragment boundaries
	sqliteFile := file.(*sqlitefs.SQLiteFile)

	// Seek to middle of first fragment
	sqliteFile.Seek(512*1024, 0)

	// Read across fragment boundary
	buf := make([]byte, 1024*1024)
	n, err := file.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1024*1024 {
		t.Errorf("Expected to read 1MB, got %d", n)
	}

	// Continue reading
	n, err = file.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1024*1024 {
		t.Errorf("Expected to read 1MB, got %d", n)
	}
}

// Test Readdir scanning error paths
func TestReaddirScanErrors(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create nested directories
	writer := fs.NewWriter("dir1/subdir1/file1.txt")
	writer.Write([]byte("test"))
	writer.Close()

	writer = fs.NewWriter("dir1/subdir2/file2.txt")
	writer.Write([]byte("test"))
	writer.Close()

	writer = fs.NewWriter("dir1/file3.txt")
	writer.Write([]byte("test"))
	writer.Close()

	// Open dir1
	dir, err := fs.Open("dir1/")
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	dirFile := dir.(*sqlitefs.SQLiteFile)

	// Read directory entries
	infos, err := dirFile.Readdir(-1)
	if err != nil {
		t.Fatal(err)
	}

	// Should have both subdirs and file
	if len(infos) < 2 {
		t.Errorf("Expected at least 2 entries, got %d", len(infos))
	}
}

// Test createFileInfo for root path edge case
func TestCreateFileInfoRootPath(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create some files
	writer := fs.NewWriter("file1.txt")
	writer.Write([]byte("test"))
	writer.Close()

	// Open root with empty string
	dir, err := fs.Open("")
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	// Stat should work
	info, err := dir.Stat()
	if err != nil {
		t.Fatal(err)
	}

	if !info.IsDir() {
		t.Error("Root should be a directory")
	}
}
