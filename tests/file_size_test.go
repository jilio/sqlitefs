package tests

import (
	"database/sql"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestGetTotalSizeFileExistsNoFragments tests getTotalSize when file exists but has no fragments
// This specifically tests lines 574-586 in getTotalSize
func TestGetTotalSizeFileExistsNoFragments(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Directly insert file metadata without fragments
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "empty.txt", "file")
	if err != nil {
		t.Fatal(err)
	}

	// Open the file - this creates a SQLiteFile instance
	f, err := fs.Open("empty.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Call Stat which internally calls getTotalSize
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	// File exists but has no fragments, so size should be 0
	if info.Size() != 0 {
		t.Fatalf("expected size 0 for file with no fragments, got %d", info.Size())
	}
}

// TestGetTotalSizeNonExistentFileOpen tests getTotalSize when file doesn't exist at all
func TestGetTotalSizeNonExistentFileOpen(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Try to open a file that doesn't exist - should fail at Open
	_, err = fs.Open("nonexistent.txt")
	if err == nil {
		t.Fatal("expected error opening non-existent file")
	}
}

// TestGetTotalSizeQueryError tests getTotalSize when database query fails
func TestGetTotalSizeQueryError(t *testing.T) {
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
	w.Write([]byte("content"))
	w.Close()

	// Open the file
	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Close database to cause query error
	db.Close()

	// Try to stat - should fail with database error
	_, err = f.Stat()
	if err == nil {
		t.Fatal("expected error when database is closed")
	}
}
