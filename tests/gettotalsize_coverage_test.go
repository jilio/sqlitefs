package tests

import (
	"database/sql"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestGetTotalSizeSpecificPath tests the specific path in getTotalSize that handles sql.ErrNoRows
// This targets lines 578-589 in getTotalSize
func TestGetTotalSizeSpecificPath(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create tables
	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	_ = fs

	// Insert file metadata directly without any fragments
	// This should trigger the sql.ErrNoRows path in getTotalSize
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "nofrags.txt", "file")
	if err != nil {
		t.Fatal(err)
	}

	// Open the file (which has metadata but no fragments)
	f, err := fs.Open("nofrags.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Call Stat which internally calls getTotalSize
	// The query will return sql.ErrNoRows because there are no fragments
	// Then it checks if file exists (which it does) and returns size 0
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}

	// Should return 0 for file with no fragments
	if info.Size() != 0 {
		t.Fatalf("expected size 0, got %d", info.Size())
	}
}

// TestGetTotalSizeNonExistentPath tests getTotalSize when file doesn't exist
// This should trigger the path that returns os.ErrNotExist
func TestGetTotalSizeNonExistentPath(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Try to open a file that doesn't exist
	// This should fail at Open(), not getTotalSize
	_, err = fs.Open("doesnotexist.txt")
	if err == nil {
		t.Fatal("expected error for non-existent file")
	}
}

// TestGetTotalSizeDatabaseClosedError tests when the EXISTS query fails
func TestGetTotalSizeDatabaseClosedError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Insert file metadata
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "test.txt", "file")
	if err != nil {
		t.Fatal(err)
	}

	// Open the file
	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Close the database to force query errors
	db.Close()

	// Try to stat - should fail with database error
	_, err = f.Stat()
	if err == nil {
		t.Fatal("expected error when database is closed")
	}
}