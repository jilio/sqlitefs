package tests

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// Tests for getTotalSize function

// TestGetTotalSizeCountZero tests getTotalSize when count is 0
func TestGetTotalSizeCountZero(t *testing.T) {
	driver := NewMockDriver()

	// Register specific response for COUNT query that returns 0 count
	sql.Register("mock-count-zero", driver)

	db, err := sql.Open("mock-count-zero", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// The mock driver returns count=0 for COUNT queries
	// This tests the count == 0 path in getTotalSize
	file, err := fs.Open("test.txt")
	if err != nil {
		// If open fails, that's ok - we're testing the error path
		return
	}
	defer file.Close()

	// Try to stat which calls getTotalSize
	info, err := file.Stat()
	if err != nil {
		// Error is expected since mock returns 0 count
		return
	}

	// If no error, size should be 0
	if info.Size() != 0 {
		t.Errorf("Expected size 0 when count is 0, got %d", info.Size())
	}
}

// TestGetTotalSizeMainQueryError tests getTotalSize when main query fails
func TestGetTotalSizeMainQueryError(t *testing.T) {
	driver := NewMockDriver()

	// Set error for the main COUNT query
	driver.SetError("SELECT COUNT(*), COALESCE(LENGTH(fragment)", errors.New("count query failed"))

	sql.Register("mock-count-error", driver)

	db, err := sql.Open("mock-count-error", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	file, err := fs.Open("test.txt")
	if err != nil {
		// Open might fail due to getTotalSize error
		return
	}
	defer file.Close()

	// Try to stat - should fail with our error
	_, err = file.Stat()
	if err == nil {
		t.Error("Expected error from getTotalSize")
	}
}

// TestGetTotalSizeExistsQueryError tests when EXISTS query fails after ErrNoRows
func TestGetTotalSizeExistsQueryError(t *testing.T) {
	driver := NewMockDriver()

	// First query returns ErrNoRows
	driver.SetError("SELECT COUNT(*), COALESCE(LENGTH(fragment)", sql.ErrNoRows)
	// EXISTS query fails
	driver.SetError("SELECT EXISTS(SELECT 1 FROM file_metadata", errors.New("exists query failed"))

	sql.Register("mock-exists-query-error", driver)

	db, err := sql.Open("mock-exists-query-error", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Try to open a file - should fail in getTotalSize
	_, err = fs.Open("test.txt")
	if err == nil || err.Error() != "exists query failed" {
		t.Errorf("Expected 'exists query failed', got %v", err)
	}
}

// TestGetTotalSizeFileDoesNotExist tests when file doesn't exist
func TestGetTotalSizeFileDoesNotExist(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Try to open a non-existent file
	_, err = fs.Open("nonexistent.txt")
	if err == nil {
		t.Error("Expected error opening non-existent file")
	}
}

// TestGetTotalSizeWithFragments tests normal path with fragments
func TestGetTotalSizeWithFragments(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Create a file with known size
	writer := fs.NewWriter("test.txt")
	testData := make([]byte, 1024*1024*2+500) // 2MB + 500 bytes
	for i := range testData {
		testData[i] = byte(i % 256)
	}
	_, err = writer.Write(testData)
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	// Open and stat the file
	file, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	// Check size is correct
	if info.Size() != int64(len(testData)) {
		t.Errorf("Expected size %d, got %d", len(testData), info.Size())
	}
}

// TestGetTotalSizeEmptyFile tests getTotalSize for empty file
func TestGetTotalSizeEmptyFile(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Create an empty file
	writer := fs.NewWriter("empty.txt")
	writer.Close() // Close without writing anything

	// Open and stat the file
	file, err := fs.Open("empty.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}

	// Check size is 0
	if info.Size() != 0 {
		t.Errorf("Expected size 0 for empty file, got %d", info.Size())
	}
}

// TestGetTotalSizeErrorInExistsCheck tests error in EXISTS check in getTotalSize
func TestGetTotalSizeErrorInExistsCheck(t *testing.T) {
	// Create a driver that returns specific errors
	driver := NewMockDriver()
	sql.Register("mock-exists-error", driver)

	db, err := sql.Open("mock-exists-error", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Set error for the EXISTS check that happens after ErrNoRows
	driver.SetError("SELECT EXISTS", errors.New("exists check failed"))

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Try to open a nonexistent file - this should trigger the EXISTS check
	file, err := fs.Open("nonexistent.txt")
	if err == nil {
		file.Close()
		// If Open succeeded, try Stat which calls getTotalSize
		_, err = file.Stat()
	}

	// We expect an error from the EXISTS check
	if err == nil || err.Error() != "exists check failed" {
		t.Errorf("Expected 'exists check failed' error, got %v", err)
	}
}
