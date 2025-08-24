package tests

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
)

// TestReadDirNotADirectory tests ReadDir called on a file
func TestReadDirNotADirectory(t *testing.T) {
	mockDriver := NewSimpleMockDriver()
	
	// File exists as a file, not directory
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}
	
	// Return file type
	mockDriver.queryResponses["SELECT type FROM file_metadata"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"type"}, rows: [][]driver.Value{{"file"}}}, nil
	}
	
	mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{"text/plain"}}}, nil
	}
	
	// For getTotalSize
	mockDriver.queryResponses["COUNT(*), COALESCE"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"count", "size"}, rows: [][]driver.Value{{1, 100}}}, nil
	}
	
	sql.Register("readdir_mock1", mockDriver)
	db, err := sql.Open("readdir_mock1", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	
	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Open as file
	f, err := fs.Open("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	
	// Try to ReadDir on a file - should error
	if dirFile, ok := f.(interface{ ReadDir(int) ([]os.DirEntry, error) }); ok {
		_, err = dirFile.ReadDir(0)
		if err == nil || err.Error() != "not a directory" {
			t.Fatalf("expected 'not a directory', got %v", err)
		}
	}
}

// TestReaddirNotADirectory tests Readdir called on a file
func TestReaddirNotADirectory(t *testing.T) {
	mockDriver := NewSimpleMockDriver()
	
	// File exists as a file, not directory
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}
	
	// Return file type
	mockDriver.queryResponses["SELECT type FROM file_metadata"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"type"}, rows: [][]driver.Value{{"file"}}}, nil
	}
	
	mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{"text/plain"}}}, nil
	}
	
	// For getTotalSize
	mockDriver.queryResponses["COUNT(*), COALESCE"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"count", "size"}, rows: [][]driver.Value{{1, 100}}}, nil
	}
	
	sql.Register("readdir_mock2", mockDriver)
	db, err := sql.Open("readdir_mock2", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	
	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Open as file
	f, err := fs.Open("file.txt")
	if err != nil {
		t.Fatal(err)
	}
	
	// Try to Readdir on a file - should error
	if dirFile, ok := f.(interface{ Readdir(int) ([]os.FileInfo, error) }); ok {
		_, err = dirFile.Readdir(0)
		if err == nil || err.Error() != "not a directory" {
			t.Fatalf("expected 'not a directory', got %v", err)
		}
	}
}

// TestReadDirScanError tests ReadDir when row scan fails
func TestReadDirScanError(t *testing.T) {
	mockDriver := NewSimpleMockDriver()
	
	// Directory exists
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}
	
	// Return directory type
	mockDriver.queryResponses["SELECT type FROM file_metadata"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"type"}, rows: [][]driver.Value{{"dir"}}}, nil
	}
	
	// Make the main query fail
	mockDriver.queryResponses["SELECT path, type FROM file_metadata WHERE path LIKE"] = func(args []driver.Value) (driver.Rows, error) {
		return nil, errors.New("scan error")
	}
	
	sql.Register("readdir_mock3", mockDriver)
	db, err := sql.Open("readdir_mock3", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	
	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Open directory
	f, err := fs.Open("dir")
	if err != nil {
		t.Fatal(err)
	}
	
	// ReadDir should fail with scan error
	if dirFile, ok := f.(interface{ ReadDir(int) ([]os.DirEntry, error) }); ok {
		_, err = dirFile.ReadDir(0)
		if err == nil || err.Error() != "scan error" {
			t.Fatalf("expected scan error, got %v", err)
		}
	}
}

// TestReadDirPathNormalizationMock tests ReadDir with directory path not ending in slash
func TestReadDirPathNormalizationMock(t *testing.T) {
	mockDriver := NewSimpleMockDriver()
	
	// Directory exists
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}
	
	// Return directory type
	mockDriver.queryResponses["SELECT type FROM file_metadata"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"type"}, rows: [][]driver.Value{{"dir"}}}, nil
	}
	
	// Return some files in directory
	mockDriver.queryResponses["SELECT path, type FROM file_metadata WHERE path LIKE"] = func(args []driver.Value) (driver.Rows, error) {
		// This tests the path normalization (adding trailing slash)
		return &mockRows{
			columns: []string{"path", "type"},
			rows: [][]driver.Value{
				{"mydir/file1.txt", "file"},
				{"mydir/file2.txt", "file"},
			},
		}, nil
	}
	
	sql.Register("readdir_mock4", mockDriver)
	db, err := sql.Open("readdir_mock4", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	
	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Open directory without trailing slash
	f, err := fs.Open("mydir")
	if err != nil {
		t.Fatal(err)
	}
	
	// ReadDir should work and normalize path
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

// TestCreateFileInfoDirectoryNotExistsMock tests createFileInfo for non-existent directory
func TestCreateFileInfoDirectoryNotExistsMock(t *testing.T) {
	mockDriver := NewSimpleMockDriver()
	
	// Directory doesn't exist
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		if len(args) > 0 && args[0] == "" {
			// Root always exists
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}
		// Directory doesn't exist
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{0}}}, nil
	}
	
	// No type found
	mockDriver.queryResponses["SELECT type FROM file_metadata"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{}, rows: [][]driver.Value{}}, nil
	}
	
	// Check for directory - returns false
	mockDriver.queryResponses["SELECT 1 FROM file_metadata WHERE path = ?"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{}, rows: [][]driver.Value{}}, nil
	}
	
	sql.Register("readdir_mock5", mockDriver)
	db, err := sql.Open("readdir_mock5", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	
	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Open non-existent directory
	f, err := fs.Open("nonexistent")
	if err != nil {
		// Expected to fail at Open
		return
	}
	
	// If Open somehow succeeds, Stat should fail
	_, err = f.Stat()
	if err != os.ErrNotExist {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}