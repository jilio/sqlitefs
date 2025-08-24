package tests

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
)

// TestGetTotalSizeExistsQueryErrorMock tests when EXISTS query fails in getTotalSize
func TestGetTotalSizeExistsQueryErrorMock(t *testing.T) {
	mockDriver := NewSimpleMockDriver()

	// File exists
	mockDriver.queryResponses["SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)"] = func(args []driver.Value) (driver.Rows, error) {
		if len(args) > 0 && args[0] == "test.txt" {
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}

	mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{"text/plain"}}}, nil
	}

	sql.Register("final_mock1", mockDriver)
	db, err := sql.Open("final_mock1", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Set up for createFileInfo
	mockDriver.queryResponses["SELECT id FROM file_metadata WHERE path = ? AND type = 'file'"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"id"}, rows: [][]driver.Value{{int64(1)}}}, nil
	}

	// Make getTotalSize COUNT query return no rows (triggers sql.ErrNoRows)
	mockDriver.queryResponses["COUNT(*)"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"count", "length"}, rows: [][]driver.Value{}}, nil
	}

	// Then make the EXISTS query fail - this tests line 579 in getTotalSize
	callCount := 0
	mockDriver.queryResponses["SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)"] = func(args []driver.Value) (driver.Rows, error) {
		callCount++
		if callCount > 1 && len(args) > 0 && args[0] == "test.txt" {
			// First call is for Open, second is in getTotalSize after ErrNoRows
			return nil, errors.New("exists query failed")
		}
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}

	// Stat should fail with the EXISTS query error
	_, err = f.Stat()
	if err == nil || err.Error() != "exists query failed" {
		t.Fatalf("expected 'exists query failed', got %v", err)
	}
}

// TestGetTotalSizeFileNotExist tests when file doesn't exist in getTotalSize
func TestGetTotalSizeFileNotExist(t *testing.T) {
	mockDriver := NewSimpleMockDriver()

	// File exists for Open
	mockDriver.queryResponses["SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}

	mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{"text/plain"}}}, nil
	}

	sql.Register("final_mock2", mockDriver)
	db, err := sql.Open("final_mock2", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Set up for createFileInfo - file doesn't exist
	mockDriver.queryResponses["SELECT id FROM file_metadata WHERE path = ? AND type = 'file'"] = func(args []driver.Value) (driver.Rows, error) {
		// Return no rows - file doesn't exist
		return &mockRows{columns: []string{}, rows: [][]driver.Value{}}, nil
	}

	// Stat should fail with ErrNotExist
	_, err = f.Stat()
	if err != os.ErrNotExist {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

// TestReadContinueOnEmptyFragment tests Read continuing when fragment is empty
func TestReadContinueOnEmptyFragment(t *testing.T) {
	mockDriver := NewSimpleMockDriver()

	// File exists
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}

	mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{"text/plain"}}}, nil
	}

	// getTotalSize returns size for 2 fragments, last fragment is 5 bytes (hello)
	mockDriver.queryResponses["COUNT(*)"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"count", "size"}, rows: [][]driver.Value{{2, 5}}}, nil
	}

	sql.Register("final_mock3", mockDriver)
	db, err := sql.Open("final_mock3", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// First fragment returns empty, second returns data
	callCount := 0
	mockDriver.queryResponses["SELECT SUBSTR(fragment"] = func(args []driver.Value) (driver.Rows, error) {
		callCount++
		if callCount == 1 {
			// First fragment is empty
			return &mockRows{columns: []string{"fragment"}, rows: [][]driver.Value{{[]byte{}}}}, nil
		}
		// Second fragment has data
		return &mockRows{columns: []string{"fragment"}, rows: [][]driver.Value{{[]byte("hello")}}}, nil
	}

	buf := make([]byte, 10)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Fatalf("expected 5 bytes, got %d", n)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("expected 'hello', got %s", string(buf[:n]))
	}
}

// TestWriteFragmentExecError tests writeFragment when Exec fails
func TestWriteFragmentExecError(t *testing.T) {
	mockDriver := NewSimpleMockDriver()

	// Allow metadata insert to succeed
	mockDriver.execResponses["INSERT OR REPLACE INTO file_metadata"] = func(args []driver.Value) (driver.Result, error) {
		return &mockResult{lastInsertId: 1, rowsAffected: 1}, nil
	}

	// Make fragment INSERT fail
	mockDriver.execResponses["INSERT INTO file_fragments"] = func(args []driver.Value) (driver.Result, error) {
		return nil, errors.New("exec failed")
	}

	// Need to handle the SELECT id query that happens in writeFragment
	mockDriver.queryResponses["SELECT id FROM file_metadata WHERE path = ?"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"id"}, rows: [][]driver.Value{{int64(1)}}}, nil
	}

	sql.Register("final_mock4", mockDriver)
	db, err := sql.Open("final_mock4", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	w := fs.NewWriter("test.txt")
	_, err = w.Write([]byte("data"))
	if err == nil {
		err = w.Close()
	}
	if err == nil || err.Error() != "exec failed" {
		t.Fatalf("expected 'exec failed', got %v", err)
	}
}

// TestOpenFileQueryError tests Open when file query fails
func TestOpenFileQueryError(t *testing.T) {
	mockDriver := NewSimpleMockDriver()

	// Make the EXISTS query fail
	mockDriver.queryResponses["SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)"] = func(args []driver.Value) (driver.Rows, error) {
		if len(args) > 0 && args[0] != "" {
			return nil, errors.New("query failed")
		}
		// Root exists
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}

	sql.Register("final_mock5", mockDriver)
	db, err := sql.Open("final_mock5", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Open should fail with query error
	_, err = fs.Open("test.txt")
	if err == nil || err.Error() != "query failed" {
		t.Fatalf("expected 'query failed', got %v", err)
	}
}

// TestReadQueryError tests Read when query fails
func TestReadQueryError(t *testing.T) {
	mockDriver := NewSimpleMockDriver()

	// File exists
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}

	mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{"text/plain"}}}, nil
	}

	// getTotalSize returns some size
	mockDriver.queryResponses["COUNT(*), COALESCE(LENGTH(fragment)"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"count", "size"}, rows: [][]driver.Value{{1, 100}}}, nil
	}

	sql.Register("final_mock6", mockDriver)
	db, err := sql.Open("final_mock6", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Make Read query fail
	mockDriver.queryResponses["SELECT SUBSTR(fragment"] = func(args []driver.Value) (driver.Rows, error) {
		return nil, errors.New("read query failed")
	}

	buf := make([]byte, 10)
	_, err = f.Read(buf)
	if err == nil || err.Error() != "read query failed" {
		t.Fatalf("expected 'read query failed', got %v", err)
	}
}
