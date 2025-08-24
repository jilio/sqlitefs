package tests

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	// "fmt"
	"io"
	"testing"

	"github.com/jilio/sqlitefs"
)

// SimpleMockDriver is a minimal mock driver for specific test scenarios
type SimpleMockDriver struct {
	queryResponses map[string]func(args []driver.Value) (driver.Rows, error)
	execResponses  map[string]func(args []driver.Value) (driver.Result, error)
}

func NewSimpleMockDriver() *SimpleMockDriver {
	return &SimpleMockDriver{
		queryResponses: make(map[string]func(args []driver.Value) (driver.Rows, error)),
		execResponses:  make(map[string]func(args []driver.Value) (driver.Result, error)),
	}
}

func (d *SimpleMockDriver) Open(name string) (driver.Conn, error) {
	return &simpleMockConn{driver: d}, nil
}

type simpleMockConn struct {
	driver *SimpleMockDriver
}

func (c *simpleMockConn) Prepare(query string) (driver.Stmt, error) {
	return &simpleMockStmt{conn: c, query: query}, nil
}

func (c *simpleMockConn) Close() error { return nil }

func (c *simpleMockConn) Begin() (driver.Tx, error) {
	return &simpleMockTx{conn: c}, nil
}

type simpleMockTx struct {
	conn *simpleMockConn
}

func (tx *simpleMockTx) Commit() error   { return nil }
func (tx *simpleMockTx) Rollback() error { return nil }

type simpleMockStmt struct {
	conn  *simpleMockConn
	query string
}

func (s *simpleMockStmt) Close() error { return nil }
func (s *simpleMockStmt) NumInput() int { 
	// Count ? placeholders in query
	count := 0
	for _, ch := range s.query {
		if ch == '?' {
			count++
		}
	}
	return count
}

func (s *simpleMockStmt) Exec(args []driver.Value) (driver.Result, error) {
	// Handle table creation
	if contains(s.query, "CREATE") || contains(s.query, "ALTER") {
		return &mockResult{}, nil
	}
	
	// Check for custom handler
	for pattern, handler := range s.conn.driver.execResponses {
		if contains(s.query, pattern) {
			return handler(args)
		}
	}
	
	return &mockResult{}, nil
}

func (s *simpleMockStmt) Query(args []driver.Value) (driver.Rows, error) {
	// Debug: log the query (first 50 chars)
	// queryStart := s.query
	// if len(queryStart) > 50 {
	// 	queryStart = queryStart[:50] + "..."
	// }
	// fmt.Printf("Query: %q, Args: %v\n", queryStart, args)
	
	// Check for custom handler
	for pattern, handler := range s.conn.driver.queryResponses {
		if contains(s.query, pattern) {
			return handler(args)
		}
	}
	
	// Default handlers for common queries
	if contains(s.query, "SELECT EXISTS") && contains(s.query, "file_metadata") {
		// Check for root directory
		if len(args) > 0 && args[0] == "" {
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}
		// Default: doesn't exist
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{0}}}, nil
	}
	
	// Empty result by default
	return &mockRows{columns: []string{}, rows: [][]driver.Value{}}, nil
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || 
		(len(s) > len(substr) && (s[:len(substr)] == substr || 
		s[len(s)-len(substr):] == substr || 
		containsMiddle(s, substr))))
}

func containsMiddle(s, substr string) bool {
	for i := 1; i < len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestGetTotalSizeNoFragmentsMock tests the specific path where file exists but has no fragments
func TestGetTotalSizeNoFragmentsMock(t *testing.T) {
	mockDriver := NewSimpleMockDriver()
	
	// Setup: file exists but COUNT query returns 0
	mockDriver.queryResponses["COUNT(*), COALESCE(LENGTH(fragment)"] = func(args []driver.Value) (driver.Rows, error) {
		// Return empty result to trigger sql.ErrNoRows
		return &mockRows{columns: []string{}, rows: [][]driver.Value{}}, nil
	}
	
	// The EXISTS check should return true (file exists)
	mockDriver.queryResponses["SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)"] = func(args []driver.Value) (driver.Rows, error) {
		if len(args) > 0 && args[0] == "test.txt" {
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{0}}}, nil
	}
	
	sql.Register("simple_mock1", mockDriver)
	db, err := sql.Open("simple_mock1", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	
	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Also need to set up for when Open checks for mime_type
	mockDriver.queryResponses["SELECT mime_type FROM file_metadata"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{"text/plain"}}}, nil
	}
	
	// Open should succeed
	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}
	
	// Setup for the createFileInfo query
	mockDriver.queryResponses["SELECT id FROM file_metadata WHERE path = ? AND type = 'file'"] = func(args []driver.Value) (driver.Rows, error) {
		if len(args) > 0 && args[0] == "test.txt" {
			return &mockRows{columns: []string{"id"}, rows: [][]driver.Value{{int64(1)}}}, nil
		}
		return &mockRows{columns: []string{}, rows: [][]driver.Value{}}, nil
	}
	
	// Setup for the SUM query in createFileInfo
	mockDriver.queryResponses["COALESCE(SUM(LENGTH(fragment))"] = func(args []driver.Value) (driver.Rows, error) {
		// Return 0 size
		return &mockRows{columns: []string{"sum"}, rows: [][]driver.Value{{int64(0)}}}, nil
	}
	
	// Setup for getTotalSize - return empty to trigger ErrNoRows, then EXISTS returns true
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	
	// Should return size 0 for file with no fragments
	if info.Size() != 0 {
		t.Fatalf("expected size 0, got %d", info.Size())
	}
}

// TestGetTotalSizeFileDoesNotExistMock tests when file doesn't exist at all
func TestGetTotalSizeFileDoesNotExistMock(t *testing.T) {
	mockDriver := NewSimpleMockDriver()
	
	// File doesn't exist
	mockDriver.queryResponses["SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)"] = func(args []driver.Value) (driver.Rows, error) {
		if len(args) > 0 && args[0] == "" {
			// Root always exists
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}
		// File doesn't exist
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{0}}}, nil
	}
	
	sql.Register("simple_mock2", mockDriver)
	db, err := sql.Open("simple_mock2", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	
	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Open should fail for non-existent file
	_, err = fs.Open("nonexistent.txt")
	if err == nil {
		t.Fatal("expected error for non-existent file")
	}
}

// TestSeekEndGetTotalSizeError tests Seek with io.SeekEnd when getTotalSize fails
func TestSeekEndGetTotalSizeError(t *testing.T) {
	mockDriver := NewSimpleMockDriver()
	
	// File exists
	mockDriver.queryResponses["SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}
	
	// Open succeeds
	mockDriver.queryResponses["SELECT mime_type FROM file_metadata"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{"text/plain"}}}, nil
	}
	
	sql.Register("simple_mock3", mockDriver)
	db, err := sql.Open("simple_mock3", "")
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
	
	// Make getTotalSize fail
	mockDriver.queryResponses["SELECT COUNT(*), COALESCE"] = func(args []driver.Value) (driver.Rows, error) {
		return nil, errors.New("database error")
	}
	
	// Try SeekEnd - should fail
	if seeker, ok := f.(io.Seeker); ok {
		_, err = seeker.Seek(0, io.SeekEnd)
		if err == nil || err.Error() != "database error" {
			t.Fatalf("expected database error, got %v", err)
		}
	}
}

// TestReadNoRowsNoDataMock tests Read when sql.ErrNoRows occurs with no data read
func TestReadNoRowsNoDataMock(t *testing.T) {
	mockDriver := NewSimpleMockDriver()
	
	// File exists
	mockDriver.queryResponses["SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}
	
	// getTotalSize returns some size
	mockDriver.queryResponses["SELECT COUNT(*), COALESCE"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"count", "size"}, rows: [][]driver.Value{{1, 100}}}, nil
	}
	
	sql.Register("simple_mock4", mockDriver)
	db, err := sql.Open("simple_mock4", "")
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
	
	// Read query returns no rows
	mockDriver.queryResponses["SELECT SUBSTR(fragment"] = func(args []driver.Value) (driver.Rows, error) {
		// Return empty to simulate sql.ErrNoRows
		return &mockRows{columns: []string{}, rows: [][]driver.Value{}}, nil
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