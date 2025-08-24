package tests

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/jilio/sqlitefs"
)

// LastPushMockDriver for the final uncovered lines
type LastPushMockDriver struct {
	queryResponses map[string]func(args []driver.Value) (driver.Rows, error)
	execResponses  map[string]func(args []driver.Value) (driver.Result, error)
}

func NewLastPushMockDriver() *LastPushMockDriver {
	return &LastPushMockDriver{
		queryResponses: make(map[string]func(args []driver.Value) (driver.Rows, error)),
		execResponses:  make(map[string]func(args []driver.Value) (driver.Result, error)),
	}
}

func (d *LastPushMockDriver) Open(name string) (driver.Conn, error) {
	return &lastPushMockConn{driver: d}, nil
}

type lastPushMockConn struct {
	driver *LastPushMockDriver
}

func (c *lastPushMockConn) Prepare(query string) (driver.Stmt, error) {
	return &lastPushMockStmt{conn: c, query: query}, nil
}

func (c *lastPushMockConn) Close() error { return nil }

func (c *lastPushMockConn) Begin() (driver.Tx, error) {
	return &lastPushMockTx{conn: c}, nil
}

type lastPushMockTx struct {
	conn *lastPushMockConn
}

func (tx *lastPushMockTx) Commit() error   { return nil }
func (tx *lastPushMockTx) Rollback() error { return nil }

type lastPushMockStmt struct {
	conn  *lastPushMockConn
	query string
}

func (s *lastPushMockStmt) Close() error { return nil }
func (s *lastPushMockStmt) NumInput() int {
	count := 0
	for _, ch := range s.query {
		if ch == '?' {
			count++
		}
	}
	return count
}

func (s *lastPushMockStmt) Exec(args []driver.Value) (driver.Result, error) {
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
	
	// Default for INSERT/UPDATE
	if contains(s.query, "INSERT") || contains(s.query, "UPDATE") {
		return &mockResult{}, nil
	}
	
	return &mockResult{}, nil
}

func (s *lastPushMockStmt) Query(args []driver.Value) (driver.Rows, error) {
	// Check for custom handler
	for pattern, handler := range s.conn.driver.queryResponses {
		if contains(s.query, pattern) {
			return handler(args)
		}
	}
	
	// Default handlers
	if contains(s.query, "SELECT EXISTS") && contains(s.query, "file_metadata") {
		if len(args) > 0 && args[0] == "" {
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{0}}}, nil
	}
	
	return &mockRows{columns: []string{}, rows: [][]driver.Value{}}, nil
}

// Test for lines 144-146: Empty fragment at EOF
func TestEmptyFragmentAtEOF(t *testing.T) {
	mockDriver := NewLastPushMockDriver()
	
	// File exists
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}
	
	mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{""}}}, nil
	}
	
	// File size equals offset
	mockDriver.queryResponses["COUNT(*), COALESCE"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"count", "size"}, rows: [][]driver.Value{{1, 100}}}, nil
	}
	
	sql.Register("lastpush1", mockDriver)
	db, err := sql.Open("lastpush1", "")
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
	
	// Set up query to return data then empty fragment
	callCount := 0
	mockDriver.queryResponses["SELECT SUBSTR(fragment"] = func(args []driver.Value) (driver.Rows, error) {
		callCount++
		if callCount == 1 {
			// Return all 100 bytes
			return &mockRows{columns: []string{"fragment"}, rows: [][]driver.Value{{make([]byte, 100)}}}, nil
		}
		// Return empty fragment when offset == size (lines 144-146)
		return &mockRows{columns: []string{"fragment"}, rows: [][]driver.Value{{[]byte{}}}}, nil
	}
	
	// Read all content
	buf := make([]byte, 200)
	n, err := f.Read(buf)
	if n != 100 {
		t.Fatalf("expected 100 bytes, got %d", n)
	}
	
	// Try to read again - should hit empty fragment at EOF
	n, err = f.Read(buf)
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// Test for lines 205-207, 228-230, 249-251, 254-255, 280-282, 315-317, 324-326, 332-334, 363-365, 386-388, 407-409, 412-413, 437-439, 448-450, 458-460, 461-463
// These are all error paths in ReadDir/Readdir that are hard to trigger
func TestAllRemainingErrorPaths(t *testing.T) {
	t.Run("CreateFileInfoDirQueryError", func(t *testing.T) {
		mockDriver := NewLastPushMockDriver()
		
		// Directory check fails (lines 205-207)
		mockDriver.queryResponses["SELECT 1 FROM file_metadata WHERE path = ?"] = func(args []driver.Value) (driver.Rows, error) {
			return nil, errors.New("dir check failed")
		}
		
		mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
			if len(args) > 0 && args[0] == "" {
				return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
			}
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{0}}}, nil
		}
		
		sql.Register("lastpush2", mockDriver)
		db, err := sql.Open("lastpush2", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}
		
		_, err = fs.Open("test")
		if err == nil || err.Error() != "dir check failed" {
			t.Fatalf("expected 'dir check failed', got %v", err)
		}
	})
	
	t.Run("SQLiteFSOpenErrors", func(t *testing.T) {
		mockDriver := NewLastPushMockDriver()
		
		// EXISTS query error (lines 79-81)
		mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
			if len(args) > 0 && args[0] != "" {
				return nil, errors.New("exists failed")
			}
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}
		
		sql.Register("lastpush3", mockDriver)
		db, err := sql.Open("lastpush3", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}
		
		_, err = fs.Open("test")
		if err == nil || err.Error() != "exists failed" {
			t.Fatalf("expected 'exists failed', got %v", err)
		}
	})
	
	t.Run("SQLiteFSTypeQueryError", func(t *testing.T) {
		mockDriver := NewLastPushMockDriver()
		
		// EXISTS returns false
		mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
			if len(args) > 0 && args[0] == "" {
				return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
			}
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{0}}}, nil
		}
		
		// Type query fails (lines 92-94)
		mockDriver.queryResponses["SELECT type FROM file_metadata"] = func(args []driver.Value) (driver.Rows, error) {
			return nil, errors.New("type failed")
		}
		
		sql.Register("lastpush4", mockDriver)
		db, err := sql.Open("lastpush4", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}
		
		_, err = fs.Open("test")
		if err == nil || err.Error() != "type failed" {
			t.Fatalf("expected 'type failed', got %v", err)
		}
	})
	
	t.Run("WriterCommitError", func(t *testing.T) {
		mockDriver := NewLastPushMockDriver()
		
		// Commit fails (lines 183-185)
		commitFail := false
		mockDriver.execResponses["COMMIT"] = func(args []driver.Value) (driver.Result, error) {
			if commitFail {
				return nil, errors.New("commit failed")
			}
			return &mockResult{}, nil
		}
		
		sql.Register("lastpush5", mockDriver)
		db, err := sql.Open("lastpush5", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}
		
		w := fs.NewWriter("test.txt")
		w.Write([]byte("data"))
		
		commitFail = true
		err = w.Close()
		if err == nil || err.Error() != "commit failed" {
			t.Fatalf("expected 'commit failed', got %v", err)
		}
	})
	
	t.Run("CreateFileInfoErrors", func(t *testing.T) {
		mockDriver := NewLastPushMockDriver()
		
		mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}
		
		mockDriver.queryResponses["SELECT type FROM file_metadata"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"type"}, rows: [][]driver.Value{{"dir"}}}, nil
		}
		
		// Directory query error (lines 520-522)
		mockDriver.queryResponses["SELECT 1 FROM file_metadata WHERE path LIKE"] = func(args []driver.Value) (driver.Rows, error) {
			return nil, errors.New("dir query failed")
		}
		
		sql.Register("lastpush6", mockDriver)
		db, err := sql.Open("lastpush6", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}
		
		f, err := fs.Open("")
		if err != nil {
			t.Fatal(err)
		}
		
		_, err = f.Stat()
		if err == nil || err.Error() != "dir query failed" {
			t.Fatalf("expected 'dir query failed', got %v", err)
		}
	})
	
	t.Run("CreateFileInfoFileQueryError", func(t *testing.T) {
		mockDriver := NewLastPushMockDriver()
		
		mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}
		
		mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{""}}}, nil
		}
		
		// File query error (lines 532-534)
		mockDriver.queryResponses["SELECT id FROM file_metadata WHERE path = ? AND type = 'file'"] = func(args []driver.Value) (driver.Rows, error) {
			return nil, errors.New("file query failed")
		}
		
		sql.Register("lastpush7", mockDriver)
		db, err := sql.Open("lastpush7", "")
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
		
		_, err = f.Stat()
		if err == nil || err.Error() != "file query failed" {
			t.Fatalf("expected 'file query failed', got %v", err)
		}
	})
	
	t.Run("CreateFileInfoSumError", func(t *testing.T) {
		mockDriver := NewLastPushMockDriver()
		
		mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}
		
		mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{""}}}, nil
		}
		
		mockDriver.queryResponses["SELECT id FROM file_metadata WHERE path = ? AND type = 'file'"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"id"}, rows: [][]driver.Value{{int64(1)}}}, nil
		}
		
		// SUM query error (lines 538-540, 541-543)
		mockDriver.queryResponses["COALESCE(SUM(LENGTH(fragment))"] = func(args []driver.Value) (driver.Rows, error) {
			return nil, errors.New("sum failed")
		}
		
		sql.Register("lastpush8", mockDriver)
		db, err := sql.Open("lastpush8", "")
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
		
		_, err = f.Stat()
		if err == nil || err.Error() != "sum failed" {
			t.Fatalf("expected 'sum failed', got %v", err)
		}
	})
	
	t.Run("GetTotalSizeRowsErr", func(t *testing.T) {
		mockDriver := NewLastPushMockDriver()
		
		mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}
		
		mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{""}}}, nil
		}
		
		// Return rows that will error (lines 582-584)
		mockDriver.queryResponses["COUNT(*), COALESCE"] = func(args []driver.Value) (driver.Rows, error) {
			return &rowsWithError{}, nil
		}
		
		sql.Register("lastpush9", mockDriver)
		db, err := sql.Open("lastpush9", "")
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
		
		// getTotalSize should fail with rows.Err()
		// This is called internally by Stat
		_, err = f.Stat()
		if err == nil || err.Error() != "rows error" {
			t.Fatalf("expected 'rows error', got %v", err)
		}
	})
}

// rowsWithError returns an error from Err()
type rowsWithError struct{}

func (r *rowsWithError) Columns() []string { return []string{"count", "size"} }
func (r *rowsWithError) Close() error      { return nil }
func (r *rowsWithError) Next(dest []driver.Value) error {
	dest[0] = int64(1)
	dest[1] = int64(100)
	return nil
}
func (r *rowsWithError) Err() error { return errors.New("rows error") }