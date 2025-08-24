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

// Mock driver specifically for uncovered lines
type UncoveredLinesMockDriver struct {
	queryResponses map[string]func(args []driver.Value) (driver.Rows, error)
	execResponses  map[string]func(args []driver.Value) (driver.Result, error)
}

func NewUncoveredLinesMockDriver() *UncoveredLinesMockDriver {
	return &UncoveredLinesMockDriver{
		queryResponses: make(map[string]func(args []driver.Value) (driver.Rows, error)),
		execResponses:  make(map[string]func(args []driver.Value) (driver.Result, error)),
	}
}

func (d *UncoveredLinesMockDriver) Open(name string) (driver.Conn, error) {
	return &uncoveredMockConn{driver: d}, nil
}

type uncoveredMockConn struct {
	driver *UncoveredLinesMockDriver
}

func (c *uncoveredMockConn) Prepare(query string) (driver.Stmt, error) {
	return &uncoveredMockStmt{conn: c, query: query}, nil
}

func (c *uncoveredMockConn) Close() error { return nil }

func (c *uncoveredMockConn) Begin() (driver.Tx, error) {
	return &uncoveredMockTx{conn: c}, nil
}

type uncoveredMockTx struct {
	conn *uncoveredMockConn
}

func (tx *uncoveredMockTx) Commit() error   { return nil }
func (tx *uncoveredMockTx) Rollback() error { return nil }

type uncoveredMockStmt struct {
	conn  *uncoveredMockConn
	query string
}

func (s *uncoveredMockStmt) Close() error { return nil }
func (s *uncoveredMockStmt) NumInput() int {
	count := 0
	for _, ch := range s.query {
		if ch == '?' {
			count++
		}
	}
	return count
}

func (s *uncoveredMockStmt) Exec(args []driver.Value) (driver.Result, error) {
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

func (s *uncoveredMockStmt) Query(args []driver.Value) (driver.Rows, error) {
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

// Test for lines 108-110: EOF with bytes read
func TestReadEOFBytesReadMock(t *testing.T) {
	mockDriver := NewUncoveredLinesMockDriver()

	// File exists
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}

	mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{""}}}, nil
	}

	// getTotalSize returns small size
	mockDriver.queryResponses["COUNT(*), COALESCE"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"count", "size"}, rows: [][]driver.Value{{1, 10}}}, nil
	}

	sql.Register("uncovered1", mockDriver)
	db, err := sql.Open("uncovered1", "")
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

	// First read returns some data
	callCount := 0
	mockDriver.queryResponses["SELECT SUBSTR(fragment"] = func(args []driver.Value) (driver.Rows, error) {
		callCount++
		if callCount == 1 {
			// Return 5 bytes
			return &mockRows{columns: []string{"fragment"}, rows: [][]driver.Value{{[]byte("hello")}}}, nil
		}
		// Second read returns empty (EOF)
		return &mockRows{columns: []string{}, rows: [][]driver.Value{}}, nil
	}

	buf := make([]byte, 10)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("expected 5 bytes, got %d", n)
	}

	// Read again to hit EOF with f.offset >= f.size (lines 108-110)
	n, err = f.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// Test for lines 128-130: NoRows with bytes read
func TestReadNoRowsBytesReadMock(t *testing.T) {
	mockDriver := NewUncoveredLinesMockDriver()

	// File exists
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}

	mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{""}}}, nil
	}

	// getTotalSize returns larger size
	mockDriver.queryResponses["COUNT(*), COALESCE"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"count", "size"}, rows: [][]driver.Value{{2, 100}}}, nil
	}

	sql.Register("uncovered2", mockDriver)
	db, err := sql.Open("uncovered2", "")
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

	// First fragment returns data, second returns NoRows
	callCount := 0
	mockDriver.queryResponses["SELECT SUBSTR(fragment"] = func(args []driver.Value) (driver.Rows, error) {
		callCount++
		if callCount == 1 {
			// Return first fragment data
			return &mockRows{columns: []string{"fragment"}, rows: [][]driver.Value{{make([]byte, 4096)}}}, nil
		}
		// Second fragment doesn't exist (NoRows)
		return &mockRows{columns: []string{}, rows: [][]driver.Value{}}, nil
	}

	buf := make([]byte, 5000)
	n, err := f.Read(buf)
	// Should read first fragment then hit NoRows with bytesReadTotal > 0 (lines 128-130)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 4096 {
		t.Fatalf("expected 4096 bytes, got %d", n)
	}
}

// Test for lines 144-146: Empty fragment at EOF
func TestReadEmptyFragmentEOFMock(t *testing.T) {
	mockDriver := NewUncoveredLinesMockDriver()

	// File exists
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}

	mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{""}}}, nil
	}

	// File size is exactly one fragment
	mockDriver.queryResponses["COUNT(*), COALESCE"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"count", "size"}, rows: [][]driver.Value{{1, 4096}}}, nil
	}

	sql.Register("uncovered3", mockDriver)
	db, err := sql.Open("uncovered3", "")
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

	// Read returns empty fragment when at EOF
	callCount := 0
	mockDriver.queryResponses["SELECT SUBSTR(fragment"] = func(args []driver.Value) (driver.Rows, error) {
		callCount++
		if callCount == 1 {
			// First read returns full fragment
			return &mockRows{columns: []string{"fragment"}, rows: [][]driver.Value{{make([]byte, 4096)}}}, nil
		}
		// Second read returns empty fragment (lines 144-146)
		return &mockRows{columns: []string{"fragment"}, rows: [][]driver.Value{{[]byte{}}}}, nil
	}

	// Read full fragment
	buf := make([]byte, 4096)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 4096 {
		t.Fatalf("expected 4096 bytes, got %d", n)
	}

	// Try to read again - should hit empty fragment at EOF
	n, err = f.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// Test for lines 205-207: createFileInfo directory query error
func TestCreateFileInfoDirQueryErrorMock(t *testing.T) {
	mockDriver := NewUncoveredLinesMockDriver()

	// File exists
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		if len(args) > 0 && args[0] == "" {
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{0}}}, nil
	}

	// Directory check query fails
	mockDriver.queryResponses["SELECT 1 FROM file_metadata WHERE path = ?"] = func(args []driver.Value) (driver.Rows, error) {
		return nil, errors.New("dir query error")
	}

	sql.Register("uncovered4", mockDriver)
	db, err := sql.Open("uncovered4", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Open should fail with directory query error
	_, err = fs.Open("nonexistent")
	if err == nil || err.Error() != "dir query error" {
		t.Fatalf("expected 'dir query error', got %v", err)
	}
}

// Test for lines 520-522, 532-534, 538-540, 541-543: createFileInfo error paths
func TestCreateFileInfoVariousErrorsMock(t *testing.T) {
	t.Run("DirectoryQueryError", func(t *testing.T) {
		mockDriver := NewUncoveredLinesMockDriver()

		// Directory query returns error
		mockDriver.queryResponses["SELECT 1 FROM file_metadata WHERE path LIKE"] = func(args []driver.Value) (driver.Rows, error) {
			return nil, errors.New("dir scan error")
		}

		mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}

		mockDriver.queryResponses["SELECT type FROM file_metadata"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"type"}, rows: [][]driver.Value{{"dir"}}}, nil
		}

		sql.Register("uncovered5", mockDriver)
		db, err := sql.Open("uncovered5", "")
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

		// Stat should trigger createFileInfo with directory path
		_, err = f.Stat()
		if err == nil || err.Error() != "dir scan error" {
			t.Fatalf("expected 'dir scan error', got %v", err)
		}
	})

	t.Run("FileQueryError", func(t *testing.T) {
		mockDriver := NewUncoveredLinesMockDriver()

		mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}

		mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{""}}}, nil
		}

		// File query returns error
		mockDriver.queryResponses["SELECT id FROM file_metadata WHERE path = ? AND type = 'file'"] = func(args []driver.Value) (driver.Rows, error) {
			return nil, errors.New("file query error")
		}

		sql.Register("uncovered6", mockDriver)
		db, err := sql.Open("uncovered6", "")
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

		// Stat should fail with file query error
		_, err = f.Stat()
		if err == nil || err.Error() != "file query error" {
			t.Fatalf("expected 'file query error', got %v", err)
		}
	})

	t.Run("SumQueryError", func(t *testing.T) {
		mockDriver := NewUncoveredLinesMockDriver()

		mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}

		mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{""}}}, nil
		}

		mockDriver.queryResponses["SELECT id FROM file_metadata WHERE path = ? AND type = 'file'"] = func(args []driver.Value) (driver.Rows, error) {
			return &mockRows{columns: []string{"id"}, rows: [][]driver.Value{{int64(1)}}}, nil
		}

		// SUM query returns error
		mockDriver.queryResponses["COALESCE(SUM(LENGTH(fragment))"] = func(args []driver.Value) (driver.Rows, error) {
			return nil, errors.New("sum query error")
		}

		sql.Register("uncovered7", mockDriver)
		db, err := sql.Open("uncovered7", "")
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

		// Stat should fail with sum query error
		_, err = f.Stat()
		if err == nil || err.Error() != "sum query error" {
			t.Fatalf("expected 'sum query error', got %v", err)
		}
	})
}

// Test for lines 582-584: getTotalSize rows.Err()
func TestGetTotalSizeRowsErrorMock(t *testing.T) {
	mockDriver := NewUncoveredLinesMockDriver()

	// File exists
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}

	mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{""}}}, nil
	}

	// Return a rows that will have an error
	mockDriver.queryResponses["COUNT(*), COALESCE"] = func(args []driver.Value) (driver.Rows, error) {
		return &errorRows{}, nil
	}

	sql.Register("uncovered8", mockDriver)
	db, err := sql.Open("uncovered8", "")
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

	// Call Stat which calls getTotalSize
	info, err := f.Stat()
	if err == nil {
		t.Fatalf("expected error from rows.Err(), got info with size %d", info.Size())
	}
}

// errorRows is a mock Rows that returns an error from Err()
type errorRows struct{}

func (r *errorRows) Columns() []string { return []string{"count", "size"} }
func (r *errorRows) Close() error      { return nil }
func (r *errorRows) Next(dest []driver.Value) error {
	dest[0] = int64(1)
	dest[1] = int64(100)
	return io.EOF
}
func (r *errorRows) Err() error { return errors.New("rows error") }

// Test for line 589: createFileInfo file doesn't exist
func TestCreateFileInfoFileNotExistMock(t *testing.T) {
	mockDriver := NewUncoveredLinesMockDriver()

	// File exists for Open
	mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
	}

	mockDriver.queryResponses["SELECT mime_type"] = func(args []driver.Value) (driver.Rows, error) {
		return &mockRows{columns: []string{"mime_type"}, rows: [][]driver.Value{{""}}}, nil
	}

	// But file doesn't exist when createFileInfo queries
	mockDriver.queryResponses["SELECT id FROM file_metadata WHERE path = ? AND type = 'file'"] = func(args []driver.Value) (driver.Rows, error) {
		// Return no rows
		return &mockRows{columns: []string{}, rows: [][]driver.Value{}}, nil
	}

	sql.Register("uncovered9", mockDriver)
	db, err := sql.Open("uncovered9", "")
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

	// Stat should return ErrNotExist
	_, err = f.Stat()
	if err != os.ErrNotExist {
		t.Fatalf("expected os.ErrNotExist, got %v", err)
	}
}

// Test for lines 79-81, 92-94: SQLiteFS Open query errors
func TestSQLiteFSOpenErrorsMock(t *testing.T) {
	t.Run("ExistsQueryError", func(t *testing.T) {
		mockDriver := NewUncoveredLinesMockDriver()

		// EXISTS query fails
		mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
			if len(args) > 0 && args[0] != "" {
				return nil, errors.New("exists query failed")
			}
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
		}

		sql.Register("uncovered10", mockDriver)
		db, err := sql.Open("uncovered10", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Open should fail with exists query error (lines 79-81)
		_, err = fs.Open("test.txt")
		if err == nil || err.Error() != "exists query failed" {
			t.Fatalf("expected 'exists query failed', got %v", err)
		}
	})

	t.Run("TypeQueryError", func(t *testing.T) {
		mockDriver := NewUncoveredLinesMockDriver()

		// EXISTS returns false, so we check type
		mockDriver.queryResponses["SELECT EXISTS"] = func(args []driver.Value) (driver.Rows, error) {
			if len(args) > 0 && args[0] == "" {
				return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{1}}}, nil
			}
			return &mockRows{columns: []string{"exists"}, rows: [][]driver.Value{{0}}}, nil
		}

		// Type query fails
		mockDriver.queryResponses["SELECT type FROM file_metadata"] = func(args []driver.Value) (driver.Rows, error) {
			return nil, errors.New("type query failed")
		}

		sql.Register("uncovered11", mockDriver)
		db, err := sql.Open("uncovered11", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Open should fail with type query error (lines 92-94)
		_, err = fs.Open("test.txt")
		if err == nil || err.Error() != "type query failed" {
			t.Fatalf("expected 'type query failed', got %v", err)
		}
	})
}

// Test for line 183-185: Writer commit error
func TestWriterCommitErrorMock(t *testing.T) {
	mockDriver := NewUncoveredLinesMockDriver()

	// Make commit fail
	commitFail := false
	mockDriver.execResponses["COMMIT"] = func(args []driver.Value) (driver.Result, error) {
		if commitFail {
			return nil, errors.New("commit failed")
		}
		return &mockResult{}, nil
	}

	sql.Register("uncovered12", mockDriver)
	db, err := sql.Open("uncovered12", "")
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

	// Set commit to fail
	commitFail = true

	err = w.Close()
	if err == nil || err.Error() != "commit failed" {
		t.Fatalf("expected 'commit failed', got %v", err)
	}
}
