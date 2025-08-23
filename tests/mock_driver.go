package tests

import (
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"sync"
)

// MockDriver is a custom SQL driver for testing
type MockDriver struct {
	mu         sync.RWMutex
	errorRules map[string]error
	data       map[string][][]driver.Value // Table -> rows
}

func NewMockDriver() *MockDriver {
	return &MockDriver{
		errorRules: make(map[string]error),
		data:       make(map[string][][]driver.Value),
	}
}

// SetError sets an error to return for queries containing the pattern
func (d *MockDriver) SetError(pattern string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.errorRules[pattern] = err
}

// ClearErrors removes all error rules
func (d *MockDriver) ClearErrors() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.errorRules = make(map[string]error)
}

// Open returns a new connection to the database
func (d *MockDriver) Open(name string) (driver.Conn, error) {
	return &mockDriverConn{driver: d, data: make(map[string][][]driver.Value)}, nil
}

type mockDriverConn struct {
	driver *MockDriver
	data   map[string][][]driver.Value
}

func (c *mockDriverConn) Prepare(query string) (driver.Stmt, error) {
	// Check if this query should error
	c.driver.mu.RLock()
	for pattern, err := range c.driver.errorRules {
		if strings.Contains(query, pattern) {
			c.driver.mu.RUnlock()
			return nil, err
		}
	}
	c.driver.mu.RUnlock()

	return &mockStmt{conn: c, query: query}, nil
}

func (c *mockDriverConn) Close() error {
	return nil
}

func (c *mockDriverConn) Begin() (driver.Tx, error) {
	// Check if BEGIN should error
	c.driver.mu.RLock()
	defer c.driver.mu.RUnlock()

	for pattern, err := range c.driver.errorRules {
		if strings.Contains("BEGIN", pattern) {
			return nil, err
		}
	}

	return &mockTx{conn: c}, nil
}

type mockTx struct {
	conn *mockDriverConn
}

func (tx *mockTx) Commit() error {
	return nil
}

func (tx *mockTx) Rollback() error {
	return nil
}

type mockStmt struct {
	conn  *mockDriverConn
	query string
}

func (s *mockStmt) Close() error {
	return nil
}

func (s *mockStmt) NumInput() int {
	// Count ? in query
	return strings.Count(s.query, "?")
}

func (s *mockStmt) Exec(args []driver.Value) (driver.Result, error) {
	// Check for errors
	s.conn.driver.mu.RLock()
	for pattern, err := range s.conn.driver.errorRules {
		if strings.Contains(s.query, pattern) {
			s.conn.driver.mu.RUnlock()
			return nil, err
		}
	}
	s.conn.driver.mu.RUnlock()

	// Handle CREATE TABLE
	if strings.Contains(s.query, "CREATE TABLE") {
		return &mockResult{}, nil
	}

	// Handle INSERT
	if strings.Contains(s.query, "INSERT") {
		return &mockResult{lastInsertId: 1, rowsAffected: 1}, nil
	}

	// Handle DELETE
	if strings.Contains(s.query, "DELETE") {
		return &mockResult{rowsAffected: 1}, nil
	}

	return &mockResult{}, nil
}

func (s *mockStmt) Query(args []driver.Value) (driver.Rows, error) {
	// Check for errors first
	s.conn.driver.mu.RLock()
	for pattern, err := range s.conn.driver.errorRules {
		if strings.Contains(s.query, pattern) {
			s.conn.driver.mu.RUnlock()
			return nil, err
		}
	}
	s.conn.driver.mu.RUnlock()

	// Handle different query types
	if strings.Contains(s.query, "SELECT EXISTS") {
		// File existence check - check if we should return false
		if strings.Contains(s.query, "nonexistent") {
			return &mockRows{
				columns: []string{"exists"},
				rows:    [][]driver.Value{{0}}, // File doesn't exist
			}, nil
		}
		return &mockRows{
			columns: []string{"exists"},
			rows:    [][]driver.Value{{1}}, // File exists
		}, nil
	}

	if strings.Contains(s.query, "SUM(LENGTH(fragment))") {
		// getTotalSize query
		// Return no rows to trigger sql.ErrNoRows
		return &mockRows{
			columns: []string{"sum"},
			rows:    [][]driver.Value{}, // No rows - will cause ErrNoRows
		}, nil
	}

	if strings.Contains(s.query, "COUNT(*)") && strings.Contains(s.query, "LENGTH(fragment)") {
		// getTotalSize alternative query
		return &mockRows{
			columns: []string{"count", "length"},
			rows:    [][]driver.Value{{0, 0}}, // No fragments
		}, nil
	}

	if strings.Contains(s.query, "SELECT fragment FROM file_fragments") {
		// Read query
		return &mockRows{
			columns: []string{"fragment"},
			rows:    [][]driver.Value{{[]byte("test data")}},
		}, nil
	}

	if strings.Contains(s.query, "SELECT path") && strings.Contains(s.query, "LIKE") {
		// ReadDir query
		return &mockRows{
			columns: []string{"path", "type"},
			rows: [][]driver.Value{
				{"dir/file1.txt", "file"},
				{"dir/file2.txt", "file"},
				{"dir/subdir/", "dir"},
			},
		}, nil
	}

	// Default empty result
	return &mockRows{
		columns: []string{},
		rows:    [][]driver.Value{},
	}, nil
}

type mockResult struct {
	lastInsertId int64
	rowsAffected int64
}

func (r *mockResult) LastInsertId() (int64, error) {
	return r.lastInsertId, nil
}

func (r *mockResult) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

type mockRows struct {
	columns []string
	rows    [][]driver.Value
	pos     int
}

func (r *mockRows) Columns() []string {
	return r.columns
}

func (r *mockRows) Close() error {
	return nil
}

func (r *mockRows) Next(dest []driver.Value) error {
	if r.pos >= len(r.rows) {
		return io.EOF
	}

	row := r.rows[r.pos]
	r.pos++

	for i, v := range row {
		if i < len(dest) {
			dest[i] = v
		}
	}

	return nil
}

// Register the driver
func init() {
	sql.Register("mockdb", NewMockDriver())
}

// Global mock driver instance for test control
var MockDriverInstance = NewMockDriver()

func init() {
	sql.Register("mockdb-controlled", MockDriverInstance)
}
