package tests

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"testing"

	"github.com/jilio/sqlitefs"
)

// TestDebugMockQueries helps us understand what queries are executed
func TestDebugMockQueries(t *testing.T) {
	driver := &DebugMockDriver{MockDriver: NewMockDriver()}
	sql.Register("debug_mock", driver)
	
	db, err := sql.Open("debug_mock", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fmt.Println("=== Creating SQLiteFS ===")
	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// First, let's create a file to test getTotalSize
	fmt.Println("\n=== Creating a file ===")
	w := fs.NewWriter("test.txt")
	w.Write([]byte("hello"))
	w.Close()
	
	fmt.Println("\n=== Opening test.txt ===")
	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("\n=== Calling Stat on test.txt ===")
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	
	fmt.Printf("\nStat result: %+v\n", info)
}

// DebugMockDriver wraps MockDriver to log all queries
type DebugMockDriver struct {
	*MockDriver
}

func (d *DebugMockDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.MockDriver.Open(name)
	if err != nil {
		return nil, err
	}
	return &debugConn{conn: conn.(*mockDriverConn)}, nil
}

type debugConn struct {
	conn *mockDriverConn
}

func (c *debugConn) Prepare(query string) (driver.Stmt, error) {
	fmt.Printf("PREPARE: %s\n", query)
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
		return nil, err
	}
	return &debugStmt{stmt: stmt.(*mockStmt), query: query}, nil
}

func (c *debugConn) Close() error {
	return c.conn.Close()
}

func (c *debugConn) Begin() (driver.Tx, error) {
	fmt.Println("BEGIN TRANSACTION")
	return c.conn.Begin()
}

type debugStmt struct {
	stmt  *mockStmt
	query string
}

func (s *debugStmt) Close() error {
	return s.stmt.Close()
}

func (s *debugStmt) NumInput() int {
	return s.stmt.NumInput()
}

func (s *debugStmt) Exec(args []driver.Value) (driver.Result, error) {
	fmt.Printf("EXEC: %s\n", s.query)
	fmt.Printf("  ARGS: %v\n", args)
	result, err := s.stmt.Exec(args)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
	}
	return result, err
}

func (s *debugStmt) Query(args []driver.Value) (driver.Rows, error) {
	fmt.Printf("QUERY: %s\n", s.query)
	fmt.Printf("  ARGS: %v\n", args)
	rows, err := s.stmt.Query(args)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
	}
	return rows, err
}