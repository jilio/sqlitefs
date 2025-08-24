package tests

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"

	"github.com/jilio/sqlitefs"
)

func TestDebugMockExec(t *testing.T) {
	mockDriver := NewSimpleMockDriver()

	// Allow metadata insert to succeed
	mockDriver.execResponses["INSERT OR REPLACE INTO file_metadata"] = func(args []driver.Value) (driver.Result, error) {
		fmt.Println("Exec: INSERT OR REPLACE INTO file_metadata")
		return &mockResult{lastInsertId: 1, rowsAffected: 1}, nil
	}

	// Make fragment INSERT fail (it's INSERT OR REPLACE)
	mockDriver.execResponses["INSERT OR REPLACE INTO file_fragments"] = func(args []driver.Value) (driver.Result, error) {
		fmt.Println("Exec: INSERT OR REPLACE INTO file_fragments - returning error")
		return nil, errors.New("exec failed")
	}

	// Need to handle the SELECT id query that happens in writeFragment
	mockDriver.queryResponses["SELECT id FROM file_metadata WHERE path = ?"] = func(args []driver.Value) (driver.Rows, error) {
		fmt.Printf("Query: SELECT id FROM file_metadata WHERE path = %v\n", args)
		return &mockRows{columns: []string{"id"}, rows: [][]driver.Value{{int64(1)}}}, nil
	}

	// The driver is already set up, no need to wrap it

	sql.Register("debug_exec", mockDriver)
	db, err := sql.Open("debug_exec", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("Creating writer...")
	w := fs.NewWriter("test.txt")

	fmt.Println("Writing data...")
	_, err = w.Write([]byte("data"))
	if err != nil {
		fmt.Printf("Write error: %v\n", err)
	}

	fmt.Println("Closing writer...")
	err = w.Close()
	fmt.Printf("Close error: %v\n", err)

	if err == nil || err.Error() != "exec failed" {
		t.Fatalf("expected 'exec failed', got %v", err)
	}
}

type debugMockConn struct {
	conn driver.Conn
}

func (c *debugMockConn) Prepare(query string) (driver.Stmt, error) {
	fmt.Printf("Prepare: %s\n", query)
	return c.conn.Prepare(query)
}

func (c *debugMockConn) Close() error {
	return c.conn.Close()
}

func (c *debugMockConn) Begin() (driver.Tx, error) {
	return c.conn.Begin()
}
