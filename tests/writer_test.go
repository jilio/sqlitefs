package tests

import (
	"database/sql"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// Tests for Writer functionality

// TestWriteToClosedWriter tests writing to a closed writer
func TestWriteToClosedWriter(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)
	writer := fs.NewWriter("test.txt")

	// Close the writer
	writer.Close()

	// Try to write - should get error
	_, err = writer.Write([]byte("test"))
	if err == nil {
		t.Error("Expected error writing to closed writer")
	}

	if err.Error() != "sqlitefs: write to closed writer" {
		t.Errorf("Expected 'write to closed writer' error, got %v", err)
	}
}
