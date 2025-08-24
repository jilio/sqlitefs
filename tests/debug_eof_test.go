package tests

import (
	"database/sql"
	"fmt"
	"io"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

func TestDebugEOF(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file with exactly 4096 bytes
	content := make([]byte, 4096)
	for i := range content {
		content[i] = byte(i % 256)
	}

	w := fs.NewWriter("test.txt")
	w.Write(content)
	w.Close()

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	sqlFile := f.(*sqlitefs.SQLiteFile)

	// Read 2000 bytes
	buf := make([]byte, 2000)
	n, err := f.Read(buf)
	fmt.Printf("Read 1: n=%d, err=%v, offset after=%d\n", n, err, sqlFile.GetOffset())

	// Read another 2000 bytes
	n, err = f.Read(buf)
	fmt.Printf("Read 2: n=%d, err=%v, offset after=%d\n", n, err, sqlFile.GetOffset())

	// Read last 96 bytes
	n, err = f.Read(buf)
	fmt.Printf("Read 3: n=%d, err=%v, offset after=%d, size=%d\n", n, err, sqlFile.GetOffset(), sqlFile.GetSize())

	if err != io.EOF {
		t.Errorf("Expected io.EOF on last read, got %v", err)
	}
	if n != 96 {
		t.Errorf("Expected 96 bytes, got %d", n)
	}
}
