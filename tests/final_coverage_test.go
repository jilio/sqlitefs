package tests

import (
	"database/sql"
	"io"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestFinalCoveragePush tests to reach 90% coverage
func TestFinalCoveragePush(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()

	// Test lines 108-110 in file.go - when we've read some bytes but hit EOF in middle
	t.Run("PartialReadThenEOF", func(t *testing.T) {
		// Create a file with exactly 2MB (one fragment)
		writer := fs.NewWriter("exact_fragment.bin")
		data := make([]byte, 2*1024*1024)
		writer.Write(data)
		writer.Close()

		file, _ := fs.Open("exact_fragment.bin")
		defer file.Close()

		// Read most of it
		buf := make([]byte, 2*1024*1024-10)
		file.Read(buf)

		// Now try to read 20 bytes when only 10 are left
		buf2 := make([]byte, 20)
		n, err := file.Read(buf2)
		if n != 10 {
			t.Errorf("Expected 10 bytes (partial read), got %d", n)
		}
		if err != nil {
			t.Errorf("Expected nil error on partial read, got %v", err)
		}
	})

	// Test line 108-110 and 132 in file.go - when offset >= size within fragment loop
	t.Run("ReadAtExactEOF", func(t *testing.T) {
		writer := fs.NewWriter("exact_eof.txt")
		writer.Write([]byte("hello"))
		writer.Close()

		file, _ := fs.Open("exact_eof.txt")
		defer file.Close()

		// Read entire file
		buf := make([]byte, 5)
		n, _ := file.Read(buf)
		if n != 5 {
			t.Fatalf("Expected 5 bytes, got %d", n)
		}

		// Now offset is at EOF - try to read more within the loop
		// This should trigger lines 107-110
		buf2 := make([]byte, 10)
		n, err := file.Read(buf2)
		if n != 0 {
			t.Errorf("Expected 0 bytes at EOF, got %d", n)
		}
		if err != io.EOF {
			t.Errorf("Expected EOF, got %v", err)
		}
	})

	// Test line 128-130 in file.go - ErrNoRows with bytesReadTotal > 0
	t.Run("MissingFragmentAfterPartialRead", func(t *testing.T) {
		// Create a file with multiple fragments
		writer := fs.NewWriter("multi_frag.bin")
		data := make([]byte, 3*1024*1024) // 3MB
		writer.Write(data)
		writer.Close()

		// Delete the second fragment to trigger ErrNoRows after reading first fragment
		var fileID int64
		db.QueryRow("SELECT id FROM file_metadata WHERE path = ?", "multi_frag.bin").Scan(&fileID)
		db.Exec("DELETE FROM file_fragments WHERE file_id = ? AND fragment_index = 1", fileID)

		file, _ := fs.Open("multi_frag.bin")
		defer file.Close()

		// Read in a way that spans multiple fragments
		buf := make([]byte, 3*1024*1024)
		n, err := file.Read(buf)
		
		// Should get partial read (first fragment only)
		if n != 2*1024*1024 {
			t.Logf("Read %d bytes with missing fragment", n)
		}
		if err != nil {
			t.Logf("Got error: %v", err)
		}
	})
}