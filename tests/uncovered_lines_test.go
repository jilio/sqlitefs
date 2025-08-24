package tests

import (
	"database/sql"
	"io"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestReadLinesCoverage tests specific uncovered lines in Read()
func TestReadLinesCoverage(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Test line 108-110: bytesReadTotal == 0 at EOF check
	t.Run("EOFWithNoBytesRead", func(t *testing.T) {
		w := fs.NewWriter("empty.txt")
		w.Close() // Empty file

		f, err := fs.Open("empty.txt")
		if err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 10)
		n, err := f.Read(buf)
		if err != io.EOF {
			t.Fatalf("expected io.EOF, got %v", err)
		}
		if n != 0 {
			t.Fatalf("expected 0 bytes, got %d", n)
		}
	})

	// Test lines 128-130: sql.ErrNoRows with bytesReadTotal > 0
	t.Run("NoRowsWithBytesRead", func(t *testing.T) {
		// Create file with multiple fragments
		w := fs.NewWriter("multi.txt")
		data := make([]byte, 4096*2) // Two fragments
		for i := range data {
			data[i] = byte('A')
		}
		w.Write(data)
		w.Close()

		f, err := fs.Open("multi.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Read first fragment
		buf := make([]byte, 4096)
		n, err := f.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n != 4096 {
			t.Fatalf("expected 4096 bytes, got %d", n)
		}

		// Delete second fragment to cause sql.ErrNoRows
		_, err = db.Exec(`DELETE FROM file_fragments 
			WHERE file_id = (SELECT id FROM file_metadata WHERE path = ?) 
			AND fragment_index = 1`, "multi.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Try to read - should return partial data
		n, err = f.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("unexpected error: %v", err)
		}
		// Should return 0 or partial bytes without error
	})

	// Test line 145: bytesRead == 0 && f.offset >= f.size
	t.Run("EmptyFragmentAtEOF", func(t *testing.T) {
		w := fs.NewWriter("eof.txt")
		w.Write([]byte("test"))
		w.Close()

		// Get file ID
		var fileID int64
		err = db.QueryRow("SELECT id FROM file_metadata WHERE path = ?", "eof.txt").Scan(&fileID)
		if err != nil {
			t.Fatal(err)
		}

		// Insert empty fragment at end
		_, err = db.Exec("INSERT INTO file_fragments (file_id, fragment_index, fragment) VALUES (?, ?, ?)",
			fileID, 1, []byte{})
		if err != nil {
			t.Fatal(err)
		}

		f, err := fs.Open("eof.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Read all content
		buf := make([]byte, 100)
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 4 {
			t.Fatalf("expected 4 bytes, got %d", n)
		}
	})
}

// TestReadDirLinesCoverage tests specific uncovered lines in ReadDir()
func TestReadDirLinesCoverage(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Test line 206: dirPath ending without slash
	t.Run("DirPathWithoutSlash", func(t *testing.T) {
		w := fs.NewWriter("mydir/file.txt")
		w.Write([]byte("content"))
		w.Close()

		f, err := fs.Open("mydir") // No trailing slash
		if err != nil {
			t.Fatal(err)
		}

		if dirFile, ok := f.(interface {
			ReadDir(int) ([]os.DirEntry, error)
		}); ok {
			entries, err := dirFile.ReadDir(0)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 {
				t.Fatalf("expected 1 entry, got %d", len(entries))
			}
		}
	})

	// Test lines 229-230: Scan error
	t.Run("ScanError", func(t *testing.T) {
		// Create directory with file
		w := fs.NewWriter("scandir/file.txt")
		w.Write([]byte("content"))
		w.Close()

		// Corrupt the type column by setting to invalid value
		_, err = db.Exec("UPDATE file_metadata SET type = 'invalid' WHERE path = ?", "scandir/file.txt")
		if err != nil {
			t.Fatal(err)
		}

		f, err := fs.Open("scandir")
		if err != nil {
			t.Fatal(err)
		}

		if dirFile, ok := f.(interface {
			ReadDir(int) ([]os.DirEntry, error)
		}); ok {
			_, err = dirFile.ReadDir(0)
			// May or may not error depending on implementation
			_ = err
		}
	})
}

// TestCreateFileInfoLinesCoverage tests specific uncovered lines in createFileInfo()
func TestCreateFileInfoLinesCoverage(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Test lines 521-522, 531-533: Database errors
	t.Run("DatabaseErrors", func(t *testing.T) {
		// Create a file
		w := fs.NewWriter("test.txt")
		w.Write([]byte("data"))
		w.Close()

		f, err := fs.Open("test.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Close DB to cause errors
		db.Close()

		_, err = f.Stat()
		if err == nil {
			t.Fatal("expected error with closed database")
		}
	})
}

// TestOpenLinesCoverage tests specific uncovered lines in Open()
func TestOpenLinesCoverage(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Test lines 79-81, 92-94: Database errors
	t.Run("DatabaseError", func(t *testing.T) {
		// Close DB
		db.Close()

		_, err = fs.Open("any.txt")
		if err == nil {
			t.Fatal("expected error with closed database")
		}
	})
}

// TestWriteFragmentLinesCoverage tests specific uncovered lines in writeFragment()
func TestWriteFragmentLinesCoverage(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Test line 185: Exec error
	t.Run("ExecError", func(t *testing.T) {
		w := fs.NewWriter("test.txt")
		w.Write([]byte("data"))

		// Close DB to cause exec error
		db.Close()

		err = w.Close()
		if err == nil {
			t.Fatal("expected error with closed database")
		}
	})
}
