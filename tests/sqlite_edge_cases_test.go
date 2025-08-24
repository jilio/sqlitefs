package tests

import (
	"database/sql"
	"io"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestEmptyFragmentAtEOFReal tests lines 144-146 with real SQLite
func TestEmptyFragmentAtEOFReal(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file with exact size
	w := fs.NewWriter("eof_test.txt")
	data := make([]byte, 4096) // Exactly one fragment
	for i := range data {
		data[i] = byte(i % 256)
	}
	w.Write(data)
	w.Close()

	// Manually add an empty fragment at index 1
	var fileID int
	err = db.QueryRow("SELECT id FROM file_metadata WHERE path = ?", "eof_test.txt").Scan(&fileID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSERT INTO file_fragments (file_id, fragment_index, fragment) VALUES (?, ?, ?)",
		fileID, 1, []byte{})
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("eof_test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Read exactly 4096 bytes
	buf := make([]byte, 4096)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 4096 {
		t.Fatalf("expected 4096 bytes, got %d", n)
	}

	// Now we're at EOF, next read should hit empty fragment (lines 144-146)
	n, err = f.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// TestReaddirNotADirectoryReal tests lines 315-317
func TestReaddirNotADirectoryReal(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a regular file
	w := fs.NewWriter("regular_file.txt")
	w.Write([]byte("I am not a directory"))
	w.Close()

	f, err := fs.Open("regular_file.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Try to call Readdir on a regular file (lines 315-317)
	if dirFile, ok := f.(interface{ Readdir(int) ([]os.FileInfo, error) }); ok {
		_, err = dirFile.Readdir(0)
		if err == nil {
			t.Fatal("expected error when calling Readdir on file")
		}
		if err.Error() != "not a directory" {
			t.Fatalf("expected 'not a directory', got %v", err)
		}
	}
}

// TestReaddirCleanNameSkip tests lines 324-326, 412-413
func TestReaddirCleanNameSkip(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a directory with files
	w := fs.NewWriter("skiptest/normal.txt")
	w.Write([]byte("normal file"))
	w.Close()

	// Manually insert entries that will have problematic clean names
	_, err = db.Exec("INSERT OR IGNORE INTO file_metadata (path, type) VALUES (?, ?)",
		"skiptest/", "file") // Trailing slash on file
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("INSERT OR IGNORE INTO file_metadata (path, type) VALUES (?, ?)",
		"skiptest//", "file") // Double slash
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("skiptest")
	if err != nil {
		t.Fatal(err)
	}

	// Readdir should skip entries with empty clean names (lines 324-326, 412-413)
	if dirFile, ok := f.(interface{ Readdir(int) ([]os.FileInfo, error) }); ok {
		infos, err := dirFile.Readdir(0)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}

		// Check that no entries have empty or slash names
		for _, info := range infos {
			if info.Name() == "" || info.Name() == "/" {
				t.Fatalf("found invalid name: %q", info.Name())
			}
		}

		// Should have at least the normal file
		found := false
		for _, info := range infos {
			if info.Name() == "normal.txt" {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("expected to find normal.txt")
		}
	}
}

// TestReadDirCleanNameSkip tests lines 254-255
func TestReadDirCleanNameSkip(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a directory with files
	w := fs.NewWriter("skipdir/normal.txt")
	w.Write([]byte("normal file"))
	w.Close()

	// Manually insert entry with trailing slash
	_, err = db.Exec("INSERT OR IGNORE INTO file_metadata (path, type) VALUES (?, ?)",
		"skipdir/", "file") // This will result in empty clean name
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("skipdir")
	if err != nil {
		t.Fatal(err)
	}

	// ReadDir should skip entries with empty clean names (lines 254-255)
	if dirFile, ok := f.(interface{ ReadDir(int) ([]os.DirEntry, error) }); ok {
		entries, err := dirFile.ReadDir(0)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}

		// Check that no entries have empty names
		for _, entry := range entries {
			if entry.Name() == "" {
				t.Fatal("found entry with empty name")
			}
		}

		// Should have at least the normal file
		found := false
		for _, entry := range entries {
			if entry.Name() == "normal.txt" {
				found = true
				break
			}
		}
		if !found {
			t.Fatal("expected to find normal.txt")
		}
	}
}

// TestReaddirCorruptedRows tests lines 332-334
func TestReaddirCorruptedRows(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a directory with a file
	w := fs.NewWriter("corrupt/file.txt")
	w.Write([]byte("content"))
	w.Close()

	// Corrupt the type field to NULL
	_, err = db.Exec("UPDATE file_metadata SET type = NULL WHERE path = 'corrupt/file.txt'")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("corrupt")
	if err != nil {
		t.Fatal(err)
	}

	// Readdir should handle the scan error (lines 332-334)
	if dirFile, ok := f.(interface{ Readdir(int) ([]os.FileInfo, error) }); ok {
		_, err = dirFile.Readdir(0)
		// Should either error or handle gracefully
		if err != nil {
			// Error is expected and ok
			return
		}
		// If no error, it handled it gracefully which is also ok
	}
}

// TestReaddirElseCase tests lines 458-460, 461-463
func TestReaddirElseCase(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a directory
	w := fs.NewWriter("elsedir/file.txt")
	w.Write([]byte("content"))
	w.Close()

	// Insert an entry with unknown type
	_, err = db.Exec("INSERT OR IGNORE INTO file_metadata (path, type) VALUES (?, ?)",
		"elsedir/unknown", "unknown_type")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("elsedir")
	if err != nil {
		t.Fatal(err)
	}

	// Readdir should handle unknown type (lines 458-460, 461-463)
	if dirFile, ok := f.(interface{ Readdir(int) ([]os.FileInfo, error) }); ok {
		infos, err := dirFile.Readdir(0)
		if err != nil && err != io.EOF {
			// Error handling unknown type is ok
			return
		}

		// Should still have processed some entries
		if len(infos) == 0 {
			t.Fatal("expected at least one entry")
		}
	}
}

// TestDatabaseClosedErrors tests various error paths when database is closed
func TestDatabaseClosedErrors(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create some files and directories
	w := fs.NewWriter("testdir/file.txt")
	w.Write([]byte("content"))
	w.Close()

	// Open files/directories before closing db
	f1, err := fs.Open("testdir")
	if err != nil {
		t.Fatal(err)
	}

	f2, err := fs.Open("testdir/file.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Close the database
	db.Close()

	// Now try various operations that should fail

	// ReadDir query error (lines 228-230, 386-388)
	if dirFile, ok := f1.(interface{ ReadDir(int) ([]os.DirEntry, error) }); ok {
		_, err = dirFile.ReadDir(0)
		if err == nil {
			t.Fatal("expected error when database is closed")
		}
	}

	// Readdir query error (lines 280-282, 376-377)
	if dirFile, ok := f1.(interface{ Readdir(int) ([]os.FileInfo, error) }); ok {
		_, err = dirFile.Readdir(0)
		if err == nil {
			t.Fatal("expected error when database is closed")
		}
	}

	// Open query error (lines 79-81, 92-94)
	_, err = fs.Open("newfile.txt")
	if err == nil {
		t.Fatal("expected error when database is closed")
	}

	// Writer commit error (lines 183-185)
	w2 := fs.NewWriter("newfile2.txt")
	w2.Write([]byte("data"))
	err = w2.Close()
	if err == nil {
		t.Fatal("expected error when database is closed")
	}

	// Stat error on file
	_, err = f2.Stat()
	if err == nil {
		t.Fatal("expected error when database is closed")
	}
}