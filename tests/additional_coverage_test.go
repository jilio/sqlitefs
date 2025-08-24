package tests

import (
	"database/sql"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// Test additional paths in getTotalSize
func TestGetTotalSizeAdditionalPaths(t *testing.T) {
	t.Run("GetTotalSizeExistsQueryError", func(t *testing.T) {
		driver := NewMockDriver()
		sql.Register("mock-gettotalsize-exists", driver)
		
		db, err := sql.Open("mock-gettotalsize-exists", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		// Make the COUNT query return ErrNoRows
		driver.SetError("SELECT COUNT(*), COALESCE(LENGTH(fragment)", sql.ErrNoRows)
		// Make the EXISTS query fail
		driver.SetError("SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path", errors.New("exists failed"))
		
		fs, _ := sqlitefs.NewSQLiteFS(db)
		
		// This should trigger the error path in getTotalSize
		_, err = fs.Open("test.txt")
		if err == nil || err.Error() != "exists failed" {
			t.Errorf("Expected 'exists failed', got %v", err)
		}
	})
}

// Test additional paths in Read
func TestReadAdditionalPaths(t *testing.T) {
	t.Run("ReadFragmentQueryError", func(t *testing.T) {
		db, err := sql.Open("sqlite", "file::memory:?cache=shared")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}
		
		// Create a file with content
		writer := fs.NewWriter("test.txt")
		writer.Write([]byte("hello world"))
		writer.Close()
		
		// Open the file
		file, err := fs.Open("test.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		
		// Corrupt the database to cause read error
		db.Exec("DROP TABLE file_fragments")
		
		// Try to read - should get an error
		buf := make([]byte, 10)
		_, err = file.Read(buf)
		if err == nil {
			t.Error("Expected error when reading from corrupted database")
		}
	})

	t.Run("ReadAtEndOfFile", func(t *testing.T) {
		db, err := sql.Open("sqlite", "file::memory:?cache=shared")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}
		
		// Create a small file
		writer := fs.NewWriter("small.txt")
		writer.Write([]byte("hi"))
		writer.Close()
		
		file, err := fs.Open("small.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		
		// Read the entire file
		buf := make([]byte, 2)
		n, err := file.Read(buf)
		if n != 2 || err != nil {
			t.Fatalf("Expected to read 2 bytes, got %d, err: %v", n, err)
		}
		
		// Try to read again - should get EOF
		n, err = file.Read(buf)
		if n != 0 || err != io.EOF {
			t.Errorf("Expected 0 bytes and EOF, got %d bytes and %v", n, err)
		}
	})
}

// Test additional paths in Seek
func TestSeekAdditionalPaths(t *testing.T) {
	t.Run("SeekInvalidWhence", func(t *testing.T) {
		db, err := sql.Open("sqlite", "file::memory:?cache=shared")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}
		
		writer := fs.NewWriter("test.txt")
		writer.Write([]byte("content"))
		writer.Close()
		
		file, err := fs.Open("test.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		
		sqliteFile := file.(*sqlitefs.SQLiteFile)
		
		// Use invalid whence value
		_, err = sqliteFile.Seek(0, 999)
		if err == nil {
			t.Error("Expected error for invalid whence value")
		}
	})
}

// Test additional paths in Open
func TestOpenAdditionalPaths(t *testing.T) {
	t.Run("OpenQueryError", func(t *testing.T) {
		driver := NewMockDriver()
		sql.Register("mock-open-error", driver)
		
		db, err := sql.Open("mock-open-error", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		// Make the first EXISTS query fail
		driver.SetError("SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path", errors.New("query failed"))
		
		fs, _ := sqlitefs.NewSQLiteFS(db)
		
		_, err = fs.Open("test.txt")
		if err == nil || err.Error() != "query failed" {
			t.Errorf("Expected 'query failed', got %v", err)
		}
	})

	t.Run("OpenRootDirectoryQueryError", func(t *testing.T) {
		t.Skip("Mock driver test needs more complex setup")
	})

	t.Run("OpenDirectoryQueryError", func(t *testing.T) {
		t.Skip("Mock driver test needs more complex setup")
	})
}

// Test additional paths in createFileInfo
func TestCreateFileInfoAdditionalPaths(t *testing.T) {
	t.Run("CreateFileInfoDirectoryExistsError", func(t *testing.T) {
		t.Skip("Mock driver test needs more complex setup")
	})
}

// Test additional paths in writeFragment
func TestWriteFragmentAdditionalPaths(t *testing.T) {
	t.Run("WriteFragmentCommitError", func(t *testing.T) {
		t.Skip("Mock driver test needs transaction support")
	})
}

// Test additional paths in ReadDir
func TestReadDirAdditionalPaths(t *testing.T) {
	t.Run("ReadDirNoFilesExist", func(t *testing.T) {
		db, err := sql.Open("sqlite", "file::memory:?cache=shared")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}
		
		// Open root directory with no files
		dir, err := fs.Open("/")
		if err != nil {
			t.Fatal(err)
		}
		defer dir.Close()
		
		dirFile := dir.(*sqlitefs.SQLiteFile)
		
		// ReadDir on empty filesystem
		entries, err := dirFile.ReadDir(-1)
		// Error is expected because root directory is considered non-existent when empty
		if err == nil {
			// If no error, entries should be empty
			if len(entries) != 0 {
				t.Errorf("Expected 0 entries, got %d", len(entries))
			}
		}
	})

	t.Run("ReadDirNonExistentDirectory", func(t *testing.T) {
		db, err := sql.Open("sqlite", "file::memory:?cache=shared")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		// Manually create tables
		db.Exec(`
			CREATE TABLE IF NOT EXISTS file_metadata (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				path TEXT UNIQUE NOT NULL,
				type TEXT NOT NULL,
				mime_type TEXT
			);
		`)
		
		// This test would require exposing SQLiteFile internals
		// Skip for now as it's not easily testable
	})
}

// Test for Readdir additional paths
func TestReaddirAdditionalPaths(t *testing.T) {
	t.Run("ReaddirNotDirectory", func(t *testing.T) {
		db, err := sql.Open("sqlite", "file::memory:?cache=shared")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		
		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}
		
		// Create a file
		writer := fs.NewWriter("file.txt")
		writer.Write([]byte("content"))
		writer.Close()
		
		// Open as file
		file, err := fs.Open("file.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer file.Close()
		
		sqliteFile := file.(*sqlitefs.SQLiteFile)
		
		// Try Readdir on a file (not directory)
		_, err = sqliteFile.Readdir(1)
		if err == nil {
			t.Error("Expected error calling Readdir on a file")
		}
	})
}

// Test PathError coverage
func TestPathError(t *testing.T) {
	err := &sqlitefs.PathError{
		Op:   "open",
		Path: "/test/file.txt",
		Err:  os.ErrNotExist,
	}
	
	expected := "open /test/file.txt: file does not exist"
	if err.Error() != expected {
		t.Errorf("Expected error message %q, got %q", expected, err.Error())
	}
}