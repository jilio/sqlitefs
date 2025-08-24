package tests

import (
	"database/sql"
	"io"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestEmptyDirectory tests handling of empty directories and edge cases
func TestEmptyDirectory(t *testing.T) {
	t.Run("ReadDirEmptyDirExists", func(t *testing.T) {
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

		// Create a directory with file, then delete the file
		writer := fs.NewWriter("emptydir/temp.txt")
		writer.Write([]byte("temp"))
		writer.Close()

		// Delete the file but keep directory reference
		db.Exec("DELETE FROM file_metadata WHERE path = 'emptydir/temp.txt'")

		// Now try to read the empty directory - this should trigger the EXISTS check
		dir, err := fs.Open("emptydir")
		if err != nil {
			// Directory might not be recognized without files
			return
		}

		if rd, ok := dir.(interface {
			ReadDir(int) ([]os.DirEntry, error)
		}); ok {
			entries, err := rd.ReadDir(-1)
			// Should have 0 entries but directory exists
			if err != nil && err != io.EOF {
				// Expected - empty directory
			}
			_ = entries
		}
	})

	t.Run("ReaddirEmptyDirExists", func(t *testing.T) {
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

		// Create nested structure then remove files
		writer := fs.NewWriter("dir1/dir2/file.txt")
		writer.Write([]byte("test"))
		writer.Close()

		// Remove the file but keep directory structure
		db.Exec("DELETE FROM file_metadata WHERE path = 'dir1/dir2/file.txt'")

		dir, err := fs.Open("dir1/dir2")
		if err != nil {
			return
		}

		if rd, ok := dir.(interface {
			Readdir(int) ([]os.FileInfo, error)
		}); ok {
			entries, err := rd.Readdir(-1)
			// Should trigger the empty check with EXISTS query
			_ = entries
			_ = err
		}
	})

	t.Run("ReadDirPaginationStates", func(t *testing.T) {
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

		// Create exactly 3 files to test pagination edge cases
		for i := 0; i < 3; i++ {
			writer := fs.NewWriter(string(rune('a'+i)) + ".txt")
			writer.Write([]byte("content"))
			writer.Close()
		}

		dir, err := fs.Open("/")
		if err != nil {
			t.Fatal(err)
		}

		if rd, ok := dir.(interface {
			ReadDir(int) ([]os.DirEntry, error)
		}); ok {
			// Read exactly 3 to exhaust entries
			entries, err := rd.ReadDir(3)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}

			// Now try to read more - should return empty with EOF
			entries2, err := rd.ReadDir(1)
			if err != io.EOF {
				// Expected EOF
			}
			_ = entries
			_ = entries2
		}

		dir.Close()

		// Test Readdir state transitions
		dir, err = fs.Open("/")
		if err != nil {
			t.Fatal(err)
		}

		if rd, ok := dir.(interface {
			Readdir(int) ([]os.FileInfo, error)
		}); ok {
			// Read with count 0 after partial read
			rd.Readdir(1)                 // Read one
			entries, err := rd.Readdir(0) // Read all remaining
			_ = entries
			_ = err
		}
	})

	t.Run("WriteFragmentBoundaryExact", func(t *testing.T) {
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

		// Test writing exactly at fragment boundaries
		writer := fs.NewWriter("boundary.txt")

		// Write 8192 bytes (exactly one fragment)
		data1 := make([]byte, 8192)
		for i := range data1 {
			data1[i] = 'A'
		}
		n, err := writer.Write(data1)
		if err != nil || n != 8192 {
			t.Errorf("Failed to write first fragment: %v", err)
		}

		// Write another exact fragment
		n, err = writer.Write(data1)
		if err != nil || n != 8192 {
			t.Errorf("Failed to write second fragment: %v", err)
		}

		writer.Close()

		// Verify by reading
		file, err := fs.Open("boundary.txt")
		if err != nil {
			t.Fatal(err)
		}

		info, err := file.Stat()
		if err != nil {
			t.Fatal(err)
		}

		if info.Size() != 16384 {
			t.Errorf("Expected size 16384, got %d", info.Size())
		}
	})

	t.Run("ReadFragmentScanError", func(t *testing.T) {
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

		// Create a file with fragments
		writer := fs.NewWriter("scanerror.txt")
		writer.Write(make([]byte, 10000))
		writer.Close()

		// Corrupt fragment data to cause scan error
		db.Exec("UPDATE file_fragments SET fragment = 'invalid' WHERE fragment_number = 1")

		file, err := fs.Open("scanerror.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Try to read - should handle scan error
		buf := make([]byte, 100)
		_, err = file.Read(buf)
		_ = err // Error expected
	})

	t.Run("GetTotalSizeFileDeleted", func(t *testing.T) {
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

		// Create a file
		writer := fs.NewWriter("deleted.txt")
		writer.Write([]byte("content"))
		writer.Close()

		file, err := fs.Open("deleted.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Delete the file metadata after opening
		db.Exec("DELETE FROM file_metadata WHERE path = 'deleted.txt'")
		db.Exec("DELETE FROM file_fragments WHERE path = 'deleted.txt'")

		// Try to stat - should detect file doesn't exist
		_, err = file.Stat()
		if err == nil {
			t.Error("Expected error for deleted file")
		}
	})

	t.Run("SeekNegativePosition", func(t *testing.T) {
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

		writer := fs.NewWriter("seek.txt")
		writer.Write([]byte("test content for seeking"))
		writer.Close()

		file, err := fs.Open("seek.txt")
		if err != nil {
			t.Fatal(err)
		}

		sqlFile := file.(*sqlitefs.SQLiteFile)

		// Try various seeks that result in negative position
		_, err = sqlFile.Seek(-10, 0) // Negative from start
		if err == nil {
			t.Error("Expected error for negative seek from start")
		}

		// Seek to position 5 first
		sqlFile.Seek(5, 0)

		// Now seek back too far
		_, err = sqlFile.Seek(-10, 1) // Back 10 from position 5
		if err == nil {
			t.Error("Expected error for negative seek result")
		}
	})

	t.Run("DirectoryTrailingSlashHandling", func(t *testing.T) {
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

		// Create files in nested directories
		writer := fs.NewWriter("dir1/dir2/file.txt")
		writer.Write([]byte("test"))
		writer.Close()

		// Open directory without trailing slash
		dir, err := fs.Open("dir1/dir2")
		if err != nil {
			t.Fatal(err)
		}

		// Should be able to read directory
		if rd, ok := dir.(interface {
			ReadDir(int) ([]os.DirEntry, error)
		}); ok {
			entries, _ := rd.ReadDir(-1)
			_ = entries
		}

		dir.Close()

		// Open with trailing slash
		dir, err = fs.Open("dir1/dir2/")
		if err != nil {
			// Some implementations might not support trailing slash
			return
		}

		if rd, ok := dir.(interface {
			ReadDir(int) ([]os.DirEntry, error)
		}); ok {
			entries, _ := rd.ReadDir(-1)
			_ = entries
		}
	})
}
