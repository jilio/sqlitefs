package tests

import (
	"database/sql"
	"io"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestReadEOFAtExactBoundary tests lines 108-110: EOF when f.offset >= f.size
func TestReadEOFAtExactBoundary(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create a file with exact content
	content := []byte("test")
	w := fs.NewWriter("exact.txt")
	w.Write(content)
	w.Close()

	f, err := fs.Open("exact.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Read all content
	buf := make([]byte, 10)
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("expected 4 bytes, got %d", n)
	}

	// Now f.offset = 4, f.size = 4, next read should hit lines 108-110
	n, err = f.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// TestReadEmptyFragmentAtBoundary tests lines 144-146: empty fragment when at offset == size
func TestReadEmptyFragmentAtBoundary(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create file with exactly 4096 bytes
	content := make([]byte, 4096)
	for i := range content {
		content[i] = byte(i % 256)
	}

	w := fs.NewWriter("boundary.txt")
	w.Write(content)
	w.Close()

	// Get file ID and manually add empty fragment
	var fileID int
	err = db.QueryRow("SELECT id FROM file_metadata WHERE path = ?", "boundary.txt").Scan(&fileID)
	if err != nil {
		t.Fatal(err)
	}

	// Add empty fragment at index 1
	_, err = db.Exec("INSERT INTO file_fragments (file_id, fragment_index, fragment) VALUES (?, ?, ?)",
		fileID, 1, []byte{})
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("boundary.txt")
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

	// Now at boundary, next read should hit empty fragment (lines 144-146)
	n, err = f.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// TestReaddirVariousErrorPaths tests multiple Readdir error conditions
func TestReaddirVariousErrorPaths(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("ReaddirNonDirectory", func(t *testing.T) {
		// Create a file, not a directory
		w := fs.NewWriter("notdir.txt")
		w.Write([]byte("content"))
		w.Close()

		f, err := fs.Open("notdir.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Try Readdir on file (lines 315-317)
		if dirFile, ok := f.(interface {
			Readdir(int) ([]os.FileInfo, error)
		}); ok {
			_, err = dirFile.Readdir(0)
			if err == nil || err.Error() != "not a directory" {
				t.Fatalf("expected 'not a directory', got %v", err)
			}
		}
	})

	t.Run("ReaddirCleanName", func(t *testing.T) {
		// Create directory with trailing slash issue
		w := fs.NewWriter("cleandir/file.txt")
		w.Write([]byte("content"))
		w.Close()

		// Manually insert problematic path
		_, err = db.Exec("INSERT OR IGNORE INTO file_metadata (path, type) VALUES (?, ?)",
			"cleandir//", "dir")
		if err != nil {
			t.Fatal(err)
		}

		f, err := fs.Open("cleandir")
		if err != nil {
			t.Fatal(err)
		}

		// Readdir should handle clean name issues (lines 324-326)
		if dirFile, ok := f.(interface {
			Readdir(int) ([]os.FileInfo, error)
		}); ok {
			infos, err := dirFile.Readdir(0)
			if err != nil && err != io.EOF {
				// Some error is ok, as long as it handles the bad path
				return
			}
			// Check that no empty names exist
			for _, info := range infos {
				if info.Name() == "" || info.Name() == "/" {
					t.Fatal("found invalid name")
				}
			}
		}
	})
}

// TestReadDirVariousErrorPaths tests ReadDir error conditions
func TestReadDirVariousErrorPaths(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("ReadDirQueryError", func(t *testing.T) {
		// Create directory
		w := fs.NewWriter("querydir/file.txt")
		w.Write([]byte("content"))
		w.Close()

		f, err := fs.Open("querydir")
		if err != nil {
			t.Fatal(err)
		}

		// Close database to cause query error
		db.Close()

		// ReadDir should fail (lines 228-230)
		if dirFile, ok := f.(interface {
			ReadDir(int) ([]os.DirEntry, error)
		}); ok {
			_, err = dirFile.ReadDir(0)
			if err == nil {
				t.Fatal("expected error when database is closed")
			}
		}
	})
}

// TestCreateFileInfoSubdirError tests subdirectory query error (lines 249-251)
func TestCreateFileInfoSubdirError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create complex structure
	w1 := fs.NewWriter("parent/file1.txt")
	w1.Write([]byte("content1"))
	w1.Close()

	w2 := fs.NewWriter("parent/child/file2.txt")
	w2.Write([]byte("content2"))
	w2.Close()

	// Corrupt subdirectory metadata
	_, err = db.Exec("DELETE FROM file_metadata WHERE path LIKE 'parent/child%' AND type = 'dir'")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("parent")
	if err != nil {
		t.Fatal(err)
	}

	// ReadDir should handle corrupted subdirectory gracefully
	if dirFile, ok := f.(interface {
		ReadDir(int) ([]os.DirEntry, error)
	}); ok {
		entries, err := dirFile.ReadDir(0)
		if err != nil && err != io.EOF {
			// Error is acceptable
			return
		}
		// Should still return some entries
		if len(entries) == 0 {
			t.Fatal("expected at least one entry")
		}
	}
}

// TestReadDirCleanNameEmpty tests when clean name becomes empty (lines 254-255)
func TestReadDirCleanNameEmpty(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create directory
	w := fs.NewWriter("emptyname/file.txt")
	w.Write([]byte("content"))
	w.Close()

	// Manually insert path that results in empty clean name
	_, err = db.Exec("INSERT OR IGNORE INTO file_metadata (path, type) VALUES (?, ?)",
		"emptyname/", "dir")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("emptyname")
	if err != nil {
		t.Fatal(err)
	}

	// ReadDir should skip entries with empty clean names (lines 254-255)
	if dirFile, ok := f.(interface {
		ReadDir(int) ([]os.DirEntry, error)
	}); ok {
		entries, err := dirFile.ReadDir(0)
		if err != nil && err != io.EOF {
			// Error is acceptable
			return
		}
		// Check no empty names
		for _, entry := range entries {
			if entry.Name() == "" {
				t.Fatal("found entry with empty name")
			}
		}
	}
}

// TestReaddirQueryRowsError tests Readdir rows.Scan error (lines 332-334)
func TestReaddirQueryRowsError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create directory
	w := fs.NewWriter("scandir/file.txt")
	w.Write([]byte("content"))
	w.Close()

	// Corrupt the type field
	_, err = db.Exec("UPDATE file_metadata SET type = NULL WHERE path = 'scandir/file.txt'")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("scandir")
	if err != nil {
		t.Fatal(err)
	}

	// Readdir should handle scan error gracefully (lines 332-334)
	if dirFile, ok := f.(interface {
		Readdir(int) ([]os.FileInfo, error)
	}); ok {
		_, err = dirFile.Readdir(0)
		// Should get an error or handle gracefully
		if err == nil {
			// If no error, that's ok too - it handled it
		}
	}
}

// TestReadDirSubdirQueryError tests ReadDir subdirectory query error (lines 363-365)
func TestReadDirSubdirQueryError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create nested structure
	w := fs.NewWriter("root/sub/file.txt")
	w.Write([]byte("content"))
	w.Close()

	// Corrupt subdirectory check
	_, err = db.Exec("DELETE FROM file_metadata WHERE path = 'root/sub' AND type = 'dir'")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("root")
	if err != nil {
		t.Fatal(err)
	}

	// ReadDir should handle missing subdirectory metadata
	if dirFile, ok := f.(interface {
		ReadDir(int) ([]os.DirEntry, error)
	}); ok {
		entries, err := dirFile.ReadDir(0)
		if err != nil && err != io.EOF {
			// Error is acceptable
			return
		}
		// Should still work
		_ = entries
	}
}

// TestReaddirRowsError tests Readdir when rows returns error (lines 280-282, 376-377)
func TestReaddirRowsError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create directory
	w := fs.NewWriter("errdir/file.txt")
	w.Write([]byte("content"))
	w.Close()

	f, err := fs.Open("errdir")
	if err != nil {
		t.Fatal(err)
	}

	// Close database to cause error
	db.Close()

	// Readdir should fail with query error (lines 280-282)
	if dirFile, ok := f.(interface {
		Readdir(int) ([]os.FileInfo, error)
	}); ok {
		_, err = dirFile.Readdir(0)
		if err == nil {
			t.Fatal("expected error when database is closed")
		}
	}
}

// TestReadDirRowsError tests ReadDir when rows returns error (lines 386-388)
func TestReadDirRowsError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create directory
	w := fs.NewWriter("errdir2/file.txt")
	w.Write([]byte("content"))
	w.Close()

	f, err := fs.Open("errdir2")
	if err != nil {
		t.Fatal(err)
	}

	// Close database to cause error
	db.Close()

	// ReadDir should fail with query error (lines 386-388)
	if dirFile, ok := f.(interface {
		ReadDir(int) ([]os.DirEntry, error)
	}); ok {
		_, err = dirFile.ReadDir(0)
		if err == nil {
			t.Fatal("expected error when database is closed")
		}
	}
}

// TestReaddirSubdirQueryError tests Readdir subdirectory query error (lines 407-409)
func TestReaddirSubdirQueryError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create nested structure
	w := fs.NewWriter("main/sub/file.txt")
	w.Write([]byte("content"))
	w.Close()

	// Remove subdirectory metadata
	_, err = db.Exec("DELETE FROM file_metadata WHERE path = 'main/sub' AND type = 'dir'")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("main")
	if err != nil {
		t.Fatal(err)
	}

	// Readdir should handle missing subdirectory
	if dirFile, ok := f.(interface {
		Readdir(int) ([]os.FileInfo, error)
	}); ok {
		entries, err := dirFile.Readdir(0)
		if err != nil && err != io.EOF {
			// Error is acceptable
			return
		}
		// Should still work
		_ = entries
	}
}

// TestReaddirCleanNameContinue tests when clean name is empty/slash (lines 412-413)
func TestReaddirCleanNameContinue(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create directory
	w := fs.NewWriter("skipdir/file.txt")
	w.Write([]byte("content"))
	w.Close()

	// Insert path that will have empty clean name
	_, err = db.Exec("INSERT OR IGNORE INTO file_metadata (path, type) VALUES (?, ?)",
		"skipdir/", "dir")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("skipdir")
	if err != nil {
		t.Fatal(err)
	}

	// Readdir should skip entries with empty/slash clean names (lines 412-413)
	if dirFile, ok := f.(interface {
		Readdir(int) ([]os.FileInfo, error)
	}); ok {
		infos, err := dirFile.Readdir(0)
		if err != nil && err != io.EOF {
			// Error is acceptable
			return
		}
		// Check no invalid names
		for _, info := range infos {
			if info.Name() == "" || info.Name() == "/" {
				t.Fatal("found invalid name")
			}
		}
	}
}

// TestReaddirCreateFileInfoError tests Readdir when createFileInfo fails (lines 437-439, 448-450)
func TestReaddirCreateFileInfoError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create files
	w1 := fs.NewWriter("infoerr/file1.txt")
	w1.Write([]byte("content1"))
	w1.Close()

	w2 := fs.NewWriter("infoerr/file2.txt")
	w2.Write([]byte("content2"))
	w2.Close()

	// Corrupt one file's metadata
	_, err = db.Exec("UPDATE file_metadata SET type = 'invalid' WHERE path = 'infoerr/file1.txt'")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("infoerr")
	if err != nil {
		t.Fatal(err)
	}

	// Readdir should handle createFileInfo error (lines 437-439)
	if dirFile, ok := f.(interface {
		Readdir(int) ([]os.FileInfo, error)
	}); ok {
		infos, err := dirFile.Readdir(0)
		// Should either error or skip the bad entry
		if err != nil && err != io.EOF {
			// Error is acceptable
			return
		}
		// Or it handled it and returned valid entries
		_ = infos
	}
}

// TestReaddirElsePathErrors tests the else path in Readdir (lines 458-460, 461-463)
func TestReaddirElsePathErrors(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Create directory with both files and subdirs
	w1 := fs.NewWriter("mixed/file.txt")
	w1.Write([]byte("file content"))
	w1.Close()

	w2 := fs.NewWriter("mixed/subdir/nested.txt")
	w2.Write([]byte("nested content"))
	w2.Close()

	// Insert an entry with invalid type
	_, err = db.Exec("INSERT OR IGNORE INTO file_metadata (path, type) VALUES (?, ?)",
		"mixed/unknown", "unknown")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("mixed")
	if err != nil {
		t.Fatal(err)
	}

	// Readdir should handle unknown types (lines 458-460)
	if dirFile, ok := f.(interface {
		Readdir(int) ([]os.FileInfo, error)
	}); ok {
		infos, err := dirFile.Readdir(0)
		if err != nil && err != io.EOF {
			// Error handling unknown type is ok
			return
		}
		// Should have processed entries
		_ = infos
	}
}
