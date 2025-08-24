package tests

import (
	"database/sql"
	"io"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestReadAtExactEOF tests Read when offset is exactly at file size
func TestReadAtExactEOF(t *testing.T) {
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
	w := fs.NewWriter("test.txt")
	w.Write(content)
	w.Close()

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Read exactly the file size
	buf := make([]byte, len(content))
	n, err := f.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != len(content) {
		t.Fatalf("expected %d bytes, got %d", len(content), n)
	}

	// Now we're at EOF, next read should return EOF immediately (line 108-109)
	n, err = f.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF at exact offset, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes at EOF, got %d", n)
	}
}

// TestReadNoRowsNoData tests when sql.ErrNoRows occurs with no data read (line 132)
func TestReadNoRowsNoData(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create a file then delete its fragments
	w := fs.NewWriter("test.txt")
	w.Write([]byte("data"))
	w.Close()

	// Delete all fragments
	_, err = db.Exec("DELETE FROM file_fragments WHERE file_id = (SELECT id FROM file_metadata WHERE path = ?)", "test.txt")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Try to read - should get EOF immediately since no fragments
	buf := make([]byte, 10)
	n, err := f.Read(buf)
	if err != io.EOF {
		t.Fatalf("expected io.EOF when no fragments, got %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 bytes, got %d", n)
	}
}

// TestSeekEndDatabaseError tests Seek with io.SeekEnd when getTotalSize fails (line 244-246)
func TestSeekEndDatabaseError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create file
	w := fs.NewWriter("test.txt")
	w.Write([]byte("content"))
	w.Close()

	f, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Close DB to cause getTotalSize to fail
	db.Close()

	// Try SeekEnd - should fail
	if seeker, ok := f.(io.Seeker); ok {
		_, err = seeker.Seek(0, io.SeekEnd)
		if err == nil {
			t.Fatal("expected error when database closed")
		}
	}
}

// TestReadDirNotDirectory tests ReadDir on a file (line 263-265)
func TestReadDirNotDirectory(t *testing.T) {
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
	w := fs.NewWriter("file.txt")
	w.Write([]byte("content"))
	w.Close()

	f, err := fs.Open("file.txt")
	if err != nil {
		t.Fatal(err)
	}

	// Try ReadDir on file - should error
	if dirFile, ok := f.(interface{ ReadDir(int) ([]os.DirEntry, error) }); ok {
		_, err = dirFile.ReadDir(0)
		if err == nil || err.Error() != "not a directory" {
			t.Fatalf("expected 'not a directory', got %v", err)
		}
	}
}

// TestReadDirPathNormalization tests directory path without trailing slash (line 278-280)
func TestReadDirPathNormalization(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Create files in directory
	w1 := fs.NewWriter("testdir/file1.txt")
	w1.Write([]byte("content1"))
	w1.Close()

	w2 := fs.NewWriter("testdir/file2.txt")
	w2.Write([]byte("content2"))
	w2.Close()

	// Open dir without trailing slash - should normalize
	f, err := fs.Open("testdir")
	if err != nil {
		t.Fatal(err)
	}

	if dirFile, ok := f.(interface{ ReadDir(int) ([]os.DirEntry, error) }); ok {
		entries, err := dirFile.ReadDir(0)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 2 {
			t.Fatalf("expected 2 entries, got %d", len(entries))
		}
	}
}

// TestReadDirCleanNameSlash tests directory entry with trailing slash (line 353-355)
func TestReadDirCleanNameSlash(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Manually insert dir with trailing slash
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "parent/subdir/", "dir")
	if err != nil {
		t.Fatal(err)
	}

	// Also add a file so parent dir has content
	w := fs.NewWriter("parent/file.txt")
	w.Write([]byte("test"))
	w.Close()

	f, err := fs.Open("parent")
	if err != nil {
		t.Fatal(err)
	}

	if dirFile, ok := f.(interface{ ReadDir(int) ([]os.DirEntry, error) }); ok {
		entries, err := dirFile.ReadDir(0)
		if err != nil {
			t.Fatal(err)
		}
		// Check all entries have valid names
		for _, entry := range entries {
			if entry.Name() == "" || entry.Name() == "/" {
				t.Fatal("invalid entry name")
			}
		}
	}
}

// TestCreateFileInfoDirNotExist tests createFileInfo for non-existent directory (line 614-616)
func TestCreateFileInfoDirNotExist(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	// Try to open non-existent directory
	_, err = fs.Open("nonexistent/dir")
	if err == nil {
		t.Fatal("expected error for non-existent directory")
	}
}

// TestGetTotalSizeFileMetadataNoFragments tests getTotalSize path (lines 574-585)
func TestGetTotalSizeFileMetadataNoFragments(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Directly insert metadata without fragments
	_, err = db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", "empty.txt", "file")
	if err != nil {
		t.Fatal(err)
	}

	f, err := fs.Open("empty.txt")
	if err != nil {
		t.Fatal(err)
	}

	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	
	// Should be size 0
	if info.Size() != 0 {
		t.Fatalf("expected size 0, got %d", info.Size())
	}
}

// TestWriteFragmentDBError tests writeFragment with database error (line 886-888)
func TestWriteFragmentDBError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Write data
	w := fs.NewWriter("test.txt")
	w.Write([]byte("data"))
	
	// Close DB before closing writer
	db.Close()
	
	// Close should fail
	err = w.Close()
	if err == nil {
		t.Fatal("expected error when DB closed")
	}
}

// TestOpenDatabaseQueryError tests Open with database error (lines 782-784)
func TestOpenDatabaseQueryError(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}
	
	// Close DB
	db.Close()

	// Try to open - should fail
	_, err = fs.Open("any.txt")
	if err == nil {
		t.Fatal("expected error when database closed")
	}
}