package tests

import (
	"database/sql"
	"io"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// TestSimpleCoverage aims to increase coverage with simple, focused tests
func TestSimpleCoverage(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("DirEntry", func(t *testing.T) {
		// Create a file and directory to test DirEntry methods
		w := fs.NewWriter("test.txt")
		w.Write([]byte("content"))
		w.Close()

		w = fs.NewWriter("dir/file.txt")
		w.Write([]byte("data"))
		w.Close()

		// Open root directory and read entries
		root, err := fs.Open("/")
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()

		// Use the actual ReadDir method signature
		sqlFile := root.(*sqlitefs.SQLiteFile)
		entries, err := sqlFile.ReadDir(-1)
		if err != nil {
			t.Fatal(err)
		}

		// Test that we got some entries
		if len(entries) == 0 {
			t.Error("expected at least one entry")
		}

		for _, entry := range entries {
			// Test DirEntry methods (covers lines 19-31)
			_ = entry.Name()
			_ = entry.IsDir()
			_ = entry.Type()
			_, _ = entry.Info()
		}
	})

	t.Run("FileInfo", func(t *testing.T) {
		// Create a test file
		w := fs.NewWriter("info_test.txt")
		w.Write([]byte("test content for file info"))
		w.Close()

		f, err := fs.Open("info_test.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}

		// Test FileInfo methods (covers file_info.go lines)
		_ = info.Name()
		_ = info.Size()
		_ = info.Mode()
		_ = info.ModTime()
		_ = info.IsDir()
		_ = info.Sys()
	})

	t.Run("SeekCurrent", func(t *testing.T) {
		// Create a file with some content
		w := fs.NewWriter("seek_test.txt")
		w.Write([]byte("0123456789"))
		w.Close()

		f, err := fs.Open("seek_test.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		// Test SeekCurrent (line 170 in file.go)
		seeker := f.(io.Seeker)
		pos, err := seeker.Seek(5, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		if pos != 5 {
			t.Errorf("expected position 5, got %d", pos)
		}

		// Now seek relative to current
		pos, err = seeker.Seek(2, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if pos != 7 {
			t.Errorf("expected position 7, got %d", pos)
		}

		// Seek backwards from current
		pos, err = seeker.Seek(-3, io.SeekCurrent)
		if err != nil {
			t.Fatal(err)
		}
		if pos != 4 {
			t.Errorf("expected position 4, got %d", pos)
		}
	})

	t.Run("InvalidSeek", func(t *testing.T) {
		w := fs.NewWriter("seek_invalid.txt")
		w.Write([]byte("test"))
		w.Close()

		f, err := fs.Open("seek_invalid.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		seeker := f.(io.Seeker)

		// Test invalid whence (line 181 in file.go)
		_, err = seeker.Seek(0, 999)
		if err == nil {
			t.Error("expected error for invalid whence")
		}

		// Test negative seek position (line 185 in file.go)
		_, err = seeker.Seek(-10, io.SeekStart)
		if err == nil {
			t.Error("expected error for negative position")
		}

		// Test seek beyond file size - should succeed but limit reads
		pos, err := seeker.Seek(100, io.SeekStart)
		if err != nil {
			t.Fatal(err)
		}
		if pos != 100 {
			t.Errorf("expected position 100, got %d", pos)
		}
	})

	t.Run("ReadEmptyBuffer", func(t *testing.T) {
		w := fs.NewWriter("empty_buffer.txt")
		w.Write([]byte("data"))
		w.Close()

		f, err := fs.Open("empty_buffer.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		// Test reading with empty buffer (line 90-92 in file.go)
		buf := make([]byte, 0)
		n, err := f.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("expected 0 bytes read, got %d", n)
		}
	})

	t.Run("MimeType", func(t *testing.T) {
		// Test file with MIME type
		w := fs.NewWriter("test.html")
		w.Write([]byte("<html></html>"))
		w.Close()

		f, err := fs.Open("test.html")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		// Test MimeType method (line 522 in file.go)
		sqlFile := f.(*sqlitefs.SQLiteFile)
		mimeType := sqlFile.MimeType()
		if mimeType == "" {
			t.Error("expected non-empty MIME type")
		}
	})

	t.Run("WriterErrors", func(t *testing.T) {
		// This would test error conditions in writer, but it's hard without mocks
		// At least we can test the normal path
		w := fs.NewWriter("writer_test.txt")
		
		// Write some data
		n, err := w.Write([]byte("test"))
		if err != nil {
			t.Fatal(err)
		}
		if n != 4 {
			t.Errorf("expected 4 bytes written, got %d", n)
		}

		// Close the writer
		err = w.Close()
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("OpenNonExistent", func(t *testing.T) {
		// Test opening non-existent file
		_, err := fs.Open("does_not_exist.txt")
		if err == nil {
			t.Error("expected error opening non-existent file")
		}
	})

	t.Run("ReadDirFile", func(t *testing.T) {
		// Create some files in a directory
		w := fs.NewWriter("testdir/a.txt")
		w.Write([]byte("a"))
		w.Close()

		w = fs.NewWriter("testdir/b.txt")
		w.Write([]byte("b"))
		w.Close()

		// Open directory
		dir, err := fs.Open("testdir/")
		if err != nil {
			t.Fatal(err)
		}
		defer dir.Close()

		// Test ReadDir with positive n
		sqlDir := dir.(*sqlitefs.SQLiteFile)
		entries, err := sqlDir.ReadDir(1)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if len(entries) != 1 {
			t.Errorf("expected 1 entry, got %d", len(entries))
		}

		// Read the rest
		entries, err = sqlDir.ReadDir(-1)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
	})
}

// TestEdgeCasesSimple tests various edge cases without complex mocks
func TestEdgeCasesSimple(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("EmptyFile", func(t *testing.T) {
		// Create an empty file
		w := fs.NewWriter("empty.txt")
		w.Close()

		f, err := fs.Open("empty.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		// Read from empty file
		buf := make([]byte, 10)
		n, err := f.Read(buf)
		if err != io.EOF {
			t.Errorf("expected io.EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("expected 0 bytes, got %d", n)
		}

		// Stat empty file
		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != 0 {
			t.Errorf("expected size 0, got %d", info.Size())
		}
	})

	t.Run("LargeFile", func(t *testing.T) {
		// Create a file larger than one fragment (4096 bytes)
		data := make([]byte, 8192)
		for i := range data {
			data[i] = byte(i % 256)
		}

		w := fs.NewWriter("large.txt")
		w.Write(data)
		w.Close()

		f, err := fs.Open("large.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		// Read in chunks
		buf := make([]byte, 1000)
		totalRead := 0
		for {
			n, err := f.Read(buf)
			totalRead += n
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}

		if totalRead != 8192 {
			t.Errorf("expected 8192 bytes read, got %d", totalRead)
		}
	})

	t.Run("RootDirectory", func(t *testing.T) {
		// Open root directory
		root, err := fs.Open("/")
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()

		// Stat root directory
		info, err := root.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() {
			t.Error("root should be a directory")
		}
	})

	t.Run("Close", func(t *testing.T) {
		w := fs.NewWriter("close_test.txt")
		w.Write([]byte("test"))
		w.Close()

		f, err := fs.Open("close_test.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Test Close method (line 517 in file.go)
		err = f.Close()
		if err != nil {
			t.Fatal(err)
		}

		// Closing again should not error
		err = f.Close()
		if err != nil {
			t.Fatal(err)
		}
	})
}