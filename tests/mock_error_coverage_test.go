package tests

import (
	"database/sql"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
)

// TestGetTotalSizeErrorPaths tests all error paths in getTotalSize using mock driver
func TestGetTotalSizeErrorPaths(t *testing.T) {
	// Register a new instance of mock driver for this test
	driver := NewMockDriver()
	sql.Register("mock_gettotalsize", driver)

	t.Run("ErrNoRowsFileExists", func(t *testing.T) {
		db, err := sql.Open("mock_gettotalsize", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Set up mock to return sql.ErrNoRows for the COUNT query
		driver.SetError("SELECT COUNT", sql.ErrNoRows)
		// But return true for EXISTS query
		driver.SetData("SELECT EXISTS", [][]interface{}{{true}})

		// This should trigger lines 574-583 in getTotalSize
		// The COUNT query returns ErrNoRows, then EXISTS returns true,
		// so it should return size 0
		f, err := fs.Open("")
		if err != nil {
			t.Fatal(err)
		}

		info, err := f.Stat()
		if err != nil {
			t.Fatal(err)
		}

		if info.Size() != 0 {
			t.Fatalf("expected size 0, got %d", info.Size())
		}
	})

	t.Run("ErrNoRowsFileNotExists", func(t *testing.T) {
		driver.ClearErrors()
		db, err := sql.Open("mock_gettotalsize", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// For the Open to succeed, we need to handle the initial queries
		// But for getTotalSize to fail properly:
		// 1. The COUNT query in getTotalSize should return no rows (triggers sql.ErrNoRows)
		driver.SetData("SELECT COUNT(*), COALESCE", [][]interface{}{}) // Empty result = sql.ErrNoRows
		// 2. The EXISTS check should return false
		driver.SetData("SELECT EXISTS", [][]interface{}{{false}})

		// This should trigger line 585: return os.ErrNotExist
		f, err := fs.Open("")
		if err != nil {
			t.Fatal(err)
		}

		_, err = f.Stat()
		if err != os.ErrNotExist {
			t.Fatalf("expected os.ErrNotExist, got %v", err)
		}
	})

	t.Run("ErrNoRowsExistsQueryFails", func(t *testing.T) {
		driver.ClearErrors()
		db, err := sql.Open("mock_gettotalsize", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Set up mock to return sql.ErrNoRows for COUNT query
		driver.SetError("SELECT COUNT", sql.ErrNoRows)
		// And error for EXISTS query - this tests line 579
		driver.SetError("SELECT EXISTS", errors.New("database error"))

		f, err := fs.Open("")
		if err != nil {
			t.Fatal(err)
		}

		_, err = f.Stat()
		if err == nil || err.Error() != "database error" {
			t.Fatalf("expected database error, got %v", err)
		}
	})

	t.Run("CountQueryError", func(t *testing.T) {
		driver.ClearErrors()
		db, err := sql.Open("mock_gettotalsize", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Set up mock to return generic error for COUNT query
		// This tests line 587
		driver.SetError("SELECT COUNT", errors.New("count query failed"))

		f, err := fs.Open("")
		if err != nil {
			t.Fatal(err)
		}

		_, err = f.Stat()
		if err == nil || err.Error() != "count query failed" {
			t.Fatalf("expected count query failed, got %v", err)
		}
	})
}

// TestReadMockErrorPaths tests all error paths in Read using mock driver
func TestReadMockErrorPaths(t *testing.T) {
	driver := NewMockDriver()
	sql.Register("mock_read", driver)

	t.Run("NoRowsNoBytesRead", func(t *testing.T) {
		db, err := sql.Open("mock_read", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Set up file metadata
		driver.SetData("SELECT id FROM file_metadata", [][]interface{}{{int64(1)}})
		driver.SetData("SELECT COUNT", [][]interface{}{{1, 100}}) // 1 fragment, 100 bytes

		// Make SUBSTR query return ErrNoRows
		// This tests line 132: return 0, io.EOF
		driver.SetError("SELECT SUBSTR", sql.ErrNoRows)

		f, err := fs.Open("test.txt")
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

	t.Run("NoRowsWithBytesRead", func(t *testing.T) {
		driver.ClearErrors()
		db, err := sql.Open("mock_read", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Set up file metadata
		driver.SetData("SELECT id FROM file_metadata", [][]interface{}{{int64(1)}})
		driver.SetData("SELECT COUNT", [][]interface{}{{2, 100}}) // 2 fragments

		f, err := fs.Open("test.txt")
		if err != nil {
			t.Fatal(err)
		}

		// First read succeeds
		driver.SetData("SELECT SUBSTR", [][]interface{}{{[]byte("hello")}})
		buf := make([]byte, 10)
		n, err := f.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		if n != 5 {
			t.Fatalf("expected 5 bytes, got %d", n)
		}

		// Second read returns ErrNoRows
		// This tests lines 128-130: return bytesReadTotal, nil
		driver.SetError("SELECT SUBSTR", sql.ErrNoRows)
		n, err = f.Read(buf)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if n != 0 {
			t.Fatalf("expected 0 bytes when no rows, got %d", n)
		}
	})

	t.Run("QueryError", func(t *testing.T) {
		driver.ClearErrors()
		db, err := sql.Open("mock_read", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Set up file metadata
		driver.SetData("SELECT id FROM file_metadata", [][]interface{}{{int64(1)}})
		driver.SetData("SELECT COUNT", [][]interface{}{{1, 100}})

		// Make SUBSTR query return generic error
		// This tests line 134: return bytesReadTotal, err
		driver.SetError("SELECT SUBSTR", errors.New("query failed"))

		f, err := fs.Open("test.txt")
		if err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 10)
		_, err = f.Read(buf)
		if err == nil || err.Error() != "query failed" {
			t.Fatalf("expected query failed, got %v", err)
		}
	})
}

// TestSeekErrorPaths tests error paths in Seek using mock driver
func TestSeekErrorPaths(t *testing.T) {
	driver := NewMockDriver()
	sql.Register("mock_seek", driver)

	t.Run("SeekEndGetTotalSizeError", func(t *testing.T) {
		db, err := sql.Open("mock_seek", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Set up file metadata
		driver.SetData("SELECT id FROM file_metadata", [][]interface{}{{int64(1)}})

		f, err := fs.Open("test.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Make getTotalSize fail
		// This tests lines 244-246 in Seek
		driver.SetError("SELECT COUNT", errors.New("getTotalSize failed"))

		if seeker, ok := f.(io.Seeker); ok {
			_, err = seeker.Seek(0, io.SeekEnd)
			if err == nil || err.Error() != "getTotalSize failed" {
				t.Fatalf("expected getTotalSize failed, got %v", err)
			}
		}
	})
}

// TestReadDirMockErrorPaths tests error paths in ReadDir using mock driver
func TestReadDirMockErrorPaths(t *testing.T) {
	driver := NewMockDriver()
	sql.Register("mock_readdir", driver)

	t.Run("NotADirectory", func(t *testing.T) {
		db, err := sql.Open("mock_readdir", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Set up as file, not directory
		driver.SetData("SELECT type FROM file_metadata", [][]interface{}{{"file"}})

		f, err := fs.Open("test.txt")
		if err != nil {
			t.Fatal(err)
		}

		// This tests lines 263-265: return nil, errors.New("not a directory")
		if dirFile, ok := f.(interface {
			ReadDir(int) ([]os.DirEntry, error)
		}); ok {
			_, err = dirFile.ReadDir(0)
			if err == nil || err.Error() != "not a directory" {
				t.Fatalf("expected 'not a directory', got %v", err)
			}
		}
	})

	t.Run("ScanError", func(t *testing.T) {
		driver.ClearErrors()
		db, err := sql.Open("mock_readdir", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Set up as directory
		driver.SetData("SELECT type FROM file_metadata", [][]interface{}{{"dir"}})
		driver.SetData("SELECT COUNT", [][]interface{}{{int64(1)}})

		// Make the main query return data that causes scan error
		// This tests lines 301-303
		driver.SetError("SELECT path, type FROM file_metadata", errors.New("scan error"))

		f, err := fs.Open("dir")
		if err != nil {
			t.Fatal(err)
		}

		if dirFile, ok := f.(interface {
			ReadDir(int) ([]os.DirEntry, error)
		}); ok {
			_, err = dirFile.ReadDir(0)
			if err == nil || err.Error() != "scan error" {
				t.Fatalf("expected scan error, got %v", err)
			}
		}
	})
}

// TestCreateFileInfoErrorPaths tests error paths in createFileInfo using mock driver
func TestCreateFileInfoErrorPaths(t *testing.T) {
	driver := NewMockDriver()
	sql.Register("mock_fileinfo", driver)

	t.Run("DirectoryExistsQueryError", func(t *testing.T) {
		db, err := sql.Open("mock_fileinfo", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Set up to trigger directory check
		driver.SetData("SELECT type FROM file_metadata", [][]interface{}{})

		// Make EXISTS query for directory fail
		// This tests lines 605-607
		driver.SetError("SELECT EXISTS", errors.New("exists query failed"))

		f, err := fs.Open("")
		if err != nil {
			t.Fatal(err)
		}

		_, err = f.Stat()
		if err == nil || err.Error() != "exists query failed" {
			t.Fatalf("expected exists query failed, got %v", err)
		}
	})

	t.Run("DirectoryNotExists", func(t *testing.T) {
		driver.ClearErrors()
		db, err := sql.Open("mock_fileinfo", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Set up to check for directory that doesn't exist
		driver.SetData("SELECT type FROM file_metadata", [][]interface{}{})
		driver.SetData("SELECT EXISTS", [][]interface{}{{false}})

		// This tests lines 614-616: return nil, os.ErrNotExist
		f, err := fs.Open("")
		if err != nil {
			t.Fatal(err)
		}

		_, err = f.Stat()
		if err != os.ErrNotExist {
			t.Fatalf("expected os.ErrNotExist, got %v", err)
		}
	})

	t.Run("FileQueryError", func(t *testing.T) {
		driver.ClearErrors()
		db, err := sql.Open("mock_fileinfo", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Set up as file
		driver.SetData("SELECT type FROM file_metadata", [][]interface{}{{"file"}})

		// Make file ID query fail
		// This tests lines 593-595
		driver.SetError("SELECT id FROM file_metadata", errors.New("file query failed"))

		f, err := fs.Open("test.txt")
		if err != nil {
			t.Fatal(err)
		}

		_, err = f.Stat()
		if err == nil || err.Error() != "file query failed" {
			t.Fatalf("expected file query failed, got %v", err)
		}
	})
}

// TestOpenErrorPaths tests error paths in Open using mock driver
func TestOpenErrorPaths(t *testing.T) {
	driver := NewMockDriver()
	sql.Register("mock_open", driver)

	t.Run("FileExistsQueryError", func(t *testing.T) {
		db, err := sql.Open("mock_open", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Make the file exists query fail
		// This tests lines 782-784
		driver.SetError("SELECT COUNT", errors.New("count query failed"))

		_, err = fs.Open("test.txt")
		if err == nil || err.Error() != "count query failed" {
			t.Fatalf("expected count query failed, got %v", err)
		}
	})

	t.Run("DirectoryExistsQueryError", func(t *testing.T) {
		driver.ClearErrors()
		db, err := sql.Open("mock_open", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// First query says file doesn't exist
		driver.SetData("SELECT COUNT", [][]interface{}{{int64(0)}})

		// Make directory exists query fail
		// This tests lines 795-797
		driver.SetError("SELECT EXISTS", errors.New("exists query failed"))

		_, err = fs.Open("test")
		if err == nil || err.Error() != "exists query failed" {
			t.Fatalf("expected exists query failed, got %v", err)
		}
	})
}

// TestWriteFragmentErrorPaths tests error paths in writeFragment using mock driver
func TestWriteFragmentErrorPaths(t *testing.T) {
	driver := NewMockDriver()
	sql.Register("mock_write", driver)

	t.Run("TransactionError", func(t *testing.T) {
		db, err := sql.Open("mock_write", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Make the transaction fail
		// This tests lines 886-888
		driver.SetError("BEGIN", errors.New("transaction failed"))

		w := fs.NewWriter("test.txt")
		_, err = w.Write([]byte("data"))
		if err == nil {
			err = w.Close()
		}
		if err == nil || err.Error() != "transaction failed" {
			t.Fatalf("expected transaction failed, got %v", err)
		}
	})

	t.Run("InsertError", func(t *testing.T) {
		driver.ClearErrors()
		db, err := sql.Open("mock_write", "")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()

		fs, err := sqlitefs.NewSQLiteFS(db)
		if err != nil {
			t.Fatal(err)
		}

		// Make the INSERT fail (it's actually INSERT OR REPLACE)
		driver.SetError("file_fragments", errors.New("insert failed"))

		w := fs.NewWriter("test.txt")
		_, err = w.Write([]byte("data"))
		if err == nil {
			err = w.Close()
		}
		if err == nil || err.Error() != "insert failed" {
			t.Fatalf("expected insert failed, got %v", err)
		}
	})
}
