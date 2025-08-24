package tests

import (
	"database/sql"
	"errors"
	"io"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// Tests for fragment operations and multi-fragment files

// TestReadContinuePath tests the continue path in Read
func TestReadContinuePath(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Create a file that spans exactly one fragment boundary
	writer := fs.NewWriter("boundary.txt")
	data := make([]byte, 1024*1024) // Exactly 1MB (one fragment)
	for i := range data {
		data[i] = byte('A' + (i % 26))
	}
	_, err = writer.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	// Write one more byte to create second fragment
	_, err = writer.Write([]byte("X"))
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	file, err := fs.Open("boundary.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Read exactly at the fragment boundary
	buf := make([]byte, 1024*1024+1)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	if n != 1024*1024+1 {
		t.Errorf("Expected to read %d bytes, got %d", 1024*1024+1, n)
	}
}

// TestReadMultipleFragmentBoundaries tests reading across multiple fragment boundaries
func TestReadMultipleFragmentBoundaries(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Create a file that spans 3 fragments
	writer := fs.NewWriter("multi.txt")
	size := 1024*1024*2 + 500000 // 2.5 MB
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 256)
	}
	_, err = writer.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	file, err := fs.Open("multi.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Read the entire file in one go
	buf := make([]byte, size)
	totalRead := 0

	for totalRead < size {
		n, err := file.Read(buf[totalRead:])
		if err != nil && err.Error() != "EOF" {
			// Ignore EOF
			if n == 0 {
				break
			}
		}
		totalRead += n
	}

	if totalRead != size {
		t.Errorf("Expected to read %d bytes, got %d", size, totalRead)
	}

	// Verify content
	for i := 0; i < 100; i++ {
		if buf[i] != byte(i%256) {
			t.Errorf("Data mismatch at position %d", i)
			break
		}
	}
}

// TestReadAfterBytesRead tests Read path where we've already read some bytes
func TestReadAfterBytesRead(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Create a file with specific content
	writer := fs.NewWriter("test.bin")
	content := make([]byte, 1024*1024*2) // 2MB to span multiple fragments
	for i := range content {
		content[i] = byte(i % 256)
	}
	_, err = writer.Write(content)
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	file, err := fs.Open("test.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Read in small chunks to test the partial read paths
	buf := make([]byte, 100)
	totalRead := 0

	for totalRead < len(content) {
		n, err := file.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		totalRead += n

		if err == io.EOF {
			break
		}
	}

	if totalRead != len(content) {
		t.Errorf("Expected to read %d bytes, got %d", len(content), totalRead)
	}
}

// TestReadEmptyFragment tests Read when fragment is empty
func TestReadEmptyFragment(t *testing.T) {
	driver := NewMockDriver()
	sql.Register("mock-empty-fragment", driver)

	db, err := sql.Open("mock-empty-fragment", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Mock driver returns empty for fragment reads
	driver.SetError("SELECT SUBSTR(fragment", sql.ErrNoRows)

	file, err := fs.Open("test.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	buf := make([]byte, 10)
	n, err := file.Read(buf)

	// Should get EOF since no fragment data
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
	if n != 0 {
		t.Errorf("Expected 0 bytes read, got %d", n)
	}
}

// TestReadContinueOnZeroBytesRead tests the continue path when bytesRead is 0
func TestReadContinueOnZeroBytesRead(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Create a file with sparse fragments
	writer := fs.NewWriter("large.bin")

	// Write a large file to create multiple fragments
	content := make([]byte, 1024*1024*3) // 3MB
	for i := range content {
		content[i] = byte(i % 256)
	}
	_, err = writer.Write(content)
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	file, err := fs.Open("large.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Seek to middle of file
	seeker := file.(*sqlitefs.SQLiteFile)
	seeker.Seek(1024*1024+500, io.SeekStart)

	// Read a very small buffer to test the continue path
	buf := make([]byte, 1)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}

	if n != 1 {
		t.Errorf("Expected 1 byte read, got %d", n)
	}
}

// TestWriteFragmentErrors tests error paths in writeFragment
func TestWriteFragmentErrors(t *testing.T) {
	// Test Begin error
	driver := NewMockDriver()
	driver.SetError("BEGIN", errors.New("begin failed"))
	sql.Register("mock-begin-fail", driver)

	db, err := sql.Open("mock-begin-fail", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)
	writer := fs.NewWriter("test.txt")

	// Write enough to trigger fragment write (need to fill buffer)
	data := make([]byte, 1024*1024)
	writer.Write(data)

	// This should trigger flush and fail at BEGIN
	err = writer.Close()

	// Expect error from Begin
	if err == nil || err.Error() != "begin failed" {
		t.Errorf("Expected 'begin failed' error, got %v", err)
	}
}
