package tests

import (
	"database/sql"
	"io"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// Tests for Seek functionality

// TestSeekFromCurrent tests Seek with SeekCurrent whence
func TestSeekFromCurrent(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Create a file
	writer := fs.NewWriter("seek.txt")
	data := []byte("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	writer.Write(data)
	writer.Close()

	file, err := fs.Open("seek.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	seeker := file.(*sqlitefs.SQLiteFile)

	// Read first 10 bytes
	buf := make([]byte, 10)
	n, err := file.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatalf("Expected to read 10 bytes, got %d", n)
	}

	// Seek forward 5 bytes from current position
	pos, err := seeker.Seek(5, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 15 {
		t.Errorf("Expected position 15, got %d", pos)
	}

	// Read and verify we're at the right position
	n, err = file.Read(buf[:1])
	if err != nil {
		t.Fatal(err)
	}
	if buf[0] != 'F' {
		t.Errorf("Expected to read 'F', got '%c'", buf[0])
	}

	// Seek backward
	pos, err = seeker.Seek(-10, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 6 {
		t.Errorf("Expected position 6, got %d", pos)
	}
}

// TestSeekFromEnd tests Seek with SeekEnd whence
func TestSeekFromEnd(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Create a file
	writer := fs.NewWriter("seekend.txt")
	data := []byte("0123456789")
	writer.Write(data)
	writer.Close()

	file, err := fs.Open("seekend.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	seeker := file.(*sqlitefs.SQLiteFile)

	// Seek to 3 bytes before end
	pos, err := seeker.Seek(-3, io.SeekEnd)
	if err != nil {
		t.Fatal(err)
	}
	if pos != 7 {
		t.Errorf("Expected position 7, got %d", pos)
	}

	// Read and verify
	buf := make([]byte, 3)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("Expected to read 3 bytes, got %d", n)
	}
	if string(buf) != "789" {
		t.Errorf("Expected to read '789', got '%s'", string(buf))
	}
}

// TestSeekNegativePosition tests seeking to negative position
func TestSeekNegativePosition(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	writer := fs.NewWriter("seek.txt")
	writer.Write([]byte("hello world"))
	writer.Close()

	file, err := fs.Open("seek.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	seeker := file.(*sqlitefs.SQLiteFile)

	// Try to seek to negative position
	_, err = seeker.Seek(-100, 0) // SeekStart with negative offset
	if err == nil {
		t.Error("Expected error seeking to negative position")
	}
}

// TestSeekBeyondFileSize tests seeking beyond the file size
func TestSeekBeyondFileSize(t *testing.T) {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	fs, _ := sqlitefs.NewSQLiteFS(db)

	// Create a small file
	writer := fs.NewWriter("small.txt")
	_, err = writer.Write([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	file, err := fs.Open("small.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	// Seek beyond file size
	seeker := file.(*sqlitefs.SQLiteFile)
	pos, err := seeker.Seek(100, io.SeekStart)
	if err != nil {
		t.Fatal(err)
	}

	if pos != 100 {
		t.Errorf("Expected position 100, got %d", pos)
	}

	// Read should return EOF
	buf := make([]byte, 10)
	n, err := file.Read(buf)
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
	if n != 0 {
		t.Errorf("Expected 0 bytes read, got %d", n)
	}
}
