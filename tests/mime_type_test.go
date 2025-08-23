package tests

import (
	"database/sql"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

func TestMimeTypeStorage(t *testing.T) {
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

	tests := []struct {
		filename     string
		expectedMime string
	}{
		{"test.txt", "text/plain; charset=utf-8"},
		{"image.jpg", "image/jpeg"},
		{"data.json", "application/json"},
		{"style.css", "text/css; charset=utf-8"},
		{"script.js", "application/javascript"},  // Go's mime package returns this
		{"document.pdf", "application/pdf"},
		{"unknown.xyz", "chemical/x-xyz"},  // .xyz is actually a chemical format
		{"unknown.unknownext", "application/octet-stream"},  // truly unknown extension
	}

	for _, tc := range tests {
		t.Run(tc.filename, func(t *testing.T) {
			// Write a file
			writer := fs.NewWriter(tc.filename)
			_, err := writer.Write([]byte("test content"))
			if err != nil {
				t.Fatal(err)
			}
			err = writer.Close()
			if err != nil {
				t.Fatal(err)
			}

			// Check that MIME type was stored in database
			var storedMime sql.NullString
			err = db.QueryRow("SELECT mime_type FROM file_metadata WHERE path = ?", tc.filename).Scan(&storedMime)
			if err != nil {
				t.Fatalf("Failed to query MIME type: %v", err)
			}

			if !storedMime.Valid {
				t.Error("MIME type is NULL in database")
			} else if storedMime.String != tc.expectedMime {
				t.Errorf("Expected MIME type %q, got %q", tc.expectedMime, storedMime.String)
			}

			// Open the file and check MIME type via the file object
			file, err := fs.Open(tc.filename)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()

			sqliteFile := file.(*sqlitefs.SQLiteFile)
			if sqliteFile.MimeType() != tc.expectedMime {
				t.Errorf("File.MimeType() returned %q, expected %q", sqliteFile.MimeType(), tc.expectedMime)
			}
		})
	}
}

func TestMimeTypeForDirectory(t *testing.T) {
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

	// Create a file in a directory to ensure directory exists
	writer := fs.NewWriter("dir/file.txt")
	writer.Write([]byte("test"))
	writer.Close()

	// Open the directory
	dir, err := fs.Open("dir/")
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	// Directory should have empty MIME type
	sqliteDir := dir.(*sqlitefs.SQLiteFile)
	if sqliteDir.MimeType() != "" {
		t.Errorf("Directory MIME type should be empty, got %q", sqliteDir.MimeType())
	}
}