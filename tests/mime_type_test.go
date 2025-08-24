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
		filename      string
		acceptedMimes []string // Accept multiple possible MIME types for environment differences
	}{
		{"test.txt", []string{"text/plain; charset=utf-8", "text/plain"}},
		{"image.jpg", []string{"image/jpeg"}},
		{"data.json", []string{"application/json"}},
		{"style.css", []string{"text/css; charset=utf-8", "text/css"}},
		{"script.js", []string{"application/javascript", "text/javascript; charset=utf-8", "text/javascript"}},
		{"document.pdf", []string{"application/pdf"}},
		{"unknown.xyz", []string{"chemical/x-xyz", "application/octet-stream"}}, // May vary by environment
		{"unknown.unknownext", []string{"application/octet-stream"}},
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
			} else {
				// Check if the stored MIME type is one of the accepted values
				found := false
				for _, accepted := range tc.acceptedMimes {
					if storedMime.String == accepted {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("MIME type %q not in accepted list %v", storedMime.String, tc.acceptedMimes)
				}
			}

			// Open the file and check MIME type via the file object
			file, err := fs.Open(tc.filename)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()

			sqliteFile := file.(*sqlitefs.SQLiteFile)
			found := false
			for _, accepted := range tc.acceptedMimes {
				if sqliteFile.MimeType() == accepted {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("File.MimeType() returned %q, not in accepted list %v", sqliteFile.MimeType(), tc.acceptedMimes)
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
