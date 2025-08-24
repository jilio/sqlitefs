package tests

import (
	"database/sql"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// This file tests error paths by corrupting the database structure

func TestWithCorruptedDatabase(t *testing.T) {
	t.Run("CorruptedFragmentsTable", func(t *testing.T) {
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
		writer := fs.NewWriter("test.txt")
		writer.Write([]byte("test"))
		writer.Close()

		// Corrupt the fragments table
		_, err = db.Exec("DROP TABLE file_fragments")
		if err != nil {
			t.Fatal(err)
		}

		// Try to read the file - should fail
		file, err := fs.Open("test.txt")
		if err != nil {
			// Expected error
			return
		}

		// Try to read - should definitely fail
		buf := make([]byte, 10)
		_, err = file.Read(buf)
		if err == nil {
			t.Error("Expected error reading from corrupted database")
		}

		// Try to stat - should fail in getTotalSize
		_, err = file.Stat()
		if err == nil {
			t.Error("Expected error in getTotalSize with missing table")
		}
	})

	t.Run("CorruptedMetadataTable", func(t *testing.T) {
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
		writer := fs.NewWriter("test.txt")
		writer.Write([]byte("test"))
		writer.Close()

		// Open the file while it's still valid
		file, err := fs.Open("test.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Now corrupt the metadata table
		_, err = db.Exec("DROP TABLE file_metadata")
		if err != nil {
			t.Fatal(err)
		}

		// Try to stat - should fail when checking file existence
		_, err = file.Stat()
		if err == nil {
			t.Error("Expected error in getTotalSize with missing metadata table")
		}

		// Try to read directory
		dir, err := fs.Open("/")
		if err != nil {
			// Expected
			return
		}

		if rd, ok := dir.(interface {
			ReadDir(int) ([]interface{}, error)
		}); ok {
			_, err = rd.ReadDir(-1)
			if err == nil {
				t.Error("Expected error reading directory with missing metadata table")
			}
		}
	})

	t.Run("InvalidSQLInTables", func(t *testing.T) {
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
		writer := fs.NewWriter("test.txt")
		writer.Write([]byte("test"))
		writer.Close()

		// Add invalid data that will cause scan errors
		_, err = db.Exec("INSERT INTO file_fragments (file_id, fragment_index, fragment) VALUES (-1, -1, NULL)")
		if err != nil {
			// Some constraint might prevent this
		}

		// Try various operations that might fail
		file, err := fs.Open("test.txt")
		if err != nil {
			return
		}

		buf := make([]byte, 10)
		file.Read(buf)
		file.Stat()
		// We're just trying to trigger error paths
	})
}

func TestForceErrorPaths(t *testing.T) {
	t.Run("ForceGetTotalSizeQueryError", func(t *testing.T) {
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
		writer := fs.NewWriter("test.txt")
		writer.Write([]byte("content"))
		writer.Close()

		// Open the file
		file, err := fs.Open("test.txt")
		if err != nil {
			t.Fatal(err)
		}

		// Break the file_fragments table structure
		db.Exec("ALTER TABLE file_fragments RENAME TO file_fragments_old")
		db.Exec("CREATE TABLE file_fragments (dummy TEXT)")

		// Stat should fail with query error
		_, err = file.Stat()
		if err == nil {
			t.Error("Expected error from getTotalSize with broken table")
		}
	})

	t.Run("ForceReadQueryError", func(t *testing.T) {
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

		// Create a file with multiple fragments
		writer := fs.NewWriter("test.txt")
		data := make([]byte, 16384) // 2 fragments
		writer.Write(data)
		writer.Close()

		// Open and read first fragment
		file, err := fs.Open("test.txt")
		if err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 8192)
		file.Read(buf) // Read first fragment

		// Break the table before reading second fragment
		db.Exec("DROP TABLE file_fragments")

		// Try to read second fragment - should fail
		_, err = file.Read(buf)
		if err == nil {
			t.Error("Expected error reading from missing table")
		}
	})

	t.Run("ForceReadDirQueryError", func(t *testing.T) {
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

		// Create directory structure
		writer := fs.NewWriter("dir/file.txt")
		writer.Write([]byte("content"))
		writer.Close()

		// Open directory
		dir, err := fs.Open("dir")
		if err != nil {
			t.Fatal(err)
		}

		// Break the metadata table
		db.Exec("DROP TABLE file_metadata")

		// Try to ReadDir - should fail
		if rd, ok := dir.(interface {
			ReadDir(int) ([]interface{}, error)
		}); ok {
			_, err = rd.ReadDir(-1)
			if err == nil {
				t.Error("Expected error from ReadDir with missing table")
			}
		}

		// Try Readdir - should fail
		if rd, ok := dir.(interface {
			Readdir(int) ([]interface{}, error)
		}); ok {
			_, err = rd.Readdir(-1)
			if err == nil {
				t.Error("Expected error from Readdir with missing table")
			}
		}
	})
}
