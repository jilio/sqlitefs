package tests

import (
	"database/sql"
	"io"
	"os"
	"testing"

	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

// setupTestDB creates a test database with shared cache for testing
func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite", "file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	return db
}

// TestBasicFileOperations tests core file creation, reading, and writing
func TestBasicFileOperations(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatalf("Failed to create SQLiteFS: %v", err)
	}
	defer fs.Close()

	t.Run("CreateAndRead", func(t *testing.T) {
		writer := fs.NewWriter("test.txt")
		data := []byte("Hello, World!")
		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
		if n != len(data) {
			t.Errorf("Expected to write %d bytes, got %d", len(data), n)
		}
		writer.Close()

		file, err := fs.Open("test.txt")
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()

		buf := make([]byte, 100)
		n, err = file.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read: %v", err)
		}
		if string(buf[:n]) != string(data) {
			t.Errorf("Expected %q, got %q", string(data), string(buf[:n]))
		}
	})

	t.Run("LargeFile", func(t *testing.T) {
		writer := fs.NewWriter("large.bin")
		size := 100 * 1024
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i % 256)
		}
		
		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("Failed to write large file: %v", err)
		}
		if n != size {
			t.Errorf("Expected to write %d bytes, got %d", size, n)
		}
		writer.Close()

		file, err := fs.Open("large.bin")
		if err != nil {
			t.Fatalf("Failed to open large file: %v", err)
		}
		defer file.Close()

		readData := make([]byte, size)
		totalRead := 0
		for totalRead < size {
			n, err := file.Read(readData[totalRead:])
			if err != nil && err != io.EOF {
				t.Fatalf("Failed to read at offset %d: %v", totalRead, err)
			}
			totalRead += n
			if err == io.EOF {
				break
			}
		}
		
		if totalRead != size {
			t.Errorf("Expected to read %d bytes, got %d", size, totalRead)
		}
		
		for i := 0; i < size; i++ {
			if readData[i] != data[i] {
				t.Errorf("Mismatch at byte %d: expected %d, got %d", i, data[i], readData[i])
				break
			}
		}
	})

	t.Run("EmptyFile", func(t *testing.T) {
		writer := fs.NewWriter("empty.txt")
		writer.Close()

		file, err := fs.Open("empty.txt")
		if err != nil {
			t.Fatalf("Failed to open empty file: %v", err)
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			t.Fatalf("Failed to stat empty file: %v", err)
		}
		if info.Size() != 0 {
			t.Errorf("Expected size 0, got %d", info.Size())
		}

		buf := make([]byte, 10)
		n, err := file.Read(buf)
		if err != io.EOF {
			t.Errorf("Expected EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("Expected 0 bytes read, got %d", n)
		}
	})

	t.Run("NonExistentFile", func(t *testing.T) {
		_, err := fs.Open("does_not_exist.txt")
		if err == nil {
			t.Error("Expected error for non-existent file")
		}
	})

	t.Run("PathVariations", func(t *testing.T) {
		writer := fs.NewWriter("path_test.txt")
		writer.Write([]byte("test"))
		writer.Close()

		paths := []string{"path_test.txt", "/path_test.txt"}
		for _, path := range paths {
			file, err := fs.Open(path)
			if err != nil {
				t.Errorf("Failed to open with path %q: %v", path, err)
			} else {
				file.Close()
			}
		}
	})
}

// TestFileSeek tests file seeking operations
func TestFileSeek(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatalf("Failed to create SQLiteFS: %v", err)
	}
	defer fs.Close()

	writer := fs.NewWriter("seek_test.txt")
	data := []byte("0123456789ABCDEFGHIJ")
	writer.Write(data)
	writer.Close()

	file, err := fs.Open("seek_test.txt")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	seeker, ok := file.(io.Seeker)
	if !ok {
		t.Fatal("File does not implement io.Seeker")
	}

	tests := []struct {
		offset   int64
		whence   int
		expected int64
		readChar byte
	}{
		{5, io.SeekStart, 5, '5'},
		{3, io.SeekCurrent, 9, '9'},
		{-5, io.SeekEnd, 15, 'F'},
		{0, io.SeekStart, 0, '0'},
	}

	for i, tt := range tests {
		pos, err := seeker.Seek(tt.offset, tt.whence)
		if err != nil {
			t.Errorf("Test %d: Seek failed: %v", i, err)
			continue
		}
		if pos != tt.expected {
			t.Errorf("Test %d: Expected position %d, got %d", i, tt.expected, pos)
		}

		buf := make([]byte, 1)
		n, err := file.Read(buf)
		if err != nil && err != io.EOF {
			t.Errorf("Test %d: Read failed: %v", i, err)
			continue
		}
		if n == 1 && buf[0] != tt.readChar {
			t.Errorf("Test %d: Expected to read '%c', got '%c'", i, tt.readChar, buf[0])
		}
	}

	// Test error cases
	_, err = seeker.Seek(0, 99) // Invalid whence
	if err == nil {
		t.Error("Expected error for invalid whence")
	}

	_, err = seeker.Seek(-10, io.SeekStart) // Negative position
	if err == nil {
		t.Error("Expected error for negative position")
	}
}

// TestFileInfo tests file metadata operations
func TestFileInfo(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatalf("Failed to create SQLiteFS: %v", err)
	}
	defer fs.Close()

	writer := fs.NewWriter("info_test.txt")
	data := []byte("Test content for file info")
	writer.Write(data)
	writer.Close()

	file, err := fs.Open("info_test.txt")
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if info.Name() != "info_test.txt" {
		t.Errorf("Expected name 'info_test.txt', got '%s'", info.Name())
	}

	if info.Size() != int64(len(data)) {
		t.Errorf("Expected size %d, got %d", len(data), info.Size())
	}

	if info.IsDir() {
		t.Error("Expected file, got directory")
	}

	if info.Mode() != 0644 {
		t.Errorf("Expected mode 0644, got %v", info.Mode())
	}

	// Test interface methods
	if info.ModTime().IsZero() {
		t.Error("ModTime should not be zero")
	}

	if info.Sys() != nil {
		t.Errorf("Sys() should return nil, got %v", info.Sys())
	}
}

// TestDirectoryOperations tests directory listing and navigation
func TestDirectoryOperations(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatalf("Failed to create SQLiteFS: %v", err)
	}
	defer fs.Close()

	// Create directory structure
	files := []struct {
		path    string
		content string
	}{
		{"file1.txt", "Content 1"},
		{"file2.txt", "Content 2"},
		{"dir1/file3.txt", "Content 3"},
		{"dir1/file4.txt", "Content 4"},
		{"dir1/subdir/file5.txt", "Content 5"},
		{"dir2/file6.txt", "Content 6"},
	}

	for _, f := range files {
		writer := fs.NewWriter(f.path)
		writer.Write([]byte(f.content))
		writer.Close()
	}

	t.Run("ReadRootDir", func(t *testing.T) {
		dir, err := fs.Open("/")
		if err != nil {
			t.Fatalf("Failed to open root directory: %v", err)
		}
		defer dir.Close()

		readDirFile, ok := dir.(interface {
			ReadDir(n int) ([]os.DirEntry, error)
		})
		if !ok {
			t.Fatal("Directory does not implement ReadDirFile")
		}

		entries, err := readDirFile.ReadDir(-1)
		if err != nil {
			t.Fatalf("Failed to read directory: %v", err)
		}

		if len(entries) != 4 {
			t.Errorf("Expected 4 entries, got %d", len(entries))
		}

		expectedNames := map[string]bool{
			"file1.txt": false,
			"file2.txt": false,
			"dir1":      true,
			"dir2":      true,
		}

		for _, entry := range entries {
			expectedIsDir, found := expectedNames[entry.Name()]
			if !found {
				t.Errorf("Unexpected entry: %s", entry.Name())
				continue
			}
			if entry.IsDir() != expectedIsDir {
				t.Errorf("Entry %s: IsDir() = %v, expected %v",
					entry.Name(), entry.IsDir(), expectedIsDir)
			}

			// Test Type() method
			mode := entry.Type()
			if entry.IsDir() {
				if mode != os.ModeDir {
					t.Errorf("Directory Type() should return ModeDir, got %v", mode)
				}
			} else {
				if mode != 0 {
					t.Errorf("File Type() should return 0, got %v", mode)
				}
			}

			// Test Info() method
			info, err := entry.Info()
			if err != nil {
				t.Errorf("Info() failed: %v", err)
			}
			if info.Name() != entry.Name() {
				t.Errorf("Info name mismatch: got %s, want %s", info.Name(), entry.Name())
			}
		}
	})

	t.Run("ReadSubDir", func(t *testing.T) {
		dir, err := fs.Open("dir1/")
		if err != nil {
			t.Fatalf("Failed to open dir1: %v", err)
		}
		defer dir.Close()

		readDirFile, ok := dir.(interface {
			ReadDir(n int) ([]os.DirEntry, error)
		})
		if !ok {
			t.Fatal("Directory does not implement ReadDirFile")
		}

		entries, err := readDirFile.ReadDir(-1)
		if err != nil {
			t.Fatalf("Failed to read directory: %v", err)
		}

		if len(entries) != 3 {
			t.Errorf("Expected 3 entries, got %d", len(entries))
		}
	})

	t.Run("DirectoryStat", func(t *testing.T) {
		dir, err := fs.Open("dir1/")
		if err != nil {
			t.Fatalf("Failed to open directory: %v", err)
		}
		defer dir.Close()

		info, err := dir.Stat()
		if err != nil {
			t.Fatalf("Failed to stat directory: %v", err)
		}

		if !info.IsDir() {
			t.Error("Expected directory, got file")
		}

		if info.Name() != "dir1" {
			t.Errorf("Expected name 'dir1', got '%s'", info.Name())
		}

		expectedMode := os.ModeDir | 0755
		if info.Mode() != expectedMode {
			t.Errorf("Expected mode %v, got %v", expectedMode, info.Mode())
		}
	})

	t.Run("NonExistentDirectory", func(t *testing.T) {
		_, err := fs.Open("nonexistent/")
		if err == nil {
			t.Error("Expected error for non-existent directory")
		}
	})

	t.Run("BackwardCompatibility", func(t *testing.T) {
		dir, err := fs.Open("/")
		if err != nil {
			t.Fatalf("Failed to open root: %v", err)
		}
		defer dir.Close()

		type readdirer interface {
			Readdir(count int) ([]os.FileInfo, error)
		}

		if file, ok := dir.(readdirer); ok {
			infos, err := file.Readdir(-1)
			if err != nil {
				t.Fatalf("Readdir failed: %v", err)
			}
			if len(infos) == 0 {
				t.Error("Expected entries from Readdir")
			}

			// Test with limit
			dir2, _ := fs.Open("/")
			defer dir2.Close()
			if file2, ok := dir2.(readdirer); ok {
				infos2, err := file2.Readdir(1)
				if err != nil {
					t.Fatalf("Readdir(1) failed: %v", err)
				}
				if len(infos2) != 1 {
					t.Errorf("Expected 1 entry with limit, got %d", len(infos2))
				}
			}
		}
	})
}

// TestWriterOperations tests various writer scenarios
func TestWriterOperations(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatalf("Failed to create SQLiteFS: %v", err)
	}
	defer fs.Close()

	t.Run("MultipleWrites", func(t *testing.T) {
		writer := fs.NewWriter("multi.txt")
		chunks := [][]byte{
			[]byte("First "),
			[]byte("Second "),
			[]byte("Third"),
		}

		totalBytes := 0
		for _, chunk := range chunks {
			n, err := writer.Write(chunk)
			if err != nil {
				t.Fatalf("Failed to write chunk: %v", err)
			}
			if n != len(chunk) {
				t.Errorf("Expected to write %d bytes, wrote %d", len(chunk), n)
			}
			totalBytes += n
		}

		writer.Close()

		if totalBytes != 18 {
			t.Errorf("Expected total of 18 bytes, got %d", totalBytes)
		}
	})

	t.Run("WriteAfterClose", func(t *testing.T) {
		writer := fs.NewWriter("closed.txt")
		writer.Write([]byte("data"))
		writer.Close()

		_, err := writer.Write([]byte("more data"))
		if err == nil {
			t.Error("Expected error when writing to closed writer")
		}
	})

	t.Run("MultipleClose", func(t *testing.T) {
		writer := fs.NewWriter("multi_close.txt")
		writer.Write([]byte("data"))
		
		err := writer.Close()
		if err != nil {
			t.Errorf("First close failed: %v", err)
		}
		
		err = writer.Close()
		if err != nil {
			t.Errorf("Second close should succeed: %v", err)
		}
	})

	t.Run("LargeFragmentWrite", func(t *testing.T) {
		writer := fs.NewWriter("fragments.bin")
		fragmentSize := 16 * 1024
		data := make([]byte, fragmentSize*2+100)
		for i := range data {
			data[i] = byte(i % 256)
		}
		
		n, err := writer.Write(data)
		if err != nil {
			t.Fatalf("Failed to write: %v", err)
		}
		if n != len(data) {
			t.Errorf("Expected to write %d bytes, got %d", len(data), n)
		}
		writer.Close()
	})
}

// TestErrorHandling tests various error conditions
func TestErrorHandling(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatalf("Failed to create SQLiteFS: %v", err)
	}
	defer fs.Close()

	t.Run("PathError", func(t *testing.T) {
		_, err := fs.Open("nonexistent")
		if err == nil {
			t.Fatal("Expected error")
		}
		
		errStr := err.Error()
		if errStr == "" {
			t.Error("Error string should not be empty")
		}
	})

	t.Run("ReadDirectory", func(t *testing.T) {
		dir, _ := fs.Open("/")
		defer dir.Close()

		buf := make([]byte, 10)
		n, err := dir.Read(buf)
		if err != io.EOF {
			t.Errorf("Expected EOF when reading directory, got %v", err)
		}
		if n != 0 {
			t.Errorf("Expected 0 bytes from directory read, got %d", n)
		}
	})
}

// TestReaddir tests the legacy Readdir method for backward compatibility
func TestReaddir(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatalf("Failed to create SQLiteFS: %v", err)
	}
	defer fs.Close()

	// Create test files
	for _, path := range []string{"file1.txt", "dir1/file2.txt", "dir1/file3.txt", "dir2/file4.txt"} {
		writer := fs.NewWriter(path)
		writer.Write([]byte("test content"))
		writer.Close()
	}

	t.Run("ReaddirWithCount", func(t *testing.T) {
		file, err := fs.Open("/")
		if err != nil {
			t.Fatalf("Failed to open root: %v", err)
		}
		defer file.Close()

		sqlFile := file.(*sqlitefs.SQLiteFile)
		
		// Read first 2 entries
		infos, err := sqlFile.Readdir(2)
		if err != nil {
			t.Fatalf("Failed to readdir: %v", err)
		}
		if len(infos) != 2 {
			t.Errorf("Expected 2 entries, got %d", len(infos))
		}
		
		// Read remaining entries
		infos, err = sqlFile.Readdir(-1)
		if err != nil {
			t.Fatalf("Failed to readdir remaining: %v", err)
		}
		if len(infos) < 1 {
			t.Errorf("Expected at least 1 more entry, got %d", len(infos))
		}
	})

	t.Run("ReaddirNonDirectory", func(t *testing.T) {
		file, err := fs.Open("file1.txt")
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()

		sqlFile := file.(*sqlitefs.SQLiteFile)
		_, err = sqlFile.Readdir(-1)
		if err == nil {
			t.Error("Expected error when calling Readdir on non-directory")
		}
	})

	t.Run("ReaddirSubdirectory", func(t *testing.T) {
		file, err := fs.Open("dir1/")
		if err != nil {
			t.Fatalf("Failed to open dir1: %v", err)
		}
		defer file.Close()

		sqlFile := file.(*sqlitefs.SQLiteFile)
		infos, err := sqlFile.Readdir(-1)
		if err != nil {
			t.Fatalf("Failed to readdir: %v", err)
		}
		if len(infos) != 2 {
			t.Errorf("Expected 2 entries in dir1, got %d", len(infos))
		}
	})

	t.Run("ReaddirEmptyDirectory", func(t *testing.T) {
		// Create an empty directory (just create a file with trailing slash)
		writer := fs.NewWriter("emptydir/placeholder")
		writer.Write([]byte(""))
		writer.Close()

		// Now try to read the empty directory
		file, err := fs.Open("emptydir/")
		if err != nil {
			t.Fatalf("Failed to open emptydir: %v", err)
		}
		defer file.Close()

		sqlFile := file.(*sqlitefs.SQLiteFile)
		infos, err := sqlFile.Readdir(-1)
		if err != nil {
			t.Fatalf("Failed to readdir empty directory: %v", err)
		}
		// Should have the placeholder file
		if len(infos) != 1 {
			t.Errorf("Expected 1 entry in emptydir, got %d", len(infos))
		}
	})
}

// TestGetTotalSize tests the getTotalSize method edge cases
func TestGetTotalSize(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatalf("Failed to create SQLiteFS: %v", err)
	}
	defer fs.Close()

	t.Run("EmptyFile", func(t *testing.T) {
		// Create an empty file
		writer := fs.NewWriter("empty.txt")
		writer.Close()

		file, err := fs.Open("empty.txt")
		if err != nil {
			t.Fatalf("Failed to open empty file: %v", err)
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			t.Fatalf("Failed to stat empty file: %v", err)
		}
		if info.Size() != 0 {
			t.Errorf("Expected size 0 for empty file, got %d", info.Size())
		}
	})

	t.Run("NonExistentFile", func(t *testing.T) {
		_, err := fs.Open("nonexistent.txt")
		if err == nil {
			t.Error("Expected error opening non-existent file")
		}
	})
	
	t.Run("FileWithFragments", func(t *testing.T) {
		// Create file with multiple fragments
		data := make([]byte, 16384*2+100) // 2+ fragments
		for i := range data {
			data[i] = byte(i % 256)
		}
		
		writer := fs.NewWriter("fragmented.txt")
		writer.Write(data)
		writer.Close()

		file, err := fs.Open("fragmented.txt")
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			t.Fatalf("Failed to stat file: %v", err)
		}
		if info.Size() != int64(len(data)) {
			t.Errorf("Expected size %d, got %d", len(data), info.Size())
		}
		
		// Also test Read to ensure getTotalSize is called
		sqlFile := file.(*sqlitefs.SQLiteFile)
		
		// Seek to near end
		sqlFile.Seek(int64(len(data)-10), io.SeekStart)
		buf := make([]byte, 20)
		n, err := file.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read: %v", err)
		}
		if n != 10 {
			t.Errorf("Expected to read 10 bytes, got %d", n)
		}
	})
}

// TestCreateFileInfo tests various createFileInfo scenarios
func TestCreateFileInfo(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatalf("Failed to create SQLiteFS: %v", err)
	}
	defer fs.Close()

	// Create test structure
	writer := fs.NewWriter("root.txt")
	writer.Write([]byte("root file"))
	writer.Close()

	writer = fs.NewWriter("subdir/file.txt")
	writer.Write([]byte("subdir file"))
	writer.Close()

	t.Run("RootFileInfo", func(t *testing.T) {
		file, err := fs.Open("/")
		if err != nil {
			t.Fatalf("Failed to open root: %v", err)
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			t.Fatalf("Failed to stat root: %v", err)
		}
		if !info.IsDir() {
			t.Error("Root should be a directory")
		}
		if info.Name() != "/" {
			t.Errorf("Expected root name '/', got %s", info.Name())
		}
	})

	t.Run("DirectoryWithTrailingSlash", func(t *testing.T) {
		file, err := fs.Open("subdir/")
		if err != nil {
			t.Fatalf("Failed to open subdir/: %v", err)
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			t.Fatalf("Failed to stat subdir/: %v", err)
		}
		if !info.IsDir() {
			t.Error("subdir/ should be a directory")
		}
	})

	t.Run("EmptyRootDirectory", func(t *testing.T) {
		// Create a new empty filesystem
		db2 := setupTestDB(t)
		defer db2.Close()

		fs2, err := sqlitefs.NewSQLiteFS(db2)
		if err != nil {
			t.Fatalf("Failed to create SQLiteFS: %v", err)
		}
		defer fs2.Close()

		// Try to open root on empty filesystem
		file, err := fs2.Open("/")
		if err != nil {
			t.Fatalf("Failed to open root on empty fs: %v", err)
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			t.Fatalf("Failed to stat root on empty fs: %v", err)
		}
		if !info.IsDir() {
			t.Error("Root should be a directory even on empty fs")
		}
	})
	
	t.Run("NonExistentDirectory", func(t *testing.T) {
		// Try to open a non-existent directory - should fail
		_, err := fs.Open("nonexistent/")
		if err == nil {
			t.Error("Expected error opening non-existent directory")
		}
	})
	
	t.Run("FileNameVariations", func(t *testing.T) {
		// Test various file name patterns
		testCases := []struct {
			path string
			name string
		}{
			{"test/", "test"},
			{"deep/nested/path/", "path"},
			{"file.txt", "file.txt"},
		}
		
		for _, tc := range testCases {
			writer := fs.NewWriter(tc.path)
			writer.Write([]byte("test"))
			writer.Close()
			
			file, err := fs.Open(tc.path)
			if err != nil {
				t.Fatalf("Failed to open %s: %v", tc.path, err)
			}
			defer file.Close()
			
			info, err := file.Stat()
			if err != nil {
				t.Fatalf("Failed to stat %s: %v", tc.path, err)
			}
			
			if info.Name() != tc.name {
				t.Errorf("For path %s, expected name %s, got %s", tc.path, tc.name, info.Name())
			}
		}
	})
}

// TestReadEdgeCases tests Read method edge cases for coverage
func TestReadEdgeCases(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatalf("Failed to create SQLiteFS: %v", err)
	}
	defer fs.Close()

	t.Run("ReadAtEndOfFile", func(t *testing.T) {
		writer := fs.NewWriter("eof.txt")
		writer.Write([]byte("test"))
		writer.Close()

		file, err := fs.Open("eof.txt")
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()

		// Read all content
		buf := make([]byte, 10)
		n, err := file.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read: %v", err)
		}
		if n != 4 {
			t.Errorf("Expected 4 bytes, got %d", n)
		}

		// Try to read again at EOF
		n, err = file.Read(buf)
		if err != io.EOF {
			t.Errorf("Expected EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("Expected 0 bytes at EOF, got %d", n)
		}
	})

	t.Run("ReadExactFragmentBoundary", func(t *testing.T) {
		// Create a file exactly one fragment size
		data := make([]byte, 16384)
		for i := range data {
			data[i] = byte(i % 256)
		}
		
		writer := fs.NewWriter("exact.txt")
		writer.Write(data)
		writer.Close()

		file, err := fs.Open("exact.txt")
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()

		// Read exactly the fragment size
		buf := make([]byte, 16384)
		n, err := file.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read: %v", err)
		}
		if n != 16384 {
			t.Errorf("Expected 16384 bytes, got %d", n)
		}

		// Verify we're at EOF
		n, err = file.Read(buf)
		if err != io.EOF {
			t.Errorf("Expected EOF after reading full fragment, got %v", err)
		}
	})
}

// TestEdgeCases tests various edge cases and error conditions
func TestEdgeCases(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	fs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		t.Fatalf("Failed to create SQLiteFS: %v", err)
	}
	defer fs.Close()


	t.Run("SeekBeyondFileSize", func(t *testing.T) {
		writer := fs.NewWriter("seektest.txt")
		writer.Write([]byte("short"))
		writer.Close()

		file, err := fs.Open("seektest.txt")
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()

		sqlFile := file.(*sqlitefs.SQLiteFile)
		
		// Seek beyond file size
		pos, err := sqlFile.Seek(1000, io.SeekStart)
		if err != nil {
			t.Fatalf("Failed to seek: %v", err)
		}
		if pos != 1000 {
			t.Errorf("Expected position 1000, got %d", pos)
		}

		// Read should return EOF
		buf := make([]byte, 10)
		n, err := file.Read(buf)
		if err != io.EOF {
			t.Errorf("Expected EOF, got %v", err)
		}
		if n != 0 {
			t.Errorf("Expected 0 bytes, got %d", n)
		}
	})

	t.Run("NegativeSeek", func(t *testing.T) {
		writer := fs.NewWriter("negseek.txt")
		writer.Write([]byte("content"))
		writer.Close()

		file, err := fs.Open("negseek.txt")
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()

		sqlFile := file.(*sqlitefs.SQLiteFile)
		
		// Try to seek to negative position
		_, err = sqlFile.Seek(-10, io.SeekStart)
		if err == nil {
			t.Error("Expected error for negative seek position")
		}
	})

	t.Run("LargeFileFragments", func(t *testing.T) {
		// Create a file larger than one fragment (16KB)
		largeData := make([]byte, 20000)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		writer := fs.NewWriter("large.txt")
		n, err := writer.Write(largeData)
		if err != nil {
			t.Fatalf("Failed to write large file: %v", err)
		}
		if n != len(largeData) {
			t.Errorf("Expected to write %d bytes, got %d", len(largeData), n)
		}
		writer.Close()

		// Read it back
		file, err := fs.Open("large.txt")
		if err != nil {
			t.Fatalf("Failed to open large file: %v", err)
		}
		defer file.Close()

		readData := make([]byte, len(largeData))
		n, err = io.ReadFull(file, readData)
		if err != nil {
			t.Fatalf("Failed to read large file: %v", err)
		}
		if n != len(largeData) {
			t.Errorf("Expected to read %d bytes, got %d", len(largeData), n)
		}

		// Verify content
		for i := range largeData {
			if readData[i] != largeData[i] {
				t.Errorf("Data mismatch at position %d: expected %d, got %d", i, largeData[i], readData[i])
				break
			}
		}
	})

	t.Run("MultipleWritesToSameFile", func(t *testing.T) {
		// Write to a file multiple times (should append)
		writer := fs.NewWriter("append.txt")
		writer.Write([]byte("first"))
		writer.Write([]byte(" "))
		writer.Write([]byte("second"))
		writer.Close()

		file, err := fs.Open("append.txt")
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()

		data, err := io.ReadAll(file)
		if err != nil {
			t.Fatalf("Failed to read file: %v", err)
		}
		expected := "first second"
		if string(data) != expected {
			t.Errorf("Expected '%s', got '%s'", expected, string(data))
		}
	})
	
	t.Run("ReadPartialFragment", func(t *testing.T) {
		// Test reading partial data from a fragment
		data := []byte("This is test data for partial reading")
		writer := fs.NewWriter("partial.txt")
		writer.Write(data)
		writer.Close()
		
		file, err := fs.Open("partial.txt")
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()
		
		// Read only first 10 bytes
		buf := make([]byte, 10)
		n, err := file.Read(buf)
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}
		if n != 10 {
			t.Errorf("Expected to read 10 bytes, got %d", n)
		}
		if string(buf) != "This is te" {
			t.Errorf("Expected 'This is te', got '%s'", string(buf))
		}
		
		// Continue reading
		n, err = file.Read(buf)
		if err != nil {
			t.Fatalf("Failed to read: %v", err)
		}
		if n != 10 {
			t.Errorf("Expected to read 10 bytes, got %d", n)
		}
		if string(buf) != "st data fo" {
			t.Errorf("Expected 'st data fo', got '%s'", string(buf))
		}
	})
	
	t.Run("SeekVariations", func(t *testing.T) {
		data := []byte("0123456789ABCDEFGHIJ")
		writer := fs.NewWriter("seekvar.txt")
		writer.Write(data)
		writer.Close()
		
		file, err := fs.Open("seekvar.txt")
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer file.Close()
		
		sqlFile := file.(*sqlitefs.SQLiteFile)
		
		// Test SeekEnd
		pos, err := sqlFile.Seek(-5, io.SeekEnd)
		if err != nil {
			t.Fatalf("Failed to seek from end: %v", err)
		}
		if pos != int64(len(data)-5) {
			t.Errorf("Expected position %d, got %d", len(data)-5, pos)
		}
		
		buf := make([]byte, 5)
		n, err := file.Read(buf)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read: %v", err)
		}
		if n != 5 {
			t.Errorf("Expected to read 5 bytes, got %d", n)
		}
		if string(buf) != "FGHIJ" {
			t.Errorf("Expected 'FGHIJ', got '%s'", string(buf))
		}
		
		// Test SeekCurrent
		sqlFile.Seek(5, io.SeekStart)
		pos, err = sqlFile.Seek(3, io.SeekCurrent)
		if err != nil {
			t.Fatalf("Failed to seek current: %v", err)
		}
		if pos != 8 {
			t.Errorf("Expected position 8, got %d", pos)
		}
	})
}

