package sqlitefs

import (
	"database/sql"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// dirEntry implements fs.DirEntry interface
type dirEntry struct {
	info os.FileInfo
}

func (d *dirEntry) Name() string {
	return d.info.Name()
}

func (d *dirEntry) IsDir() bool {
	return d.info.IsDir()
}

func (d *dirEntry) Type() fs.FileMode {
	return d.info.Mode().Type()
}

func (d *dirEntry) Info() (fs.FileInfo, error) {
	return d.info, nil
}

// SQLiteFile implements the fs.File and fs.ReadDirFile interfaces.
type SQLiteFile struct {
	db     *sql.DB
	path   string
	offset int64 // current offset for read operations
	size   int64 // total file size
	isDir  bool  // whether this is a directory
}

// NewSQLiteFile creates a new SQLiteFile instance for the given path.
func NewSQLiteFile(db *sql.DB, path string) (*SQLiteFile, error) {
	// Check if path is a directory (ends with /)
	isDir := false
	if path == "" || path == "/" || (len(path) > 0 && path[len(path)-1] == '/') {
		isDir = true
	}

	file := &SQLiteFile{
		db:    db,
		path:  path,
		isDir: isDir,
	}

	// Initialize file size if it's not a directory
	if !isDir {
		size, err := file.getTotalSize()
		if err != nil {
			return nil, err
		}
		file.size = size
	}

	return file, nil
}

func (f *SQLiteFile) Read(p []byte) (int, error) {
	// Return EOF for directory reads
	if f.isDir {
		return 0, io.EOF
	}

	// Return EOF if we're at the end of the file
	if f.offset >= f.size {
		return 0, io.EOF
	}

	bytesReadTotal := 0
	for {
		// Calculate current fragment index and offset within that fragment
		fragmentIndex := f.offset / fragmentSize
		internalOffset := f.offset % fragmentSize

		// Determine how many bytes to read from the current fragment
		readLength := min(fragmentSize-internalOffset, int64(len(p))-int64(bytesReadTotal))

		// If we've reached the end of the file, return what we've read so far
		if f.offset >= f.size {
			if bytesReadTotal == 0 {
				return 0, io.EOF
			}
			return bytesReadTotal, nil
		}

		// SQL query to read a substring of the fragment
		query := `
			SELECT SUBSTR(fragment, ?, ?) 
			FROM file_fragments 
			WHERE file_id = (SELECT id FROM file_metadata WHERE path = ?) 
			AND fragment_index = ?
		`
		row := f.db.QueryRow(query, internalOffset+1, readLength, f.path, fragmentIndex)

		var fragment []byte
		err := row.Scan(&fragment)
		if err != nil {
			if err == sql.ErrNoRows {
				// If we've read some data, return the count
				if bytesReadTotal > 0 {
					return bytesReadTotal, nil
				}
				// Otherwise return EOF
				return 0, io.EOF
			}
			return bytesReadTotal, err
		}

		// Copy the read data to buffer p
		bytesRead := copy(p[bytesReadTotal:], fragment)
		bytesReadTotal += bytesRead
		f.offset += int64(bytesRead) // Update file offset

		// If bytesRead is 0 and this is the last fragment, return what we've read
		if bytesRead == 0 {
			if f.offset >= f.size {
				return bytesReadTotal, nil
			}
			continue // Continue reading the next fragment
		}

		// If we've read all requested data, return the result
		if bytesReadTotal == len(p) {
			break
		}
	}
	return bytesReadTotal, nil
}

func (f *SQLiteFile) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = f.offset + offset
	case io.SeekEnd:
		totalSize, err := f.getTotalSize()
		if err != nil {
			return 0, err
		}
		newOffset = totalSize + offset
	default:
		return 0, errors.New("sqlitefs: invalid whence")
	}

	if newOffset < 0 {
		return 0, errors.New("sqlitefs: negative position")
	}

	f.offset = newOffset
	return newOffset, nil
}

// ReadDir implements the fs.ReadDirFile interface.
func (f *SQLiteFile) ReadDir(n int) ([]fs.DirEntry, error) {
	// Return an error if this is not a directory
	if !f.isDir {
		return nil, errors.New("not a directory")
	}

	// Ensure path ends with / for directory queries
	dirPath := f.path
	if !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}

	// Query to get files in the directory
	query := `
		SELECT path, type 
		FROM file_metadata 
		WHERE path LIKE ? AND path != ?
	`
	rows, err := f.db.Query(query, dirPath+"%", dirPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []fs.DirEntry
	var seenPaths = make(map[string]bool)
	var path, fileType string

	for rows.Next() {
		err := rows.Scan(&path, &fileType)
		if err != nil {
			return nil, err
		}

		// Skip the directory itself
		if path == dirPath {
			continue
		}

		// Extract the immediate child name
		relPath := strings.TrimPrefix(path, dirPath)
		parts := strings.SplitN(relPath, "/", 2)
		childName := parts[0]

		// If this is a subdirectory entry, add a trailing slash
		isSubDir := len(parts) > 1 || strings.HasSuffix(path, "/")
		childPath := dirPath + childName
		if isSubDir && !strings.HasSuffix(childPath, "/") {
			childPath += "/"
		}

		// Skip if we've already seen this immediate child
		if seenPaths[childPath] {
			continue
		}
		seenPaths[childPath] = true

		// Create FileInfo for this child
		fileInfo, err := f.createFileInfo(childPath)
		if err != nil {
			return nil, err
		}

		// Convert FileInfo to DirEntry
		entries = append(entries, &dirEntry{info: fileInfo})

		if n > 0 && len(entries) >= n {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// If no entries were found, check if the directory exists
	if len(entries) == 0 {
		var exists bool
		err := f.db.QueryRow("SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path LIKE ?)", dirPath+"%").Scan(&exists)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, errors.New("directory not found")
		}
	}

	return entries, nil
}

// Readdir is kept for backward compatibility
func (f *SQLiteFile) Readdir(count int) ([]os.FileInfo, error) {
	// Return an error if this is not a directory
	if !f.isDir {
		return nil, errors.New("not a directory")
	}

	// Ensure path ends with / for directory queries
	dirPath := f.path
	if !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}

	// Query to get files in the directory
	query := `
		SELECT path, type 
		FROM file_metadata 
		WHERE path LIKE ? AND path != ?
	`
	rows, err := f.db.Query(query, dirPath+"%", dirPath)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fileInfos []os.FileInfo
	var seenPaths = make(map[string]bool)
	var path, fileType string

	for rows.Next() {
		err := rows.Scan(&path, &fileType)
		if err != nil {
			return nil, err
		}

		// Skip the directory itself
		if path == dirPath {
			continue
		}

		// Extract the immediate child name
		relPath := strings.TrimPrefix(path, dirPath)
		parts := strings.SplitN(relPath, "/", 2)
		childName := parts[0]

		// If this is a subdirectory entry, add a trailing slash
		isSubDir := len(parts) > 1 || strings.HasSuffix(path, "/")
		childPath := dirPath + childName
		if isSubDir && !strings.HasSuffix(childPath, "/") {
			childPath += "/"
		}

		// Skip if we've already seen this immediate child
		if seenPaths[childPath] {
			continue
		}
		seenPaths[childPath] = true

		// Create FileInfo for this child
		fileInfo, err := f.createFileInfo(childPath)
		if err != nil {
			return nil, err
		}

		fileInfos = append(fileInfos, fileInfo)

		if count > 0 && len(fileInfos) >= count {
			break
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	// If no entries were found, check if the directory exists
	if len(fileInfos) == 0 {
		var exists bool
		err := f.db.QueryRow("SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path LIKE ?)", dirPath+"%").Scan(&exists)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, errors.New("directory not found")
		}
	}

	return fileInfos, nil
}

func (f *SQLiteFile) Stat() (os.FileInfo, error) {
	return f.createFileInfo(f.path)
}

func (f *SQLiteFile) Close() error {
	return nil
}

func (f *SQLiteFile) createFileInfo(path string) (os.FileInfo, error) {
	// Determine if the path is a directory
	isDir := strings.HasSuffix(path, "/")

	var size int64
	var modTime time.Time = time.Now() // Use current time as default

	if !isDir {
		// Get file size
		query := `
			SELECT SUM(LENGTH(fragment)) 
			FROM file_fragments 
			WHERE file_id = (SELECT id FROM file_metadata WHERE path = ?)
		`
		err := f.db.QueryRow(query, path).Scan(&size)
		if err != nil {
			if err == sql.ErrNoRows {
				// If no fragments found, check if the file exists in metadata
				var exists bool
				err = f.db.QueryRow("SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)", path).Scan(&exists)
				if err != nil {
					return nil, err
				}
				if !exists {
					return nil, os.ErrNotExist
				}
				// File exists but has no content
				size = 0
			} else {
				return nil, err
			}
		}
	} else {
		// For directories, check if they exist by looking for files with this prefix
		var exists bool
		dirPath := path
		if !strings.HasSuffix(dirPath, "/") {
			dirPath += "/"
		}

		err := f.db.QueryRow("SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path LIKE ?)", dirPath+"%").Scan(&exists)
		if err != nil {
			return nil, err
		}
		if !exists {
			// Also check if this exact path exists in metadata (empty directory)
			err = f.db.QueryRow("SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)", path).Scan(&exists)
			if err != nil {
				return nil, err
			}
			if !exists {
				return nil, os.ErrNotExist
			}
		}
	}

	// Get the base name, handling special cases
	name := filepath.Base(path)
	if name == "/" || name == "." || name == "" {
		name = "/"
	} else if strings.HasSuffix(path, "/") && name != "/" {
		// Remove trailing slash for directory names
		name = filepath.Base(strings.TrimSuffix(path, "/"))
	}

	return &fileInfo{
		name:    name,
		size:    size,
		modTime: modTime,
		isDir:   isDir,
	}, nil
}

func (f *SQLiteFile) getTotalSize() (int64, error) {
	// Get the number of fragments and the size of the last fragment
	query := `
	SELECT COUNT(*), COALESCE(LENGTH(fragment), 0)
	FROM file_fragments
	WHERE file_id = (SELECT id FROM file_metadata WHERE path = ?)
	ORDER BY fragment_index DESC
	LIMIT 1;
	`

	var count, lastFragmentSize int
	err := f.db.QueryRow(query, f.path).Scan(&count, &lastFragmentSize)
	if err != nil {
		if err == sql.ErrNoRows {
			// Check if the file exists in metadata but has no fragments
			var exists bool
			err = f.db.QueryRow("SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)", f.path).Scan(&exists)
			if err != nil {
				return 0, err
			}
			if exists {
				// File exists but has no content
				return 0, nil
			}
			return 0, os.ErrNotExist
		}
		return 0, err
	}

	if count == 0 {
		return 0, nil
	}

	// Calculate the total file size
	totalSize := int64((count-1)*fragmentSize + lastFragmentSize)
	return totalSize, nil
}
