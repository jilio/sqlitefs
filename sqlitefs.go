package sqlitefs

import (
	"database/sql"
	"errors"
	"io/fs"
	"sync"
)

type writeRequest struct {
	path     string
	data     []byte
	index    int
	mimeType string
	respCh   chan error
}

type SQLiteFS struct {
	db       *sql.DB
	writeCh  chan writeRequest
	writerWg sync.WaitGroup
}

var _ fs.FS = (*SQLiteFS)(nil)

// NewSQLiteFS создает новый экземпляр SQLiteFS с заданной базой данных.
// Проверяет наличие необходимых таблиц и создает их при отсутствии.
func NewSQLiteFS(db *sql.DB) (*SQLiteFS, error) {
	fs := &SQLiteFS{
		db:      db,
		writeCh: make(chan writeRequest),
	}

	err := fs.createTablesIfNeeded()
	if err != nil {
		return nil, err
	}

	fs.writerWg.Add(1)
	go fs.writerLoop()

	return fs, nil
}

// NewWriter creates a new writer for the specified path.
func (fs *SQLiteFS) NewWriter(path string) *SQLiteWriter {
	return NewSQLiteWriter(fs, path)
}

// Open opens the named file.
func (fs *SQLiteFS) Open(name string) (fs.File, error) {
	// Clean the path - remove leading slash if present
	if name == "" || name == "." {
		name = "/"
	}

	// Remove leading slash for database lookup
	dbPath := name
	if len(dbPath) > 0 && dbPath[0] == '/' {
		dbPath = dbPath[1:]
	}

	// Check if the file exists directly
	var exists bool
	err := fs.db.QueryRow("SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)", dbPath).Scan(&exists)
	if err != nil {
		return nil, err
	}

	if exists {
		return NewSQLiteFile(fs.db, dbPath)
	}

	// If not found directly, check if it's a directory by looking for files with this prefix
	// This handles the case where the directory itself isn't explicitly stored
	dirPath := dbPath
	if len(dirPath) > 0 && dirPath[len(dirPath)-1] != '/' {
		dirPath += "/"
	}

	err = fs.db.QueryRow("SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path LIKE ? LIMIT 1)", dirPath+"%").Scan(&exists)
	if err != nil {
		return nil, err
	}

	if exists {
		// It's a directory, create a directory file
		return NewSQLiteFile(fs.db, dirPath)
	}

	return nil, fs.Error("file does not exist", name)
}

// Error returns a formatted error that includes the path
func (fs *SQLiteFS) Error(msg, path string) error {
	return &PathError{Op: "open", Path: path, Err: errors.New(msg)}
}

// PathError records an error and the operation and file path that caused it.
type PathError struct {
	Op   string
	Path string
	Err  error
}

func (e *PathError) Error() string {
	return e.Op + " " + e.Path + ": " + e.Err.Error()
}

// createTablesIfNeeded создает таблицы file_metadata и file_fragments, если они еще не созданы.
func (fs *SQLiteFS) createTablesIfNeeded() error {
	_, err := fs.db.Exec(`
        CREATE TABLE IF NOT EXISTS file_metadata (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT UNIQUE NOT NULL,
            type TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS file_fragments (
            file_id INTEGER NOT NULL,
            fragment_index INTEGER NOT NULL,
            fragment BLOB NOT NULL,
            PRIMARY KEY (file_id, fragment_index),
            FOREIGN KEY (file_id) REFERENCES file_metadata(id)
        );
        CREATE INDEX IF NOT EXISTS idx_file_metadata_path ON file_metadata(path);
        CREATE INDEX IF NOT EXISTS idx_file_fragments_length ON file_fragments(file_id, length(fragment));
    `)

	return err
}

func (fs *SQLiteFS) writerLoop() {
	defer fs.writerWg.Done()

	for req := range fs.writeCh {
		var err error
		if req.mimeType != "" {
			err = fs.createFileRecord(req.path, req.mimeType)
		} else {
			err = fs.writeFragment(req.path, req.data, req.index)
		}
		req.respCh <- err
	}
}

func (fs *SQLiteFS) createFileRecord(path, mimeType string) error {
	_, err := fs.db.Exec("INSERT OR REPLACE INTO file_metadata (path, type) VALUES (?, ?)", path, mimeType)
	return err
}

func (fs *SQLiteFS) writeFragment(path string, data []byte, index int) error {
	tx, err := fs.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var fileID int64
	err = tx.QueryRow("SELECT id FROM file_metadata WHERE path = ?", path).Scan(&fileID)
	if err != nil {
		return err
	}

	_, err = tx.Exec("INSERT OR REPLACE INTO file_fragments (file_id, fragment_index, fragment) VALUES (?, ?, ?)",
		fileID, index, data)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (fs *SQLiteFS) Close() error {
	close(fs.writeCh)
	fs.writerWg.Wait()
	return fs.db.Close()
}
