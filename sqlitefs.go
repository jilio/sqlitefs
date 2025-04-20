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

// Open открывает файл по указанному пути.
func (fs *SQLiteFS) Open(name string) (fs.File, error) {
	// Проверка существования файла в базе данных
	var exists bool
	err := fs.db.QueryRow("SELECT EXISTS(SELECT 1 FROM file_metadata WHERE path = ?)", name).Scan(&exists)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, errors.New("file does not exist")
	}

	// Создание и возврат объекта, реализующего интерфейс File
	return NewSQLiteFile(fs.db, name)
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
