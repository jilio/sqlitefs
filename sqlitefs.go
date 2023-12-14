package sqlitefs

import (
	"database/sql"
	"errors"
	"net/http"
)

// SQLiteFS реализует интерфейс http.FileSystem для файлов, хранящихся в SQLite.
type SQLiteFS struct {
	db *sql.DB
}

// NewSQLiteFS создает новый экземпляр SQLiteFS с заданной базой данных.
// Проверяет наличие необходимых таблиц и создает их при отсутствии.
func NewSQLiteFS(db *sql.DB) (*SQLiteFS, error) {
	fs := &SQLiteFS{db: db}

	// Создание таблиц, если они не существуют
	err := fs.createTablesIfNeeded()
	if err != nil {
		return nil, err
	}

	return fs, nil
}

// Open открывает файл по указанному пути.
func (fs *SQLiteFS) Open(name string) (http.File, error) {
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
	return NewSQLiteFile(fs.db, name), nil
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
