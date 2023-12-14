package sqlitefs

import (
	"database/sql"
	"mime"
	"path/filepath"
)

const fragmentSize = 16 * 1024 // 16 КБ

type SQLiteWriter struct {
	db            *sql.DB
	path          string
	buffer        []byte
	fragmentSize  int
	fragmentIndex int
	fileID        int
}

func NewSQLiteWriter(db *sql.DB, path string) *SQLiteWriter {
	return &SQLiteWriter{
		db:           db,
		path:         path,
		fragmentSize: fragmentSize,
		buffer:       make([]byte, 0, fragmentSize),
	}
}

func (w *SQLiteWriter) Write(p []byte) (n int, err error) {
	n = len(p)
	startIndex := 0

	for startIndex < n {
		// Определяем, сколько данных можно добавить в буфер
		remainingSpace := w.fragmentSize - len(w.buffer)
		endIndex := min(n, startIndex+remainingSpace)

		// Добавляем часть данных p в буфер
		w.buffer = append(w.buffer, p[startIndex:endIndex]...)

		// Проверяем, нужно ли записать фрагмент
		if len(w.buffer) == w.fragmentSize {
			err = w.writeFragment()
			if err != nil {
				return startIndex, err
			}
		}

		startIndex = endIndex
	}

	return n, nil
}
func (w *SQLiteWriter) writeFragment() error {
	// Проверка на наличие файла в базе данных и создание записи, если необходимо
	if w.fragmentIndex == 0 {
		err := w.createFileRecord()
		if err != nil {
			return err
		}
	}

	// Запись фрагмента файла
	_, err := w.db.Exec("INSERT INTO file_fragments (file_id, fragment_index, fragment) VALUES (?, ?, ?)", w.fileID, w.fragmentIndex, w.buffer)
	if err != nil {
		return err
	}

	// Очистка буфера и увеличение индекса фрагмента
	w.buffer = make([]byte, 0, w.fragmentSize)
	w.fragmentIndex++

	return nil
}

func (w *SQLiteWriter) createFileRecord() error {
	// Определение MIME-типа файла
	ext := filepath.Ext(w.path)
	mimeType := mime.TypeByExtension(ext)
	if mimeType == "" {
		mimeType = "application/octet-stream" // Значение по умолчанию
	}

	_, err := w.db.Exec("INSERT INTO file_metadata (path, type) VALUES (?, ?)", w.path, mimeType)
	if err != nil {
		return err
	}

	// Получение и сохранение ID файла
	err = w.db.QueryRow("SELECT id FROM file_metadata WHERE path = ?", w.path).Scan(&w.fileID)
	return err
}

func (w *SQLiteWriter) Close() error {
	if len(w.buffer) > 0 {
		// Запись оставшегося буфера как последнего фрагмента
		return w.writeFragment()
	}
	return nil
}
