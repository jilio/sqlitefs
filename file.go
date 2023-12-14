package sqlitefs

import (
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SQLiteFile реализует интерфейс http.File.
type SQLiteFile struct {
	db     *sql.DB
	path   string
	offset int64 // текущее смещение для операций чтения
	// Дополнительные поля, если необходимо
}

// NewSQLiteFile создает новый экземпляр SQLiteFile для заданного пути.
func NewSQLiteFile(db *sql.DB, path string) *SQLiteFile {
	return &SQLiteFile{
		db:   db,
		path: path,
	}
}

func (f *SQLiteFile) Read(p []byte) (int, error) {
	// todo: неправильно происходит чтение
	// В случае, если нужно прочитать буфер, который покрывает несколько фрагментов,
	// то происходит чтение только первого фрагмента

	fragmentIndex := f.offset / fragmentSize
	internalOffset := f.offset % fragmentSize
	readLength := int64(len(p))
	if internalOffset+readLength > fragmentSize {
		readLength = fragmentSize - internalOffset
	}

	query := `SELECT SUBSTR(fragment, ?, ?) FROM file_fragments WHERE file_id = (SELECT id FROM file_metadata WHERE path = ?) AND fragment_index = ?`
	row := f.db.QueryRow(query, internalOffset+1, readLength, f.path, fragmentIndex)

	var fragment []byte
	err := row.Scan(&fragment)
	if err != nil {
		if err == sql.ErrNoRows {
			// Нет данных для чтения, возможно достигнут конец файла
			return 0, io.EOF
		}
		return 0, err
	}

	bytesRead := copy(p, fragment)
	f.offset += int64(bytesRead)

	// Возвращаем EOF, если не было прочитано никаких данных
	if bytesRead == 0 {
		return 0, io.EOF
	}

	return bytesRead, nil
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

func (f *SQLiteFile) Readdir(count int) ([]os.FileInfo, error) {
	// Проверяем, является ли путь директорией
	if !strings.HasSuffix(f.path, "/") {
		f.path += "/"
	}

	// Запрос на получение файлов в директории
	query := `SELECT path, type FROM file_metadata WHERE path LIKE ?`
	rows, err := f.db.Query(query, f.path+"%")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fileInfos []os.FileInfo
	var path, fileType string

	for rows.Next() {
		err := rows.Scan(&path, &fileType)
		if err != nil {
			return nil, err
		}

		// Создание FileInfo
		fileInfo, err := f.createFileInfo(path)
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

	return fileInfos, nil
}

func (f *SQLiteFile) Stat() (os.FileInfo, error) {
	return f.createFileInfo(f.path)
}

func (f *SQLiteFile) Close() error {
	return nil
}

func (f *SQLiteFile) createFileInfo(path string) (os.FileInfo, error) {
	// Определение, является ли путь директорией
	isDir := strings.HasSuffix(path, "/")

	var size int64

	if !isDir {
		query := `SELECT SUM(LENGTH(fragment)) FROM file_fragments WHERE file_id = (SELECT id FROM file_metadata WHERE path = ?)`
		err := f.db.QueryRow(query, path).Scan(&size)
		if err != nil {
			return nil, err
		}
	}

	name := filepath.Base(path)

	return &fileInfo{
		name:    name,
		size:    size,
		modTime: time.Time{}, // todo: получение времени модификации
		isDir:   isDir,
	}, nil
}

func (f *SQLiteFile) getTotalSize() (int64, error) {
	// Получение количества фрагментов и размера последнего фрагмента
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
		return 0, err
	}

	// Вычисление общего размера файла
	totalSize := int64((count-1)*fragmentSize + lastFragmentSize)
	return totalSize, nil
}
