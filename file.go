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
	size   int64 // общий размер файла
}

// NewSQLiteFile создает новый экземпляр SQLiteFile для заданного пути.
func NewSQLiteFile(db *sql.DB, path string) (*SQLiteFile, error) {
	file := &SQLiteFile{
		db:   db,
		path: path,
	}

	// Инициализация размера файла
	size, err := file.getTotalSize()
	if err != nil {
		return nil, err
	}
	file.size = size

	return file, nil
}

func (f *SQLiteFile) Read(p []byte) (int, error) {
	bytesReadTotal := 0
	for {
		// Вычисляем индекс текущего фрагмента и смещение внутри этого фрагмента
		fragmentIndex := f.offset / fragmentSize
		internalOffset := f.offset % fragmentSize

		// Определяем, сколько байтов нужно прочитать из текущего фрагмента
		readLength := min(fragmentSize-internalOffset, int64(len(p))-int64(bytesReadTotal))

		// Если запрос выходит за пределы файла, возвращаем EOF
		if f.offset >= f.size {
			if bytesReadTotal == 0 {
				return 0, io.EOF
			}
			return bytesReadTotal, nil
		}

		// SQL-запрос для чтения подстроки фрагмента
		query := `SELECT SUBSTR(fragment, ?, ?) FROM file_fragments WHERE file_id = (SELECT id FROM file_metadata WHERE path = ?) AND fragment_index = ?`
		row := f.db.QueryRow(query, internalOffset+1, readLength, f.path, fragmentIndex)

		var fragment []byte
		err := row.Scan(&fragment)
		if err != nil {
			if err == sql.ErrNoRows {
				// Если мы прочитали некоторые данные, возвращаем их количество
				if bytesReadTotal > 0 {
					return bytesReadTotal, nil
				}
				// Иначе возвращаем EOF
				return 0, io.EOF
			}
			return bytesReadTotal, err
		}

		// Копируем прочитанные данные в буфер p
		bytesRead := copy(p[bytesReadTotal:], fragment)
		bytesReadTotal += bytesRead
		f.offset += int64(bytesRead) // Обновляем смещение в файле

		// Если bytesRead равно 0 и это последний фрагмент, возвращаем то, что прочитано
		if bytesRead == 0 {
			if f.offset >= f.size {
				return bytesReadTotal, nil
			}
			continue // Продолжаем чтение следующего фрагмента
		}

		// Если мы прочитали все запрошенные данные, возвращаем результат
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
