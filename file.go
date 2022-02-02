package sqlitefs

import (
	"errors"
	"io/fs"
	"strconv"
	"time"
)

type File struct {
	ID         string    `db:"id"`
	Name       string    `db:"name"`
	Content    []byte    `db:"content"`
	ModifiedAt time.Time `db:"modified_at"`

	fs     *FS
	offset int64
}

func (f File) Close() error {
	return nil
}

func (f *File) Read(buf []byte) (int, error) {
	var contentFragment string
	row := f.fs.db.QueryRow(`
		SELECT
			substr(content, ?, ?)
		FROM
			`+f.fs.tableName+`
		WHERE
			name = ?;`,
		f.offset+1,
		len(buf)+1,
		f.Name,
	)
	err := row.Scan(&contentFragment)
	if err != nil {
		return 0, err
	}

	f.offset += int64(len(buf))

	// todo: handle err
	return copy(buf, []byte(contentFragment)), nil
}

func (f File) Seek(offset int64, whence int) (int64, error) {
	if whence > 2 {
		return 0, errors.New("sqlitefs: invalid whence")
	}
	f.offset += offset
	return f.offset, nil
}

func (f File) Stat() (fs.FileInfo, error) {
	var info FileInfo

	row := f.fs.db.QueryRow(`
		select
			name,
			length(content) as size,
			modified_at
		from
			`+f.fs.tableName+`
		where
			name = ?;`,
		f.Name,
	)

	var modifiedAt string
	err := row.Scan(&info.name, &info.size, &modifiedAt)
	if err != nil {
		return info, err
	}

	ts, err := strconv.ParseInt(modifiedAt, 10, 64)
	info.modifiedAt = time.Unix(ts, 0)
	return info, err
}

func (f File) Readdir(count int) ([]fs.FileInfo, error) {
	return nil, errors.New("sqlitefs: readdir not implemented")
}
