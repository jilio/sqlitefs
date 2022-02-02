package sqlitefs

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"
)

type FS struct {
	db        *sql.DB
	tableName string
}

func NewFS(db *sql.DB, tableName string) *FS {
	return &FS{
		db:        db,
		tableName: tableName,
	}
}

func (sfs FS) Init() error {
	if sfs.tableName == "" {
		return errors.New("sqlitefs: table name cannot be empty")
	}

	_, err := sfs.db.Exec(`
		create table if not exists ` + sfs.tableName + `(
			id text primary key,
			name text unique not null,
			modified_at text default 0,
			content blob
		);`,
	)
	return err
}

func (sfs FS) Open(name string) (http.File, error) {
	file := &File{
		Name: strings.TrimLeft(name, "/"),
		fs:   &sfs,
	}
	return file, nil
}
