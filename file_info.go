package sqlitefs

import (
	"io/fs"
	"time"
)

type FileInfo struct {
	name       string    `db:"name"`
	size       int64     `db:"size"`
	modifiedAt time.Time `db:"modified_at"`
}

func (info FileInfo) Name() string {
	return info.name
}

func (info FileInfo) Size() int64 {
	return info.size
}

func (info FileInfo) ModTime() time.Time {
	return info.modifiedAt
}

func (info FileInfo) IsDir() bool {
	return false
}

func (info FileInfo) Sys() interface{} {
	return nil
}

func (info FileInfo) Mode() fs.FileMode {
	return fs.ModePerm
}
