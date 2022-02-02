package sqlitefs

import (
	"database/sql"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestSqliteFS(t *testing.T) {
	dir, err := ioutil.TempDir("", "test-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	fn := filepath.Join(dir, "sqlitefs_test.db")

	db, err := sql.Open("sqlite", fn)
	if err != nil {
		panic(err)
	}
	sfs := NewFS(db, "files")

	t.Run("Init ok", func(t *testing.T) {
		if err := sfs.Init(); err != nil {
			t.Logf(err.Error())
			t.Fail()
		}

		_, err = sfs.db.Exec(`
			insert into files(
				id,
				name,
				modified_at,
				content
			) values (
				"test_id",
				"test.txt",
				"1643836492",
				"jilio stories"
			);`,
		)
		if err != nil {
			t.Logf(err.Error())
			t.Fail()
		}
	})

	t.Run("File read ok", func(t *testing.T) {
		f := File{Name: "test.txt", fs: sfs}

		buf := make([]byte, 32)
		n, err := f.Read(buf)
		if err != nil {
			t.Logf(err.Error())
			t.Fail()
		}
		if n != len("jilio stories") {
			t.Fail()
		}
	})

	t.Run("File info ok", func(t *testing.T) {
		f := File{Name: "test.txt", fs: sfs}
		info, err := f.Stat()
		if err != nil {
			t.Logf(err.Error())
			t.Fail()
		}

		if info.Name() != "test.txt" {
			t.Fail()
		}
		if info.ModTime() != time.Unix(1643836492, 0) {
			t.Fail()
		}
		if info.Size() != int64(len("jilio stories")) {
			t.Fail()
		}
	})
}
