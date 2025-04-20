package sqlitefs

import (
	"errors"
	"mime"
	"path/filepath"
)

const fragmentSize = 16 * 1024 // 16 КБ

type SQLiteWriter struct {
	fs            *SQLiteFS
	path          string
	buffer        []byte
	fragmentSize  int
	fragmentIndex int
	fileCreated   bool
	closed        bool
}

// NewSQLiteWriter creates a new SQLiteWriter for the specified path.
// Deprecated: Use SQLiteFS.NewWriter instead.
func NewSQLiteWriter(fs *SQLiteFS, path string) *SQLiteWriter {
	return &SQLiteWriter{
		fs:           fs,
		path:         path,
		fragmentSize: fragmentSize,
		buffer:       make([]byte, 0, fragmentSize),
	}
}

func (w *SQLiteWriter) Write(p []byte) (n int, err error) {
	if w.closed {
		return 0, errors.New("sqlitefs: write to closed writer")
	}

	n = len(p)
	w.buffer = append(w.buffer, p...)

	for len(w.buffer) >= w.fragmentSize {
		err = w.writeFragment()
		if err != nil {
			return len(p) - len(w.buffer), err
		}
	}

	return n, nil
}

func (w *SQLiteWriter) writeFragment() error {
	if !w.fileCreated {
		err := w.createFileRecord()
		if err != nil {
			return err
		}
		w.fileCreated = true
	}

	writeSize := min(len(w.buffer), w.fragmentSize)

	respCh := make(chan error)
	w.fs.writeCh <- writeRequest{
		path:   w.path,
		data:   w.buffer[:writeSize],
		index:  w.fragmentIndex,
		respCh: respCh,
	}
	err := <-respCh

	if err == nil {
		w.buffer = w.buffer[writeSize:]
		w.fragmentIndex++
	}

	return err
}

func (w *SQLiteWriter) createFileRecord() error {
	ext := filepath.Ext(w.path)
	mimeType := mime.TypeByExtension(ext)
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	respCh := make(chan error)
	w.fs.writeCh <- writeRequest{
		path:     w.path,
		mimeType: mimeType,
		respCh:   respCh,
	}
	return <-respCh
}

func (w *SQLiteWriter) Close() error {
	if w.closed {
		return nil
	}

	for len(w.buffer) > 0 || !w.fileCreated {
		err := w.writeFragment()
		if err != nil {
			return err
		}
	}

	w.closed = true
	return nil
}
