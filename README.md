# SQLiteFS

[![GoDoc](https://godoc.org/github.com/jilio/sqlitefs?status.svg)](https://godoc.org/github.com/jilio/sqlitefs)
[![Test and Coverage](https://github.com/jilio/sqlitefs/actions/workflows/test.yml/badge.svg)](https://github.com/jilio/sqlitefs/actions/workflows/test.yml)
[![Go Coverage](https://github.com/jilio/sqlitefs/wiki/coverage.svg)](https://raw.githack.com/wiki/jilio/sqlitefs/coverage.html)
[![Go Report Card](https://goreportcard.com/badge/github.com/jilio/sqlitefs)](https://goreportcard.com/report/github.com/jilio/sqlitefs)

SQLiteFS is a Go package that implements the `fs.FS` interface using SQLite as a storage backend. This allows you to store and access files directly from a SQLite database, which can be useful for embedded applications, resource-constrained systems, or when you need a unified storage for both files and metadata.

## Features

- Implementation of the `fs.FS` interface
- File storage in SQLite database
- Support for concurrent writes through a shared channel
- Fragmented file storage for efficient handling of large files
- Automatic MIME type detection for files

## Installation

To use SQLiteFS in your Go project, run the following command:

```sh
go get github.com/jilio/sqlitefs
```

## Usage

Here's a simple example of how to use SQLiteFS:

```go
package main

import (
 "database/sql"
 "fmt"
 "io/fs"
 "log"

 "github.com/jilio/sqlitefs"
 _ "modernc.org/sqlite"
)

func main() {
 // Open a connection to the SQLite database
 db, err := sql.Open("sqlite", "files.db")
 if err != nil {
  log.Fatal(err)
 }
 defer db.Close()

 // Create a new instance of SQLiteFS
 sqliteFS, err := sqlitefs.NewSQLiteFS(db)
 if err != nil {
  log.Fatal(err)
 }
 defer sqliteFS.Close()

 // Write a file
 writer := sqliteFS.NewWriter("example.txt")
 _, err = writer.Write([]byte("Hello, SQLiteFS!"))
 if err != nil {
  log.Fatal(err)
 }
 err = writer.Close()
 if err != nil {
  log.Fatal(err)
 }

 // Read a file
 file, err := sqliteFS.Open("example.txt")
 if err != nil {
  log.Fatal(err)
 }
 defer file.Close()

 content, err := fs.ReadFile(sqliteFS, "example.txt")
 if err != nil {
  log.Fatal(err)
 }

 fmt.Printf("File content: %s\n", content)
}
```

## License

[MIT License](LICENSE)
