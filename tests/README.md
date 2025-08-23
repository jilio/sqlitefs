# SQLiteFS Tests

This directory contains integration tests for SQLiteFS using the modernc.org/sqlite driver.

## Running Tests

From this directory:

```bash
go test -v
```

## Test Coverage

The tests cover:

- Basic file operations (create, read, write)
- Large file handling with multiple fragments
- File seeking and positioning
- File metadata and info
- Directory operations and listing
- Empty files
- Non-existent files
- Path variations

## Note

These tests use a real SQLite database (in-memory) to ensure the filesystem works correctly with actual SQL operations. The main module remains dependency-free while these integration tests validate the implementation.
