# Claude Code Instructions

## Before Creating Pull Requests

Before creating any pull request, you MUST:

1. Run `go fmt ./...` to format all Go code in the main module
2. Run tests with `cd tests && go test ./...` to ensure all tests pass
3. Check test coverage with `cd tests && go test -coverprofile=coverage.out -coverpkg=github.com/jilio/sqlitefs ./... && go tool cover -func=coverage.out`

## Code Quality Standards

- Maintain 100% test coverage for core functionality
- Follow Go idioms and best practices
- Keep implementations simple and focused
