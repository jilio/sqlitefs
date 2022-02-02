# sqlitefs

**sqlitefs** - пакет для подключения sqlite3 таблицы в качестве файловой системы.

## Использование

Для корректной работы в базе данных должна быть таблица для хранения файлов. Если таблицы нет, то её можно создать при помощи `Init` (см. тесты).

Если таблица для файлов есть, то можно пользоваться (см. `examples/ginexample`):

```go
db, err := sql.Open("sqlite", "sample.db")
if err != nil {
    panic(err)
}

sfs := sqlitefs.NewFS(db, "files")

r := gin.Default()
r.GET("/aquila.png", func(c *gin.Context) {
    c.FileFromFS("images/aquila.png", sfs)
})
r.Run()
```