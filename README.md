# sqlitefs

**sqlitefs** - пакет для подключения sqlite3 таблицы в качестве файловой системы.

<img src="sqlitefs.png" width="100"/>

## Использование

`go get github.com/jilio/sqlitefs`

```go
db, err := sql.Open("sqlite", "sample.db")
if err != nil {
    panic(err)
}
defer db.Close()

sfs, err := sqlitefs.NewSQLiteFS(db)
if err != nil {
    panic(err)
}

r := gin.Default()
r.GET("/logo.png", func(c *gin.Context) {
    c.FileFromFS("/images/sqlitefs.png", sfs)
})
r.Run() 
```
