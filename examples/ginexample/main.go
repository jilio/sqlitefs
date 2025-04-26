package main

import (
	"database/sql"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

func main() {
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
		c.FileFromFS("/images/sqlitefs.png", http.FS(sfs))
	})
	r.Run() // listen and serve on 0.0.0.0:8080
}
