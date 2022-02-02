package main

import (
	"database/sql"

	"github.com/gin-gonic/gin"
	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "sample.db")
	if err != nil {
		panic(err)
	}

	sfs := sqlitefs.NewFS(db, "files")

	r := gin.Default()
	r.GET("/aquila.png", func(c *gin.Context) {
		c.FileFromFS("images/aquila.png", sfs)
	})
	r.Run() // listen and serve on 0.0.0.0:8080
}
