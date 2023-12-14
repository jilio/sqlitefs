package main

import (
	"database/sql"
	"io"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/jilio/sqlitefs"
	_ "modernc.org/sqlite"
)

func main() {
	// Удаление существующего файла базы данных, если он существует
	if _, err := os.Stat("sample.db"); err == nil {
		os.Remove("sample.db")
	}

	// Открытие подключения к базе данных SQLite
	db, err := sql.Open("sqlite", "sample.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Использование SQLiteFS
	sfs, err := sqlitefs.NewSQLiteFS(db)
	if err != nil {
		log.Fatal(err)
	}

	// Использование SQLiteWriter для записи файла
	writer := sqlitefs.NewSQLiteWriter(db, "images/sqlitefs.png")
	defer writer.Close()

	file, err := os.Open("sqlitefs.png")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	_, err = io.Copy(writer, file)
	if err != nil {
		log.Fatal(err)
	}

	// Чтение файла из sqlitefs
	originalFile, err := sfs.Open("images/sqlitefs.png")
	if err != nil {
		log.Fatalf("Ошибка при открытии файла из sqlitefs: %v", err)
	}
	defer originalFile.Close()

	// Создание нового файла в обычной файловой системе
	newFile, err := os.Create("new_sqlitefs.png") // Создаем файл в текущей директории
	if err != nil {
		log.Fatal(err)
	}
	defer newFile.Close()

	// Копирование содержимого файла
	bytesCopied, err := io.Copy(newFile, originalFile)
	if err != nil {
		log.Fatal(err)
	}

	// Проверка успешности операции
	log.Printf("Файл успешно скопирован. Байт скопировано: %d\n", bytesCopied)

	r := gin.Default()
	r.GET("/logo.png", func(c *gin.Context) {
		c.FileFromFS("images/sqlitefs.png", sfs)
	})
	r.Run() // listen and serve on 0.0.0.0:8080
}
