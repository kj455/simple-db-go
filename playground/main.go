package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/kj455/simple-db/pkg/driver"
	"golang.org/x/exp/rand"
)

func main() {
	const driverName = "simple"
	dataSourceName := RandomString(30)
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		log.Fatalln("Failed to open database:", err)
	}
	defer db.Close()

	query := "create table T1(A int, B varchar(9))"
	if _, err := db.Exec(query); err != nil {
		log.Fatalln("Failed to create table:", err)
	}

	defer func() {
		_, err := db.Exec("delete from T1")
		if err != nil {
			log.Fatalln("Failed to delete table:", err)
		}
		log.Println("Successfully deleted table.")
	}()

	n := 200
	log.Print("Inserting", n, "random records.")
	for i := 0; i < n; i++ {
		a := i
		b := "rec" + fmt.Sprint(a)
		stmt := fmt.Sprintf("insert into T1(A,B) values(%d, '%s')", a, b)
		_, err := db.Exec(stmt)
		if err != nil {
			log.Fatalln("Failed to execute insert:", err)
		}
	}
	log.Println("Inserted", n, "records.")

	query = "select A, B from T1 where A = 100"
	rows, err := db.Query(query)
	if err != nil {
		log.Fatalln("Failed to execute query:", err)
	}
	defer rows.Close()

	fields, err := rows.Columns()
	if err != nil {
		log.Fatalln("Failed to get columns:", err)
	}
	log.Println("Columns:", fields)
	for rows.Next() {
		var a int
		var b string
		err = rows.Scan(&a, &b)
		if err != nil {
			log.Fatalln("Failed to scan row:", err)
		}
		log.Printf("Matched: A=%d, B=%s\n", a, b)
	}
	log.Println("Done.")
}

func RandomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
