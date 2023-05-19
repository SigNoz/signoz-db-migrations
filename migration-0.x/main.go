package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/jmoiron/sqlx"
	"go.signoz.io/sqlite-migration-0.x/migrate"
)

var (
	db *sqlx.DB
)

// initDB initalize database
func initDB(dataSourceName string) error {
	var err error

	// open database connection
	db, err = sqlx.Connect("sqlite3", dataSourceName)
	return err
}

func main() {
	dataSource := flag.String("dataSource", "signoz.db", "Data Source path")
	flag.Parse()

	fmt.Println("Data Source path: ", *dataSource)

	if _, err := os.Stat(*dataSource); os.IsNotExist(err) {
		log.Fatalf("data source file does not exist: %s", *dataSource)
	}

	// inialize database
	err := initDB(*dataSource)
	if err != nil {
		log.Fatalln(err)
	}

	migrate.Run(db)
}
