package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {

	// accept the datasource for the sqlite db using cmd flags else use default value of signoz.db
	dataSource := flag.String("dataSource", "signoz.db", "Data Source path")
	flag.Parse()

	fmt.Println("Data Source path: ", *dataSource)

	// if the datasource file doesn't exisit log the error
	if _, err := os.Stat(*dataSource); os.IsNotExist(err) {
		log.Fatalf("data source file does not exist: %s", *dataSource)
	}

	// inialize database
	err := initDB(*dataSource)
	if err != nil {
		log.Fatalln(err)
	}

	// migrate dashboards
	migrateDashboards()
}
