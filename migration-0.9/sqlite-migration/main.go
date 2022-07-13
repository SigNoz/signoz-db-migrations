package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	dataSource := flag.String("dataSource", "signoz.db", "Data Source path")
	flag.Parse()
	fmt.Println(*dataSource)

	fmt.Println("Data Source path: ", *dataSource)

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
