package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var (
	db *sqlx.DB
)

type Dashboard struct {
	Id        int       `db:"id"`
	Uuid      string    `db:"uuid"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
	Data      string    `db:"data"`
}

type Layout struct {
	H      int    `json:"h"`
	I      string `json:"i"`
	Moved  bool   `json:"moved"`
	Static bool   `json:"static"`
	W      int    `json:"w"`
	X      int    `json:"x"`
	Y      int    `json:"y"`
}

type Query struct {
	Legend string `json:"legend"`
	Query  string `json:"query"`
}

type Data struct {
	Legend    string        `json:"legend"`
	Query     string        `json:"query"`
	QueryData []interface{} `json:"queryData"`
}

type QueryData struct {
	Data         []Data `json:"data"`
	Error        bool   `json:"error"`
	ErrorMessage string `json:"errorMessage"`
	Loading      bool   `json:"loading"`
}

type Widgets struct {
	Description    string    `json:"description"`
	ID             string    `json:"id"`
	IsStacked      bool      `json:"isStacked"`
	NullZeroValues string    `json:"nullZeroValues"`
	Opacity        string    `json:"opacity"`
	PanelTypes     string    `json:"panelTypes"`
	Query          []Query   `json:"query"`
	QueryData      QueryData `json:"queryData"`
	TimePreferance string    `json:"timePreferance"`
	Title          string    `json:"title"`
	YAxisUnit      string    `json:"yAxisUnit"`
}

type DashboardData struct {
	Layout  []Layout  `json:"layout"`
	Title   string    `json:"title"`
	Widgets []Widgets `json:"widgets"`
}

// initDB initalize database
func initDB(dataSourceName string) error {
	var err error

	// open database connection
	db, err = sqlx.Connect("sqlite3", dataSourceName)
	if err != nil {
		return err
	}

	// createTable()

	return nil
}

func main() {
	dataSource := flag.String("dataSource", "signoz.db", "Data Source path")
	flag.Parse()
	fmt.Println("Data Source path:", *dataSource)

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

func alterData(data string) string {
	var dd *DashboardData

	err := json.Unmarshal([]byte(data), &dd)
	if err != nil {
		log.Fatalln(err)
	}

	for i, widget := range dd.Widgets {
		for j := i; j < len(dd.Layout); j++ {
			if dd.Layout[j].I == widget.ID {
				break
			} else if strings.Contains(dd.Layout[j].I, widget.ID) ||
				dd.Layout[j].I == "" ||
				dd.Layout[j].I == "empty" ||
				dd.Layout[j].I == fmt.Sprint(j+1) {
				dd.Layout[j].I = widget.ID
				break
			}
		}
	}

	newData, err := json.Marshal(dd)
	if err != nil {
		log.Fatalln(err)
	}

	return string(newData)
}

func updateData(id int, data string) {
	sql := `
		UPDATE dashboards
		SET data = :data
		WHERE id = :id
	`

	_, err := db.Exec(sql, data, id)
	if err != nil {
		log.Fatalln(err)
	}
}

func migrateDashboards() {
	var dashboards []Dashboard

	sql := `
		SELECT id, uuid, data FROM dashboards
	`

	err := db.Select(&dashboards, sql)
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("Total Dashboard found: %d\n", len(dashboards))
	for _, dashboard := range dashboards {
		log.Printf("%s\n", dashboard.Uuid)
	}

	for _, dashboard := range dashboards {
		dashboard.Data = alterData(dashboard.Data)
		updateData(dashboard.Id, dashboard.Data)

		log.Printf("Dashboard %s updated\n", dashboard.Uuid)
	}

	log.Println("Dashboards migrated")
}

// func createTable() {
// 	sql := `
// 		CREATE TABLE IF NOT EXISTS dashboards (
// 			id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
// 			uuid TEXT NOT NULL,
// 			created_at DATETIME NOT NULL,
// 			updated_at DATETIME NOT NULL,
// 			data TEXT NOT NULL
// 		)
// 	`

// 	_, err := db.Exec(sql)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// }

// func insertData(d *Dashboard) {
// 	dashboard := Dashboard{
// 		Uuid:      d.Uuid,
// 		Data:      d.Data,
// 		CreatedAt: d.CreatedAt,
// 		UpdatedAt: d.UpdatedAt,
// 	}

// 	// Uuid:      "f0d4e7e1-f0e4-4d4a-9d67-f92df94bf34f",
// 	// CreatedAt: time.Now(),
// 	// UpdatedAt: time.Now(),
// 	// Data:      "Dashboard Data",

// 	sql := `
// 		INSERT INTO dashboards (uuid, created_at, updated_at, data)
// 		VALUES (:uuid, :created_at, :updated_at, :data)
// 	`

// 	_, err := db.NamedExec(sql, dashboard)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}

// 	log.Println("Data inserted")
// }

// func queryData() {
// 	var dashboards []Dashboard

// 	sql := `
// 		SELECT id, uuid FROM dashboards
// 	`

// 	err := db.Select(&dashboards, sql)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}

// 	for _, dashboard := range dashboards {
// 		log.Printf("%+v\n", dashboard)
// 	}
// }

// func deleteData(id int) {
// 	sql := `
// 		DELETE FROM dashboards
// 		WHERE id = :id
// 	`

// 	_, err := db.Exec(sql, id)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// }
