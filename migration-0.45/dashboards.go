package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var (
	db *sqlx.DB
)

type Dashboard struct {
	Id        int       `json:"id" db:"id"`
	Uuid      string    `json:"uuid" db:"uuid"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	CreateBy  *string   `json:"created_by" db:"created_by"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
	UpdateBy  *string   `json:"updated_by" db:"updated_by"`
	Data      string    `json:"data" db:"data"`
	Locked    *int      `json:"isLocked" db:"locked"`
}
type DashboardData struct {
	Description string              		`json:"description"`
	Tags        []string            		`json:"tags"`
	Name        string    					`json:"name"`
	Layout      []Layout            		`json:"layout"`
	Title       string              		`json:"title"`
	Widgets     []map[string]interface{}	`json:"widgets"`
	Variables   map[string]interface{}  	`json:"variables"`
	Version     string 				    	`json:"version"`
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


// initDB initalize database
func initDB(dataSourceName string) error {
	var err error

	// open database connection
	db, err = sqlx.Connect("sqlite3", dataSourceName)
	return err
}

func migrateDData(data string) string {
	var dd *DashboardData
	var ddNew DashboardData

	err := json.Unmarshal([]byte(data), &dd)
	if err != nil {
		log.Fatalln(err)
	}
	ddNew.Title = dd.Title
	ddNew.Description = dd.Description
	ddNew.Tags = dd.Tags
	ddNew.Name = dd.Name
	ddNew.Widgets = dd.Widgets
	ddNew.Variables = dd.Variables
	ddNew.Version = dd.Version
	ddNew.Layout = make([]Layout, len(dd.Layout))

	for i := range dd.Layout {
		ddNew.Layout[i] = dd.Layout[i]
		ddNew.Layout[i].H = 2 * ddNew.Layout[i].H
	}

	newData, err := json.Marshal(ddNew)
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

func migrateDashboards(){

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
		data := migrateDData(dashboard.Data)
		
		dashboard.Data = data
		updateData(dashboard.Id, dashboard.Data)

		log.Printf("Dashboard %s updated\n", dashboard.Uuid)
	}

	log.Println("Dashboards migrated")

}

