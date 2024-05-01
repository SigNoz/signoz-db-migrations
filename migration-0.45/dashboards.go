package main

import (
	"encoding/json"
	"log"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var (
	db *sqlx.DB
)

type Dashboard struct {
	Id   int    `json:"id" db:"id"`
	Uuid string `json:"uuid" db:"uuid"`
	Data string `json:"data" db:"data"`
}
type DashboardData struct {
	Description             string                   `json:"description"`
	Tags                    []string                 `json:"tags"`
	Name                    string                   `json:"name"`
	Layout                  []Layout                 `json:"layout"`
	Title                   string                   `json:"title"`
	Widgets                 []map[string]interface{} `json:"widgets"`
	Variables               map[string]interface{}   `json:"variables"`
	Version                 interface{}              `json:"version"`
	UploadedGrafana         bool                     `json:"uploadedGrafana"`
	Uuid                    string                   `json:"uuid"`
	CollapsableRowsMigrated bool                     `json:"collapsableRowsMigrated"`
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

func migrateDData(data string) (string, bool) {
	var dd *DashboardData
	var ddNew DashboardData

	err := json.Unmarshal([]byte(data), &dd)
	if err != nil {
		log.Printf("Cannot unmarshal the dashboard %s,%v", dd.Uuid, err)
		return "", false
	}

	if dd.CollapsableRowsMigrated {
		log.Printf("Dashboard %s skipped migration as already updated\n", dd.Uuid)
		return "", false
	}

	ddNew.Title = dd.Title
	ddNew.Description = dd.Description
	ddNew.Tags = dd.Tags
	ddNew.Name = dd.Name
	ddNew.Widgets = dd.Widgets
	ddNew.Variables = dd.Variables
	ddNew.Version = dd.Version
	ddNew.UploadedGrafana = dd.UploadedGrafana
	ddNew.Uuid = dd.Uuid
	ddNew.Layout = make([]Layout, len(dd.Layout))

	// added this new boolean to not affect already migrated dashboards
	ddNew.CollapsableRowsMigrated = true

	for i := range dd.Layout {
		ddNew.Layout[i] = dd.Layout[i]
		ddNew.Layout[i].H = 2 * ddNew.Layout[i].H
	}

	newData, err := json.Marshal(ddNew)
	if err != nil {
		log.Printf("Cannot marshal the dashboard %s,%v", dd.Uuid, err)
		return "", false
	}

	return string(newData), true
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
		data, changed := migrateDData(dashboard.Data)

		if !changed {
			continue
		}

		dashboard.Data = data
		updateData(dashboard.Id, dashboard.Data)

		log.Printf("Dashboard %s updated\n", dashboard.Uuid)
	}

	log.Println("Dashboards migrated")

}
