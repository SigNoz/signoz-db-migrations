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
	Slug      string    `json:"-" db:"-"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`
	CreateBy  *string   `json:"created_by" db:"created_by"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
	UpdateBy  *string   `json:"updated_by" db:"updated_by"`
	Title     string    `json:"-" db:"-"`
	Data      string      `json:"data" db:"data"`
	Locked    *int      `json:"isLocked" db:"locked"`
}

type Data map[string]interface{}

type Widget struct {
	Description    string             `json:"description"`
	ID             string             `json:"id"`
	IsStacked      bool               `json:"isStacked"`
	NullZeroValues string             `json:"nullZeroValues"`
	Opacity        string             `json:"opacity"`
	PanelTypes     string             `json:"panelTypes"`
	Query          Query              `json:"query"`
	QueryData      QueryDataDashboard `json:"queryData"`
	TimePreferance string             `json:"timePreferance"`
	Title          string             `json:"title"`
	YAxisUnit      string             `json:"yAxisUnit"`
	QueryType      int                `json:"queryType"`
}

type QueryDataDashboard struct {
	Data         Data   `json:"data"`
	Error        bool   `json:"error"`
	ErrorMessage string `json:"errorMessage"`
	Loading      bool   `json:"loading"`
}

type Query struct {
	ClickHouse     []ClickHouseQueryDashboard `json:"clickHouse"`
	PromQL         []PromQueryDashboard       `json:"promQL"`
	MetricsBuilder MetricsBuilder             `json:"metricsBuilder"`
	QueryType      int                        `json:"queryType"`
}

type MetricsBuilder struct {
	Formulas     []string       `json:"formulas"`
	QueryBuilder []QueryBuilder `json:"queryBuilder"`
}

type QueryBuilder struct {
	AggregateOperator interface{} `json:"aggregateOperator"`
	Disabled          bool        `json:"disabled"`
	GroupBy           []string    `json:"groupBy"`
	Legend            string      `json:"legend"`
	MetricName        string      `json:"metricName"`
	Name              string      `json:"name"`
	TagFilters        TagFilters  `json:"tagFilters"`
	ReduceTo          interface{} `json:"reduceTo"`
}

type TagFilters struct {
	StringTagKeys []string `json:"stringTagKeys" ch:"stringTagKeys"`
	NumberTagKeys []string `json:"numberTagKeys" ch:"numberTagKeys"`
	BoolTagKeys   []string `json:"boolTagKeys" ch:"boolTagKeys"`
}

type PromQueryDashboard struct {
	Query    string `json:"query"`
	Disabled bool   `json:"disabled"`
	Name     string `json:"name"`
	Legend   string `json:"legend"`
}

type ClickHouseQueryDashboard struct {
	Legend   string `json:"legend"`
	Name     string `json:"name"`
	Query    string `json:"rawQuery"`
	Disabled bool   `json:"disabled"`
}


type DashboardData struct {
	Description string              `json:"description"`
	Tags        []string            `json:"tags"`
	Name        string    			`json:"name"`
	Layout      []Layout            `json:"layout"`
	Title       string              `json:"title"`
	Widgets     []Widget            `json:"widgets"`
	Variables   map[string]Variable `json:"variables"`
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

type Variable struct {
	AllSelected      bool   `json:"allSelected"`
	CustomValue      string `json:"customValue"`
	Description      string `json:"description"`
	ModificationUUID string `json:"modificationUUID"`
	MultiSelect      bool   `json:"multiSelect"`
	QueryValue       string `json:"queryValue"`
	SelectedValue    string `json:"selectedValue"`
	ShowALLOption    bool   `json:"showALLOption"`
	Sort             string `json:"sort"`
	TextboxValue     string `json:"textboxValue"`
	Type             string `json:"type"`
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
		log.Fatalln(err)
	}
	ddNew.Title = dd.Title
	ddNew.Description = dd.Description
	ddNew.Tags = dd.Tags
	ddNew.Name = dd.Name
	ddNew.Widgets = dd.Widgets
	ddNew.Variables = dd.Variables
	ddNew.Layout = make([]Layout, len(dd.Layout))

	for i := range dd.Layout {
		ddNew.Layout[i] = dd.Layout[i]
		ddNew.Layout[i].H = 2 * ddNew.Layout[i].H
	}

	newData, err := json.Marshal(ddNew)
	if err != nil {
		log.Fatalln(err)
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

