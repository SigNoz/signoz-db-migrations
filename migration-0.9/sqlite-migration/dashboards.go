package main

import (
	"encoding/json"
	"fmt"
	"log"
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

type QueryDataNew struct {
	Data         Data   `json:"data"`
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
	Description string    `json:"description"`
	Tags        []string  `json:"tags"`
	Name        string    `json:"name"`
	Layout      []Layout  `json:"layout"`
	Title       string    `json:"title"`
	Widgets     []Widgets `json:"widgets"`
}

type PromQuery struct {
	Query    string `json:"query"`
	Stats    string `json:"stats,omitempty"`
	Disabled bool   `json:"disabled"`
}

type ClickHouseQuery struct {
	Legend   string `json:"legend"`
	Name     string `json:"name"`
	Query    string `json:"rawQuery"`
	Disabled bool   `json:"disabled"`
}

type TagFilterItem struct {
	Key   string `json:"key"`
	OP    string `json:"op"`
	Value string `json:"value"`
}

type TagFilters struct {
	OP    string          `json:"op"`
	Items []TagFilterItem `json:"items"`
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

type MetricsBuilder struct {
	Formulas     []string       `json:"formulas"`
	QueryBuilder []QueryBuilder `json:"queryBuilder"`
}

type PromQueryNew struct {
	Query    string `json:"query"`
	Disabled bool   `json:"disabled"`
	Name     string `json:"name"`
	Legend   string `json:"legend"`
}

type QueryNew struct {
	ClickHouse     []ClickHouseQuery `json:"clickHouse"`
	PromQL         []PromQueryNew    `json:"promQL"`
	MetricsBuilder MetricsBuilder    `json:"metricsBuilder"`
	QueryType      int               `json:"queryType"`
}

type WidgetsNew struct {
	Description    string       `json:"description"`
	ID             string       `json:"id"`
	IsStacked      bool         `json:"isStacked"`
	NullZeroValues string       `json:"nullZeroValues"`
	Opacity        string       `json:"opacity"`
	PanelTypes     string       `json:"panelTypes"`
	Query          QueryNew     `json:"query"`
	QueryData      QueryDataNew `json:"queryData"`
	TimePreferance string       `json:"timePreferance"`
	Title          string       `json:"title"`
	YAxisUnit      string       `json:"yAxisUnit"`
	QueryType      int          `json:"queryType"`
}

type DashboardDataNew struct {
	Description string       `json:"description"`
	Tags        []string     `json:"tags"`
	Name        string       `json:"name"`
	Layout      []Layout     `json:"layout"`
	Title       string       `json:"title"`
	Widgets     []WidgetsNew `json:"widgets"`
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
	var ddNew DashboardDataNew

	err := json.Unmarshal([]byte(data), &dd)
	if err != nil {
		var tempDashboardData DashboardDataNew
		newDashErr := json.Unmarshal([]byte(data), &tempDashboardData)
		if newDashErr == nil {
			log.Println("New dashboard data found, skipping")
			return "", false
		} else {
			log.Fatalln(err)
		}
	}
	ddNew.Layout = dd.Layout
	ddNew.Title = dd.Title
	ddNew.Description = dd.Description
	ddNew.Tags = dd.Tags
	ddNew.Name = dd.Name

	ddNew.Widgets = make([]WidgetsNew, len(dd.Widgets))

	for i, widget := range dd.Widgets {
		ddNew.Widgets[i].Description = widget.Description
		ddNew.Widgets[i].ID = widget.ID
		ddNew.Widgets[i].IsStacked = widget.IsStacked
		ddNew.Widgets[i].NullZeroValues = widget.NullZeroValues
		ddNew.Widgets[i].Opacity = widget.Opacity
		ddNew.Widgets[i].PanelTypes = widget.PanelTypes
		ddNew.Widgets[i].Query = QueryNew{
			ClickHouse: []ClickHouseQuery{
				{Name: "A"},
			},
			MetricsBuilder: MetricsBuilder{
				Formulas: []string{}, QueryBuilder: []QueryBuilder{
					{
						AggregateOperator: 1,
						Name:              "A",
						TagFilters:        TagFilters{OP: "AND", Items: []TagFilterItem{}},
						ReduceTo:          1,
						GroupBy:           []string{},
					},
				},
			},
			PromQL:    []PromQueryNew{},
			QueryType: 3,
		}
		name := 65
		for j, q := range widget.Query {
			ddNew.Widgets[i].Query.PromQL = append(ddNew.Widgets[i].Query.PromQL, PromQueryNew{Query: q.Query, Legend: q.Legend, Name: fmt.Sprintf("%c", j+name)})
		}
		ddNew.Widgets[i].QueryData = QueryDataNew{Data: Data{
			QueryData: make([]interface{}, 0),
		}}
		ddNew.Widgets[i].QueryType = 3
		ddNew.Widgets[i].TimePreferance = widget.TimePreferance
		ddNew.Widgets[i].Title = widget.Title
		ddNew.Widgets[i].YAxisUnit = widget.YAxisUnit
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
