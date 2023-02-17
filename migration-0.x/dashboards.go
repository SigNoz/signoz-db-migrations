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

type v3_Dashboard struct {
	Id        int       `db:"id"`
	Uuid      string    `db:"uuid"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
	Data      string    `db:"data"`
}

type v3_Layout struct {
	H      int    `json:"h"`
	I      string `json:"i"`
	Moved  bool   `json:"moved"`
	Static bool   `json:"static"`
	W      int    `json:"w"`
	X      int    `json:"x"`
	Y      int    `json:"y"`
}

type v3_ClickHouseQuery struct {
	Legend   string `json:"legend"`
	Name     string `json:"name"`
	Query    string `json:"rawQuery"`
	Disabled bool   `json:"disabled"`
}

type v3_FilterSet struct {
	Operator string          `json:"op,omitempty"`
	Items    []v3_FilterItem `json:"items"`
}

type v3_FilterItem struct {
	Key      string      `json:"key"`
	Value    interface{} `json:"value"`
	Operator string      `json:"op"`
}

type v3_OrderBy struct {
	ColumnName string `json:"columnName"`
	Order      string `json:"order"`
}

type v3_Having struct {
	ColumnName string      `json:"columnName"`
	Operator   string      `json:"operator"`
	Value      interface{} `json:"value"`
}

type v3_BuilderQuery struct {
	QueryName          string        `json:"queryName"`
	DataSource         string        `json:"dataSource"`
	AggregateOperator  string        `json:"aggregateOperator"`
	AggregateAttribute string        `json:"aggregateAttribute,omitempty"`
	Filters            *v3_FilterSet `json:"filters,omitempty"`
	GroupBy            []string      `json:"groupBy,omitempty"`
	Expression         string        `json:"expression"`
	Disabled           bool          `json:"disabled"`
	Having             []v3_Having   `json:"having,omitempty"`
	Limit              uint64        `json:"limit"`
	Offset             uint64        `json:"offset"`
	PageSize           uint64        `json:"pageSize"`
	OrderBy            []v3_OrderBy  `json:"orderBy,omitempty"`
	ReduceTo           string        `json:"reduceTo,omitempty"`
	SelectColumns      []string      `json:"selectColumns,omitempty"`
}

type v3_BuilderQueries struct {
	Formulas     []string          `json:"formulas"`
	BuilderQuery []v3_BuilderQuery `json:"queryBuilder"`
}

type v3_PromQueryNew struct {
	Query    string `json:"query"`
	Disabled bool   `json:"disabled"`
	Name     string `json:"name"`
	Legend   string `json:"legend"`
}

type v3_QueryNew struct {
	ClickHouse     []v3_ClickHouseQuery `json:"chQueries"`
	PromQL         []v3_PromQueryNew    `json:"promQueries"`
	BuilderQueries v3_BuilderQueries    `json:"builderQueries"`
	QueryType      string               `json:"queryType"`
	PanelType      string               `json:"panelType"`
}

type v3_WidgetsNew struct {
	Description    string      `json:"description"`
	ID             string      `json:"id"`
	IsStacked      bool        `json:"isStacked"`
	NullZeroValues string      `json:"nullZeroValues"`
	Opacity        string      `json:"opacity"`
	PanelTypes     string      `json:"panelTypes"`
	Query          v3_QueryNew `json:"query"`
	TimePreferance string      `json:"timePreferance"`
	Title          string      `json:"title"`
	YAxisUnit      string      `json:"yAxisUnit"`
	QueryType      int         `json:"queryType"`
}

type v3_DashboardDataNew struct {
	Description string          `json:"description"`
	Tags        []string        `json:"tags"`
	Name        string          `json:"name"`
	Layout      []Layout        `json:"layout"`
	Title       string          `json:"title"`
	Widgets     []v3_WidgetsNew `json:"widgets"`
}

// initDB initalize database
func initDB(dataSourceName string) error {
	var err error

	// open database connection
	db, err = sqlx.Connect("sqlite3", dataSourceName)
	return err
}

func stringQueryTypeFromInt(queryType int) string {
	switch queryType {
	case 1:
		return "builder"
	case 2:
		return "promql"
	case 3:
		return "clickhouse"
	default:
		return "builder"
	}
}

func stringAggrOperatorFromInt(aggrType int) string {
	switch aggrType {
	case 1:
		return "sum"
	case 2:
		return "avg"
	case 3:
		return "min"
	case 4:
		return "max"
	case 5:
		return "count"
	default:
		return "sum"
	}
}

func migrateDData(data string) (string, bool) {
	var dd *DashboardDataNew
	var ddNew v3_DashboardDataNew

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

	ddNew.Widgets = make([]v3_WidgetsNew, len(dd.Widgets))

	for i, widget := range dd.Widgets {
		ddNew.Widgets[i].Description = widget.Description
		ddNew.Widgets[i].ID = widget.ID
		ddNew.Widgets[i].IsStacked = widget.IsStacked
		ddNew.Widgets[i].NullZeroValues = widget.NullZeroValues
		ddNew.Widgets[i].Opacity = widget.Opacity
		ddNew.Widgets[i].PanelTypes = widget.PanelTypes
		ddNew.Widgets[i].Query = v3_QueryNew{
			ClickHouse: []v3_ClickHouseQuery{
				{Name: "A"},
			},
			BuilderQueries: v3_BuilderQueries{
				Formulas: []string{}, BuilderQuery: []v3_BuilderQuery{},
			},
			PromQL:    []v3_PromQueryNew{},
			QueryType: stringQueryTypeFromInt(widget.QueryType),
		}
		name := 65
		for j, q := range widget.Query.ClickHouse {
			ddNew.Widgets[i].Query.ClickHouse[j].Query = q.Query
			ddNew.Widgets[i].Query.ClickHouse[j].Name = string(name)
			ddNew.Widgets[i].Query.ClickHouse[j].Disabled = q.Disabled
			name++
		}
		for j, q := range widget.Query.PromQL {
			ddNew.Widgets[i].Query.PromQL[j].Query = q.Query
			ddNew.Widgets[i].Query.PromQL[j].Name = string(name)
			ddNew.Widgets[i].Query.PromQL[j].Disabled = q.Disabled
			ddNew.Widgets[i].Query.PromQL[j].Legend = q.Legend
			name++
		}
		for j, q := range widget.Query.MetricsBuilder.QueryBuilder {
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].QueryName = q.Name
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].AggregateOperator = stringAggrOperatorFromInt(q.AggregateOperator.(int))
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].DataSource = "metrics"
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].AggregateAttribute = q.MetricName
			fs := &v3_FilterSet{}
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].Filters = fs
			for _, item := range q.TagFilters.Items {
				fs.Items = append(fs.Items, v3_FilterItem{
					Key:      item.Key,
					Operator: item.OP,
					Value:    item.Value,
				})
			}
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].GroupBy = q.GroupBy
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].Expression = q.Name
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].ReduceTo = q.ReduceTo
			name++
		}
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
