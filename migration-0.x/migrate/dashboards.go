package migrate

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"go.signoz.io/sqlite-migration-0.x/migrate/model"
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
	Data         Data   `json:"data"`
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
	Key   string      `json:"key"`
	OP    string      `json:"op"`
	Value interface{} `json:"value"`
}

type TagFilters struct {
	OP    string          `json:"op"`
	Items []TagFilterItem `json:"items"`
}

type QueryBuilder struct {
	AggregateOperator int        `json:"aggregateOperator"`
	Disabled          bool       `json:"disabled"`
	GroupBy           []string   `json:"groupBy"`
	Legend            string     `json:"legend"`
	MetricName        string     `json:"metricName"`
	Name              string     `json:"name"`
	TagFilters        TagFilters `json:"tagFilters"`
	ReduceTo          int        `json:"reduceTo"`
}

type Formula struct {
	Disabled   bool   `json:"disabled"`
	Expression string `json:"expression"`
	Legend     string `json:"legend"`
	Name       string `json:"name"`
	QueryName  string `json:"queryName"`
}

type MetricsBuilder struct {
	Formulas     []Formula      `json:"formulas"`
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
}

type VariableNew struct {
	CustomValue   string `json:"customValue"`
	Description   string `json:"description"`
	MultiSelect   bool   `json:"multiSelect"`
	Name          string `json:"name"`
	QueryValue    string `json:"queryValue"`
	ShowALLOption bool   `json:"showALLOption"`
	Sort          string `json:"sort"`
	TextboxValue  string `json:"textboxValue"`
	Type          string `json:"type"`
}

type DashboardDataNew struct {
	Description string                 `json:"description"`
	Tags        []string               `json:"tags"`
	Name        string                 `json:"name"`
	Layout      []Layout               `json:"layout"`
	Title       string                 `json:"title"`
	Widgets     []WidgetsNew           `json:"widgets"`
	Variables   map[string]VariableNew `json:"variables"`
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
	ID       string       `json:"id"`
	Key      AttributeKey `json:"key"`
	Value    interface{}  `json:"value"`
	Operator string       `json:"op"`
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

type AttributeKeyDataType string
type AttributeKeyType string

type AttributeKey struct {
	Key      string               `json:"key"`
	DataType AttributeKeyDataType `json:"dataType"`
	Type     AttributeKeyType     `json:"type"`
	IsColumn bool                 `json:"isColumn"`
}

type v3_BuilderQuery struct {
	QueryName          string         `json:"queryName"`
	DataSource         string         `json:"dataSource"`
	AggregateOperator  string         `json:"aggregateOperator"`
	AggregateAttribute AttributeKey   `json:"aggregateAttribute"`
	StepInterval       int            `json:"stepInterval"`
	Filters            *v3_FilterSet  `json:"filters"`
	GroupBy            []AttributeKey `json:"groupBy"`
	Expression         string         `json:"expression"`
	Disabled           bool           `json:"disabled"`
	Having             []v3_Having    `json:"having"`
	Legend             string         `json:"legend"`
	Limit              uint64         `json:"limit"`
	Offset             uint64         `json:"offset"`
	PageSize           uint64         `json:"pageSize"`
	OrderBy            []v3_OrderBy   `json:"orderBy"`
	ReduceTo           string         `json:"reduceTo"`
	SelectColumns      []AttributeKey `json:"selectColumns,omitempty"`
}

type v3_BuilderQueries struct {
	Formulas     []Formula         `json:"queryFormulas"`
	BuilderQuery []v3_BuilderQuery `json:"queryData"`
}

type v3_PromQueryNew struct {
	Query    string `json:"query"`
	Disabled bool   `json:"disabled"`
	Name     string `json:"name"`
	Legend   string `json:"legend"`
}

type v3_QueryNew struct {
	ClickHouse     []v3_ClickHouseQuery `json:"clickhouse_sql"`
	PromQL         []v3_PromQueryNew    `json:"promql"`
	BuilderQueries v3_BuilderQueries    `json:"builder"`
	QueryType      string               `json:"queryType"`
}

type v3_WidgetsNew struct {
	Description    string      `json:"description"`
	ID             string      `json:"id"`
	IsStacked      bool        `json:"isStacked"`
	NullZeroValues string      `json:"nullZeroValues"`
	Opacity        string      `json:"opacity"`
	PanelTypes     string      `json:"panelTypes"`
	Query          v3_QueryNew `json:"query"`
	QueryData      QueryData   `json:"queryData"`
	TimePreferance string      `json:"timePreferance"`
	Title          string      `json:"title"`
	YAxisUnit      string      `json:"yAxisUnit"`
}

type v3_DashboardDataNew struct {
	Description string                 `json:"description"`
	Tags        []string               `json:"tags"`
	Name        string                 `json:"name"`
	Layout      []Layout               `json:"layout"`
	Title       string                 `json:"title"`
	Widgets     []v3_WidgetsNew        `json:"widgets"`
	Variables   map[string]VariableNew `json:"variables"`
}

func stringQueryTypeFromInt(queryType int) string {
	switch queryType {
	case int(model.QUERY_BUILDER):
		return "builder"
	case int(model.CLICKHOUSE):
		return "clickhouse_sql"
	case int(model.PROM):
		return "promql"
	default:
		return "builder"
	}
}

func aggregateOperatorIntToString(aggType int) string {
	switch aggType {
	case int(model.NOOP):
		return "noop"
	case int(model.COUNT):
		return "count"
	case int(model.COUNT_DISTINCT):
		return "count_distinct"
	case int(model.SUM):
		return "sum"
	case int(model.AVG):
		return "avg"
	case int(model.MAX):
		return "max"
	case int(model.MIN):
		return "min"
	case int(model.P05):
		return "p05"
	case int(model.P10):
		return "p10"
	case int(model.P20):
		return "p20"
	case int(model.P25):
		return "p25"
	case int(model.P50):
		return "p50"
	case int(model.P75):
		return "p75"
	case int(model.P90):
		return "p90"
	case int(model.P95):
		return "p95"
	case int(model.P99):
		return "p99"
	case int(model.RATE):
		return "rate"
	case int(model.SUM_RATE):
		return "sum_rate"
	case int(model.RATE_SUM):
		return "rate_sum"
	case int(model.RATE_AVG):
		return "rate_avg"
	case int(model.RATE_MAX):
		return "rate_max"
	case int(model.RATE_MIN):
		return "rate_min"
	case int(model.HIST_QUANTILE_50):
		return "hist_quantile_50"
	case int(model.HIST_QUANTILE_75):
		return "hist_quantile_75"
	case int(model.HIST_QUANTILE_90):
		return "hist_quantile_90"
	case int(model.HIST_QUANTILE_95):
		return "hist_quantile_95"
	case int(model.HIST_QUANTILE_99):
		return "hist_quantile_99"
	default:
		return "noop"
	}
}

func stringAggrOperatorFromInt(aggrType int) string {
	switch aggrType {
	case 1:
		return "last"
	case 2:
		return "sum"
	case 3:
		return "avg"
	case 4:
		return "max"
	case 5:
		return "min"
	default:
		return "last"
	}
}

func newPanelType(old string) string {
	switch old {
	case "TIME_SERIES":
		return "graph"
	case "VALUE":
		return "value"
	}
	return old
}

func newPanelTypeFromInt(old int) string {
	switch old {
	case int(model.TIME_SERIES):
		return "graph"
	case int(model.QUERY_VALUE):
		return "value"
	}
	return "graph"
}

func migrateDashboardData(data string) (string, bool) {
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
	// copy variables
	ddNew.Variables = make(map[string]VariableNew)
	for _, variable := range dd.Variables {
		ddNew.Variables[variable.Name] = variable
	}

	// copy widgets

	ddNew.Widgets = make([]v3_WidgetsNew, len(dd.Widgets))

	for i, widget := range dd.Widgets {
		ddNew.Widgets[i].Description = widget.Description
		ddNew.Widgets[i].ID = widget.ID
		ddNew.Widgets[i].IsStacked = widget.IsStacked
		ddNew.Widgets[i].NullZeroValues = widget.NullZeroValues
		ddNew.Widgets[i].Opacity = widget.Opacity
		ddNew.Widgets[i].PanelTypes = newPanelType(widget.PanelTypes)
		ddNew.Widgets[i].Query = v3_QueryNew{
			ClickHouse: make([]v3_ClickHouseQuery, len(widget.Query.ClickHouse)),
			BuilderQueries: v3_BuilderQueries{
				Formulas: make([]Formula, len(widget.Query.MetricsBuilder.Formulas)), BuilderQuery: make([]v3_BuilderQuery, len(widget.Query.MetricsBuilder.QueryBuilder)),
			},
			PromQL:    make([]v3_PromQueryNew, len(widget.Query.PromQL)),
			QueryType: stringQueryTypeFromInt(widget.Query.QueryType),
		}
		ddNew.Widgets[i].QueryData = QueryData{
			Data:         Data{},
			Error:        false,
			ErrorMessage: "",
			Loading:      false,
		}
		ddNew.Widgets[i].QueryData.Data = Data{
			Legend:    "",
			Query:     "",
			QueryData: make([]interface{}, 0),
		}
		for j, q := range widget.Query.ClickHouse {
			ddNew.Widgets[i].Query.ClickHouse[j].Query = q.Query
			ddNew.Widgets[i].Query.ClickHouse[j].Name = q.Name
			ddNew.Widgets[i].Query.ClickHouse[j].Disabled = q.Disabled
			ddNew.Widgets[i].Query.ClickHouse[j].Legend = q.Legend
		}
		for j, q := range widget.Query.PromQL {
			ddNew.Widgets[i].Query.PromQL[j].Query = q.Query
			ddNew.Widgets[i].Query.PromQL[j].Name = q.Name
			ddNew.Widgets[i].Query.PromQL[j].Disabled = q.Disabled
			ddNew.Widgets[i].Query.PromQL[j].Legend = q.Legend
		}
		for j, q := range widget.Query.MetricsBuilder.QueryBuilder {
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].QueryName = q.Name
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].Expression = q.Name
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].AggregateOperator = aggregateOperatorIntToString(q.AggregateOperator)
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].DataSource = "metrics"
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].StepInterval = 60
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].AggregateAttribute = AttributeKey{Key: q.MetricName, DataType: "float64", Type: "", IsColumn: true}
			fs := &v3_FilterSet{Items: make([]v3_FilterItem, 0), Operator: "AND"}
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].Filters = fs
			for _, item := range q.TagFilters.Items {
				fs.Items = append(fs.Items, v3_FilterItem{
					ID:       uuid.NewString(),
					Key:      AttributeKey{Key: item.Key, Type: "tag", DataType: "string", IsColumn: false},
					Operator: strings.ToLower(item.OP),
					Value:    item.Value,
				})
			}
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].GroupBy = make([]AttributeKey, 0)
			for _, item := range q.GroupBy {
				ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].GroupBy = append(ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].GroupBy, AttributeKey{
					Key:      item,
					DataType: "string",
					Type:     "tag",
					IsColumn: false,
				})
			}
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].Having = make([]v3_Having, 0)
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].OrderBy = make([]v3_OrderBy, 0)
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].SelectColumns = make([]AttributeKey, 0)
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].ReduceTo = stringAggrOperatorFromInt(q.ReduceTo)
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].Disabled = q.Disabled
			ddNew.Widgets[i].Query.BuilderQueries.BuilderQuery[j].Legend = q.Legend
			ddNew.Widgets[i].TimePreferance = widget.TimePreferance
			ddNew.Widgets[i].Title = widget.Title
			ddNew.Widgets[i].YAxisUnit = widget.YAxisUnit
		}

		// formulas
		for j, f := range widget.Query.MetricsBuilder.Formulas {
			ddNew.Widgets[i].Query.BuilderQueries.Formulas[j].Disabled = f.Disabled
			ddNew.Widgets[i].Query.BuilderQueries.Formulas[j].Expression = f.Expression
			ddNew.Widgets[i].Query.BuilderQueries.Formulas[j].Legend = f.Legend
			ddNew.Widgets[i].Query.BuilderQueries.Formulas[j].Name = f.Name
			ddNew.Widgets[i].Query.BuilderQueries.Formulas[j].QueryName = f.Name
		}
	}
	newData, err := json.Marshal(ddNew)
	if err != nil {
		log.Fatalln(err)
	}
	return string(newData), true
}

func updateData(id int, data string, db *sqlx.DB) {
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

func migrateDashboards(db *sqlx.DB) {
	var dashboards []Dashboard

	sql := `
		SELECT * FROM dashboards
	`

	err := db.Select(&dashboards, sql)
	if err != nil {
		log.Fatalln("Error in processing sql query: ", err)
	}

	log.Printf("Total Dashboard found: %d\n", len(dashboards))
	for _, dashboard := range dashboards {
		log.Printf("%s\n", dashboard.Uuid)
	}

	for _, dashboard := range dashboards {
		data, changed := migrateDashboardData(dashboard.Data)
		if !changed {
			continue
		}
		dashboard.Data = data
		updateData(dashboard.Id, dashboard.Data, db)

		log.Printf("Dashboard %s updated\n", dashboard.Uuid)
	}

	log.Println("Dashboards migrated")
}
