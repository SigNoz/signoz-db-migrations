package migrate

import (
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"go.signoz.io/sqlite-migration-0.x/migrate/model"
	v3 "go.signoz.io/sqlite-migration-0.x/migrate/v3"
	"go.uber.org/zap"
)

type RuleType string

type StoredRule struct {
	Id        int       `json:"id" db:"id"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
	Data      string    `json:"data" db:"data"`
}

type StoredRuleNew struct {
	Id        int       `json:"id" db:"id"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
	Data      string    `json:"data" db:"data"`
}

// GettableRule has info for an alerting rules.
type GettableRule struct {
	// Id    string `json:"id"`
	// State string `json:"state"`
	PostableRule
}

type GettableRuleNew struct {
	// Id    string `json:"id"`
	// State string `json:"state"`
	PostableRuleNew
}

type PostableRule struct {
	Alert       string   `yaml:"alert,omitempty" json:"alert,omitempty"`
	AlertType   string   `yaml:"alertType,omitempty" json:"alertType,omitempty"`
	Description string   `yaml:"description,omitempty" json:"description,omitempty"`
	RuleType    RuleType `yaml:"ruleType,omitempty" json:"ruleType,omitempty"`
	EvalWindow  Duration `yaml:"evalWindow,omitempty" json:"evalWindow,omitempty"`
	Frequency   Duration `yaml:"frequency,omitempty" json:"frequency,omitempty"`

	RuleCondition *RuleCondition    `yaml:"condition,omitempty" json:"condition,omitempty"`
	Labels        map[string]string `yaml:"labels,omitempty" json:"labels,omitempty"`
	Annotations   map[string]string `yaml:"annotations,omitempty" json:"annotations,omitempty"`

	Disabled bool `json:"disabled"`

	// Source captures the source url where rule has been created
	Source string `json:"source,omitempty"`

	PreferredChannels []string `json:"preferredChannels,omitempty"`

	// legacy
	Expr    string `yaml:"expr,omitempty" json:"expr,omitempty"`
	OldYaml string `json:"yaml,omitempty"`
}

type PostableRuleNew struct {
	Alert       string   `yaml:"alert,omitempty" json:"alert,omitempty"`
	AlertType   string   `yaml:"alertType,omitempty" json:"alertType,omitempty"`
	Description string   `yaml:"description,omitempty" json:"description,omitempty"`
	RuleType    RuleType `yaml:"ruleType,omitempty" json:"ruleType,omitempty"`
	EvalWindow  Duration `yaml:"evalWindow,omitempty" json:"evalWindow,omitempty"`
	Frequency   Duration `yaml:"frequency,omitempty" json:"frequency,omitempty"`

	RuleCondition *RuleConditionNew `yaml:"condition,omitempty" json:"condition,omitempty"`
	Labels        map[string]string `yaml:"labels,omitempty" json:"labels,omitempty"`
	Annotations   map[string]string `yaml:"annotations,omitempty" json:"annotations,omitempty"`

	Disabled bool `json:"disabled"`

	// Source captures the source url where rule has been created
	Source string `json:"source,omitempty"`

	PreferredChannels []string `json:"preferredChannels,omitempty"`

	// legacy
	Expr    string `yaml:"expr,omitempty" json:"expr,omitempty"`
	OldYaml string `json:"yaml,omitempty"`
}

type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*d = Duration(time.Duration(value))
		return nil
	case string:
		tmp, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		*d = Duration(tmp)

		return nil
	default:
		return errors.New("invalid duration")
	}
}

type MatchType string

const (
	MatchTypeNone MatchType = "0"
	AtleastOnce   MatchType = "1"
	AllTheTimes   MatchType = "2"
	OnAverage     MatchType = "3"
	InTotal       MatchType = "4"
)

type RuleCondition struct {
	CompositeMetricQuery *model.CompositeMetricQuery `json:"compositeMetricQuery,omitempty" yaml:"compositeMetricQuery,omitempty"`
	CompareOp            string                      `yaml:"op,omitempty" json:"op,omitempty"`
	Target               *float64                    `yaml:"target,omitempty" json:"target,omitempty"`
	MatchType            `json:"matchType,omitempty"`
}

// AlertState denotes the state of an active alert.
type AlertState int

const (
	StateInactive AlertState = iota
	StatePending
	StateFiring
	StateDisabled
)

func (s AlertState) String() string {
	switch s {
	case StateInactive:
		return "inactive"
	case StatePending:
		return "pending"
	case StateFiring:
		return "firing"
	case StateDisabled:
		return "disabled"
	}
	panic(errors.Errorf("unknown alert state: %d", s))
}

type BaseLabels interface {
	Len() int
	Swap(i, j int)
	Less(i, j int) bool
	String() string
	Hash() uint64
	HashForLabels(b []byte, names ...string) (uint64, []byte)
	Get(name string) string
	Has(name string) bool
	Map() map[string]string
}

type Alert struct {
	State AlertState

	Labels      BaseLabels
	Annotations BaseLabels

	GeneratorURL string

	// list of preferred receivers, e.g. slack
	Receivers []string

	Value      float64
	ActiveAt   time.Time
	FiredAt    time.Time
	ResolvedAt time.Time
	LastSentAt time.Time
	ValidUntil time.Time
}

const (
	RuleTypeThreshold = "threshold_rule"
	RuleTypeProm      = "promql_rule"
)

type NamedAlert struct {
	Name string
	*Alert
}

type RuleConditionNew struct {
	CompositeQuery *v3.CompositeQuery `json:"compositeQuery,omitempty" yaml:"compositeMetricQuery,omitempty"`
	CompareOp      string             `yaml:"op,omitempty" json:"op,omitempty"`
	Target         *float64           `yaml:"target,omitempty" json:"target,omitempty"`
	MatchType      `json:"matchType,omitempty"`
}

func GetStoredRules(db *sqlx.DB) ([]StoredRule, error) {

	rules := []StoredRule{}

	query := `SELECT id, updated_at, data FROM rules`

	err := db.Select(&rules, query)

	if err != nil {
		zap.S().Debug("Error in processing sql query: ", err)
		return nil, err
	}

	return rules, nil
}

func migrateRule(rule StoredRule) (StoredRuleNew, error) {

	var err error

	ruleStruct := GettableRule{}
	err = json.Unmarshal([]byte(rule.Data), &ruleStruct)
	if err != nil {
		panic(err)
	}

	ruleStructNew := GettableRuleNew{}
	ruleStructNew.Alert = ruleStruct.Alert
	ruleStructNew.AlertType = ruleStruct.AlertType
	ruleStructNew.Description = ruleStruct.Description
	ruleStructNew.RuleType = ruleStruct.RuleType
	ruleStructNew.EvalWindow = ruleStruct.EvalWindow
	ruleStructNew.Frequency = ruleStruct.Frequency
	ruleStructNew.RuleCondition = &RuleConditionNew{}
	ruleStructNew.RuleCondition.CompositeQuery = &v3.CompositeQuery{}
	ruleStructNew.Labels = ruleStruct.Labels
	ruleStructNew.Annotations = ruleStruct.Annotations
	ruleStructNew.Disabled = ruleStruct.Disabled
	ruleStructNew.Source = ruleStruct.Source
	ruleStructNew.PreferredChannels = ruleStruct.PreferredChannels
	ruleStructNew.Expr = ruleStruct.Expr
	ruleStructNew.OldYaml = ruleStruct.OldYaml

	if ruleStruct.RuleCondition != nil {
		ruleStructNew.RuleCondition.CompareOp = ruleStruct.RuleCondition.CompareOp
		ruleStructNew.RuleCondition.Target = ruleStruct.RuleCondition.Target
		ruleStructNew.RuleCondition.MatchType = ruleStruct.RuleCondition.MatchType
		ruleStructNew.RuleCondition.CompositeQuery.ClickHouseQueries = make(map[string]*v3.ClickHouseQuery)
		for name, q := range ruleStruct.RuleCondition.CompositeMetricQuery.ClickHouseQueries {
			ruleStructNew.RuleCondition.CompositeQuery.ClickHouseQueries[name] = &v3.ClickHouseQuery{
				Query:    q.Query,
				Disabled: q.Disabled,
			}
		}
		ruleStructNew.RuleCondition.CompositeQuery.PromQueries = make(map[string]*v3.PromQuery)
		for name, q := range ruleStruct.RuleCondition.CompositeMetricQuery.PromQueries {
			ruleStructNew.RuleCondition.CompositeQuery.PromQueries[name] = &v3.PromQuery{
				Query:    q.Query,
				Disabled: q.Disabled,
				Stats:    q.Stats,
			}
		}

		ruleStructNew.RuleCondition.CompositeQuery.BuilderQueries = make(map[string]*v3.BuilderQuery)
		for name, q := range ruleStruct.RuleCondition.CompositeMetricQuery.BuilderQueries {
			ruleStructNew.RuleCondition.CompositeQuery.BuilderQueries[name] = &v3.BuilderQuery{
				QueryName:         q.QueryName,
				StepInterval:      60,
				DataSource:        "metrics",
				AggregateOperator: v3.AggregateOperator(aggregateOperatorIntToString(int(q.AggregateOperator))),
				AggregateAttribute: v3.AttributeKey{
					Key:      q.MetricName,
					DataType: "float64",
					Type:     "",
					IsColumn: true,
				},
				Filters:    &v3.FilterSet{Items: make([]v3.FilterItem, 0), Operator: "AND"},
				GroupBy:    make([]v3.AttributeKey, len(q.GroupingTags)),
				Expression: q.Expression,
				Disabled:   q.Disabled,
				ReduceTo:   v3.ReduceToOperator((stringAggrOperatorFromInt(int(q.ReduceTo)))),
			}
			for i, tag := range q.GroupingTags {
				ruleStructNew.RuleCondition.CompositeQuery.BuilderQueries[name].GroupBy[i] = v3.AttributeKey{
					Key:      tag,
					DataType: "string",
					Type:     "tag",
					IsColumn: false,
				}
			}

			filterSet := &v3.FilterSet{}
			if q.TagFilters != nil {
				for _, filter := range q.TagFilters.Items {
					filterSet.Items = append(filterSet.Items, v3.FilterItem{
						Key: v3.AttributeKey{
							Key:      filter.Key,
							DataType: "string",
							Type:     "tag",
							IsColumn: false,
						},
						Operator: v3.FilterOperator(strings.ToLower(filter.Operator)),
						Value:    filter.Value,
					})
				}
			}
			filterSet.Operator = "AND"
			ruleStructNew.RuleCondition.CompositeQuery.BuilderQueries[name].Filters = filterSet
		}

		ruleStructNew.RuleCondition.CompositeQuery.PanelType = v3.PanelType(newPanelTypeFromInt(int(ruleStruct.RuleCondition.CompositeMetricQuery.PanelType)))
		ruleStructNew.RuleCondition.CompositeQuery.QueryType = v3.QueryType(stringQueryTypeFromInt(int(ruleStruct.RuleCondition.CompositeMetricQuery.QueryType)))
	}

	res, _ := json.Marshal(ruleStructNew)

	newRule := StoredRuleNew{
		Id:        rule.Id,
		UpdatedAt: rule.UpdatedAt,
		Data:      string(res),
	}

	return newRule, err
}

func migrateRules(db *sqlx.DB) error {

	rules, err := GetStoredRules(db)

	if err != nil {
		return err
	}

	log.Printf("Migrating %d rules", len(rules))

	for _, rule := range rules {
		log.Printf("Migrating rule %d", rule.Id)
		newRule, err := migrateRule(rule)
		if err != nil {
			panic(err)
		}
		query := `UPDATE rules SET data = ? WHERE id = ?`
		_, err = db.Exec(query, newRule.Data, newRule.Id)
		if err != nil {
			panic(err)
		}
	}
	log.Printf("Migrated %d rules", len(rules))

	return nil
}
