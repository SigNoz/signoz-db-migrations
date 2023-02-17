package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// this file contains common structs and methods used by
// rule engine

// how long before re-sending the alert
const resolvedRetention = 15 * time.Minute

const (
	// AlertMetricName is the metric name for synthetic alert timeseries.
	alertMetricName = "ALERTS"

	// AlertForStateMetricName is the metric name for 'for' state of alert.
	alertForStateMetricName = "ALERTS_FOR_STATE"

	TestAlertPostFix = "_TEST_ALERT"
)

type RuleType string

const (
	RuleTypeThreshold = "threshold_rule"
	RuleTypeProm      = "promql_rule"
)

type RuleHealth string

const (
	HealthUnknown RuleHealth = "unknown"
	HealthGood    RuleHealth = "ok"
	HealthBad     RuleHealth = "err"
)

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

type Alert struct {
	State AlertState

	Labels      labels.BaseLabels
	Annotations labels.BaseLabels

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

func (a *Alert) needsSending(ts time.Time, resendDelay time.Duration) bool {
	if a.State == StatePending {
		return false
	}

	// if an alert has been resolved since the last send, resend it
	if a.ResolvedAt.After(a.LastSentAt) {
		return true
	}

	return a.LastSentAt.Add(resendDelay).Before(ts)
}

type NamedAlert struct {
	Name string
	*Alert
}

type CompareOp string

const (
	CompareOpNone CompareOp = "0"
	ValueIsAbove  CompareOp = "1"
	ValueIsBelow  CompareOp = "2"
	ValueIsEq     CompareOp = "3"
	ValueIsNotEq  CompareOp = "4"
)

func ResolveCompareOp(cop CompareOp) string {
	switch cop {
	case ValueIsAbove:
		return ">"
	case ValueIsBelow:
		return "<"
	case ValueIsEq:
		return "=="
	case ValueIsNotEq:
		return "!="
	}
	return ""
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
	CompareOp            CompareOp                   `yaml:"op,omitempty" json:"op,omitempty"`
	Target               *float64                    `yaml:"target,omitempty" json:"target,omitempty"`
	MatchType            `json:"matchType,omitempty"`
}

func (rc *RuleCondition) IsValid() bool {

	if rc.CompositeMetricQuery == nil {
		return false
	}

	if rc.QueryType() == model.QUERY_BUILDER {
		if rc.Target == nil {
			return false
		}
		if rc.CompareOp == "" {
			return false
		}
	}
	if rc.QueryType() == model.PROM {

		if len(rc.CompositeMetricQuery.PromQueries) == 0 {
			return false
		}
	}
	return true
}

// QueryType is a short hand method to get query type
func (rc *RuleCondition) QueryType() model.QueryType {
	if rc.CompositeMetricQuery != nil {
		return rc.CompositeMetricQuery.QueryType
	}
	return 0
}

// String is useful in printing rule condition in logs
func (rc *RuleCondition) String() string {
	if rc == nil {
		return ""
	}
	data, _ := json.Marshal(*rc)
	return string(data)
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

// prepareRuleGeneratorURL creates an appropriate url
// for the rule. the URL is sent in slack messages as well as
// to other systems and allows backtracking to the rule definition
// from the third party systems.
func prepareRuleGeneratorURL(ruleId string, source string) string {
	if source == "" {
		return source
	}

	// check if source is a valid url
	_, err := url.Parse(source)
	if err != nil {
		return ""
	}
	// since we capture window.location when a new rule is created
	// we end up with rulesource host:port/alerts/new. in this case
	// we want to replace new with rule id parameter

	hasNew := strings.LastIndex(source, "new")
	if hasNew > -1 {
		ruleURL := fmt.Sprintf("%sedit?ruleId=%s", source[0:hasNew], ruleId)
		return ruleURL
	}

	return source
}

type StoredRule struct {
	Id        int       `json:"id" db:"id"`
	UpdatedAt time.Time `json:"updated_at" db:"updated_at"`
	Data      string    `json:"data" db:"data"`
}

// GettableRule has info for an alerting rules.
type GettableRule struct {
	Id    string `json:"id"`
	State string `json:"state"`
	PostableRule
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
