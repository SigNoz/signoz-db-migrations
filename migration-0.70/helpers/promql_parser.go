package helpers

import (
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"strings"
)

type PromQLTransformer struct {
	Replacements map[string]string
}

func (t *PromQLTransformer) Transform(query string) (string, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", err
	}

	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		vs, ok := node.(*parser.VectorSelector)
		if !ok {
			return nil
		}

		// Replace metric name if in map
		if newName, exists := t.Replacements[vs.Name]; exists {
			vs.Name = newName
		}

		// Replace label matchers - label names and values
		for _, m := range vs.LabelMatchers {
			// Replace label name
			if newLabelName, exists := t.Replacements[m.Name]; exists {
				m.Name = newLabelName
			}
			// Replace label value (only for supported matcher types)
			if newVal, exists := t.Replacements[m.Value]; exists {
				m.Value = newVal
			}
		}

		return nil
	})

	return expr.String(), nil
}

func ExtractPromMetrics(query string, metricsSet map[string]MetricResult) ([]MetricResult, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return nil, err
	}

	metricsMap := make(map[string]string)

	// Walk AST and collect metric names from VectorSelector nodes
	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		if vs, ok := node.(*parser.VectorSelector); ok {
			for _, m := range vs.LabelMatchers {
				if m.Name == "__name__" {
					metricsMap[m.Value] = m.Name
				}
			}
		}
		return nil
	})

	// Convert set to slice
	metrics := make([]MetricResult, 0, len(metricsSet))
	for k, _ := range metricsMap {
		metrics = append(metrics, metricsSet[k])
	}

	return metrics, nil
}

func TransformPromQLQuery(query string, metricResult []MetricResult) (string, error) {
	// 1) Parse the expression
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", err
	}

	// 2) Build replacement map
	repl := make(map[string]string)
	for _, m := range metricResult {
		repl = MergeMaps(repl, m.NormToUnNormAttrMap)
		if m.UnNormMetricName != "" {
			repl[m.NormMetricName] = m.UnNormMetricName
		}
	}

	// Helper function to transform a VectorSelector
	transformVectorSelector := func(vs *parser.VectorSelector) {
		if orig := vs.Name; orig != "" {
			// Use replacement if available, otherwise use original
			mapped := orig
			if r, ok := repl[orig]; ok {
				mapped = r
			}

			// Always convert to label selector format
			vs.Name = ""

			// Remove any existing __name__ matchers to avoid duplication
			var filtered []*labels.Matcher
			for _, matcher := range vs.LabelMatchers {
				if matcher.Name != "__name__" {
					filtered = append(filtered, matcher)
				}
			}
			vs.LabelMatchers = filtered

			// Add the metric name as an unlabeled matcher (empty name)
			nameMatcher := &labels.Matcher{
				Type:  labels.MatchEqual,
				Name:  "",
				Value: mapped,
			}
			vs.LabelMatchers = append([]*labels.Matcher{nameMatcher}, vs.LabelMatchers...)
		}

		// Transform label matchers
		for _, lm := range vs.LabelMatchers {
			if lm.Name == "" {
				continue
			}
			if r, ok := repl[lm.Name]; ok {
				lm.Name = r
			}
			if r, ok := repl[lm.Value]; ok {
				lm.Value = r
			}
		}
	}

	// 3) Traverse AST and transform all nodes
	parser.Inspect(expr, func(node parser.Node, _ []parser.Node) error {
		switch sel := node.(type) {
		case *parser.VectorSelector:
			transformVectorSelector(sel)

		case *parser.MatrixSelector:
			if vs, ok := sel.VectorSelector.(*parser.VectorSelector); ok {
				transformVectorSelector(vs)
			}

		case *parser.SubqueryExpr:
			if vs, ok := sel.Expr.(*parser.VectorSelector); ok {
				transformVectorSelector(vs)
			}

		case *parser.AggregateExpr:
			// Transform grouping labels
			for i, lbl := range sel.Grouping {
				key := string(lbl)
				if r, ok := repl[key]; ok {
					sel.Grouping[i] = r
				}
			}

		case *parser.BinaryExpr:
			if vm := sel.VectorMatching; vm != nil {
				// 1) rewrite any on/ignoring labels from repl, then wrap in quotes
				for i, lbl := range vm.MatchingLabels {
					name := string(lbl)
					if r, ok := repl[name]; ok {
						name = r
					}
					vm.MatchingLabels[i] = `"` + name + `"`
				}
				// 2) if you have a group_left or group_right, wrap those too
				for i, lbl := range vm.Include {
					name := string(lbl)
					if r, ok := repl[name]; ok {
						name = r
					}
					vm.Include[i] = `"` + name + `"`
				}
			}
		}
		return nil
	})

	// 4) Custom string rendering to handle the unlabeled matcher format
	return renderExprWithCustomFormat(expr), nil
}

// Custom renderer to convert {""="value"} to {"value"}
func renderExprWithCustomFormat(expr parser.Expr) string {
	result := expr.String()

	// Handle the unlabeled matcher format conversion
	// This converts {""="metric_name", to {"metric_name",
	for {
		before := result
		result = strings.Replace(result, `{""="`, `{"`, 1)
		result = strings.Replace(result, `,""="`, `,"`, 1)
		if result == before {
			break
		}
	}

	return result
}
