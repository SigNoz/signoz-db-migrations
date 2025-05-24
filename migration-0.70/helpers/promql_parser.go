package helpers

import "github.com/prometheus/prometheus/promql/parser"

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

	// Walk AST and collect metric names from VectorSelector nodes
	parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
		if vs, ok := node.(*parser.VectorSelector); ok {
			metricsSet[vs.Name] = MetricResult{}
		}
		return nil
	})

	// Convert set to slice
	metrics := make([]MetricResult, 0, len(metricsSet))
	for _, m := range metricsSet {
		metrics = append(metrics, m)
	}

	return metrics, nil
}

func TransformPromQLQuery(query string, metricResult []MetricResult) (string, error) {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return "", err
	}

	replacements := make(map[string]string)
	for _, metric := range metricResult {
		replacements = MergeMaps(replacements, metric.NormToUnNormAttrMap)
		replacements[metric.NormMetricName] = replacements[metric.UnNormMetricName]
	}

	t := PromQLTransformer{
		Replacements: replacements,
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
