package helpers

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/AfterShip/clickhouse-sql-parser/parser"
)

type QueryTransformer struct {
	mappings map[string]MetricResult // old -> new mapping
}

func NewQueryTransformer(mappings map[string]MetricResult) *QueryTransformer {
	return &QueryTransformer{mappings: mappings}
}

func (qt *QueryTransformer) TransformQuery(query string, metrics []MetricResult, attrMap map[string]string) (string, error) {
	query = SanitizeQuery(query)
	p := parser.NewParser(query)
	stmts, err := p.ParseStmts()
	if err != nil {
		return "", fmt.Errorf("failed to parse query: %w", err)
	}

	mappings := make(map[string]string)
	for _, metric := range metrics {
		mappings = MergeMaps(mappings, metric.NormToUnNormAttrMap)
		mappings[metric.NormMetricName] = metric.UnNormMetricName
	}

	if len(mappings) == 0 {
		mappings = attrMap
	}

	for _, stmt := range stmts {
		visitor := &NameReplacementVisitor{mappings: mappings, attrMap: attrMap}
		if err := stmt.Accept(visitor); err != nil {
			return "", fmt.Errorf("failed to transform query: %w", err)
		}
	}

	var result strings.Builder
	for i, stmt := range stmts {
		if i > 0 {
			result.WriteString(";\n")
		}
		result.WriteString(stmt.String())
	}

	return result.String(), nil
}

type NameReplacementVisitor struct {
	parser.DefaultASTVisitor
	mappings map[string]string
	attrMap  map[string]string
}

func needsQuoting(_ string) bool {
	return true
}

func (v *NameReplacementVisitor) transformIdentifier(ident *parser.Ident) {
	if strings.HasPrefix(ident.Name, "$") {
		name := strings.TrimPrefix(ident.Name, "$")
		if newName, ok := v.mappings[name]; ok {
			ident.Name = "$" + newName
		}
		if newName, ok := v.attrMap[name]; ok {
			ident.Name = "$" + newName
		}

	} else {
		if newName, exists := v.mappings[ident.Name]; exists {
			ident.Name = newName
			if needsQuoting(newName) {
				ident.QuoteType = parser.BackTicks
			}
		}
		if newName, exists := v.attrMap[ident.Name]; exists {
			ident.Name = newName
			if needsQuoting(newName) {
				ident.QuoteType = parser.BackTicks
			}
		}
	}
}

func (v *NameReplacementVisitor) VisitIdent(expr *parser.Ident) error {
	v.transformIdentifier(expr)
	return nil
}

func (v *NameReplacementVisitor) VisitStringLiteral(expr *parser.StringLiteral) error {
	if newName, exists := v.mappings[expr.Literal]; exists {
		expr.Literal = newName
	}
	if newName, exists := v.attrMap[expr.Literal]; exists {
		expr.Literal = newName
	}
	return nil
}

func (v *NameReplacementVisitor) VisitNestedIdentifier(expr *parser.NestedIdentifier) error {
	v.transformIdentifier(expr.Ident)
	if expr.DotIdent != nil {
		v.transformIdentifier(expr.DotIdent)
	}
	return nil
}

func (v *NameReplacementVisitor) VisitColumnIdentifier(expr *parser.ColumnIdentifier) error {
	if expr.Column != nil {
		v.transformIdentifier(expr.Column)
	}
	return nil
}

func ExtractMetrics(query string, metricsMap map[string]MetricResult) ([]string, error) {
	p := parser.NewParser(query)
	stmts, err := p.ParseStmts()
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	// Use a set to track matched metrics
	matchedMetrics := make(map[string]struct{})

	extractor := &MetricExtractor{
		metricsMap:       metricsMap,
		matchedMetricSet: matchedMetrics,
	}

	for _, stmt := range stmts {
		if err := stmt.Accept(extractor); err != nil {
			return nil, fmt.Errorf("failed to extract metrics: %w", err)
		}
	}

	// Convert set keys to slice
	var result []string
	for metric := range matchedMetrics {
		result = append(result, metric)
	}

	return result, nil
}

type MetricExtractor struct {
	parser.DefaultASTVisitor

	metricsMap       map[string]MetricResult
	matchedMetricSet map[string]struct{}
}

func (e *MetricExtractor) VisitIdent(expr *parser.Ident) error {
	// Check if this identifier is a known metric name
	if _, exists := e.metricsMap[expr.Name]; exists {
		e.matchedMetricSet[expr.Name] = struct{}{}
	}
	return nil
}

func (e *MetricExtractor) VisitStringLiteral(expr *parser.StringLiteral) error {
	// Sometimes metric names appear as string literals
	if _, exists := e.metricsMap[expr.Literal]; exists {
		e.matchedMetricSet[expr.Literal] = struct{}{}
	}
	return nil
}

func MergeMaps(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

func ConvertTemplateToNamedParams(query string) string {
	// Match optional whitespace, optional leading dot, then the name
	re := regexp.MustCompile(`{{\s*\.?([A-Za-z_][A-Za-z0-9_]*)\s*}}`)
	return re.ReplaceAllString(query, `$$$1`) // $$ => literal $, $1 => captured name
}

func SanitizeQuery(query string) string {
	// Normalize line endings: remove all '\r'
	query = strings.ReplaceAll(query, "\r", "")

	// Replace multiple newlines (2 or more) with a single newline
	reMultipleNewlines := regexp.MustCompile(`\n{2,}`)
	query = reMultipleNewlines.ReplaceAllString(query, "\n")

	// Trim spaces on each line
	lines := strings.Split(query, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " \t")
	}

	// Remove lines that are completely empty after trimming
	cleanLines := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			cleanLines = append(cleanLines, line)
		}
	}

	return strings.Join(cleanLines, "\n")
}
