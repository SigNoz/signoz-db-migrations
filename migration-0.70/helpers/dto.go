package helpers

// result from checking one metric pair
type MetricResult struct {
	NormMetricName      string
	UnNormMetricName    string
	NormAttributes      []string
	UnNormAttributes    []string
	NormToUnNormAttrMap map[string]string
	Err                 error
}
