package main

import (
	"migration-0.70/helpers"
	"testing"
)

// MetricResult holds mappings from underscore-style to dot-style names
// for both metrics and labels.

func TestTransformPromQLQuery(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		metricResult []helpers.MetricResult
		want         string
	}{
		{
			name:  "simple sum rate by underscore",
			input: `sum(rate(container_cpu_utilization{k8s_namespace_name="ns"}[5m])) by (k8s_pod_name)`,
			metricResult: []helpers.MetricResult{{
				NormToUnNormAttrMap: map[string]string{
					"container_cpu_utilization": "container.cpu.utilization",
					"k8s_namespace_name":        "k8s.namespace.name",
					"k8s_pod_name":              "k8s.pod.name",
				},
				NormMetricName:   "container_cpu_utilization",
				UnNormMetricName: "container.cpu.utilization",
			}},
			want: `sum by ("k8s.pod.name") (rate({"container.cpu.utilization","k8s.namespace.name"="ns"}[5m]))`,
		},
		{
			name:  "histogram_quantile underscore",
			input: `histogram_quantile(0.95, sum(rate(request_duration_bucket{job="api"}[5m])) by (le))`,
			metricResult: []helpers.MetricResult{{
				NormToUnNormAttrMap: map[string]string{
					"request_duration_bucket": "request.duration.bucket",
				},
				NormMetricName:   "request_duration_bucket",
				UnNormMetricName: "request.duration.bucket",
			}},
			want: `histogram_quantile(0.95, sum by (le) (rate({"request.duration.bucket",job="api"}[5m])))`,
		},
		{
			name:  "nested arithmetic underscore",
			input: `max_over_time(foo_bar_total{env=~"prod|stag"}[1h] offset 30m) / ignoring(instance) group_left sum(rate(other_metric[5m])) without (pod)`,
			metricResult: []helpers.MetricResult{
				{NormToUnNormAttrMap: map[string]string{"foo_bar_total": "foo.bar.total"}, NormMetricName: "foo_bar_total", UnNormMetricName: "foo.bar.total"},
				{NormToUnNormAttrMap: map[string]string{"other_metric": "other.metric"}, NormMetricName: "other_metric", UnNormMetricName: "other.metric"},
			},
			want: `max_over_time({"foo.bar.total",env=~"prod|stag"}[1h] offset 30m) / ignoring ("instance") group_left () sum without (pod) (rate({"other.metric"}[5m]))`,
		},
		{
			name:  "subquery and scalar underscore",
			input: `avg_over_time(my_metric[5m:1m]) > scalar(up{job="svc"} offset 1h)`,
			metricResult: []helpers.MetricResult{
				{NormToUnNormAttrMap: map[string]string{"my_metric": "my.metric"}, NormMetricName: "my_metric", UnNormMetricName: "my.metric"},
				{NormToUnNormAttrMap: map[string]string{}, NormMetricName: "up", UnNormMetricName: "up"},
			},
			want: `avg_over_time({"my.metric"}[5m:1m]) > scalar({"up",job="svc"} offset 1h)`,
		},
		{
			name:  "recording unless underscore",
			input: `rate(http_requests_total[5m]) unless on(method) rate(errors_total{status!~"2.."}[5m])`,
			metricResult: []helpers.MetricResult{{
				NormToUnNormAttrMap: map[string]string{"http_requests_total": "http.requests.total", "errors_total": "errors.total"},
				NormMetricName:      "http_requests_total",
				UnNormMetricName:    "http.requests.total",
			}},
			want: `rate({"http.requests.total"}[5m]) unless on ("method") rate({"errors.total",status!~"2.."}[5m])`,
		},
		{
			name:  "deep nesting by/without underscore",
			input: `sum without(job,instance)(rate(metric_a[1m])) + sum by(region,service)(irate(metric_b{foo="bar"}[2m]))`,
			metricResult: []helpers.MetricResult{{
				NormToUnNormAttrMap: map[string]string{"metric_a": "metric.a", "metric_b": "metric.b"},
				NormMetricName:      "metric_a",
				UnNormMetricName:    "metric.a",
			}},
			want: `sum without (job, instance) (rate({"metric.a"}[1m])) + sum by (region, service) (irate({"metric.b",foo="bar"}[2m]))`,
		},
		{
			name:  "topk bottomk underscore",
			input: `topk(3, rate(cpu_usage[30s])) + bottomk(2, memory_usage_bytes)`,
			metricResult: []helpers.MetricResult{
				{NormToUnNormAttrMap: map[string]string{"cpu_usage": "cpu.usage"}, NormMetricName: "cpu_usage", UnNormMetricName: "cpu.usage"},
				{NormToUnNormAttrMap: map[string]string{"memory_usage_bytes": "memory.usage.bytes"}, NormMetricName: "memory_usage_bytes", UnNormMetricName: "memory.usage.bytes"},
			},
			want: `topk(3, rate({"cpu.usage"}[30s])) + bottomk(2, {"memory.usage.bytes"})`,
		},
		{
			name:  "quantile sort underscore",
			input: `quantile(0.5, foo_bar) and sort_desc(error_count{service=~"a|b"})`,
			metricResult: []helpers.MetricResult{{
				NormToUnNormAttrMap: map[string]string{"foo_bar": "foo.bar", "error_count": "error.count"},
				NormMetricName:      "foo_bar",
				UnNormMetricName:    "foo.bar",
			}},
			want: `quantile(0.5, {"foo.bar"}) and sort_desc({"error.count",service=~"a|b"})`,
		},
		{
			name:  "absent absent_over_time underscore",
			input: `absent(http_requests_total{code!~"2.."}) or absent_over_time(disk_io[5m])`,
			metricResult: []helpers.MetricResult{
				{NormToUnNormAttrMap: map[string]string{"http_requests_total": "http.requests.total", "disk_io": "disk.io"}, NormMetricName: "http_requests_total", UnNormMetricName: "http.requests.total"},
				{NormToUnNormAttrMap: map[string]string{"disk_io": "disk.io"}, NormMetricName: "disk_io", UnNormMetricName: "disk.io"},
			},
			want: `absent({"http.requests.total",code!~"2.."}) or absent_over_time({"disk.io"}[5m])`,
		},
		{
			name:  "label_join underscore",
			input: `label_join(my_metric{tag="x"}, "newtag", "-", "tag")`,
			metricResult: []helpers.MetricResult{{
				NormToUnNormAttrMap: map[string]string{"my_metric": "my.metric"},
				NormMetricName:      "my_metric",
				UnNormMetricName:    "my.metric",
			}},
			want: `label_join({"my.metric",tag="x"}, "newtag", "-", "tag")`,
		},
		{
			name:  "binary matching on ignoring underscore",
			input: `foo{a="b"} + on(a) group_right(b) bar{c!~"d.*"}`,
			metricResult: []helpers.MetricResult{{
				NormToUnNormAttrMap: map[string]string{"foo": "foo.metric", "bar": "bar.metric"},
				NormMetricName:      "foo",
				UnNormMetricName:    "foo.metric",
			}},
			want: `{"foo.metric",a="b"} + on ("a") group_right ("b") {"bar.metric",c!~"d.*"}`,
		},
		{
			name:  "binary matching on underscore",
			input: `(system_memory_usage{k8s_cluster_name="$k8s_cluster_name",k8s_node_name="$k8s_node_name",state=~"buffered|cached|free|used"})`,
			metricResult: []helpers.MetricResult{{
				NormToUnNormAttrMap: map[string]string{"k8s_cluster_name": "k8s.cluster.name", "k8s_node_name": "k8s.node.name"},
				NormMetricName:      "system_memory_usage",
				UnNormMetricName:    "system.memory.usage",
			}},
			want: `({"system.memory.usage","k8s.cluster.name"="$k8s.cluster.name","k8s.node.name"="$k8s.node.name",state=~"buffered|cached|free|used"})`,
		},
	}

	for _, tc := range tests {
		got, err := helpers.TransformPromQLQuery(tc.input, tc.metricResult)
		if err != nil {
			t.Errorf("%s: unexpected error: %v", tc.name, err)
			continue
		}
		if got != tc.want {
			t.Errorf("%s:\n got:  %s\n want: %s", tc.name, got, tc.want)
		}
	}
}
