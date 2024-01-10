import unittest
import json
from .alerts import update_alert

class TestUpdateDashboard(unittest.TestCase):
    fields = {
        "attributes_string_service_name": ['service.name', 'string', 'attributes'],
        "resources_string_container_name": ['container.name', 'string', 'resources'],
        "attributes_float64_extra_info_request_timings_sys_schedule_thread":['extra_info.request_timings.sys.schedule_thread', 'float64', 'attributes'],
        "resources_string_service_name": ['service.name', 'String', 'resources']
    }
    def test_update_dashboard_builder_query(self):
        old_alert = {
            "id": "64",
            "state": "firing",
            "alert": "nitya test",
            "alertType": "LOGS_BASED_ALERT",
            "ruleType": "threshold_rule",
            "evalWindow": "24h0m0s",
            "condition": {
                "compositeQuery": {
                    "builderQueries": {
                        "A": {
                            "queryName": "A",
                            "stepInterval": 60,
                            "dataSource": "logs",
                            "aggregateOperator": "count",
                            "aggregateAttribute": {
                                "key": "",
                                "dataType": "",
                                "type": "",
                                "isColumn": False,
                                "isJSON": False
                            },
                            "filters": {
                                "op": "AND",
                                "items": [
                                    {
                                        "key": {
                                            "key": "service_name",
                                            "dataType": "string",
                                            "type": "tag",
                                            "isColumn": False,
                                            "isJSON": False
                                        },
                                        "value": "customer_client",
                                        "op": "="
                                    }
                                ]
                            },
                            "groupBy": [
                                {
                                    "key": "container_name",
                                    "dataType": "string",
                                    "type": "resource",
                                    "isColumn": False,
                                    "isJSON": False
                                }
                            ],
                            "expression": "A",
                            "disabled": True,
                            "legend": "{{container_name}}",
                            "limit": 0,
                            "offset": 0,
                            "pageSize": 0,
                            "orderBy": [
                                {
                                    "columnName": "container_name",
                                    "order": "asc"
                                }
                            ],
                            "reduceTo": "sum"
                        },
                        "F1": {
                            "queryName": "F1",
                            "stepInterval": 0,
                            "dataSource": "",
                            "aggregateOperator": "",
                            "aggregateAttribute": {
                                "key": "",
                                "dataType": "",
                                "type": "",
                                "isColumn": False,
                                "isJSON": False
                            },
                            "expression": "A * 100",
                            "disabled": False,
                            "legend": "{{container_name}}",
                            "limit": 0,
                            "offset": 0,
                            "pageSize": 0
                        }
                    },
                    "chQueries": {
                        "A": {
                            "query": "select \ntoStartOfInterval(fromUnixTimestamp64Nano(timestamp), INTERVAL 30 MINUTE) AS interval, \ntoFloat64(count()) as value \nFROM signoz_logs.distributed_logs  \nWHERE timestamp BETWEEN {{.start_timestamp_nano}} AND {{.end_timestamp_nano}}  \nGROUP BY interval;\n\n-- available variables:\n-- \t{{.start_timestamp_nano}}\n-- \t{{.end_timestamp_nano}}\n\n-- required columns (or alias):\n-- \tvalue\n-- \tinterval",
                            "disabled": False
                        }
                    },
                    "promQueries": {
                        "A": {
                            "query": "",
                            "disabled": False
                        }
                    },
                    "panelType": "graph",
                    "queryType": "builder"
                },
                "op": "1",
                "target": 20000,
                "matchType": "4",
                "selectedQueryName": "F1"
            },
            "labels": {
                "details": "https://stagingapp.signoz.io/logs/logs-explorer",
                "severity": "info"
            },
            "annotations": {
                "description": "This alert is fired when the defined metric (current value: {{$value}}) crosses the threshold ({{$threshold}})",
                "summary": "The rule threshold is set to {{$threshold}}, and the observed metric value is {{$value}}"
            },
            "disabled": False,
            "source": "",
            "createAt": "2024-01-09T09:09:27.236105749Z",
            "createBy": "prashant@signoz.io",
            "updateAt": "2024-01-09T09:09:27.236105812Z",
            "updateBy": "prashant@signoz.io"
        }

        expected_updated_alert = {
            "id": "64",
            "state": "firing",
            "alert": "nitya test",
            "alertType": "LOGS_BASED_ALERT",
            "ruleType": "threshold_rule",
            "evalWindow": "24h0m0s",
            "condition": {
                "compositeQuery": {
                    "builderQueries": {
                        "A": {
                            "queryName": "A",
                            "stepInterval": 60,
                            "dataSource": "logs",
                            "aggregateOperator": "count",
                            "aggregateAttribute": {
                                "key": "",
                                "dataType": "",
                                "type": "",
                                "isColumn": False,
                                "isJSON": False
                            },
                            "filters": {
                                "op": "AND",
                                "items": [
                                    {
                                        "key": {
                                            "key": "service.name",
                                            "dataType": "string",
                                            "type": "tag",
                                            "isColumn": False,
                                            "isJSON": False
                                        },
                                        "value": "customer_client",
                                        "op": "="
                                    }
                                ]
                            },
                            "groupBy": [
                                {
                                    "key": "container.name",
                                    "dataType": "string",
                                    "type": "resource",
                                    "isColumn": False,
                                    "isJSON": False
                                }
                            ],
                            "expression": "A",
                            "disabled": True,
                            "legend": "{{container.name}}",
                            "limit": 0,
                            "offset": 0,
                            "pageSize": 0,
                            "orderBy": [
                                {
                                    "columnName": "container.name",
                                    "order": "asc"
                                }
                            ],
                            "reduceTo": "sum"
                        },
                        "F1": {
                            "queryName": "F1",
                            "stepInterval": 0,
                            "dataSource": "",
                            "aggregateOperator": "",
                            "aggregateAttribute": {
                                "key": "",
                                "dataType": "",
                                "type": "",
                                "isColumn": False,
                                "isJSON": False
                            },
                            "expression": "A * 100",
                            "disabled": False,
                            "legend": "{{container.name}}",
                            "limit": 0,
                            "offset": 0,
                            "pageSize": 0
                        }
                    },
                    "chQueries": {
                        "A": {
                            "query": "select \ntoStartOfInterval(fromUnixTimestamp64Nano(timestamp), INTERVAL 30 MINUTE) AS interval, \ntoFloat64(count()) as value \nFROM signoz_logs.distributed_logs  \nWHERE timestamp BETWEEN {{.start_timestamp_nano}} AND {{.end_timestamp_nano}}  \nGROUP BY interval;\n\n-- available variables:\n-- \t{{.start_timestamp_nano}}\n-- \t{{.end_timestamp_nano}}\n\n-- required columns (or alias):\n-- \tvalue\n-- \tinterval",
                            "disabled": False
                        }
                    },
                    "promQueries": {
                        "A": {
                            "query": "",
                            "disabled": False
                        }
                    },
                    "panelType": "graph",
                    "queryType": "builder"
                },
                "op": "1",
                "target": 20000,
                "matchType": "4",
                "selectedQueryName": "F1"
            },
            "labels": {
                "details": "https://stagingapp.signoz.io/logs/logs-explorer",
                "severity": "info"
            },
            "annotations": {
                "description": "This alert is fired when the defined metric (current value: {{$value}}) crosses the threshold ({{$threshold}})",
                "summary": "The rule threshold is set to {{$threshold}}, and the observed metric value is {{$value}}"
            },
            "disabled": False,
            "source": "",
            "createAt": "2024-01-09T09:09:27.236105749Z",
            "createBy": "prashant@signoz.io",
            "updateAt": "2024-01-09T09:09:27.236105812Z",
            "updateBy": "prashant@signoz.io"
        }

        resp = update_alert(old_alert, self.fields)
        equal = sorted(resp.items()) == sorted(expected_updated_alert.items())
        self.assertTrue(equal==True)
    
    def test_update_alert_SQL_query(self):
        old_alert = {
            "id": "7",
            "state": "firing",
            "alert": "Test Alert",
            "alertType": "METRIC_BASED_ALERT",
            "ruleType": "threshold_rule",
            "evalWindow": "5m0s",
            "condition": {
                "compositeQuery": {
                    "builderQueries": {
                        "A": {
                            "queryName": "A",
                            "stepInterval": 60,
                            "dataSource": "metrics",
                            "aggregateOperator": "count",
                            "aggregateAttribute": {
                                "key": "",
                                "dataType": "",
                                "type": "",
                                "isColumn": False,
                                "isJSON": False
                            },
                            "filters": {
                                "op": "AND",
                                "items": []
                            },
                            "expression": "A",
                            "disabled": False,
                            "limit": 0,
                            "offset": 0,
                            "pageSize": 0,
                            "reduceTo": "sum"
                        }
                    },
                    "chQueries": {
                        "A": {
                            "query": "SELECT\n    toStartOfInterval(fromUnixTimestamp64Nano(timestamp), toIntervalSecond(1800)) AS ts,\n    attribute_string_server AS server,\n    quantile(0.9)(attributes_float64_value[indexOf(attributes_float64_key, 'extra_info_request_timings_sys_schedule_thread')]) AS value\nFROM signoz_logs.distributed_logs\nWHERE ((timestamp \u003e= {{.start_timestamp_nano}}) AND (timestamp \u003c= {{.end_timestamp_nano}})) AND (attribute_string_team = 'ai-prod') AND (attribute_string_component = 'ai_proxy_server') AND (attribute_string_server_exists = true) AND has(attributes_float64_key, 'extra_info_request_timings_sys_schedule_thread')\nGROUP BY\n    server,\n    ts\nHAVING value \u003c 2000\nORDER BY value DESC",
                            "disabled": False
                        }
                    },
                    "promQueries": {
                        "A": {
                            "query": "",
                            "disabled": False
                        }
                    },
                    "panelType": "graph",
                    "queryType": "clickhouse_sql"
                },
                "op": "1",
                "target": 1,
                "matchType": "1"
            },
            "labels": {
            },
            "annotations": {
                "description": "This alert is fired when the defined metric (current value: {{$value}}) crosses the threshold ({{$threshold}})",
                "summary": "The rule threshold is set to {{$threshold}}, and the observed metric value is {{$value}}"
            },
            "disabled": False,
            "source": "",
            "preferredChannels": [
            ],
        }
        

        expected_updated_alert = {
            "id": "7",
            "state": "firing",
            "alert": "Test Alert",
            "alertType": "METRIC_BASED_ALERT",
            "ruleType": "threshold_rule",
            "evalWindow": "5m0s",
            "condition": {
                "compositeQuery": {
                    "builderQueries": {
                        "A": {
                            "queryName": "A",
                            "stepInterval": 60,
                            "dataSource": "metrics",
                            "aggregateOperator": "count",
                            "aggregateAttribute": {
                                "key": "",
                                "dataType": "",
                                "type": "",
                                "isColumn": False,
                                "isJSON": False
                            },
                            "filters": {
                                "op": "AND",
                                "items": []
                            },
                            "expression": "A",
                            "disabled": False,
                            "limit": 0,
                            "offset": 0,
                            "pageSize": 0,
                            "reduceTo": "sum"
                        }
                    },
                    "chQueries": {
                        "A": {
                            "query": "SELECT\n    toStartOfInterval(fromUnixTimestamp64Nano(timestamp), toIntervalSecond(1800)) AS ts,\n    attribute_string_server AS server,\n    quantile(0.9)(attributes_float64_value[indexOf(attributes_float64_key, 'extra_info.request_timings.sys.schedule_thread')]) AS value\nFROM signoz_logs.distributed_logs\nWHERE ((timestamp \u003e= {{.start_timestamp_nano}}) AND (timestamp \u003c= {{.end_timestamp_nano}})) AND (attribute_string_team = 'ai-prod') AND (attribute_string_component = 'ai_proxy_server') AND (attribute_string_server_exists = true) AND has(attributes_float64_key, 'extra_info.request_timings.sys.schedule_thread')\nGROUP BY\n    server,\n    ts\nHAVING value \u003c 2000\nORDER BY value DESC",
                            "disabled": False
                        }
                    },
                    "promQueries": {
                        "A": {
                            "query": "",
                            "disabled": False
                        }
                    },
                    "panelType": "graph",
                    "queryType": "clickhouse_sql"
                },
                "op": "1",
                "target": 1,
                "matchType": "1"
            },
            "labels": {
            },
            "annotations": {
                "description": "This alert is fired when the defined metric (current value: {{$value}}) crosses the threshold ({{$threshold}})",
                "summary": "The rule threshold is set to {{$threshold}}, and the observed metric value is {{$value}}"
            },
            "disabled": False,
            "source": "",
            "preferredChannels": [
            ],
        }
        
        resp = update_alert(old_alert, self.fields)
        # print(json.dumps(resp))
        # print(json.dumps(expected_updated_alert))
        equal = sorted(resp.items()) == sorted(expected_updated_alert.items())
        self.assertTrue(equal==True)
        
    def test_update_alert_SQL_query_materialized(self):
        old_alert = {
            "id": "7",
            "state": "firing",
            "alert": "Test Alert",
            "alertType": "METRIC_BASED_ALERT",
            "ruleType": "threshold_rule",
            "evalWindow": "5m0s",
            "condition": {
                "compositeQuery": {
                    "builderQueries": {
                        "A": {
                            "queryName": "A",
                            "stepInterval": 60,
                            "dataSource": "metrics",
                            "aggregateOperator": "count",
                            "aggregateAttribute": {
                                "key": "",
                                "dataType": "",
                                "type": "",
                                "isColumn": False,
                                "isJSON": False
                            },
                            "filters": {
                                "op": "AND",
                                "items": []
                            },
                            "expression": "A",
                            "disabled": False,
                            "limit": 0,
                            "offset": 0,
                            "pageSize": 0,
                            "reduceTo": "sum"
                        }
                    },
                    "chQueries": {
                        "A": {
                            "query": "select resource_string_service_name as service, count() as count_per_min from signoz_logs.distributed_logs where timestamp>toUnixTimestamp64Nano(now64() - INTERVAL 1 MINUTE) group by resource_string_service_name order by count_per_min desc limit 20;",
                            "disabled": False
                        }
                    },
                    "promQueries": {
                        "A": {
                            "query": "",
                            "disabled": False
                        }
                    },
                    "panelType": "graph",
                    "queryType": "clickhouse_sql"
                },
                "op": "1",
                "target": 1,
                "matchType": "1"
            },
            "labels": {
            },
            "annotations": {
                "description": "This alert is fired when the defined metric (current value: {{$value}}) crosses the threshold ({{$threshold}})",
                "summary": "The rule threshold is set to {{$threshold}}, and the observed metric value is {{$value}}"
            },
            "disabled": False,
            "source": "",
            "preferredChannels": [
            ],
        }
        

        expected_updated_alert = {
            "id": "7",
            "state": "firing",
            "alert": "Test Alert",
            "alertType": "METRIC_BASED_ALERT",
            "ruleType": "threshold_rule",
            "evalWindow": "5m0s",
            "condition": {
                "compositeQuery": {
                    "builderQueries": {
                        "A": {
                            "queryName": "A",
                            "stepInterval": 60,
                            "dataSource": "metrics",
                            "aggregateOperator": "count",
                            "aggregateAttribute": {
                                "key": "",
                                "dataType": "",
                                "type": "",
                                "isColumn": False,
                                "isJSON": False
                            },
                            "filters": {
                                "op": "AND",
                                "items": []
                            },
                            "expression": "A",
                            "disabled": False,
                            "limit": 0,
                            "offset": 0,
                            "pageSize": 0,
                            "reduceTo": "sum"
                        }
                    },
                    "chQueries": {
                        "A": {
                            "query": "select resource_string_service$$name as service, count() as count_per_min from signoz_logs.distributed_logs where timestamp>toUnixTimestamp64Nano(now64() - INTERVAL 1 MINUTE) group by resource_string_service$$name order by count_per_min desc limit 20;",
                            "disabled": False
                        }
                    },
                    "promQueries": {
                        "A": {
                            "query": "",
                            "disabled": False
                        }
                    },
                    "panelType": "graph",
                    "queryType": "clickhouse_sql"
                },
                "op": "1",
                "target": 1,
                "matchType": "1"
            },
            "labels": {
            },
            "annotations": {
                "description": "This alert is fired when the defined metric (current value: {{$value}}) crosses the threshold ({{$threshold}})",
                "summary": "The rule threshold is set to {{$threshold}}, and the observed metric value is {{$value}}"
            },
            "disabled": False,
            "source": "",
            "preferredChannels": [
            ],
        }
        
        resp = update_alert(old_alert, self.fields)
        # print(json.dumps(resp))
        # print(json.dumps(expected_updated_alert))
        equal = sorted(resp.items()) == sorted(expected_updated_alert.items())
        self.assertTrue(equal==True)
        


unittest.main()