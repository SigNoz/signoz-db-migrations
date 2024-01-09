import unittest
import json
from .dashboards import update_dashboard

class TestUpdateDashboard(unittest.TestCase):
    fields = {
        "attributes_float64_extra_info_latency": ['extra_info.latency', 'float64', 'attributes'],
        "attributes_string_service_name": ['service.name', 'string', 'attributes'],
        "attributes_string_extra_info_url_route": ['extra_info.url_route', 'string', 'attributes'],
        "attributes_string_request_context_test_name": ['request_context.test_name', 'String', 'attributes']
    }
    def test_update_dashboard_builder_query(self):
        old_dashboard = {
            "title": "title 1",
            "widgets": [
            {
                    "description": "",
                    "id": "4f9bfc33-4d32-42c1-80c3-18a8756d5ad4",
                    "isStacked": False,
                    "nullZeroValues": "zero",
                    "opacity": "1",
                    "panelTypes": "graph",
                    "query": {
                        "builder": {
                            "queryData": [
                                {
                                    "aggregateAttribute": {
                                        "dataType": "float64",
                                        "id": "extra_info_latency--float64--tag--false",
                                        "isColumn": False,
                                        "isJSON": False,
                                        "key": "extra_info_latency",
                                        "type": "tag"
                                    },
                                    "aggregateOperator": "avg",
                                    "dataSource": "logs",
                                    "disabled": False,
                                    "expression": "A",
                                    "filters": {
                                        "items": [
                                            {
                                                "id": "d423dcc3",
                                                "key": {
                                                    "dataType": "string",
                                                    "id": "service_name--string--tag--true",
                                                    "isColumn": True,
                                                    "isJSON": False,
                                                    "key": "service_name",
                                                    "type": "tag"
                                                },
                                                "op": "=",
                                                "value": "search_service"
                                            },
                                        ],
                                        "op": "AND"
                                    },
                                    "groupBy": [
                                        {
                                            "dataType": "string",
                                            "id": "extra_info_url_route--string--tag--false",
                                            "isColumn": False,
                                            "isJSON": False,
                                            "key": "extra_info_url_route",
                                            "type": "tag"
                                        }
                                    ],
                                    "having": [],
                                    "legend": "{{extra_info_url_route}}",
                                    "limit": None,
                                    "orderBy": [
                                        {
                                            "columnName": "#SIGNOZ_VALUE",
                                            "order": "desc"
                                        }
                                    ],
                                    "queryName": "A",
                                    "reduceTo": "sum",
                                    "stepInterval": 240
                                }
                            ],
                            "queryFormulas": []
                        },
                        "clickhouse_sql": [
                            {
                                "disabled": False,
                                "legend": "",
                                "name": "A",
                                "query": ""
                            }
                        ],
                        "id": "ad35f727-095a-42ad-a949-fae60a381d81",
                        "promql": [
                            {
                                "disabled": False,
                                "legend": "",
                                "name": "A",
                                "query": ""
                            }
                        ],
                        "queryType": "builder"
                    },
                    "thresholds": [],
                    "timePreferance": "GLOBAL_TIME",
                    "title": "Average Latency",
                    "yAxisUnit": "none"
                }
            ]
        }

        expected_updated_dashboard = {
            "title": "title 1",
            "widgets": [
            {
                    "description": "",
                    "id": "4f9bfc33-4d32-42c1-80c3-18a8756d5ad4",
                    "isStacked": False,
                    "nullZeroValues": "zero",
                    "opacity": "1",
                    "panelTypes": "graph",
                    "query": {
                        "builder": {
                            "queryData": [
                                {
                                    "aggregateAttribute": {
                                        "dataType": "float64",
                                        "id": "extra_info_latency--float64--tag--false",
                                        "isColumn": False,
                                        "isJSON": False,
                                        "key": "extra_info.latency",
                                        "type": "tag"
                                    },
                                    "aggregateOperator": "avg",
                                    "dataSource": "logs",
                                    "disabled": False,
                                    "expression": "A",
                                    "filters": {
                                        "items": [
                                            {
                                                "id": "d423dcc3",
                                                "key": {
                                                    "dataType": "string",
                                                    "id": "service_name--string--tag--true",
                                                    "isColumn": True,
                                                    "isJSON": False,
                                                    "key": "service.name",
                                                    "type": "tag"
                                                },
                                                "op": "=",
                                                "value": "search_service"
                                            },
                                        ],
                                        "op": "AND"
                                    },
                                    "groupBy": [
                                        {
                                            "dataType": "string",
                                            "id": "extra_info_url_route--string--tag--false",
                                            "isColumn": False,
                                            "isJSON": False,
                                            "key": "extra_info.url_route",
                                            "type": "tag"
                                        }
                                    ],
                                    "having": [],
                                    "legend": "{{extra_info.url_route}}",
                                    "limit": None,
                                    "orderBy": [
                                        {
                                            "columnName": "#SIGNOZ_VALUE",
                                            "order": "desc"
                                        }
                                    ],
                                    "queryName": "A",
                                    "reduceTo": "sum",
                                    "stepInterval": 240
                                }
                            ],
                            "queryFormulas": []
                        },
                        "clickhouse_sql": [
                            {
                                "disabled": False,
                                "legend": "",
                                "name": "A",
                                "query": ""
                            }
                        ],
                        "id": "ad35f727-095a-42ad-a949-fae60a381d81",
                        "promql": [
                            {
                                "disabled": False,
                                "legend": "",
                                "name": "A",
                                "query": ""
                            }
                        ],
                        "queryType": "builder"
                    },
                    "thresholds": [],
                    "timePreferance": "GLOBAL_TIME",
                    "title": "Average Latency",
                    "yAxisUnit": "none"
                }
            ]
        }

        resp = update_dashboard(old_dashboard, self.fields)
        equal = sorted(resp.items()) == sorted(expected_updated_dashboard.items())
        self.assertTrue(equal==True)
    
    def test_update_dashboard_SQL_query(self):
        old_dashboard = {
            "title": "clickhouse sql",
            "widgets": [
                {
                    "description": "",
                    "id": "39456fe3-1143-4f4a-92e1-e145869ecc21",
                    "isStacked": False,
                    "nullZeroValues": "zero",
                    "opacity": "1",
                    "panelTypes": "graph",
                    "query": {
                        "builder": {
                        },
                        "clickhouse_sql": [
                            {
                                "disabled": False,
                                "legend": "{{request_context.test_name}}",
                                "name": "A",
                                "query": "\nSELECT A.request_context_test_name as request_context_test_name, A.ts as ts, A.value / B.value * 100 as value FROM (\nSELECT toStartOfInterval(fromUnixTimestamp64Nano(timestamp), INTERVAL 240 SECOND) AS ts, \n    attributes_string_value[indexOf(attributes_string_key, 'request_context_test_name')] as request_context_test_name, \n    toFloat64(count(*)) as value from signoz_logs.distributed_logs \n    where \n        (timestamp \u003e toUnixTimestamp64Nano(now64() - INTERVAL 7 DAY)) AND \n        attribute_string_component = 'search_service' AND \n        has(JSONExtract(JSON_QUERY(body, '$.request_context.source[*]'), 'Array(String)'), 'Postman') AND \n        body ILIKE '%COMPLETED run%' AND JSONExtract(JSON_VALUE(body, '$.extra_info.response_code'), 'Int64') = 200 AND \n        indexOf(attributes_string_key, 'request_context_test_name') \u003e 0 AND\n        attribute_string_env = {{.env}}\n    group by \n        request_context_test_name,ts order by value DESC) as A\nINNER JOIN(\nSELECT toStartOfInterval(fromUnixTimestamp64Nano(timestamp), INTERVAL 240 SECOND) AS ts, \n    attributes_string_value[indexOf(attributes_string_key, 'request_context_test_name')] as request_context_test_name, \n    toFloat64(count(*)) as value from signoz_logs.distributed_logs \n    where \n        (timestamp \u003e toUnixTimestamp64Nano(now64() - INTERVAL 7 DAY)) AND \n        attribute_string_component = 'search_service' AND \n        has(JSONExtract(JSON_QUERY(body, '$.request_context.source[*]'), 'Array(String)'), 'Postman') AND \n        body ILIKE '%COMPLETED run%' AND \n        indexOf(attributes_string_key, 'request_context_test_name') \u003e 0 AND\n        attribute_string_env = {{.env}}\n    group by request_context_test_name,ts order by value DESC) as B\n ON A.ts = B.ts\n"
                            }
                        ],
                        "id": "787e8fc0-2f1d-4e34-b2a9-b708573ab54b",
                        "promql": [
                        ],
                        "queryType": "clickhouse_sql"
                    },
                    "thresholds": [],
                    "timePreferance": "GLOBAL_TIME",
                    "title": "SuccessRate",
                    "yAxisUnit": "none"
                }
            ]
        }

        expected_updated_dashboard = {
            "title": "clickhouse sql",
            "widgets": [
                {
                    "description": "",
                    "id": "39456fe3-1143-4f4a-92e1-e145869ecc21",
                    "isStacked": False,
                    "nullZeroValues": "zero",
                    "opacity": "1",
                    "panelTypes": "graph",
                    "query": {
                        "builder": {
                        },
                        "clickhouse_sql": [
                            {
                                "disabled": False,
                                "legend": "{{request_context.test_name}}",
                                "name": "A",
                                "query": "\nSELECT A.request_context_test_name as request_context_test_name, A.ts as ts, A.value / B.value * 100 as value FROM (\nSELECT toStartOfInterval(fromUnixTimestamp64Nano(timestamp), INTERVAL 240 SECOND) AS ts, \n    attributes_string_value[indexOf(attributes_string_key, 'request_context.test_name')] as request_context_test_name, \n    toFloat64(count(*)) as value from signoz_logs.distributed_logs \n    where \n        (timestamp \u003e toUnixTimestamp64Nano(now64() - INTERVAL 7 DAY)) AND \n        attribute_string_component = 'search_service' AND \n        has(JSONExtract(JSON_QUERY(body, '$.request_context.source[*]'), 'Array(String)'), 'Postman') AND \n        body ILIKE '%COMPLETED run%' AND JSONExtract(JSON_VALUE(body, '$.extra_info.response_code'), 'Int64') = 200 AND \n        indexOf(attributes_string_key, 'request_context.test_name') \u003e 0 AND\n        attribute_string_env = {{.env}}\n    group by \n        request_context_test_name,ts order by value DESC) as A\nINNER JOIN(\nSELECT toStartOfInterval(fromUnixTimestamp64Nano(timestamp), INTERVAL 240 SECOND) AS ts, \n    attributes_string_value[indexOf(attributes_string_key, 'request_context.test_name')] as request_context_test_name, \n    toFloat64(count(*)) as value from signoz_logs.distributed_logs \n    where \n        (timestamp \u003e toUnixTimestamp64Nano(now64() - INTERVAL 7 DAY)) AND \n        attribute_string_component = 'search_service' AND \n        has(JSONExtract(JSON_QUERY(body, '$.request_context.source[*]'), 'Array(String)'), 'Postman') AND \n        body ILIKE '%COMPLETED run%' AND \n        indexOf(attributes_string_key, 'request_context.test_name') \u003e 0 AND\n        attribute_string_env = {{.env}}\n    group by request_context_test_name,ts order by value DESC) as B\n ON A.ts = B.ts\n"
                            }
                        ],
                        "id": "787e8fc0-2f1d-4e34-b2a9-b708573ab54b",
                        "promql": [
                        ],
                        "queryType": "clickhouse_sql"
                    },
                    "thresholds": [],
                    "timePreferance": "GLOBAL_TIME",
                    "title": "SuccessRate",
                    "yAxisUnit": "none"
                }
            ]
        }
        resp = update_dashboard(old_dashboard, self.fields)
        equal = sorted(resp.items()) == sorted(expected_updated_dashboard.items())
        self.assertTrue(equal==True)
        


unittest.main()