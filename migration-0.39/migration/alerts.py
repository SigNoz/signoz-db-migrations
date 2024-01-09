import sqlite3
import json
from clickhouse_driver import Client
import sys
import json
from migration.fields import update_field



def update_alert(json_data, fields):
    try:
        data = json.loads(json_data)
    except json.JSONDecodeError:
        print("Invalid JSON format.")
        return

    query_type = data['condition']['compositeQuery']['queryType']
    if query_type == "clickhouse_sql":
        # print(testing)
        # there will only be one query for ch in alerts
        clickhouse_sql = data['condition']['compositeQuery']['chQueries']['A']
        if "query" not in clickhouse_sql.keys():
            print("Query not found : {} , type: {}".format(data['alert'], query_type))
            print(clickhouse_sql)
        clickhouse_sql =clickhouse_sql["query"]
        
        if "signoz_logs.distributed_logs"  in clickhouse_sql:
            print("Alert : {} , type: {}".format(data['alert'], query_type))
            for old_name, updated_attribute in fields.items():
                # check if attribute is there
                # check is done by checking # attributes_string_key, 'request_context_test_name'
                check = updated_attribute[2] + "_" + updated_attribute[1] + "_key, '" + updated_attribute[0].replace('.', '_') + "'"
                if check in clickhouse_sql:
                    updated = updated_attribute[2] + "_" + updated_attribute[1] + "_key, '" + updated_attribute[0] + "'"
                    clickhouse_sql = clickhouse_sql.replace(check, updated)

                
                # check if materialized column is there if yes then replace it
                materialized_name = updated_attribute[2][:-1] + "_" + updated_attribute[1][:-1] + "_" + updated_attribute[0].replace('.', '_')
                if materialized_name in clickhouse_sql:
                    updated_materialized_name = updated_attribute[2][:-1] + "_" + updated_attribute[1][:-1] + "_" + updated_attribute[0].replace('.', '$$')
                    clickhouse_sql = clickhouse_sql.replace(materialized_name, updated_materialized_name)
                data['condition']['compositeQuery']['chQueries']['A']['query'] = clickhouse_sql 
                      
    elif query_type == "builder":
        groupByNames = {}
        for name, builderQuery in data['condition']['compositeQuery']['builderQueries'].items():
            data_source = builderQuery.get('dataSource', 'No DataSource found')

            ## dont allow alerts which 
            if data_source != "logs" and name == builderQuery["expression"]:
                break
            print("Alert : {} , type: {}".format(data['alert'], query_type))


            if name == builderQuery["expression"]:
                # only for formulas
                # update aggregate attribute
                builderQuery["aggregateAttribute"], updated = update_field(builderQuery["aggregateAttribute"], fields)
                
                # update filters
                for j in range(0, len(builderQuery["filters"]["items"])):
                    builderQuery["filters"]["items"][j]["key"], updated = update_field(builderQuery["filters"]["items"][j]["key"], fields)

                # update group by 
                if "groupBy" in builderQuery.keys():
                    for j in range(0, len(builderQuery["groupBy"])):
                        oldKey = builderQuery["groupBy"][j]["key"]
                        builderQuery["groupBy"][j], updated = update_field(builderQuery["groupBy"][j],fields)
                        if updated:
                            groupByNames[oldKey] = builderQuery["groupBy"][j]["key"]
                
                # update order by
                if "orderBy" in builderQuery.keys():
                    for j in range(0, len(builderQuery["orderBy"])):
                        if builderQuery["orderBy"][j]["columnName"] in groupByNames.keys():
                            builderQuery["orderBy"][j]["columnName"] = groupByNames[builderQuery["orderBy"][j]["columnName"]]

            # for both formulas and queries
            # update the legends
            if "legend" in builderQuery.keys():
                for key, value in groupByNames.items():
                    if r"{{" + key + r"}}" in builderQuery["legend"]:
                        builderQuery["legend"] = builderQuery["legend"].replace(r"{{" + key + r"}}", r"{{" + value + r"}}")

            # update the data
            data['condition']['compositeQuery']['builderQueries'][name] = builderQuery

    return data
                


def updateAlerts(cursor, fields):
    for row in cursor.execute('SELECT id, data FROM rules'):
        update_alert(row[1], fields)
        # print(row)
        # break