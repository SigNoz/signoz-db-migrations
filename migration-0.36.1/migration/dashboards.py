import sqlite3
import json
from clickhouse_driver import Client
import sys
import json
from migration.fields import update_field

def update_dashboard(data, fields):
    for index in range(len(data['widgets'])):
        widget = data['widgets'][index]
        if 'query' not in widget or 'builder' not in widget['query']:
            print(f"Skipping widget with ID {widget.get('id')}. Missing 'query' or 'builder' key.")
            continue

        query_type = widget['query']['queryType']
        if query_type == "clickhouse_sql":
            for i in range(len(widget["query"]["clickhouse_sql"])):
                sql = widget["query"]["clickhouse_sql"][i]
                
                if "query" not in sql.keys():
                    # print("query not found for Dashboard : {} , panel: {}, type: {}".format(data['title'], widget['title'], query_type))
                    continue
                
                if "signoz_logs.distributed_logs"  not in sql["query"]:
                    continue

                print("Dashboard : {} , panel: {}, type: {}".format(data['title'], widget['title'], query_type))
                for old_name, updated_attribute in fields.items():
                    # check if attribute is there
                    # check is done by checking # attributes_string_key, 'request_context_test_name'
                    check = updated_attribute[2] + "_" + updated_attribute[1].lower() + "_key, '" + updated_attribute[0].replace('.', '_') + "'"

                    if check in sql["query"]:
                        updated = updated_attribute[2] + "_" + updated_attribute[1].lower() + "_key, '" + updated_attribute[0] + "'"
                        sql["query"] = sql["query"].replace(check, updated)

                    
                    # check if materialized column is there if yes then replace it
                    materialized_name = updated_attribute[2][:-1] + "_" + updated_attribute[1].lower() + "_" + updated_attribute[0].replace('.', '_')
                    if materialized_name in sql["query"]:
                        updated_materialized_name =  updated_attribute[2][:-1] + "_" + updated_attribute[1].lower() + "_" + updated_attribute[0].replace('.', '$$')
                        sql["query"] = sql["query"].replace(materialized_name, updated_materialized_name) 
        elif query_type == "builder":
            groupByNames = {}
            for i  in range(len(widget['query']['builder']['queryData'])):
                query_data = widget['query']['builder']['queryData'][i]
                data_source = query_data.get('dataSource', 'No DataSource found')
                if data_source != "logs":
                    continue
                print("Dashboard : {} , panel: {}, type: {}".format(data['title'], widget['title'], query_type))

                # update aggregate attribute
                query_data["aggregateAttribute"], updated = update_field(query_data["aggregateAttribute"], fields)
                
                # update filters
                for j in range(0, len(query_data["filters"]["items"])):
                    query_data["filters"]["items"][j]["key"], updated = update_field(query_data["filters"]["items"][j]["key"], fields)

                # update group by 
                for j in range(0, len(query_data["groupBy"])):
                    oldKey = query_data["groupBy"][j]["key"]
                    query_data["groupBy"][j], updated = update_field(query_data["groupBy"][j],fields)
                    if updated:
                        groupByNames[oldKey] = query_data["groupBy"][j]["key"]
                
                # update order by
                for j in range(0, len(query_data["orderBy"])):
                    if query_data["orderBy"][j]["columnName"] in groupByNames.keys():
                        query_data["orderBy"][j]["columnName"] = groupByNames[query_data["orderBy"][j]["columnName"]]

                # update the legends
                for key, value in groupByNames.items():
                    if r"{{" + key + r"}}" in query_data["legend"]:
                        query_data["legend"] = query_data["legend"].replace(r"{{" + key + r"}}", r"{{" + value + r"}}")
                # update the data
                widget['query']['builder']['queryData'][i] = query_data

            for i  in range(len(widget['query']['builder']['queryFormulas'])):
                query_formula = widget['query']['builder']['queryFormulas'][i]
                for key, value in groupByNames.items():
                    if r"{{" + key + r"}}" in query_formula["legend"]:
                        query_data["legend"] = query_formula["legend"].replace(r"{{" + key + r"}}", r"{{" + value + r"}}")
                
                # update the data
                widget['query']['builder']['queryFormulas'][i] = query_formula
        data['widgets'][index] = widget
    return data

def update_db(conn, id, dashboard):
    cursor = conn.cursor()
    q = """UPDATE dashboards SET data = ? WHERE id = ?"""
    data = json.dumps(dashboard).encode('utf-8')
    x= cursor.execute(q, (data, id))
    conn.commit()
    cursor.close()


def updateDashboards(conn, fields):
    # The result of a "cursor.execute" can be iterated over by row
    cursor = conn.cursor()
    for row in cursor.execute('SELECT id, uuid, data FROM dashboards'):
        json_str = row[2].decode('utf-8')
        try:
            data = json.loads(json_str)
        except json.JSONDecodeError:
            print("Invalid JSON format.")
            return
        if 'widgets' not in data:
            print("Invalid JSON structure. Missing 'data' or 'widgets' key.")
            return
        dashboard = update_dashboard(data, fields)
        update_db(conn, row[0], dashboard)
    # Be sure to close the connection
    cursor.close()