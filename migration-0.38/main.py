import argparse
from migration.alerts import updateAlerts
from migration.dashboards import updateDashboards
from migration.fields import getFields
from clickhouse_driver import Client
import sqlite3

parser = argparse.ArgumentParser()

parser.add_argument("-host", "--host", default="127.0.0.1", help="Clickhouse host")
parser.add_argument("-port", "--port", default="9000", help="Clickhouse port")
parser.add_argument("-user", "--user", default="default", help="Clickhouse username")
parser.add_argument("-password", "--password", default="", help="Clickhouse Password")
parser.add_argument("--data_source","--data_source",default ="db", help = "Data Source path of sqlite db")
args = parser.parse_args()

if __name__ == "__main__":
    client = Client(host=args.host, port=args.port, user=args.user, password=args.password)
    fields = getFields(client)
    if len(fields) == 0:
        print("Nothing to migrate....")
        exit(0)
    print("fields from clickhouse -------------------------------------")
    print(fields)
    print("------------------------------------------------------------\n\n")
    con = sqlite3.connect(args.data_source)
    print("updating dashboards ----------------------------------------")
    updateDashboards(con,fields)
    print("\n\nupdating alerts ----------------------------------------")
    updateAlerts(con,fields)
    print("\n\ncommiting changes --------------------------------------")
    con.commit()
    con.close()