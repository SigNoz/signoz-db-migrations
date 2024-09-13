package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"
)

type LogField struct {
	Name      string `json:"name" ch:"name"`
	DataType  string `json:"dataType" ch:"datatype"`
	Type      string `json:"type"`
	IndexType string `json:"indexType"`
}

type ShowCreateTableStatement struct {
	Statement string `json:"statement" ch:"statement"`
}

const (
	attribute = "attribute"
	resource  = "resource"
)

type TTLResp struct {
	EngineFull string `ch:"engine_full"`
}

// Connection for clickhouse
func connect(host string, port string, userName string, password string) (clickhouse.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%s", host, port)},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: userName,
				Password: password,
			},
			//Debug:           true,
		})
	)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			zap.S().Info(fmt.Sprintf("Catch exception [%d] %s \n%s", exception.Code, exception.Message, exception.StackTrace))
		}
		return nil, err
	}
	return conn, nil
}

// in the new schema we are completely removing support for old underscore attributes
// so we are not copying them
func removeUnderscoreFields(fields []LogField, tableStatement string) []LogField {
	lookup := map[string]LogField{}
	for _, v := range fields {
		lookup[v.Name+v.DataType] = v
	}

	// if the corresponding underscore attribute exists then remove it from lookup
	for k, _ := range lookup {
		if strings.Contains(k, ".") {
			if _, ok := lookup[strings.ReplaceAll(k, ".", "_")]; ok {
				delete(lookup, strings.ReplaceAll(k, ".", "_"))
			}
		}
	}

	// check if the fields are materialized and only return the materialized ones
	newFields := []LogField{}
	for _, val := range lookup {
		colname := fmt.Sprintf("%s_%s_%s", strings.ToLower(val.Type), strings.ToLower(val.DataType), strings.ReplaceAll(val.Name, ".", "\\$\\$"))
		if hasMaterializedColumn(tableStatement, colname, strings.ToLower(val.DataType)) {
			newFields = append(newFields, val)
		}
	}

	return newFields
}

// return all fields which needs to add to new schema
func GetSelectedFields(conn clickhouse.Conn) ([]LogField, error) {
	ctx := context.Background()
	// get attribute keys
	attributes := []LogField{}
	query := "SELECT DISTINCT name, datatype from signoz_logs.distributed_logs_attribute_keys group by name, datatype"
	err := conn.Select(ctx, &attributes, query)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while getting attribute keys. Err=%v", err))
		return nil, err
	}

	// get resource keys
	resources := []LogField{}
	query = "SELECT DISTINCT name, datatype from signoz_logs.distributed_logs_resource_keys group by name, datatype"
	err = conn.Select(ctx, &resources, query)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while getting resource keys. Err=%v", err))
		return nil, err
	}

	for i := 0; i < len(attributes); i++ {
		attributes[i].Type = attribute
	}

	for i := 0; i < len(resources); i++ {
		resources[i].Type = resource
	}

	statement, err := getTableStatement(conn)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while table statement. Err=%v", err))
		return nil, err
	}

	attributes = removeUnderscoreFields(attributes, statement)
	resources = removeUnderscoreFields(resources, statement)

	attributes = append(attributes, resources...)

	return attributes, nil
}

func getTableStatement(conn clickhouse.Conn) (string, error) {
	ctx := context.Background()
	statements := []ShowCreateTableStatement{}
	query := "SHOW CREATE TABLE signoz_logs.logs"
	err := conn.Select(ctx, &statements, query)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while getting logs table statement. Err=%v", err))
		return "", err
	}
	return statements[0].Statement, nil
}

func hasMaterializedColumn(tableStatement, field, dataType string) bool {
	// check the type as well
	regex := fmt.Sprintf("`%s` (?i)(%s) DEFAULT", field, dataType)

	res, err := regexp.MatchString(regex, tableStatement)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while matching regex. Err=%v", err))
		return false
	}
	return res
}

func getClickhouseLogsColumnDataType(dataType string) string {
	dataType = strings.ToLower(dataType)
	if dataType == "int64" || dataType == "float64" {
		return "number"
	}
	if dataType == "bool" {
		return "bool"
	}
	return "string"
}

func addMaterializedColumnsAndAddIndex(conn clickhouse.Conn, fields []LogField) error {
	ctx := context.Background()
	for _, field := range fields {
		// columns name is <type>_<name>_<datatype>
		colname := fmt.Sprintf("%s_%s_%s", strings.ToLower(field.Type), getClickhouseLogsColumnDataType(field.DataType), strings.ReplaceAll(field.Name, ".", "$$"))
		keyColName := fmt.Sprintf("%s_%s", field.Type+"s", getClickhouseLogsColumnDataType(field.DataType))

		// create column in logs table
		for _, table := range []string{"logs_v2", "distributed_logs_v2"} {
			zap.S().Info(fmt.Sprintf("creating materialized for: %s i.e %s", field.Name, colname))
			query := fmt.Sprintf("ALTER TABLE signoz_logs.%s on cluster cluster ADD COLUMN IF NOT EXISTS %s %s DEFAULT %s['%s'] CODEC(ZSTD(1))",
				table, colname, field.DataType, keyColName, field.Name)
			err := conn.Exec(context.Background(), query)
			if err != nil {
				zap.S().Error(fmt.Errorf("error while creating materialized column on logs table. Err=%v", err))
			}

			query = fmt.Sprintf("ALTER TABLE signoz_logs.%s ON CLUSTER cluster ADD COLUMN IF NOT EXISTS %s_exists bool DEFAULT if(mapContains(%s, '%s') != 0, true, false) CODEC(ZSTD(1))",
				table,
				colname,
				keyColName,
				field.Name,
			)
			err = conn.Exec(ctx, query)
			if err != nil {
				zap.S().Error(fmt.Errorf("error while creating exists column on logs table. Err=%v", err))
				return err
			}
		}

		// index not required for bool attributes as the cardinality is only two
		if strings.ToLower(field.DataType) == "bool" {
			continue
		}

		zap.S().Info(fmt.Sprintf("Create index: %s_idx", colname))
		query := fmt.Sprintf("ALTER TABLE signoz_logs.logs_v2 on cluster cluster ADD INDEX IF NOT EXISTS %s_idx (%s) TYPE bloom_filter(0.01) GRANULARITY 64", colname, colname)
		err := conn.Exec(context.Background(), query)
		if err != nil {
			zap.S().Error(fmt.Errorf("error while renaming index. Err=%v", err))
			return err
		}

	}
	return nil
}

func parseTTL(queryResp string) (delTTL int, moveTTL int, coldStorageName string, err error) {
	zap.L().Info("Parsing TTL from: ", zap.String("queryResp", queryResp))
	deleteTTLExp := regexp.MustCompile(`toIntervalSecond\(([0-9]*)\)`)
	moveTTLExp := regexp.MustCompile(`toIntervalSecond\(([0-9]*)\) TO VOLUME '(?P<name>\S+)'`)

	delTTL, moveTTL = -1, -1

	m := deleteTTLExp.FindStringSubmatch(queryResp)
	if len(m) > 1 {
		seconds_int, err := strconv.Atoi(m[1])
		if err != nil {
			return -1, -1, coldStorageName, err
		}
		delTTL = seconds_int
	}

	m = moveTTLExp.FindStringSubmatch(queryResp)
	if len(m) == 3 {
		seconds_int, err := strconv.Atoi(m[1])
		if err != nil {
			return -1, -1, coldStorageName, err
		}
		moveTTL = seconds_int

		coldStorageName = m[2]
	}

	return delTTL, moveTTL, coldStorageName, nil
}

func GetTTL(conn clickhouse.Conn, tableName string) (int, int, string, error) {
	query := fmt.Sprintf("SELECT engine_full FROM system.tables where name='%s'", tableName)

	ctx := context.Background()
	resp := []TTLResp{}
	err := conn.Select(ctx, &resp, query)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while getting logs table statement. Err=%v", err))
		return -1, -1, "", err
	}
	if len(resp) > 0 {
		return parseTTL(resp[0].EngineFull)
	}

	zap.S().Error(fmt.Sprintln("No TTL found for table: ", tableName))
	return -1, -1, "", nil
}

func updateTTL(conn clickhouse.Conn, delTTL, moveTTL int, coldVolume string) error {
	if delTTL != -1 {
		q := "ALTER TABLE signoz_logs.logs_v2 ON CLUSTER cluster MODIFY TTL toDateTime(timestamp / 1000000000) + " +
			fmt.Sprintf("INTERVAL %v SECOND DELETE", delTTL)
		qRes := "ALTER TABLE signoz_logs.logs_v2_resource ON CLUSTER cluster MODIFY TTL toDateTime(seen_at_ts_bucket_start) + toIntervalSecond(1800) + " +
			fmt.Sprintf("INTERVAL %v SECOND DELETE", delTTL)

		if moveTTL != -1 {
			q += fmt.Sprintf(", toDateTime(timestamp / 1000000000) + INTERVAL %v SECOND TO VOLUME '%s'", moveTTL, coldVolume)
			qRes += fmt.Sprintf(", toDateTime(seen_at_ts_bucket_start) + toIntervalSecond(1800) + INTERVAL %v SECOND TO VOLUME '%s'", moveTTL, coldVolume)

			for _, table := range []string{"logs_v2", "logs_v2_resource"} {
				policyReq := fmt.Sprintf("ALTER TABLE signoz_logs.%s ON CLUSTER cluster MODIFY SETTING storage_policy='tiered'", table)
				err := conn.Exec(context.Background(), policyReq)
				if err != nil {
					zap.S().Error(fmt.Errorf("error while setting storage policy. Err=%v", err))
					return err
				}
			}
		}

		q += " SETTINGS distributed_ddl_task_timeout = -1"
		qRes += " SETTINGS distributed_ddl_task_timeout = -1"

		err := conn.Exec(context.Background(), q)
		if err != nil {
			zap.S().Error(fmt.Errorf("error while updating ttl for table. Err=%v", err))
			return err
		}
		err = conn.Exec(context.Background(), qRes)
		if err != nil {
			zap.S().Error(fmt.Errorf("error while updating ttl for table. Err=%v", err))
			return err
		}
	}
	return nil
}

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	undo := zap.ReplaceGlobals(logger)
	defer undo()

	start := time.Now()
	hostFlag := flag.String("host", "127.0.0.1", "clickhouse host")
	portFlag := flag.String("port", "9000", "clickhouse port")
	userNameFlag := flag.String("userName", "default", "clickhouse username")
	passwordFlag := flag.String("password", "", "clickhouse password")
	flag.Parse()
	zap.S().Debug(fmt.Sprintf("Params: %s %s %s", *hostFlag, *portFlag, *userNameFlag))

	conn, err := connect(*hostFlag, *portFlag, *userNameFlag, *passwordFlag)
	if err != nil {
		zap.S().Fatal("Error while connecting to clickhouse", zap.Error(err))
	}

	// get the fields to be added to new schema
	fields, err := GetSelectedFields(conn)
	if err != nil {
		zap.S().Fatal("Error while getting fields", zap.Error(err))
		os.Exit(1)
	}

	// add the fields to new schema
	err = addMaterializedColumnsAndAddIndex(conn, fields)
	if err != nil {
		zap.S().Fatal("Error while renaming materialized columns", zap.Error(err))
		os.Exit(1)

	}

	// get the ttl of the old table
	delOld, moveOld, oldColdStorageName, err := GetTTL(conn, "logs")
	if err != nil {
		zap.S().Error(err.Error())
	}

	// get the ttl of the current table
	delCurr, moveCurr, _, err := GetTTL(conn, "logs_v2")
	if err != nil {
		zap.S().Error(err.Error())
	}

	// if current table cold storage is configured don't change it.
	if moveCurr != -1 {
		zap.S().Info("Since cold storage is already set not changing it")
	}

	// if ttl is same don't change it
	if delOld == delCurr && moveOld == moveCurr {
		zap.S().Info("no change is required, TTL is same")
	}

	zap.S().Info(fmt.Sprintf("Setting TTL values: delete:%v, move: %v, coldStorage: %s", delOld, moveOld, oldColdStorageName))
	// update the TTL
	err = updateTTL(conn, delOld, moveOld, oldColdStorageName)
	if err != nil {
		zap.S().Error(err.Error())
	}
	zap.S().Info(fmt.Sprintf("Completed migration in: %s", time.Since(start)))
}
