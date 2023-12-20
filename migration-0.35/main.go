package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"
)

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

// remove this after sometime
func getDotFields(fields []LogField, tablestatement string) []LogField {
	lookup := map[string]LogField{}
	for _, v := range fields {
		lookup[v.Name+v.DataType] = v
	}

	updatedFields := []LogField{}
	for k, val := range lookup {
		if strings.Contains(k, ".") {
			// only capture ones which has a materialized columns
			if uVal, ok := lookup[strings.ReplaceAll(k, ".", "_")]; ok {
				colname := fmt.Sprintf("%s_%s_%s", strings.ToLower(uVal.Type), strings.ToLower(uVal.DataType), uVal.Name)
				if hasMaterializedColumn(tablestatement, colname, uVal.DataType) {
					updatedFields = append(updatedFields, val)
				}
			}

		}
	}
	return updatedFields
}

func GetFields(conn clickhouse.Conn) ([]LogField, error) {
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

	statement, err := GetTableStatement(conn)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while table statement. Err=%v", err))
		return nil, err
	}

	attributes = getDotFields(attributes, statement)
	resources = getDotFields(resources, statement)

	attributes = append(attributes, resources...)

	return attributes, nil
}

func GetTableStatement(conn clickhouse.Conn) (string, error) {
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

func TruncateTagAttributes(conn clickhouse.Conn) error {
	query := "truncate table signoz_logs.tag_attributes on cluster cluster;"
	err := conn.Exec(context.Background(), query)
	if err != nil {
		zap.S().Error(fmt.Errorf("error truncating tag attributes table,  Err=%v", err))
		return err
	}
	return nil
}

func hasMaterializedColumn(tableStatement, field, dataType string) bool {
	regex := fmt.Sprintf("`%s` (?i)(%s) MATERIALIZED", field, dataType)
	res, err := regexp.MatchString(regex, tableStatement)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while matching regex. Err=%v", err))
		return false
	}

	if !res {
		// try checking for default as well
		regex := fmt.Sprintf("`%s` (?i)(%s) DEFAULT", field, dataType)
		res, err = regexp.MatchString(regex, tableStatement)
		if err != nil {
			zap.S().Error(fmt.Errorf("error while matching regex. Err=%v", err))
			return false
		}
	}
	return res
}

func addMaterializedColumnsAndAddIndex(conn clickhouse.Conn, fields []LogField) error {
	ctx := context.Background()
	for _, field := range fields {
		// columns name is <type>_<name>_<datatype>
		colname := fmt.Sprintf("%s_%s_%s", strings.ToLower(field.Type), strings.ToLower(field.DataType), strings.ReplaceAll(field.Name, ".", "$$"))
		keyColName := fmt.Sprintf("%s_%s_key", field.Type+"s", strings.ToLower(field.DataType))
		valueColName := fmt.Sprintf("%s_%s_value", field.Type+"s", strings.ToLower(field.DataType))

		// create column in logs table
		for _, table := range []string{"logs", "distributed_logs"} {
			zap.S().Info(fmt.Sprintf("creating materialized for: %s i.e %s", field.Name, colname))
			query := fmt.Sprintf("ALTER TABLE signoz_logs.%s on cluster cluster ADD COLUMN IF NOT EXISTS %s %s DEFAULT %s[indexOf(%s, '%s')] CODEC(ZSTD(1))",
				table, colname, field.DataType, valueColName, keyColName, field.Name)
			err := conn.Exec(context.Background(), query)
			if err != nil {
				zap.S().Error(fmt.Errorf("error while creating materialized column on logs table. Err=%v", err))
			}

			query = fmt.Sprintf("ALTER TABLE signoz_logs.%s ON CLUSTER cluster ADD COLUMN IF NOT EXISTS %s_exists bool DEFAULT if(indexOf(%s, '%s') != 0, true, false) CODEC(ZSTD(1))",
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
		query := fmt.Sprintf("ALTER TABLE signoz_logs.logs on cluster cluster ADD INDEX IF NOT EXISTS %s_idx (%s) TYPE bloom_filter(0.01) GRANULARITY 64", colname, colname)
		err := conn.Exec(context.Background(), query)
		if err != nil {
			zap.S().Error(fmt.Errorf("error while renaming index. Err=%v", err))
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

	fields, err := GetFields(conn)
	if err != nil {
		zap.S().Fatal("Error while getting fields", zap.Error(err))
		os.Exit(1)
	}

	err = addMaterializedColumnsAndAddIndex(conn, fields)
	if err != nil {
		zap.S().Fatal("Error while renaming materialized columns", zap.Error(err))
		os.Exit(1)

	}

	// truncate tag attributes
	err = TruncateTagAttributes(conn)
	if err != nil {
		zap.S().Fatal("Error truncating tag attributes", zap.Error(err))
		os.Exit(1)

	}

	zap.S().Info(fmt.Sprintf("Completed migration in: %s", time.Since(start)))
}
