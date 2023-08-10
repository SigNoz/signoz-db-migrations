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
	Name     string `json:"name" ch:"name"`
	DataType string `json:"dataType" ch:"datatype"`
	Type     string `json:"type"`
}

type ShowCreateTableStatement struct {
	Statement string `json:"statement" ch:"statement"`
}

const (
	attribute = "attribute"
	resource  = "resource"
)

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

func hasIndex(tableStatement, field string) bool {
	return strings.Contains(tableStatement, fmt.Sprintf("INDEX %s_idx", field))
}

func hasMaterializedColumn(tableStatement, field, dataType string) bool {
	// check the type as well
	regex := fmt.Sprintf("`%s` (?i)(%s) MATERIALIZED", field, dataType)
	res, err := regexp.MatchString(regex, tableStatement)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while matching regex. Err=%v", err))
		return false
	}
	return res
}

var topLevel = map[string]struct{}{
	"timestamp":       {},
	"id":              {},
	"trace_id":        {},
	"span_id":         {},
	"trace_flags":     {},
	"severity_text":   {},
	"severity_number": {},
	"body":            {},
}

func isTopLevel(name string) bool {
	if _, ok := topLevel[name]; ok {
		return true
	}
	return false
}

func removeIndexes(conn clickhouse.Conn, fields []LogField, tableStatement string) error {
	for _, field := range fields {
		if isTopLevel(field.Name) {
			// ignore for top level fields
			continue
		}
		if hasIndex(tableStatement, field.Name) {
			// drop the index
			zap.S().Info(fmt.Sprintf("Dropping index: %s_idx", field.Name))
			query := fmt.Sprintf("ALTER TABLE signoz_logs.logs on cluster cluster DROP INDEX IF EXISTS %s_idx ", field.Name)
			err := conn.Exec(context.Background(), query)
			if err != nil {
				zap.S().Error(fmt.Errorf("error while dropping index. Err=%v", err))
				return err
			}
		}
	}
	return nil
}

func renameMaterializedColumnsAndAddIndex(conn clickhouse.Conn, fields []LogField, tableStatement string) error {
	ctx := context.Background()
	for _, field := range fields {
		if isTopLevel(field.Name) {
			// ignore for top level fields
			continue
		}
		if hasMaterializedColumn(tableStatement, field.Name, field.DataType) {
			// columns name is <type>_<name>_<datatype>
			colname := fmt.Sprintf("%s_%s_%s", strings.ToLower(field.Type), strings.ToLower(field.DataType), field.Name)

			// rename in logs table
			zap.S().Info(fmt.Sprintf("Renaming materialized column: %s to %s", field.Name, colname))
			query := fmt.Sprintf("ALTER TABLE signoz_logs.logs on cluster cluster RENAME COLUMN IF EXISTS %s TO %s ", field.Name, colname)
			err := conn.Exec(context.Background(), query)
			if err != nil {
				zap.S().Error(fmt.Errorf("error while renaming materialized column on logs table. Err=%v", err))
			}

			// rename in distributed logs table
			query = fmt.Sprintf("ALTER TABLE signoz_logs.distributed_logs RENAME COLUMN IF EXISTS %s TO %s", field.Name, colname)
			err = conn.Exec(context.Background(), query)
			if err != nil {
				zap.S().Error(fmt.Errorf("error while renaming materialized column on distributed logs table. Err=%v", err))
				return err
			}

			// create exists column
			keyColName := fmt.Sprintf("%s_%s_key", field.Type+"s", strings.ToLower(field.DataType))
			query = fmt.Sprintf("ALTER TABLE signoz_logs.logs ON CLUSTER cluster ADD COLUMN IF NOT EXISTS %s_exists bool MATERIALIZED if(indexOf(%s, '%s') != 0, true, false) CODEC(LZ4)",
				colname,
				keyColName,
				field.Name,
			)
			err = conn.Exec(ctx, query)
			if err != nil {
				zap.S().Error(fmt.Errorf("error while creating exists column on logs table. Err=%v", err))
				return err
			}

			query = fmt.Sprintf("ALTER TABLE signoz_logs.distributed_logs ON CLUSTER cluster ADD COLUMN IF NOT EXISTS %s_exists bool",
				colname,
			)
			err = conn.Exec(ctx, query)
			if err != nil {
				zap.S().Error(fmt.Errorf("error while creating exists column on distributed logs table. Err=%v", err))
				return err
			}

			zap.S().Info(fmt.Sprintf("Create index: %s_idx", colname))
			query = fmt.Sprintf("ALTER TABLE signoz_logs.logs on cluster cluster ADD INDEX IF NOT EXISTS %s_idx (%s) TYPE bloom_filter(0.01) GRANULARITY 64", colname, colname)
			err = conn.Exec(context.Background(), query)
			if err != nil {
				zap.S().Error(fmt.Errorf("error while renaming index. Err=%v", err))
				return err
			}
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
	zap.S().Debug(fmt.Sprintf("Params: %s %s %s %s", *hostFlag, *portFlag, *userNameFlag, *passwordFlag))

	conn, err := connect(*hostFlag, *portFlag, *userNameFlag, *passwordFlag)
	if err != nil {
		zap.S().Fatal("Error while connecting to clickhouse", zap.Error(err))
	}

	// Add default indexes for the data in otel collector
	fields, err := GetFields(conn)
	if err != nil {
		zap.S().Fatal("Error while getting fields", zap.Error(err))
		os.Exit(1)
	}

	statement, err := GetTableStatement(conn)
	if err != nil {
		zap.S().Fatal("Error while getting table statement", zap.Error(err))
		os.Exit(1)
	}

	// We are ignoring the top level keys as even if they are created we don't want to change them
	err = removeIndexes(conn, fields, statement)
	if err != nil {
		zap.S().Fatal("Error while renaming indexes", zap.Error(err))
		os.Exit(1)
	}

	err = renameMaterializedColumnsAndAddIndex(conn, fields, statement)
	if err != nil {
		zap.S().Fatal("Error while renaming materialized columns", zap.Error(err))
		os.Exit(1)

	}

	zap.S().Info(fmt.Sprintf("Completed migration in: %s", time.Since(start)))
}
