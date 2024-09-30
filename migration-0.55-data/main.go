package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	driver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/SigNoz/signoz-otel-collector/exporter/clickhouselogsexporter/logsv2"
	"go.uber.org/zap"
)

type SigNozLogV2 struct {
	Ts_bucket_start      string `json:"ts_bucket_start" ch:"ts_bucket_start"`
	Resource_fingerprint string `json:"resource_fingerprint" ch:"resource_fingerprint"`
	SigNozLog
}

type SigNozLog struct {
	Timestamp          uint64             `json:"timestamp" ch:"timestamp"`
	Observed_timestamp uint64             `json:"observed_timestamp" ch:"observed_timestamp"`
	ID                 string             `json:"id" ch:"id"`
	TraceID            string             `json:"trace_id" ch:"trace_id"`
	SpanID             string             `json:"span_id" ch:"span_id"`
	TraceFlags         uint32             `json:"trace_flags" ch:"trace_flags"`
	SeverityText       string             `json:"severity_text" ch:"severity_text"`
	SeverityNumber     uint8              `json:"severity_number" ch:"severity_number"`
	Body               string             `json:"body" ch:"body"`
	Resources_string   map[string]string  `json:"resources_string" ch:"resources_string"`
	Attributes_string  map[string]string  `json:"attributes_string" ch:"attributes_string"`
	Attributes_number  map[string]float64 `json:"attributes_float" ch:"attributes_number"`
	Attributes_bool    map[string]bool    `json:"attributes_bool" ch:"attributes_bool"`
	ScopeName          string             `json:"scope_name" ch:"scope_name"`
	ScopeVersion       string             `json:"scope_version" ch:"scope_version"`
	Scope_string       map[string]string  `json:"scope_string" ch:"scope_string"`
}

type statementSendDuration struct {
	Name     string
	duration time.Duration
}

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

func getCountOfLogs(conn clickhouse.Conn, endTimestamp int64, dbName, tableName string) uint64 {
	ctx := context.Background()
	q := fmt.Sprintf("SELECT count(*) as count FROM %s.%s WHERE timestamp < ?", dbName, tableName)
	var count uint64
	err := conn.QueryRow(ctx, q, endTimestamp).Scan(&count)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while getting count of logs. Err=%v \n", err))
		return 0
	}
	return count
}

func checkTimestampUniqueness(conn clickhouse.Conn, dbName, tableName string, timestamp int64) (bool, error) {
	var count uint64
	query := fmt.Sprintf(`
		SELECT count() 
		FROM %s.%s 
		WHERE timestamp = ?
	`, dbName, tableName)

	ctx := context.Background()
	err := conn.QueryRow(ctx, query, timestamp).Scan(&count)
	if err != nil {
		return false, err
	}

	return count == 1, nil // Unique if count is 1
}

// fetchBatchWithTimestamp retrieves a batch of 30K timestamps and the last timestamp in the batch
func fetchBatchWithTimestamp(conn clickhouse.Conn, dbName, tableName string, endTimestamp, offset, limit int64) ([]SigNozLog, int64, error) {
	var logs []SigNozLog
	var lastTimestamp uint64
	ctx := context.Background()

	// converting attributes_int64 and attributes_float64 to attributes_number while selecting from the old table
	query := "SELECT " +
		"timestamp, id, trace_id, span_id, trace_flags, severity_text, severity_number, body," +
		"CAST((attributes_string_key, attributes_string_value), 'Map(String, String)') as  attributes_string," +
		"CAST((arrayConcat(attributes_int64_key, attributes_float64_key), arrayConcat(CAST(attributes_int64_value AS Array(Float64)), attributes_float64_value)), 'Map(String, Float64)') as  attributes_number," +
		"CAST((attributes_bool_key, attributes_bool_value), 'Map(String, Bool)') as  attributes_bool," +
		"CAST((resources_string_key, resources_string_value), 'Map(String, String)') as resources_string from %s.%s " +
		"where timestamp < ? order by timestamp desc limit ? offset ?"

	query = fmt.Sprintf(query, dbName, tableName)

	err := conn.Select(ctx, &logs, query, endTimestamp, limit, offset)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while reading logs. Err=%v \n", err))
		return nil, 0, err
	}
	if len(logs) == 0 {
		return nil, 0, nil
	}
	lastTimestamp = logs[len(logs)-1].Timestamp

	return logs, int64(lastTimestamp), nil
}

// it tries to get logs by using timestamp as the key.
// but if it lands in a timestamp which is not unique it will use limit offset for the next batch
func processBatchesOfLogs(sourceConn, destConn clickhouse.Conn, endTimestamp, batchSize int64, sourceDbName, destDbName, sourceTableName, destTableName, destResourceTableName string) error {
	var lastTimestamp int64
	offset := int64(0)

	for {
		// Fetch a batch of 30K rows
		batch, tempLastTimestamp, err := fetchBatchWithTimestamp(sourceConn, sourceDbName, sourceTableName, endTimestamp, offset, batchSize)
		if err != nil {
			return err
		}

		// If the batch is empty, exit the loop
		if len(batch) == 0 {
			break
		}

		// process the batch of data
		err = processAndWriteBatch(destConn, batch, destDbName, destTableName, destResourceTableName)
		if err != nil {
			return err
		}

		// Check if the last entry's timestamp is unique
		isUnique, err := checkTimestampUniqueness(sourceConn, sourceDbName, sourceTableName, lastTimestamp)
		if err != nil {
			return err
		}

		// If unique, collect the timestamps; if not, increment the offset and continue
		if isUnique {
			zap.S().Info(fmt.Sprintf("Found unique timestamp %d \n", lastTimestamp))
			lastTimestamp = tempLastTimestamp
			// reset offset to 0
			offset = 0
		} else {
			zap.S().Info(fmt.Sprintf("Found non-unique timestamp %d. Incrementing offset to %d \n", lastTimestamp, offset+batchSize))
			offset += batchSize
		}
	}

	return nil
}

func tsBucket(ts int64, bucketSize int64) int64 {
	return (int64(ts) / int64(bucketSize)) * int64(bucketSize)
}

func processAndWriteBatch(destConn clickhouse.Conn, logs []SigNozLog, destDbName, destTableName, destResourceTableName string) error {
	ctx := context.Background()

	// go through each log and generate fingerprint for the batch
	resourcesSeen := map[int64]map[string]string{}

	var insertLogsStmtV2 driver.Batch
	var insertResourcesStmtV2 driver.Batch

	defer func() {
		if insertLogsStmtV2 != nil {
			_ = insertLogsStmtV2.Abort()
		}
		if insertResourcesStmtV2 != nil {
			_ = insertResourcesStmtV2.Abort()
		}
	}()

	insertLogsStmtV2, err := destConn.PrepareBatch(
		ctx,
		fmt.Sprintf(insertLogsSQLTemplateV2, destDbName, destTableName),
		driver.WithReleaseConnection(),
	)
	if err != nil {
		return fmt.Errorf("PrepareBatchV2:%w", err)
	}

	for _, log := range logs {

		serializedRes, err := json.Marshal(log.Resources_string)
		if err != nil {
			fmt.Errorf("couldn't serialize log resource JSON: %w", err)
			return err
		}

		resourceJson := string(serializedRes)

		lBucketStart := tsBucket(int64(log.Timestamp/1000000000), 1800)

		if _, exists := resourcesSeen[int64(lBucketStart)]; !exists {
			resourcesSeen[int64(lBucketStart)] = map[string]string{}
		}

		resourcesForFingerprint := map[string]any{}
		for key, value := range log.Resources_string {
			resourcesForFingerprint[key] = value
		}

		fp, exists := resourcesSeen[int64(lBucketStart)][resourceJson]
		if !exists {
			fp = logsv2.CalculateFingerprint(resourcesForFingerprint, logsv2.ResourceHierarchy())
			resourcesSeen[int64(lBucketStart)][resourceJson] = fp
		}

		err = insertLogsStmtV2.Append(
			uint64(lBucketStart),
			fp,
			log.Timestamp,
			log.Observed_timestamp,
			log.ID,
			log.TraceID,
			log.SpanID,
			log.TraceFlags,
			log.SeverityText,
			log.SeverityNumber,
			log.Body,
			log.Attributes_string,
			log.Attributes_number,
			log.Attributes_bool,
			log.Resources_string,
			log.ScopeName,
			log.ScopeVersion,
			log.Scope_string,
		)
		if err != nil {
			return fmt.Errorf("StatementAppendLogsV2:%w", err)
		}

	}

	insertResourcesStmtV2, err = destConn.PrepareBatch(
		ctx,
		fmt.Sprintf("INSERT into %s.%s", destDbName, destResourceTableName),
		driver.WithReleaseConnection(),
	)
	if err != nil {
		return fmt.Errorf("couldn't PrepareBatch for inserting resource fingerprints :%w", err)
	}

	for bucketTs, resources := range resourcesSeen {
		for resourceLabels, fingerprint := range resources {
			insertResourcesStmtV2.Append(
				resourceLabels,
				fingerprint,
				bucketTs,
			)
		}
	}

	var wg sync.WaitGroup
	chErr := make(chan error, 2)
	chDuration := make(chan statementSendDuration, 2)

	wg.Add(2)

	go send(insertLogsStmtV2, destTableName, chDuration, chErr, &wg)
	go send(insertResourcesStmtV2, destResourceTableName, chDuration, chErr, &wg)

	wg.Wait()
	close(chErr)

	// check the errors
	for i := 0; i < 2; i++ {
		if r := <-chErr; r != nil {
			return fmt.Errorf("StatementSend:%w", r)
		}
	}

	for i := 0; i < 2; i++ {
		sendDuration := <-chDuration
		zap.L().Info("Send duration", zap.String("table", sendDuration.Name), zap.Duration("duration", sendDuration.duration))
	}

	return nil
}

func send(statement driver.Batch, tableName string, durationCh chan<- statementSendDuration, chErr chan<- error, wg *sync.WaitGroup) {
	defer wg.Done()
	start := time.Now()
	err := statement.Send()
	chErr <- err
	durationCh <- statementSendDuration{
		Name:     tableName,
		duration: time.Since(start),
	}
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
	batchSize := flag.Int64("batch_size", 30000, "clickhouse password")
	endTimestamp := flag.Int64("end_ts", 0, "end timestamp in ns")
	sourceDbName := flag.String("source_db", "signoz_logs", "database name")
	destDbName := flag.String("dest_db", "signoz_logs", "dest database name")
	sourceTableName := flag.String("source_table", "distributed_logs", "table name")
	destTableName := flag.String("dest_table", "distributed_logs_v2", "dest table name")
	destResourceTableName := flag.String("resource_table", "distributed_logs_v2_resource", "dest resource table name")

	flag.Parse()
	// zap.S().Debug(fmt.Sprintf("Params: %s %s %s", *hostFlag, *portFlag, *userNameFlag))

	sourceConn, err := connect(*hostFlag, *portFlag, *userNameFlag, *passwordFlag)
	if err != nil {
		zap.S().Fatal("Error while connecting to clickhouse", zap.Error(err))
	}

	destConn, err := connect(*hostFlag, *portFlag, *userNameFlag, *passwordFlag)
	if err != nil {
		zap.S().Fatal("Error while connecting to clickhouse", zap.Error(err))
	}
	defer sourceConn.Close()
	defer destConn.Close()

	// get the total count of logs for that range of time.
	count := getCountOfLogs(sourceConn, *endTimestamp, *sourceDbName, *sourceTableName)
	if count == 0 {
		zap.S().Info("No logs to migrate")
		os.Exit(0)
	}
	zap.S().Info(fmt.Sprintf("Total count of logs: %d", count))

	err = processBatchesOfLogs(sourceConn, destConn, *endTimestamp, *batchSize, *sourceDbName, *destDbName, *sourceTableName, *destTableName, *destResourceTableName)
	if err != nil {
		zap.S().Fatal("Error while migrating logs", zap.Error(err))
		os.Exit(1)
	}
	zap.S().Info(fmt.Sprintf("Completed migration in: %s", time.Since(start)))
}

const insertLogsSQLTemplateV2 = `INSERT INTO %s.%s (
	ts_bucket_start,
	resource_fingerprint,
	timestamp,
	observed_timestamp,
	id,
	trace_id,
	span_id,
	trace_flags,
	severity_text,
	severity_number,
	body,
	attributes_string,
	attributes_number,
	attributes_bool,
	resources_string,
	scope_name,
	scope_version,
	scope_string
	) VALUES (
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?,
		?
		)`
