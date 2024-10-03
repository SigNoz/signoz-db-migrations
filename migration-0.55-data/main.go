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

func connect(host string, port string, userName string, password string, database string) (clickhouse.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{fmt.Sprintf("%s:%s", host, port)},
			Auth: clickhouse.Auth{
				Database: database,
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

func getCountOfLogs(conn clickhouse.Conn, startTimestamp, endTimestamp int64, tableName string) uint64 {
	ctx := context.Background()
	q := fmt.Sprintf("SELECT count(*) as count FROM %s WHERE timestamp> ? and timestamp <= ?", tableName)
	var count uint64
	err := conn.QueryRow(ctx, q, startTimestamp, endTimestamp).Scan(&count)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while getting count of logs. Err=%v \n", err))
		return 0
	}
	return count
}

// fetchBatch retrieves all data in the timerange
func fetchBatch(conn clickhouse.Conn, tableName string, start, end int64) ([]SigNozLog, error) {
	var logs []SigNozLog
	ctx := context.Background()

	// converting attributes_int64 and attributes_float64 to attributes_number while selecting from the old table
	query := "SELECT " +
		"timestamp, id, trace_id, span_id, trace_flags, severity_text, severity_number, body," +
		"CAST((attributes_string_key, attributes_string_value), 'Map(String, String)') as  attributes_string," +
		"CAST((arrayConcat(attributes_int64_key, attributes_float64_key), arrayConcat(CAST(attributes_int64_value AS Array(Float64)), attributes_float64_value)), 'Map(String, Float64)') as  attributes_number," +
		"CAST((attributes_bool_key, attributes_bool_value), 'Map(String, Bool)') as  attributes_bool," +
		"CAST((resources_string_key, resources_string_value), 'Map(String, String)') as resources_string from %s " +
		"where timestamp > ? and timestamp <= ?"

	query = fmt.Sprintf(query, tableName)

	err := conn.Select(ctx, &logs, query, start, end)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while reading logs. Err=%v \n", err))
		return nil, err
	}

	return logs, nil
}

// it fetches logs from the sourceConn and writes to the destConn
func processBatchesOfLogs(sourceConn, destConn clickhouse.Conn, startTimestamp, endTimestamp int64, batchDuration, batchSize int, sourceTableName, destTableName, destResourceTableName string) error {

	// convert minutes to ns
	batchDurationNs := int64(batchDuration) * 60 * 1e9

	var start int64
	end := endTimestamp

	var done bool

	// we will need start and end
	for {

		if done {
			break
		}

		start = end - batchDurationNs

		// last duration
		if start < startTimestamp {
			start = startTimestamp
			done = true
		}

		// Fetch all rows in that time range i.e >start and <=end
		data, err := fetchBatch(sourceConn, sourceTableName, start, end)
		if err != nil {
			return err
		}

		len := len(data)

		zap.L().Debug("migrating data: ", zap.Int64("start", start), zap.Int64("end", end), zap.Int("length", len))

		// process the data according to the batch size
		for i := 0; i < len; i += batchSize {
			batchEnd := i + batchSize
			if batchEnd > len {
				batchEnd = len // Adjust the end if it exceeds the array length
			}
			batch := data[i:batchEnd]

			// process the batch of data
			err = processAndWriteBatch(destConn, batch, destTableName, destResourceTableName)
			if err != nil {
				return err
			}
		}

		end = start
	}

	return nil
}

func tsBucket(ts int64, bucketSize int64) int64 {
	return (int64(ts) / int64(bucketSize)) * int64(bucketSize)
}

// writes a single batch to destConn
func processAndWriteBatch(destConn clickhouse.Conn, logs []SigNozLog, destTableName, destResourceTableName string) error {
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
		fmt.Sprintf(insertLogsSQLTemplateV2, destTableName),
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
		fmt.Sprintf("INSERT into %s", destResourceTableName),
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
	batchDuration := flag.Int("batch_duration", 5, "batch duration in minutes")
	batchSize := flag.Int("batch_size", 30000, "clickhouse password")
	startTimestamp := flag.Int64("start_ts", 0, "start timestamp in ns")
	endTimestamp := flag.Int64("end_ts", 0, "end timestamp in ns")
	sourceDbName := flag.String("source_db", "signoz_logs", "database name")
	destDbName := flag.String("dest_db", "signoz_logs", "dest database name")
	sourceTableName := flag.String("source_table", "distributed_logs", "table name")
	destTableName := flag.String("dest_table", "distributed_logs_v2", "dest table name")
	destResourceTableName := flag.String("resource_table", "distributed_logs_v2_resource", "dest resource table name")

	flag.Parse()

	sourceConn, err := connect(*hostFlag, *portFlag, *userNameFlag, *passwordFlag, *sourceDbName)
	if err != nil {
		zap.S().Fatal("Error while connecting to clickhouse", zap.Error(err))
	}
	defer sourceConn.Close()

	destConn, err := connect("localhost", *portFlag, *userNameFlag, *passwordFlag, *destDbName)
	if err != nil {
		zap.S().Fatal("Error while connecting to clickhouse", zap.Error(err))
	}

	defer destConn.Close()

	// get the total count of logs for that range of time.
	count := getCountOfLogs(sourceConn, *startTimestamp, *endTimestamp, *sourceTableName)
	if count == 0 {
		zap.S().Info("No logs to migrate")
		os.Exit(0)
	}
	zap.S().Info(fmt.Sprintf("Total count of logs: %d", count))

	err = processBatchesOfLogs(sourceConn, destConn, *startTimestamp, *endTimestamp, *batchDuration, *batchSize, *sourceTableName, *destTableName, *destResourceTableName)
	if err != nil {
		zap.S().Fatal("Error while migrating logs", zap.Error(err))
		os.Exit(1)
	}
	zap.S().Info(fmt.Sprintf("Completed migration in: %s", time.Since(start)))
}

const insertLogsSQLTemplateV2 = `INSERT INTO %s (
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
