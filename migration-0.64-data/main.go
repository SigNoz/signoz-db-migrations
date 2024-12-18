package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	driver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/SigNoz/signoz-otel-collector/exporter/clickhouselogsexporter/logsv2"
	"go.uber.org/zap"
)

// IndexV2 represents the source table schema
type IndexV2 struct {
	Timestamp        time.Time `ch:"timestamp"`
	TraceID          string    `ch:"traceID"`
	SpanID           string    `ch:"spanID"`
	ParentSpanID     string    `ch:"parentSpanID"`
	Name             string    `ch:"name"`
	Kind             int8      `ch:"kind"`
	SpanKind         string    `ch:"spanKind"`
	DurationNano     uint64    `ch:"durationNano"`
	StatusCode       int16     `ch:"statusCode"`
	StatusMessage    string    `ch:"statusMessage"`
	StatusCodeString string    `ch:"statusCodeString"`

	StringTagMap    map[string]string  `ch:"stringTagMap"`
	NumberTagMap    map[string]float64 `ch:"numberTagMap"`
	BoolTagMap      map[string]bool    `ch:"boolTagMap"`
	ResourceTagsMap map[string]string  `ch:"resourceTagsMap"`

	Events             []string `ch:"events"`
	ExternalHttpMethod string   `ch:"externalHttpMethod"`
	ExternalHttpUrl    string   `ch:"externalHttpUrl"`
	DbName             string   `ch:"dbName"`
	DbOperation        string   `ch:"dbOperation"`
	HttpMethod         string   `ch:"httpMethod"`
	HttpUrl            string   `ch:"httpUrl"`
	HttpHost           string   `ch:"httpHost"`
	HasError           bool     `ch:"hasError"`
	ResponseStatusCode string   `ch:"responseStatusCode"`
	IsRemote           string   `ch:"isRemote"`

	// Commented these as these are materialized columns in the new schema and they will be extracted from attributes
	// ServiceName        string             `ch:"serviceName"`
	// HttpRoute          string             `ch:"httpRoute"`
	// MsgSystem          string             `ch:"msgSystem"`
	// MsgOperation       string             `ch:"msgOperation"`
	// DbSystem           string             `ch:"dbSystem"`
	// RpcSystem          string             `ch:"rpcSystem"`
	// RpcService         string             `ch:"rpcService"`
	// RpcMethod          string             `ch:"rpcMethod"`
	// PeerService string   `ch:"peerService"`
}

// IndexV3 represents the destination table schema
// Not used anywhere, just for reference
type IndexV3 struct {
	TsBucketStart       uint64    `ch:"ts_bucket_start"`
	ResourceFingerprint string    `ch:"resource_fingerprint"`
	Timestamp           time.Time `ch:"timestamp"`
	TraceID             string    `ch:"trace_id"`
	SpanID              string    `ch:"span_id"`
	ParentSpanID        string    `ch:"parent_span_id"`
	Name                string    `ch:"name"`
	Kind                int8      `ch:"kind"`
	KindString          string    `ch:"kind_string"`
	DurationNano        uint64    `ch:"duration_nano"`
	StatusCode          int16     `ch:"status_code"`
	StatusMessage       string    `ch:"status_message"`
	StatusCodeString    string    `ch:"status_code_string"`

	AttributesString map[string]string  `ch:"attributes_string"`
	AttributesNumber map[string]float64 `ch:"attributes_number"`
	AttributesBool   map[string]bool    `ch:"attributes_bool"`
	ResourcesString  map[string]string  `ch:"resources_string"`

	Events             []string `ch:"events"`
	ExternalHttpMethod string   `ch:"external_http_method"`
	ExternalHttpUrl    string   `ch:"external_http_url"`
	DbName             string   `ch:"db_name"`
	DbOperation        string   `ch:"db_operation"`
	HttpMethod         string   `ch:"http_method"`
	HttpUrl            string   `ch:"http_url"`
	HttpHost           string   `ch:"http_host"`
	HasError           bool     `ch:"has_error"`
	ResponseStatusCode string   `ch:"response_status_code"`
	IsRemote           string   `ch:"is_remote"`

	// get references from the signoz_spans tabls
	Links string `ch:"links"`

	// These are new and the values are not present in old table
	// TraceState          string             `ch:"trace_state"`
	// Flags uint32 `ch:"flags"`
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
			Debug: false,
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

func getCountOfTraces(conn clickhouse.Conn, startTimestamp, endTimestamp int64, tableName string) int64 {
	ctx := context.Background()
	// Directly use the nanosecond timestamps in the query
	q := fmt.Sprintf("SELECT count(*) as count FROM %s WHERE timestamp > fromUnixTimestamp64Nano(?) AND timestamp <= fromUnixTimestamp64Nano(?)", tableName)
	var count uint64
	err := conn.QueryRow(ctx, q, startTimestamp, endTimestamp).Scan(&count)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while getting count of traces. Err=%v \n", err))
		return 0
	}
	return int64(count)
}

// fetchBatch retrieves all data in the timerange
func fetchIndexBatch(conn clickhouse.Conn, tableName string, start, end int64) ([]IndexV2, error) {
	var traces []IndexV2
	ctx := context.Background()

	query := "SELECT " +
		"timestamp, traceID, spanID, parentSpanID, name, kind, spanKind, durationNano, statusCode, statusMessage, statusCodeString, " +
		"stringTagMap, numberTagMap, boolTagMap, resourceTagsMap, events, externalHttpMethod, externalHttpUrl, dbName, dbOperation, httpMethod, " +
		"httpUrl, httpHost, hasError, responseStatusCode, isRemote from signoz_traces.%s " + "where timestamp > fromUnixTimestamp64Nano(?) and timestamp <= fromUnixTimestamp64Nano(?)"

	query = fmt.Sprintf(query, tableName)

	err := conn.Select(ctx, &traces, query, start, end)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while reading traces. Err=%v \n", err))
		return nil, err
	}

	return traces, nil
}

type Span struct {
	TraceID string `ch:"traceID"`
	Model   string `ch:"model"`
}

func fetchSpansBatch(conn clickhouse.Conn, tableName string, start, end int64) ([]Span, error) {
	var spans []Span
	ctx := context.Background()

	query := "SELECT " +
		"traceID, model from signoz_traces.%s " + "where timestamp > fromUnixTimestamp64Nano(?) and timestamp <= fromUnixTimestamp64Nano(?)"

	query = fmt.Sprintf(query, tableName)

	err := conn.Select(ctx, &spans, query, start, end)
	if err != nil {
		zap.S().Error(fmt.Errorf("error while reading spans. Err=%v \n", err))
		return nil, err
	}

	return spans, nil
}

type Model struct {
	SpanID string      `json:"spanID"`
	Ref    interface{} `json:"references"`
}

// it fetches traces from the sourceConn and writes to the destConn
func processBatchesOfTraces(sourceConn, destConn clickhouse.Conn, startTimestamp, endTimestamp int64, batchDuration, batchSize int, sourceTableName, sourceSpansTableName, destTableName, destResourceTableName string) error {

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
		data, err := fetchIndexBatch(sourceConn, sourceTableName, start, end)
		if err != nil {
			return err
		}

		spans, err := fetchSpansBatch(sourceConn, sourceSpansTableName, start, end)
		if err != nil {
			return err
		}

		// create a map of traceID to span
		spanIDToLinks := map[string]string{}
		for _, span := range spans {
			m := Model{}
			err = json.Unmarshal([]byte(span.Model), &m)
			if err != nil {
				return fmt.Errorf("error while unmarshalling span model. Err=%v \n", err)
			}
			ref, err := json.Marshal(m.Ref)
			if err != nil {
				return fmt.Errorf("error while marshalling span model. Err=%v \n", err)
			}
			spanIDToLinks[m.SpanID] = string(ref)
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
			err = processAndWriteBatch(destConn, batch, spanIDToLinks, destTableName, destResourceTableName)
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
func processAndWriteBatch(destConn clickhouse.Conn, traces []IndexV2, spanIDToLinks map[string]string, destTableName, destResourceTableName string) error {
	ctx := context.Background()

	// go through each log and generate fingerprint for the batch
	resourcesSeen := map[int64]map[string]string{}

	var insertTraceStmtV3 driver.Batch
	var insertTraceResourcesStmtV3 driver.Batch

	defer func() {
		if insertTraceStmtV3 != nil {
			_ = insertTraceStmtV3.Abort()
		}
		if insertTraceResourcesStmtV3 != nil {
			_ = insertTraceResourcesStmtV3.Abort()
		}
	}()

	insertTraceStmtV3, err := destConn.PrepareBatch(
		ctx,
		fmt.Sprintf(insertTraceSQLTemplateV3, destTableName),
		driver.WithReleaseConnection(),
	)
	if err != nil {
		return fmt.Errorf("PrepareBatchV2:%w", err)
	}

	for _, trace := range traces {

		serializedRes, err := json.Marshal(trace.ResourceTagsMap)
		if err != nil {
			return fmt.Errorf("couldn't serialize log resource JSON: %w", err)
		}

		resourceJson := string(serializedRes)

		lBucketStart := tsBucket(trace.Timestamp.Unix(), 1800)

		if _, exists := resourcesSeen[int64(lBucketStart)]; !exists {
			resourcesSeen[int64(lBucketStart)] = map[string]string{}
		}

		resourcesForFingerprint := map[string]any{}
		for key, value := range trace.ResourceTagsMap {
			resourcesForFingerprint[key] = value
		}

		fp, exists := resourcesSeen[int64(lBucketStart)][resourceJson]
		if !exists {
			fp = logsv2.CalculateFingerprint(resourcesForFingerprint, logsv2.ResourceHierarchy())
			resourcesSeen[int64(lBucketStart)][resourceJson] = fp
		}

		links, exists := spanIDToLinks[trace.SpanID]
		if !exists {
			zap.S().Warn(fmt.Errorf("spanID not found in spanIDToLinks. SpanID=%s", trace.SpanID))
		}

		err = insertTraceStmtV3.Append(
			uint64(lBucketStart),
			fp,
			trace.Timestamp,
			trace.TraceID,
			trace.SpanID,
			trace.ParentSpanID,
			trace.Name,
			trace.Kind,
			trace.SpanKind,
			trace.DurationNano,
			trace.StatusCode,
			trace.StatusMessage,
			trace.StatusCodeString,

			trace.StringTagMap,
			trace.NumberTagMap,
			trace.BoolTagMap,
			trace.ResourceTagsMap,

			trace.Events,
			trace.ExternalHttpMethod,
			trace.ExternalHttpUrl,
			trace.DbName,
			trace.DbOperation,
			trace.HttpMethod,
			trace.HttpUrl,
			trace.HttpHost,
			trace.HasError,
			trace.ResponseStatusCode,
			trace.IsRemote,

			links,
		)
		if err != nil {
			return fmt.Errorf("StatementAppendTracesV3:%w", err)
		}

	}

	insertTraceResourcesStmtV3, err = destConn.PrepareBatch(
		ctx,
		fmt.Sprintf("INSERT into %s", destResourceTableName),
		driver.WithReleaseConnection(),
	)
	if err != nil {
		return fmt.Errorf("couldn't PrepareBatch for inserting resource fingerprints :%w", err)
	}

	for bucketTs, resources := range resourcesSeen {
		for resourceLabels, fingerprint := range resources {
			insertTraceResourcesStmtV3.Append(
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

	go send(insertTraceStmtV3, destTableName, chDuration, chErr, &wg)
	go send(insertTraceResourcesStmtV3, destResourceTableName, chDuration, chErr, &wg)

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
	destHostFlag := flag.String("dest_host", "127.0.0.1", "clickhouse host")
	destPortFlag := flag.String("dest_port", "9000", "clickhouse port")
	userNameFlag := flag.String("username", "default", "clickhouse username")
	passwordFlag := flag.String("password", "", "clickhouse password")
	destUserNameFlag := flag.String("dest_username", "default", "clickhouse username")
	destPasswordFlag := flag.String("dest_password", "", "clickhouse password")
	batchDuration := flag.Int("batch_duration", 5, "batch duration in minutes")
	batchSize := flag.Int("batch_size", 30000, "clickhouse password")
	startTimestamp := flag.Int64("start_ts", 0, "start timestamp in ns")
	endTimestamp := flag.Int64("end_ts", 0, "end timestamp in ns")
	sourceDbName := flag.String("source_db", "signoz_traces", "database name")
	destDbName := flag.String("dest_db", "signoz_traces", "dest database name")
	sourceTableName := flag.String("source_table", "distributed_signoz_index_v2", "table name")
	sourceSpansTableName := flag.String("source_spans_table", "distributed_tmp_signoz_spans", "source spans table name")
	destTableName := flag.String("dest_table", "distributed_signoz_index_v3", "dest table name")
	destResourceTableName := flag.String("resource_table", "distributed_traces_v3_resource", "dest resource table name")
	countDeltaAllowed := flag.Int("count_delta_allowed", 1000, "count delta allowed")

	flag.Parse()

	sourceConn, err := connect(*hostFlag, *portFlag, *userNameFlag, *passwordFlag, *sourceDbName)
	if err != nil {
		zap.S().Fatal("Error while connecting to clickhouse", zap.Error(err))
	}
	defer sourceConn.Close()

	destConn, err := connect(*destHostFlag, *destPortFlag, *destUserNameFlag, *destPasswordFlag, *destDbName)
	if err != nil {
		zap.S().Fatal("Error while connecting to clickhouse", zap.Error(err))
	}

	defer destConn.Close()

	// get the total count of traces for that range of time.
	count_index := getCountOfTraces(sourceConn, *startTimestamp, *endTimestamp, *sourceTableName)
	if count_index == 0 {
		zap.S().Info("No spans to migrate from index table")
		os.Exit(0)
	}

	count_spans := getCountOfTraces(sourceConn, *startTimestamp, *endTimestamp, *sourceSpansTableName)
	if count_spans == 0 {
		zap.S().Info("No spans to migrate from spans table")
		os.Exit(0)
	}

	zap.S().Info(fmt.Sprintf("Count of index table: %d", count_index))
	zap.S().Info(fmt.Sprintf("Count of spans table: %d", count_spans))

	if math.Abs(float64(count_index-count_spans)) > float64(*countDeltaAllowed) {
		zap.S().Fatal("Count delta between index table and spans table is more than allowed delta")
	}

	err = processBatchesOfTraces(sourceConn, destConn, *startTimestamp, *endTimestamp, *batchDuration, *batchSize, *sourceTableName, *sourceSpansTableName, *destTableName, *destResourceTableName)
	if err != nil {
		zap.S().Fatal("Error while migrating traces", zap.Error(err))
		os.Exit(1)
	}
	zap.S().Info(fmt.Sprintf("Completed migration in: %s", time.Since(start)))
}

const insertTraceSQLTemplateV3 = `INSERT INTO %s (
	ts_bucket_start,
	resource_fingerprint,
	timestamp,
	trace_id,
	span_id,
	parent_span_id,
	name,
	kind,
	kind_string,
	duration_nano,
	status_code,
	status_message,
	status_code_string,
	
	attributes_string,
	attributes_number,
	attributes_bool,
	resources_string,

	events,
	external_http_method,
	external_http_url,
	db_name,
	db_operation,
	http_method,
	http_url,
	http_host,
	has_error,
	response_status_code,
	is_remote,
	links
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
