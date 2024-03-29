package main

import (
	"context"
	"crypto/md5"
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

type DBResponseTotal struct {
	NumTotal uint64 `ch:"numTotal"`
}

type DBResponseServices struct {
	ServiceName string    `ch:"serviceName"`
	Mint        time.Time `ch:"mint"`
	Maxt        time.Time `ch:"maxt"`
	NumTotal    uint64    `ch:"numTotal"`
}

type DBResponseTTL struct {
	EngineFull string `ch:"engine_full"`
}

type SignozErrorIndex struct {
	ErrorID             string    `json:"errorId" ch:"errorID"`
	ExceptionType       string    `json:"exceptionType" ch:"exceptionType"`
	ExceptionStacktrace string    `json:"exceptionStacktrace" ch:"exceptionStacktrace"`
	ExceptionEscaped    string    `json:"exceptionEscaped" ch:"exceptionEscaped"`
	ExceptionMsg        string    `json:"exceptionMessage" ch:"exceptionMessage"`
	Timestamp           time.Time `json:"timestamp" ch:"timestamp"`
	SpanID              string    `json:"spanID" ch:"spanID"`
	TraceID             string    `json:"traceID" ch:"traceID"`
	ParentSpanID        string    `json:"parentSpanID" ch:"parentSpanID"`
	ServiceName         string    `json:"serviceName" ch:"serviceName"`
}

type SignozErrorIndexV2 struct {
	ErrorID             string    `json:"errorId" ch:"errorID"`
	ExceptionType       string    `json:"exceptionType" ch:"exceptionType"`
	ExceptionStacktrace string    `json:"exceptionStacktrace" ch:"exceptionStacktrace"`
	ExceptionEscaped    bool      `json:"exceptionEscaped" ch:"exceptionEscaped"`
	ExceptionMsg        string    `json:"exceptionMessage" ch:"exceptionMessage"`
	Timestamp           time.Time `json:"timestamp" ch:"timestamp"`
	SpanID              string    `json:"spanID" ch:"spanID"`
	TraceID             string    `json:"traceID" ch:"traceID"`
	ServiceName         string    `json:"serviceName" ch:"serviceName"`
	GroupID             string    `json:"groupID" ch:"groupID"`
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

func processSpans(data []SignozErrorIndex) []SignozErrorIndexV2 {
	processedSpans := []SignozErrorIndexV2{}
	for _, span := range data {
		hmd5 := md5.Sum([]byte(span.ServiceName + span.ExceptionType + span.ExceptionMsg))
		ErrorGroupID := fmt.Sprintf("%x", hmd5)
		processedData := SignozErrorIndexV2{
			Timestamp:           span.Timestamp,
			TraceID:             span.TraceID,
			SpanID:              span.SpanID,
			ServiceName:         span.ServiceName,
			GroupID:             ErrorGroupID,
			ErrorID:             span.ErrorID,
			ExceptionType:       span.ExceptionType,
			ExceptionMsg:        span.ExceptionMsg,
			ExceptionStacktrace: span.ExceptionStacktrace,
			ExceptionEscaped:    stringToBool(span.ExceptionEscaped),
		}
		processedSpans = append(processedSpans, processedData)
	}

	return processedSpans
}

func stringToBool(s string) bool {
	if strings.ToLower(s) == "true" {
		return true
	}
	return false
}

func readTotalRows(conn clickhouse.Conn, serviceName string, startTime uint64, endTime uint64) (uint64, error) {
	ctx := context.Background()
	result := []DBResponseTotal{}
	if serviceName != "" {
		te := fmt.Sprintf("SELECT count() as numTotal FROM signoz_traces.signoz_error_index where serviceName='%s' AND timestamp>= '%v' AND timestamp<= '%v'", serviceName, startTime, endTime)
		if err := conn.Select(ctx, &result, te); err != nil {
			return 0, err
		}
	} else {
		if err := conn.Select(ctx, &result, "SELECT count() as numTotal FROM signoz_traces.signoz_error_index"); err != nil {
			return 0, err
		}
	}
	if len(result) == 0 {
		return 0, nil
	}
	return result[0].NumTotal, nil
}

func readServices(conn clickhouse.Conn) ([]DBResponseServices, error) {
	ctx := context.Background()
	result := []DBResponseServices{}
	if err := conn.Select(ctx, &result, "SELECT serviceName, MIN(timestamp) as mint, MAX(timestamp) as maxt, count() as numTotal FROM signoz_traces.signoz_error_index group by serviceName order by serviceName"); err != nil {
		return nil, err
	}
	return result, nil
}

func readSpans(conn clickhouse.Conn, serviceName string, endTime uint64, startTime uint64, limit int64, offset int64) ([]SignozErrorIndex, error) {
	ctx := context.Background()
	result := []SignozErrorIndex{}
	te := ""
	if limit != -1 {
		te = fmt.Sprintf("SELECT * FROM signoz_traces.signoz_error_index where serviceName='%s' AND timestamp>= '%v' AND timestamp<= '%v' ORDER BY timestamp LIMIT %v OFFSET %v", serviceName, startTime, endTime, limit, offset)
	} else {
		te = fmt.Sprintf("SELECT * FROM signoz_traces.signoz_error_index where serviceName='%s' AND timestamp>= '%v' AND timestamp<= '%v'", serviceName, startTime, endTime)
	}
	if err := conn.Select(ctx, &result, te); err != nil {
		return nil, err
	}
	return result, nil
}

func write(conn clickhouse.Conn, batchSpans []SignozErrorIndexV2) error {
	zap.S().Info(fmt.Sprintf("Writing %v rows", len(batchSpans)))
	err := writeErrorIndexV2(conn, batchSpans)
	return err
}

func writeErrorIndexV2(conn clickhouse.Conn, batchSpans []SignozErrorIndexV2) error {
	ctx := context.Background()
	statement, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO signoz_traces.signoz_error_index_v2"))
	if err != nil {
		zap.S().Error("Error preparing statement: ", err)
		return err
	}
	// zap.S().Debug("Length of batchSpans: ", len(batchSpans))
	for _, span := range batchSpans {
		defer func() {
			if err := recover(); err != nil {
				fmt.Println(span)
				zap.S().Fatal("panic occurred:", err)
			}
		}()
		err = statement.Append(
			span.Timestamp,
			span.ErrorID,
			span.GroupID,
			span.TraceID,
			span.SpanID,
			span.ServiceName,
			span.ExceptionType,
			span.ExceptionMsg,
			span.ExceptionStacktrace,
			span.ExceptionEscaped,
		)
		if err != nil {
			return err
		}
	}

	return statement.Send()
}

func dropOldTables(conn clickhouse.Conn) {
	ctx := context.Background()
	zap.S().Info("Dropping signoz_error_index table")
	err := conn.Exec(ctx, "DROP TABLE IF EXISTS signoz_traces.signoz_error_index")
	if err != nil {
		zap.S().Fatal(err, "Error dropping signoz_error_index table")
	}
	zap.S().Info("Successfully dropped signoz_error_index")
}

func parseTTL(queryResp string) int {

	zap.S().Debug(fmt.Sprintf("Parsing TTL from: %s", queryResp))
	deleteTTLExp := regexp.MustCompile(`toIntervalSecond\(([0-9]*)\)`)

	var delTTL int = -1

	m := deleteTTLExp.FindStringSubmatch(queryResp)
	if len(m) > 1 {
		seconds_int, err := strconv.Atoi(m[1])
		if err != nil {
			return -1
		}
		delTTL = seconds_int / 3600
	}

	return delTTL
}

func getTracesTTL(conn clickhouse.Conn) (int, error) {
	var dbResp []DBResponseTTL
	ctx := context.Background()
	query := fmt.Sprintf("SELECT engine_full FROM system.tables WHERE name='%v' AND database='%v'", "signoz_error_index", "signoz_traces")
	err := conn.Select(ctx, &dbResp, query)

	if err != nil {
		zap.S().Error(fmt.Errorf("error while getting ttl. Err=%v", err))
		return 0, err
	}
	if len(dbResp) == 0 {
		return -1, fmt.Errorf("no ttl found")
	} else {
		delTTL := parseTTL(dbResp[0].EngineFull)
		return delTTL, nil
	}
}

func setTracesTTL(conn clickhouse.Conn, delTTL int) error {
	zap.S().Info(fmt.Sprintf("Setting TTL to %v\nSetting TTL might take very long depending upon data and resources, please be patient...", delTTL))
	tableName := "signoz_traces.signoz_error_index_v2"
	req := fmt.Sprintf(
		"ALTER TABLE %v MODIFY TTL toDateTime(timestamp) + INTERVAL %v SECOND DELETE",
		tableName, delTTL*3600)

	zap.S().Debug(fmt.Sprintf("Executing TTL request: %s", req))
	if err := conn.Exec(context.Background(), req); err != nil {
		zap.S().Error(fmt.Errorf("Error in executing set TTL query: %s", err.Error()))
		return err
	}
	return nil
}

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()
	undo := zap.ReplaceGlobals(logger)
	defer undo()

	start := time.Now()
	batchSize := flag.Uint64("batchSize", 70000, "size of batch which is read and write to clickhouse")
	serviceFlag := flag.String("service", "", "serviceName")
	timeFlag := flag.Uint64("timeNano", 0, "timestamp in nano seconds")
	hostFlag := flag.String("host", "127.0.0.1", "clickhouse host")
	portFlag := flag.String("port", "9000", "clickhouse port")
	userNameFlag := flag.String("userName", "default", "clickhouse username")
	passwordFlag := flag.String("password", "", "clickhouse password")
	dropOldTable := flag.Bool("dropOldTable", true, "clear old clickhouse data if migration was successful")
	flag.Parse()
	zap.S().Debug(fmt.Sprintf("Params: %s %s %s %s", *hostFlag, *portFlag, *userNameFlag, *passwordFlag))

	conn, err := connect(*hostFlag, *portFlag, *userNameFlag, *passwordFlag)
	if err != nil {
		zap.S().Fatal("Error while connecting to clickhouse", zap.Error(err))
	}
	moveTTL, err := getTracesTTL(conn)
	if err == nil && moveTTL > 1 {
		setTracesTTL(conn, moveTTL)
	} else {
		zap.S().Info("No TTL found, skipping TTL migration")
	}
	rows, err := readTotalRows(conn, "", 0, 0)
	if err != nil {
		zap.S().Error(err, "error while reading total rows")
		return
	}
	if rows == 0 {
		zap.S().Info("No data found in clickhouse")
		os.Exit(0)
	}
	zap.S().Info(fmt.Sprintf("There are total %v rows, starting migration...", rows))
	services, err := readServices(conn)
	if err != nil {
		zap.S().Fatal(err, "error while reading services")
		return
	}
	skip := true

	// Iterate over services and migrate data
	for _, service := range services {
		start := uint64(service.Maxt.UnixNano())

		// If service is provided, start migration from that service
		if *serviceFlag != "" && skip {
			if service.ServiceName != *serviceFlag {
				continue
			} else {
				zap.S().Info("Starting from service: ", service.ServiceName)
				skip = false
				// If time is provided, start migration from that time
				if *timeFlag != 0 {
					start = *timeFlag
					zap.S().Info(fmt.Sprintf("Processing remaining rows of serviceName: %s and Timestamp: %s", service.ServiceName, time.Unix(0, int64(start))))
				}
			}
		}
		if skip {
			zap.S().Info(fmt.Sprintf("Processing %v rows of serviceName %s", service.NumTotal, service.ServiceName))
		}
		// Calculate average rows per second (rps)
		rps := service.NumTotal / ((uint64(service.Maxt.Unix()) - uint64(service.Mint.Unix())) + 1)
		zap.S().Info(fmt.Sprintf("RPS: %v", rps))

		// Calculate time period for an average single batch
		timePeriod := (*batchSize * 1000000000) / (rps + 1)
		zap.S().Info(fmt.Sprintf("Time Period (nS): %v", timePeriod))
		for start >= uint64(service.Mint.UnixNano()) {
			rows, err := readTotalRows(conn, service.ServiceName, start-timePeriod, start)
			if err != nil {
				zap.S().Fatal(err, "error while reading total rows")
				return
			}
			if rows == 0 {
				start -= timePeriod
				continue
			}
			
			// If rows are more than batch size, then split them into batches
			if rows >= *batchSize {
				// fmt.Println("Rows: ", rows)
				offset := int64(0)
				batchRow := rows + *batchSize
				for batchRow >= *batchSize {
					batchSpans, err := readSpans(conn, service.ServiceName, start, start-timePeriod, int64(*batchSize), offset)
					if err != nil {
						zap.S().Fatal(err, "error while reading spans")
						return
					}
					// fmt.Println("Size of batch ", len(batchSpans))
					// fmt.Println("batchRows left: ", batchRow)
					if len(batchSpans) > 0 {
						processedSpans := processSpans(batchSpans)
						err = write(conn, processedSpans)
						if err != nil {
							zap.S().Fatal(err, "error while writing spans")
							return
						}
					}
					batchRow -= *batchSize
					offset += int64(*batchSize)
				}
				zap.S().Info(fmt.Sprintf("ServiceName: %s \nMigrated till: %s \nTimeNano: %v \n_________**********************************_________", service.ServiceName, time.Unix(0, int64(start-uint64(timePeriod))), start-uint64(timePeriod)))
				start -= timePeriod
			} else {
				batchSpans, err := readSpans(conn, service.ServiceName, start, start-timePeriod, -1, -1)
				if err != nil {
					zap.S().Fatal(err, "error while reading spans")
					return
				}
				if len(batchSpans) > 0 {
					processedSpans := processSpans(batchSpans)
					err = write(conn, processedSpans)
					if err != nil {
						zap.S().Fatal(err, "error while writing spans")
						return
					}
					zap.S().Info(fmt.Sprintf("ServiceName: %s \nMigrated till: %s \nTimeNano: %v \n_________**********************************_________", service.ServiceName, time.Unix(0, int64(start-uint64(timePeriod))), start-uint64(timePeriod)))
				}
				start -= timePeriod
			}
		}
	}
	zap.S().Info(fmt.Sprintf("Completed migration in: %s", time.Since(start)))
	if *dropOldTable {
		dropOldTables(conn)
	}

}
