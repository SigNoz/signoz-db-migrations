package main

import (
	"context"
	"crypto/md5"
	"flag"
	"fmt"
	"log"
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
			fmt.Printf("Catch exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
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

func readTotalRows(conn clickhouse.Conn) (uint64, error) {
	ctx := context.Background()
	result := []DBResponseTotal{}
	if err := conn.Select(ctx, &result, "SELECT count() as numTotal FROM signoz_traces.signoz_error_index"); err != nil {
		return 0, err
	}
	if len(result) == 0 {
		fmt.Println("Total Rows: ", result[0].NumTotal)
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

func readSpans(conn clickhouse.Conn, serviceName string, endTime uint64, startTime uint64) ([]SignozErrorIndex, error) {
	ctx := context.Background()
	result := []SignozErrorIndex{}
	te := fmt.Sprintf("SELECT * FROM signoz_traces.signoz_error_index where serviceName='%s' AND timestamp>= '%v' AND timestamp<= '%v'", serviceName, startTime, endTime)
	if err := conn.Select(ctx, &result, te); err != nil {
		return nil, err
	}
	return result, nil
}

func write(conn clickhouse.Conn, batchSpans []SignozErrorIndexV2) error {
	fmt.Printf("Writing %v rows\n", len(batchSpans))
	err := writeErrorIndexV2(conn, batchSpans)
	return err
}

func writeErrorIndexV2(conn clickhouse.Conn, batchSpans []SignozErrorIndexV2) error {
	ctx := context.Background()
	statement, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO signoz_traces.signoz_error_index_v2"))
	for _, span := range batchSpans {
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
	fmt.Println("Dropping signoz_error_index table")
	err := conn.Exec(ctx, "DROP TABLE IF EXISTS signoz_traces.signoz_error_index")
	if err != nil {
		log.Fatal(err, " Error dropping signoz_error_index_v2 table")
	}
	fmt.Println("Successfully dropped signoz_error_index")
}

func parseTTL(queryResp string) int {

	zap.S().Debugf("Parsing TTL from: %s", queryResp)
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
	fmt.Printf("Setting TTL to %v\nSetting TTL might take very long depending upon data and resources, please be patient....\n", delTTL)
	tableName := "signoz_traces.signoz_error_index_v2"
	req := fmt.Sprintf(
		"ALTER TABLE %v MODIFY TTL toDateTime(timestamp) + INTERVAL %v SECOND DELETE",
		tableName, delTTL*3600)

	zap.S().Debugf("Executing TTL request: %s\n", req)
	if err := conn.Exec(context.Background(), req); err != nil {
		zap.S().Error(fmt.Errorf("Error in executing set TTL query: %s", err.Error()))
		return err
	}
	return nil
}

func main() {
	start := time.Now()
	timePeriod := uint64(60000000000) // seconds
	serviceFlag := flag.String("service", "", "serviceName")
	timeFlag := flag.Uint64("timeNano", 0, "timestamp in nano seconds")
	hostFlag := flag.String("host", "127.0.0.1", "clickhouse host")
	portFlag := flag.String("port", "9000", "clickhouse port")
	userNameFlag := flag.String("userName", "default", "clickhouse username")
	passwordFlag := flag.String("password", "", "clickhouse password")
	dropOldTable := flag.Bool("dropOldTable", true, "clear old clickhouse data if migration was successful")
	flag.Parse()
	fmt.Println(*hostFlag, *portFlag, *userNameFlag, *passwordFlag)

	conn, err := connect(*hostFlag, *portFlag, *userNameFlag, *passwordFlag)
	if err != nil {
		log.Fatal(err, " Error while connecting to clickhouse")
		return
	}
	moveTTL, err := getTracesTTL(conn)
	if err == nil && moveTTL > 1 {
		setTracesTTL(conn, moveTTL)
	} else {
		fmt.Println("No TTL found, skipping TTL migration")
	}
	rows, err := readTotalRows(conn)
	if err != nil {
		log.Fatal(err, " error while reading total rows")
		return
	}
	if rows == 0 {
		fmt.Println("No data found in clickhouse")
		return
	}
	fmt.Printf("There are total %v rows, starting migration... \n", rows)
	services, err := readServices(conn)
	if err != nil {
		log.Fatal(err, " error while reading services")
		return
	}
	skip := true

	for _, service := range services {
		start := uint64(service.Maxt.UnixNano())

		if *serviceFlag != "" && skip {
			if service.ServiceName != *serviceFlag {
				continue
			} else {
				fmt.Println("Starting from service: ", service.ServiceName)
				skip = false
				if *timeFlag != 0 {
					start = *timeFlag
					fmt.Printf("\nProcessing remaining rows of serviceName: %s and Timestamp: %s \n", service.ServiceName, time.Unix(0, int64(start)))
				}
			}
		}
		if skip {
			fmt.Printf("\nProcessing %v rows of serviceName %s \n", service.NumTotal, service.ServiceName)
		}
		rps := service.NumTotal / ((uint64(service.Maxt.Unix()) - uint64(service.Mint.Unix())) + 1)
		// fmt.Printf("\nRPS: %v \n", rps)
		timePeriod = 70000000000000 / (rps + 1)
		// fmt.Printf("\nTime Period: %v \n", timePeriod)
		for start >= uint64(service.Mint.UnixNano()) {
			batchSpans, err := readSpans(conn, service.ServiceName, start, start-timePeriod)
			if err != nil {
				log.Fatal(err, " error while reading spans")
				return
			}
			if len(batchSpans) > 0 {
				processedSpans := processSpans(batchSpans)
				err = write(conn, processedSpans)
				if err != nil {
					log.Fatal(err, " error while writing spans")
					return
				}
				fmt.Printf("ServiceName: %s \nMigrated till: %s \nTimeNano: %v \n_________**********************************_________ \n", service.ServiceName, time.Unix(0, int64(start-uint64(timePeriod))), start-uint64(timePeriod))
			}
			start -= timePeriod
		}
	}
	fmt.Println("Completed migration in: ", time.Since(start))
	if *dropOldTable {
		dropOldTables(conn)
	}

}
