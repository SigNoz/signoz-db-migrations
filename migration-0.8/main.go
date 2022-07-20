package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"regexp"
	"strconv"
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

type Event struct {
	Name         string            `json:"name,omitempty"`
	TimeUnixNano uint64            `json:"timeUnixNano,omitempty"`
	AttributeMap map[string]string `json:"attributeMap,omitempty"`
	IsError      bool              `json:"isError,omitempty"`
}

type TraceModel struct {
	TraceId           string            `json:"traceId,omitempty"`
	SpanId            string            `json:"spanId,omitempty"`
	Name              string            `json:"name,omitempty"`
	DurationNano      uint64            `json:"durationNano,omitempty"`
	StartTimeUnixNano uint64            `json:"startTimeUnixNano,omitempty"`
	ServiceName       string            `json:"serviceName,omitempty"`
	Kind              int8              `json:"kind,omitempty"`
	References        string            `json:"references,omitempty"`
	StatusCode        int16             `json:"statusCode,omitempty"`
	TagMap            map[string]string `json:"tagMap,omitempty"`
	Events            []string          `json:"event,omitempty"`
	HasError          bool              `json:"hasError,omitempty"`
}

type SignozIndex struct {
	Timestamp          time.Time         `ch:"timestamp" json:"timestamp"`
	SpanID             string            `ch:"spanID" json:"spanID"`
	TraceID            string            `ch:"traceID" json:"traceID"`
	ParentSpanID       string            `ch:"parentSpanID"`
	ServiceName        string            `ch:"serviceName" json:"serviceName"`
	Name               string            `ch:"name"`
	Kind               int32             `ch:"kind"`
	StatusCode         int64             `ch:"statusCode"`
	ExternalHttpMethod string            `ch:"externalHttpMethod"`
	ExternalHttpUrl    string            `ch:"externalHttpUrl"`
	Component          string            `ch:"component"`
	DbSystem           string            `ch:"dbSystem"`
	DbOperation        string            `ch:"dbOperation"`
	DbName             string            `ch:"dbName"`
	PeerService        string            `ch:"peerService"`
	Events             []string          `ch:"events"`
	Tags               []string          `ch:"tags"`
	TagsKeys           []string          `ch:"tagsKeys"`
	TagsValues         []string          `ch:"tagsValues"`
	References         string            `ch:"references"`
	HasError           int32             `ch:"hasError"`
	DurationNano       uint64            `ch:"durationNano"`
	HttpCode           string            `ch:"httpCode"`
	HttpMethod         string            `ch:"httpMethod"`
	HttpUrl            string            `ch:"httpUrl"`
	HttpRoute          string            `ch:"httpRoute"`
	HttpHost           string            `ch:"httpHost"`
	GRPCode            string            `ch:"gRPCCode"`
	GRPMethod          string            `ch:"gRPCMethod"`
	MsgSystem          string            `ch:"msgSystem"`
	MsgOperation       string            `ch:"msgOperation"`
	TagMap             map[string]string `ch:"tagMap"`
}

type SignozIndexV2 struct {
	TraceId            string            `json:"traceId,omitempty"`
	SpanId             string            `json:"spanId,omitempty"`
	ParentSpanId       string            `json:"parentSpanId,omitempty"`
	Name               string            `json:"name,omitempty"`
	DurationNano       uint64            `json:"durationNano,omitempty"`
	StartTimeUnixNano  uint64            `json:"startTimeUnixNano,omitempty"`
	ServiceName        string            `json:"serviceName,omitempty"`
	Kind               int8              `json:"kind,omitempty"`
	StatusCode         int16             `json:"statusCode,omitempty"`
	ExternalHttpMethod string            `json:"externalHttpMethod,omitempty"`
	HttpUrl            string            `json:"httpUrl,omitempty"`
	HttpMethod         string            `json:"httpMethod,omitempty"`
	HttpHost           string            `json:"httpHost,omitempty"`
	HttpRoute          string            `json:"httpRoute,omitempty"`
	HttpCode           string            `json:"httpCode,omitempty"`
	MsgSystem          string            `json:"msgSystem,omitempty"`
	MsgOperation       string            `json:"msgOperation,omitempty"`
	ExternalHttpUrl    string            `json:"externalHttpUrl,omitempty"`
	Component          string            `json:"component,omitempty"`
	DBSystem           string            `json:"dbSystem,omitempty"`
	DBName             string            `json:"dbName,omitempty"`
	DBOperation        string            `json:"dbOperation,omitempty"`
	PeerService        string            `json:"peerService,omitempty"`
	Events             []string          `json:"event,omitempty"`
	TagMap             map[string]string `json:"tagMap,omitempty"`
	HasError           bool              `json:"hasError,omitempty"`
	GRPCCode           string            `json:"gRPCCode,omitempty"`
	GRPCMethod         string            `json:"gRPCMethod,omitempty"`
	TraceModel         TraceModel        `json:"traceModel,omitempty"`
	ErrorEvent         Event             `json:"errorEvent,omitempty"`
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

func processSpans(data []SignozIndex) []SignozIndexV2 {
	processedSpans := []SignozIndexV2{}
	for _, span := range data {
		traceModel := TraceModel{
			TraceId:           span.TraceID,
			SpanId:            span.SpanID,
			Name:              span.Name,
			DurationNano:      span.DurationNano,
			StartTimeUnixNano: uint64(span.Timestamp.Unix()),
			ServiceName:       span.ServiceName,
			Kind:              int8(span.Kind),
			References:        span.References,
			StatusCode:        int16(span.StatusCode),
			TagMap:            span.TagMap,
			Events:            span.Events,
			HasError:          intToBool(span.HasError),
		}
		processedData := SignozIndexV2{
			StartTimeUnixNano:  uint64(span.Timestamp.UnixNano()),
			TraceId:            span.TraceID,
			SpanId:             span.SpanID,
			ParentSpanId:       span.ParentSpanID,
			ServiceName:        span.ServiceName,
			Name:               span.Name,
			Kind:               int8(span.Kind),
			DurationNano:       span.DurationNano,
			StatusCode:         int16(span.StatusCode),
			ExternalHttpMethod: span.ExternalHttpMethod,
			ExternalHttpUrl:    span.ExternalHttpUrl,
			Component:          span.Component,
			DBSystem:           span.DbSystem,
			DBName:             span.DbName,
			DBOperation:        span.DbOperation,
			PeerService:        span.PeerService,
			Events:             span.Events,
			HttpMethod:         span.HttpMethod,
			HttpUrl:            span.HttpUrl,
			HttpRoute:          span.HttpRoute,
			HttpHost:           span.HttpHost,
			HttpCode:           span.HttpCode,
			MsgSystem:          span.MsgSystem,
			MsgOperation:       span.MsgOperation,
			TagMap:             span.TagMap,
			HasError:           intToBool(span.HasError),
			TraceModel:         traceModel,
		}
		processedSpans = append(processedSpans, processedData)
	}

	return processedSpans
}

func intToBool(i int32) bool {
	if i == 0 {
		return false
	}
	return true
}
func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func readTotalRows(conn clickhouse.Conn) (uint64, error) {
	ctx := context.Background()
	result := []DBResponseTotal{}
	if err := conn.Select(ctx, &result, "SELECT count() as numTotal FROM signoz_index"); err != nil {
		return 0, err
	}
	fmt.Println("Total Rows: ", result[0].NumTotal)
	return result[0].NumTotal, nil
}

func readServices(conn clickhouse.Conn) ([]DBResponseServices, error) {
	ctx := context.Background()
	result := []DBResponseServices{}
	if err := conn.Select(ctx, &result, "SELECT serviceName, MIN(timestamp) as mint, MAX(timestamp) as maxt, count() as numTotal FROM signoz_index group by serviceName order by serviceName"); err != nil {
		return nil, err
	}
	return result, nil
}

func readSpans(conn clickhouse.Conn, serviceName string, endTime uint64, startTime uint64) ([]SignozIndex, error) {
	ctx := context.Background()
	result := []SignozIndex{}
	te := fmt.Sprintf("SELECT * FROM signoz_index where serviceName='%s' AND timestamp>= '%v' AND timestamp<= '%v'", serviceName, startTime, endTime)
	if err := conn.Select(ctx, &result, te); err != nil {
		return nil, err
	}
	return result, nil
}

func write(conn clickhouse.Conn, batchSpans []SignozIndexV2) error {
	fmt.Printf("Writing %v rows\n", len(batchSpans))
	err := writeIndex(conn, batchSpans)
	err = writeModel(conn, batchSpans)
	return err
}

func writeIndex(conn clickhouse.Conn, batchSpans []SignozIndexV2) error {
	ctx := context.Background()
	statement, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO signoz_traces.signoz_index_v2"))
	if err != nil {
		log.Println("Error preparing statement of write index: ", err)
		return err
	}
	for _, span := range batchSpans {
		log.Println("Span: ", span)
		unixNano := time.Unix(0, int64(span.StartTimeUnixNano))
		log.Println("Unix: ", unixNano)
		err = statement.Append(
			unixNano,
			span.TraceId,
			span.SpanId,
			span.ParentSpanId,
			span.ServiceName,
			span.Name,
			span.Kind,
			span.DurationNano,
			span.StatusCode,
			span.ExternalHttpMethod,
			span.ExternalHttpUrl,
			span.Component,
			span.DBSystem,
			span.DBName,
			span.DBOperation,
			span.PeerService,
			span.Events,
			span.HttpMethod,
			span.HttpUrl,
			span.HttpCode,
			span.HttpRoute,
			span.HttpHost,
			span.MsgSystem,
			span.MsgOperation,
			span.HasError,
			span.TagMap,
			span.GRPCMethod,
			span.GRPCCode,
		)
		if err != nil {
			return err
		}
	}

	return statement.Send()
}

func writeModel(conn clickhouse.Conn, batchSpans []SignozIndexV2) error {
	ctx := context.Background()
	statement, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO signoz_traces.signoz_spans"))
	if err != nil {
		log.Println("Error preparing statement of write model: ", err)
		return err
	}
	for _, span := range batchSpans {
		var serialized []byte

		serialized, err = json.Marshal(span.TraceModel)

		if err != nil {
			return err
		}

		err = statement.Append(time.Unix(0, int64(span.StartTimeUnixNano)), span.TraceId, string(serialized))
		if err != nil {
			return err
		}
	}

	return statement.Send()
}

func dropOldTables(conn clickhouse.Conn) {
	ctx := context.Background()
	fmt.Println("Dropping signoz_index table")
	err := conn.Exec(ctx, "DROP TABLE IF EXISTS signoz_index")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Successfully dropped signoz_index")
	fmt.Println("Dropping signoz_error_index table")
	err = conn.Exec(ctx, "DROP TABLE IF EXISTS signoz_error_index")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Successfully dropped signoz_error_index")
}

func parseTTL(queryResp string) (int, int) {

	zap.S().Debugf("Parsing TTL from: %s", queryResp)
	deleteTTLExp := regexp.MustCompile(`toIntervalSecond\(([0-9]*)\)`)
	moveTTLExp := regexp.MustCompile(`toIntervalSecond\(([0-9]*)\) TO VOLUME`)

	var delTTL, moveTTL int = -1, -1

	m := deleteTTLExp.FindStringSubmatch(queryResp)
	if len(m) > 1 {
		seconds_int, err := strconv.Atoi(m[1])
		if err != nil {
			return -1, -1
		}
		delTTL = seconds_int / 3600
	}

	m = moveTTLExp.FindStringSubmatch(queryResp)
	if len(m) > 1 {
		seconds_int, err := strconv.Atoi(m[1])
		if err != nil {
			return -1, -1
		}
		moveTTL = seconds_int / 3600
	}

	return delTTL, moveTTL
}

func getTracesTTL(conn clickhouse.Conn) (*DBResponseTTL, error) {
	var dbResp []DBResponseTTL
	ctx := context.Background()
	query := fmt.Sprintf("SELECT engine_full FROM system.tables WHERE name='%v'", "signoz_index")

	err := conn.Select(ctx, &dbResp, query)

	if err != nil {
		zap.S().Error(fmt.Errorf("error while getting ttl. Err=%v", err))
		return nil, err
	}
	if len(dbResp) == 0 {
		return nil, nil
	} else {
		return &dbResp[0], nil
	}
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
		log.Fatal(err)
	}

	rows, err := readTotalRows(conn)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("There are total %v rows, starting migration... \n", rows)
	services, err := readServices(conn)
	if err != nil {
		log.Fatal(err)
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
				log.Fatal(err)
			}
			if len(batchSpans) > 0 {
				processedSpans := processSpans(batchSpans)
				err = write(conn, processedSpans)
				if err != nil {
					log.Fatal(err)
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
