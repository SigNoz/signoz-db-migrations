package main

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"go.uber.org/zap"
)

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

func updateTTL(conn clickhouse.Conn, cluster string, delTTL, moveTTL int, coldVolume string) error {
	if delTTL != -1 {
		q := "ALTER TABLE signoz_traces.signoz_index_v3 ON CLUSTER " + cluster + " MODIFY TTL toDateTime(timestamp) + " +
			fmt.Sprintf("INTERVAL %v SECOND DELETE", delTTL)
		qS := "ALTER TABLE signoz_traces.trace_summary ON CLUSTER " + cluster + " MODIFY TTL toDateTime(end) + " +
			fmt.Sprintf("INTERVAL %v SECOND DELETE", delTTL)
		qRes := "ALTER TABLE signoz_traces.traces_v3_resource ON CLUSTER " + cluster + " MODIFY TTL toDateTime(seen_at_ts_bucket_start) + toIntervalSecond(1800) + " +
			fmt.Sprintf("INTERVAL %v SECOND DELETE", delTTL)

		if moveTTL != -1 {
			q += fmt.Sprintf(", toDateTime(timestamp) + INTERVAL %v SECOND TO VOLUME '%s'", moveTTL, coldVolume)
			qRes += fmt.Sprintf(", toDateTime(seen_at_ts_bucket_start) + toIntervalSecond(1800) + INTERVAL %v SECOND TO VOLUME '%s'", moveTTL, coldVolume)

			for _, table := range []string{"signoz_index_v3", "traces_v3_resource"} {
				policyReq := fmt.Sprintf("ALTER TABLE signoz_traces.%s ON CLUSTER %s MODIFY SETTING storage_policy='tiered'", table, cluster)
				err := conn.Exec(context.Background(), policyReq)
				if err != nil {
					zap.S().Error(fmt.Errorf("error while setting storage policy. Err=%v", err))
					return err
				}
			}
		}

		q += " SETTINGS materialize_ttl_after_modify = 0"
		qRes += " SETTINGS materialize_ttl_after_modify = 0"
		qS += " SETTINGS materialize_ttl_after_modify = 0"

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
		err = conn.Exec(context.Background(), qS)
		if err != nil {
			zap.S().Error(fmt.Errorf("error while updating ttl for summary table. Err=%v", err))
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
	cluster := flag.String("cluster", "cluster", "clickhouse cluster")
	flag.Parse()
	zap.S().Debug(fmt.Sprintf("Params: %s %s %s", *hostFlag, *portFlag, *userNameFlag))

	conn, err := connect(*hostFlag, *portFlag, *userNameFlag, *passwordFlag)
	if err != nil {
		zap.S().Fatal("Error while connecting to clickhouse", zap.Error(err))
	}

	// get the ttl of the old table
	delOld, moveOld, oldColdStorageName, err := GetTTL(conn, "signoz_index_v2")
	if err != nil {
		zap.S().Error(err.Error())
	}
	fmt.Println(delOld, moveOld, oldColdStorageName)

	// get the ttl of the current table
	delCurr, moveCurr, _, err := GetTTL(conn, "signoz_index_v3")
	if err != nil {
		zap.S().Error(err.Error())
	}

	fmt.Println(delCurr, moveCurr)

	// if current table cold storage is configured don't change it.
	if moveCurr != -1 {
		zap.S().Info("Since cold storage is already set not changing it")
	}

	// if ttl is same don't change it
	if delOld == delCurr && moveOld == moveCurr {
		zap.S().Info("no change is required, TTL is same")
	} else {
		zap.S().Info(fmt.Sprintf("Setting TTL values: delete:%v, move: %v, coldStorage: %s", delOld, moveOld, oldColdStorageName))
		// update the TTL
		err = updateTTL(conn, *cluster, delOld, moveOld, oldColdStorageName)
		if err != nil {
			zap.S().Error(err.Error())
		}
	}
	zap.S().Info(fmt.Sprintf("Completed migration in: %s", time.Since(start)))
}
