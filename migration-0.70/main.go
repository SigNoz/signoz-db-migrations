package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	_ "github.com/mattn/go-sqlite3"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"golang.org/x/sync/errgroup"
	"migration-0.70/helpers"
	internal "migration-0.70/internal"
)

const (
	signozMetricDBName       = "signoz_metrics"
	signozSampleTableName    = "distributed_samples_v4"
	signozTSTableNameV4      = "distributed_time_series_v4"
	signozTSTableNameV41Week = "distributed_time_series_v4_1week"
)

type ts struct {
	Env           string            `ch:"env"`
	Temporality   string            `ch:"temporality"`
	MetricName    string            `ch:"metric_name"`
	Description   string            `ch:"description"`
	Unit          string            `ch:"unit"`
	Type          string            `ch:"type"`
	IsMonotonic   bool              `ch:"is_monotonic"`
	Fingerprint   uint64            `ch:"fingerprint"`
	UnixMilli     int64             `ch:"unix_milli"`
	Labels        string            `ch:"labels"`
	Attrs         map[string]string `ch:"attrs"`
	ScopeAttrs    map[string]string `ch:"scope_attrs"`
	ResourceAttrs map[string]string `ch:"resource_attrs"`
	Normalized    bool              `ch:"__normalized"`
}

type metricSample struct {
	env         string                         `ch:"env"`
	temporality pmetric.AggregationTemporality `ch:"temporality"`
	metricName  string                         `ch:"metric_name"`
	fingerprint uint64                         `ch:"fingerprint"`
	unixMilli   int64                          `ch:"unix_milli"`
	value       float64                        `ch:"value"`
	flags       uint32                         `ch:"flags"`
}

func getClickhouseConn(pool int) (clickhouse.Conn, error) {
	cfg := helpers.LoadClickhouseConfig()
	opts := helpers.NewClickHouseOptions(cfg)
	opts.MaxOpenConns = pool
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse connection: %w", err)
	}
	// Verify connection is alive
	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		var exception *clickhouse.Exception
		if errors.As(err, &exception) {
			return nil, fmt.Errorf("[ClickHouse][%d] %s\n%s", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}
	return conn, nil
}

// struct for workers to get corresponding attributes
type metricJob struct {
	normMetricName   string
	unNormMetricName string
}

func commonPreRun(maxOpenConns int) (clickhouse.Conn, map[string]helpers.MetricResult, map[string]string, error) {
	conn, err := getClickhouseConn(maxOpenConns)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("error connecting to ClickHouse: %w", err)
	}
	// 1) fetch normalized metrics
	metrics, missing, err := GetCorrespondingNormalizedMetrics(conn)
	if err != nil {
		conn.Close()
		return nil, nil, nil, fmt.Errorf("error getting metric names: %w", err)
	}
	// overlay fallback maps
	defaultMetrics := map[string]string{
		"certmanager_http_acme_client_request_duration_seconds":     "certmanager_http_acme_client_request_duration_seconds",
		"redis_latency_percentiles_usec":                            "redis_latency_percentiles_usec",
		"go_gc_duration_seconds":                                    "go_gc_duration_seconds",
		"nginx_ingress_controller_ingress_upstream_latency_seconds": "nginx_ingress_controller_ingress_upstream_latency_seconds",
	}
	defaultAttr := map[string]string{
		"k8s_node_name": "k8s.node.name",
		"quantile":      "quantile",
	}
	notFoundMetricsMap := helpers.OverlayFromEnv(defaultMetrics, "NOT_FOUND_METRICS_MAP")
	for _, m := range missing {
		if v, ok := notFoundMetricsMap[m]; ok {
			metrics[m] = v
		} else {
			conn.Close()
			return nil, nil, nil, fmt.Errorf("missing metrics map entry for %s", m)
		}
	}
	// 2) build attribute map
	notFoundAttrMap := helpers.OverlayFromEnv(defaultAttr, "NOT_FOUND_ATTR_MAP")
	metricDetails, attrMap, err := buildMetricDetails(conn, metrics, notFoundAttrMap)
	if err != nil {
		conn.Close()
		return nil, nil, nil, fmt.Errorf("error building metric details: %w", err)
	}
	return conn, metricDetails, attrMap, nil
}

func migrateHighRetention(maxOpenConns, workers int) error {
	conn, metricDetails, _, err := commonPreRun(maxOpenConns)
	if err != nil {
		return err
	}
	defer conn.Close()

	// get timestamps
	firstTS, err := getFirstTimeStampforNormalizedData(conn)
	if err != nil {
		return fmt.Errorf("error getting first timestamp: %w", err)
	}
	lastTS, err := getfirstTimeStampforNonNormalizedData(conn)
	if err != nil {
		return fmt.Errorf("error getting last timestamp: %w", err)
	}

	reader := bufio.NewReader(os.Stdin)
	if firstTS < lastTS {
		fmt.Printf("Migrate data [%d…%d]? (yes): ", firstTS, lastTS)
		input, _ := reader.ReadString('\n')
		if strings.TrimSpace(input) != "yes" {
			return nil
		}
		resAttrs, scopeAttrs, pointAttrs, err := getAllDifferentMetricsAttributes(conn)
		if err != nil {
			return fmt.Errorf("error getting all attributes: %w", err)
		}
		g, ctx := errgroup.WithContext(context.Background())
		sem := make(chan struct{}, workers)
		for t := firstTS; t < lastTS; t += 3600 {
			start, end := t, min(t+3600, lastTS)
			sem <- struct{}{}
			g.Go(func() error {
				defer func() { <-sem }()
				return fetchAndInsertTimeSeriesV4(ctx, conn, start, end, metricDetails, resAttrs, scopeAttrs, pointAttrs)
			})
		}
		if err := g.Wait(); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
		log.Printf("Data migration completed [%d…%d]", firstTS, lastTS)
	}
	return nil
}

func migrateMeta(maxOpenConns int, dbPath string) error {
	conn, metricDetails, attrMap, err := commonPreRun(maxOpenConns)
	if err != nil {
		return err
	}
	defer conn.Close()
	//reader := bufio.NewReader(os.Stdin)
	//fmt.Printf("Migrate alerts & dashboards in %s? (yes): ", dbPath)
	//input, _ := reader.ReadString('\n')
	//if strings.TrimSpace(input) != "yes" {
	//	return nil
	//}
	//orig := dbPath
	//copyDB := filepath.Join(filepath.Dir(orig), filepath.Base(orig)+".copy")
	//if err := copyFile(orig, copyDB); err != nil {
	//	return fmt.Errorf("failed to copy DB: %w", err)
	//}
	//fmt.Println("✅ copied DB to", dbPath)

	transformer := helpers.NewQueryTransformer(metricDetails)
	migrator := DashAlertsMigrator{queryTransformer: transformer}

	if err := migrator.migrateDashboards(metricDetails, attrMap, dbPath); err != nil {
		return fmt.Errorf("dashboards migration failed: %w", err)
	}
	if err := migrator.migrateRules(metricDetails, attrMap, dbPath); err != nil {
		return fmt.Errorf("rules migration failed: %w", err)
	}
	//if err := os.Remove(orig); err != nil {
	//	return fmt.Errorf("remove original DB: %w", err)
	//}
	//if err := os.Rename(copyDB, orig); err != nil {
	//	return fmt.Errorf("replace DB: %w", err)
	//}
	log.Printf("Alerts & dashboards migration completed in %s", dbPath)
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <migrate-data|migrate-meta> [flags]\n", os.Args[0])
		os.Exit(1)
	}
	cmd := os.Args[1]
	switch cmd {
	case "migrate-data":
		fs := flag.NewFlagSet("migrate-data", flag.ExitOnError)
		workers := fs.Int("workers", helpers.EnvOrInt("MIGRATE_WORKERS", 4), "hour-windows to process")
		maxOpen := fs.Int("max-open-conns", helpers.EnvOrInt("MIGRATE_MAX_OPEN_CONNS", 16), "ClickHouse pool size")
		if err := fs.Parse(os.Args[2:]); err != nil {
			log.Fatalf("failed to parse flags for %s: %v", cmd, err)
		}
		if err := migrateHighRetention(*maxOpen, *workers); err != nil {
			log.Fatalf("data migration failed: %v", err)
		}

	case "migrate-meta":
		fs := flag.NewFlagSet("migrate-meta", flag.ExitOnError)
		maxOpen := fs.Int("max-open-conns", helpers.EnvOrInt("MIGRATE_MAX_OPEN_CONNS", 16), "ClickHouse pool size")
		dbp := fs.String("db-path", helpers.EnvOr("SQL_DB_PATH", "./signoz.db"), "SQLite DB path")
		if err := fs.Parse(os.Args[2:]); err != nil {
			log.Fatalf("failed to parse flags for %s: %v", cmd, err)
		}
		if err := migrateMeta(*maxOpen, *dbp); err != nil {
			log.Fatalf("alerts/dashboard migration failed: %v", err)
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown command %q\n", cmd)
		os.Exit(2)
	}
}

type DashAlertsMigrator struct {
	queryTransformer *helpers.QueryTransformer
}

func buildMetricDetails(conn clickhouse.Conn, metrics map[string]string, notFoundAttrMap map[string]string) (map[string]helpers.MetricResult, map[string]string, error) {

	var workerCount = 4

	jobs := make(chan metricJob, len(metrics))
	results := make(chan helpers.MetricResult, len(metrics))

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				// do the work
				attrmap, normAttrs, unNormAttrs, err := checkAllAttributesOfTwoMetrics(conn, job.normMetricName, job.unNormMetricName)
				results <- helpers.MetricResult{job.normMetricName, job.unNormMetricName, normAttrs, unNormAttrs, attrmap, err}
			}
		}()
	}

	// enqueue all jobs
	go func() {
		for k, n := range metrics {
			jobs <- metricJob{k, n}
		}
		close(jobs)
	}()

	// once all workers are done, close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// --- collect results ---
	validMetrics := make(map[string]string, len(metrics))
	nonValidMetrics := make(map[string]string, len(metrics))
	var mu sync.Mutex
	metricDetails := make(map[string]helpers.MetricResult)
	allAttributeMap := make(map[string]string)
	for res := range results {
		metricDetails[res.NormMetricName] = res
		if res.Err != nil {
			log.Fatalf("error checking metric %s → %s: %v", res.NormMetricName, res.UnNormMetricName, res.Err)
		} else {
			//log.Printf("metrics name %s -> %s", res.key, res.name) // uncomment this line to check for valid metrics.
		}
		allAttributeMap = helpers.MergeMaps(allAttributeMap, res.NormToUnNormAttrMap)
		switch {
		case len(res.NormAttributes) == 0 && len(res.UnNormAttributes) == 0:
			mu.Lock()
			validMetrics[res.NormMetricName] = res.UnNormMetricName
			//log.Printf("valid metric name: %s to %s", res.key, res.name)
			mu.Unlock()

		default:
			// anything else is "non-valid"
			mu.Lock()
			nonValidMetrics[res.NormMetricName] = res.UnNormMetricName
			mu.Unlock()

			// still log the details for visibility
			if len(res.NormAttributes) > 0 {
				log.Printf("extra attributes in underscore metric %s: %v", res.NormMetricName, res.NormAttributes)
			}
			if len(res.UnNormAttributes) > 0 {
				log.Printf("extra attributes in dot metric %s: %v", res.UnNormMetricName, res.UnNormAttributes)
			}
		}
	}

	notFound := make(map[string]struct{})

	for _, metricDetail := range metricDetails {
		if len(metricDetail.NormAttributes) != 0 {
			for _, attr := range metricDetail.NormAttributes {
				if _, ok := allAttributeMap[attr]; !ok {
					if _, ok := notFoundAttrMap[attr]; !ok {
						notFound[attr] = struct{}{}
					} else {
						allAttributeMap[attr] = notFoundAttrMap[attr]
						metricDetail.NormToUnNormAttrMap[attr] = notFoundAttrMap[attr]
					}
				} else {
					metricDetail.NormToUnNormAttrMap[attr] = allAttributeMap[attr]
				}
			}
		}
	}

	for k, v := range notFoundAttrMap {
		allAttributeMap[k] = v
	}

	if len(notFound) > 0 {
		return nil, nil, fmt.Errorf("attributes not found in any metrics: %v", notFound)
	}

	//for not found metric

	log.Printf("metrics ready for conversion: %+v", validMetrics)

	return metricDetails, allAttributeMap, nil
}

func getAllDifferentMetricsAttributes(conn clickhouse.Conn) (map[string]struct{}, map[string]struct{}, map[string]struct{}, error) {
	query := fmt.Sprintf(`SELECT
    arraySort(groupUniqArrayIf(attr_name, attr_type = 'resource')) AS resource_attrs,
    arraySort(groupUniqArrayIf(attr_name, attr_type = 'scope'   )) AS scope_attrs,
    arraySort(groupUniqArrayIf(attr_name, attr_type = 'point'   )) AS point_attrs
FROM signoz_metrics.distributed_metadata`)
	ctx := cappedCHContext(context.Background())
	rows, err := conn.Query(ctx, query)
	if err != nil {
		log.Fatalf("error getting all metric attributes: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if rows.Err() != nil {
			return nil, nil, nil, rows.Err()
		}
		return nil, nil, nil, fmt.Errorf("no data for metric %q", query)
	}

	var resSlice, scopeSlice, attrSlice []string
	if err := rows.Scan(&resSlice, &scopeSlice, &attrSlice); err != nil {
		return nil, nil, nil, fmt.Errorf("scan failed: %w", err)
	}

	toSet := func(xs []string) map[string]struct{} {
		m := make(map[string]struct{}, len(xs))
		for _, x := range xs {
			m[x] = struct{}{}
		}
		return m
	}

	return toSet(resSlice), toSet(scopeSlice), toSet(attrSlice), nil
}

func fetchAndInsertTimeSeriesV4(ctx context.Context, conn clickhouse.Conn, start, end int64, metricDetails map[string]helpers.MetricResult, allResourceAttrs, allScopeAttrs, allPointAttrs map[string]struct{}) error {
	const maxRowsPerBatch = 1_000_000

	ctx = cappedCHContext(ctx)

	queryTS := fmt.Sprintf(`
		SELECT
			env,
			temporality,
			metric_name,
			description,
			unit,
			type,
			is_monotonic,
			fingerprint,
			unix_milli,
			labels,
			attrs,
			scope_attrs,
			resource_attrs,
			__normalized
		FROM %s.%s
		WHERE __normalized = true
		  AND unix_milli >= ? AND  unix_milli < ?
		ORDER BY unix_milli`,
		signozMetricDBName, signozTSTableNameV4)

	rowsTS, err := conn.Query(ctx, queryTS, start, end)
	if err != nil {
		return fmt.Errorf("time-series query: %w", err)
	}
	defer rowsTS.Close()

	/* helpers that always give you a fresh batch */
	newTSBatch := func() (driver.Batch, error) {
		return conn.PrepareBatch(ctx,
			fmt.Sprintf("INSERT INTO %s.%s",
				signozMetricDBName, signozTSTableNameV4))
	}
	newSamplesBatch := func() (driver.Batch, error) {
		return conn.PrepareBatch(ctx,
			fmt.Sprintf("INSERT INTO %s.%s",
				signozMetricDBName, signozSampleTableName))
	}

	tsBatch, err := newTSBatch()
	if err != nil {
		return err
	}
	rowsInTSBatch := 0

	/* collected while streaming, needed later for samples-lookup */
	fingerprintMap := make(map[uint64]uint64)

	/* --- stream rows --------------------------------------------------- */
	for rowsTS.Next() {
		var norm ts
		if err := rowsTS.Scan(
			&norm.Env,
			&norm.Temporality,
			&norm.MetricName,
			&norm.Description,
			&norm.Unit,
			&norm.Type,
			&norm.IsMonotonic,
			&norm.Fingerprint,
			&norm.UnixMilli,
			&norm.Labels,
			&norm.Attrs,
			&norm.ScopeAttrs,
			&norm.ResourceAttrs,
			&norm.Normalized,
		); err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		labelMap := make(map[string]string)
		if err := json.Unmarshal([]byte(norm.Labels), &labelMap); err != nil {
			return fmt.Errorf("labels unmarshal: %w", err)
		}

		md := metricDetails[norm.MetricName]

		resourceAttrs := make(map[string]string)
		scopeAttrs := make(map[string]string)
		pointAttrs := make(map[string]string)

		for k, v := range labelMap {
			clean := md.NormToUnNormAttrMap[k]

			if _, ok := allResourceAttrs[clean]; ok {
				resourceAttrs[clean] = v

			} else if _, ok := allScopeAttrs[clean]; ok {
				scopeAttrs[clean] = v

			} else if _, ok := allPointAttrs[clean]; ok {
				pointAttrs[clean] = v
			}
		}

		resFP := internal.NewFingerprint(internal.ResourceFingerprintType,
			internal.InitialOffset, attrsToPMap(resourceAttrs), nil)
		scopeFP := internal.NewFingerprint(internal.ScopeFingerprintType,
			resFP.Hash(), attrsToPMap(scopeAttrs), nil)

		temporality := toOtelTemporality(norm.Temporality)

		pointFP := internal.NewFingerprint(
			internal.PointFingerprintType,
			scopeFP.Hash(),
			attrsToPMap(pointAttrs),
			map[string]string{"__temporality__": temporality.String()},
		)

		var unNorm ts
		unNorm.Env = norm.Env
		unNorm.Temporality = norm.Temporality
		unNorm.MetricName = md.UnNormMetricName
		unNorm.Description = norm.Description
		unNorm.Unit = norm.Unit
		unNorm.Type = norm.Type
		unNorm.IsMonotonic = norm.IsMonotonic
		unNorm.Fingerprint = pointFP.HashWithName(unNorm.MetricName)
		unNorm.UnixMilli = norm.UnixMilli
		unNorm.Normalized = false
		unNorm.ScopeAttrs = scopeFP.AttributesAsMap()
		unNorm.ResourceAttrs = resFP.AttributesAsMap()
		unNorm.Attrs = pointFP.AttributesAsMap()
		unNorm.Labels = internal.NewLabelsAsJSONString(
			unNorm.MetricName, unNorm.Attrs,
			unNorm.ScopeAttrs, unNorm.ResourceAttrs)

		fingerprintMap[norm.Fingerprint] = unNorm.Fingerprint

		/* batch INSERT --------------------------------------------------- */
		if err := tsBatch.Append(
			unNorm.Env,
			unNorm.Temporality,
			unNorm.MetricName,
			unNorm.Description,
			unNorm.Unit,
			unNorm.Type,
			unNorm.IsMonotonic,
			unNorm.Fingerprint,
			unNorm.UnixMilli,
			unNorm.Labels,
			unNorm.Attrs,
			unNorm.ScopeAttrs,
			unNorm.ResourceAttrs,
			unNorm.Normalized,
		); err != nil {
			return fmt.Errorf("append TS: %w", err)
		}
		rowsInTSBatch++
		if rowsInTSBatch == maxRowsPerBatch {
			if err := tsBatch.Send(); err != nil {
				return fmt.Errorf("flush TS batch: %w", err)
			}
			tsBatch, err = newTSBatch()
			if err != nil {
				return err
			}
			rowsInTSBatch = 0
		}
	}
	if err := rowsTS.Err(); err != nil {
		return err
	}
	/* flush any remainder */
	if rowsInTSBatch > 0 {
		if err := tsBatch.Send(); err != nil {
			return fmt.Errorf("final TS flush: %w", err)
		}
	}

	/* ------------------------------------------------------------------
	   2) READ & WRITE THE SAMPLE ROWS (same batching pattern)
	   ------------------------------------------------------------------ */
	if len(fingerprintMap) == 0 {
		return nil
	}

	fps := make([]uint64, 0, len(fingerprintMap))
	for fp := range fingerprintMap {
		fps = append(fps, fp)
	}

	querySamples := fmt.Sprintf(`
		    SELECT
        env,
        temporality,
        metric_name,
        fingerprint,
        unix_milli,
        value,
        flags
    FROM %s.%s
    WHERE fingerprint IN GLOBAl (
        SELECT fingerprint
        FROM %s.%s
        WHERE __normalized = true
          AND unix_milli >= ? AND unix_milli < ?
    )
      AND unix_milli >= ? AND unix_milli < ?`,
		signozMetricDBName, signozSampleTableName, signozMetricDBName, signozTSTableNameV4)

	rowsSamples, err := conn.Query(ctx, querySamples, start, end, start, end)
	if err != nil {
		return fmt.Errorf("samples query: %w", err)
	}
	defer rowsSamples.Close()

	samplesBatch, err := newSamplesBatch()
	if err != nil {
		return err
	}
	rowsInSamplesBatch := 0

	for rowsSamples.Next() {
		var normS metricSample
		if err := rowsSamples.Scan(
			&normS.env,
			&normS.temporality,
			&normS.metricName,
			&normS.fingerprint,
			&normS.unixMilli,
			&normS.value,
			&normS.flags,
		); err != nil {
			return fmt.Errorf("samples scan: %w", err)
		}

		var unNormS metricSample
		md := metricDetails[normS.metricName]
		unNormS.env = normS.env
		unNormS.temporality = normS.temporality
		unNormS.metricName = md.UnNormMetricName
		unNormS.fingerprint = fingerprintMap[normS.fingerprint]
		unNormS.unixMilli = normS.unixMilli
		unNormS.value = normS.value
		unNormS.flags = normS.flags

		if err := samplesBatch.Append(
			unNormS.env,
			unNormS.temporality,
			unNormS.metricName,
			unNormS.fingerprint,
			unNormS.unixMilli,
			unNormS.value,
			unNormS.flags,
		); err != nil {
			return fmt.Errorf("append samples: %w", err)
		}
		rowsInSamplesBatch++
		if rowsInSamplesBatch == maxRowsPerBatch {
			if err := samplesBatch.Send(); err != nil {
				return fmt.Errorf("flush samples batch: %w", err)
			}
			samplesBatch, err = newSamplesBatch()
			if err != nil {
				return err
			}
			rowsInSamplesBatch = 0
		}
	}
	if err := rowsSamples.Err(); err != nil {
		return err
	}
	if rowsInSamplesBatch > 0 {
		if err := samplesBatch.Send(); err != nil {
			return fmt.Errorf("final samples flush: %w", err)
		}
	}
	log.Printf("migration success for windows start: %v, and end: %v", start, end)
	return nil
}

/* ----------------- helpers -------------------------------------------- */

// attrsToPMap converts a plain map[string]string to pcommon.Map.
func attrsToPMap(src map[string]string) pcommon.Map {
	dst := pcommon.NewMap()
	for k, v := range src {
		dst.PutStr(k, v)
	}
	return dst
}

// toOtelTemporality maps DB string → pmetric.AggregationTemporality.
func toOtelTemporality(dbVal string) pmetric.AggregationTemporality {
	switch dbVal {
	case "Cumulative":
		return pmetric.AggregationTemporalityCumulative
	case "Delta":
		return pmetric.AggregationTemporalityDelta
	default:
		return pmetric.AggregationTemporalityUnspecified
	}
}

func countOfNormalizedRowsTs(conn clickhouse.Conn, firstStamp int64, lastStamp int64) (uint64, error) {
	query := fmt.Sprintf(`select count(*) from %s.%s where __normalized = true and unix_milli >= ? and unix_milli <= ?`, signozMetricDBName, signozTSTableNameV4)
	ctx := cappedCHContext(context.Background())
	rows, err := conn.Query(ctx, query, firstStamp, lastStamp)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var counts uint64
	for rows.Next() {
		if err := rows.Scan(&counts); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return counts, nil
}

func getFirstTimeStampforNormalizedData(conn clickhouse.Conn) (int64, error) {
	query := fmt.Sprintf(`select min(unix_milli) from %s.%s where __normalized = true`, signozMetricDBName, signozTSTableNameV4)
	ctx := cappedCHContext(context.Background())
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var ts int64
	for rows.Next() {
		if err := rows.Scan(&ts); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return ts, nil
}

func getfirstTimeStampforNonNormalizedData(conn clickhouse.Conn) (int64, error) {
	query := fmt.Sprintf(`select min(unix_milli) from %s.%s where __normalized = false`, signozMetricDBName, signozTSTableNameV4)
	ctx := cappedCHContext(context.Background())
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var ts int64
	for rows.Next() {
		if err := rows.Scan(&ts); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return ts, nil
}

var specialCharRegex = regexp.MustCompile(`[^a-zA-Z0-9]`)

// sanitize removes all non-alphanumeric characters from s.
func sanitize(s string) string {
	return specialCharRegex.ReplaceAllString(s, "")
}

func GetCorrespondingNormalizedMetrics(
	conn clickhouse.Conn,
) (map[string]string, []string, error) {
	// 1) Fetch all distinct metric names + normalized flags
	query := "SELECT DISTINCT metric_name, toUInt8(__normalized) FROM %s.%s"
	chContext := cappedCHContext(context.Background())
	rows, err := conn.Query(
		chContext,
		fmt.Sprintf(query, signozMetricDBName, signozTSTableNameV4),
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	// 2) Group by the sanitized name, remembering both versions
	type pair struct {
		normalizedName   string
		unnormalizedName string
	}
	cleanedMap := make(map[string]*pair)

	for rows.Next() {
		var (
			metricName string
			normalized uint8
		)
		if err := rows.Scan(&metricName, &normalized); err != nil {
			return nil, nil, err
		}
		sanitized := sanitize(metricName)
		p, ok := cleanedMap[sanitized]
		if !ok {
			p = &pair{}
			cleanedMap[sanitized] = p
		}
		if normalized != 0 {
			// this is the “normalized” version
			p.normalizedName = metricName
		} else {
			// this is the original (un-normalized) version
			p.unnormalizedName = metricName
		}
	}

	if rows.Err() != nil {
		return nil, nil, rows.Err()
	}
	// 3) Build the result: normalized → un-normalized (or "" if missing)
	result := make(map[string]string, len(cleanedMap))
	for _, p := range cleanedMap {
		if p.normalizedName != "" {
			result[p.normalizedName] = p.unnormalizedName
		}
	}

	var missing []string
	for _, p := range cleanedMap {
		if p.normalizedName != "" && p.unnormalizedName == "" {
			missing = append(missing, p.normalizedName)
		}
	}

	return result, missing, nil
}

var scrubRe = regexp.MustCompile(`[^0-9A-Za-z]+`)

func checkAllAttributesOfTwoMetrics(
	conn clickhouse.Conn,
	metricNormTrue, metricNormFalse string,
) (
// map each rawTrue key → all rawFalse keys with the same cleaned key
	normAttrsToUnNormAttrs map[string]string,
// original keys present only in metricTrue
	keysPresentInNormMetric []string,
// original keys present only in metricFalse
	keysPresentInUnNormMetric []string,
	err error,
) {
	// 1) Fetch raw attribute lists
	rawNormTrueAttrs, err := fetchMetaAttrs(conn, metricNormTrue, true)
	if err != nil {
		return nil, nil, nil, err
	}
	rawNormFalseAttrs, err := fetchMetaAttrs(conn, metricNormFalse, false)
	if err != nil {
		return nil, nil, nil, err
	}

	// 2) Build maps from cleaned key → original keys
	cleanKeysToOrigKeysNormAttrs := make(map[string]string, len(rawNormTrueAttrs))
	for _, r := range rawNormTrueAttrs {
		clean := scrubRe.ReplaceAllString(r, "")
		cleanKeysToOrigKeysNormAttrs[clean] = r
	}
	cleanKeysToOrigKeysUnNormAttrs := make(map[string]string, len(rawNormFalseAttrs))
	for _, r := range rawNormFalseAttrs {
		clean := scrubRe.ReplaceAllString(r, "")
		if g, ok := cleanKeysToOrigKeysUnNormAttrs[clean]; ok {
			if !strings.Contains(g, ".") && strings.Contains(r, ".") {
				cleanKeysToOrigKeysUnNormAttrs[clean] = r
			}
		} else {
			cleanKeysToOrigKeysUnNormAttrs[clean] = r
		}
	}

	// 4) Extract cleaned key sets
	cleanNormTrueAttrs := make([]string, 0, len(cleanKeysToOrigKeysNormAttrs))
	for k := range cleanKeysToOrigKeysNormAttrs {
		cleanNormTrueAttrs = append(cleanNormTrueAttrs, k)
	}
	cleanNormFalseAttrs := make([]string, 0, len(cleanKeysToOrigKeysUnNormAttrs))
	for k := range cleanKeysToOrigKeysUnNormAttrs {
		cleanNormFalseAttrs = append(cleanNormFalseAttrs, k)
	}

	// 5) Compute diffs on the cleaned names
	missingCleanInNormTrueAttrs := diff(cleanNormTrueAttrs, cleanNormFalseAttrs)  // in trueMap only
	missingCleanInNormFalseAttrs := diff(cleanNormFalseAttrs, cleanNormTrueAttrs) // in falseMap only

	// 6) Map those back to original names
	for _, cleanKey := range missingCleanInNormTrueAttrs {
		keysPresentInNormMetric = append(keysPresentInNormMetric, cleanKeysToOrigKeysNormAttrs[cleanKey])
	}
	for _, cleanKey := range missingCleanInNormFalseAttrs {
		keysPresentInUnNormMetric = append(keysPresentInUnNormMetric, cleanKeysToOrigKeysUnNormAttrs[cleanKey])
	}

	normAttrsToUnNormAttrs = make(map[string]string, len(cleanKeysToOrigKeysNormAttrs))
	for clean, rTrue := range cleanKeysToOrigKeysNormAttrs {
		if rFalse, ok := cleanKeysToOrigKeysUnNormAttrs[clean]; ok {
			if prev, exists := normAttrsToUnNormAttrs[rTrue]; exists {
				if !strings.Contains(prev, ".") && strings.Contains(rFalse, ".") {
					normAttrsToUnNormAttrs[rTrue] = rFalse
				}
			} else {
				normAttrsToUnNormAttrs[rTrue] = rFalse
			}
		}
	}

	return normAttrsToUnNormAttrs, keysPresentInNormMetric, keysPresentInUnNormMetric, nil
}

func fetchMetaAttrs(conn clickhouse.Conn, metricName string, normalized bool) ([]string, error) {
	// Pull all attribute names for one metric
	q := fmt.Sprintf(`SELECT
    groupUniqArray(k) AS attribute_names
FROM   %s.%s
ARRAY JOIN
    JSONExtractKeys(labels) AS k
WHERE
    metric_name   = ?
    AND __normalized = ?
    AND NOT startsWithUTF8(k, '__')
    `, signozMetricDBName, signozTSTableNameV41Week)
	ctx := cappedCHContext(context.Background())
	rows, err := conn.Query(ctx, q, metricName, normalized)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		if err := rows.Scan(&out); err != nil {
			return nil, err
		}
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	return out, nil
}

func diff(a, b []string) []string {
	mb := make(map[string]struct{}, len(b))
	for _, x := range b {
		mb[x] = struct{}{}
	}
	var out []string
	for _, x := range a {
		if _, found := mb[x]; !found {
			out = append(out, x)
		}
	}
	return out
}

func cappedCHContext(parent context.Context) context.Context {
	return clickhouse.Context(parent,
		clickhouse.WithSettings(clickhouse.Settings{
			"max_memory_usage":                   2 * 500 * 1024 * 1024, // 1000 MB
			"max_bytes_before_external_group_by": 100 * 1024 * 1024,     // 100 MB
			"max_bytes_before_external_sort":     100 * 1024 * 1024,     // 100 MB
			"max_execution_time":                 90,                    // 30 s
			"max_threads":                        10,                    // 2 threads
		}),
	)
}

func (m *DashAlertsMigrator) migrateDashboards(
	metricMap map[string]helpers.MetricResult,
	attrMap map[string]string,
	copyDB string,
) error {
	// open the copy
	db, err := sql.Open("sqlite3", copyDB)
	if err != nil {
		return fmt.Errorf("failed to open DB %s: %w", copyDB, err)
	}
	defer db.Close()

	// prepare in-string replacers once
	replacers := buildReplacers(metricMap, attrMap)

	// select each dashboard row
	rows, err := db.Query(`SELECT id, data FROM dashboards`)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	type row struct {
		id   interface{}
		data []byte
	}
	var toUpdate []row

	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.data); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		// parse into a map so we can selectively mutate
		var dash map[string]interface{}
		if err := json.Unmarshal(r.data, &dash); err != nil {
			log.Printf("⚠️ skipping dashboard %v: invalid JSON", r.id)
			continue
		}

		// apply only the dashboard‐specific paths
		err := m.applyReplacementsToDashboard(dash, metricMap, attrMap, replacers)
		if err != nil {
			log.Printf("error getting for dashboard-id: %v, for error  - %v for file name - %v", r.id, err, copyDB)
			continue
		}

		// marshal back
		newBytes, err := json.Marshal(dash)
		if err != nil {
			return fmt.Errorf("re-marshal failed for dashboard %v: %w", r.id, err)
		}

		// if changed, queue update
		if !jsonEqual(r.data, newBytes) {
			toUpdate = append(toUpdate, row{id: r.id, data: newBytes})
		}
	}

	// apply updates in a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}
	stmt, err := tx.Prepare(`UPDATE dashboards SET data = ? WHERE id = ?`)
	if err != nil {
		return fmt.Errorf("prepare failed: %w", err)
	}
	defer stmt.Close()

	for _, r := range toUpdate {
		if _, err := stmt.Exec(r.data, r.id); err != nil {
			return fmt.Errorf("update failed for dashboard %v: %w", r.id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	fmt.Printf("✅ updated %d dashboards in %s\n", len(toUpdate), filepath.Base(copyDB))
	return nil
}

type replacer struct {
	re         *regexp.Regexp
	dot        string
	metricName map[string]struct {
		re  *regexp.Regexp
		dot string
	}
}

func buildReplacers(
	metricMap map[string]helpers.MetricResult,
	attrMap map[string]string,
) []replacer {
	var reps []replacer

	for under, m := range metricMap {
		metricAttrMap := make(map[string]struct {
			re  *regexp.Regexp
			dot string
		})

		// Add nested attr-level replacers
		for normName, unNormName := range m.NormToUnNormAttrMap {
			patt := `\b` + regexp.QuoteMeta(unNormName) + `\b`
			metricAttrMap[normName] = struct {
				re  *regexp.Regexp
				dot string
			}{
				re:  regexp.MustCompile(patt),
				dot: unNormName,
			}
		}

		patt := `\b` + regexp.QuoteMeta(under) + `\b`
		reps = append(reps, replacer{
			re:         regexp.MustCompile(patt),
			dot:        m.UnNormMetricName,
			metricName: metricAttrMap,
		})
	}

	for under, dot := range attrMap {
		patt := `\b` + regexp.QuoteMeta(under) + `\b`
		reps = append(reps, replacer{
			re:         regexp.MustCompile(patt),
			dot:        dot,
			metricName: nil, // No nested attrMap here
		})
	}

	sort.Slice(reps, func(i, j int) bool {
		return len(reps[i].dot) > len(reps[j].dot)
	})
	return reps
}

func buildReplacer(attrMap map[string]string) []replacer {
	var reps []replacer
	for under, dot := range attrMap {
		patt := `\b` + regexp.QuoteMeta(under) + `\b`
		reps = append(reps, replacer{
			re:         regexp.MustCompile(patt),
			dot:        dot,
			metricName: nil, // No nested attrMap here
		})
	}
	sort.Slice(reps, func(i, j int) bool {
		return len(reps[i].dot) > len(reps[j].dot)
	})
	return reps
}

func traverse(
	v interface{},
	metricMap map[string]helpers.MetricResult,
	attrMap map[string]string,
	replacers []replacer,
) interface{} {
	switch x := v.(type) {
	case string:
		// exact-match on whole string
		if m, ok := metricMap[x]; ok {
			return m.UnNormMetricName
		}
		if dot, ok := attrMap[x]; ok {
			return dot
		}

		// in-string replacements, one per token
		return applyReplacers(x, replacers)

	case []interface{}:
		for i, e := range x {
			x[i] = traverse(e, metricMap, attrMap, replacers)
		}
		return x

	case map[string]interface{}:
		newMap := make(map[string]interface{}, len(x))
		for k, e := range x {
			newKey := k
			if m, ok := metricMap[k]; ok {
				newKey = m.UnNormMetricName
			} else if dot, ok := attrMap[k]; ok {
				newKey = dot
			}
			newMap[newKey] = traverse(e, metricMap, attrMap, replacers)
		}
		return newMap

	default:
		return v
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := out.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func jsonEqual(a, b []byte) bool {
	var oa, ob interface{}
	if err := json.Unmarshal(a, &oa); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &ob); err != nil {
		return false
	}
	return reflect.DeepEqual(oa, ob)
}

func replacePlaceholders(query string, vars map[string]string) string {
	for k, v := range vars {
		placeholder := "$" + k
		query = strings.ReplaceAll(query, placeholder, "$"+v)
	}
	return query
}

func (m *DashAlertsMigrator) applyReplacementsToDashboard(
	dash map[string]interface{},
	metricMap map[string]helpers.MetricResult,
	attrMap map[string]string,
	replacers []replacer,
) error {

	if migrated, ok := dash["dotMigrated"].(bool); ok && migrated {
		return nil
	}

	var keysToRename []struct {
		oldKey string
		newKey string
	}
	variabledMap := make(map[string]string)
	// 1) Variables
	if varsRaw, ok := dash["variables"]; ok {
		if vars, ok := varsRaw.(map[string]interface{}); ok {

			for oldKey, vRaw := range vars {
				newKeyRaw := traverse(oldKey, metricMap, attrMap, replacers) // rename key using traverse
				newKey, ok := newKeyRaw.(string)
				if !ok {
					newKey = oldKey
				}
				if vi, ok := vRaw.(map[string]interface{}); ok {
					// Your existing per-variable value updates (queryValue, name fields) here
					for _, field := range []string{"queryValue"} {
						if sRaw, exists := vi[field]; exists {
							if s, ok := sRaw.(string); ok && s != "" {
								query := helpers.ConvertTemplateToNamedParams(s)
								metrics, err := helpers.ExtractMetrics(query, metricMap)
								if err != nil {
									return err
								}
								var metricResults []helpers.MetricResult
								for _, metric := range metrics {
									metricResults = append(metricResults, metricMap[metric])
								}
								query, err = m.queryTransformer.TransformQuery(query, metricResults, attrMap)
								if err != nil {
									return err
								}
								vi[field] = replacePlaceholders(query, attrMap)
							}
						}
					}
					for _, field := range []string{"name"} {
						if sRaw, exists := vi[field]; exists {
							if s, ok := sRaw.(string); ok && s != "" {
								vi[field] = traverse(s, metricMap, attrMap, replacers)
								variabledMap[s] = vi[field].(string)
							}
						}
					}
				}
				if newKey != oldKey {
					keysToRename = append(keysToRename, struct{ oldKey, newKey string }{oldKey, newKey})
				}
			}

			// Rename keys in the map after iteration
			for _, kr := range keysToRename {
				vars[kr.newKey] = vars[kr.oldKey]
				delete(vars, kr.oldKey)
			}
		}
	}

	variabledMapReplacer := buildReplacer(variabledMap)

	// helper to process a single widget-like object
	processWidget := func(wi map[string]interface{}) error {
		// A) builder.queryData
		if queryRaw, ok := wi["query"]; ok {
			if qObj, ok := queryRaw.(map[string]interface{}); ok {
				// builder
				if builderRaw, ok := qObj["builder"]; ok {
					if qb, ok := builderRaw.(map[string]interface{}); ok {
						// queryData array
						if qdRaw, exists := qb["queryData"]; exists {
							if qd, ok := qdRaw.([]interface{}); ok {
								for i, item := range qd {
									if entry, ok := item.(map[string]interface{}); ok {
										if dsRaw, exists2 := entry["dataSource"]; exists2 {
											if ds, ok2 := dsRaw.(string); ok2 && ds == "metrics" {
												qd[i] = traverse(entry, metricMap, attrMap, replacers)
											} else {
												qd[i] = traverse(entry, map[string]helpers.MetricResult{}, variabledMap, variabledMapReplacer)
											}
										}
									}
								}
							}
						}
						// queryFormulas
						if qfRaw, exists := qb["queryFormulas"]; exists {
							if qfArr, ok := qfRaw.([]interface{}); ok {
								for _, fi := range qfArr {
									if f, ok := fi.(map[string]interface{}); ok {
										if exprRaw, exists2 := f["expression"]; exists2 {
											if expr, ok2 := exprRaw.(string); ok2 {
												f["expression"] = traverse(expr, metricMap, attrMap, replacers)
											}
										}
										if lgRaw, exists2 := f["legend"]; exists2 {
											if lg, ok2 := lgRaw.(string); ok2 {
												f["legend"] = traverse(lg, metricMap, attrMap, replacers)
											}
										}
									}
								}
							}
						}
					}
				}
				// B) clickhouse_sql
				if chRaw, exists := qObj["clickhouse_sql"]; exists {
					if chArr, ok := chRaw.([]interface{}); ok {
						for _, ciRaw := range chArr {
							if cqi, ok := ciRaw.(map[string]interface{}); ok {
								if qRaw, exists2 := cqi["query"]; exists2 {
									if q, ok2 := qRaw.(string); ok2 && q != "" {
										if strings.Contains(q, "signoz_metrics") {
											query := helpers.ConvertTemplateToNamedParams(q)
											metrics, err := helpers.ExtractMetrics(query, metricMap)
											if err != nil {
												return err
											}
											var metricResults []helpers.MetricResult
											for _, metric := range metrics {
												metricResults = append(metricResults, metricMap[metric])
											}
											cqi["query"], err = m.queryTransformer.TransformQuery(query, metricResults, attrMap)
											if err != nil {
												return err
											}
											if q, ok := cqi["legend"].(string); ok && q != "" {
												cqi["legend"] = traverse(q, metricMap, attrMap, replacers)
											}
										} else {
											query := helpers.ConvertTemplateToNamedParams(q)
											var err error
											cqi["query"], err = m.queryTransformer.TransformQuery(query, []helpers.MetricResult{}, attrMap)
											if err != nil {
												return err
											}
											if q, ok := cqi["legend"].(string); ok && q != "" {
												cqi["legend"] = traverse(q, metricMap, attrMap, replacers)
											}
										}
									}
								}
							}
						}
					}
				}
				// C) promql
				if prRaw, exists := qObj["promql"]; exists {
					if prArr, ok := prRaw.([]interface{}); ok {
						for _, piRaw := range prArr {
							if pqi, ok := piRaw.(map[string]interface{}); ok {
								if qRaw, exists2 := pqi["query"]; exists2 {
									if q, ok2 := qRaw.(string); ok2 && q != "" {
										q = helpers.ConvertTemplateToNamedParams(q)
										metrics, err := helpers.ExtractPromMetrics(q, metricMap)
										if err != nil {
											return err
										}
										pqi["query"], err = helpers.TransformPromQLQuery(q, metrics)
										if err != nil {
											return err
										}
									}
								}
								if q, ok := pqi["legend"].(string); ok && q != "" {
									pqi["legend"] = traverse(q, metricMap, attrMap, replacers)
								}
							}
						}
					}
				}
			}
		}
		return nil
	}

	// 2) Widgets array
	if wArrRaw, ok := dash["widgets"]; ok {
		if wArr, ok := wArrRaw.([]interface{}); ok {
			for _, wRaw := range wArr {
				if wi, ok := wRaw.(map[string]interface{}); ok {
					err := processWidget(wi)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	// 3) panelMap
	if pmRaw, ok := dash["panelMap"]; ok {
		if pm, ok := pmRaw.(map[string]interface{}); ok {
			for _, vRaw := range pm {
				if wi, ok := vRaw.(map[string]interface{}); ok {
					err := processWidget(wi)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	dash["dotMigrated"] = true
	return nil
}

func (m *DashAlertsMigrator) migrateRules(
	metricMap map[string]helpers.MetricResult,
	attrMap map[string]string,
	dbPath string,
) error {
	// 1) open DB
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open DB %s: %w", dbPath, err)
	}
	defer db.Close()

	// 2) build in-string replacers once
	replacers := buildReplacers(metricMap, attrMap)

	// 3) select id + data
	rows, err := db.Query(`SELECT id, data FROM rule`)
	if err != nil {
		return fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	type ruleRow struct {
		id   interface{}
		data []byte
	}
	var toUpdate []ruleRow

	for rows.Next() {
		var r ruleRow
		if err := rows.Scan(&r.id, &r.data); err != nil {
			return fmt.Errorf("scan failed: %w", err)
		}

		// parse into a map so we can selectively mutate
		var alertObj map[string]interface{}
		if err := json.Unmarshal(r.data, &alertObj); err != nil {
			log.Printf("⚠️ skipping rule %v: invalid JSON", r.id)
			continue
		}

		// only mutate promQueries, chQueries, builder.queryData where dataSource=="metrics"
		err := m.applyReplacementsToAlert(alertObj, metricMap, attrMap, replacers)
		if err != nil {
			log.Printf("error getting for alert-id: %v, for error  - %v", r.id, err)
			continue
		}

		// marshal back
		newBytes, err := json.Marshal(alertObj)
		if err != nil {
			return fmt.Errorf("re-marshal failed for rule %v: %w", r.id, err)
		}

		// semantic compare
		if !jsonEqual(r.data, newBytes) {
			toUpdate = append(toUpdate, ruleRow{id: r.id, data: newBytes})
		}
	}

	// 4) apply updates in a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}
	stmt, err := tx.Prepare(`UPDATE rule SET data = ? WHERE id = ?`)
	if err != nil {
		return fmt.Errorf("prepare failed: %w", err)
	}
	defer stmt.Close()

	for _, r := range toUpdate {
		if _, err := stmt.Exec(r.data, r.id); err != nil {
			return fmt.Errorf("update failed for rule %v: %w", r.id, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	fmt.Printf("✅ updated %d rules in %s\n", len(toUpdate), dbPath)
	return nil
}

func (m *DashAlertsMigrator) applyReplacementsToAlert(
	alert map[string]interface{},
	metricMap map[string]helpers.MetricResult,
	attrMap map[string]string,
	replacers []replacer,
) error {

	if migrated, ok := alert["dotMigrated"].(bool); ok && migrated {
		return nil
	}

	// helper to replace in a standalone SQL/PromQL string

	// 1) Navigate to compositeQuery
	condRaw, ok := alert["condition"]
	if !ok {
		return errors.New("alert condition not found in alert")
	}
	cond, ok := condRaw.(map[string]interface{})
	if !ok {
		return errors.New("alert condition not found in alert")
	}
	cqRaw, ok := cond["compositeQuery"]
	if !ok {
		return errors.New("alert composite query not found in alert")
	}
	cq, ok := cqRaw.(map[string]interface{})
	if !ok {
		return errors.New("alert composite query not found in alert")
	}

	// 2) promQueries
	if pqRaw, ok := cq["promQueries"]; ok {
		if pq, ok := pqRaw.(map[string]interface{}); ok {
			for _, v := range pq {
				if entry, ok := v.(map[string]interface{}); ok {
					if qr, ok := entry["query"].(string); ok && qr != "" {
						qr = helpers.ConvertTemplateToNamedParams(qr)
						metrics, err := helpers.ExtractPromMetrics(qr, metricMap)
						if err != nil {
							return err
						}
						entry["query"], err = helpers.TransformPromQLQuery(qr, metrics)
						if err != nil {
							return err
						}
					}
					if q, ok := entry["legend"].(string); ok && q != "" {
						entry["legend"] = traverse(q, metricMap, attrMap, replacers)
					}
				}
			}
		}
	}

	// 3) chQueries
	if chRaw, ok := cq["chQueries"]; ok {
		if chq, ok := chRaw.(map[string]interface{}); ok {
			for _, v := range chq {
				if entry, ok := v.(map[string]interface{}); ok {
					if q, ok := entry["query"].(string); ok {
						if strings.Contains(q, "signoz_metrics") {
							query := helpers.ConvertTemplateToNamedParams(q)
							metrics, err := helpers.ExtractMetrics(query, metricMap)
							if err != nil {
								return err
							}
							var metricResults []helpers.MetricResult
							for _, metric := range metrics {
								metricResults = append(metricResults, metricMap[metric])
							}
							entry["query"], err = m.queryTransformer.TransformQuery(query, metricResults, attrMap)
							if err != nil {
								return err
							}
						}
					}
					if q, ok := entry["legend"].(string); ok && q != "" {
						entry["legend"] = traverse(q, metricMap, attrMap, replacers)
					}
				}
			}
		}
	}

	// 4) builderQueries (METRIC_BASED_ALERT)
	if bqRaw, ok := cq["builderQueries"]; ok {
		if bq, ok := bqRaw.(map[string]interface{}); ok {
			// First check if any query is metrics
			hasMetrics := false
			for _, v := range bq {
				if entry, ok := v.(map[string]interface{}); ok {
					if ds, ok := entry["dataSource"].(string); ok && ds == "metrics" {
						hasMetrics = true
						break
					}
				}
			}

			// If any metrics query found, run traverse() on every query
			if hasMetrics {
				for key, v := range bq {
					if entry, ok := v.(map[string]interface{}); ok {
						bq[key] = traverse(entry, metricMap, attrMap, replacers)
					}
				}
			}
		}
	}

	// 5) builder.queryData (other alerts)
	if builderRaw, ok := cq["builder"]; ok {
		if builder, ok := builderRaw.(map[string]interface{}); ok {
			if qdRaw, exists := builder["queryData"]; exists {
				if qd, ok := qdRaw.([]interface{}); ok {
					for i, item := range qd {
						if entry, ok := item.(map[string]interface{}); ok {
							if ds, ok := entry["dataSource"].(string); ok && ds == "metrics" {
								qd[i] = traverse(entry, metricMap, attrMap, replacers)
							}
						}
					}
				}
			}
			if qfRaw, exists := builder["queryFormulas"]; exists {
				if qfArr, ok := qfRaw.([]interface{}); ok {
					for _, fi := range qfArr {
						if f, ok := fi.(map[string]interface{}); ok {
							if exprRaw, exists2 := f["expression"]; exists2 {
								if expr, ok2 := exprRaw.(string); ok2 {
									f["expression"] = traverse(expr, metricMap, attrMap, replacers)
								}
							}
							if lgRaw, exists2 := f["legend"]; exists2 {
								if lg, ok2 := lgRaw.(string); ok2 {
									f["legend"] = traverse(lg, metricMap, attrMap, replacers)
								}
							}
						}
					}
				}
			}
		}
	}

	// 6) annotations
	if annRaw, ok := alert["annotations"]; ok {
		if ann, ok := annRaw.(map[string]interface{}); ok {
			if d, ok := ann["description"].(string); ok {
				ann["description"] = traverse(d, metricMap, attrMap, replacers)
			}
			if s, ok := ann["summary"].(string); ok {
				ann["summary"] = traverse(s, metricMap, attrMap, replacers)
			}
		}
	}

	// 7) labels
	if lblsRaw, ok := alert["labels"]; ok {
		if lbls, ok := lblsRaw.(map[string]interface{}); ok {
			newLabels := make(map[string]interface{}, len(lbls))
			for k, v := range lbls {
				newKey := traverse(k, metricMap, attrMap, replacers)
				if sv, ok := v.(string); ok {
					if sn, ok := newKey.(string); ok && sn != "" {
						newLabels[sn] = traverse(sv, metricMap, attrMap, replacers)
					} else {
						newLabels[sn] = v
					}
				}
			}
			alert["labels"] = newLabels
		}
	}

	// 8) update embedded compositeQuery in source URL
	if srcRaw, ok := alert["source"].(string); ok {
		if u, err := url.Parse(srcRaw); err == nil {
			vals := u.Query()
			if cqEnc := vals.Get("compositeQuery"); cqEnc != "" {
				// decode twice
				if once, err := url.QueryUnescape(cqEnc); err == nil {
					if twice, err := url.QueryUnescape(once); err == nil {
						var embedded interface{}
						if err := json.Unmarshal([]byte(twice), &embedded); err == nil {
							// apply same transformations to embedded.compositeQuery
							if emap, ok := embedded.(map[string]interface{}); ok {
								err := m.applyReplacementsToAlert(
									map[string]interface{}{"condition": map[string]interface{}{"compositeQuery": emap}},
									metricMap, attrMap, replacers,
								)
								if err != nil {
									return err
								}
								if newBytes, err := json.Marshal(emap); err == nil {
									esc1 := url.QueryEscape(string(newBytes))
									esc2 := url.QueryEscape(esc1)
									vals.Set("compositeQuery", esc2)
									u.RawQuery = vals.Encode()
									alert["source"] = u.String()
								}
							}
						}
					}
				}
			}
		}
	}

	alert["dotMigrated"] = true
	return nil
}

var tokenRe = regexp.MustCompile(`\b\w+\b`)

func applyReplacers(s string, replacers []replacer) string {
	var sb strings.Builder
	last := 0

	for _, loc := range tokenRe.FindAllStringIndex(s, -1) {
		start, end := loc[0], loc[1]
		token := s[start:end]

		// write any non-token prefix
		sb.WriteString(s[last:start])

		// find first matching replacer
		replaced := false
		for _, r := range replacers {
			if r.re.MatchString(token) {
				sb.WriteString(r.dot)
				replaced = true
				break
			}
		}

		if !replaced {
			sb.WriteString(token)
		}

		last = end
	}

	// write trailing suffix
	sb.WriteString(s[last:])
	return sb.String()
}
