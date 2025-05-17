package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"golang.org/x/sync/errgroup"
	"migration-0.70/helpers"
	internal "migration-0.70/internal"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"log"
	"regexp"
	"sync"
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

// result from checking one metric pair
type metricResult struct {
	normMetricName      string
	unNormMetricName    string
	normAttributes      []string
	unNormAttributes    []string
	normToUnNormAttrMap map[string]string
	err                 error
}

func main() {
	var (
		workers      = flag.Int("workers", helpers.EnvOrInt("MIGRATE_WORKERS", 4), "concurrent hour-windows to process")
		maxOpenConns = flag.Int("max-open-conns", helpers.EnvOrInt("MIGRATE_MAX_OPEN_CONNS", 16), "ClickHouse connection pool size")
	)
	flag.Parse()

	defaultsAttrs := map[string]string{
		"quantile": "quantile",
	}
	defaultsMerics := map[string]string{
		"certmanager_http_acme_client_request_duration_seconds":     "certmanager_http_acme_client_request_duration_seconds",
		"redis_latency_percentiles_usec":                            "redis_latency_percentiles_usec",
		"go_gc_duration_seconds":                                    "go_gc_duration_seconds",
		"nginx_ingress_controller_ingress_upstream_latency_seconds": "nginx_ingress_controller_ingress_upstream_latency_seconds",
	}
	notFoundAttrMap := helpers.OverlayFromEnv(defaultsAttrs, "NOT_FOUND_ATTR_MAP")
	notFoundMetricsMap := helpers.OverlayFromEnv(defaultsMerics, "NOT_FOUND_METRICS_MAP")

	conn, err := getClickhouseConn(*maxOpenConns)
	if err != nil {
		log.Fatalf("error connecting to ClickHouse: %v", err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("error closing ClickHouse connection: %v", err)
		}
	}()

	metrics, missing, err := GetCorrespondingNormalizedMetrics(conn)
	if err != nil {
		log.Fatalf("error getting all metric names: %v", err)
	}

	log.Printf("metrics total: %v", len(metrics))

	log.Printf("metrics missing: %v", missing)

	for _, metric := range missing {
		if _, ok := notFoundMetricsMap[metric]; !ok {
			log.Fatalf("missing metrics, please add it to NOT_FOUND_METRIC_MAP: %s", metric)
		} else {
			metrics[metric] = notFoundMetricsMap[metric]
		}
	}

	metricDetails, _, err := buildMetricDetails(conn, metrics, notFoundAttrMap)
	if err != nil {
		log.Fatalf("error building metric details: %v", err)
	}

	//lets start insertion

	//check from where insertion needs to be start
	firstTimesStamp, err := getFirstTimeStampforNormalizedData(conn)
	if err != nil {
		log.Fatalf("error getting first timestamp: %v", err)
	}
	lastTimeStamp, err := getfirstTimeStampforNonNormalizedData(conn)
	if err != nil {
		log.Fatalf("error getting last timestamp: %v", err)
	}
	//how many rows to be inserted
	log.Printf("migrating window [%d … %d) (%.1f h total)",
		firstTimesStamp, lastTimeStamp, float64(lastTimeStamp-firstTimesStamp)/3_600_000)

	tsCount, err := countOfNormalizedRowsTs(conn, firstTimesStamp, lastTimeStamp)
	if err != nil {
		log.Fatalf("error counting rows: %v", err)
	}
	log.Printf("total ts rows: %d", tsCount)
	//first fetch last an hour data
	allResourceAttrs, allScopeAttrs, allPointAttr, err := getAllDifferentMetricsAttributes(conn)
	if err != nil {
		log.Fatalf("error getting all attributes: %v", err)
	}

	g, errGroupCtx := errgroup.WithContext(context.Background())
	sem := make(chan struct{}, *workers)

	for t := firstTimesStamp; t < lastTimeStamp; t += 3600 {
		start, end := t, min(t+3600, lastTimeStamp)

		sem <- struct{}{}
		st, en := start, end
		g.Go(func() error {
			defer func() { <-sem }()
			return fetchAndInsertTimeSeriesV4(errGroupCtx, conn, st, en, metricDetails, allResourceAttrs, allScopeAttrs, allPointAttr)
		})
	}

	if err := g.Wait(); err != nil {
		log.Fatalf("error migration failed: %v", err)
	}
	log.Printf("migration finished")

}

func buildMetricDetails(conn clickhouse.Conn, metrics map[string]string, notFoundAttrMap map[string]string) (map[string]metricResult, map[string]string, error) {

	var workerCount = 4

	jobs := make(chan metricJob, len(metrics))
	results := make(chan metricResult, len(metrics))

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				// do the work
				attrmap, normAttrs, unNormAttrs, err := checkAllAttributesOfTwoMetrics(conn, job.normMetricName, job.unNormMetricName)
				results <- metricResult{job.normMetricName, job.unNormMetricName, normAttrs, unNormAttrs, attrmap, err}
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
	metricDetails := make(map[string]metricResult)
	allAttributeMap := make(map[string]string)
	for res := range results {
		metricDetails[res.normMetricName] = res
		if res.err != nil {
			log.Fatalf("error checking metric %s → %s: %v", res.normMetricName, res.unNormMetricName, res.err)
		} else {
			//log.Printf("metrics name %s -> %s", res.key, res.name) // uncomment this line to check for valid metrics.
		}
		allAttributeMap = mergeMaps(allAttributeMap, res.normToUnNormAttrMap)
		switch {
		case len(res.normAttributes) == 0 && len(res.unNormAttributes) == 0:
			mu.Lock()
			validMetrics[res.normMetricName] = res.unNormMetricName
			//log.Printf("valid metric name: %s to %s", res.key, res.name)
			mu.Unlock()

		default:
			// anything else is "non-valid"
			mu.Lock()
			nonValidMetrics[res.normMetricName] = res.unNormMetricName
			mu.Unlock()

			// still log the details for visibility
			if len(res.normAttributes) > 0 {
				log.Printf("extra attributes in underscore metric %s: %v", res.normMetricName, res.normAttributes)
			}
			if len(res.unNormAttributes) > 0 {
				log.Printf("extra attributes in dot metric %s: %v", res.unNormMetricName, res.unNormAttributes)
			}
		}
	}

	notFound := make(map[string]struct{})

	for _, metricDetail := range metricDetails {
		if len(metricDetail.normAttributes) != 0 {
			for _, attr := range metricDetail.normAttributes {
				if _, ok := allAttributeMap[attr]; !ok {
					if _, ok := notFoundAttrMap[attr]; !ok {
						notFound[attr] = struct{}{}
					} else {
						allAttributeMap[attr] = notFoundAttrMap[attr]
						metricDetail.normToUnNormAttrMap[attr] = notFoundAttrMap[attr]
					}
				} else {
					metricDetail.normToUnNormAttrMap[attr] = allAttributeMap[attr]
				}
			}
		}
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
		return nil, nil, nil, fmt.Errorf("no data for metric %q")
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

func fetchAndInsertTimeSeriesV4(ctx context.Context, conn clickhouse.Conn, start, end int64, metricDetails map[string]metricResult, allResourceAttrs, allScopeAttrs, allPointAttrs map[string]struct{}) error {
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
			clean := md.normToUnNormAttrMap[k]

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
		unNorm.MetricName = md.unNormMetricName
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
		unNormS.metricName = md.unNormMetricName
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
		cleanKeysToOrigKeysUnNormAttrs[clean] = r
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
			normAttrsToUnNormAttrs[rTrue] = rFalse
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

func mergeMaps(a, b map[string]string) map[string]string {
	out := make(map[string]string, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}
