package connector

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"
)

// profileInterval is how often the history of awaiting queries is fetched.
// The system views take seconds to read, so awaiting queries are batched.
const profileInterval = 30 * time.Second

// queryHistory reads the warehouse's query history: time spent queued,
// waiting on locks, planning, compiling and executing, the blocks a query's
// steps read and spilled, and what a COPY loaded.
type queryHistory struct {
	uri string
}

func newQueryProfiler(cfg config, bindings []*binding) *boilerplate.QueryProfiler {
	var paths = make([][]string, len(bindings))
	var locations = make([]sql.InfoTableLocation, len(bindings))
	for i, b := range bindings {
		paths[i] = b.target.Path
		locations[i] = b.target.InfoLocation
	}
	var uri = cfg.toURI()
	return boilerplate.NewQueryProfiler(&queryHistory{uri: uri}, profileInterval, nil, paths, newTableContext(uri, locations))
}

// execTraced runs a statement of the binding in the transaction, profiled as
// the binding's.
func (d *transactor) execTraced(ctx context.Context, txn pgx.Tx, binding int, query string, args ...any) (pgconn.CommandTag, error) {
	var _, trace = d.profiler.Trace(ctx, binding, boilerplate.StatementKind(query))
	tag, err := txn.Exec(ctx, query, args...)
	if err == nil {
		setQueryID(ctx, txn, trace)
	}
	trace.Finish(err)
	return tag, err
}

// setQueryID asks for the ID of the query the transaction just ran, if it was
// slow. The query must have run on the compute nodes: the ID of one that ran
// on the leader alone is that of an earlier query.
func setQueryID(ctx context.Context, txn pgx.Tx, trace *boilerplate.QueryTrace) {
	if !trace.Slow() {
		return
	}
	var id int64
	if err := txn.QueryRow(ctx, "SELECT pg_last_query_id()").Scan(&id); err == nil && id > 0 {
		trace.SetID(strconv.FormatInt(id, 10))
	}
}

func (h *queryHistory) Fetch(ctx context.Context, ids []string) (map[string]log.Fields, error) {
	conn, err := pgx.Connect(ctx, h.uri)
	if err != nil {
		return nil, fmt.Errorf("connecting: %w", err)
	}
	defer conn.Close(ctx)

	var placeholders = make([]string, len(ids))
	var args = make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		if args[i], err = strconv.ParseInt(id, 10, 64); err != nil {
			return nil, fmt.Errorf("query id %q: %w", id, err)
		}
	}
	var in = strings.Join(placeholders, ", ")

	rows, err := conn.Query(ctx, fmt.Sprintf(`
SELECT query_id, TRIM(status), elapsed_time, queue_time, execution_time, compile_time,
	planning_time, lock_wait_time, returned_rows, returned_bytes, result_cache_hit, TRIM(compute_type)
FROM sys_query_history
WHERE query_id IN (%s) AND TRIM(status) IN ('success', 'failed', 'canceled')`, in), args...)
	if err != nil {
		return nil, historyErr("querying history", err)
	}
	var out = make(map[string]log.Fields)
	var id int64
	var status, computeType string
	var elapsed, queue, execution, compile, planning, lockWait, returnedRows, returnedBytes *int64
	var cacheHit *bool
	if _, err := pgx.ForEachRow(rows, []any{&id, &status, &elapsed, &queue, &execution, &compile,
		&planning, &lockWait, &returnedRows, &returnedBytes, &cacheHit, &computeType}, func() error {
		var fields = log.Fields{
			"status":        status,
			"totalMs":       micros(elapsed),
			"executionMs":   micros(execution),
			"compilationMs": micros(compile),
			"planningMs":    micros(planning),
			"rowsProduced":  deref(returnedRows),
			"bytesProduced": deref(returnedBytes),
		}
		if queued := micros(queue); queued > 0 {
			fields["queuedMs"] = queued
		}
		if blocked := micros(lockWait); blocked > 0 {
			fields["blockedMs"] = blocked
		}
		if cacheHit != nil && *cacheHit {
			fields["resultCacheHit"] = true
		}
		// A query not on the primary compute ran on concurrency scaling.
		if computeType != "" && computeType != "primary" {
			fields["computeType"] = computeType
		}
		out[strconv.FormatInt(id, 10)] = fields
		return nil
	}); err != nil {
		return nil, historyErr("reading history", err)
	} else if len(out) == 0 {
		return out, nil
	}

	// Each step's scan reads rows and blocks of a table, and its other
	// operators may spill to disk.
	rows, err = conn.Query(ctx, fmt.Sprintf(`
SELECT query_id,
	SUM(CASE WHEN TRIM(step_name) = 'scan' THEN input_rows ELSE 0 END),
	SUM(blocks_read), SUM(local_read_io), SUM(remote_read_io),
	SUM(spilled_block_local_disk), SUM(spilled_block_remote_disk)
FROM sys_query_detail
WHERE query_id IN (%s) AND TRIM(metrics_level) = 'step'
GROUP BY query_id`, in), args...)
	if err != nil {
		return nil, historyErr("querying query detail", err)
	}
	var rowsScanned, blocksRead, localReadIO, remoteReadIO, spilledLocal, spilledRemote *int64
	if _, err := pgx.ForEachRow(rows, []any{&id, &rowsScanned, &blocksRead, &localReadIO, &remoteReadIO, &spilledLocal, &spilledRemote}, func() error {
		if fields := out[strconv.FormatInt(id, 10)]; fields != nil {
			fields["rowsScanned"] = deref(rowsScanned)
			fields["blocksRead"] = deref(blocksRead)
			fields["localReadIO"] = deref(localReadIO)
			fields["remoteReadIO"] = deref(remoteReadIO)
			fields["spillLocalBlocks"] = deref(spilledLocal)
			fields["spillRemoteBlocks"] = deref(spilledRemote)
		}
		return nil
	}); err != nil {
		return nil, historyErr("reading query detail", err)
	}

	// Alerts flag what the planner or executor found wrong with a step, such
	// as missing statistics or a nested loop join.
	rows, err = conn.Query(ctx, fmt.Sprintf(`
SELECT DISTINCT query_id, TRIM(alert) FROM sys_query_detail
WHERE query_id IN (%s) AND TRIM(alert) <> ''`, in), args...)
	if err != nil {
		return nil, historyErr("querying query alerts", err)
	}
	var alert string
	if _, err := pgx.ForEachRow(rows, []any{&id, &alert}, func() error {
		if fields := out[strconv.FormatInt(id, 10)]; fields != nil {
			alerts, _ := fields["alerts"].([]string)
			fields["alerts"] = append(alerts, alert)
		}
		return nil
	}); err != nil {
		return nil, historyErr("reading query alerts", err)
	}

	rows, err = conn.Query(ctx, fmt.Sprintf(`
SELECT query_id, SUM(loaded_rows), SUM(loaded_bytes), SUM(source_file_count), SUM(source_file_bytes)
FROM sys_load_history
WHERE query_id IN (%s)
GROUP BY query_id`, in), args...)
	if err != nil {
		return nil, historyErr("querying load history", err)
	}
	var loadedRows, loadedBytes, sourceFiles, sourceFileBytes *int64
	if _, err := pgx.ForEachRow(rows, []any{&id, &loadedRows, &loadedBytes, &sourceFiles, &sourceFileBytes}, func() error {
		if fields := out[strconv.FormatInt(id, 10)]; fields != nil {
			fields["loadedRows"] = deref(loadedRows)
			fields["loadedBytes"] = deref(loadedBytes)
			fields["sourceFiles"] = deref(sourceFiles)
			fields["sourceFileBytes"] = deref(sourceFileBytes)
		}
		return nil
	}); err != nil {
		return nil, historyErr("reading load history", err)
	}

	return out, nil
}

// historyErr marks an error of reading a system view the user isn't permitted
// to as such.
func historyErr(doing string, err error) error {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "42501" {
		return fmt.Errorf("%s: %w: %w", doing, boilerplate.ErrQueryHistoryDenied, err)
	}
	return fmt.Errorf("%s: %w", doing, err)
}

// micros converts a duration in microseconds to milliseconds.
func micros(v *int64) int64 { return deref(v) / 1000 }

func deref(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

// newTableContext reads a table's distribution, sort key and how much of it
// is unsorted or lacks statistics, and the warehouse's version and capacity.
func newTableContext(uri string, locations []sql.InfoTableLocation) boilerplate.TableContext {
	return func(ctx context.Context, binding int) (log.Fields, error) {
		conn, err := pgx.Connect(ctx, uri)
		if err != nil {
			return nil, fmt.Errorf("connecting: %w", err)
		}
		defer conn.Close(ctx)

		var out = make(log.Fields)
		var errs []error

		table, err := describeTable(ctx, conn, locations[binding])
		if len(table) > 0 {
			out["table"] = table
		}
		errs = append(errs, err)

		warehouse, err := describeWarehouse(ctx, conn)
		if len(warehouse) > 0 {
			out["warehouse"] = warehouse
		}
		errs = append(errs, err)

		return out, errors.Join(errs...)
	}
}

// describeTable reads the table's row of SVV_TABLE_INFO, which only shows
// tables that have rows, to users granted it.
func describeTable(ctx context.Context, conn *pgx.Conn, loc sql.InfoTableLocation) (log.Fields, error) {
	var distStyle, sortKey, encoded *string
	var sortKeys, size, rows *int64
	var pctUsed, unsorted, statsOff, skewRows, sortBenefit *float64
	if err := conn.QueryRow(ctx, `
SELECT TRIM(diststyle), TRIM(sortkey1), sortkey_num::bigint, TRIM(encoded), size::bigint, tbl_rows::bigint,
	pct_used::float8, unsorted::float8, stats_off::float8, skew_rows::float8, vacuum_sort_benefit::float8
FROM svv_table_info
WHERE database = current_database() AND LOWER("schema") = LOWER($1) AND LOWER("table") = LOWER($2)`,
		loc.TableSchema, loc.TableName,
	).Scan(&distStyle, &sortKey, &sortKeys, &encoded, &size, &rows, &pctUsed, &unsorted, &statsOff, &skewRows, &sortBenefit); errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("svv_table_info: %w", err)
	}

	var out = make(log.Fields)
	for name, v := range map[string]*string{"distStyle": distStyle, "sortKey": sortKey, "encoded": encoded} {
		if v != nil {
			out[name] = *v
		}
	}
	// size is in 1 MB blocks.
	for name, v := range map[string]*int64{"sortKeys": sortKeys, "sizeMB": size, "rows": rows} {
		if v != nil {
			out[name] = *v
		}
	}
	for name, v := range map[string]*float64{
		"pctUsed": pctUsed, "unsortedPct": unsorted, "statsOffPct": statsOff, "skewRows": skewRows, "vacuumSortBenefit": sortBenefit,
	} {
		if v != nil {
			out[name] = *v
		}
	}
	return out, nil
}

var redshiftVersionRe = regexp.MustCompile(`Redshift ([0-9.]+)`)

// describeWarehouse reads the warehouse's version, and the capacity of a
// serverless workgroup or the nodes of a provisioned cluster, whichever the
// warehouse has a view of.
func describeWarehouse(ctx context.Context, conn *pgx.Conn) (log.Fields, error) {
	var out = make(log.Fields)
	var errs []error

	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		errs = append(errs, fmt.Errorf("version: %w", err))
	} else if m := redshiftVersionRe.FindStringSubmatch(version); m != nil {
		out["version"] = m[1]
	}

	var rpus *float64
	var nodes *int64
	if err := conn.QueryRow(ctx, "SELECT compute_capacity::float8 FROM sys_serverless_usage ORDER BY start_time DESC LIMIT 1").Scan(&rpus); err == nil {
		if rpus != nil {
			out["serverlessRPUs"] = *rpus
		}
	} else if err := conn.QueryRow(ctx, "SELECT COUNT(DISTINCT node) FROM stv_slices").Scan(&nodes); err == nil {
		if nodes != nil {
			out["nodes"] = *nodes
		}
	} else {
		errs = append(errs, fmt.Errorf("warehouse capacity: %w", err))
	}

	return out, errors.Join(errs...)
}
