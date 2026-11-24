package connector

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	mssql "github.com/microsoft/go-mssqldb"
	log "github.com/sirupsen/logrus"
)

// profileInterval is how often the history of awaiting queries is fetched.
// Query insights records finished queries in batches, minutes apart.
const profileInterval = 2 * time.Minute

// historyWindow bounds the history searched for awaiting queries, all of which
// finished within the profiler's wait limit.
var historyWindow = boilerplate.QueryProfileMaxWait + 5*time.Minute

// mssqlPermissionDenied is raised for an object the user may not read.
const mssqlPermissionDenied = 229

// queryHistory reads the warehouse's query insights: time spent waiting to
// start and running, CPU time, the data scanned from remote storage, memory
// and disk, and whether the SQL pool was under pressure.
type queryHistory struct {
	cfg config
}

func newQueryProfiler(cfg config, bindings []*binding) *boilerplate.QueryProfiler {
	var paths = make([][]string, len(bindings))
	var locations = make([]sql.InfoTableLocation, len(bindings))
	for i, b := range bindings {
		paths[i] = b.target.Path
		locations[i] = b.target.InfoLocation
	}
	return boilerplate.NewQueryProfiler(&queryHistory{cfg: cfg}, profileInterval, nil, paths, newTableContext(cfg, locations))
}

// labelQuery labels the query's last statement with a new ID of the trace,
// which query insights records the statement under.
func labelQuery(trace *boilerplate.QueryTrace, query string) string {
	var label = uuid.NewString()
	trace.SetID(label)
	return strings.TrimRight(query, "; \t\n") + "\nOPTION (LABEL = '" + label + "');"
}

func (h *queryHistory) Fetch(ctx context.Context, ids []string) (map[string]log.Fields, error) {
	db, err := h.cfg.db()
	if err != nil {
		return nil, fmt.Errorf("creating db: %w", err)
	}
	defer db.Close()

	var placeholders = make([]string, len(ids))
	var args = make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("@p%d", i+1)
		args[i] = id
	}

	// A query that ran while its SQL pool reported pressure was competing for
	// the capacity's resources.
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`
SELECT h.label, CAST(h.distributed_statement_id AS VARCHAR(36)), h.status, h.total_elapsed_time_ms,
	DATEDIFF_BIG(millisecond, h.submit_time, h.start_time), h.allocated_cpu_time_ms, h.row_count,
	h.data_scanned_remote_storage_mb, h.data_scanned_memory_mb, h.data_scanned_disk_mb,
	h.result_cache_hit, h.sql_pool_name,
	(SELECT COUNT(*) FROM queryinsights.sql_pool_insights p
		WHERE p.sql_pool_name = h.sql_pool_name AND p.is_pool_under_pressure = 1
		AND p.timestamp >= h.submit_time AND p.timestamp <= h.end_time)
FROM queryinsights.exec_requests_history h
WHERE h.label IN (%s) AND h.end_time >= DATEADD(second, -%d, SYSUTCDATETIME())`,
		strings.Join(placeholders, ", "), int(historyWindow.Seconds())), args...)
	if err != nil {
		return nil, historyErr(err)
	}
	defer rows.Close()

	var out = make(map[string]log.Fields)
	for rows.Next() {
		var label, statementID, status string
		var pool stdsql.NullString
		var total, startDelay, cpu, rowCount, cacheHit, pressure stdsql.NullInt64
		var remoteMB, memoryMB, diskMB stdsql.NullFloat64
		if err := rows.Scan(&label, &statementID, &status, &total, &startDelay, &cpu, &rowCount,
			&remoteMB, &memoryMB, &diskMB, &cacheHit, &pool, &pressure); err != nil {
			return nil, fmt.Errorf("scanning query insights: %w", err)
		}
		var fields = log.Fields{
			"statementId":     statementID,
			"status":          status,
			"totalMs":         total.Int64,
			"cpuMs":           cpu.Int64,
			"rowCount":        rowCount.Int64,
			"scannedRemoteMB": remoteMB.Float64,
			"scannedMemoryMB": memoryMB.Float64,
			"scannedDiskMB":   diskMB.Float64,
		}
		// Query insights doesn't break down the time from a query's
		// submission until it starts running.
		if startDelay.Int64 > 0 {
			fields["startDelayMs"] = startDelay.Int64
		}
		if cacheHit.Int64 != 0 {
			fields["resultCacheHit"] = cacheHit.Int64
		}
		if pool.String != "" {
			fields["sqlPool"] = pool.String
		}
		if pressure.Int64 > 0 {
			fields["poolUnderPressure"] = true
		}
		out[label] = fields
	}
	if err := rows.Err(); err != nil {
		return nil, historyErr(err)
	}
	return out, nil
}

// historyErr marks an error of reading query insights the user isn't
// permitted to as such.
func historyErr(err error) error {
	if mssqlErr, ok := errors.AsType[mssql.Error](err); ok && mssqlErr.Number == mssqlPermissionDenied {
		return fmt.Errorf("querying query insights: %w: %w", boilerplate.ErrQueryHistoryDenied, err)
	}
	return fmt.Errorf("querying query insights: %w", err)
}

// newTableContext reads the table's statistics and the warehouse's version,
// capacity and settings. The warehouse has no view of a table's rows, size or
// files.
func newTableContext(cfg config, locations []sql.InfoTableLocation) boilerplate.TableContext {
	return func(ctx context.Context, binding int) (log.Fields, error) {
		db, err := cfg.db()
		if err != nil {
			return nil, fmt.Errorf("creating db: %w", err)
		}
		defer db.Close()

		var out = make(log.Fields)
		var errs []error

		table, err := describeTable(ctx, db, locations[binding])
		if len(table) > 0 {
			out["table"] = table
		}
		errs = append(errs, err)

		warehouse, err := describeWarehouse(ctx, db)
		if len(warehouse) > 0 {
			out["warehouse"] = warehouse
		}
		errs = append(errs, err)

		return out, errors.Join(errs...)
	}
}

// describeTable reads how many column statistics the table has and when the
// most and least recently updated of them were updated.
func describeTable(ctx context.Context, db *stdsql.DB, loc sql.InfoTableLocation) (log.Fields, error) {
	var count int64
	var newest, oldest stdsql.NullTime
	if err := db.QueryRowContext(ctx, `
SELECT COUNT(*), MAX(STATS_DATE(s.object_id, s.stats_id)), MIN(STATS_DATE(s.object_id, s.stats_id))
FROM sys.stats s
JOIN sys.tables t ON t.object_id = s.object_id
JOIN sys.schemas sc ON sc.schema_id = t.schema_id
WHERE sc.name = @p1 AND t.name = @p2 AND (s.auto_created = 1 OR s.user_created = 1)`,
		loc.TableSchema, loc.TableName,
	).Scan(&count, &newest, &oldest); err != nil {
		return nil, fmt.Errorf("sys.stats: %w", err)
	}

	var out = log.Fields{"statistics": count}
	if newest.Valid {
		out["statisticsNewest"] = newest.Time.UTC().Format(time.RFC3339)
	}
	if oldest.Valid {
		out["statisticsOldest"] = oldest.Time.UTC().Format(time.RFC3339)
	}
	return out, nil
}

// describeWarehouse reads the warehouse's version and settings, and the
// capacity and state of its SQL pools as last reported.
func describeWarehouse(ctx context.Context, db *stdsql.DB) (log.Fields, error) {
	var out = make(log.Fields)
	var errs []error

	var version string
	if err := db.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&version); err != nil {
		errs = append(errs, fmt.Errorf("version: %w", err))
	} else {
		maps.Copy(out, versionFields(version))
	}

	var vOrder, resultSetCaching, autoCreateStats, autoUpdateStats stdsql.NullBool
	if err := db.QueryRowContext(ctx, `
SELECT is_vorder_enabled, is_result_set_caching_on, is_auto_create_stats_on, is_auto_update_stats_on
FROM sys.databases WHERE name = DB_NAME()`,
	).Scan(&vOrder, &resultSetCaching, &autoCreateStats, &autoUpdateStats); err != nil {
		errs = append(errs, fmt.Errorf("sys.databases: %w", err))
	} else {
		for name, v := range map[string]stdsql.NullBool{
			"vOrder": vOrder, "resultSetCaching": resultSetCaching, "autoCreateStats": autoCreateStats, "autoUpdateStats": autoUpdateStats,
		} {
			if v.Valid {
				out[name] = v.Bool
			}
		}
	}

	pools, err := describePools(ctx, db)
	maps.Copy(out, pools)
	errs = append(errs, err)

	return out, errors.Join(errs...)
}

// versionFields picks the engine version and build date from @@VERSION.
func versionFields(version string) log.Fields {
	var out = make(log.Fields)
	var lines = strings.Split(version, "\n")
	if _, v, ok := strings.Cut(lines[0], " - "); ok {
		out["version"] = strings.TrimSpace(v)
	}
	if len(lines) > 1 {
		if build := strings.TrimSpace(lines[1]); build != "" {
			out["build"] = build
		}
	}
	return out
}

// describePools reads the latest report of each SQL pool: the capacity it
// runs on, the share of the capacity it may use, and whether it's under
// pressure.
func describePools(ctx context.Context, db *stdsql.DB) (log.Fields, error) {
	rows, err := db.QueryContext(ctx, `
SELECT p.sql_pool_name, p.current_workspace_capacity, p.max_resource_percentage, p.is_pool_under_pressure
FROM queryinsights.sql_pool_insights p
WHERE p.timestamp = (SELECT MAX(l.timestamp) FROM queryinsights.sql_pool_insights l WHERE l.sql_pool_name = p.sql_pool_name)`)
	if err != nil {
		return nil, fmt.Errorf("sql_pool_insights: %w", err)
	}
	defer rows.Close()

	var out = make(log.Fields)
	var pools = make(map[string]log.Fields)
	for rows.Next() {
		var name string
		var capacity stdsql.NullString
		var maxResource stdsql.NullInt64
		var pressure stdsql.NullBool
		if err := rows.Scan(&name, &capacity, &maxResource, &pressure); err != nil {
			return nil, fmt.Errorf("sql_pool_insights: %w", err)
		}
		if capacity.String != "" {
			out["capacity"] = capacity.String
		}
		pools[name] = log.Fields{"maxResourcePercentage": maxResource.Int64, "underPressure": pressure.Bool}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sql_pool_insights: %w", err)
	}
	if len(pools) > 0 {
		out["sqlPools"] = pools
	}
	return out, nil
}
