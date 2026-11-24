package connector

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// profileInterval is how often the history of awaiting queries is fetched.
const profileInterval = 15 * time.Second

// queryHistory reads MotherDuck's record of finished queries: time spent
// waiting on resources and executing, the bytes moved between the connector
// and the duckling, the bytes spilled to disk, and the duckling's size.
// Reading it takes the "view query history" permission.
type queryHistory struct {
	db *stdsql.DB
}

// newQueryProfiler profiles the queries of bindings of the database, given as
// a SQL literal. db serves the history and table context their own
// connections, apart from the one the transactor runs its queries on.
func newQueryProfiler(db *stdsql.DB, database string, bindings []*binding) *boilerplate.QueryProfiler {
	var paths = make([][]string, len(bindings))
	var locations = make([]sql.InfoTableLocation, len(bindings))
	for i, b := range bindings {
		paths[i] = b.target.Path
		locations[i] = b.target.InfoLocation
	}
	return boilerplate.NewQueryProfiler(&queryHistory{db: db}, profileInterval, nil, paths, newTableContext(db, database, locations))
}

// queryRower is the transactor's connection, or a transaction on it.
type queryRower interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *stdsql.Row
}

// lastQueryID is the ID of the query the connection just ran. Every query has
// an ID, this one too, so it must be the next query on the connection.
func lastQueryID(ctx context.Context, conn queryRower) (string, error) {
	var id string
	var err = conn.QueryRowContext(ctx, "SELECT CAST(md_last_query_id() AS VARCHAR)").Scan(&id)
	return id, err
}

// setQueryID gives the trace the ID of the query the connection just ran, if
// the query was slow.
func setQueryID(ctx context.Context, conn queryRower, trace *boilerplate.QueryTrace) {
	if !trace.Slow() {
		return
	}
	if id, err := lastQueryID(ctx, conn); err == nil {
		trace.SetID(id)
	}
}

// execTraced runs a commit query of the binding in the transaction, profiled
// as the binding's.
func execTraced(ctx context.Context, txn *stdsql.Tx, profile *boilerplate.QueryProfile, binding int, query string, args ...any) (stdsql.Result, error) {
	var _, trace = profile.Trace(ctx, binding, boilerplate.StatementKind(query))
	res, err := txn.ExecContext(ctx, query, args...)
	setQueryID(ctx, txn, trace)
	trace.Finish(err)
	return res, err
}

// commitTraced commits the transaction, profiled as a query of no binding.
func commitTraced(ctx context.Context, txn *stdsql.Tx, profiler *boilerplate.QueryProfiler) error {
	var _, trace = profiler.Trace(ctx, -1, "commit")
	var err = txn.Commit()
	trace.Finish(err)
	return err
}

// historyQuery reads queries by ID from RECENT_QUERIES, which lists a query
// as soon as it starts, and QUERY_HISTORY, which it moves to after a delay.
// The views return a limited number of an organization's queries, to which
// they apply a filter on a literal first.
const historyQuery = `
SELECT CAST(query_id AS VARCHAR), end_time IS NOT NULL, epoch_ms(total_elapsed_time), epoch_ms(execution_time),
	epoch_ms(wait_time), error_type, instance_type,
	CAST(bytes_uploaded AS BIGINT), CAST(bytes_downloaded AS BIGINT), CAST(bytes_spilled_to_disk AS BIGINT)
FROM md_information_schema.recent_queries WHERE query_id IN (%[1]s)
UNION ALL
SELECT CAST(query_id AS VARCHAR), end_time IS NOT NULL, epoch_ms(total_elapsed_time), epoch_ms(execution_time),
	epoch_ms(wait_time), error_type, instance_type,
	CAST(bytes_uploaded AS BIGINT), CAST(bytes_downloaded AS BIGINT), CAST(bytes_spilled_to_disk AS BIGINT)
FROM md_information_schema.query_history WHERE query_id IN (%[1]s)`

func (h *queryHistory) Fetch(ctx context.Context, ids []string) (map[string]log.Fields, error) {
	var literals = make([]string, len(ids))
	for i, id := range ids {
		parsed, err := uuid.Parse(id)
		if err != nil {
			return nil, fmt.Errorf("query id %q: %w", id, err)
		}
		literals[i] = fmt.Sprintf("'%s'::UUID", parsed)
	}

	rows, err := h.db.QueryContext(ctx, fmt.Sprintf(historyQuery, strings.Join(literals, ", ")))
	if err != nil {
		return nil, historyErr(err)
	}
	defer rows.Close()

	var out = make(map[string]log.Fields)
	for rows.Next() {
		var q historyRow
		if err := rows.Scan(&q.id, &q.ended, &q.total, &q.execution, &q.wait, &q.errorType, &q.instanceType,
			&q.uploaded, &q.downloaded, &q.spilled); err != nil {
			return nil, fmt.Errorf("scanning history: %w", err)
		}
		// A query that hasn't ended is listed with the metrics it has so far.
		if q.ended {
			out[q.id] = q.fields()
		}
	}
	if err := rows.Err(); err != nil {
		return nil, historyErr(err)
	}
	return out, nil
}

// historyErr marks the error of a user who may not view query history as
// such.
func historyErr(err error) error {
	if strings.Contains(err.Error(), "Missing permission for query history") {
		return fmt.Errorf("%w: %w", boilerplate.ErrQueryHistoryDenied, err)
	}
	return fmt.Errorf("querying history: %w", err)
}

type historyRow struct {
	id                            string
	ended                         bool
	total, execution, wait        stdsql.NullInt64
	errorType, instanceType       stdsql.NullString
	uploaded, downloaded, spilled stdsql.NullInt64
}

func (q historyRow) fields() log.Fields {
	var out = log.Fields{
		"totalMs":          q.total.Int64,
		"executionMs":      q.execution.Int64,
		"uploadedBytes":    q.uploaded.Int64,
		"downloadedBytes":  q.downloaded.Int64,
		"spillToDiskBytes": q.spilled.Int64,
	}
	if q.wait.Int64 > 0 {
		out["waitMs"] = q.wait.Int64
	}
	if q.errorType.String != "" {
		out["errorType"] = q.errorType.String
	}
	if q.instanceType.String != "" {
		out["instanceType"] = q.instanceType.String
	}
	return out
}

// newTableContext reads a table's estimated rows, which lag its writes by
// minutes, its columns and indexes, the data and delete files of a DuckLake
// table, and the duckling's size and version. database is a SQL literal.
func newTableContext(db *stdsql.DB, database string, locations []sql.InfoTableLocation) boilerplate.TableContext {
	return func(ctx context.Context, binding int) (log.Fields, error) {
		var out = make(log.Fields)
		var errs []error

		warehouse, err := describeWarehouse(ctx, db, database)
		if len(warehouse) > 0 {
			out["warehouse"] = warehouse
		}
		errs = append(errs, err)

		table, err := describeTable(ctx, db, database, locations[binding], warehouse["databaseType"] == "DUCKLAKE")
		if len(table) > 0 {
			out["table"] = table
		}
		errs = append(errs, err)

		return out, errors.Join(errs...)
	}
}

func describeWarehouse(ctx context.Context, db *stdsql.DB, database string) (log.Fields, error) {
	var out = make(log.Fields)
	var errs []error

	for _, read := range []struct{ field, query string }{
		{"ducklingSize", "SELECT type FROM md_live_duckling_size()"},
		{"duckdbVersion", "SELECT version()"},
		{"extensionVersion", "SELECT md_extension_version FROM md_pragma_version()"},
		// A database attached from another account isn't listed.
		{"databaseType", "SELECT type FROM md_information_schema.databases WHERE name = " + database},
	} {
		var value stdsql.NullString
		if err := db.QueryRowContext(ctx, read.query).Scan(&value); errors.Is(err, stdsql.ErrNoRows) {
			continue
		} else if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", read.field, err))
		} else if value.String != "" {
			out[read.field] = value.String
		}
	}
	return out, errors.Join(errs...)
}

func describeTable(ctx context.Context, db *stdsql.DB, database string, loc sql.InfoTableLocation, duckLake bool) (log.Fields, error) {
	var out = make(log.Fields)
	var errs []error

	var rows, columns, indexes stdsql.NullInt64
	var primaryKey stdsql.NullBool
	if err := db.QueryRowContext(ctx, `
SELECT CAST(estimated_size AS BIGINT), CAST(column_count AS BIGINT), CAST(index_count AS BIGINT), has_primary_key
FROM duckdb_tables()
WHERE database_name = `+database+` AND lower(schema_name) = lower(?) AND lower(table_name) = lower(?)`,
		loc.TableSchema, loc.TableName,
	).Scan(&rows, &columns, &indexes, &primaryKey); err != nil {
		errs = append(errs, fmt.Errorf("duckdb_tables: %w", err))
	} else {
		out["estimatedRows"] = rows.Int64
		out["columns"] = columns.Int64
		if indexes.Int64 > 0 {
			out["indexes"] = indexes.Int64
		}
		if primaryKey.Bool {
			out["primaryKey"] = true
		}
	}

	if duckLake {
		fields, err := describeDuckLakeTable(ctx, db, database, loc)
		for k, v := range fields {
			out[k] = v
		}
		errs = append(errs, err)
	}
	return out, errors.Join(errs...)
}

// describeDuckLakeTable reads the table's data files, and the delete files
// its queries must apply to them. ducklake_table_info doesn't name a table's
// schema, so a table whose name another schema shares is left out.
func describeDuckLakeTable(ctx context.Context, db *stdsql.DB, database string, loc sql.InfoTableLocation) (log.Fields, error) {
	rows, err := db.QueryContext(ctx, `
SELECT CAST(file_count AS BIGINT), CAST(file_size_bytes AS BIGINT), CAST(delete_file_count AS BIGINT), CAST(delete_file_size_bytes AS BIGINT)
FROM ducklake_table_info(`+database+`) WHERE lower(table_name) = lower(?)`, loc.TableName)
	if err != nil {
		return nil, fmt.Errorf("ducklake_table_info: %w", err)
	}
	defer rows.Close()

	var out log.Fields
	var tables int
	for rows.Next() {
		var files, fileBytes, deleteFiles, deleteFileBytes stdsql.NullInt64
		if err := rows.Scan(&files, &fileBytes, &deleteFiles, &deleteFileBytes); err != nil {
			return nil, fmt.Errorf("ducklake_table_info: %w", err)
		}
		out = log.Fields{
			"files":           files.Int64,
			"fileBytes":       fileBytes.Int64,
			"deleteFiles":     deleteFiles.Int64,
			"deleteFileBytes": deleteFileBytes.Int64,
		}
		tables++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ducklake_table_info: %w", err)
	} else if tables != 1 {
		return nil, nil
	}
	return out, nil
}
