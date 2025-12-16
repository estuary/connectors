package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
)

const (
	// As a rule, if a single backfill query takes more than a few _minutes_ something is
	// pretty wrong.
	//
	// However there are some cases where we might need to wait a while. The main ones are
	// when a backfill requires a full-table sort, or when we want to try and consume the
	// entire table in a single query. In the former case, if a sort takes >2h then it's
	// probably never going to succeed. In the latter, there is data moving constantly so
	// we just need to take that into account rather than using a fixed deadline.
	//
	// So if more than <backfillQueryTimeout> goes by _without any data returned_ we'll
	// fail the capture so it can retry.
	backfillQueryTimeout = 2 * time.Hour
)

// ShouldBackfill returns true if a given table's contents should be backfilled, given
// that we intend to capture that table.
func (db *sqlserverDatabase) ShouldBackfill(streamID sqlcapture.StreamID) bool {
	// Allow the setting "*.*" to skip backfilling any tables.
	if db.config.Advanced.SkipBackfills == "*.*" {
		return false
	}

	if db.config.Advanced.SkipBackfills != "" {
		// This repeated splitting is a little inefficient, but this check is done at
		// most once per table during connector startup and isn't really worth caching.
		for _, skipStreamID := range strings.Split(db.config.Advanced.SkipBackfills, ",") {
			if strings.EqualFold(streamID.String(), skipStreamID) {
				return false
			}
		}
	}
	return true
}

// ScanTableChunk fetches a chunk of rows from the specified table, resuming from the `resumeAfter` row key if non-nil.
func (db *sqlserverDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, callback func(event sqlcapture.ChangeEvent) error) (bool, []byte, error) {
	var keyColumns = state.KeyColumns
	var resumeAfter = state.Scanned
	var schema, table = info.Schema, info.Name
	var streamID = sqlcapture.JoinStreamID(schema, table)

	var columnTypes = make(map[string]interface{})
	for name, column := range info.Columns {
		columnTypes[name] = column.DataType
	}

	var computedColumns []string
	if details, ok := info.ExtraDetails.(*sqlserverTableDiscoveryDetails); ok {
		computedColumns = details.ComputedColumns
	}

	// Compute backfill query and arguments list
	var query string
	var args []any
	switch state.Mode {
	case sqlcapture.TableStateKeylessBackfill:
		log.WithFields(log.Fields{
			"stream": streamID,
			"offset": state.BackfilledCount,
		}).Debug("scanning keyless table chunk")
		query = db.keylessScanQuery(info, schema, table)
		args = []any{state.BackfilledCount}
	case sqlcapture.TableStatePreciseBackfill, sqlcapture.TableStateUnfilteredBackfill:
		if resumeAfter != nil {
			var resumeKey, err = sqlcapture.UnpackTuple(resumeAfter, decodeKeyFDB)
			if err != nil {
				return false, nil, fmt.Errorf("error unpacking resume key for %q: %w", streamID, err)
			}
			if len(resumeKey) != len(keyColumns) {
				return false, nil, fmt.Errorf("expected %d resume-key values but got %d", len(keyColumns), len(resumeKey))
			}
			log.WithFields(log.Fields{
				"stream":     streamID,
				"keyColumns": keyColumns,
				"resumeKey":  resumeKey,
			}).Debug("scanning subsequent table chunk")
			query = db.buildScanQuery(false, keyColumns, columnTypes, schema, table)
			args = resumeKey
		} else {
			log.WithFields(log.Fields{
				"stream":     streamID,
				"keyColumns": keyColumns,
			}).Debug("scanning initial table chunk")
			query = db.buildScanQuery(true, keyColumns, columnTypes, schema, table)
		}
	default:
		return false, nil, fmt.Errorf("invalid backfill mode %q", state.Mode)
	}

	log.WithFields(log.Fields{"query": query, "args": args}).Debug("executing query")

	// Set up watchdog timer
	var watchdogCtx, cancelWatchdog = context.WithCancel(ctx)
	defer cancelWatchdog()
	var watchdogTimer = time.AfterFunc(backfillQueryTimeout, func() {
		log.Warn("backfill watchdog: no progress detected, canceling query")
		cancelWatchdog()
	})
	defer watchdogTimer.Stop()

	rows, err := db.conn.QueryContext(watchdogCtx, query, args...)
	if err != nil {
		return false, nil, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer rows.Close()

	// Set up the necessary slices for generic scanning over query rows
	cnames, err := rows.Columns()
	if err != nil {
		return false, nil, err
	}
	var vals = make([]any, len(cnames))
	var vptrs = make([]any, len(vals))
	for idx := range vals {
		vptrs[idx] = &vals[idx]
	}

	// Preprocess result metadata for efficient row handling.
	var outputColumnNames = make([]string, len(cnames))         // Names of all columns, with omitted columns set to ""
	var outputColumnTypes = make([]any, len(cnames))            // Types of all columns, with omitted columns set to nil
	var outputTranscoders = make([]jsonTranscoder, len(cnames)) // Transcoders for all columns, with omitted columns set to nil
	var rowKeyTranscoders = make([]fdbTranscoder, len(cnames))  // Transcoders for all columns, with omitted columns set to nil
	for idx, name := range cnames {
		// Computed column values cannot be captured via CDC, so we have to exclude them from
		// backfills as well or we'd end up with incorrect/stale values. Better not to have a
		// property at all in those circumstances.
		if slices.Contains(computedColumns, name) {
			continue
		}

		outputColumnNames[idx] = name
		outputColumnTypes[idx] = columnTypes[name]
		outputTranscoders[idx] = db.backfillJSONTranscoder(columnTypes[name])
		if slices.Contains(keyColumns, name) {
			rowKeyTranscoders[idx] = db.backfillFDBTranscoder(columnTypes[name])
		}
	}
	outputColumnNames = append(outputColumnNames, "_meta") // Add '_meta' property to the row shape

	var keyIndices []int // The indices of the key columns in the row values. Only set for keyed backfills.
	if state.Mode != sqlcapture.TableStateKeylessBackfill {
		keyIndices = make([]int, len(keyColumns))
		for idx, colName := range keyColumns {
			var resultIndex = slices.Index(cnames, colName)
			if resultIndex < 0 {
				return false, nil, fmt.Errorf("key column %q not found in result columns for %q", colName, streamID)
			}
			keyIndices[idx] = resultIndex
		}
	}
	var backfillEventInfo = &sqlserverChangeSharedInfo{
		StreamID:    streamID,
		Shape:       encrow.NewShape(outputColumnNames),
		Transcoders: outputTranscoders,
	}

	// Preallocate reusable buffers for change event processing
	var reused struct {
		lsn    []byte               // Reused buffer for the LSN, which is always the empty bytes for backfills
		seqval []byte               // Reused buffer for the sequence value, which holds the row offset in a backfill
		event  sqlserverChangeEvent // Reused event struct to avoid allocations in the hot path
	}
	reused.lsn = LSN{}
	reused.seqval = make([]byte, 10) // Sequence values are always 10 bytes, though we only use the last 8 for backfills

	// Iterate over the result set yielding each row as a change event.
	var resultRows int               // Count of rows received within the current backfill chunk
	var rowKey = make([]byte, 0, 64) // The row key of the most recent row processed
	var rowOffset = state.BackfilledCount
	for rows.Next() {
		watchdogTimer.Reset(backfillQueryTimeout) // Reset on data movement

		if err := rows.Scan(vptrs...); err != nil {
			return false, nil, fmt.Errorf("error scanning result row: %w", err)
		}

		if state.Mode == sqlcapture.TableStateKeylessBackfill {
			rowKey = []byte(fmt.Sprintf("B%019d", rowOffset)) // A 19 digit decimal number is sufficient to hold any 63-bit integer
		} else {
			rowKey = rowKey[:0]
			for _, n := range keyIndices {
				rowKey, err = rowKeyTranscoders[n].TranscodeFDB(rowKey, vals[n])
				if err != nil {
					return false, nil, fmt.Errorf("error encoding column %q at index %d: %w", cnames[n], n, err)
				}
			}
		}

		binary.BigEndian.PutUint64(reused.seqval[2:], uint64(rowOffset))
		reused.event = sqlserverChangeEvent{
			Shared: backfillEventInfo,
			Meta: sqlserverChangeMetadata{
				Operation: sqlcapture.InsertOp,
				Source: sqlserverSourceInfo{
					SourceCommon: sqlcapture.SourceCommon{
						Schema:   schema,
						Snapshot: true,
						Table:    table,
					},
					LSN:    reused.lsn,
					SeqVal: reused.seqval,
					Tag:    db.config.Advanced.SourceTag,
				},
			},
			RowKey: rowKey,
			Values: vals,
		}
		if err := callback(&reused.event); err != nil {
			return false, nil, fmt.Errorf("error processing change event: %w", err)
		}
		resultRows++
		rowOffset++
	}
	var backfillComplete = resultRows < db.config.Advanced.BackfillChunkSize
	err = rows.Err()
	if err != nil && watchdogCtx.Err() == context.Canceled {
		err = fmt.Errorf("backfill query canceled after %s due to lack of progress for table %q", backfillQueryTimeout.String(), streamID)
	}
	return backfillComplete, rowKey, err
}

func (db *sqlserverDatabase) keylessScanQuery(_ *sqlcapture.DiscoveryInfo, schemaName, tableName string) string {
	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT * FROM [%s].[%s]", schemaName, tableName)
	fmt.Fprintf(query, " ORDER BY %%%%physloc%%%%")
	fmt.Fprintf(query, " OFFSET @p1 ROWS FETCH FIRST %d ROWS ONLY;", db.config.Advanced.BackfillChunkSize)
	return query.String()
}

func (db *sqlserverDatabase) buildScanQuery(start bool, keyColumns []string, columnTypes map[string]interface{}, schemaName, tableName string) string {
	var pkey []string
	var args []string
	for idx, colName := range keyColumns {
		var quotedName = quoteColumnName(colName)
		if _, ok := columnTypes[colName].(*sqlserverTextColumnType); ok {
			args = append(args, fmt.Sprintf("@KeyColumn%d", idx+1))
		} else {
			args = append(args, fmt.Sprintf("@p%d", idx+1))
		}
		pkey = append(pkey, quotedName)
	}

	var query = new(strings.Builder)
	fmt.Fprintf(query, "BEGIN ")
	if !start {
		for idx, colName := range keyColumns {
			if textInfo, ok := columnTypes[colName].(*sqlserverTextColumnType); ok {
				fmt.Fprintf(query, "DECLARE @KeyColumn%[1]d AS %[2]s = @p%[1]d; ", idx+1, textInfo.FullType)
			}
		}
	}
	fmt.Fprintf(query, "SELECT * FROM [%s].[%s]", schemaName, tableName)
	if !start {
		for i := range pkey {
			if i == 0 {
				fmt.Fprintf(query, " WHERE (")
			} else {
				fmt.Fprintf(query, ") OR (")
			}

			for j := 0; j < i; j++ {
				fmt.Fprintf(query, "%s = %s AND ", pkey[j], args[j])
			}
			fmt.Fprintf(query, "%s > %s", pkey[i], args[i])
		}
		fmt.Fprintf(query, ")")
	}
	fmt.Fprintf(query, " ORDER BY %s", strings.Join(pkey, ", "))
	fmt.Fprintf(query, " OFFSET 0 ROWS FETCH FIRST %d ROWS ONLY;", db.config.Advanced.BackfillChunkSize)
	fmt.Fprintf(query, " END")
	return query.String()
}

func quoteColumnName(name string) string {
	// From https://learn.microsoft.com/en-us/sql/relational-databases/databases/database-identifiers
	// it appears to always be valid to take an identifier for which quoting is optional and enclose
	// it in brackets.
	//
	// It is not immediately clear whether there are any special characters which need further
	// escaping in a bracket delimited identifier, but for now adding brackets is strictly better
	// than not adding brackets.
	return `[` + name + `]`
}

type sqlserverTableStatistics struct {
	RowCount int64 // Row count from sys.partitions
	Err      error // Error from querying statistics, if any (cached to avoid performance concerns)
}

func (db *sqlserverDatabase) queryTableStatistics(ctx context.Context, schema, table string) (*sqlserverTableStatistics, error) {
	var streamID = sqlcapture.JoinStreamID(schema, table)
	if db.tableStatistics == nil {
		db.tableStatistics = make(map[sqlcapture.StreamID]*sqlserverTableStatistics)
	}

	// Return cached result if available
	if cached := db.tableStatistics[streamID]; cached != nil {
		return cached, cached.Err
	}

	// Query sys.partitions for row count estimate
	var query = `
		SELECT SUM(p.rows)
		FROM sys.partitions p
		JOIN sys.tables t ON p.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id
		WHERE s.name = @p1 AND t.name = @p2
		  AND p.index_id IN (0, 1)`
	var rowCount sql.NullInt64
	if err := db.conn.QueryRowContext(ctx, query, schema, table).Scan(&rowCount); err != nil {
		var stats = &sqlserverTableStatistics{Err: fmt.Errorf("error querying table statistics for %q: %w", streamID, err)}
		db.tableStatistics[streamID] = stats
		return stats, stats.Err
	}

	var stats = &sqlserverTableStatistics{
		RowCount: rowCount.Int64,
	}

	log.WithFields(log.Fields{
		"stream":   streamID,
		"rowCount": stats.RowCount,
	}).Debug("queried table statistics")

	db.tableStatistics[streamID] = stats
	return stats, nil
}

func (db *sqlserverDatabase) EstimatedRowCounts(ctx context.Context, tables []sqlcapture.TableID) (map[sqlcapture.TableID]int, error) {
	var result = make(map[sqlcapture.TableID]int)
	for _, table := range tables {
		var stats, err = db.queryTableStatistics(ctx, table.Schema, table.Table)
		if err != nil {
			return nil, err
		}
		result[table] = int(stats.RowCount)
	}
	return result, nil
}
