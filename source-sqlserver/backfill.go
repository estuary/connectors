package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/estuary/connectors/go/capture/sqlserver/backfill"
	"github.com/estuary/connectors/go/capture/sqlserver/change"
	"github.com/estuary/connectors/go/capture/sqlserver/datatypes"
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

// ScanTableChunk fetches a chunk of rows from the specified table, resuming after the row key in `state.Scanned` if non-nil.
func (db *sqlserverDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, backfillFilter string, callback func(event sqlcapture.ChangeEvent) error) (bool, []byte, error) {
	var scan = db.newChunkScan(info, state, callback)

	// A backfill never switches from key order to partition order. A fresh one goes by
	// partition when the table can be backfilled that way, and one already underway goes
	// by partition only if its state holds a partition. So a key-ordered backfill stays
	// key-ordered even if partition-aware backfills are enabled while it is in progress,
	// since its cursor has passed rows in every partition.
	if state.Mode != sqlcapture.TableStateKeylessBackfill {
		var metadata, err = decodeBackfillMetadata(state.Metadata)
		if err != nil {
			return false, nil, err
		}

		var partitionIndex, unusedReason = partitionedBackfillIndex(info, state)
		var partitionOrdered = state.Scanned != nil && metadata.Partition != nil

		// Warn once, on the first chunk of a backfill, when the table could have been
		// backfilled by partition but its binding rules that out.
		if unusedReason != "" && state.Scanned == nil {
			log.WithFields(log.Fields{
				"stream": scan.streamID,
				"reason": unusedReason,
			}).Warn("table could be backfilled one partition at a time but will be backfilled in key order, which sorts the whole table for every chunk")
		}

		switch {
		case partitionIndex != nil && (state.Scanned == nil || partitionOrdered):
			return db.scanPartitioned(ctx, scan, partitionIndex, metadata.Partition, backfillFilter)
		case partitionOrdered:
			// The resume key is a position within one partition, and in key order it
			// would skip the rows of later partitions which sort below it. So the only
			// safe way to continue in key order is from the beginning.
			log.WithField("stream", scan.streamID).Warn("partition-ordered backfill can no longer continue by partition" +
				" (partition-aware backfills disabled, or the table's key index is no longer partitioned on a non-leading key column)," +
				" restarting its backfill in key order")
			state.Scanned, state.Metadata = nil, clearedMetadata
		}
	}

	query, args, err := backfill.BuildBackfillQuery(state, scan.schema, scan.table, scan.columnTypes, scan.chunkSize, backfillFilter)
	if err != nil {
		return false, nil, err
	}
	rows, rowKey, err := scan.run(ctx, query, args)
	if err != nil {
		return false, nil, err
	}
	return rows < scan.chunkSize, rowKey, nil
}

// chunkScan runs the backfill queries of one chunk, that is one ScanTableChunk call,
// against a table and emits their result rows as change events.
type chunkScan struct {
	db             *sqlserverDatabase
	state          *sqlcapture.TableState
	callback       func(event sqlcapture.ChangeEvent) error
	streamID       sqlcapture.StreamID
	schema, table  string
	chunkSize      int
	columnTypes    map[string]any
	excludeColumns []string
	emitted        int // Rows emitted by this scan so far, across all of its queries.
}

func (db *sqlserverDatabase) newChunkScan(info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, callback func(event sqlcapture.ChangeEvent) error) *chunkScan {
	var columnTypes = make(map[string]any)
	for name, column := range info.Columns {
		columnTypes[name] = column.DataType
	}

	// Computed column values cannot be captured via CDC, so we have to exclude them from
	// backfills as well or we'd end up with incorrect/stale values. Better not to have a
	// property at all in those circumstances.
	var excludeColumns []string
	if details, ok := info.ExtraDetails.(*sqlserverTableDiscoveryDetails); ok {
		excludeColumns = details.ComputedColumns
	}

	return &chunkScan{
		db:             db,
		state:          state,
		callback:       callback,
		streamID:       sqlcapture.JoinStreamID(info.Schema, info.Name),
		schema:         info.Schema,
		table:          info.Name,
		chunkSize:      db.config.Advanced.BackfillChunkSize,
		columnTypes:    columnTypes,
		excludeColumns: excludeColumns,
	}
}

// run executes one backfill query and emits its rows, returning the number of rows and
// the row key of the last one.
func (scan *chunkScan) run(ctx context.Context, query string, args []any) (int, []byte, error) {
	var db, state, streamID = scan.db, scan.state, scan.streamID
	var keyColumns = state.KeyColumns
	var columnTypes = scan.columnTypes

	log.WithFields(log.Fields{"query": query, "args": args}).Debug("executing query")

	// If this is the first chunk being backfilled from this table, log the query for debugging.
	db.logBackfillQuery(streamID, query, args)

	// Set up watchdog timer and run the query
	var watchdogCtx, cancelWatchdog = context.WithCancel(ctx)
	defer cancelWatchdog()
	var watchdogTimer = time.AfterFunc(backfillQueryTimeout, func() {
		log.Warn("backfill watchdog: no progress detected, canceling query")
		cancelWatchdog()
	})
	defer watchdogTimer.Stop()

	rows, err := db.conn.QueryContext(watchdogCtx, query, args...)
	if err != nil {
		return 0, nil, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer rows.Close()

	// Set up the necessary slices for generic scanning over query rows
	cnames, err := rows.Columns()
	if err != nil {
		return 0, nil, err
	}

	// Preprocess result metadata for efficient row handling.
	var outputColumnNames = make([]string, len(cnames))                   // Names of all columns, with omitted columns set to ""
	var outputTranscoders = make([]datatypes.JSONTranscoder, len(cnames)) // Transcoders for all columns, with omitted columns set to nil
	var rowKeyTranscoders = make([]datatypes.FDBTranscoder, len(cnames))  // Transcoders for all columns, with omitted columns set to nil
	for idx, name := range cnames {
		if slices.Contains(scan.excludeColumns, name) {
			continue // Leave outputColumnNames[idx] unset for this column
		}

		outputColumnNames[idx] = name
		outputTranscoders[idx] = datatypes.NewJSONTranscoder(columnTypes[name], db.datatypesConfig)
		if slices.Contains(keyColumns, name) {
			rowKeyTranscoders[idx] = datatypes.NewFDBTranscoder(columnTypes[name])
		}
	}
	outputColumnNames = append(outputColumnNames, "_meta") // Add '_meta' property to the row shape

	var backfillEventInfo = &change.SharedInfo{
		StreamID:    streamID,
		Shape:       encrow.NewShape(outputColumnNames),
		Transcoders: outputTranscoders,
	}

	var keyIndices []int // The indices of the key columns in the row values. Only set for keyed backfills.
	if state.Mode != sqlcapture.TableStateKeylessBackfill {
		keyIndices = make([]int, len(keyColumns))
		for idx, colName := range keyColumns {
			var resultIndex = slices.Index(cnames, colName)
			if resultIndex < 0 {
				return 0, nil, fmt.Errorf("key column %q not found in result columns for %q", colName, streamID)
			}
			keyIndices[idx] = resultIndex
		}
	}

	// Preallocate reusable buffers for change event processing
	var reused struct {
		lsn    []byte               // Reused buffer for the LSN, which is always the empty bytes for backfills
		seqval []byte               // Reused buffer for the sequence value, which holds the row offset in a backfill
		event  sqlserverChangeEvent // Reused event struct to avoid allocations in the hot path
	}
	reused.lsn = LSN{}
	reused.seqval = make([]byte, 10) // Sequence values are always 10 bytes, though we only use the last 8 for backfills

	var vals = make([]any, len(cnames))
	var vptrs = make([]any, len(vals))
	for idx := range vals {
		vptrs[idx] = &vals[idx]
	}

	// Iterate over the result set yielding each row as a change event.
	var resultRows int               // Count of rows received within the current backfill query
	var rowKey = make([]byte, 0, 64) // The row key of the most recent row processed
	for rows.Next() {
		watchdogTimer.Reset(backfillQueryTimeout) // Reset on data movement

		if err := rows.Scan(vptrs...); err != nil {
			return 0, nil, fmt.Errorf("error scanning result row: %w", err)
		}

		var rowOffset = state.BackfilledCount + scan.emitted
		rowKey = rowKey[:0]
		if state.Mode == sqlcapture.TableStateKeylessBackfill {
			rowKey = fmt.Appendf(rowKey, "B%019d", rowOffset) // A 19 digit decimal number is sufficient to hold any 63-bit integer
		} else {
			for _, n := range keyIndices {
				rowKey, err = rowKeyTranscoders[n].TranscodeFDB(rowKey, vals[n])
				if err != nil {
					return 0, nil, fmt.Errorf("error encoding column %q at index %d: %w", cnames[n], n, err)
				}
			}
		}

		binary.BigEndian.PutUint64(reused.seqval[2:], uint64(rowOffset))
		reused.event = sqlserverChangeEvent{
			Shared: backfillEventInfo,
			Meta: sqlserverChangeMetadataCDC{
				Operation: sqlcapture.InsertOp,
				Source: sqlserverSourceInfoCDC{
					SourceCommon: sqlcapture.SourceCommon{
						Schema:   scan.schema,
						Snapshot: true,
						Table:    scan.table,
					},
					LSN:    reused.lsn,
					SeqVal: reused.seqval,
					Tag:    db.config.Advanced.SourceTag,
				},
			},
			RowKey: rowKey,
			Values: vals,
		}
		if err := scan.callback(&reused.event); err != nil {
			return 0, nil, fmt.Errorf("error processing change event: %w", err)
		}
		resultRows++
		scan.emitted++
	}
	err = rows.Err()
	if err != nil && watchdogCtx.Err() == context.Canceled {
		err = fmt.Errorf("backfill query canceled after %s due to lack of progress for table %q", backfillQueryTimeout.String(), streamID)
	}
	return resultRows, rowKey, err
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
