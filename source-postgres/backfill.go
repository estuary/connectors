package main

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sirupsen/logrus"
)

var statementTimeoutRegexp = regexp.MustCompile(`canceling statement due to statement timeout`)

// ScanTableChunk fetches a chunk of rows from the specified table, resuming from `resumeKey` if non-nil.
func (db *postgresDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, callback func(event sqlcapture.ChangeEvent) error) (bool, []byte, error) {
	var keyColumns = state.KeyColumns
	var resumeAfter = state.Scanned
	var schema, table = info.Schema, info.Name
	var streamID = sqlcapture.JoinStreamID(schema, table)
	var logEntry = logrus.WithField("stream", streamID)

	var columnTypes = make(map[string]interface{})
	for name, column := range info.Columns {
		columnTypes[name] = column.DataType
	}

	var generatedColumns []string
	if details, ok := info.ExtraDetails.(*postgresTableDiscoveryDetails); ok {
		generatedColumns = details.GeneratedColumns
	}

	// Compute backfill query and arguments list
	var query string
	var args []any

	var usingCTID bool                  // True when we're running a CTID-ordered backfill
	var afterCTID, untilCTID pgtype.TID // When using CTID, these are the lower and upper bounds for the backfill query
	var tableMaximumPageID int64        // When using CTID, this is a conservative (over)estimate of the maximum page ID for the table, used to determine when backfill is complete

	switch state.Mode {
	case sqlcapture.TableStateKeylessBackfill:
		var stats, err = db.queryTableStatistics(ctx, schema, table)
		if err != nil {
			return false, nil, err
		}
		tableMaximumPageID = stats.MaxPageID

		if resumeAfter == nil {
			resumeAfter = []byte("(0,0)")
		}
		if err := afterCTID.Scan(string(resumeAfter)); err != nil {
			return false, nil, fmt.Errorf("error parsing resume CTID for %q: %w", streamID, err)
		} else if !afterCTID.Valid {
			return false, nil, fmt.Errorf("internal error: invalid resume CTID value %q for %q", resumeAfter, streamID)
		}

		// This is a fairly simple heuristic, but it should work well enough in practice. We
		// assume that each page holds at most 100 rows (average row size of 82 bytes). This
		// is an overestimate for most real tables, but that just means we usually get fewer
		// than <chunkSize> rows per query, which is fine and the numbers are still reasonable
		// for any plausible table.
		//
		// Since the CTID / Page ID spaces are independent for each partition of a partitioned
		// table, we also divide the pages-per-query count by the number of partitions, again
		// so that we get approximately <chunkSize> rows per query regardless of partitioning.
		//
		// Then we just add 1 so that we always request at least one full page no matter what.
		var pagesPerChunk = (db.config.Advanced.BackfillChunkSize / (100 * stats.PartitionCount)) + 1

		untilCTID = afterCTID
		untilCTID.BlockNumber += uint32(pagesPerChunk)
		untilCTID.OffsetNumber = 0

		logEntry.WithField("after", afterCTID).WithField("until", untilCTID).Debug("scanning keyless table chunk")
		query = db.keylessScanQuery(info, schema, table)
		args = []any{afterCTID, untilCTID}
		usingCTID = true
	case sqlcapture.TableStatePreciseBackfill, sqlcapture.TableStateUnfilteredBackfill:
		var isPrecise = (state.Mode == sqlcapture.TableStatePreciseBackfill)
		if resumeAfter != nil {
			var resumeKey, err = sqlcapture.UnpackTuple(resumeAfter, decodeKeyFDB)
			if err != nil {
				return false, nil, fmt.Errorf("error unpacking resume key for %q: %w", streamID, err)
			}
			if len(resumeKey) != len(keyColumns) {
				return false, nil, fmt.Errorf("expected %d resume-key values but got %d", len(keyColumns), len(resumeKey))
			}
			logEntry.WithFields(logrus.Fields{
				"keyColumns": keyColumns,
				"resumeKey":  resumeKey,
			}).Debug("scanning subsequent table chunk")
			query = db.buildScanQuery(false, isPrecise, keyColumns, columnTypes, schema, table)
			args = resumeKey
		} else {
			logEntry.WithField("keyColumns", keyColumns).Debug("scanning initial table chunk")
			query = db.buildScanQuery(true, isPrecise, keyColumns, columnTypes, schema, table)
		}
	default:
		return false, nil, fmt.Errorf("invalid backfill mode %q", state.Mode)
	}

	// If this is the first chunk being backfilled, run an `EXPLAIN` on it and log the results
	db.explainQuery(ctx, streamID, query, args)

	// Execute the backfill query to fetch rows from the database
	logEntry.WithFields(logrus.Fields{"query": query, "args": args}).Debug("executing query")
	rows, err := db.conn.Query(ctx, query, args...)
	if err != nil {
		return false, nil, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer rows.Close()

	// Preprocess the result column information for efficient result processing.
	var typeMap = rows.Conn().TypeMap()
	var resultColumns = rows.FieldDescriptions()
	var resultColumnNames = make([]string, len(resultColumns))         // Names of all columns
	var outputColumnNames = make([]string, len(resultColumns))         // Names of all columns, with omitted columns (generated plus xmin) set to ""
	var outputTranscoders = make([]jsonTranscoder, len(resultColumns)) // Transcoders for all columns, with omitted columns set to nil
	var rowKeyTranscoders = make([]fdbTranscoder, len(resultColumns))  // Transcoders for row key columns, with omitted columns set to nil
	for idx, col := range resultColumns {
		var columnName = string(col.Name)
		resultColumnNames[idx] = columnName

		// We omit the XMIN column because we never want to capture its value in the output.
		//
		// We omit generated columns because they can't be captured by CDC, which means we
		// mustn't capture them via backfills either or we'll end up with stale values.
		// Best not to capture them at all.
		if columnName == "xmin" || slices.Contains(generatedColumns, columnName) {
			continue
		}

		var columnInfo *sqlcapture.ColumnInfo
		var isPrimaryKey bool
		if info != nil {
			if x, ok := info.Columns[columnName]; ok {
				columnInfo = &x
			}
			isPrimaryKey = slices.Contains(info.PrimaryKey, columnName)
		}

		outputColumnNames[idx] = columnName
		outputTranscoders[idx] = db.constructJSONTranscoder(columnInfo.DataType, isPrimaryKey, typeMap, &resultColumns[idx])
		if slices.Contains(keyColumns, columnName) {
			rowKeyTranscoders[idx] = db.constructFDBTranscoder(typeMap, &resultColumns[idx])
		}
	}
	// Append the '_meta' column to the output column names list so event metadata can
	// be added in the appropriate spot during JSON serialization.
	outputColumnNames = append(outputColumnNames, "_meta")
	var xminIndex = slices.Index(resultColumnNames, "xmin") // The index of the xmin column, if it exists. Used for XID filtering.

	var keyIndices []int // The indices of the key columns in the row values. Only set for keyed backfills.
	if !usingCTID {
		keyIndices = make([]int, len(keyColumns))
		for idx, colName := range keyColumns {
			var resultIndex = slices.Index(resultColumnNames, colName)
			if resultIndex < 0 {
				return false, nil, fmt.Errorf("key column %q not found in result columns for %q", colName, streamID)
			}
			keyIndices[idx] = resultIndex
		}
	}

	// Construct the backfill metadata struct which will be shared across all events.
	var backfillEventInfo = &postgresChangeSharedInfo{
		StreamID:    streamID,
		Shape:       encrow.NewShape(outputColumnNames),
		Transcoders: outputTranscoders,
	}

	// Process the results into `changeEvent` structs and return them
	var totalRows, resultRows int    // totalRows counts DB returned rows, resultRows counts rows after XID filtering
	var rowKey = make([]byte, 0, 64) // The row key of the most recent row processed, for keyed backfills only
	var rowOffset = state.BackfilledCount
	var event = postgresChangeEvent{} // Preallocated event struct, reused for each row
	logEntry.Debug("translating query rows to change events")
	for rows.Next() {
		var rawValues = rows.RawValues()
		totalRows++

		// Compute row key and update 'nextRowKey' before XID filtering, so that we can resume
		// correctly when doing an XMIN-filtered backfill even if no result rows satisfy the filter.
		if !usingCTID {
			rowKey = rowKey[:0] // Reset the row key buffer for each row
			for _, n := range keyIndices {
				rowKey, err = rowKeyTranscoders[n].TranscodeFDB(rowKey, rawValues[n])
				if err != nil {
					return false, nil, fmt.Errorf("error encoding column %q at index %d: %w", resultColumnNames[n], n, err)
				}
			}
		}

		// Filter result rows based on XMIN values. Since we 'SELECT xmin::text' we can safely
		// assume that the raw bytes of the result value are the string representation of the XID.
		if db.config.Advanced.MinimumBackfillXID != "" && xminIndex >= 0 {
			var xminStr = string(rawValues[xminIndex])
			xmin, err := strconv.ParseUint(xminStr, 10, 64)
			if err != nil {
				return false, nil, fmt.Errorf("invalid xmin value %q: %w", xminStr, err)
			}
			minXID, _ := strconv.ParseUint(db.config.Advanced.MinimumBackfillXID, 10, 64) // Error ignored as it's validated at config load

			if compareXID32(uint32(xmin), uint32(minXID)) < 0 {
				continue // Skip this row
			}
		}

		event = postgresChangeEvent{
			Info: backfillEventInfo,
			Meta: postgresChangeMetadata{
				Operation: sqlcapture.InsertOp, // All backfill events are inserts
				Source: postgresSource{
					SourceCommon: sqlcapture.SourceCommon{
						Millis:   0, // Not known.
						Schema:   schema,
						Snapshot: true,
						Table:    table,
					},
					Location: [3]int{-1, rowOffset, 0},
				},
			},
			RowKey: rowKey,
			Values: rawValues,
		}
		if err := callback(&event); err != nil {
			return false, nil, fmt.Errorf("error processing change event: %w", err)
		}
		resultRows++ // Only increment for rows that passed the filter and were sent
		rowOffset++
	}

	if err := rows.Err(); err != nil {
		// As a special case, consider statement timeouts to be not an error so long as we got at least
		// one row back (resultRows > 0) and we're using a keyed backfill (usingCTID == false), where we
		// expect to receive rows in ascending key order and can resume from the last row received.
		//
		// This allows us to make partial progress on slow databases with statement timeouts set, without
		// having to fine-tune the backfill chunk size.
		//
		// Because of the CTID related caveat, we might want to eventually implement logic to automatically
		// lower the chunk size when we see a statement timeout.
		if pgErr, ok := err.(*pgconn.PgError); ok && statementTimeoutRegexp.MatchString(pgErr.Message) && totalRows > 0 && !usingCTID {
			logrus.WithFields(logrus.Fields{
				"stream":     streamID,
				"totalRows":  totalRows,
				"resultRows": resultRows,
			}).Warn("backfill query interrupted by statement timeout; partial progress saved")

			// Even if resultRows is 0, if totalRows > 0, we made progress scanning, so we don't return an error.
			// The next chunk will resume correctly based on the last row's key/ctid regardless of XID filtering.
			return false, rowKey, nil
		}
		return false, nil, err
	}

	// In CTID mode, the backfill is complete when the upper bound CTID of our query is >= our estimated
	// maximum page ID for the table, and the resume cursor is always the upper-bound CTID of the query.
	if usingCTID {
		var backfillComplete = int64(untilCTID.BlockNumber) >= tableMaximumPageID
		var resumeCursor = []byte(fmt.Sprintf("(%d,%d)", untilCTID.BlockNumber, untilCTID.OffsetNumber))
		logEntry.WithFields(logrus.Fields{
			"totalRows":  totalRows,
			"resultRows": resultRows,
			"chunkSize":  db.config.Advanced.BackfillChunkSize,
			"complete":   backfillComplete,
		}).Debug("finished processing table chunk")
		return backfillComplete, resumeCursor, nil
	}

	// In keyed mode, the backfill is complete if the *total* number of rows returned by the DB was less
	// than the requested chunk size. This correctly handles cases where the last chunk might be filtered
	// down to zero rows by XMIN.
	var backfillComplete = totalRows < db.config.Advanced.BackfillChunkSize
	logEntry.WithFields(logrus.Fields{
		"totalRows":  totalRows,
		"resultRows": resultRows,
		"chunkSize":  db.config.Advanced.BackfillChunkSize,
		"complete":   backfillComplete,
	}).Debug("finished processing table chunk")
	return backfillComplete, rowKey, nil
}

// WriteWatermark writes the provided string into the 'watermarks' table.
func (db *postgresDatabase) WriteWatermark(ctx context.Context, watermark string) error {
	logrus.WithField("watermark", watermark).Debug("writing watermark")
	var query = fmt.Sprintf(`INSERT INTO %s (slot, watermark) VALUES ($1,$2) ON CONFLICT (slot) DO UPDATE SET watermark = $2;`, db.config.Advanced.WatermarksTable)
	var _, err = db.conn.Exec(ctx, query, db.config.Advanced.SlotName, watermark)
	if err != nil {
		return fmt.Errorf("error upserting new watermark for slot %q: %w", db.config.Advanced.SlotName, err)
	}
	return nil
}

// WatermarksTable returns the name of the table to which WriteWatermarks writes UUIDs.
func (db *postgresDatabase) WatermarksTable() sqlcapture.StreamID {
	var bits = strings.SplitN(db.config.Advanced.WatermarksTable, ".", 2)
	return sqlcapture.JoinStreamID(bits[0], bits[1])
}

// The set of column types for which we need to specify `COLLATE "C"` to get
// proper ordering and comparison. Represented as a map[string]bool so that it can be
// combined with the "is the column typename a string" check into one if statement.
var columnBinaryKeyComparison = map[string]bool{
	"varchar": true,
	"bpchar":  true,
	"text":    true,
}

// Construct a filter expression for the backfill query which excludes rows based on their xmin.
// Includes a random aspect so that we receive a trickle of data even while processing parts of
// the table which are fully excluded. When XID filtering is not in use the resulting expression
// is empty since we don't need any of that.
func (db *postgresDatabase) backfillQueryFilterXMIN() string {
	if db.config.Advanced.MinimumBackfillXID != "" {
		const filterValidXID = "(xmin::text::bigint >= 3)"
		var filterLowerXID = fmt.Sprintf("((((xmin::text::bigint - %s::bigint)<<32)>>32) >= 0)", db.config.Advanced.MinimumBackfillXID)

		// Note: We use XMAX here because it's the largest of the "current XID" values and we never want to
		// exclude anything except for rows whose xmin values are from earlier wraparound epochs.
		const filterUpperXID = "((((txid_snapshot_xmax(txid_current_snapshot()) - xmin::text::bigint)<<32)>>32) >= 0)"

		// Final query expression:
		//   - Select 0.1% of rows at random to ensure we have a trickle of data movement even in fully excluded parts of the table.
		//   - Exclude rows with invalid XIDs. This may not actually be necessary as PostgreSQL should never send those, but it can't hurt.
		//   - Exclude rows whose XMIN value is less than the configured minimum.
		//   - Exclude rows whose XMIN valid is _greater_ than the current server XID, as they're from earlier wraparound epochs.
		return fmt.Sprintf("((RANDOM() < 0.001) OR (%s AND %s AND %s))", filterValidXID, filterLowerXID, filterUpperXID)
	}
	return ""
}

func (db *postgresDatabase) keylessScanQuery(_ *sqlcapture.DiscoveryInfo, schemaName, tableName string) string {
	var query = new(strings.Builder)

	// Include xmin when using XID filtering
	var xidFiltered = db.config.Advanced.MinimumBackfillXID != ""
	if xidFiltered {
		fmt.Fprintf(query, `SELECT xmin::text AS xmin, * FROM "%s"."%s"`, schemaName, tableName)
	} else {
		fmt.Fprintf(query, `SELECT * FROM "%s"."%s"`, schemaName, tableName)
	}

	fmt.Fprintf(query, ` WHERE ctid > $1 AND ctid <= $2`)
	if xidFiltered {
		fmt.Fprintf(query, ` AND %s`, db.backfillQueryFilterXMIN())
	}
	return query.String()
}

func (db *postgresDatabase) buildScanQuery(start, isPrecise bool, keyColumns []string, columnTypes map[string]interface{}, schemaName, tableName string) string {
	// Construct lists of key specifiers and placeholders. They will be joined with commas and used in the query itself.
	var pkey []string
	var args []string
	for idx, colName := range keyColumns {
		var quotedName = quoteColumnName(colName)
		// If a precise backfill is requested *and* the column type requires binary ordering for precise
		// backfill comparisons to work, add the 'COLLATE "C"' qualifier to the column name.
		if colType, ok := columnTypes[colName].(string); ok && isPrecise && columnBinaryKeyComparison[colType] {
			pkey = append(pkey, quotedName+` COLLATE "C"`)
		} else {
			pkey = append(pkey, quotedName)
		}
		args = append(args, fmt.Sprintf("$%d", idx+1))
	}

	// Generate a list of individual WHERE clauses which should be ANDed together
	var whereClauses []string
	if !start {
		whereClauses = append(whereClauses, fmt.Sprintf(`(%s) > (%s)`, strings.Join(pkey, ", "), strings.Join(args, ", ")))
	}

	var xidFiltered = db.config.Advanced.MinimumBackfillXID != ""
	if xidFiltered {
		whereClauses = append(whereClauses, db.backfillQueryFilterXMIN())
	}

	// Construct the query itself
	var query = new(strings.Builder)
	if xidFiltered {
		fmt.Fprintf(query, `SELECT xmin::text AS xmin, * FROM "%s"."%s"`, schemaName, tableName)
	} else {
		fmt.Fprintf(query, `SELECT * FROM "%s"."%s"`, schemaName, tableName)
	}

	if len(whereClauses) > 0 {
		fmt.Fprintf(query, " WHERE %s", strings.Join(whereClauses, " AND "))
	}
	fmt.Fprintf(query, ` ORDER BY %s`, strings.Join(pkey, ", "))
	fmt.Fprintf(query, " LIMIT %d;", db.config.Advanced.BackfillChunkSize)
	return query.String()
}

func quoteColumnName(name string) string {
	// From https://www.postgresql.org/docs/14/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS:
	//
	//     Quoted identifiers can contain any character, except the character with code zero.
	//     (To include a double quote, write two double quotes.)
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func (db *postgresDatabase) explainQuery(ctx context.Context, streamID sqlcapture.StreamID, query string, args []interface{}) {
	// Only EXPLAIN the backfill query once per connector invocation
	if db.explained == nil {
		db.explained = make(map[sqlcapture.StreamID]struct{})
	}
	if _, ok := db.explained[streamID]; ok {
		return
	}
	db.explained[streamID] = struct{}{}

	// Ask the database to EXPLAIN the backfill query
	var explainQuery = "EXPLAIN " + query
	logrus.WithFields(logrus.Fields{
		"id":    streamID,
		"query": explainQuery,
	}).Info("explain backfill query")
	explainResult, err := db.conn.Query(ctx, explainQuery, args...)
	if err != nil {
		logrus.WithField("id", streamID).Error("unable to execute query")
		return
	}
	defer explainResult.Close()

	// Log the response, doing a bit of extra work to make it readable
	var keys = explainResult.FieldDescriptions()
	for explainResult.Next() {
		var vals, err = explainResult.Values()
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"id":  streamID,
				"err": err,
			}).Error("error getting row value")
			return
		}

		var result []string
		for idx, val := range vals {
			result = append(result, fmt.Sprintf("%s=%v", string(keys[idx].Name), val))
		}
		logrus.WithFields(logrus.Fields{
			"id":       streamID,
			"response": result,
		}).Info("explain backfill query")
	}
}

func compareXID32(a, b uint32) int {
	// Compare two XIDs according to circular comparison logic such that
	// a < b if B lies in the range [a, a+2^31).
	if a == b {
		return 0 // a == b
	} else if ((b - a) & 0x80000000) == 0 {
		return -1 // a < b
	}
	return 1 // a > b
}

type postgresTableStatistics struct {
	RelPages       int64 // Approximate number of pages in the table
	RelTuples      int64 // Approximate number of live tuples in the table
	MaxPageID      int64 // Conservative upper bound for the maximum page ID
	PartitionCount int   // Number of partitions for the table, including the parent table itself
}

const queryTableStatistics = `
SELECT c.relpages, c.reltuples,
  -- Maximum page ID across all partitions and the parent table. Each partition is a
  -- separate table with its own page numbering. We multiply by a 1.05 fudge factor just
  -- to make extra sure the result will be greater than any possible live row's page ID.
  CEIL(1.05*GREATEST(
    (pg_relation_size(c.oid) / current_setting('block_size')::int),
    COALESCE((
      SELECT MAX(pg_relation_size(inhrelid) / current_setting('block_size')::int)
      FROM pg_inherits WHERE inhparent = c.oid
    ), 0)
  )) AS max_page_id,
  -- Partition count, plus one for the parent table itself
  (SELECT COUNT(inhrelid)::int + 1 FROM pg_inherits WHERE inhparent = c.oid) AS partition_count
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2;`

func (db *postgresDatabase) queryTableStatistics(ctx context.Context, schema, table string) (*postgresTableStatistics, error) {
	var streamID = sqlcapture.JoinStreamID(schema, table)
	if db.tableStatistics == nil {
		db.tableStatistics = make(map[sqlcapture.StreamID]*postgresTableStatistics)
	}
	if db.tableStatistics[streamID] != nil {
		return db.tableStatistics[streamID], nil
	}

	var stats = &postgresTableStatistics{}
	if err := db.conn.QueryRow(ctx, queryTableStatistics, schema, table).Scan(&stats.RelPages, &stats.RelTuples, &stats.MaxPageID, &stats.PartitionCount); err != nil {
		return nil, fmt.Errorf("error querying table statistics for %q: %w", streamID, err)
	}
	logrus.WithFields(logrus.Fields{
		"stream":     streamID,
		"relPages":   stats.RelPages,
		"relTuples":  stats.RelTuples,
		"maxPageID":  stats.MaxPageID,
		"partitions": stats.PartitionCount,
	}).Info("queried table statistics")
	db.tableStatistics[streamID] = stats
	return stats, nil
}
