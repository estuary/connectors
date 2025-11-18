package main

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/sirupsen/logrus"
)

var statementTimeoutRegexp = regexp.MustCompile(`maximum statement execution time exceeded`)

func (db *mysqlDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, callback func(event sqlcapture.ChangeEvent) error) (bool, []byte, error) {
	var keyColumns = state.KeyColumns
	var resumeAfter = state.Scanned
	var schema, table = info.Schema, info.Name
	var streamID = sqlcapture.JoinStreamID(schema, table)

	var columnTypes = make(map[string]interface{})
	for name, column := range info.Columns {
		columnTypes[name] = column.DataType
	}

	// Compute backfill query and arguments list
	var query string
	var args []any

	switch state.Mode {
	case sqlcapture.TableStateKeylessBackfill:
		logrus.WithFields(logrus.Fields{
			"stream": streamID,
			"offset": state.BackfilledCount,
		}).Debug("scanning keyless table chunk")
		query = db.keylessScanQuery(info, schema, table)
		args = []any{state.BackfilledCount}
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

			logrus.WithFields(logrus.Fields{
				"stream":     streamID,
				"keyColumns": keyColumns,
				"resumeKey":  resumeKey,
			}).Debug("scanning subsequent table chunk")

			// Splat the resume key into runs of arguments matching the predicate built by buildScanQuery.
			// This is super gross, and is a work-around for lack of indexed positional argument support,
			// AND lack of implementation for named arguments in our MySQL client.
			// See: https://github.com/go-sql-driver/mysql/issues/561
			for i := range resumeKey {
				args = append(args, resumeKey[:i+1]...)
			}
			query = db.buildScanQuery(false, isPrecise, keyColumns, columnTypes, schema, table)
		} else {
			logrus.WithFields(logrus.Fields{
				"stream":     streamID,
				"keyColumns": keyColumns,
			}).Debug("scanning initial table chunk")
			query = db.buildScanQuery(true, isPrecise, keyColumns, columnTypes, schema, table)
		}
	default:
		return false, nil, fmt.Errorf("invalid backfill mode %q", state.Mode)
	}

	// If this is the first chunk being backfilled, run an `EXPLAIN` on it and log the results
	db.explainQuery(streamID, query, args)

	// Execute the backfill query to fetch rows from the database
	logrus.WithFields(logrus.Fields{"query": query, "args": args}).Debug("executing query")

	// Construct prototype change metadata which will be copied for all rows of this binlog event
	var prototypeChangeMetadata = mysqlChangeMetadata{
		Operation: sqlcapture.InsertOp,
		Source: mysqlSourceInfo{
			SourceCommon: sqlcapture.SourceCommon{
				Millis:   0, // Not known.
				Schema:   schema,
				Snapshot: true,
				Table:    table,
			},
			Cursor: mysqlChangeEventCursor{
				BinlogFile: "backfill",
			},
			Tag: db.config.Advanced.SourceTag,
		},
	}

	var resultRows int    // Count of rows received within the current backfill chunk
	var nextRowKey []byte // The row key from which a subsequent backfill chunk should resume
	var rowOffset = state.BackfilledCount

	// Efficient indices and transcoder arrays for value processing. Constructed when we
	// process the first result row.
	var isFirstResult = true
	var columnNames []string                    // Names of all columns, in table order.
	var rowKeyIndices []int                     // Indices of key columns in the table, in key order.
	var outputColumnNames []string              // Names of all columns with omitted columns set to "", in table order, plus _meta.
	var outputTranscoders []jsonTranscoder      // Transcoders from DB values to JSON, with omitted columns set to nil.
	var rowKeyTranscoders []fdbTranscoder       // Transcoders from DB values to FDB row keys, with non-key columns set to nil.
	var sharedChangeInfo *mysqlChangeSharedInfo // Shared info struct reused across multiple change events from this table.
	var values []any                            // Values of the current row, in table order.

	var result mysql.Result
	defer result.Close() // Ensure the resultset allocated during QueryStreaming is returned to the pool when done
	if err := db.conn.WithTimeout(BackfillQueryTimeout).QueryStreaming(query, args, &result, func(row []mysql.FieldValue) error {
		if isFirstResult {
			isFirstResult = false
			values = make([]any, len(result.Fields))
			columnNames = make([]string, len(result.Fields))
			rowKeyIndices = make([]int, len(keyColumns))
			outputColumnNames = make([]string, len(result.Fields))
			outputTranscoders = make([]jsonTranscoder, len(result.Fields))
			rowKeyTranscoders = make([]fdbTranscoder, len(result.Fields))
			var err error
			for idx, field := range result.Fields {
				var colName = string(field.Name)
				columnNames[idx] = colName
				outputColumnNames[idx] = colName
				outputTranscoders[idx], err = db.constructJSONTranscoder(true, columnTypes[colName])
				if err != nil {
					return fmt.Errorf("error constructing JSON transcoder for column %q of type %v: %w", colName, columnTypes[colName], err)
				}
				if slices.Contains(keyColumns, colName) {
					rowKeyTranscoders[idx], err = db.constructFDBTranscoder(true, columnTypes[colName])
					if err != nil {
						return fmt.Errorf("error constructing FDB transcoder for column %q of type %v: %w", colName, columnTypes[colName], err)
					}
				}
			}
			outputColumnNames = append(outputColumnNames, "_meta")
			for keyIndex, keyColName := range keyColumns {
				var columnIndex = slices.Index(columnNames, keyColName)
				if columnIndex < 0 {
					return fmt.Errorf("key column %q not found in relation %q", keyColName, streamID)
				}
				rowKeyIndices[keyIndex] = columnIndex
			}
			sharedChangeInfo = &mysqlChangeSharedInfo{
				StreamID:    streamID,
				Shape:       encrow.NewShape(outputColumnNames),
				Transcoders: outputTranscoders,
			}
		}

		for idx := range row {
			values[idx] = row[idx].Value()
		}

		var rowKey []byte
		var err error
		if state.Mode == sqlcapture.TableStateKeylessBackfill {
			rowKey = []byte(fmt.Sprintf("B%019d", rowOffset)) // A 19 digit decimal number is sufficient to hold any 63-bit integer
		} else {
			rowKey, err = encodeRowKey(columnNames, rowKeyIndices, rowKeyTranscoders, values)
			if err != nil {
				return fmt.Errorf("error encoding row key for %q: %w", streamID, err)
			}
		}
		nextRowKey = rowKey

		var event = &mysqlChangeEvent{
			Info:   sharedChangeInfo,
			Meta:   prototypeChangeMetadata,
			RowKey: rowKey,
			Values: values,
		}
		event.Meta.Source.Cursor.RowIndex = rowOffset
		if err := callback(event); err != nil {
			return fmt.Errorf("error processing change event: %w", err)
		}
		resultRows++
		rowOffset++
		return nil
	}); err != nil {
		// As a special case, consider statement timeouts to be not an error so long as we got
		// at least one row back (resultRows > 0). This allows us to make partial progress on
		// slow databases with statement timeouts set, without adjusting the chunk size.
		if statementTimeoutRegexp.MatchString(err.Error()) && resultRows > 0 {
			logrus.WithFields(logrus.Fields{
				"stream":     streamID,
				"resultRows": resultRows,
			}).Warn("backfill query interrupted by statement timeout; partial progress saved")

			// Since we made progress, we don't return an error and we do return an updated cursor.
			return false, nextRowKey, nil
		}
		return false, nil, fmt.Errorf("error executing backfill: %w", err)
	}

	var backfillComplete = resultRows < db.config.Advanced.BackfillChunkSize
	return backfillComplete, nextRowKey, nil
}

// The set of MySQL column types for which we need to specify `BINARY` ordering
// and comparison. Represented as a map[string]bool so that it can be combined
// with the "is the column typename a string" check into a single if statement.
var columnBinaryKeyComparison = map[string]bool{
	"char":       true,
	"varchar":    true,
	"tinytext":   true,
	"text":       true,
	"mediumtext": true,
	"longtext":   true,
}

func (db *mysqlDatabase) keylessScanQuery(_ *sqlcapture.DiscoveryInfo, schemaName, tableName string) string {
	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT * FROM `%s`.`%s`", schemaName, tableName)
	fmt.Fprintf(query, " LIMIT %d", db.config.Advanced.BackfillChunkSize)
	fmt.Fprintf(query, " OFFSET ?;")
	return query.String()
}

func (db *mysqlDatabase) buildScanQuery(start, isPrecise bool, keyColumns []string, columnTypes map[string]interface{}, schemaName, tableName string) string {
	// Construct lists of key specifiers and placeholders. They will be joined with commas and used in the query itself.
	var pkey []string
	for _, colName := range keyColumns {
		var quotedName = quoteColumnName(colName)
		// If a precise backfill is requested *and* the column type requires binary ordering for precise
		// backfill comparisons to work, add the 'BINARY' qualifier to the column name.
		if colType, ok := columnTypes[colName].(string); ok && isPrecise && columnBinaryKeyComparison[colType] {
			pkey = append(pkey, "BINARY "+quotedName)
		} else {
			pkey = append(pkey, quotedName)
		}
	}

	// Construct the query itself.
	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT * FROM `%s`.`%s`", schemaName, tableName)

	if !start {
		for i := 0; i != len(pkey); i++ {
			if i == 0 {
				fmt.Fprintf(query, " WHERE (")
			} else {
				fmt.Fprintf(query, ") OR (")
			}

			for j := 0; j != i; j++ {
				fmt.Fprintf(query, "%s = ? AND ", pkey[j])
			}
			fmt.Fprintf(query, "%s > ?", pkey[i])
		}
		fmt.Fprintf(query, ")")
	}
	fmt.Fprintf(query, " ORDER BY %s", strings.Join(pkey, ", "))
	fmt.Fprintf(query, " LIMIT %d;", db.config.Advanced.BackfillChunkSize)
	return query.String()
}

func quoteColumnName(name string) string {
	// Per https://dev.mysql.com/doc/refman/8.0/en/identifiers.html, the identifier quote character
	// is the backtick (`). If the identifier itself contains a backtick, it must be doubled.
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (db *mysqlDatabase) explainQuery(streamID sqlcapture.StreamID, query string, args []interface{}) {
	// Only EXPLAIN the backfill query once per connector invocation
	if db.explained == nil {
		db.explained = make(map[sqlcapture.StreamID]struct{})
	}
	if _, ok := db.explained[streamID]; ok {
		return
	}
	db.explained[streamID] = struct{}{}

	// Ask the database to explain the backfill query execution plan.
	var explainResult, err = db.conn.Execute("EXPLAIN "+query, args...)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"query": query,
			"args":  args,
			"err":   err,
		}).Warn("failed to explain query")
		return
	}
	defer explainResult.Close()

	// Log the response, doing a bit of extra work to make it readable
	for _, row := range explainResult.Values {
		var result []string
		for idx, field := range row {
			var key = string(explainResult.Fields[idx].Name)
			var val = field.Value()
			if bs, ok := val.([]byte); ok {
				val = string(bs)
			}
			result = append(result, fmt.Sprintf("%s=%v", key, val))
		}
		logrus.WithFields(logrus.Fields{
			"id":       streamID,
			"query":    query,
			"response": result,
		}).Info("explain backfill query")
	}
}

type mysqlTableStatistics struct {
	TableRows int64 // Estimated row count from information_schema.tables
	Err       error // Error from querying statistics, if any (cached to avoid performance concerns)
}

func (db *mysqlDatabase) queryTableStatistics(ctx context.Context, schema, table string) (*mysqlTableStatistics, error) {
	var streamID = sqlcapture.JoinStreamID(schema, table)

	// Initialize cache if needed
	if db.tableStatistics == nil {
		db.tableStatistics = make(map[sqlcapture.StreamID]*mysqlTableStatistics)
	}

	// Return cached result if available
	if cached := db.tableStatistics[streamID]; cached != nil {
		return cached, cached.Err
	}

	// Query information_schema.tables for row count estimate
	var query = `SELECT table_rows FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`
	var result, err = db.conn.Execute(query, schema, table)
	if err != nil {
		var stats = &mysqlTableStatistics{Err: fmt.Errorf("error querying table statistics for %q: %w", streamID, err)}
		db.tableStatistics[streamID] = stats
		return stats, stats.Err
	}
	defer result.Close()

	if len(result.Values) == 0 {
		var stats = &mysqlTableStatistics{Err: fmt.Errorf("table %q not found in information_schema.tables", streamID)}
		db.tableStatistics[streamID] = stats
		return stats, stats.Err
	}

	var stats = &mysqlTableStatistics{
		TableRows: result.Values[0][0].AsInt64(),
	}

	logrus.WithFields(logrus.Fields{
		"stream":    streamID,
		"tableRows": stats.TableRows,
	}).Debug("queried table statistics")

	db.tableStatistics[streamID] = stats
	return stats, nil
}

func (db *mysqlDatabase) EstimatedRowCounts(ctx context.Context, tables []sqlcapture.TableID) (map[sqlcapture.TableID]int, error) {
	var result = make(map[sqlcapture.TableID]int)
	for _, table := range tables {
		var stats, err = db.queryTableStatistics(ctx, table.Schema, table.Table)
		if err != nil {
			return nil, err
		}
		result[table] = int(stats.TableRows)
	}
	return result, nil
}
