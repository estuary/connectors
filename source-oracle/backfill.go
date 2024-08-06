package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/sirupsen/logrus"
)

var statementTimeoutRegexp = regexp.MustCompile(`canceling statement due to statement timeout`)

// ScanTableChunk fetches a chunk of rows from the specified table, resuming from `resumeKey` if non-nil.
func (db *oracleDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, callback func(event *sqlcapture.ChangeEvent) error) (bool, error) {
	logrus.WithField("state", state).Debug("ScanChunk")
	var keyColumns = state.KeyColumns
	var resumeAfter = state.Scanned
	var schema, table = info.Schema, info.Name
	var streamID = sqlcapture.JoinStreamID(schema, table)
	var logEntry = logrus.WithField("stream", streamID)

	var columnTypes = make(map[string]oracleColumnType)
	for name, column := range info.Columns {
		columnTypes[name] = column.DataType.(oracleColumnType)
	}

	// Compute backfill query and arguments list
	var query string
	var args []any
	switch state.Mode {
	case sqlcapture.TableModeKeylessBackfill:
		// A is the lexicographically smallest character in base64 encoding, so this is the smallest possible base64 encoded string
		var afterRowID = "AAAAAAAAAAAAAAAAAA"
		if resumeAfter != nil {
			afterRowID = string(resumeAfter)
		}
		logEntry.WithField("rowid", afterRowID).Debug("scanning keyless table chunk")
		query = db.keylessScanQuery(info, schema, table)
		args = []any{afterRowID}

	case sqlcapture.TableModePreciseBackfill:
		if resumeAfter != nil {
			var resumeKey, err = sqlcapture.UnpackTuple(resumeAfter, decodeKeyFDB)
			if err != nil {
				return false, fmt.Errorf("error unpacking resume key for %q: %w", streamID, err)
			}
			if len(resumeKey) != len(keyColumns) {
				return false, fmt.Errorf("expected %d resume-key values but got %d", len(keyColumns), len(resumeKey))
			}
			logEntry.WithFields(logrus.Fields{
				"keyColumns": keyColumns,
				"resumeKey":  resumeKey,
			}).Debug("scanning subsequent table chunk")
			query = db.buildScanQuery(false, info, keyColumns, columnTypes, schema, table)
			for idx, k := range resumeKey {
				args = append(args, sql.Named(fmt.Sprintf("p%d", idx+1), k))
			}
		} else {
			logEntry.WithField("keyColumns", keyColumns).Debug("scanning initial table chunk")
			query = db.buildScanQuery(true, info, keyColumns, columnTypes, schema, table)
		}
	default:
		return false, fmt.Errorf("invalid backfill mode %q", state.Mode)
	}

	// If this is the first chunk being backfilled, run an `EXPLAIN` on it and log the results
	db.explainQuery(ctx, streamID, query, args)

	// Execute the backfill query to fetch rows from the database
	logEntry.WithFields(logrus.Fields{"query": query, "args": args}).Debug("executing query")
	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer rows.Close()

	// Process the results into `changeEvent` structs and return them
	cols, err := rows.Columns()
	if err != nil {
		return false, fmt.Errorf("rows.Columns: %w", err)
	}
	var resultRows int // Count of rows received within the current backfill chunk
	var rowOffset = state.BackfilledCount
	logEntry.Debug("translating query rows to change events")
	for rows.Next() {
		fields, err := scanToMap(rows, cols, columnTypes)
		if err != nil {
			return false, err
		}
		var rowid = fields["ROWID"].(string)
		delete(fields, "ROWID")

		var rowKey []byte
		if state.Mode == sqlcapture.TableModeKeylessBackfill {
			rowKey = []byte(rowid)
		} else {
			rowKey, err = sqlcapture.EncodeRowKey(keyColumns, fields, columnTypes, encodeKeyFDB)
			if err != nil {
				return false, fmt.Errorf("error encoding row key for %q: %w", streamID, err)
			}
		}

		if err := translateRecordFields(info, fields); err != nil {
			return false, fmt.Errorf("error backfilling table %q: %w", table, err)
		}

		var event = &sqlcapture.ChangeEvent{
			Operation: sqlcapture.InsertOp,
			RowKey:    rowKey,
			Source: &oracleSource{
				SourceCommon: sqlcapture.SourceCommon{
					Millis:   0, // Not known.
					Schema:   schema,
					Snapshot: true,
					Table:    table,
				},
				RowID: rowid,
			},
			Before: nil,
			After:  fields,
		}
		if err := callback(event); err != nil {
			return false, fmt.Errorf("error processing change event: %w", err)
		}
		resultRows++
		rowOffset++
	}

	if err := rows.Err(); err != nil {
		return false, err
	}

	var backfillComplete = resultRows < db.config.Advanced.BackfillChunkSize
	return backfillComplete, nil
}

func scanToMap(rows *sql.Rows, cols []string, colTypes map[string]oracleColumnType) (map[string]any, error) {
	var fieldsArr = make([]any, len(cols))
	var fieldsPtr = make([]any, len(cols))
	for idx, col := range cols {
		if col == "ROWID" {
			var rowid = ""
			fieldsArr[idx] = &rowid
		} else {
			fieldsArr[idx] = new(any)
		}
		fieldsPtr[idx] = &fieldsArr[idx]
	}
	if err := rows.Scan(fieldsPtr...); err != nil {
		return nil, fmt.Errorf("scanning row: %w", err)
	}

	var fields = make(map[string]any, len(cols))
	for idx, col := range cols {
		fields[col] = fieldsArr[idx]
	}

	return fields, nil
}

// -1 means a < b
// 0 means a == b
// 1 means a > b
func compareBase64(a, b string) (int, error) {
	if abytes, err := base64.RawStdEncoding.DecodeString(a); err != nil {
		return 0, fmt.Errorf("base64 decoding: %w", err)
	} else if bbytes, err := base64.RawStdEncoding.DecodeString(b); err != nil {
		return 0, fmt.Errorf("base64 decoding: %w", err)
	} else {
		return bytes.Compare(abytes, bbytes), nil
	}
}

// WriteWatermark writes the provided string into the 'watermarks' table.
func (db *oracleDatabase) WriteWatermark(ctx context.Context, watermark string) error {
	logrus.WithField("watermark", watermark).Debug("writing watermark")
	var query = fmt.Sprintf(`MERGE INTO %s USING dual ON (slot=:slot) WHEN MATCHED THEN UPDATE SET watermark=:watermark WHEN NOT MATCHED THEN INSERT (slot, watermark) VALUES (:slot,:watermark)`, db.config.Advanced.WatermarksTable)
	var _, err = db.conn.ExecContext(ctx, query, sql.Named("slot", db.config.Advanced.NodeID), sql.Named("watermark", watermark))
	if err != nil {
		return fmt.Errorf("error upserting new watermark for slot %q: %w", db.config.Advanced.NodeID, err)
	}
	return nil
}

// WatermarksTable returns the name of the table to which WriteWatermarks writes UUIDs.
func (db *oracleDatabase) WatermarksTable() string {
	return db.config.Advanced.WatermarksTable
}

// The set of column types for which we need to specify `COLLATE BINARY` to get
// proper ordering and comparison. Represented as a map[string]bool so that it can be
// combined with the "is the column typename a string" check into one if statement.
// See https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/COLLATE-Operator.html
var columnBinaryKeyComparison = map[string]bool{
	"varchar2": true,
	"char":     true,
	"nvarchar": true,
	"nchar":    true,
}

// render a "cast" expression for a column so that we can cast it to the
// type and format we expect for that column (e.g. for timestamps we expect a certain format with UTC timezone, etc.)
func castColumn(col sqlcapture.ColumnInfo) string {
	var dataType = col.DataType.(oracleColumnType).original
	var isDateTime = dataType == "DATE" || strings.HasPrefix(dataType, "TIMESTAMP")
	var isInterval = dataType == "INTERVAL"

	if !isDateTime && !isInterval {
		return quoteColumnName(col.Name)
	}

	if isInterval {
		return fmt.Sprintf("TO_CHAR(%s) AS %s", quoteColumnName(col.Name), quoteColumnName(col.Name))
	}

	var dataScale int
	if index := strings.Index(dataType, "("); index > -1 {
		var endIndex = strings.Index(dataType, ")")
		var err error
		dataScale, err = strconv.Atoi(dataType[index+1 : endIndex])
		if err != nil {
			panic(err)
		}
	}

	var out = fmt.Sprintf("TO_CHAR(%s", quoteColumnName(col.Name))
	var format = ""
	if strings.Contains(dataType, "TIME ZONE") && !strings.Contains(dataType, "LOCAL TIME ZONE") {
		format = `'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"'`
	} else if dataScale > 0 {
		format = `'YYYY-MM-DD"T"HH24:MI:SS.FF'`
	} else {
		format = `'YYYY-MM-DD"T"HH24:MI:SS'`
	}

	if strings.Contains(dataType, "TIME ZONE") {
		out = out + " AT TIME ZONE 'UTC'"
	}

	out = out + fmt.Sprintf(", %s) AS %s", format, quoteColumnName(col.Name))
	return out
}

// Keyless scan uses ROWID to order the rows. Note that this only ensures eventual consistency
// since ROWIDs do not always increasing (new rows can use smaller ROWIDs if space is available in an earlier block)
// but since we will capture changes since the start of the backfill using SCN tracking, we will eventually be consistent
func (db *oracleDatabase) keylessScanQuery(info *sqlcapture.DiscoveryInfo, schemaName, tableName string) string {
	var query = new(strings.Builder)
	var columnSelect []string
	for _, col := range info.Columns {
		columnSelect = append(columnSelect, castColumn(col))
	}
	fmt.Fprintf(query, `SELECT ROWID, %s FROM "%s"."%s"`, strings.Join(columnSelect, ","), schemaName, tableName)
	fmt.Fprintf(query, ` WHERE ROWID > :1`)
	fmt.Fprintf(query, ` ORDER BY ROWID ASC`)
	fmt.Fprintf(query, ` FETCH NEXT %d ROWS ONLY`, db.config.Advanced.BackfillChunkSize)
	return query.String()
}

func (db *oracleDatabase) buildScanQuery(start bool, info *sqlcapture.DiscoveryInfo, keyColumns []string, columnTypes map[string]oracleColumnType, schemaName, tableName string) string {
	// Construct lists of key specifiers and placeholders. They will be joined with commas and used in the query itself.
	var pkey []string
	var args []string
	for idx, colName := range keyColumns {
		var quotedName = quoteColumnName(colName)
		// If a precise backfill is requested *and* the column type requires binary ordering for precise
		// backfill comparisons to work, add the 'COLLATE BINARY' qualifier to the column name.
		if columnTypes[colName].jsonType == "string" && columnBinaryKeyComparison[colName] {
			pkey = append(pkey, quotedName+" COLLATE BINARY")
		} else {
			pkey = append(pkey, quotedName)
		}
		args = append(args, fmt.Sprintf(":p%d", idx+1))
	}

	// Construct the query itself
	var query = new(strings.Builder)
	var columnSelect []string
	for _, col := range info.Columns {
		columnSelect = append(columnSelect, castColumn(col))
	}
	fmt.Fprintf(query, `SELECT ROWID, %s FROM "%s"."%s"`, strings.Join(columnSelect, ","), schemaName, tableName)
	if !start {
		for i := 0; i != len(pkey); i++ {
			if i == 0 {
				fmt.Fprintf(query, " WHERE (")
			} else {
				fmt.Fprintf(query, ") OR (")
			}

			for j := 0; j != i; j++ {
				fmt.Fprintf(query, "%s = %s AND ", pkey[j], args[j])
			}
			fmt.Fprintf(query, "%s > %s", pkey[i], args[i])
		}
		fmt.Fprintf(query, ")")
	}
	fmt.Fprintf(query, " ORDER BY %s ASC", strings.Join(pkey, ", "))
	fmt.Fprintf(query, ` FETCH NEXT %d ROWS ONLY`, db.config.Advanced.BackfillChunkSize)
	return query.String()
}

func (db *oracleDatabase) explainQuery(ctx context.Context, streamID, query string, args []interface{}) {
	// Only EXPLAIN the backfill query once per connector invocation
	if db.explained == nil {
		db.explained = make(map[sqlcapture.StreamID]struct{})
	}
	if _, ok := db.explained[streamID]; ok {
		return
	}
	db.explained[streamID] = struct{}{}

	// Ask the database to EXPLAIN the backfill query
	var explainQuery = "EXPLAIN PLAN FOR " + query
	logrus.WithFields(logrus.Fields{
		"id":    streamID,
		"query": explainQuery,
	}).Info("explain backfill query")
	_, err := db.conn.ExecContext(ctx, explainQuery, args...)
	if err != nil {
		logrus.WithFields(logrus.Fields{"id": streamID, "err": err}).Error("unable to explain query")
		return
	}
	rows, err := db.conn.QueryContext(ctx, "SELECT * FROM table(dbms_xplan.display)")
	if err != nil {
		logrus.WithFields(logrus.Fields{"id": streamID, "err": err}).Error("unable to explain query")
		return
	}
	defer rows.Close()

	// Log the response, doing a bit of extra work to make it readable
	cols, err := rows.Columns()
	if err != nil {
		logrus.WithFields(logrus.Fields{"id": streamID, "err": err}).Error("unable to explain query")
		return
	}
	for rows.Next() {
		// Scan the row values and copy into the equivalent map
		var fields = logrus.Fields{"streamID": streamID}
		var fieldsPtr = make([]any, len(cols))
		for idx, col := range cols {
			fields[col] = new(any)
			fieldsPtr[idx] = fields[col]
		}

		if err := rows.Scan(fieldsPtr...); err != nil {
			logrus.WithFields(logrus.Fields{
				"id":  streamID,
				"err": err,
			}).Error("error getting row value")
			return
		}

		for key, v := range fields {
			val := reflect.ValueOf(v)
			if val.Kind() == reflect.Ptr {
				val = val.Elem()
			}
			fields[key] = val
		}

		logrus.WithFields(fields).Info("explain backfill query")
	}
}
