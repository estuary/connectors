package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/sirupsen/logrus"
)

func (db *mysqlDatabase) WriteWatermark(ctx context.Context, watermark string) error {
	logrus.WithField("watermark", watermark).Debug("writing watermark")

	var query = fmt.Sprintf(`REPLACE INTO %s (slot, watermark) VALUES (?,?);`, db.config.Advanced.WatermarksTable)
	var results, err = db.conn.Execute(query, db.config.Advanced.NodeID, watermark)
	if err != nil {
		return fmt.Errorf("error upserting new watermark for slot %d: %w", db.config.Advanced.NodeID, err)
	}
	results.Close()
	return nil
}

func (db *mysqlDatabase) WatermarksTable() string {
	return db.config.Advanced.WatermarksTable
}

func (db *mysqlDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, keyColumns []string, resumeAfter []byte) ([]*sqlcapture.ChangeEvent, error) {
	var schema, table = info.Schema, info.Name
	var streamID = sqlcapture.JoinStreamID(schema, table)

	// Decode the resumeAfter key if non-nil
	var err error
	var resumeKey []interface{}
	if resumeAfter != nil {
		resumeKey, err = sqlcapture.UnpackTuple(resumeAfter, decodeKeyFDB)
		if err != nil {
			return nil, fmt.Errorf("error unpacking resume key for %q: %w", streamID, err)
		}
		if len(resumeKey) != len(keyColumns) {
			return nil, fmt.Errorf("expected %d resume-key values but got %d", len(keyColumns), len(resumeKey))
		}
	}

	logrus.WithFields(logrus.Fields{
		"stream":     streamID,
		"keyColumns": keyColumns,
		"resumeKey":  resumeKey,
	}).Debug("scanning table chunk")

	var columnTypes = make(map[string]interface{})
	for name, column := range info.Columns {
		columnTypes[name] = column.DataType
	}

	// Build a query to fetch the next `backfillChunkSize` rows from the database.
	var query = db.buildScanQuery(resumeKey == nil, keyColumns, columnTypes, schema, table)
	// Splat the resume key into runs of arguments matching the predicate built by buildScanQuery.
	// This is super gross, and is a work-around for lack of indexed positional argument support,
	// AND lack of implementation for named arguments in our MySQL client.
	// See: https://github.com/go-sql-driver/mysql/issues/561
	var args []interface{}
	for i := range resumeKey {
		args = append(args, resumeKey[:i+1]...)
	}

	// If this is the first chunk being backfilled, run an `EXPLAIN` on it and log the results
	db.explainQuery(streamID, query, args)

	// Execute the backfill query to fetch rows from the database
	logrus.WithFields(logrus.Fields{"query": query, "args": args}).Debug("executing query")
	results, err := db.conn.Execute(query, args...)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer results.Close()

	// Process the results into `changeEvent` structs and return them
	var events []*sqlcapture.ChangeEvent
	logrus.WithFields(logrus.Fields{
		"stream": streamID,
		"rows":   len(results.Values),
	}).Debug("translating query rows to change events")
	for _, row := range results.Values {
		var fields = make(map[string]interface{})
		for idx, val := range row {
			fields[string(results.Fields[idx].Name)] = val.Value()
		}
		var rowKey, err = sqlcapture.EncodeRowKey(keyColumns, fields, columnTypes, encodeKeyFDB)
		if err != nil {
			return nil, fmt.Errorf("error encoding row key for %q: %w", streamID, err)
		}
		if err := db.translateRecordFields(columnTypes, fields); err != nil {
			return nil, fmt.Errorf("error backfilling table %q: %w", table, err)
		}

		logrus.WithField("fields", fields).Trace("got row")
		events = append(events, &sqlcapture.ChangeEvent{
			Operation: sqlcapture.InsertOp,
			RowKey:    rowKey,
			Source: &mysqlSourceInfo{
				SourceCommon: sqlcapture.SourceCommon{
					Millis:   0, // Not known.
					Schema:   schema,
					Snapshot: true,
					Table:    table,
				},
			},
			Before: nil,
			After:  fields,
		})
	}
	return events, nil
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

func (db *mysqlDatabase) buildScanQuery(start bool, keyColumns []string, columnTypes map[string]interface{}, schemaName, tableName string) string {
	// Construct lists of key specifiers and placeholders. They will be joined with commas and used in the query itself.
	var pkey []string
	for _, colName := range keyColumns {
		var quotedName = quoteColumnName(colName)
		if colType, ok := columnTypes[colName].(string); ok && columnBinaryKeyComparison[colType] {
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

func (db *mysqlDatabase) explainQuery(streamID, query string, args []interface{}) {
	// Only EXPLAIN the backfill query once per connector invocation
	if db.explained == nil {
		db.explained = make(map[string]struct{})
	}
	if _, ok := db.explained[streamID]; ok {
		return
	}
	db.explained[streamID] = struct{}{}

	// Ask the database to analyze the backfill query.
	// ANALYZE is not universally supported, so try a few forms.
	var explainResult, err = db.conn.Execute("ANALYZE "+query, args...)
	if err != nil {
		explainResult, err = db.conn.Execute("EXPLAIN ANALYZE "+query, args...)
	}
	if err != nil {
		explainResult, err = db.conn.Execute("EXPLAIN "+query, args...)
	}
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"query": query,
			"err":   err,
		}).Error("unable to explain query")
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
