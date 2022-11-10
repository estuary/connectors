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
		return fmt.Errorf("error upserting new watermark for slot %q: %w", db.config.Advanced.NodeID, err)
	}
	results.Close()
	return nil
}

func (db *mysqlDatabase) WatermarksTable() string {
	return db.config.Advanced.WatermarksTable
}

func (db *mysqlDatabase) ScanTableChunk(ctx context.Context, info sqlcapture.TableInfo, keyColumns []string, resumeKey []interface{}) ([]sqlcapture.ChangeEvent, error) {
	var schema, table = info.Schema, info.Name
	logrus.WithFields(logrus.Fields{
		"schema":     schema,
		"table":      table,
		"keyColumns": keyColumns,
		"resumeKey":  resumeKey,
	}).Debug("scanning table chunk")

	var columnTypes = make(map[string]interface{})
	for name, column := range info.Columns {
		columnTypes[name] = column.DataType
	}

	// Build and execute a query to fetch the next `backfillChunkSize` rows from the database
	var query = db.buildScanQuery(resumeKey == nil, keyColumns, schema, table)
	logrus.WithFields(logrus.Fields{"query": query, "args": resumeKey}).Debug("executing query")
	results, err := db.conn.Execute(query, resumeKey...)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer results.Close()

	// Process the results into `changeEvent` structs and return them
	var events []sqlcapture.ChangeEvent
	logrus.WithFields(logrus.Fields{
		"schema": schema,
		"table":  table,
		"rows":   len(results.Values),
	}).Debug("translating query rows to change events")
	for _, row := range results.Values {
		var fields = make(map[string]interface{})
		for idx, val := range row {
			fields[string(results.Fields[idx].Name)] = val.Value()
		}
		if err := translateRecordFields(columnTypes, fields); err != nil {
			return nil, fmt.Errorf("error backfilling table %q: %w", table, err)
		}

		logrus.WithField("fields", fields).Trace("got row")
		events = append(events, sqlcapture.ChangeEvent{
			Operation: sqlcapture.InsertOp,
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

func (db *mysqlDatabase) buildScanQuery(start bool, keyColumns []string, schemaName, tableName string) string {
	// Construct strings like `(foo, bar, baz)` and `(?, ?, ?)` for use in the query
	var pkey, args string
	for idx, colName := range keyColumns {
		if idx > 0 {
			pkey += ", "
			args += ", "
		}
		pkey += colName
		args += "?"
	}

	// Construct the query itself
	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT * FROM %s.%s", schemaName, tableName)
	if !start {
		fmt.Fprintf(query, " WHERE (%s) > (%s)", pkey, args)
	}
	fmt.Fprintf(query, " ORDER BY %s", pkey)
	fmt.Fprintf(query, " LIMIT %d;", db.config.Advanced.BackfillChunkSize)
	return query.String()
}
