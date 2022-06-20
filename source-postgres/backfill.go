package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/jackc/pglogrepl"
	"github.com/sirupsen/logrus"
)

// ScanTableChunk fetches a chunk of rows from the specified table, resuming from `resumeKey` if non-nil.
func (db *postgresDatabase) ScanTableChunk(ctx context.Context, info sqlcapture.TableInfo, keyColumns []string, resumeKey []interface{}) ([]sqlcapture.ChangeEvent, error) {
	var schema, table = info.Schema, info.Name
	logrus.WithFields(logrus.Fields{
		"schema":     schema,
		"table":      table,
		"keyColumns": keyColumns,
		"resumeKey":  resumeKey,
	}).Debug("scanning table chunk")

	// Build and execute a query to fetch the next `backfillChunkSize` rows from the database
	var query = buildScanQuery(resumeKey == nil, keyColumns, schema, table)
	logrus.WithFields(logrus.Fields{"query": query, "args": resumeKey}).Debug("executing query")
	rows, err := db.conn.Query(ctx, query, resumeKey...)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer rows.Close()

	// Process the results into `changeEvent` structs and return them
	var cols = rows.FieldDescriptions()
	var events []sqlcapture.ChangeEvent
	for rows.Next() {
		// Scan the row values and copy into the equivalent map
		var vals, err = rows.Values()
		if err != nil {
			return nil, fmt.Errorf("unable to get row values: %w", err)
		}
		var fields = make(map[string]interface{})
		for idx := range cols {
			fields[string(cols[idx].Name)] = vals[idx]
		}
		if err := translateRecordFields(&info, fields); err != nil {
			return nil, fmt.Errorf("error backfilling table %q: %w", table, err)
		}

		events = append(events, sqlcapture.ChangeEvent{
			Operation: sqlcapture.InsertOp,
			Source: &postgresSource{
				SourceCommon: sqlcapture.SourceCommon{
					Millis:   0, // Not known.
					Schema:   schema,
					Snapshot: true,
					Table:    table,
				},
				Location: [3]pglogrepl.LSN{},
			},
			Before: nil,
			After:  fields,
		})
	}
	return events, nil
}

// WriteWatermark writes the provided string into the 'watermarks' table.
func (db *postgresDatabase) WriteWatermark(ctx context.Context, watermark string) error {
	logrus.WithField("watermark", watermark).Debug("writing watermark")

	var query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (slot TEXT PRIMARY KEY, watermark TEXT);", db.config.Advanced.WatermarksTable)
	rows, err := db.conn.Query(ctx, query)
	if err != nil {
		return fmt.Errorf("error creating watermarks table: %w", err)
	}
	rows.Close()

	query = fmt.Sprintf(`INSERT INTO %s (slot, watermark) VALUES ($1,$2) ON CONFLICT (slot) DO UPDATE SET watermark = $2;`, db.config.Advanced.WatermarksTable)
	rows, err = db.conn.Query(ctx, query, db.config.Advanced.SlotName, watermark)
	if err != nil {
		return fmt.Errorf("error upserting new watermark for slot %q: %w", db.config.Advanced.SlotName, err)
	}
	rows.Close()
	return nil
}

// WatermarksTable returns the name of the table to which WriteWatermarks writes UUIDs.
func (db *postgresDatabase) WatermarksTable() string {
	return db.config.Advanced.WatermarksTable
}

// backfillChunkSize controls how many rows will be read from the database in a
// single query. In normal use it acts like a constant, it's just a variable here
// so that it can be lowered in tests to exercise chunking behavior more easily.
var backfillChunkSize = 4096

func buildScanQuery(start bool, keyColumns []string, schemaName, tableName string) string {
	// Construct strings like `(foo, bar, baz)` and `($1, $2, $3)` for use in the query
	var pkey, args string
	for idx, colName := range keyColumns {
		if idx > 0 {
			pkey += ", "
			args += ", "
		}
		pkey += colName
		args += fmt.Sprintf("$%d", idx+1)
	}

	// Construct the query itself
	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT * FROM %s.%s", schemaName, tableName)
	if !start {
		fmt.Fprintf(query, " WHERE (%s) > (%s)", pkey, args)
	}
	fmt.Fprintf(query, " ORDER BY (%s)", pkey)
	fmt.Fprintf(query, " LIMIT %d;", backfillChunkSize)
	return query.String()
}
