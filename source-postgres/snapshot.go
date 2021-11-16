package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

// TODO(wgd): Define an interface with a `ScanTableChunk(ctx, streamID, keyColumns, resumeKey)` and
// a `WriteWatermark(ctx, table, slot)` method which serves as the "database backfill" API
// that we'll have to implement for other databases.

// scanTableChunk fetches a chunk of rows from the specified table, resuming from the provided
// `resumeKey` if non-nil. This is the entrypoint to the database-specific portion of the
// backfill process.
func scanTableChunk(ctx context.Context, conn *pgx.Conn, streamID string, keyColumns []string, resumeKey []byte) ([]*changeEvent, error) {
	logrus.WithFields(logrus.Fields{
		"streamID":   streamID,
		"keyColumns": keyColumns,
		"resumeKey":  base64.StdEncoding.EncodeToString(resumeKey),
	}).Debug("scanning table chunk")

	// Split "public.foo" tableID into "public" schema and "foo" table name
	var parts = strings.SplitN(streamID, ".", 2)
	var schemaName, tableName = parts[0], parts[1]

	// Build and execute a query to fetch the next `snapshotChunkSize` rows from the database
	var query = buildScanQuery(resumeKey == nil, keyColumns, schemaName, tableName)
	var args []interface{}
	if resumeKey != nil {
		var err error
		args, err = unpackTuple(resumeKey)
		if err != nil {
			return nil, fmt.Errorf("error unpacking encoded tuple: %w", err)
		}
		if len(args) != len(keyColumns) {
			return nil, fmt.Errorf("expected %d primary-key values but got %d", len(keyColumns), len(args))
		}
	}
	logrus.WithField("query", query).WithField("args", args).Debug("executing query")
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer rows.Close()

	// Process the results into `changeEvent` structs and return them
	var cols = rows.FieldDescriptions()
	var events []*changeEvent
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

		events = append(events, &changeEvent{
			Type:      "Insert",
			Namespace: schemaName,
			Table:     tableName,
			Fields:    fields,
		})
	}
	return events, nil
}

// writeWatermark writes a new random UUID into the 'watermarks' table and returns the
// UUID. This is also an entrypoint to the database-specific portion of the backfill.
func writeWatermark(ctx context.Context, conn *pgx.Conn, table, slot string) (string, error) {
	// Generate a watermark UUID
	var wm = uuid.New().String()

	var query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (slot TEXT PRIMARY KEY, watermark TEXT);", table)
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return "", fmt.Errorf("error creating watermarks table: %w", err)
	}
	rows.Close()

	query = fmt.Sprintf(`INSERT INTO %s (slot, watermark) VALUES ($1,$2) ON CONFLICT (slot) DO UPDATE SET watermark = $2;`, table)
	rows, err = conn.Query(ctx, query, slot, wm)
	if err != nil {
		return "", fmt.Errorf("error upserting new watermark for slot %q: %w", slot, err)
	}
	rows.Close()

	logrus.WithField("watermark", wm).Debug("wrote watermark")
	return wm, nil
}

// snapshotChunkSize controls how many rows will be read from the database in a
// single query. In normal use it acts like a constant, it's just a variable here
// so that it can be lowered in tests to exercise chunking behavior more easily.
var snapshotChunkSize = 4096

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
	fmt.Fprintf(query, " LIMIT %d;", snapshotChunkSize)
	return query.String()
}
