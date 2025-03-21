package main

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sirupsen/logrus"
)

var statementTimeoutRegexp = regexp.MustCompile(`canceling statement due to statement timeout`)

// ScanTableChunk fetches a chunk of rows from the specified table, resuming from `resumeKey` if non-nil.
func (db *postgresDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, callback func(event *sqlcapture.ChangeEvent) error) (bool, error) {
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
	var disableParallelWorkers bool
	var query string
	var args []any
	switch state.Mode {
	case sqlcapture.TableStateKeylessBackfill:
		var afterCTID = "(0,0)"
		if resumeAfter != nil {
			afterCTID = string(resumeAfter)
		}
		logEntry.WithField("ctid", afterCTID).Debug("scanning keyless table chunk")
		query = db.keylessScanQuery(info, schema, table)
		args = []any{afterCTID}
		disableParallelWorkers = true
	case sqlcapture.TableStatePreciseBackfill, sqlcapture.TableStateUnfilteredBackfill:
		var isPrecise = (state.Mode == sqlcapture.TableStatePreciseBackfill)
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
			query = db.buildScanQuery(false, isPrecise, keyColumns, columnTypes, schema, table)
			args = resumeKey
		} else {
			logEntry.WithField("keyColumns", keyColumns).Debug("scanning initial table chunk")
			query = db.buildScanQuery(true, isPrecise, keyColumns, columnTypes, schema, table)
		}
	default:
		return false, fmt.Errorf("invalid backfill mode %q", state.Mode)
	}

	// Keyless backfill queries need to return results in CTID order, but we can't ask
	// for that because `ORDER BY ctid` forces a sort, so we rely on it being true as an
	// implementation detail of how `WHERE ctid > $1` queries execute in practice. But
	// parallel query execution breaks that assumption, so we need to force the query
	// planner to not use parallel workers in such cases.
	if disableParallelWorkers {
		if _, err := db.conn.Exec(ctx, "SET max_parallel_workers_per_gather TO 0"); err != nil {
			logrus.WithField("err", err).Warn("error attempting to disable parallel workers")
		} else {
			defer func() {
				if _, err := db.conn.Exec(ctx, "SET max_parallel_workers_per_gather TO DEFAULT"); err != nil {
					logrus.WithField("err", err).Warn("error resetting max_parallel_workers to default")
				}
			}()
		}
	}

	// If this is the first chunk being backfilled, run an `EXPLAIN` on it and log the results
	db.explainQuery(ctx, streamID, query, args)

	// Execute the backfill query to fetch rows from the database
	logEntry.WithFields(logrus.Fields{"query": query, "args": args}).Debug("executing query")
	rows, err := db.conn.Query(ctx, query, args...)
	if err != nil {
		return false, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer rows.Close()

	// Process the results into `changeEvent` structs and return them
	var cols = rows.FieldDescriptions()
	var resultRows int // Count of rows received within the current backfill chunk
	var rowOffset = state.BackfilledCount
	var prevTID pgtype.TID // Used when processing keyless backfill results to sanity-check ordering
	logEntry.Debug("translating query rows to change events")
	for rows.Next() {
		// Scan the row values and copy into the equivalent map
		var vals, err = rows.Values()
		if err != nil {
			return false, fmt.Errorf("unable to get row values: %w", err)
		}
		var fields = make(map[string]interface{})
		for idx := range cols {
			fields[string(cols[idx].Name)] = vals[idx]
		}
		for _, name := range generatedColumns {
			// Generated column values cannot be captured via CDC, so we have to exclude them from
			// backfills as well or we could end up with incorrect/stale values. Better not to have
			// a property at all in those circumstances.
			delete(fields, name)
		}

		var rowKey []byte
		if state.Mode == sqlcapture.TableStateKeylessBackfill {
			var ctid = fields["ctid"].(pgtype.TID)
			delete(fields, "ctid")

			if !ctid.Valid {
				return false, fmt.Errorf("internal error: invalid ctid value %#v", ctid)
			}
			rowKey = []byte(fmt.Sprintf("(%d,%d)", ctid.BlockNumber, ctid.OffsetNumber))

			// Sanity check that rows are returned in ascending CTID order within a given backfill chunk
			if (ctid.BlockNumber < prevTID.BlockNumber) || ((ctid.BlockNumber == prevTID.BlockNumber) && (ctid.OffsetNumber <= prevTID.OffsetNumber)) {
				return false, fmt.Errorf("internal error: ctid ordering sanity check failed: %v <= %v", ctid, prevTID)
			}
			prevTID = ctid
		} else {
			rowKey, err = sqlcapture.EncodeRowKey(keyColumns, fields, columnTypes, encodeKeyFDB)
			if err != nil {
				return false, fmt.Errorf("error encoding row key for %q: %w", streamID, err)
			}
		}
		if err := db.translateRecordFields(info, fields); err != nil {
			return false, fmt.Errorf("error backfilling table %q: %w", table, err)
		}

		var event = &sqlcapture.ChangeEvent{
			Operation: sqlcapture.InsertOp,
			RowKey:    rowKey,
			Source: &postgresSource{
				SourceCommon: sqlcapture.SourceCommon{
					Millis:   0, // Not known.
					Schema:   schema,
					Snapshot: true,
					Table:    table,
				},
				Location: [3]int{-1, rowOffset, 0},
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
		// As a special case, consider statement timeouts to be not an error so long as we got
		// at least one row back. This allows us to make partial progress on slow databases with
		// statement timeouts set, without having to fine-tune the backfill chunk size.
		if err, ok := err.(*pgconn.PgError); ok && statementTimeoutRegexp.MatchString(err.Message) && resultRows > 0 {
			logrus.WithField("rows", resultRows).Warn("backfill query interrupted by statement timeout")
			return false, nil
		}
		return false, err
	}

	var backfillComplete = resultRows < db.config.Advanced.BackfillChunkSize
	return backfillComplete, nil
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
func (db *postgresDatabase) WatermarksTable() string {
	return db.config.Advanced.WatermarksTable
}

// The set of column types for which we need to specify `COLLATE "C"` to get
// proper ordering and comparison. Represented as a map[string]bool so that it can be
// combined with the "is the column typename a string" check into one if statement.
var columnBinaryKeyComparison = map[string]bool{
	"varchar": true,
	"bpchar":  true,
	"text":    true,
}

func (db *postgresDatabase) keylessScanQuery(_ *sqlcapture.DiscoveryInfo, schemaName, tableName string) string {
	var query = new(strings.Builder)
	fmt.Fprintf(query, `SELECT ctid, * FROM "%s"."%s"`, schemaName, tableName)
	fmt.Fprintf(query, ` WHERE ctid > $1`)
	if db.config.Advanced.MinimumBackfillXID != "" {
		fmt.Fprintf(query, ` AND (((xmin::text::bigint - %s::bigint)<<32)>>32) > 0 AND xmin::text::bigint >= 3`, db.config.Advanced.MinimumBackfillXID)
	}
	if db.config.Advanced.MaximumBackfillXID != "" {
		fmt.Fprintf(query, ` AND (((%s::bigint - xmin::text::bigint)<<32)>>32) > 0 AND xmin::text::bigint >= 3`, db.config.Advanced.MaximumBackfillXID)
	}
	fmt.Fprintf(query, ` LIMIT %d;`, db.config.Advanced.BackfillChunkSize)
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
	if db.config.Advanced.MinimumBackfillXID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf(`(((xmin::text::bigint - %s::bigint)<<32)>>32) > 0 AND xmin::text::bigint >= 3`, db.config.Advanced.MinimumBackfillXID))
	}
	if db.config.Advanced.MaximumBackfillXID != "" {
		whereClauses = append(whereClauses, fmt.Sprintf(`(((%s::bigint - xmin::text::bigint)<<32)>>32) > 0 AND xmin::text::bigint >= 3`, db.config.Advanced.MaximumBackfillXID))
	}

	// Construct the query itself
	var query = new(strings.Builder)
	fmt.Fprintf(query, `SELECT * FROM "%s"."%s"`, schemaName, tableName)
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

func (db *postgresDatabase) explainQuery(ctx context.Context, streamID, query string, args []interface{}) {
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
