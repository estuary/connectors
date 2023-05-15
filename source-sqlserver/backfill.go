package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
)

// ShouldBackfill returns true if a given table's contents should be backfilled, given
// that we intend to capture that table.
func (db *sqlserverDatabase) ShouldBackfill(streamID string) bool {
	if db.config.Advanced.SkipBackfills != "" {
		// This repeated splitting is a little inefficient, but this check is done at
		// most once per table during connector startup and isn't really worth caching.
		for _, skipStreamID := range strings.Split(db.config.Advanced.SkipBackfills, ",") {
			if streamID == strings.ToLower(skipStreamID) {
				return false
			}
		}
	}
	return true
}

// ScanTableChunk fetches a chunk of rows from the specified table, resuming from the `resumeAfter` row key if non-nil.
func (db *sqlserverDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState) ([]*sqlcapture.ChangeEvent, error) {
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
	case sqlcapture.TableModeKeylessBackfill:
		log.WithFields(log.Fields{
			"stream": streamID,
			"offset": state.BackfilledCount,
		}).Debug("scanning keyless table chunk")
		query = db.keylessScanQuery(info, schema, table)
		args = []any{state.BackfilledCount}
	case sqlcapture.TableModeBackfill:
		if resumeAfter != nil {
			var resumeKey, err = sqlcapture.UnpackTuple(resumeAfter, decodeKeyFDB)
			if err != nil {
				return nil, fmt.Errorf("error unpacking resume key for %q: %w", streamID, err)
			}
			if len(resumeKey) != len(keyColumns) {
				return nil, fmt.Errorf("expected %d resume-key values but got %d", len(keyColumns), len(resumeKey))
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
		return nil, fmt.Errorf("invalid backfill mode %q", state.Mode)
	}

	log.WithFields(log.Fields{"query": query, "args": args}).Debug("executing query")
	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer rows.Close()

	// Set up the necessary slices for generic scanning over query rows
	cnames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var vals = make([]any, len(cnames))
	var vptrs = make([]any, len(vals))
	for idx := range vals {
		vptrs[idx] = &vals[idx]
	}

	// Iterate over the result set appending change events to the list
	var events []*sqlcapture.ChangeEvent
	var rowOffset = state.BackfilledCount
	for rows.Next() {
		if err := rows.Scan(vptrs...); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		var fields = make(map[string]interface{})
		for idx, name := range cnames {
			fields[name] = vals[idx]
		}

		var rowKey []byte
		if state.Mode == sqlcapture.TableModeKeylessBackfill {
			rowKey = []byte(fmt.Sprintf("B%019d", rowOffset)) // A 19 digit decimal number is sufficient to hold any 63-bit integer
		} else {
			rowKey, err = sqlcapture.EncodeRowKey(keyColumns, fields, columnTypes, encodeKeyFDB)
			if err != nil {
				return nil, fmt.Errorf("error encoding row key for %q: %w", streamID, err)
			}
		}
		if err := db.translateRecordFields(columnTypes, fields); err != nil {
			return nil, fmt.Errorf("error backfilling table %q: %w", table, err)
		}

		log.WithField("fields", fields).Trace("got row")
		var seqval = make([]byte, 10)
		binary.BigEndian.PutUint64(seqval[2:], uint64(rowOffset))
		events = append(events, &sqlcapture.ChangeEvent{
			Operation: sqlcapture.InsertOp,
			RowKey:    rowKey,
			Source: &sqlserverSourceInfo{
				SourceCommon: sqlcapture.SourceCommon{
					Schema:   schema,
					Snapshot: true,
					Table:    table,
				},
				LSN:    []byte{},
				SeqVal: seqval,
			},
			Before: nil,
			After:  fields,
		})
		rowOffset++
	}
	return events, nil
}

// The set of column types for which we need to specify a text collation to
// get sane ordering and comparison of row keys. Represented as a map[string]bool
// so that it can be combined with the "is the column typename a string" check
// into one if statement.
var columnTypeCollatedText = map[string]bool{
	"char":     true,
	"varchar":  true,
	"nchar":    true,
	"nvarchar": true,
}

func (db *sqlserverDatabase) keylessScanQuery(info *sqlcapture.DiscoveryInfo, schemaName, tableName string) string {
	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT * FROM %s.%s", schemaName, tableName)
	fmt.Fprintf(query, " ORDER BY %%%%physloc%%%%")
	fmt.Fprintf(query, " OFFSET @p1 ROWS FETCH FIRST %d ROWS ONLY;", db.config.Advanced.BackfillChunkSize)
	return query.String()
}

func (db *sqlserverDatabase) buildScanQuery(start bool, keyColumns []string, columnTypes map[string]interface{}, schemaName, tableName string) string {
	var pkey []string
	var args []string
	for idx, colName := range keyColumns {
		var quotedName = quoteColumnName(colName)
		args = append(args, fmt.Sprintf("@p%d", idx+1))
		//
		if colType, ok := columnTypes[colName].(string); ok && columnTypeCollatedText[colType] {
			// The Latin1_General_100_BIN2_UTF8 collation basically means "just shut up
			// and order lexicographically by Unicode code point". And UTF-8 is designed
			// such that bytewise lexicographic ordering matches Unicode code-point ordering,
			// so this should hopefully match the internal scan key ordering in all cases.
			pkey = append(pkey, quotedName+` COLLATE Latin1_General_100_BIN2_UTF8`)
		} else {
			pkey = append(pkey, quotedName)
		}
	}

	var query = new(strings.Builder)
	fmt.Fprintf(query, "SELECT * FROM %s.%s", schemaName, tableName)
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
