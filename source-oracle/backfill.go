package main

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/sirupsen/logrus"
)

const StartingRowID = "AAAAAAAAAAAAAAAAAA"

// ScanTableChunk fetches a chunk of rows from the specified table, resuming from `resumeKey` if non-nil.
func (db *oracleDatabase) ScanTableChunk(ctx context.Context, info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState, callback func(event *sqlcapture.ChangeEvent) error) (bool, []byte, error) {
	logrus.WithField("state", state).Debug("backfill: ScanChunk")
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
	var query, rowidRangeEnd, lastTableRowID string
	var args []any

	switch state.Mode {
	case sqlcapture.TableStateKeylessBackfill:
		if err := db.fetchBackfillRowIDRange(ctx, schema, table); err != nil {
			return false, nil, fmt.Errorf("error fetching backfill rowid range: %w", err)
		}
		lastTableRowID = db.backfillRowIDRanges[streamID][len(db.backfillRowIDRanges[streamID])-1]

		var afterRowID = db.backfillRowIDRanges[streamID][0]
		if resumeAfter != nil {
			afterRowID = string(resumeAfter)
		}

		logEntry.WithField("rowid", afterRowID).Debug("backfill: scanning keyless table chunk")

		query, rowidRangeEnd = db.keylessScanQuery(info, streamID, afterRowID, db.backfillRowIDRanges[streamID])

	case sqlcapture.TableStatePreciseBackfill, sqlcapture.TableStateUnfilteredBackfill:
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
			}).Debug("backfill: scanning subsequent table chunk")
			query = db.buildScanQuery(false, info, keyColumns, columnTypes, schema, table)
			for idx, k := range resumeKey {
				args = append(args, sql.Named(fmt.Sprintf("p%d", idx+1), k))
			}
		} else {
			logEntry.WithField("keyColumns", keyColumns).Debug("scanning initial table chunk")
			query = db.buildScanQuery(true, info, keyColumns, columnTypes, schema, table)
		}
	default:
		return false, nil, fmt.Errorf("invalid backfill mode %q", state.Mode)
	}

	// If this is the first chunk being backfilled, run an `EXPLAIN` on it and log the results
	db.explainQuery(ctx, streamID, query, args)

	// Execute the backfill query to fetch rows from the database
	logEntry.WithFields(logrus.Fields{"query": query, "args": args}).Debug("executing query")
	rows, err := db.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return false, nil, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer rows.Close()

	// Process the results into `changeEvent` structs and return them
	cols, err := rows.Columns()
	if err != nil {
		return false, nil, fmt.Errorf("rows.Columns: %w", err)
	}
	var resultRows int    // Count of rows received within the current backfill chunk
	var nextRowKey []byte // The row key from which a subsequent backfill chunk should resume
	logEntry.Debug("backfill: translating query rows to change events")

	var fields = make(map[string]any, len(cols)-1)
	var rowid string

	var rowOffset = state.BackfilledCount
	for rows.Next() {
		if rowid, err = scanToMap(rows, cols, fields); err != nil {
			return false, nil, err
		}

		var rowKey []byte
		if state.Mode == sqlcapture.TableStateKeylessBackfill {
			rowKey = []byte(rowid)

			// Keyless backfills are not ordered, so we need to check every row to see if it includes the last ROWID known
			// at the time of starting the backfill which is the last rowid in the backfill range slice
		} else {
			rowKey, err = sqlcapture.EncodeRowKey(keyColumns, fields, columnTypes, encodeKeyFDB)
			if err != nil {
				return false, nil, fmt.Errorf("error encoding row key for %q: %w", streamID, err)
			}
		}
		nextRowKey = rowKey

		if err := translateRecordFields(info, fields); err != nil {
			return false, nil, fmt.Errorf("error backfilling table %q: %w", table, err)
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

				// Artificial RSID and SSN during backfill
				RSID: "0x000000.00000000.0000",
				SSN:  rowOffset,
			},
			Before: nil,
			After:  fields,
		}
		if err := callback(event); err != nil {
			return false, nil, fmt.Errorf("error processing change event: %w", err)
		}
		resultRows++
		rowOffset++
	}

	if err := rows.Err(); err != nil {
		return false, nil, err
	}

	var backfillComplete bool
	switch state.Mode {
	case sqlcapture.TableStateKeylessBackfill:
		backfillComplete = rowidRangeEnd == lastTableRowID
	case sqlcapture.TableStatePreciseBackfill, sqlcapture.TableStateUnfilteredBackfill:
		backfillComplete = resultRows < db.config.Advanced.BackfillChunkSize
	default:
		return false, nil, fmt.Errorf("invalid backfill mode %q", state.Mode)
	}

	// for keyless backfill tables it is possible that we query a rowid range which does not have any rows,
	// to avoid getting stuck in a loop nextRowKey has to be set to the end of the range
	if rowidRangeEnd != "" {
		nextRowKey = []byte(rowidRangeEnd)
	}

	return backfillComplete, nextRowKey, nil
}

func scanToMap(rows *sql.Rows, cols []string, fields map[string]any) (string, error) {
	var fieldsArr = make([]any, len(cols))
	var rowid string
	for idx, col := range cols {
		if col == "ROWID" {
			fieldsArr[idx] = &rowid
		} else {
			fieldsArr[idx] = new(any)
		}
	}
	if err := rows.Scan(fieldsArr...); err != nil {
		return "", fmt.Errorf("scanning row: %w", err)
	}

	for idx, col := range cols {
		if col == "ROWID" {
			continue
		}
		fields[col] = *(fieldsArr[idx].(*any))
	}

	return rowid, nil
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
func (db *oracleDatabase) WatermarksTable() sqlcapture.StreamID {
	var bits = strings.SplitN(db.config.Advanced.WatermarksTable, ".", 2)
	return sqlcapture.JoinStreamID(bits[0], bits[1])
}

func (db *oracleDatabase) fetchBackfillRowIDRange(ctx context.Context, schemaName string, tableName string) error {
	var streamID = sqlcapture.JoinStreamID(schemaName, tableName)
	if _, ok := db.backfillRowIDRanges[streamID]; ok {
		return nil
	}

	// This query starts by reading dba_extents which has information on data blocks of tables
	// and splits those blocks into 1g buckets by calculating how many blocks are required to fill
	// 1gb based on db_block_size, then dividing up the blocks into buckets using width_bucket,
	// calculating the range begin end values necessary to construct rowids and
	// finally generating ROWID ranges based on these buckets so we can do backfills 1gb at a time
	var query = fmt.Sprintf(`
    with extents_data as (
      select o.data_object_id, e.file_id, e.block_id, e.blocks
      from dba_extents e
      join all_objects o
      on (e.owner, e.segment_name, e.segment_type) = ((o.owner, o.object_name, o.object_type))
        and decode(e.partition_name, o.subobject_name, 0, 1) = 0
      where e.segment_type in ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
        and e.owner = '%s'
        and e.segment_name = '%s'
    )
    , extents_with_sums as (
      select sum(blocks) over() total_blocks,
        sum(blocks) over(order by data_object_id, file_id, block_id) - blocks cumul_prev_blocks,
        sum(blocks) over(order by data_object_id, file_id, block_id) cumul_current_blocks,
        greatest(1, FLOOR((sum(blocks) over())/(SELECT (1024*1024*1024) / TO_NUMBER(value) FROM v$parameter WHERE name='db_block_size'))) num_buckets,
        e.*
      from extents_data e
    )
    , extents_with_buckets as (
      select width_bucket(cumul_prev_blocks, 1, total_blocks + 1, num_buckets) prev_bucket,
        width_bucket(cumul_prev_blocks+1, 1, total_blocks + 1, num_buckets) first_bucket,
        width_bucket(cumul_current_blocks, 1, total_blocks + 1, num_buckets) last_bucket,
        e.*
      from extents_with_sums e
    )
    , selected_extents as (
      select *
      from extents_with_buckets
      where cumul_current_blocks = round((last_bucket * total_blocks) / num_buckets)
        or prev_bucket < last_bucket
    )
    , expanded_extents as (
      select first_bucket + level - 1 bucket,
        case level when 1 then cumul_prev_blocks
          else round(((first_bucket + level - 2) * total_blocks) / num_buckets)
        end start_blocks,
        case first_bucket + level - 1 when last_bucket then cumul_current_blocks - 1
          else round(((first_bucket + level - 1) * total_blocks) / num_buckets) - 1
        end end_blocks,
        e.*
      from selected_extents e
      connect by cumul_prev_blocks = prior cumul_prev_blocks
        and first_bucket + level -1 <= last_bucket
        and prior sys_guid() is not null
    )
    , answer as (
      select bucket,
        min(data_object_id)
          keep (dense_rank first order by cumul_prev_blocks) first_data_object_id,
        min(file_id)
          keep (dense_rank first order by cumul_prev_blocks) first_file_id,
        min(block_id + start_blocks - cumul_prev_blocks)
          keep (dense_rank first order by cumul_prev_blocks) first_block_id,
        max(end_blocks) + 1 - min(start_blocks) blocks
      from expanded_extents
      group by bucket
    ) select
    dbms_rowid.rowid_create(
      1, first_data_object_id, first_file_id, first_block_id, 0
    ) rowid_start
    from answer
    order by bucket
    `, schemaName, tableName)

	var rows, err = db.conn.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("querying rowid range: %w", err)
	}
	defer rows.Close()

	db.backfillRowIDRanges[streamID] = make([]string, 0)

	for rows.Next() {
		var rowidStart string
		if err := rows.Scan(&rowidStart); err != nil {
			return fmt.Errorf("scanning rowid range: %w", err)
		}

		db.backfillRowIDRanges[streamID] = append(db.backfillRowIDRanges[streamID], rowidStart)
	}

	var row = db.conn.QueryRowContext(ctx, fmt.Sprintf(`SELECT MAX(ROWID) FROM "%s"."%s"`, schemaName, tableName))
	if err != nil {
		return fmt.Errorf("querying max ROWID: %w", err)
	}
	var lastRowID string
	if err := row.Scan(&lastRowID); err != nil {
		return fmt.Errorf("scanning max ROWID: %w", err)
	}

	db.backfillRowIDRanges[streamID] = append(db.backfillRowIDRanges[streamID], lastRowID)

	if len(db.backfillRowIDRanges[streamID]) < 2 {
		return fmt.Errorf("expected at least one rowid range (2 items), instead got %v", db.backfillRowIDRanges[streamID])
	}

	logrus.WithFields(logrus.Fields{
		"ranges":   fmt.Sprintf("%v", db.backfillRowIDRanges[streamID]),
		"streamID": streamID,
	}).Debug("backfill: rowid ranges")

	return nil
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
	var dataType = col.DataType.(oracleColumnType).Original
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
	var format string
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
// since ROWIDs are not always increasing (new rows can use smaller ROWIDs if space is available in an earlier block)
// but since we will capture changes since the start of the backfill using SCN tracking, we will eventually be consistent
func (db *oracleDatabase) keylessScanQuery(info *sqlcapture.DiscoveryInfo, streamID sqlcapture.StreamID, afterRowID string, rowidRanges []string) (string, string) {
	var query = new(strings.Builder)
	var columnSelect []string
	for _, col := range info.Columns {
		columnSelect = append(columnSelect, castColumn(col))
	}

	var idx = slices.Index(rowidRanges, afterRowID)
	// It is possible for rowidRanges to change across restarts, and thus afterRowID will not match
	// the same ranges. In this case we re-backfill the last range
	if idx == -1 {
		var nextIdx = slices.IndexFunc(rowidRanges, func(rowid string) bool {
			return afterRowID == rowid || base64LessThan(afterRowID, rowid)
		})

		if nextIdx == -1 {
			panic(fmt.Sprintf("expected to find ROWID range for %s in %v", afterRowID, rowidRanges))
		}

		idx = nextIdx - 1
		if idx < 0 {
			idx = 0
		}
	}

	if idx == len(rowidRanges)-1 {
		panic(fmt.Sprintf("unexpected keylessScanQuery with last ROWID as afterRowID: %s, %v", afterRowID, rowidRanges))
	}

	var rowidStart = rowidRanges[idx]
	var rowidEnd = rowidRanges[idx+1]

	// It is faster to first find the smallest and the largest ROWIDs of the range we want to cover and then query
	// all of the data in that range, instead of ordering all rows based on ROWID and then filtering the ROWID
	fmt.Fprintf(query, `SELECT ROWID, %s FROM "%s"."%s"`, strings.Join(columnSelect, ","), streamID.Schema, streamID.Table)
	fmt.Fprintf(query, ` WHERE ROWID >= '%s'`, rowidStart)
	fmt.Fprintf(query, ` AND ROWID <= '%s'`, rowidEnd)
	return query.String(), rowidEnd
}

func (db *oracleDatabase) buildScanQuery(start bool, info *sqlcapture.DiscoveryInfo, keyColumns []string, columnTypes map[string]oracleColumnType, schemaName, tableName string) string {
	// Construct lists of key specifiers and placeholders. They will be joined with commas and used in the query itself.
	var pkey []string
	var args []string
	for idx, colName := range keyColumns {
		var quotedName = quoteColumnName(colName)
		// If a precise backfill is requested *and* the column type requires binary ordering for precise
		// backfill comparisons to work, add the 'COLLATE BINARY' qualifier to the column name.
		if columnTypes[colName].JsonType == "string" && columnBinaryKeyComparison[colName] {
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

func (db *oracleDatabase) explainQuery(ctx context.Context, streamID sqlcapture.StreamID, query string, args []interface{}) {
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

	rows, err := db.conn.QueryContext(ctx, "SELECT PLAN_TABLE_OUTPUT FROM table(dbms_xplan.display)")
	if err != nil {
		logrus.WithFields(logrus.Fields{"id": streamID, "err": err}).Error("unable to explain query")
		return
	}
	defer rows.Close()

	var outputLines []string
	for rows.Next() {
		var outputLine string

		if err := rows.Scan(&outputLine); err != nil {
			logrus.WithFields(logrus.Fields{
				"id":  streamID,
				"err": err,
			}).Error("error getting row value")
			return
		}

		outputLines = append(outputLines, outputLine)
	}

	logrus.WithFields(logrus.Fields{
		"streamID": streamID,
		"output":   strings.Join(outputLines, "\n"),
	}).Info("explain backfill query")
}
