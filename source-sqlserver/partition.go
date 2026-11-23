package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/estuary/connectors/go/capture/sqlserver/backfill"
	"github.com/estuary/connectors/go/capture/sqlserver/datatypes"
	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
)

// Partition-aware backfills
//
// SQL Server cannot merge the per-partition streams of a partitioned index into one
// ordered stream. When a table's key index is partitioned on a column other
// than its leading key column, the global `ORDER BY key` of a keyed backfill
// chunk therefore sorts every row past the resume key, in every partition, for
// every chunk. This does not scale for large tables.
//
// The connector instead backfills such tables one partition at a time, in
// ascending partition order, and in key order within each partition. `Scanned`
// stays an ordinary row key, and TableState.Metadata holds the boundary range
// of the partition it belongs to. Because the backfilled region is then not a
// prefix of key order, these tables use Unfiltered mode.
//
// A SPLIT or MERGE can alter a partition function while a backfill is in
// progress and make the resume key point past rows which were never read. So
// the connector finds the current partition by its stored range before every
// query, and after every query it checks that the partition function still has
// the boundaries the partition was found in. When the partition no longer has
// its stored range, the backfill re-reads from the start of the partition which
// now covers the start of that range.
//
// A range only identifies a partition of the function and column it was recorded under,
// so it names both. When the table has moved to another partition function or column, or
// the function has changed its range direction, the backfill starts over from the first
// partition.

// partitionedIndex describes an index on a partition scheme. Only those which pass
// discoverPartitionedIndexes can be backfilled one partition at a time.
type partitionedIndex struct {
	KeyColumns   []string // The index key columns, in order.
	Column       string   // The partition column.
	FunctionName string   // The name of the partition function.
	Partitions   int      // The number of partitions when discovered, not counting the one below a NULL boundary.
}

// partitionRange identifies a partition by its partition function, the column that
// function is applied to, and its boundary values, which SQL Server renders as canonical
// strings. A nil bound is the open end of the first or last partition.
type partitionRange struct {
	Function   string  `json:"function"`
	Column     string  `json:"column"`
	Lo         *string `json:"lo"`
	Hi         *string `json:"hi"`
	RangeRight bool    `json:"range_right"`
}

func (r partitionRange) Equal(other partitionRange) bool {
	var equalBound = func(x, y *string) bool {
		return (x == nil && y == nil) || (x != nil && y != nil && *x == *y)
	}

	return r.Function == other.Function && r.Column == other.Column && r.RangeRight == other.RangeRight &&
		equalBound(r.Lo, other.Lo) && equalBound(r.Hi, other.Hi)
}

func (r partitionRange) String() string {
	var bound = func(x *string, open string) string {
		if x == nil {
			return open
		}
		return *x
	}
	if r.RangeRight {
		return fmt.Sprintf("[%s, %s)", bound(r.Lo, "-inf"), bound(r.Hi, "+inf"))
	}
	return fmt.Sprintf("(%s, %s]", bound(r.Lo, "-inf"), bound(r.Hi, "+inf"))
}

// backfillMetadata is the connector's TableState.Metadata during a backfill.
type backfillMetadata struct {
	// Partition is the boundary range of the partition holding the resume key of a
	// partition-ordered backfill. Absent for key-ordered backfills.
	Partition *partitionRange `json:"partition,omitempty"`
}

func decodeBackfillMetadata(raw json.RawMessage) (*backfillMetadata, error) {
	var metadata = new(backfillMetadata)
	if len(raw) == 0 || string(raw) == "null" {
		return metadata, nil
	}
	if err := json.Unmarshal(raw, metadata); err != nil {
		return nil, fmt.Errorf("error decoding backfill metadata: %w", err)
	}
	return metadata, nil
}

// clearedMetadata removes TableState.Metadata from the checkpoint.
var clearedMetadata = json.RawMessage("null")

// isPartitionColumnSupported reports whether the partition-aware backfill can track a
// column's partitions. It identifies a partition by its boundary values rendered as
// `CONVERT(..., 126)` strings, so it only allows types for which that rendering is known
// to distinguish every value.
func isPartitionColumnSupported(dataType any) bool {
	switch t := dataType.(type) {
	case *datatypes.TextColumnType:
		return slices.Contains([]string{"char", "varchar", "nchar", "nvarchar"}, t.Type)
	case string:
		return slices.Contains([]string{
			"tinyint", "smallint", "int", "bigint", "decimal", "numeric", "uniqueidentifier",
			"date", "time", "datetime", "datetime2", "datetimeoffset", "smalldatetime",
		}, t)
	}
	return false
}

func queryDiscoverPartitionedIndexes(predicate string) string {
	return fmt.Sprintf(`
SELECT sch.name, tbl.name, idx.index_id, col.name, ic.key_ordinal, ic.partition_ordinal, pf.name,
       pf.fanout - (SELECT COUNT(*) FROM sys.partition_range_values prv WHERE prv.function_id = pf.function_id AND prv.value IS NULL)
  FROM sys.indexes idx
  JOIN sys.tables tbl ON tbl.object_id = idx.object_id
  JOIN sys.schemas sch ON sch.schema_id = tbl.schema_id
  JOIN sys.partition_schemes ps ON ps.data_space_id = idx.data_space_id
  JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
  JOIN sys.index_columns ic ON ic.object_id = idx.object_id AND ic.index_id = idx.index_id
  JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id
  WHERE (ic.key_ordinal > 0 OR ic.partition_ordinal = 1) AND %s
  ORDER BY sch.name, tbl.name, idx.index_id, ic.key_ordinal;`, predicate)
}

// getPartitionedIndexes lists every index on a partition scheme, grouped by table. It
// records the partition column whether or not that column is also a key column.
func getPartitionedIndexes(ctx context.Context, conn *sql.DB, requested []sqlcapture.TableID) (map[sqlcapture.StreamID][]partitionedIndex, error) {
	var indexesByStream = make(map[sqlcapture.StreamID][]partitionedIndex)
	if len(requested) == 0 {
		return indexesByStream, nil
	}

	var tablesFilter, args = tableIDsPredicate("sch.name", "tbl.name", requested, 1)
	var rows, err = conn.QueryContext(ctx, queryDiscoverPartitionedIndexes(tablesFilter), args...)
	if err != nil {
		return nil, fmt.Errorf("error querying partitioned indexes: %w", err)
	}
	defer rows.Close()

	var currentStream sqlcapture.StreamID
	var currentIndexID = -1

	for rows.Next() {
		var tableSchema, tableName, columnName, functionName string
		var indexID, keyOrdinal, partitionOrdinal, partitions int

		if err := rows.Scan(&tableSchema, &tableName, &indexID, &columnName, &keyOrdinal, &partitionOrdinal, &functionName, &partitions); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}

		var streamID = sqlcapture.JoinStreamID(tableSchema, tableName)
		if streamID != currentStream || indexID != currentIndexID {
			indexesByStream[streamID] = append(indexesByStream[streamID], partitionedIndex{FunctionName: functionName, Partitions: partitions})
			currentStream, currentIndexID = streamID, indexID
		}

		var current = &indexesByStream[streamID][len(indexesByStream[streamID])-1]
		if partitionOrdinal == 1 {
			current.Column = columnName
		}
		if keyOrdinal > 0 {
			current.KeyColumns = append(current.KeyColumns, columnName)
			if len(current.KeyColumns) != keyOrdinal {
				return nil, fmt.Errorf("internal error: partitioned index key ordering failure: index %d on stream %q: column %q appears out of order", indexID, streamID, columnName)
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating partitioned indexes: %w", err)
	}

	return indexesByStream, nil
}

// discoverPartitionedIndexes records on each table the index by which the connector can
// backfill its discovered key one partition at a time, if there is one. That is an index
// on the key columns, on a partition scheme with more than one partition, whose partition
// column is a supported type and a key column other than the leading one. When it is the
// leading key column, key order already is partition order.
func (db *sqlserverDatabase) discoverPartitionedIndexes(ctx context.Context, dst map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo, requested []sqlcapture.TableID) error {
	if !db.featureFlags["partition_aware_backfill"] {
		return nil
	}

	var partitioned, err = getPartitionedIndexes(ctx, db.conn, requested)
	if err != nil {
		return err
	}

	for streamID, indexes := range partitioned {
		var info, ok = dst[streamID]
		if !ok {
			continue
		}

		var details = info.ExtraDetails.(*sqlserverTableDiscoveryDetails)

		for _, index := range indexes {
			if !slices.Equal(index.KeyColumns, info.PrimaryKey) || index.Partitions < 2 || slices.Index(index.KeyColumns, index.Column) < 1 {
				continue
			}

			var dataType = info.Columns[index.Column].DataType
			var computed = slices.Contains(details.ComputedColumns, index.Column)
			if computed || !isPartitionColumnSupported(dataType) {
				log.WithFields(log.Fields{
					"stream":   streamID,
					"column":   index.Column,
					"type":     fmt.Sprint(dataType),
					"computed": computed,
				}).Warn("table will be backfilled in key order, its partition column is computed or of a type partition-aware backfills don't support")
				continue
			}
			details.PartitionedKey = &index
			break
		}
	}
	return nil
}

// partitionedBackfillIndex returns the index by which to backfill a table one partition
// at a time, or nil when the backfill should be key-ordered. When the table has such an
// index but this backfill can't use it, unusedReason says why. It is empty otherwise.
func partitionedBackfillIndex(info *sqlcapture.DiscoveryInfo, state *sqlcapture.TableState) (index *partitionedIndex, unusedReason string) {
	var details, _ = info.ExtraDetails.(*sqlserverTableDiscoveryDetails)
	if details == nil || details.PartitionedKey == nil {
		return nil, ""
	}

	// Precise backfills filter replication events by key order, which partition order
	// would make wrong. Only tables in Unfiltered mode can be backfilled by partition.
	if state.Mode != sqlcapture.TableStateUnfilteredBackfill {
		return nil, "the binding's backfill mode is Precise, set it to Normal or Automatic to backfill by partition"
	}

	if !slices.Equal(details.PartitionedKey.KeyColumns, state.KeyColumns) {
		return nil, "the backfill key differs from the key of the table's partitioned index"
	}

	return details.PartitionedKey, ""
}

// partitionFilter extends a backfill filter to select only partition n of an index. An
// equality on the partition id restricts the query to one partition, and SQL Server then
// runs it as a seek in key order with no sort.
func partitionFilter(index *partitionedIndex, n int, backfillFilter string) string {
	var filter = fmt.Sprintf("$PARTITION.%s(%s) = %d", backfill.QuoteIdentifier(index.FunctionName), backfill.QuoteIdentifier(index.Column), n)

	if backfillFilter != "" {
		filter += fmt.Sprintf(" AND (%s)", backfillFilter)
	}

	return filter
}

// partitionLayout is the live boundary list of a partition function.
//
// A partition function may have a NULL boundary value. NULL sorts below every other
// value, so it is always the first boundary. Under RANGE LEFT the partition below it holds
// only NULLs, and under RANGE RIGHT it holds nothing. The partition column is a
// non-nullable key column, so that partition never has rows.
//
// The layout numbers partitions the same way SQL Server does. With a NULL boundary, SQL
// Server counts the empty partition below it, so the first partition with rows is number
// 2. The layout leaves a NULL boundary out of Boundaries and sets LeadingNull instead, so
// partition ranges are the same with or without one. A backfill which is resuming in the
// first partition with rows therefore keeps its stored range when someone adds or removes
// a NULL boundary.
type partitionLayout struct {
	Function    string   // The name of the partition function.
	Column      string   // The column the function is applied to.
	Boundaries  []string // Canonical boundary values, ascending, without a NULL boundary.
	RangeRight  bool
	LeadingNull bool // Whether the function's first boundary value is NULL.
}

// First returns the number of the first partition which can hold rows.
func (l *partitionLayout) First() int {
	if l.LeadingNull {
		return 2
	}
	return 1
}

// Count returns the number of partitions, which is also the number of the last one.
func (l *partitionLayout) Count() int { return len(l.Boundaries) + l.First() }

func (l *partitionLayout) Equal(other *partitionLayout) bool {
	return l.Function == other.Function && l.Column == other.Column && l.RangeRight == other.RangeRight &&
		l.LeadingNull == other.LeadingNull && slices.Equal(l.Boundaries, other.Boundaries)
}

// Range returns the boundary range of partition n, for n from First to Count.
func (l *partitionLayout) Range(n int) partitionRange {
	var i = n - l.First() // The index of partition n among the partitions which can hold rows.
	var r = partitionRange{Function: l.Function, Column: l.Column, RangeRight: l.RangeRight}

	if i >= 1 {
		r.Lo = &l.Boundaries[i-1]
	}
	if i < len(l.Boundaries) {
		r.Hi = &l.Boundaries[i]
	}

	return r
}

// canLocate reports whether r is a range of this layout's partition function, column and
// range direction. The bounds of any other range say nothing about this layout's partitions.
func (l *partitionLayout) canLocate(r partitionRange) bool {
	return r.Function == l.Function && r.Column == l.Column && r.RangeRight == l.RangeRight
}

// locatePartitionNumber returns the number of the partition which begins at r's lower bound,
// and whether that partition still has range r. ok is false when the bound is no longer a boundary,
// which is when the partition was merged into the one before it.
//
// When the layout can't locate r at all, it returns the first partition. Starting anywhere
// later would never read the rows of this layout which sort below r, or which equal its
// lower bound when the range direction has changed.
func (l *partitionLayout) locatePartitionNumber(r partitionRange) (n int, unchanged, ok bool) {
	n = l.First()
	if !l.canLocate(r) {
		return n, false, true
	}
	if r.Lo != nil {
		var idx = slices.Index(l.Boundaries, *r.Lo)
		if idx < 0 {
			return 0, false, false
		}
		n = l.First() + idx + 1
	}
	return n, l.Range(n).Equal(r), true
}

// readPartitionLayout reads the live boundary list of an index's partition function.
func (db *sqlserverDatabase) readPartitionLayout(ctx context.Context, index *partitionedIndex) (*partitionLayout, error) {
	var function = index.FunctionName
	const query = `
SELECT pf.boundary_value_on_right, prv.boundary_id, CONVERT(nvarchar(4000), prv.value, 126)
  FROM sys.partition_functions pf
  LEFT JOIN sys.partition_range_values prv ON prv.function_id = pf.function_id
  WHERE pf.name = @p1
  ORDER BY prv.boundary_id;`
	var rows, err = db.conn.QueryContext(ctx, query, function)
	if err != nil {
		return nil, fmt.Errorf("error reading boundaries of partition function %q: %w", function, err)
	}
	defer rows.Close()

	var layout *partitionLayout
	for rows.Next() {
		var rangeRight bool
		var boundaryID sql.NullInt64 // The LEFT JOIN gives a function without boundaries one row with a NULL boundary_id.
		var boundary sql.NullString
		if err := rows.Scan(&rangeRight, &boundaryID, &boundary); err != nil {
			return nil, fmt.Errorf("error reading boundaries of partition function %q: %w", function, err)
		}
		if layout == nil {
			layout = &partitionLayout{Function: function, Column: index.Column, RangeRight: rangeRight}
		}
		if !boundaryID.Valid {
			continue
		}
		if !boundary.Valid {
			layout.LeadingNull = true // NULL sorts below every other value, so it is always the first boundary.
			continue
		}
		layout.Boundaries = append(layout.Boundaries, boundary.String)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error reading boundaries of partition function %q: %w", function, err)
	}
	if layout == nil {
		return nil, fmt.Errorf("partition function %q not found", function)
	}
	return layout, nil
}

// partitionOf returns the number of the partition that holds a boundary value.
//
// The value is a string from readPartitionLayout. SQL Server turns it back into the
// partition function's input type before looking up the partition. For every type that
// isPartitionColumnSupported allows, that gives back the original value.
func (db *sqlserverDatabase) partitionOf(ctx context.Context, function, value string) (int, error) {
	var n int
	var query = fmt.Sprintf(`SELECT $PARTITION.%s(@p1);`, backfill.QuoteIdentifier(function))
	if err := db.conn.QueryRowContext(ctx, query, value).Scan(&n); err != nil {
		return 0, fmt.Errorf("error finding the partition of %q in partition function %q: %w", value, function, err)
	}
	return n, nil
}

// locatePartition returns the number of the partition in layout from which to continue a
// backfill that was reading a partition with range r, and whether that partition still
// has range r.
//
// When it doesn't, the backfill has to re-read the returned partition from its start.
// That is the partition which now covers the start of r, since the backfill has already
// read everything before it. It begins at r's lower bound if that is still a boundary,
// and otherwise is the one r's partition was merged into. When the table was
// repartitioned, it is the first partition.
//
// A partition number only means something for the layout it was computed against, and
// the function may have changed since layout was read. The caller has to confirm that it
// hasn't before relying on the number.
func (db *sqlserverDatabase) locatePartition(ctx context.Context, layout *partitionLayout, r partitionRange) (int, bool, error) {
	if n, unchanged, ok := layout.locatePartitionNumber(r); ok {
		return n, unchanged, nil
	}
	var n, err = db.partitionOf(ctx, layout.Function, *r.Lo)
	return n, false, err
}

// logPartitionChange reports that the partition with range expected no longer exists.
func logPartitionChange(streamID sqlcapture.StreamID, layout *partitionLayout, expected partitionRange, n int) {
	var cause = "lower bound is still a partition boundary (split of the partition, or merge with the next one)"
	switch {
	case !layout.canLocate(expected):
		cause = "table was repartitioned (another partition function or column, or the function's range direction changed)"
	case expected.Lo != nil && !slices.Contains(layout.Boundaries, *expected.Lo):
		cause = "lower bound is no longer a partition boundary (merge with the previous partition)"
	}
	log.WithFields(log.Fields{
		"stream":    streamID,
		"function":  layout.Function,
		"column":    layout.Column,
		"expected":  expected.String(),
		"cause":     cause,
		"partition": n,
		"restart":   layout.Range(n).String(),
	}).Info("partitions changed during backfill, re-reading a partition from its start")
}

// scanPartitioned backfills the next chunk of a table one partition at a time. It returns
// the same values as ScanTableChunk.
//
// A call normally returns the rows of one query: the next chunk of the current partition.
// When that partition has no rows left it reads the following partitions from their
// start, skipping empty ones, until one returns rows or none are left.
func (db *sqlserverDatabase) scanPartitioned(ctx context.Context, scan *chunkScan, index *partitionedIndex, stored *partitionRange, backfillFilter string) (bool, []byte, error) {
	var state = scan.state

	var layout, err = db.readPartitionLayout(ctx, index)
	if err != nil {
		return false, nil, err
	}

	// target is the range of the partition to read, or nil for the first partition. resume
	// is whether to continue from the resume key rather than from the partition's start.
	var target *partitionRange
	if state.Scanned != nil {
		target = stored
	}
	var resume = target != nil

	// Reading a partition from its start is a backfill query without a resume key.
	var fromStart = *state
	fromStart.Scanned = nil

	for {
		// Find the partition to read. The resume key is only good for a partition which
		// still has the target range, so any other is read from its start.
		var n, isPartitionIntact = layout.First(), true
		if target != nil {
			if n, isPartitionIntact, err = db.locatePartition(ctx, layout, *target); err != nil {
				return false, nil, err
			}
		}
		resume = resume && isPartitionIntact
		var queryState = &fromStart
		if resume {
			queryState = state
		}

		query, args, err := backfill.BuildBackfillQuery(queryState, scan.schema, scan.table, scan.columnTypes, scan.chunkSize, partitionFilter(index, n, backfillFilter))
		if err != nil {
			return false, nil, err
		}
		rows, lastKey, err := scan.run(ctx, query, args)
		if err != nil {
			return false, nil, err
		}

		// Confirm the rows came from the intended partition, which they did if the
		// function still has the layout n was found in. A SPLIT or MERGE can't run during
		// the query, but one can run between the layout read and the query. If one did,
		// find the target again in the new layout. That reads some rows a second time.
		after, err := db.readPartitionLayout(ctx, index)
		if err != nil {
			return false, nil, err
		}
		if !after.Equal(layout) {
			layout = after
			continue
		}

		var current = layout.Range(n)
		if !isPartitionIntact {
			logPartitionChange(scan.streamID, layout, *target, n)
		}
		if rows > 0 {
			if !resume {
				log.WithFields(log.Fields{
					"stream":     scan.streamID,
					"partition":  n,
					"partitions": layout.Count(),
					"range":      current.String(),
					"backfilled": state.BackfilledCount + scan.emitted - rows,
				}).Info("backfilling next partition")
			}
			state.Metadata, err = json.Marshal(&backfillMetadata{Partition: &current})
			return false, lastKey, err
		}

		// The partition has no rows left, so go on to the start of the next one.
		if n == layout.Count() {
			state.Metadata = clearedMetadata
			return true, nil, nil
		}
		var next = layout.Range(n + 1)
		target, resume = &next, false
	}
}
