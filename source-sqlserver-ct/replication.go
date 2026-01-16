package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/connectors/go/capture/sqlserver/backfill"
	"github.com/estuary/connectors/go/capture/sqlserver/change"
	"github.com/estuary/connectors/go/capture/sqlserver/datatypes"
	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
)

// ReplicationStream constructs a new ReplicationStream object, from which
// a neverending sequence of change events can be read.
func (db *sqlserverDatabase) ReplicationStream(ctx context.Context, startCursorJSON json.RawMessage) (sqlcapture.ReplicationStream, error) {
	var stream = &sqlserverCTReplicationStream{
		db:   db,
		conn: db.conn,
		cfg:  db.config,

		tables:        make(map[sqlcapture.StreamID]*tableReplicationInfo),
		tableVersions: make(map[sqlcapture.StreamID]int64),
	}

	// Parse per-table version map from the cursor JSON.
	// The cursor is serialized as a map with string keys like "schema.table",
	// so we need to convert those back to StreamID structs.
	if len(startCursorJSON) > 0 && string(startCursorJSON) != "null" {
		var stringKeyVersions map[string]int64
		if err := json.Unmarshal(startCursorJSON, &stringKeyVersions); err != nil {
			return nil, fmt.Errorf("error parsing cursor: %w", err)
		}
		for k, v := range stringKeyVersions {
			streamID, err := sqlcapture.ParseStreamIDKey(k)
			if err != nil {
				return nil, fmt.Errorf("error parsing cursor key %q: %w", k, err)
			}
			stream.tableVersions[streamID] = v
		}
	}

	// Handle cursor override flags. These are internal mechanisms for Estuary support.
	// The forceResetCursor flag overrides the entire cursor, resetting all tables to the specified version.
	// The initialBackfillCursor flag sets the version used when initializing newly-added tables.
	if db.forceResetCursor != "" {
		version, err := strconv.ParseInt(db.forceResetCursor, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid force_reset_cursor value %q: %w", db.forceResetCursor, err)
		}
		log.WithField("version", version).Warn("forcibly resetting all table versions")
		for streamID := range stream.tableVersions {
			stream.tableVersions[streamID] = version
		}
		stream.initialVersion = version // Also use for any newly-added tables
	} else if db.initialBackfillCursor != "" {
		version, err := strconv.ParseInt(db.initialBackfillCursor, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid initial_backfill_cursor value %q: %w", db.initialBackfillCursor, err)
		}
		log.WithField("version", version).Info("using initial backfill cursor for new tables")
		stream.initialVersion = version
	}

	return stream, nil
}

type sqlserverCTReplicationStream struct {
	db   *sqlserverDatabase
	conn *sql.DB
	cfg  *Config

	// Per-table version tracking
	tableVersions    map[sqlcapture.StreamID]int64
	pendingDeletions []sqlcapture.StreamID // Tables pending deletion from the cursor (deactivated or binding removed)
	initialVersion   int64                 // Version to use when initializing new tables (0 means use current version)

	// Per-table replication metadata
	tables map[sqlcapture.StreamID]*tableReplicationInfo
}

type tableReplicationInfo struct {
	Schema      string
	Table       string
	KeyColumns  []string // Key column names in key order
	Columns     []string // Sorted list of all column names
	ColumnTypes map[string]any
}

func (rs *sqlserverCTReplicationStream) ActivateTable(ctx context.Context, streamID sqlcapture.StreamID, keyColumns []string, discovery *sqlcapture.DiscoveryInfo, metadataJSON json.RawMessage) error {
	log.WithField("table", streamID).Trace("activate table")

	// Validate that configured key columns are available in Change Tracking.
	if !slices.Equal(discovery.PrimaryKey, keyColumns) {
		return fmt.Errorf("configured primary key %q does not match the table's current primary key %q; Change Tracking requires matching keys", keyColumns, discovery.PrimaryKey)
	}

	// Build sorted column list and type map from discovery information. Note that
	// this is fixed at activation time - if the table schema changes (columns added
	// or removed), those changes won't be reflected until the capture task is
	// restarted and re-discovers the table.
	var columns []string
	var columnTypes = make(map[string]any)
	for columnName, columnInfo := range discovery.Columns {
		columns = append(columns, columnName)
		columnTypes[columnName] = columnInfo.DataType
	}
	slices.Sort(columns)

	rs.tables[streamID] = &tableReplicationInfo{
		Schema:      discovery.Schema,
		Table:       discovery.Name,
		KeyColumns:  keyColumns,
		Columns:     columns,
		ColumnTypes: columnTypes,
	}

	// If this table doesn't have a version in our cursor yet, initialize it.
	// Use the initialVersion if set (from cursor override flags), otherwise query the current version.
	if _, ok := rs.tableVersions[streamID]; !ok {
		var version int64
		if rs.initialVersion != 0 {
			version = rs.initialVersion
			log.WithFields(log.Fields{"stream": streamID, "version": version}).Debug("initialized table version from override")
		} else {
			var err error
			version, err = ctGetCurrentVersion(ctx, rs.conn)
			if err != nil {
				return fmt.Errorf("error getting current CT version for table %q: %w", streamID, err)
			}
			log.WithFields(log.Fields{"stream": streamID, "version": version}).Debug("initialized table version to current")
		}
		rs.tableVersions[streamID] = version
	}

	log.WithFields(log.Fields{"stream": streamID}).Debug("activated table")
	return nil
}

func (rs *sqlserverCTReplicationStream) deactivateTable(streamID sqlcapture.StreamID) {
	delete(rs.tables, streamID)
	delete(rs.tableVersions, streamID)
	rs.pendingDeletions = append(rs.pendingDeletions, streamID)
}

func (rs *sqlserverCTReplicationStream) StartReplication(ctx context.Context, discovery map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo) error {
	// Any table in the cursor that wasn't activated before StartReplication has had its binding removed.
	// Mark these for deletion from the cursor.
	for streamID := range rs.tableVersions {
		if _, ok := rs.tables[streamID]; !ok {
			log.WithField("stream", streamID).Debug("marking stale cursor entry for deletion")
			rs.pendingDeletions = append(rs.pendingDeletions, streamID)
			delete(rs.tableVersions, streamID)
		}
	}
	return nil
}

func (rs *sqlserverCTReplicationStream) StreamToFence(ctx context.Context, fenceAfter time.Duration, callback func(event sqlcapture.DatabaseEvent) error) error {
	// Parse polling interval for sleep between polls
	pollingInterval, err := time.ParseDuration(rs.cfg.Advanced.PollingInterval)
	if err != nil {
		return fmt.Errorf("invalid polling interval %q: %w", rs.cfg.Advanced.PollingInterval, err)
	}

	// Flush any pending cursor deletions from removed bindings or deactivated tables.
	if len(rs.pendingDeletions) > 0 {
		log.WithField("deletions", rs.pendingDeletions).Debug("flushing pending cursor deletions")
		if err := callback(&sqlserverCommitEventCT{Deletions: rs.pendingDeletions}); err != nil {
			return err
		}
		rs.pendingDeletions = nil
	}

	// Timed streaming phase: poll repeatedly until fenceAfter deadline
	if fenceAfter > 0 {
		log.WithField("versions", rs.tableVersions).Debug("beginning timed streaming phase")
		var deadline = time.Now().Add(fenceAfter)
		for time.Now().Before(deadline) {
			toVersion, err := ctGetCurrentVersion(ctx, rs.conn)
			if err != nil {
				return fmt.Errorf("error getting current CT version: %w", err)
			}
			if err := rs.pollChanges(ctx, toVersion, callback); err != nil {
				return err
			}
			// Sleep until next poll or deadline, whichever comes first
			var sleepDuration = min(pollingInterval, time.Until(deadline))
			if sleepDuration > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(sleepDuration):
				}
			}
		}
	}
	log.WithField("versions", rs.tableVersions).Debug("finished timed streaming phase")

	// Establish the fence position by querying the current CT version.
	fenceVersion, err := ctGetCurrentVersion(ctx, rs.conn)
	if err != nil {
		return fmt.Errorf("error getting current version for fence: %w", err)
	}

	// Check if we're already at or past the fence version for all tables (idle DB).
	var allAtFence = true
	for _, v := range rs.tableVersions {
		if v < fenceVersion {
			allAtFence = false
			break
		}
	}

	if !allAtFence {
		// Poll one more time to ensure we're caught up before returning
		if err := rs.pollChanges(ctx, fenceVersion, callback); err != nil {
			return err
		}
	}

	// Output the latest state of all table cursors
	return callback(&sqlserverCommitEventCT{Versions: maps.Clone(rs.tableVersions)})
}

func (rs *sqlserverCTReplicationStream) Acknowledge(ctx context.Context, cursorJSON json.RawMessage) error {
	return nil
}

func (rs *sqlserverCTReplicationStream) Close(ctx context.Context) error {
	log.Debug("replication stream close requested")
	return nil
}

func (rs *sqlserverCTReplicationStream) pollChanges(ctx context.Context, toVersion int64, callback func(event sqlcapture.DatabaseEvent) error) error {
	// Batch fetch min valid versions for all CT-enabled tables
	minValidVersions, err := ctGetMinValidVersions(ctx, rs.conn)
	if err != nil {
		return fmt.Errorf("error getting min valid CT versions: %w", err)
	}

	// Build list of tables to poll, handling expired CT data along the way
	var tablesToPoll []*ctTablePollInfo
	for streamID, info := range rs.tables {
		fromVersion := rs.tableVersions[streamID]

		// Check that the table still has change tracking enabled and data hasn't expired
		var invalidCause error
		if minValid, ok := minValidVersions[streamID]; !ok {
			invalidCause = fmt.Errorf("change tracking no longer enabled for table %q", streamID)
		} else if fromVersion < minValid {
			invalidCause = fmt.Errorf("change tracking data expired for table %q (version %d < min valid %d)", streamID, fromVersion, minValid)
		}
		if invalidCause != nil {
			log.WithError(invalidCause).Warn("change tracking unavailable for table")
			if err := callback(&sqlcapture.TableDropEvent{StreamID: streamID, Cause: invalidCause.Error()}); err != nil {
				return err
			}
			rs.deactivateTable(streamID)
			continue
		}

		// Skip table if there are no new changes
		if fromVersion >= toVersion {
			continue
		}

		tablesToPoll = append(tablesToPoll, &ctTablePollInfo{
			StreamID:    streamID,
			Schema:      info.Schema,
			Table:       info.Table,
			KeyColumns:  info.KeyColumns,
			Columns:     info.Columns,
			ColumnTypes: info.ColumnTypes,
			FromVersion: fromVersion,
			ToVersion:   toVersion,
		})
	}

	// Poll each table, updating versions and emitting a small checkpoint after each one
	for _, pollInfo := range tablesToPoll {
		if err := rs.pollTable(ctx, pollInfo, callback); err != nil {
			return fmt.Errorf("error polling changes for table %q: %w", pollInfo.StreamID, err)
		}
		rs.tableVersions[pollInfo.StreamID] = toVersion
		if err := callback(&sqlserverCommitEventCT{
			Versions: map[sqlcapture.StreamID]int64{
				pollInfo.StreamID: toVersion,
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

type ctTablePollInfo struct {
	StreamID    sqlcapture.StreamID
	Schema      string
	Table       string
	KeyColumns  []string // Key column names in key order
	Columns     []string // Sorted list of all column names
	ColumnTypes map[string]any
	FromVersion int64
	ToVersion   int64
}

// ctMetadataColumns are the CT system columns that are always selected first in change queries.
// These columns are not part of the output schema and are used internally to determine
// the change version and operation type.
var ctMetadataColumns = []string{"CT.SYS_CHANGE_VERSION", "CT.SYS_CHANGE_OPERATION"}

func (rs *sqlserverCTReplicationStream) pollTable(ctx context.Context, info *ctTablePollInfo, callback func(event sqlcapture.DatabaseEvent) error) error {
	// Build the list of columns to select, starting with the CT metadata columns
	var selectColumns = slices.Clone(ctMetadataColumns)
	var pkJoinConditions []string

	// Add primary key columns from CT
	for _, pkCol := range info.KeyColumns {
		var quotedCol = backfill.QuoteIdentifier(pkCol)
		selectColumns = append(selectColumns, "CT."+quotedCol)
		pkJoinConditions = append(pkJoinConditions, fmt.Sprintf("CT.%s = T.%s", quotedCol, quotedCol))
	}

	// Add data columns from the source table (excluding PK columns already added).
	// info.Columns is already sorted for deterministic ordering.
	for _, colName := range info.Columns {
		if !slices.Contains(info.KeyColumns, colName) {
			selectColumns = append(selectColumns, "T."+backfill.QuoteIdentifier(colName))
		}
	}

	var quotedTable = backfill.QuoteTableName(info.Schema, info.Table)
	var query = fmt.Sprintf(`
		SELECT %s
		FROM CHANGETABLE(CHANGES %s, @p1) AS CT
		LEFT OUTER JOIN %s AS T ON %s
		WHERE CT.SYS_CHANGE_VERSION > @p1 AND CT.SYS_CHANGE_VERSION <= @p2
		ORDER BY CT.SYS_CHANGE_VERSION`,
		strings.Join(selectColumns, ", "),
		quotedTable,
		quotedTable,
		strings.Join(pkJoinConditions, " AND "),
	)

	rows, err := rs.conn.QueryContext(ctx, query, info.FromVersion, info.ToVersion)
	if err != nil {
		return fmt.Errorf("error querying changes: %w", err)
	}
	defer rows.Close()

	// Set up slices for generic scanning over query result rows
	cnames, err := rows.Columns()
	if err != nil {
		return err
	}
	var scanVals = make([]any, len(cnames))
	var scanPtrs = make([]any, len(scanVals))
	for idx := range scanVals {
		scanPtrs[idx] = &scanVals[idx]
	}

	// Preprocess result metadata for efficient row handling.
	var outputColumnNames = make([]string, len(cnames))                   // Names of all columns, with omitted columns set to ""
	var outputColumnTypes = make([]any, len(cnames))                      // Types of all columns, with omitted columns set to nil
	var outputTranscoders = make([]datatypes.JSONTranscoder, len(cnames)) // Transcoders for all columns, with omitted columns set to nil
	var rowKeyTranscoders = make([]datatypes.FDBTranscoder, len(cnames))  // Transcoders for key columns, with non-key columns set to nil
	var deleteNullability = make([]bool, len(cnames))                     // Whether nil values of a particular column should be included in delete events (always no for CT)
	for idx, name := range cnames {
		// Skip CT metadata columns (SYS_CHANGE_VERSION, SYS_CHANGE_OPERATION)
		if idx < len(ctMetadataColumns) {
			continue
		}

		outputColumnNames[idx] = name
		outputColumnTypes[idx] = info.ColumnTypes[name]
		outputTranscoders[idx] = datatypes.NewJSONTranscoder(info.ColumnTypes[name], rs.db.datatypesConfig)
		if slices.Contains(info.KeyColumns, name) {
			rowKeyTranscoders[idx] = datatypes.NewFDBTranscoder(info.ColumnTypes[name])
		}
	}
	outputColumnNames = append(outputColumnNames, "_meta")

	// Find key column indices for row key generation
	var keyIndices []int
	for _, colName := range info.KeyColumns {
		for idx, name := range cnames {
			if name == colName {
				keyIndices = append(keyIndices, idx)
				break
			}
		}
	}

	var replicationEventInfo = &change.SharedInfo{
		StreamID:          info.StreamID,
		Shape:             encrow.NewShape(outputColumnNames),
		Transcoders:       outputTranscoders,
		DeleteNullability: deleteNullability,
	}

	var rowKey = make([]byte, 0, 64)
	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return fmt.Errorf("error scanning result row: %w", err)
		}

		// Extract version and operation
		changeVersion, ok := scanVals[0].(int64)
		if !ok {
			return fmt.Errorf("invalid SYS_CHANGE_VERSION value: %v", scanVals[0])
		}

		var operation sqlcapture.ChangeOp
		opStr, ok := scanVals[1].(string)
		if !ok {
			return fmt.Errorf("invalid SYS_CHANGE_OPERATION value: %v", scanVals[1])
		}
		switch opStr {
		case "I":
			operation = sqlcapture.InsertOp
		case "U":
			operation = sqlcapture.UpdateOp
		case "D":
			operation = sqlcapture.DeleteOp
		default:
			return fmt.Errorf("unknown change operation: %q", opStr)
		}

		// Build row key
		rowKey = rowKey[:0]
		for _, idx := range keyIndices {
			if rowKeyTranscoders[idx] != nil {
				rowKey, err = rowKeyTranscoders[idx].TranscodeFDB(rowKey, scanVals[idx])
				if err != nil {
					return fmt.Errorf("error encoding key column %q: %w", cnames[idx], err)
				}
			}
		}

		var event = &sqlserverChangeEvent{
			Shared: replicationEventInfo,
			Meta: sqlserverChangeMetadataCT{
				Operation: operation,
				Source: sqlserverSourceInfoCT{
					SourceCommon: sqlcapture.SourceCommon{
						Schema: info.Schema,
						Table:  info.Table,
					},
					Version: changeVersion,
					Tag:     rs.db.config.Advanced.SourceTag,
				},
			},
			RowKey: rowKey,
			Values: scanVals,
		}
		if err := callback(event); err != nil {
			return err
		}
	}

	return rows.Err()
}

// ctGetCurrentVersion returns the current Change Tracking version for the database.
func ctGetCurrentVersion(ctx context.Context, conn *sql.DB) (int64, error) {
	var version int64
	const query = `SELECT CHANGE_TRACKING_CURRENT_VERSION();`
	if err := conn.QueryRowContext(ctx, query).Scan(&version); err != nil {
		return 0, fmt.Errorf("error querying current CT version: %w", err)
	}
	return version, nil
}

// ctGetMinValidVersions returns the minimum valid Change Tracking version for all CT-enabled tables.
// Changes older than a table's min valid version have been cleaned up and are no longer available.
func ctGetMinValidVersions(ctx context.Context, conn *sql.DB) (map[sqlcapture.StreamID]int64, error) {
	const query = `
		SELECT
			s.name AS schema_name,
			t.name AS table_name,
			ct.min_valid_version
		FROM sys.change_tracking_tables ct
		JOIN sys.tables t ON ct.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id;`

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error querying min valid CT versions: %w", err)
	}
	defer rows.Close()

	var result = make(map[sqlcapture.StreamID]int64)
	for rows.Next() {
		var schema, table string
		var minValid int64
		if err := rows.Scan(&schema, &table, &minValid); err != nil {
			return nil, fmt.Errorf("error scanning min valid version row: %w", err)
		}
		result[sqlcapture.JoinStreamID(schema, table)] = minValid
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating min valid versions: %w", err)
	}
	return result, nil
}

// ctEnabledTables returns the set of stream IDs for tables that have Change Tracking enabled.
func (db *sqlserverDatabase) ctEnabledTables(ctx context.Context) (map[sqlcapture.StreamID]bool, error) {
	var query = `
		SELECT s.name AS schema_name, t.name AS table_name
		FROM sys.change_tracking_tables ctt
		JOIN sys.tables t ON ctt.object_id = t.object_id
		JOIN sys.schemas s ON t.schema_id = s.schema_id;
	`
	rows, err := db.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("error listing CT-enabled tables: %w", err)
	}
	defer rows.Close()

	var result = make(map[sqlcapture.StreamID]bool)
	for rows.Next() {
		var schemaName, tableName string
		if err := rows.Scan(&schemaName, &tableName); err != nil {
			return nil, fmt.Errorf("error scanning result row: %w", err)
		}
		result[sqlcapture.JoinStreamID(schemaName, tableName)] = true
	}
	return result, nil
}
