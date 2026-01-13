package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
	log "github.com/sirupsen/logrus"
)

// ReplicationStream constructs a new ReplicationStream object, from which
// a neverending sequence of change events can be read.
func (db *sqlserverDatabase) ReplicationStream(ctx context.Context, startCursorJSON json.RawMessage) (sqlcapture.ReplicationStream, error) {
	var stream = &sqlserverCTReplicationStream{db: db, conn: db.conn, cfg: db.config}
	if err := stream.open(ctx); err != nil {
		return nil, fmt.Errorf("error opening replication stream: %w", err)
	}

	// Parse per-table version map from the cursor JSON.
	// The cursor is serialized as a map with string keys like "schema.table",
	// so we need to convert those back to StreamID structs.
	stream.tableVersions = make(map[sqlcapture.StreamID]int64)
	if len(startCursorJSON) > 0 && string(startCursorJSON) != "null" {
		var stringKeyVersions map[string]int64
		if err := json.Unmarshal(startCursorJSON, &stringKeyVersions); err != nil {
			return nil, fmt.Errorf("error parsing cursor: %w", err)
		}
		for k, v := range stringKeyVersions {
			if parts := strings.SplitN(k, ".", 2); len(parts) == 2 {
				stream.tableVersions[sqlcapture.JoinStreamID(parts[0], parts[1])] = v
			}
		}
	}

	// Handle cursor override flags. These are internal mechanisms for Estuary support.
	// The forceResetCursor flag overrides the entire cursor, resetting all tables to the specified version.
	// The initialBackfillCursor flag sets the version used when initializing newly-added tables.
	if db.forceResetCursor != "" {
		var version, err = parseVersionString(db.forceResetCursor)
		if err != nil {
			return nil, fmt.Errorf("invalid force_reset_cursor value %q: %w", db.forceResetCursor, err)
		}
		log.WithField("version", version).Warn("forcibly resetting all table versions")
		for streamID := range stream.tableVersions {
			stream.tableVersions[streamID] = version
		}
		stream.initialVersion = version // Also use for any newly-added tables
	} else if db.initialBackfillCursor != "" {
		var version, err = parseVersionString(db.initialBackfillCursor)
		if err != nil {
			return nil, fmt.Errorf("invalid initial_backfill_cursor value %q: %w", db.initialBackfillCursor, err)
		}
		log.WithField("version", version).Info("using initial backfill cursor for new tables")
		stream.initialVersion = version
	}

	return stream, nil
}

func parseVersionString(s string) (int64, error) {
	var version int64
	if _, err := fmt.Sscanf(s, "%d", &version); err != nil {
		return 0, fmt.Errorf("must be an integer: %w", err)
	}
	return version, nil
}

type sqlserverCTReplicationStream struct {
	db   *sqlserverDatabase
	conn *sql.DB
	cfg  *Config

	// Per-table version tracking
	tableVersions  map[sqlcapture.StreamID]int64
	initialVersion int64 // Version to use when initializing new tables (0 means use current version)

	// Per-table replication metadata
	tables map[sqlcapture.StreamID]*tableReplicationInfo
}

type tableReplicationInfo struct {
	Schema      string
	Table       string
	KeyColumns  []string
	Columns     map[string]sqlcapture.ColumnInfo
	ColumnTypes map[string]any
}

func (rs *sqlserverCTReplicationStream) open(ctx context.Context) error {
	// Verify Change Tracking is enabled
	var ctEnabled, err = isCTEnabled(ctx, rs.conn)
	if err != nil {
		return fmt.Errorf("error checking CT status: %w", err)
	}
	if !ctEnabled {
		return fmt.Errorf("Change Tracking is not enabled on this database")
	}

	rs.tables = make(map[sqlcapture.StreamID]*tableReplicationInfo)
	return nil
}

func (rs *sqlserverCTReplicationStream) ActivateTable(ctx context.Context, streamID sqlcapture.StreamID, keyColumns []string, discovery *sqlcapture.DiscoveryInfo, metadataJSON json.RawMessage) error {
	log.WithField("table", streamID).Trace("activate table")

	// Validate that configured key columns are available in Change Tracking.
	// CT only provides the actual database PK columns, so any override must use those.
	for _, keyCol := range keyColumns {
		if !slices.Contains(discovery.PrimaryKey, keyCol) {
			return fmt.Errorf("configured primary key column %q is not part of the table's actual primary key %v; Change Tracking only provides actual primary key columns", keyCol, discovery.PrimaryKey)
		}
	}

	// Build a map of column types from discovery information. Note that this
	// column list is fixed at activation time - if the table schema changes
	// (columns added or removed), those changes won't be reflected until the
	// capture task is restarted and re-discovers the table.
	var columnTypes = make(map[string]any)
	for columnName, columnInfo := range discovery.Columns {
		columnTypes[columnName] = columnInfo.DataType
	}

	rs.tables[streamID] = &tableReplicationInfo{
		Schema:      discovery.Schema,
		Table:       discovery.Name,
		KeyColumns:  keyColumns,
		Columns:     discovery.Columns,
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

func (rs *sqlserverCTReplicationStream) deactivateTable(streamID sqlcapture.StreamID) error {
	delete(rs.tables, streamID)
	delete(rs.tableVersions, streamID)
	return nil
}

func (rs *sqlserverCTReplicationStream) StartReplication(ctx context.Context, discovery map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo) error {
	// No background work needed - polling happens directly in StreamToFence
	return nil
}

func (rs *sqlserverCTReplicationStream) StreamToFence(ctx context.Context, fenceAfter time.Duration, callback func(event sqlcapture.DatabaseEvent) error) error {
	// Parse polling interval for sleep between polls
	pollingInterval, err := time.ParseDuration(rs.cfg.Advanced.PollingInterval)
	if err != nil {
		return fmt.Errorf("invalid polling interval %q: %w", rs.cfg.Advanced.PollingInterval, err)
	}

	// Timed streaming phase: poll repeatedly until fenceAfter deadline
	if fenceAfter > 0 {
		log.WithField("versions", rs.tableVersions).Debug("beginning timed streaming phase")
		var deadline = time.Now().Add(fenceAfter)
		for time.Now().Before(deadline) {
			if err := rs.pollChanges(ctx, callback); err != nil {
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

	if allAtFence {
		log.WithFields(log.Fields{"versions": rs.tableVersions, "fence": fenceVersion}).Debug("already at fence, emitting synthetic commit")
		var commitVersions = make(map[sqlcapture.StreamID]int64, len(rs.tableVersions))
		maps.Copy(commitVersions, rs.tableVersions)
		return callback(&sqlserverCTCommitEvent{Versions: commitVersions})
	}

	// One more poll to reach the fence. Since pollChanges queries the current version at
	// call time and polls all tables to that version, this will bring us to at least the
	// fence version.
	log.WithFields(log.Fields{"versions": rs.tableVersions, "fence": fenceVersion}).Debug("polling to reach fence")
	return rs.pollChanges(ctx, callback)
}

func (rs *sqlserverCTReplicationStream) Acknowledge(ctx context.Context, cursorJSON json.RawMessage) error {
	return nil
}

func (rs *sqlserverCTReplicationStream) Close(ctx context.Context) error {
	log.Debug("replication stream close requested")
	return nil
}

func (rs *sqlserverCTReplicationStream) pollChanges(ctx context.Context, callback func(event sqlcapture.DatabaseEvent) error) error {
	// Get the current CT version
	currentVersion, err := ctGetCurrentVersion(ctx, rs.conn)
	if err != nil {
		return err
	}

	// Poll each table sequentially
	for streamID, info := range rs.tables {
		fromVersion := rs.tableVersions[streamID]

		// Check if the table's version is still valid
		minValid, err := ctGetMinValidVersion(ctx, rs.conn, info.Schema, info.Table)
		if err != nil {
			// Errors checking the CT version are fatal rather than triggering a table
			// deactivation, because they're likely transient network/connection issues
			// rather than a real problem with the table.
			return fmt.Errorf("error getting min valid CT version for table %q: %w", streamID, err)
		}
		if fromVersion < minValid {
			log.WithFields(log.Fields{
				"stream":      streamID,
				"fromVersion": fromVersion,
				"minValid":    minValid,
			}).Warn("change tracking data has expired, will trigger backfill")
			var invalidCause = fmt.Sprintf("change tracking data expired for table %q", streamID)
			if err := callback(&sqlcapture.TableDropEvent{
				StreamID: streamID,
				Cause:    invalidCause,
			}); err != nil {
				return err
			}
			if err := rs.deactivateTable(streamID); err != nil {
				return err
			}
			continue
		}

		// Skip table if there are no new changes
		if fromVersion >= currentVersion {
			continue
		}

		// Poll this table
		var pollInfo = &ctTablePollInfo{
			StreamID:    streamID,
			Schema:      info.Schema,
			Table:       info.Table,
			KeyColumns:  info.KeyColumns,
			Columns:     info.Columns,
			ColumnTypes: info.ColumnTypes,
			FromVersion: fromVersion,
			ToVersion:   currentVersion,
		}
		if err := rs.pollTable(ctx, pollInfo, callback); err != nil {
			return fmt.Errorf("error polling changes for table %q: %w", streamID, err)
		}
	}

	// Update table versions and emit commit event
	newVersions := make(map[sqlcapture.StreamID]int64)
	for streamID := range rs.tables {
		if _, ok := rs.tableVersions[streamID]; ok {
			rs.tableVersions[streamID] = currentVersion
			newVersions[streamID] = currentVersion
		}
	}

	var event = &sqlserverCTCommitEvent{Versions: newVersions}
	log.WithField("event", event).Debug("checkpoint after polling")
	return callback(event)
}

type ctTablePollInfo struct {
	StreamID    sqlcapture.StreamID
	Schema      string
	Table       string
	KeyColumns  []string
	Columns     map[string]sqlcapture.ColumnInfo
	ColumnTypes map[string]any
	FromVersion int64
	ToVersion   int64
}

func (rs *sqlserverCTReplicationStream) pollTable(ctx context.Context, info *ctTablePollInfo, callback func(event sqlcapture.DatabaseEvent) error) error {
	// Build the list of columns to select
	var selectColumns []string
	var pkJoinConditions []string

	// First add the CT metadata columns
	selectColumns = append(selectColumns,
		"CT.SYS_CHANGE_VERSION",
		"CT.SYS_CHANGE_OPERATION",
	)

	// Add primary key columns from CT
	for _, pkCol := range info.KeyColumns {
		selectColumns = append(selectColumns, fmt.Sprintf("CT.[%s]", pkCol))
		pkJoinConditions = append(pkJoinConditions, fmt.Sprintf("CT.[%s] = T.[%s]", pkCol, pkCol))
	}

	// Add data columns from the source table (excluding PK columns already added)
	for colName := range info.Columns {
		if slices.Contains(info.KeyColumns, colName) {
			continue // Already added as PK column
		}
		selectColumns = append(selectColumns, fmt.Sprintf("T.[%s]", colName))
	}

	var query = fmt.Sprintf(`
		SELECT %s
		FROM CHANGETABLE(CHANGES [%s].[%s], @p1) AS CT
		LEFT OUTER JOIN [%s].[%s] AS T ON %s
		WHERE CT.SYS_CHANGE_VERSION > @p1 AND CT.SYS_CHANGE_VERSION <= @p2
		ORDER BY CT.SYS_CHANGE_VERSION`,
		strings.Join(selectColumns, ", "),
		info.Schema, info.Table,
		info.Schema, info.Table,
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

	// Build column name list and transcoders for the output shape
	var outputColumnNames = make([]string, 0, len(cnames))
	var outputColumnTypes = make([]any, 0, len(cnames))
	var outputTranscoders = make([]jsonTranscoder, 0, len(cnames))
	var rowKeyTranscoders = make([]fdbTranscoder, 0, len(cnames))
	var deleteNullability = make([]bool, 0, len(cnames))

	// Skip the first two columns (SYS_CHANGE_VERSION, SYS_CHANGE_OPERATION) from output
	for idx, name := range cnames {
		if idx < 2 {
			// CT metadata columns, skip from output
			outputColumnNames = append(outputColumnNames, "")
			outputColumnTypes = append(outputColumnTypes, nil)
			outputTranscoders = append(outputTranscoders, nil)
			rowKeyTranscoders = append(rowKeyTranscoders, nil)
			deleteNullability = append(deleteNullability, false)
			continue
		}

		outputColumnNames = append(outputColumnNames, name)
		outputColumnTypes = append(outputColumnTypes, info.ColumnTypes[name])
		outputTranscoders = append(outputTranscoders, rs.db.replicationJSONTranscoder(info.ColumnTypes[name]))

		if slices.Contains(info.KeyColumns, name) {
			rowKeyTranscoders = append(rowKeyTranscoders, rs.db.replicationFDBTranscoder(info.ColumnTypes[name]))
			deleteNullability = append(deleteNullability, true) // Key columns always have values from CT
		} else {
			rowKeyTranscoders = append(rowKeyTranscoders, nil)
			deleteNullability = append(deleteNullability, false) // Non-key columns are NULL for deletes (from LEFT OUTER JOIN)
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

	var replicationEventInfo = &sqlserverChangeSharedInfo{
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

		// Make a unique copy of the values slice
		var vals = make([]any, len(scanVals))
		copy(vals, scanVals)

		// Extract version and operation
		changeVersion, ok := vals[0].(int64)
		if !ok {
			return fmt.Errorf("invalid SYS_CHANGE_VERSION value: %v", vals[0])
		}

		var operation sqlcapture.ChangeOp
		opStr, ok := vals[1].(string)
		if !ok {
			return fmt.Errorf("invalid SYS_CHANGE_OPERATION value: %v", vals[1])
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
				rowKey, err = rowKeyTranscoders[idx].TranscodeFDB(rowKey, vals[idx])
				if err != nil {
					return fmt.Errorf("error encoding key column %q: %w", cnames[idx], err)
				}
			}
		}

		var event = &sqlserverChangeEvent{
			Shared: replicationEventInfo,
			Meta: sqlserverChangeMetadata{
				Operation: operation,
				Source: sqlserverCTSourceInfo{
					SourceCommon: sqlcapture.SourceCommon{
						Schema: info.Schema,
						Table:  info.Table,
					},
					Version: changeVersion,
					Tag:     rs.db.config.Advanced.SourceTag,
				},
			},
			RowKey: rowKey,
			Values: vals,
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

// ctGetMinValidVersion returns the minimum valid Change Tracking version for a table.
// Changes older than this version have been cleaned up and are no longer available.
func ctGetMinValidVersion(ctx context.Context, conn *sql.DB, schema, table string) (int64, error) {
	var version int64
	var query = `SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@p1));`
	var objectName = fmt.Sprintf("[%s].[%s]", schema, table)
	if err := conn.QueryRowContext(ctx, query, objectName).Scan(&version); err != nil {
		return 0, fmt.Errorf("error querying min valid CT version for %s.%s: %w", schema, table, err)
	}
	return version, nil
}
