package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sirupsen/logrus"
	"vitess.io/vitess/go/vt/sqlparser"
)

// replicationBufferSize controls how many change events can be buffered in the
// replicationStream before it stops receiving further events from MySQL.
// In normal use it's a constant, it's just a variable so that tests are more
// likely to exercise blocking sends and backpressure.
var replicationBufferSize = 1024

func (db *mysqlDatabase) StartReplication(ctx context.Context, startCursor string, activeTables map[string]bool, discovery map[string]sqlcapture.TableInfo, metadata map[string]json.RawMessage) (sqlcapture.ReplicationStream, error) {
	var parsedMetadata = make(map[string]*mysqlTableMetadata)
	for streamID, metadataJSON := range metadata {
		var parsed *mysqlTableMetadata
		if metadataJSON != nil {
			if err := json.Unmarshal(metadataJSON, &parsed); err != nil {
				return nil, fmt.Errorf("error parsing metadata JSON: %w", err)
			}
		}
		parsedMetadata[streamID] = parsed
	}

	var host, port, err = splitHostPort(db.config.Address)
	if err != nil {
		return nil, fmt.Errorf("invalid mysql address: %w", err)
	}
	var syncer = replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: uint32(db.config.Advanced.NodeID),
		Flavor:   "mysql", // TODO(wgd): See what happens if we change this and run against MariaDB?
		Host:     host,
		Port:     uint16(port),
		User:     db.config.User,
		Password: db.config.Password,
	})

	var pos mysql.Position
	if startCursor != "" {
		var binlogName, binlogPos, err = splitCursor(startCursor)
		if err != nil {
			return nil, fmt.Errorf("invalid resume cursor: %w", err)
		}
		pos.Name = binlogName
		pos.Pos = uint32(binlogPos)
	} else {
		// Get the initial binlog cursor from the source database's latest binlog position
		var results, err = db.conn.Execute("SHOW MASTER STATUS;")
		if err != nil {
			return nil, fmt.Errorf("error getting latest binlog position: %w", err)
		}
		if len(results.Values) == 0 {
			return nil, fmt.Errorf("failed to query latest binlog position (is binary logging enabled on %q?)", host)
		}
		var row = results.Values[0]
		pos.Name = string(row[0].AsString())
		pos.Pos = uint32(row[1].AsInt64())
		logrus.WithField("pos", pos).Debug("initialized binlog position")
	}

	logrus.WithFields(logrus.Fields{"pos": pos}).Info("starting replication")
	streamer, err := syncer.StartSync(pos)
	if err != nil {
		return nil, fmt.Errorf("error starting binlog sync: %w", err)
	}

	var streamCtx, streamCancel = context.WithCancel(ctx)
	var stream = &mysqlReplicationStream{
		syncer:       syncer,
		streamer:     streamer,
		events:       make(chan sqlcapture.ChangeEvent, replicationBufferSize),
		cancel:       streamCancel,
		errCh:        make(chan error),
		activeTables: activeTables,
		discovery:    discovery,
		metadata:     parsedMetadata,
	}
	go func() {
		var err = stream.run(streamCtx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		syncer.Close()
		close(stream.events)
		stream.errCh <- err
	}()

	return stream, nil
}

func splitCursor(cursor string) (string, int64, error) {
	seps := strings.Split(cursor, ":")
	if len(seps) != 2 {
		return "", 0, fmt.Errorf("input %q must have <logfile>:<position> shape", cursor)
	}
	position, err := strconv.ParseInt(seps[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid position value %q: %w", seps[1], err)
	}
	return seps[0], position, nil
}

func splitHostPort(addr string) (string, int64, error) {
	seps := strings.Split(addr, ":")
	if len(seps) != 2 {
		return "", 0, fmt.Errorf("input %q must have <host>:<port> shape", addr)
	}
	port, err := strconv.ParseInt(seps[1], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port number %q: %w", seps[1], err)
	}
	return seps[0], port, nil
}

type mysqlReplicationStream struct {
	syncer        *replication.BinlogSyncer
	streamer      *replication.BinlogStreamer
	events        chan sqlcapture.ChangeEvent
	cancel        context.CancelFunc
	errCh         chan error
	gtidTimestamp time.Time // The OriginalCommitTimestamp value of the last GTID Event

	// Mutex protecting the table activity and metadata maps, so
	// that they can be modified from the main thread but still
	// read by the replication thread.
	tablesMutex   sync.RWMutex
	activeTables  map[string]bool
	discovery     map[string]sqlcapture.TableInfo
	metadata      map[string]*mysqlTableMetadata
	dirtyMetadata []string
}

type mysqlTableMetadata struct {
	Schema mysqlTableSchema `json:"schema"`
}

type mysqlTableSchema struct {
	Columns     []string          `json:"columns"`
	ColumnTypes map[string]string `json:"types"`
}

func (rs *mysqlReplicationStream) run(ctx context.Context) error {
	// Initialize metadata (as necessary) for all active tables.
	for streamID, capture := range rs.activeTables {
		if !capture {
			continue
		} else if err := rs.ActivateTable(streamID); err != nil {
			return fmt.Errorf("error activating %q at replication start: %w", streamID, err)
		}
	}

	for {
		// Send "Metadata Change" events to the consumer where applicable.
		//
		// Note that the work is divided in two here so that the mutex-acquiring
		// part cannot block and the blocking send doesn't hold the mutex. This
		// helps avoid a (very unlikely) deadlock with the main thread calling
		// ActivateTable() while the events buffer is full.
		var metadataEvents []sqlcapture.ChangeEvent
		rs.tablesMutex.RLock()
		if rs.dirtyMetadata != nil {
			for _, streamID := range rs.dirtyMetadata {
				var bs, err = json.Marshal(rs.metadata[streamID])
				if err != nil {
					return fmt.Errorf("error serializing metadata JSON for %q: %w", streamID, err)
				}
				metadataEvents = append(metadataEvents, sqlcapture.ChangeEvent{
					Operation: sqlcapture.MetadataOp,
					Metadata: &sqlcapture.MetadataChangeEvent{
						StreamID: streamID,
						Metadata: json.RawMessage(bs),
					},
				})
			}
			rs.dirtyMetadata = nil
		}
		rs.tablesMutex.RUnlock()
		for _, metadataEvent := range metadataEvents {
			rs.events <- metadataEvent
		}

		// Process the next binlog event from the database.
		var event, err = rs.streamer.GetEvent(ctx)
		if err != nil {
			return fmt.Errorf("error getting next event: %w", err)
		}

		switch data := event.Event.(type) {
		case *replication.RowsEvent:
			var schema, table = string(data.Table.Schema), string(data.Table.Table)
			var streamID = sqlcapture.JoinStreamID(schema, table)
			// Skip change events from tables which aren't being captured
			if !rs.tableActive(streamID) {
				continue
			}

			var sourceMeta = &mysqlSourceInfo{
				SourceCommon: sqlcapture.SourceCommon{
					Millis: rs.gtidTimestamp.UnixMilli(),
					Schema: schema,
					Table:  table,
				},
			}

			// Get column names and types from persistent metadata. If available, allow
			// override the persistent column name tracking using binlog row metadata.
			var metadata, ok = rs.tableMetadata(streamID)
			if !ok {
				return fmt.Errorf("missing metadata for stream %q", streamID)
			}
			var columnTypes = metadata.Schema.ColumnTypes
			var columnNames = data.Table.ColumnNameString()
			if len(columnNames) == 0 {
				columnNames = metadata.Schema.Columns
			}
			if len(columnNames) == 0 {
				return fmt.Errorf("unknown column names for stream %q (go.estuary.dev/eiKbOh)", streamID)
			}

			switch event.Header.EventType {
			case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				for _, row := range data.Rows {
					var after = decodeRow(streamID, columnNames, row)
					if err := translateRecordFields(columnTypes, after); err != nil {
						return fmt.Errorf("error translating 'after' of %q InsertOp: %w", streamID, err)
					}
					rs.events <- sqlcapture.ChangeEvent{
						Operation: sqlcapture.InsertOp,
						Source:    sourceMeta,
						After:     after,
					}
				}
			case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				for rowIdx := range data.Rows {
					// Update events contain alternating (before, after) pairs of rows
					if rowIdx%2 == 1 {
						var before = decodeRow(streamID, columnNames, data.Rows[rowIdx-1])
						var after = decodeRow(streamID, columnNames, data.Rows[rowIdx])
						if err := translateRecordFields(columnTypes, before); err != nil {
							return fmt.Errorf("error translating 'before' of %q UpdateOp: %w", streamID, err)
						}
						if err := translateRecordFields(columnTypes, after); err != nil {
							return fmt.Errorf("error translating 'after' of %q UpdateOp: %w", streamID, err)
						}
						rs.events <- sqlcapture.ChangeEvent{
							Operation: sqlcapture.UpdateOp,
							Source:    sourceMeta,
							Before:    before,
							After:     after,
						}
					}
				}
			case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				for _, row := range data.Rows {
					var before = decodeRow(streamID, columnNames, row)
					if err := translateRecordFields(columnTypes, before); err != nil {
						return fmt.Errorf("error translating 'before' of %q DeleteOp: %w", streamID, err)
					}
					rs.events <- sqlcapture.ChangeEvent{
						Operation: sqlcapture.DeleteOp,
						Source:    sourceMeta,
						Before:    before,
					}
				}
			default:
				return fmt.Errorf("unknown row event type: %q", event.Header.EventType)
			}
		case *replication.XIDEvent:
			var cursor = rs.syncer.GetNextPosition()
			logrus.WithFields(logrus.Fields{
				"xid":    data.XID,
				"cursor": cursor,
			}).Trace("XID Event")
			rs.events <- sqlcapture.ChangeEvent{
				Operation: sqlcapture.FlushOp,
				Source: &mysqlSourceInfo{
					FlushCursor: fmt.Sprintf("%s:%d", cursor.Name, cursor.Pos),
				},
			}
		case *replication.TableMapEvent:
			logrus.WithField("data", data).Trace("Table Map Event")
		case *replication.GTIDEvent:
			logrus.WithField("data", data).Trace("GTID Event")
			rs.gtidTimestamp = data.OriginalCommitTime()
		case *replication.PreviousGTIDsEvent:
			logrus.WithField("gtids", data.GTIDSets).Trace("PreviousGTIDs Event")
		case *replication.QueryEvent:
			if err := rs.handleQuery(string(data.Schema), string(data.Query)); err != nil {
				return fmt.Errorf("error processing query event: %w", err)
			}
		case *replication.RotateEvent:
			logrus.WithField("data", data).Trace("Rotate Event")
		case *replication.FormatDescriptionEvent:
			logrus.WithField("data", data).Trace("Format Description Event")
		default:
			return fmt.Errorf("unhandled event type: %q", event.Header.EventType)
		}
	}
}

func decodeRow(streamID string, colNames []string, row []interface{}) map[string]interface{} {
	var fields = make(map[string]interface{})
	for idx, val := range row {
		// If change events contain more values than we have column names,
		// something may have gone wrong. However, that "something" could
		// be benign, for instance during catchup streaming when a stream
		// is newly added and a column was deleted since the last binlog
		// offset checkpoint.
		if idx >= len(colNames) {
			logrus.WithFields(logrus.Fields{
				"stream":   streamID,
				"expected": len(colNames),
				"actual":   len(row),
			}).Warn("row change event contains more values than expected (go.estuary.dev/sCSfKS)")
		} else {
			fields[colNames[idx]] = val
		}
	}
	return fields
}

func (rs *mysqlReplicationStream) handleQuery(schema, query string) error {
	// There are basically three types of query events we might receive:
	//   * An INSERT/UPDATE/DELETE query is an error, we should never receive
	//     these if the server's `binlog_format` is set to ROW as it should be
	//     for CDC to work properly.
	//   * Various DDL queries like CREATE/ALTER/DROP/TRUNCATE/RENAME TABLE,
	//     which should in general be treated like errors *if they occur on
	//     a table we're capturing*, though we expect to eventually handle
	//     some subset of possible alterations like adding/renaming columns.
	//   * Some other queries like BEGIN and CREATE DATABASE and other things
	//     that we don't care about, either because they change things that
	//     don't impact our capture or because we get the relevant information
	//     by some other means.
	logrus.WithField("query", query).Debug("handling query event")

	var stmt, err = sqlparser.ParseStrictDDL(query)
	if err != nil {
		return fmt.Errorf("error parsing query: %w", err)
	}
	logrus.WithField("stmt", fmt.Sprintf("%#v", stmt)).Debug("parsed query")

	switch stmt := stmt.(type) {
	case *sqlparser.Begin:
	case *sqlparser.CreateDatabase:
	case *sqlparser.CreateTable:
		logrus.WithField("query", query).Trace("ignoring benign query")
	case *sqlparser.AlterTable:
		if streamID := resolveTableName(schema, stmt.Table); rs.tableActive(streamID) {
			return fmt.Errorf("unsupported ALTER TABLE operation on %q (go.estuary.dev/eVVwet)", streamID)
		}
	case *sqlparser.DropTable:
		for _, table := range stmt.FromTables {
			if streamID := resolveTableName(schema, table); rs.tableActive(streamID) {
				return fmt.Errorf("unsupported DROP TABLE operation on %q (go.estuary.dev/eVVwet)", streamID)
			}
		}
	case *sqlparser.TruncateTable:
		if streamID := resolveTableName(schema, stmt.Table); rs.tableActive(streamID) {
			return fmt.Errorf("unsupported TRUNCATE TABLE operation on %q (go.estuary.dev/eVVwet)", streamID)
		}
	case *sqlparser.RenameTable:
		for _, pair := range stmt.TablePairs {
			if streamID := resolveTableName(schema, pair.FromTable); rs.tableActive(streamID) {
				return fmt.Errorf("unsupported RENAME TABLE operation on %q (go.estuary.dev/eVVwet)", streamID)
			}
			if streamID := resolveTableName(schema, pair.ToTable); rs.tableActive(streamID) {
				return fmt.Errorf("unsupported RENAME TABLE operation on %q (go.estuary.dev/eVVwet)", streamID)
			}
		}
	default:
		return fmt.Errorf("unsupported query %q (go.estuary.dev/eVVwet)", query)
	}

	return nil
}

func resolveTableName(defaultSchema string, name sqlparser.TableName) string {
	var schema, table = name.Qualifier.String(), name.Name.String()
	if schema == "" {
		schema = defaultSchema
	}
	return sqlcapture.JoinStreamID(schema, table)
}

func (rs *mysqlReplicationStream) tableMetadata(streamID string) (*mysqlTableMetadata, bool) {
	rs.tablesMutex.RLock()
	defer rs.tablesMutex.RUnlock()
	var meta, ok = rs.metadata[streamID]
	return meta, ok
}

func (rs *mysqlReplicationStream) tableActive(streamID string) bool {
	rs.tablesMutex.RLock()
	defer rs.tablesMutex.RUnlock()
	return rs.activeTables[streamID]
}

func (rs *mysqlReplicationStream) ActivateTable(streamID string) error {
	rs.tablesMutex.Lock()
	defer rs.tablesMutex.Unlock()

	// Mark the table active. This is redundant for `ActivateTable()` calls
	// during replication stream startup, but doesn't hurt anything and helps
	// keep the control flow simpler.
	rs.activeTables[streamID] = true

	// Do nothing if metadata is already initialized for this stream.
	if metadata := rs.metadata[streamID]; metadata != nil {
		return nil
	}

	// Otherwise construct new metadata based on discovery info.
	logrus.WithField("stream", streamID).Debug("initializing table metadata")
	var metadata = new(mysqlTableMetadata)
	if discovery, ok := rs.discovery[streamID]; ok {
		var colTypes = make(map[string]string)
		for colName, colInfo := range discovery.Columns {
			colTypes[colName] = colInfo.DataType
		}

		metadata.Schema.Columns = discovery.ColumnNames
		metadata.Schema.ColumnTypes = colTypes

		logrus.WithFields(logrus.Fields{
			"stream":  streamID,
			"columns": metadata.Schema.Columns,
			"types":   metadata.Schema.ColumnTypes,
		}).Debug("initialized table metadata")
	}
	rs.metadata[streamID] = metadata
	rs.dirtyMetadata = append(rs.dirtyMetadata, streamID)
	return nil
}

func (rs *mysqlReplicationStream) Events() <-chan sqlcapture.ChangeEvent {
	return rs.events
}

func (rs *mysqlReplicationStream) Acknowledge(ctx context.Context, cursor string) error {
	// No acknowledgements are necessary or possible in MySQL. The binlog is just
	// a series of logfiles on disk which get erased after log rotation according
	// to a time-based retention policy, without any server-side "have all clients
	// consumed these events" tracking.
	//
	// See also: The 'Binlog Retention Sanity Check' logic in source-mysql/main.go
	return nil
}

func (rs *mysqlReplicationStream) Close(ctx context.Context) error {
	rs.cancel()
	return <-rs.errCh
}
