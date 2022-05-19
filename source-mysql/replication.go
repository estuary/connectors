package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sirupsen/logrus"
)

// replicationBufferSize controls how many change events can be buffered in the
// replicationStream before it stops receiving further events from MySQL.
// In normal use it's a constant, it's just a variable so that tests are more
// likely to exercise blocking sends and backpressure.
var replicationBufferSize = 1024

func (db *mysqlDatabase) StartReplication(ctx context.Context, startCursor string, captureTables map[string]bool, discovery map[string]sqlcapture.TableInfo, metadata map[string]json.RawMessage) (sqlcapture.ReplicationStream, error) {
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
		syncer:        syncer,
		streamer:      streamer,
		captureTables: captureTables,
		discovery:     discovery,
		metadata:      parsedMetadata,
		events:        make(chan sqlcapture.ChangeEvent, replicationBufferSize),
		cancel:        streamCancel,
	}
	go func() {
		defer close(stream.events)
		defer syncer.Close()
		if err := stream.run(streamCtx); err != nil && !errors.Is(err, context.Canceled) {
			logrus.WithField("err", err).Fatal("replication stream error")
		}
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
	captureTables map[string]bool
	discovery     map[string]sqlcapture.TableInfo
	metadata      map[string]*mysqlTableMetadata
	events        chan sqlcapture.ChangeEvent
	cancel        context.CancelFunc
	gtidTimestamp time.Time // The OriginalCommitTimestamp value of the last GTID Event
}

type mysqlTableMetadata struct {
	Schema mysqlTableSchema `json:"schema"`
}

type mysqlTableSchema struct {
	Columns     []string          `json:"columns"`
	ColumnTypes map[string]string `json:"types"`
}

func (rs *mysqlReplicationStream) run(ctx context.Context) error {
	// Initialize metadata for any tables that don't already have some.
	for streamID, capture := range rs.captureTables {
		if !capture {
			continue
		}
		if metadata := rs.metadata[streamID]; metadata != nil {
			continue
		}

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
		// TODO(wgd): Emit some sort of 'metadata change' to rs.events right here
		rs.metadata[streamID] = metadata
	}

	for {
		var event, err = rs.streamer.GetEvent(ctx)
		if err != nil {
			return fmt.Errorf("error getting next event: %w", err)
		}

		switch data := event.Event.(type) {
		case *replication.RowsEvent:
			var schema, table = string(data.Table.Schema), string(data.Table.Table)
			var streamID = sqlcapture.JoinStreamID(schema, table)
			// Skip change events from tables which aren't being captured
			if !rs.captureTables[streamID] {
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
			var metadata, ok = rs.metadata[streamID]
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
					var after = decodeRow(columnNames, row)
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
						var before = decodeRow(columnNames, data.Rows[rowIdx-1])
						var after = decodeRow(columnNames, data.Rows[rowIdx])
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
					var before = decodeRow(columnNames, row)
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
			// Receiving a query event isn't _necessarily_ a problem, but it could be an indication
			// that the server's `binlog_format` is not set correctly. Even when it's correctly set
			// to `ROW`, Query events will still be sent for DDL statements, so we don't want to
			// return an error here, but we do want these logs to be fairly visible.
			logrus.WithFields(logrus.Fields{
				"event": data,
				"query": string(data.Query),
			}).Info("Query Event")
		case *replication.RotateEvent:
			logrus.WithField("data", data).Trace("Rotate Event")
		case *replication.FormatDescriptionEvent:
			logrus.WithField("data", data).Trace("Format Description Event")
		default:
			return fmt.Errorf("unhandled event type: %q", event.Header.EventType)
		}
	}
}

func decodeRow(colNames []string, row []interface{}) map[string]interface{} {
	var fields = make(map[string]interface{})
	for idx, val := range row {
		// TODO(wgd): Do something to handle the edge case where a
		// column is removed from a table which is then added to the
		// capture, so discovery names fewer columns than actually
		// exist in the row change events during catchup streaming.
		var name = colNames[idx]
		if bs, ok := val.([]byte); ok {
			val = string(bs)
		}
		fields[name] = val
	}
	return fields
}

func (rs *mysqlReplicationStream) Events() <-chan sqlcapture.ChangeEvent {
	return rs.events
}

func (rs *mysqlReplicationStream) Acknowledge(ctx context.Context, cursor string) error {
	// TODO(wgd): Figure out how MySQL advances whatever WAL-reserving cursor it has.
	// I assume it's associated with the Server ID stuff, but I'm not sure if we're
	// expected to explicitly advance it?
	return nil
}

func (rs *mysqlReplicationStream) Close(ctx context.Context) error {
	rs.cancel()
	return nil
}
