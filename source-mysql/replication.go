package main

import (
	"context"
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

func (db *mysqlDatabase) StartReplication(ctx context.Context, startCursor string) (sqlcapture.ReplicationStream, error) {
	var host, port, err = splitHostPort(db.config.Address)
	if err != nil {
		return nil, fmt.Errorf("invalid mysql address: %w", err)
	}
	var syncer = replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID: uint32(db.config.ServerID),
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
		logrus.WithField("pos", pos).Info("initialized binlog position")
	}

	streamer, err := syncer.StartSync(pos)
	if err != nil {
		return nil, fmt.Errorf("error starting binlog sync: %w", err)
	}

	var streamCtx, streamCancel = context.WithCancel(ctx)
	var stream = &mysqlReplicationStream{
		syncer:   syncer,
		streamer: streamer,
		events:   make(chan sqlcapture.ChangeEvent, replicationBufferSize),
		cancel:   streamCancel,
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
	events        chan sqlcapture.ChangeEvent
	cancel        context.CancelFunc
	gtidTimestamp time.Time // The OriginalCommitTimestamp value of the last GTID Event
}

func (rs *mysqlReplicationStream) run(ctx context.Context) error {
	for {
		var event, err = rs.streamer.GetEvent(ctx)
		if err != nil {
			return fmt.Errorf("error getting next event: %w", err)
		}

		switch data := event.Event.(type) {
		case *replication.RowsEvent:
			var schema, table = string(data.Table.Schema), string(data.Table.Table)
			if schema == "mysql" {
				continue
			}

			var sourceMeta = &mysqlSourceInfo{
				SourceCommon: sqlcapture.SourceCommon{
					Millis: rs.gtidTimestamp.UnixMilli(),
					Schema: schema,
					Table:  table,
				},
			}

			var columnNames = data.Table.ColumnNameString()
			if len(columnNames) == 0 {
				return fmt.Errorf("binlog doesn't include column names (are you running with '--binlog-row-metadata=FULL'?)")
			}

			switch event.Header.EventType {
			case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				for _, row := range data.Rows {
					var after = decodeRow(columnNames, row)
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
