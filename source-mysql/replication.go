package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"github.com/segmentio/encoding/json"
	"github.com/sirupsen/logrus"
	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	// replicationBufferSize controls how many change events can be buffered in the
	// replicationStream before it stops receiving further events from MySQL.
	// In normal use it's a constant, it's just a variable so that tests are more
	// likely to exercise blocking sends and backpressure.
	replicationBufferSize = 256

	// binlogEventCacheCount controls how many binlog events will be buffered inside
	// the client library before we receive them.
	binlogEventCacheCount = 256

	// streamToFenceWatchdogTimeout is the length of time after which a stream-to-fence
	// operation will error out if no further events are received when there ought to be
	// some. This should never be hit in normal operation, and exists only so that certain
	// rare failure modes produce an error rather than blocking forever.
	streamToFenceWatchdogTimeout = 5 * time.Minute
)

// InconsistentMetadataError represents a mismatch between expected and actual
// column metadata. The custom error type allows us to respond by backfilling
// the table automatically.
type InconsistentMetadataError struct {
	StreamID sqlcapture.StreamID
	Message  string
}

func (e *InconsistentMetadataError) Error() string {
	return e.Message
}

func (db *mysqlDatabase) ReplicationStream(ctx context.Context, startCursorJSON json.RawMessage) (sqlcapture.ReplicationStream, error) {
	var address = db.config.Address
	// If SSH Tunnel is configured, we are going to create a tunnel from localhost:5432
	// to address through the bastion server, so we use the tunnel's address
	if db.config.NetworkTunnel != nil && db.config.NetworkTunnel.SSHForwarding != nil && db.config.NetworkTunnel.SSHForwarding.SSHEndpoint != "" {
		address = "localhost:3306"
	}
	var host, port, err = splitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("invalid mysql address: %w", err)
	}

	// Decode start cursor from a JSON quoted string into its actual string contents
	startCursor, err := unmarshalJSONString(startCursorJSON)
	if err != nil {
		return nil, fmt.Errorf("invalid start cursor JSON: %w", err)
	}

	// If we have no resume cursor but we do have an initial backfill cursor, use that as the start position.
	if startCursor == "" && db.initialBackfillCursor != "" {
		logrus.WithField("cursor", db.initialBackfillCursor).Info("using initial backfill cursor as start position")
		startCursor = db.initialBackfillCursor
	}

	// If the `force_reset_cursor=XYZ` hackery flag is set, use that as the start position regardless of anything else.
	if db.forceResetCursor != "" {
		logrus.WithField("cursor", db.forceResetCursor).Info("forcibly modified resume cursor")
		startCursor = db.forceResetCursor
	}

	var pos mysql.Position
	if startCursor != "" {
		pos, err = parseCursor(startCursor)
		if err != nil {
			return nil, fmt.Errorf("invalid resume cursor: %w", err)
		}
		logrus.WithField("pos", pos).Debug("resuming from binlog position")
	} else {
		pos, err = db.queryBinlogPosition()
		if err != nil {
			return nil, err
		}
		logrus.WithField("pos", pos).Debug("initialized binlog position")
	}

	var syncConfig = replication.BinlogSyncerConfig{
		ServerID: uint32(db.config.Advanced.NodeID),
		Flavor:   "mysql", // TODO(wgd): See what happens if we change this and run against MariaDB?
		Host:     host,
		Port:     uint16(port),
		User:     db.config.User,
		Password: db.config.Password,
		// TODO(wgd): Maybe add 'serverName' checking as described over in Connect()
		TLSConfig: &tls.Config{InsecureSkipVerify: true},
		// Request that timestamp values coming via replication be interpreted as UTC.
		TimestampStringLocation: time.UTC,

		// Limit connection retries so unreachability eventually bubbles up into task failure.
		MaxReconnectAttempts: 10,

		// Request heartbeat events from the server every 30s and time out waiting after 5m.
		// The heartbeats ensure that the TCP connection remains alive when no changes are
		// occurring and the read timeout ensures that we will detect silent link failures
		// and fail+restart the capture within a few minutes at worst.
		HeartbeatPeriod: 30 * time.Second,
		ReadTimeout:     5 * time.Minute,

		// Output replication log messages with Logrus the same as our own connector messages.
		Logger: logrus.StandardLogger(),

		// Allow the binlog syncer to buffer a few events internally for speed, but not too many.
		EventCacheCount: binlogEventCacheCount,
	}

	logrus.WithFields(logrus.Fields{"pos": pos}).Info("starting replication")
	var streamer *replication.BinlogStreamer
	var syncer = replication.NewBinlogSyncer(syncConfig)
	if streamer, err = syncer.StartSync(pos); err == nil {
		logrus.Debug("replication connected with TLS")
	} else {
		syncer.Close()
		syncConfig.TLSConfig = nil
		syncer = replication.NewBinlogSyncer(syncConfig)
		if streamer, err = syncer.StartSync(pos); err == nil {
			logrus.Info("replication connected without TLS")
		} else {
			return nil, fmt.Errorf("error starting binlog sync: %w", err)
		}
	}

	var stream = &mysqlReplicationStream{
		db:            db,
		syncer:        syncer,
		streamer:      streamer,
		startPosition: pos,
		fencePosition: pos,
	}
	stream.tables.active = make(map[sqlcapture.StreamID]struct{})
	stream.tables.discovery = make(map[sqlcapture.StreamID]sqlcapture.DiscoveryInfo)
	stream.tables.metadata = make(map[sqlcapture.StreamID]*mysqlTableMetadata)
	stream.tables.keyColumns = make(map[sqlcapture.StreamID][]string)
	stream.tables.nonTransactional = make(map[sqlcapture.StreamID]bool)
	return stream, nil
}

func parseCursor(cursor string) (mysql.Position, error) {
	seps := strings.Split(cursor, ":")
	if len(seps) != 2 {
		return mysql.Position{}, fmt.Errorf("input %q must have <logfile>:<offset> shape", cursor)
	}
	offset, err := strconv.ParseInt(seps[1], 10, 64)
	if err != nil {
		return mysql.Position{}, fmt.Errorf("invalid offset value %q: %w", seps[1], err)
	}
	return mysql.Position{
		Name: seps[0],
		Pos:  uint32(offset),
	}, nil
}

func unmarshalJSONString(bs json.RawMessage) (string, error) {
	if bs == nil {
		return "", nil
	}
	var str string
	if err := json.Unmarshal(bs, &str); err != nil {
		return "", err
	}
	return str, nil
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
	db            *mysqlDatabase
	syncer        *replication.BinlogSyncer
	streamer      *replication.BinlogStreamer
	startPosition mysql.Position                // The start position when the stream was created. Never updated.
	fencePosition mysql.Position                // The latest fence position, updated at the end of each StreamToFence cycle.
	events        chan sqlcapture.DatabaseEvent // Output channel from replication worker goroutine

	cancel context.CancelFunc // Cancellation thunk for the replication worker goroutine
	errCh  chan error         // Error output channel for the replication worker goroutine

	gtidTimestamp time.Time // The OriginalCommitTimestamp value of the last GTID Event
	gtidString    string    // The GTID value of the last GTID event, formatted as a "<uuid>:<counter>" string.

	uncommittedChanges      bool // True when there have been row changes since the last commit was processed.
	nonTransactionalChanges bool // True when there have been row changes *on non-transactional tables* since the last commit was processed.

	// The active tables set and associated metadata, guarded by a
	// mutex so it can be modified from the main goroutine while it's
	// read from the replication goroutine.
	tables struct {
		sync.RWMutex
		active           map[sqlcapture.StreamID]struct{}
		discovery        map[sqlcapture.StreamID]sqlcapture.DiscoveryInfo
		metadata         map[sqlcapture.StreamID]*mysqlTableMetadata
		keyColumns       map[sqlcapture.StreamID][]string
		nonTransactional map[sqlcapture.StreamID]bool
		dirtyMetadata    []sqlcapture.StreamID
	}
}

type mysqlTableMetadata struct {
	Schema         mysqlTableSchema `json:"schema"`
	DefaultCharset string           `json:"charset,omitempty"`
}

type mysqlTableSchema struct {
	Columns     []string               `json:"columns"`
	ColumnTypes map[string]interface{} `json:"types"`
}

func (rs *mysqlReplicationStream) StartReplication(ctx context.Context, _ map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo) error {
	if rs.cancel != nil {
		return fmt.Errorf("internal error: replication stream already started")
	}

	var streamCtx, streamCancel = context.WithCancel(ctx)
	rs.events = make(chan sqlcapture.DatabaseEvent, replicationBufferSize)
	rs.errCh = make(chan error)
	rs.cancel = streamCancel

	go func() {
		var err = rs.run(streamCtx, rs.startPosition)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		rs.syncer.Close()
		close(rs.events)
		rs.errCh <- err
	}()
	return nil
}

func (rs *mysqlReplicationStream) run(ctx context.Context, startCursor mysql.Position) error {
	var cursor = startCursor

	// The MySQL binlog protocol and event framing represent file offsets as 32-bit unsigned
	// integers in several places. This is normally fine, as there's a soft limit of 1GB on
	// binlog file sizes. Unfortunately it's a soft limit and it's possible to force MySQL
	// to store an arbitrarily large amount of data in a single file.
	//
	// Thus file offsets greater than 2^32 can become a bit...difficult to work with. There
	// are two places where we handle binlog offsets:
	//   - A resume cursor (file+offset) is included with every state checkpoint emitted.
	//   - A cursor value is embedded in the source metadata field of every change event,
	//     and this value is used as the fallback collection key for keyless tables so we
	//     need it to be, if not correct, then at least unique and monotonically increasing
	//     within a single file.
	//
	// Since the COM_BINLOG_DUMP command only takes a 4-byte offset field it isn't possible
	// to resume replication from an offset >4GB after a restart, so we shouldn't emit any
	// checkpoints after that offset is reached in a particular binlog file.
	//
	// I trust our ability to detect that overflow has occurred more than I trust the
	// correctness of our extended-binlog-offset tracking code, so the two purposes are
	// separated as much as possible here.
	var binlogOffsetOverflow bool
	var binlogEstimatedOffset = uint64(cursor.Pos)

	// The vitess SQL parser can be initialized with a few options that aren't relevant to
	// us. Somewhat notably, the MySQLServerVersion is used when parsing version-specific
	// MySQL comments in the form of /*!50708 sql here */. If the MySqlServerVersion is
	// less than the version in that comment, the special comment is not parsed. If
	// nothing is set for MySQLServerVersion, the default is 8.0.40. Also Note that in a
	// previous version of the vitess SQL parser that we used the default was 5.07.09, but
	// I don't think this should have any practical implications on our usage of the
	// parser.
	parser, err := sqlparser.New(sqlparser.Options{})
	if err != nil {
		return fmt.Errorf("creating SQL parser: %w", err)
	}

	for {
		// Process the next binlog event from the database.
		var event, err = rs.streamer.GetEvent(ctx)
		if err != nil {
			return fmt.Errorf("error getting next event: %w", err)
		}

		if event.Header.LogPos > 0 {
			// Since no single binlog event can exceed 1GB (and this unlike the binlog file
			// size is an actual hard limit), overflow can be detected when we first see an
			// event wrap around such that the log_pos header field is smaller than the last
			if event.Header.LogPos < cursor.Pos && !binlogOffsetOverflow {
				logrus.WithFields(logrus.Fields{
					"cursor": cursor.String(),
					"logpos": event.Header.LogPos,
				}).Warn("binlog offset overflow detected")
				binlogOffsetOverflow = true
			}

			// So long as the binlog hasn't exceeded 4GB then our estimated offset used in
			// change event metadata should just be the literal position coming from the
			// binlog. After it overflows just sum up all the event sizes after that point,
			// which is "correct enough" for the document cursor even if I wouldn't trust
			// it for replication resumption. But as previously noted, we can't resume from
			// a point >4GB anyway so that's a moot point.
			if !binlogOffsetOverflow {
				binlogEstimatedOffset = uint64(event.Header.LogPos)

				// We only update the cursor when the offset hasn't overflowed, so that the
				// cursor value is always a valid resume point.
				cursor.Pos = event.Header.LogPos
			} else {
				binlogEstimatedOffset += uint64(event.Header.EventSize)
			}
		}

		// Events which are neither row changes nor commits need to be reported as an
		// implicit FlushEvent if and only if there are no uncommitted changes. This
		// avoids certain edge cases in the positional fence implementation.
		var implicitFlush = false

		switch data := event.Event.(type) {
		case *replication.RowsEvent:
			var eventCursor = mysqlChangeEventCursor{
				BinlogFile:   cursor.Name,
				BinlogOffset: binlogEstimatedOffset,
			}
			if err := rs.handleRowsEvent(ctx, event, eventCursor); err != nil {
				var metadataErr = &InconsistentMetadataError{}
				if errors.As(err, &metadataErr) {
					logrus.WithField("stream", metadataErr.StreamID).Warn("detected inconsistent metadata for stream, will backfill")
					if err := rs.emitEvent(ctx, &sqlcapture.TableDropEvent{
						StreamID: metadataErr.StreamID,
						Cause:    metadataErr.Message,
					}); err != nil {
						return fmt.Errorf("error emitting TableDropEvent(%q) after inconsistent metadata: %w", metadataErr.StreamID, err)
					} else if err := rs.deactivateTable(metadataErr.StreamID); err != nil {
						return fmt.Errorf("error deactivating table %q after inconsistent metadata: %w", metadataErr.StreamID, err)
					}
					// not returning, we just converted the error into a successful TableDropEvent
				} else {
					return err
				}
			}
		case *replication.XIDEvent:
			logrus.WithFields(logrus.Fields{
				"xid":         data.XID,
				"cursor":      cursor,
				"real_offset": binlogEstimatedOffset,
			}).Trace("XID Event")
			// There are two ways we might handle a binlog offset overflow:
			//
			//   1. Continue emitting checkpoints, but never advance the cursor. This would
			//      result in duplicate committed Flow changes.
			//   2. Stop emitting checkpoints entirely. This results in exactly-once capture
			//      of the replication events even when the offset overflows, but at the cost
			//      of treating the whole >4GB portion of the oversize binlog file as a single
			//      transaction.
			//
			// We have opted to go with option (2) here. Option (1) would be implemented by not
			// having this condition here at all and simply continuing to emit checkpoints. We
			// also stop advancing the cursor position during offset overflow, so that part is
			// handled elsewhere already.
			if !binlogOffsetOverflow {
				if err := rs.emitEvent(ctx, &mysqlCommitEvent{Position: cursor}); err != nil {
					return err
				}
				rs.uncommittedChanges = false      // Reset uncommitted changes
				rs.nonTransactionalChanges = false // Reset uncommitted non-transactional changes
			}
		case *replication.TableMapEvent:
			logrus.WithField("data", data).Trace("Table Map Event")
		case *replication.GTIDEvent:
			implicitFlush = true // Implicit FlushEvent conversion permitted
			logrus.WithField("data", data).Trace("GTID Event")
			rs.gtidTimestamp = data.OriginalCommitTime()

			if len(data.SID) != 16 || (data.GNO == 0 && bytes.Equal(data.SID, []byte("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))) {
				rs.gtidString = ""
			} else {
				var sourceUUID, err = uuid.FromBytes(data.SID)
				if err != nil {
					return fmt.Errorf("internal error: failed to parse GTID source %q: %w", data.SID, err)
				}
				rs.gtidString = fmt.Sprintf("%s:%d", sourceUUID.String(), data.GNO)
			}
		case *replication.PreviousGTIDsEvent:
			implicitFlush = true // Implicit FlushEvent conversion permitted
			logrus.WithField("gtids", data.GTIDSets).Trace("PreviousGTIDs Event")
		case *replication.QueryEvent:
			if string(data.Query) == "COMMIT" && rs.nonTransactionalChanges {
				// If there are uncommitted non-transactional changes, we should treat a
				// "COMMIT" query event as a transaction commit. This logic should match
				// the XIDEvent handling.
				if !binlogOffsetOverflow {
					if err := rs.emitEvent(ctx, &mysqlCommitEvent{Position: cursor}); err != nil {
						return err
					}
					rs.uncommittedChanges = false      // Reset all uncommitted changes
					rs.nonTransactionalChanges = false // Reset uncommitted non-transactional changes
				}
			} else {
				implicitFlush = true // Implicit FlushEvent conversion permitted
				if err := rs.handleQuery(ctx, parser, string(data.Schema), string(data.Query)); err != nil {
					return fmt.Errorf("error processing query event: %w", err)
				}
			}
		case *replication.RotateEvent:
			implicitFlush = true // Implicit FlushEvent conversion permitted
			cursor.Name = string(data.NextLogName)
			cursor.Pos = uint32(data.Position)
			binlogOffsetOverflow = false
			binlogEstimatedOffset = uint64(data.Position)
			logrus.WithFields(logrus.Fields{
				"name": cursor.Name,
				"pos":  cursor.Pos,
			}).Trace("Rotate Event")
		case *replication.FormatDescriptionEvent:
			implicitFlush = true // Implicit FlushEvent conversion permitted
			logrus.WithField("data", data).Trace("Format Description Event")
		case *replication.GenericEvent:
			implicitFlush = true // Implicit FlushEvent conversion permitted
			if event.Header.EventType == replication.HEARTBEAT_EVENT {
				logrus.Debug("received server heartbeat")
			} else {
				logrus.WithField("event", event.Header.EventType.String()).Debug("Generic Event")
			}
		case *replication.RowsQueryEvent:
			implicitFlush = true // Implicit FlushEvent conversion permitted
			var queryBytes = data.Query
			var truncated = len(queryBytes) > 200
			if truncated {
				queryBytes = queryBytes[:200]
			}
			logrus.WithFields(logrus.Fields{
				"query":        string(queryBytes),
				"query_length": len(data.Query),
				"truncated":    truncated,
			}).Debug("ignoring Rows Query Event")
		case *replication.IntVarEvent:
			implicitFlush = true // Implicit FlushEvent conversion permitted
			logrus.WithField("type", data.Type).WithField("value", data.Value).Debug("ignoring IntVar Event")
		default:
			return fmt.Errorf("unhandled event type: %q", event.Header.EventType)
		}

		// If the binlog event is eligible for implicit FlushEvent reporting and there
		// are no uncommitted changes currently pending (for which we would want to wait
		// until a real commit event to flush the output) then report a new FlushEvent
		// with the latest position.
		if implicitFlush && !rs.uncommittedChanges && !binlogOffsetOverflow {
			if err := rs.emitEvent(ctx, &mysqlCommitEvent{Position: cursor}); err != nil {
				return err
			}
		}
	}
}

func (rs *mysqlReplicationStream) handleRowsEvent(ctx context.Context, event *replication.BinlogEvent, eventCursor mysqlChangeEventCursor) error {
	var data, ok = event.Event.(*replication.RowsEvent)
	if !ok {
		return fmt.Errorf("internal error: expected RowsEvent, got %T", event.Event)
	}

	var schema, table = string(data.Table.Schema), string(data.Table.Table)
	var streamID = sqlcapture.JoinStreamID(schema, table)

	// Skip change events from tables which aren't being captured. Send a KeepaliveEvent
	// to indicate that we are actively receiving events, just not ones that need to be
	// decoded and processed further.
	if !rs.tableActive(streamID) {
		return rs.emitEvent(ctx, &sqlcapture.KeepaliveEvent{})
	}

	var nonTransactionalTable = rs.isNonTransactional(streamID)

	// Get column names and types from persistent metadata. If available, allow
	// override the persistent column name tracking using binlog row metadata.
	metadata, ok := rs.tableMetadata(streamID)
	if !ok || metadata == nil {
		return fmt.Errorf("missing metadata for stream %q", streamID)
	}
	var columnTypes = metadata.Schema.ColumnTypes
	var columnNames = data.Table.ColumnNameString()
	if len(columnNames) == 0 {
		columnNames = metadata.Schema.Columns
	}

	keyColumns, ok := rs.keyColumns(streamID)
	if !ok {
		return fmt.Errorf("unknown key columns for stream %q", streamID)
	}

	// Construct efficient indices and transcoder arrays for value processing
	//
	// Note that we could probably squeeze out some additional performance by
	// caching this across multiple RowsEvents the way we do for PostgreSQL
	// replication processing info, but doing that for MySQL is a tricky cache
	// invalidation problem that didn't seem worthwhile just yet.
	var err error
	var rowKeyIndices = make([]int, len(keyColumns))                 // Indices of key columns in the table, in key order.
	var outputColumnNames = make([]string, len(columnNames))         // Names of all columns with omitted columns set to "", in table order, plus _meta.
	var outputTranscoders = make([]jsonTranscoder, len(columnNames)) // Transcoders from DB values to JSON, with omitted columns set to nil.
	var rowKeyTranscoders = make([]fdbTranscoder, len(columnNames))  // Transcoders from DB values to FDB row keys, with non-key columns set to nil.
	for idx, colName := range columnNames {
		if colName == "DB_ROW_HASH_1" && columnTypes[colName] == nil {
			// MariaDB versions 10.4 and up include a synthetic `DB_ROW_HASH_1` column in the
			// binlog row change events for certain tables with unique hash indices (see issue
			// https://github.com/estuary/connectors/issues/1344). We don't want to capture
			// this synthetic column.
			outputColumnNames[idx] = ""
			continue
		}
		outputColumnNames[idx] = colName
		outputTranscoders[idx], err = rs.db.constructJSONTranscoder(false, columnTypes[colName])
		if err != nil {
			return fmt.Errorf("error constructing JSON transcoder for column %q of type %v: %w", colName, columnTypes[colName], err)
		}
		if slices.Contains(keyColumns, colName) {
			rowKeyTranscoders[idx], err = rs.db.constructFDBTranscoder(false, columnTypes[colName])
			if err != nil {
				return fmt.Errorf("error constructing FDB transcoder for column %q of type %v: %w", colName, columnTypes[colName], err)
			}
		}
	}
	outputColumnNames = append(outputColumnNames, "_meta")
	for keyIndex, keyColName := range keyColumns {
		var columnIndex = slices.Index(columnNames, keyColName)
		if columnIndex < 0 {
			return fmt.Errorf("key column %q not found in relation %q", keyColName, streamID)
		}
		rowKeyIndices[keyIndex] = columnIndex
	}
	var sharedChangeInfo = &mysqlChangeSharedInfo{
		StreamID:    streamID,
		Shape:       encrow.NewShape(outputColumnNames),
		Transcoders: outputTranscoders,
	}

	// Construct prototype change metadata which will be copied for all rows of this binlog event
	var sourceMillis int64
	if !rs.gtidTimestamp.IsZero() {
		sourceMillis = rs.gtidTimestamp.UnixMilli()
	}
	var eventTxID string
	if rs.db.includeTxIDs[streamID] {
		eventTxID = rs.gtidString
	}
	var prototypeChangeMetadata = mysqlChangeMetadata{
		Info: sharedChangeInfo,
		Source: mysqlSourceInfo{
			SourceCommon: sqlcapture.SourceCommon{
				Schema: schema,
				Table:  table,
				Millis: sourceMillis,
			},
			Cursor: mysqlChangeEventCursor{
				BinlogFile:   eventCursor.BinlogFile,
				BinlogOffset: eventCursor.BinlogOffset,
			},
			TxID: eventTxID,
			Tag:  rs.db.config.Advanced.SourceTag,
		},
	}

	var events []sqlcapture.DatabaseEvent
	switch event.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		for rowIdx, row := range data.Rows {
			var values, err = decodeRow(streamID, len(columnNames), row, data.SkippedColumns[rowIdx], nil)
			if err != nil {
				return fmt.Errorf("error decoding row values: %w", err)
			}
			rowKey, err := encodeRowKey(columnNames, rowKeyIndices, rowKeyTranscoders, values)
			if err != nil {
				return fmt.Errorf("error encoding row key for %q: %w", streamID, err)
			}
			var event = &mysqlChangeEvent{
				Info:   sharedChangeInfo,
				Meta:   prototypeChangeMetadata,
				RowKey: rowKey,
				Values: values,
			}
			event.Meta.Operation = sqlcapture.InsertOp
			event.Meta.Source.Cursor.RowIndex = rowIdx
			events = append(events, event)
		}
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		for rowIdx := range data.Rows {
			// Update events contain alternating (before, after) pairs of rows
			if rowIdx%2 == 1 {
				before, err := decodeRow(streamID, len(columnNames), data.Rows[rowIdx-1], data.SkippedColumns[rowIdx-1], nil)
				if err != nil {
					return fmt.Errorf("error decoding 'before' row values: %w", err)
				}
				after, err := decodeRow(streamID, len(columnNames), data.Rows[rowIdx], data.SkippedColumns[rowIdx], before)
				if err != nil {
					return fmt.Errorf("error decoding 'after' row values: %w", err)
				}
				rowKeyBefore, err := encodeRowKey(columnNames, rowKeyIndices, rowKeyTranscoders, before)
				if err != nil {
					return fmt.Errorf("error encoding 'before' row key for %q: %w", streamID, err)
				}
				rowKeyAfter, err := encodeRowKey(columnNames, rowKeyIndices, rowKeyTranscoders, after)
				if err != nil {
					return fmt.Errorf("error encoding 'after' row key for %q: %w", streamID, err)
				}
				if !bytes.Equal(rowKeyBefore, rowKeyAfter) {
					// When the row key is changed by an update, translate it into a synthetic pair: a delete
					// event of the old row-state, plus an insert event of the new row-state.
					//
					// Since updates consist of paired row-states (before, then after) which we iterate
					// over two-at-a-time, it is consistent to have the row-index portion of the event
					// cursor be the before-state index for this deletion and the after-state index for
					// the insert.
					var deleteEvent = &mysqlChangeEvent{
						Info:   sharedChangeInfo,
						Meta:   prototypeChangeMetadata,
						RowKey: rowKeyBefore,
						Values: before,
					}
					deleteEvent.Meta.Operation = sqlcapture.DeleteOp
					deleteEvent.Meta.Source.Cursor.RowIndex = rowIdx - 1

					var insertEvent = &mysqlChangeEvent{
						Info:   sharedChangeInfo,
						Meta:   prototypeChangeMetadata,
						RowKey: rowKeyAfter,
						Values: after,
					}
					insertEvent.Meta.Operation = sqlcapture.InsertOp
					insertEvent.Meta.Source.Cursor.RowIndex = rowIdx
					events = append(events, deleteEvent, insertEvent)
				} else {
					var event = &mysqlChangeEvent{
						Info:   sharedChangeInfo,
						Meta:   prototypeChangeMetadata,
						RowKey: rowKeyAfter,
						Values: after,
					}
					event.Meta.Before = before
					event.Meta.Operation = sqlcapture.UpdateOp
					event.Meta.Source.Cursor.RowIndex = rowIdx
					events = append(events, event)
				}
			}
		}
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		for rowIdx, row := range data.Rows {
			var values, err = decodeRow(streamID, len(columnNames), row, data.SkippedColumns[rowIdx], nil)
			if err != nil {
				return fmt.Errorf("error decoding row values: %w", err)
			}
			rowKey, err := encodeRowKey(columnNames, rowKeyIndices, rowKeyTranscoders, values)
			if err != nil {
				return fmt.Errorf("error encoding row key for %q: %w", streamID, err)
			}
			var event = &mysqlChangeEvent{
				Info:   sharedChangeInfo,
				Meta:   prototypeChangeMetadata,
				RowKey: rowKey,
				Values: values,
			}
			event.Meta.Operation = sqlcapture.DeleteOp
			event.Meta.Source.Cursor.RowIndex = rowIdx
			events = append(events, event)
		}
	default:
		return fmt.Errorf("unknown row event type: %q", event.Header.EventType)
	}
	for _, event := range events {
		if err := rs.emitEvent(ctx, event); err != nil {
			return err
		}
	}
	if len(events) > 0 {
		rs.uncommittedChanges = true // Set uncommitted changes flag
		if nonTransactionalTable {
			rs.nonTransactionalChanges = true // Set uncommitted non-transactional changes flag
		}
	}
	return nil
}

// A special value used to indicate that a tuple value is omitted entirely.
var rowValueOmitted any = &omittedRowValue{}

type omittedRowValue struct{}

func decodeRow(streamID sqlcapture.StreamID, expectedArity int, row []any, skips []int, before []any) ([]any, error) {
	// If we have more or fewer values than expected, something has gone wrong
	// with our metadata tracking and this should trigger an automatic re-backfill.
	if len(row) != expectedArity {
		if expectedArity == 0 {
			return nil, fmt.Errorf("metadata error (go.estuary.dev/eiKbOh): unknown column names for stream %q", streamID)
		}
		return nil, &InconsistentMetadataError{
			StreamID: streamID,
			Message:  fmt.Sprintf("%d columns found when %d are expected", len(row), expectedArity),
		}
	}

	var fields = make([]any, expectedArity)
	copy(fields, row)
	for _, idx := range skips {
		fields[idx] = rowValueOmitted
	}
	for idx, val := range before {
		if fields[idx] == rowValueOmitted {
			fields[idx] = val
		}
	}
	return fields, nil
}

func encodeRowKey(columnNames []string, rowKeyIndices []int, rowKeyTranscoders []fdbTranscoder, values []any) ([]byte, error) {
	var err error
	var rowKey []byte
	for _, n := range rowKeyIndices {
		rowKey, err = rowKeyTranscoders[n].TranscodeFDB(rowKey, values[n])
		if err != nil {
			return nil, fmt.Errorf("error encoding row key column %q at index %d: %w", columnNames[n], n, err)
		}
	}
	return rowKey, nil
}

// Query Events in the MySQL binlog are normalized enough that we can use
// prefix matching to detect many types of query that we just completely
// don't care about. This is good, because the Vitess SQL parser disagrees
// with the binlog Query Events for some statements like GRANT and CREATE USER.
// TODO(johnny): SET STATEMENT is not safe in the general case, and we want to re-visit
// by extracting and ignoring a SET STATEMENT stanza prior to parsing.
var silentIgnoreQueriesRe = regexp.MustCompile(`(?i)^(BEGIN|COMMIT|ROLLBACK|SAVEPOINT .*|# [^\n]*)$`)
var createDefinerRegex = `CREATE\s*(OR REPLACE){0,1}\s*(ALGORITHM\s*=\s*[^ ]+)*\s*DEFINER`
var ignoreQueriesRe = regexp.MustCompile(`(?i)^(BEGIN|COMMIT|GRANT|REVOKE|CREATE USER|` + createDefinerRegex + `|DROP USER|ALTER USER|DROP PROCEDURE|DROP FUNCTION|DROP TRIGGER|SET STATEMENT|CREATE EVENT|ALTER EVENT|DROP EVENT)`)

func (rs *mysqlReplicationStream) handleQuery(ctx context.Context, parser *sqlparser.Parser, schema, query string) error {
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
	if silentIgnoreQueriesRe.MatchString(query) {
		logrus.WithField("query", query).Trace("silently ignoring query event")
		return nil
	}
	if ignoreQueriesRe.MatchString(query) {
		logrus.WithField("query", query).Info("ignoring query event")
		return nil
	}
	logrus.WithField("query", query).Info("handling query event")

	var stmt, err = parser.Parse(query)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"query": query,
			"err":   err,
		}).Warn("failed to parse query event, ignoring it")
		return nil
	}
	logrus.WithField("stmt", fmt.Sprintf("%#v", stmt)).Debug("parsed query")

	switch stmt := stmt.(type) {
	case *sqlparser.CreateDatabase, *sqlparser.AlterDatabase, *sqlparser.CreateTable, *sqlparser.Savepoint, *sqlparser.Flush:
		logrus.WithField("query", query).Debug("ignoring benign query")
	case *sqlparser.CreateView, *sqlparser.AlterView, *sqlparser.DropView:
		// All view creation/deletion/alterations should be fine to ignore since we don't capture from views.
		logrus.WithField("query", query).Debug("ignoring benign query")
	case *sqlparser.DropDatabase:
		// Remember that In MySQL land "database" is a synonym for the usual SQL concept "schema"
		if streamIDs := rs.tablesInSchema(stmt.GetDatabaseName()); len(streamIDs) > 0 {
			logrus.WithFields(logrus.Fields{
				"query":     query,
				"schema":    stmt.GetDatabaseName(),
				"streamIDs": streamIDs,
			}).Info("dropped all tables in schema")
			for _, streamID := range streamIDs {
				if err := rs.emitEvent(ctx, &sqlcapture.TableDropEvent{
					StreamID: streamID,
					Cause:    fmt.Sprintf("schema %q was dropped by query %q", streamID, query),
				}); err != nil {
					return err
				} else if err := rs.deactivateTable(streamID); err != nil {
					return fmt.Errorf("cannot deactivate table %q after DROP DATABASE: %w", streamID, err)
				}
			}
		} else {
			logrus.WithField("query", query).Debug("ignorable dropped schema (not being captured from)")
		}
	case *sqlparser.AlterTable:
		if streamID := resolveTableName(schema, stmt.Table); rs.tableActive(streamID) {
			logrus.WithFields(logrus.Fields{
				"query":         query,
				"partitionSpec": stmt.PartitionSpec,
				"alterOptions":  stmt.AlterOptions,
			}).Info("parsed components of ALTER TABLE statement")

			if stmt.PartitionSpec == nil || len(stmt.AlterOptions) != 0 {
				if err := rs.handleAlterTable(ctx, stmt, query, streamID); err != nil {
					return fmt.Errorf("cannot handle table alteration %q: %w", query, err)
				}
			}
		}
	case *sqlparser.DropTable:
		for _, table := range stmt.FromTables {
			if streamID := resolveTableName(schema, table); rs.tableActive(streamID) {
				// Indicate that change streaming for this table has failed.
				if err := rs.emitEvent(ctx, &sqlcapture.TableDropEvent{
					StreamID: streamID,
					Cause:    fmt.Sprintf("table %q was dropped by query %q", streamID, query),
				}); err != nil {
					return err
				} else if err := rs.deactivateTable(streamID); err != nil {
					return err
				}
			}
		}
	case *sqlparser.TruncateTable:
		if streamID := resolveTableName(schema, stmt.Table); rs.tableActive(streamID) {
			// Once we have a concept of collection-level truncation we will probably
			// want to either handle this like a dropped-and-recreated table or else
			// use another mechanism to produce the appropriate "the collection is
			// now truncated" signals here. But for now ignoring is still the best
			// we can do.
			logrus.WithField("table", streamID).Warn("ignoring TRUNCATE on active table")
		}
	case *sqlparser.RenameTable:
		for _, pair := range stmt.TablePairs {
			if streamID := resolveTableName(schema, pair.FromTable); rs.tableActive(streamID) {
				// Indicate that change streaming for this table has failed.
				if err := rs.emitEvent(ctx, &sqlcapture.TableDropEvent{
					StreamID: streamID,
					Cause:    fmt.Sprintf("table %q was renamed by query %q", streamID, query),
				}); err != nil {
					return err
				} else if err := rs.deactivateTable(streamID); err != nil {
					return err
				}
			}
		}
	case *sqlparser.Insert:
		table, err := stmt.Table.TableName()
		if err != nil {
			return fmt.Errorf("internal error: could no determine table name for Insert query %s: %w", query, err)
		}
		if streamID := resolveTableName(schema, table); rs.tableActive(streamID) {
			return fmt.Errorf("unsupported DML query (go.estuary.dev/IK5EVx): %s", query)
		}
	case *sqlparser.Update:
		// Determine whether any of the table expressions in the UPDATE statement refer
		// to an active table. The table expression grammar gets complicated, so if we
		// can't tell then we'll assume that the table is active and return an error.
		var possiblyActiveTables bool
		for _, table := range stmt.TableExprs {
			if tableExpr, ok := table.(*sqlparser.AliasedTableExpr); !ok {
				logrus.WithField("query", query).Warnf("unsupported table expression type %T in UPDATE statement", table)
				possiblyActiveTables = true
			} else if table, err := tableExpr.TableName(); err != nil {
				logrus.WithField("query", query).WithError(err).Warn("failed to resolve table name from UPDATE statement")
				possiblyActiveTables = true
			} else if streamID := resolveTableName(schema, table); rs.tableActive(streamID) {
				logrus.WithField("streamID", streamID).WithField("query", query).Warn("UPDATE on active table")
				possiblyActiveTables = true
			} else {
				logrus.WithField("streamID", streamID).WithField("query", query).Debug("ignoring UPDATE on inactive table")
			}
		}

		// If any table(s) in the UPDATE statement are possibly active then it's a fatal error.
		if possiblyActiveTables {
			return fmt.Errorf("unsupported DML query (go.estuary.dev/IK5EVx): %s", query)
		}
	case *sqlparser.Delete:
		for _, target := range stmt.Targets {
			if streamID := resolveTableName(schema, target); rs.tableActive(streamID) {
				return fmt.Errorf("unsupported DML query (go.estuary.dev/IK5EVx): %s", query)
			}
		}
	case *sqlparser.OtherAdmin, *sqlparser.Analyze:
		logrus.WithField("query", query).Debug("ignoring benign query")
	default:
		return fmt.Errorf("unhandled query (go.estuary.dev/ceqr74): unhandled type %q: %q", reflect.TypeOf(stmt).String(), query)
	}

	return nil
}

func (rs *mysqlReplicationStream) handleAlterTable(ctx context.Context, stmt *sqlparser.AlterTable, query string, streamID sqlcapture.StreamID) error {
	// This lock and assignment to `meta` isn't actually needed unless we are able to handle the
	// alteration. But if we can't handle the alteration the connector is probably going to crash,
	// so any performance implication is negligible at that point and it makes things a little
	// easier to get the lock here.
	rs.tables.Lock()
	defer rs.tables.Unlock()
	meta := rs.tables.metadata[streamID]

	for _, alterOpt := range stmt.AlterOptions {
		switch alter := alterOpt.(type) {
		// These should be all of the table alterations which might possibly impact our capture
		// in ways we don't currently support, so the default behavior can be to log and ignore.
		case *sqlparser.RenameColumn:
			var oldName = alter.OldName.Name.String()
			var newName = alter.NewName.Name.String()

			var colIndex = findColumnIndex(meta.Schema.Columns, oldName)
			if colIndex == -1 {
				return fmt.Errorf("unknown column %q", oldName)
			}
			oldName = meta.Schema.Columns[colIndex] // Use the actual column name from the metadata
			meta.Schema.Columns[colIndex] = newName

			var colType = meta.Schema.ColumnTypes[oldName]
			meta.Schema.ColumnTypes[oldName] = nil
			meta.Schema.ColumnTypes[newName] = colType
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed RENAME COLUMN alteration")
		case *sqlparser.RenameTableName:
			return fmt.Errorf("unsupported table alteration (go.estuary.dev/eVVwet): %s", query)
		case *sqlparser.ChangeColumn:
			var oldName = alter.OldColumn.Name.String()
			var oldIndex = findColumnIndex(meta.Schema.Columns, oldName)
			if oldIndex == -1 {
				return fmt.Errorf("unknown column %q", oldName)
			}
			oldName = meta.Schema.Columns[oldIndex] // Use the actual column name from the metadata
			meta.Schema.Columns = slices.Delete(meta.Schema.Columns, oldIndex, oldIndex+1)

			var newName = alter.NewColDefinition.Name.String()
			var newType = translateDataType(meta, alter.NewColDefinition.Type)
			var newIndex = oldIndex
			if alter.First {
				newIndex = 0
			} else if alter.After != nil {
				var afterName = alter.After.Name.String()
				var afterIndex = findColumnIndex(meta.Schema.Columns, afterName)
				if afterIndex == -1 {
					return fmt.Errorf("unknown column %q", afterName)
				}
				newIndex = afterIndex + 1
			}
			meta.Schema.Columns = slices.Insert(meta.Schema.Columns, newIndex, newName)
			meta.Schema.ColumnTypes[oldName] = nil // Set to nil rather than delete so that JSON patch merging deletes it
			meta.Schema.ColumnTypes[newName] = newType
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed CHANGE COLUMN alteration")
		case *sqlparser.ModifyColumn:
			var colName = alter.NewColDefinition.Name.String()
			var oldIndex = findColumnIndex(meta.Schema.Columns, colName)
			if oldIndex == -1 {
				return fmt.Errorf("unknown column %q", colName)
			}
			colName = meta.Schema.Columns[oldIndex] // Use the actual column name from the metadata
			meta.Schema.Columns = slices.Delete(meta.Schema.Columns, oldIndex, oldIndex+1)

			var newType = translateDataType(meta, alter.NewColDefinition.Type)
			var newIndex = oldIndex
			if alter.First {
				newIndex = 0
			} else if alter.After != nil {
				var afterName = alter.After.Name.String()
				var afterIndex = findColumnIndex(meta.Schema.Columns, afterName)
				if afterIndex == -1 {
					return fmt.Errorf("unknown column %q", afterName)
				}
				newIndex = afterIndex + 1
			}

			meta.Schema.Columns = slices.Insert(meta.Schema.Columns, newIndex, colName)
			meta.Schema.ColumnTypes[colName] = newType
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed MODIFY COLUMN alteration")
		case *sqlparser.AddColumns:
			var insertAt = len(meta.Schema.Columns)
			if alter.First {
				insertAt = 0
			} else if after := alter.After; after != nil {
				var afterIndex = findColumnIndex(meta.Schema.Columns, after.Name.String())
				if afterIndex == -1 {
					return fmt.Errorf("unknown column %q", after.Name.String())
				}
				insertAt = afterIndex + 1
			}

			var newCols []string
			for _, col := range alter.Columns {
				newCols = append(newCols, col.Name.String())
				var dataType = translateDataType(meta, col.Type)
				meta.Schema.ColumnTypes[col.Name.String()] = dataType
			}

			meta.Schema.Columns = slices.Insert(meta.Schema.Columns, insertAt, newCols...)
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed CHANGE COLUMN alteration")
		case *sqlparser.DropColumn:
			var colName = alter.Name.Name.String()
			var oldIndex = findColumnIndex(meta.Schema.Columns, colName)
			if oldIndex == -1 {
				return fmt.Errorf("unknown column %q", colName)
			}
			colName = meta.Schema.Columns[oldIndex] // Use the actual column name from the metadata
			meta.Schema.Columns = slices.Delete(meta.Schema.Columns, oldIndex, oldIndex+1)
			meta.Schema.ColumnTypes[colName] = nil // Set to nil rather than delete so that JSON patch merging deletes it
			logrus.WithField("columns", meta.Schema.Columns).WithField("types", meta.Schema.ColumnTypes).Info("processed CHANGE COLUMN alteration")
		default:
			logrus.WithField("query", query).Info("ignorable table alteration")
		}
	}

	var bs, err = json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("error serializing metadata JSON for %q: %w", streamID, err)
	}
	if err := rs.emitEvent(ctx, &sqlcapture.MetadataEvent{
		StreamID: streamID,
		Metadata: json.RawMessage(bs),
	}); err != nil {
		return err
	}

	return nil
}

// findColumnIndex performs a case-insensitive search for a column name in a slice of column names.
// It returns the index of the first matching column, or -1 if no match is found.
//
// According to https://dev.mysql.com/doc/refman/8.4/en/identifier-case-sensitivity.html:
// > [...] column [...] names are not case-sensitive on any platform, nor are column aliases.
func findColumnIndex(columns []string, name string) int {
	for i, col := range columns {
		if strings.EqualFold(col, name) {
			return i
		}
	}
	return -1
}

func translateDataType(meta *mysqlTableMetadata, t *sqlparser.ColumnType) any {
	switch typeName := strings.ToLower(t.Type); typeName {
	case "enum":
		return &mysqlColumnType{Type: typeName, EnumValues: append([]string{""}, unquoteEnumValues(t.EnumValues)...)}
	case "set":
		return &mysqlColumnType{Type: typeName, EnumValues: unquoteEnumValues(t.EnumValues)}
	case "tinyint", "smallint", "mediumint", "int", "bigint":
		return &mysqlColumnType{Type: typeName, Unsigned: t.Unsigned}
	case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
		var charset string
		if t.Charset.Name != "" {
			charset = t.Charset.Name // If explicitly specified, the declared charset wins
		} else if t.Options.Collate != "" {
			charset = charsetFromCollation(t.Options.Collate) // If only a collation is declared, figure out what charset that implies
		} else if meta.DefaultCharset != "" {
			charset = meta.DefaultCharset // In the absence of a column-specific declaration, use the default table charset
		} else {
			charset = mysqlDefaultCharset // Finally fall back to UTF-8 if nothing else supersedes that
		}
		return &mysqlColumnType{Type: typeName, Charset: charset}
	case "binary":
		var columnLength int
		if t.Length == nil {
			columnLength = 1 // A type of just 'BINARY' is allowed and is a synonym for 'BINARY(1)'
		} else {
			columnLength = *t.Length
		}
		return &mysqlColumnType{Type: typeName, MaxLength: columnLength}
	default:
		return typeName
	}
}

func resolveTableName(defaultSchema string, name sqlparser.TableName) sqlcapture.StreamID {
	var schema, table = name.Qualifier.String(), name.Name.String()
	if schema == "" {
		schema = defaultSchema
	}
	return sqlcapture.JoinStreamID(schema, table)
}

func (rs *mysqlReplicationStream) tableMetadata(streamID sqlcapture.StreamID) (*mysqlTableMetadata, bool) {
	rs.tables.RLock()
	defer rs.tables.RUnlock()
	var meta, ok = rs.tables.metadata[streamID]
	return meta, ok
}

func (rs *mysqlReplicationStream) tableActive(streamID sqlcapture.StreamID) bool {
	rs.tables.RLock()
	defer rs.tables.RUnlock()
	var _, ok = rs.tables.active[streamID]
	return ok
}

func (rs *mysqlReplicationStream) tablesInSchema(schema string) []sqlcapture.StreamID {
	rs.tables.RLock()
	defer rs.tables.RUnlock()

	var tables []sqlcapture.StreamID
	for streamID := range rs.tables.active {
		if strings.EqualFold(streamID.Schema, schema) {
			tables = append(tables, streamID)
		}
	}
	return tables
}

// isNonTransactional returns true if the stream ID refers to a table which
// uses a storage engine such as MyISAM which doesn't support transactions.
// Changes to non-transactional tables are never followed by a commit event,
// so we have to take care of that for ourselves.
func (rs *mysqlReplicationStream) isNonTransactional(streamID sqlcapture.StreamID) bool {
	rs.tables.RLock()
	defer rs.tables.RUnlock()
	return rs.tables.nonTransactional[streamID]
}

func (rs *mysqlReplicationStream) keyColumns(streamID sqlcapture.StreamID) ([]string, bool) {
	rs.tables.RLock()
	defer rs.tables.RUnlock()
	var keyColumns, ok = rs.tables.keyColumns[streamID]
	return keyColumns, ok
}

func (rs *mysqlReplicationStream) ActivateTable(ctx context.Context, streamID sqlcapture.StreamID, keyColumns []string, discovery *sqlcapture.DiscoveryInfo, metadataJSON json.RawMessage) error {
	rs.tables.Lock()
	defer rs.tables.Unlock()

	// Do nothing if the table is already active.
	if _, ok := rs.tables.active[streamID]; ok {
		return nil
	}

	// Extract some details from the provided discovery info, if present.
	var nonTransactional bool
	if discovery != nil {
		if extraDetails, ok := discovery.ExtraDetails.(*mysqlTableDiscoveryDetails); ok {
			nonTransactional = extraDetails.StorageEngine == "MyISAM"
		}
	}

	// If metadata JSON is present then parse it into a usable object. Otherwise
	// initialize new metadata based on discovery results.
	var metadata *mysqlTableMetadata
	if metadataJSON != nil {
		if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
			return fmt.Errorf("error parsing metadata JSON: %w", err)
		}
		// Fix up complex (non-string) column types, since the JSON round-trip
		// will turn *mysqlColumnType values into map[string]interface{}.
		for column, columnType := range metadata.Schema.ColumnTypes {
			if columnType, ok := columnType.(map[string]interface{}); ok {
				var parsedType mysqlColumnType
				if err := mapstructure.Decode(columnType, &parsedType); err == nil {
					metadata.Schema.ColumnTypes[column] = &parsedType
				}
			}
		}

		// This is a temporary piece of migration logic added in March of 2024. In the PR
		// https://github.com/estuary/connectors/pull/1336 the ordering of enum cases in this
		// metadata was changed to simplify the decoding logic. Specifically the order of the
		// cases was changed from ["A", "B", ""] to ["", "A", "B"] to more directly mirror how
		// MySQL represents the illegal-enum value "" as integer 0. However this introduced a bug
		// when the new indexing code was used with older metadata from before that change, and
		// by the time this was discovered there were captures in production with the new ordering
		// in their checkpointed metadata, so a simple revert was not an option.
		//
		// The solution is to push forward and add the missing migration step, so that upon table
		// activation any metadata containing the old ordering will be updated to the new. Once all
		// metadata in production has been so updated, it will be safe to remove this logic and the
		// associated 'TestEnumDecodingFix' test case.
		for column, columnType := range metadata.Schema.ColumnTypes {
			if enumType, ok := columnType.(*mysqlColumnType); ok && enumType.Type == "enum" {
				if len(enumType.EnumValues) > 0 && enumType.EnumValues[0] != "" {
					var logEntry = logrus.WithField("column", column)
					var vals = enumType.EnumValues
					logEntry.WithField("vals", vals).Warn("old enum metadata ordering detected, will migrate")
					if vals[len(vals)-1] == "" {
						logEntry.Info("trimming empty-string case from the end")
						vals = vals[:len(vals)-1]
					}
					vals = append([]string{""}, vals...)
					logEntry.WithField("vals", vals).Info("migrated old enum metadata")
					enumType.EnumValues = vals
				}
			}
		}
	} else if discovery != nil {
		// If metadata JSON is not present, construct new default metadata based on the discovery info.
		logrus.WithField("stream", streamID).Debug("initializing table metadata")
		metadata = new(mysqlTableMetadata)
		var colTypes = make(map[string]interface{})
		for colName, colInfo := range discovery.Columns {
			colTypes[colName] = colInfo.DataType
		}

		metadata.Schema.Columns = discovery.ColumnNames
		metadata.Schema.ColumnTypes = colTypes
		if extraDetails, ok := discovery.ExtraDetails.(*mysqlTableDiscoveryDetails); ok {
			metadata.DefaultCharset = extraDetails.DefaultCharset
		}

		logrus.WithFields(logrus.Fields{
			"stream":  streamID,
			"columns": metadata.Schema.Columns,
			"types":   metadata.Schema.ColumnTypes,
			"charset": metadata.DefaultCharset,
		}).Debug("initialized table metadata")
	}

	// Finally, mark the table as active and store the updated metadata.
	rs.tables.active[streamID] = struct{}{}
	rs.tables.keyColumns[streamID] = keyColumns
	rs.tables.metadata[streamID] = metadata
	rs.tables.nonTransactional[streamID] = nonTransactional
	rs.tables.dirtyMetadata = append(rs.tables.dirtyMetadata, streamID)
	return nil
}

func (rs *mysqlReplicationStream) deactivateTable(streamID sqlcapture.StreamID) error {
	rs.tables.Lock()
	defer rs.tables.Unlock()

	delete(rs.tables.active, streamID)
	delete(rs.tables.keyColumns, streamID)
	delete(rs.tables.metadata, streamID) // No need to mark metadata as dirty, just forget it
	delete(rs.tables.nonTransactional, streamID)
	return nil
}

func (rs *mysqlReplicationStream) emitEvent(ctx context.Context, event sqlcapture.DatabaseEvent) error {
	select {
	case rs.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rs *mysqlReplicationStream) StreamToFence(ctx context.Context, fenceAfter time.Duration, callback func(event sqlcapture.DatabaseEvent) error) error {
	// Report "Metadata Change" events when necessary.
	rs.tables.RLock()
	if len(rs.tables.dirtyMetadata) > 0 {
		for _, streamID := range rs.tables.dirtyMetadata {
			var bs, err = json.Marshal(rs.tables.metadata[streamID])
			if err != nil {
				return fmt.Errorf("error serializing metadata JSON for %q: %w", streamID, err)
			}
			if err := callback(&sqlcapture.MetadataEvent{
				StreamID: streamID,
				Metadata: json.RawMessage(bs),
			}); err != nil {
				return err
			}
		}
		rs.tables.dirtyMetadata = nil
	}
	rs.tables.RUnlock()

	// Time-based event streaming until the fenceAfter duration is reached.
	var timedEventsSinceFlush int
	var latestFlushPosition = rs.fencePosition
	logrus.WithField("cursor", latestFlushPosition).Debug("beginning timed streaming phase")
	if fenceAfter > 0 {
		var deadline = time.NewTimer(fenceAfter)
		defer deadline.Stop()
	loop:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-deadline.C:
				break loop
			case event, ok := <-rs.events:
				if !ok {
					return sqlcapture.ErrFenceNotReached
				} else if err := callback(event); err != nil {
					return err
				}
				timedEventsSinceFlush++
				if event, ok := event.(*mysqlCommitEvent); ok {
					latestFlushPosition = event.Position
					timedEventsSinceFlush = 0
				}
			}
		}
	}
	logrus.WithField("cursor", latestFlushPosition).Debug("finished timed streaming phase")

	// Establish a binlog-position fence.
	var fencePosition, err = rs.db.queryBinlogPosition()
	if err != nil {
		return fmt.Errorf("error establishing binlog fence position: %w", err)
	}
	logrus.WithField("cursor", latestFlushPosition).WithField("target", fencePosition.Pos).Debug("beginning fenced streaming phase")

	if fencePosition == latestFlushPosition {
		// As an internal sanity check, we assert that it should never be possible
		// to hit this early exit unless the database has been idle since the last
		// flush event we observed.
		if timedEventsSinceFlush > 0 {
			return fmt.Errorf("internal error: sanity check failed: already at fence after processing %d changes during timed phase", timedEventsSinceFlush)
		}

		// Mark the position of the flush event as the latest fence before returning.
		rs.fencePosition = latestFlushPosition

		// Since we're still at a valid flush position and those are always between
		// transactions, we can safely emit a synthetic FlushEvent here. This means
		// that every StreamToFence operation ends in a flush, and is helpful since
		// there's a lot of implicit assumptions of regular events / flushes.
		return callback(&mysqlCommitEvent{Position: latestFlushPosition})
	}

	// Given that the early-exit fast path was not taken, there must be further data for
	// us to read. Thus if we sit idle for a nontrivial length of time without reaching
	// our fence position, something is wrong and we should error out instead of blocking
	// forever.
	var fenceWatchdog = time.NewTimer(streamToFenceWatchdogTimeout)

	// Stream replication events until the fence is reached.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-fenceWatchdog.C:
			return fmt.Errorf("replication became idle while streaming from %q to an established fence at %q", latestFlushPosition, fencePosition)
		case event, ok := <-rs.events:
			fenceWatchdog.Reset(streamToFenceWatchdogTimeout)
			if !ok {
				return sqlcapture.ErrFenceNotReached
			} else if err := callback(event); err != nil {
				return err
			}

			// The first flush event whose cursor position is equal to or after the fence
			// position ends the stream-to-fence operation.
			if event, ok := event.(*mysqlCommitEvent); ok {
				if event.Position.Compare(fencePosition) >= 0 {
					logrus.WithField("cursor", event.Position).Debug("finished fenced streaming phase")
					rs.fencePosition = event.Position
					return nil
				}
			}
		}
	}
}

func (rs *mysqlReplicationStream) Acknowledge(ctx context.Context, cursor json.RawMessage) error {
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

func (db *mysqlDatabase) ReplicationDiagnostics(ctx context.Context) error {
	var query = func(q string) {
		logrus.WithField("query", q).Info("running diagnostics query")
		var result, err = db.conn.Execute(q)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"query": q,
				"err":   err,
			}).Error("unable to execute diagnostics query")
			return
		}
		defer result.Close()

		if len(result.Values) == 0 {
			logrus.WithField("query", q).Info("no results")
		}
		for _, row := range result.Values {
			var logFields = logrus.Fields{}
			for idx, column := range row {
				var key = string(result.Fields[idx].Name)
				var val = column.Value()
				if bs, ok := val.([]byte); ok {
					val = string(bs)
				}
				logFields[key] = val
			}
			logrus.WithFields(logFields).Info("got diagnostic row")
		}
	}

	query("SELECT @@GLOBAL.log_bin;")
	query("SELECT @@GLOBAL.binlog_format;")
	query("SHOW PROCESSLIST;")
	query("SHOW BINARY LOGS;")
	var newspeakQueries = db.versionProduct == "MySQL" && ((db.versionMajor == 8 && db.versionMinor >= 4) || db.versionMajor > 8)
	if newspeakQueries {
		query("SHOW BINARY LOG STATUS;")
		query("SHOW REPLICAS;")
	} else {
		query("SHOW MASTER STATUS;")
		query("SHOW SLAVE HOSTS;")
	}
	return nil
}
