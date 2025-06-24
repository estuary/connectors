package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/estuary/connectors/go/encrow"
	"github.com/estuary/connectors/sqlcapture"
	"github.com/google/uuid"
	"github.com/jackc/pgio"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sirupsen/logrus"
)

var slotInUseRe = regexp.MustCompile(`replication slot ".*" is active for PID`)

func (db *postgresDatabase) ReplicationStream(ctx context.Context, startCursorJSON json.RawMessage) (sqlcapture.ReplicationStream, error) {
	// Replication database connection used for event streaming
	connConfig, err := pgconn.ParseConfig(db.config.ToURI())
	if err != nil {
		return nil, err
	}
	if connConfig.ConnectTimeout == 0 {
		connConfig.ConnectTimeout = 30 * time.Second
	}
	connConfig.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database for replication: %w", err)
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

	var slot, publication = db.config.Advanced.SlotName, db.config.Advanced.PublicationName

	// Obtain the current WAL flush location on the server. We will need this either to
	// initialize our cursor or to sanity-check it.
	serverFlushLSN, err := queryLatestServerLSN(ctx, db.conn)
	if err != nil {
		return nil, err
	}

	var startLSN pglogrepl.LSN
	if startCursor != "" {
		// Parse the cursor into an LSN value
		startLSN, err = pglogrepl.ParseLSN(startCursor)
		if err != nil {
			return nil, fmt.Errorf("error parsing start cursor: %w", err)
		}

		// Check that our start cursor is less than or equal to the server flush LSN.
		// Since our cursor should always be <= the latest server LSN (since it represents
		// a WAL commit event that we received, captured, and acknowledged), this should
		// only ever happen when this is effectively a different server (in a broad sense
		// which includes situations like "DB version upgrade")
		//
		// TODO(wgd): Upgrade this check to an actual failure once it's been verified to work in prod.
		if startLSN > serverFlushLSN {
			logrus.WithFields(logrus.Fields{
				"resumeLSN": startLSN.String(),
				"flushLSN":  serverFlushLSN.String(),
			}).Warn("resume cursor mismatch: resume LSN is greater than server flush LSN")
		}
	} else {
		// If no start cursor is specified, we are free to begin replication from the latest tail-end of the WAL.

		// By default the connector is expected to manage the replication slot automatically.
		if db.featureFlags["create_replication_slot"] {
			// We begin by dropping and recreating the slot. This avoids situations where the
			// 'restart_lsn' is significantly behind the point we actually want to start from,
			// and also has the happy side-effect of making it so that merely hitting the
			// "Backfill Everything" button in the UI is all that a user has to do to recover
			// after replication slot invalidation.
			//
			// This is always safe to do, because if our start cursor is reset then we don't
			// care about any prior state in the replication slot, and if we're able to drop
			// it then we also have the necessary permissions to recreate it. Any errors here
			// aren't fatal, just to be on the safe side.
			if err := recreateReplicationSlot(ctx, db.conn, slot); err != nil {
				logrus.WithField("err", err).Debug("error recreating replication slot")
			}
		}

		// Initialize our start LSN to the current server flush LSN.
		startLSN = serverFlushLSN
	}

	// Check that the slot's `confirmed_flush_lsn` is less than or equal to our resume cursor value.
	// This is necessary because Postgres deliberately allows clients to specify an older start LSN,
	// and then ignores that and uses the confirmed LSN instead. Supposedly this simplifies writing
	// clients in some cases, but in our case it never helps, and instead it causes trouble if/when
	// the replication slot is dropped and recreated.
	//
	// TODO(wgd): Upgrade this check to an actual failure once it's been verified to work in prod.
	if slotInfo, err := queryReplicationSlotInfo(ctx, db.conn, slot); err != nil {
		logrus.WithField("err", err).Warn("error querying replication slot info")
	} else if slotInfo == nil {
		// This should never happen, since the "does the slot exist" validation check still exists and
		// runs before we reach this part of the code. But I want this section of new logic here to be
		// completely unable to produce a fatal error, and double-checking to make extra sure we don't
		// accidentally dereference a null pointer doesn't really hurt anything.
		logrus.WithField("slot", slot).Warn("replication slot has no metadata (and probably doesn't exist?)")
	} else if slotInfo.Active {
		logrus.WithField("slot", slot).Warn("replication slot is already active (is another capture already running against this database?)")
	} else if slotInfo.WALStatus == "lost" {
		logrus.WithField("slot", slot).Warn("replication slot was invalidated by the server, it must be deleted and all bindings backfilled")
	} else if slotInfo.ConfirmedFlushLSN == nil {
		logrus.WithField("slot", slot).Warn("replication slot has no confirmed_flush_lsn (and is likely still being created but blocked on a long-running transaction)")
	} else if startLSN < *slotInfo.ConfirmedFlushLSN {
		logrus.WithFields(logrus.Fields{
			"slot":         slot,
			"confirmedLSN": slotInfo.ConfirmedFlushLSN.String(),
			"startLSN":     startLSN.String(),
		}).Warn("resume cursor mismatch: resume LSN is less than replication slot confirmed_flush_lsn (this means the slot was probably deleted+recreated and all bindings need to be backfilled)")
	}

	logrus.WithFields(logrus.Fields{
		"startLSN":    startLSN.String(),
		"publication": publication,
		"slot":        slot,
	}).Info("starting replication")

	if err := pglogrepl.StartReplication(ctx, conn, slot, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			`"proto_version" '1'`,
			fmt.Sprintf(`"publication_names" '%s'`, publication),
		},
	}); err != nil {
		conn.Close(ctx)
		// The number one source of errors at this point in the capture is that another
		// capture task is already running, using the replication slot we want. We can
		// give the user a more friendly error message to help them understand that.
		if err, ok := err.(*pgconn.PgError); ok {
			if slotInUseRe.MatchString(err.Message) {
				return nil, fmt.Errorf("another capture is already running against this database: %s", err.Message)
			}
		}
		return nil, fmt.Errorf("unable to start replication: %w", err)
	}

	hijacked, err := conn.Hijack()
	if err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("unable to hijack replication connection: %w", err)
	}

	var typeMap = pgtype.NewMap()
	if err := registerDatatypeTweaks(ctx, db.conn, typeMap); err != nil {
		return nil, err
	}

	var stream = &replicationStream{
		db:       db,
		conn:     hijacked,
		pubName:  publication,
		replSlot: slot,

		rxbuf:              make([]byte, 0, rxBufferSize),
		reusedChangeEvent:  new(postgresChangeEvent),
		reusedCommitEvent:  new(postgresCommitEvent),
		reusedBeforeValues: make([][]byte, 0, 32),
		reusedAfterValues:  make([][]byte, 0, 32),

		ackLSN:          uint64(startLSN),
		lastTxnEndLSN:   startLSN,
		nextTxnFinalLSN: 0,
		nextTxnMillis:   0,
		typeMap:         typeMap,

		previousFenceLSN: startLSN,
	}
	// Allocate table information maps
	stream.tables.relations = make(map[uint32]*pglogrepl.RelationMessage)
	stream.tables.relationStreamID = make(map[uint32]sqlcapture.StreamID)
	stream.tables.discovery = make(map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo)
	stream.tables.keyColumns = make(map[sqlcapture.StreamID][]string)
	stream.tables.infoCache = make(map[sqlcapture.StreamID]*tableProcessingInfo)
	return stream, nil
}

// postgresSource is source metadata for data capture events.
//
// For more nuance on LSN vs Last LSN vs Final LSN, see:
//
//	https://github.com/postgres/postgres/blob/a8fd13cab0ba815e9925dc9676e6309f699b5f72/src/include/replication/reorderbuffer.h#L260-L280
//
// See also how Materialize deserializes Postgres Debezium envelopes:
//
//	https://github.com/MaterializeInc/materialize/blob/4fca6f51338b0da9a44dd3a75a5a4a5da37a9733/src/interchange/src/avro/envelope_debezium.rs#L275
type postgresSource struct {
	sqlcapture.SourceCommon

	// This is a compact array to reduce noise in generated JSON outputs,
	// and because a lexicographic ordering is also a correct event ordering.
	Location [3]int `json:"loc" jsonschema:"description=Location of this WAL event as [last Commit.EndLSN; event LSN; current Begin.FinalLSN]. See https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html"`

	// Fields which are part of the Debezium Postgres representation but are not included here:
	// * `lsn` is the log sequence number of this event. It's equal to loc[1].
	// * `sequence` is a string-serialized JSON array which embeds a lexicographic
	//    ordering of all events. It's equal to [loc[0], loc[1]].

	TxID uint32 `json:"txid,omitempty" jsonschema:"description=The 32-bit transaction ID assigned by Postgres to the commit which produced this change."`
}

// Named constants for the LSN locations within a postgresSource.Location.
const (
	PGLocLastCommitEndLSN = 0 // Index of last Commit.EndLSN in postgresSource.Location.
	PGLocEventLSN         = 1 // Index of this event LSN in postgresSource.Location.
	PGLocBeginFinalLSN    = 2 // Index of current Begin.FinalLSN in postgresSource.Location.
)

func (s *postgresSource) Common() sqlcapture.SourceCommon {
	return s.SourceCommon
}

// A replicationStream represents the process of receiving PostgreSQL
// Logical Replication events, managing keepalives and status updates,
// and translating changes into a more friendly representation.
type replicationStream struct {
	db       *postgresDatabase
	conn     *pgconn.HijackedConn // The PostgreSQL replication connection, hijacked from PGX so we can parse the stream directly.
	pubName  string               // The name of the PostgreSQL publication to use
	replSlot string               // The name of the PostgreSQL replication slot to use

	rxbuf []byte // Logical replication message stream receive/decode buffer.
	rxoff int    // Logical replication message stream receive/decode buffer offset.

	reusedChangeEvent  *postgresChangeEvent // Reusable event object, so change decoding doesn't allocate.
	reusedCommitEvent  *postgresCommitEvent // Reusable event object, so change decoding doesn't allocate.
	reusedRowKey       []byte               // Reusable slice for row key serialization, so change decoding doesn't allocate.
	reusedBeforeValues [][]byte             // Reusable slice of wire protocol values, so change decoding doesn't allocate.
	reusedAfterValues  [][]byte             // Reusable slice of wire protocol values, so change decoding doesn't allocate.

	workerCtx       context.Context    // The context used for the worker goroutine, which is canceled when the stream is closed.
	cancelWorkerCtx context.CancelFunc // The cancel function for the worker context, used to stop the worker goroutine.

	previousFenceLSN pglogrepl.LSN // // The most recently reached fence LSN, updated at the end of each StreamToFence cycle.

	ackLSN          uint64        // The most recently Ack'd LSN, passed to startReplication or updated via CommitLSN.
	previousAckLSN  pglogrepl.LSN // The last acknowledged LSN sent to the server in a Standby Status Update. Only used to reduce log spam at INFO level.
	lastTxnEndLSN   pglogrepl.LSN // End LSN (record + 1) of the last completed transaction.
	nextTxnFinalLSN pglogrepl.LSN // Final LSN of the commit currently being processed, or zero if between transactions.
	nextTxnMillis   int64         // Unix timestamp (in millis) at which the change originally occurred.
	nextTxnXID      uint32        // XID of the commit currently being processed.

	// typeMap is a sort of type registry used when decoding values from the database.
	typeMap *pgtype.Map

	// Information about tables of possible interest, guarded by a mutex so that they can
	// be activated concurrently with ongoing replication processing.
	//
	// The `discovery` map is filled out by `ActivateTable()` calls while the `relations`
	// map is filled out by messages received from the database, and the `computed` map
	// is computed by combining the two sources of information. We have to keep track of
	// the inputs separately because a table may be activated before or after we receive
	// a RelationMessage for it, and we may receive additional RelationMessages later on
	// if the table definition changes.
	tables struct {
		sync.RWMutex
		discovery        map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo // Discovery information for active tables, keyed by StreamID.
		keyColumns       map[sqlcapture.StreamID][]string                  // Key column names for active table, keyed by StreamID.
		relations        map[uint32]*pglogrepl.RelationMessage             // Relation messages received from the database, keyed by Relation ID.
		relationStreamID map[uint32]sqlcapture.StreamID                    // Relation ID to StreamID mapping, used to look up the StreamID for a RelationMessage.
		infoCache        map[sqlcapture.StreamID]*tableProcessingInfo      // Cached processing information, keyed by StreamID.
	}
}

// tableProcessingInfo holds precomputed information about a particular active table
// so that we can efficiently process and decode its change events.
type tableProcessingInfo struct {
	StreamID          sqlcapture.StreamID        // The StreamID of this relation.
	Schema, Table     string                     // Schema and table name of this relation
	Relation          *pglogrepl.RelationMessage // The RelationMessage which describes this relation.
	KeyColumns        []int                      // Indices of key columns in the table, in key order.
	ColumnNames       []string                   // Names of all columns of the table, in table column order.
	RowKeyTranscoders []fdbTranscoder            // Transcoders for row key serialization, in table column order.
	Shared            *postgresChangeSharedInfo  // Shared information used for change event processing.
}

const standbyStatusInterval = 10 * time.Second

var (
	// streamToFenceWatchdogTimeout is the length of time after which a stream-to-fence
	// operation will error out if no further events are received when there ought to be
	// some. This should never be hit in normal operation, and exists only so that certain
	// rare failure modes produce an error rather than blocking forever.
	streamToFenceWatchdogTimeout = 12 * 60 * time.Minute

	// rxBufferSize is the initial size of the receive buffer into which replication messages are read.
	rxBufferSize = 1 * 1024 * 1024 // 1 MiB
)

func (s *replicationStream) StreamToFence(ctx context.Context, fenceAfter time.Duration, callback func(event sqlcapture.DatabaseEvent) error) error {
	// Time-based event streaming until the fenceAfter duration is reached.
	var timedEventsSinceCommit int
	var latestCommitLSN = s.previousFenceLSN
	if fenceAfter > 0 {
		logrus.WithField("cursor", latestCommitLSN).Debug("beginning timed streaming phase")
		var relayCtx, _ = context.WithTimeout(ctx, fenceAfter)
		if err := s.relayChanges(relayCtx, func(event sqlcapture.DatabaseEvent) error {
			if err := callback(event); err != nil {
				return err
			}
			timedEventsSinceCommit++
			if event, ok := event.(*postgresCommitEvent); ok {
				latestCommitLSN = event.CommitLSN
				timedEventsSinceCommit = 0
			}
			return nil
		}); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
	}
	logrus.WithField("cursor", latestCommitLSN.String()).Debug("finished timed streaming phase")

	// Establish a fence based on the latest server LSN
	var fenceLSN, err = queryLatestServerLSN(ctx, s.db.conn)
	if err != nil {
		return fmt.Errorf("error establishing WAL fence position: %w", err)
	}
	logrus.WithField("cursor", latestCommitLSN.String()).WithField("target", fenceLSN.String()).Debug("beginning fenced streaming phase")
	if fenceLSN == latestCommitLSN {
		// As an internal sanity check, we assert that it should never be possible
		// to hit this early exit unless the database has been idle since the last
		// flush event we observed.
		if timedEventsSinceCommit > 0 {
			return fmt.Errorf("internal error: sanity check failed: already at fence after processing %d changes during timed phase", timedEventsSinceCommit)
		}

		// Mark the position of the flush event as the latest fence before returning.
		s.previousFenceLSN = latestCommitLSN

		// Since we're still at a valid flush position and those are always between
		// transactions, we can safely emit a synthetic FlushEvent here. This means
		// that every StreamToFence operation ends in a flush, and is helpful since
		// there's a lot of implicit assumptions of regular events / flushes.
		*s.reusedCommitEvent = postgresCommitEvent{CommitLSN: latestCommitLSN}
		return callback(s.reusedCommitEvent)
	}

	// After establishing a target fence position, issue a watermark write. This ensures
	// that there will always be a change event whose commit LSN is greater than the target,
	// which avoids certain classes of failure which can otherwise occur when capturing
	// from an idle database.
	if !s.db.config.Advanced.ReadOnlyCapture {
		if err := s.db.WriteWatermark(ctx, uuid.New().String()); err != nil {
			return err
		}
	}

	// Stream replication events until the fence is reached or the watchdog timeout hits.
	//
	// Given that the early-exit fast path was not taken, there must be further data for
	// us to read. Thus if we sit idle for a nontrivial length of time without reaching
	// our fence position, something is wrong and we should error out instead of blocking
	// forever.
	var relayCtx, cancelRelayCtx = context.WithCancelCause(ctx)
	var fenceWatchdog = time.AfterFunc(streamToFenceWatchdogTimeout, func() {
		var err error
		if s.db.config.Advanced.ReadOnlyCapture {
			err = fmt.Errorf("replication became idle (in read-only mode) while streaming from %q to an established fence at %q", latestCommitLSN, fenceLSN.String())
		}
		err = fmt.Errorf("replication became idle while streaming from %q to an established fence at %q", latestCommitLSN, fenceLSN.String())
		cancelRelayCtx(err)
	})
	if err := s.relayChanges(relayCtx, func(event sqlcapture.DatabaseEvent) error {
		fenceWatchdog.Reset(streamToFenceWatchdogTimeout)
		if err := callback(event); err != nil {
			return err
		}

		// The first flush event whose cursor position is equal to or after the fence
		// position ends the stream-to-fence operation.
		if event, ok := event.(*postgresCommitEvent); ok {
			// It might be a bit inefficient to re-parse every flush cursor here, but
			// realistically it's probably not a significant slowdown and it would be
			// more work to preserve cursors as a typed value instead of a string.
			var eventLSN = event.CommitLSN
			if eventLSN >= fenceLSN {
				logrus.WithField("cursor", eventLSN.String()).Debug("finished fenced streaming phase")
				s.previousFenceLSN = eventLSN
				cancelRelayCtx(nil) // Stop the relay loop so we can exit cleanly. A nil cause means no error.
			}
		}
		return nil
	}); errors.Is(err, context.Canceled) {
		return context.Cause(ctx)
	} else if err != nil {
		return err
	}
	return nil
}

func (s *replicationStream) StartReplication(ctx context.Context, discovery map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo) error {
	// Launch a little helper routine to just send standby status updates regularly.
	s.workerCtx, s.cancelWorkerCtx = context.WithCancel(ctx)
	go func() {
		if err := s.sendStandbyStatusUpdate(ctx); err != nil {
			logrus.WithError(err).Fatal("failed to send standby status update")
		}
		var ticker = time.NewTicker(standbyStatusInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.workerCtx.Done():
				return
			case <-ticker.C:
				if err := s.sendStandbyStatusUpdate(ctx); err != nil {
					logrus.WithError(err).Fatal("failed to send standby status update")
				}
			}
		}
	}()

	return nil
}

func (s *replicationStream) relayChanges(ctx context.Context, callback func(event sqlcapture.DatabaseEvent) error) error {
	for ctx.Err() == nil {
		// Try to receive another message from the database.
		var msg, err = s.receiveMessage(ctx)
		if err != nil {
			return err
		}

		// Parse the XLogData header
		xld, err := pglogrepl.ParseXLogData(msg[1:])
		if err != nil {
			return fmt.Errorf("error parsing XLogData: %w", err)
		}

		// Once a message arrives, decode it and dispatch to the callback.
		event, err := s.decodeMessage(xld.WALStart, xld.WALData)
		if err != nil {
			return fmt.Errorf("error decoding message: %w", err)
		} else if event == nil {
			continue
		} else if err := callback(event); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// receiveMessage reads and returns the next logical replication message from the database,
// blocking until a message is available, the context is cancelled, or an error occurs. It
// returns the complete XLogData ('w') message bytes, which are valid only until the next
// call to receiveMessage.
func (s *replicationStream) receiveMessage(ctx context.Context) ([]byte, error) {
	for {
		// 1. Check if a complete message exists in the buffer at rxoff. If so, parse and return it.
		var msgType byte
		var msgLen int
		if len(s.rxbuf)-s.rxoff >= 5 {
			// Read the message type and length from the buffer.
			msgType = s.rxbuf[s.rxoff]
			msgLen = int(binary.BigEndian.Uint32(s.rxbuf[s.rxoff+1 : s.rxoff+5])) // Length includes the 4-byte length field itself.
			if len(s.rxbuf)-s.rxoff >= msgLen+1 {
				// Slice out this message payload and move the offset forward to after this message.
				var msgData = s.rxbuf[s.rxoff+5 : s.rxoff+1+msgLen]
				s.rxoff += msgLen + 1

				//logrus.WithFields(logrus.Fields{"type": msgType, "len": msgLen, "data": msgData}).Debug("receiveMessage")

				switch msgType {
				case 'S': // ParameterStatus
					continue // Ignore and keep looking for something we care about.
				case 'd': // CopyData messages are all we care about
					switch msgData[0] {
					case pglogrepl.PrimaryKeepaliveMessageByteID:
						var pkm, err = pglogrepl.ParsePrimaryKeepaliveMessage(msgData[1:])
						if err != nil {
							return nil, fmt.Errorf("error parsing keepalive: %w", err)
						}
						if pkm.ReplyRequested {
							if err := s.sendStandbyStatusUpdate(ctx); err != nil {
								return nil, fmt.Errorf("error sending standby status update: %w", err)
							}
						}
						continue // Keep looking for something we care about.
					case pglogrepl.XLogDataByteID:
						return msgData, nil // Return the XLogData message bytes directly.
					default:
						return nil, fmt.Errorf("unknown CopyData message: %v", msgData)
					}
				default:
					return nil, fmt.Errorf("unexpected message type %q", msgType)
				}
			}
		}

		// 2. Shift any residual data down to the start of the buffer in preparation for reading more.
		if s.rxoff > 0 {
			copy(s.rxbuf, s.rxbuf[s.rxoff:])         // Move the residual data to the start of the buffer.
			s.rxbuf = s.rxbuf[:len(s.rxbuf)-s.rxoff] // Reslice the buffer to match the new length.
			s.rxoff = 0                              // Reset the offset to the start of the buffer.
		}

		// 3. Read more data from the connection into the buffer. If the buffer is full, return an error.
		//    We will eventually need to handle this more gracefully by growing the buffer or something.
		if len(s.rxbuf) >= cap(s.rxbuf) {
			return nil, fmt.Errorf("replication buffer overflow: capacity %d filled but cannot hold message %q of length %d", cap(s.rxbuf), msgType, msgLen+1)
		}
		var n, err = s.conn.Conn.Read(s.rxbuf[len(s.rxbuf):cap(s.rxbuf)])
		//logrus.WithFields(logrus.Fields{"residual": len(s.rxbuf), "received": n}).Warn("receiveMessage: read from connection")
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("connection closed while reading messages: %w", err)
			}
			return nil, fmt.Errorf("error reading messages from connection: %w", err)
		}
		s.rxbuf = s.rxbuf[:len(s.rxbuf)+n] // Resize the buffer to include the new data.
	}
}

func (s *replicationStream) decodeMessage(lsn pglogrepl.LSN, data []byte) (sqlcapture.DatabaseEvent, error) {
	switch pglogrepl.MessageType(data[0]) {
	// Transaction Control: Begin and Commit
	case pglogrepl.MessageTypeBegin:
		var msg pglogrepl.BeginMessage
		if err := msg.Decode(data[1:]); err != nil {
			return nil, fmt.Errorf("error decoding BEGIN message: %w", err)
		}
		if s.nextTxnFinalLSN != 0 {
			return nil, fmt.Errorf("got BEGIN message while another transaction in progress")
		}
		s.nextTxnFinalLSN = msg.FinalLSN
		s.nextTxnMillis = msg.CommitTime.UnixMilli()
		s.nextTxnXID = msg.Xid
		return nil, nil
	case pglogrepl.MessageTypeCommit:
		var msg pglogrepl.CommitMessage
		if err := msg.Decode(data[1:]); err != nil {
			return nil, fmt.Errorf("error decoding COMMIT message: %w", err)
		}
		if s.nextTxnFinalLSN == 0 {
			return nil, fmt.Errorf("got COMMIT message without a transaction in progress")
		} else if s.nextTxnFinalLSN != msg.CommitLSN {
			return nil, fmt.Errorf("got COMMIT message with unexpected CommitLSN (%d; expected %d)",
				msg.CommitLSN, s.nextTxnFinalLSN)
		}
		s.nextTxnFinalLSN = 0
		s.nextTxnMillis = 0
		s.nextTxnXID = 0
		s.lastTxnEndLSN = msg.TransactionEndLSN

		*s.reusedCommitEvent = postgresCommitEvent{CommitLSN: msg.TransactionEndLSN}
		return s.reusedCommitEvent, nil

	// Change Events: Insert, Update, and Delete
	case pglogrepl.MessageTypeInsert:
		// Insert
		//   Byte1('I')   Identifies the message as an insert message.
		//   -- Int32 (XID)  XID of the transaction (only present for streamed transactions). (Only in protocol v2)
		//   Int32 (OID)  OID of the relation corresponding to the ID in the relation message.
		//   Byte1('N')   Identifies the following TupleData message as a new tuple.
		//   TupleData    TupleData message part representing the contents of new tuple.
		if len(data) < 8 {
			return nil, fmt.Errorf("INSERT message too short: %d bytes, expected at least 8", len(data))
		}
		var relID = binary.BigEndian.Uint32(data[1:5])
		if data[5] != 'N' {
			return nil, fmt.Errorf("expected 'N' tuple type in INSERT message, got %q", data[5])
		}
		var tupleData = data[6:]
		return s.decodeChangeEvent(sqlcapture.InsertOp, lsn, 0, nil, tupleData, relID)
	case pglogrepl.MessageTypeUpdate:
		// Update
		//   Byte1('U')   Identifies the message as an update message.
		//   -- Int32 (XID)  XID of the transaction (only present for streamed transactions). (Only in protocol v2)
		//   Int32 (OID)  OID of the relation corresponding to the ID in the relation message.
		//   Byte1('K')   Identifies the following TupleData submessage as a key. This field is optional and is only present if the update changed data in any of the column(s) that are part of the REPLICA IDENTITY index.
		//   Byte1('O')   Identifies the following TupleData submessage as an old tuple. This field is optional and is only present if table in which the update happened has REPLICA IDENTITY set to FULL.
		//   TupleData    TupleData message part representing the contents of the old tuple or primary key. Only present if the previous 'O' or 'K' part is present.
		//   Byte1('N')   Identifies the following TupleData message as a new tuple.
		//   TupleData    TupleData message part representing the contents of a new tuple.
		// The Update message may contain either a 'K' message part or an 'O' message part or neither of them, but never both of them.
		if len(data) < 6 {
			return nil, fmt.Errorf("UPDATE message too short: %d bytes, expected at least 6", len(data))
		}
		var relID = binary.BigEndian.Uint32(data[1:5])
		var oldTupleType byte
		var oldTuple []byte
		var newTuple []byte
		switch data[5] {
		case 'K', 'O': // Key or old tuple followed by new tuple
			oldTupleType = data[5]
			var idx = scanTupleEnd(data[6:]) // Get the index of the next byte after the old tuple
			if data[idx] != 'N' {
				return nil, fmt.Errorf("expected 'N' tuple type after 'K' or 'O' in UPDATE message, got %q", data[idx])
			}
			oldTuple = data[6:idx]
			newTuple = data[idx+1:]
		case 'N': // New tuple only
			oldTupleType = 0 // No old tuple
			oldTuple = nil
			newTuple = data[6:]

		default:
			return nil, fmt.Errorf("expected 'N', 'K', or 'O' tuple type in UPDATE message, got %q", data[6])
		}
		return s.decodeChangeEvent(sqlcapture.UpdateOp, lsn, oldTupleType, oldTuple, newTuple, relID)

	case pglogrepl.MessageTypeDelete:
		// Delete
		//   Byte1('D')   Identifies the message as a delete message.
		//   -- Int32 (XID)  XID of the transaction (only present for streamed transactions). (Only in protocol v2)
		//   Int32 (OID)  OID of the relation corresponding to the ID in the relation message.
		//   Byte1('K')   Identifies the following TupleData submessage as a key. This field is present if the table in which the delete has happened uses an index as REPLICA IDENTITY.
		//   Byte1('O')   Identifies the following TupleData message as an old tuple. This field is present if the table in which the delete happened has REPLICA IDENTITY set to FULL.
		//   TupleData    TupleData message part representing the contents of the old tuple or primary key, depending on the previous field.
		// The Delete message may contain either a 'K' message part or an 'O' message part, but never both of them.
		if len(data) < 8 {
			return nil, fmt.Errorf("DELETE message too short: %d bytes, expected at least 8", len(data))
		}
		var relID = binary.BigEndian.Uint32(data[1:5])
		var oldTupleType = data[5]
		if oldTupleType != 'K' && oldTupleType != 'O' {
			return nil, fmt.Errorf("expected 'O' or 'K' tuple type in DELETE message, got %q", oldTupleType)
		}
		var tupleData = data[6:]
		return s.decodeChangeEvent(sqlcapture.DeleteOp, lsn, oldTupleType, tupleData, nil, relID)

	// Other Messages: Origin, Relation, Type, and Truncate
	case pglogrepl.MessageTypeOrigin:
		// Origin messages are sent when the postgres instance we're capturing from
		// is itself replicating from another source instance. They indicate the original
		// source of the transaction. We just ignore these messages for now, though in the
		// future it might be desirable to add the origin as a `_meta` property. Sauce:
		// https://www.highgo.ca/2020/04/18/the-origin-in-postgresql-logical-decoding/
		var msg pglogrepl.OriginMessage
		if err := msg.Decode(data[1:]); err != nil {
			return nil, fmt.Errorf("error decoding ORIGIN message: %w", err)
		}
		logrus.WithFields(logrus.Fields{
			"originName": msg.Name,
			"originLSN":  msg.CommitLSN,
		}).Trace("ignoring Origin message")
		return nil, nil
	case pglogrepl.MessageTypeRelation:
		var msg pglogrepl.RelationMessage
		if err := msg.Decode(data[1:]); err != nil {
			return nil, fmt.Errorf("error decoding RELATION message: %w", err)
		}
		return nil, s.handleRelationMessage(&msg)
	case pglogrepl.MessageTypeType:
		// There are five kinds of user-defined datatype in Postgres:
		//  - Domain types declared via 'CREATE DOMAIN name AS data_type'
		//  - Tuples declared via 'CREATE TYPE name AS (...elements...)'
		//  - Ranges declared via 'CREATE TYPE name AS RANGE (SUBTYPE = subtype, ...)'
		//  - Enums declared via 'CREATE TYPE name AS ENUM (...values...)'
		//  - Custom scalars defined with input and output functions
		//
		// We ignore custom scalar types as an option because they're incredibly niche.
		//
		// When a TypeMessage informs us about a tuple, range, or enum type it gives
		// us the OID of the type and the 'schema.typename' name of the custom type.
		// We receive no information about the element types or legal values of the
		// user-defined type, and so we don't bother to do anything with the info and
		// instead just let our unknown OID handling treat them as generic text.
		//
		// When a TypeMessage informs us about a *domain* type however, it gives us
		// the OID of the type and the 'schema.typename' of the *base type*. When the
		// base type is a builtin Postgres datatype the schema is omitted, and this
		// implicitly means 'pg_catalog'.
		//
		// Ideally we want domain types to be decoded the same as a value of whichever
		// base datatype it corresponds to. This is accomplished using a bit of logic
		// that looks up the base name in the type map and then registers the user type
		// OID as having the same name and codec.
		var msg pglogrepl.TypeMessage
		if err := msg.Decode(data[1:]); err != nil {
			return nil, fmt.Errorf("error decoding TYPE message: %w", err)
		}
		logrus.WithFields(logrus.Fields{
			"oid":       msg.DataType,
			"namespace": msg.Namespace,
			"name":      msg.Name,
		}).Debug("user type definition")
		if msg.Namespace == "" {
			if baseType, ok := s.typeMap.TypeForName(msg.Name); ok {
				s.typeMap.RegisterType(&pgtype.Type{
					OID:   msg.DataType,
					Name:  baseType.Name,
					Codec: baseType.Codec,
				})
			} else {
				logrus.WithFields(logrus.Fields{
					"oid":  msg.DataType,
					"name": msg.Name,
				}).Warn("unknown type name for user-defined type")
			}
		}
		return nil, nil
	case pglogrepl.MessageTypeTruncate:
		var msg pglogrepl.TruncateMessage
		if err := msg.Decode(data[1:]); err != nil {
			return nil, fmt.Errorf("error decoding TRUNCATE message: %w", err)
		}
		for _, relID := range msg.RelationIDs {
			var streamID, ok = s.streamIDFromRelationID(relID)
			if !ok {
				return nil, fmt.Errorf("got TRUNCATE message for unknown relation ID %d", relID)
			}
			if s.tableActive(streamID) {
				logrus.WithField("table", streamID).Warn("ignoring TRUNCATE on active table")
			}
		}
		return nil, nil

	default:
		// There shouldn't be any other message types in the replication stream,
		// but we might as well handle them gracefully if they show up.
		if len(data) > 8 {
			data = data[:8]
		}
		return nil, fmt.Errorf("unsupported replication message type %q in %q", data[0], string(data)+"...")
	}
}

func (s *replicationStream) decodeChangeEvent(
	op sqlcapture.ChangeOp, // Operation of this event.
	lsn pglogrepl.LSN, // LSN of this event.
	beforeType uint8, // Postgres TupleType (0, 'K' for key, 'O' for old full tuple, 'N' for new).
	before, after []byte, // Before and after tuple data. Either may be nil.
	relID uint32, // Relation ID to which tuple data pertains.
) (sqlcapture.DatabaseEvent, error) {
	if s.nextTxnFinalLSN == 0 {
		return nil, fmt.Errorf("got %q message without a transaction in progress", op)
	}

	var info, active, err = s.getTableProcessingInfo(relID)
	if err != nil {
		return nil, fmt.Errorf("error getting table processing info: %w", err)
	}
	// If this change event is on a table we're not capturing, skip doing any further processing on it.
	if !active {
		// Return a KeepaliveEvent to indicate that we're actively receiving and discarding
		// change data. This avoids situations where a sufficiently large transaction full
		// of changes on a not-currently-active table causes the fenced streaming watchdog
		// to trigger.
		return &sqlcapture.KeepaliveEvent{}, nil
	}

	// Decode the before and after tuples into slices of wire protocol values.
	beforeTupleValues, err := postgresTupleValues(&s.reusedBeforeValues, before, beforeType, nil)
	if err != nil {
		return nil, fmt.Errorf("error decoding 'before' tuple: %w", err)
	}
	afterTupleValues, err := postgresTupleValues(&s.reusedAfterValues, after, 'N', beforeTupleValues)
	if err != nil {
		return nil, fmt.Errorf("error decoding 'after' tuple: %w", err)
	}

	// Shuffle things around so that we have a consistent concept of "values" versus
	// the before-values which are only present for updates.
	var values, beforeValues [][]byte
	switch op {
	case sqlcapture.InsertOp:
		values = afterTupleValues
	case sqlcapture.UpdateOp:
		values = afterTupleValues
		beforeValues = beforeTupleValues
	case sqlcapture.DeleteOp:
		values = beforeTupleValues
	default:
		return nil, fmt.Errorf("unexpected change operation %q", op)
	}

	// Compute row key for this change event, if key columns are defined.
	var rowKey []byte
	if info.KeyColumns != nil {
		rowKey = s.reusedRowKey[:0] // Reset the reused row key buffer.
		for _, n := range info.KeyColumns {
			rowKey, err = info.RowKeyTranscoders[n](rowKey, values[n])
			if err != nil {
				return nil, fmt.Errorf("error encoding row key column %q at index %d: %w", info.ColumnNames[n], n, err)
			}
		}
		s.reusedRowKey = rowKey // Update in case of resizing
	}

	_ = beforeValues // TODO(wgd): Include the before tuple values so that updates with full REPLICA IDENTITY work correctly.
	*s.reusedChangeEvent = postgresChangeEvent{
		Info: info.Shared,
		Meta: postgresChangeMetadata{
			Operation: op,
			Source: postgresSource{
				SourceCommon: sqlcapture.SourceCommon{
					Millis: s.nextTxnMillis,
					Schema: info.Schema,
					Table:  info.Table,
				},
				Location: [3]int{
					int(s.lastTxnEndLSN),
					int(lsn),
					int(s.nextTxnFinalLSN),
				},
			},
		},
		RowKey: rowKey,
		Values: values,
	}
	if s.db.includeTxIDs[info.StreamID] {
		s.reusedChangeEvent.Meta.Source.TxID = s.nextTxnXID
	}
	return s.reusedChangeEvent, nil
}

func scanTupleEnd(data []byte) int {
	// TupleData
	//   Int16    Number of columns.
	//
	// Next, one of the following submessages appears for each column (except generated columns):
	//   Byte1('n')  Identifies the data as NULL value.
	//   Byte1('u')  Identifies unchanged TOASTed value (the actual value is not sent).
	//   Byte1('t')  Identifies the data as text formatted value.
	//   Byte1('b')  Identifies the data as binary formatted value.
	//   Int32       Length of the column value.
	//   Byten       The value of the column, either in binary or in text format. (As specified in the preceding format byte). n is the above length.
	var off = 0                                     // Offset into the data slice
	var ncol = int16(binary.BigEndian.Uint16(data)) // Number of columns in the tuple
	for _ = range ncol {
		switch data[off] {
		case 'n', 'u': // Move past the type byte, no value.
			off += 1
		case 't', 'b': // Move past the type byte, length, and value.
			off += 5 + int(binary.BigEndian.Uint32(data[off+1:]))
		}
	}
	return off
}

func postgresTupleValues(valsPtr *[][]byte, data []byte, tupleType uint8, before [][]byte) ([][]byte, error) {
	if data == nil {
		return nil, nil
	}
	if tupleType != 0 && tupleType != 'K' && tupleType != 'O' && tupleType != 'N' {
		return nil, fmt.Errorf("unexpected tuple type %q", tupleType)
	}

	var off = 2                                   // Offset into the tuple data slice
	var ncol = int(binary.BigEndian.Uint16(data)) // Number of columns in the tuple
	var vals = (*valsPtr)[:0]                     // Load the reusable values slice and reset to zero
	for i := range ncol {
		switch data[off] {
		case 'n':
			vals = append(vals, nil)
			off += 1
		case 'u':
			vals = append(vals, before[i])
			off += 1
		case 'b':
			// This is a binary value, which we don't currently support.
			return nil, fmt.Errorf("binary column data type 'b' is not supported in replication")
		case 't':
			// Text value, read the length and then the value.
			off += 1
			var valueSize = int(binary.BigEndian.Uint32(data[off:]))
			off += 4
			vals = append(vals, data[off:off+valueSize])
			off += valueSize
		}
	}
	*valsPtr = vals // Store back into the pointer in case it was resized.
	return vals, nil
}

func (s *replicationStream) handleRelationMessage(msg *pglogrepl.RelationMessage) error {
	var streamID = sqlcapture.JoinStreamID(msg.Namespace, msg.RelationName)
	s.tables.Lock()
	s.tables.relations[msg.RelationID] = msg
	s.tables.relationStreamID[msg.RelationID] = streamID
	delete(s.tables.infoCache, streamID) // Invalidate cached table processing info.
	s.tables.Unlock()
	return nil
}

func (s *replicationStream) ActivateTable(ctx context.Context, streamID sqlcapture.StreamID, keyColumns []string, discovery *sqlcapture.DiscoveryInfo, metadataJSON json.RawMessage) error {
	s.tables.Lock()
	s.tables.discovery[streamID] = discovery
	s.tables.keyColumns[streamID] = keyColumns
	delete(s.tables.infoCache, streamID) // Invalidate cached table processing info.
	s.tables.Unlock()
	return nil
}

func (s *replicationStream) streamIDFromRelationID(relID uint32) (sqlcapture.StreamID, bool) {
	s.tables.RLock()
	var streamID, ok = s.tables.relationStreamID[relID]
	s.tables.RUnlock()
	return streamID, ok
}

func (s *replicationStream) tableActive(streamID sqlcapture.StreamID) bool {
	s.tables.RLock()
	var _, ok = s.tables.discovery[streamID]
	s.tables.RUnlock()
	return ok
}

func (s *replicationStream) keyColumns(streamID sqlcapture.StreamID) ([]string, bool) {
	s.tables.RLock()
	var keyColumns, ok = s.tables.keyColumns[streamID]
	s.tables.RUnlock()
	return keyColumns, ok
}

// getTableProcessingInfo retrieves the processing info for a table given its relation ID.
// If the info is not already cached, it computes it and stores it in the cache.
// It returns the processing info, a boolean indicating if the table is active, and an error.
// An inactive table returns info=nil and active=false, but no error.
func (s *replicationStream) getTableProcessingInfo(relID uint32) (info *tableProcessingInfo, active bool, err error) {
	// If we have cached processing info for this relation ID, return it. It should never
	// be possible to have a relation ID here for which we don't know the stream ID, but
	// we have an error check just in case.
	s.tables.RLock()
	streamID, ok := s.streamIDFromRelationID(relID)
	if !ok {
		s.tables.RUnlock()
		return nil, false, fmt.Errorf("unknown relation ID %d", relID)
	}
	cachedInfo, ok := s.tables.infoCache[streamID]
	if ok {
		s.tables.RUnlock()
		return cachedInfo, true, nil
	}
	s.tables.RUnlock()

	// Acquire a write lock so we can update the cache.
	s.tables.Lock()
	defer s.tables.Unlock()

	discovery, ok := s.tables.discovery[streamID]
	if !ok {
		return nil, false, nil
	}
	var keyColumns, _ = s.tables.keyColumns[streamID]  // Skipping 'ok' check here because keyColumns should always be in sync with discovery.
	var relationMessage, _ = s.tables.relations[relID] // Skipping 'ok' check here because relations should always be in sync with streamIDFromRelationID

	processingInfo, err := s.computeTableProcessingInfo(streamID, discovery, keyColumns, relationMessage)
	if err != nil {
		return nil, false, fmt.Errorf("error computing table processing info for %q: %w", streamID, err)
	}
	s.tables.infoCache[streamID] = processingInfo
	return processingInfo, true, nil
}

func (s *replicationStream) computeTableProcessingInfo(
	streamID sqlcapture.StreamID,
	discovery *sqlcapture.DiscoveryInfo,
	keyColumnNames []string,
	relationMessage *pglogrepl.RelationMessage,
) (*tableProcessingInfo, error) {
	var keyColumns = make([]int, len(keyColumnNames))                            // Indices of key columns in the table, in key order.
	var columnNames = make([]string, len(relationMessage.Columns))               // Names of all columns, in table order.
	var outputColumnNames = make([]string, len(relationMessage.Columns))         // Names of all columns with omitted columns set to "", in table order.
	var outputTranscoders = make([]jsonTranscoder, len(relationMessage.Columns)) // Transcoders from DB values to JSON, with omitted columns set to nil.
	var rowKeyTranscoders = make([]fdbTranscoder, len(relationMessage.Columns))  // Transcoders from DB values to FDB row keys, with non-key columns set to nil.

	for idx, col := range relationMessage.Columns {
		columnNames[idx] = col.Name
		outputColumnNames[idx] = col.Name
		var columnInfo, ok = discovery.Columns[col.Name]
		if !ok {
			return nil, fmt.Errorf("column %q not found in discovery info for relation %q", col.Name, streamID)
		}
		var isPrimaryKey = slices.Contains(discovery.PrimaryKey, col.Name)
		outputTranscoders[idx] = s.db.replicationJSONTranscoder(s.typeMap, col.DataType, &columnInfo, isPrimaryKey)
		if slices.Contains(keyColumnNames, col.Name) {
			rowKeyTranscoders[idx] = s.db.replicationFDBTranscoder(s.typeMap, col.DataType, &columnInfo)
		}
	}

	// Add the _meta property to the end of the output columns list. This matches up with
	// a special-case in the postgresChangeEvent serialization logic.
	outputColumnNames = append(outputColumnNames, "_meta")

	for keyIndex, keyColName := range keyColumnNames {
		var columnIndex = slices.Index(columnNames, keyColName)
		if columnIndex < 0 {
			return nil, fmt.Errorf("key column %q not found in relation %q", keyColName, streamID)
		}
		keyColumns[keyIndex] = columnIndex
	}

	return &tableProcessingInfo{
		StreamID:          streamID,
		Schema:            relationMessage.Namespace,
		Table:             relationMessage.RelationName,
		Relation:          relationMessage,
		KeyColumns:        keyColumns,
		ColumnNames:       columnNames,
		RowKeyTranscoders: rowKeyTranscoders,
		Shared: &postgresChangeSharedInfo{
			StreamID:    streamID,
			Shape:       encrow.NewShape(outputColumnNames),
			Transcoders: outputTranscoders,
		},
	}, nil
}

// Acknowledge informs the ReplicationStream that all messages up to the specified
// LSN [1] have been persisted, and that a future restart will never need to return
// to older portions of the transaction log. This fact will be communicated to the
// database in a periodic status update, whereupon the replication slot's "Restart
// LSN" may be advanced accordingly.
//
// [1] The handling of LSNs and replication slot advancement is complicated, but
// luckily most of the complexity is handled within PostgreSQL. Just be aware that
// we're not necessarily receiving messages in the literal order that they appear
// in the WAL, and that PostgreSQL is doing a lot of Magic behind the scenes in
// order to present the illusion that these changes occurred in order without any
// interleaving between transactions.
//
// Thus you shouldn't expect that a specific "Committed LSN" value will necessarily
// advance the "Restart LSN" to the same point, but so long as you ignore the details
// things will work out in the end.
func (s *replicationStream) Acknowledge(ctx context.Context, cursorJSON json.RawMessage) error {
	var cursor, err = unmarshalJSONString(cursorJSON)
	if err != nil {
		return fmt.Errorf("error unmarshalling acknowledge cursor: %w", err)
	}
	if cursor == "" {
		// The empty cursor will be acknowledged once at startup when all bindings
		// are new, because the initial state checkpoint will have an unspecified
		// cursor value on purpose. We don't need to do anything with those.
		return nil
	}
	lsn, err := pglogrepl.ParseLSN(cursor)
	if err != nil {
		return fmt.Errorf("error parsing acknowledge cursor: %w", err)
	}
	atomic.StoreUint64(&s.ackLSN, uint64(lsn))
	return nil
}

func (s *replicationStream) sendStandbyStatusUpdate(ctx context.Context) error {
	var ackLSN = pglogrepl.LSN(atomic.LoadUint64(&s.ackLSN))
	if ackLSN != s.previousAckLSN {
		// Log at info level whenever we're about to confirm a different LSN from last time.
		logrus.WithField("ackLSN", ackLSN.String()).Info("advancing confirmed LSN")
		s.previousAckLSN = ackLSN
	} else {
		logrus.WithField("ackLSN", ackLSN.String()).Debug("sending Standby Status Update")
	}

	// Message payload fields
	var walWritePosition = ackLSN
	var walFlushPosition = ackLSN
	var walApplyPosition = ackLSN
	var clientTime = time.Now()
	var replyRequested = false

	// Convert client time to microseconds since the Y2K epoch, since that's what the protocol message uses.
	const microsecFromUnixEpochToY2K = 946684800 * 1000000
	var clientTimeMicroseconds = clientTime.Unix()*1000000 + int64(clientTime.Nanosecond())/1000
	clientTimeMicroseconds -= microsecFromUnixEpochToY2K // Convert to microseconds since the Y2K epoch.

	// Construct message bytes (1 byte type, 4 byte length, 34 byte body)
	var msg = make([]byte, 0, 39)
	msg = append(msg, 'd')          // Message Type (CopyData = 'd')
	msg = pgio.AppendInt32(msg, 38) // Length (message body plus this length field)
	msg = append(msg, pglogrepl.StandbyStatusUpdateByteID)
	msg = pgio.AppendUint64(msg, uint64(walWritePosition))
	msg = pgio.AppendUint64(msg, uint64(walFlushPosition))
	msg = pgio.AppendUint64(msg, uint64(walApplyPosition))
	msg = pgio.AppendInt64(msg, clientTimeMicroseconds)
	if replyRequested {
		msg = append(msg, 1)
	} else {
		msg = append(msg, 0)
	}

	if _, err := s.conn.Conn.Write(msg); err != nil {
		return fmt.Errorf("error sending Standby Status Update: %w", err)
	}
	return nil
}

func (s *replicationStream) Close(ctx context.Context) error {
	logrus.Debug("replication stream close requested")

	// Convert back to a pgconn connection so we can close it cleanly with a termination message.
	var conn, err = pgconn.Construct(s.conn)
	if err != nil {
		return fmt.Errorf("error constructing pgconn from connection: %w", err)
	}
	conn.Close(ctx)
	s.cancelWorkerCtx() // Stop the standby status update goroutine.
	return nil
}

func (db *postgresDatabase) ReplicationDiagnostics(ctx context.Context) error {
	var query = func(q string) {
		logrus.WithField("query", q).Info("running diagnostics query")
		var result, err = db.conn.Query(ctx, q)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"query": q,
				"err":   err,
			}).Error("unable to execute diagnostics query")
			return
		}
		defer result.Close()

		var numResults int
		var keys = result.FieldDescriptions()
		for result.Next() {
			numResults++
			var row, err = result.Values()
			if err != nil {
				logrus.WithField("err", err).Error("unable to process result row")
				continue
			}

			var logFields = logrus.Fields{}
			for idx, val := range row {
				logFields[string(keys[idx].Name)] = val
			}
			logrus.WithFields(logFields).Info("got diagnostic row")
		}
		if numResults == 0 {
			logrus.WithField("query", q).Info("no results")
		}
	}

	query("SELECT * FROM " + db.WatermarksTable().String() + ";")
	query("SELECT * FROM pg_replication_slots;")
	query("SELECT pg_current_wal_flush_lsn(), pg_current_wal_insert_lsn(), pg_current_wal_lsn();")
	return nil
}

// marshalJSONString returns the serialized JSON representation of the provided string.
//
// A Go string can always be successfully serialized into JSON.
func marshalJSONString(str string) json.RawMessage {
	var bs, err = json.Marshal(str)
	if err != nil {
		panic(fmt.Errorf("internal error: failed to marshal string %q to JSON: %w", str, err))
	}
	return bs
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
