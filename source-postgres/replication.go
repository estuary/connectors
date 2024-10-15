package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/estuary/connectors/sqlcapture"
	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/sirupsen/logrus"
)

var slotInUseRe = regexp.MustCompile(`replication slot ".*" is active for PID`)

func (db *postgresDatabase) ReplicationStream(ctx context.Context, startCursor string) (sqlcapture.ReplicationStream, error) {
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

	var startLSN pglogrepl.LSN
	if startCursor != "" {
		startLSN, err = pglogrepl.ParseLSN(startCursor)
		if err != nil {
			return nil, fmt.Errorf("error parsing start cursor: %w", err)
		}
	} else {
		// If no start cursor is specified, initialize to the current WAL flush position
		// obtained via the `IDENTIFY_SYSTEM` command.
		var sysident, err = pglogrepl.IdentifySystem(ctx, conn)
		if err != nil {
			return nil, fmt.Errorf("unable to read WAL flush LSN from database: %w", err)
		}
		startLSN = sysident.XLogPos
	}

	var slot, publication = db.config.Advanced.SlotName, db.config.Advanced.PublicationName

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
	} else if startLSN < slotInfo.ConfirmedFlushLSN {
		logrus.WithFields(logrus.Fields{
			"slot":         slot,
			"confirmedLSN": slotInfo.ConfirmedFlushLSN.String(),
			"startLSN":     startLSN.String(),
		}).Warn("replication slot confirmed_flush_lsn greater than connector start LSN (this means it was probably deleted+recreated and all bindings need to be backfilled)")
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

	var typeMap = pgtype.NewMap()
	if err := registerDatatypeTweaks(typeMap); err != nil {
		return nil, err
	}

	var stream = &replicationStream{
		db:       db,
		conn:     conn,
		pubName:  publication,
		replSlot: slot,

		ackLSN:          uint64(startLSN),
		lastTxnEndLSN:   startLSN,
		nextTxnFinalLSN: 0,
		nextTxnMillis:   0,
		typeMap:         typeMap,
		relations:       make(map[uint32]*pglogrepl.RelationMessage),
		// standbyStatusDeadline is left uninitialized so an update will be sent ASAP
	}
	stream.tables.active = make(map[string]struct{})
	stream.tables.keyColumns = make(map[string][]string)
	stream.tables.discovery = make(map[string]*sqlcapture.DiscoveryInfo)
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
	conn     *pgconn.PgConn // The PostgreSQL replication connection
	pubName  string         // The name of the PostgreSQL publication to use
	replSlot string         // The name of the PostgreSQL replication slot to use

	cancel   context.CancelFunc            // Cancel function for the replication goroutine's context
	errCh    chan error                    // Error channel for the final exit status of the replication goroutine
	events   chan sqlcapture.DatabaseEvent // The channel to which replication events will be written
	eventBuf sqlcapture.DatabaseEvent      // A single-element buffer used in between 'receiveMessage' and the output channel

	ackLSN          uint64        // The most recently Ack'd LSN, passed to startReplication or updated via CommitLSN.
	previousAckLSN  pglogrepl.LSN // The last acknowledged LSN sent to the server in a Standby Status Update. Only used to reduce log spam at INFO level.
	lastTxnEndLSN   pglogrepl.LSN // End LSN (record + 1) of the last completed transaction.
	nextTxnFinalLSN pglogrepl.LSN // Final LSN of the commit currently being processed, or zero if between transactions.
	nextTxnMillis   int64         // Unix timestamp (in millis) at which the change originally occurred.
	nextTxnXID      uint32        // XID of the commit currently being processed.

	// standbyStatusDeadline is the time at which we need to stop receiving
	// replication messages and go send a Standby Status Update message to
	// the DB.
	standbyStatusDeadline time.Time

	// typeMap is a sort of type registry used when decoding values from the database.
	typeMap *pgtype.Map

	// relations keeps track of all "Relation Messages" from the database. These
	// messages tell us about the integer ID corresponding to a particular table
	// and other information about the table structure at a particular moment.
	relations map[uint32]*pglogrepl.RelationMessage

	// The 'active tables' set, guarded by a mutex so it can be modified from
	// the main goroutine while it's read by the replication goroutine.
	tables struct {
		sync.RWMutex
		active     map[string]struct{}
		keyColumns map[string][]string
		discovery  map[string]*sqlcapture.DiscoveryInfo
	}
}

const standbyStatusInterval = 10 * time.Second

// replicationBufferSize controls how many change events can be buffered in the
// replicationStream before it stops receiving further events from PostgreSQL.
// In normal use it's a constant, it's just a variable so that tests are more
// likely to exercise blocking sends and backpressure.
//
// This buffer has been set to a fairly small value, because larger buffers can
// cause OOM kills when the incoming data rate exceeds the rate at which we're
// serializing data and getting it into Gazette journals.
var replicationBufferSize = 16

func (s *replicationStream) StreamToFence(ctx context.Context, fenceAfter time.Duration, callback func(event sqlcapture.DatabaseEvent) error) error {
	// Time-based event streaming until the fenceAfter duration is reached.
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
			case event, ok := <-s.events:
				if !ok {
					return sqlcapture.ErrFenceNotReached
				} else if err := callback(event); err != nil {
					return err
				}
			}
		}
	}

	// Establish a watermark-based fence position.
	var fenceWatermark = uuid.New().String()
	if err := s.db.WriteWatermark(ctx, fenceWatermark); err != nil {
		return fmt.Errorf("error establishing watermark fence: %w", err)
	}

	// Stream replication events until the fence is reached.
	var fenceReached = false
	var watermarkStreamID = s.db.WatermarksTable()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-s.events:
			if !ok {
				return sqlcapture.ErrFenceNotReached
			} else if err := callback(event); err != nil {
				return err
			}

			// Mark the fence as reached when we observe a change event on the watermarks stream
			// with the expected value.
			if event, ok := event.(*sqlcapture.ChangeEvent); ok {
				if event.Operation != sqlcapture.DeleteOp && event.Source.Common().StreamID() == watermarkStreamID {
					var actual = event.After["watermark"]
					if actual == nil {
						actual = event.After["WATERMARK"]
					}
					logrus.WithFields(logrus.Fields{"expected": fenceWatermark, "actual": actual}).Debug("watermark change")
					if actual == fenceWatermark {
						fenceReached = true
					}
				}
			}

			// The flush event following the watermark change ends the stream-to-fence operation.
			if _, ok := event.(*sqlcapture.FlushEvent); ok && fenceReached {
				return nil
			}
		}
	}
}

func (s *replicationStream) StartReplication(ctx context.Context, discovery map[sqlcapture.StreamID]*sqlcapture.DiscoveryInfo) error {
	// Activate replication for the watermarks table.
	var watermarks = s.db.config.Advanced.WatermarksTable
	var watermarksInfo = discovery[watermarks]
	if watermarksInfo == nil {
		return fmt.Errorf("error activating replication for watermarks table %q: table missing from latest autodiscovery", watermarks)
	}
	if err := s.ActivateTable(ctx, watermarks, watermarksInfo.PrimaryKey, watermarksInfo, nil); err != nil {
		return fmt.Errorf("error activating replication for watermarks table %q: %w", watermarks, err)
	}

	var streamCtx, streamCancel = context.WithCancel(ctx)
	s.events = make(chan sqlcapture.DatabaseEvent, replicationBufferSize)
	s.errCh = make(chan error)
	s.cancel = streamCancel

	go func() {
		var err = s.run(streamCtx)
		// Context cancellation typically occurs only in tests, and for test stability
		// it should be considered a clean shutdown and not necessarily an error.
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		// Unexpected EOF errors come from dropped connections or database restarts
		// and should be considered a clean shutdown and not necessarily an error since
		// most of the time we'll just restart and continue where we left off.
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = nil
		}
		// Always take up to 1 second to notify the database that we're done
		var closeCtx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		s.conn.Close(closeCtx)
		cancel()
		close(s.events)
		s.errCh <- err
	}()
	return nil
}

// run is the main loop of the replicationStream which combines message
// receiving/relaying with periodic standby status updates.
func (s *replicationStream) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if time.Now().After(s.standbyStatusDeadline) {
			if err := s.sendStandbyStatusUpdate(ctx); err != nil {
				return fmt.Errorf("failed to send status update: %w", err)
			}
			s.standbyStatusDeadline = time.Now().Add(standbyStatusInterval)
		}

		var workCtx, cancelWorkCtx = context.WithDeadline(ctx, s.standbyStatusDeadline)
		var err = s.relayMessages(workCtx)
		cancelWorkCtx()
		if err != nil {
			return fmt.Errorf("failed to relay messages: %w", err)
		}
	}
}

// relayMessages receives logical replication messages from PostgreSQL and sends
// them on the stream's output channel until its context is cancelled. This is
// expected to happen every 10 seconds, so that a standby status update can be
// sent.
func (s *replicationStream) relayMessages(ctx context.Context) error {
	for {
		// If there's already a change event which needs to be sent to the consumer,
		// try to do so until/unless the context expires first.
		if s.eventBuf != nil {
			select {
			case <-ctx.Done():
				return nil
			case s.events <- s.eventBuf:
				s.eventBuf = nil
			}
		}

		// In tbe absence of a buffered message, go try to receive another from
		// the database.
		var lsn, msg, err = s.receiveMessage(ctx)
		if pgconn.Timeout(err) {
			return nil
		}
		if err != nil {
			return err
		}

		// Once a message arrives, decode it and buffer the result until the next
		// time this function is invoked.
		event, err := s.decodeMessage(lsn, msg)
		if err != nil {
			return fmt.Errorf("error decoding message: %w", err)
		}
		s.eventBuf = event
	}
}

func (s *replicationStream) decodeMessage(lsn pglogrepl.LSN, msg pglogrepl.Message) (sqlcapture.DatabaseEvent, error) {
	// Some notes on the Logical Replication / pgoutput message stream, since
	// as far as I can tell this isn't documented anywhere but comments in the
	// relevant PostgreSQL sources.
	//
	// The stream of messages we're processing here isn't necessarily in the
	// same order as the underlying PostgreSQL WAL. The 'Reorder Buffer' logic
	// (https://github.com/postgres/postgres/blob/master/src/backend/replication/logical/reorderbuffer.c)
	// assembles individual changes into transactions and then only when it's
	// committed does the output plugin get invoked to send us these messages.
	//
	// Consequently, we can rely on getting a BEGIN, then some changes, and
	// finally a COMMIT message in that order. This is why only the BEGIN
	// message includes an XID, it's implicit in any subsequent messages.
	//
	// The `Relation` messages are how `pgoutput` tells us about the columns
	// of a particular table at the time a transaction was performed. Change
	// messages won't include this information, instead they just encode a
	// sequence of values along with a "Relation ID" which can be used to
	// look it up. We will only be given a particular relation once (unless
	// it changes on the server) in a given replication session, so entries
	// in the relations mapping will never be removed.
	switch msg := msg.(type) {
	case *pglogrepl.RelationMessage:
		s.relations[msg.RelationID] = msg
		return nil, nil
	case *pglogrepl.OriginMessage:
		// Origin messages are sent when the postgres instance we're capturing from
		// is itself replicating from another source instance. They indicate the original
		// source of the transaction. We just ignore these messages for now, though in the
		// future it might be desirable to add the origin as a `_meta` property. Sauce:
		// https://www.highgo.ca/2020/04/18/the-origin-in-postgresql-logical-decoding/
		logrus.WithFields(logrus.Fields{
			"originName": msg.Name,
			"originLSN":  msg.CommitLSN,
		}).Trace("ignoring Origin message")
		return nil, nil
	case *pglogrepl.TypeMessage:
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
	case *pglogrepl.BeginMessage:
		if s.nextTxnFinalLSN != 0 {
			return nil, fmt.Errorf("got BEGIN message while another transaction in progress")
		}
		s.nextTxnFinalLSN = msg.FinalLSN
		s.nextTxnMillis = msg.CommitTime.UnixMilli()
		s.nextTxnXID = msg.Xid
		return nil, nil
	case *pglogrepl.InsertMessage:
		return s.decodeChangeEvent(sqlcapture.InsertOp, lsn, 0, nil, msg.Tuple, msg.RelationID)
	case *pglogrepl.UpdateMessage:
		return s.decodeChangeEvent(sqlcapture.UpdateOp, lsn, msg.OldTupleType, msg.OldTuple, msg.NewTuple, msg.RelationID)
	case *pglogrepl.DeleteMessage:
		return s.decodeChangeEvent(sqlcapture.DeleteOp, lsn, msg.OldTupleType, msg.OldTuple, nil, msg.RelationID)
	case *pglogrepl.CommitMessage:
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
		var event = &sqlcapture.FlushEvent{
			Cursor: s.lastTxnEndLSN.String(),
		}
		logrus.WithField("lsn", s.lastTxnEndLSN).Debug("commit event")
		return event, nil
	case *pglogrepl.TruncateMessage:
		for _, relID := range msg.RelationIDs {
			var relation = s.relations[relID]
			var streamID = sqlcapture.JoinStreamID(relation.Namespace, relation.RelationName)
			if s.tableActive(streamID) {
				logrus.WithField("table", streamID).Warn("ignoring TRUNCATE on active table")
			}
		}
		return nil, nil
	}

	// Unhandled messages are considered a fatal error. There are a bunch of
	// oddball message types that aren't currently implemented in this connector
	// (e.g. truncate, streaming transactions, or two-phase commits) and if we
	// blithely ignored them and continued we're pretty much guaranteed to end
	// up in an inconsistent state with the Postgres tables. Much better to die
	// quickly and give humans a chance to fix things.
	//
	// If a human is reading this comment because this happened to you, you have
	// three options to resolve the problem:
	//
	//   1. Implement additional logic to handle whatever edge case you hit.
	//
	//   2. If your use-case can tolerate a completely different set of
	//      tradeoffs you can use the Airbyte Postgres source connector
	//      in NON-CDC mode.
	//
	//      Note that in CDC mode that connector uses Debezium, which will choke
	//      on these same edge cases, with the exception of TRUNCATE/TYPE/ORIGIN.
	//      (See https://github.com/debezium/debezium/blob/52333596dec168a22674e04f8cd94b1f56607f73/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/pgoutput/PgOutputMessageDecoder.java#L106)
	//
	//   3. Force a full refresh of the relevant table. This will re-scan the
	//      current table contents and then begin replication after the event
	//      that caused this to fail.
	//
	//      But of course if this wasn't a one-off event it will fail again the
	//      next time a problematic statement is executed on this table.
	return nil, fmt.Errorf("unhandled message type %q: %v", msg.Type(), msg)
}

func (s *replicationStream) decodeChangeEvent(
	op sqlcapture.ChangeOp, // Operation of this event.
	lsn pglogrepl.LSN, // LSN of this event.
	beforeType uint8, // Postgres TupleType (0, 'K' for key, 'O' for old full tuple, 'N' for new).
	before, after *pglogrepl.TupleData, // Before and after tuple data. Either may be nil.
	relID uint32, // Relation ID to which tuple data pertains.
) (sqlcapture.DatabaseEvent, error) {
	if s.nextTxnFinalLSN == 0 {
		return nil, fmt.Errorf("got %q message without a transaction in progress", op)
	}

	var rel, ok = s.relations[relID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID %d", relID)
	}

	// If this change event is on a table we're not capturing, skip doing any
	// further processing on it.
	var streamID = sqlcapture.JoinStreamID(rel.Namespace, rel.RelationName)
	if !s.tableActive(streamID) {
		return nil, nil
	}

	bf, err := s.decodeTuple(before, beforeType, rel, nil)
	if err != nil {
		return nil, fmt.Errorf("'before' tuple: %w", err)
	}
	af, err := s.decodeTuple(after, 'N', rel, bf)
	if err != nil {
		return nil, fmt.Errorf("'after' tuple: %w", err)
	}

	keyColumns, ok := s.keyColumns(streamID)
	if !ok {
		return nil, fmt.Errorf("unknown key columns for stream %q", streamID)
	}
	var rowKey []byte
	if op == sqlcapture.InsertOp || op == sqlcapture.UpdateOp {
		rowKey, err = sqlcapture.EncodeRowKey(keyColumns, af, nil, encodeKeyFDB)
	} else {
		rowKey, err = sqlcapture.EncodeRowKey(keyColumns, bf, nil, encodeKeyFDB)
	}
	if err != nil {
		return nil, fmt.Errorf("error encoding row key for %q: %w", streamID, err)
	}

	discovery, ok := s.tables.discovery[streamID]
	if !ok {
		return nil, fmt.Errorf("unknown discovery info for stream %q", streamID)
	}
	if err := translateRecordFields(discovery, bf); err != nil {
		return nil, fmt.Errorf("error translating 'before' tuple: %w", err)
	}
	if err := translateRecordFields(discovery, af); err != nil {
		return nil, fmt.Errorf("error translating 'after' tuple: %w", err)
	}

	var sourceInfo = &postgresSource{
		SourceCommon: sqlcapture.SourceCommon{
			Millis:   s.nextTxnMillis,
			Schema:   rel.Namespace,
			Snapshot: false,
			Table:    rel.RelationName,
		},
		Location: [3]int{
			int(s.lastTxnEndLSN),
			int(lsn),
			int(s.nextTxnFinalLSN),
		},
	}
	if s.db.includeTxIDs[streamID] {
		sourceInfo.TxID = s.nextTxnXID
	}
	var event = &sqlcapture.ChangeEvent{
		Operation: op,
		RowKey:    rowKey,
		Source:    sourceInfo,
		Before:    bf,
		After:     af,
	}
	return event, nil
}

func (s *replicationStream) decodeTuple(
	tuple *pglogrepl.TupleData,
	tupleType uint8,
	rel *pglogrepl.RelationMessage,
	before map[string]interface{},
) (map[string]interface{}, error) {

	var keyOnly bool
	switch tupleType {
	case 0, 'K':
		keyOnly = true
	case 'O':
		// Old tuple with REPLICA IDENTITY FULL set.
	case 'N':
		// New tuple.
	default:
		return nil, fmt.Errorf("unexpected tupleType %q", tupleType)
	}

	if tuple == nil {
		return nil, nil
	}

	var fields = make(map[string]interface{})
	for idx, col := range tuple.Columns {
		if keyOnly && (rel.Columns[idx].Flags&1) == 0 {
			// Skip non-key column because it may not be null-able,
			// but will be null in this tuple encoding.
			// Better to not emit it at all.
			continue
		}
		var colName = rel.Columns[idx].Name

		switch col.DataType {
		case 'n':
			fields[colName] = nil
		case 't':
			var val, err = s.decodeTextColumnData(col.Data, rel.Columns[idx].DataType)
			if err != nil {
				return nil, fmt.Errorf("error decoding column data: %w", err)
			}
			fields[colName] = val
		case 'u':
			// This fields is a TOAST value which is unchanged in this event.
			// Depending on the REPLICA IDENTITY, the value may be available
			// in the "before" tuple of the record. If not, we simply omit it
			// from the event output.
			if val, ok := before[colName]; ok {
				fields[colName] = val
			}
		default:
			return nil, fmt.Errorf("unhandled column data type %v", col.DataType)
		}
	}
	return fields, nil
}

func (s *replicationStream) decodeTextColumnData(data []byte, dataType uint32) (any, error) {
	var dt, ok = s.typeMap.TypeForOID(dataType)
	if !ok {
		// Replication values always use the text encoding, and in the absence of any specific
		// data type mapping we want to treat them as generic text strings, which is just a cast.
		logrus.WithFields(logrus.Fields{
			"oid":  dataType,
			"text": string(data),
		}).Trace("unknown OID, decoding as generic text")
		return string(data), nil
	}

	var val, err = dt.Codec.DecodeValue(s.typeMap, dataType, pgtype.TextFormatCode, data)
	// The only known situations where a valid Postgres timestamp may fail to parse
	// are when the year is greater than 9999 or less than 0. We don't support years
	// outside of that range because timestamps are serialized to RFC3339, but generally
	// years >9999 aren't deliberate, they're dumb typos like `20221`, so normalizing
	// these all to the same error value is as good as any other option.
	if _, ok := err.(*time.ParseError); ok {
		return negativeInfinityTimestamp, nil
	}
	return val, err
}

// receiveMessage reads and parses the next replication message from the database,
// blocking until a message is available, the context is cancelled, or an error
// occurs.
func (s *replicationStream) receiveMessage(ctx context.Context) (pglogrepl.LSN, pglogrepl.Message, error) {
	for {
		var msg, err = s.conn.ReceiveMessage(ctx)
		if err != nil {
			return 0, nil, err
		}

		switch msg := msg.(type) {
		case *pgproto3.ParameterStatus:
			logrus.WithFields(logrus.Fields{
				"name":  msg.Name,
				"value": msg.Value,
			}).Debug("ignoring parameter status message")
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				var pkm, err = pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return 0, nil, fmt.Errorf("error parsing keepalive: %w", err)
				}
				if pkm.ReplyRequested {
					s.standbyStatusDeadline = time.Now()
				}
			case pglogrepl.XLogDataByteID:
				var xld, err = pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return 0, nil, fmt.Errorf("error parsing XLogData: %w", err)
				}
				msg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return 0, nil, fmt.Errorf("error parsing logical replication message: %w", err)
				}
				return xld.WALStart, msg, nil
			default:
				return 0, nil, fmt.Errorf("unknown CopyData message: %v", msg)
			}
		default:
			return 0, nil, fmt.Errorf("unexpected message: %#v", msg)
		}
	}
}

func (s *replicationStream) tableActive(streamID string) bool {
	s.tables.RLock()
	defer s.tables.RUnlock()
	var _, ok = s.tables.active[streamID]
	return ok
}

func (s *replicationStream) keyColumns(streamID string) ([]string, bool) {
	s.tables.RLock()
	defer s.tables.RUnlock()
	var keyColumns, ok = s.tables.keyColumns[streamID]
	return keyColumns, ok
}

func (s *replicationStream) ActivateTable(ctx context.Context, streamID string, keyColumns []string, discovery *sqlcapture.DiscoveryInfo, metadataJSON json.RawMessage) error {
	s.tables.Lock()
	defer s.tables.Unlock()
	s.tables.active[streamID] = struct{}{}
	s.tables.keyColumns[streamID] = keyColumns
	s.tables.discovery[streamID] = discovery
	return nil
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
func (s *replicationStream) Acknowledge(ctx context.Context, cursor string) error {
	if cursor == "" {
		// The empty cursor will be acknowledged once at startup when all bindings
		// are new, because the initial state checkpoint will have an unspecified
		// cursor value on purpose. We don't need to do anything with those.
		return nil
	}
	var lsn, err = pglogrepl.ParseLSN(cursor)
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
	return pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: ackLSN,
	})
}

func (s *replicationStream) Close(ctx context.Context) error {
	logrus.Debug("replication stream close requested")
	s.cancel()
	return <-s.errCh
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

	query("SELECT * FROM " + db.WatermarksTable() + ";")
	query("SELECT * FROM pg_replication_slots;")
	query("SELECT pg_current_wal_flush_lsn(), pg_current_wal_insert_lsn(), pg_current_wal_lsn();")
	return nil
}
