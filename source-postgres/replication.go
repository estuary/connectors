package main

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/sirupsen/logrus"
)

// ChangeOp encodes a change operation type.
// It's compatible with Debezium's change event representation.
// TODO(johnny): Factor into a shared package.
type ChangeOp string

const (
	// InsertOp is an INSERT operation.
	InsertOp ChangeOp = "c"
	// UpdateOp is an UPDATE operation.
	UpdateOp ChangeOp = "u"
	// DeleteOp is a DELETE operation.
	DeleteOp ChangeOp = "d"
	// FlushOp is an internal-only ChangeOp which flushes a completed
	// transaction, but is not actually serialized.
	FlushOp ChangeOp = "x"
)

// SourceCommon is Debezium-compatible source metadata for data capture events.
// See Debezium docs at:
// 	https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-create-events
// See also how materialize deserializes Debezium envelopes:
// 	https://github.com/MaterializeInc/materialize/blob/4fca6f51338b0da9a44dd3a75a5a4a5da37a9733/src/interchange/src/avro/envelope_debezium.rs#L275
// TODO(johnny): Factor into a shared package.
type SourceCommon struct {
	// Version string for this connector. Example: "v1.2.3".
	Version string `json:"version,omitempty"`
	// Connector name. Example: "source-postgres".
	Connector string `json:"connector"`
	// Name of this capture (?). Example: "acmeCo/cdc-from-postgres".
	// The Debezium connectors use strings like "PostgreSQL_server",
	// and it's expected that this string composes with Schema and
	// Table to produce an identifier under which an Avro schema is
	// registered with a schema registry, like
	// "PostgreSQL_server.inventory.customers.Value".
	Name string `json:"name,omitempty"`
	// Unix milliseconds at which this record was recorded in the WAL.
	Millis int64 `json:"ts_ms,omitempty"`
	// Database is the logical database name.
	Database string `json:"db,omitempty"`
	// Schema is the logical database schema.
	Schema string `json:"schema"`
	// Table is the table name, within the database and schema.
	Table string `json:"table"`
	// TxID is an identifier which monotonically increases with
	// each transaction, and defines its event boundaries.
	//
	// COMPATIBILITY: Debezium defines "txId" only within the PostgreSQL
	// connector, but we generalize it to all implemented connectors.
	TxID uint64 `json:"txId,omitempty"`
	// Snapshot distinguishes events emitted from a backfill,
	// versus ongoing reads of the replication log.
	Snapshot bool `json:"snapshot,omitempty"`
}

type postgresSource struct {
	// Sequence is a string-serialized JSON array which embeds a lexicographic
	// ordering of all events. Its contents are database-dependent.
	// For PostgreSQL, it models: [last-committed-LSN, current-LSN].
	Sequence string `json:"sequence,omitempty"`
	// LSN is the log sequence number of the current record event.
	EventLSN pglogrepl.LSN `json:"lsn,omitempty"`

	SourceCommon
}

type changeEvent struct {
	Operation ChangeOp               `json:"op"`
	Source    postgresSource         `json:"source"`
	Before    map[string]interface{} `json:"before,omitempty"`
	After     map[string]interface{} `json:"after,omitempty"`
}

// keyFields returns suitable fields for extracting the event primary key.
func (e *changeEvent) keyFields() map[string]interface{} {
	if e.Operation == DeleteOp {
		return e.Before
	}
	return e.After
}

// A replicationStream represents the process of receiving PostgreSQL
// Logical Replication events, managing keepalives and status updates,
// and translating changes into a more friendly representation. There
// is no built-in concurrency, so Process() must be called reasonably
// soon after StartReplication() in order to not time out.
type replicationStream struct {
	ackLSN          uint64             // The most recently Ack'd LSN, passed to startReplication or updated via CommitLSN.
	cancel          context.CancelFunc // Cancel function for the replication goroutine's context
	conn            *pgconn.PgConn     // The PostgreSQL replication connection
	eventBuf        *changeEvent       // A single-element buffer used in between 'receiveMessage' and the output channel
	events          chan *changeEvent  // The channel to which replication events will be written
	lastTxnEndLSN   pglogrepl.LSN      // End LSN (record + 1) of the last completed transaction.
	nextTxnFinalLSN pglogrepl.LSN      // Final LSN of the commit currently being processed, or zero if between transactions.
	nextTxnMillis   int64              // Unix timestamp (in millis) at which the change originally occurred.
	pubName         string             // The name of the PostgreSQL publication to use
	replSlot        string             // The name of the PostgreSQL replication slot to use

	// standbyStatusDeadline is the time at which we need to stop receiving
	// replication messages and go send a Standby Status Update message to
	// the DB.
	standbyStatusDeadline time.Time

	// connInfo is a sort of type registry used when decoding values
	// from the database.
	connInfo *pgtype.ConnInfo

	// relations keeps track of all "Relation Messages" from the database. These
	// messages tell us about the integer ID corresponding to a particular table
	// and other information about the table structure at a particular moment.
	relations map[uint32]*pglogrepl.RelationMessage
}

const standbyStatusInterval = 10 * time.Second

// replicationBufferSize controls how many change events can be buffered in the
// replicationStream before it stops receiving further events from PostgreSQL.
// In normal use it's a constant, it's just a variable so that tests are more
// likely to exercise blocking sends and backpressure.
var replicationBufferSize = 1024

func startReplication(ctx context.Context, conn *pgconn.PgConn, slot, publication string, startLSN pglogrepl.LSN) (*replicationStream, error) {
	// If we don't have a valid `startLSN` from a previous capture, it gets initialized
	// to the current WAL flush position obtained via the `IDENTIFY_SYSTEM` command.
	if startLSN == 0 {
		var sysident, err = pglogrepl.IdentifySystem(ctx, conn)
		if err != nil {
			return nil, fmt.Errorf("unable to read WAL flush LSN from database: %w", err)
		}
		startLSN = sysident.XLogPos
	}

	logrus.WithFields(logrus.Fields{
		"startLSN":    startLSN,
		"publication": publication,
		"slot":        slot,
	}).Debug("starting replication")

	var stream = &replicationStream{
		replSlot:        slot,
		pubName:         publication,
		ackLSN:          uint64(startLSN),
		lastTxnEndLSN:   startLSN,
		nextTxnFinalLSN: 0,
		nextTxnMillis:   0,
		conn:            conn,
		connInfo:        pgtype.NewConnInfo(),
		relations:       make(map[uint32]*pglogrepl.RelationMessage),
		// standbyStatusDeadline is left uninitialized so an update will be sent ASAP
		events: make(chan *changeEvent, replicationBufferSize),
	}

	// Create the publication and replication slot, ignoring the inevitable errors
	// when they already exist. We could in theory add some extra logic to check,
	// but why bother when PostgreSQL will already do what we need?
	_ = conn.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES;`, stream.pubName)).Close()
	_ = conn.Exec(ctx, fmt.Sprintf(`CREATE_REPLICATION_SLOT %s LOGICAL pgoutput;`, stream.replSlot)).Close()

	if err := pglogrepl.StartReplication(ctx, stream.conn, slot, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			`"proto_version" '1'`,
			fmt.Sprintf(`"publication_names" '%s'`, stream.pubName),
		},
	}); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("unable to start replication: %w", err)
	}

	var streamCtx, streamCancel = context.WithCancel(ctx)
	stream.cancel = streamCancel
	go func() {
		defer close(stream.events)
		defer stream.conn.Close(ctx)
		if err := stream.run(streamCtx); err != nil && !errors.Is(err, context.Canceled) {
			logrus.WithField("err", err).Fatal("replication stream error")
		}
	}()

	return stream, nil
}

func (s *replicationStream) Events() <-chan *changeEvent {
	return s.events
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

func (s *replicationStream) decodeMessage(lsn pglogrepl.LSN, msg pglogrepl.Message) (*changeEvent, error) {
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
	case *pglogrepl.BeginMessage:
		if s.nextTxnFinalLSN != 0 {
			return nil, fmt.Errorf("got BEGIN message while another transaction in progress")
		}
		s.nextTxnFinalLSN = msg.FinalLSN
		s.nextTxnMillis = msg.CommitTime.UnixMilli()
		return nil, nil
	case *pglogrepl.InsertMessage:
		return s.decodeChangeEvent(InsertOp, lsn, 0, nil, msg.Tuple, msg.RelationID)
	case *pglogrepl.UpdateMessage:
		return s.decodeChangeEvent(UpdateOp, lsn, msg.OldTupleType, msg.OldTuple, msg.NewTuple, msg.RelationID)
	case *pglogrepl.DeleteMessage:
		return s.decodeChangeEvent(DeleteOp, lsn, msg.OldTupleType, msg.OldTuple, nil, msg.RelationID)
	case *pglogrepl.CommitMessage:
		if s.nextTxnFinalLSN == 0 {
			return nil, fmt.Errorf("got COMMIT message without a transaction in progress")
		} else if s.nextTxnFinalLSN != msg.CommitLSN {
			return nil, fmt.Errorf("got COMMIT message with unexpected CommitLSN (%d; expected %d)",
				msg.CommitLSN, s.nextTxnFinalLSN)
		}
		s.nextTxnFinalLSN = 0
		s.nextTxnMillis = 0
		s.lastTxnEndLSN = msg.TransactionEndLSN

		var event = &changeEvent{
			Operation: FlushOp,
			Source:    postgresSource{EventLSN: s.lastTxnEndLSN},
		}
		return event, nil
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
	op ChangeOp, // Operation of this event.
	lsn pglogrepl.LSN, // LSN of this event.
	beforeType uint8, // Postgres TupleType (0, 'K' for key, 'O' for old full tuple, 'N' for new).
	before, after *pglogrepl.TupleData, // Before and after tuple data. Either may be nil.
	relID uint32, // Relation ID to which tuple data pertains.
) (*changeEvent, error) {
	if s.nextTxnFinalLSN == 0 {
		return nil, fmt.Errorf("got %q message without a transaction in progress", op)
	}

	var rel, ok = s.relations[relID]
	if !ok {
		return nil, fmt.Errorf("unknown relation ID %d", relID)
	}

	bf, err := s.decodeTuple(before, beforeType, rel)
	if err != nil {
		return nil, fmt.Errorf("'before' tuple: %w", err)
	}
	af, err := s.decodeTuple(after, 'N', rel)
	if err != nil {
		return nil, fmt.Errorf("'after' tuple: %w", err)
	}

	var event = &changeEvent{
		Operation: op,
		Source: postgresSource{
			Sequence: fmt.Sprintf("\"[%d,%d]\"", s.lastTxnEndLSN, lsn),
			EventLSN: lsn,
			SourceCommon: SourceCommon{
				Connector: "source-postgres",
				Database:  "", // TODO(johnny).
				Millis:    s.nextTxnMillis,
				Name:      "", // TODO(johnny).
				Schema:    rel.Namespace,
				Snapshot:  false,
				Table:     rel.RelationName,
				TxID:      uint64(s.nextTxnFinalLSN),
				Version:   "", // TODO(johnny).
			},
		},
		Before: bf,
		After:  af,
	}
	return event, nil
}

func (s *replicationStream) decodeTuple(
	tuple *pglogrepl.TupleData,
	tupleType uint8,
	rel *pglogrepl.RelationMessage,
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
		default:
			return nil, fmt.Errorf("unhandled column data type %v", col.DataType)
		}
	}
	return fields, nil
}

func (s *replicationStream) decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
	var decoder pgtype.TextDecoder
	if dt, ok := s.connInfo.DataTypeForOID(dataType); ok {
		decoder, ok = dt.Value.(pgtype.TextDecoder)
		if !ok {
			decoder = &pgtype.GenericText{}
		}
	} else {
		decoder = &pgtype.GenericText{}
	}
	if err := decoder.DecodeText(s.connInfo, data); err != nil {
		return nil, err
	}
	return decoder.(pgtype.Value).Get(), nil
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
			return 0, nil, fmt.Errorf("unexpected message: %v", msg)
		}
	}
}

// AcknowledgeLSN informs the ReplicationStream that all messages up to the specified
// LSN [1] have been persisted, and that a future restart will never need to return
// to older portions of the transaction log. This fact will be communicated to the
// database in a periodic status update, whereupon the replication slot's "Restart
// LSN" may be advanced accordingly.
//
// TODO(wgd): At present this function is never called. That's because there is no
// way for Flow to tell us that our output has been durably committed downstream,
// and no reliable heuristic we could use either. For now the committed LSN only
// "advances" by being set at startup. Due to how connector restarts work this
// will actually kind of work, but at the cost of retaining more WAL data than
// we actually need (until the connector restarts), and causing a "hiccup" of
// replication latency when the restarted connector makes PostgreSQL go back
// through all that extra buffered data before reaching new changes.
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
func (s *replicationStream) AcknowledgeLSN(lsn pglogrepl.LSN) {
	atomic.StoreUint64(&s.ackLSN, uint64(lsn))
}

func (s *replicationStream) sendStandbyStatusUpdate(ctx context.Context) error {
	var ackLSN = pglogrepl.LSN(atomic.LoadUint64(&s.ackLSN))
	logrus.WithField("ackLSN", ackLSN).Debug("sending Standby Status Update")
	return pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: ackLSN,
	})
}

func (s *replicationStream) Close(ctx context.Context) error {
	logrus.Debug("replication stream close requested")
	s.cancel()
	return nil
}
