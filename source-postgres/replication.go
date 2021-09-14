package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type ChangeEventHandler func(evt *ChangeEvent) error

type ChangeEvent struct {
	Type      string
	XID       pgtype.XID
	LSN       pglogrepl.LSN
	Namespace string
	Table     string
	Fields    map[string]interface{}
}

// A ReplicationStream represents the process of receiving PostgreSQL
// Logical Replication events, managing keepalives and status updates,
// and translating changes into a more friendly representation.
type ReplicationStream struct {
	replSlot   string
	pubName    string
	currentLSN pglogrepl.LSN
	commitLSN  uint64
	conn       *pgconn.PgConn

	standbyStatusDeadline time.Time

	connInfo       *pgtype.ConnInfo
	relations      map[uint32]*pglogrepl.RelationMessage
	inTransaction  bool
	transactionXID uint32
}

const standbyStatusInterval = 10 * time.Second

func StartReplication(ctx context.Context, conn *pgconn.PgConn, slot, publication string, startLSN pglogrepl.LSN) (*ReplicationStream, error) {
	stream := &ReplicationStream{
		replSlot:   slot,
		pubName:    publication,
		currentLSN: startLSN,
		// The "Commit LSN" is initialized to `startLSN` when the replication stream is created,
		// and will never be updated after that. Why is that? It's because the Source Connector
		// "API" we're using has very limited avenues for feedback from the data consumer to us.
		//
		// For context, `commitLSN` is the LSN that we send to PostgreSQL in `StandbyStatusUpdate`
		// messages to tell the DB that we're done with the WAL up to a given point and will never
		// request to restart streaming from an older point. So we have to update it *eventually*
		// in order to avoid an unbounded space leak, but it's not really critical to update it
		// frequently, and we can be pretty conservative about advancing it.
		//
		// The problem is that we don't have many guarantees about when our records and state
		// updates will be durably committed by the consumer. At any moment our connector here
		// might get killed and then re-launched with a state that we emitted hours ago, so
		// without the consumer giving us an explicit acknowledgement we can't assume that it's
		// safe to advance the `commitLSN` cursor. But there is no such ACK message, is there?
		//
		// Well, we actually do get one form of acknowledgement: we know that when the connector
		// is restarted with a provided `state.json` to resume from, that state must have gotten
		// committed by the consumer. So provided that the connector is periodically killed and
		// restarted, we will periodically advance the `commitLSN` and thereby allow PostgreSQL
		// to free older segments of the WAL.
		//
		// This hinges on connectors getting shut down and restarted every so often, which is a
		// guarantee that should ideally be provided by the runtime anyway.
		commitLSN: uint64(startLSN),
		conn:      conn,
		connInfo:  pgtype.NewConnInfo(),
		relations: make(map[uint32]*pglogrepl.RelationMessage),
	}

	logrus.WithFields(logrus.Fields{
		"startLSN":    stream.currentLSN,
		"publication": stream.pubName,
		"slot":        stream.replSlot,
	}).Info("starting replication")

	if err := pglogrepl.StartReplication(ctx, stream.conn, stream.replSlot, stream.currentLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			`"proto_version" '1'`,
			fmt.Sprintf(`"publication_names" '%s'`, stream.pubName),
		},
	}); err != nil {
		conn.Close(ctx)
		return nil, errors.Wrap(err, "unable to start replication")
	}

	// Send one status update immediately on startup
	stream.standbyStatusDeadline = time.Now()
	return stream, nil
}

func (s *ReplicationStream) Process(ctx context.Context, handler ChangeEventHandler) error {
	for {
		xld, err := s.receiveXLogData(ctx)
		if err != nil {
			return err
		}
		msg, err := pglogrepl.Parse(xld.WALData)
		if err != nil {
			return errors.Wrap(err, "error parsing logical replication message")
		}

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
		case *pglogrepl.BeginMessage:
			if s.inTransaction {
				return errors.New("got BEGIN message while another transaction in progress")
			}
			s.inTransaction = true
			s.transactionXID = msg.Xid
		case *pglogrepl.InsertMessage:
			if err := s.processChange(ctx, msg.Type().String(), xld, msg.Tuple, msg.RelationID, handler); err != nil {
				return err
			}
		case *pglogrepl.UpdateMessage:
			if err := s.processChange(ctx, msg.Type().String(), xld, msg.NewTuple, msg.RelationID, handler); err != nil {
				return err
			}
		case *pglogrepl.DeleteMessage:
			if err := s.processChange(ctx, msg.Type().String(), xld, msg.OldTuple, msg.RelationID, handler); err != nil {
				return err
			}
		case *pglogrepl.CommitMessage:
			if !s.inTransaction {
				return errors.New("got COMMIT message without a transaction in progress")
			}
			evt := &ChangeEvent{
				Type: "Commit",
				LSN:  msg.TransactionEndLSN,
			}
			evt.XID.Set(s.transactionXID)
			if err := handler(evt); err != nil {
				return err
			}
			s.inTransaction = false
			s.transactionXID = 0
		default:
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
			//      IN NON-CDC MODE.
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
			return errors.Errorf("unhandled message (Type=%v, WALStart=%q)", msg.Type(), xld.WALStart)
		}
	}
}

func (s *ReplicationStream) processChange(ctx context.Context, eventType string, xld pglogrepl.XLogData, tuple *pglogrepl.TupleData, relID uint32, handler ChangeEventHandler) error {
	if !s.inTransaction {
		return errors.Errorf("got %s message without a transaction in progress", eventType)
	}

	rel, ok := s.relations[relID]
	if !ok {
		return errors.Errorf("unknown relation ID %d", relID)
	}

	fields := make(map[string]interface{})
	if tuple != nil {
		for idx, col := range tuple.Columns {
			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n':
				fields[colName] = nil
			case 't':
				val, err := s.decodeTextColumnData(col.Data, rel.Columns[idx].DataType)
				if err != nil {
					return errors.Wrap(err, "error decoding column data")
				}
				fields[colName] = val
			default:
				return errors.Errorf("unhandled column data type %v", col.DataType)
			}
		}
	}

	evt := &ChangeEvent{
		Type:      eventType,
		LSN:       xld.WALStart,
		Namespace: rel.Namespace,
		Table:     rel.RelationName,
		Fields:    fields,
	}
	evt.XID.Set(s.transactionXID)
	if err := handler(evt); err != nil {
		return errors.Wrap(err, "error handling change event")
	}
	return nil
}

func (s *ReplicationStream) decodeTextColumnData(data []byte, dataType uint32) (interface{}, error) {
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

// receiveXLogData returns the next transaction log message from the database,
// blocking until a message is available, the context is cancelled, or an error
// occurs. In the process it takes care of sending Standby Status Update messages
// back to the database, so it must be called regularly over the life of a
// replication stream.
func (s *ReplicationStream) receiveXLogData(ctx context.Context) (pglogrepl.XLogData, error) {
	for {
		select {
		case <-ctx.Done():
			return pglogrepl.XLogData{}, ctx.Err()
		default:
		}

		if time.Now().After(s.standbyStatusDeadline) {
			if err := s.sendStandbyStatusUpdate(ctx); err != nil {
				return pglogrepl.XLogData{}, errors.Wrap(err, "failed to send status update")
			}
			s.standbyStatusDeadline = time.Now().Add(standbyStatusInterval)
		}

		receiveCtx, cancelReceiveCtx := context.WithDeadline(ctx, s.standbyStatusDeadline)
		msg, err := s.conn.ReceiveMessage(receiveCtx)
		cancelReceiveCtx()
		if pgconn.Timeout(err) {
			continue
		}
		if err != nil {
			return pglogrepl.XLogData{}, err
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return pglogrepl.XLogData{}, errors.Wrap(err, "error parsing keepalive")
				}
				if pkm.ReplyRequested {
					s.standbyStatusDeadline = time.Now()
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return xld, errors.Wrap(err, "error parsing XLogData")
				}
				s.currentLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				return xld, nil
			default:
				logrus.WithField("message", msg).Warn("unknown CopyData message")
			}
		default:
			logrus.WithField("message", msg).Warn("unexpected message")
		}
	}
}

func (s *ReplicationStream) sendStandbyStatusUpdate(ctx context.Context) error {
	commitLSN := pglogrepl.LSN(atomic.LoadUint64(&s.commitLSN))
	return pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: commitLSN,
	})
}

func (s *ReplicationStream) Close(ctx context.Context) error {
	return s.conn.Close(ctx)
}
