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
	"github.com/sirupsen/logrus"
)

type changeEventHandler func(evt *changeEvent) error

type changeEvent struct {
	Type      string
	XID       pgtype.XID
	LSN       pglogrepl.LSN
	Namespace string
	Table     string
	Fields    map[string]interface{}
}

// A replicationStream represents the process of receiving PostgreSQL
// Logical Replication events, managing keepalives and status updates,
// and translating changes into a more friendly representation. There
// is no built-in concurrency, so Process() must be called reasonably
// soon after StartReplication() in order to not time out.
type replicationStream struct {
	replSlot   string         // The name of the PostgreSQL replication slot to use
	pubName    string         // The name of the PostgreSQL publication to use
	currentLSN pglogrepl.LSN  // The LSN of the most currently received message
	commitLSN  uint64         // The most recently *committed* LSN, given to us by startReplication or the CommitLSN() method
	conn       *pgconn.PgConn // The PostgreSQL replication connection

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

	inTransaction  bool   // inTransaction is true when we're in between a BEGIN/COMMIT message pair.
	transactionXID uint32 // transactionXID holds the XID of the current transaction when inTransaction is true.
}

const standbyStatusInterval = 10 * time.Second

func startReplication(ctx context.Context, conn *pgconn.PgConn, slot, publication string, startLSN pglogrepl.LSN) (*replicationStream, error) {
	stream := &replicationStream{
		replSlot:   slot,
		pubName:    publication,
		currentLSN: startLSN,
		commitLSN:  uint64(startLSN),
		conn:       conn,
		connInfo:   pgtype.NewConnInfo(),
		relations:  make(map[uint32]*pglogrepl.RelationMessage),
		// standbyStatusDeadline is left uninitialized so an update will be sent ASAP
	}

	logrus.WithFields(logrus.Fields{
		"startLSN":    stream.currentLSN,
		"publication": stream.pubName,
		"slot":        stream.replSlot,
	}).Info("starting replication")

	// Create the publication and replication slot, ignoring the inevitable errors
	// when they already exist. We could in theory add some extra logic to check,
	// but why bother when PostgreSQL will already do what we need?
	_ = conn.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION %s FOR ALL TABLES;`, stream.pubName)).Close()
	_ = conn.Exec(ctx, fmt.Sprintf(`CREATE_REPLICATION_SLOT %s LOGICAL pgoutput;`, stream.replSlot)).Close()

	if err := pglogrepl.StartReplication(ctx, stream.conn, stream.replSlot, stream.currentLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			`"proto_version" '1'`,
			fmt.Sprintf(`"publication_names" '%s'`, stream.pubName),
		},
	}); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("unable to start replication: %w", err)
	}
	return stream, nil
}

// process receives logical replication messages from PostgreSQL and invokes
// the provided handler on each change/commit event. Process also sends the
// occasional 'Standby Status Update' message back to PostgreSQL to keep the
// connection open and communicate the current "committed LSN".
//
// Process will only return if an error occurs or its context times out.
func (s *replicationStream) process(ctx context.Context, handler changeEventHandler) error {
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

		receiveCtx, cancelReceiveCtx := context.WithDeadline(ctx, s.standbyStatusDeadline)
		msg, err := s.receiveMessage(receiveCtx)
		cancelReceiveCtx()
		if pgconn.Timeout(err) {
			continue
		}
		if err != nil {
			return err
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
				return fmt.Errorf("got BEGIN message while another transaction in progress")
			}
			s.inTransaction = true
			s.transactionXID = msg.Xid
		case *pglogrepl.InsertMessage:
			if err := s.processChange(ctx, msg.Type().String(), msg.Tuple, msg.RelationID, handler); err != nil {
				return err
			}
		case *pglogrepl.UpdateMessage:
			if err := s.processChange(ctx, msg.Type().String(), msg.NewTuple, msg.RelationID, handler); err != nil {
				return err
			}
		case *pglogrepl.DeleteMessage:
			if err := s.processChange(ctx, msg.Type().String(), msg.OldTuple, msg.RelationID, handler); err != nil {
				return err
			}
		case *pglogrepl.CommitMessage:
			if !s.inTransaction {
				return fmt.Errorf("got COMMIT message without a transaction in progress")
			}
			evt := &changeEvent{
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
			return fmt.Errorf("unhandled message type %q: %v", msg.Type(), msg)
		}
	}
}

func (s *replicationStream) processChange(ctx context.Context, eventType string, tuple *pglogrepl.TupleData, relID uint32, handler changeEventHandler) error {
	if !s.inTransaction {
		return fmt.Errorf("got %s message without a transaction in progress", eventType)
	}

	rel, ok := s.relations[relID]
	if !ok {
		return fmt.Errorf("unknown relation ID %d", relID)
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
					return fmt.Errorf("error decoding column data: %w", err)
				}
				fields[colName] = val
			default:
				return fmt.Errorf("unhandled column data type %v", col.DataType)
			}
		}
	}

	evt := &changeEvent{
		Type:      eventType,
		LSN:       s.currentLSN,
		Namespace: rel.Namespace,
		Table:     rel.RelationName,
		Fields:    fields,
	}
	evt.XID.Set(s.transactionXID)
	if err := handler(evt); err != nil {
		return fmt.Errorf("error handling change event: %w", err)
	}
	return nil
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
func (s *replicationStream) receiveMessage(ctx context.Context) (pglogrepl.Message, error) {
	for {
		msg, err := s.conn.ReceiveMessage(ctx)
		if err != nil {
			return nil, err
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return nil, fmt.Errorf("error parsing keepalive: %w", err)
				}
				if pkm.ReplyRequested {
					s.standbyStatusDeadline = time.Now()
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return nil, fmt.Errorf("error parsing XLogData: %w", err)
				}
				s.currentLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				msg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return nil, fmt.Errorf("error parsing logical replication message: %w", err)
				}
				return msg, nil
			default:
				return nil, fmt.Errorf("unknown CopyData message: %v", msg)
			}
		default:
			return nil, fmt.Errorf("unexpected message: %v", msg)
		}
	}
}

// CommitLSN informs the ReplicationStream that all messages up to the specified
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
func (s *replicationStream) CommitLSN(lsn pglogrepl.LSN) {
	atomic.StoreUint64(&s.commitLSN, uint64(lsn))
}

func (s *replicationStream) sendStandbyStatusUpdate(ctx context.Context) error {
	commitLSN := pglogrepl.LSN(atomic.LoadUint64(&s.commitLSN))
	logrus.WithField("commitLSN", commitLSN).Debug("sending Standby Status Update")
	return pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: commitLSN,
	})
}

func (s *replicationStream) Close(ctx context.Context) error {
	return s.conn.Close(ctx)
}
