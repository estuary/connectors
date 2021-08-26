package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/pkg/errors"
)

type ChangeEventHandler func(event string, lsn pglogrepl.LSN, namespace, table string, fields map[string]interface{}) error

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

	connInfo  *pgtype.ConnInfo
	relations map[uint32]*pglogrepl.RelationMessage
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

	log.Printf("Streaming events from LSN %q", stream.currentLSN)
	if err := pglogrepl.StartReplication(ctx, stream.conn, stream.replSlot, stream.currentLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			`"proto_version" '1'`,
			fmt.Sprintf(`"publication_names" '%s'`, stream.pubName),
		},
	}); err != nil {
		conn.Close(ctx)
		return nil, errors.Wrap(err, "unable to start replication")
	}

	stream.standbyStatusDeadline = time.Now().Add(standbyStatusInterval)
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

		switch msg := msg.(type) {
		case *pglogrepl.InsertMessage:
			s.processChange(ctx, msg.Type().String(), xld, msg.Tuple, msg.RelationID, handler)
		case *pglogrepl.UpdateMessage:
			s.processChange(ctx, msg.Type().String(), xld, msg.NewTuple, msg.RelationID, handler)
		case *pglogrepl.DeleteMessage:
			s.processChange(ctx, msg.Type().String(), xld, msg.OldTuple, msg.RelationID, handler)
		case *pglogrepl.BeginMessage:
			handler("Begin", msg.FinalLSN, "", "", nil)
		case *pglogrepl.CommitMessage:
			handler("Commit", msg.TransactionEndLSN, "", "", nil)
		case *pglogrepl.RelationMessage:
			// Keep track of the relation in order to understand future Insert/Update/Delete messages
			//
			// TODO(wgd): How do we know when to delete a relation? If it's guaranteed that they won't
			// be reused across transactions then we could keep track of BEGIN/COMMIT counts and clear
			// the relations map whenever we reach a point outside of any transactions, but I'm not
			// sure if that's guaranteed to happen either on a busy database.
			s.relations[msg.RelationID] = msg
		default:
			log.Printf("Unhandled Message (Type=%v, WALStart=%q)", xld.WALStart, msg.Type())
		}
	}
}

func (s *ReplicationStream) processChange(ctx context.Context, event string, xld pglogrepl.XLogData, tuple *pglogrepl.TupleData, relID uint32, handler ChangeEventHandler) error {
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

	if err := handler(event, xld.WALStart, rel.Namespace, rel.RelationName, fields); err != nil {
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
			s.sendStandbyStatusUpdate(ctx)
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
				log.Printf("Received unknown CopyData message: %v", msg)
			}
		default:
			log.Printf("Received unexpected message: %v", msg)
		}
	}
}

func (s *ReplicationStream) sendStandbyStatusUpdate(ctx context.Context) error {
	commitLSN := pglogrepl.LSN(atomic.LoadUint64(&s.commitLSN))
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: commitLSN,
	}); err != nil {
		log.Fatalln("SendStandbyStatusUpdate failed:", err)
		return err
	}
	return nil
}

func (s *ReplicationStream) Close(ctx context.Context) error {
	return s.conn.Close(ctx)
}
