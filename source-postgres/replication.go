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
	startLSN   pglogrepl.LSN
	currentLSN pglogrepl.LSN
	commitLSN  uint64
	conn       *pgconn.PgConn

	closed                bool
	standbyStatusDeadline time.Time

	connInfo  *pgtype.ConnInfo
	relations map[uint32]pglogrepl.RelationMessage
}

const standbyStatusInterval = 10 * time.Second

func StartReplication(ctx context.Context, conn *pgconn.PgConn, slot, publication string, startLSN pglogrepl.LSN) (*ReplicationStream, error) {
	stream := &ReplicationStream{
		replSlot:   slot,
		pubName:    publication,
		startLSN:   startLSN,
		currentLSN: startLSN,
		commitLSN:  uint64(startLSN),
		conn:       conn,
		connInfo:   pgtype.NewConnInfo(),
		relations:  make(map[uint32]pglogrepl.RelationMessage),
	}

	log.Printf("Streaming events from LSN %q", stream.currentLSN)
	if err := pglogrepl.StartReplication(ctx, stream.conn, stream.replSlot, stream.startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			`"proto_version" '1'`,
			fmt.Sprintf(`"publication_names" '%s'`, stream.pubName),
		},
	}); err != nil {
		return nil, errors.Wrap(err, "unable to start replication")
	}

	stream.standbyStatusDeadline = time.Now().Add(standbyStatusInterval)
	return stream, nil
}

func (s *ReplicationStream) Process(ctx context.Context, handler ChangeEventHandler) error {
	for {
		if s.closed {
			return errors.New("stream has been closed")
		}

		xld, err := s.receiveXLogData(ctx)
		if err != nil {
			return err
		}

		msg, err := pglogrepl.Parse(xld.WALData)
		if err != nil {
			return errors.Wrap(err, "error parsing logical replication message")
		}

		var relID uint32
		var tuple *pglogrepl.TupleData
		switch msg := msg.(type) {
		case *pglogrepl.InsertMessage:
			tuple, relID = msg.Tuple, msg.RelationID
		case *pglogrepl.UpdateMessage:
			tuple, relID = msg.NewTuple, msg.RelationID
		case *pglogrepl.DeleteMessage:
			tuple, relID = msg.OldTuple, msg.RelationID

		case *pglogrepl.BeginMessage:
			log.Printf("Begin(WALStart=%q, FinalLSN=%q, XID=%v)", xld.WALStart, msg.FinalLSN, msg.Xid)
			handler("Begin", msg.FinalLSN, "", "", nil)
			continue
		case *pglogrepl.CommitMessage:
			log.Printf("Commit(WALStart=%q, CommitLSN=%q, TXEndLSN=%q)", xld.WALStart, msg.CommitLSN, msg.TransactionEndLSN)
			handler("Commit", msg.TransactionEndLSN, "", "", nil)
			continue
		case *pglogrepl.RelationMessage:
			// Keep track of the relation in order to understand future Insert/Update/Delete messages
			//
			// TODO(wgd): How do we know when to delete a relation? Worst-case we can use a timestamp
			// and a separate grooming thread to delete them after some time window has elapsed, but
			// I haven't been able to find any documentation of any of these logical replication
			// messages so I'm hesitant to make assumptions about anything.
			s.relations[msg.RelationID] = *msg
			continue
		default:
			log.Printf("UnhandledMessage(WALStart=%q, Type=%v)", xld.WALStart, msg.Type())
			continue
		}

		// Because all message cases other than Insert/Update/Delete return,
		// at this point we're handling actual data messages.
		rel, ok := s.relations[relID]
		if !ok {
			return errors.Errorf("unknown relation %d", relID)
		}

		fields := make(map[string]interface{})
		log.Printf("%s(WALStart=%q, RID=%v, Namespace=%v, RelName=%v)", msg.Type(), xld.WALStart, rel.RelationID, rel.Namespace, rel.RelationName)
		if tuple != nil {
			for idx, col := range tuple.Columns {
				log.Printf("  (name=%q, type=%q, data=%q)", rel.Columns[idx].Name, col.DataType, col.Data)

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

		if err := handler(msg.Type().String(), xld.WALStart, rel.Namespace, rel.RelationName, fields); err != nil {
			return errors.Wrap(err, "error handling change event")
		}
	}
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
				log.Printf("KeepAlive: ServerWALEnd=%q, ReplyRequested=%v", pkm.ServerWALEnd, pkm.ReplyRequested)
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
	log.Printf("Sent Standby Status Update with LSN=%q", commitLSN)
	return nil
}

func (s *ReplicationStream) Close() error {
	if s.closed {
		return errors.New("stream already closed")
	}
	s.closed = true
	return nil
}
