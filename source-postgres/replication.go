package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/estuary/connectors/go-types/airbyte"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/jackc/pgtype"
	"github.com/pkg/errors"
)

type ChangeEventHandler func(namespace, table string, eventType string, fields map[string]interface{}) error
type XLogEventHandler func(ctx context.Context, xld pglogrepl.XLogData) error

// One change that could simplify things a bit would be to have the
// `ChangeEventHandler` type be an interface with `HandleData()`
// and `HandleState()` methods. This would allow us to avoid
// outputting directly to stdout.

func streamReplicationEvents(ctx context.Context, conn *pgconn.PgConn, slotName, pubName string, startLSN pglogrepl.LSN, handler ChangeEventHandler) error {
	connInfo := pgtype.NewConnInfo()
	relations := make(map[uint32]pglogrepl.RelationMessage)
	return streamXLogEvents(ctx, conn, slotName, pubName, startLSN, func(ctx context.Context, xld pglogrepl.XLogData) error {
		msg, err := pglogrepl.Parse(xld.WALData)
		if err != nil {
			return errors.Wrap(err, "error parsing logical replication message")
		}

		log.Printf("XLogData(Type=%s, WALStart=%q, Length=%d)", msg.Type(), xld.WALStart, len(xld.WALData))
		if xld.ServerWALEnd != xld.WALStart {
			log.Printf("  !!! ServerWALEnd=%q", xld.ServerWALEnd)
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
			return nil
		case *pglogrepl.CommitMessage:
			log.Printf("Commit(WALStart=%q, CommitLSN=%q, TXEndLSN=%q)", xld.WALStart, msg.CommitLSN, msg.TransactionEndLSN)
			rawState, err := json.Marshal(ResumeState{
				ResumeLSN: msg.CommitLSN,
			})
			if err != nil {
				return errors.Wrap(err, "error encoding state message")
			}
			if err := json.NewEncoder(os.Stdout).Encode(airbyte.Message{
				Type:  airbyte.MessageTypeState,
				State: &airbyte.State{Data: json.RawMessage(rawState)},
			}); err != nil {
				return errors.Wrap(err, "error writing state message")
			}
			return nil
		case *pglogrepl.RelationMessage:
			// Keep track of the relation in order to understand future Insert/Update/Delete messages
			//
			// TODO(wgd): How do we know when to delete a relation? Worst-case we can use a timestamp
			// and a separate grooming thread to delete them after some time window has elapsed, but
			// I haven't been able to find any documentation of any of these logical replication
			// messages so I'm hesitant to make assumptions about anything.
			relations[msg.RelationID] = *msg
			return nil
		default:
			log.Printf("UnhandledMessage(WALStart=%q, Type=%v)", xld.WALStart, msg.Type())
			return nil
		}

		// Because all message cases other than Insert/Update/Delete return,
		// at this point we're handling actual data messages.
		rel, ok := relations[relID]
		if !ok {
			return errors.Errorf("unknown relation %d", relID)
		}

		fields := make(map[string]interface{})
		log.Printf("%s(WALStart=%q, RID=%v, Namespace=%v, RelName=%v)", xld.WALStart, msg.Type(), rel.RelationID, rel.Namespace, rel.RelationName)
		if tuple != nil {
			for idx, col := range tuple.Columns {
				log.Printf("  (name=%q, type=%q, data=%q)", rel.Columns[idx].Name, col.DataType, col.Data)

				colName := rel.Columns[idx].Name
				switch col.DataType {
				case 'n':
					fields[colName] = nil
				case 't':
					val, err := decodeTextColumnData(connInfo, col.Data, rel.Columns[idx].DataType, rel.Columns[idx].TypeModifier)
					if err != nil {
						return errors.Wrap(err, "error decoding column data")
					}
					fields[colName] = val
				default:
					return errors.Errorf("unhandled column data type %v", col.DataType)
				}
			}
		}

		if err := handler(rel.Namespace, rel.RelationName, msg.Type().String(), fields); err != nil {
			return errors.Wrap(err, "error handling change event")
		}
		return nil
	})
}

func decodeTextColumnData(connInfo *pgtype.ConnInfo, data []byte, dataType, typeMod uint32) (interface{}, error) {
	var decoder pgtype.TextDecoder
	if dt, ok := connInfo.DataTypeForOID(dataType); ok {
		decoder, ok = dt.Value.(pgtype.TextDecoder)
		if !ok {
			decoder = &pgtype.GenericText{}
		}
	} else {
		decoder = &pgtype.GenericText{}
	}
	if err := decoder.DecodeText(connInfo, data); err != nil {
		return nil, err
	}
	return decoder.(pgtype.Value).Get(), nil
}

func streamXLogEvents(ctx context.Context, conn *pgconn.PgConn, slotName, pubName string, startLSN pglogrepl.LSN, handler XLogEventHandler) error {
	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return errors.Wrap(err, "unable to identify system")
	}
	log.Printf("IdentifySystem: %v", sysident)

	if startLSN == 0 {
		startLSN = sysident.XLogPos
	} else {
		log.Printf("Resuming from LSN %q", startLSN)
	}

	if err = pglogrepl.StartReplication(ctx, conn, slotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			`"proto_version" '1'`,
			fmt.Sprintf(`"publication_names" '%s'`, pubName),
		},
	}); err != nil {
		return errors.Wrap(err, "unable to start replication")
	}

	clientXLogPos := startLSN
	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if time.Now().After(nextStandbyMessageDeadline) {
			// TODO(wgd): Set `WALFlushPosition` and/or `WALApplyPosition` to inform
			// Postgres about permanent changes to the replication slot LSN. In theory
			// we can advance the slot LSN after each new state update is emitted, since
			// at that point if the connector is killed and restarted it should only
			// resume from that newer LSN. However we might want to delay that a bit
			// since there's no *confirmation* of a state update.
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: clientXLogPos,
			}); err != nil {
				log.Fatalln("SendStandbyStatusUpdate failed:", err)
			}
			log.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		receiveCtx, cancelReceiveCtx := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		msg, err := conn.ReceiveMessage(receiveCtx)
		cancelReceiveCtx()
		if pgconn.Timeout(err) {
			continue
		}
		if err != nil {
			return errors.Wrap(err, "error receiving message")
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return errors.Wrap(err, "error parsing keepalive")
				}
				log.Printf("KeepAlive: ServerWALEnd=%q, ReplyRequested=%v", pkm.ServerWALEnd, pkm.ReplyRequested)
				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Now()
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return errors.Wrap(err, "error parsing XLogData")
				}
				if err := handler(ctx, xld); err != nil {
					return err
				}
				clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			default:
				log.Printf("Received unknown CopyData message: %v", msg)
			}
		default:
			log.Printf("Received unexpected message: %v", msg)
		}
	}
}
