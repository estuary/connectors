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
	"github.com/pkg/errors"
)

type ChangeEventHandler func(namespace, table string, eventType string, fields map[string]interface{}) error

func streamReplicationEvents(ctx context.Context, conn *pgconn.PgConn, slotName, pubName string, startLSN pglogrepl.LSN, handler ChangeEventHandler) error {
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
			if err := pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: clientXLogPos}); err != nil {
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
				if err := processXLogData(ctx, xld, handler); err != nil {
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

// TODO: Don't use a global for these
type RelationID = uint32

var relations = make(map[RelationID]pglogrepl.RelationMessage)

func processXLogData(ctx context.Context, xld pglogrepl.XLogData, handler ChangeEventHandler) error {
	msg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return errors.Wrap(err, "error parsing logical replication message")
	}

	log.Printf("XLogData(Type=%s, WALStart=%q, Length=%d)", msg.Type(), xld.WALStart, len(xld.WALData))
	if xld.ServerWALEnd != xld.WALStart {
		log.Printf("  !!! ServerWALEnd=%q", xld.ServerWALEnd)
	}

	switch msg := msg.(type) {
	case *pglogrepl.BeginMessage:
		log.Printf("Begin(finalLSN=%q, xid=%v)", msg.FinalLSN, msg.Xid)
	case *pglogrepl.CommitMessage:
		log.Printf("Commit(commitLSN=%q, txEndLSN=%q)", msg.CommitLSN, msg.TransactionEndLSN)
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
	// case *pglogrepl.OriginMessage:
	// 	log.Printf("  Origin()")
	// case *pglogrepl.TypeMessage:
	// 	log.Printf("  Type()")
	// case *pglogrepl.TruncateMessage:
	// 	log.Printf("  Truncate()")
	case *pglogrepl.RelationMessage:
		// Keep track of the relation in order to understand future Insert/Update/Delete messages
		//
		// TODO(wgd): How do we know when to delete a relation? Worst-case we can use a timestamp
		// and a separate grooming thread to delete them after some time window has elapsed, but
		// I haven't been able to find any documentation of any of these logical replication
		// messages so I'm hesitant to make assumptions about anything.
		relations[msg.RelationID] = *msg
	case *pglogrepl.InsertMessage:
		return processDataMessage(ctx, msg.Type(), msg.RelationID, msg.Tuple, handler)
	case *pglogrepl.UpdateMessage:
		return processDataMessage(ctx, msg.Type(), msg.RelationID, msg.NewTuple, handler)
	case *pglogrepl.DeleteMessage:
		return processDataMessage(ctx, msg.Type(), msg.RelationID, msg.OldTuple, handler)
	default:
		log.Printf("Unhandled message type %q", msg.Type())
	}
	return nil
}

func processDataMessage(ctx context.Context, msgType pglogrepl.MessageType, relID RelationID, tuple *pglogrepl.TupleData, handler ChangeEventHandler) error {
	rel, ok := relations[relID]
	if !ok {
		return errors.Errorf("unknown relation %d", relID)
	}

	fields := make(map[string]interface{})
	log.Printf("%s(rid=%v, namespace=%v, relName=%v)", msgType, rel.RelationID, rel.Namespace, rel.RelationName)
	if tuple != nil {
		for idx, col := range tuple.Columns {
			log.Printf("  (name=%q, type=%q, data=%q)", rel.Columns[idx].Name, col.DataType, col.Data)

			colName := rel.Columns[idx].Name
			switch col.DataType {
			case 'n':
				fields[colName] = nil
			case 't':
				fields[colName] = encodeColumnData(col.Data, rel.Columns[idx].DataType, rel.Columns[idx].TypeModifier)
			default:
				return errors.Errorf("unhandled column data type %v", col.DataType)
			}
		}
	}

	if err := handler(rel.Namespace, rel.RelationName, msgType.String(), fields); err != nil {
		return errors.Wrap(err, "error handling change event")
	}
	return nil
}

func encodeColumnData(data []byte, dataType, typeMod uint32) interface{} {
	// TODO(wgd): Use `dataType` and `typeMod` to more intelligently convert
	// text-format values from Postgres into JSON-encodable values.
	return string(data)
}
