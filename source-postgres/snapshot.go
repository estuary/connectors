package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

type TableSnapshotStream struct {
	conn        *pgx.Conn
	transaction pgx.Tx
	txLSN       pglogrepl.LSN
}

func SnapshotTable(ctx context.Context, conn *pgx.Conn) (*TableSnapshotStream, error) {
	transaction, err := conn.BeginTx(ctx, pgx.TxOptions{
		// We could probably get away with `RepeatableRead` isolation here, but
		// I see no reason not to err on the side of getting the strongest
		// guarantees that we can, especially since the full scan is not the
		// normal mode of operation and happens just once per table.
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		conn.Close(ctx)
		return nil, errors.Wrap(err, "unable to begin transaction")
	}
	stream := &TableSnapshotStream{
		conn:        conn,
		transaction: transaction,
	}
	if err := stream.queryTransactionLSN(ctx); err != nil {
		stream.transaction.Rollback(ctx)
		stream.conn.Close(ctx)
		return nil, err
	}
	return stream, nil
}

func (s *TableSnapshotStream) queryTransactionLSN(ctx context.Context) error {
	// TODO(wgd): Using `pg_current_wal_lsn()` isn't technically correct, ideally
	// there would be some query we could execute to get an LSN corresponding to
	// the current transaction. But this will do until it becomes a problem or I
	// can find something better.
	//
	// I'll note that this is why Netflix's "DBLog" uses a "Watermark Table"
	// with a UUID for syncing up SELECT results with WAL entries, and say no
	// more about it for now.
	var txLSN pglogrepl.LSN
	if err := s.transaction.QueryRow(ctx, `SELECT * FROM pg_current_wal_lsn();`).Scan(&txLSN); err != nil {
		return errors.Wrap(err, "unable to query transaction LSN")
	}
	s.txLSN = txLSN
	return nil
}

func (s *TableSnapshotStream) TransactionLSN() pglogrepl.LSN {
	return s.txLSN
}

const (
	selectQueryStart = `SELECT ctid, * FROM %s.%s ORDER BY ctid LIMIT %d;`
	selectQueryNext  = `SELECT ctid, * FROM %s.%s WHERE ctid > $1 ORDER BY ctid LIMIT %d;`
	selectChunkSize  = 1
)

func (s *TableSnapshotStream) ScanStart(ctx context.Context, namespace, table string, handler ChangeEventHandler) (int, pgtype.TID, error) {
	log.Printf("Scanning table %q from start", table)
	rows, err := s.conn.Query(ctx, fmt.Sprintf(selectQueryStart, namespace, table, selectChunkSize))
	if err != nil {
		return 0, pgtype.TID{}, errors.Wrap(err, "unable to execute query")
	}
	defer rows.Close()
	return s.processQueryResults(ctx, namespace, table, rows, handler)
}

func (s *TableSnapshotStream) ScanFrom(ctx context.Context, namespace, table string, prevTID pgtype.TID, handler ChangeEventHandler) (int, pgtype.TID, error) {
	log.Printf("Scanning table %q from previous TID '(%d,%d)'", table, prevTID.BlockNumber, prevTID.OffsetNumber)
	rows, err := s.conn.Query(ctx, fmt.Sprintf(selectQueryNext, namespace, table, selectChunkSize), prevTID)
	if err != nil {
		return 0, prevTID, errors.Wrap(err, "unable to execute query")
	}
	defer rows.Close()
	return s.processQueryResults(ctx, namespace, table, rows, handler)
}

func (s *TableSnapshotStream) processQueryResults(ctx context.Context, namespace, table string, rows pgx.Rows, handler ChangeEventHandler) (int, pgtype.TID, error) {
	cols := rows.FieldDescriptions()
	var lastTID pgtype.TID
	var rowsProcessed int
	for rows.Next() {
		fields := make(map[string]interface{})
		vals, err := rows.Values()
		if err != nil {
			return rowsProcessed, lastTID, errors.Wrap(err, "unable to get row values")
		}
		for idx, val := range vals {
			colName := string(cols[idx].Name)
			if colName == "ctid" {
				if tid, ok := val.(pgtype.TID); ok {
					lastTID = tid
				}
				continue
			}
			fields[colName] = val
		}
		if err := handler(&ChangeEvent{
			Type:      "Insert",
			LSN:       s.txLSN,
			Namespace: namespace,
			Table:     table,
			Fields:    fields,
		}); err != nil {
			return rowsProcessed, lastTID, errors.Wrap(err, "error handling change event")
		}
		rowsProcessed++
	}
	return rowsProcessed, lastTID, nil
}

func (s *TableSnapshotStream) Close(ctx context.Context) error {
	if err := s.transaction.Rollback(ctx); err != nil {
		return err
	}
	return s.conn.Close(ctx)
}
