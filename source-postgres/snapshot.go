package main

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/pkg/errors"
)

type TableSnapshotStream struct {
	conn        *pgx.Conn
	transaction pgx.Tx
	namespace   string
	table       string
	txLSN       pglogrepl.LSN
}

func SnapshotTable(ctx context.Context, conn *pgx.Conn, namespace, table string) (*TableSnapshotStream, error) {
	transaction, err := conn.BeginTx(ctx, pgx.TxOptions{
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
		namespace:   namespace,
		table:       table,
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

func (s *TableSnapshotStream) Process(ctx context.Context, handler ChangeEventHandler) error {
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`SELECT * FROM %s.%s;`, s.namespace, s.table))
	if err != nil {
		return errors.Wrap(err, "unable to execute query")
	}
	defer rows.Close()
	cols := rows.FieldDescriptions()
	for rows.Next() {
		fields := make(map[string]interface{})
		vals, err := rows.Values()
		if err != nil {
			return errors.Wrap(err, "unable to get row values")
		}
		for idx, val := range vals {
			colName := string(cols[idx].Name)
			fields[colName] = val
		}
		if err := handler("Insert", s.txLSN, s.namespace, s.table, fields); err != nil {
			return errors.Wrap(err, "error handling change event")
		}
	}
	return nil
}

func (s *TableSnapshotStream) Close(ctx context.Context) error {
	if err := s.transaction.Rollback(ctx); err != nil {
		return err
	}
	return s.conn.Close(ctx)
}
