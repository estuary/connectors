package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
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
	selectQueryStart = `SELECT * FROM %s.%s ORDER BY %s LIMIT %d;`
	selectQueryNext  = `SELECT * FROM %s.%s WHERE %s > $1 ORDER BY %s LIMIT %d;`
	selectChunkSize  = 2 // TODO(wgd): Make this much much larger after testing
)

func buildScanQuery(namespace, table string, pkey []string, isStart bool) string {
	pkeyTupleExpr := ""
	argsTupleExpr := ""
	for idx, keyName := range pkey {
		if idx > 0 {
			pkeyTupleExpr += ", "
			argsTupleExpr += ", "
		}
		pkeyTupleExpr += keyName
		argsTupleExpr += fmt.Sprintf("$%d", idx+1)
	}

	query := fmt.Sprintf("SELECT * FROM %s.%s", namespace, table)
	if !isStart {
		query += fmt.Sprintf(" WHERE (%s) > (%s)", pkeyTupleExpr, argsTupleExpr)
	}
	query += fmt.Sprintf(" ORDER BY (%s)", pkeyTupleExpr)
	query += fmt.Sprintf(" LIMIT %d;", selectChunkSize)
	log.Printf("Built Scan Query: %q", query)
	return query
}

func (s *TableSnapshotStream) ScanStart(ctx context.Context, namespace, table string, pkey []string, handler ChangeEventHandler) (int, []byte, error) {
	log.Printf("Scanning table %q from start", table)
	time.Sleep(1 * time.Second)
	query := buildScanQuery(namespace, table, pkey, true)
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return 0, nil, errors.Wrap(err, "unable to execute query")
	}
	defer rows.Close()
	return s.dispatchResults(ctx, namespace, table, pkey, rows, handler)
}

func (s *TableSnapshotStream) ScanFrom(ctx context.Context, namespace, table string, pkey []string, prevKey []byte, handler ChangeEventHandler) (int, []byte, error) {
	log.Printf("Scanning table %q from previous key %q", table, base64.StdEncoding.EncodeToString(prevKey))
	args, err := unpackTuple(prevKey)
	if err != nil {
		return 0, nil, errors.Wrap(err, "error unpacking encoded tuple")
	}
	if len(args) != len(pkey) {
		return 0, nil, errors.Errorf("expected %d primary-key values but got %d", len(pkey), len(args))
	}
	query := buildScanQuery(namespace, table, pkey, false)
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return 0, prevKey, errors.Wrap(err, "unable to execute query")
	}
	defer rows.Close()
	return s.dispatchResults(ctx, namespace, table, pkey, rows, handler)
}

func (s *TableSnapshotStream) dispatchResults(ctx context.Context, namespace, table string, pkey []string, rows pgx.Rows, handler ChangeEventHandler) (int, []byte, error) {
	cols := rows.FieldDescriptions()
	var lastKey []byte
	var rowsProcessed int
	for rows.Next() {
		// Fill out the `fields` map with column name-value mappings
		fields := make(map[string]interface{})
		vals, err := rows.Values()
		if err != nil {
			return rowsProcessed, nil, errors.Wrap(err, "unable to get row values")
		}
		for idx, val := range vals {
			colName := string(cols[idx].Name)
			fields[colName] = val
		}

		// Encode this row's primary key as a serialized tuple, and as a sanity
		// check require that it be greater than the previous row's key.
		keyFields := make([]interface{}, len(pkey))
		for idx, colName := range pkey {
			keyFields[idx] = fields[colName]
		}
		rowKey, err := packTuple(keyFields)
		if err != nil {
			return rowsProcessed, nil, errors.Wrap(err, "unable to encode row primary key")
		}
		// TODO(wgd): Should this check be made optional or break-glass-able somehow?
		if lastKey != nil && compareTuples(lastKey, rowKey) >= 0 {
			// This is an opportunistic sanity-check of what should be an invariant.
			// This check should only fail if PostgreSQL returns result rows in an order
			// which does not match the lexicographical sort order of the encoded tuples.
			//
			// The failure mode that this is guarding against is very much a corner case:
			//
			//   + If the ordering of encoded tuples doesn't precisely match the equivalent
			//     PostgreSQL `ORDER BY` ordering.
			//   + ...and the scan of some particular table is interrupted and restarted
			//     across multiple runs of this source connector.
			//   + ...and there are concurrent modifications to the table, on rows which
			//     fall in different "connector restarts" according to PostgreSQL and our
			//     own tuple comparison.
			//
			// Then it is possible for these concurrent modifications to either be repeated
			// (the replication event should have been filtered but wasn't) or omitted (the
			// replication event was filtered when it shouldn't have been) once replication
			// begins.
			return rowsProcessed, nil, errors.Errorf("primary key ordering failure: prev=%q, next=%q", lastKey, rowKey)
		}
		lastKey = rowKey

		if err := handler(&ChangeEvent{
			Type:      "Insert",
			LSN:       s.txLSN,
			Namespace: namespace,
			Table:     table,
			Fields:    fields,
		}); err != nil {
			return rowsProcessed, nil, errors.Wrap(err, "error handling change event")
		}
		rowsProcessed++
	}
	return rowsProcessed, lastKey, nil
}

func (s *TableSnapshotStream) Close(ctx context.Context) error {
	if err := s.transaction.Rollback(ctx); err != nil {
		return err
	}
	return s.conn.Close(ctx)
}
