package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	"github.com/estuary/protocols/fdb/tuple"
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
	return s.processQueryResults(ctx, namespace, table, pkey, rows, handler)
}

// We translate a list of column values (representing the primary key of a
// database row) into a list of bytes using the FoundationDB tuple encoding
// package. This tuple encoding has the useful property that lexicographic
// comparison of these bytes is equivalent to lexicographic comparison of
// the equivalent values, and thus we don't have to write code to do that.
//
// Encoding primary keys like this also makes `state.json` round-tripping
// across restarts trivial.
func packTuple(xs []interface{}) (bs []byte, err error) {
	var t []tuple.TupleElement
	for _, x := range xs {
		// Values not natively supported by the FoundationDB tuple encoding code
		// must be converted into ones that are.
		switch x := x.(type) {
		case int32:
			t = append(t, int(x))
		default:
			t = append(t, x)
		}
	}

	// The `Pack()` function doesn't have an error return value, and instead
	// will just panic with a string value if given an unsupported value type.
	// This defer will catch and translate these panics into proper error returns.
	defer func() {
		if r := recover(); r != nil {
			if errStr, ok := r.(string); ok {
				err = errors.New(errStr)
			} else {
				panic(r)
			}
		}
	}()
	return tuple.Tuple(t).Pack(), nil
}

func unpackTuple(bs []byte) ([]interface{}, error) {
	t, err := tuple.Unpack(bs)
	if err != nil {
		return nil, err
	}
	var xs []interface{}
	for _, elem := range t {
		xs = append(xs, elem)
	}
	return xs, nil
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
	return s.processQueryResults(ctx, namespace, table, pkey, rows, handler)
}

func (s *TableSnapshotStream) processQueryResults(ctx context.Context, namespace, table string, pkey []string, rows pgx.Rows, handler ChangeEventHandler) (int, []byte, error) {
	cols := rows.FieldDescriptions()
	// Allocate the `lastKey` array just once, we'll give it new values each time through the loop
	lastKey := make([]interface{}, len(pkey))
	var rowsProcessed int
	for rows.Next() {
		fields := make(map[string]interface{})
		vals, err := rows.Values()
		if err != nil {
			return rowsProcessed, nil, errors.Wrap(err, "unable to get row values")
		}
		// Fill out the `fields` map with column name-value mappings
		for idx, val := range vals {
			colName := string(cols[idx].Name)
			fields[colName] = val
		}
		// Fill out the `lastKey` array with field values corresponding to `pkey` column names
		for idx, colName := range pkey {
			lastKey[idx] = fields[colName]
		}
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
	if rowsProcessed == 0 {
		return 0, nil, nil
	}
	key, err := packTuple(lastKey)
	if err != nil {
		return rowsProcessed, nil, err
	}
	return rowsProcessed, key, nil
}

func (s *TableSnapshotStream) Close(ctx context.Context) error {
	if err := s.transaction.Rollback(ctx); err != nil {
		return err
	}
	return s.conn.Close(ctx)
}
