package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

// A databaseSnapshot represents a long-running read transaction which
// can be used to query the contents of various tables.
type databaseSnapshot struct {
	Transaction pgx.Tx
	TxLSN       pglogrepl.LSN
}

// A tableSnapshot represents a consistent view of a single table in
// the database. A TableSnapshot cannot be closed because it's really
// just a struct combining a DatabaseSnapshot with a specific table
// name and scanning key.
type tableSnapshot struct {
	DB         *databaseSnapshot
	SchemaName string
	TableName  string
	ScanKey    []string

	scanFromQuery string
}

// snapshotChunkSize controls how many rows will be read from the database in a
// single query. In normal use it acts like a constant, it's just a variable here
// so that it can be lowered in tests to exercise chunking behavior more easily.
var snapshotChunkSize = 4096

func snapshotDatabase(ctx context.Context, conn *pgx.Conn) (*databaseSnapshot, error) {
	transaction, err := conn.BeginTx(ctx, pgx.TxOptions{
		// We could probably get away with `RepeatableRead` isolation here, but
		// I see no reason not to err on the side of getting the strongest
		// guarantees that we can, especially since the full scan is not the
		// normal mode of operation and happens just once per table.
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to begin transaction: %w", err)
	}
	snapshot := &databaseSnapshot{
		Transaction: transaction,
	}
	// TODO(wgd): Empirically this function doesn't have a stable value during our read
	// transaction, and I don't know what ordering guarantees we might actually have.
	// Even though this is what Debezium does I remain unconvinced that it actually works
	// in all circumstances. Revisit this after writing some sort of concurrent-changes
	// stress test which could reveal problems.
	if err := transaction.QueryRow(ctx, `SELECT * FROM pg_current_wal_lsn();`).Scan(&snapshot.TxLSN); err != nil {
		snapshot.Close(ctx)
		return nil, fmt.Errorf("unable to query transaction LSN: %w", err)
	}
	return snapshot, nil
}

func (s *databaseSnapshot) TransactionLSN() pglogrepl.LSN {
	return s.TxLSN
}

func (s *databaseSnapshot) Table(tableID string, scanKey []string) *tableSnapshot {
	// Split "public.foo" tableID into "public" schema and "foo" table name
	parts := strings.SplitN(tableID, ".", 2)
	schemaName, tableName := parts[0], parts[1]

	return &tableSnapshot{
		DB:         s,
		SchemaName: schemaName,
		TableName:  tableName,
		ScanKey:    scanKey,
	}
}

func (s *databaseSnapshot) Close(ctx context.Context) error {
	return s.Transaction.Rollback(ctx)
}

func (s *tableSnapshot) TransactionLSN() pglogrepl.LSN {
	return s.DB.TransactionLSN()
}

func (s *tableSnapshot) buildScanQuery(start bool) string {
	// We cache the query used in start=false cases (ScanFrom) to avoid
	// redundantly building it over and over for no reason.
	if !start && s.scanFromQuery != "" {
		return s.scanFromQuery
	}

	// Construct strings like `(foo, bar, baz)` and `($1, $2, $3)` for use in the query
	var pkey, args string
	for idx, colName := range s.ScanKey {
		if idx > 0 {
			pkey += ", "
			args += ", "
		}
		pkey += colName
		args += fmt.Sprintf("$%d", idx+1)
	}

	// Construct the query itself
	query := new(strings.Builder)
	fmt.Fprintf(query, "SELECT * FROM %s.%s", s.SchemaName, s.TableName)
	if !start {
		fmt.Fprintf(query, " WHERE (%s) > (%s)", pkey, args)
	}
	fmt.Fprintf(query, " ORDER BY (%s)", pkey)
	fmt.Fprintf(query, " LIMIT %d;", snapshotChunkSize)

	// Cache where applicable and return
	if !start {
		s.scanFromQuery = query.String()
		return s.scanFromQuery
	}
	return query.String()
}

func (s *tableSnapshot) ScanStart(ctx context.Context, handler changeEventHandler) (int, []byte, error) {
	logrus.WithFields(logrus.Fields{
		"namespace":  s.SchemaName,
		"table":      s.TableName,
		"primaryKey": s.ScanKey,
		"txLSN":      s.TransactionLSN(),
	}).Info("starting table scan")

	query := s.buildScanQuery(true)
	logrus.WithField("query", query).Debug("executing query")
	rows, err := s.DB.Transaction.Query(ctx, query)
	if err != nil {
		return 0, nil, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer rows.Close()
	return s.dispatchResults(ctx, rows, handler)
}

func (s *tableSnapshot) ScanFrom(ctx context.Context, prevKey []byte, handler changeEventHandler) (int, []byte, error) {
	logrus.WithFields(logrus.Fields{
		"namespace":  s.SchemaName,
		"table":      s.TableName,
		"primaryKey": s.ScanKey,
		"resumeKey":  base64.StdEncoding.EncodeToString(prevKey),
	}).Debug("scanning next chunk")

	args, err := unpackTuple(prevKey)
	if err != nil {
		return 0, nil, fmt.Errorf("error unpacking encoded tuple: %w", err)
	}
	if len(args) != len(s.ScanKey) {
		return 0, nil, fmt.Errorf("expected %d primary-key values but got %d", len(s.ScanKey), len(args))
	}
	query := s.buildScanQuery(false)
	logrus.WithField("query", query).WithField("args", args).Debug("executing query")
	rows, err := s.DB.Transaction.Query(ctx, query, args...)
	if err != nil {
		return 0, prevKey, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer rows.Close()
	return s.dispatchResults(ctx, rows, handler)
}

func (s *tableSnapshot) dispatchResults(ctx context.Context, rows pgx.Rows, handler changeEventHandler) (int, []byte, error) {
	cols := rows.FieldDescriptions()
	var lastKey []byte
	var rowsProcessed int
	for rows.Next() {
		// Fill out the `fields` map with column name-value mappings
		fields := make(map[string]interface{})
		vals, err := rows.Values()
		if err != nil {
			return rowsProcessed, nil, fmt.Errorf("unable to get row values: %w", err)
		}
		for idx, val := range vals {
			colName := string(cols[idx].Name)
			fields[colName] = val
		}

		// Encode this row's primary key as a serialized tuple, and as a sanity
		// check require that it be greater than the previous row's key.
		keyFields := make([]interface{}, len(s.ScanKey))
		for idx, colName := range s.ScanKey {
			keyFields[idx] = fields[colName]
		}
		rowKey, err := packTuple(keyFields)
		if err != nil {
			return rowsProcessed, nil, fmt.Errorf("unable to encode row primary key: %w", err)
		}

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
		if lastKey != nil && compareTuples(lastKey, rowKey) >= 0 {
			return rowsProcessed, nil, fmt.Errorf("primary key ordering failure: prev=%q, next=%q", lastKey, rowKey)
		}
		lastKey = rowKey

		if err := handler(&changeEvent{
			Type:      "Insert",
			LSN:       s.TransactionLSN(),
			Namespace: s.SchemaName,
			Table:     s.TableName,
			Fields:    fields,
		}); err != nil {
			return rowsProcessed, nil, fmt.Errorf("error handling change event: %w", err)
		}
		rowsProcessed++
	}
	return rowsProcessed, lastKey, nil
}
