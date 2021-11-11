package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v4"
	"github.com/sirupsen/logrus"
)

// TODO(wgd): Define an interface with a `ScanTableChunk(ctx, streamID, keyColumns, resumeKey)` and
// a `WriteWatermark(ctx, table, slot)` method which serves as the "database backfill" API
// that we'll have to implement for other databases.

// scanTableChunk fetches a chunk of rows from the specified table, resuming from the provided
// `resumeKey` if non-nil. This is the entrypoint to the database-specific portion of the
// backfill process.
func scanTableChunk(ctx context.Context, conn *pgx.Conn, streamID string, keyColumns []string, resumeKey []byte) ([]*changeEvent, error) {
	var snapshot, err = snapshotDatabase(ctx, conn)
	if err != nil {
		return nil, fmt.Errorf("error creating database snapshot: %w", err)
	}
	defer snapshot.Close(ctx)

	events, err := snapshot.Table(streamID, keyColumns).ScanChunk(ctx, resumeKey)
	if err != nil {
		return nil, fmt.Errorf("error scanning table: %w", err)
	}

	return events, nil
}

// writeWatermark writes a new random UUID into the 'watermarks' table and returns the
// UUID. This is also an entrypoint to the database-specific portion of the backfill.
func writeWatermark(ctx context.Context, conn *pgx.Conn, table, slot string) (string, error) {
	// Generate a watermark UUID
	var wm = uuid.New().String()

	var query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (slot TEXT PRIMARY KEY, watermark TEXT);", table)
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return "", fmt.Errorf("error creating watermarks table: %w", err)
	}
	rows.Close()

	query = fmt.Sprintf(`INSERT INTO %s (slot, watermark) VALUES ($1,$2) ON CONFLICT (slot) DO UPDATE SET watermark = $2;`, table)
	rows, err = conn.Query(ctx, query, slot, wm)
	if err != nil {
		return "", fmt.Errorf("error upserting new watermark for slot %q: %w", slot, err)
	}
	rows.Close()

	logrus.WithField("watermark", wm).Debug("wrote watermark")
	return wm, nil
}

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
	KeyColumns []string

	scanFromQuery string
}

// snapshotChunkSize controls how many rows will be read from the database in a
// single query. In normal use it acts like a constant, it's just a variable here
// so that it can be lowered in tests to exercise chunking behavior more easily.
var snapshotChunkSize = 4096

func snapshotDatabase(ctx context.Context, conn *pgx.Conn) (*databaseSnapshot, error) {
	var transaction, err = conn.BeginTx(ctx, pgx.TxOptions{
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
	var snapshot = &databaseSnapshot{
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

func (s *databaseSnapshot) Table(tableID string, keyColumns []string) *tableSnapshot {
	// Split "public.foo" tableID into "public" schema and "foo" table name
	var parts = strings.SplitN(tableID, ".", 2)
	var schemaName, tableName = parts[0], parts[1]

	return &tableSnapshot{
		DB:         s,
		SchemaName: schemaName,
		TableName:  tableName,
		KeyColumns: keyColumns,
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
	for idx, colName := range s.KeyColumns {
		if idx > 0 {
			pkey += ", "
			args += ", "
		}
		pkey += colName
		args += fmt.Sprintf("$%d", idx+1)
	}

	// Construct the query itself
	var query = new(strings.Builder)
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

// ScanChunk requests a chunk of rows from the table snapshot, transforms each row into
// an `Insert` change event and gives it to the provided handler. If `resumeKey` is nil
// the very first chunk of the table is scanned, otherwise it must contain the final
// key from a previous chunk, and this chunk will begin immediately after that.
func (s *tableSnapshot) ScanChunk(ctx context.Context, resumeKey []byte) ([]*changeEvent, error) {
	var logFields = logrus.Fields{
		"namespace":  s.SchemaName,
		"table":      s.TableName,
		"keyColumns": s.KeyColumns,
		"resumeKey":  base64.StdEncoding.EncodeToString(resumeKey),
		"txLSN":      s.TransactionLSN(),
	}
	logrus.WithFields(logFields).Debug("scanning table chunk")

	var query = s.buildScanQuery(resumeKey == nil)
	var args []interface{}
	if resumeKey != nil {
		var err error
		args, err = unpackTuple(resumeKey)
		if err != nil {
			return nil, fmt.Errorf("error unpacking encoded tuple: %w", err)
		}
		if len(args) != len(s.KeyColumns) {
			return nil, fmt.Errorf("expected %d primary-key values but got %d", len(s.KeyColumns), len(args))
		}
	}

	logrus.WithField("query", query).WithField("args", args).Debug("executing query")
	rows, err := s.DB.Transaction.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("unable to execute query %q: %w", query, err)
	}
	defer rows.Close()
	return s.processResults(ctx, rows)
}

func (s *tableSnapshot) processResults(ctx context.Context, rows pgx.Rows) ([]*changeEvent, error) {
	var cols = rows.FieldDescriptions()
	var events []*changeEvent
	for rows.Next() {
		// Scan the row values and copy into the equivalent map
		var vals, err = rows.Values()
		if err != nil {
			return nil, fmt.Errorf("unable to get row values: %w", err)
		}
		var fields = make(map[string]interface{})
		for idx := range cols {
			fields[string(cols[idx].Name)] = vals[idx]
		}

		events = append(events, &changeEvent{
			Type:      "Insert",
			Namespace: s.SchemaName,
			Table:     s.TableName,
			Fields:    fields,
		})
	}
	return events, nil
}
