//go:build !nodb

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgtype"
	pgxStd "github.com/jackc/pgx/v4/stdlib"
	"github.com/stretchr/testify/require"
)

func TestMultiBatchTransaction(t *testing.T) {
	var ctx = context.Background()

	var db, err = sql.Open("pgx", "postgresql://postgres:postgres@localhost:5432/postgres")
	require.NoError(t, err)
	conn, err := pgxStd.AcquireConn(db)
	require.NoError(t, err)

	// First half of transaction creates a temporary table, prepares a statement
	// to load into it, and does some loads.
	var batch = newPGBatch()
	batch.queue(nil, "begin;")
	batch.queue(nil, "create temporary table foobar (key integer not null, value text) on commit delete rows;")
	batch.queue(nil, "prepare xyz as insert into foobar (key, value) values ($1, $2);")
	batch.queueParams(nil, "execute xyz", 1, "hi")
	batch.queueParams(nil, "execute xyz", 2, nil)
	require.NoError(t, batch.roundTrip(ctx, conn.PgConn()))

	// Second half completes loads, scans the table, commits, and scans again.
	var actual, empty snap
	batch.queueParams(nil, "execute xyz", 3, json.RawMessage(`{"bye":42}`))
	batch.queue(nil, "deallocate prepare xyz;")
	batch.queue(debugSnap(conn.ConnInfo(), &actual), "select key, value from foobar;")
	batch.queue(nil, "commit;")
	batch.queue(debugSnap(conn.ConnInfo(), &empty), "select key, value from foobar;")
	require.NoError(t, batch.roundTrip(ctx, conn.PgConn()))

	// Expect we scanned our full table into `actual`.
	require.Equal(t, snap{
		Names: []string{"key", "value"},
		Rows: [][]interface{}{
			{int32(1), "hi"},
			{int32(2), nil},
			{int32(3), `{"bye":42}`},
		},
		Tag: "SELECT 3",
	}, actual)
	// Empty scan reflects that rows were dropped.
	require.Equal(t, snap{
		Names: []string{"key", "value"},
		Rows:  nil,
		Tag:   "SELECT 0",
	}, empty)
}

func TestBatchScanTypes(t *testing.T) {
	var ctx = context.Background()

	var db, err = sql.Open("pgx", "postgresql://postgres:postgres@localhost:5432/postgres")
	require.NoError(t, err)
	conn, err := pgxStd.AcquireConn(db)
	require.NoError(t, err)

	// Variables to hold scan results.
	var intVal int
	var docVal json.RawMessage
	var bitsVal []bool

	var selectFn = func(rr *pgconn.ResultReader) error {
		for rr.NextRow() {
			require.NoError(t, scanRow(conn.ConnInfo(), rr.FieldDescriptions(), rr.Values(),
				&intVal, &docVal, &bitsVal))
		}
		return nil
	}

	var batch = newPGBatch()
	batch.queue(nil, "begin;")
	batch.queue(nil, "create temporary table foobar (int integer, doc json, bits boolean[]) on commit delete rows;")
	batch.queue(nil, `insert into foobar (int, doc, bits) values (42, '{"hello":"world"}', '{true, false, true}');`)
	batch.queue(selectFn, `select * from foobar;`)
	batch.queue(nil, "rollback;")
	require.NoError(t, batch.roundTrip(ctx, conn.PgConn()))

	// Expect we scanned out expected values.
	require.Equal(t, 42, intVal)
	require.Equal(t, `{"hello":"world"}`, string(docVal))
	require.Equal(t, []bool{true, false, true}, bitsVal)
}

func TestBatchErrorCases(t *testing.T) {
	var ctx = context.Background()

	var db, err = sql.Open("pgx", "postgresql://postgres:postgres@localhost:5432/postgres")
	require.NoError(t, err)
	conn, err := pgxStd.AcquireConn(db)
	require.NoError(t, err)

	// Case: use of multiple queues for a single statement.
	var batch = newPGBatch()
	batch.queue(nil, "select ")
	batch.queue(nil, "'hello',")
	batch.queue(nil, "'world';")

	require.EqualError(t, batch.roundTrip(ctx, conn.PgConn()),
		"all server results received, but still have 2 remaining handlers")

	// Case: use of one queue for multiple statements.
	batch = newPGBatch()

	batch.queue(nil, "select 'hello'; select 'world' as foo, 5 as bar;")

	require.EqualError(t, batch.roundTrip(ctx, conn.PgConn()),
		"all queued queries are processed but have extra server result SELECT 1 with fields [foo bar] (OID [25 23])")
}

type snap struct {
	Names []string
	Rows  [][]interface{}
	Tag   string
}

func debugSnap(
	connInfo *pgtype.ConnInfo,
	out *snap,
) func(rr *pgconn.ResultReader) error {
	return func(rr *pgconn.ResultReader) error {
		*out = snap{} // Reset.

		var fields = rr.FieldDescriptions()
		for _, f := range rr.FieldDescriptions() {
			out.Names = append(out.Names, string(f.Name))
		}
		for rr.NextRow() {
			var row, scan = make([]interface{}, len(fields)), make([]interface{}, len(fields))
			for i := range scan {
				scan[i] = &row[i] // Scan into *interface{} to allocate a runtime value.
			}
			if err := scanRow(connInfo, fields, rr.Values(), scan...); err != nil {
				return err
			}
			out.Rows = append(out.Rows, row)
		}

		var tag, err = rr.Close()
		out.Tag = string(tag)
		return err
	}
}
