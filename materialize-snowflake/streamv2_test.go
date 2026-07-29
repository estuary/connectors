package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

// testSidecarPython returns a python interpreter from a venv with the real
// sidecar package (and the snowpipe-streaming SDK) installed, building the
// venv on first use. The venv is shared across test runs; the sidecar package
// itself is installed editable so source changes are always picked up.
func testSidecarPython(t *testing.T) string {
	t.Helper()
	systemPython, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	var venv = filepath.Join(os.TempDir(), "snowpipe-sidecar-test-venv")
	var python = filepath.Join(venv, "bin", "python")
	if _, err := os.Stat(python); err != nil {
		t.Logf("building sidecar test venv at %s", venv)
		if out, err := exec.Command(systemPython, "-m", "venv", venv).CombinedOutput(); err != nil {
			t.Fatalf("creating venv: %v: %s", err, out)
		}
		if out, err := exec.Command(filepath.Join(venv, "bin", "pip"), "install", "--quiet", "-e", "./sidecar").CombinedOutput(); err != nil {
			os.RemoveAll(venv)
			t.Fatalf("installing sidecar package: %v: %s", err, out)
		}
	}
	return python
}

// TestStreamV2Manager drives the direct-append write path against live
// Snowflake, which is the seam this design rests on: everything it guarantees
// is a statement about what Snowflake ends up holding.
func TestStreamV2Manager(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var ctx = context.Background()
	var cfg = mustGetCfg(t)
	t.Setenv("SNOWPIPE_SIDECAR_PYTHON", testSidecarPython(t))

	dsn, err := cfg.toURI(true, "")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	var accountName string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&accountName))

	var tableName = "STREAMV2_TEST"
	var cleanup = func() {
		db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	}
	cleanup()
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (key TEXT, intcol NUMBER, doc VARIANT);", tableName))
	require.NoError(t, err)
	defer cleanup()

	// Each subtest uses its own state key, and so its own channel, so that the
	// committed offset token one subtest leaves behind cannot reach another.
	var target = func(stateKey string) sql.Table {
		return sql.Table{
			TableShape: sql.TableShape{Binding: 0},
			Identifier: tableName,
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `INTCOL`}, {Identifier: `DOC`}},
			StateKey:   stateKey,
		}
	}

	var fullRange = &pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32}

	var newManager = func(keyRange *pf.RangeSpec) *streamV2Manager {
		var m = newStreamV2Manager(ctx, &cfg, "test/streamV2Materialization", accountName, keyRange)
		t.Cleanup(m.stop)
		return m
	}

	var countRows = func() int {
		var count int
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s;", tableName)).Scan(&count))
		return count
	}

	var truncate = func(t *testing.T) {
		_, err := db.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s;", tableName))
		require.NoError(t, err)
	}

	// writeRows stores the documents of the half-open range [lo, hi), which is
	// what a replay of the same transaction produces again.
	var writeRows = func(m *streamV2Manager, lo, hi int) {
		for i := lo; i < hi; i++ {
			require.NoError(t, m.writeRow(ctx, 0, []any{
				fmt.Sprintf("key-%d", i),
				i,
				json.RawMessage(fmt.Sprintf(`{"nested":{"i":%d},"arr":[1,2]}`, i)),
			}))
		}
	}

	// smallBatches cuts batches every two documents, so that a handful of rows
	// exercises the boundaries a production batch needs ten thousand for.
	var smallBatches = func(t *testing.T) {
		var restore = streamV2BatchRows
		t.Cleanup(func() { streamV2BatchRows = restore })
		streamV2BatchRows = 2
	}

	t.Run("happy path", func(t *testing.T) {
		truncate(t)
		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, tableName, target("happy.v1"), nil)

		writeRows(m, 0, 100)
		items, err := m.flush(ctx)
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.Equal(t, int64(100), items[0].Counter)
		require.NoError(t, m.waitCommit(ctx, items[0]))
		require.Equal(t, 100, countRows())

		// VARIANT columns must round-trip as real JSON objects, not strings.
		var docType string
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(`SELECT TYPEOF(doc) FROM %s WHERE key = 'key-7';`, tableName)).Scan(&docType))
		require.Equal(t, "OBJECT", docType)

		// A transaction which stores nothing for the binding reports no item, so
		// the counter already in the checkpoint stands.
		items, err = m.flush(ctx)
		require.NoError(t, err)
		require.Empty(t, items)

		// The counter continues across transactions rather than restarting.
		writeRows(m, 100, 150)
		items, err = m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(150), items[0].Counter)
		require.NoError(t, m.waitCommit(ctx, items[0]))
		require.Equal(t, 150, countRows())
	})

	t.Run("total buffered bytes are bounded", func(t *testing.T) {
		// A ceiling small enough to be crossed by nearly every document appends
		// buffers far short of a full batch. Batch boundaries are not part of the
		// contract, so the only thing that may change is the number of appends.
		truncate(t)
		var restore = streamV2MaxBufferedBytes
		t.Cleanup(func() { streamV2MaxBufferedBytes = restore })
		streamV2MaxBufferedBytes = 200

		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, tableName, target("bounded.v1"), nil)

		writeRows(m, 0, 50)
		items, err := m.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(50), items[0].Counter)
		require.NoError(t, m.waitCommit(ctx, items[0]))
		require.Equal(t, 50, countRows())
	})

	t.Run("recovery skips documents Snowflake already committed", func(t *testing.T) {
		truncate(t)
		smallBatches(t)
		var tgt = target("recovery.v1")

		// An interrupted transaction: two batches reach Snowflake and commit,
		// then the sidecar dies before flush, so no checkpoint is ever written
		// for this channel.
		var m1 = newManager(fullRange)
		m1.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		var channel = m1.bindings[0].channel

		writeRows(m1, 0, 5)
		require.NoError(t, m1.bindings[0].pipe.wait())
		_, err = m1.client.WaitCommit(ctx, channel, "4")
		require.NoError(t, err)
		require.Equal(t, 4, countRows())
		m1.sup.kill()

		// The runtime replays the identical document sequence. Snowflake already
		// holds the first four, so only the fifth is appended.
		var m2 = newManager(fullRange)
		m2.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)

		writeRows(m2, 0, 5)
		require.Equal(t, int64(4), m2.bindings[0].skip)
		items, err := m2.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(5), items[0].Counter)
		require.NoError(t, m2.waitCommit(ctx, items[0]))
		require.Equal(t, 5, countRows())

		// A second interruption, now with a checkpoint counter to reconcile the
		// committed token against.
		var checkpointed = items[0]
		writeRows(m2, 5, 8)
		require.NoError(t, m2.bindings[0].pipe.wait())
		_, err = m2.client.WaitCommit(ctx, channel, "7")
		require.NoError(t, err)
		require.Equal(t, 7, countRows())
		m2.sup.kill()

		var m3 = newManager(fullRange)
		m3.addBinding(cfg.Database, cfg.Schema, tableName, tgt, checkpointed)

		writeRows(m3, 5, 8)
		require.Equal(t, int64(7), m3.bindings[0].skip)
		items, err = m3.flush(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(8), items[0].Counter)
		require.NoError(t, m3.waitCommit(ctx, items[0]))
		require.Equal(t, 8, countRows())
	})

	t.Run("refusals", func(t *testing.T) {
		truncate(t)
		smallBatches(t)
		var tgt = target("refusal.v1")

		// Commit a transaction, then leave a further batch committed in
		// Snowflake beyond it, as an interruption would.
		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, tableName, tgt, nil)
		writeRows(m, 0, 2)
		items, err := m.flush(ctx)
		require.NoError(t, err)
		require.NoError(t, m.waitCommit(ctx, items[0]))
		var checkpointed = *items[0]

		writeRows(m, 2, 4)
		require.NoError(t, m.bindings[0].pipe.wait())
		_, err = m.client.WaitCommit(ctx, m.bindings[0].channel, "4")
		require.NoError(t, err)
		m.stop()

		// Each refusal is reported before the document which provoked it can be
		// appended, and the row count is unchanged by the attempt.
		var rowsBefore = countRows()

		t.Run("committed token below the checkpoint counter", func(t *testing.T) {
			var ahead = checkpointed
			ahead.Counter = 100

			var m2 = newManager(fullRange)
			m2.addBinding(cfg.Database, cfg.Schema, tableName, tgt, &ahead)
			require.ErrorContains(t, m2.writeRow(ctx, 0, []any{"k", 1, json.RawMessage(`{}`)}), "has lost committed data")
			require.Equal(t, rowsBefore, countRows())
		})

		t.Run("key range changed with uncommitted appends", func(t *testing.T) {
			// The shard kept its key-begin, and so its channel, but now covers
			// half the range: a replay would carry a different set of documents.
			var m2 = newManager(&pf.RangeSpec{KeyEnd: math.MaxUint32 / 2, RClockEnd: math.MaxUint32})
			m2.addBinding(cfg.Database, cfg.Schema, tableName, tgt, &checkpointed)
			require.ErrorContains(t, m2.writeRow(ctx, 0, []any{"k", 1, json.RawMessage(`{}`)}), "shard key range has changed")
			require.Equal(t, rowsBefore, countRows())
		})
	})

	t.Run("a row Snowflake rejects fails the transaction", func(t *testing.T) {
		// A row Snowflake's ingestion rejects takes a silent path of its own:
		// the append succeeds, the commit wait succeeds because the committed
		// offset token advances over the row, and the row is simply not in the
		// table. Snowflake reports it only in the channel's row-error
		// statistics, so the commit wait is where the transaction has to learn
		// of it rather than acknowledging rows it did not deliver.
		//
		// A null against a NOT NULL column is the rejection this reproduces:
		// omitting a column from the appended row object is how a nil converted
		// value reaches Snowflake as SQL NULL, and the connector marks a column
		// NOT NULL only for a field the collection schema requires, so the
		// runtime does not in fact deliver a nil for one. Nothing here is
		// specific to nulls — any rejected row takes the same path.
		var notNullTable = "STREAMV2_TEST_NOT_NULL"
		_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE TABLE %s (KEY TEXT, VAL VARIANT NOT NULL);", notNullTable))
		require.NoError(t, err)
		defer db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", notNullTable))

		var notNullTarget = sql.Table{
			TableShape: sql.TableShape{Binding: 0, DeltaUpdates: true},
			Identifier: notNullTable,
			Keys:       []sql.Column{{Identifier: `KEY`}},
			Values:     []sql.Column{{Identifier: `VAL`}},
			StateKey:   "notnull.v1",
		}

		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, notNullTable, notNullTarget, nil)
		var channel = m.bindings[0].channel

		require.NoError(t, m.writeRow(ctx, 0, []any{"kept", json.RawMessage(`{"a":1}`)}))
		require.NoError(t, m.writeRow(ctx, 0, []any{"dropped", nil}))
		items, err := m.flush(ctx)
		require.NoError(t, err)

		var waitErr = m.waitCommit(ctx, items[0])
		require.Error(t, waitErr)
		require.ErrorContains(t, waitErr, channel)

		// The failure carries Snowflake's own account of the rejection, rather
		// than this connector's guess at what Snowflake objected to.
		status, err := m.client.ChannelStatus(ctx, channel)
		require.NoError(t, err)
		require.Equal(t, int64(1), status.RowsErrorCount)
		require.NotEmpty(t, status.LastErrorMessage)
		require.ErrorContains(t, waitErr, status.LastErrorMessage)
		t.Logf("commit wait failed with: %s", waitErr)

		// The rejected row really is absent, which is what the failure is about.
		var keys string
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT LISTAGG(KEY, ',') FROM %s;", notNullTable)).Scan(&keys))
		require.Equal(t, "kept", keys)

		// The failure outlives the session that saw it. Snowflake's count is
		// cumulative for the life of the channel, and the transaction which
		// provoked the rejection never got to acknowledge, so a restart still
		// reconciles against a checkpoint accounting for none of it: it refuses
		// as the channel opens rather than replaying the transaction and
		// acknowledging it with the row still missing.
		m.sup.kill()
		var m2 = newManager(fullRange)
		m2.addBinding(cfg.Database, cfg.Schema, notNullTable, notNullTarget, items[0])
		require.ErrorContains(t,
			m2.writeRow(ctx, 0, []any{"kept", json.RawMessage(`{"a":1}`)}),
			"rejected and discarded by Snowflake")

		// Backfilling the binding is the recovery the failure asks for: it
		// rotates the channel, whose row-error count starts at zero along with
		// its document counter.
		var backfilled = notNullTarget
		backfilled.StateKey = "notnull.v2"

		var m3 = newManager(fullRange)
		m3.addBinding(cfg.Database, cfg.Schema, notNullTable, backfilled, nil)
		require.NoError(t, m3.writeRow(ctx, 0, []any{"backfilled", json.RawMessage(`{"a":2}`)}))
		items, err = m3.flush(ctx)
		require.NoError(t, err)
		require.NoError(t, m3.waitCommit(ctx, items[0]))

		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT LISTAGG(KEY, ',') WITHIN GROUP (ORDER BY KEY) FROM %s;", notNullTable)).Scan(&keys))
		require.Equal(t, "backfilled,kept", keys)
	})

	t.Run("unsupported table fails loud", func(t *testing.T) {
		// Streaming into a view is not possible. The SDK validates lazily, and
		// more lazily than the append: opening a channel against a view succeeds,
		// and so does appending to it — the rejection reaches the SDK's
		// background informer afterwards. So an incompatible table cannot be
		// detected before Store (there is no silent fall-through for one), and
		// what the connector sees is the commit wait failing promptly with a
		// classified error rather than hanging.
		_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE VIEW STREAMV2_TEST_VIEW AS SELECT * FROM %s;", tableName))
		require.NoError(t, err)
		defer db.ExecContext(ctx, "DROP VIEW IF EXISTS STREAMV2_TEST_VIEW;")

		var viewTarget = target("view.v1")
		viewTarget.Identifier = "STREAMV2_TEST_VIEW"

		var m = newManager(fullRange)
		m.addBinding(cfg.Database, cfg.Schema, "STREAMV2_TEST_VIEW", viewTarget, nil)

		require.NoError(t, m.writeRow(ctx, 0, []any{"k", 1, json.RawMessage(`{}`)}))
		items, err := m.flush(ctx)
		require.NoError(t, err)

		var start = time.Now()
		err = m.waitCommit(ctx, items[0])
		require.Error(t, err)
		var scErr *sidecarError
		require.ErrorAs(t, err, &scErr)
		t.Logf("view ingestion failed after %s: code %s message %s", time.Since(start), scErr.Code, scErr.Message)
	})
}

// TestStreamV2Datatypes sweeps every Snowflake column type this connector's
// dialect emits through the direct-append write path, one table and channel per
// type, and snapshots what Snowflake ends up holding.
//
// This coverage used to be a data-types binding of the integration-harness
// fixture. That fixture now proves only that the write path refuses a task which
// is not on the v2 runtime, which is all `flowctl preview` can drive, so the type
// sweep lives at the seam that actually appends: the manager against live
// Snowflake. The values of each type follow the classic path's sweep in
// TestStreamDatatypes, narrowed to the shapes this connector's converters emit,
// so that the two write paths can be compared column by column.
func TestStreamV2Datatypes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	var ctx = context.Background()
	var cfg = mustGetCfg(t)
	t.Setenv("SNOWPIPE_SIDECAR_PYTHON", testSidecarPython(t))

	dsn, err := cfg.toURI(true, "")
	require.NoError(t, err)
	db, err := stdsql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	var accountName string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT CURRENT_ACCOUNT()").Scan(&accountName))

	var templates = renderTemplates(testDialect)

	// One manager, and so one sidecar, serves every type: each type is a binding
	// of its own, with a state key of its own and therefore a channel of its own.
	var m = newStreamV2Manager(ctx, &cfg, "test/streamV2Datatypes", accountName,
		&pf.RangeSpec{KeyEnd: math.MaxUint32, RClockEnd: math.MaxUint32})
	t.Cleanup(m.stop)

	// The three timestamp types take the same values: offsets Snowflake must
	// resolve rather than truncate, and both ends of the range it accepts.
	var timestamps = []any{
		"2022-08-17T04:32:19.987654321Z",
		"2022-08-17T04:32:19.987654321+02:00",
		"2022-08-17T04:32:19.987654321+03:30",
		"2022-08-17T04:32:19.987654321-07:00",
		"0000-01-01T00:00:00.000000000Z",
		"0000-01-01T00:00:00.000000000+23:59",
		"0000-01-01T00:00:00.000000000-23:59",
		"9999-12-31T23:59:59.999999999Z",
		"9999-12-31T23:59:59.999999999+23:59",
		"9999-12-31T23:59:59.999999999-23:59",
		nil,
	}

	for binding, tt := range []struct {
		ddl  string
		vals []any
	}{
		{
			ddl: "INTEGER",
			// Both ends of what NUMBER(38,0) holds, which a `format: integer`
			// string reaches as a big.Int, along with a magnitude only a float64
			// can carry.
			vals: []any{-1, 0, 1, 1234, math.MaxInt64, nil, math.MinInt64, float64(math.MaxUint64) * 10_000, minSnowflakeInteger.BigInt(), maxSnowflakeInteger.BigInt(), uint64(math.MaxUint64)},
		},
		{
			ddl: "TEXT",
			// A binary field materialized without the native BINARY column type
			// arrives as raw bytes and lands base64-encoded, as it does through a
			// staged file, which decodes only columns that are BINARY.
			vals: []any{"hello", "", nil, "1234", "true", "1234.1234", []byte("hello bytes")},
		},
		{
			ddl: "BINARY",
			// The column type native_binary_column_type produces, which is on by
			// default. A []byte is JSON-encoded as base64 on its way to the SDK,
			// so these prove Snowflake decodes that back into the column's bytes
			// — including bytes which are not valid UTF-8, and so could not have
			// survived as text.
			vals: []any{[]byte("hello bytes"), []byte{}, nil, []byte{0x00, 0xff, 0xfe}, []byte{0xde, 0xad, 0xbe, 0xef}},
		},
		{
			ddl: "VARIANT",
			// Every value of a VARIANT column passes through ToJsonBytes, so the
			// connector only ever appends pre-encoded JSON or nothing. An absent
			// or JSON-null field must land as SQL NULL, as it does through the
			// classic path, and not as a VARIANT holding null: the two encodings
			// answer `IS NULL` and `TYPEOF` differently, so a table written by
			// one path would not be interchangeable with one written by the other.
			vals: []any{
				json.RawMessage(`"hello"`),
				json.RawMessage(`""`),
				nil,
				json.RawMessage(`1234`),
				json.RawMessage(`true`),
				json.RawMessage(`1234.1234`),
				json.RawMessage(`[1,2,3]`),
				json.RawMessage(`{"some":"doc"}`),
				json.RawMessage(`{"nested":{"deep":["a",1,null,{"b":true},[]]},"unicode":"你好 🌟"}`),
			},
		},
		{
			ddl:  "BOOLEAN",
			vals: []any{true, false, nil},
		},
		{
			ddl: "FLOAT",
			// "NaN", "inf" and "-inf" are the sentinels this dialect substitutes
			// for the special values of a `format: number` string, which have no
			// JSON representation of their own.
			vals: []any{int64(1234), nil, 1234.0, 4321.1234, math.MaxFloat64, math.SmallestNonzeroFloat64, "NaN", "inf", "-inf"},
		},
		{
			ddl:  "DATE",
			vals: []any{"2022-03-21", nil, "0000-01-01", "9999-12-31"},
		},
		{
			ddl:  "TIMESTAMP_LTZ",
			vals: timestamps,
		},
		{
			ddl:  "TIMESTAMP_TZ",
			vals: timestamps,
		},
		{
			ddl:  "TIMESTAMP_NTZ",
			vals: timestamps,
		},
	} {
		t.Run(tt.ddl, func(t *testing.T) {
			var tbl = sql.Table{
				TableShape: sql.TableShape{Binding: binding, DeltaUpdates: true},
				Identifier: "STREAMV2_DATATYPES_" + tt.ddl,
				Keys:       []sql.Column{{Identifier: `KEY`, MappedType: sql.MappedType{DDL: "INTEGER"}}},
				Values:     []sql.Column{{Identifier: `VAL`, MappedType: sql.MappedType{DDL: tt.ddl}}},
				StateKey:   "datatypes." + tt.ddl,
			}

			// Dropping the table drops the channels opened against it, so each
			// run starts from a channel with no committed offset token and
			// nothing to skip.
			var cleanup = func() {
				db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", tbl.Identifier))
			}
			cleanup()
			t.Cleanup(cleanup)

			var createQuery strings.Builder
			require.NoError(t, templates.createTargetTable.Execute(&createQuery, &tbl))
			_, err := db.ExecContext(ctx, createQuery.String())
			require.NoError(t, err)

			m.addBinding(cfg.Database, cfg.Schema, tbl.Identifier, tbl, nil)
			for i, val := range tt.vals {
				require.NoError(t, m.writeRow(ctx, binding, []any{i, val}))
			}

			items, err := m.flush(ctx)
			require.NoError(t, err)
			require.Len(t, items, 1, "only the binding stored to this transaction reports an item")
			require.Equal(t, int64(len(tt.vals)), items[binding].Counter)
			require.NoError(t, m.waitCommit(ctx, items[binding]))

			dump, err := sql.StdDumpTable(ctx, db, tbl)
			require.NoError(t, err)
			cupaloy.SnapshotT(t, dump)
		})
	}
}
