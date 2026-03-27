//go:build !nodb

package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func testConfig() config {
	return config{
		Address: "localhost:9000",
		Credentials: credentialConfig{
			AuthType:         UserPass,
			usernamePassword: usernamePassword{Username: "flow", Password: "flow"},
		},
		Database: "flow",
		Advanced: advancedConfig{
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}
}

func clickHouseGetSchema(ctx context.Context, db *stdsql.DB, database string, table string) (string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"SELECT name, type FROM system.columns WHERE database = '%s' AND table = '%s' ORDER BY position",
		database, table,
	))
	if err != nil {
		return "", err
	}
	defer rows.Close()

	type foundColumn struct {
		Name string
		Type string
	}

	cols := []foundColumn{}
	for rows.Next() {
		var c foundColumn
		if err := rows.Scan(&c.Name, &c.Type); err != nil {
			return "", err
		}
		cols = append(cols, c)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	slices.SortFunc(cols, func(a, b foundColumn) int {
		return strings.Compare(a.Name, b.Name)
	})

	var out strings.Builder
	enc := json.NewEncoder(&out)
	for _, c := range cols {
		if err := enc.Encode(c); err != nil {
			return "", err
		}
	}

	return out.String(), nil
}

func TestValidateAndApply(t *testing.T) {
	cfg := testConfig()

	resourceConfig := tableConfig{
		Table: "target",
	}

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newClickHouseDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := clickHouseGetSchema(t.Context(), db, cfg.Database, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE IF EXISTS %s;", clickHouseDialect(cfg.Database).Identifier(resourceConfig.Table)))
		},
	)
}

func TestValidateAndApplyMigrations(t *testing.T) {
	cfg := testConfig()

	resourceConfig := tableConfig{
		Table: "target",
	}

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	sql.RunValidateAndApplyMigrationsTests(
		t,
		newClickHouseDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := clickHouseGetSchema(t.Context(), db, cfg.Database, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, cols []string, values []string) {
			t.Helper()

			dialect := clickHouseDialect(cfg.Database)
			var keys = make([]string, len(cols))
			for i, col := range cols {
				keys[i] = dialect.Identifier(col)
			}
			keys = append(keys, dialect.Identifier("flow_published_at"))
			values = append(values, "'2024-09-13 01:01:01'")
			keys = append(keys, dialect.Identifier("flow_document"))
			values = append(values, "'{}'")
			keys = append(keys, "`_is_deleted`")
			values = append(values, "0")
			q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", dialect.Identifier(resourceConfig.Table), strings.Join(keys, ","), strings.Join(values, ","))
			_, err := db.ExecContext(t.Context(), q)

			require.NoError(t, err)
		},
		func(t *testing.T) string {
			t.Helper()

			rows, err := sql.DumpTestTable(t, db, clickHouseDialect(cfg.Database).Identifier(resourceConfig.Table))

			require.NoError(t, err)

			return rows
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(t.Context(), fmt.Sprintf("DROP TABLE IF EXISTS %s;", clickHouseDialect(cfg.Database).Identifier(resourceConfig.Table)))
		},
	)
}

func TestPrereqs(t *testing.T) {
	cfg := testConfig()

	tests := []struct {
		name string
		cfg  func(config) config
		want []string
	}{
		{
			name: "valid",
			cfg:  func(cfg config) config { return cfg },
			want: nil,
		},
		{
			name: "wrong username",
			cfg: func(cfg config) config {
				cfg.Credentials.Username = "wrong" + cfg.Credentials.Username
				return cfg
			},
			want: []string{"unable to connect to ClickHouse"},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) config {
				cfg.Credentials.Password = "wrong" + cfg.Credentials.Password
				return cfg
			},
			want: []string{"unable to connect to ClickHouse"},
		},
		{
			name: "wrong address",
			cfg: func(cfg config) config {
				cfg.Address = "localhost:19000"
				return cfg
			},
			want: []string{"unable to connect to ClickHouse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var actual = preReqs(t.Context(), tt.cfg(cfg)).Unwrap()

			require.Equal(t, len(tt.want), len(actual))
			for i := 0; i < len(tt.want); i++ {
				require.ErrorContains(t, actual[i], tt.want[i])
			}
		})
	}
}

var testTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// flowPublishedAtProjection is the standard Estuary metadata timestamp used as
// the ReplacingMergeTree version column.
var flowPublishedAtProjection = sql.Projection{
	Projection: pf.Projection{
		Field: "flow_published_at",
		Inference: pf.Inference{
			Types:   []string{"string"},
			Exists:  pf.Inference_MUST,
			String_: &pf.Inference_String{Format: "date-time"},
		},
	},
}

// metaOpProjection is the operation type metadata field. Its value drives the
// _is_deleted MATERIALIZED column: "d" means deleted, anything else means live.
var metaOpProjection = sql.Projection{
	Projection: pf.Projection{
		Field: "_meta/op",
		Inference: pf.Inference{
			Types:   []string{"string"},
			Exists:  pf.Inference_MUST,
			String_: &pf.Inference_String{},
		},
	},
}

// buildTestTable constructs a resolved sql.Table with one string key ("id"),
// one nullable string value ("value"), _meta/op, flow_published_at, and a document column ("flow_document").
func buildTestTable(t *testing.T, dialect sql.Dialect, tableName string) sql.Table {
	t.Helper()

	var shape = sql.TableShape{
		Path:    sql.TablePath{tableName},
		Binding: 0,
		Keys: []sql.Projection{
			{
				Projection: pf.Projection{
					Field: "id",
					Inference: pf.Inference{
						Types:   []string{"string"},
						Exists:  pf.Inference_MUST,
						String_: &pf.Inference_String{},
					},
				},
			},
		},
		Values: []sql.Projection{
			{
				Projection: pf.Projection{
					Field: "value",
					Inference: pf.Inference{
						Types:   []string{"string", "null"},
						Exists:  pf.Inference_MAY,
						String_: &pf.Inference_String{},
					},
				},
			},
			metaOpProjection,
			flowPublishedAtProjection,
		},
		Document: &sql.Projection{
			Projection: pf.Projection{
				Field: "flow_document",
				Inference: pf.Inference{
					Types:  []string{"object"},
					Exists: pf.Inference_MUST,
				},
			},
		},
	}

	table, err := sql.ResolveTable(shape, dialect)
	require.NoError(t, err)
	return table
}

func TestInstallFence(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var ep = &sql.Endpoint[config]{Config: cfg, Dialect: clickHouseDialect(cfg.Database)}

	c, err := newClient(ctx, "test", ep)
	require.NoError(t, err)
	defer c.Close()

	_, err = c.InstallFence(ctx, sql.Table{}, sql.Fence{})
	require.ErrorContains(t, err, "fencing is not supported")
}

func TestExecStatements(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var ep = &sql.Endpoint[config]{Config: cfg, Dialect: clickHouseDialect(cfg.Database)}

	c, err := newClient(ctx, "test", ep)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.ExecStatements(ctx, []string{"SELECT 1"}))
}

func TestTruncateTable(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var ep = &sql.Endpoint[config]{Config: cfg, Dialect: dialect}
	var tableName = "test_truncate"

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	// Create a table and insert a row.
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id String, flow_published_at DateTime64(6, 'UTC'), _is_deleted UInt8 DEFAULT 0) ENGINE = ReplacingMergeTree(flow_published_at, _is_deleted) ORDER BY (id)",
		dialect.Identifier(tableName),
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	})

	_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, flow_published_at) VALUES ('row1', '2024-01-01 00:00:00')", dialect.Identifier(tableName)))
	require.NoError(t, err)

	c, err := newClient(ctx, "test", ep)
	require.NoError(t, err)
	defer c.Close()

	stmt, applyFn, err := c.TruncateTable(ctx, []string{tableName})
	require.NoError(t, err)
	require.Contains(t, stmt, "TRUNCATE TABLE")
	require.NoError(t, applyFn(ctx))

	// Verify table is empty.
	var count int
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf("SELECT count() FROM %s", dialect.Identifier(tableName))).Scan(&count))
	require.Equal(t, 0, count)
}

func TestOpenNativeConn(t *testing.T) {
	var cfg = testConfig()
	conn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.Ping(t.Context()))
}

func TestDestroy(t *testing.T) {
	var cfg = testConfig()
	var tr = &transactor{}
	var err error
	tr.store.conn, err = clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.load.conn, err = clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)

	tr.Destroy()

	// After Destroy the connections should be closed; Ping should fail.
	require.Error(t, tr.store.conn.Ping(t.Context()))
	require.Error(t, tr.load.conn.Ping(t.Context()))
}

func TestAddBinding(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_add_binding"
	var table = buildTestTable(t, dialect, tableName)

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	createSQL, err := sql.RenderTableTemplate(table, tpls.targetCreateTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	})

	var tr = &transactor{dialect: dialect, templates: tpls, cfg: cfg, _range: &pf.RangeSpec{}}
	loadConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.load.conn = loadConn
	defer tr.load.conn.Close()
	storeConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.store.conn = storeConn
	defer tr.store.conn.Close()
	require.NoError(t, tr.addBinding(ctx, table))

	require.Len(t, tr.bindings, 1)
	require.NotEmpty(t, tr.bindings[0].load.querySQL)
	require.NotEmpty(t, tr.bindings[0].load.insertSQL)
	require.NotEmpty(t, tr.bindings[0].store.insertSQL)
	require.Contains(t, tr.bindings[0].load.querySQL, "flow_document")
	require.Contains(t, tr.bindings[0].store.insertSQL, "INSERT INTO")
}

// storeRows creates the stage table, inserts rows via PrepareBatch, and moves
// all partitions to the target table — the same sequence as Store + commit.
func storeRows(t *testing.T, ctx context.Context, conn chdriver.Conn, b *binding, database string, rows ...[]any) {
	t.Helper()
	require.NoError(t, conn.Exec(ctx, b.store.createTableSQL))

	batch, err := conn.PrepareBatch(ctx, b.store.insertSQL)
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, batch.Append(row...))
	}
	require.NoError(t, batch.Send())

	partRows, err := conn.Query(ctx, b.store.queryPartsSQL, database)
	require.NoError(t, err)
	for partRows.Next() {
		var partitionID string
		require.NoError(t, partRows.Scan(&partitionID))
		require.NoError(t, conn.Exec(ctx, b.store.movePartitionSQL, partitionID))
	}
	require.NoError(t, partRows.Err())
}

// loadDocuments inserts keys into the binding's temp table, runs the load query
// (which JOINs the temp table with the target table), collects documents, then
// truncates the temp table for the next load cycle.
func loadDocuments(t *testing.T, ctx context.Context, loadConn chdriver.Conn, b *binding, keys ...[]any) []string {
	t.Helper()

	require.NoError(t, loadConn.Exec(ctx, b.load.createTableSQL))

	batch, err := loadConn.PrepareBatch(ctx, b.load.insertSQL)
	require.NoError(t, err)
	for _, key := range keys {
		require.NoError(t, batch.Append(key...))
	}
	require.NoError(t, batch.Send())

	rows, err := loadConn.Query(ctx, b.load.querySQL)
	require.NoError(t, err)

	var docs []string
	for rows.Next() {
		var bindingIdx int32
		var doc string
		require.NoError(t, rows.Scan(&bindingIdx, &doc))
		docs = append(docs, doc)
	}
	_ = rows.Close()
	require.NoError(t, rows.Err())

	require.NoError(t, loadConn.Exec(ctx, b.load.dropTableSQL))

	return docs
}

func TestStoreAndLoadDataPath(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_data_path"
	var table = buildTestTable(t, dialect, tableName)

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	// Drop any stale table from a previous run, then create fresh.
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	createSQL, err := sql.RenderTableTemplate(table, tpls.targetCreateTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	})

	// Build the binding to get rendered SQL.
	var tr = &transactor{dialect: dialect, templates: tpls, cfg: cfg, _range: &pf.RangeSpec{}}
	loadConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.load.conn = loadConn
	defer tr.load.conn.Close()

	// Open a native connection for store batch inserts.
	storeConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.store.conn = storeConn
	defer storeConn.Close()
	require.NoError(t, tr.addBinding(ctx, table))
	var b = tr.bindings[0]

	// Store: insert a row via stage table, then move to target.
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{"k1", "v1", "c", testTime, `{"id":"k1"}`})

	// Load: query back the document via temp table JOIN.
	docs := loadDocuments(t, ctx, tr.load.conn, b, []any{"k1"})
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"id":"k1"}`, docs[0])

	// Store a delete row (_meta/op="d") with full record.
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{"k1", nil, "d", testTime.Add(time.Second), `{"id":"k1"}`})

	// Load again: FINAL should exclude the tombstone (_is_deleted is inferred by the MATERIALIZED expression).
	require.Empty(t, loadDocuments(t, ctx, tr.load.conn, b, []any{"k1"}))
}

func TestPrepareNewTransactor(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)

	var ep = &sql.Endpoint[config]{
		Config:  cfg,
		Dialect: dialect,
	}
	// BindingEvents zero value is safe: all methods check the enabled flag first.
	var be = &m.BindingEvents{}
	var open = pm.Request_Open{Range: &pf.RangeSpec{}}

	// No bindings — returns a valid transactor.
	txn, err := newTransactor(ctx, "test", nil, ep, sql.Fence{}, nil, open, nil, be)
	require.NoError(t, err)
	require.NotNil(t, txn)
	txn.Destroy()

	// One binding — transactor has one binding.
	var table = buildTestTable(t, dialect, "test_prepare_txn")
	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	createSQL, err := sql.RenderTableTemplate(table, tpls.targetCreateTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier("test_prepare_txn")))
	})

	txn, err = newTransactor(ctx, "test", nil, ep, sql.Fence{}, []sql.Table{table}, open, nil, be)
	require.NoError(t, err)
	require.NotNil(t, txn)

	var concrete = txn.(*transactor)
	require.Len(t, concrete.bindings, 1)
	txn.Destroy()
}

// TestHardDeleteTombstone verifies that delete rows with _meta/op="d"
// (which infer _is_deleted=1 via the MATERIALIZED expression) are hidden by FINAL.
func TestHardDeleteTombstone(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_hard_delete"

	// Build a table with a mix of nullable and non-nullable value columns.
	var shape = sql.TableShape{
		Path:    sql.TablePath{tableName},
		Binding: 0,
		Keys: []sql.Projection{
			{
				Projection: pf.Projection{
					Field: "id",
					Inference: pf.Inference{
						Types:   []string{"string"},
						Exists:  pf.Inference_MUST,
						String_: &pf.Inference_String{},
					},
				},
			},
		},
		Values: []sql.Projection{
			{
				// Non-nullable string value.
				Projection: pf.Projection{
					Field: "required_str",
					Inference: pf.Inference{
						Types:   []string{"string"},
						Exists:  pf.Inference_MUST,
						String_: &pf.Inference_String{},
					},
				},
			},
			{
				// Nullable string value.
				Projection: pf.Projection{
					Field: "optional_str",
					Inference: pf.Inference{
						Types:   []string{"string", "null"},
						Exists:  pf.Inference_MAY,
						String_: &pf.Inference_String{},
					},
				},
			},
			{
				// Non-nullable bool value.
				Projection: pf.Projection{
					Field: "required_bool",
					Inference: pf.Inference{
						Types:  []string{"boolean"},
						Exists: pf.Inference_MUST,
					},
				},
			},
			{
				// Non-nullable integer value.
				Projection: pf.Projection{
					Field: "required_int",
					Inference: pf.Inference{
						Types:  []string{"integer"},
						Exists: pf.Inference_MUST,
					},
				},
			},
			metaOpProjection,
			flowPublishedAtProjection,
		},
		Document: &sql.Projection{
			Projection: pf.Projection{
				Field: "flow_document",
				Inference: pf.Inference{
					Types:  []string{"object"},
					Exists: pf.Inference_MUST,
				},
			},
		},
	}
	table, err := sql.ResolveTable(shape, dialect)
	require.NoError(t, err)

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	createSQL, err := sql.RenderTableTemplate(table, tpls.targetCreateTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	})

	var tr = &transactor{dialect: dialect, templates: tpls, cfg: cfg, _range: &pf.RangeSpec{}}
	loadConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.load.conn = loadConn
	defer tr.load.conn.Close()
	storeConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.store.conn = storeConn
	defer storeConn.Close()
	require.NoError(t, tr.addBinding(ctx, table))
	var b = tr.bindings[0]

	// Insert a live row.
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{"k1", "hello", "opt", true, int64(42), "c", testTime, `{"id":"k1"}`})

	// Verify it loads.
	require.Len(t, loadDocuments(t, ctx, tr.load.conn, b, []any{"k1"}), 1)

	// Store a delete row with full record values, exactly as Store does.
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{"k1", "hello", "opt", true, int64(42), "d", testTime.Add(time.Second), `{"id":"k1"}`})

	// Load again: tombstone should hide the row.
	require.Empty(t, loadDocuments(t, ctx, tr.load.conn, b, []any{"k1"}))
}

// setupTable drops any stale table, creates it from the template, builds a
// transactor + binding, opens native conns, and registers cleanup. It returns
// the first binding, a store connection, and a load connection (with temp tables).
func setupTable(t *testing.T, ctx context.Context, cfg config, dialect sql.Dialect, tpls templates, table sql.Table, tableName string) (*binding, chdriver.Conn, chdriver.Conn) {
	t.Helper()

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	createSQL, err := sql.RenderTableTemplate(table, tpls.targetCreateTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanDB := clickhouse.OpenDB(cfg.newClickhouseOptions())
		defer cleanDB.Close()
		_, _ = cleanDB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	})

	var tr = &transactor{dialect: dialect, templates: tpls, cfg: cfg, _range: &pf.RangeSpec{}}
	loadConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.load.conn = loadConn
	t.Cleanup(func() { _ = tr.load.conn.Close() })

	storeConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.store.conn = storeConn
	t.Cleanup(func() { _ = storeConn.Close() })

	require.NoError(t, tr.addBinding(ctx, table))
	return tr.bindings[0], storeConn, tr.load.conn
}

// buildCompositeKeyTable constructs a resolved sql.Table with a two-column
// composite key: "tenant" (string, MUST) + "id" (integer, MUST), one nullable
// string value, and the standard document column.
func buildCompositeKeyTable(t *testing.T, dialect sql.Dialect, tableName string) sql.Table {
	t.Helper()

	var shape = sql.TableShape{
		Path:    sql.TablePath{tableName},
		Binding: 0,
		Keys: []sql.Projection{
			{
				Projection: pf.Projection{
					Field: "tenant",
					Inference: pf.Inference{
						Types:   []string{"string"},
						Exists:  pf.Inference_MUST,
						String_: &pf.Inference_String{},
					},
				},
			},
			{
				Projection: pf.Projection{
					Field: "id",
					Inference: pf.Inference{
						Types:  []string{"integer"},
						Exists: pf.Inference_MUST,
					},
				},
			},
		},
		Values: []sql.Projection{
			{
				Projection: pf.Projection{
					Field: "value",
					Inference: pf.Inference{
						Types:   []string{"string", "null"},
						Exists:  pf.Inference_MAY,
						String_: &pf.Inference_String{},
					},
				},
			},
			metaOpProjection,
			flowPublishedAtProjection,
		},
		Document: &sql.Projection{
			Projection: pf.Projection{
				Field: "flow_document",
				Inference: pf.Inference{
					Types:  []string{"object"},
					Exists: pf.Inference_MUST,
				},
			},
		},
	}

	table, err := sql.ResolveTable(shape, dialect)
	require.NoError(t, err)
	return table
}

func TestLoadNonExistentKey(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_load_nonexistent"
	var table = buildTestTable(t, dialect, tableName)

	b, _, loadConn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Query for a key that doesn't exist in the empty table.
	require.Empty(t, loadDocuments(t, ctx, loadConn, b, []any{"nonexistent"}))
}

func TestLoadMultipleKeys(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_load_multi"
	var table = buildTestTable(t, dialect, tableName)

	b, storeConn, loadConn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Store 3 rows.
	storeRows(t, ctx, storeConn, b, cfg.Database,
		[]any{"k1", "v1", "c", testTime, `{"id":"k1"}`},
		[]any{"k2", "v2", "c", testTime, `{"id":"k2"}`},
		[]any{"k3", "v3", "c", testTime, `{"id":"k3"}`},
	)

	// Load all 3 keys.
	docs := loadDocuments(t, ctx, loadConn, b, []any{"k1"}, []any{"k2"}, []any{"k3"})
	require.Len(t, docs, 3)
	sort.Strings(docs)
	require.JSONEq(t, `{"id":"k1"}`, docs[0])
	require.JSONEq(t, `{"id":"k2"}`, docs[1])
	require.JSONEq(t, `{"id":"k3"}`, docs[2])

	// Load subset (k1, k3).
	docs = loadDocuments(t, ctx, loadConn, b, []any{"k1"}, []any{"k3"})
	require.Len(t, docs, 2)
	sort.Strings(docs)
	require.JSONEq(t, `{"id":"k1"}`, docs[0])
	require.JSONEq(t, `{"id":"k3"}`, docs[1])

	// Load mix of existing + non-existing (k1, k99).
	docs = loadDocuments(t, ctx, loadConn, b, []any{"k1"}, []any{"k99"})
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"id":"k1"}`, docs[0])
}

func TestCompositeKeyStoreAndLoad(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_composite_key"
	var table = buildCompositeKeyTable(t, dialect, tableName)

	b, storeConn, loadConn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Store 3 rows with composite keys: (tenant, id).
	storeRows(t, ctx, storeConn, b, cfg.Database,
		[]any{"acme", int64(1), "v1", "c", testTime, `{"tenant":"acme","id":1}`},
		[]any{"acme", int64(2), "v2", "c", testTime, `{"tenant":"acme","id":2}`},
		[]any{"beta", int64(1), "v3", "c", testTime, `{"tenant":"beta","id":1}`},
	)

	// Load (acme,1) → doc1.
	docs := loadDocuments(t, ctx, loadConn, b, []any{"acme", int64(1)})
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"tenant":"acme","id":1}`, docs[0])

	// Load (beta,1) → doc3.
	docs = loadDocuments(t, ctx, loadConn, b, []any{"beta", int64(1)})
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"tenant":"beta","id":1}`, docs[0])

	// Load (acme,99) → empty.
	require.Empty(t, loadDocuments(t, ctx, loadConn, b, []any{"acme", int64(99)}))

	// Multi-key load: (acme,1) + (beta,1) → 2 docs.
	docs = loadDocuments(t, ctx, loadConn, b, []any{"acme", int64(1)}, []any{"beta", int64(1)})
	require.Len(t, docs, 2)
	sort.Strings(docs)
	require.JSONEq(t, `{"tenant":"acme","id":1}`, docs[0])
	require.JSONEq(t, `{"tenant":"beta","id":1}`, docs[1])
}

// TestVersionDeduplication verifies that when multiple inserts for the same key
// have increasing flow_published_at timestamps, querying with FINAL returns the
// row with the highest timestamp. Per the ReplacingMergeTree docs: the row with
// the maximum version value is retained at merge time.
func TestVersionDeduplication(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_version_dedup"
	var table = buildTestTable(t, dialect, tableName)

	b, storeConn, _ := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Simulate 3 successive Store commits for the same key, each in a separate
	// batch with increasing flow_published_at timestamps.
	for seq := 1; seq <= 3; seq++ {
		doc := fmt.Sprintf(`{"id":"k1","seq":%d}`, seq)
		ts := testTime.Add(time.Duration(seq) * time.Second)
		storeRows(t, ctx, storeConn, b, cfg.Database, []any{"k1", fmt.Sprintf("v%d", seq), "c", ts, doc})
	}

	// Query ClickHouse directly with FINAL to verify deduplication picks the
	// most recently inserted row.
	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	var doc string
	err := db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT flow_document FROM %s FINAL WHERE _is_deleted = 0 AND id = 'k1'",
		dialect.Identifier(tableName),
	)).Scan(&doc)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"k1","seq":3}`, doc)
}

func TestStoreBatchMultipleRows(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_store_batch"
	var table = buildTestTable(t, dialect, tableName)

	b, storeConn, loadConn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Store 3 rows in a single batch, then move to target.
	storeRows(t, ctx, storeConn, b, cfg.Database,
		[]any{"k1", "v1", "c", testTime, `{"id":"k1"}`},
		[]any{"k2", "v2", "c", testTime, `{"id":"k2"}`},
		[]any{"k3", "v3", "c", testTime, `{"id":"k3"}`},
	)

	// Load all 3 → all present.
	docs := loadDocuments(t, ctx, loadConn, b, []any{"k1"}, []any{"k2"}, []any{"k3"})
	require.Len(t, docs, 3)
}

func TestMultiBindingStoreAndLoad(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)

	var tableNameA = "test_multi_bind_a"
	var tableNameB = "test_multi_bind_b"
	var tableA = buildTestTable(t, dialect, tableNameA)
	var tableB = buildTestTable(t, dialect, tableNameB)
	tableB.Binding = 1

	// Set up both tables manually since setupTable builds a single-binding transactor.
	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	for _, tn := range []string{tableNameA, tableNameB} {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tn)))
	}
	for _, tbl := range []sql.Table{tableA, tableB} {
		createSQL, err := sql.RenderTableTemplate(tbl, tpls.targetCreateTable)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, createSQL)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		cleanDB := clickhouse.OpenDB(cfg.newClickhouseOptions())
		defer cleanDB.Close()
		for _, tn := range []string{tableNameA, tableNameB} {
			_, _ = cleanDB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tn)))
		}
	})

	// Build transactor with 2 bindings.
	var tr = &transactor{dialect: dialect, templates: tpls, cfg: cfg, _range: &pf.RangeSpec{}}
	loadConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.load.conn = loadConn
	t.Cleanup(func() {
		_ = tr.load.conn.Close()
	})

	storeConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.store.conn = storeConn
	require.NoError(t, tr.addBinding(ctx, tableA))
	require.NoError(t, tr.addBinding(ctx, tableB))
	require.Len(t, tr.bindings, 2)
	defer storeConn.Close()

	var bA = tr.bindings[0]
	var bB = tr.bindings[1]

	// Store a row into table A.
	storeRows(t, ctx, storeConn, bA, cfg.Database, []any{"keyA", "valA", "c", testTime, `{"id":"keyA"}`})

	// Store a row into table B.
	storeRows(t, ctx, storeConn, bB, cfg.Database, []any{"keyB", "valB", "c", testTime, `{"id":"keyB"}`})

	// Load from table A → correct doc.
	docs := loadDocuments(t, ctx, tr.load.conn, bA, []any{"keyA"})
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"id":"keyA"}`, docs[0])

	// Load from table B → correct doc.
	docs = loadDocuments(t, ctx, tr.load.conn, bB, []any{"keyB"})
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"id":"keyB"}`, docs[0])

	// Cross-table: key from table A queried against table B → empty.
	require.Empty(t, loadDocuments(t, ctx, tr.load.conn, bB, []any{"keyA"}))
}

func TestCompositeKeyTombstone(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_composite_tombstone"
	var table = buildCompositeKeyTable(t, dialect, tableName)

	b, storeConn, loadConn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Store a live row (acme, 1).
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{"acme", int64(1), "val", "c", testTime, `{"tenant":"acme","id":1}`})

	// Load → present.
	docs := loadDocuments(t, ctx, loadConn, b, []any{"acme", int64(1)})
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"tenant":"acme","id":1}`, docs[0])

	// Store delete row with _meta/op="d", using full record.
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{"acme", int64(1), "val", "d", testTime.Add(time.Second), `{"tenant":"acme","id":1}`})

	// Load → empty (tombstone hides the row).
	require.Empty(t, loadDocuments(t, ctx, loadConn, b, []any{"acme", int64(1)}))
}
