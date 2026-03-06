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

	db := cfg.openDB()
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

	db := cfg.openDB()
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
			keys = append(keys, "`_version`")
			values = append(values, "1")
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

// buildTestTable constructs a resolved sql.Table with one string key ("id"),
// one nullable string value ("value"), and a document column ("flow_document").
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

	db := cfg.openDB()
	defer db.Close()

	// Create a table and insert a row.
	_, err := db.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (id String, _version UInt64, _is_deleted UInt8 DEFAULT 0) ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY (id)",
		dialect.Identifier(tableName),
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	})

	_, err = db.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s (id, _version) VALUES ('row1', 1)", dialect.Identifier(tableName)))
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
	conn, err := cfg.openNativeConn()
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.Ping(t.Context()))
}

func TestDestroy(t *testing.T) {
	var cfg = testConfig()
	conn, err := cfg.openNativeConn()
	require.NoError(t, err)

	var tr = &transactor{}
	tr.store.conn = conn

	tr.Destroy()

	// After Destroy the connection should be closed; Ping should fail.
	require.Error(t, conn.Ping(t.Context()))
}

func TestAddBinding(t *testing.T) {
	var cfg = testConfig()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var table = buildTestTable(t, dialect, "test_add_binding")

	var tr = &transactor{dialect: dialect, templates: tpls, cfg: cfg}
	require.NoError(t, tr.addBinding(table))

	require.Len(t, tr.bindings, 1)
	require.NotEmpty(t, tr.bindings[0].loadQuerySQL)
	require.NotEmpty(t, tr.bindings[0].storeInsertSQL)
	require.Contains(t, tr.bindings[0].loadQuerySQL, "flow_document")
	require.Contains(t, tr.bindings[0].storeInsertSQL, "INSERT INTO")
}

// scanDocuments collects all flow_document values from a load query result set.
func scanDocuments(t *testing.T, rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}) []string {
	t.Helper()
	var docs []string
	for rows.Next() {
		var doc string
		require.NoError(t, rows.Scan(&doc))
		docs = append(docs, doc)
	}
	_ = rows.Close()
	require.NoError(t, rows.Err())
	return docs
}

func TestStoreAndLoadDataPath(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_data_path"
	var table = buildTestTable(t, dialect, tableName)

	db := cfg.openDB()
	defer db.Close()

	// Drop any stale table from a previous run, then create fresh.
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	})

	// Build the binding to get rendered SQL.
	var tr = &transactor{dialect: dialect, templates: tpls, cfg: cfg}
	require.NoError(t, tr.addBinding(table))
	var b = tr.bindings[0]

	// Open a native connection for batch insert + query.
	conn, err := cfg.openNativeConn()
	require.NoError(t, err)
	defer conn.Close()

	// Store: insert a row via native batch.
	batch, err := conn.PrepareBatch(ctx, b.storeInsertSQL)
	require.NoError(t, err)
	require.NoError(t, batch.Append("k1", "v1", `{"id":"k1"}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Send())

	// Load: query back the document via GroupSet IN parameter.
	var groupSets = []clickhouse.GroupSet{{Value: []any{"k1"}}}
	rows, err := conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)

	docs := scanDocuments(t, rows)
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"id":"k1"}`, docs[0])

	// Store a tombstone row (higher version, _is_deleted=1).
	// Use tombstoneValue for non-nullable columns: "value" is nullable (nil),
	// "flow_document" is non-nullable (empty string).
	batch, err = conn.PrepareBatch(ctx, b.storeInsertSQL)
	require.NoError(t, err)
	require.NoError(t, batch.Append(
		"k1",
		tombstoneValue(table.Values[0]),
		tombstoneValue(*table.Document),
		uint64(2), uint8(1),
	))
	require.NoError(t, batch.Send())

	// Load again: FINAL + _is_deleted=0 filter should exclude the tombstone.
	rows, err = conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	require.Empty(t, scanDocuments(t, rows))
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
	var factory = prepareNewTransactor(tpls)

	// No bindings — returns a valid transactor.
	txn, err := factory(ctx, "test", nil, ep, sql.Fence{}, nil, pm.Request_Open{}, nil, be)
	require.NoError(t, err)
	require.NotNil(t, txn)
	txn.Destroy()

	// One binding — transactor has one binding.
	var table = buildTestTable(t, dialect, "test_prepare_txn")
	db := cfg.openDB()
	defer db.Close()

	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier("test_prepare_txn")))
	})

	txn, err = factory(ctx, "test", nil, ep, sql.Fence{}, []sql.Table{table}, pm.Request_Open{}, nil, be)
	require.NoError(t, err)
	require.NotNil(t, txn)

	var concrete = txn.(*transactor)
	require.Len(t, concrete.bindings, 1)
	txn.Destroy()
}

// TestHardDeleteTombstone verifies that tombstone rows with typed zero values
// (from tombstoneValue) can be batch-inserted into a table with non-nullable
// columns and are correctly hidden by FINAL + _is_deleted=0.
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

	db := cfg.openDB()
	defer db.Close()

	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	})

	var tr = &transactor{dialect: dialect, templates: tpls, cfg: cfg}
	require.NoError(t, tr.addBinding(table))
	var b = tr.bindings[0]

	conn, err := cfg.openNativeConn()
	require.NoError(t, err)
	defer conn.Close()

	// Insert a live row.
	batch, err := conn.PrepareBatch(ctx, b.storeInsertSQL)
	require.NoError(t, err)
	require.NoError(t, batch.Append("k1", "hello", "opt", true, int64(42), `{"id":"k1"}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Send())

	// Verify it loads.
	var groupSets = []clickhouse.GroupSet{{Value: []any{"k1"}}}
	rows, err := conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	require.Len(t, scanDocuments(t, rows), 1)

	// Build a tombstone row using tombstoneValue for each value + document column,
	// exactly as Store does.
	var converted []any
	converted = append(converted, "k1") // key
	for i := range table.Values {
		converted = append(converted, tombstoneValue(table.Values[i]))
	}
	converted = append(converted, tombstoneValue(*table.Document))
	converted = append(converted, uint64(2), uint8(1))

	batch, err = conn.PrepareBatch(ctx, b.storeInsertSQL)
	require.NoError(t, err)
	require.NoError(t, batch.Append(converted...))
	require.NoError(t, batch.Send())

	// Load again: tombstone should hide the row.
	rows, err = conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	require.Empty(t, scanDocuments(t, rows))
}

// setupTable drops any stale table, creates it from the template, builds a
// transactor + binding, opens a native conn, and registers cleanup. It returns
// the first binding and the native connection.
func setupTable(t *testing.T, ctx context.Context, cfg config, dialect sql.Dialect, tpls templates, table sql.Table, tableName string) (*binding, chdriver.Conn) {
	t.Helper()

	db := cfg.openDB()
	defer db.Close()

	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanDB := cfg.openDB()
		defer cleanDB.Close()
		_, _ = cleanDB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	})

	var tr = &transactor{dialect: dialect, templates: tpls, cfg: cfg}
	require.NoError(t, tr.addBinding(table))

	conn, err := cfg.openNativeConn()
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return tr.bindings[0], conn
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

	b, conn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Query for a key that doesn't exist in the empty table.
	var groupSets = []clickhouse.GroupSet{{Value: []any{"nonexistent"}}}
	rows, err := conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	require.Empty(t, scanDocuments(t, rows))
}

func TestLoadMultipleKeys(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_load_multi"
	var table = buildTestTable(t, dialect, tableName)

	b, conn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Store 3 rows.
	batch, err := conn.PrepareBatch(ctx, b.storeInsertSQL)
	require.NoError(t, err)
	require.NoError(t, batch.Append("k1", "v1", `{"id":"k1"}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Append("k2", "v2", `{"id":"k2"}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Append("k3", "v3", `{"id":"k3"}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Send())

	// Load all 3 keys.
	groupSets := []clickhouse.GroupSet{
		{Value: []any{"k1"}},
		{Value: []any{"k2"}},
		{Value: []any{"k3"}},
	}
	rows, err := conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	docs := scanDocuments(t, rows)
	require.Len(t, docs, 3)
	sort.Strings(docs)
	require.JSONEq(t, `{"id":"k1"}`, docs[0])
	require.JSONEq(t, `{"id":"k2"}`, docs[1])
	require.JSONEq(t, `{"id":"k3"}`, docs[2])

	// Load subset (k1, k3).
	groupSets = []clickhouse.GroupSet{
		{Value: []any{"k1"}},
		{Value: []any{"k3"}},
	}
	rows, err = conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	docs = scanDocuments(t, rows)
	require.Len(t, docs, 2)
	sort.Strings(docs)
	require.JSONEq(t, `{"id":"k1"}`, docs[0])
	require.JSONEq(t, `{"id":"k3"}`, docs[1])

	// Load mix of existing + non-existing (k1, k99).
	groupSets = []clickhouse.GroupSet{
		{Value: []any{"k1"}},
		{Value: []any{"k99"}},
	}
	rows, err = conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	docs = scanDocuments(t, rows)
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

	b, conn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Store 3 rows with composite keys: (tenant, id).
	batch, err := conn.PrepareBatch(ctx, b.storeInsertSQL)
	require.NoError(t, err)
	require.NoError(t, batch.Append("acme", int64(1), "v1", `{"tenant":"acme","id":1}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Append("acme", int64(2), "v2", `{"tenant":"acme","id":2}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Append("beta", int64(1), "v3", `{"tenant":"beta","id":1}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Send())

	// Load (acme,1) → doc1.
	groupSets := []clickhouse.GroupSet{{Value: []any{"acme", int64(1)}}}
	rows, err := conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	docs := scanDocuments(t, rows)
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"tenant":"acme","id":1}`, docs[0])

	// Load (beta,1) → doc3.
	groupSets = []clickhouse.GroupSet{{Value: []any{"beta", int64(1)}}}
	rows, err = conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	docs = scanDocuments(t, rows)
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"tenant":"beta","id":1}`, docs[0])

	// Load (acme,99) → empty.
	groupSets = []clickhouse.GroupSet{{Value: []any{"acme", int64(99)}}}
	rows, err = conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	require.Empty(t, scanDocuments(t, rows))

	// Multi-key load: (acme,1) + (beta,1) → 2 docs.
	groupSets = []clickhouse.GroupSet{
		{Value: []any{"acme", int64(1)}},
		{Value: []any{"beta", int64(1)}},
	}
	rows, err = conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	docs = scanDocuments(t, rows)
	require.Len(t, docs, 2)
	sort.Strings(docs)
	require.JSONEq(t, `{"tenant":"acme","id":1}`, docs[0])
	require.JSONEq(t, `{"tenant":"beta","id":1}`, docs[1])
}

func TestVersionDeduplication(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_version_dedup"
	var table = buildTestTable(t, dialect, tableName)

	b, conn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Store the same key at versions 1, 2, 3 with distinct documents.
	for v := uint64(1); v <= 3; v++ {
		batch, err := conn.PrepareBatch(ctx, b.storeInsertSQL)
		require.NoError(t, err)
		doc := fmt.Sprintf(`{"id":"k1","version":%d}`, v)
		require.NoError(t, batch.Append("k1", fmt.Sprintf("v%d", v), doc, v, uint8(0)))
		require.NoError(t, batch.Send())
	}

	// Load → exactly 1 doc, matching the highest version.
	var groupSets = []clickhouse.GroupSet{{Value: []any{"k1"}}}
	rows, err := conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	docs := scanDocuments(t, rows)
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"id":"k1","version":3}`, docs[0])
}

func TestStoreBatchMultipleRows(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_store_batch"
	var table = buildTestTable(t, dialect, tableName)

	b, conn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Single PrepareBatch, append 3 rows, then Send.
	batch, err := conn.PrepareBatch(ctx, b.storeInsertSQL)
	require.NoError(t, err)
	require.NoError(t, batch.Append("k1", "v1", `{"id":"k1"}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Append("k2", "v2", `{"id":"k2"}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Append("k3", "v3", `{"id":"k3"}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Send())

	// Load all 3 → all present.
	groupSets := []clickhouse.GroupSet{
		{Value: []any{"k1"}},
		{Value: []any{"k2"}},
		{Value: []any{"k3"}},
	}
	rows, err := conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	docs := scanDocuments(t, rows)
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

	// Set up both tables manually since setupTable builds a single-binding transactor.
	db := cfg.openDB()
	defer db.Close()

	for _, tn := range []string{tableNameA, tableNameB} {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tn)))
	}
	for _, tbl := range []sql.Table{tableA, tableB} {
		createSQL, err := sql.RenderTableTemplate(tbl, tpls.createTargetTable)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, createSQL)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		cleanDB := cfg.openDB()
		defer cleanDB.Close()
		for _, tn := range []string{tableNameA, tableNameB} {
			_, _ = cleanDB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tn)))
		}
	})

	// Build transactor with 2 bindings.
	var tr = &transactor{dialect: dialect, templates: tpls, cfg: cfg}
	require.NoError(t, tr.addBinding(tableA))
	require.NoError(t, tr.addBinding(tableB))
	require.Len(t, tr.bindings, 2)

	conn, err := cfg.openNativeConn()
	require.NoError(t, err)
	defer conn.Close()

	var bA = tr.bindings[0]
	var bB = tr.bindings[1]

	// Store a row into table A.
	batch, err := conn.PrepareBatch(ctx, bA.storeInsertSQL)
	require.NoError(t, err)
	require.NoError(t, batch.Append("keyA", "valA", `{"id":"keyA"}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Send())

	// Store a row into table B.
	batch, err = conn.PrepareBatch(ctx, bB.storeInsertSQL)
	require.NoError(t, err)
	require.NoError(t, batch.Append("keyB", "valB", `{"id":"keyB"}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Send())

	// Load from table A → correct doc.
	groupSets := []clickhouse.GroupSet{{Value: []any{"keyA"}}}
	rows, err := conn.Query(ctx, bA.loadQuerySQL, groupSets)
	require.NoError(t, err)
	docs := scanDocuments(t, rows)
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"id":"keyA"}`, docs[0])

	// Load from table B → correct doc.
	groupSets = []clickhouse.GroupSet{{Value: []any{"keyB"}}}
	rows, err = conn.Query(ctx, bB.loadQuerySQL, groupSets)
	require.NoError(t, err)
	docs = scanDocuments(t, rows)
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"id":"keyB"}`, docs[0])

	// Cross-table: key from table A queried against table B → empty.
	groupSets = []clickhouse.GroupSet{{Value: []any{"keyA"}}}
	rows, err = conn.Query(ctx, bB.loadQuerySQL, groupSets)
	require.NoError(t, err)
	require.Empty(t, scanDocuments(t, rows))
}

func TestCompositeKeyTombstone(t *testing.T) {
	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect)
	var tableName = "test_composite_tombstone"
	var table = buildCompositeKeyTable(t, dialect, tableName)

	b, conn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Store a live row (acme, 1).
	batch, err := conn.PrepareBatch(ctx, b.storeInsertSQL)
	require.NoError(t, err)
	require.NoError(t, batch.Append("acme", int64(1), "val", `{"tenant":"acme","id":1}`, uint64(1), uint8(0)))
	require.NoError(t, batch.Send())

	// Load → present.
	groupSets := []clickhouse.GroupSet{{Value: []any{"acme", int64(1)}}}
	rows, err := conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	docs := scanDocuments(t, rows)
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"tenant":"acme","id":1}`, docs[0])

	// Store tombstone at higher version with _is_deleted=1.
	var converted []any
	converted = append(converted, "acme", int64(1)) // keys
	for i := range table.Values {
		converted = append(converted, tombstoneValue(table.Values[i]))
	}
	converted = append(converted, tombstoneValue(*table.Document))
	converted = append(converted, uint64(2), uint8(1))

	batch, err = conn.PrepareBatch(ctx, b.storeInsertSQL)
	require.NoError(t, err)
	require.NoError(t, batch.Append(converted...))
	require.NoError(t, batch.Send())

	// Load → empty (tombstone hides the row).
	rows, err = conn.Query(ctx, b.loadQuerySQL, groupSets)
	require.NoError(t, err)
	require.Empty(t, scanDocuments(t, rows))
}
