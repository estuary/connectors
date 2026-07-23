package main

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	m "github.com/estuary/connectors/go/materialize"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func testConfig() config {
	return config{
		Address: "localhost:9900",
		Credentials: credentialConfig{
			AuthType:         UserPass,
			usernamePassword: usernamePassword{Username: "flow", Password: "flow"},
		},
		Database: "flow",
		Advanced: advancedConfig{
			SSLMode:      "disable",
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}
}

func TestPrereqs(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

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
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

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
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var ep = &sql.Endpoint[config]{Config: cfg, Dialect: clickHouseDialect(cfg.Database)}

	c, err := newClient(ctx, "test", ep)
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, c.ExecStatements(ctx, []string{"SELECT 1"}))
}

func TestTruncateTable(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

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

// TestAlterTableOrphanedSortingKeyColumn verifies that AlterTable does not make
// a sorting-key or partition-key column nullable, which ClickHouse forbids for
// key columns (code 524). A pre-existing table (adopted via
// allow_existing_tables_for_new_bindings) can have non-nullable key columns that
// are not in the field selection, which the boilerplate orphan sweep marks as
// newly nullable. Such columns must be skipped, while non-key orphaned columns
// must still be widened.
func TestAlterTableOrphanedSortingKeyColumn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var ep = &sql.Endpoint[config]{Config: cfg, Dialect: dialect}
	var tableName = "test_orphaned_sorting_key"
	var table = buildTestTable(t, dialect, tableName)

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))

	// The table reflects an older keying of the data: legacy_key participates
	// in ORDER BY but is no longer part of the collection or field selection,
	// and orphaned_value is a non-key column that is also no longer selected.
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE %s (
			id String,
			legacy_key String,
			orphaned_value String,
			value Nullable(String),
			`+"`_meta/op`"+` String,
			flow_published_at DateTime64(6, 'UTC'),
			flow_document String,
			_is_deleted UInt8 DEFAULT 0
		) ENGINE = ReplacingMergeTree(flow_published_at, _is_deleted)
		ORDER BY (id, legacy_key)`,
		dialect.Identifier(tableName),
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	})

	c, err := newClient(ctx, "test", ep)
	require.NoError(t, err)
	defer c.Close()

	var is = boilerplate.NewInfoSchema(
		sql.ToLocatePathFn(dialect.TableLocator),
		dialect.SchemaLocator,
		dialect.ColumnLocator,
		dialect.CaseInsensitiveColumns,
		dialect.CaseInsensitiveResources,
	)
	require.NoError(t, c.PopulateInfoSchema(ctx, is, [][]string{{tableName}}))
	existing := is.GetResource([]string{tableName})
	require.NotNil(t, existing)

	// Mirror the orphan sweep of materialize-boilerplate's computeBindingUpdate:
	// every existing non-nullable column that isn't in the field selection is
	// marked as newly nullable and arrives at AlterTable as a DropNotNulls entry.
	var inSelection = make(map[string]bool)
	for _, col := range table.Columns() {
		inSelection[col.Field] = true
	}
	var dropNotNulls []boilerplate.ExistingField
	var droppedNames []string
	for _, f := range existing.AllFields() {
		if !inSelection[f.Name] && !f.Nullable {
			dropNotNulls = append(dropNotNulls, f)
			droppedNames = append(droppedNames, f.Name)
		}
	}
	require.ElementsMatch(t, []string{"legacy_key", "orphaned_value"}, droppedNames)

	_, applyFn, err := c.AlterTable(ctx, sql.TableAlter{Table: table, DropNotNulls: dropNotNulls})
	require.NoError(t, err)
	require.NoError(t, applyFn(ctx))

	// The sorting-key column must be left untouched, and the non-key orphaned
	// column must still be widened to Nullable.
	colTypes := make(map[string]string)
	rows, err := db.QueryContext(ctx, fmt.Sprintf(
		"SELECT name, type FROM system.columns WHERE database = %s AND table = %s",
		dialect.Literal(cfg.Database), dialect.Literal(tableName),
	))
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var name, typ string
		require.NoError(t, rows.Scan(&name, &typ))
		colTypes[name] = typ
	}
	require.NoError(t, rows.Err())
	require.Equal(t, "String", colTypes["legacy_key"])
	require.Equal(t, "Nullable(String)", colTypes["orphaned_value"])
}

func TestOpenNativeConn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	conn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	defer conn.Close()

	require.NoError(t, conn.Ping(t.Context()))
}

func TestDestroy(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

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
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var tableName = "test_add_binding"
	var table = buildTestTable(t, dialect, tableName)

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
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

// storeRows ensures the persistent stage table, inserts rows via PrepareBatch,
// and moves all partitions to the target table — the same sequence as
// ensure + Store + commit.
func storeRows(t *testing.T, ctx context.Context, conn chdriver.Conn, b *binding, database string, rows ...[]any) {
	t.Helper()
	require.NoError(t, conn.Exec(ctx, b.store.createTableSQL))
	require.NoError(t, conn.Exec(ctx, b.store.truncateSQL))

	batch, err := conn.PrepareBatch(ctx, b.store.insertSQL)
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, batch.Append(row...))
	}
	require.NoError(t, batch.Send())

	partRows, err := conn.Query(ctx, b.store.queryPartsSQL)
	require.NoError(t, err)
	var totalRows int64
	for partRows.Next() {
		var partitionID string
		var partitionRows uint64
		require.NoError(t, partRows.Scan(&partitionID, &partitionRows))
		totalRows += int64(partitionRows)
		require.NoError(t, conn.Exec(ctx, b.store.movePartitionSQL, partitionID))
	}
	require.NoError(t, partRows.Err())
	require.Equal(t, int64(len(rows)), totalRows, "the consistent read must account for every staged row")
}

// loadDocuments ensures the persistent load table, truncates any keys from a
// previous round, inserts this round's keys, verifies the key count, and runs
// the load query (which JOINs the temp table with the target table).
func loadDocuments(t *testing.T, ctx context.Context, loadConn chdriver.Conn, b *binding, keys ...[]any) []string {
	t.Helper()

	require.NoError(t, loadConn.Exec(ctx, b.load.createTableSQL))
	require.NoError(t, loadConn.Exec(ctx, b.load.truncateSQL))

	batch, err := loadConn.PrepareBatch(ctx, b.load.insertSQL)
	require.NoError(t, err)
	for _, key := range keys {
		require.NoError(t, batch.Append(key...))
	}
	require.NoError(t, batch.Send())

	var counted uint64
	require.NoError(t, loadConn.QueryRow(ctx, b.load.countKeysSQL).Scan(&counted))
	require.EqualValues(t, len(keys), counted, "the consistent read must account for every inserted key")

	rows, err := loadConn.Query(ctx, b.load.querySQL)
	require.NoError(t, err)

	var docs []string
	for rows.Next() {
		var doc string
		require.NoError(t, rows.Scan(&doc))
		docs = append(docs, doc)
	}
	_ = rows.Close()

	return docs
}

func TestStoreAndLoadDataPath(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	cfg.HardDelete = true
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var tableName = "test_data_path"
	var table = buildTestTable(t, dialect, tableName)

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
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
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{"k1", "v1", "c", testTime, `{"id":"k1"}`, uint8(0)})

	// Load: query back the document via temp table JOIN.
	docs := loadDocuments(t, ctx, tr.load.conn, b, []any{"k1"})
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"id":"k1"}`, docs[0])

	// Store a delete row (_meta/op="d") with full record.
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{"k1", nil, "d", testTime.Add(time.Second), `{"id":"k1"}`, uint8(1)})

	// Load again: FINAL should exclude the tombstone.
	require.Empty(t, loadDocuments(t, ctx, tr.load.conn, b, []any{"k1"}))
}

func TestPrepareNewTransactor(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)

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

	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
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

// TestNewTransactorRejectsMissingIsDeletedColumn verifies that a standard-updates
// binding materializing to a pre-existing table that lacks the connector-internal
// _is_deleted column fails at session start with a clear, actionable error rather
// than crash-looping every Store with a cryptic ClickHouse "No such column
// _is_deleted" (code 16). Regression test for issue #4834.
func TestNewTransactorRejectsMissingIsDeletedColumn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	cfg.HardDelete = true
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var ep = &sql.Endpoint[config]{Config: cfg, Dialect: dialect}
	var be = &m.BindingEvents{}
	var open = pm.Request_Open{Range: &pf.RangeSpec{}}

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	t.Run("rejects adopted table without _is_deleted", func(t *testing.T) {
		var tableName = "test_missing_is_deleted"
		var table = buildTestTable(t, dialect, tableName)

		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
		// A table adopted via allow_existing_tables_for_new_bindings that was
		// pre-created WITHOUT the connector's internal _is_deleted column.
		_, err := db.ExecContext(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				id String,
				value Nullable(String),
				`+"`_meta/op`"+` String,
				flow_published_at DateTime64(6, 'UTC'),
				flow_document String
			) ENGINE = ReplacingMergeTree(flow_published_at)
			ORDER BY (id)`,
			dialect.Identifier(tableName),
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
		})

		_, err = newTransactor(ctx, "test", nil, ep, sql.Fence{}, []sql.Table{table}, open, nil, be)
		require.Error(t, err)
		require.ErrorContains(t, err, "_is_deleted")
		require.ErrorContains(t, err, tableName)
	})

	t.Run("accepts table created with _is_deleted", func(t *testing.T) {
		var tableName = "test_present_is_deleted"
		var table = buildTestTable(t, dialect, tableName)

		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
		createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, createSQL)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
		})

		txn, err := newTransactor(ctx, "test", nil, ep, sql.Fence{}, []sql.Table{table}, open, nil, be)
		require.NoError(t, err)
		txn.Destroy()
	})

	t.Run("ignores delta-updates binding without _is_deleted", func(t *testing.T) {
		var tableName = "test_delta_no_is_deleted"
		var table = buildTestTable(t, dialect, tableName)
		table.DeltaUpdates = true

		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
		_, err := db.ExecContext(ctx, fmt.Sprintf(`
			CREATE TABLE %s (
				id String,
				value Nullable(String),
				`+"`_meta/op`"+` String,
				flow_published_at DateTime64(6, 'UTC'),
				flow_document String
			) ENGINE = MergeTree()
			ORDER BY (id)`,
			dialect.Identifier(tableName),
		))
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
		})

		txn, err := newTransactor(ctx, "test", nil, ep, sql.Fence{}, []sql.Table{table}, open, nil, be)
		require.NoError(t, err)
		txn.Destroy()
	})
}

// TestHardDeleteTombstone verifies that delete rows with _meta/op="d"
// (which infer _is_deleted=1 via the MATERIALIZED expression) are hidden by FINAL.
func TestHardDeleteTombstone(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	cfg.HardDelete = true
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
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
						Types:   []string{"integer"},
						Exists:  pf.Inference_MUST,
						Numeric: &pf.Inference_Numeric{},
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
	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
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
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{"k1", "hello", "opt", true, int64(42), "c", testTime, `{"id":"k1"}`, uint8(0)})

	// Verify it loads.
	require.Len(t, loadDocuments(t, ctx, tr.load.conn, b, []any{"k1"}), 1)

	// Store a delete row with full record values, exactly as Store does.
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{"k1", "hello", "opt", true, int64(42), "d", testTime.Add(time.Second), `{"id":"k1"}`, uint8(1)})

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
	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
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
						Types:   []string{"integer"},
						Exists:  pf.Inference_MUST,
						Numeric: &pf.Inference_Numeric{},
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
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var tableName = "test_load_nonexistent"
	var table = buildTestTable(t, dialect, tableName)

	b, _, loadConn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Query for a key that doesn't exist in the empty table.
	require.Empty(t, loadDocuments(t, ctx, loadConn, b, []any{"nonexistent"}))
}

func TestLoadMultipleKeys(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
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
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
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
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
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
		"SELECT flow_document FROM %s FINAL WHERE id = 'k1'",
		dialect.Identifier(tableName),
	)).Scan(&doc)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":"k1","seq":3}`, doc)
}

func TestStoreBatchMultipleRows(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
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
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)

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
		createSQL, err := sql.RenderTableTemplate(tbl, tpls.createTargetTable)
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
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	cfg.HardDelete = true
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var tableName = "test_composite_tombstone"
	var table = buildCompositeKeyTable(t, dialect, tableName)

	b, storeConn, loadConn := setupTable(t, ctx, cfg, dialect, tpls, table, tableName)

	// Store a live row (acme, 1).
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{"acme", int64(1), "val", "c", testTime, `{"tenant":"acme","id":1}`, uint8(0)})

	// Load → present.
	docs := loadDocuments(t, ctx, loadConn, b, []any{"acme", int64(1)})
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"tenant":"acme","id":1}`, docs[0])

	// Store delete row with _meta/op="d", using full record.
	storeRows(t, ctx, storeConn, b, cfg.Database, []any{"acme", int64(1), "val", "d", testTime.Add(time.Second), `{"tenant":"acme","id":1}`, uint8(1)})

	// Load → empty (tombstone hides the row).
	require.Empty(t, loadDocuments(t, ctx, loadConn, b, []any{"acme", int64(1)}))
}

func TestMovePartitionMissingTarget(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var tableName = "test_move_partition_missing_target"
	var table = buildTestTable(t, dialect, tableName)

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	// Create the target table so that CREATE TABLE ... AS can clone its schema
	// for the store stage table.
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)

	var tr = &transactor{dialect: dialect, templates: tpls, cfg: cfg, _range: &pf.RangeSpec{}}
	storeConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.store.conn = storeConn
	defer storeConn.Close()
	require.NoError(t, tr.addBinding(ctx, table))
	var b = tr.bindings[0]

	// Ensure the store stage table is empty and insert a row.
	require.NoError(t, storeConn.Exec(ctx, b.store.createTableSQL))
	require.NoError(t, storeConn.Exec(ctx, b.store.truncateSQL))
	batch, err := storeConn.PrepareBatch(ctx, b.store.insertSQL)
	require.NoError(t, err)
	require.NoError(t, batch.Append("k1", "v1", "c", testTime, `{"id":"k1"}`))
	require.NoError(t, batch.Send())

	// Drop the target table so that MOVE PARTITION has nowhere to go.
	_, err = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	require.NoError(t, err)

	// Attempt to move partitions to the now-missing target table.
	rows, err := storeConn.Query(ctx, b.store.queryPartsSQL)
	require.NoError(t, err)

	var moveErr error
	for rows.Next() {
		var partitionID string
		var partitionRows uint64
		require.NoError(t, rows.Scan(&partitionID, &partitionRows))
		if err = storeConn.Exec(ctx, b.store.movePartitionSQL, partitionID); err != nil {
			moveErr = err
			break
		}
	}
	_ = rows.Close()

	require.Error(t, moveErr)
	var exc *clickhouseproto.Exception
	require.ErrorAs(t, moveErr, &exc)
	require.EqualValues(t, chproto.ErrUnknownTable, exc.Code)
}

// newTestTransactor creates a target table and a transactor with a single
// binding for it, returning both. The target is dropped and re-created; the
// binding's stage table is NOT touched (tests exercise the ensure/recovery
// paths themselves).
func newTestTransactor(t *testing.T, ctx context.Context, tableName string) (*transactor, *binding) {
	t.Helper()

	var cfg = testConfig()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var table = buildTestTable(t, dialect, tableName)

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)

	var tr = &transactor{
		dialect:   dialect,
		templates: tpls,
		cfg:       cfg,
		_range:    &pf.RangeSpec{},
		state:     make(connectorState),
	}
	storeConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.store.conn = storeConn
	t.Cleanup(func() { _ = storeConn.Close() })
	require.NoError(t, tr.addBinding(ctx, table))
	return tr, tr.bindings[0]
}

func stageTestRows(t *testing.T, ctx context.Context, tr *transactor, b *binding, rows ...[]any) {
	t.Helper()
	batch, err := tr.store.conn.PrepareBatch(ctx, b.store.insertSQL)
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, batch.Append(row...))
	}
	require.NoError(t, batch.Send())
}

func countTable(t *testing.T, ctx context.Context, tr *transactor, identifier string) uint64 {
	t.Helper()
	var count uint64
	require.NoError(t, tr.store.conn.QueryRow(ctx,
		fmt.Sprintf("SELECT count() FROM %s SETTINGS select_sequential_consistency = 1", identifier)).Scan(&count))
	return count
}

// TestMoveRefusesOnStoredRowsMismatch exercises the row-accounting guard in
// moveStorePartitionsToTarget: when the authoritative count does not account
// for every row recorded in stateItem.StoredRows, the commit must fail
// without moving partitions, preserving the staged rows for a retry. With a
// matching count the move proceeds and empties the stage table, and recovery
// of an already-committed (empty) stage succeeds without error.
func TestMoveRefusesOnStoredRowsMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var ctx = t.Context()
	var tableName = "test_move_refuses_on_mismatch"
	tr, b := newTestTransactor(t, ctx, tableName)
	stageName := tr.dialect.Identifier(storeTableName(b.target, 0))
	targetName := tr.dialect.Identifier(tableName)

	// Ensure + stage two rows.
	require.NoError(t, tr.ensureTempTables(ctx, b))
	stageTestRows(t, ctx, tr, b,
		[]any{"k1", "v1", "c", testTime, `{"id":"k1"}`},
		[]any{"k2", "v2", "c", testTime, `{"id":"k2"}`})

	si := &stateItem{StoredRows: 3} // deliberately wrong: only 2 rows were staged

	// Mismatch: the commit must fail, naming the counts, ...
	err := tr.moveStorePartitionsToTarget(ctx, b, si, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "refusing to commit store table")
	require.Contains(t, err.Error(), "contains 2 staged rows")
	require.Contains(t, err.Error(), "3 rows were stored")

	// ... the stage table must survive with its rows intact, and nothing may
	// have reached the target table.
	require.EqualValues(t, 2, countTable(t, ctx, tr, stageName))
	require.EqualValues(t, 0, countTable(t, ctx, tr, targetName))

	// With the correct count the commit proceeds: rows land in the target and
	// the stage table is left empty by the moves.
	si.StoredRows = 2
	require.NoError(t, tr.moveStorePartitionsToTarget(ctx, b, si, false))
	require.EqualValues(t, 2, countTable(t, ctx, tr, targetName))
	require.EqualValues(t, 0, countTable(t, ctx, tr, stageName))

	// Recovery of an already-committed (empty) stage is not data loss: it
	// must succeed without error even though StoredRows > 0.
	require.NoError(t, tr.moveStorePartitionsToTarget(ctx, b, si, true))
}

// TestAcknowledgeRecoveryMatrix exercises the first-Acknowledge ensure pass
// across the recovery matrix: pending commits are recovered (fully or
// partially applied), leftover rows of uncommitted transactions are
// truncated, missing temp tables are created, and schema-drifted temp tables
// are re-created.
func TestAcknowledgeRecoveryMatrix(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)
	var ctx = t.Context()

	t.Run("pending state with all rows staged is recovered", func(t *testing.T) {
		tr, b := newTestTransactor(t, ctx, "test_recovery_full")
		require.NoError(t, tr.ensureTempTables(ctx, b))
		stageTestRows(t, ctx, tr, b, []any{"k1", "v1", "c", testTime, `{"id":"k1"}`})

		tr.ensured = false
		tr.recovery = true
		tr.state[b.target.StateKey] = &stateItem{StoredRows: 1}
		_, err := tr.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)
		require.EqualValues(t, 1, countTable(t, ctx, tr, tr.dialect.Identifier("test_recovery_full")))
		require.Empty(t, tr.state)
	})

	t.Run("pending state with partial rows is recovered without the equality guard", func(t *testing.T) {
		tr, b := newTestTransactor(t, ctx, "test_recovery_partial")
		require.NoError(t, tr.ensureTempTables(ctx, b))
		stageTestRows(t, ctx, tr, b, []any{"k1", "v1", "c", testTime, `{"id":"k1"}`})

		// StoredRows says 3, but only 1 row remains staged -- as if a prior
		// process had moved part of the commit before crashing. Recovery must
		// move the remainder rather than failing the equality check forever.
		tr.ensured = false
		tr.recovery = true
		tr.state[b.target.StateKey] = &stateItem{StoredRows: 3}
		_, err := tr.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)
		require.EqualValues(t, 1, countTable(t, ctx, tr, tr.dialect.Identifier("test_recovery_partial")))
	})

	t.Run("pending state with a missing store table recovers as a no-op", func(t *testing.T) {
		tr, b := newTestTransactor(t, ctx, "test_recovery_missing_store")
		// The store table was lost out-of-band (e.g. dropped) while the
		// connector state still records a pending commit referencing it. There
		// are no staged rows left to recover, so Acknowledge must not crash on
		// the missing table -- it must treat it as nothing to recover, clear
		// the pending state, and (re-)create the temp table so the next
		// transaction can stage into it.
		require.NoError(t, tr.store.conn.Exec(ctx, b.store.dropTableSQL))

		tr.ensured = false
		tr.recovery = true
		tr.state[b.target.StateKey] = &stateItem{StoredRows: 1}
		_, err := tr.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)
		require.Empty(t, tr.state)

		// The store table is back, empty, and ready for the next round's store.
		stageName := tr.dialect.Identifier(storeTableName(b.target, 0))
		require.EqualValues(t, 0, countTable(t, ctx, tr, stageName))
		stageTestRows(t, ctx, tr, b, []any{"k1", "v1", "c", testTime, `{"id":"k1"}`})
		require.EqualValues(t, 1, countTable(t, ctx, tr, stageName))
	})

	t.Run("leftover rows of an uncommitted transaction are truncated", func(t *testing.T) {
		tr, b := newTestTransactor(t, ctx, "test_recovery_leftovers")
		require.NoError(t, tr.ensureTempTables(ctx, b))
		stageTestRows(t, ctx, tr, b, []any{"k1", "v1", "c", testTime, `{"id":"k1"}`})

		// No pending state for the binding: the staged row was never
		// committed and must be discarded, not moved.
		tr.ensured = false
		_, err := tr.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)
		require.EqualValues(t, 0, countTable(t, ctx, tr, tr.dialect.Identifier(storeTableName(b.target, 0))))
		require.EqualValues(t, 0, countTable(t, ctx, tr, tr.dialect.Identifier("test_recovery_leftovers")))
	})

	t.Run("missing temp tables are created", func(t *testing.T) {
		tr, b := newTestTransactor(t, ctx, "test_recovery_fresh")
		_ = tr.store.conn.Exec(ctx, b.store.dropTableSQL)

		tr.ensured = false
		_, err := tr.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)
		require.EqualValues(t, 0, countTable(t, ctx, tr, tr.dialect.Identifier(storeTableName(b.target, 0))))
	})

	t.Run("schema-drifted stage table is re-created", func(t *testing.T) {
		tr, b := newTestTransactor(t, ctx, "test_recovery_drift")
		require.NoError(t, tr.ensureTempTables(ctx, b))
		// Simulate a migration that ran in a prior session: the target gained
		// a column the stage table doesn't have.
		stageName := tr.dialect.Identifier(storeTableName(b.target, 0))
		require.NoError(t, tr.store.conn.Exec(ctx,
			fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", stageName, tr.dialect.Identifier("value"))))

		tr.ensured = false
		_, err := tr.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)

		// The stage table must once again match the target (and so accept
		// inserts of the full column set).
		stageTestRows(t, ctx, tr, b, []any{"k1", "v1", "c", testTime, `{"id":"k1"}`})
		require.EqualValues(t, 1, countTable(t, ctx, tr, stageName))
	})

	t.Run("schema drift is reconciled after a trivial pending-commit recovery", func(t *testing.T) {
		// Reproduces issue #4817. A committed-but-unacknowledged transaction
		// left pending state behind, but its rows had already been moved to the
		// target before the prior crash -- so the store table is empty. In the
		// interim an Apply RPC migrated the target, so the store table's
		// structure no longer matches. On the first Acknowledge,
		// moveStorePartitionsToTarget(recovery=true) trivially succeeds against
		// the empty store table; the drift must still be reconciled so the next
		// commit's MOVE PARTITION does not fail with code 122 ("Tables have
		// different structure"), which would otherwise crash-loop recovery
		// forever.
		tr, b := newTestTransactor(t, ctx, "test_recovery_drift_pending")
		require.NoError(t, tr.ensureTempTables(ctx, b))
		stageName := tr.dialect.Identifier(storeTableName(b.target, 0))
		targetName := tr.dialect.Identifier("test_recovery_drift_pending")

		// Migrate the target as the Apply RPC would while the commit was
		// pending; the store table keeps its pre-migration structure.
		require.NoError(t, tr.store.conn.Exec(ctx,
			fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s String", targetName, tr.dialect.Identifier("extra"))))

		// Pending state with rows already moved (StoredRows > 0, empty stage):
		// this is what drives Acknowledge through the trivial recovery move.
		tr.ensured = false
		tr.recovery = true
		tr.state[b.target.StateKey] = &stateItem{StoredRows: 1}
		_, err := tr.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)

		// The next transaction must stage and commit cleanly. On the unfixed
		// connector the store table is still pre-migration and the move fails
		// with code 122.
		stageTestRows(t, ctx, tr, b, []any{"k1", "v1", "c", testTime, `{"id":"k1"}`})
		require.NoError(t, tr.moveStorePartitionsToTarget(ctx, b, &stateItem{StoredRows: 1}, false))
		require.EqualValues(t, 1, countTable(t, ctx, tr, targetName))
		require.EqualValues(t, 0, countTable(t, ctx, tr, stageName))
	})

	t.Run("pre-StoredRows checkpoint state recovers without executing persisted SQL", func(t *testing.T) {
		tr, b := newTestTransactor(t, ctx, "test_recovery_upgrade")
		require.NoError(t, tr.ensureTempTables(ctx, b))
		stageTestRows(t, ctx, tr, b, []any{"k1", "v1", "c", testTime, `{"id":"k1"}`})

		// A checkpoint written by a pre-#4623 connector: it persists rendered
		// SQL (including a single-column parts query) and has no StoredRows.
		// Recovery must ignore the persisted SQL entirely and use freshly
		// rendered statements.
		oldState := fmt.Sprintf(`{%q: {
			"QueryPartsSQL": "\nSELECT DISTINCT partition_id FROM system.parts\nWHERE table = 'obsolete' AND database = ? AND active\nSETTINGS select_sequential_consistency = 1;\n",
			"MovePartitionSQL": "\nALTER TABLE obsolete MOVE PARTITION ID ? TO TABLE also_obsolete;\n",
			"DropTableSQL": "\nDROP TABLE IF EXISTS obsolete;\n"
		}}`, b.target.StateKey)
		require.NoError(t, tr.UnmarshalState([]byte(oldState)))
		require.True(t, tr.recovery)

		tr.ensured = false
		_, err := tr.Acknowledge(ctx, nil, nil)
		require.NoError(t, err)
		require.EqualValues(t, 1, countTable(t, ctx, tr, tr.dialect.Identifier("test_recovery_upgrade")))
	})

	t.Run("load table is truncated between rounds", func(t *testing.T) {
		// Covered end-to-end by loadDocuments-based tests; this asserts the
		// truncate-between-rounds behavior directly.
		tr, b := newTestTransactor(t, ctx, "test_load_truncate_rounds")
		require.NoError(t, tr.store.conn.Exec(ctx, b.load.createTableSQL))
		require.NoError(t, tr.store.conn.Exec(ctx, b.load.truncateSQL))

		batch, err := tr.store.conn.PrepareBatch(ctx, b.load.insertSQL)
		require.NoError(t, err)
		require.NoError(t, batch.Append("k1"))
		require.NoError(t, batch.Send())
		var counted uint64
		require.NoError(t, tr.store.conn.QueryRow(ctx, b.load.countKeysSQL).Scan(&counted))
		require.EqualValues(t, 1, counted)

		// Next round: truncate, insert a different key; the count must not
		// include the previous round's key.
		require.NoError(t, tr.store.conn.Exec(ctx, b.load.truncateSQL))
		batch, err = tr.store.conn.PrepareBatch(ctx, b.load.insertSQL)
		require.NoError(t, err)
		require.NoError(t, batch.Append("k2"))
		require.NoError(t, batch.Send())
		require.NoError(t, tr.store.conn.QueryRow(ctx, b.load.countKeysSQL).Scan(&counted))
		require.EqualValues(t, 1, counted)
	})

	t.Run("subset drain leaves other bindings' staged rows pending", func(t *testing.T) {
		// The Apply RPC invokes Acknowledge with only the state keys of
		// bindings receiving schema updates: pending rows of every other
		// binding must remain staged, and their state entries retained.
		tr, b1 := newTestTransactor(t, ctx, "test_subset_a")
		b1.target.StateKey = "test_subset_a.v1"

		table2 := buildTestTable(t, tr.dialect, "test_subset_b")
		table2.StateKey = "test_subset_b.v1"
		createSQL, err := sql.RenderTableTemplate(table2, tr.templates.createTargetTable)
		require.NoError(t, err)
		require.NoError(t, tr.store.conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tr.dialect.Identifier("test_subset_b"))))
		require.NoError(t, tr.store.conn.Exec(ctx, createSQL))
		require.NoError(t, tr.addBinding(ctx, table2))
		b2 := tr.bindings[1]

		require.NoError(t, tr.ensureTempTables(ctx, b1))
		require.NoError(t, tr.ensureTempTables(ctx, b2))
		stageTestRows(t, ctx, tr, b1, []any{"k1", "v1", "c", testTime, `{"id":"k1"}`})
		stageTestRows(t, ctx, tr, b2, []any{"k2", "v2", "c", testTime, `{"id":"k2"}`})

		tr.ensured = false
		tr.recovery = true
		tr.state[b1.target.StateKey] = &stateItem{StoredRows: 1}
		tr.state[b2.target.StateKey] = &stateItem{StoredRows: 1}

		state, err := tr.Acknowledge(ctx, nil, []string{b1.target.StateKey})
		require.NoError(t, err)
		require.NotNil(t, state)
		require.JSONEq(t, fmt.Sprintf(`{%q: null}`, b1.target.StateKey), string(state.UpdatedJson))
		require.True(t, state.MergePatch)

		// b1's staged row was moved to its target; b2's remains staged with
		// its state entry intact.
		require.EqualValues(t, 1, countTable(t, ctx, tr, tr.dialect.Identifier("test_subset_a")))
		require.EqualValues(t, 0, countTable(t, ctx, tr, tr.dialect.Identifier("test_subset_b")))
		require.EqualValues(t, 1, countTable(t, ctx, tr, tr.dialect.Identifier(storeTableName(b2.target, 0))))
		require.Nil(t, tr.state[b1.target.StateKey])
		require.NotNil(t, tr.state[b2.target.StateKey])
	})
}
