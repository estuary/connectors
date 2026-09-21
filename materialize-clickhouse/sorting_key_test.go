package connector

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	m "github.com/estuary/connectors/go/materialize"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestParseSortingKey(t *testing.T) {
	for _, tt := range []struct {
		name      string
		key       string
		wantCols  []string
		wantPlain bool
	}{
		{name: "empty", key: "", wantPlain: true},
		{name: "whitespace", key: "  ", wantPlain: true},
		{name: "single", key: "id", wantCols: []string{"id"}, wantPlain: true},
		{name: "several", key: "DateTime, PNodeId, TimeZone", wantCols: []string{"DateTime", "PNodeId", "TimeZone"}, wantPlain: true},
		{name: "quoted with a space", key: "PriceDate, `Contract Code`, Expiry", wantCols: []string{"PriceDate", "Contract Code", "Expiry"}, wantPlain: true},
		{name: "quoted with a comma", key: "`a,b`, c", wantCols: []string{"a,b", "c"}, wantPlain: true},
		{name: "escaped backtick", key: "`a``b`", wantCols: []string{"a`b"}, wantPlain: true},
		{name: "function", key: "toYear(DateTime)", wantCols: []string{}, wantPlain: false},
		{name: "function among columns", key: "id, cityHash64(name)", wantCols: []string{"id"}, wantPlain: false},
		{name: "nested tuple", key: "a, (b, c)", wantCols: []string{"a"}, wantPlain: false},
		{name: "arithmetic", key: "id + 1", wantCols: []string{}, wantPlain: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cols, allPlain := parseSortingKey(tt.key)
			require.Equal(t, tt.wantPlain, allPlain)
			require.Equal(t, tt.wantCols, cols)
		})
	}
}

func TestCheckSortingKey(t *testing.T) {
	const replacing = "ReplacingMergeTree"

	for _, tt := range []struct {
		name     string
		want     []string
		meta     tableKeyMeta
		exists   bool
		wantErr  bool
		wantWarn bool
	}{
		{name: "exact match", want: []string{"id"}, meta: tableKeyMeta{replacing, "id"}, exists: true},
		{name: "reordered", want: []string{"tenant", "id"}, meta: tableKeyMeta{replacing, "id, tenant"}, exists: true},
		{name: "quoted name with a space", want: []string{"Contract Code"}, meta: tableKeyMeta{replacing, "`Contract Code`"}, exists: true},
		{name: "empty sorting key", want: []string{"id"}, meta: tableKeyMeta{replacing, ""}, exists: true, wantErr: true},
		{name: "partial key", want: []string{"tenant", "id"}, meta: tableKeyMeta{replacing, "tenant"}, exists: true, wantErr: true},
		{name: "superset key warns", want: []string{"id"}, meta: tableKeyMeta{replacing, "id, extra"}, exists: true, wantWarn: true},
		{name: "reordered superset warns", want: []string{"id"}, meta: tableKeyMeta{replacing, "extra, id"}, exists: true, wantWarn: true},
		{name: "expression key", want: []string{"DateTime"}, meta: tableKeyMeta{replacing, "toYear(DateTime)"}, exists: true, wantErr: true},
		{name: "expression hiding one of two keys", want: []string{"id", "ts"}, meta: tableKeyMeta{replacing, "id, toDate(ts)"}, exists: true, wantErr: true},
		{name: "expression beside every key warns", want: []string{"id"}, meta: tableKeyMeta{replacing, "id, toDate(ts)"}, exists: true, wantWarn: true},
		{name: "nested tuple beside every key warns", want: []string{"a"}, meta: tableKeyMeta{replacing, "a, (b, c)"}, exists: true, wantWarn: true},
		{name: "cloud replicated engine", want: []string{"id"}, meta: tableKeyMeta{"SharedReplacingMergeTree", ""}, exists: true, wantErr: true},
		{name: "replicated engine", want: []string{"id"}, meta: tableKeyMeta{"ReplicatedReplacingMergeTree", ""}, exists: true, wantErr: true},
		{name: "non-merging engine", want: []string{"id"}, meta: tableKeyMeta{"MergeTree", ""}, exists: true},
		{name: "summing engine", want: []string{"id"}, meta: tableKeyMeta{"SummingMergeTree", ""}, exists: true, wantErr: true},
		{name: "absent table", want: []string{"id"}, meta: tableKeyMeta{}, exists: false},
		{name: "binding without keys", want: nil, meta: tableKeyMeta{replacing, ""}, exists: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			warning, err := checkSortingKey("`t`", tt.want, tt.meta, tt.exists)
			if tt.wantWarn {
				require.NoError(t, err)
				require.Contains(t, warning, "sorting key")
				require.Contains(t, warning, tt.meta.engine)
				return
			}
			require.Empty(t, warning)
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.ErrorContains(t, err, "sorting key")
			require.ErrorContains(t, err, tt.meta.engine)
		})
	}
}

// createRawTable creates tableName with the given column DDL, engine and ORDER
// BY, registering its removal. It stands in for a table the connector did not
// create: one adopted through allow_existing_tables_for_new_bindings, restored
// from a backup, or built by another tool at the same name.
func createRawTable(t *testing.T, ctx context.Context, cfg config, tableName, columns, engine, orderBy string) {
	t.Helper()

	var dialect = clickHouseDialect(cfg.Database)
	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	var names = []string{tableName, storeTableName(sql.Table{TableShape: sql.TableShape{Path: sql.TablePath{tableName}}}, 0)}
	for _, name := range names {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s SYNC", dialect.Identifier(name)))
	}
	t.Cleanup(func() {
		for _, name := range names {
			_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s SYNC", dialect.Identifier(name)))
		}
	})

	_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (%s) ENGINE = %s ORDER BY %s",
		dialect.Identifier(tableName), columns, engine, orderBy))
	require.NoError(t, err)
}

// testTableColumns is the column DDL of buildTestTable, and compositeColumns of
// buildCompositeKeyTable, as the connector itself would render it.
const (
	testTableColumns = "id String, value Nullable(String), `_meta/op` String, " +
		"flow_published_at DateTime64(6, 'UTC'), flow_document String"
	compositeColumns = "tenant String, id Int64, value Nullable(String), `_meta/op` String, " +
		"flow_published_at DateTime64(6, 'UTC'), flow_document String"
	// validateTableColumns matches the key field of the shared base spec used by
	// validatePartitionByReq.
	validateTableColumns = "key String, flow_published_at DateTime64(6, 'UTC'), flow_document String"
)

// TestNewTransactorRejectsSortingKeyMismatch covers the session-start refusal for
// a target table whose sorting key does not cover its binding's key fields. Such
// a table makes ClickHouse collapse rows that are not duplicates, both as they
// are staged and during the target's own background merges, which destroyed
// 300,699 rows down to 9 in production on 2026-08-11. The connector cannot repair
// it -- ClickHouse has no ALTER for a sorting key -- so it must refuse to write.
func TestNewTransactorRejectsSortingKeyMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var ep = &sql.Endpoint[config]{Config: cfg, Dialect: dialect}
	var be = &m.BindingEvents{}
	var open = pm.Request_Open{Range: &pf.RangeSpec{}}

	openTransactor := func(t *testing.T, table sql.Table) error {
		txn, err := newTransactor(ctx, "test", nil, ep, sql.Fence{}, []sql.Table{table}, open, nil, be)
		if txn != nil {
			t.Cleanup(txn.Destroy)
		}
		return err
	}

	t.Run("rejects an empty sorting key", func(t *testing.T) {
		var tableName = "test_sorting_key_empty"
		createRawTable(t, ctx, cfg, tableName, testTableColumns,
			"ReplacingMergeTree(flow_published_at)", "tuple()")

		err := openTransactor(t, buildTestTable(t, dialect, tableName))
		require.Error(t, err)
		require.ErrorContains(t, err, tableName)
		require.ErrorContains(t, err, "sorting key")
	})

	t.Run("rejects a partial sorting key", func(t *testing.T) {
		var tableName = "test_sorting_key_partial"
		createRawTable(t, ctx, cfg, tableName, compositeColumns,
			"ReplacingMergeTree(flow_published_at)", "(tenant)")

		err := openTransactor(t, buildCompositeKeyTable(t, dialect, tableName))
		require.Error(t, err)
		require.ErrorContains(t, err, tableName)
		require.ErrorContains(t, err, "sorting key")
	})

	t.Run("accepts a superset sorting key", func(t *testing.T) {
		// A wider key destroys nothing -- it only leaves superseded rows
		// unreduced. Tables reach this state when a collection is re-keyed and
		// the table keeps a column from its older sorting key, and they work, so
		// refusing to run would break them.
		var tableName = "test_sorting_key_superset"
		createRawTable(t, ctx, cfg, tableName, compositeColumns,
			"ReplacingMergeTree(flow_published_at)", "(tenant, id, `_meta/op`)")

		require.NoError(t, openTransactor(t, buildCompositeKeyTable(t, dialect, tableName)))
	})

	t.Run("rejects an expression sorting key", func(t *testing.T) {
		var tableName = "test_sorting_key_expression"
		createRawTable(t, ctx, cfg, tableName, testTableColumns,
			"ReplacingMergeTree(flow_published_at)", "(cityHash64(id))")

		err := openTransactor(t, buildTestTable(t, dialect, tableName))
		require.Error(t, err)
		require.ErrorContains(t, err, tableName)
	})

	t.Run("accepts a reordered sorting key", func(t *testing.T) {
		// Which rows collapse depends only on the set of key columns, so a
		// different column order is a storage-layout choice, not a correctness
		// problem. Rejecting it would break working tables.
		var tableName = "test_sorting_key_reordered"
		createRawTable(t, ctx, cfg, tableName, compositeColumns,
			"ReplacingMergeTree(flow_published_at)", "(id, tenant)")

		require.NoError(t, openTransactor(t, buildCompositeKeyTable(t, dialect, tableName)))
	})

	t.Run("accepts a table the connector created", func(t *testing.T) {
		var tableName = "test_sorting_key_correct"
		var table = buildTestTable(t, dialect, tableName)

		db := clickhouse.OpenDB(cfg.newClickhouseOptions())
		defer db.Close()
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s SYNC", dialect.Identifier(tableName)))
		createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, createSQL)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s SYNC", dialect.Identifier(tableName)))
		})

		require.NoError(t, openTransactor(t, table))
	})

	t.Run("accepts a table that does not exist yet", func(t *testing.T) {
		// Apply creates it with the correct sorting key.
		var tableName = "test_sorting_key_absent"
		db := clickhouse.OpenDB(cfg.newClickhouseOptions())
		defer db.Close()
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s SYNC", dialect.Identifier(tableName)))

		require.NoError(t, openTransactor(t, buildTestTable(t, dialect, tableName)))
	})

	t.Run("ignores a mismatched key on a non-replacing engine", func(t *testing.T) {
		// Plain MergeTree never merges rows together, so its sorting key cannot
		// destroy anything.
		var tableName = "test_sorting_key_delta"
		createRawTable(t, ctx, cfg, tableName, compositeColumns, "MergeTree", "(tenant)")

		var table = buildCompositeKeyTable(t, dialect, tableName)
		table.DeltaUpdates = true
		require.NoError(t, openTransactor(t, table))
	})

	t.Run("checks a delta binding pointed at a replacing engine", func(t *testing.T) {
		// A delta binding is only safe on an engine that does not merge rows.
		// Adopting a ReplacingMergeTree table makes its sorting key matter again.
		var tableName = "test_sorting_key_delta_replacing"
		createRawTable(t, ctx, cfg, tableName, compositeColumns,
			"ReplacingMergeTree(flow_published_at)", "tuple()")

		var table = buildCompositeKeyTable(t, dialect, tableName)
		table.DeltaUpdates = true
		err := openTransactor(t, table)
		require.Error(t, err)
		require.ErrorContains(t, err, tableName)
	})
}

// TestValidateRejectsSortingKeyMismatch covers the same refusal at Validate, so
// the problem surfaces when the materialization is published rather than after
// it starts running.
func TestValidateRejectsSortingKeyMismatch(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var tableName = "test_validate_sorting_key"

	t.Run("rejects an empty sorting key", func(t *testing.T) {
		createRawTable(t, ctx, cfg, tableName, validateTableColumns,
			"ReplacingMergeTree(flow_published_at)", "tuple()")

		_, err := NewDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, "", 0, nil))
		require.Error(t, err)
		require.ErrorContains(t, err, tableName)
		require.ErrorContains(t, err, "sorting key")
	})

	t.Run("accepts a matching sorting key", func(t *testing.T) {
		createRawTable(t, ctx, cfg, tableName, validateTableColumns,
			"ReplacingMergeTree(flow_published_at)", "(key)")

		_, err := NewDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, "", 0, nil))
		require.NoError(t, err)
	})
}
