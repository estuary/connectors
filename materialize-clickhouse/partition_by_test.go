package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)

func TestPartitionExpr(t *testing.T) {
	for _, tt := range []struct {
		name    string
		raw     string
		want    string
		wantErr bool
	}{
		{name: "nil config", raw: "", want: ""},
		{name: "empty object", raw: "{}", want: ""},
		{name: "absent field", raw: `{"table":"t"}`, want: ""},
		{name: "expression", raw: `{"table":"t","partition_by":"toYYYYMM(ts)"}`, want: "toYYYYMM(ts)"},
		{name: "trimmed", raw: `{"table":"t","partition_by":"  toYYYYMM(ts)\t"}`, want: "toYYYYMM(ts)"},
		{name: "invalid json", raw: `{`, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := partitionExpr(json.RawMessage(tt.raw))
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestMustRecreateResourcePartitionBy(t *testing.T) {
	var c = &client{}
	var binding = func(raw string) *pf.MaterializationSpec_Binding {
		return &pf.MaterializationSpec_Binding{ResourceConfigJson: json.RawMessage(raw)}
	}

	for _, tt := range []struct {
		name       string
		last, next *pf.MaterializationSpec_Binding
		want       bool
	}{
		{name: "nil last", last: nil, next: binding(`{"table":"t"}`), want: false},
		{name: "nil next", last: binding(`{"table":"t"}`), next: nil, want: false},
		{name: "both absent", last: binding(`{"table":"t"}`), next: binding(`{"table":"t"}`), want: false},
		{name: "absent to empty", last: binding(`{"table":"t"}`), next: binding(`{"table":"t","partition_by":""}`), want: false},
		{name: "whitespace only difference", last: binding(`{"table":"t","partition_by":"toYYYYMM(ts)"}`), next: binding(`{"table":"t","partition_by":" toYYYYMM(ts) "}`), want: false},
		{name: "absent to expression", last: binding(`{"table":"t"}`), next: binding(`{"table":"t","partition_by":"toYYYYMM(ts)"}`), want: true},
		{name: "changed expression", last: binding(`{"table":"t","partition_by":"toYYYYMM(ts)"}`), next: binding(`{"table":"t","partition_by":"toYYYYMMDD(ts)"}`), want: true},
		{name: "expression removed", last: binding(`{"table":"t","partition_by":"toYYYYMM(ts)"}`), next: binding(`{"table":"t"}`), want: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := c.MustRecreateResource(nil, tt.last, tt.next)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSQLGenerationPartitionBy(t *testing.T) {
	var rawConfig = json.RawMessage(`{"table":"target_table","partition_by":"toYYYYMM(flow_published_at)"}`)

	var snap strings.Builder
	for _, tc := range []struct {
		name       string
		hardDelete bool
		delta      bool
	}{
		{name: "standard hard-delete", hardDelete: true},
		{name: "standard soft-delete", hardDelete: false},
		{name: "delta", hardDelete: true, delta: true},
	} {
		var tpls = renderTemplates(testDialect, tc.hardDelete)
		var table = buildTestTable(t, testDialect, "target_table")
		table.DeltaUpdates = tc.delta
		table.ResourceConfigJson = rawConfig
		if tc.delta {
			table.Document = nil
		}

		for _, tpl := range []string{"createTargetTable", "createStoreTable"} {
			var testcase = tc.name + " " + tpl
			snap.WriteString("--- Begin " + testcase + " ---\n")
			var rendered string
			var err error
			if tpl == "createTargetTable" {
				rendered, err = sql.RenderTableTemplate(table, tpls.createTargetTable)
			} else {
				rendered, err = renderTableAndRangeKey(table, 0, tpls.createStoreTable)
			}
			require.NoError(t, err)
			snap.WriteString(rendered)
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	// A composite expression is emitted verbatim inside the parenthesized
	// PARTITION BY clause.
	var composite = buildTestTable(t, testDialect, "composite_table")
	composite.ResourceConfigJson = json.RawMessage(`{"table":"composite_table","partition_by":"value, toYYYYMM(flow_published_at)"}`)
	rendered, err := sql.RenderTableTemplate(composite, renderTemplates(testDialect, true).createTargetTable)
	require.NoError(t, err)
	snap.WriteString("--- Begin composite expression createTargetTable ---\n")
	snap.WriteString(rendered)
	snap.WriteString("--- End composite expression createTargetTable ---\n\n")

	cupaloy.SnapshotT(t, snap.String())
}

// loadBaseSpec returns the shared boilerplate test spec, whose single binding
// materializes the key/value collection with a full field selection.
func loadBaseSpec(t *testing.T) *pf.MaterializationSpec {
	t.Helper()

	specBytes, err := os.ReadFile("../materialize-boilerplate/testdata/validate_apply_test_cases/generated_specs/base.flow.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))
	return &spec
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return raw
}

// specWithPartitionBy clones the base spec's binding into a spec bound to the
// given table with the given partition_by and backfill counter.
func specWithPartitionBy(t *testing.T, cfg config, table string, partitionBy string, backfill uint32) *pf.MaterializationSpec {
	t.Helper()

	var spec = loadBaseSpec(t)
	spec.ConfigJson = mustMarshal(t, cfg)
	spec.Bindings[0].ResourceConfigJson = mustMarshal(t, tableConfig{Table: table, PartitionBy: partitionBy})
	spec.Bindings[0].ResourcePath = []string{table}
	spec.Bindings[0].Backfill = backfill
	return spec
}

func validatePartitionByReq(t *testing.T, cfg config, table string, partitionBy string, backfill uint32, lastSpec *pf.MaterializationSpec) *pm.Request_Validate {
	t.Helper()

	var spec = loadBaseSpec(t)
	return &pm.Request_Validate{
		Name:                spec.Name,
		ConnectorType:       pf.MaterializationSpec_IMAGE,
		ConfigJson:          mustMarshal(t, cfg),
		LastMaterialization: lastSpec,
		Bindings: []*pm.Request_Validate_Binding{{
			ResourceConfigJson: mustMarshal(t, tableConfig{Table: table, PartitionBy: partitionBy}),
			Collection:         spec.Bindings[0].Collection,
			Backfill:           backfill,
		}},
	}
}

func countDryRunTables(t *testing.T, ctx context.Context, cfg config) int {
	t.Helper()

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()

	var count int
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE 'flow_partition_dryrun_%'",
	).Scan(&count))
	return count
}

func TestValidatePartitionBy(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var tableName = "test_validate_partition_by"

	t.Run("dry-run accepts a valid expression", func(t *testing.T) {
		resp, err := newClickHouseDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, "toYYYYMM(flow_published_at)", 0, nil))
		require.NoError(t, err)
		for _, pc := range resp.Bindings[0].ProjectionConstraints {
			require.NotEqual(t, pm.Response_Validated_Constraint_INCOMPATIBLE, pc.Constraint.Type)
		}
		require.Zero(t, countDryRunTables(t, ctx, cfg), "dry-run scratch tables must be dropped")
	})

	t.Run("dry-run rejects a bad expression", func(t *testing.T) {
		_, err := newClickHouseDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, "toYYYYMM(no_such_column)", 0, nil))
		require.ErrorContains(t, err, "no_such_column")
		require.Zero(t, countDryRunTables(t, ctx, cfg), "dry-run scratch tables must be dropped even on failure")
	})

	t.Run("change without backfill is INCOMPATIBLE", func(t *testing.T) {
		var lastSpec = specWithPartitionBy(t, cfg, tableName, "", 0)
		resp, err := newClickHouseDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, "toYYYYMM(flow_published_at)", 0, lastSpec))
		require.NoError(t, err)

		var incompatible []string
		for _, pc := range resp.Bindings[0].ProjectionConstraints {
			if pc.Constraint.Type == pm.Response_Validated_Constraint_INCOMPATIBLE {
				require.Contains(t, pc.Constraint.Reason, "partition_by")
				incompatible = append(incompatible, pc.Field)
			}
		}
		// The constraint must cover every projection, not just key fields: an
		// INCOMPATIBLE constraint on an unselected field is non-fatal to the
		// control plane, and delta-updates bindings may deselect their key
		// fields entirely. Only a constraint on a *selected* field forces the
		// backfill, so every field the user might have selected needs one.
		require.Contains(t, incompatible, "key")
		require.Contains(t, incompatible, "requiredString")
		require.Contains(t, incompatible, "flow_document")
	})

	t.Run("change with a backfill bump is allowed", func(t *testing.T) {
		var lastSpec = specWithPartitionBy(t, cfg, tableName, "", 0)
		resp, err := newClickHouseDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, "toYYYYMM(flow_published_at)", 1, lastSpec))
		require.NoError(t, err)
		for _, pc := range resp.Bindings[0].ProjectionConstraints {
			require.NotEqual(t, pm.Response_Validated_Constraint_INCOMPATIBLE, pc.Constraint.Type)
		}
	})

	t.Run("unchanged expression is not re-verified", func(t *testing.T) {
		// Schema evolution can remove the projection a long-established
		// table's partition expression references. The existing table keeps
		// its column and keeps working, so Validate must not re-run the
		// dry-run (which would fail on the missing column) for an expression
		// identical to the last applied spec's.
		var lastSpec = specWithPartitionBy(t, cfg, tableName, "toYYYYMM(no_such_column)", 0)
		_, err := newClickHouseDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, "toYYYYMM(no_such_column)", 0, lastSpec))
		require.NoError(t, err)
	})

	t.Run("unchanged expression on a disabled binding needs no backfill", func(t *testing.T) {
		var lastSpec = specWithPartitionBy(t, cfg, tableName, "toYYYYMM(flow_published_at)", 0)
		lastSpec.InactiveBindings = lastSpec.Bindings
		lastSpec.Bindings = nil
		resp, err := newClickHouseDriver().Validate(ctx, validatePartitionByReq(t, cfg, tableName, " toYYYYMM(flow_published_at) ", 0, lastSpec))
		require.NoError(t, err)
		for _, pc := range resp.Bindings[0].ProjectionConstraints {
			require.NotEqual(t, pm.Response_Validated_Constraint_INCOMPATIBLE, pc.Constraint.Type)
		}
	})
}

func TestApplyPartitionByBackfillRecreates(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tableName = "test_apply_partition_by"

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()
	_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(tableName)))
	})

	partitionKey := func() string {
		var pk string
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT partition_key FROM system.tables WHERE database = currentDatabase() AND name = %s", dialect.Literal(tableName),
		)).Scan(&pk))
		return pk
	}

	var specA = specWithPartitionBy(t, cfg, tableName, "toYYYYMM(flow_published_at)", 0)
	_, err := newClickHouseDriver().Apply(ctx, &pm.Request_Apply{Materialization: specA, Version: "test"})
	require.NoError(t, err)
	require.Equal(t, "toYYYYMM(flow_published_at)", partitionKey())

	var specB = specWithPartitionBy(t, cfg, tableName, "toYYYYMMDD(flow_published_at)", 1)
	_, err = newClickHouseDriver().Apply(ctx, &pm.Request_Apply{Materialization: specB, Version: "test", LastMaterialization: specA, LastVersion: "test"})
	require.NoError(t, err)
	require.Equal(t, "toYYYYMMDD(flow_published_at)", partitionKey())
}

// partitionedTestTable is buildTestTable with a partition_by resource config
// attached, so rendered DDL includes the PARTITION BY clause.
func partitionedTestTable(t *testing.T, dialect sql.Dialect, tableName string, partitionBy string) sql.Table {
	t.Helper()

	var table = buildTestTable(t, dialect, tableName)
	table.ResourceConfigJson = mustMarshal(t, tableConfig{Table: tableName, PartitionBy: partitionBy})
	return table
}

func TestPartitionByDataPath(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var tableName = "test_partition_by_data_path"
	var table = partitionedTestTable(t, dialect, tableName, "toYYYYMM(flow_published_at)")

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

	// A single commit spanning three months moves three distinct partitions
	// from the store table to the target.
	storeRows(t, ctx, storeConn, b, cfg.Database,
		[]any{"k1", "v1", "c", testTime, `{"id":"k1"}`},
		[]any{"k2", "v2", "c", testTime.AddDate(0, 1, 0), `{"id":"k2"}`},
		[]any{"k3", "v3", "c", testTime.AddDate(0, 2, 0), `{"id":"k3"}`},
	)

	var rowCount, partCount uint64
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT count(), uniqExact(_partition_id) FROM %s", dialect.Identifier(tableName),
	)).Scan(&rowCount, &partCount))
	require.EqualValues(t, 3, rowCount)
	require.EqualValues(t, 3, partCount)

	var staged uint64
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT count() FROM %s", dialect.Identifier(storeTableName(table, 0)),
	)).Scan(&staged))
	require.Zero(t, staged, "all staged partitions must have moved to the target")

	// The rows remain loadable through the standard load path.
	docs := loadDocuments(t, ctx, tr.load.conn, b, []any{"k2"})
	require.Len(t, docs, 1)
	require.JSONEq(t, `{"id":"k2"}`, docs[0])
}

// TestRecoveryAfterPartitionChangingBackfill covers the window where a
// backfill re-created the target with a different PARTITION BY while a
// commit was still pending: the store table holds staged rows keyed on the
// old expression, so MOVE PARTITION into the new target can never succeed
// (code 36, "Tables have different partition key"). Those rows belong to a
// table generation that no longer exists -- the backfill re-sends
// everything -- so recovery must discard them and re-create the store table
// rather than crash-looping on the move.
func TestRecoveryAfterPartitionChangingBackfill(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var tableName = "test_recovery_partition_backfill"
	var table = partitionedTestTable(t, dialect, tableName, "toYYYYMM(flow_published_at)")

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()
	var storeName = storeTableName(table, 0)
	for _, name := range []string{tableName, storeName} {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(name)))
	}
	t.Cleanup(func() {
		for _, name := range []string{tableName, storeName} {
			_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(name)))
		}
	})

	// The target as the backfill re-created it, with the new partition key.
	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)

	// The store table as the pre-backfill session left it: same columns, old
	// (default, empty) partition key.
	var stale = buildTestTable(t, dialect, storeName)
	staleSQL, err := sql.RenderTableTemplate(stale, tpls.createTargetTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, staleSQL)
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
	defer storeConn.Close()
	loadConn, err := clickhouse.Open(cfg.newClickhouseOptions())
	require.NoError(t, err)
	tr.load.conn = loadConn
	defer loadConn.Close()
	require.NoError(t, tr.addBinding(ctx, table))
	var b = tr.bindings[0]

	// A staged row of the pending pre-backfill commit.
	stageTestRows(t, ctx, tr, b, []any{"k1", "v1", "c", testTime, `{"id":"k1"}`})

	tr.ensured = false
	tr.recovery = true
	tr.state[b.target.StateKey] = &stateItem{StoredRows: 1}
	_, err = tr.Acknowledge(ctx)
	require.NoError(t, err)
	require.Empty(t, tr.state)

	// The stale staged row was discarded, not moved into the new target.
	require.EqualValues(t, 0, countTable(t, ctx, tr, dialect.Identifier(tableName)))

	// The store table was re-created with the target's partition key and
	// accepts a fresh round of staged rows.
	var pk string
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT partition_key FROM system.tables WHERE database = currentDatabase() AND name = %s", dialect.Literal(storeName),
	)).Scan(&pk))
	require.Equal(t, "toYYYYMM(flow_published_at)", pk)
	stageTestRows(t, ctx, tr, b, []any{"k2", "v2", "c", testTime, `{"id":"k2"}`})
	require.EqualValues(t, 1, countTable(t, ctx, tr, dialect.Identifier(storeName)))
}

// TestAlterTableRejectsPartitionKeyColumnMigration verifies that a column
// type migration targeting a column referenced by the table's PARTITION BY
// key fails up front with an actionable error. ClickHouse forbids both
// MODIFY COLUMN (code 524) and DROP COLUMN (code 47) on partition-key
// columns, so the multi-step rename migration can never succeed -- without
// the guard it fails midway with a raw server error and leaves a temporary
// column behind.
func TestAlterTableRejectsPartitionKeyColumnMigration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var tableName = "test_partition_key_migration"
	var table = partitionedTestTable(t, dialect, tableName, "toYYYYMM(flow_published_at)")

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

	c, err := newClient(ctx, "test", &sql.Endpoint[config]{Config: cfg, Dialect: dialect})
	require.NoError(t, err)
	defer c.Close()

	// A DateTime64 -> String migration, exactly as the generic driver would
	// construct it when the collection's schema widens the field.
	var col sql.Column
	for _, cc := range table.Columns() {
		if cc.Field == "flow_published_at" {
			col = *cc
		}
	}
	require.NotEmpty(t, col.Field)
	var strProjection = sql.Projection{
		Projection: pf.Projection{
			Field: "flow_published_at",
			Inference: pf.Inference{
				Types:   []string{"string"},
				Exists:  pf.Inference_MUST,
				String_: &pf.Inference_String{},
			},
		},
	}
	var mapped = dialect.MapType(&strProjection, sql.FieldConfig{})
	var spec = dialect.MigratableTypes.FindMigrationSpec(
		boilerplate.ExistingField{Name: "flow_published_at", Type: "DateTime64(6, 'UTC')"}, mapped)
	require.NotNil(t, spec)
	col.MappedType = mapped

	_, _, err = c.AlterTable(ctx, sql.TableAlter{
		Table: table,
		ColumnTypeChanges: []sql.ColumnTypeMigration{{
			Column:               col,
			MigrationSpec:        *spec,
			OriginalColumnExists: true,
		}},
	})
	require.ErrorContains(t, err, "backfill")
	require.ErrorContains(t, err, "flow_published_at")
}

// TestStoreTablePartitionDrift verifies that the ensure pass re-creates a
// persistent store table whose partition key has drifted from the target's,
// which happens when a backfill re-creates the target with a different
// partition_by. Without re-creation every MOVE PARTITION would fail.
func TestStoreTablePartitionDrift(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	ensureDockerUp(t)

	var cfg = testConfig()
	var ctx = t.Context()
	var dialect = clickHouseDialect(cfg.Database)
	var tpls = renderTemplates(dialect, cfg.HardDelete)
	var tableName = "test_partition_drift"
	var table = partitionedTestTable(t, dialect, tableName, "toYYYYMM(flow_published_at)")

	db := clickhouse.OpenDB(cfg.newClickhouseOptions())
	defer db.Close()
	var storeName = storeTableName(table, 0)
	for _, name := range []string{tableName, storeName} {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(name)))
	}
	t.Cleanup(func() {
		for _, name := range []string{tableName, storeName} {
			_, _ = db.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", dialect.Identifier(name)))
		}
	})

	createSQL, err := sql.RenderTableTemplate(table, tpls.createTargetTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createSQL)
	require.NoError(t, err)

	// Simulate the stale store table left behind by a pre-backfill session:
	// identical columns, but no partition key.
	var unpartitioned = buildTestTable(t, dialect, storeName)
	staleSQL, err := sql.RenderTableTemplate(unpartitioned, tpls.createTargetTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, staleSQL)
	require.NoError(t, err)

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

	require.NoError(t, tr.ensureTempTables(ctx, tr.bindings[0]))

	partitionKey := func(name string) string {
		var pk string
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT partition_key FROM system.tables WHERE database = currentDatabase() AND name = %s", dialect.Literal(name),
		)).Scan(&pk))
		return pk
	}
	require.Equal(t, "toYYYYMM(flow_published_at)", partitionKey(storeName), "drifted store table must be re-created with the target's partition key")

	// A commit through the re-created store table succeeds.
	storeRows(t, ctx, storeConn, tr.bindings[0], cfg.Database,
		[]any{"k1", "v1", "c", testTime, `{"id":"k1"}`},
		[]any{"k2", "v2", "c", testTime.AddDate(0, 1, 0), `{"id":"k2"}`},
	)

	var rowCount uint64
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT count() FROM %s", dialect.Identifier(tableName),
	)).Scan(&rowCount))
	require.EqualValues(t, 2, rowCount)
}
