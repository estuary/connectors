package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"
)


func mustGetCfg(t *testing.T) config {
	if os.Getenv("TEST_DATABASE") != "yes" {
		t.Skipf("skipping %q: ${TEST_DATABASE} != \"yes\"", t.Name())
		return config{}
	}

	out := config{
		Advanced: advancedConfig{
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}

	for _, prop := range []struct {
		key  string
		dest *string
	}{
		{"FABRIC_WAREHOUSE_CLIENT_ID", &out.ClientID},
		{"FABRIC_WAREHOUSE_CLIENT_SECRET", &out.ClientSecret},
		{"FABRIC_WAREHOUSE_WAREHOUSE", &out.Warehouse},
		{"FABRIC_WAREHOUSE_SCHEMA", &out.Schema},
		{"FABRIC_WAREHOUSE_CONNECTION_STRING", &out.ConnectionString},
		{"FABRIC_WAREHOUSE_STORAGE_ACCOUNT_NAME", &out.StorageAccountName},
		{"FABRIC_WAREHOUSE_STORAGE_ACCOUNT_KEY", &out.StorageAccountKey},
		{"FABRIC_WAREHOUSE_CONTAINER_NAME", &out.ContainerName},
	} {
		*prop.dest = os.Getenv(prop.key)
	}

	require.NoError(t, out.Validate())

	return out
}

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	cfg := mustGetCfg(t)

	resourceConfig := tableConfig{
		Table:     "target",
		Schema:    cfg.Schema,
		Delta:     true,
		warehouse: cfg.Warehouse,
	}

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			db, err := cfg.db()
			require.NoError(t, err)
			defer db.Close()

			sch, err := getSchema(ctx, db, cfg.Warehouse, resourceConfig.Schema, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T) {
			t.Helper()

			db, err := cfg.db()
			require.NoError(t, err)
			defer db.Close()

			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", testDialect.Identifier(cfg.Warehouse, resourceConfig.Schema, resourceConfig.Table)))
		},
	)
}

func TestValidateAndApplyMigrations(t *testing.T) {
	ctx := context.Background()

	cfg := mustGetCfg(t)

	resourceConfig := tableConfig{
		Table:     "target",
		Schema:    cfg.Schema,
		warehouse: cfg.Warehouse,
	}

	db, err := cfg.db()
	require.NoError(t, err)
	defer db.Close()

	sql.RunValidateAndApplyMigrationsTests(
		t,
		newDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := getSchema(ctx, db, cfg.Warehouse, resourceConfig.Schema, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, cols []string, values []string) {
			t.Helper()

			var keys = make([]string, len(cols))
			for i, col := range cols {
				keys[i] = testDialect.Identifier(col)
			}
			for i := range values {
				if values[i] == "true" {
					values[i] = "1"
				} else if values[i] == "false" {
					values[i] = "0"
				}
			}

			keys = append(keys, testDialect.Identifier("_meta/flow_truncated"))
			values = append(values, "0")
			keys = append(keys, testDialect.Identifier("flow_published_at"))
			values = append(values, "'2024-09-13 01:01:01'")
			keys = append(keys, testDialect.Identifier("flow_document"))
			values = append(values, "'{}'")
			q := fmt.Sprintf("insert into %s (%s) VALUES (%s);", testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table), strings.Join(keys, ","), strings.Join(values, ","))
			_, err = db.ExecContext(ctx, q)

			require.NoError(t, err)
		},
		func(t *testing.T) string {
			t.Helper()

			rows, err := sql.DumpTestTable(t, db, testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table))

			require.NoError(t, err)

			return rows
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", testDialect.Identifier(resourceConfig.Schema, resourceConfig.Table)))
		},
	)
}

func TestFencingCases(t *testing.T) {
	var ctx = context.Background()

	var cfg = mustGetCfg(t)

	c, err := newClient(ctx, &sql.Endpoint[config]{Config: cfg})
	require.NoError(t, err)
	defer c.Close()

	sql.RunFenceTestCases(t,
		c,
		[]string{cfg.Warehouse, cfg.Schema, "temp_test_fencing_checkpoints"},
		testDialect,
		testTemplates.createTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var fenceUpdate strings.Builder
			if err := testTemplates.updateFence.Execute(&fenceUpdate, fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}
			return c.ExecStatements(ctx, []string{fenceUpdate.String()})
		},
		func(table sql.Table) (out string, err error) {
			out, err = sql.StdDumpTable(ctx, c.(*client).db, table)
			return strings.Replace(out, "\"checkpoint\"", "checkpoint", 1), err
		},
	)
}

func TestSpecification(t *testing.T) {
	var resp, err = newDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}

func getSchema(ctx context.Context, db *stdsql.DB, warehouse, schema, table string) (string, error) {
	q := fmt.Sprintf(`
	select column_name, is_nullable, data_type
	from INFORMATION_SCHEMA.COLUMNS
	where 
		table_catalog = '%s' 
		and table_schema = '%s'
		and table_name = '%s';
`,
		warehouse,
		schema,
		table,
	)

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	type foundColumn struct {
		Name     string
		Nullable string // string "YES" or "NO"
		Type     string
	}

	cols := []foundColumn{}
	for rows.Next() {
		var c foundColumn
		if err := rows.Scan(&c.Name, &c.Nullable, &c.Type); err != nil {
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
