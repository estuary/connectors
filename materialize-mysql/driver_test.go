//go:build !nodb

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
	"time"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql-v2"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"

	_ "github.com/go-sql-driver/mysql"
)

//go:generate ../materialize-boilerplate/testdata/generate-spec-proto.sh testdata/apply-changes.flow.yaml

func testConfig() config {
	return config{
		Address:  "localhost:3306",
		User:     "flow",
		Password: "flow",
		Database: "flow",
		Timezone: "UTC",
		Advanced: advancedConfig{
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}
}

func testMariaConfig() config {
	return config{
		Address:  "localhost:3305",
		User:     "flow",
		Password: "flow",
		Database: "flow",
		Timezone: "UTC",
		Advanced: advancedConfig{
			FeatureFlags: "allow_existing_tables_for_new_bindings",
		},
	}
}

func TestValidateAndApply(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig()

	resourceConfig := tableConfig{
		Table: "target",
	}

	db, err := stdsql.Open("mysql", cfg.ToURI())
	require.NoError(t, err)
	defer db.Close()

	boilerplate.RunValidateAndApplyTestCases(
		t,
		newMysqlDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := sql.StdGetSchema(ctx, db, "def", cfg.Database, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", testDialect.Identifier(resourceConfig.Table)))
		},
	)
}

func TestValidateAndApplyMigrations(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig()

	resourceConfig := tableConfig{
		Table: "target",
	}

	db, err := stdsql.Open("mysql", cfg.ToURI())
	require.NoError(t, err)
	defer db.Close()

	sql.RunValidateAndApplyMigrationsTests(
		t,
		newMysqlDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := sql.StdGetSchema(ctx, db, "def", cfg.Database, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, cols []string, values []string) {
			t.Helper()

			var keys = make([]string, len(cols))
			for i, col := range cols {
				keys[i] = testDialect.Identifier(col)
			}
			keys = append(keys, testDialect.Identifier("_meta/flow_truncated"))
			values = append(values, "0")
			keys = append(keys, testDialect.Identifier("flow_published_at"))
			values = append(values, "'2024-09-13 01:01:01'")
			keys = append(keys, testDialect.Identifier("flow_document"))
			values = append(values, "'{}'")
			q := fmt.Sprintf("insert into %s (%s) VALUES (%s);", testDialect.Identifier(resourceConfig.Table), strings.Join(keys, ","), strings.Join(values, ","))
			_, err = db.ExecContext(ctx, q)

			require.NoError(t, err)
		},
		func(t *testing.T) string {
			t.Helper()

			rows, err := sql.DumpTestTable(t, db, testDialect.Identifier(resourceConfig.Table))

			require.NoError(t, err)

			return rows
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", testDialect.Identifier(resourceConfig.Table)))
		},
	)
}

func TestValidateAndApplyMigrationsMariaDB(t *testing.T) {
	ctx := context.Background()

	cfg := testMariaConfig()

	resourceConfig := tableConfig{
		Table: "target",
	}

	db, err := stdsql.Open("mysql", cfg.ToURI())
	require.NoError(t, err)
	defer db.Close()

	sql.RunValidateAndApplyMigrationsTests(
		t,
		newMysqlDriver(),
		cfg,
		resourceConfig,
		func(t *testing.T) string {
			t.Helper()

			sch, err := sql.StdGetSchema(ctx, db, "def", cfg.Database, resourceConfig.Table)
			require.NoError(t, err)

			return sch
		},
		func(t *testing.T, cols []string, values []string) {
			t.Helper()

			var keys = make([]string, len(cols))
			for i, col := range cols {
				keys[i] = testDialect.Identifier(col)
			}
			keys = append(keys, testDialect.Identifier("_meta/flow_truncated"))
			values = append(values, "0")
			keys = append(keys, testDialect.Identifier("flow_published_at"))
			values = append(values, "'2024-09-13 01:01:01'")
			keys = append(keys, testDialect.Identifier("flow_document"))
			values = append(values, "'{}'")
			q := fmt.Sprintf("insert into %s (%s) VALUES (%s);", testDialect.Identifier(resourceConfig.Table), strings.Join(keys, ","), strings.Join(values, ","))
			_, err = db.ExecContext(ctx, q)

			require.NoError(t, err)
		},
		func(t *testing.T) string {
			t.Helper()

			rows, err := sql.DumpTestTable(t, db, testDialect.Identifier(resourceConfig.Table))

			require.NoError(t, err)

			return rows
		},
		func(t *testing.T) {
			t.Helper()
			_, _ = db.ExecContext(ctx, fmt.Sprintf("drop table %s;", testDialect.Identifier(resourceConfig.Table)))
		},
	)
}

func TestApplyChanges(t *testing.T) {
	ctx := context.Background()

	cfg := testConfig()
	configJson, err := json.Marshal(cfg)
	require.NoError(t, err)

	resourceConfig := tableConfig{
		Table: "data_types",
	}
	resourceConfigJson, err := json.Marshal(resourceConfig)
	require.NoError(t, err)

	db, err := stdsql.Open("mysql", cfg.ToURI())
	require.NoError(t, err)
	defer db.Close()

	cleanup := func() {
		_, _ = db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s;", testDialect.Identifier(resourceConfig.Table)))
	}
	cleanup()
	defer cleanup()

	// These data types are somewhat interesting as test-cases to make sure we don't lose anything
	// about them when dropping nullability constraints, since we must re-state the column
	// definition when doing that.
	q := fmt.Sprintf(`
	CREATE TABLE %s(
		vc123 VARCHAR(123) NOT NULL,
		vc256 VARCHAR(256) NOT NULL,
		t TEXT NOT NULL,
		lt LONGTEXT NOT NULL,
		n60 NUMERIC(65,0) NOT NULL,
		d DECIMAL NOT NULL,
		n NUMERIC NOT NULL,
		mn NUMERIC(30,5) NOT NULL,
		b123 BINARY(123) NOT NULL,
		vb123 VARBINARY(123) NOT NULL
	);
	`,
		testDialect.Identifier(resourceConfig.Table),
	)

	_, err = db.ExecContext(ctx, q)
	require.NoError(t, err)

	// Initial snapshot of the table, as created.
	original, err := snapshotTable(ctx, db, "def", cfg.Database, resourceConfig.Table)
	require.NoError(t, err)

	// Apply the spec, which will drop the nullability constraints because none of the fields are in
	// the materialized collection.
	specBytes, err := os.ReadFile("testdata/generated_specs/apply-changes.flow.proto")
	require.NoError(t, err)
	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(specBytes))

	spec.ConfigJson = configJson
	spec.Bindings[0].ResourceConfigJson = resourceConfigJson
	spec.Bindings[0].ResourcePath = []string{translateFlowIdentifier(resourceConfig.Table)}

	_, err = newMysqlDriver().Apply(ctx, &pm.Request_Apply{
		Materialization: &spec,
		Version:         "",
	})
	require.NoError(t, err)

	// Snapshot of the table after our apply.
	new, err := snapshotTable(ctx, db, "def", cfg.Database, resourceConfig.Table)
	require.NoError(t, err)

	var snap strings.Builder
	snap.WriteString("Pre-Apply Table Schema:\n")
	snap.WriteString(original)
	snap.WriteString("\nPost-Apply Table Schema:\n")
	snap.WriteString(new)
	cupaloy.SnapshotT(t, snap.String())
}

func TestFencingCases(t *testing.T) {
	var ctx = context.Background()
	var dialect = testDialect
	var templates = renderTemplates(dialect)

	c, err := prepareNewClient(time.UTC)(ctx, &sql.Endpoint[config]{Config: testConfig()})
	require.NoError(t, err)
	defer c.Close()

	sql.RunFenceTestCases(t,
		c,
		[]string{"temp_test_fencing_checkpoints"},
		dialect,
		templates.createTargetTable,
		func(table sql.Table, fence sql.Fence) error {
			var fenceUpdate strings.Builder
			if err := templates.updateFence.Execute(&fenceUpdate, fence); err != nil {
				return fmt.Errorf("evaluating fence template: %w", err)
			}
			return c.ExecStatements(ctx, []string{fenceUpdate.String()})
		},
		func(table sql.Table) (out string, err error) {
			return sql.StdDumpTable(ctx, c.(*client).db, table)
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
				cfg.User = "wrong" + cfg.User
				return cfg
			},
			want: []string{"incorrect username or password (1045): Access denied for user 'wrongflow'"},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) config {
				cfg.Password = "wrong" + cfg.Password
				return cfg
			},
			want: []string{"incorrect username or password (1045): Access denied for user 'flow'"},
		},
		{
			name: "wrong database",
			cfg: func(cfg config) config {
				cfg.Database = "wrong" + cfg.Database
				return cfg
			},
			want: []string{"database \"wrongflow\" cannot be accessed, it might not exist or you do not have permission to access it ("},
		},
		{
			name: "wrong address",
			cfg: func(cfg config) config {
				cfg.Address = cfg.Address + ".wrong"
				return cfg
			},
			want: []string{fmt.Sprintf("host at address %q cannot be found", cfg.Address+".wrong")},
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var actual = preReqs(ctx, tt.cfg(cfg), "").Unwrap()

			require.Equal(t, len(tt.want), len(actual))
			for i := 0; i < len(tt.want); i++ {
				require.ErrorContains(t, actual[i], tt.want[i])
			}
		})
	}
}

func snapshotTable(ctx context.Context, db *stdsql.DB, catalog string, schema string, name string) (string, error) {
	q := fmt.Sprintf(`
	select column_name, is_nullable, data_type, column_type
	from information_schema.columns
	where 
		table_catalog = '%s' 
		and table_schema = '%s'
		and table_name = '%s';
`,
		catalog,
		schema,
		name,
	)

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	type foundColumn struct {
		Name       string
		Nullable   string
		Type       string
		ColumnType string
	}

	cols := []foundColumn{}
	for rows.Next() {
		var c foundColumn
		if err := rows.Scan(&c.Name, &c.Nullable, &c.Type, &c.ColumnType); err != nil {
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
