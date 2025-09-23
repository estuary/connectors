package main

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"

	_ "github.com/go-sql-driver/mysql"
)

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

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	makeResourceFn := func(table string, delta bool) tableConfig {
		return tableConfig{
			Table: table,
			Delta: delta,
		}
	}

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newMysqlDriver(), "testdata/materialize.flow.yaml", makeResourceFn, nil)
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newMysqlDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newMysqlDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})

	t.Run("fence", func(t *testing.T) {
		var templates = renderTemplates(testDialect)
		sql.RunFencingTest(
			t,
			newMysqlDriver(),
			"testdata/fence.flow.yaml",
			makeResourceFn,
			templates.createTargetTable,
			func(ctx context.Context, client sql.Client, fence sql.Fence) error {
				var fenceUpdate strings.Builder
				if err := templates.updateFence.Execute(&fenceUpdate, fence); err != nil {
					return fmt.Errorf("evaluating fence template: %w", err)
				}
				return client.ExecStatements(ctx, []string{fenceUpdate.String()})
			},
		)
	})

	t.Run("apply changes", func(t *testing.T) {
		boilerplate.RunTestAllTasks(t, "testdata/apply-changes.flow.yaml", func(t *testing.T, _ []byte, taskName string, cfg config) {
			t.Run(taskName, func(t *testing.T) {
				ctx := context.Background()

				db, err := stdsql.Open("mysql", cfg.ToURI())
				require.NoError(t, err)
				defer db.Close()

				tableName := "testing"

				cleanup := func() { db.ExecContext(ctx, fmt.Sprintf("DROP TABLE %s;", testDialect.Identifier(tableName))) }
				cleanup()
				t.Cleanup(cleanup)

				// These data types are somewhat interesting as test-cases to make sure
				// we don't lose anything about them when dropping nullability
				// constraints, since we must re-state the column definition when doing
				// that.
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
					testDialect.Identifier(tableName),
				)

				_, err = db.ExecContext(ctx, q)
				require.NoError(t, err)

				// Initial snapshot of the table, as created.
				original, err := snapshotTable(ctx, db, "def", cfg.Database, tableName)
				require.NoError(t, err)

				// Apply the spec, which will drop the nullability constraints because
				// none of the fields are in the materialized collection.
				boilerplate.RunFlowctl(
					t,
					"preview",
					"--name", taskName,
					"--source", "testdata/apply-changes.flow.yaml",
					"--fixture", "testdata/fixture.empty.json",
					"--network", "flow-test",
				)

				// Snapshot of the table after our apply.
				new, err := snapshotTable(ctx, db, "def", cfg.Database, tableName)
				require.NoError(t, err)

				var snap strings.Builder
				snap.WriteString("Pre-Apply Table Schema:\n")
				snap.WriteString(original)
				snap.WriteString("\nPost-Apply Table Schema:\n")
				snap.WriteString(new)
				cupaloy.SnapshotT(t, snap.String())
			})
		})
	})
}

func TestPrereqs(t *testing.T) {
	t.Skip("todo: fix pre-reqs tests")

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
			var actual = preReqs(ctx, tt.cfg(cfg)).Unwrap()

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
