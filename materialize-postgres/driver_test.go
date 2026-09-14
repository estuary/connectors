package connector

import (
	"context"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func testConfig() config {
	return config{
		Address:  *dbAddress,
		User:     "flow",
		Password: "flow",
		Database: "flow",
		Schema:   "public",
		Advanced: advancedConfig{
			NoFlowDocument: true,
			FeatureFlags:   "allow_existing_tables_for_new_bindings",
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

	// Enum type names embed the full table name, which includes the test-run-specific random
	// suffix. The test framework strips the full rndSuffix via strings.ReplaceAll, but when
	// the table name is truncated to 42 bytes inside the enum type name (hash mode), only a
	// prefix of rndSuffix survives. We strip that truncated prefix here too.
	// Additionally, the 8-hex hash in hash-mode type names is computed from the random table
	// name and therefore changes per run; normalize it to a stable placeholder.
	actionDescSanitizers := []func(string) string{
		func(s string) string {
			// Strip truncated test-suffix components (_<uuid8>_flow_test_<partial_ts>) that
			// remain after the full rndSuffix has already been removed by the framework.
			return regexp.MustCompile(`_[0-9a-f]{8}_flow_test_\d+`).ReplaceAllString(s, "")
		},
		func(s string) string {
			// Normalize the content-addressed hash in hash-mode enum type names.
			return regexp.MustCompile(`_[0-9a-f]{8}_flow_enum`).ReplaceAllString(s, "_<hash>_flow_enum")
		},
	}

	require.NoError(t, exec.Command("docker", "compose", "-f", "docker-compose.yaml", "up", "--wait").Run())
	t.Cleanup(func() {
		exec.Command("docker", "compose", "-f", "docker-compose.yaml", "down", "-v").Run()
	})

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), "testdata/materialize.flow.yaml", makeResourceFn, actionDescSanitizers,
			sql.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal})
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, NewDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, NewDriver(), "testdata/migrate.flow.yaml", makeResourceFn, actionDescSanitizers)
	})

	t.Run("fence", func(t *testing.T) {
		sql.RunFencingTest(
			t,
			NewDriver(),
			"testdata/fence.flow.yaml",
			makeResourceFn,
			testTemplates.createTargetTable,
			func(ctx context.Context, c sql.Client, fence sql.Fence) error {
				var fenceUpdate strings.Builder
				if err := testTemplates.updateFence.Execute(&fenceUpdate, fence); err != nil {
					return fmt.Errorf("evaluating fence template: %w", err)
				}
				// The fence update is parameterized, so it must be executed with
				// bind arguments rather than as a plain statement, mirroring the
				// driver's own fenced-off detection via RowsAffected.
				res, err := c.(*client).db.ExecContext(ctx, fenceUpdate.String(), fenceUpdateArgs(fence)...)
				if err != nil {
					return fmt.Errorf("updating fence: %w", err)
				} else if rows, err := res.RowsAffected(); err != nil {
					return fmt.Errorf("fetching fence update rows: %w", err)
				} else if rows != 1 {
					return fmt.Errorf("this instance was fenced off by another")
				}
				return nil
			},
		)
	})

	t.Run("truncate", testTruncate)
}

// truncateFixtureLiterals gives an insertable SQL literal for each field
// selected by the base.flow.proto testdata fixture's binding, keyed by
// field name; flow_published_at is set per row by the caller.
var truncateFixtureLiterals = map[string]string{
	"key":             "'k1'",
	"optionalBoolean": "true",
	"optionalInteger": "2",
	"optionalString":  "'opt'",
	"requiredBoolean": "true",
	"requiredInteger": "1",
	"requiredString":  "'req'",
	"flow_document":   "'{}'",
}

func loadTruncateFixtureBinding(t *testing.T) *pf.MaterializationSpec_Binding {
	t.Helper()
	raw, err := os.ReadFile("../materialize-boilerplate/testdata/validate_apply_test_cases/generated_specs/base.flow.proto")
	require.NoError(t, err)

	var spec pf.MaterializationSpec
	require.NoError(t, spec.Unmarshal(raw))
	return spec.Bindings[0]
}

func buildTruncateTestTable(t *testing.T, specBinding *pf.MaterializationSpec_Binding, deltaUpdates bool) sql.Table {
	t.Helper()
	var tableName = "truncate_test_" + uuid.NewString()[:8]
	var shape = sql.BuildTableShape("test/truncate", specBinding, 0, []string{"public", tableName}, deltaUpdates)
	table, err := sql.ResolveTable(shape, testDialect)
	require.NoError(t, err)
	return table
}

func createTruncateTestTable(t *testing.T, ctx context.Context, conn *pgx.Conn, table sql.Table) {
	t.Helper()
	ddl, err := sql.RenderTableTemplate(table, testTemplates.createTargetTable)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, ddl)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", table.Identifier))
	})
}

func seedTruncateTestRow(t *testing.T, ctx context.Context, conn *pgx.Conn, table sql.Table, key string, publishedAt time.Time) {
	t.Helper()
	var literals = maps.Clone(truncateFixtureLiterals)
	literals["key"] = "'" + key + "'"
	literals["flow_published_at"] = "'" + publishedAt.Format(time.RFC3339Nano) + "'"

	var cols, vals []string
	for _, col := range table.Columns() {
		lit, ok := literals[col.Field]
		require.True(t, ok, "no seed literal for field %q", col.Field)
		cols = append(cols, col.Identifier)
		vals = append(vals, lit)
	}

	var stmt = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table.Identifier, strings.Join(cols, ", "), strings.Join(vals, ", "))
	_, err := conn.Exec(ctx, stmt)
	require.NoError(t, err)
}

func truncateTestRemainingKeys(t *testing.T, ctx context.Context, conn *pgx.Conn, table sql.Table) []string {
	t.Helper()
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT key FROM %s ORDER BY key", table.Identifier))
	require.NoError(t, err)
	defer rows.Close()

	var out []string
	for rows.Next() {
		var key string
		require.NoError(t, rows.Scan(&key))
		out = append(out, key)
	}
	require.NoError(t, rows.Err())
	return out
}

func testTruncate(t *testing.T) {
	var ctx = context.Background()

	var cfg = testConfig()
	cfg.Address = "localhost:5435" // docker-compose maps the postgres port here
	uri, err := cfg.ToURI(ctx)
	require.NoError(t, err)
	conn, err := pgx.Connect(ctx, uri)
	require.NoError(t, err)
	defer conn.Close(ctx)

	var specBinding = loadTruncateFixtureBinding(t)

	// T carries a nanosecond fraction; flooring to the second must still
	// keep a row published within the same whole second as T.
	var before = time.Date(2026, 9, 14, 12, 0, 0, 500000000, time.UTC)

	for _, deltaUpdates := range []bool{false, true} {
		var name = "standard"
		if deltaUpdates {
			name = "delta-updates"
		}
		t.Run(name, func(t *testing.T) {
			var table = buildTruncateTestTable(t, specBinding, deltaUpdates)
			createTruncateTestTable(t, ctx, conn, table)

			seedTruncateTestRow(t, ctx, conn, table, "k1", before.Add(-2*time.Second))
			seedTruncateTestRow(t, ctx, conn, table, "k2", before.Add(-300*time.Millisecond))
			seedTruncateTestRow(t, ctx, conn, table, "k3", before.Add(1*time.Second))

			var tr = &transactor{dialect: testDialect}
			tr.store.conn = conn
			tr.bindings = []*binding{{target: table}}

			deleted, err := tr.Truncate(ctx, 0, before)
			require.NoError(t, err)
			require.Equal(t, int64(1), deleted)
			require.Equal(t, []string{"k2", "k3"}, truncateTestRemainingKeys(t, ctx, conn, table))

			deleted, err = tr.Truncate(ctx, 0, before)
			require.NoError(t, err)
			require.Equal(t, int64(0), deleted)
		})
	}

	t.Run("missing flow_published_at", func(t *testing.T) {
		var stripped = *specBinding
		var values []string
		for _, f := range stripped.FieldSelection.Values {
			if f != "flow_published_at" {
				values = append(values, f)
			}
		}
		stripped.FieldSelection.Values = values

		var table = buildTruncateTestTable(t, &stripped, false)

		var tr = &transactor{dialect: testDialect}
		tr.store.conn = conn
		tr.bindings = []*binding{{target: table}}

		var hook = logtest.NewGlobal()
		defer hook.Reset()

		deleted, err := tr.Truncate(ctx, 0, before)
		require.NoError(t, err)
		require.Equal(t, int64(0), deleted)

		var entries = hook.AllEntries()
		require.Len(t, entries, 2)
		require.Equal(t, log.WarnLevel, entries[0].Level)
		require.Equal(t, "backfill truncation skipped", entries[0].Message)
		require.Equal(t, log.InfoLevel, entries[1].Level)
		require.Equal(t, "connectorStatus", entries[1].Data["eventType"])
	})
}

func TestPrereqs(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	t.Skip("todo: fix pre-reqs tests")

	cfg := testConfig()

	tests := []struct {
		name string
		cfg  func(config) config
		want []error
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
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong password",
			cfg: func(cfg config) config {
				cfg.Password = "wrong" + cfg.Password
				return cfg
			},
			want: []error{fmt.Errorf("incorrect username or password")},
		},
		{
			name: "wrong database",
			cfg: func(cfg config) config {
				cfg.Database = "wrong" + cfg.Database
				return cfg
			},
			want: []error{fmt.Errorf("database %q does not exist", "wrong"+cfg.Database)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, preReqs(context.Background(), tt.cfg(cfg)).Unwrap())
		})
	}
}
