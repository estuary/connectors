package connector

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

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

	actionDescSanitizers := []func(string) string{
		func(s string) string {
			return regexp.MustCompile(`(sp_rename\s+'[^.]+\.\\?")[a-fA-F0-9\-]{36}(\\?"')`).ReplaceAllString(s, `${1}<uuid>${2}`)
		},
		func(s string) string {
			return regexp.MustCompile(`(CREATE TABLE\s+[^.]+\.\\?")[a-fA-F0-9\-]{36}(\\?")`).ReplaceAllString(s, `${1}<uuid>${2}`)
		},
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), "testdata/materialize.flow.yaml", makeResourceFn, nil,
			sql.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal})
	})

	t.Run("materialize-no-flow-document", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), "testdata/materialize.flow.yaml", makeResourceFn, nil,
			sql.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal,
				ConfigOverrides: map[string]any{"advanced.no_flow_document": true}})
	})

	t.Run("truncate", func(t *testing.T) {
		if testutil.RuntimeV1() {
			t.Skip("backfill signals require runtime-next")
		}
		testutil.RunTestAllTasks(t, "testdata/truncate.flow.yaml", func(t *testing.T, bundled []byte, taskName string, cfg config) {
			var ctx = context.Background()
			var task = sql.PrepareTestTask(t, NewDriver(), bundled, taskName, cfg, makeResourceFn)

			db, err := cfg.db()
			require.NoError(t, err)
			defer db.Close()

			testutil.RunFlowctl(t, "raw", "preview-next",
				"--name", task.Name,
				"--source", task.SourcePath,
				"--fixture", "testdata/truncate.fixture.json",
				"--shards", "1",
				"--timeout", "5m",
				"--network", "flow-test",
			)

			// The fixture stores ids 1-3, then re-stores only id 1 during a
			// backfill. A table with a flow_published_at column loses the rows
			// published before the backfill, unless it's in delta-updates mode:
			// delta-updates both skips truncation and never upserts, so every
			// store appends a row.
			for table, want := range map[string][]int64{
				"truncate_standard":        {1},
				"truncate_delta":           {1, 1, 2, 3},
				"truncate_no_published_at": {1, 2, 3},
			} {
				identifier := testDialect.Identifier(cfg.Warehouse, cfg.Schema, task.Resource(t, table))
				rows, err := db.QueryContext(ctx, "SELECT id FROM "+identifier+" ORDER BY id;")
				require.NoError(t, err)
				var ids []int64
				for rows.Next() {
					var id int64
					require.NoError(t, rows.Scan(&id))
					ids = append(ids, id)
				}
				require.NoError(t, rows.Err())
				require.NoError(t, rows.Close())
				require.Equal(t, want, ids, table)
			}
		})
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, NewDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, NewDriver(), "testdata/migrate.flow.yaml", makeResourceFn, actionDescSanitizers)
	})
	t.Run("key-change-migrate", func(t *testing.T) {
		sql.RunKeyChangeMigrationTest(t, NewDriver(), "testdata/key-change.flow.yaml", makeResourceFn, actionDescSanitizers)
	})

	t.Run("fence", func(t *testing.T) {
		sql.RunFencingTest(
			t,
			NewDriver(),
			"testdata/fence.flow.yaml",
			makeResourceFn,
			testTemplates.createTargetTable,
			func(ctx context.Context, client sql.Client, fence sql.Fence) error {
				var fenceUpdate strings.Builder
				if err := testTemplates.updateFence.Execute(&fenceUpdate, fence); err != nil {
					return fmt.Errorf("evaluating fence template: %w", err)
				}
				return client.ExecStatements(ctx, []string{fenceUpdate.String()})
			},
		)
	})
}
