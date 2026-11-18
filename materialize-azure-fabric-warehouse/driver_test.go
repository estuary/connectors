package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	createRunCheckpointsTable(t)

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

// createRunCheckpointsTable creates a checkpoints table for this run alone and
// points the connector at it, both in this process and in those flowctl starts,
// until the test ends. Fabric Warehouse detects write conflicts per table, so
// overlapping runs sharing flow_checkpoints_v1 abort each other's commits on it.
// Creating it up front keeps it out of every test's Apply, so snapshots are
// unchanged.
func createRunCheckpointsTable(t *testing.T) {
	t.Helper()
	var ctx = context.Background()
	var name = fmt.Sprintf("%s_%s_flow_test_%d", sql.DefaultFlowCheckpoints, uuid.NewString()[:8], time.Now().Unix())
	var prev = sql.DefaultFlowCheckpoints
	sql.DefaultFlowCheckpoints = name
	t.Cleanup(func() { sql.DefaultFlowCheckpoints = prev })
	t.Setenv("TEST_FLOW_CHECKPOINTS_TABLE", name)

	raw, err := testutil.ReadConfigFile("testdata/config.yaml")
	require.NoError(t, err)
	var cfg config
	require.NoError(t, json.Unmarshal(raw, &cfg))
	db, err := cfg.db()
	require.NoError(t, err)

	checkpoints, err := sql.ResolveTable(*sql.FlowCheckpointsTable([]string{cfg.Warehouse, cfg.Schema}), testDialect)
	require.NoError(t, err)
	createCheckpoints, err := sql.RenderTableTemplate(checkpoints, testTemplates.createTargetTable)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, createCheckpoints)
	require.NoError(t, err)

	t.Cleanup(func() {
		defer db.Close()
		if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+checkpoints.Identifier); err != nil {
			t.Logf("dropping %s: %s", checkpoints.Identifier, err)
		}
	})
}
