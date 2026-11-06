package connector

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/estuary/connectors/go/common"
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
		return tableConfig{Table: table}
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, sqlDriver, "testdata/materialize.flow.yaml", makeResourceFn, nil,
			sql.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal})
	})

	t.Run("truncate", func(t *testing.T) {
		if testutil.RuntimeV1() {
			t.Skip("backfill signals require runtime-next")
		}
		testutil.RunTestAllTasks(t, "testdata/truncate.flow.yaml", func(t *testing.T, bundled []byte, taskName string, cfg config) {
			var ctx = context.Background()
			var task = sql.PrepareTestTask(t, sqlDriver, bundled, taskName, cfg, makeResourceFn)

			clients, err := newSpannerClients(ctx, cfg)
			require.NoError(t, err)
			defer clients.Close()

			testutil.RunFlowctl(t, "raw", "preview-next",
				"--name", task.Name,
				"--source", task.SourcePath,
				"--fixture", "testdata/truncate.fixture.json",
				"--shards", "1",
				"--timeout", "5m",
				"--network", "flow-test",
			)

			// The fixture stores ids 1-3, then re-stores only id 1 during a
			// backfill. Only the table with a flow_published_at column loses the
			// rows published before the backfill.
			for table, want := range map[string][]int64{
				"truncate_standard":        {1},
				"truncate_no_published_at": {1, 2, 3},
			} {
				iter := clients.dataClient.Single().Query(ctx, spanner.Statement{SQL: "SELECT id FROM " + task.Resource(t, table) + " ORDER BY id"})
				var ids []int64
				require.NoError(t, iter.Do(func(row *spanner.Row) error {
					var id int64
					if err := row.Columns(&id); err != nil {
						return err
					}
					ids = append(ids, id)
					return nil
				}))
				require.Equal(t, want, ids, table)
			}
		})
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, sqlDriver, "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, sqlDriver, "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})

	t.Run("fence", func(t *testing.T) {
		var testDialect = createSpannerDialect(common.ResolveFlagDefaults(featureFlagDefaults, common.CreatedAt{}))
		var testTemplates = renderTemplates(testDialect, false)

		sql.RunFencingTest(
			t,
			sqlDriver,
			"testdata/fence.flow.yaml",
			makeResourceFn,
			testTemplates.createTargetTable,
			func(ctx context.Context, sqlClient sql.Client, fence sql.Fence) error {
				var fenceUpdate strings.Builder
				if err := testTemplates.updateFence.Execute(&fenceUpdate, fence); err != nil {
					return fmt.Errorf("evaluating fence template: %w", err)
				}
				spannerClient := sqlClient.(*client).dataClient
				_, err := spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
					_, err := txn.Update(ctx, spanner.Statement{SQL: fenceUpdate.String()})
					return err
				})
				return err
			},
		)
	})
}
