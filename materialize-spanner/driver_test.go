package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"cloud.google.com/go/spanner"
	databasepb "cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/estuary/connectors/go/common"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"gopkg.in/yaml.v3"
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
		var ctx = context.Background()

		rawCfg, err := os.ReadFile("testdata/config.local.yaml")
		require.NoError(t, err)
		var cfgMap map[string]any
		require.NoError(t, yaml.Unmarshal(rawCfg, &cfgMap))
		cfgJSON, err := json.Marshal(cfgMap)
		require.NoError(t, err)
		var cfg config
		require.NoError(t, json.Unmarshal(cfgJSON, &cfg))

		clients, err := newSpannerClients(ctx, cfg)
		require.NoError(t, err)
		defer clients.Close()

		ddlCtx, cancel := context.WithTimeout(ctx, ddlTimeout)
		defer cancel()
		op, err := clients.adminClient.UpdateDatabaseDdl(ddlCtx, &databasepb.UpdateDatabaseDdlRequest{
			Database: clients.dbPath,
			Statements: []string{
				"DROP TABLE IF EXISTS truncate_standard",
				"DROP TABLE IF EXISTS truncate_no_published_at",
			},
		})
		require.NoError(t, err)
		require.NoError(t, op.Wait(ddlCtx))

		checkpointsIter := clients.dataClient.Single().Query(ctx, spanner.Statement{
			SQL:    "SELECT table_name FROM information_schema.tables WHERE table_name = @table",
			Params: map[string]interface{}{"table": "flow_checkpoints_v1"},
		})
		_, err = checkpointsIter.Next()
		checkpointsIter.Stop()
		if err == nil {
			_, err = clients.dataClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				_, err := txn.Update(ctx, spanner.Statement{
					SQL:    "DELETE FROM flow_checkpoints_v1 WHERE materialization = @materialization",
					Params: map[string]interface{}{"materialization": "acmeCo/tests/materialize-spanner-truncate"},
				})
				return err
			})
			require.NoError(t, err)
		} else if err != iterator.Done {
			require.NoError(t, err)
		}

		testutil.RunFlowctl(t, "raw", "preview-next",
			"--name", "acmeCo/tests/materialize-spanner-truncate",
			"--source", "testdata/truncate.flow.yaml",
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
			iter := clients.dataClient.Single().Query(ctx, spanner.Statement{SQL: "SELECT id FROM " + table + " ORDER BY id"})
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
