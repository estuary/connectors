package connector

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/estuary/connectors/go/common"
	m "github.com/estuary/connectors/go/materialize"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
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
				c := sqlClient.(*client)
				return updateFence(ctx, c.dataClient, c.ep.Dialect.Identifier(fence.TablePath...), fence)
			},
		)
	})

	t.Run("stale fence", func(t *testing.T) {
		ctx := context.Background()
		cfg := config{ProjectID: testProjectID, InstanceID: testInstanceID, Database: testDatabase}

		ep, err := sqlDriver.NewEndpoint(ctx, cfg, common.ResolveFlagDefaults(featureFlagDefaults, common.CreatedAt{}))
		require.NoError(t, err)
		sqlClient, err := ep.NewClient(ctx, "the/materialization", ep)
		require.NoError(t, err)
		c := sqlClient.(*client)
		defer c.Close()

		path := sql.TablePath{fmt.Sprintf("temp_test_stale_fence_%d", time.Now().UnixNano())}
		shape := sql.FlowCheckpointsTable(nil)
		shape.Path = path
		table, err := sql.ResolveTable(*shape, ep.Dialect)
		require.NoError(t, err)
		createSQL, err := sql.RenderTableTemplate(table, ep.CreateTableTemplate)
		require.NoError(t, err)
		require.NoError(t, c.ExecStatements(ctx, []string{createSQL}))
		defer c.ExecStatements(ctx, []string{"DROP TABLE " + table.Identifier})

		install := func() sql.Fence {
			fence, err := c.InstallFence(ctx, table, sql.Fence{
				TablePath:       path,
				Materialization: "the/materialization",
				KeyEnd:          math.MaxUint32,
				Checkpoint:      pm.ExplicitZeroCheckpoint,
			})
			require.NoError(t, err)
			return fence
		}

		zombie := install()
		current := install()
		require.Equal(t, zombie.Fence+1, current.Fence)

		current.Checkpoint = []byte("current")
		require.NoError(t, updateFence(ctx, c.dataClient, table.Identifier, current))

		zombie.Checkpoint = []byte("zombie")
		require.ErrorContains(t, updateFence(ctx, c.dataClient, table.Identifier, zombie), "fenced off")

		require.Equal(t, []byte("current"), install().Checkpoint)
	})
}
