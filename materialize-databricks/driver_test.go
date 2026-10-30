package connector

import (
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

	m "github.com/estuary/connectors/go/materialize"
	testutil "github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
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

	sanitizers := []func(string) string{
		func(s string) string {
			return regexp.MustCompile(`[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}`).
				ReplaceAllString(s, "<uuid>")
		},
	}

	// Databricks supports runtime-v2 scale-out, so its integration test always
	// runs with two shards.
	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), "testdata/materialize.flow.yaml", makeResourceFn, sanitizers,
			sql.RuntimeConfig{Shards: 2, Fidelity: m.FidelityExact})
	})
	t.Run("truncate", func(t *testing.T) {
		if testutil.RuntimeV1() {
			t.Skip("backfill signals require runtime-next")
		}
		testutil.RunTestAllTasks(t, "testdata/truncate.flow.yaml", func(t *testing.T, _ []byte, taskName string, cfg config) {
			var ctx = context.Background()
			db, err := stdsql.Open("databricks", cfg.ToURI(taskName))
			require.NoError(t, err)
			defer db.Close()

			var tables = []string{"truncate_standard", "truncate_delta", "truncate_no_published_at"}
			for _, table := range tables {
				var identifier = fmt.Sprintf("`%s`.`%s`.`%s`", cfg.CatalogName, cfg.SchemaName, table)
				_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+identifier+";")
				require.NoError(t, err)
			}

			// Two shards, so that the primary truncates after applying a peer's
			// staged stores.
			testutil.RunFlowctl(t, "raw", "preview-next",
				"--name", taskName,
				"--source", "testdata/truncate.flow.yaml",
				"--fixture", "testdata/truncate.fixture.json",
				"--shards", "2",
				"--timeout", "10m",
			)

			// The fixture stores ids 1-3, then re-stores only id 1 during a
			// backfill. Only the standard table with a flow_published_at column
			// loses the rows published before the backfill.
			for table, want := range map[string][]int64{
				"truncate_standard":        {1},
				"truncate_delta":           {1, 1, 2, 3},
				"truncate_no_published_at": {1, 2, 3},
			} {
				var identifier = fmt.Sprintf("`%s`.`%s`.`%s`", cfg.CatalogName, cfg.SchemaName, table)
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
	t.Run("apply-drain", func(t *testing.T) {
		testutil.RunTestAllTasks(t, "testdata/apply.flow.yaml", func(t *testing.T, bundled []byte, taskName string, cfg config) {
			tableName := fmt.Sprintf("applydrain%s_flow_test_%d", uuid.NewString()[:8], time.Now().Unix())
			res := makeResourceFn(tableName, false).WithDefaults(cfg)

			seedPending := func(t *testing.T, appliedSpec *pf.MaterializationSpec) json.RawMessage {
				// A staged transaction is a set of queries persisted in the
				// connector state under the binding's state key and the
				// staging shard's key range.
				query := sql.DrainSeedInsertQuery(t, NewDriver(), cfg, appliedSpec, "'{}'")
				state, err := json.Marshal(map[string]any{
					appliedSpec.Bindings[0].StateKey: map[string]any{
						"00000000-ffffffff": map[string]any{
							"Queries":  []string{query},
							"ToDelete": []string{},
						},
					},
				})
				require.NoError(t, err)
				return state
			}

			verifyDrained := func(t *testing.T, _ *pf.MaterializationSpec, _ []string, rows [][]any) {
				require.Len(t, rows, 1, "the staged transaction's row must have been committed")
			}

			sql.RunApplyDrainTest(t, NewDriver(), cfg, res, seedPending, verifyDrained)
		})
	})
	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, NewDriver(), "testdata/migrate.flow.yaml", makeResourceFn, sanitizers)
	})
	t.Run("key-change-migrate", func(t *testing.T) {
		sql.RunKeyChangeMigrationTest(t, NewDriver(), "testdata/key-change.flow.yaml", makeResourceFn, sanitizers)
	})
}
