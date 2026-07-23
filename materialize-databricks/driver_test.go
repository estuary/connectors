package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"testing"
	"time"

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
	// runs with two shards, which requires its scale_out feature flag.
	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newDatabricksDriver(), "testdata/materialize.flow.yaml", makeResourceFn, sanitizers,
			sql.RuntimeV2Config{Shards: 2, ExtraFeatureFlags: []string{"scale_out"}})
	})
	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newDatabricksDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})
	t.Run("apply-drain", func(t *testing.T) {
		testutil.RunTestAllTasks(t, "testdata/apply.flow.yaml", func(t *testing.T, bundled []byte, taskName string, cfg config) {
			tableName := fmt.Sprintf("applydrain%s_flow_test_%d", uuid.NewString()[:8], time.Now().Unix())
			res := makeResourceFn(tableName, false).WithDefaults(cfg)

			seedPending := func(t *testing.T, appliedSpec *pf.MaterializationSpec) json.RawMessage {
				// A staged transaction is a set of queries persisted in the
				// connector state, keyed by the binding's state key.
				query := sql.DrainSeedInsertQuery(t, newDatabricksDriver(), cfg, appliedSpec, "'{}'")
				state, err := json.Marshal(map[string]any{
					appliedSpec.Bindings[0].StateKey: map[string]any{
						"Queries":  []string{query},
						"ToDelete": []string{},
					},
				})
				require.NoError(t, err)
				return state
			}

			verifyDrained := func(t *testing.T, _ *pf.MaterializationSpec, _ []string, rows [][]any) {
				require.Len(t, rows, 1, "the staged transaction's row must have been committed")
			}

			sql.RunApplyDrainTest(t, newDatabricksDriver(), cfg, res, seedPending, verifyDrained)
		})
	})
	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newDatabricksDriver(), "testdata/migrate.flow.yaml", makeResourceFn, sanitizers)
	})
}
