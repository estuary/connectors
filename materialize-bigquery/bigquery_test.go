package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/estuary/connectors/go/blob"
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
		}
	}

	actionDescSanitizers := []func(string) string{
		func(s string) string {
			return regexp.MustCompile(`"JobPrefix":\s*"[^"]*"`).ReplaceAllString(s, `"JobPrefix": "<uuid>"`)
		},
		func(s string) string {
			return regexp.MustCompile(`"gs://[^/]+/[^"]*"`).ReplaceAllString(s, `"gs://[bucket]/<uuid>"`)
		},
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newBigQueryDriver(), "testdata/materialize.flow.yaml", makeResourceFn, actionDescSanitizers)
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newBigQueryDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("apply-drain", func(t *testing.T) {
		testutil.RunTestAllTasks(t, "testdata/apply.flow.yaml", func(t *testing.T, bundled []byte, taskName string, cfg config) {
			ctx := context.Background()
			tableName := fmt.Sprintf("applydrain%s_flow_test_%d", uuid.NewString()[:8], time.Now().Unix())
			res := makeResourceFn(tableName, false).WithDefaults(cfg)

			seedPending := func(t *testing.T, appliedSpec *pf.MaterializationSpec) json.RawMessage {
				// A staged transaction is a query persisted in the connector
				// state, keyed by the binding's state key, along with the GCS
				// files it consumes as an external table and the job prefix
				// making its execution idempotent. The staged file must exist
				// since Acknowledge deletes it after the query succeeds.
				credOption, err := cfg.CredentialsClientOption()
				require.NoError(t, err)
				bucket, err := blob.NewGCSBucket(ctx, cfg.Bucket, credOption)
				require.NoError(t, err)

				fileKey := path.Join(cfg.effectiveBucketPath(), fmt.Sprintf("applydrain-%s.json", uuid.NewString()))
				require.NoError(t, bucket.Upload(ctx, fileKey, strings.NewReader("")))

				query := sql.DrainSeedInsertQuery(t, newBigQueryDriver(), cfg, appliedSpec, "JSON '{}'")
				state, err := json.Marshal(map[string]any{
					appliedSpec.Bindings[0].StateKey: map[string]any{
						"Query":         query,
						"SourceURIs":    []string{bucket.URI(fileKey)},
						"JobPrefix":     uuid.NewString(),
						"TempTableName": "flow_temp_table_0",
					},
				})
				require.NoError(t, err)
				return state
			}

			verifyDrained := func(t *testing.T, _ *pf.MaterializationSpec, _ []string, rows [][]any) {
				require.Len(t, rows, 1, "the staged transaction's row must have been committed")
			}

			sql.RunApplyDrainTest(t, newBigQueryDriver(), cfg, res, seedPending, verifyDrained)
		})
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newBigQueryDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})

	// Toggling objects_and_arrays_as_json migrates the object column and the
	// flow_document column between JSON and STRING in place. This exercises the
	// root document JSON<->text migration end-to-end, the scenario that requires
	// the root document to be migratable (rather than needing a backfill).
	t.Run("flow_document-migration", func(t *testing.T) {
		sql.RunFeatureFlagMigrationTest(t, newBigQueryDriver(), "testdata/migrate-doc.flow.yaml", makeResourceFn, []sql.FeatureFlagMigrationPhase{
			{FeatureFlags: "objects_and_arrays_as_json", Fixture: "testdata/fixture.doc-migrate.json"},    // materialize as JSON
			{FeatureFlags: "no_objects_and_arrays_as_json", Fixture: "testdata/fixture.doc-migrate.json"}, // migrate JSON -> STRING
			{FeatureFlags: "objects_and_arrays_as_json", Fixture: "testdata/fixture.doc-migrate.json"},    // migrate STRING -> JSON
		}, actionDescSanitizers)
	})
}

