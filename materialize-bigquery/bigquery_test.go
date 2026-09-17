package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/estuary/connectors/go/blob"
	m "github.com/estuary/connectors/go/materialize"
	testutil "github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
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
		started := time.Now()
		sql.RunMaterializationTest(t, NewDriver().sqlDriver, "testdata/materialize.flow.yaml", makeResourceFn, actionDescSanitizers,
			sql.RuntimeConfig{Shards: 1, Fidelity: m.FidelityExact})

		// Every load results table created by this run must be deleted after
		// read-back. Tables older than this run belong to another run and are
		// ignored.
		testutil.RunTestAllTasks(t, "testdata/materialize.flow.yaml", func(t *testing.T, _ []byte, taskName string, cfg config) {
			credOption, err := cfg.CredentialsClientOption()
			require.NoError(t, err)
			bq, err := bigquery.NewClient(t.Context(), cfg.ProjectID, credOption)
			require.NoError(t, err)
			t.Cleanup(func() { bq.Close() })

			prefix := loadResultsTablePrefix + translateFlowIdentifier(taskName)
			cutoff := started.Add(-time.Minute) // Tolerates skew between this clock and BigQuery's.
			var leaked []string
			it := bq.DatasetInProject(cfg.ProjectID, cfg.Dataset).Tables(t.Context())
			for {
				tbl, err := it.Next()
				if err == iterator.Done {
					break
				}
				require.NoError(t, err)
				if !strings.HasPrefix(tbl.TableID, prefix) {
					continue
				}
				md, err := tbl.Metadata(t.Context(), bigquery.WithMetadataView(bigquery.BasicMetadataView))
				require.NoError(t, err)
				if md.CreationTime.Before(cutoff) {
					t.Logf("ignoring load results table %s from an earlier run", tbl.TableID)
					continue
				}
				leaked = append(leaked, tbl.TableID)
			}
			require.Empty(t, leaked, "leaked load results tables")
		})
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, NewDriver().sqlDriver, "testdata/apply.flow.yaml", makeResourceFn)
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

				query := sql.DrainSeedInsertQuery(t, NewDriver().sqlDriver, cfg, appliedSpec, "JSON '{}'")
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

			sql.RunApplyDrainTest(t, NewDriver().sqlDriver, cfg, res, seedPending, verifyDrained)
		})
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, NewDriver().sqlDriver, "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})

	// Toggling objects_and_arrays_as_json migrates the object column and the
	// flow_document column between JSON and STRING in place. This exercises the
	// root document JSON<->text migration end-to-end, the scenario that requires
	// the root document to be migratable (rather than needing a backfill).
	t.Run("flow_document-migration", func(t *testing.T) {
		sql.RunFeatureFlagMigrationTest(t, NewDriver().sqlDriver, "testdata/migrate-doc.flow.yaml", makeResourceFn, []sql.FeatureFlagMigrationPhase{
			{FeatureFlags: "objects_and_arrays_as_json", Fixture: "testdata/fixture.doc-migrate.json"},    // materialize as JSON
			{FeatureFlags: "no_objects_and_arrays_as_json", Fixture: "testdata/fixture.doc-migrate.json"}, // migrate JSON -> STRING
			{FeatureFlags: "objects_and_arrays_as_json", Fixture: "testdata/fixture.doc-migrate.json"},    // migrate STRING -> JSON
		}, actionDescSanitizers)
	})
}
