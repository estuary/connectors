package connector

import (
	"compress/gzip"
	"context"
	stdsql "database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sql-go/driverctx"
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
}

// TestRecoverAfterAddedColumn recovers pending entries whose files were staged
// before a column was added to the binding, in each entry shape and commit
// query form. Rows of those files lack the column, which must commit as NULL.
func TestRecoverAfterAddedColumn(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	var ctx = context.Background()
	raw, err := testutil.ReadConfigFile("testdata/config.local.yaml")
	require.NoError(t, err)
	var cfg config
	require.NoError(t, json.Unmarshal(raw, &cfg))

	var dialect = createDatabricksDialect(nil)
	wsClient, err := databricks.NewWorkspaceClient(cfg.workspaceConfig())
	require.NoError(t, err)
	db, err := stdsql.Open("databricks", cfg.ToURI("recover-added-column"))
	require.NoError(t, err)
	defer db.Close()

	var tableName = fmt.Sprintf("recover_added_column_%s_flow_test_%d", uuid.NewString()[:8], time.Now().Unix())
	var col = func(field, ddl, bareDDL string) sql.Column {
		return sql.Column{
			Identifier: dialect.Identifier(field),
			Projection: sql.Projection{Projection: pf.Projection{Field: field}},
			MappedType: sql.MappedType{DDL: ddl, BareDDL: bareDDL},
		}
	}
	var doc = col("flow_document", "STRING", "STRING")
	var target = sql.Table{
		TableShape: sql.TableShape{Path: sql.TablePath{cfg.SchemaName, tableName}},
		Identifier: dialect.Identifier(cfg.SchemaName, tableName),
		Keys:       []sql.Column{col("id", "LONG NOT NULL", "LONG")},
		Values:     []sql.Column{col("val", "STRING", "STRING"), col("added", "STRING", "STRING")},
		Document:   &doc,
		StateKey:   tableName + ".v1",
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE VOLUME IF NOT EXISTS `%s`.`%s`", cfg.SchemaName, volumeName))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE TABLE %s (id LONG NOT NULL, val STRING, added STRING, flow_document STRING)", target.Identifier))
	require.NoError(t, err)
	t.Cleanup(func() { db.ExecContext(ctx, "DROP TABLE IF EXISTS "+target.Identifier) })

	var d = &transactor{
		cfg:                   cfg,
		cp:                    make(connectorState),
		peerShardsCheckpoints: make(rangeCheckpoints),
		primary:               true,
		rangeKey:              fullRangeKey,
		files:                 wsClient.Files,
		be:                    &m.BindingEvents{},
		ep:                    &sql.Endpoint[config]{Config: cfg, Dialect: dialect},
		templates:             renderTemplates(dialect),
	}
	require.NoError(t, d.addBinding(target))
	var b = d.bindings[0]

	// stage uploads a file of one row which lacks the added column, as the
	// binding staged it before the column existed.
	var stage = func(t *testing.T, id int, remotePath string) {
		var local = filepath.Join(t.TempDir(), "staged.json.gz")
		var f, err = os.Create(local)
		require.NoError(t, err)
		var gz = gzip.NewWriter(f)
		_, err = fmt.Fprintf(gz, `{"id":%d,"val":"v","flow_document":"{}","_flow_delete":false}`+"\n", id)
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		require.NoError(t, f.Close())

		var putCtx = driverctx.NewContextWithStagingInfo(ctx, []string{filepath.Dir(local)})
		_, err = db.ExecContext(putCtx, fmt.Sprintf(`PUT '%s' INTO '%s' OVERWRITE`, local, remotePath))
		require.NoError(t, err)
	}

	for idx, tc := range []struct {
		name       string
		directory  bool
		needsMerge bool
	}{
		{"root file merge", false, true},
		{"directory merge", true, true},
		{"directory copy", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var id = idx + 1
			var name = uuid.NewString()
			var item = &checkpointItem{NeedsMerge: tc.needsMerge}
			if tc.directory {
				item.Directory = name
				item.Fields = []string{"id", "val", "flow_document", "_flow_delete"}
				stage(t, id, filepath.Join(b.rootStagingPath, name, "staged.json.gz"))
			} else {
				item.StagedFiles = []string{name + ".json.gz"}
				item.ToDelete = []string{filepath.Join(b.rootStagingPath, name+".json.gz")}
				stage(t, id, item.ToDelete[0])
			}
			d.cp = make(connectorState)
			d.cp.add(target.StateKey, fullRangeKey, item)
			d.cpRecovery = true

			_, err := d.acknowledgeApply(ctx, db, m.StateKeyFilter(nil))
			require.NoError(t, err)

			var val string
			var added stdsql.NullString
			require.NoError(t, db.QueryRowContext(ctx,
				fmt.Sprintf("SELECT val, added FROM %s WHERE id = %d", target.Identifier, id)).Scan(&val, &added))
			require.Equal(t, "v", val)
			require.False(t, added.Valid)
		})
	}
}
