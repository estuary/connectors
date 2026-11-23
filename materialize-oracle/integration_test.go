package connector

import (
	"bytes"
	"context"
	stdsql "database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/estuary/flow/go/protocols/fdb/tuple"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	pb "go.gazette.dev/core/broker/protocol"
	"go.gazette.dev/core/consumer/protocol"
)

func integrationSource(t *testing.T, source string, cfg config) string {
	t.Helper()
	bundled := testutil.RunFlowctl(t, "raw", "bundle", "--source", source)
	var err error
	for name, task := range gjson.GetBytes(bundled, "materializations").Map() {
		prefix := "materializations." + name
		bundled, err = sjson.SetBytes(bundled, prefix+".endpoint.local.config", cfg)
		require.NoError(t, err)
		if cfg.Advanced.NoFlowDocument {
			for i, binding := range task.Get("bindings").Array() {
				var exclude []string
				for _, field := range binding.Get("fields.exclude").Array() {
					exclude = append(exclude, field.String())
				}
				exclude = append(exclude, "flow_document")
				bundled, err = sjson.SetBytes(bundled, fmt.Sprintf("%s.bindings.%d.fields.exclude", prefix, i), exclude)
				require.NoError(t, err)
			}
		}
	}
	if cfg.Advanced.NoFlowDocument {
		// Column-based loads need the whole metadata object to preserve UUIDs,
		// and a projection of the operation to recognize source deletes.
		for name := range gjson.GetBytes(bundled, "collections").Map() {
			prefix := "collections." + name + ".schema.properties._meta"
			bundled, err = sjson.SetBytes(bundled, prefix+".type", "object")
			require.NoError(t, err)
			bundled, err = sjson.SetBytes(bundled, prefix+".properties.op.type", "string")
			require.NoError(t, err)
		}
	}
	path := filepath.Join(t.TempDir(), "test.flow.yaml")
	require.NoError(t, os.WriteFile(path, bundled, 0o600))
	return path
}

func testTruncateAfterBackfill(t *testing.T) {
	if testutil.RuntimeV1() {
		t.Skip("backfill signals require runtime-next")
	}
	for _, tc := range []struct {
		name  string
		flags string
		want  []int64
	}{
		{"default", "", []int64{1}},
		{"disabled", "no_truncate_after_backfill", []int64{1, 2, 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := integrationConfig()
			cfg.Advanced.FeatureFlags = tc.flags
			source := integrationSource(t, "testdata/truncate.flow.yaml", cfg)
			testutil.RunTestAllTasks(t, source, func(t *testing.T, bundled []byte, name string, cfg config) {
				task := sql.PrepareTestTask(t, NewDriver().Driver, bundled, name, cfg, func(table string, delta bool) tableConfig {
					return tableConfig{Table: table, Delta: delta}
				})
				db, err := openDB(cfg)
				require.NoError(t, err)
				defer db.Close()
				dialect := createOracleDialect(cfg.DefaultNamespace())
				runPreview(t, task, "testdata/truncate.fixture.json")

				// The backfill rewrites id 1 but omits ids 2 and 3. Only standard
				// updates with a publication timestamp may delete the omitted rows.
				for table, want := range map[string][]int64{
					"truncate_standard":        tc.want,
					"truncate_delta":           {1, 1, 2, 3},
					"truncate_no_published_at": {1, 2, 3},
				} {
					rows, err := db.QueryContext(t.Context(), "SELECT ID FROM "+dialect.Identifier(task.Path(t, table)...)+" ORDER BY ID")
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
	}
}

func testLiveDeltaUpdatesTransition(t *testing.T) {
	testutil.SkipRuntimeV1(t)
	cfg := integrationConfig()
	cfg.Advanced.FeatureFlags = ""
	source := integrationSource(t, "testdata/lifecycle.flow.yaml", cfg)
	testutil.RunTestAllTasks(t, source, func(t *testing.T, bundled []byte, name string, cfg config) {
		driver := NewDriver()
		task := sql.PrepareTestTask(t, driver.Driver, bundled, name, cfg, func(table string, delta bool) tableConfig {
			return tableConfig{Table: table, Delta: delta}
		})
		path := task.Path(t, "lifecycle")
		dialect := createOracleDialect(cfg.DefaultNamespace())
		identifier := dialect.Identifier(path...)
		db, err := openDB(cfg)
		require.NoError(t, err)
		defer db.Close()

		makeSpec := func(delta bool, backfill uint32) *pf.MaterializationSpec {
			spec := testMaterialization(t, cfg)
			spec.Name = pf.Materialization(task.Name)
			binding := spec.Bindings[0]
			binding.ResourcePath = path
			binding.ResourceConfigJson, err = json.Marshal(tableConfig{Schema: path[0], Table: path[1], Delta: delta})
			require.NoError(t, err)
			binding.DeltaUpdates, binding.Backfill = delta, backfill
			return spec
		}
		validate := func(spec, last *pf.MaterializationSpec) error {
			binding := spec.Bindings[0]
			_, err := driver.Validate(t.Context(), &pm.Request_Validate{
				Name: spec.Name, ConfigJson: spec.ConfigJson, ConnectorType: spec.ConnectorType, LastMaterialization: last,
				Bindings: []*pm.Request_Validate_Binding{{ResourceConfigJson: binding.ResourceConfigJson, Collection: binding.Collection, Backfill: binding.Backfill}},
			})
			return err
		}
		open := func(spec *pf.MaterializationSpec) (m.Transactor, *pm.Response_Opened) {
			tr, opened, _, err := driver.NewTransactor(t.Context(), pm.Request_Open{
				Materialization: spec, Version: fmt.Sprintf("v%d", spec.Bindings[0].Backfill), Range: &pf.RangeSpec{KeyEnd: 0xffffffff, RClockEnd: 0xffffffff},
			}, nil)
			require.NoError(t, err)
			t.Cleanup(tr.Destroy)
			return tr, opened
		}
		store := func(tr m.Transactor, status string, checkpoint *protocol.Checkpoint) error {
			document, err := json.Marshal(map[string]any{"id": 2, "status": status})
			require.NoError(t, err)
			start, err := tr.Store(storeIterator(t, storeRequest(string(document), tuple.Tuple{status}, false, false)))
			require.NoError(t, err)
			_, future := start(t.Context(), checkpoint)
			if future != nil {
				return future.Err()
			}
			return nil
		}
		checkRows := func(want ...string) {
			rows, err := db.QueryContext(t.Context(), "SELECT ID, STATUS FROM "+identifier)
			require.NoError(t, err)
			defer rows.Close()
			var statuses []string
			for rows.Next() {
				var id int64
				var status string
				require.NoError(t, rows.Scan(&id, &status))
				require.Equal(t, int64(2), id)
				statuses = append(statuses, status)
			}
			require.NoError(t, rows.Err())
			require.ElementsMatch(t, want, statuses)
		}
		checkPrimaryKey := func(want int) {
			location := dialect.TableLocator(path)
			var count int
			require.NoError(t, db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM ALL_CONSTRAINTS
WHERE OWNER = :1 AND TABLE_NAME = :2 AND CONSTRAINT_TYPE = 'P'`, location.TableSchema, location.TableName).Scan(&count))
			require.Equal(t, want, count)
		}

		standard := makeSpec(false, 0)
		require.NoError(t, validate(standard, nil))
		_, err = driver.Apply(t.Context(), &pm.Request_Apply{Materialization: standard, Version: "v0"})
		require.NoError(t, err)
		checkPrimaryKey(1)
		checkpoint := &protocol.Checkpoint{Sources: map[pb.Journal]protocol.Checkpoint_Source{
			"test/journal": {ReadThrough: 1234},
		}}
		first, _ := open(standard)
		require.NoError(t, store(first, "initial", checkpoint))
		first.Destroy()
		checkRows("initial")

		// Reopen from the committed checkpoint without deleting it, then fence
		// this session and verify its attempted store is rolled back.
		reopened, opened := open(standard)
		require.Equal(t, checkpoint, opened.RuntimeCheckpoint)
		require.NoError(t, store(reopened, "updated", checkpoint))
		checkRows("updated")
		current, opened := open(standard)
		require.Equal(t, checkpoint, opened.RuntimeCheckpoint)
		require.ErrorContains(t, store(reopened, "fenced", checkpoint), "fenced off")
		current.Destroy()
		reopened.Destroy()
		checkRows("updated")

		require.ErrorContains(t, validate(makeSpec(true, 0), standard), "changing delta_updates requires a backfill")
		delta := makeSpec(true, 1)
		require.NoError(t, validate(delta, standard))
		_, err = driver.Apply(t.Context(), &pm.Request_Apply{
			Materialization: delta, LastMaterialization: standard, Version: "v1", LastVersion: "v0",
		})
		require.NoError(t, err)
		checkPrimaryKey(0)
		checkRows()
		appender, _ := open(delta)
		require.NoError(t, store(appender, "first delta", checkpoint))
		require.NoError(t, store(appender, "second delta", checkpoint))
		checkRows("first delta", "second delta")
	})
}

func runOracleMaterializationTests(t *testing.T) {
	for _, tc := range []struct {
		name           string
		noFlowDocument bool
	}{
		{"materialize", false},
		{"materialize-no-flow-document", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := integrationConfig()
			cfg.Advanced.NoFlowDocument = tc.noFlowDocument
			dialect := createOracleDialect(cfg.DefaultNamespace())
			db, err := openDB(cfg)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })

			const taskName = "acmeCo/tests/materialize-oracle"
			bundled := testutil.RunFlowctl(t, "raw", "bundle", "--source", "testdata/oracle.flow.yaml")
			bundled, err = sjson.SetBytes(bundled, "materializations."+taskName+".endpoint.local.config", cfg)
			require.NoError(t, err)
			if tc.noFlowDocument {
				// The endpoint option makes the root document optional; field
				// selection must explicitly exclude it to test tables without it.
				for i := range gjson.GetBytes(bundled, "materializations."+taskName+".bindings").Array() {
					bundled, err = sjson.SetBytes(bundled, fmt.Sprintf("materializations.%s.bindings.%d.fields.exclude", taskName, i), []string{"flow_document"})
					require.NoError(t, err)
				}
			}
			task := sql.PrepareTestTask(t, NewDriver().Driver, bundled, taskName, cfg, func(table string, delta bool) tableConfig {
				return tableConfig{Table: table, Delta: delta}
			})

			initial, err := os.ReadFile("testdata/initial.fixture.json")
			require.NoError(t, err)
			longText := strings.Repeat("x", 5000)
			initial = bytes.ReplaceAll(initial, []byte("__LONG_TEXT__"), []byte(longText))
			initialPath := filepath.Join(t.TempDir(), "initial.fixture.json")
			require.NoError(t, os.WriteFile(initialPath, initial, 0o600))

			var snap strings.Builder
			var stagingTables []int
			for _, phase := range []struct {
				name    string
				fixture string
			}{
				{"initial", initialPath},
				{"updates", "testdata/updates.fixture.json"},
			} {
				// Each invocation has fresh runtime state and must load existing
				// rows to preserve fields omitted by the second fixture. A fresh
				// runtime cannot resume the checkpoint of an earlier one.
				if phase.name != "initial" {
					clearCheckpoint(t, db, cfg, task.Name)
				}
				runPreview(t, task, phase.fixture)
				snap.WriteString(phase.name + "\n")
				for _, table := range []string{"orders", "orders_delta", "Type Samples", "keyed"} {
					identifier := dialect.Identifier(task.Path(t, table)...)
					query := "SELECT ID, STATUS, CANARY, AMOUNT FROM " + identifier
					if table == "Type Samples" {
						query = `SELECT ID, ACTIVE, TO_CHAR(EVENT_TIME, 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZH:TZM') AS EVENT_TIME,
SMALL_TEXT, DBMS_LOB.GETLENGTH(LONG_TEXT) AS LONG_LENGTH, DETAILS, TAGS, "select", "field.with.dots" FROM ` + identifier
					} else if table == "keyed" {
						query = "SELECT TENANT_ID, SKU, EVENT_TIME, CODE, NOTE FROM " + identifier
					}
					cols, rows, err := sql.SnapshotTestTableQuery(t.Context(), db, identifier, query)
					require.NoError(t, err)
					data, err := testutil.RenderRows(cols, rows)
					require.NoError(t, err)
					snap.WriteString(table + "\n" + data)

					var published int
					require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(FLOW_PUBLISHED_AT) FROM "+identifier).Scan(&published))
					require.Equal(t, len(rows), published)
				}

				var staging int
				require.NoError(t, db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM USER_TABLES WHERE TEMPORARY = 'Y' AND TABLE_NAME LIKE 'FLOW_%'").Scan(&staging))
				stagingTables = append(stagingTables, staging)

				typesTable := dialect.Identifier(task.Path(t, "Type Samples")...)
				var actualText string
				require.NoError(t, db.QueryRowContext(t.Context(), "SELECT LONG_TEXT FROM "+typesTable+" WHERE ID = 1").Scan(&actualText))
				require.Equal(t, longText, actualText)
				if tc.noFlowDocument {
					// Optional metadata must carry the UUID needed by backfill-aware loads.
					var metadata string
					require.NoError(t, db.QueryRowContext(t.Context(), `SELECT "_meta" FROM `+dialect.Identifier(task.Path(t, "orders")...)+" WHERE ID = 2").Scan(&metadata))
					require.NotEmpty(t, gjson.Get(metadata, "uuid").Str)
				} else {
					var document string
					require.NoError(t, db.QueryRowContext(t.Context(), "SELECT FLOW_DOCUMENT FROM "+dialect.Identifier(task.Path(t, "orders")...)+" WHERE ID = 2").Scan(&document))
					var decoded map[string]any
					require.NoError(t, json.Unmarshal([]byte(document), &decoded))
					require.Equal(t, float64(2), decoded["id"])
					require.Equal(t, "o2", decoded["canary"])
				}
			}

			orderTypes := map[string]string{
				"ID": "NUMBER", "STATUS": "CLOB", "CANARY": "CLOB", "AMOUNT": "BINARY_DOUBLE",
				"FLOW_PUBLISHED_AT": "TIMESTAMP(9) WITH TIME ZONE",
			}
			sampleTypes := map[string]string{
				"ID": "NUMBER", "ACTIVE": "NUMBER", "EVENT_TIME": "TIMESTAMP(9) WITH TIME ZONE",
				"SMALL_TEXT": "VARCHAR2", "LONG_TEXT": "CLOB", "DETAILS": "CLOB", "TAGS": "CLOB",
				"select": "CLOB", "field.with.dots": "CLOB", "FLOW_PUBLISHED_AT": "TIMESTAMP(9) WITH TIME ZONE",
			}
			keyedTypes := map[string]string{
				"TENANT_ID": "VARCHAR2", "SKU": "VARCHAR2", "EVENT_TIME": "VARCHAR2", "CODE": "VARCHAR2", "NOTE": "CLOB",
				"FLOW_PUBLISHED_AT": "TIMESTAMP(9) WITH TIME ZONE",
			}
			if tc.noFlowDocument {
				orderTypes["_meta"] = "CLOB"
				orderTypes["_meta/op"] = "CLOB"
				sampleTypes["_meta"] = "CLOB"
				sampleTypes["_meta/op"] = "CLOB"
				keyedTypes["_meta"] = "CLOB"
				keyedTypes["_meta/op"] = "CLOB"
			}
			checkColumnTypes(t, db, dialect, task.Path(t, "orders"), orderTypes, !tc.noFlowDocument)
			checkColumnTypes(t, db, dialect, task.Path(t, "Type Samples"), sampleTypes, !tc.noFlowDocument)
			checkColumnTypes(t, db, dialect, task.Path(t, "keyed"), keyedTypes, !tc.noFlowDocument)
			// Each preview is a new session, which reuses the earlier session's staging tables.
			require.Positive(t, stagingTables[0])
			require.Equal(t, stagingTables[0], stagingTables[1])
			checkFencing(t, db, cfg, task.Name)
			cupaloy.SnapshotT(t, snap.String())
		})
	}
}

func runPreview(t *testing.T, task testutil.TestTask, fixture string) {
	t.Helper()
	args := []string{"raw", "preview-next", "--shards", "1"}
	if testutil.RuntimeV1() {
		args = []string{"preview", "--sessions=-1,0"}
	}
	args = append(args, "--name", task.Name, "--source", task.SourcePath,
		"--fixture", fixture, "--timeout", "5m", "--network", "flow-test")
	testutil.RunFlowctl(t, args...)
}

func clearCheckpoint(t *testing.T, db *stdsql.DB, cfg config, taskName string) {
	t.Helper()
	table, err := sql.ResolveTable(*sql.FlowCheckpointsTable([]string{cfg.DefaultNamespace()}), createOracleDialect(cfg.DefaultNamespace()))
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), "DELETE FROM "+table.Identifier+" WHERE MATERIALIZATION = :1", taskName)
	require.NoError(t, err)
}

func checkColumnTypes(t *testing.T, db *stdsql.DB, dialect sql.Dialect, path []string, want map[string]string, document bool) {
	t.Helper()
	if document {
		want["FLOW_DOCUMENT"] = "CLOB"
	}
	location := dialect.TableLocator(path)
	rows, err := db.QueryContext(t.Context(), `SELECT COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS
WHERE OWNER = :1 AND TABLE_NAME = :2`, location.TableSchema, location.TableName)
	require.NoError(t, err)
	defer rows.Close()
	actual := make(map[string]string)
	for rows.Next() {
		var name, dataType string
		require.NoError(t, rows.Scan(&name, &dataType))
		actual[name] = dataType
	}
	require.NoError(t, rows.Err())
	require.Equal(t, want, actual)
}

func checkFencing(t *testing.T, db *stdsql.DB, cfg config, taskName string) {
	t.Helper()
	ctx := context.Background()
	dialect := createOracleDialect(cfg.DefaultNamespace())
	table, err := sql.ResolveTable(*sql.FlowCheckpointsTable([]string{cfg.DefaultNamespace()}), dialect)
	require.NoError(t, err)
	var fence int64
	var encoded string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT FENCE, CHECKPOINT FROM "+table.Identifier+
		" WHERE MATERIALIZATION = :1 AND KEY_BEGIN = 0 AND KEY_END = 4294967295", taskName).Scan(&fence, &encoded))
	checkpoint, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)
	require.NotEmpty(t, checkpoint)
	require.NotEqual(t, pm.ExplicitZeroCheckpoint, checkpoint)
	var decoded protocol.Checkpoint
	require.NoError(t, decoded.Unmarshal(checkpoint))

	var previous = sql.Fence{TablePath: table.Path, Materialization: pf.Materialization(taskName), KeyEnd: 0xffffffff, Fence: fence}
	installed, err := installFence(ctx, db, table, previous)
	require.NoError(t, err)
	require.Equal(t, fence+1, installed.Fence)
	require.Equal(t, checkpoint, installed.Checkpoint)

	var update strings.Builder
	require.NoError(t, renderTemplates(dialect).updateFence.Execute(&update, previous))
	for _, tc := range []struct {
		fence int64
		rows  int64
	}{{previous.Fence, 0}, {installed.Fence, 1}} {
		result, err := db.ExecContext(ctx, update.String(), go_ora.Clob{String: encoded, Valid: true}, taskName, 0, uint32(0xffffffff), tc.fence)
		require.NoError(t, err)
		rows, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, tc.rows, rows)
	}
}

func integrationConfig() config {
	cfg := testConfig()
	cfg.Address = "localhost:1522"
	cfg.HardDelete = true
	for _, item := range []struct {
		env string
		out *string
	}{
		{"ORACLE_ADDRESS", &cfg.Address},
		{"ORACLE_SERVICE_NAME", &cfg.ServiceName},
		{"ORACLE_USER", &cfg.User},
		{"ORACLE_PASSWORD", &cfg.Password},
		{"ORACLE_SCHEMA", &cfg.Schema},
	} {
		if value := os.Getenv(item.env); value != "" {
			*item.out = value
		}
	}
	return cfg
}
