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
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	pm "github.com/estuary/flow/go/protocols/materialize"
	go_ora "github.com/sijms/go-ora/v2"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.gazette.dev/core/consumer/protocol"
)

func TestIntegration(t *testing.T) {
	if !integrationTestsEnabled() {
		t.Skip("skipping Oracle integration test: set TEST_DATABASE=yes or CONNECTOR=materialize-oracle")
	}

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
			bundled := testutil.RunFlowctl(t, "raw", "bundle", "--source", "testdata/materialize.flow.yaml")
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
				if !tc.noFlowDocument {
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
				orderTypes["_meta/op"] = "CLOB"
				sampleTypes["_meta/op"] = "CLOB"
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
