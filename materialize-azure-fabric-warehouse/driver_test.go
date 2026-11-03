package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	m "github.com/estuary/connectors/go/materialize"
	"github.com/estuary/connectors/materialize-boilerplate/testutil"
	sql "github.com/estuary/connectors/materialize-sql"
	"github.com/stretchr/testify/require"
)

// mustGetCfg decrypts testdata/config.yaml, which uses sops's
// encrypted_suffix convention: encrypted values keep a "_sops" suffix on
// their key after decryption, so it's stripped here to match the connector's
// own json tags.
func mustGetCfg(t *testing.T) config {
	t.Helper()

	jsonBytes, err := exec.Command("sops", "--decrypt", "--output-type", "json", "testdata/config.yaml").Output()
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(jsonBytes, &raw))
	for k, v := range raw {
		if stripped, ok := strings.CutSuffix(k, "_sops"); ok {
			raw[stripped] = v
			delete(raw, k)
		}
	}
	delete(raw, "sops")

	strippedJSON, err := json.Marshal(raw)
	require.NoError(t, err)

	var cfg config
	require.NoError(t, json.Unmarshal(strippedJSON, &cfg))
	require.NoError(t, cfg.Validate())

	return cfg
}

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

	actionDescSanitizers := []func(string) string{
		func(s string) string {
			return regexp.MustCompile(`(sp_rename\s+'[^.]+\.\\?")[a-fA-F0-9\-]{36}(\\?"')`).ReplaceAllString(s, `${1}<uuid>${2}`)
		},
		func(s string) string {
			return regexp.MustCompile(`(CREATE TABLE\s+[^.]+\.\\?")[a-fA-F0-9\-]{36}(\\?")`).ReplaceAllString(s, `${1}<uuid>${2}`)
		},
	}

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), "testdata/materialize.flow.yaml", makeResourceFn, nil,
			sql.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal})
	})

	t.Run("materialize-no-flow-document", func(t *testing.T) {
		sql.RunMaterializationTest(t, NewDriver(), "testdata/materialize.flow.yaml", makeResourceFn, nil,
			sql.RuntimeConfig{Shards: 1, Fidelity: m.FidelityTotal,
				ConfigOverrides: map[string]any{"advanced.no_flow_document": true}})
	})

	t.Run("truncate", func(t *testing.T) {
		if testutil.RuntimeV1() {
			t.Skip("backfill signals require runtime-next")
		}
		var ctx = context.Background()
		var cfg = mustGetCfg(t)

		db, err := cfg.db()
		require.NoError(t, err)
		defer db.Close()

		for _, table := range []string{"truncate_standard", "truncate_delta", "truncate_no_published_at"} {
			_, err := db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s;", testDialect.Identifier(cfg.Warehouse, cfg.Schema, table)))
			require.NoError(t, err)
		}

		checkpoints := testDialect.Identifier(cfg.Warehouse, cfg.Schema, "flow_checkpoints_v1")
		_, err = db.ExecContext(ctx, fmt.Sprintf(
			"IF OBJECT_ID(N'%s.flow_checkpoints_v1', N'U') IS NOT NULL DELETE FROM %s WHERE materialization = %s;",
			cfg.Schema, checkpoints, testDialect.Literal("acmeCo/tests/materialize-azure-fabric-warehouse-truncate"),
		))
		require.NoError(t, err)

		testutil.RunFlowctl(t, "raw", "preview-next",
			"--name", "acmeCo/tests/materialize-azure-fabric-warehouse-truncate",
			"--source", "testdata/truncate.flow.yaml",
			"--fixture", "testdata/truncate.fixture.json",
			"--shards", "1",
			"--timeout", "5m",
			"--network", "flow-test",
		)

		// The fixture stores ids 1-3, then re-stores only id 1 during a
		// backfill. A table with a flow_published_at column loses the rows
		// published before the backfill, unless it's in delta-updates mode:
		// delta-updates both skips truncation and never upserts, so every
		// store appends a row.
		for table, want := range map[string][]int64{
			"truncate_standard":        {1},
			"truncate_delta":           {1, 1, 2, 3},
			"truncate_no_published_at": {1, 2, 3},
		} {
			rows, err := db.QueryContext(ctx, "SELECT id FROM "+testDialect.Identifier(cfg.Warehouse, cfg.Schema, table)+" ORDER BY id;")
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

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, NewDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, NewDriver(), "testdata/migrate.flow.yaml", makeResourceFn, actionDescSanitizers)
	})
	t.Run("key-change-migrate", func(t *testing.T) {
		sql.RunKeyChangeMigrationTest(t, NewDriver(), "testdata/key-change.flow.yaml", makeResourceFn, actionDescSanitizers)
	})

	t.Run("fence", func(t *testing.T) {
		sql.RunFencingTest(
			t,
			NewDriver(),
			"testdata/fence.flow.yaml",
			makeResourceFn,
			testTemplates.createTargetTable,
			func(ctx context.Context, client sql.Client, fence sql.Fence) error {
				var fenceUpdate strings.Builder
				if err := testTemplates.updateFence.Execute(&fenceUpdate, fence); err != nil {
					return fmt.Errorf("evaluating fence template: %w", err)
				}
				return client.ExecStatements(ctx, []string{fenceUpdate.String()})
			},
		)
	})
}
