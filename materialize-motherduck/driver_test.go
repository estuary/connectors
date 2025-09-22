package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
	"github.com/stretchr/testify/require"

	_ "github.com/marcboeker/go-duckdb/v2"
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

	t.Run("materialize", func(t *testing.T) {
		sql.RunMaterializationTest(t, newDuckDriver(), "testdata/materialize.flow.yaml", makeResourceFn)
	})

	t.Run("apply", func(t *testing.T) {
		sql.RunApplyTest(t, newDuckDriver(), "testdata/apply.flow.yaml", makeResourceFn)
	})

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newDuckDriver(), "testdata/migrate.flow.yaml", makeResourceFn)
	})

	t.Run("fence", func(t *testing.T) {
		sql.RunFencingTest(
			t,
			newDuckDriver(),
			"testdata/fence.flow.yaml",
			makeResourceFn,
			tplCreateTargetTable,
			func(ctx context.Context, client sql.Client, fence sql.Fence) error {
				var fenceUpdate strings.Builder
				if err := tplUpdateFence.Execute(&fenceUpdate, fence); err != nil {
					return fmt.Errorf("evaluating fence template: %w", err)
				}
				return client.ExecStatements(ctx, []string{fenceUpdate.String()})
			},
		)
	})
}

func TestSpecification(t *testing.T) {
	var resp, err = newDuckDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
