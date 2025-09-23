package main

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pm "github.com/estuary/flow/go/protocols/materialize"
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

	t.Run("migrate", func(t *testing.T) {
		sql.RunMigrationTest(t, newBigQueryDriver(), "testdata/migrate.flow.yaml", makeResourceFn, nil)
	})
}

func TestSpecification(t *testing.T) {
	var resp, err = newBigQueryDriver().
		Spec(context.Background(), &pm.Request_Spec{})
	require.NoError(t, err)

	formatted, err := json.MarshalIndent(resp, "", "  ")
	require.NoError(t, err)

	cupaloy.SnapshotT(t, formatted)
}
