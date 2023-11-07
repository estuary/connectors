package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sqlDriver "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	var spec *pf.MaterializationSpec
	var specJson, err = os.ReadFile("testdata/spec.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(specJson, &spec))

	var shape1 = sqlDriver.BuildTableShape(spec, 0, tableConfig{
		Schema: "a-schema",
		Table:  "target_table",
		Delta:  false,
	})
	var shape2 = sqlDriver.BuildTableShape(spec, 1, tableConfig{
		Schema:         "default",
		Table:          "Delta Updates",
		Delta:          true,
	})
	shape2.Document = nil // TODO(johnny): this is a bit gross.

	table1, err := sqlDriver.ResolveTable(shape1, databricksDialect)
	require.NoError(t, err)
	table2, err := sqlDriver.ResolveTable(shape2, databricksDialect)
	require.NoError(t, err)

	var snap strings.Builder

	for _, tbl := range []sqlDriver.Table{table1, table2} {
		for _, tpl := range []*template.Template{
			tplCreateLoadTable,
			tplCreateStoreTable,
			tplLoadQuery,
			tplTruncateLoad,
			tplTruncateStore,
			tplDropLoad,
			tplDropStore,
			tplMergeInto,
			tplCopyIntoDirect,
			tplCopyIntoStore,
			tplCopyIntoLoad,
		} {
			var testcase = tbl.Identifier + " " + tpl.Name()

      var tplData = Template{Table: &tbl, StagingPath: "test-staging-path", ShardRange: "shard-range"}
			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(&snap, &tplData))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}

		for _, tpl := range []*template.Template{
			tplCreateTargetTable,
		} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(&snap, &tbl))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	var shapeNoValues = sqlDriver.BuildTableShape(spec, 2, tableConfig{
		Table: "target_table_no_values_materialized",
		Delta: false,
	})
	tableNoValues, err := sqlDriver.ResolveTable(shapeNoValues, databricksDialect)
	require.NoError(t, err)

	snap.WriteString("--- Begin " + "target_table_no_values_materialized mergeInto" + " ---")
  var tplData = Template{Table: &tableNoValues, StagingPath: "test-staging-path", ShardRange: "shard-range"}
	require.NoError(t, tplMergeInto.Execute(&snap, &tplData))
	snap.WriteString("--- End " + "target_table_no_values_materialized mergeInto" + " ---\n\n")

	cupaloy.SnapshotT(t, snap.String())
}
