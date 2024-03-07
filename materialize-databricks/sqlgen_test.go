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
		Schema: "default",
		Table:  "Delta Updates",
		Delta:  true,
	})
	shape2.Document = nil // TODO(johnny): this is a bit gross.

	table1, err := sqlDriver.ResolveTable(shape1, databricksDialect)
	require.NoError(t, err)
	table2, err := sqlDriver.ResolveTable(shape2, databricksDialect)
	require.NoError(t, err)

	var snap strings.Builder

	for _, tbl := range []sqlDriver.Table{table1, table2} {
		for _, tpl := range []*template.Template{
			tplLoadQuery,
			tplMergeInto,
			tplCopyIntoDirect,
		} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			var tplData = tableWithFiles{Table: &tbl, StagingPath: "test-staging-path", Files: []string{"file1", "file2"}}
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

	snap.WriteString("--- Begin alter table add columns ---")
	require.NoError(t, tplAlterTableColumns.Execute(&snap, sqlDriver.TableAlter{
		Table: table1,
		AddColumns: []sqlDriver.Column{
			{Identifier: "first_new_column", MappedType: sqlDriver.MappedType{NullableDDL: "STRING"}},
			{Identifier: "second_new_column", MappedType: sqlDriver.MappedType{NullableDDL: "BOOL"}},
		},
	}))
	snap.WriteString("--- End alter table add columns ---\n\n")

	var shapeNoValues = sqlDriver.BuildTableShape(spec, 2, tableConfig{
		Table: "target_table_no_values_materialized",
		Delta: false,
	})
	tableNoValues, err := sqlDriver.ResolveTable(shapeNoValues, databricksDialect)
	require.NoError(t, err)

	snap.WriteString("--- Begin " + "target_table_no_values_materialized mergeInto" + " ---")
	var tplData = tableWithFiles{Table: &tableNoValues, StagingPath: "test-staging-path", Files: []string{"file2", "file3"}}
	require.NoError(t, tplMergeInto.Execute(&snap, &tplData))
	snap.WriteString("--- End " + "target_table_no_values_materialized mergeInto" + " ---\n\n")

	cupaloy.SnapshotT(t, snap.String())
}
