package main

import (
	"encoding/json"
	"github.com/bradleyjkemp/cupaloy"
	"os"
	"strings"
	"testing"
	"text/template"

	sqlDriver "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

var testDialect = starburstDialect("public")

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

	table1, err := sqlDriver.ResolveTable(shape1, testDialect)
	require.NoError(t, err)
	table2, err := sqlDriver.ResolveTable(shape2, testDialect)
	require.NoError(t, err)

	templates := renderTemplates(testDialect)

	var snap strings.Builder

	for _, tbl := range []sqlDriver.Table{table1, table2} {
		for _, tpl := range []*template.Template{
			templates.fetchVersionAndSpec,
			templates.createTargetTable,
			templates.createOrReplaceTargetTable,
			templates.createLoadTempTable,
			templates.dropLoadTempTable,
			templates.loadQuery,
			templates.createStoreTempTable,
			templates.dropStoreTempTable,
			templates.mergeIntoTarget,
		} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(&snap, &tbl))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	snap.WriteString("--- Begin alter table add columns ---")
	type AlterTableTemplateParams struct {
		TableIdentifier  string
		ColumnIdentifier string
		NullableDDL      string
	}
	require.NoError(t, templates.alterTableColumns.Execute(&snap,
		AlterTableTemplateParams{table1.Identifier, "first_new_column", "STRING"}))
	snap.WriteString("--- End alter table add columns ---\n\n")

	var shapeNoValues = sqlDriver.BuildTableShape(spec, 2, tableConfig{
		Table: "target_table_no_values_materialized",
		Delta: false,
	})
	tableNoValues, err := sqlDriver.ResolveTable(shapeNoValues, testDialect)
	require.NoError(t, err)

	snap.WriteString("--- Begin " + "target_table_no_values_materialized mergeInto" + " ---")
	require.NoError(t, templates.mergeIntoTarget.Execute(&snap, &tableNoValues))
	snap.WriteString("--- End " + "target_table_no_values_materialized mergeInto" + " ---\n\n")

	cupaloy.SnapshotT(t, snap.String())
}
