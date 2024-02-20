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

var testDialect = starburstDialect

func TestSQLGeneration(t *testing.T) {
	var spec *pf.MaterializationSpec
	var specJson, err = os.ReadFile("testdata/spec.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(specJson, &spec))

	var shape1 = sqlDriver.BuildTableShape(spec, 0, tableConfig{
		Schema: "a-schema",
		Table:  "target_table",
	})

	table1, err := sqlDriver.ResolveTable(shape1, testDialect)
	require.NoError(t, err)

	templates := renderTemplates(testDialect)

	var snap strings.Builder

	for _, tpl := range []*template.Template{
		templates.fetchVersionAndSpec,
		templates.createTargetTable,
		templates.createLoadTempTable,
		templates.dropLoadTempTable,
		templates.loadQuery,
		templates.createStoreTempTable,
		templates.dropStoreTempTable,
		templates.mergeIntoTarget,
	} {
		var testcase = table1.Identifier + " " + tpl.Name()

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(&snap, &table1))
		snap.WriteString("--- End " + testcase + " ---\n\n")
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
	})
	tableNoValues, err := sqlDriver.ResolveTable(shapeNoValues, testDialect)
	require.NoError(t, err)

	snap.WriteString("--- Begin " + "target_table_no_values_materialized mergeInto" + " ---")
	require.NoError(t, templates.mergeIntoTarget.Execute(&snap, &tableNoValues))
	snap.WriteString("--- End " + "target_table_no_values_materialized mergeInto" + " ---\n\n")

	cupaloy.SnapshotT(t, snap.String())
}
