package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"

	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

var targetTableDialect = starburstTrinoDialect
var tempTableDialect = starburstHiveDialect

func TestSQLGeneration(t *testing.T) {
	var spec *pf.MaterializationSpec
	var specJson, err = os.ReadFile("testdata/spec.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(specJson, &spec))

	var shape1 = sql.BuildTableShape(spec.Name.String(), spec.Bindings[0], 0, []string{"a-schema", "target_table"}, false)
	targetTable, err := sql.ResolveTable(shape1, targetTableDialect)
	require.NoError(t, err)
	tempTable, err := sql.ResolveTable(shape1, tempTableDialect)
	require.NoError(t, err)

	targetTableTemplates := renderTemplates(targetTableDialect)
	tempTableTemplates := renderTemplates(tempTableDialect)

	var snap strings.Builder

	for _, tpl := range []*template.Template{
		targetTableTemplates.fetchVersionAndSpec,
		targetTableTemplates.createTargetTable,
		targetTableTemplates.loadQuery,
		targetTableTemplates.mergeIntoTarget,
	} {
		var testcase = targetTable.Identifier + " " + tpl.Name()

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(&snap, &targetTable))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	for _, tpl := range []*template.Template{
		tempTableTemplates.createLoadTempTable,
		tempTableTemplates.dropLoadTempTable,
		tempTableTemplates.createStoreTempTable,
		tempTableTemplates.dropStoreTempTable,
	} {
		var testcase = tempTable.Identifier + " " + tpl.Name()

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(&snap, &tempTable))
		snap.WriteString("--- End " + testcase + " ---\n\n")
	}

	snap.WriteString("--- Begin alter table add columns ---")
	type AlterTableTemplateParams struct {
		TableIdentifier  string
		ColumnIdentifier string
		NullableDDL      string
	}
	require.NoError(t, targetTableTemplates.alterTableColumns.Execute(&snap,
		AlterTableTemplateParams{targetTable.Identifier, "first_new_column", "STRING"}))
	snap.WriteString("--- End alter table add columns ---\n\n")

	var shapeNoValues = sql.BuildTableShape(spec.Name.String(), spec.Bindings[2], 2, []string{"a-schema", "target_table_no_values_materialized"}, false)
	tableNoValues, err := sql.ResolveTable(shapeNoValues, targetTableDialect)
	require.NoError(t, err)

	snap.WriteString("--- Begin " + "target_table_no_values_materialized mergeInto" + " ---")
	require.NoError(t, targetTableTemplates.mergeIntoTarget.Execute(&snap, &tableNoValues))
	snap.WriteString("--- End " + "target_table_no_values_materialized mergeInto" + " ---\n\n")

	cupaloy.SnapshotT(t, snap.String())
}
