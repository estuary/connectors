package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	boilerplate "github.com/estuary/connectors/materialize-boilerplate"
	sqlDriver "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

var testDialect = snowflakeDialect("public")

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
		endpointSchema: "default",
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
			templates["createTargetTable"],
			templates["replaceTargetTable"],
		} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(&snap, &tbl))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}

		for _, tpl := range []*template.Template{
			templates["loadQuery"],
			templates["copyInto"],
			templates["mergeInto"],
		} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			var tf = tableAndFile{
				Table: tbl,
				File:  "test-file",
			}

			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(&snap, &tf))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	addCols := []sqlDriver.Column{
		{Identifier: "first_new_column", MappedType: sqlDriver.MappedType{NullableDDL: "STRING"}},
		{Identifier: "second_new_column", MappedType: sqlDriver.MappedType{NullableDDL: "BOOL"}},
	}
	dropNotNulls := []boilerplate.EndpointField{
		{
			Name:               "first_required_column",
			Nullable:           true,
			Type:               "string",
			CharacterMaxLength: 0,
		},
		{
			Name:               "second_required_column",
			Nullable:           true,
			Type:               "bool",
			CharacterMaxLength: 0,
		},
	}

	for _, testcase := range []struct {
		name         string
		addColumns   []sqlDriver.Column
		dropNotNulls []boilerplate.EndpointField
	}{
		{
			name:         "alter table add columns and drop not nulls",
			addColumns:   addCols,
			dropNotNulls: dropNotNulls,
		},
		{
			name:       "alter table add columns",
			addColumns: addCols,
		},
		{
			name:         "alter table drop not nulls",
			dropNotNulls: dropNotNulls,
		},
	} {
		snap.WriteString("--- Begin " + testcase.name + " ---")
		require.NoError(t, templates["alterTableColumns"].Execute(&snap, sqlDriver.TableAlter{
			Table:        table1,
			AddColumns:   testcase.addColumns,
			DropNotNulls: testcase.dropNotNulls,
		}))
		snap.WriteString("--- End " + testcase.name + " ---\n\n")
	}

	var shapeNoValues = sqlDriver.BuildTableShape(spec, 2, tableConfig{
		Table: "target_table_no_values_materialized",
		Delta: false,
	})
	tableNoValues, err := sqlDriver.ResolveTable(shapeNoValues, testDialect)
	require.NoError(t, err)

	var tf = tableAndFile{
		Table: tableNoValues,
		File:  "test-file",
	}

	snap.WriteString("--- Begin " + "target_table_no_values_materialized mergeInto" + " ---")
	require.NoError(t, templates["mergeInto"].Execute(&snap, &tf))
	snap.WriteString("--- End " + "target_table_no_values_materialized mergeInto" + " ---\n\n")

	cupaloy.SnapshotT(t, snap.String())
}
