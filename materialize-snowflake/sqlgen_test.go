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
	"github.com/google/uuid"
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
		withUUID := TableWithUUID{
			Table:      &tbl,
			RandomUUID: uuid.UUID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}.String(),
		}

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

			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(&snap, &withUUID))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	addCols := []sqlDriver.Column{
		{Identifier: "first_new_column", MappedType: sqlDriver.MappedType{NullableDDL: "STRING"}},
		{Identifier: "second_new_column", MappedType: sqlDriver.MappedType{NullableDDL: "BOOL"}},
	}
	dropNotNulls := []sqlDriver.Column{
		{Identifier: "first_required_column", MappedType: sqlDriver.MappedType{NullableDDL: "STRING"}},
		{Identifier: "second_required_column", MappedType: sqlDriver.MappedType{NullableDDL: "BOOL"}},
	}

	for _, testcase := range []struct {
		name         string
		addColumns   []sqlDriver.Column
		dropNotNulls []sqlDriver.Column
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

	tableNoValuesWithUUID := TableWithUUID{
		Table:      &tableNoValues,
		RandomUUID: uuid.UUID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}.String(),
	}

	snap.WriteString("--- Begin " + "target_table_no_values_materialized mergeInto" + " ---")
	require.NoError(t, templates["mergeInto"].Execute(&snap, &tableNoValuesWithUUID))
	snap.WriteString("--- End " + "target_table_no_values_materialized mergeInto" + " ---\n\n")

	var fence = sqlDriver.Fence{
		TablePath:       sqlDriver.TablePath{"path", "To", "checkpoints"},
		Checkpoint:      []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		Fence:           123,
		Materialization: pf.Materialization("some/Materialization"),
		KeyBegin:        0x00112233,
		KeyEnd:          0xffeeddcc,
	}
	snap.WriteString("--- Begin Fence Update ---")
	require.NoError(t, templates["updateFence"].Execute(&snap, fence))
	snap.WriteString("--- End Fence Update ---\n")

	cupaloy.SnapshotT(t, snap.String())
}
