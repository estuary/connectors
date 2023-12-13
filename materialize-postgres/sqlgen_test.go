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
		Schema: "",
		Table:  "Delta Updates",
		Delta:  true,
	})
	shape2.Document = nil // TODO(johnny): this is a bit gross.

	table1, err := sqlDriver.ResolveTable(shape1, pgDialect)
	require.NoError(t, err)
	table2, err := sqlDriver.ResolveTable(shape2, pgDialect)
	require.NoError(t, err)

	var snap strings.Builder

	for _, tpl := range []*template.Template{
		tplCreateTargetTable,
		tplReplaceTargetTable,
		tplCreateLoadTable,
		tplLoadInsert,
		tplLoadQuery,
		tplStoreInsert,
		tplStoreUpdate,
	} {
		for _, tbl := range []sqlDriver.Table{table1, table2} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---\n")
			require.NoError(t, tpl.Execute(&snap, &tbl))
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
		snap.WriteString("--- Begin " + testcase.name + " ---\n")
		require.NoError(t, tplAlterTableColumns.Execute(&snap, sqlDriver.TableAlter{
			Table:        table1,
			AddColumns:   testcase.addColumns,
			DropNotNulls: testcase.dropNotNulls,
		}))
		snap.WriteString("--- End " + testcase.name + " ---\n\n")
	}

	var shapeNoValues = sqlDriver.BuildTableShape(spec, 2, tableConfig{
		Schema: "",
		Table:  "target_table_no_values_materialized",
		Delta:  false,
	})
	tableNoValues, err := sqlDriver.ResolveTable(shapeNoValues, pgDialect)
	require.NoError(t, err)

	snap.WriteString("--- Begin " + "target_table_no_values_materialized storeUpdate" + " ---\n")
	require.NoError(t, tplStoreUpdate.Execute(&snap, &tableNoValues))
	snap.WriteString("--- End " + "target_table_no_values_materialized storeUpdate" + " ---\n\n")

	var fence = sqlDriver.Fence{
		TablePath:       sqlDriver.TablePath{"path", "To", "checkpoints"},
		Checkpoint:      []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		Fence:           123,
		Materialization: pf.Materialization("some/Materialization"),
		KeyBegin:        0x00112233,
		KeyEnd:          0xffeeddcc,
	}
	snap.WriteString("--- Begin Fence Install ---\n")
	require.NoError(t, tplInstallFence.Execute(&snap, fence))
	snap.WriteString("--- End Fence Install ---\n")

	snap.WriteString("--- Begin Fence Update ---\n")
	require.NoError(t, tplUpdateFence.Execute(&snap, fence))
	snap.WriteString("--- End Fence Update ---\n")

	cupaloy.SnapshotT(t, snap.String())
}

func TestDateTimeColumn(t *testing.T) {
	var mapped, err = pgDialect.MapType(&sqlDriver.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "TIMESTAMPTZ NOT NULL", mapped.DDL)

	parsed, err := mapped.Converter("2022-04-04T10:09:08.234567Z")
	require.Equal(t, "2022-04-04T10:09:08.234567Z", parsed)
	require.NoError(t, err)
}

func TestTruncatedIdentifier(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "no truncation",
			input: "hello",
			want:  "hello",
		},
		{
			name:  "truncate ASCII",
			input: strings.Repeat("a", 64),
			want:  strings.Repeat("a", 63),
		},
		{
			name:  "truncate UTF-8",
			input: strings.Repeat("a", 61) + "码",
			want:  strings.Repeat("a", 61),
		},
		{
			name:  "empty input",
			input: "",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, truncatedIdentifier(tt.input))
		})
	}
}
