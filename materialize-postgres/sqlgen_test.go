package main

import (
	"strings"
	"testing"
	"text/template"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

var testDialect = createPgDialect(featureFlagDefaults)
var testTemplates = renderTemplates(testDialect)

func TestSQLGeneration(t *testing.T) {

	snap, _ := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string) []string {
			return []string{table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				testTemplates.createTargetTable,
				testTemplates.createLoadTable,
				testTemplates.loadInsert,
				testTemplates.loadQuery,
				testTemplates.loadQueryNoFlowDocumen,
				testTemplates.storeInsert,
				testTemplates.storeUpdate,
				testTemplates.deleteQuery,
			},
			TplAddColumns:    testTemplates.alterTableColumns,
			TplDropNotNulls:  testTemplates.alterTableColumns,
			TplCombinedAlter: testTemplates.alterTableColumns,
			TplInstallFence:  testTemplates.installFence,
			TplUpdateFence:   testTemplates.updateFence,
		},
	)

	cupaloy.SnapshotT(t, snap.String())
}

func TestDateTimeColumn(t *testing.T) {

	var mapped = testDialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
		},
	}, sql.FieldConfig{})
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

func TestLoadQueryNoFlowDocumentTemplate(t *testing.T) {
	// Create a test table with root-level columns
	table := sql.Table{
		TableShape: sql.TableShape{
			Path:    []string{"test_schema", "test_table"},
			Binding: 0,
		},
		Identifier: `"test_schema"."test_table"`,
		Keys: []sql.Column{
			{
				Projection: sql.Projection{
					Projection: pf.Projection{Field: "id", Ptr: "/id"},
				},
				Identifier: `"id"`,
			},
		},
		Values: []sql.Column{
			{
				Projection: sql.Projection{
					Projection: pf.Projection{Field: "name", Ptr: "/name"},
				},
				Identifier: `"name"`,
			},
			{
				Projection: sql.Projection{
					Projection: pf.Projection{Field: "age", Ptr: "/age"},
				},
				Identifier: `"age"`,
			},
			{
				Projection: sql.Projection{
					Projection: pf.Projection{Field: "nested_field", Ptr: "/nested/field"},
				},
				Identifier: `"nested_field"`,
			},
		},
	}

	// Test the loadQueryNoFlowDocument template
	result, err := sql.RenderTableTemplate(table, tplLoadQueryNoFlowDocument)
	require.NoError(t, err)

	cupaloy.SnapshotT(t, result)

	// Verify that nested fields are not included in the JSON reconstruction
	require.Contains(t, result, `'id', r."id"`)
	require.Contains(t, result, `'name', r."name"`)
	require.Contains(t, result, `'age', r."age"`)
	require.NotContains(t, result, "nested_field") // Should not be included as it's not root-level
}
