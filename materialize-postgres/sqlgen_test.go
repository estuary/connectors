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
				testTemplates.loadQueryNoFlowDocument,
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

	// Exercise the source-postgres -> materialize-postgres JSONB round-trip:
	// a value field carrying the application/vnd.postgresql.jsonb+json
	// contentMediaType must render as JSONB, while a sibling field without
	// the annotation stays on JSON.
	jsonbTable := buildJSONBTestTable(t)
	jsonbName := "createTargetTable [jsonb contentMediaType]"
	snap.WriteString("--- Begin " + jsonbName + " ---\n")
	require.NoError(t, testTemplates.createTargetTable.Execute(snap, &jsonbTable))
	snap.WriteString("--- End " + jsonbName + " ---\n\n")

	cupaloy.SnapshotT(t, snap.String())
}

// buildJSONBTestTable assembles a synthetic Table with two value projections:
// one carrying the jsonb contentMediaType so it should map to JSONB, and one
// without so it should default to JSON.
func buildJSONBTestTable(t *testing.T) sql.Table {
	t.Helper()

	const jsonbMediaType = "application/vnd.estuary.postgresql.jsonb+json"
	multipleTypes := []string{"object", "string", "array", "number", "boolean", "null"}

	mkValue := func(field, contentType string) sql.Column {
		p := sql.Projection{
			Projection: pf.Projection{
				Field: field,
				Ptr:   "/" + field,
				Inference: pf.Inference{
					Types:            multipleTypes,
					ContentMediaType: contentType,
					Exists:           pf.Inference_MAY,
				},
			},
		}
		return sql.Column{
			Projection: p,
			MappedType: testDialect.MapType(&p, sql.FieldConfig{}),
			Identifier: testDialect.Identifier(field),
		}
	}

	keyProj := sql.Projection{
		Projection: pf.Projection{
			Field:        "id",
			Ptr:          "/id",
			IsPrimaryKey: true,
			Inference: pf.Inference{
				Types:  []string{"integer"},
				Exists: pf.Inference_MUST,
			},
		},
	}
	keyCol := sql.Column{
		Projection: keyProj,
		MappedType: testDialect.MapType(&keyProj, sql.FieldConfig{}),
		Identifier: testDialect.Identifier("id"),
		MustExist:  true,
	}

	tableName := "jsonb_round_trip"
	return sql.Table{
		TableShape: sql.TableShape{
			Path:    []string{"public", tableName},
			Comment: "Generated for materialize-postgres jsonb round-trip test",
		},
		Identifier: testDialect.Identifier("public", tableName),
		Keys:       []sql.Column{keyCol},
		Values: []sql.Column{
			mkValue("plain_json", ""),
			mkValue("jsonb_col", jsonbMediaType),
		},
	}
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

func TestJSONBContentMediaType(t *testing.T) {
	jsonbMediaType := "application/vnd.estuary.postgresql.jsonb+json"

	mapWithMediaType := func(types []string, contentType string) string {
		return testDialect.MapType(&sql.Projection{
			Projection: pf.Projection{
				Inference: pf.Inference{
					Types:            types,
					ContentMediaType: contentType,
					Exists:           pf.Inference_MUST,
				},
			},
		}, sql.FieldConfig{}).DDL
	}

	require.Equal(t,
		"JSONB NOT NULL",
		mapWithMediaType([]string{"object", "string", "array", "number", "boolean"}, jsonbMediaType),
		"MULTIPLE-typed field with jsonb contentMediaType should map to JSONB")
	require.Equal(t,
		"JSON NOT NULL",
		mapWithMediaType([]string{"object", "string", "array", "number", "boolean"}, "application/json"),
		"MULTIPLE-typed field with application/json contentMediaType should map to JSON")
	require.Equal(t,
		"JSON NOT NULL",
		mapWithMediaType([]string{"object", "string", "array", "number", "boolean"}, ""),
		"MULTIPLE-typed field without contentMediaType should default to JSON")
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
