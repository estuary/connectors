package main

import (
	"testing"
	"text/template"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	sql "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

var testDialect = sqlServerDialect("Latin1_General_100_BIN2_UTF8", "dbo")

func TestSQLGeneration(t *testing.T) {
	var templates = renderTemplates(testDialect)

	snap, _ := sql.RunSqlGenTests(
		t,
		testDialect,
		func(table string) []string {
			return []string{table}
		},
		sql.TestTemplates{
			TableTemplates: []*template.Template{
				templates.tempLoadTruncate,
				templates.tempStoreTruncate,
				templates.createLoadTable,
				templates.createStoreTable,
				templates.createTargetTable,
				templates.directCopy,
				templates.mergeInto,
				templates.loadInsert,
				templates.loadQuery,
			},
			TplAddColumns:  templates.alterTableColumns,
			TplUpdateFence: templates.updateFence,
		},
	)

	cupaloy.SnapshotT(t, snap.String())
}

func TestDateTimeColumn(t *testing.T) {
	var dialect = testDialect
	var mapped = dialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
		},
	}, sql.FieldConfig{})
	require.Equal(t, "DATETIME2 NOT NULL", mapped.DDL)

	expected, err := time.Parse(time.RFC3339Nano, "2022-04-04T10:09:08.234567Z")
	require.NoError(t, err)
	parsed, err := mapped.Converter("2022-04-04T10:09:08.234567Z")
	require.Equal(t, expected, parsed)
	require.NoError(t, err)
}

func TestDateTimePKColumn(t *testing.T) {
	var dialect = testDialect
	var mapped = dialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
			IsPrimaryKey: true,
		},
	}, sql.FieldConfig{})
	require.Equal(t, "varchar(900) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL", mapped.DDL)
}

func TestTimeColumn(t *testing.T) {
	var dialect = testDialect
	var mapped = dialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "time"},
				Exists:  pf.Inference_MUST,
			},
		},
	}, sql.FieldConfig{})
	require.Equal(t, "TIME NOT NULL", mapped.DDL)

	expected, err := time.Parse("15:04:05Z07:00", "10:09:08.234567Z")
	require.NoError(t, err)
	parsed, err := mapped.Converter("10:09:08.234567Z")
	require.Equal(t, expected, parsed)
	require.NoError(t, err)
}

func TestBinaryColumn(t *testing.T) {
	var dialect = testDialect
	var mapped = dialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "base64"},
				Exists:  pf.Inference_MUST,
			},
			IsPrimaryKey: false,
		},
	}, sql.FieldConfig{})
	require.Equal(t, "varchar(MAX) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL", mapped.DDL)
}

func TestBinaryPKColumn(t *testing.T) {
	var dialect = testDialect
	var mapped = dialect.MapType(&sql.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "base64"},
				Exists:  pf.Inference_MUST,
			},
			IsPrimaryKey: true,
		},
	}, sql.FieldConfig{})
	require.Equal(t, "varchar(900) COLLATE Latin1_General_100_BIN2_UTF8 NOT NULL", mapped.DDL)
}
