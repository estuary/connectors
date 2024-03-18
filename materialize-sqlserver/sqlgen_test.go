package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"text/template"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	sqlDriver "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

var testDialect = sqlServerDialect("Latin1_General_100_BIN2_UTF8", "dbo")

func TestSQLGeneration(t *testing.T) {
	var dialect = testDialect
	var templates = renderTemplates(dialect)

	var spec *pf.MaterializationSpec
	var specJson, err = os.ReadFile("testdata/spec.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(specJson, &spec))

	var shape1 = sqlDriver.BuildTableShape(spec, 0, tableConfig{
		Table: "target_table",
		Delta: false,
	})
	var shape2 = sqlDriver.BuildTableShape(spec, 1, tableConfig{
		Table: "Delta Updates",
		Delta: true,
	})
	shape2.Document = nil // TODO(johnny): this is a bit gross.

	table1, err := sqlDriver.ResolveTable(shape1, dialect)
	require.NoError(t, err)
	table2, err := sqlDriver.ResolveTable(shape2, dialect)
	require.NoError(t, err)

	var snap strings.Builder

	for _, tpl := range []*template.Template{
		templates.tempLoadTableName,
		templates.tempStoreTableName,
		templates.tempLoadTruncate,
		templates.tempStoreTruncate,
		templates.createLoadTable,
		templates.createStoreTable,
		templates.createTargetTable,
		templates.directCopy,
		templates.mergeInto,
		templates.loadInsert,
		templates.loadQuery,
	} {
		for _, tbl := range []sqlDriver.Table{table1, table2} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---\n")
			require.NoError(t, tpl.Execute(&snap, &tbl))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	snap.WriteString("--- Begin alter table add columns ---\n")
	require.NoError(t, templates.alterTableColumns.Execute(&snap, sqlDriver.TableAlter{
		Table: table1,
		AddColumns: []sqlDriver.Column{
			{Identifier: "first_new_column", MappedType: sqlDriver.MappedType{NullableDDL: "STRING"}},
			{Identifier: "second_new_column", MappedType: sqlDriver.MappedType{NullableDDL: "BOOL"}},
		},
	}))
	snap.WriteString("--- End alter table add columns ---\n\n")

	var fence = sqlDriver.Fence{
		TablePath:       sqlDriver.TablePath{"path", "To", "checkpoints"},
		Checkpoint:      []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		Fence:           123,
		Materialization: pf.Materialization("some/Materialization"),
		KeyBegin:        0x00112233,
		KeyEnd:          0xffeeddcc,
	}

	snap.WriteString("--- Begin Fence Update ---\n")
	require.NoError(t, templates.updateFence.Execute(&snap, fence))
	snap.WriteString("--- End Fence Update ---\n")

	cupaloy.SnapshotT(t, snap.String())
}

func TestDateTimeColumn(t *testing.T) {
	var dialect = testDialect
	var mapped, err = dialect.MapType(&sqlDriver.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "DATETIME2 NOT NULL", mapped.DDL)

	expected, err := time.Parse(time.RFC3339Nano, "2022-04-04T10:09:08.234567Z")
	require.NoError(t, err)
	parsed, err := mapped.Converter("2022-04-04T10:09:08.234567Z")
	require.Equal(t, expected, parsed)
	require.NoError(t, err)
}

func TestDateTimePKColumn(t *testing.T) {
	var dialect = testDialect
	var mapped, err = dialect.MapType(&sqlDriver.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
			IsPrimaryKey: true,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "DATETIME2 NOT NULL", mapped.DDL)
}

func TestTimeColumn(t *testing.T) {
	var dialect = testDialect
	var mapped, err = dialect.MapType(&sqlDriver.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "time"},
				Exists:  pf.Inference_MUST,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "TIME NOT NULL", mapped.DDL)

	expected, err := time.Parse("15:04:05Z07:00", "10:09:08.234567Z")
	require.NoError(t, err)
	parsed, err := mapped.Converter("10:09:08.234567Z")
	require.Equal(t, expected, parsed)
	require.NoError(t, err)
}
