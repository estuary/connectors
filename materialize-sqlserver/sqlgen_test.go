package main

import (
	"time"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	sqlDriver "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	var dialect = sqlServerDialect("Latin1_General_100_BIN2_UTF8")
	var templates = renderTemplates(dialect)

	var spec *pf.MaterializationSpec
	var specJson, err = os.ReadFile("testdata/spec.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(specJson, &spec))

	var shape1 = sqlDriver.BuildTableShape(spec, 0, tableConfig{
		Table:  "target_table",
		Delta:  false,
	})
	var shape2 = sqlDriver.BuildTableShape(spec, 1, tableConfig{
		Table:  "Delta Updates",
		Delta:  true,
	})
	shape2.Document = nil // TODO(johnny): this is a bit gross.

	table1, err := sqlDriver.ResolveTable(shape1, dialect)
	require.NoError(t, err)
	table2, err := sqlDriver.ResolveTable(shape2, dialect)
	require.NoError(t, err)

	var snap strings.Builder

	var templateNames = []string{
		"tempLoadTableName",
		"tempStoreTableName",
		"tempLoadTruncate",
		"tempStoreTruncate",
		"createLoadTable",
		"createStoreTable",
		"createTargetTable",
		"directCopy",
		"mergeInto",
		"loadInsert",
		"loadQuery",
	}
	for _, key := range templateNames {
		var tpl = templates[key]
		for _, tbl := range []sqlDriver.Table{table1, table2} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---\n")
			require.NoError(t, tpl.Execute(&snap, &tbl))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	var fence = sqlDriver.Fence{
		TablePath:       sqlDriver.TablePath{"path", "To", "checkpoints"},
		Checkpoint:      []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		Fence:           123,
		Materialization: pf.Materialization("some/Materialization"),
		KeyBegin:        0x00112233,
		KeyEnd:          0xffeeddcc,
	}

	snap.WriteString("--- Begin Fence Update ---\n")
	require.NoError(t, templates["updateFence"].Execute(&snap, fence))
	snap.WriteString("--- End Fence Update ---\n")

	cupaloy.SnapshotT(t, snap.String())
}

func TestDateTimeColumn(t *testing.T) {
	var dialect = sqlServerDialect("Latin1_General_100_BIN2_UTF8")
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
	var dialect = sqlServerDialect("Latin1_General_100_BIN2_UTF8")
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
	var dialect = sqlServerDialect("Latin1_General_100_BIN2_UTF8")
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
