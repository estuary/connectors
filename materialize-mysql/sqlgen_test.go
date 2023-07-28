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

func TestSQLGeneration(t *testing.T) {
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

	table1, err := sqlDriver.ResolveTable(shape1, mysqlDialect)
	require.NoError(t, err)
	table2, err := sqlDriver.ResolveTable(shape2, mysqlDialect)
	require.NoError(t, err)

	var snap strings.Builder

	for _, tpl := range []*template.Template{
		tplCreateTargetTable,
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

	var shapeNoValues = sqlDriver.BuildTableShape(spec, 2, tableConfig{
		Table:  "target_table_no_values_materialized",
		Delta:  false,
	})
	tableNoValues, err := sqlDriver.ResolveTable(shapeNoValues, mysqlDialect)
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
	var mapped, err = mysqlDialect.MapType(&sqlDriver.Projection{
		Projection: pf.Projection{
			Inference: pf.Inference{
				Types:   []string{"string"},
				String_: &pf.Inference_String{Format: "date-time"},
				Exists:  pf.Inference_MUST,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, "TIMESTAMP NOT NULL", mapped.DDL)

	parsed, err := mapped.Converter("2022-04-04T10:09:08.234567Z")
	require.Equal(t, "2022-04-04T10:09:08.234567", parsed)
	require.NoError(t, err)
}

func TestTimeColumn(t *testing.T) {
	var mapped, err = mysqlDialect.MapType(&sqlDriver.Projection{
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

	parsed, err := mapped.Converter("10:09:08.234567Z")
	require.Equal(t, "10:09:08.234567", parsed)
	require.NoError(t, err)
}
