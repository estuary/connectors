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
		Schema:   "a-schema",
		Table:    "target_table",
		Delta:    false,
		database: "default",
	})
	var shape2 = sqlDriver.BuildTableShape(spec, 1, tableConfig{
		Schema:   "a-schema",
		Table:    "Delta Updates",
		Delta:    true,
		database: "default",
	})
	shape2.Document = nil

	table1, err := sqlDriver.ResolveTable(shape1, duckDialect)
	require.NoError(t, err)
	table2, err := sqlDriver.ResolveTable(shape2, duckDialect)
	require.NoError(t, err)

	var snap strings.Builder

	for _, tbl := range []sqlDriver.Table{table1, table2} {
		for _, tpl := range []*template.Template{
			tplCreateTargetTable,
			tplLoadQuery,
			tplStoreDeleteQuery,
			tplStoreQuery,
		} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(&snap, &queryParams{
				Table: tbl,
				Files: []string{"s3://bucket/file1", "s3://bucket/file2"},
			}))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	var fence = sqlDriver.Fence{
		TablePath:       sqlDriver.TablePath{"checkpoints"},
		Checkpoint:      []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		Fence:           123,
		Materialization: pf.Materialization("some/Materialization"),
		KeyBegin:        0x00112233,
		KeyEnd:          0xffeeddcc,
	}

	snap.WriteString("--- Begin Fence Update ---")
	require.NoError(t, tplUpdateFence.Execute(&snap, fence))
	snap.WriteString("--- End Fence Update ---")

	cupaloy.SnapshotT(t, snap.String())
}
