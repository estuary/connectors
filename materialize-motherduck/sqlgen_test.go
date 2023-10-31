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

	var shape = sqlDriver.BuildTableShape(spec, 1, tableConfig{
		Table:    "delta_updates",
		Schema:   "schema",
		Delta:    true,
		database: "db",
	})
	shape.Document = nil

	table, err := sqlDriver.ResolveTable(shape, duckDialect)
	require.NoError(t, err)

	var snap strings.Builder

	for _, tpl := range []*template.Template{
		tplCreateTargetTable,
		tplStoreQuery,
	} {
		var testcase = table.Identifier + " " + tpl.Name()

		snap.WriteString("--- Begin " + testcase + " ---")
		require.NoError(t, tpl.Execute(&snap, &s3Params{
			Table:  table,
			Bucket: "a-bucket",
			Key:    "key.jsonl",
		}))
		snap.WriteString("--- End " + testcase + " ---\n\n")
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
