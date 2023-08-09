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

	table1, err := sqlDriver.ResolveTable(shape1, rsDialect)
	require.NoError(t, err)
	table2, err := sqlDriver.ResolveTable(shape2, rsDialect)
	require.NoError(t, err)

	var snap strings.Builder

	for _, tpl := range []*template.Template{
		tplCreateTargetTable,
		tplCreateStoreTable,
		tplStoreUpdateDeleteExisting,
		tplStoreUpdate,
		tplLoadQuery,
	} {
		for _, tbl := range []sqlDriver.Table{table1, table2} {
			var testcase = tbl.Identifier + " " + tpl.Name()

			snap.WriteString("--- Begin " + testcase + " ---")
			require.NoError(t, tpl.Execute(&snap, &tbl))
			snap.WriteString("--- End " + testcase + " ---\n\n")
		}
	}

	for _, tbl := range []sqlDriver.Table{table1, table2} {
		tpl := tplCreateLoadTable
		var testcase = tbl.Identifier + " " + tpl.Name()

		data := loadTableParams{
			Target: tbl,
		}

		snap.WriteString("--- Begin " + testcase + " (no varchar length) ---")
		require.NoError(t, tpl.Execute(&snap, data))
		snap.WriteString("--- End " + testcase + " (no varchar length) ---\n\n")
	}

	for _, tbl := range []sqlDriver.Table{table1, table2} {
		tpl := tplCreateLoadTable
		var testcase = tbl.Identifier + " " + tpl.Name()

		data := loadTableParams{
			Target:        tbl,
			VarCharLength: 400,
		}

		snap.WriteString("--- Begin " + testcase + " (with varchar length) ---")
		require.NoError(t, tpl.Execute(&snap, data))
		snap.WriteString("--- End " + testcase + " (with varchar length) ---\n\n")
	}

	var fence = sqlDriver.Fence{
		TablePath:       sqlDriver.TablePath{"path", "To", "checkpoints"},
		Checkpoint:      []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		Fence:           123,
		Materialization: pf.Materialization("some/Materialization"),
		KeyBegin:        0x00112233,
		KeyEnd:          0xffeeddcc,
	}

	snap.WriteString("--- Begin Fence Update ---")
	require.NoError(t, tplUpdateFence.Execute(&snap, fence))
	snap.WriteString("--- End Fence Update ---\n\n")

	var copyParams = copyFromS3Params{
		Destination:    "my_temp_table",
		ObjectLocation: "s3://some_bucket",
		Config: config{
			AWSAccessKeyID:     "accessKeyID",
			AWSSecretAccessKey: "secretKey",
			Region:             "us-somewhere-1",
		},
		TruncateColumns: true,
	}

	snap.WriteString("--- Begin Copy From S3 With Truncation---")
	require.NoError(t, tplCopyFromS3.Execute(&snap, copyParams))
	snap.WriteString("--- End Copy From S3 With Truncation ---\n\n")

	copyParams.TruncateColumns = false

	snap.WriteString("--- Begin Copy From S3 Without Truncation---")
	require.NoError(t, tplCopyFromS3.Execute(&snap, copyParams))
	snap.WriteString("--- End Copy From S3 Without Truncation ---")

	cupaloy.SnapshotT(t, snap.String())
}
