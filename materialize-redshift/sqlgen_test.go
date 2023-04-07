package main

import (
	"database/sql"
	"strings"
	"testing"
	"text/template"
	"fmt"
	"encoding/json"

	"github.com/bradleyjkemp/cupaloy"
	sqlDriver "github.com/estuary/connectors/materialize-sql"
	pf "github.com/estuary/flow/go/protocols/flow"
	"github.com/stretchr/testify/require"
)

func TestSQLGeneration(t *testing.T) {
	var spec *pf.MaterializationSpec
	var specJson, err = os.ReadFile("testdata/spec.json")
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(specJson, &spec); err != nil {
		panic(err)
	}

	// TODO(whb): These keys are manually set as "nullable" for now to test the query generation
	// with nullable keys. Once flow supports nullable keys natively, this should be cleaned up.
	spec.Bindings[0].Collection.Projections[7].Inference.Exists = pf.Inference_MAY
	spec.Bindings[0].Collection.Projections[7].Inference.Types = append(spec.Bindings[0].Collection.Projections[7].Inference.Types, "null")
	spec.Bindings[1].Collection.Projections[2].Inference.Exists = pf.Inference_MAY
	spec.Bindings[1].Collection.Projections[2].Inference.Types = append(spec.Bindings[1].Collection.Projections[2].Inference.Types, "null")

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
		tplCreateLoadTable,
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
	}

	snap.WriteString("--- Begin Copy From S3 ---")
	require.NoError(t, tplCopyFromS3.Execute(&snap, copyParams))
	snap.WriteString("--- End Copy From S3 ---\n\n")

	cupaloy.SnapshotT(t, snap.String())
}
